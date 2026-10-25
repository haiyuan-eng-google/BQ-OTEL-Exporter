// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package googlehttp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/api/option"
)

type lifecycleConnections struct {
	opened atomic.Int64
	closed atomic.Int64
	done   chan struct{}
	once   sync.Once
}

func (c *lifecycleConnections) state(_ net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		c.opened.Add(1)
	case http.StateClosed:
		c.closed.Add(1)
		c.once.Do(func() { close(c.done) })
	}
}

func lifecycleTLSCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, roots
}

func lifecycleServer(t *testing.T, protocol string, cert tls.Certificate, handler http.Handler) (*httptest.Server, *lifecycleConnections) {
	t.Helper()
	connections := &lifecycleConnections{done: make(chan struct{})}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.ProtoMajor == 2) != (protocol == "http2") {
			t.Errorf("request protocol=%s, want %s", r.Proto, protocol)
		}
		handler.ServeHTTP(w, r)
	}))
	server.EnableHTTP2 = protocol == "http2"
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	server.Config.ConnState = connections.state
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, connections
}

func authenticatedLifecycleRequest(ctx context.Context, client *http.Client, endpoint string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("API status=%d", response.StatusCode)
	}
	return errors.Join(readErr, closeErr)
}

func TestNewClientClosesAuthenticatedTransports(t *testing.T) {
	for _, protocol := range []string{"http1", "http2"} {
		for _, failure := range []string{"success", "transport-error", "endpoint-error"} {
			t.Run(protocol+"/"+failure, func(t *testing.T) {
				t.Setenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false")
				t.Setenv("GOOGLE_CLOUD_UNIVERSE_DOMAIN", "")
				t.Setenv("GOOGLE_API_USE_MTLS_ENDPOINT", "auto")
				cert, roots := lifecycleTLSCertificate(t)
				var tokenRequests, apiRequests atomic.Int64
				tokenServer, tokenConnections := lifecycleServer(t, protocol, cert, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					tokenRequests.Add(1)
					if err := r.ParseForm(); err != nil {
						t.Error(err)
					}
					if r.Form.Get("subject_token") != "test-subject" {
						t.Errorf("subject token=%q", r.Form.Get("subject_token"))
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprint(w, `{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`)
				}))
				apiServer, apiConnections := lifecycleServer(t, protocol, cert, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					apiRequests.Add(1)
					if r.Header.Get("Authorization") != "Bearer test-token" {
						t.Errorf("Authorization=%q", r.Header.Get("Authorization"))
					}
					_, _ = fmt.Fprint(w, "ok")
				}))
				originalBase := http.DefaultTransport
				base := originalBase.(*http.Transport).Clone()
				base.TLSClientConfig = &tls.Config{RootCAs: roots}
				http.DefaultTransport = base
				t.Cleanup(func() { base.CloseIdleConnections(); http.DefaultTransport = originalBase })
				subject := filepath.Join(t.TempDir(), "subject")
				adc := filepath.Join(t.TempDir(), "credentials.json")
				if err := os.WriteFile(subject, []byte("test-subject"), 0o600); err != nil {
					t.Fatal(err)
				}
				data := fmt.Sprintf(`{"type":"external_account","audience":"test-audience","subject_token_type":"urn:ietf:params:oauth:token-type:jwt","token_url":%q,"credential_source":{"file":%q}}`, tokenServer.URL, subject)
				if err := os.WriteFile(adc, []byte(data), 0o600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", adc)
				baseline := goleak.IgnoreCurrent()
				// Normal constructors need no token or API request. Inject failures only
				// after the real authenticated transport has used both pools, exercising
				// cleanup of resources a partially successful constructor might retain.
				sentinel := errors.New("constructor failed after authenticated request")
				originalTransport, originalClient := newAPITransport, newAPIClient
				t.Cleanup(func() { newAPITransport, newAPIClient = originalTransport, originalClient })
				newAPITransport = func(ctx context.Context, base http.RoundTripper, opts ...option.ClientOption) (http.RoundTripper, error) {
					rt, err := originalTransport(ctx, base, opts...)
					if err == nil && failure == "transport-error" {
						if err := authenticatedLifecycleRequest(ctx, &http.Client{Transport: rt}, apiServer.URL); err != nil {
							return nil, err
						}
						return nil, sentinel
					}
					return rt, err
				}
				newAPIClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
					client, endpoint, err := originalClient(ctx, opts...)
					if err == nil && failure == "endpoint-error" {
						if err := authenticatedLifecycleRequest(ctx, client, apiServer.URL); err != nil {
							return nil, "", err
						}
						return nil, "", sentinel
					}
					return client, endpoint, err
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, endpoint, cleanup, err := NewClient(ctx, []string{"test-scope"}, option.WithEndpoint(apiServer.URL))
				if failure == "success" {
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(cleanup)
					if err := authenticatedLifecycleRequest(ctx, client, endpoint); err != nil {
						t.Fatal(err)
					}
					cleanup()
				} else if !errors.Is(err, sentinel) || client != nil || cleanup != nil {
					t.Fatalf("constructor result: client=%v cleanup provided=%t err=%v", client, cleanup != nil, err)
				}
				if tokenRequests.Load() != 1 || apiRequests.Load() != 1 || tokenConnections.opened.Load() != 1 || apiConnections.opened.Load() != 1 {
					t.Fatalf("token requests/connections=%d/%d API requests/connections=%d/%d; want one each", tokenRequests.Load(), tokenConnections.opened.Load(), apiRequests.Load(), apiConnections.opened.Load())
				}
				for _, pool := range []struct {
					name        string
					connections *lifecycleConnections
				}{{"token", tokenConnections}, {"API", apiConnections}} {
					select {
					case <-pool.connections.done:
					case <-time.After(2 * time.Second):
						t.Errorf("%s connection still open after cleanup: opened=%d closed=%d", pool.name, pool.connections.opened.Load(), pool.connections.closed.Load())
					}
					if pool.connections.closed.Load() != 1 {
						t.Errorf("%s closed connections=%d, want 1", pool.name, pool.connections.closed.Load())
					}
				}
				goleak.VerifyNone(t, baseline)
			})
		}
	}
}
