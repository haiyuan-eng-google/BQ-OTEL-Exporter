// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/auth/credentials"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/offload"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/quarantinereplay"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

type localCertificates struct {
	ca     *x509.CertPool
	server tls.Certificate
	client tls.Certificate
	config string
	leaf   []byte
}

func newLocalCertificates(t *testing.T) localCertificates {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(mustParseCertificate(t, caDER))
	makeLeaf := func(serial int64, usage x509.ExtKeyUsage) (tls.Certificate, []byte, []byte) {
		leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "loopback"},
			NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &leafKey.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		rawKey, err := x509.MarshalPKCS8PrivateKey(leafKey)
		if err != nil {
			t.Fatal(err)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: rawKey})
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatal(err)
		}
		return cert, certPEM, keyPEM
	}
	server, _, _ := makeLeaf(2, x509.ExtKeyUsageServerAuth)
	client, certPEM, keyPEM := makeLeaf(3, x509.ExtKeyUsageClientAuth)
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writePrivateFile(t, certFile, certPEM)
	writePrivateFile(t, keyFile, keyPEM)
	config := filepath.Join(dir, "certificate_config.json")
	writePrivateFile(t, config, []byte(fmt.Sprintf(`{"cert_configs":{"workload":{"cert_path":%q,"key_path":%q}}}`, certFile, keyFile)))
	return localCertificates{ca: pool, server: server, client: client, config: config, leaf: client.Certificate[0]}
}

func mustParseCertificate(t *testing.T, der []byte) *x509.Certificate {
	t.Helper()
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func writePrivateFile(t *testing.T, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(name, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func localMTLSServer(t *testing.T, certs localCertificates, handler http.Handler) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	return localMTLSServerProtocol(t, certs, handler, "http1")
}

func localMTLSServerProtocol(t *testing.T, certs localCertificates, handler http.Handler, protocol string) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	closed := make(chan struct{})
	var once sync.Once
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = protocol == "http2"
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certs.server}, ClientCAs: certs.ca, ClientAuth: tls.RequireAndVerifyClientCert}
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			once.Do(func() { close(closed) })
		}
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	original := http.DefaultTransport
	base := original.(*http.Transport).Clone()
	base.TLSClientConfig = &tls.Config{RootCAs: certs.ca}
	http.DefaultTransport = base
	t.Cleanup(func() { base.CloseIdleConnections(); http.DefaultTransport = original })
	return server, closed
}

func waitForPoolClose(t *testing.T, closed <-chan struct{}) {
	t.Helper()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("token connection still open after cleanup")
	}
}

// Default certificate providers are process singletons. Each scenario must start
// before any other test initializes those providers, including in integration builds.
func TestGCSClientCertificates(t *testing.T) {
	if source := os.Getenv("BQ_TEST_GCS_CERT_SOURCE"); source != "" {
		testGCSClientCertificates(t, source)
		return
	}
	for _, source := range []string{"explicit", "ambient"} {
		t.Run(source, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestGCSClientCertificates$", "-test.v")
			cmd.Env = append(os.Environ(), "BQ_TEST_GCS_CERT_SOURCE="+source)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("certificate subprocess: %v\n%s", err, output)
			}
		})
	}
}

func testGCSClientCertificates(t *testing.T, source string) {
	t.Helper()
	certs := newLocalCertificates(t)
	t.Setenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", "true")
	t.Setenv("GOOGLE_API_CERTIFICATE_CONFIG", certs.config)
	server, _ := localMTLSServer(t, certs, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = fmt.Fprint(w, `{"generation":"1"}`)
	}))
	baseline := goleak.IgnoreCurrent()
	var calls atomic.Int64
	opts := []option.ClientOption{option.WithEndpoint(server.URL), option.WithoutAuthentication()}
	if source == "explicit" {
		opts = append(opts, option.WithClientCertSource(func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { calls.Add(1); return &certs.client, nil }))
	}
	for _, operation := range []string{"offload", "replay"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if operation == "offload" {
				store, err := offload.NewGCSBlobStore(ctx, opts...)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				if _, err := store.WriteObject(ctx, "bucket", "object", "text/plain", []byte("payload")); err != nil {
					t.Fatalf("mTLS upload: %v (certificate callbacks=%d)", err, calls.Load())
				}
			} else {
				fetch, cleanup, err := quarantinereplay.NewGCSObjectFetcher(ctx, opts...)
				if err != nil {
					t.Fatal(err)
				}
				defer cleanup()
				if _, err := fetch(ctx, "gs://bucket/object"); err != nil {
					t.Fatalf("mTLS download: %v (certificate callbacks=%d)", err, calls.Load())
				}
			}
		})
	}
	if source == "explicit" && calls.Load() < 2 {
		t.Errorf("certificate callbacks=%d, want at least two", calls.Load())
	}
	goleak.VerifyNone(t, baseline)
}

func TestWorkloadCertificateSelectionAndCleanup(t *testing.T) {
	if scenario := os.Getenv("BQ_TEST_WORKLOAD_SCENARIO"); scenario != "" {
		parts := strings.Split(scenario, "/")
		testWorkloadCertificateSelectionAndCleanup(t, parts[0], parts[1], parts[2])
		return
	}
	for _, location := range []string{"default", "explicit"} {
		for _, input := range []string{"json", "adc"} {
			for _, protocol := range []string{"http1", "http2"} {
				scenario := location + "/" + input + "/" + protocol
				t.Run(scenario, func(t *testing.T) {
					cmd := exec.Command(os.Args[0], "-test.run=^TestWorkloadCertificateSelectionAndCleanup$", "-test.v")
					cmd.Env = append(os.Environ(), "BQ_TEST_WORKLOAD_SCENARIO="+scenario)
					if output, err := cmd.CombinedOutput(); err != nil {
						t.Fatalf("workload subprocess: %v\n%s", err, output)
					}
				})
			}
		}
	}
}

func testWorkloadCertificateSelectionAndCleanup(t *testing.T, location, input, protocol string) {
	t.Helper()
	t.Setenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false")
	certs := newLocalCertificates(t)
	trustGeneratedRoots(t, certs)
	// An explicit credential location must win over the ambient configuration.
	ambient := certs.config
	if location == "explicit" {
		ambient = filepath.Join(t.TempDir(), "missing-config.json")
	}
	t.Setenv("GOOGLE_API_CERTIFICATE_CONFIG", ambient)
	var exchanges atomic.Int64
	server, closed := localMTLSServerProtocol(t, certs, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exchanges.Add(1)
		if (r.ProtoMajor == 2) != (protocol == "http2") {
			t.Errorf("token protocol=%s", r.Proto)
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		var chain []string
		if err := json.Unmarshal([]byte(r.Form.Get("subject_token")), &chain); err != nil {
			t.Error(err)
		}
		if len(chain) != 1 || chain[0] != base64.StdEncoding.EncodeToString(certs.leaf) {
			t.Errorf("unexpected subject chain: %v", chain)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"access_token":"workload-token","token_type":"Bearer","expires_in":3600}`)
	}), protocol)
	baseline := goleak.IgnoreCurrent()
	certificate := `{"use_default_certificate_config":true}`
	if location == "explicit" {
		certificate = fmt.Sprintf(`{"certificate_config_location":%q}`, certs.config)
	}
	data := []byte(fmt.Sprintf(`{"type":"external_account","audience":"test-audience","subject_token_type":"urn:ietf:params:oauth:token-type:mtls","token_url":%q,"credential_source":{"certificate":%s}}`, server.URL, certificate))
	if input == "adc" {
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", writeCredentialsFile(t, string(data)))
		data = nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	creds, cleanup, err := googlehttp.Credentials(credentials.DetectOptions{Scopes: []string{scopeRead}}, data)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	token, err := creds.Token(ctx)
	if err != nil {
		t.Fatalf("workload certificate token exchange: %v", err)
	}
	if token.Value != "workload-token" || exchanges.Load() != 1 {
		t.Fatalf("token=%v exchanges=%d", token, exchanges.Load())
	}
	cleanup()
	waitForPoolClose(t, closed)
	goleak.VerifyNone(t, baseline)
}

func TestWriterWorkloadCertificate(t *testing.T) {
	if os.Getenv("BQ_TEST_WRITER_WORKLOAD") == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestWriterWorkloadCertificate$", "-test.v")
		cmd.Env = append(os.Environ(), "BQ_TEST_WRITER_WORKLOAD=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("writer subprocess: %v\n%s", err, output)
		}
		return
	}
	t.Setenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false")
	certs := newLocalCertificates(t)
	trustGeneratedRoots(t, certs)
	t.Setenv("GOOGLE_API_CERTIFICATE_CONFIG", certs.config)
	server, closed := localMTLSServer(t, certs, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"access_token":"writer-token","token_type":"Bearer","expires_in":3600}`)
	}))
	data := fmt.Sprintf(`{"type":"external_account","audience":"test-audience","subject_token_type":"urn:ietf:params:oauth:token-type:mtls","token_url":%q,"credential_source":{"certificate":{"use_default_certificate_config":true}}}`, server.URL)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", writeCredentialsFile(t, data))
	e := newTestSignalExporter(validConfig(), "traces", schema.SpansTableSchema())
	ctx, cancel := context.WithCancel(context.Background())
	e.lifetimeCancel = cancel
	defer func() { _ = e.shutdownStatus(context.Background(), false) }()
	opts, err := e.writerClientOptions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := internaloption.AuthCreds(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creds.Token(ctx); err != nil {
		t.Fatalf("writer workload certificate token exchange: %v", err)
	}
	if err := e.shutdownStatus(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	waitForPoolClose(t, closed)
}

func trustGeneratedRoots(t *testing.T, certs localCertificates) {
	t.Helper()
	// Workload detection constructs its own TLS config. A fresh subprocess lets
	// the standard verifier trust our generated CA without changing system roots.
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",x509usefallbackroots=1")
	x509.SetFallbackRoots(certs.ca)
}

func TestAPIClientPreservesTLSDialer(t *testing.T) {
	certs := newLocalCertificates(t)
	server, closed := localMTLSServer(t, certs, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, "ok") }))
	base := http.DefaultTransport.(*http.Transport)
	var calls atomic.Int64
	dialer := &tls.Dialer{Config: &tls.Config{RootCAs: certs.ca, Certificates: []tls.Certificate{certs.client}}}
	base.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		calls.Add(1)
		return dialer.DialContext(ctx, network, address)
	}
	client, _, cleanup, err := googlehttp.NewClient(context.Background(), nil, option.WithoutAuthentication(), option.WithEndpoint(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	cleanup()
	waitForPoolClose(t, closed)
	if calls.Load() != 1 {
		t.Fatalf("TLS dialer calls=%d, want 1", calls.Load())
	}
}
