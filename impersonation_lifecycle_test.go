// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/httptransport"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

type impersonationLoopbackTransport struct {
	base     http.RoundTripper
	endpoint *url.URL
}

func (r impersonationLoopbackTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	copyReq := req.Clone(req.Context())
	endpoint := *req.URL
	endpoint.Scheme, endpoint.Host = r.endpoint.Scheme, r.endpoint.Host
	copyReq.URL = &endpoint
	return r.base.RoundTrip(copyReq)
}

func impersonationTestCredentials(t *testing.T, handler http.Handler) (*signalExporter, *auth.Credentials, *atomic.Int64) {
	t.Helper()
	var sourceRequests atomic.Int64
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sourceRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"access_token":"source-token","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(sourceServer.Close)
	subject := filepath.Join(t.TempDir(), "subject-token")
	writePrivateFile(t, subject, []byte("subject"))
	data := fmt.Sprintf(`{"type":"external_account","audience":"test-audience","subject_token_type":"urn:ietf:params:oauth:token-type:jwt","token_url":%q,"credential_source":{"file":%q}}`, sourceServer.URL, subject)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", writeCredentialsFile(t, data))
	t.Setenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Header.Get("Authorization") != "Bearer source-token" {
			t.Errorf("impersonation Authorization=%q", r.Header.Get("Authorization"))
		}
		if !strings.Contains(r.URL.Path, "serviceAccounts/target@example.com:generateAccessToken") {
			t.Errorf("impersonation path=%q", r.URL.Path)
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	original := newBigQueryHTTPClient
	t.Cleanup(func() { newBigQueryHTTPClient = original })
	newBigQueryHTTPClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, *http.Transport, error) {
		client, base, err := original(ctx, opts...)
		if err == nil {
			client.Transport = impersonationLoopbackTransport{base: client.Transport, endpoint: endpoint}
		}
		return client, base, err
	}
	cfg := validConfig()
	cfg.Credentials.ImpersonateServiceAccount = "target@example.com"
	e := newTestSignalExporter(cfg, "traces", schema.SpansTableSchema())
	lifetime, cancel := context.WithCancel(context.Background())
	e.lifetimeCancel = cancel
	// The exporter passes this context to writerClientOptions during Start.
	opts, err := e.writerClientOptions(lifetime)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.shutdownStatus(context.Background(), false) })
	creds, err := internaloption.AuthCreds(lifetime, opts)
	if err != nil {
		t.Fatal(err)
	}
	// The REST auth middleware must recognize the synchronous cache rather than
	// wrapping it with another cache that refreshes on a background context.
	if _, err := httptransport.NewClient(&httptransport.Options{Credentials: creds, BaseRoundTripper: http.DefaultTransport}); err != nil {
		t.Fatal(err)
	}
	return e, creds, &sourceRequests
}

func TestExplicitImpersonationCancellation(t *testing.T) {
	for _, mode := range []string{"pre-cancelled", "rpc", "shutdown", "after-shutdown", "stale-shutdown", "stale-refresh-shutdown", "stale-refresh-rpc"} {
		t.Run(mode, func(t *testing.T) {
			baseline := append([]goleak.Option{goleak.IgnoreCurrent()}, integrationGoleakOptions()...)
			t.Cleanup(func() { goleak.VerifyNone(t, baseline...) })
			var requests atomic.Int64
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			defer close(release)
			e, creds, sourceRequests := impersonationTestCredentials(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count := requests.Add(1)
				if mode == "pre-cancelled" || mode == "after-shutdown" || mode == "stale-shutdown" || ((mode == "stale-refresh-shutdown" || mode == "stale-refresh-rpc") && count == 1) {
					expiry := time.Now().Add(time.Hour)
					if mode == "stale-shutdown" || mode == "stale-refresh-shutdown" || mode == "stale-refresh-rpc" {
						expiry = time.Now().Add(2 * time.Second)
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprintf(w, `{"accessToken":"impersonated","expireTime":%q}`, expiry.UTC().Format(time.RFC3339))
					return
				}
				started <- struct{}{}
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			switch mode {
			case "pre-cancelled":
				cancel()
				_, err := creds.Token(ctx)
				if !errors.Is(err, context.Canceled) || requests.Load() != 0 || sourceRequests.Load() != 0 {
					t.Fatalf("pre-cancelled token: err=%v source requests=%d impersonation requests=%d", err, sourceRequests.Load(), requests.Load())
				}
			case "after-shutdown":
				if err := e.shutdownStatus(context.Background(), false); err != nil {
					t.Fatal(err)
				}
				_, err := creds.Token(ctx)
				if !errors.Is(err, context.Canceled) || requests.Load() != 0 || sourceRequests.Load() != 0 {
					t.Fatalf("first token after shutdown: err=%v source requests=%d impersonation requests=%d", err, sourceRequests.Load(), requests.Load())
				}
			case "stale-shutdown":
				if _, err := creds.Token(ctx); err != nil {
					t.Fatal(err)
				}
				if err := e.shutdownStatus(context.Background(), false); err != nil {
					t.Fatal(err)
				}
				// A stale or expired token must not trigger a new refresh after cleanup.
				time.Sleep(2100 * time.Millisecond)
				_, err := creds.Token(ctx)
				if !errors.Is(err, context.Canceled) || requests.Load() != 1 {
					t.Fatalf("token after shutdown: err=%v impersonation requests=%d, want cancellation and one request", err, requests.Load())
				}
			default:
				if mode == "stale-refresh-shutdown" || mode == "stale-refresh-rpc" {
					if _, err := creds.Token(ctx); err != nil {
						t.Fatal(err)
					}
				}
				done := make(chan error, 1)
				go func() { _, err := creds.Token(ctx); done <- err }()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("impersonation request did not start")
				}
				if mode == "rpc" || mode == "stale-refresh-rpc" {
					cancel()
				} else if err := e.shutdownStatus(context.Background(), false); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("blocked token error=%v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("blocked impersonation fetch ignored cancellation")
				}
			}
			if err := e.shutdownStatus(context.Background(), false); err != nil {
				t.Fatal(err)
			}
		})
	}
}
