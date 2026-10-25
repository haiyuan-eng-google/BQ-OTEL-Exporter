// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/offload"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/quarantinereplay"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

func TestDestinationAdminCloseReleasesHTTPTransport(t *testing.T) {
	connections := make(chan net.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Keep the peer open without a server goroutine: only the owned
		// client's readLoop/writeLoop can remain after admin.Close.
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		connections <- conn
		_, _ = fmt.Fprint(rw, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")
		_ = rw.Flush()
	}))
	t.Cleanup(server.Close)
	// Only goroutines already running before the client exists (including
	// the test listener) are excluded from this local check.
	opts := append([]goleak.Option{goleak.IgnoreCurrent()}, integrationGoleakOptions()...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	admin, err := newDestinationAdmin(ctx, "test-project", "test-dataset",
		option.WithoutAuthentication(), option.WithEndpoint(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	// Fallback cleanup runs after the assertion, so a failing test does not
	// leave either end of the connection for VerifyTestMain.
	t.Cleanup(admin.(*bigQueryDestinationAdmin).httpBase.CloseIdleConnections)
	if _, err := admin.DatasetMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	conn := <-connections
	t.Cleanup(func() { _ = conn.Close() })
	if err := admin.Close(); err != nil {
		t.Fatal(err)
	}
	goleak.VerifyNone(t, opts...)
}

func TestSignalExporterClosesAuthTransports(t *testing.T) {
	for _, protocol := range []string{"http1", "http2"} {
		for _, source := range []string{"json", "file", "adc"} {
			for _, failStart := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/failStart=%t", protocol, source, failStart), func(t *testing.T) {
					server, closed := newTransportLifecycleServer(t, protocol, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						_, _ = fmt.Fprint(w, `{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`)
					}))
					subject := filepath.Join(t.TempDir(), "subject-token")
					if err := os.WriteFile(subject, []byte("test-subject"), 0o600); err != nil {
						t.Fatal(err)
					}
					data := fmt.Sprintf(`{"type":"external_account","audience":"test-audience","subject_token_type":"urn:ietf:params:oauth:token-type:jwt","token_url":%q,"credential_source":{"file":%q}}`, server.URL, subject)
					cfg := validConfig()
					switch source {
					case "json":
						cfg.Credentials.JSON = configopaque.String(data)
					case "file":
						cfg.Credentials.File = writeCredentialsFile(t, data)
					case "adc":
						t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", writeCredentialsFile(t, data))
					}
					originalAdmin, originalWriter := newDestinationAdmin, newManagedWriterClient
					t.Cleanup(func() {
						newDestinationAdmin, newManagedWriterClient = originalAdmin, originalWriter
					})
					newDestinationAdmin = func(context.Context, string, string, ...option.ClientOption) (destinationAdmin, error) {
						return &fakeDestinationAdmin{tableMetadata: &bq.TableMetadata{Schema: mustBigQuerySchema(t, schema.SpansTableSchema())}}, nil
					}
					startErr := errors.New("writer construction failed after token fetch")
					newManagedWriterClient = func(ctx context.Context, _ string, opts ...option.ClientOption) (*managedwriter.Client, error) {
						creds, err := internaloption.AuthCreds(ctx, opts)
						if err != nil {
							return nil, err
						}
						if _, err := creds.Token(ctx); err != nil {
							return nil, err
						}
						if failStart {
							return nil, startErr
						}
						return &managedwriter.Client{}, nil
					}
					e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())
					err := e.start(context.Background(), nil)
					// The constructor double has no gRPC resources to close.
					e.client = nil
					t.Cleanup(func() { _ = e.shutdownStatus(context.Background(), false) })
					if failStart {
						if !errors.Is(err, startErr) {
							t.Fatalf("start = %v, want %v", err, startErr)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						if err := e.shutdownStatus(context.Background(), false); err != nil {
							t.Fatal(err)
						}
					}
					select {
					case <-closed:
					case <-time.After(3 * time.Second):
						t.Fatal("token connection still open after exporter cleanup")
					}
				})
			}
		}
	}
}

func TestGCSClientsCloseOwnedTransports(t *testing.T) {
	for _, protocol := range []string{"http1", "http2"} {
		for _, client := range []string{"offload", "replay"} {
			t.Run(protocol+"/"+client, func(t *testing.T) {
				server, closed := newTransportLifecycleServer(t, protocol, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					_, _ = fmt.Fprint(w, `{"generation":"1"}`)
				}))
				ctx := context.Background()
				opts := []option.ClientOption{option.WithEndpoint(server.URL), option.WithoutAuthentication()}
				if client == "offload" {
					store, err := offload.NewGCSBlobStore(ctx, opts...)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = store.Close() })
					if _, err := store.WriteObject(ctx, "bucket", "object", "text/plain", []byte("payload")); err != nil {
						t.Fatal(err)
					}
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					fetch, cleanup, err := quarantinereplay.NewGCSObjectFetcher(ctx, opts...)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(cleanup)
					if _, err := fetch(ctx, "gs://bucket/object"); err != nil {
						t.Fatal(err)
					}
					cleanup()
				}
				select {
				case <-closed:
				case <-time.After(3 * time.Second):
					t.Fatal("GCS connection still open after cleanup")
				}
			})
		}
	}
}

func newTransportLifecycleServer(t *testing.T, protocol string, handler http.Handler) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	closed := make(chan struct{})
	var once sync.Once
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.ProtoMajor == 2) != (protocol == "http2") {
			t.Errorf("request protocol = %s", r.Proto)
		}
		handler.ServeHTTP(w, r)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			once.Do(func() { close(closed) })
		}
	}
	if protocol == "http2" {
		server.EnableHTTP2 = true
		server.StartTLS()
		original := http.DefaultTransport
		http.DefaultTransport = server.Client().Transport.(*http.Transport).Clone()
		t.Cleanup(func() { http.DefaultTransport = original })
	} else {
		server.Start()
	}
	t.Cleanup(server.Close)
	return server, closed
}
