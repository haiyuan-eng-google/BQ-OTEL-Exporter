// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/offload"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// TestSignalExporterRetainsDataPlaneAuthCleanupUntilShutdown verifies that
// closing the startup BigQuery metadata client inside signalExporter.start
// unwinds only the metadata client's authCleanup callbacks, retaining the
// writer and offload authCleanup callbacks until exporter shutdown so HTTP/2
// token-refresh connections opened during data-plane appends or uploads after
// start() returns are closed on shutdown.
func TestSignalExporterRetainsDataPlaneAuthCleanupUntilShutdown(t *testing.T) {
	server, closed := newTransportLifecycleServer(t, "http2", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`)
	}))
	opts := append([]goleak.Option{goleak.IgnoreCurrent()}, integrationGoleakOptions()...)
	subject := filepath.Join(t.TempDir(), "subject-token")
	if err := os.WriteFile(subject, []byte("test-subject"), 0o600); err != nil {
		t.Fatal(err)
	}
	data := fmt.Sprintf(
		`{"type":"external_account","audience":"test-audience","subject_token_type":"urn:ietf:params:oauth:token-type:jwt","token_url":%q,"credential_source":{"file":%q}}`,
		server.URL, subject,
	)

	cfg := validConfig()
	cfg.Credentials.JSON = configopaque.String(data)
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "test-offload-bucket"

	originalAdmin, originalWriter, originalBlob := newDestinationAdmin, newManagedWriterClient, newBlobStore
	t.Cleanup(func() {
		newDestinationAdmin, newManagedWriterClient, newBlobStore = originalAdmin, originalWriter, originalBlob
	})

	var retainedWriterOpts []option.ClientOption
	var retainedOffloadOpts []option.ClientOption
	newDestinationAdmin = func(context.Context, string, string, ...option.ClientOption) (destinationAdmin, error) {
		return &fakeDestinationAdmin{
			tableMetadata: &bq.TableMetadata{
				Schema: mustBigQuerySchema(t, schema.SpansTableSchema()),
			},
		}, nil
	}
	newManagedWriterClient = func(_ context.Context, _ string, clientOpts ...option.ClientOption) (*managedwriter.Client, error) {
		retainedWriterOpts = append([]option.ClientOption(nil), clientOpts...)
		return &managedwriter.Client{}, nil
	}
	newBlobStore = func(_ context.Context, clientOpts ...option.ClientOption) (offload.BlobStore, error) {
		retainedOffloadOpts = append([]option.ClientOption(nil), clientOpts...)
		return offload.NewMemoryBlobStore(), nil
	}

	exp := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())
	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatalf("start() unexpected error: %v", err)
	}
	exp.client = nil
	t.Cleanup(func() { _ = exp.shutdownStatus(context.Background(), false) })

	if len(exp.authCleanup) != 2 {
		t.Fatalf("len(exp.authCleanup) = %d after start(), want 2 (writer + offload authCleanup retained after metadata cleanup)", len(exp.authCleanup))
	}

	// Simulate data-plane token fetches for both writer and offload occurring after start() has returned.
	for _, tc := range []struct {
		name string
		opts []option.ClientOption
	}{
		{"writer", retainedWriterOpts},
		{"offload", retainedOffloadOpts},
	} {
		creds, err := internaloption.AuthCreds(context.Background(), tc.opts)
		if err != nil {
			t.Fatalf("%s internaloption.AuthCreds after start() failed: %v", tc.name, err)
		}
		if _, err := creds.Token(context.Background()); err != nil {
			t.Fatalf("%s creds.Token() after start() failed: %v", tc.name, err)
		}
	}

	if err := exp.shutdownStatus(context.Background(), false); err != nil {
		t.Fatalf("shutdown() unexpected error: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("token connection still open after exporter shutdown")
	}
	goleak.VerifyNone(t, opts...)
}
