// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package testbench

import (
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/api/option"
)

func TestGeneratorAllSevenFrameworksAndMCP(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		PayloadBytes:   8192,
		PlantSecrets:   true,
		UniquePayloads: true,
	})

	// 28 spans = 7 frameworks * 4-span tree (invoke_agent, chat, execute_tool, mcp).
	td := gen.TracesBatch(28)
	if got := td.SpanCount(); got != 28 {
		t.Fatalf("TracesBatch(28) SpanCount = %d, want 28", got)
	}

	seenFrameworks := make(map[string]bool)
	seenOps := make(map[string]int)
	seenMCP := make(map[string]bool)
	var secretSpans int

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		fw, ok := rs.Resource().Attributes().Get("gen_ai.framework")
		if !ok {
			t.Fatalf("resource %d missing gen_ai.framework", i)
		}
		seenFrameworks[fw.Str()] = true
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				if op, ok := sp.Attributes().Get("gen_ai.operation.name"); ok {
					seenOps[op.Str()]++
				}
				if mcpMethod, ok := sp.Attributes().Get("mcp.method.name"); ok {
					seenMCP[mcpMethod.Str()] = true
				}
				if _, ok := sp.Attributes().Get("client.api_key"); ok {
					secretSpans++
				}
				if msg, ok := sp.Attributes().Get("gen_ai.input.messages"); ok {
					if len(msg.Str()) != 8192 {
						t.Fatalf("gen_ai.input.messages len = %d, want 8192", len(msg.Str()))
					}
				}
			}
		}
	}

	if len(seenFrameworks) != 7 {
		t.Fatalf("seenFrameworks = %d (%v), want all 7 frameworks", len(seenFrameworks), seenFrameworks)
	}
	if seenOps["invoke_agent"] == 0 || seenOps["chat"] == 0 || seenOps["execute_tool"] == 0 {
		t.Fatalf("missing expected gen_ai.operation.name values: %v", seenOps)
	}
	if len(seenMCP) == 0 {
		t.Fatal("expected MCP child spans with mcp.method.name attribute")
	}
	if secretSpans == 0 {
		t.Fatal("expected planted secret attributes when PlantSecrets=true")
	}

	ld := gen.LogsBatch(14)
	if got := ld.LogRecordCount(); got != 14 {
		t.Fatalf("LogsBatch(14) LogRecordCount = %d, want 14", got)
	}

	md := gen.MetricsBatch(10)
	if got := md.DataPointCount(); got != 10 {
		t.Fatalf("MetricsBatch(10) DataPointCount = %d, want 10", got)
	}
}

func TestRunBQFakeAllSignalsAndRedaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := RunnerConfig{
		Mode:               ModeBQFake,
		Signals:            []string{"traces", "logs", "metrics"},
		TargetRate:         2000,
		Duration:           250 * time.Millisecond,
		BatchSize:          50,
		Workers:            2,
		StreamPoolSize:     2,
		ConnectionPoolSize: 2,
		Redaction:          true,
		PayloadBytes:       1024,
	}

	report, err := Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}
	if len(report.Results) != 3 {
		t.Fatalf("len(report.Results) = %d, want 3", len(report.Results))
	}

	for _, res := range report.Results {
		if res.Telemetry.AcknowledgedRows <= 0 {
			t.Errorf("signal %s AcknowledgedRows = %d, want > 0", res.Signal, res.Telemetry.AcknowledgedRows)
		}
		if res.Telemetry.AppendBytesTotal <= 0 {
			t.Errorf("signal %s AppendBytesTotal = %d, want > 0", res.Signal, res.Telemetry.AppendBytesTotal)
		}
		if res.AchievedRowsPerSec <= 0 {
			t.Errorf("signal %s AchievedRowsPerSec = %f, want > 0", res.Signal, res.AchievedRowsPerSec)
		}
		if res.Latency.Samples <= 0 {
			t.Errorf("signal %s Latency.Samples = %d, want > 0", res.Signal, res.Latency.Samples)
		}
		if res.Telemetry.RedactedValuesTotal <= 0 {
			t.Errorf("signal %s RedactedValuesTotal = %d, want > 0 when Redaction=true", res.Signal, res.Telemetry.RedactedValuesTotal)
		}
		if res.Telemetry.RedactedByMatchType["key"] <= 0 || res.Telemetry.RedactedByMatchType["value"] <= 0 {
			t.Errorf("signal %s RedactedByMatchType = %v, want key > 0 and value > 0", res.Signal, res.Telemetry.RedactedByMatchType)
		}
	}

	rawJSON, err := FormatJSON(report)
	if err != nil {
		t.Fatalf("FormatJSON() error: %v", err)
	}
	var decoded BenchmarkReport
	if err := json.Unmarshal([]byte(rawJSON), &decoded); err != nil {
		t.Fatalf("json.Unmarshal(FormatJSON) error: %v", err)
	}
	if len(decoded.Results) != 3 {
		t.Fatalf("decoded results = %d, want 3", len(decoded.Results))
	}

	md := FormatMarkdown(report)
	for _, want := range []string{"| `traces` |", "| `logs` |", "| `metrics` |", "Achieved Rows/s"} {
		if !strings.Contains(md, want) {
			t.Errorf("FormatMarkdown missing %q:\n%s", want, md)
		}
	}
}

func TestRunBQFakeBackpressureAndLocalOffload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	offloadDir := t.TempDir()
	cfg := RunnerConfig{
		Mode:                  ModeBQFake,
		Signals:               []string{"traces", "logs"},
		Duration:              300 * time.Millisecond,
		BatchSize:             32,
		Workers:               2,
		StreamPoolSize:        2,
		ConnectionPoolSize:    2,
		Redaction:             true,
		Backpressure:          true,
		SimulateAckDelay:      15 * time.Millisecond,
		SimulateThrottleEvery: 4,
		PayloadBytes:          4096,
		OffloadDir:            offloadDir,
		OffloadThresholdBytes: 2048,
	}

	report, err := Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run() with backpressure and offload error: %v", err)
	}
	for _, res := range report.Results {
		if res.Telemetry.OffloadedPayloads <= 0 {
			t.Errorf("signal %s OffloadedPayloads = %d, want > 0", res.Signal, res.Telemetry.OffloadedPayloads)
		}
		if res.Telemetry.OffloadedBytes <= 0 {
			t.Errorf("signal %s OffloadedBytes = %d, want > 0", res.Signal, res.Telemetry.OffloadedBytes)
		}
		if res.Telemetry.OffloadDedupHits <= 0 {
			t.Errorf("signal %s OffloadDedupHits = %d, want > 0", res.Signal, res.Telemetry.OffloadDedupHits)
		}
		if res.Telemetry.RedactedValuesTotal <= 0 {
			t.Errorf("signal %s RedactedValuesTotal = %d, want > 0 when Redaction=true with local offload", res.Signal, res.Telemetry.RedactedValuesTotal)
		}
		if len(res.Telemetry.BackpressureEvents) == 0 {
			t.Errorf("signal %s expected BackpressureEvents to be recorded", res.Signal)
		}
		if res.Telemetry.BackpressureLevel <= 0 {
			t.Errorf("signal %s BackpressureLevel = %f, want > 0 under simulated ack delay and throttling", res.Signal, res.Telemetry.BackpressureLevel)
		}
	}

	// Verify Step-0 redaction ran before local-dir offload so no cleartext
	// secret patterns ever reached disk, and at least one offloaded blob on
	// disk contains the "[REDACTED]" mask replacement (non-vacuous proof that
	// planted secrets inside >= OffloadThresholdBytes payloads were redacted
	// prior to offload).
	var (
		totalBlobs    int
		redactedBlobs int
	)
	err = filepath.WalkDir(offloadDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		totalBlobs++
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		content := string(raw)
		for _, forbidden := range []string{"sk-proj-", "ya29.", "AKIA", "secret-key-"} {
			if strings.Contains(content, forbidden) {
				t.Errorf("offloaded file %s contains unredacted secret substring %q", path, forbidden)
			}
		}
		if strings.Contains(content, "[REDACTED]") {
			redactedBlobs++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking offloadDir: %v", err)
	}
	if totalBlobs == 0 {
		t.Fatal("expected offloaded files on disk in offloadDir, found 0")
	}
	if redactedBlobs == 0 {
		t.Fatalf("expected >= 1 offloaded file in %s to contain [REDACTED] placeholder, found 0 across %d files", offloadDir, totalBlobs)
	}
}

func TestRunStreamPoolSweepAndValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := RunnerConfig{
		Mode:      ModeBQFake,
		Signals:   []string{"traces"},
		Duration:  150 * time.Millisecond,
		BatchSize: 40,
		Workers:   2,
	}
	report, err := RunStreamPoolSweep(ctx, cfg, []int{1, 2})
	if err != nil {
		t.Fatalf("RunStreamPoolSweep error: %v", err)
	}
	if len(report.Results) != 2 {
		t.Fatalf("len(report.Results) = %d, want 2", len(report.Results))
	}
	if report.Results[0].StreamPoolSize != 1 || report.Results[1].StreamPoolSize != 2 {
		t.Fatalf("unexpected stream pool sizes: %d, %d", report.Results[0].StreamPoolSize, report.Results[1].StreamPoolSize)
	}

	// Verify Normalize auto-generates otel_bench_<timestamp> when EphemeralDataset=true
	// and DatasetID is empty, and allows subsequent Normalize calls on the generated config.
	ephemeralCfg := RunnerConfig{
		Mode:             ModeLive,
		ProjectID:        "test-project",
		EphemeralDataset: true,
	}
	if err := ephemeralCfg.Normalize(); err != nil {
		t.Fatalf("Normalize(ephemeralCfg) unexpected error: %v", err)
	}
	if !strings.HasPrefix(ephemeralCfg.DatasetID, "otel_bench_") {
		t.Fatalf("ephemeralCfg.DatasetID = %q, want prefix otel_bench_", ephemeralCfg.DatasetID)
	}
	if err := ephemeralCfg.Normalize(); err != nil {
		t.Fatalf("second Normalize(ephemeralCfg) unexpected error: %v", err)
	}

	invalidCases := []RunnerConfig{
		{Mode: "invalid"},
		{Mode: ModeBQFake, Signals: []string{"profiles"}},
		{Mode: ModeBQFake, StreamPoolSize: 32},
		{Mode: ModeBQFake, ConnectionPoolSize: 32},
		{Mode: ModeLive, ProjectID: ""},
		{Mode: ModeLive, ProjectID: "test-project", DatasetID: ""},
		{Mode: ModeLive, ProjectID: "test-project", DatasetID: "prod_otel", EphemeralDataset: true},
		{Mode: ModeBQFake, DatasetID: "prod_otel", EphemeralDataset: true},
		{Mode: ModeBQFake, OffloadDir: "/tmp/a", OffloadGCSBucket: "bkt"},
		{Mode: ModeBQFake, OffloadGCSBucket: "bkt"},
	}
	for _, tc := range invalidCases {
		if err := tc.Normalize(); err == nil {
			t.Errorf("Normalize(%+v) succeeded, want error", tc)
		}
	}
}

func TestDeleteDatasetWithContentsAndDatasetExistsHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		getCalls    atomic.Int32
		deleteCalls atomic.Int32
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getCalls.Add(1)
			switch {
			case strings.HasSuffix(r.URL.Path, "/datasets/existing_ds"):
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"datasetReference":{"projectId":"test-proj","datasetId":"existing_ds"}}`))
				return
			case strings.HasSuffix(r.URL.Path, "/datasets/forbidden_ds"):
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":{"code":403,"message":"Access Denied"}}`))
				return
			case strings.HasSuffix(r.URL.Path, "/datasets/error_ds"):
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"code":500,"message":"Internal Server Error"}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":404,"message":"Not found"}}`))
		case http.MethodDelete:
			deleteCalls.Add(1)
			if r.URL.Query().Get("deleteContents") != "true" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	opts := []option.ClientOption{
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	}

	exists, err := datasetExists(ctx, "test-proj", "existing_ds", opts...)
	if err != nil {
		t.Fatalf("datasetExists(existing_ds) error: %v", err)
	}
	if !exists {
		t.Fatal("datasetExists(existing_ds) = false, want true")
	}

	exists, err = datasetExists(ctx, "test-proj", "otel_bench_absent", opts...)
	if err != nil {
		t.Fatalf("datasetExists(otel_bench_absent) error: %v", err)
	}
	if exists {
		t.Fatal("datasetExists(otel_bench_absent) = true, want false")
	}

	// Verify datasetExists fails closed on non-404 HTTP errors (403 Forbidden and 500 Internal Server Error).
	if _, err := datasetExists(ctx, "test-proj", "forbidden_ds", opts...); err == nil {
		t.Fatal("datasetExists(forbidden_ds) succeeded, want non-nil error (fail closed)")
	}
	ctx500, cancel500 := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel500()
	if _, err := datasetExists(ctx500, "test-proj", "error_ds", opts...); err == nil {
		t.Fatal("datasetExists(error_ds) succeeded, want non-nil error (fail closed)")
	}

	if err := DeleteDatasetWithContents(ctx, "test-proj", "otel_bench_absent", opts...); err != nil {
		t.Fatalf("DeleteDatasetWithContents error: %v", err)
	}
	if getCalls.Load() < 4 {
		t.Fatalf("getCalls = %d, want >= 4", getCalls.Load())
	}
	if deleteCalls.Load() != 1 {
		t.Fatalf("deleteCalls = %d, want 1", deleteCalls.Load())
	}
}
