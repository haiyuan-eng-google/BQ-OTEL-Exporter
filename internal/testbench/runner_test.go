// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package testbench

import (
	"bytes"
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

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/api/option"
)

func TestGeneratorAllSevenFrameworksAndMCP(t *testing.T) {
	genCfg := GeneratorConfig{
		PayloadBytes:   8192,
		PlantSecrets:   true,
		UniquePayloads: true,
		Seed:           7,
		BaseTime:       time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC),
	}
	gen := NewGenerator(genCfg)
	gen2 := NewGenerator(genCfg)

	// 28 spans = 7 frameworks * 4-span tree (invoke_agent, chat, execute_tool, mcp).
	td := gen.TracesBatch(28)
	td2 := gen2.TracesBatch(28)
	if got := td.SpanCount(); got != 28 {
		t.Fatalf("TracesBatch(28) SpanCount = %d, want 28", got)
	}
	traceBytes1, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatalf("MarshalTraces(td) error: %v", err)
	}
	traceBytes2, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td2)
	if err != nil {
		t.Fatalf("MarshalTraces(td2) error: %v", err)
	}
	if !bytes.Equal(traceBytes1, traceBytes2) {
		t.Fatal("TracesBatch protobuf bytes differ across two fresh NewGenerator instances with identical config")
	}

	seenFrameworks := make(map[string]bool)
	seenOps := make(map[string]int)
	seenMCP := make(map[string]bool)
	spanPairs := make(map[string]bool)
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
				spanPairs[sp.TraceID().String()+"/"+sp.SpanID().String()] = true
				if op, ok := sp.Attributes().Get("gen_ai.operation.name"); ok {
					seenOps[op.Str()]++
				}
				if mcpMethod, ok := sp.Attributes().Get("mcp.method.name"); ok {
					m := mcpMethod.Str()
					seenMCP[m] = true
					if _, ok := sp.Attributes().Get("server.address"); !ok {
						t.Errorf("MCP span (%s) missing server.address", m)
					}
					if _, ok := sp.Attributes().Get("jsonrpc.request.id"); !ok {
						t.Errorf("MCP span (%s) missing jsonrpc.request.id", m)
					}
					switch m {
					case "tools/call":
						if _, ok := sp.Attributes().Get("gen_ai.tool.name"); !ok {
							t.Error("MCP tools/call span missing gen_ai.tool.name")
						}
					case "resources/read":
						if _, ok := sp.Attributes().Get("mcp.resource.uri"); !ok {
							t.Error("MCP resources/read span missing mcp.resource.uri")
						}
					case "prompts/get":
						if _, ok := sp.Attributes().Get("gen_ai.prompt.name"); !ok {
							t.Error("MCP prompts/get span missing gen_ai.prompt.name")
						}
					}
				}
				if _, ok := sp.Attributes().Get("client.api_key"); ok {
					secretSpans++
				}
				if msg, ok := sp.Attributes().Get("gen_ai.input.messages"); ok {
					if len(msg.Str()) != 8192 {
						t.Fatalf("gen_ai.input.messages len = %d, want 8192", len(msg.Str()))
					}
					if !json.Valid([]byte(msg.Str())) {
						t.Fatalf("gen_ai.input.messages is not valid JSON: %.120s", msg.Str())
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
	for _, wantMethod := range []string{"tools/call", "resources/read", "prompts/get"} {
		if !seenMCP[wantMethod] {
			t.Fatalf("missing MCP method %q in %v", wantMethod, seenMCP)
		}
	}
	if secretSpans == 0 {
		t.Fatal("expected planted secret attributes when PlantSecrets=true")
	}

	ld := gen.LogsBatch(14)
	ld2 := gen2.LogsBatch(14)
	if got := ld.LogRecordCount(); got != 14 {
		t.Fatalf("LogsBatch(14) LogRecordCount = %d, want 14", got)
	}
	logBytes1, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	if err != nil {
		t.Fatalf("MarshalLogs(ld) error: %v", err)
	}
	logBytes2, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld2)
	if err != nil {
		t.Fatalf("MarshalLogs(ld2) error: %v", err)
	}
	if !bytes.Equal(logBytes1, logBytes2) {
		t.Fatal("LogsBatch protobuf bytes differ across two fresh NewGenerator instances with identical config")
	}
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		sls := ld.ResourceLogs().At(i).ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				lr := lrs.At(k)
				key := lr.TraceID().String() + "/" + lr.SpanID().String()
				if !spanPairs[key] {
					t.Fatalf("log record correlated pair %s not found in emitted TracesBatch(28)", key)
				}
			}
		}
	}

	md := gen.MetricsBatch(10)
	md2 := gen2.MetricsBatch(10)
	if got := md.DataPointCount(); got != 10 {
		t.Fatalf("MetricsBatch(10) DataPointCount = %d, want 10", got)
	}
	metricBytes1, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	if err != nil {
		t.Fatalf("MarshalMetrics(md) error: %v", err)
	}
	metricBytes2, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md2)
	if err != nil {
		t.Fatalf("MarshalMetrics(md2) error: %v", err)
	}
	if !bytes.Equal(metricBytes1, metricBytes2) {
		t.Fatal("MetricsBatch protobuf bytes differ across two fresh NewGenerator instances with identical config")
	}
	dps := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Histogram().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		ex := dps.At(i).Exemplars().At(0)
		key := ex.TraceID().String() + "/" + ex.SpanID().String()
		if !spanPairs[key] {
			t.Fatalf("metric exemplar correlated pair %s not found in emitted TracesBatch(28)", key)
		}
	}

	// Verify payloadFor produces valid JSON across all UniquePayloads x PlantSecrets combinations.
	for _, unique := range []bool{false, true} {
		for _, secrets := range []bool{false, true} {
			gCombo := NewGenerator(GeneratorConfig{
				PayloadBytes:   1024,
				UniquePayloads: unique,
				PlantSecrets:   secrets,
			})
			for seq := uint64(0); seq < 16; seq++ {
				p := gCombo.payloadFor(seq)
				if len(p) != 1024 {
					t.Fatalf("payloadFor(seq=%d, unique=%t, secrets=%t) len = %d, want 1024", seq, unique, secrets, len(p))
				}
				if !json.Valid([]byte(p)) {
					t.Fatalf("payloadFor(seq=%d, unique=%t, secrets=%t) invalid JSON: %s", seq, unique, secrets, p)
				}
			}
		}
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
		SimulateThrottleEvery: 2,
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
		if res.Telemetry.Retries <= 0 {
			t.Errorf("signal %s Retries = %d, want > 0 under SimulateThrottleEvery=%d", res.Signal, res.Telemetry.Retries, cfg.SimulateThrottleEvery)
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

func TestRunPerBatchCallTimeoutDoesNotExitWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Batch 1 (seq == 1) and every 3rd batch receive a 120ms ack delay while
	// per-batch CallTimeout is 40ms, so delayed batches fail with
	// context.DeadlineExceeded while the parent ctx remains active and the single
	// worker continues processing subsequent non-delayed batches.
	cfg := RunnerConfig{
		Mode:               ModeBQFake,
		Signals:            []string{"traces"},
		Duration:           350 * time.Millisecond,
		BatchSize:          10,
		Workers:            1,
		StreamPoolSize:     1,
		ConnectionPoolSize: 1,
		SimulateAckDelay:   120 * time.Millisecond,
		CallTimeout:        40 * time.Millisecond,
	}
	report, err := Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if len(report.Results) != 1 {
		t.Fatalf("len(report.Results) = %d, want 1", len(report.Results))
	}
	res := report.Results[0]
	if res.BatchesFailed <= 0 {
		t.Fatalf("BatchesFailed = %d, want > 0 when per-batch CallTimeout < SimulateAckDelay", res.BatchesFailed)
	}
	if res.BatchesSucceeded <= 0 {
		t.Fatalf("BatchesSucceeded = %d, want > 0 (worker must continue after per-batch CallTimeout)", res.BatchesSucceeded)
	}
}

func TestRunUniquePayloadsAndZeroAckErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. UniquePayloads=true generates fresh batches beyond 8 batches with 0 dedup hits.
	offloadDir := t.TempDir()
	uniqueCfg := RunnerConfig{
		Mode:                  ModeBQFake,
		Signals:               []string{"traces"},
		Duration:              180 * time.Millisecond,
		BatchSize:             4,
		Workers:               2,
		StreamPoolSize:        2,
		ConnectionPoolSize:    2,
		PayloadBytes:          1024,
		UniquePayloads:        true,
		OffloadDir:            offloadDir,
		OffloadThresholdBytes: 512,
	}
	uniqueRep, err := Run(ctx, uniqueCfg)
	if err != nil {
		t.Fatalf("Run(UniquePayloads=true) error: %v", err)
	}
	if uniqueRep.Results[0].BatchesSucceeded <= 8 {
		t.Fatalf("BatchesSucceeded = %d, want > 8", uniqueRep.Results[0].BatchesSucceeded)
	}
	if uniqueRep.Results[0].Telemetry.OffloadDedupHits != 0 {
		t.Fatalf("OffloadDedupHits = %d, want 0 when UniquePayloads=true", uniqueRep.Results[0].Telemetry.OffloadDedupHits)
	}

	// 2. All-rejected run (payload exceeds 256 KiB limits.max_value_bytes without offload)
	// must report AchievedRowsPerSec == 0 and return a non-nil error.
	rejectCfg := RunnerConfig{
		Mode:               ModeBQFake,
		Signals:            []string{"traces"},
		Duration:           120 * time.Millisecond,
		BatchSize:          8,
		Workers:            1,
		StreamPoolSize:     1,
		ConnectionPoolSize: 1,
		PayloadBytes:       2 * 1024 * 1024,
	}
	rejectRep, err := Run(ctx, rejectCfg)
	if err == nil {
		t.Fatal("Run() with 100% rejected rows succeeded, want non-nil error")
	}
	if rejectRep == nil || len(rejectRep.Results) != 1 {
		t.Fatalf("expected 1 result on rejected report, got %+v", rejectRep)
	}
	if rejectRep.Results[0].AchievedRowsPerSec != 0 {
		t.Fatalf("AchievedRowsPerSec = %f, want 0 when AcknowledgedRows == 0", rejectRep.Results[0].AchievedRowsPerSec)
	}
	if rejectRep.Results[0].Telemetry.RejectedRows <= 0 {
		t.Fatalf("RejectedRows = %d, want > 0", rejectRep.Results[0].Telemetry.RejectedRows)
	}

	// 3. Parent context cancellation during run must return a non-nil error.
	shortCtx, cancelShort := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancelShort()
	if _, err := Run(shortCtx, RunnerConfig{
		Mode:             ModeBQFake,
		Signals:          []string{"traces"},
		Duration:         2 * time.Second,
		BatchSize:        20,
		Workers:          1,
		SimulateAckDelay: 100 * time.Millisecond,
	}); err == nil {
		t.Fatal("Run() with canceled parent context succeeded, want non-nil error")
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
		{Mode: ModeBQFake, Duration: -time.Second},
		{Mode: ModeBQFake, BatchSize: -1},
		{Mode: ModeBQFake, Workers: -1},
		{Mode: ModeBQFake, StreamPoolSize: -1},
		{Mode: ModeBQFake, ConnectionPoolSize: -1},
		{Mode: ModeBQFake, PayloadBytes: -1},
		{Mode: ModeBQFake, TargetRate: -1},
		{Mode: ModeBQFake, SimulateThrottleEvery: -1},
		{Mode: ModeBQFake, SimulateAckDelay: -time.Millisecond},
		{Mode: ModeBQFake, CallTimeout: -time.Millisecond},
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
