// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bigqueryexporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/quarantinereplay"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
)

// memOffloadStore implements both offload.BlobStore and quarantinereplay.ObjectFetcher
// for hermetic verification of the offload + Step-0 redaction + quarantine replay
// pipeline when a live GCS bucket is not configured.
type memOffloadStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemOffloadStore() *memOffloadStore {
	return &memOffloadStore{objects: make(map[string][]byte)}
}

func (m *memOffloadStore) WriteObject(_ context.Context, bucket, object string, _ string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	uri := "gs://" + bucket + "/" + object
	if _, exists := m.objects[uri]; !exists {
		m.objects[uri] = bytes.Clone(data)
	}
	return "1", nil
}

func (m *memOffloadStore) Fetch(_ context.Context, uri string) ([]byte, error) {
	if idx := strings.IndexByte(uri, '?'); idx >= 0 {
		uri = uri[:idx]
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[uri]
	if !ok {
		return nil, fmt.Errorf("object %q not found", uri)
	}
	return bytes.Clone(data), nil
}

func (m *memOffloadStore) Close() error { return nil }

func TestLiveGCSOffloadAndQuarantineReplay(t *testing.T) {
	t.Run("HermeticRoundTrip", testHermeticOffloadAndQuarantineReplay)
	t.Run("LiveGCSAndBigQueryRoundTrip", testLiveGCSAndBigQueryOffloadRoundTrip)
}

func testHermeticOffloadAndQuarantineReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	srv := bqfake.NewServer(t)
	wireFakeHooks(t, srv)

	store := newMemOffloadStore()
	cfg := validConfig()
	cfg.Project = "offload-test-proj"
	cfg.Dataset = "otel_offload_hermetic"
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	cfg.Quarantine.Enabled = true
	cfg.Quarantine.MaxRawPayloadBytes = 256
	cfg.Redaction.Enabled = true
	cfg.Redaction.RedactLogBody = true
	cfg.Redaction.BlockedValuePatterns = []string{`sk-live-secret-token-[0-9a-f]+`}
	cfg.Limits.MaxAttributeCount = 4
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "hermetic-offload-bucket"
	cfg.Offload.Prefix = "hermetic-prefix"
	cfg.Offload.Authorizer = "us.conn-authorizer"
	cfg.Offload.MinBytes = 256

	traces, err := newTracesExporter(e2eSettings(), cfg)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	traces.offloader.SetStore(store)
	startE2EExporter(t, traces)

	logs, err := newLogsExporter(e2eSettings(), cfg)
	if err != nil {
		t.Fatalf("newLogsExporter: %v", err)
	}
	logs.offloader.SetStore(store)
	startE2EExporter(t, logs)

	const plantedSecret = "sk-live-secret-token-9876543210abcdef"
	td, ld := buildOffloadAndQuarantinePayloads(plantedSecret)

	if err := traces.pushTraceData(ctx, td); err != nil {
		t.Fatalf("pushTraceData: %v", err)
	}
	if err := logs.pushLogData(ctx, ld); err != nil {
		t.Fatalf("pushLogData: %v", err)
	}

	// 1. Verify Step-0 redaction happened BEFORE GCS upload on every stored object.
	store.mu.Lock()
	numObjects := len(store.objects)
	for uri, data := range store.objects {
		if strings.Contains(string(data), plantedSecret) {
			t.Fatalf("object %s contains unredacted secret %q", uri, plantedSecret)
		}
		if !strings.Contains(string(data), "[REDACTED]") {
			t.Errorf("object %s missing [REDACTED] marker: %s", uri, string(data))
		}
	}
	store.mu.Unlock()
	if numObjects < 4 {
		t.Fatalf("uploaded %d objects, want at least 4 (valid span, valid log, quarantined span, quarantined log)", numObjects)
	}

	// 2. Verify payload_refs on valid span and log rows.
	spanRows := srv.Rows(cfg.Traces.Table)
	if len(spanRows) != 1 {
		t.Fatalf("otel_spans rows = %d, want 1", len(spanRows))
	}
	assertFakeRowPayloadRef(t, store, spanRows[0], "gs://hermetic-offload-bucket/hermetic-prefix/spans/", "us.conn-authorizer")

	logRows := srv.Rows(cfg.Logs.Table)
	if len(logRows) != 1 {
		t.Fatalf("otel_logs rows = %d, want 1", len(logRows))
	}
	assertFakeRowPayloadRef(t, store, logRows[0], "gs://hermetic-offload-bucket/hermetic-prefix/logs/", "us.conn-authorizer")

	// 3. Verify quarantine rows carry offloaded payload_ref and replay cleanly via quarantinereplay.Replay.
	qRows := srv.Rows(cfg.Quarantine.Table)
	if len(qRows) != 2 {
		t.Fatalf("otel_quarantine rows = %d, want 2 (1 span + 1 log)", len(qRows))
	}
	jsonlBuf := formatFakeQuarantineRowsAsJSONL(t, qRows)

	sink := quarantinereplay.NewMemorySink()
	res, err := quarantinereplay.Replay(ctx, bytes.NewReader(jsonlBuf), sink, quarantinereplay.Options{
		ObjectFetcher: store.Fetch,
	})
	if err != nil {
		t.Fatalf("quarantinereplay.Replay: %v", err)
	}
	if res.TotalRead != 2 || res.ReplayedSpans != 1 || res.ReplayedLogs != 1 || res.FailedUnmarshal != 0 || res.FailedExport != 0 {
		t.Fatalf("Replay Summary = %+v, want TotalRead=2 ReplayedSpans=1 ReplayedLogs=1 Failed=0", res)
	}
	if len(sink.Traces) != 1 || len(sink.Logs) != 1 {
		t.Fatalf("replayed batches traces=%d logs=%d, want 1 and 1", len(sink.Traces), len(sink.Logs))
	}
}

func testLiveGCSAndBigQueryOffloadRoundTrip(t *testing.T) {
	bucket := os.Getenv("BQ_OFFLOAD_BUCKET")
	project := os.Getenv("BQ_PROJECT")
	if bucket == "" || project == "" {
		if os.Getenv("REQUIRE_LIVE_GCS") == "1" {
			t.Fatalf("REQUIRE_LIVE_GCS=1 is set but BQ_PROJECT=%q BQ_OFFLOAD_BUCKET=%q", project, bucket)
		}
		t.Skip("BQ_OFFLOAD_BUCKET or BQ_PROJECT is unset; skipping live GCS + BigQuery offload round-trip")
	}
	location := os.Getenv("BQ_LOCATION")
	if location == "" {
		location = "US"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	runNano := time.Now().UnixNano()
	dataset := fmt.Sprintf("otel_offload_it_%d", runNano)
	prefix := fmt.Sprintf("it-offload-%d", runNano)

	httpClient, _, closeHTTP, err := googlehttp.NewClient(ctx, []string{
		bigqueryapi.BigqueryScope,
		"https://www.googleapis.com/auth/devstorage.read_write",
	})
	if err != nil {
		t.Fatalf("create cleanup/query HTTP client: %v", err)
	}
	admin, err := bq.NewClient(ctx, project, option.WithHTTPClient(httpClient))
	if err != nil {
		closeHTTP()
		t.Fatalf("create BigQuery admin client: %v", err)
	}
	var uploadedURIs []string
	var uriMu sync.Mutex
	recordURI := func(uri string) {
		if uri == "" {
			return
		}
		uriMu.Lock()
		uploadedURIs = append(uploadedURIs, uri)
		uriMu.Unlock()
	}

	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close BigQuery admin client: %v", err)
		}
		closeHTTP()
	})
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		uriMu.Lock()
		uris := append([]string(nil), uploadedURIs...)
		uriMu.Unlock()
		for _, uri := range uris {
			deleteGCSObject(cleanupCtx, t, httpClient, uri)
		}
		if err := admin.Dataset(dataset).DeleteWithContents(cleanupCtx); err != nil {
			t.Errorf("cleanup dataset %s.%s: %v", project, dataset, err)
		}
	})

	cfg := validConfig()
	cfg.Project = project
	cfg.Dataset = dataset
	cfg.Location = location
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	cfg.Quarantine.Enabled = true
	cfg.Quarantine.MaxRawPayloadBytes = 256
	cfg.Redaction.Enabled = true
	cfg.Redaction.RedactLogBody = true
	cfg.Redaction.BlockedValuePatterns = []string{`sk-live-secret-token-[0-9a-f]+`}
	cfg.Limits.MaxAttributeCount = 4
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = bucket
	cfg.Offload.Prefix = prefix
	cfg.Offload.Authorizer = "us.live-authorizer"
	cfg.Offload.MinBytes = 256

	settings := component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}

	traces, err := newTracesExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	if err := traces.start(ctx, nil); err != nil {
		t.Fatalf("traces.start: %v", err)
	}
	retainExporterAuthCleanup(traces.signalExporter)
	defer func() { _ = traces.shutdown(context.Background()) }()

	logs, err := newLogsExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newLogsExporter: %v", err)
	}
	if err := logs.start(ctx, nil); err != nil {
		t.Fatalf("logs.start: %v", err)
	}
	retainExporterAuthCleanup(logs.signalExporter)
	defer func() { _ = logs.shutdown(context.Background()) }()

	const plantedSecret = "sk-live-secret-token-9876543210abcdef"
	td, ld := buildOffloadAndQuarantinePayloads(plantedSecret)

	if err := traces.pushTraceData(ctx, td); err != nil {
		t.Fatalf("pushTraceData: %v", err)
	}
	if err := logs.pushLogData(ctx, ld); err != nil {
		t.Fatalf("pushLogData: %v", err)
	}

	for _, tbl := range []string{cfg.Traces.Table + "_dedup", cfg.Logs.Table + "_dedup", cfg.Quarantine.Table} {
		if err := waitForRows(ctx, admin, project, dataset, tbl); err != nil {
			t.Fatalf("waitForRows(%s): %v", tbl, err)
		}
	}

	fetcher, closeFetcher, err := quarantinereplay.NewGCSObjectFetcher(ctx)
	if err != nil {
		t.Fatalf("NewGCSObjectFetcher: %v", err)
	}
	defer closeFetcher()

	// Verify live BigQuery payload_refs on otel_spans_dedup and otel_logs_dedup.
	for _, tc := range []struct {
		table      string
		wantPrefix string
	}{
		{cfg.Traces.Table + "_dedup", "gs://" + bucket + "/" + prefix + "/spans/"},
		{cfg.Logs.Table + "_dedup", "gs://" + bucket + "/" + prefix + "/logs/"},
	} {
		q := admin.Query(fmt.Sprintf(`
			SELECT
				payload_refs[OFFSET(0)].storage_mode,
				payload_refs[OFFSET(0)].byte_size,
				payload_refs[OFFSET(0)].sha256,
				payload_refs[OFFSET(0)].object_ref.uri,
				payload_refs[OFFSET(0)].object_ref.version,
				payload_refs[OFFSET(0)].object_ref.authorizer
			FROM `+"`%s.%s.%s`"+`
			WHERE ARRAY_LENGTH(payload_refs) > 0
			LIMIT 1
		`, project, dataset, tc.table))
		it, err := q.Read(ctx)
		if err != nil {
			t.Fatalf("query %s payload_refs: %v", tc.table, err)
		}
		var vals []bq.Value
		if err := it.Next(&vals); err != nil {
			t.Fatalf("read %s payload_refs: %v", tc.table, err)
		}
		mode, _ := vals[0].(string)
		byteSize, _ := vals[1].(int64)
		wantSHA, _ := vals[2].(string)
		uri, _ := vals[3].(string)
		version, _ := vals[4].(string)
		authorizer, _ := vals[5].(string)
		recordURI(uri)

		if mode != "gcs_object_ref" {
			t.Errorf("%s storage_mode = %q, want gcs_object_ref", tc.table, mode)
		}
		if !strings.HasPrefix(uri, tc.wantPrefix) {
			t.Errorf("%s object_ref.uri = %q, want prefix %q", tc.table, uri, tc.wantPrefix)
		}
		if version == "" {
			t.Errorf("%s object_ref.version is empty", tc.table)
		}
		if authorizer != "us.live-authorizer" {
			t.Errorf("%s object_ref.authorizer = %q, want us.live-authorizer", tc.table, authorizer)
		}

		blob, err := fetcher(ctx, uri)
		if err != nil {
			t.Fatalf("fetch live GCS object %s: %v", uri, err)
		}
		if int64(len(blob)) != byteSize {
			t.Errorf("%s fetched len = %d, want byte_size %d", tc.table, len(blob), byteSize)
		}
		sum := sha256.Sum256(blob)
		if gotSHA := hex.EncodeToString(sum[:]); gotSHA != wantSHA {
			t.Errorf("%s fetched sha256 = %s, want %s", tc.table, gotSHA, wantSHA)
		}
		if strings.Contains(string(blob), plantedSecret) {
			t.Fatalf("%s live GCS object %s contains unredacted secret", tc.table, uri)
		}
		if !strings.Contains(string(blob), "[REDACTED]") {
			t.Errorf("%s live GCS object %s missing [REDACTED] marker", tc.table, uri)
		}
	}

	// Query otel_quarantine and replay via quarantinereplay.Replay + live GCS fetcher.
	q := admin.Query(fmt.Sprintf(`
		SELECT
			signal,
			error_code,
			COALESCE(raw_payload, ''),
			COALESCE(payload_ref.uri, ''),
			COALESCE(payload_ref.version, ''),
			COALESCE(payload_ref.authorizer, ''),
			IF(payload_ref.details IS NULL, '', TO_JSON_STRING(payload_ref.details))
		FROM `+"`%s.%s.%s`"+`
	`, project, dataset, cfg.Quarantine.Table))
	it, err := q.Read(ctx)
	if err != nil {
		t.Fatalf("query otel_quarantine: %v", err)
	}
	var jsonl bytes.Buffer
	enc := json.NewEncoder(&jsonl)
	quarantineCount := 0
	for {
		var vals []bq.Value
		err := it.Next(&vals)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Fatalf("read otel_quarantine: %v", err)
		}
		quarantineCount++
		uri, _ := vals[3].(string)
		recordURI(uri)
		version, _ := vals[4].(string)
		authorizer, _ := vals[5].(string)
		detailsStr, _ := vals[6].(string)
		row := quarantinereplay.Record{
			Signal:     vals[0].(string),
			ErrorCode:  vals[1].(string),
			RawPayload: vals[2].(string),
		}
		if uri != "" {
			row.PayloadRef = &quarantinereplay.ObjectRef{
				URI:        uri,
				Version:    version,
				Authorizer: authorizer,
			}
			if detailsStr != "" {
				row.PayloadRef.Details = json.RawMessage(detailsStr)
			}
		}
		if err := enc.Encode(row); err != nil {
			t.Fatalf("encode quarantine row: %v", err)
		}
	}
	if quarantineCount != 2 {
		t.Fatalf("otel_quarantine rows = %d, want 2", quarantineCount)
	}

	liveSink := quarantinereplay.NewMemorySink()
	res, err := quarantinereplay.Replay(ctx, &jsonl, liveSink, quarantinereplay.Options{
		ObjectFetcher: fetcher,
	})
	if err != nil {
		t.Fatalf("live quarantinereplay.Replay: %v", err)
	}
	if res.TotalRead != 2 || res.ReplayedSpans != 1 || res.ReplayedLogs != 1 || res.FailedUnmarshal != 0 || res.FailedExport != 0 {
		t.Fatalf("live Replay Summary = %+v, want TotalRead=2 ReplayedSpans=1 ReplayedLogs=1 Failed=0", res)
	}
	if len(liveSink.Traces) != 1 || len(liveSink.Logs) != 1 {
		t.Fatalf("live replayed batches traces=%d logs=%d, want 1 and 1", len(liveSink.Traces), len(liveSink.Logs))
	}
}

func buildOffloadAndQuarantinePayloads(secret string) (ptrace.Traces, plog.Logs) {
	now := pcommon.NewTimestampFromTime(time.Now().UTC())
	end := pcommon.NewTimestampFromTime(time.Now().UTC().Add(25 * time.Millisecond))
	largePrompt := strings.Repeat("Prompt context block with bearer token: Authorization: Bearer "+secret+"\n", 8)

	// Traces: 1 valid span with oversized attribute, plus 1 span rejected for
	// exceeding MaxAttributesPerRecord=4 whose quarantine payload exceeds 256B.
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "offload-test-service")
	ss := rs.ScopeSpans().AppendEmpty()

	validSpan := ss.Spans().AppendEmpty()
	validSpan.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	validSpan.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	validSpan.SetName("chat gemini-2.5-pro")
	validSpan.SetStartTimestamp(now)
	validSpan.SetEndTimestamp(end)
	validSpan.Attributes().PutStr("gen_ai.operation.name", "chat")
	validSpan.Attributes().PutStr("gen_ai.prompt", largePrompt)
	validSpan.Attributes().PutStr("api_key", secret)

	badSpan := ss.Spans().AppendEmpty()
	badSpan.SetTraceID(pcommon.TraceID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9})
	badSpan.SetSpanID(pcommon.SpanID{9, 9, 9, 9, 9, 9, 9, 9})
	badSpan.SetName("quarantined-oversize-span")
	badSpan.SetStartTimestamp(now)
	badSpan.SetEndTimestamp(end)
	badSpan.Attributes().PutStr("gen_ai.prompt", largePrompt)
	badSpan.Attributes().PutStr("api_key", secret)
	badSpan.Attributes().PutStr("extra.1", "v1")
	badSpan.Attributes().PutStr("extra.2", "v2")
	badSpan.Attributes().PutStr("extra.3", "v3")
	badSpan.Attributes().PutStr("extra.4", "v4")

	// Logs: 1 valid log with oversized body, plus 1 log rejected for exceeding
	// MaxAttributesPerRecord=4 whose quarantine payload exceeds 256B.
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "offload-test-service")
	sl := rl.ScopeLogs().AppendEmpty()

	validLog := sl.LogRecords().AppendEmpty()
	validLog.SetTimestamp(now)
	validLog.SetObservedTimestamp(now)
	validLog.SetEventName("gen_ai.user.message")
	validLog.Body().SetStr(largePrompt)
	validLog.Attributes().PutStr("api_key", secret)

	badLog := sl.LogRecords().AppendEmpty()
	badLog.SetTimestamp(now)
	badLog.SetObservedTimestamp(now)
	badLog.SetEventName("gen_ai.choice")
	badLog.Body().SetStr(largePrompt)
	badLog.Attributes().PutStr("api_key", secret)
	badLog.Attributes().PutStr("extra.1", "v1")
	badLog.Attributes().PutStr("extra.2", "v2")
	badLog.Attributes().PutStr("extra.3", "v3")
	badLog.Attributes().PutStr("extra.4", "v4")

	return td, ld
}

func assertFakeRowPayloadRef(t *testing.T, store *memOffloadStore, row map[string]any, wantURIPrefix, wantAuthorizer string) {
	t.Helper()
	var first map[string]any
	switch refs := row["payload_refs"].(type) {
	case []map[string]any:
		if len(refs) > 0 {
			first = refs[0]
		}
	case []any:
		if len(refs) > 0 {
			first, _ = refs[0].(map[string]any)
		}
	}
	if first == nil {
		t.Fatalf("row payload_refs = %#v, want non-empty slice", row["payload_refs"])
	}
	if got := first["storage_mode"]; got != "gcs_object_ref" {
		t.Errorf("storage_mode = %v, want gcs_object_ref", got)
	}
	objRef, ok := first["object_ref"].(map[string]any)
	if !ok {
		t.Fatalf("object_ref = %#v, want map", first["object_ref"])
	}
	uri, _ := objRef["uri"].(string)
	if !strings.HasPrefix(uri, wantURIPrefix) {
		t.Errorf("object_ref.uri = %q, want prefix %q", uri, wantURIPrefix)
	}
	if got := objRef["authorizer"]; got != wantAuthorizer {
		t.Errorf("object_ref.authorizer = %v, want %q", got, wantAuthorizer)
	}
	blob, err := store.Fetch(context.Background(), uri)
	if err != nil {
		t.Fatalf("store.Fetch(%q): %v", uri, err)
	}
	sum := sha256.Sum256(blob)
	if gotSHA, wantSHA := first["sha256"], hex.EncodeToString(sum[:]); gotSHA != wantSHA {
		t.Errorf("payload_refs[0].sha256 = %v, want %s", gotSHA, wantSHA)
	}
}

func formatFakeQuarantineRowsAsJSONL(t *testing.T, rows []map[string]any) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range rows {
		qr := quarantinereplay.Record{
			Signal:     fmt.Sprint(r["signal"]),
			ErrorCode:  fmt.Sprint(r["error_code"]),
			RawPayload: stringOrEmpty(r["raw_payload"]),
		}
		if pref, ok := r["payload_ref"].(map[string]any); ok && pref != nil {
			qr.PayloadRef = &quarantinereplay.ObjectRef{
				URI:        stringOrEmpty(pref["uri"]),
				Version:    stringOrEmpty(pref["version"]),
				Authorizer: stringOrEmpty(pref["authorizer"]),
			}
			if details := stringOrEmpty(pref["details"]); details != "" {
				qr.PayloadRef.Details = json.RawMessage(details)
			}
		}
		if err := enc.Encode(qr); err != nil {
			t.Fatalf("encode quarantine row: %v", err)
		}
	}
	return buf.Bytes()
}

func stringOrEmpty(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func deleteGCSObject(ctx context.Context, t *testing.T, httpClient *stdhttp.Client, gsURI string) {
	t.Helper()
	rest := strings.TrimPrefix(gsURI, "gs://")
	bucket, object, ok := strings.Cut(rest, "/")
	if !ok || bucket == "" || object == "" {
		return
	}
	endpoint := fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/o/%s",
		url.PathEscape(bucket), url.PathEscape(object))
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodDelete, endpoint, nil)
	if err != nil {
		t.Errorf("build GCS delete request for %s: %v", gsURI, err)
		return
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Errorf("delete GCS object %s: %v", gsURI, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusNoContent && resp.StatusCode != stdhttp.StatusOK && resp.StatusCode != stdhttp.StatusNotFound {
		t.Errorf("delete GCS object %s returned HTTP %d", gsURI, resp.StatusCode)
	}
}
