// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/quarantinereplay"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
)

type memOffloadEntry struct {
	data    []byte
	version string
}

// memOffloadStore implements both offload.BlobStore and quarantinereplay.ObjectFetcher
// for hermetic verification of the offload + Step-0 redaction + quarantine replay
// pipeline during default unit test runs (`go test ./...`).
type memOffloadStore struct {
	mu      sync.Mutex
	seq     int64
	objects map[string]memOffloadEntry
}

func newMemOffloadStore() *memOffloadStore {
	return &memOffloadStore{objects: make(map[string]memOffloadEntry)}
}

func (m *memOffloadStore) WriteObject(_ context.Context, bucket, object string, _ string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	uri := "gs://" + bucket + "/" + object
	if existing, exists := m.objects[uri]; exists {
		return existing.version, nil
	}
	m.seq++
	ver := strconv.FormatInt(m.seq, 10)
	m.objects[uri] = memOffloadEntry{
		data:    bytes.Clone(data),
		version: ver,
	}
	return ver, nil
}

func (m *memOffloadStore) Fetch(_ context.Context, uri string) ([]byte, error) {
	var wantGen string
	if idx := strings.IndexByte(uri, '?'); idx >= 0 {
		query := uri[idx+1:]
		uri = uri[:idx]
		if after, ok := strings.CutPrefix(query, "generation="); ok {
			wantGen = after
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.objects[uri]
	if !ok {
		return nil, fmt.Errorf("object %q not found", uri)
	}
	if wantGen != "" && wantGen != entry.version {
		return nil, fmt.Errorf("object %q generation = %q, want %q", uri, entry.version, wantGen)
	}
	return bytes.Clone(entry.data), nil
}

func (m *memOffloadStore) Close() error { return nil }

func TestHermeticOffloadAndQuarantineReplay(t *testing.T) {
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
	for uri, entry := range store.objects {
		if strings.Contains(string(entry.data), plantedSecret) {
			t.Fatalf("object %s contains unredacted secret %q", uri, plantedSecret)
		}
		if !strings.Contains(string(entry.data), "[REDACTED]") {
			t.Errorf("object %s missing [REDACTED] marker: %s", uri, string(entry.data))
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
	if err := assertFakeRowPayloadRef(
		store, spanRows[0],
		"gs://hermetic-offload-bucket/hermetic-prefix/spans/",
		"us.conn-authorizer",
		"attributes.gen_ai.prompt",
		"text/plain; charset=utf-8",
	); err != nil {
		t.Fatalf("otel_spans payload_refs: %v", err)
	}

	logRows := srv.Rows(cfg.Logs.Table)
	if len(logRows) != 1 {
		t.Fatalf("otel_logs rows = %d, want 1", len(logRows))
	}
	if err := assertFakeRowPayloadRef(
		store, logRows[0],
		"gs://hermetic-offload-bucket/hermetic-prefix/logs/",
		"us.conn-authorizer",
		"body",
		"text/plain; charset=utf-8",
	); err != nil {
		t.Fatalf("otel_logs payload_refs: %v", err)
	}

	// 3. Verify quarantine rows carry offloaded payload_ref and replay cleanly via quarantinereplay.Replay.
	qRows := srv.Rows(cfg.Quarantine.Table)
	if len(qRows) != 2 {
		t.Fatalf("otel_quarantine rows = %d, want 2 (1 span + 1 log)", len(qRows))
	}
	jsonlBuf, err := formatFakeQuarantineRowsAsJSONL(store, qRows)
	if err != nil {
		t.Fatalf("formatFakeQuarantineRowsAsJSONL: %v", err)
	}

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
	if err := assertReplayedBatchesRedacted(sink, plantedSecret); err != nil {
		t.Fatalf("replayed sink redaction check: %v", err)
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

func assertFakeRowPayloadRef(
	store *memOffloadStore,
	row map[string]any,
	wantURIPrefix, wantAuthorizer, wantAttrPath, wantMIMEType string,
) error {
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
		return fmt.Errorf("row payload_refs = %#v, want non-empty slice", row["payload_refs"])
	}
	if got := first["storage_mode"]; got != "gcs_object_ref" {
		return fmt.Errorf("storage_mode = %v, want gcs_object_ref", got)
	}
	if got := first["attribute_path"]; got != wantAttrPath {
		return fmt.Errorf("attribute_path = %v, want %q", got, wantAttrPath)
	}
	if got := fmt.Sprint(first["part_index"]); got != "0" {
		return fmt.Errorf("part_index = %v, want 0", first["part_index"])
	}
	if got := first["mime_type"]; got != wantMIMEType {
		return fmt.Errorf("mime_type = %v, want %q", got, wantMIMEType)
	}
	objRef, ok := first["object_ref"].(map[string]any)
	if !ok {
		return fmt.Errorf("object_ref = %#v, want map", first["object_ref"])
	}
	uri, _ := objRef["uri"].(string)
	if !strings.HasPrefix(uri, wantURIPrefix) {
		return fmt.Errorf("object_ref.uri = %q, want prefix %q", uri, wantURIPrefix)
	}
	version := stringOrEmpty(objRef["version"])
	if version == "" {
		return fmt.Errorf("object_ref.version is empty")
	}
	if got := objRef["authorizer"]; got != wantAuthorizer {
		return fmt.Errorf("object_ref.authorizer = %v, want %q", got, wantAuthorizer)
	}
	details := stringOrEmpty(objRef["details"])
	if details == "" {
		return fmt.Errorf("object_ref.details is empty")
	}
	blob, err := store.Fetch(context.Background(), uri+"?generation="+version)
	if err != nil {
		return fmt.Errorf("store.Fetch(%q): %w", uri, err)
	}
	if gotBytes := fmt.Sprint(first["byte_size"]); gotBytes != strconv.Itoa(len(blob)) {
		return fmt.Errorf("payload_refs[0].byte_size = %v, want %d", first["byte_size"], len(blob))
	}
	sum := sha256.Sum256(blob)
	wantSHA := hex.EncodeToString(sum[:])
	if gotSHA := first["sha256"]; gotSHA != wantSHA {
		return fmt.Errorf("payload_refs[0].sha256 = %v, want %s", gotSHA, wantSHA)
	}
	if !strings.Contains(details, wantSHA) {
		return fmt.Errorf("object_ref.details = %s, want sha256 %s", details, wantSHA)
	}
	return nil
}

func formatFakeQuarantineRowsAsJSONL(store *memOffloadStore, rows []map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i, r := range rows {
		qr := quarantinereplay.Record{
			Signal:     fmt.Sprint(r["signal"]),
			ErrorCode:  fmt.Sprint(r["error_code"]),
			RawPayload: stringOrEmpty(r["raw_payload"]),
		}
		if qr.RawPayload != "" {
			return nil, fmt.Errorf("quarantine row %d has non-empty inline raw_payload (%d bytes), want offloaded payload_ref", i, len(qr.RawPayload))
		}
		pref, ok := r["payload_ref"].(map[string]any)
		if !ok || pref == nil {
			return nil, fmt.Errorf("quarantine row %d missing payload_ref map: %#v", i, r["payload_ref"])
		}
		uri := stringOrEmpty(pref["uri"])
		if uri == "" {
			return nil, fmt.Errorf("quarantine row %d has empty payload_ref.uri", i)
		}
		version := stringOrEmpty(pref["version"])
		if version == "" {
			return nil, fmt.Errorf("quarantine row %d has empty payload_ref.version", i)
		}
		details := stringOrEmpty(pref["details"])
		if details == "" {
			return nil, fmt.Errorf("quarantine row %d has empty payload_ref.details", i)
		}
		blob, err := store.Fetch(context.Background(), uri+"?generation="+version)
		if err != nil {
			return nil, fmt.Errorf("quarantine row %d store.Fetch(%s?generation=%s): %w", i, uri, version, err)
		}
		sum := sha256.Sum256(blob)
		wantSHA := hex.EncodeToString(sum[:])
		if len(blob) < 256 {
			return nil, fmt.Errorf("quarantine row %d offloaded blob size = %d, want >= 256", i, len(blob))
		}
		if !strings.Contains(details, wantSHA) || !strings.Contains(details, `"content_type":"application/json"`) {
			return nil, fmt.Errorf("quarantine row %d payload_ref.details = %s, want sha256=%s and content_type=application/json", i, details, wantSHA)
		}
		qr.PayloadRef = &quarantinereplay.ObjectRef{
			URI:        uri,
			Version:    version,
			Authorizer: stringOrEmpty(pref["authorizer"]),
			Details:    json.RawMessage(details),
		}
		if err := enc.Encode(qr); err != nil {
			return nil, fmt.Errorf("encode quarantine row %d: %w", i, err)
		}
	}
	return buf.Bytes(), nil
}

func assertReplayedBatchesRedacted(sink *quarantinereplay.MemorySink, secret string) error {
	traceJSON, err := (&ptrace.JSONMarshaler{}).MarshalTraces(sink.Traces[0])
	if err != nil {
		return fmt.Errorf("marshal replayed trace: %w", err)
	}
	if strings.Contains(string(traceJSON), secret) {
		return fmt.Errorf("replayed trace contains unredacted secret %q", secret)
	}
	if !strings.Contains(string(traceJSON), "[REDACTED]") {
		return fmt.Errorf("replayed trace missing [REDACTED] marker: %s", string(traceJSON))
	}

	logJSON, err := (&plog.JSONMarshaler{}).MarshalLogs(sink.Logs[0])
	if err != nil {
		return fmt.Errorf("marshal replayed log: %w", err)
	}
	if strings.Contains(string(logJSON), secret) {
		return fmt.Errorf("replayed log contains unredacted secret %q", secret)
	}
	if !strings.Contains(string(logJSON), "[REDACTED]") {
		return fmt.Errorf("replayed log missing [REDACTED] marker: %s", string(logJSON))
	}
	return nil
}

func stringOrEmpty(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// isExpectedContinuousQueryDryRunError reports whether err from a dry-run
// Continuous Query EXPORT DATA ... CLOUD_PUBSUB job on an on-demand project
// without an Enterprise/Enterprise Plus CONTINUOUS reservation or Pub/Sub
// topic is an expected environment/reservation/topic-permission rejection
// rather than a SQL syntax or EXPORT DATA option error (such as "Unsupported
// export format").
func isExpectedContinuousQueryDryRunError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "syntax error") ||
		strings.Contains(msg, "unsupported export format") ||
		strings.Contains(msg, "unrecognized name") ||
		strings.Contains(msg, "table not found") {
		return false
	}
	return strings.Contains(msg, "continuous query") ||
		strings.Contains(msg, "continuous queries") ||
		strings.Contains(msg, "reservation") ||
		strings.Contains(msg, "enterprise") ||
		strings.Contains(msg, "pubsub") ||
		strings.Contains(msg, "cloud_pubsub")
}

// shouldDeleteDatasetOnCreate reports whether an integration test owns the
// dataset and may register DeleteWithContents cleanup (true only when Dataset.Create
// succeeded without error, never when Dataset.Create returned 409 AlreadyExists
// or any other error).
func shouldDeleteDatasetOnCreate(createErr error) bool {
	return createErr == nil
}

func TestIsExpectedContinuousQueryDryRunError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"unsupported export format", errors.New("googleapi: Error 400: Unsupported export format: CLOUD_PUBSUB, invalid"), false},
		{"syntax error mentioning continuous", errors.New("googleapi: Error 400: Syntax error: Unexpected identifier \"continuous\" at [1:1]"), false},
		{"unrecognized column", errors.New("googleapi: Error 400: Unrecognized name: bad_col"), false},
		{"enterprise reservation required", errors.New("googleapi: Error 400: Continuous queries require an Enterprise or Enterprise Plus reservation"), true},
		{"pubsub permission denied", errors.New("googleapi: Error 403: Access Denied: Cloud_Pubsub topic permission denied"), true},
	} {
		if got := isExpectedContinuousQueryDryRunError(tc.err); got != tc.want {
			t.Errorf("%s: isExpectedContinuousQueryDryRunError(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

func TestShouldDeleteDatasetOnCreate(t *testing.T) {
	if !shouldDeleteDatasetOnCreate(nil) {
		t.Error("shouldDeleteDatasetOnCreate(nil) = false, want true")
	}
	if shouldDeleteDatasetOnCreate(errors.New("googleapi: Error 409: Already Exists: Dataset")) {
		t.Error("shouldDeleteDatasetOnCreate(409 Already Exists) = true, want false")
	}
}
