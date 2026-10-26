// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/quarantinereplay"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
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

// Overwrite replaces an existing object in memOffloadStore with a new generation
// and payload so tests can verify generation-pinned reads against overwritten URIs.
func (m *memOffloadStore) Overwrite(uri string, data []byte) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	ver := strconv.FormatInt(m.seq, 10)
	m.objects[uri] = memOffloadEntry{
		data:    bytes.Clone(data),
		version: ver,
	}
	return ver
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

// isAllowedContinuousQueryDryRunReason reports whether a BigQuery ErrorProto or
// googleapi.ErrorItem Reason can represent an expected Continuous Query
// edition/reservation or Pub/Sub topic permission gate on an on-demand test
// project. Backend/service failures, rate/quota/resource limits, missing
// datasets/tables, and empty reasons are never eligible for test skip.
func isAllowedContinuousQueryDryRunReason(reason string) bool {
	switch reason {
	case "invalid", "invalidQuery", "billingTierLimitExceeded", "accessDenied":
		return true
	default:
		return false
	}
}

// isExpectedContinuousQueryDryRunError reports whether err from a dry-run
// Continuous Query EXPORT DATA ... CLOUD_PUBSUB job on an on-demand project
// without an Enterprise/Enterprise Plus CONTINUOUS reservation or Pub/Sub
// topic is an expected environment/reservation/topic-permission rejection
// rather than a SQL syntax/construct error, EXPORT DATA option error, missing
// table/dataset, project-level bigquery.jobs.create denial, or 5xx/backend error.
// rejectContinuousQueryMessage reports whether msg contains a substring that
// indicates a SQL/construct/option error, missing resource, backend failure, or
// quota/rate limit — none of which are eligible for an entitlement/topic skip.
func rejectContinuousQueryMessage(msg string) bool {
	m := strings.ToLower(msg)
	for _, reject := range []string{
		"syntax error",
		"unrecognized",
		"invalid value for option",
		"unsupported",
		"do not support",
		"does not support",
		"is not supported in this query",
		"not supported in continuous",
		"group by",
		"order by",
		"join",
		"window",
		"aggregate",
		"subquery",
		"not found",
		"dataset",
		"table",
		"column",
		"bigquery.jobs.create",
		"bigquery.tables",
		"bigquery.datasets",
		"bigquery.routines",
		"internal error",
		"backend error",
		"service unavailable",
		"rate limit",
		"quota exceeded",
		"resources exceeded",
	} {
		if strings.Contains(m, reject) {
			return true
		}
	}
	return false
}

// isAllowedContinuousQueryMessage reports whether msg contains a substring
// indicating an expected entitlement, assignment, or topic-permission gate.
func isAllowedContinuousQueryMessage(code int, msg string) bool {
	m := strings.ToLower(msg)
	hasReservationOrEditionGate := (strings.Contains(m, "continuous queries") || strings.Contains(m, "continuous query")) &&
		(strings.Contains(m, "require the project to have a continuous assignment") ||
			strings.Contains(m, "continuous assignment to a reservation") ||
			strings.Contains(m, "require an enterprise or enterprise plus reservation") ||
			strings.Contains(m, "continuous query reservation") ||
			strings.Contains(m, "are not enabled") ||
			strings.Contains(m, "is not enabled") ||
			strings.Contains(m, "not supported in standard edition") ||
			strings.Contains(m, "without an enterprise") ||
			strings.Contains(m, "billing tier"))
	hasPubSubPermGate := (code == http.StatusForbidden || strings.Contains(m, "access denied") || strings.Contains(m, "permission denied")) &&
		(strings.Contains(m, "permission") || strings.Contains(m, "access denied")) &&
		(strings.Contains(m, "pubsub.topics") ||
			strings.Contains(m, "cloud_pubsub") ||
			strings.Contains(m, "/topics/"))
	return hasReservationOrEditionGate || hasPubSubPermGate
}

// isExpectedContinuousQueryDryRunError reports whether err from a dry-run
// Continuous Query EXPORT DATA ... CLOUD_PUBSUB job on an on-demand project
// without an Enterprise/Enterprise Plus CONTINUOUS reservation or Pub/Sub
// topic is an expected environment/reservation/topic-permission rejection
// rather than a SQL syntax/construct error, EXPORT DATA option error, missing
// table/dataset, project-level bigquery.jobs.create denial, or 5xx/backend error.
//
// Every non-nil ErrorProto/ErrorItem message is classified independently:
// the primary message and every secondary message must each be an allowed
// entitlement, assignment, or topic rejection, and none may contain a
// rejected substring. This prevents a valid primary reservation-rejection
// from masking a secondary SQL syntax error.
func isExpectedContinuousQueryDryRunError(err error) bool {
	if err == nil {
		return false
	}
	code := extractHTTPStatusCode(err)
	// Only HTTP 400 (edition/reservation capability check) and HTTP 403
	// (accessDenied on Pub/Sub topic or CONTINUOUS assignment) can qualify as
	// expected environment capability rejections; 404, 5xx, and unknown errors
	// must always fail the test.
	if code != http.StatusBadRequest && code != http.StatusForbidden {
		return false
	}
	// Collect the primary message and every structured ErrorItem message (even
	// when empty or whitespace-only) so an empty/unclassified message fails the
	// allow check rather than being silently ignored.
	var messages []string
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		messages = append(messages, apiErr.Message)
		for _, item := range apiErr.Errors {
			if !isAllowedContinuousQueryDryRunReason(item.Reason) {
				return false
			}
			messages = append(messages, item.Message)
		}
	} else {
		messages = append(messages, primaryErrorMessage(err))
	}
	// Check the trailing reason on the primary message.
	rawMsg := primaryErrorMessage(err)
	if idx := strings.LastIndex(rawMsg, ","); idx >= 0 {
		if trailingReason := strings.TrimSpace(rawMsg[idx+1:]); trailingReason != "" && !strings.Contains(trailingReason, " ") {
			if !isAllowedContinuousQueryDryRunReason(trailingReason) {
				return false
			}
		}
	}
	// Every collected message must be non-empty and pass the reject and allow
	// checks independently.
	for _, msg := range messages {
		if strings.TrimSpace(msg) == "" {
			return false
		}
		if rejectContinuousQueryMessage(msg) {
			return false
		}
		if !isAllowedContinuousQueryMessage(code, msg) {
			return false
		}
	}
	return true
}

var googleapiStatusRe = regexp.MustCompile(`googleapi:\s*Error\s+(\d{3})\b`)

func primaryErrorMessage(err error) string {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Message != "" {
		return apiErr.Message
	}
	msg := err.Error()
	if before, _, ok := strings.Cut(msg, "\nDetails:"); ok {
		return before
	}
	return msg
}

func extractHTTPStatusCode(err error) int {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code != 0 {
		return apiErr.Code
	}
	if m := googleapiStatusRe.FindStringSubmatch(err.Error()); len(m) == 2 {
		if n, convErr := strconv.Atoi(m[1]); convErr == nil {
			return n
		}
	}
	return 0
}

// validateContinuousExportDryRunJob inspects both the RPC error and the returned
// *bigqueryapi.Job payload from rawSvc.Jobs.Insert(...).Do(). BigQuery can return
// HTTP 200 with job.Status.ErrorResult populated or Statistics == nil; those must
// never be treated as validated dry-runs.
func validateContinuousExportDryRunJob(job *bigqueryapi.Job, err error) (skipReason string, fatalErr error) {
	if err != nil {
		if isExpectedContinuousQueryDryRunError(err) {
			return primaryErrorMessage(err), nil
		}
		return "", err
	}
	if job == nil {
		return "", errors.New("Jobs.Insert returned nil job without error")
	}
	if job.Status == nil {
		return "", errors.New("Jobs.Insert returned job with nil Status")
	}
	if job.Status.ErrorResult != nil {
		if !isAllowedContinuousQueryDryRunReason(job.Status.ErrorResult.Reason) {
			return "", fmt.Errorf("job.Status.ErrorResult = %s: %s", job.Status.ErrorResult.Reason, job.Status.ErrorResult.Message)
		}
		for i, item := range job.Status.Errors {
			if item == nil {
				return "", fmt.Errorf("job.Status.Errors[%d] is nil", i)
			}
			if !isAllowedContinuousQueryDryRunReason(item.Reason) {
				return "", fmt.Errorf("job.Status.Errors[%d] = %s: %s", i, item.Reason, item.Message)
			}
		}
		code := http.StatusBadRequest
		if job.Status.ErrorResult.Reason == "accessDenied" {
			code = http.StatusForbidden
		}
		// Keep the original ErrorResult.Message separate from the composite
		// diagnostic so an empty ErrorResult.Message is classified as-is by
		// isExpectedContinuousQueryDryRunError rather than being masked by a
		// non-empty secondary message.
		primaryMsg := job.Status.ErrorResult.Message
		compositeDiag := primaryMsg
		errItems := []googleapi.ErrorItem{{
			Reason:  job.Status.ErrorResult.Reason,
			Message: primaryMsg,
		}}
		for _, item := range job.Status.Errors {
			if item != nil {
				errItems = append(errItems, googleapi.ErrorItem{
					Reason:  item.Reason,
					Message: item.Message,
				})
				if item.Message != "" && !strings.Contains(compositeDiag, item.Message) {
					if compositeDiag == "" {
						compositeDiag = item.Message
					} else {
						compositeDiag += " | " + item.Message
					}
				}
			}
		}
		statusErr := &googleapi.Error{
			Code:    code,
			Message: primaryMsg,
			Errors:  errItems,
		}
		if isExpectedContinuousQueryDryRunError(statusErr) {
			return fmt.Sprintf("googleapi: Error %d: %s", code, compositeDiag), nil
		}
		return "", fmt.Errorf("job.Status.ErrorResult = %s: %s", job.Status.ErrorResult.Reason, compositeDiag)
	}
	if len(job.Status.Errors) > 0 {
		for i, item := range job.Status.Errors {
			if item == nil {
				return "", fmt.Errorf("job.Status.Errors[%d] is nil", i)
			}
		}
		first := job.Status.Errors[0]
		return "", fmt.Errorf("job.Status.Errors[0] = %s: %s", first.Reason, first.Message)
	}
	if job.Statistics == nil {
		return "", errors.New("Jobs.Insert dry-run returned nil job.Statistics")
	}
	return "", nil
}

// shouldDeleteDatasetOnCreate reports whether an integration test owns the
// dataset and may register DeleteWithContents cleanup (true only when Dataset.Create
// succeeded without error, never when Dataset.Create returned 409 AlreadyExists
// or any other error).
func shouldDeleteDatasetOnCreate(createErr error) bool {
	return createErr == nil
}

func uniqueEphemeralDatasetID(prefix string) string {
	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixMilli(), hex.EncodeToString(rnd[:]))
}

// createOwnedEphemeralDataset creates dataset via Dataset.Create(ctx, &bq.DatasetMetadata{Location: location})
// and registers DeleteWithContents on cleanup ONLY if Dataset.Create returns nil.
// Deletion errors are reported through the reportCleanupError callback so they
// are never silently discarded.
func createOwnedEphemeralDataset(
	ctx context.Context,
	admin *bq.Client,
	dataset, location string,
	registerCleanup func(func()),
	reportCleanupError func(error),
) error {
	createErr := admin.Dataset(dataset).Create(ctx, &bq.DatasetMetadata{Location: location})
	if !shouldDeleteDatasetOnCreate(createErr) {
		return createErr
	}
	registerCleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := admin.Dataset(dataset).DeleteWithContents(cleanCtx); err != nil {
			reportCleanupError(fmt.Errorf("delete ephemeral dataset %s: %w", dataset, err))
		}
	})
	return nil
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
		{"probe 1: 500 backend error mentioning reservation", errors.New("googleapi: Error 500: Internal error while checking continuous query reservation"), false},
		{"probe 1b: 503 backend outage mentioning continuous query", errors.New("googleapi: Error 503: Service unavailable for continuous query"), false},
		{"probe 2: 400 invalidQuery continuous queries do not support ORDER BY", errors.New("googleapi: Error 400: Continuous queries do not support ORDER BY, invalidQuery"), false},
		{"probe 3: 400 invalidQuery invalid value for option format CLOUD_PUBSUB", errors.New("googleapi: Error 400: Invalid value for option 'format': CLOUD_PUBSUB, invalidQuery"), false},
		{"probe 4: 404 notFound dataset my-enterprise-proj:otel_cq_it", errors.New("googleapi: Error 404: Not found: Dataset my-enterprise-proj:otel_cq_it, notFound"), false},
		{"repro b: 400 invalidQuery GROUP BY is not supported in this query", errors.New("googleapi: Error 400: Continuous queries: GROUP BY is not supported in this query, invalidQuery"), false},
		{"repro c: 400 invalidQuery do not support JOIN in Enterprise edition", errors.New("googleapi: Error 400: Continuous queries do not support JOIN in Enterprise edition, invalidQuery"), false},
		{"repro d: 403 bigquery.jobs.create on project named my-pubsub-proj", errors.New("googleapi: Error 403: Access Denied: Project my-pubsub-proj: User does not have bigquery.jobs.create permission in project my-pubsub-proj"), false},
		{"structured googleapi.Error with backendError reason", &googleapi.Error{
			Code:    http.StatusBadRequest,
			Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation.",
			Errors:  []googleapi.ErrorItem{{Reason: "backendError", Message: "backend failure"}},
		}, false},
		{"structured googleapi.Error with invalidQuery reason on reservation gate", &googleapi.Error{
			Code:    http.StatusBadRequest,
			Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation. No such reservation was found in region US.",
			Errors:  []googleapi.ErrorItem{{Reason: "invalidQuery", Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation."}},
		}, true},
		{"structured googleapi.Error with live BigQuery invalid reason on reservation gate", &googleapi.Error{
			Code:    http.StatusBadRequest,
			Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation. No such reservation was found in region US.",
			Errors:  []googleapi.ErrorItem{{Reason: "invalid", Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation. No such reservation was found in region US."}},
		}, true},
		{"enterprise reservation required", errors.New("googleapi: Error 400: Continuous queries require an Enterprise or Enterprise Plus reservation"), true},
		{"pubsub permission denied", errors.New("googleapi: Error 403: Access Denied: Cloud_Pubsub topic permission denied"), true},
		{"pubsub.topics permission denied", errors.New("googleapi: Error 403: Access Denied: User does not have pubsub.topics.publish permission on /topics/otel-alerts"), true},
	} {
		if got := isExpectedContinuousQueryDryRunError(tc.err); got != tc.want {
			t.Errorf("%s: isExpectedContinuousQueryDryRunError(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}

	// Probe 5: HTTP 200 *bigqueryapi.Job carrying Status.ErrorResult or nil Statistics.
	jobWithErrResult := &bigqueryapi.Job{
		Status: &bigqueryapi.JobStatus{
			ErrorResult: &bigqueryapi.ErrorProto{
				Reason:  "invalidQuery",
				Message: "Continuous queries do not support ORDER BY",
			},
		},
		Statistics: &bigqueryapi.JobStatistics{},
	}
	if skipReason, fatalErr := validateContinuousExportDryRunJob(jobWithErrResult, nil); fatalErr == nil || skipReason != "" {
		t.Fatalf("validateContinuousExportDryRunJob(Status.ErrorResult invalidQuery ORDER BY) = (skip=%q, err=%v), want non-nil fatalErr", skipReason, fatalErr)
	}
	// Repro a: HTTP 200 *bigqueryapi.Job with Reason: "backendError" and reservation text must fail, never skip.
	jobBackendErr := &bigqueryapi.Job{
		Status: &bigqueryapi.JobStatus{
			ErrorResult: &bigqueryapi.ErrorProto{
				Reason:  "backendError",
				Message: "Internal error while checking continuous query reservation",
			},
		},
		Statistics: &bigqueryapi.JobStatistics{},
	}
	if skipReason, fatalErr := validateContinuousExportDryRunJob(jobBackendErr, nil); fatalErr == nil || skipReason != "" {
		t.Fatalf("validateContinuousExportDryRunJob(Status.ErrorResult backendError) = (skip=%q, err=%v), want non-nil fatalErr", skipReason, fatalErr)
	}
	// Secondary job.Status.Errors item with disallowed Reason must also fail even if ErrorResult.Reason is invalidQuery.
	jobSecondaryBackendErr := &bigqueryapi.Job{
		Status: &bigqueryapi.JobStatus{
			ErrorResult: &bigqueryapi.ErrorProto{
				Reason:  "invalidQuery",
				Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation.",
			},
			Errors: []*bigqueryapi.ErrorProto{
				{
					Reason:  "backendError",
					Message: "Sub-request backendError",
				},
			},
		},
		Statistics: &bigqueryapi.JobStatistics{},
	}
	if skipReason, fatalErr := validateContinuousExportDryRunJob(jobSecondaryBackendErr, nil); fatalErr == nil || skipReason != "" {
		t.Fatalf("validateContinuousExportDryRunJob(Status.Errors backendError) = (skip=%q, err=%v), want non-nil fatalErr", skipReason, fatalErr)
	}
	// P1-1 regression: primary reservation-rejection message + secondary SQL
	// syntax error must FAIL, not skip, even though the primary is a valid
	// entitlement gate.  Both as a returned *bigqueryapi.Job and as a direct
	// googleapi.Error.
	jobMixedReservationAndSyntax := &bigqueryapi.Job{
		Status: &bigqueryapi.JobStatus{
			ErrorResult: &bigqueryapi.ErrorProto{
				Reason:  "invalidQuery",
				Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation.",
			},
			Errors: []*bigqueryapi.ErrorProto{
				{
					Reason:  "invalidQuery",
					Message: `Syntax error: Unexpected identifier "continuous"`,
				},
			},
		},
		Statistics: &bigqueryapi.JobStatistics{},
	}
	if skipReason, fatalErr := validateContinuousExportDryRunJob(jobMixedReservationAndSyntax, nil); fatalErr == nil || skipReason != "" {
		t.Fatalf("validateContinuousExportDryRunJob(primary reservation + secondary SQL syntax) = (skip=%q, err=%v), want non-nil fatalErr", skipReason, fatalErr)
	}
	// P1-1 regression: same mixed case as a direct googleapi.Error (RPC path).
	directMixedErr := &googleapi.Error{
		Code:    http.StatusBadRequest,
		Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation.",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation."},
			{Reason: "invalidQuery", Message: `Syntax error: Unexpected identifier "continuous"`},
		},
	}
	if isExpectedContinuousQueryDryRunError(directMixedErr) {
		t.Fatalf("isExpectedContinuousQueryDryRunError(primary reservation + secondary SQL syntax) = true, want false")
	}
	// Round-8 P1-1 regression: empty or whitespace-only primary/secondary
	// structured messages must fail closed across both returned-job and
	// direct-RPC googleapi.Error adapters (all 4 empty-primary / empty-secondary
	// combinations plus whitespace variants).
	for _, emptyText := range []string{"", "   \t\n  "} {
		// (1) Returned-job adapter: reservation primary + empty secondary item.
		jobEmptySecondary := &bigqueryapi.Job{
			Status: &bigqueryapi.JobStatus{
				ErrorResult: &bigqueryapi.ErrorProto{
					Reason:  "invalidQuery",
					Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation.",
				},
				Errors: []*bigqueryapi.ErrorProto{
					{
						Reason:  "invalidQuery",
						Message: emptyText,
					},
				},
			},
			Statistics: &bigqueryapi.JobStatistics{},
		}
		if skipReason, fatalErr := validateContinuousExportDryRunJob(jobEmptySecondary, nil); fatalErr == nil || skipReason != "" {
			t.Fatalf("validateContinuousExportDryRunJob(reservation primary + empty secondary %q) = (skip=%q, err=%v), want (\"\", non-nil fatalErr)", emptyText, skipReason, fatalErr)
		}

		// (2) Returned-job adapter: empty primary + reservation secondary item.
		jobEmptyPrimary := &bigqueryapi.Job{
			Status: &bigqueryapi.JobStatus{
				ErrorResult: &bigqueryapi.ErrorProto{
					Reason:  "invalidQuery",
					Message: emptyText,
				},
				Errors: []*bigqueryapi.ErrorProto{
					{
						Reason:  "invalidQuery",
						Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation.",
					},
				},
			},
			Statistics: &bigqueryapi.JobStatistics{},
		}
		if skipReason, fatalErr := validateContinuousExportDryRunJob(jobEmptyPrimary, nil); fatalErr == nil || skipReason != "" {
			t.Fatalf("validateContinuousExportDryRunJob(empty primary %q + reservation secondary) = (skip=%q, err=%v), want (\"\", non-nil fatalErr)", emptyText, skipReason, fatalErr)
		}

		// (3) Direct-RPC googleapi.Error adapter: reservation primary + empty secondary item.
		directEmptySecondary := &googleapi.Error{
			Code:    http.StatusBadRequest,
			Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation.",
			Errors: []googleapi.ErrorItem{
				{Reason: "invalidQuery", Message: emptyText},
			},
		}
		if isExpectedContinuousQueryDryRunError(directEmptySecondary) {
			t.Fatalf("isExpectedContinuousQueryDryRunError(reservation primary + empty secondary %q) = true, want false", emptyText)
		}
		if skipReason, fatalErr := validateContinuousExportDryRunJob(nil, directEmptySecondary); fatalErr == nil || skipReason != "" {
			t.Fatalf("validateContinuousExportDryRunJob(direct reservation primary + empty secondary %q) = (skip=%q, err=%v), want (\"\", non-nil fatalErr)", emptyText, skipReason, fatalErr)
		}

		// (4) Direct-RPC googleapi.Error adapter: empty primary + reservation secondary item.
		directEmptyPrimary := &googleapi.Error{
			Code:    http.StatusBadRequest,
			Message: emptyText,
			Errors: []googleapi.ErrorItem{
				{Reason: "invalidQuery", Message: "Continuous queries require the project to have a CONTINUOUS assignment to a reservation."},
			},
		}
		if isExpectedContinuousQueryDryRunError(directEmptyPrimary) {
			t.Fatalf("isExpectedContinuousQueryDryRunError(empty primary %q + reservation secondary) = true, want false", emptyText)
		}
		if skipReason, fatalErr := validateContinuousExportDryRunJob(nil, directEmptyPrimary); fatalErr == nil || skipReason != "" {
			t.Fatalf("validateContinuousExportDryRunJob(direct empty primary %q + reservation secondary) = (skip=%q, err=%v), want (\"\", non-nil fatalErr)", emptyText, skipReason, fatalErr)
		}
	}
	jobNilStats := &bigqueryapi.Job{
		Status: &bigqueryapi.JobStatus{},
	}
	if skipReason, fatalErr := validateContinuousExportDryRunJob(jobNilStats, nil); fatalErr == nil || skipReason != "" {
		t.Fatalf("validateContinuousExportDryRunJob(nil Statistics) = (skip=%q, err=%v), want non-nil fatalErr", skipReason, fatalErr)
	}
	// P2-1 regression: Status.Errors containing a nil entry must not panic;
	// validateContinuousExportDryRunJob must return a fatal validation error.
	jobNilErrorEntry := &bigqueryapi.Job{
		Status: &bigqueryapi.JobStatus{
			Errors: []*bigqueryapi.ErrorProto{nil},
		},
		Statistics: &bigqueryapi.JobStatistics{},
	}
	if skipReason, fatalErr := validateContinuousExportDryRunJob(jobNilErrorEntry, nil); fatalErr == nil || skipReason != "" {
		t.Fatalf("validateContinuousExportDryRunJob(Status.Errors=[nil]) = (skip=%q, err=%v), want non-nil fatalErr for nil error entry", skipReason, fatalErr)
	}
	jobOK := &bigqueryapi.Job{
		Status:     &bigqueryapi.JobStatus{},
		Statistics: &bigqueryapi.JobStatistics{TotalBytesProcessed: 0},
	}
	if skipReason, fatalErr := validateContinuousExportDryRunJob(jobOK, nil); fatalErr != nil || skipReason != "" {
		t.Fatalf("validateContinuousExportDryRunJob(valid dry-run job) = (skip=%q, err=%v), want (\"\", nil)", skipReason, fatalErr)
	}
}

func TestShouldDeleteDatasetOnCreate(t *testing.T) {
	if !shouldDeleteDatasetOnCreate(nil) {
		t.Error("shouldDeleteDatasetOnCreate(nil) = false, want true")
	}
	if shouldDeleteDatasetOnCreate(errors.New("googleapi: Error 409: Already Exists: Dataset")) {
		t.Error("shouldDeleteDatasetOnCreate(409 Already Exists) = true, want false")
	}
	ds1 := uniqueEphemeralDatasetID("otel_exporter_it")
	ds2 := uniqueEphemeralDatasetID("otel_exporter_it")
	if !strings.HasPrefix(ds1, "otel_exporter_it_") || ds1 == ds2 {
		t.Errorf("uniqueEphemeralDatasetID produced non-unique or malformed IDs: %q vs %q", ds1, ds2)
	}
}

func TestEphemeralDatasetOwnershipFake409NeverDeletes(t *testing.T) {
	var createCalls atomic.Int32
	var deleteCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/datasets"):
			createCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":{"code":409,"message":"Already Exists: Dataset test-proj:preexisting_ds","errors":[{"reason":"duplicate"}]}}`))
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/datasets/"):
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	ctx := context.Background()
	admin, err := bq.NewClient(ctx, "test-proj",
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("bq.NewClient: %v", err)
	}
	defer func() { _ = admin.Close() }()

	// Use a callback pair that captures cleanup registration so we can
	// verify delete-on-409 is NOT registered.
	var cleanups []func()
	var capturedErrors []string
	createErr := createOwnedEphemeralDataset(ctx, admin, "preexisting_ds", "US",
		func(fn func()) { cleanups = append(cleanups, fn) },
		func(err error) { capturedErrors = append(capturedErrors, err.Error()) },
	)
	if createErr == nil {
		t.Fatal("createOwnedEphemeralDataset on 409 response returned nil error, want 409 error")
	}
	for _, fn := range cleanups {
		fn()
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("POST /datasets calls = %d, want 1", got)
	}
	if got := deleteCalls.Load(); got != 0 {
		t.Fatalf("DELETE /datasets calls after 409 = %d, want 0 (must never delete pre-existing dataset)", got)
	}

	// Also verify signalExporter.ensureDataset ownership tracking (createdDataset)
	// across: (1) pre-existing 200 OK DatasetMetadata, (2) 404 DatasetMetadata +
	// concurrent 409 CreateDataset, and (3) 404 DatasetMetadata + 200 OK CreateDataset.
	var mode atomic.Int32 // 1 = existing 200, 2 = 404 then 409, 3 = 404 then 200
	var ensureDeleteCalls atomic.Int32
	ensureSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch mode.Load() {
		case 1:
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/datasets/") {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"datasetReference":{"projectId":"test-proj","datasetId":"ds"},"location":"US"}`))
				return
			}
		case 2:
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/datasets/") {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":{"code":404,"message":"Not found: Dataset test-proj:ds","errors":[{"reason":"notFound"}]}}`))
				return
			}
			if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/datasets") {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":{"code":409,"message":"Already Exists: Dataset test-proj:ds","errors":[{"reason":"duplicate"}]}}`))
				return
			}
		case 3:
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/datasets/") {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":{"code":404,"message":"Not found: Dataset test-proj:ds","errors":[{"reason":"notFound"}]}}`))
				return
			}
			if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/datasets") {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"datasetReference":{"projectId":"test-proj","datasetId":"ds"},"location":"US"}`))
				return
			}
		}
		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/datasets/") {
			ensureDeleteCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ensureSrv.Close()

	ensureClient, err := bq.NewClient(ctx, "test-proj",
		option.WithEndpoint(ensureSrv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("bq.NewClient(ensureSrv): %v", err)
	}
	defer func() { _ = ensureClient.Close() }()

	destAdmin := &bigQueryDestinationAdmin{
		client:   ensureClient,
		httpBase: &http.Transport{},
		dataset:  ensureClient.Dataset("ds"),
	}
	cfg := createDefaultConfig().(*Config)
	cfg.Project = "test-proj"
	cfg.Dataset = "ds"
	cfg.Location = "US"
	cfg.AutoCreate.Dataset = true

	// Case 1: Pre-existing 200 OK DatasetMetadata -> createdDataset must remain false.
	mode.Store(1)
	seExisting := &signalExporter{cfg: cfg, logger: zap.NewNop()}
	if err := seExisting.ensureDataset(ctx, destAdmin); err != nil {
		t.Fatalf("ensureDataset(existing 200 OK) error: %v", err)
	}
	if seExisting.createdDataset.Load() {
		t.Fatal("ensureDataset(existing 200 OK) set createdDataset=true, want false")
	}

	// Case 2: 404 DatasetMetadata + 409 AlreadyExists CreateDataset -> createdDataset must remain false.
	mode.Store(2)
	se409 := &signalExporter{cfg: cfg, logger: zap.NewNop()}
	if err := se409.ensureDataset(ctx, destAdmin); err != nil {
		t.Fatalf("ensureDataset(404 then 409) error: %v", err)
	}
	if se409.createdDataset.Load() {
		t.Fatal("ensureDataset(404 then 409) set createdDataset=true, want false")
	}
	if ensureDeleteCalls.Load() != 0 {
		t.Fatalf("DELETE /datasets calls after 200/409 = %d, want 0", ensureDeleteCalls.Load())
	}

	// Case 3: 404 DatasetMetadata + 200 OK CreateDataset -> createdDataset must be true.
	mode.Store(3)
	seCreated := &signalExporter{cfg: cfg, logger: zap.NewNop()}
	if err := seCreated.ensureDataset(ctx, destAdmin); err != nil {
		t.Fatalf("ensureDataset(404 then 200 OK) error: %v", err)
	}
	if !seCreated.createdDataset.Load() {
		t.Fatal("ensureDataset(404 then 200 OK) set createdDataset=false, want true")
	}
}

// TestCreateOwnedEphemeralDatasetDeleteFailureReportsError verifies that when
// Dataset.Create succeeds (200 OK) but DeleteWithContents fails (403/500), the
// cleanup error is reported through the reportCleanupError callback rather than
// silently discarded.
func TestCreateOwnedEphemeralDatasetDeleteFailureReportsError(t *testing.T) {
	var deleteCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/datasets"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"datasetReference":{"projectId":"test-proj","datasetId":"ephemeral_ds"},"location":"US"}`))
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/datasets/"):
			deleteCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"code":403,"message":"Permission denied: datasets.delete"}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	ctx := context.Background()
	admin, err := bq.NewClient(ctx, "test-proj",
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("bq.NewClient: %v", err)
	}
	defer func() { _ = admin.Close() }()

	var capturedErrors []string
	cleanupFn := func(fn func()) { fn() }
	createErr := createOwnedEphemeralDataset(ctx, admin, "ephemeral_ds", "US",
		cleanupFn,
		func(err error) { capturedErrors = append(capturedErrors, err.Error()) },
	)
	if createErr != nil {
		t.Fatalf("createOwnedEphemeralDataset returned error on 200 OK: %v", createErr)
	}
	if deleteCalls.Load() == 0 {
		t.Fatalf("DELETE /datasets calls = %d, want >= 1 (cleanup must attempt delete)", deleteCalls.Load())
	}
	if len(capturedErrors) == 0 {
		t.Fatalf("DeleteWithContents failure was not reported through reportCleanupError; want >= 1 error")
	}
	if !strings.Contains(capturedErrors[0], "delete ephemeral dataset") {
		t.Fatalf("DeleteWithContents error message = %q, want to contain 'delete ephemeral dataset'", capturedErrors[0])
	}
}

// startAndRegisterColdStartCleanup runs the cold-start sequence that both
// TestAutoCreateLiveBigQuery (integration_auto_create_test.go) and
// TestColdStartCleanupRegistration (this file) must share: call startFn,
// register cleanup reading se.createdDataset.Load() inside the helper (so the
// caller cannot pass a stale or forced value), then fail when startErr is
// non-nil. Moving the registerColdStartCleanup call below the startErr check,
// replacing se.createdDataset.Load() with true, or dropping the startErr != nil
// fatalf check breaks the hermetic test.
//
// This helper has no //go:build integration tag, so it is compiled in both
// `go test ./...` and `go test -tags=integration ./...`.
func startAndRegisterColdStartCleanup(
	ctx context.Context,
	startFn func(context.Context) error,
	se *signalExporter,
	registerCleanup func(func()),
	reportCleanupError func(error),
	admin *bq.Client,
	dataset string,
	fatalf func(string, ...any),
) {
	startErr := startFn(ctx)
	registerColdStartCleanup(registerCleanup, reportCleanupError, admin, dataset, se.createdDataset.Load())
	if startErr != nil {
		fatalf("cold start failed: %v", startErr)
	}
}

// registerColdStartCleanup registers a DeleteWithContents cleanup for a
// pre-created dataset only when owned is true. When owned is false (dataset
// was pre-created and is not owned by this test), no DELETE is registered.
func registerColdStartCleanup(registerCleanup func(func()), reportCleanupError func(error), admin *bq.Client, dataset string, owned bool) {
	if !owned {
		return
	}
	registerCleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := admin.Dataset(dataset).DeleteWithContents(cleanCtx); err != nil {
			reportCleanupError(fmt.Errorf("delete ephemeral dataset %s: %w", dataset, err))
		}
	})
}

// TestColdStartCleanupRegistration verifies that registerColdStartCleanup
// issues zero DELETEs when owned=false and exactly one DELETE when owned=true,
// and that deletion errors reach the reportCleanupError callback.
func TestColdStartCleanupRegistration(t *testing.T) {
	var deleteCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/datasets/"):
			deleteCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"code":403,"message":"Permission denied"}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	ctx := context.Background()
	admin, err := bq.NewClient(ctx, "test-proj",
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("bq.NewClient: %v", err)
	}
	defer func() { _ = admin.Close() }()

	// owned=false: no DELETE should be registered.
	deleteCalls.Store(0)
	var errsFalse []string
	registerColdStartCleanup(func(fn func()) { fn() }, func(err error) { errsFalse = append(errsFalse, err.Error()) }, admin, "ds_owned_false", false)
	if deleteCalls.Load() != 0 {
		t.Fatalf("owned=false: DELETE calls = %d, want 0", deleteCalls.Load())
	}

	// owned=true: exactly one DELETE, and the error must reach the callback.
	deleteCalls.Store(0)
	var errsTrue []string
	registerColdStartCleanup(func(fn func()) { fn() }, func(err error) { errsTrue = append(errsTrue, err.Error()) }, admin, "ds_owned_true", true)
	if deleteCalls.Load() != 1 {
		t.Fatalf("owned=true: DELETE calls = %d, want 1", deleteCalls.Load())
	}
	if len(errsTrue) == 0 {
		t.Fatalf("owned=true: DeleteWithContents 403 was not reported through reportCleanupError")
	}
	if !strings.Contains(errsTrue[0], "delete ephemeral dataset") {
		t.Fatalf("owned=true: error = %q, want 'delete ephemeral dataset'", errsTrue[0])
	}

	// Round-8 P2-1 regression: verify the cold-start sequence when dataset
	// creation succeeds (setting createdDataset=true) and a later step in
	// prepareSignalDestination (creating the destination table) fails.
	// Cleanup must still be registered before failing on startErr so
	// creates=1 deletes=1, whereas pre-existing datasets (200 OK metadata or
	// 409 Conflict create) that fail later during table creation get deletes=0.
	var coldMode atomic.Int32 // 1 = 404 ds + 200 create + 500 table, 2 = 200 ds + 500 table, 3 = 404 ds + 409 create + 500 table
	var coldCreates atomic.Int32
	var coldDeletes atomic.Int32
	var coldDeleteStatus atomic.Int32
	coldDeleteStatus.Store(http.StatusNoContent)

	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/datasets/cold_ds"):
			if coldMode.Load() == 2 {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"datasetReference":{"projectId":"test-proj","datasetId":"cold_ds"},"location":"US"}`))
				return
			}
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":404,"message":"Not found: Dataset test-proj:cold_ds","errors":[{"reason":"notFound"}]}}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/datasets"):
			coldCreates.Add(1)
			if coldMode.Load() == 3 {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":{"code":409,"message":"Already Exists: Dataset test-proj:cold_ds","errors":[{"reason":"duplicate"}]}}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"datasetReference":{"projectId":"test-proj","datasetId":"cold_ds"},"location":"US"}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/tables/"):
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":404,"message":"Not found: Table","errors":[{"reason":"notFound"}]}}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/tables"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":400,"message":"Simulated table creation failure after dataset create","errors":[{"reason":"invalid"}]}}`))
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/datasets/cold_ds"):
			coldDeletes.Add(1)
			st := int(coldDeleteStatus.Load())
			w.WriteHeader(st)
			if st >= 400 {
				_, _ = w.Write([]byte(`{"error":{"code":403,"message":"Delete forbidden on cold_ds"}}`))
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer coldSrv.Close()

	coldClient, err := bq.NewClient(ctx, "test-proj",
		option.WithEndpoint(coldSrv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("bq.NewClient(coldSrv): %v", err)
	}
	defer func() { _ = coldClient.Close() }()

	coldAdmin := &bigQueryDestinationAdmin{
		client:   coldClient,
		httpBase: &http.Transport{},
		dataset:  coldClient.Dataset("cold_ds"),
	}
	coldCfg := createDefaultConfig().(*Config)
	coldCfg.Project = "test-proj"
	coldCfg.Dataset = "cold_ds"
	coldCfg.Location = "US"
	coldCfg.AutoCreate.Dataset = true
	coldCfg.AutoCreate.Tables = true

	// Subcase A: Dataset created (200 OK), then table creation fails (400), and
	// cleanup delete fails (403) -> creates=1, deletes=1, delete error reported.
	// The shared helper must register cleanup BEFORE failing on startErr;
	// moving the register below the fatalf check means cleanup is never
	// registered when start fails, and the fatalf closure catches that.
	coldMode.Store(1)
	coldCreates.Store(0)
	coldDeletes.Store(0)
	coldDeleteStatus.Store(http.StatusForbidden)
	seFailAfterCreate := &signalExporter{
		cfg:    coldCfg,
		logger: zap.NewNop(),
		table:  "otel_spans",
		schema: schema.SpansTableSchema(),
	}
	var coldCleanups []func()
	var coldCleanupErrs []string
	fatalfCalled := false
	startAndRegisterColdStartCleanup(
		ctx,
		func(ctx context.Context) error { return seFailAfterCreate.prepareSignalDestination(ctx, coldAdmin) },
		seFailAfterCreate,
		func(fn func()) { coldCleanups = append(coldCleanups, fn) },
		func(err error) { coldCleanupErrs = append(coldCleanupErrs, err.Error()) },
		coldClient,
		coldCfg.Dataset,
		func(format string, args ...any) {
			// fatalf for subcase A: start is expected to fail, so this is
			// reached only after cleanup was registered. Verify that
			// cleanup was indeed registered before this point; if not,
			// the helper has the wrong ordering (mutation).
			if len(coldCleanups) == 0 {
				t.Fatalf("subcase A: startAndRegisterColdStartCleanup called fatalf before registering cleanup: "+format, args...)
			}
			fatalfCalled = true
		},
	)
	if !fatalfCalled {
		t.Fatal("expected fatalf to be called on startErr != nil")
	}
	if !seFailAfterCreate.createdDataset.Load() {
		t.Fatal("createdDataset = false after 200 OK CreateDataset and 400 CreateTable, want true")
	}
	for _, fn := range coldCleanups {
		fn()
	}
	if coldCreates.Load() != 1 || coldDeletes.Load() != 1 {
		t.Fatalf("after-create/before-start-complete failure: creates=%d deletes=%d, want creates=1 deletes=1", coldCreates.Load(), coldDeletes.Load())
	}
	if len(coldCleanupErrs) != 1 || !strings.Contains(coldCleanupErrs[0], "delete ephemeral dataset cold_ds") {
		t.Fatalf("after-create cleanup errors = %v, want 1 error containing 'delete ephemeral dataset cold_ds'", coldCleanupErrs)
	}

	// Subcase B: Pre-existing dataset (200 OK on metadata) fails during table
	// creation -> deletes=0. The shared helper reads createdDataset (false)
	// inside, so forcing true would register an unwanted DELETE.
	coldMode.Store(2)
	coldCreates.Store(0)
	coldDeletes.Store(0)
	sePreExisting := &signalExporter{
		cfg:    coldCfg,
		logger: zap.NewNop(),
		table:  "otel_spans",
		schema: schema.SpansTableSchema(),
	}
	coldCleanups = nil
	fatalfCalled = false
	startAndRegisterColdStartCleanup(
		ctx,
		func(ctx context.Context) error { return sePreExisting.prepareSignalDestination(ctx, coldAdmin) },
		sePreExisting,
		func(fn func()) { coldCleanups = append(coldCleanups, fn) },
		func(err error) { t.Errorf("unexpected cleanup error: %v", err) },
		coldClient,
		coldCfg.Dataset,
		func(format string, args ...any) {
			if len(coldCleanups) != 0 {
				t.Fatalf("subcase B: cleanup registered for pre-existing dataset (createdDataset should be false): "+format, args...)
			}
			fatalfCalled = true
		},
	)
	if !fatalfCalled {
		t.Fatal("expected fatalf to be called on startErr != nil")
	}
	for _, fn := range coldCleanups {
		fn()
	}
	if coldCreates.Load() != 0 || coldDeletes.Load() != 0 {
		t.Fatalf("pre-existing 200 dataset failure: creates=%d deletes=%d, want creates=0 deletes=0", coldCreates.Load(), coldDeletes.Load())
	}

	// Subcase C: Concurrent 409 Conflict on dataset create + table creation
	// failure -> creates=1, deletes=0. createdDataset is false (409 is not a
	// successful create), so no DELETE is registered.
	coldMode.Store(3)
	coldCreates.Store(0)
	coldDeletes.Store(0)
	seConflict := &signalExporter{
		cfg:    coldCfg,
		logger: zap.NewNop(),
		table:  "otel_spans",
		schema: schema.SpansTableSchema(),
	}
	coldCleanups = nil
	fatalfCalled = false
	startAndRegisterColdStartCleanup(
		ctx,
		func(ctx context.Context) error { return seConflict.prepareSignalDestination(ctx, coldAdmin) },
		seConflict,
		func(fn func()) { coldCleanups = append(coldCleanups, fn) },
		func(err error) { t.Errorf("unexpected cleanup error: %v", err) },
		coldClient,
		coldCfg.Dataset,
		func(format string, args ...any) {
			if len(coldCleanups) != 0 {
				t.Fatalf("subcase C: cleanup registered for 409-conflict dataset (createdDataset should be false): "+format, args...)
			}
			fatalfCalled = true
		},
	)
	if !fatalfCalled {
		t.Fatal("expected fatalf to be called on startErr != nil")
	}
	for _, fn := range coldCleanups {
		fn()
	}
	if coldCreates.Load() != 1 || coldDeletes.Load() != 0 {
		t.Fatalf("409 conflict dataset failure: creates=%d deletes=%d, want creates=1 deletes=0", coldCreates.Load(), coldDeletes.Load())
	}

	// Subcase D: Successful startup (startErr == nil) after dataset creation
	// (createdDataset == true) registers cleanup without invoking fatalf.
	seSuccess := &signalExporter{
		cfg:    coldCfg,
		logger: zap.NewNop(),
		table:  "otel_spans",
		schema: schema.SpansTableSchema(),
	}
	coldCleanups = nil
	fatalfCalled = false
	startAndRegisterColdStartCleanup(
		ctx,
		func(context.Context) error {
			seSuccess.createdDataset.Store(true)
			return nil
		},
		seSuccess,
		func(fn func()) { coldCleanups = append(coldCleanups, fn) },
		func(err error) { t.Errorf("unexpected cleanup error: %v", err) },
		coldClient,
		coldCfg.Dataset,
		func(format string, args ...any) {
			fatalfCalled = true
			t.Fatalf("subcase D: unexpected fatalf when startErr == nil: "+format, args...)
		},
	)
	if fatalfCalled {
		t.Fatal("subcase D: expected fatalf not to be called when startErr == nil")
	}
	if len(coldCleanups) != 1 {
		t.Fatalf("subcase D: len(coldCleanups) = %d, want 1 when startErr == nil and createdDataset == true", len(coldCleanups))
	}
}

func TestOffloadPayloadRefRejectsStaleOrWrongGenerationOnOverwrite(t *testing.T) {
	store := newMemOffloadStore()
	origBytes := bytes.Repeat([]byte("original-redacted-payload-"), 12)
	ver1, err := store.WriteObject(context.Background(), "hermetic-bucket", "prefix/spans/obj.txt", "text/plain", origBytes)
	if err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	uri := "gs://hermetic-bucket/prefix/spans/obj.txt"
	sum1 := sha256.Sum256(origBytes)
	sha1 := hex.EncodeToString(sum1[:])

	row := map[string]any{
		"payload_refs": []map[string]any{
			{
				"storage_mode":   "gcs_object_ref",
				"attribute_path": "attributes.gen_ai.prompt",
				"part_index":     int64(0),
				"mime_type":      "text/plain",
				"byte_size":      int64(len(origBytes)),
				"sha256":         sha1,
				"object_ref": map[string]any{
					"uri":        uri,
					"version":    ver1,
					"authorizer": "us.conn",
					"details":    fmt.Sprintf(`{"sha256":%q,"content_type":"text/plain"}`, sha1),
				},
			},
		},
	}
	if err := assertFakeRowPayloadRef(store, row, "gs://hermetic-bucket/prefix/spans/", "us.conn", "attributes.gen_ai.prompt", "text/plain"); err != nil {
		t.Fatalf("assertFakeRowPayloadRef before overwrite: %v", err)
	}

	// Overwrite the same URI with the ORIGINAL bytes (same bytes, new generation)
	// so the stale-generation check is independent of payload integrity (size
	// and checksum).  Removing the ?generation= parameter or disabling the fake
	// store's generation comparison must make this test fail.
	ver2 := store.Overwrite(uri, origBytes)
	if ver2 == ver1 {
		t.Fatalf("Overwrite version = %q, want != %q", ver2, ver1)
	}
	// Fetching with the original generation ver1 must fail on an overwritten
	// store even though the bytes are identical, because the generation
	// pinned in the payload_ref no longer matches the current object.
	if err := assertFakeRowPayloadRef(store, row, "gs://hermetic-bucket/prefix/spans/", "us.conn", "attributes.gen_ai.prompt", "text/plain"); err == nil {
		t.Fatal("assertFakeRowPayloadRef succeeded after same-bytes overwrite with new generation, want generation mismatch error")
	}

	qRows := []map[string]any{
		{
			"signal":      "traces",
			"error_code":  "limit_exceeded",
			"raw_payload": "",
			"payload_ref": map[string]any{
				"uri":        uri,
				"version":    ver1,
				"authorizer": "us.conn",
				"details":    fmt.Sprintf(`{"sha256":%q,"content_type":"application/json"}`, sha1),
			},
		},
	}
	if _, err := formatFakeQuarantineRowsAsJSONL(store, qRows); err == nil {
		t.Fatal("formatFakeQuarantineRowsAsJSONL succeeded with stale generation after overwrite, want generation mismatch error")
	}
}

func TestTerraformGitHubWIFOIDCClaimMappingAndCondition(t *testing.T) {
	raw, err := os.ReadFile("deploy/terraform/ci_wif.tf")
	if err != nil {
		t.Fatalf("read deploy/terraform/ci_wif.tf: %v", err)
	}
	tf := string(raw)

	// Extract attribute_mapping entries from google_iam_workload_identity_pool_provider.github_ci.
	mapBlockRe := regexp.MustCompile(`(?s)attribute_mapping\s*=\s*\{(.*?)\n\s*\}`)
	mm := mapBlockRe.FindStringSubmatch(tf)
	if mm == nil {
		t.Fatal("deploy/terraform/ci_wif.tf missing attribute_mapping block")
	}
	entryRe := regexp.MustCompile(`"([^"]+)"\s*=\s*"([^"]+)"`)
	mapping := make(map[string]string)
	for _, m := range entryRe.FindAllStringSubmatch(mm[1], -1) {
		mapping[m[1]] = m[2]
	}

	// Regression check for P1-1: attribute.workflow_ref MUST map from
	// assertion.workflow_ref (always present on schedule and workflow_dispatch),
	// never assertion.job_workflow_ref.
	if got := mapping["attribute.workflow_ref"]; got != "assertion.workflow_ref" {
		t.Fatalf("attribute_mapping[\"attribute.workflow_ref\"] = %q, want \"assertion.workflow_ref\"", got)
	}

	condBlockRe := regexp.MustCompile(`(?s)attribute_condition\s*=\s*join\(" && ",\s*\[(.*?)\n\s*\]\)`)
	cm := condBlockRe.FindStringSubmatch(tf)
	if cm == nil {
		t.Fatal("deploy/terraform/ci_wif.tf missing attribute_condition join block")
	}
	condText := cm[1]
	for _, wantClause := range []string{
		`attribute.repository_owner_id == \"${var.github_repository_owner_id}\"`,
		`attribute.repository_id == \"${var.github_repository_id}\"`,
		`attribute.repository == \"${var.github_repository}\"`,
		`attribute.ref == \"refs/heads/main\"`,
		`attribute.workflow_ref == \"${var.github_repository}/${var.github_workflow_ref}@refs/heads/main\"`,
		`attribute.event_name in [${join(", ", formatlist("%q", var.github_allowed_events))}]`,
	} {
		if !strings.Contains(condText, wantClause) {
			t.Fatalf("attribute_condition missing clause %q in:\n%s", wantClause, condText)
		}
	}

	const (
		cfgRepo        = "haiyuan-eng-google/BQ-OTEL-Exporter"
		cfgRepoID      = "123456789"
		cfgOwnerID     = "987654321"
		cfgWorkflowRef = ".github/workflows/live-integration.yml"
	)
	allowedEvents := []string{"schedule", "workflow_dispatch"}

	// evaluateWIF simulates GCP Workload Identity Pool Provider attribute_mapping +
	// attribute_condition against a raw GitHub Actions OIDC JWT assertion payload.
	evaluateWIF := func(assertion map[string]string) bool {
		attrs := make(map[string]string, len(mapping))
		for attrKey, expr := range mapping {
			claimName := strings.TrimPrefix(expr, "assertion.")
			attrs[attrKey] = assertion[claimName]
		}
		return attrs["attribute.repository_owner_id"] == cfgOwnerID &&
			attrs["attribute.repository_id"] == cfgRepoID &&
			attrs["attribute.repository"] == cfgRepo &&
			attrs["attribute.ref"] == "refs/heads/main" &&
			attrs["attribute.workflow_ref"] == cfgRepo+"/"+cfgWorkflowRef+"@refs/heads/main" &&
			slices.Contains(allowedEvents, attrs["attribute.event_name"])
	}

	validBase := func() map[string]string {
		return map[string]string{
			"sub":                 "repo:haiyuan-eng-google/BQ-OTEL-Exporter:ref:refs/heads/main",
			"actor":               "haiyuan-eng-google",
			"repository":          cfgRepo,
			"repository_id":       cfgRepoID,
			"repository_owner_id": cfgOwnerID,
			"ref":                 "refs/heads/main",
			// Top-level GitHub Actions schedule/workflow_dispatch OIDC tokens emit
			// workflow_ref (and do NOT need job_workflow_ref).
			"workflow_ref": "haiyuan-eng-google/BQ-OTEL-Exporter/.github/workflows/live-integration.yml@refs/heads/main",
			"event_name":   "schedule",
		}
	}

	cases := []struct {
		name   string
		mutate func(m map[string]string)
		want   bool
	}{
		{
			name:   "1. positive: scheduled run on refs/heads/main",
			mutate: func(m map[string]string) { m["event_name"] = "schedule" },
			want:   true,
		},
		{
			name:   "2. positive: manual workflow_dispatch on refs/heads/main",
			mutate: func(m map[string]string) { m["event_name"] = "workflow_dispatch" },
			want:   true,
		},
		{
			name: "3. negative: fork pull_request",
			mutate: func(m map[string]string) {
				m["event_name"] = "pull_request"
				m["ref"] = "refs/pull/70/merge"
			},
			want: false,
		},
		{
			name: "4. negative: pull_request_target on refs/heads/main",
			mutate: func(m map[string]string) {
				m["event_name"] = "pull_request_target"
			},
			want: false,
		},
		{
			name: "5. negative: other workflow file on refs/heads/main",
			mutate: func(m map[string]string) {
				m["workflow_ref"] = "haiyuan-eng-google/BQ-OTEL-Exporter/.github/workflows/ci.yml@refs/heads/main"
			},
			want: false,
		},
		{
			name: "6. negative: tag ref",
			mutate: func(m map[string]string) {
				m["ref"] = "refs/tags/v0.1.0"
				m["workflow_ref"] = "haiyuan-eng-google/BQ-OTEL-Exporter/.github/workflows/live-integration.yml@refs/tags/v0.1.0"
			},
			want: false,
		},
		{
			name: "7. negative: wrong repository_owner_id",
			mutate: func(m map[string]string) {
				m["repository_owner_id"] = "111111111"
			},
			want: false,
		},
	}

	for _, tc := range cases {
		claims := validBase()
		tc.mutate(claims)
		if got := evaluateWIF(claims); got != tc.want {
			t.Errorf("%s: evaluateWIF = %v, want %v (claims=%v)", tc.name, got, tc.want, claims)
		}
	}
}
