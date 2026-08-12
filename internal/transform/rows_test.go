// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var fixedNow = time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)

func testOpts() Options {
	return Options{Limits: DefaultLimits(), Now: func() time.Time { return fixedNow }}
}

func newTraces() (ptrace.Traces, ptrace.Span) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	rs.SetSchemaUrl("https://example.test/resource")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("scope")
	ss.Scope().SetVersion("1.2.3")
	ss.SetSchemaUrl("https://example.test/scope")

	span := ss.Spans().AppendEmpty()
	span.SetName("GET /cart")
	span.SetKind(ptrace.SpanKindServer)
	span.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	span.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_123_456_789))
	span.SetEndTimestamp(pcommon.Timestamp(1_700_000_001_123_456_789))
	span.Status().SetCode(ptrace.StatusCodeOk)
	span.Attributes().PutInt("http.status_code", 200)
	return td, span
}

func TestSpanRow(t *testing.T) {
	td, _ := newTraces()
	rows, rejects := SpanRows(td, testOpts())
	if len(rejects) != 0 {
		t.Fatalf("unexpected rejections: %+v", rejects)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	r := rows[0]

	checks := map[string]any{
		"trace_id":                  "0102030405060708090a0b0c0d0e0f10",
		"span_id":                   "0102030405060708",
		"name":                      "GET /cart",
		"kind":                      "Server",
		"service_name":              "checkout",
		"status_code":               "Ok",
		"start_time_unix_nano":      "1700000000123456789",
		"end_time_unix_nano":        "1700000001123456789",
		"duration_ns":               int64(1_000_000_000),
		"resource_schema_url":       "https://example.test/resource",
		"scope_schema_url":          "https://example.test/scope",
		"scope_name":                "scope",
		"export_received_timestamp": fixedNow.UnixMicro(),
	}
	for k, want := range checks {
		if got := r[k]; got != want {
			t.Errorf("%s = %v (%T), want %v (%T)", k, got, got, want, want)
		}
	}

	// TIMESTAMP columns truncate to microseconds; the raw nanos live in the
	// paired string column.
	if got := r["start_timestamp"]; got != int64(1_700_000_000_123_456) {
		t.Errorf("start_timestamp = %v, want microsecond truncation", got)
	}
	if got, want := r["span_attributes"], `{"http.status_code":{"int_value":"200"}}`; got != want {
		t.Errorf("span_attributes = %v, want %v", got, want)
	}
}

// An absent optional value must leave the column NULL, which means omitting
// the key. Writing "" would turn "no parent" into "a parent whose ID is the
// empty string".
func TestAbsentOptionalColumnsAreOmitted(t *testing.T) {
	td, _ := newTraces()
	rows, _ := SpanRows(td, testOpts())
	for _, col := range []string{"parent_span_id", "trace_state", "status_message"} {
		if _, present := rows[0][col]; present {
			t.Errorf("%s should be omitted when unset, so the column stays NULL", col)
		}
	}
}

func TestSpanEventsAndLinks(t *testing.T) {
	td, span := newTraces()
	e := span.Events().AppendEmpty()
	e.SetName("exception")
	e.SetTimestamp(pcommon.Timestamp(1_700_000_000_500_000_000))
	e.Attributes().PutStr("exception.type", "IOError")

	l := span.Links().AppendEmpty()
	l.SetTraceID(pcommon.TraceID{9})
	l.SetSpanID(pcommon.SpanID{8})

	rows, _ := SpanRows(td, testOpts())
	events, ok := rows[0]["events"].([]Row)
	if !ok || len(events) != 1 {
		t.Fatalf("events = %#v, want one row", rows[0]["events"])
	}
	if events[0]["name"] != "exception" {
		t.Errorf("event name = %v", events[0]["name"])
	}
	if events[0]["time_unix_nano"] != "1700000000500000000" {
		t.Errorf("event raw nanos = %v", events[0]["time_unix_nano"])
	}
	links, ok := rows[0]["links"].([]Row)
	if !ok || len(links) != 1 {
		t.Fatalf("links = %#v, want one row", rows[0]["links"])
	}
}

func newLogs() (plog.Logs, plog.LogRecord) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "checkout")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	lr.SetObservedTimestamp(pcommon.Timestamp(1_700_000_000_500_000_000))
	lr.SetSeverityNumber(plog.SeverityNumberError)
	lr.SetSeverityText("ERROR")
	lr.Body().SetStr("disk full")
	return ld, lr
}

func TestLogRow(t *testing.T) {
	ld, _ := newLogs()
	rows, rejects := LogRows(ld, testOpts())
	if len(rejects) != 0 || len(rows) != 1 {
		t.Fatalf("rows=%d rejects=%+v", len(rows), rejects)
	}
	r := rows[0]

	if r["timestamp_source"] != "event" {
		t.Errorf("timestamp_source = %v, want event", r["timestamp_source"])
	}
	if r["body"] != `{"string_value":"disk full"}` {
		t.Errorf("body = %v", r["body"])
	}
	fp, _ := r["record_fingerprint"].(string)
	if !strings.HasPrefix(fp, "v1:") || len(fp) != len("v1:")+64 {
		t.Errorf("record_fingerprint = %q, want v1: plus 64 hex chars", fp)
	}
}

// §7.2 fallback chain for the partition key. OTLP permits both timestamps to
// be zero, and a row with no partition key cannot be written at all.
func TestLogTimestampFallback(t *testing.T) {
	tests := []struct {
		name       string
		ts, obs    pcommon.Timestamp
		wantSource string
	}{
		{"event time wins", 1_000_000_000, 2_000_000_000, "event"},
		{"observed when event is zero", 0, 2_000_000_000, "observed"},
		{"exporter when both are zero", 0, 0, "exporter"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ld, lr := newLogs()
			lr.SetTimestamp(tc.ts)
			lr.SetObservedTimestamp(tc.obs)
			rows, _ := LogRows(ld, testOpts())
			if got := rows[0]["timestamp_source"]; got != tc.wantSource {
				t.Errorf("timestamp_source = %v, want %v", got, tc.wantSource)
			}
			if rows[0]["timestamp"] == nil {
				t.Error("timestamp must never be NULL: it is the partition key")
			}
		})
	}
}

// Exact identity requires both halves. An ID without a trusted namespace is
// not an identity, because two producers could mint the same ID.
func TestSourceRecordIdentity(t *testing.T) {
	t.Run("both halves present", func(t *testing.T) {
		ld, lr := newLogs()
		lr.Attributes().PutStr("log.record.uid", "abc-123")
		opts := testOpts()
		opts.SourceRecordNamespace = "prod-fleet"
		opts.SourceRecordIDAttribute = "log.record.uid"

		rows, _ := LogRows(ld, opts)
		if rows[0]["source_record_namespace"] != "prod-fleet" || rows[0]["source_record_id"] != "abc-123" {
			t.Errorf("identity tuple not populated: %v / %v",
				rows[0]["source_record_namespace"], rows[0]["source_record_id"])
		}
	})

	t.Run("no namespace configured", func(t *testing.T) {
		ld, lr := newLogs()
		lr.Attributes().PutStr("log.record.uid", "abc-123")
		opts := testOpts()
		opts.SourceRecordIDAttribute = "log.record.uid"

		rows, _ := LogRows(ld, opts)
		if _, ok := rows[0]["source_record_id"]; ok {
			t.Error("source_record_id must stay NULL without a trusted namespace")
		}
	})
}

// The fingerprint is what dedup partitions on, so identical content must hash
// identically and any content change must move it.
func TestFingerprintStability(t *testing.T) {
	build := func(body string) string {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "svc")
		lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		lr.Body().SetStr(body)
		lr.Attributes().PutStr("a", "1")
		lr.Attributes().PutStr("b", "2")
		rows, _ := LogRows(ld, testOpts())
		return rows[0]["record_fingerprint"].(string)
	}

	if build("same") != build("same") {
		t.Error("identical records must produce identical fingerprints")
	}
	if build("same") == build("different") {
		t.Error("differing bodies must produce differing fingerprints")
	}
}

// export_received_timestamp is ingestion-local. If it fed the fingerprint,
// every retry of the same record would hash differently and deduplication
// would never match anything.
func TestFingerprintIgnoresIngestionTime(t *testing.T) {
	fp := func(now time.Time) string {
		ld, _ := newLogs()
		opts := testOpts()
		opts.Now = func() time.Time { return now }
		rows, _ := LogRows(ld, opts)
		return rows[0]["record_fingerprint"].(string)
	}
	if fp(fixedNow) != fp(fixedNow.Add(72*time.Hour)) {
		t.Fatal("fingerprint must not depend on export_received_timestamp")
	}
}

// Field boundaries must be unambiguous: moving a character from the end of one
// field to the start of the next must not collide.
func TestFingerprintFieldBoundaries(t *testing.T) {
	fp := func(sevText, eventName string) string {
		ld, lr := newLogs()
		lr.SetSeverityText(sevText)
		lr.SetEventName(eventName)
		rows, _ := LogRows(ld, testOpts())
		return rows[0]["record_fingerprint"].(string)
	}
	if fp("ab", "c") == fp("a", "bc") {
		t.Fatal("adjacent fields collide: the canonical form is ambiguous")
	}
}
