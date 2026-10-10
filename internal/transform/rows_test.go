// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
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

// validSpan gives span the IDs and timestamps the span contract requires, so
// a test that builds its own span exercises only the property it targets.
func validSpan(span ptrace.Span) ptrace.Span {
	span.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	span.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	span.SetEndTimestamp(pcommon.Timestamp(1_700_000_001_000_000_000))
	return span
}

// TestSpanRow pins the complete row for a representative span: every column,
// every nested STRUCT field, and the Go type the encoder expects for each.
func TestSpanRow(t *testing.T) {
	td, _ := newTraces()
	rows, rejects := SpanRows(td, testOpts())
	if len(rejects) != 0 {
		t.Fatalf("unexpected rejections: %+v", rejects)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}

	want := Row{
		// TIMESTAMP columns truncate to microseconds; the exact nanos live in
		// the paired INT64 column.
		"start_time":             int64(1_700_000_000_123_456),
		"start_time_unix_nano":   int64(1_700_000_000_123_456_789),
		"end_time":               int64(1_700_000_001_123_456),
		"end_time_unix_nano":     int64(1_700_000_001_123_456_789),
		"receive_time":           fixedNow.UnixMicro(),
		"receive_time_unix_nano": fixedNow.UnixNano(),
		"duration_nano":          int64(1_000_000_000),

		"trace_id": "0102030405060708090a0b0c0d0e0f10",
		"span_id":  "0102030405060708",
		"flags":    int64(0),

		"name":      "GET /cart",
		"kind":      int64(2),
		"kind_name": "SPAN_KIND_SERVER",
		"status":    Row{"code": int64(1), "code_name": "STATUS_CODE_OK"},
		// Plain JSON plus a sidecar recording the OTLP type that the plain
		// form loses: the value is a decimal string, the schema says int64.
		"attributes":               `{"http.status_code":"200"}`,
		"attributes_schema":        `{"http.status_code":"int64"}`,
		"dropped_attributes_count": int64(0),

		"service_name": "checkout",
		"resource": Row{
			"attributes":               `{"service.name":"checkout"}`,
			"attributes_schema":        `{"service.name":"string"}`,
			"dropped_attributes_count": int64(0),
		},
		"resource_schema_url": "https://example.test/resource",
		"instrumentation_scope": Row{
			"name":                     "scope",
			"version":                  "1.2.3",
			"attributes":               "{}",
			"attributes_schema":        "{}",
			"dropped_attributes_count": int64(0),
		},
		"scope_schema_url": "https://example.test/scope",

		"dropped_events_count": int64(0),
		"dropped_links_count":  int64(0),

		"schema_version": schema.Version,
	}

	got := rows[0]
	for col, w := range want {
		if g, ok := got[col]; !ok || !reflect.DeepEqual(g, w) {
			t.Errorf("%s = %#v, want %#v", col, g, w)
		}
	}
	for col, g := range got {
		if _, ok := want[col]; !ok {
			t.Errorf("unexpected column %s = %#v", col, g)
		}
	}
}

// An absent optional value must leave the column NULL, which means omitting
// the key. Writing "" would turn "no parent" into "a parent whose ID is the
// empty string".
func TestAbsentOptionalColumnsAreOmitted(t *testing.T) {
	td, _ := newTraces()
	rows, _ := SpanRows(td, testOpts())
	for _, col := range []string{"parent_span_id", "trace_state", "events", "links", "payload_refs"} {
		if _, present := rows[0][col]; present {
			t.Errorf("%s should be omitted when unset, so the column stays NULL", col)
		}
	}
	if _, present := rows[0]["status"].(Row)["message"]; present {
		t.Error("status.message should be omitted when unset, so the field stays NULL")
	}

	// A span with no resource attributes, scope or schema URLs.
	bare := ptrace.NewTraces()
	validSpan(bare.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty())
	rows, _ = SpanRows(bare, testOpts())
	for _, col := range []string{"service_name", "resource_schema_url", "scope_schema_url"} {
		if _, present := rows[0][col]; present {
			t.Errorf("%s should be omitted when unset, so the column stays NULL", col)
		}
	}
	for _, fld := range []string{"name", "version"} {
		if _, present := rows[0]["instrumentation_scope"].(Row)[fld]; present {
			t.Errorf("instrumentation_scope.%s should be omitted when unset", fld)
		}
	}
}

func TestSpanEventsAndLinks(t *testing.T) {
	td, span := newTraces()
	e := span.Events().AppendEmpty()
	e.SetName("exception")
	e.SetTimestamp(pcommon.Timestamp(1_700_000_000_500_000_123))
	e.Attributes().PutStr("exception.type", "IOError")
	e.SetDroppedAttributesCount(1)

	l := span.Links().AppendEmpty()
	l.SetTraceID(pcommon.TraceID{9})
	l.SetSpanID(pcommon.SpanID{8})
	l.SetFlags(1)
	l.Attributes().PutBool("sampled", true)

	rows, _ := SpanRows(td, testOpts())
	events, ok := rows[0]["events"].([]Row)
	if !ok || len(events) != 1 {
		t.Fatalf("events = %#v, want one row", rows[0]["events"])
	}
	wantEvent := Row{
		"time":                     int64(1_700_000_000_500_000),
		"time_unix_nano":           int64(1_700_000_000_500_000_123),
		"name":                     "exception",
		"attributes":               `{"exception.type":"IOError"}`,
		"attributes_schema":        `{"exception.type":"string"}`,
		"dropped_attributes_count": int64(1),
	}
	if !reflect.DeepEqual(events[0], wantEvent) {
		t.Errorf("event = %#v, want %#v", events[0], wantEvent)
	}

	links, ok := rows[0]["links"].([]Row)
	if !ok || len(links) != 1 {
		t.Fatalf("links = %#v, want one row", rows[0]["links"])
	}
	// The unset trace_state is omitted, so the field stays NULL.
	wantLink := Row{
		"trace_id":                 "09000000000000000000000000000000",
		"span_id":                  "0800000000000000",
		"flags":                    int64(1),
		"attributes":               `{"sampled":true}`,
		"attributes_schema":        `{"sampled":"bool"}`,
		"dropped_attributes_count": int64(0),
	}
	if !reflect.DeepEqual(links[0], wantLink) {
		t.Errorf("link = %#v, want %#v", links[0], wantLink)
	}
}

// OTLP lets an event omit its time. time_unix_nano keeps the zero as sent,
// but the TIMESTAMP stays NULL rather than claiming the Unix epoch.
func TestEventWithoutTimeHasNullTimestamp(t *testing.T) {
	td, span := newTraces()
	span.Events().AppendEmpty().SetName("untimed")
	rows, rejects := SpanRows(td, testOpts())
	if len(rejects) != 0 {
		t.Fatalf("an untimed event must not reject its span: %+v", rejects)
	}
	ev := rows[0]["events"].([]Row)[0]
	if _, present := ev["time"]; present {
		t.Errorf("time = %v, want it omitted so the field stays NULL", ev["time"])
	}
	if got := ev["time_unix_nano"]; got != int64(0) {
		t.Errorf("time_unix_nano = %#v, want the raw zero", got)
	}
}

// kind and status.code keep the raw OTLP number. The name columns carry the
// protobuf enum name, and stay NULL for a number outside the enum rather than
// inventing one.
func TestSpanKindAndStatusNames(t *testing.T) {
	for kind, wantName := range map[ptrace.SpanKind]string{
		ptrace.SpanKindUnspecified: "SPAN_KIND_UNSPECIFIED",
		ptrace.SpanKindInternal:    "SPAN_KIND_INTERNAL",
		ptrace.SpanKindServer:      "SPAN_KIND_SERVER",
		ptrace.SpanKindClient:      "SPAN_KIND_CLIENT",
		ptrace.SpanKindProducer:    "SPAN_KIND_PRODUCER",
		ptrace.SpanKindConsumer:    "SPAN_KIND_CONSUMER",
		ptrace.SpanKind(9):         "",
	} {
		td, span := newTraces()
		span.SetKind(kind)
		rows, _ := SpanRows(td, testOpts())
		if got := rows[0]["kind"]; got != int64(kind) {
			t.Errorf("kind %d: kind = %#v", kind, got)
		}
		if got := rows[0]["kind_name"]; (wantName == "" && got != nil) || (wantName != "" && got != wantName) {
			t.Errorf("kind %d: kind_name = %#v, want %q (empty means NULL)", kind, got, wantName)
		}
	}

	for code, wantName := range map[ptrace.StatusCode]string{
		ptrace.StatusCodeUnset: "STATUS_CODE_UNSET",
		ptrace.StatusCodeOk:    "STATUS_CODE_OK",
		ptrace.StatusCodeError: "STATUS_CODE_ERROR",
		ptrace.StatusCode(7):   "",
	} {
		td, span := newTraces()
		span.Status().SetCode(code)
		span.Status().SetMessage("upstream timed out")
		rows, _ := SpanRows(td, testOpts())
		status := rows[0]["status"].(Row)
		if got := status["code"]; got != int64(code) {
			t.Errorf("status %d: code = %#v", code, got)
		}
		if got := status["code_name"]; (wantName == "" && got != nil) || (wantName != "" && got != wantName) {
			t.Errorf("status %d: code_name = %#v, want %q (empty means NULL)", code, got, wantName)
		}
		if got := status["message"]; got != "upstream timed out" {
			t.Errorf("status %d: message = %#v", code, got)
		}
	}
}

// Every NOT NULL span column must be fillable with a value that means what
// the column says. A span that cannot be is rejected before a row is built,
// and only that span: the spans around it are still written.
func TestSpanContractViolationsAreRejected(t *testing.T) {
	tests := map[string]struct {
		mutate    func(ptrace.Span)
		reason    Reason
		fieldPath string
	}{
		"empty trace id": {func(s ptrace.Span) { s.SetTraceID(pcommon.NewTraceIDEmpty()) }, ReasonInvalidID, "trace_id"},
		"empty span id":  {func(s ptrace.Span) { s.SetSpanID(pcommon.NewSpanIDEmpty()) }, ReasonInvalidID, "span_id"},
		"no start time":  {func(s ptrace.Span) { s.SetStartTimestamp(0) }, ReasonMissingTimestamp, "start_time"},
		"no end time":    {func(s ptrace.Span) { s.SetEndTimestamp(0) }, ReasonMissingTimestamp, "end_time"},
		"start time beyond int64": {
			func(s ptrace.Span) { s.SetStartTimestamp(math.MaxInt64 + 1) },
			ReasonTimestampOverflow, "start_time_unix_nano",
		},
		"end time beyond int64": {
			func(s ptrace.Span) { s.SetEndTimestamp(math.MaxUint64) },
			ReasonTimestampOverflow, "end_time_unix_nano",
		},
		"event time beyond int64": {
			func(s ptrace.Span) { s.Events().AppendEmpty().SetTimestamp(math.MaxInt64 + 1) },
			ReasonTimestampOverflow, "events.time_unix_nano",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			td, bad := newTraces()
			tc.mutate(bad)
			validSpan(td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().AppendEmpty())

			rows, rejects := SpanRows(td, testOpts())
			if len(rows) != 1 {
				t.Fatalf("kept %d rows, want only the valid neighbor", len(rows))
			}
			want := Rejection{Index: 0, Reason: tc.reason, FieldPath: tc.fieldPath}
			if len(rejects) != 1 || rejects[0] != want {
				t.Fatalf("rejections = %+v, want [%+v]", rejects, want)
			}
		})
	}
}

// INT64 holds every OTLP timestamp up to math.MaxInt64 nanoseconds, in the
// year 2262. Only values beyond that are rejected.
func TestSpanTimestampsAtTheInt64LimitAreKept(t *testing.T) {
	td, span := newTraces()
	span.SetStartTimestamp(math.MaxInt64)
	span.SetEndTimestamp(math.MaxInt64)
	span.Events().AppendEmpty().SetTimestamp(math.MaxInt64)

	rows, rejects := SpanRows(td, testOpts())
	if len(rejects) != 0 || len(rows) != 1 {
		t.Fatalf("rows=%d rejects=%+v, want the span kept", len(rows), rejects)
	}
	if got := rows[0]["end_time_unix_nano"]; got != int64(math.MaxInt64) {
		t.Errorf("end_time_unix_nano = %#v, want math.MaxInt64", got)
	}
	if got := rows[0]["end_time"]; got != int64(math.MaxInt64/1000) {
		t.Errorf("end_time = %#v, want math.MaxInt64 nanos in micros", got)
	}
	if got := rows[0]["events"].([]Row)[0]["time_unix_nano"]; got != int64(math.MaxInt64) {
		t.Errorf("events.time_unix_nano = %#v, want math.MaxInt64", got)
	}
}

// Clock skew between hosts can put a span's end before its start. The raw
// times are kept as sent, but a negative duration means nothing, so
// duration_nano clamps to zero.
func TestNegativeSpanDurationClampsToZero(t *testing.T) {
	td, span := newTraces()
	span.SetStartTimestamp(pcommon.Timestamp(2_000_000_000))
	span.SetEndTimestamp(pcommon.Timestamp(1_000_000_000))

	rows, rejects := SpanRows(td, testOpts())
	if len(rejects) != 0 {
		t.Fatalf("clock skew must not reject the span: %+v", rejects)
	}
	if got := rows[0]["duration_nano"]; got != int64(0) {
		t.Errorf("duration_nano = %#v, want 0", got)
	}
	if rows[0]["start_time_unix_nano"] != int64(2_000_000_000) || rows[0]["end_time_unix_nano"] != int64(1_000_000_000) {
		t.Errorf("raw times were altered: start=%v end=%v",
			rows[0]["start_time_unix_nano"], rows[0]["end_time_unix_nano"])
	}
}

// BigQuery STRING columns must hold valid UTF-8, and nothing on the OTLP wire
// guarantees it. Invalid sequences become U+FFFD instead of failing the row.
func TestSpanStringsAreValidUTF8(t *testing.T) {
	const bad = "a\xffb"
	td, span := newTraces()
	rs := td.ResourceSpans().At(0)
	rs.SetSchemaUrl(bad)
	rs.Resource().Attributes().PutStr("service.name", bad)
	ss := rs.ScopeSpans().At(0)
	ss.SetSchemaUrl(bad)
	ss.Scope().SetName(bad)
	ss.Scope().SetVersion(bad)
	span.SetName(bad)
	span.TraceState().FromRaw(bad)
	span.Status().SetMessage(bad)
	span.Attributes().PutStr(bad, bad)
	span.Events().AppendEmpty().SetName(bad)
	span.Links().AppendEmpty().TraceState().FromRaw(bad)

	rows, rejects := SpanRows(td, testOpts())
	if len(rejects) != 0 {
		t.Fatalf("invalid UTF-8 must be repaired, not rejected: %+v", rejects)
	}
	if got, want := rows[0]["name"], "a\uFFFDb"; got != want {
		t.Errorf("name = %q, want %q", got, want)
	}
	assertValidUTF8(t, "row", rows[0])
}

func assertValidUTF8(t *testing.T, path string, v any) {
	t.Helper()
	switch v := v.(type) {
	case string:
		if !utf8.ValidString(v) {
			t.Errorf("%s = %q is not valid UTF-8", path, v)
		}
	case Row:
		for k, fv := range v {
			assertValidUTF8(t, path+"."+k, fv)
		}
	case []Row:
		for i, r := range v {
			assertValidUTF8(t, fmt.Sprintf("%s[%d]", path, i), r)
		}
	}
}

// Spans under one resource reuse its encoded attributes, but each row must
// get its own STRUCT values, or a change to one row would leak into another.
func TestSpansDoNotShareNestedRows(t *testing.T) {
	td, _ := newTraces()
	validSpan(td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().AppendEmpty())

	rows, _ := SpanRows(td, testOpts())
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	for _, col := range []string{"resource", "instrumentation_scope"} {
		first, second := rows[0][col].(Row), rows[1][col].(Row)
		if !reflect.DeepEqual(first, second) {
			t.Errorf("%s differs between two spans of one resource and scope", col)
		}
		first["attributes"] = "changed"
		if second["attributes"] == "changed" {
			t.Errorf("%s is a single map shared by two rows", col)
		}
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
