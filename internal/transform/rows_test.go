// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
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

// fingerprintPattern is the record_fingerprint form the contract specifies.
var fingerprintPattern = regexp.MustCompile(`^fp1:[0-9a-f]{64}$`)

func newLogs() (plog.Logs, plog.LogRecord) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "checkout")
	rl.SetSchemaUrl("https://example.test/resource")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("scope")
	sl.Scope().SetVersion("1.2.3")
	sl.SetSchemaUrl("https://example.test/scope")

	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_123_456_789))
	lr.SetObservedTimestamp(pcommon.Timestamp(1_700_000_000_500_000_789))
	lr.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	lr.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	lr.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
	lr.SetEventName("disk.alert")
	lr.SetSeverityNumber(plog.SeverityNumberError)
	lr.SetSeverityText("ERROR")
	lr.Body().SetStr("disk full")
	lr.Attributes().PutInt("disk.used_pct", 97)
	return ld, lr
}

// TestLogRow pins the complete row for a representative log record: every
// column, every nested STRUCT field, and the Go type the encoder expects for
// each.
func TestLogRow(t *testing.T) {
	ld, _ := newLogs()
	rows, rejects := LogRows(ld, testOpts())
	if len(rejects) != 0 {
		t.Fatalf("unexpected rejections: %+v", rejects)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	got := rows[0]

	// The fingerprint is a hash, so the tests below pin its properties rather
	// than its value; here it must have the contract's form. With no upstream
	// record ID, insert_id is the fingerprint itself.
	fp, _ := got["record_fingerprint"].(string)
	if !fingerprintPattern.MatchString(fp) {
		t.Errorf("record_fingerprint = %q, want fp1: plus 64 lowercase hex chars", fp)
	}
	if got["insert_id"] != fp {
		t.Errorf("insert_id = %#v, want the record fingerprint %q", got["insert_id"], fp)
	}

	want := Row{
		// TIMESTAMP columns truncate to microseconds; the exact nanos live in
		// the paired INT64 column.
		"timestamp":               int64(1_700_000_000_123_456),
		"timestamp_source":        "event",
		"time_unix_nano":          int64(1_700_000_000_123_456_789),
		"observed_timestamp":      int64(1_700_000_000_500_000),
		"observed_time_unix_nano": int64(1_700_000_000_500_000_789),
		"receive_time":            fixedNow.UnixMicro(),
		"receive_time_unix_nano":  fixedNow.UnixNano(),

		"trace_id": "0102030405060708090a0b0c0d0e0f10",
		"span_id":  "0102030405060708",
		"flags":    int64(1),

		"event_name":      "disk.alert",
		"severity_number": int64(plog.SeverityNumberError),
		"severity_text":   "ERROR",
		// The body is plain JSON too: a string body is a JSON string.
		"body":                     `"disk full"`,
		"attributes":               `{"disk.used_pct":"97"}`,
		"attributes_schema":        `{"disk.used_pct":"int64"}`,
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

		// No log name attribute is set, so log_name is the service name.
		"log_name":           "checkout",
		"insert_id":          fp,
		"record_fingerprint": fp,
		"schema_version":     schema.Version,
	}
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
// the key. The raw time columns are NOT NULL and keep the zeros as sent, and
// the NOT NULL identity columns still get a value.
func TestAbsentOptionalLogColumnsAreOmitted(t *testing.T) {
	// A record with no times, body or IDs, under no resource attributes,
	// scope or schema URLs.
	bare := plog.NewLogs()
	bare.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	rows, rejects := LogRows(bare, testOpts())
	if len(rejects) != 0 || len(rows) != 1 {
		t.Fatalf("rows=%d rejects=%+v, want the bare record kept", len(rows), rejects)
	}
	r := rows[0]

	for _, col := range []string{
		"observed_timestamp", "trace_id", "span_id", "event_name", "severity_text", "body",
		"service_name", "resource_schema_url", "scope_schema_url", "payload_refs",
	} {
		if _, present := r[col]; present {
			t.Errorf("%s should be omitted when unset, so the column stays NULL", col)
		}
	}
	for _, fld := range []string{"name", "version"} {
		if _, present := r["instrumentation_scope"].(Row)[fld]; present {
			t.Errorf("instrumentation_scope.%s should be omitted when unset", fld)
		}
	}

	for col, want := range map[string]any{
		"time_unix_nano":          int64(0),
		"observed_time_unix_nano": int64(0),
		"timestamp":               fixedNow.UnixMicro(),
		"timestamp_source":        "exporter",
		"log_name":                "default",
		"schema_version":          schema.Version,
	} {
		if got := r[col]; got != want {
			t.Errorf("%s = %#v, want %#v", col, got, want)
		}
	}
	if fp, _ := r["record_fingerprint"].(string); !fingerprintPattern.MatchString(fp) || r["insert_id"] != fp {
		t.Errorf("insert_id = %#v, record_fingerprint = %#v, want both the record fingerprint",
			r["insert_id"], r["record_fingerprint"])
	}
}

// §7.2 fallback chain for the partition key. OTLP permits both timestamps to
// be zero, and a row with no partition key cannot be written at all.
func TestLogTimestampFallback(t *testing.T) {
	tests := []struct {
		name       string
		ts, obs    pcommon.Timestamp
		wantSource string
		wantMicros int64
	}{
		{"event time wins", 1_000_000_000, 2_000_000_000, "event", 1_000_000},
		{"observed when event is zero", 0, 2_000_000_000, "observed", 2_000_000},
		{"exporter when both are zero", 0, 0, "exporter", fixedNow.UnixMicro()},
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
			if got := rows[0]["timestamp"]; got != tc.wantMicros {
				t.Errorf("timestamp = %#v, want %d", got, tc.wantMicros)
			}
		})
	}
}

// Before OTLP gave log records a top-level event name, producers carried it
// in an event.name attribute. The field wins when both are set, and the
// attribute stays among the record's attributes either way.
func TestEventNameFallsBackToAttribute(t *testing.T) {
	tests := map[string]struct {
		field     string
		attrs     map[string]any
		want      string // empty means NULL
		wantAttrs string
	}{
		"attribute only": {
			"", map[string]any{"event.name": "legacy.alert"},
			"legacy.alert", `{"event.name":"legacy.alert"}`,
		},
		"field wins": {
			"disk.alert", map[string]any{"event.name": "legacy.alert"},
			"disk.alert", `{"event.name":"legacy.alert"}`,
		},
		"non-string attribute": {
			"", map[string]any{"event.name": int64(7)},
			"", `{"event.name":"7"}`,
		},
		"neither": {"", nil, "", "{}"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ld, lr := newLogs()
			lr.SetEventName(tc.field)
			if err := lr.Attributes().FromRaw(tc.attrs); err != nil {
				t.Fatal(err)
			}

			rows, _ := LogRows(ld, testOpts())
			got, present := rows[0]["event_name"]
			if (tc.want == "" && present) || (tc.want != "" && got != tc.want) {
				t.Errorf("event_name = %#v, want %q (empty means NULL)", got, tc.want)
			}
			if got := rows[0]["attributes"]; got != tc.wantAttrs {
				t.Errorf("attributes = %#v, want %s", got, tc.wantAttrs)
			}
		})
	}
}

// log_name comes from the configured attribute, then the well-known log name
// attributes in order, then the service name, then "default". Only non-empty
// strings count: a value of another type, or an empty one, is passed over.
func TestLogNameResolution(t *testing.T) {
	tests := map[string]struct {
		configured string
		service    string
		attrs      map[string]any
		want       string
	}{
		"configured attribute wins": {
			"log.file.name", "checkout",
			map[string]any{"log.file.name": "app.log", "gcp.log_name": "gcp"},
			"app.log",
		},
		"first well-known attribute": {
			"log.file.name", "checkout",
			map[string]any{"gcp.log_name": "gcp", "logging.googleapis.com/logName": "lg", "log.iostream": "stdout"},
			"gcp",
		},
		"second well-known attribute": {
			"", "checkout",
			map[string]any{"logging.googleapis.com/logName": "lg", "log.iostream": "stdout"},
			"lg",
		},
		"third well-known attribute": {
			"", "checkout",
			map[string]any{"log.iostream": "stdout"},
			"stdout",
		},
		"service name": {"", "checkout", nil, "checkout"},
		"default":      {"", "", nil, "default"},
		"unconfigured attribute is not consulted": {
			"", "checkout",
			map[string]any{"log.file.name": "app.log"},
			"checkout",
		},
		"non-string values are passed over": {
			"log.file.name", "checkout",
			map[string]any{"log.file.name": int64(7), "gcp.log_name": true, "log.iostream": "stderr"},
			"stderr",
		},
		"empty values are passed over": {
			"log.file.name", "",
			map[string]any{"log.file.name": "", "gcp.log_name": ""},
			"default",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ld, lr := newLogs()
			res := map[string]any{}
			if tc.service != "" {
				res["service.name"] = tc.service
			}
			if err := ld.ResourceLogs().At(0).Resource().Attributes().FromRaw(res); err != nil {
				t.Fatal(err)
			}
			if err := lr.Attributes().FromRaw(tc.attrs); err != nil {
				t.Fatal(err)
			}
			opts := testOpts()
			opts.LogNameAttribute = tc.configured

			rows, _ := LogRows(ld, opts)
			if got := rows[0]["log_name"]; got != tc.want {
				t.Errorf("log_name = %#v, want %q", got, tc.want)
			}
		})
	}
}

// insert_id comes from the configured attribute, then the well-known insert
// ID attributes in order, then the record fingerprint. As for log_name, only
// non-empty strings count.
func TestInsertIDResolution(t *testing.T) {
	tests := map[string]struct {
		configured string
		attrs      map[string]any
		want       string // empty means the record's own fingerprint
	}{
		"configured attribute wins": {
			"my.id",
			map[string]any{"my.id": "abc", "logging.googleapis.com/insertId": "lg"},
			"abc",
		},
		"first well-known attribute": {
			"my.id",
			map[string]any{
				"logging.googleapis.com/insertId": "lg", "gcp.insert.id": "g1",
				"gcp.insert_id": "g2", "log.record.uid": "uid",
			},
			"lg",
		},
		"second well-known attribute": {
			"",
			map[string]any{"gcp.insert.id": "g1", "gcp.insert_id": "g2", "log.record.uid": "uid"},
			"g1",
		},
		"third well-known attribute": {
			"",
			map[string]any{"gcp.insert_id": "g2", "log.record.uid": "uid"},
			"g2",
		},
		"fourth well-known attribute": {"", map[string]any{"log.record.uid": "uid"}, "uid"},
		"record fingerprint":          {"", nil, ""},
		"unconfigured attribute is not consulted": {
			"",
			map[string]any{"my.id": "abc"},
			"",
		},
		"non-string values are passed over": {
			"my.id",
			map[string]any{"my.id": int64(5), "logging.googleapis.com/insertId": []any{"lg"}, "log.record.uid": "uid"},
			"uid",
		},
		"empty values are passed over": {
			"my.id",
			map[string]any{"my.id": "", "gcp.insert.id": ""},
			"",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ld, lr := newLogs()
			if err := lr.Attributes().FromRaw(tc.attrs); err != nil {
				t.Fatal(err)
			}
			opts := testOpts()
			opts.InsertIDAttribute = tc.configured

			rows, _ := LogRows(ld, opts)
			fp, _ := rows[0]["record_fingerprint"].(string)
			if !fingerprintPattern.MatchString(fp) {
				t.Errorf("record_fingerprint = %q: it must be written whatever insert_id holds", fp)
			}
			want := tc.want
			if want == "" {
				want = fp
			}
			if got := rows[0]["insert_id"]; got != want {
				t.Errorf("insert_id = %#v, want %q", got, want)
			}
		})
	}
}

// Identical records in one batch are distinct log entries, so they must not
// share an identity, or the dedup view would collapse them into one. The
// intra_batch_ordinal keeps them apart, and since a retry repeats the batch
// in the same order, it reproduces every fingerprint.
func TestIdenticalRecordsInABatchGetDistinctFingerprints(t *testing.T) {
	fingerprints := func(ld plog.Logs) []string {
		t.Helper()
		rows, rejects := LogRows(ld, testOpts())
		if len(rejects) != 0 {
			t.Fatalf("unexpected rejections: %+v", rejects)
		}
		fps := make([]string, len(rows))
		for i, r := range rows {
			fps[i], _ = r["record_fingerprint"].(string)
			if r["insert_id"] != fps[i] {
				t.Errorf("row %d: insert_id = %#v, want its fingerprint", i, r["insert_id"])
			}
		}
		return fps
	}

	// The batch is [A, A, B, A], where B differs from A only in its body.
	ld, a := newLogs()
	records := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	a.CopyTo(records.AppendEmpty())
	b := records.AppendEmpty()
	a.CopyTo(b)
	b.Body().SetStr("disk ok")
	a.CopyTo(records.AppendEmpty())

	fps := fingerprints(ld)
	if len(fps) != 4 {
		t.Fatalf("got %d rows, want 4", len(fps))
	}
	if fps[0] == fps[1] || fps[0] == fps[3] || fps[1] == fps[3] {
		t.Errorf("identical records share a fingerprint: %q", []string{fps[0], fps[1], fps[3]})
	}
	if again := fingerprints(ld); !reflect.DeepEqual(again, fps) {
		t.Errorf("a retry of the batch changed its fingerprints:\n got %q\nwant %q", again, fps)
	}

	// The first of the identical records has ordinal 0, as the record sent on
	// its own does, and B is unaffected by the A's around it.
	aloneA, _ := newLogs()
	aloneB, lr := newLogs()
	lr.Body().SetStr("disk ok")
	if want := fingerprints(aloneA)[0]; fps[0] != want {
		t.Errorf("first A = %q, want %q, the fingerprint of A sent alone", fps[0], want)
	}
	if want := fingerprints(aloneB)[0]; fps[2] != want {
		t.Errorf("B = %q, want %q, the fingerprint of B sent alone", fps[2], want)
	}
}

// The row stores values as plain JSON, in which an int64 and its decimal
// string look the same. The fingerprint must still tell them apart, or two
// distinct records would share an identity.
func TestFingerprintDistinguishesTypes(t *testing.T) {
	row := func(mutate func(plog.LogRecord)) Row {
		ld, lr := newLogs()
		mutate(lr)
		rows, _ := LogRows(ld, testOpts())
		return rows[0]
	}
	for _, tc := range []struct {
		name            string
		col             string
		asInt, asString func(plog.LogRecord)
	}{
		{
			"attribute", "attributes",
			func(lr plog.LogRecord) { lr.Attributes().PutInt("n", 42) },
			func(lr plog.LogRecord) { lr.Attributes().PutStr("n", "42") },
		},
		{
			"body", "body",
			func(lr plog.LogRecord) { lr.Body().SetInt(42) },
			func(lr plog.LogRecord) { lr.Body().SetStr("42") },
		},
	} {
		i, s := row(tc.asInt), row(tc.asString)
		if i[tc.col] != s[tc.col] {
			t.Fatalf("%s: plain JSON differs (%v vs %v), so this case proves nothing", tc.name, i[tc.col], s[tc.col])
		}
		if i["record_fingerprint"] == s["record_fingerprint"] {
			t.Errorf("%s: an int64 and its decimal string share a fingerprint", tc.name)
		}
	}
}

// Every NOT NULL log column must be fillable with a value that means what
// the column says. A record whose times do not fit INT64 is rejected before a
// row is built, and only that record: the records around it are still
// written.
func TestLogContractViolationsAreRejected(t *testing.T) {
	tests := map[string]struct {
		mutate    func(plog.LogRecord)
		fieldPath string
	}{
		"time beyond int64": {
			func(lr plog.LogRecord) { lr.SetTimestamp(math.MaxInt64 + 1) },
			"time_unix_nano",
		},
		"observed time beyond int64": {
			func(lr plog.LogRecord) { lr.SetObservedTimestamp(math.MaxUint64) },
			"observed_time_unix_nano",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ld, bad := newLogs()
			tc.mutate(bad)
			ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty()

			rows, rejects := LogRows(ld, testOpts())
			if len(rows) != 1 {
				t.Fatalf("kept %d rows, want only the valid neighbor", len(rows))
			}
			want := Rejection{Index: 0, Reason: ReasonTimestampOverflow, FieldPath: tc.fieldPath}
			if len(rejects) != 1 || rejects[0] != want {
				t.Fatalf("rejections = %+v, want [%+v]", rejects, want)
			}
		})
	}
}

// INT64 holds every OTLP timestamp up to math.MaxInt64 nanoseconds, in the
// year 2262. Only values beyond that are rejected.
func TestLogTimestampsAtTheInt64LimitAreKept(t *testing.T) {
	ld, lr := newLogs()
	lr.SetTimestamp(math.MaxInt64)
	lr.SetObservedTimestamp(math.MaxInt64)

	rows, rejects := LogRows(ld, testOpts())
	if len(rejects) != 0 || len(rows) != 1 {
		t.Fatalf("rows=%d rejects=%+v, want the record kept", len(rows), rejects)
	}
	for col, want := range map[string]int64{
		"time_unix_nano":          math.MaxInt64,
		"observed_time_unix_nano": math.MaxInt64,
		"timestamp":               math.MaxInt64 / 1000,
		"observed_timestamp":      math.MaxInt64 / 1000,
	} {
		if got := rows[0][col]; got != want {
			t.Errorf("%s = %#v, want %d", col, got, want)
		}
	}
}

// BigQuery STRING columns must hold valid UTF-8, and nothing on the OTLP wire
// guarantees it. Invalid sequences become U+FFFD instead of failing the row.
func TestLogStringsAreValidUTF8(t *testing.T) {
	const bad = "a\xffb"
	ld, lr := newLogs()
	rl := ld.ResourceLogs().At(0)
	rl.SetSchemaUrl(bad)
	rl.Resource().Attributes().PutStr("service.name", bad)
	sl := rl.ScopeLogs().At(0)
	sl.SetSchemaUrl(bad)
	sl.Scope().SetName(bad)
	sl.Scope().SetVersion(bad)
	lr.SetEventName(bad)
	lr.SetSeverityText(bad)
	lr.Body().SetStr(bad)
	lr.Attributes().PutStr(bad, bad)
	lr.Attributes().PutStr("gcp.log_name", bad)
	lr.Attributes().PutStr("log.record.uid", bad)

	rows, rejects := LogRows(ld, testOpts())
	if len(rejects) != 0 {
		t.Fatalf("invalid UTF-8 must be repaired, not rejected: %+v", rejects)
	}
	for _, col := range []string{"event_name", "severity_text", "service_name", "log_name", "insert_id"} {
		if got, want := rows[0][col], "a\uFFFDb"; got != want {
			t.Errorf("%s = %q, want %q", col, got, want)
		}
	}
	assertValidUTF8(t, "row", rows[0])
}

// Records under one resource reuse its encoded attributes, but each row must
// get its own STRUCT values, or a change to one row would leak into another.
func TestLogsDoNotShareNestedRows(t *testing.T) {
	ld, _ := newLogs()
	ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty()

	rows, _ := LogRows(ld, testOpts())
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	for _, col := range []string{"resource", "instrumentation_scope"} {
		first, second := rows[0][col].(Row), rows[1][col].(Row)
		if !reflect.DeepEqual(first, second) {
			t.Errorf("%s differs between two records of one resource and scope", col)
		}
		first["attributes"] = "changed"
		if second["attributes"] == "changed" {
			t.Errorf("%s is a single map shared by two rows", col)
		}
	}
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

// receive_time is ingestion-local. If it fed the fingerprint, every retry of
// the same record would hash differently and deduplication would never match
// anything.
func TestFingerprintIgnoresIngestionTime(t *testing.T) {
	fp := func(now time.Time) string {
		ld, _ := newLogs()
		opts := testOpts()
		opts.Now = func() time.Time { return now }
		rows, _ := LogRows(ld, opts)
		return rows[0]["record_fingerprint"].(string)
	}
	if fp(fixedNow) != fp(fixedNow.Add(72*time.Hour)) {
		t.Fatal("fingerprint must not depend on receive_time")
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
