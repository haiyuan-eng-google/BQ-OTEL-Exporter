// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"

import (
	"math"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/otlpjson"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// Row is one BigQuery row, keyed by column name.
//
// TIMESTAMP columns hold microseconds since the Unix epoch as int64, which is
// what the Storage Write API expects on the wire. An absent optional column is
// omitted from the map rather than set to a zero value, so NULL stays distinct
// from "empty".
type Row map[string]any

// Rejection records a permanently invalid record. It carries no telemetry
// content: Reason and FieldPath are bounded, and Index locates the record
// within the batch without identifying it.
type Rejection struct {
	Index     int
	Reason    Reason
	FieldPath string
	Limit     int
	Actual    int
}

// Options configures row construction.
type Options struct {
	Limits Limits
	// SourceRecordNamespace is the trusted namespace for source_record_id.
	// It comes from deployment-controlled configuration, never from an
	// application-supplied attribute, because it participates in record
	// identity.
	SourceRecordNamespace string
	// SourceRecordIDAttribute names the log attribute carrying a stable
	// source record ID, when the instrumentation supplies one.
	SourceRecordIDAttribute string
	// Now supplies the exporter receive time stamped on every row.
	// Injectable so tests are deterministic.
	Now func() time.Time
}

func (o Options) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

const serviceNameAttr = "service.name"

// SpanRows converts a traces batch into rows, dropping records that violate
// the span contract or breach the FR8 structural limits and reporting them as
// rejections.
//
// Index in a rejection is the position of the span in a flattened walk of the
// batch, which is the same order the caller sees.
func SpanRows(td ptrace.Traces, opts Options) ([]Row, []Rejection) {
	received := opts.now()
	rows := make([]Row, 0, td.SpanCount())
	var rejects []Rejection
	idx := -1

	for _, rs := range td.ResourceSpans().All() {
		resource := rs.Resource()
		res := encodeBag(resource.Attributes(), resource.DroppedAttributesCount())
		svc := sanitize(stringAttr(resource.Attributes(), serviceNameAttr))

		for _, ss := range rs.ScopeSpans().All() {
			scope := ss.Scope()
			sc := encodedScope{
				name:    sanitize(scope.Name()),
				version: sanitize(scope.Version()),
				bag:     encodeBag(scope.Attributes(), scope.DroppedAttributesCount()),
			}

			for _, span := range ss.Spans().All() {
				idx++
				if bad := checkSpan(span, resource, scope, opts.Limits); bad != nil {
					rejects = append(rejects, Rejection{idx, bad.Reason, bad.FieldPath, bad.Limit, bad.Actual})
					continue
				}
				rows = append(rows, spanRow(span, rs.SchemaUrl(), ss.SchemaUrl(), res, sc, svc, received))
			}
		}
	}
	return rows, rejects
}

func checkSpan(span ptrace.Span, resource pcommon.Resource, scope pcommon.InstrumentationScope, l Limits) *Violation {
	if bad := validateSpan(span); bad != nil {
		return bad
	}
	if bad := l.checkMap(resource.Attributes(), "resource.attributes"); bad != nil {
		return bad
	}
	if bad := l.checkMap(scope.Attributes(), "instrumentation_scope.attributes"); bad != nil {
		return bad
	}
	if bad := l.checkMap(span.Attributes(), "attributes"); bad != nil {
		return bad
	}
	if n := span.Events().Len(); n > l.MaxEventCount {
		return &Violation{ReasonEventCount, "events", l.MaxEventCount, n}
	}
	if n := span.Links().Len(); n > l.MaxLinkCount {
		return &Violation{ReasonLinkCount, "links", l.MaxLinkCount, n}
	}
	for _, e := range span.Events().All() {
		// Checked here rather than in validateSpan so the walk stays
		// bounded by the event count limit above.
		if overflowsInt64(e.Timestamp()) {
			return &Violation{Reason: ReasonTimestampOverflow, FieldPath: "events.time_unix_nano"}
		}
		if bad := l.checkMap(e.Attributes(), "events.attributes"); bad != nil {
			return bad
		}
	}
	for _, lk := range span.Links().All() {
		if bad := l.checkMap(lk.Attributes(), "links.attributes"); bad != nil {
			return bad
		}
	}
	return nil
}

// validateSpan enforces the NOT NULL and INT64 columns of the span contract
// before any row is built. A span that fails has no valid row: writing it
// anyway would either fail the whole append or store a value the column does
// not mean, such as an all-zero trace ID or a start time of the Unix epoch.
func validateSpan(span ptrace.Span) *Violation {
	switch {
	case span.TraceID().IsEmpty():
		return &Violation{Reason: ReasonInvalidID, FieldPath: "trace_id"}
	case span.SpanID().IsEmpty():
		return &Violation{Reason: ReasonInvalidID, FieldPath: "span_id"}
	case span.StartTimestamp() == 0:
		return &Violation{Reason: ReasonMissingTimestamp, FieldPath: "start_time"}
	case span.EndTimestamp() == 0:
		return &Violation{Reason: ReasonMissingTimestamp, FieldPath: "end_time"}
	case overflowsInt64(span.StartTimestamp()):
		return &Violation{Reason: ReasonTimestampOverflow, FieldPath: "start_time_unix_nano"}
	case overflowsInt64(span.EndTimestamp()):
		return &Violation{Reason: ReasonTimestampOverflow, FieldPath: "end_time_unix_nano"}
	}
	return nil
}

// overflowsInt64 reports whether raw OTLP nanos, an unsigned fixed64, are too
// large for an INT64 *_unix_nano column.
func overflowsInt64(t pcommon.Timestamp) bool {
	return uint64(t) > math.MaxInt64
}

// encodedBag is an attribute bag encoded once and reused by every row that
// shares it, as all spans under one resource or scope do.
type encodedBag struct {
	attributes, schema string
	dropped            int64
}

func encodeBag(m pcommon.Map, dropped uint32) encodedBag {
	attrs, attrsSchema := otlpjson.EncodeMapWithSchema(m)
	return encodedBag{attributes: attrs, schema: attrsSchema, dropped: int64(dropped)}
}

// row renders the attributes, attributes_schema and dropped_attributes_count
// fields that every attribute-carrying STRUCT has. Each call returns a new
// map, so rows that share a bag never share a mutable value.
func (b encodedBag) row() Row {
	return Row{
		"attributes":               b.attributes,
		"attributes_schema":        b.schema,
		"dropped_attributes_count": b.dropped,
	}
}

// encodedScope is an instrumentation scope encoded once per scope.
type encodedScope struct {
	name, version string
	bag           encodedBag
}

func (s encodedScope) row() Row {
	r := s.bag.row()
	putStrIfSet(r, "name", s.name)
	putStrIfSet(r, "version", s.version)
	return r
}

func spanRow(
	span ptrace.Span,
	resSchemaURL, scopeSchemaURL string,
	res encodedBag,
	scope encodedScope,
	svc string,
	received time.Time,
) Row {
	// validateSpan has already guaranteed both timestamps are set and fit
	// INT64, so these conversions are exact.
	start := span.StartTimestamp()
	end := span.EndTimestamp()
	attrs, attrsSchema := otlpjson.EncodeMapWithSchema(span.Attributes())

	r := Row{
		"start_time":             micros(start),
		"start_time_unix_nano":   int64(start),
		"end_time":               micros(end),
		"end_time_unix_nano":     int64(end),
		"receive_time":           received.UnixMicro(),
		"receive_time_unix_nano": received.UnixNano(),
		// Clock skew between hosts can put end before start. A negative
		// duration means nothing, so it clamps to zero; the raw times stay
		// as sent.
		"duration_nano": max(0, int64(end)-int64(start)),

		"trace_id": traceIDString(span.TraceID()),
		"span_id":  spanIDString(span.SpanID()),
		"flags":    int64(span.Flags()),

		"name":                     sanitize(span.Name()),
		"kind":                     int64(span.Kind()),
		"status":                   statusRow(span.Status()),
		"attributes":               attrs,
		"attributes_schema":        attrsSchema,
		"dropped_attributes_count": int64(span.DroppedAttributesCount()),

		"resource":              res.row(),
		"instrumentation_scope": scope.row(),

		"dropped_events_count": int64(span.DroppedEventsCount()),
		"dropped_links_count":  int64(span.DroppedLinksCount()),

		"schema_version": schema.Version,
	}

	// Absent optional values are omitted so the column stays NULL. An empty
	// parent_span_id means "no parent", which is not the same as an empty
	// string.
	putStrIfSet(r, "parent_span_id", spanIDString(span.ParentSpanID()))
	putStrIfSet(r, "trace_state", sanitize(span.TraceState().AsRaw()))
	putStrIfSet(r, "kind_name", spanKindName(span.Kind()))
	putStrIfSet(r, "service_name", svc)
	putStrIfSet(r, "resource_schema_url", sanitize(resSchemaURL))
	putStrIfSet(r, "scope_schema_url", sanitize(scopeSchemaURL))

	if n := span.Events().Len(); n > 0 {
		events := make([]Row, 0, n)
		for _, e := range span.Events().All() {
			ev := encodeBag(e.Attributes(), e.DroppedAttributesCount()).row()
			ev["name"] = sanitize(e.Name())
			ev["time_unix_nano"] = int64(e.Timestamp())
			// OTLP lets an event omit its time. The raw column keeps the zero
			// as sent, but the TIMESTAMP stays NULL rather than claiming the
			// Unix epoch.
			if e.Timestamp() != 0 {
				ev["time"] = micros(e.Timestamp())
			}
			events = append(events, ev)
		}
		r["events"] = events
	}

	if n := span.Links().Len(); n > 0 {
		links := make([]Row, 0, n)
		for _, lk := range span.Links().All() {
			link := encodeBag(lk.Attributes(), lk.DroppedAttributesCount()).row()
			link["flags"] = int64(lk.Flags())
			putStrIfSet(link, "trace_id", traceIDString(lk.TraceID()))
			putStrIfSet(link, "span_id", spanIDString(lk.SpanID()))
			putStrIfSet(link, "trace_state", sanitize(lk.TraceState().AsRaw()))
			links = append(links, link)
		}
		r["links"] = links
	}

	// payload_refs stays NULL: every payload is currently written inline.
	return r
}

// statusRow builds the status STRUCT. It is always present, because an unset
// status is still a status: code 0, STATUS_CODE_UNSET.
func statusRow(s ptrace.Status) Row {
	r := Row{"code": int64(s.Code())}
	putStrIfSet(r, "code_name", statusCodeName(s.Code()))
	putStrIfSet(r, "message", sanitize(s.Message()))
	return r
}

// spanKindName returns the OTLP protobuf enum name for k. A value outside the
// enum yields "", which leaves kind_name NULL while kind keeps the number the
// producer sent.
func spanKindName(k ptrace.SpanKind) string {
	switch k {
	case ptrace.SpanKindUnspecified:
		return "SPAN_KIND_UNSPECIFIED"
	case ptrace.SpanKindInternal:
		return "SPAN_KIND_INTERNAL"
	case ptrace.SpanKindServer:
		return "SPAN_KIND_SERVER"
	case ptrace.SpanKindClient:
		return "SPAN_KIND_CLIENT"
	case ptrace.SpanKindProducer:
		return "SPAN_KIND_PRODUCER"
	case ptrace.SpanKindConsumer:
		return "SPAN_KIND_CONSUMER"
	}
	return ""
}

// statusCodeName returns the OTLP protobuf enum name for c, or "" for a value
// outside the enum, as spanKindName does.
func statusCodeName(c ptrace.StatusCode) string {
	switch c {
	case ptrace.StatusCodeUnset:
		return "STATUS_CODE_UNSET"
	case ptrace.StatusCodeOk:
		return "STATUS_CODE_OK"
	case ptrace.StatusCodeError:
		return "STATUS_CODE_ERROR"
	}
	return ""
}

// sanitize replaces invalid UTF-8 in a free-text field with U+FFFD. BigQuery
// STRING columns must hold valid UTF-8, and one malformed byte sequence would
// otherwise fail the row.
func sanitize(s string) string {
	return strings.ToValidUTF8(s, "\uFFFD")
}

// LogRows converts a logs batch into rows.
func LogRows(ld plog.Logs, opts Options) ([]Row, []Rejection) {
	received := opts.now()
	rows := make([]Row, 0, ld.LogRecordCount())
	var rejects []Rejection
	idx := -1

	for _, rl := range ld.ResourceLogs().All() {
		resource := rl.Resource()
		resAttrs := otlpjson.EncodeMap(resource.Attributes())
		svc := stringAttr(resource.Attributes(), serviceNameAttr)

		for _, sl := range rl.ScopeLogs().All() {
			scope := sl.Scope()
			scopeAttrs := otlpjson.EncodeMap(scope.Attributes())

			for _, lr := range sl.LogRecords().All() {
				idx++
				if bad := checkLog(lr, resource, scope, opts.Limits); bad != nil {
					rejects = append(rejects, Rejection{idx, bad.Reason, bad.FieldPath, bad.Limit, bad.Actual})
					continue
				}
				rows = append(rows, logRow(lr, rl.SchemaUrl(), sl.SchemaUrl(),
					resource, scope, resAttrs, scopeAttrs, svc, received, opts))
			}
		}
	}
	return rows, rejects
}

func checkLog(lr plog.LogRecord, resource pcommon.Resource, scope pcommon.InstrumentationScope, l Limits) *Violation {
	if bad := l.checkMap(resource.Attributes(), "resource_attributes"); bad != nil {
		return bad
	}
	if bad := l.checkMap(scope.Attributes(), "scope_attributes"); bad != nil {
		return bad
	}
	if bad := l.checkMap(lr.Attributes(), "log_attributes"); bad != nil {
		return bad
	}
	return l.checkValue(lr.Body(), "body", 1)
}

func logRow(
	lr plog.LogRecord,
	resSchemaURL, scopeSchemaURL string,
	resource pcommon.Resource,
	scope pcommon.InstrumentationScope,
	resAttrs, scopeAttrs, svc string,
	received time.Time,
	opts Options,
) Row {
	// §7.2: the partition key falls back through event time, observed time,
	// then ingestion time. OTLP permits both timestamps to be zero, and a row
	// with no partition key cannot be written at all.
	partitionMicros := received.UnixMicro()
	source := schema.TimestampSourceExporter
	switch {
	case lr.Timestamp() != 0:
		partitionMicros = micros(lr.Timestamp())
		source = schema.TimestampSourceEvent
	case lr.ObservedTimestamp() != 0:
		partitionMicros = micros(lr.ObservedTimestamp())
		source = schema.TimestampSourceObserved
	}

	r := Row{
		"timestamp":                 partitionMicros,
		"timestamp_source":          source,
		"time_unix_nano":            nanoString(lr.Timestamp()),
		"observed_time_unix_nano":   nanoString(lr.ObservedTimestamp()),
		"export_received_timestamp": received.UnixMicro(),
		"flags":                     int64(lr.Flags()),
		"severity_number":           int64(lr.SeverityNumber()),
		"body":                      otlpjson.EncodeValue(lr.Body()),
		"resource_attributes":       resAttrs,
		"scope_attributes":          scopeAttrs,
		"log_attributes":            otlpjson.EncodeMap(lr.Attributes()),
		"dropped_attributes_count":  int64(lr.DroppedAttributesCount()),
		"schema_version":            schema.Version,
		"record_fingerprint": LogFingerprint(
			lr, resource, resSchemaURL, scope, scopeSchemaURL),

		"resource_dropped_attributes_count": int64(resource.DroppedAttributesCount()),
		"scope_dropped_attributes_count":    int64(scope.DroppedAttributesCount()),
	}

	if lr.ObservedTimestamp() != 0 {
		r["observed_timestamp"] = micros(lr.ObservedTimestamp())
	}
	putStrIfSet(r, "trace_id", traceIDString(lr.TraceID()))
	putStrIfSet(r, "span_id", spanIDString(lr.SpanID()))
	putStrIfSet(r, "event_name", lr.EventName())
	putStrIfSet(r, "severity_text", lr.SeverityText())
	putStrIfSet(r, "service_name", svc)
	putStrIfSet(r, "resource_schema_url", resSchemaURL)
	putStrIfSet(r, "scope_schema_url", scopeSchemaURL)
	putStrIfSet(r, "scope_name", scope.Name())
	putStrIfSet(r, "scope_version", scope.Version())

	// Exact identity is only claimed when both halves of the tuple are
	// present: an ID without a trusted namespace is not an identity, since
	// two producers could mint the same ID.
	if opts.SourceRecordNamespace != "" && opts.SourceRecordIDAttribute != "" {
		if id := stringAttr(lr.Attributes(), opts.SourceRecordIDAttribute); id != "" {
			r["source_record_namespace"] = opts.SourceRecordNamespace
			r["source_record_id"] = id
		}
	}

	return r
}

func micros(t pcommon.Timestamp) int64 {
	return int64(t) / int64(time.Microsecond)
}

// nanoString renders raw OTLP nanos as a decimal string. The value is an
// unsigned fixed64, so it does not fit INT64 across its whole domain, and
// BigQuery TIMESTAMP cannot hold nanosecond precision either.
func nanoString(t pcommon.Timestamp) string {
	return uint64String(uint64(t))
}

func uint64String(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

func putStrIfSet(r Row, key, val string) {
	if val != "" {
		r[key] = val
	}
}

func stringAttr(m pcommon.Map, key string) string {
	if v, ok := m.Get(key); ok && v.Type() == pcommon.ValueTypeStr {
		return v.Str()
	}
	return ""
}
