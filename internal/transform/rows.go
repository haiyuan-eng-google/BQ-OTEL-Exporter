// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"

import (
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
	// Now supplies export_received_timestamp. Injectable so tests are
	// deterministic.
	Now func() time.Time
}

func (o Options) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

const serviceNameAttr = "service.name"

// SpanRows converts a traces batch into rows, dropping records that breach the
// FR8 structural limits and reporting them as rejections.
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
		resAttrs := otlpjson.EncodeMap(resource.Attributes())
		svc := stringAttr(resource.Attributes(), serviceNameAttr)

		for _, ss := range rs.ScopeSpans().All() {
			scope := ss.Scope()
			scopeAttrs := otlpjson.EncodeMap(scope.Attributes())

			for _, span := range ss.Spans().All() {
				idx++
				if bad := checkSpan(span, resource, scope, opts.Limits); bad != nil {
					rejects = append(rejects, Rejection{idx, bad.Reason, bad.FieldPath, bad.Limit, bad.Actual})
					continue
				}
				rows = append(rows, spanRow(span, rs.SchemaUrl(), ss.SchemaUrl(),
					resource, scope, resAttrs, scopeAttrs, svc, received))
			}
		}
	}
	return rows, rejects
}

func checkSpan(span ptrace.Span, resource pcommon.Resource, scope pcommon.InstrumentationScope, l Limits) *Violation {
	if bad := l.checkMap(resource.Attributes(), "resource_attributes"); bad != nil {
		return bad
	}
	if bad := l.checkMap(scope.Attributes(), "scope_attributes"); bad != nil {
		return bad
	}
	if bad := l.checkMap(span.Attributes(), "span_attributes"); bad != nil {
		return bad
	}
	if n := span.Events().Len(); n > l.MaxEventCount {
		return &Violation{ReasonEventCount, "events", l.MaxEventCount, n}
	}
	if n := span.Links().Len(); n > l.MaxLinkCount {
		return &Violation{ReasonLinkCount, "links", l.MaxLinkCount, n}
	}
	for _, e := range span.Events().All() {
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

func spanRow(
	span ptrace.Span,
	resSchemaURL, scopeSchemaURL string,
	resource pcommon.Resource,
	scope pcommon.InstrumentationScope,
	resAttrs, scopeAttrs, svc string,
	received time.Time,
) Row {
	start := span.StartTimestamp()
	end := span.EndTimestamp()

	r := Row{
		"start_timestamp":           micros(start),
		"start_time_unix_nano":      nanoString(start),
		"export_received_timestamp": received.UnixMicro(),
		"trace_id":                  traceIDString(span.TraceID()),
		"span_id":                   spanIDString(span.SpanID()),
		"name":                      span.Name(),
		"kind":                      span.Kind().String(),
		"schema_version":            schema.Version,
		"span_attributes":           otlpjson.EncodeMap(span.Attributes()),
		"resource_attributes":       resAttrs,
		"scope_attributes":          scopeAttrs,
		"flags":                     int64(span.Flags()),
		"dropped_attributes_count":  int64(span.DroppedAttributesCount()),
		"dropped_events_count":      int64(span.DroppedEventsCount()),
		"dropped_links_count":       int64(span.DroppedLinksCount()),

		"resource_dropped_attributes_count": int64(resource.DroppedAttributesCount()),
		"scope_dropped_attributes_count":    int64(scope.DroppedAttributesCount()),
	}

	// Absent optional values are omitted so the column stays NULL. An empty
	// parent_span_id means "no parent", which is not the same as an empty
	// string.
	putIfNotEmpty(r, "end_timestamp", micros(end), end != 0)
	putIfNotEmpty(r, "end_time_unix_nano", nanoString(end), end != 0)
	if end > start {
		r["duration_ns"] = int64(end - start)
	}
	putStrIfSet(r, "parent_span_id", spanIDString(span.ParentSpanID()))
	putStrIfSet(r, "trace_state", span.TraceState().AsRaw())
	putStrIfSet(r, "status_code", span.Status().Code().String())
	putStrIfSet(r, "status_message", span.Status().Message())
	putStrIfSet(r, "service_name", svc)
	putStrIfSet(r, "resource_schema_url", resSchemaURL)
	putStrIfSet(r, "scope_schema_url", scopeSchemaURL)
	putStrIfSet(r, "scope_name", scope.Name())
	putStrIfSet(r, "scope_version", scope.Version())

	if n := span.Events().Len(); n > 0 {
		events := make([]Row, 0, n)
		for _, e := range span.Events().All() {
			ev := Row{
				"name":                     e.Name(),
				"timestamp":                micros(e.Timestamp()),
				"time_unix_nano":           nanoString(e.Timestamp()),
				"attributes":               otlpjson.EncodeMap(e.Attributes()),
				"dropped_attributes_count": int64(e.DroppedAttributesCount()),
			}
			events = append(events, ev)
		}
		r["events"] = events
	}

	if n := span.Links().Len(); n > 0 {
		links := make([]Row, 0, n)
		for _, lk := range span.Links().All() {
			links = append(links, Row{
				"trace_id":                 traceIDString(lk.TraceID()),
				"span_id":                  spanIDString(lk.SpanID()),
				"trace_state":              lk.TraceState().AsRaw(),
				"flags":                    int64(lk.Flags()),
				"attributes":               otlpjson.EncodeMap(lk.Attributes()),
				"dropped_attributes_count": int64(lk.DroppedAttributesCount()),
			})
		}
		r["links"] = links
	}

	return r
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

func putIfNotEmpty(r Row, key string, val any, set bool) {
	if set {
		r[key] = val
	}
}

func stringAttr(m pcommon.Map, key string) string {
	if v, ok := m.Get(key); ok && v.Type() == pcommon.ValueTypeStr {
		return v.Str()
	}
	return ""
}
