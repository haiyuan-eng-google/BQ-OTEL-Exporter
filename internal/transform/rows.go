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
	// LogNameAttribute names a string log record attribute that carries the
	// record's log_name. A record without it falls back to the well-known log
	// name attributes, then to its service name, then to "default".
	LogNameAttribute string
	// InsertIDAttribute names a string log record attribute that carries an
	// upstream record ID for insert_id. A record without it falls back to the
	// well-known insert ID attributes, then to its record fingerprint.
	//
	// An upstream ID is trusted as the record's identity: the dedup view keeps
	// one row per (timestamp, time_unix_nano, service_name, log_name,
	// insert_id), so records that share all five collapse into one.
	InsertIDAttribute string
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

const (
	serviceNameAttr = "service.name"
	eventNameAttr   = "event.name"
	defaultLogName  = "default"
)

// Well-known log record attributes consulted, in order, for log_name and
// insert_id when the configured attribute is unset or absent.
var (
	logNameAttrs  = []string{"gcp.log_name", "logging.googleapis.com/logName", "log.iostream"}
	insertIDAttrs = []string{"logging.googleapis.com/insertId", "gcp.insert.id", "gcp.insert_id", "log.record.uid"}
)

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

// LogRows converts a logs batch into rows, dropping records that violate the
// log contract or breach the FR8 structural limits and reporting them as
// rejections.
//
// Index in a rejection is the position of the record in a flattened walk of
// the batch, which is the same order the caller sees. Fingerprints are
// assigned in the same walk, so a retry of the batch reproduces them.
func LogRows(ld plog.Logs, opts Options) ([]Row, []Rejection) {
	received := opts.now()
	rows := make([]Row, 0, ld.LogRecordCount())
	var rejects []Rejection
	fingerprints := fingerprinter{}
	idx := -1

	for _, rl := range ld.ResourceLogs().All() {
		resource := rl.Resource()
		res := encodeBag(resource.Attributes(), resource.DroppedAttributesCount())
		svc := sanitize(stringAttr(resource.Attributes(), serviceNameAttr))

		for _, sl := range rl.ScopeLogs().All() {
			scope := sl.Scope()
			sc := encodedScope{
				name:    sanitize(scope.Name()),
				version: sanitize(scope.Version()),
				bag:     encodeBag(scope.Attributes(), scope.DroppedAttributesCount()),
			}
			origin := logOrigin{
				res:            res,
				resSchemaURL:   rl.SchemaUrl(),
				scope:          sc,
				scopeSchemaURL: sl.SchemaUrl(),
				svc:            svc,
				canonical:      canonicalOrigin(res, rl.SchemaUrl(), scope, sc.bag, sl.SchemaUrl()),
			}

			for _, lr := range sl.LogRecords().All() {
				idx++
				if bad := checkLog(lr, resource, scope, opts.Limits); bad != nil {
					rejects = append(rejects, Rejection{idx, bad.Reason, bad.FieldPath, bad.Limit, bad.Actual})
					continue
				}
				// Only kept records consume an intra_batch_ordinal. A rejected
				// record is rejected again on every retry, so the ordinals of
				// the others still repeat.
				rows = append(rows, logRow(lr, origin, fingerprints, received, opts))
			}
		}
	}
	return rows, rejects
}

func checkLog(lr plog.LogRecord, resource pcommon.Resource, scope pcommon.InstrumentationScope, l Limits) *Violation {
	if bad := validateLog(lr); bad != nil {
		return bad
	}
	if bad := l.checkMap(resource.Attributes(), "resource.attributes"); bad != nil {
		return bad
	}
	if bad := l.checkMap(scope.Attributes(), "instrumentation_scope.attributes"); bad != nil {
		return bad
	}
	if bad := l.checkMap(lr.Attributes(), "attributes"); bad != nil {
		return bad
	}
	return l.checkValue(lr.Body(), "body", 1)
}

// validateLog enforces the INT64 columns of the log contract before any row is
// built. Unlike a span, a log record may leave both of its times unset: the
// partition key then falls back to the receive time, and the raw columns keep
// the zeros as sent.
func validateLog(lr plog.LogRecord) *Violation {
	switch {
	case overflowsInt64(lr.Timestamp()):
		return &Violation{Reason: ReasonTimestampOverflow, FieldPath: "time_unix_nano"}
	case overflowsInt64(lr.ObservedTimestamp()):
		return &Violation{Reason: ReasonTimestampOverflow, FieldPath: "observed_time_unix_nano"}
	}
	return nil
}

// logOrigin is the resource and scope context that every log record under one
// scope shares, encoded once.
type logOrigin struct {
	res            encodedBag
	resSchemaURL   string
	scope          encodedScope
	scopeSchemaURL string
	svc            string
	// canonical is the part of each record's canonical form that the
	// resource and scope contribute.
	canonical string
}

func logRow(
	lr plog.LogRecord,
	origin logOrigin,
	fingerprints fingerprinter,
	received time.Time,
	opts Options,
) Row {
	// The partition key falls back through event time, observed time, then
	// receive time. OTLP permits both record times to be zero, and a row with
	// no partition key cannot be written at all. validateLog has already
	// guaranteed both raw times fit INT64, so these conversions are exact.
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

	attrs := encodeBag(lr.Attributes(), lr.DroppedAttributesCount())
	body, bodySchema := otlpjson.EncodePlainValueWithSchema(lr.Body())
	fingerprint := fingerprints.next(canonicalLogRecord(lr, attrs, body, bodySchema, origin.canonical))

	r := Row{
		"timestamp":               partitionMicros,
		"timestamp_source":        source,
		"time_unix_nano":          int64(lr.Timestamp()),
		"observed_time_unix_nano": int64(lr.ObservedTimestamp()),
		"receive_time":            received.UnixMicro(),
		"receive_time_unix_nano":  received.UnixNano(),

		"flags": int64(lr.Flags()),

		"severity_number":          int64(lr.SeverityNumber()),
		"attributes":               attrs.attributes,
		"attributes_schema":        attrs.schema,
		"dropped_attributes_count": attrs.dropped,

		"resource":              origin.res.row(),
		"instrumentation_scope": origin.scope.row(),

		"log_name":           logName(lr.Attributes(), origin.svc, opts.LogNameAttribute),
		"insert_id":          insertID(lr.Attributes(), fingerprint, opts.InsertIDAttribute),
		"record_fingerprint": fingerprint,
		"schema_version":     schema.Version,
	}

	// Absent optional values are omitted so the column stays NULL. OTLP lets
	// a record omit its observed time: the raw column keeps the zero as sent,
	// but the TIMESTAMP stays NULL rather than claiming the Unix epoch. An
	// empty body is likewise absent rather than JSON null.
	if lr.ObservedTimestamp() != 0 {
		r["observed_timestamp"] = micros(lr.ObservedTimestamp())
	}
	if lr.Body().Type() != pcommon.ValueTypeEmpty {
		r["body"] = body
	}
	putStrIfSet(r, "trace_id", traceIDString(lr.TraceID()))
	putStrIfSet(r, "span_id", spanIDString(lr.SpanID()))
	putStrIfSet(r, "event_name", eventName(lr))
	putStrIfSet(r, "severity_text", sanitize(lr.SeverityText()))
	putStrIfSet(r, "service_name", origin.svc)
	putStrIfSet(r, "resource_schema_url", sanitize(origin.resSchemaURL))
	putStrIfSet(r, "scope_schema_url", sanitize(origin.scopeSchemaURL))

	// payload_refs stays NULL: every payload is currently written inline.
	return r
}

// eventName returns the record's event name, falling back to a string
// event.name attribute, which carried event names before OTLP gave log
// records a top-level field for them.
func eventName(lr plog.LogRecord) string {
	if name := sanitize(lr.EventName()); name != "" {
		return name
	}
	return sanitize(stringAttr(lr.Attributes(), eventNameAttr))
}

// logName resolves log_name: the configured attribute, then the well-known
// log name attributes, then the service name, then "default".
func logName(attrs pcommon.Map, svc, configured string) string {
	if name := firstStringAttr(attrs, configured, logNameAttrs); name != "" {
		return name
	}
	if svc != "" {
		return svc
	}
	return defaultLogName
}

// insertID resolves insert_id: the configured attribute, then the well-known
// insert ID attributes, then the record fingerprint.
func insertID(attrs pcommon.Map, fingerprint, configured string) string {
	if id := firstStringAttr(attrs, configured, insertIDAttrs); id != "" {
		return id
	}
	return fingerprint
}

// firstStringAttr returns the first non-empty string value among the
// configured key, when set, and then the well-known keys, made valid UTF-8.
// A value of any other type is skipped rather than rendered: these columns
// hold names and identifiers, which OTLP producers send as strings.
func firstStringAttr(attrs pcommon.Map, configured string, wellKnown []string) string {
	if configured != "" {
		if v := sanitize(stringAttr(attrs, configured)); v != "" {
			return v
		}
	}
	for _, key := range wellKnown {
		if v := sanitize(stringAttr(attrs, key)); v != "" {
			return v
		}
	}
	return ""
}

func micros(t pcommon.Timestamp) int64 {
	return int64(t) / int64(time.Microsecond)
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
