// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"

import (
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
)

// The TableSchema values here are the canonical typed contracts. Table
// creation and row encoding both derive from them; schema.go carries the
// equivalent operator-run DDL and the shared deduplication queries.

const (
	req  = storagepb.TableFieldSchema_REQUIRED
	opt  = storagepb.TableFieldSchema_NULLABLE
	rep  = storagepb.TableFieldSchema_REPEATED
	str  = storagepb.TableFieldSchema_STRING
	i64  = storagepb.TableFieldSchema_INT64
	f64  = storagepb.TableFieldSchema_DOUBLE
	bl   = storagepb.TableFieldSchema_BOOL
	ts   = storagepb.TableFieldSchema_TIMESTAMP
	jsn  = storagepb.TableFieldSchema_JSON
	strc = storagepb.TableFieldSchema_STRUCT
)

func f(name string, t storagepb.TableFieldSchema_Type, m storagepb.TableFieldSchema_Mode) *storagepb.TableFieldSchema {
	return &storagepb.TableFieldSchema{Name: name, Type: t, Mode: m}
}

func record(name string, m storagepb.TableFieldSchema_Mode, fields ...*storagepb.TableFieldSchema) *storagepb.TableFieldSchema {
	return &storagepb.TableFieldSchema{Name: name, Type: strc, Mode: m, Fields: fields}
}

// SpansTableSchema is the v0alpha2 span contract: 30 columns, grouped by
// family in the same order as SpansDDL.
func SpansTableSchema() *storagepb.TableSchema {
	return &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		// Time family.
		f("start_time", ts, req),
		f("start_time_unix_nano", i64, req),
		f("end_time", ts, req),
		f("end_time_unix_nano", i64, req),
		f("receive_time", ts, req),
		f("receive_time_unix_nano", i64, req),
		f("duration_nano", i64, req),

		// Identity family.
		f("trace_id", str, req),
		f("span_id", str, req),
		f("parent_span_id", str, opt),
		f("trace_state", str, opt),
		f("flags", i64, opt),

		// Span payload family.
		f("name", str, req),
		f("kind", i64, req),
		f("kind_name", str, opt),
		record("status", opt,
			f("code", i64, opt),
			f("code_name", str, opt),
			f("message", str, opt),
		),
		f("attributes", jsn, opt),
		f("attributes_schema", jsn, opt),
		f("dropped_attributes_count", i64, opt),

		// Resource and instrumentation scope family.
		f("service_name", str, opt),
		resourceRecord(),
		f("resource_schema_url", str, opt),
		scopeRecord(),
		f("scope_schema_url", str, opt),

		// Signal detail: inline events and links.
		record("events", rep,
			f("time", ts, opt),
			f("time_unix_nano", i64, opt),
			f("name", str, opt),
			f("attributes", jsn, opt),
			f("attributes_schema", jsn, opt),
			f("dropped_attributes_count", i64, opt),
		),
		f("dropped_events_count", i64, opt),
		record("links", rep,
			f("trace_id", str, opt),
			f("span_id", str, opt),
			f("trace_state", str, opt),
			f("flags", i64, opt),
			f("attributes", jsn, opt),
			f("attributes_schema", jsn, opt),
			f("dropped_attributes_count", i64, opt),
		),
		f("dropped_links_count", i64, opt),

		// Multimodal and oversized payload offloading.
		payloadRefsRecord(),

		// Contract metadata.
		f("schema_version", str, req),
	}}
}

// resourceRecord, scopeRecord and payloadRefsRecord build the column families
// the span, log and metric contracts share, so the tables cannot drift apart
// on them.
func resourceRecord() *storagepb.TableFieldSchema {
	return record("resource", opt,
		f("attributes", jsn, opt),
		f("attributes_schema", jsn, opt),
		f("dropped_attributes_count", i64, opt),
	)
}

func scopeRecord() *storagepb.TableFieldSchema {
	return record("instrumentation_scope", opt,
		f("name", str, opt),
		f("version", str, opt),
		f("attributes", jsn, opt),
		f("attributes_schema", jsn, opt),
		f("dropped_attributes_count", i64, opt),
	)
}

// payloadRefsRecord describes payloads moved out of the row. object_ref has
// the shape of a BigQuery ObjectRef value (uri, version, authorizer, details).
func payloadRefsRecord() *storagepb.TableFieldSchema {
	return record("payload_refs", rep,
		f("attribute_path", str, opt),
		f("part_index", i64, opt),
		f("mime_type", str, opt),
		f("storage_mode", str, opt),
		f("byte_size", i64, opt),
		f("sha256", str, opt),
		objectRefRecord("object_ref"),
	)
}

// objectRefRecord is a nullable STRUCT with the shape of a BigQuery ObjectRef
// value. payload_refs.object_ref and the quarantine payload_ref column share
// it, so an offloaded payload is referenced the same way everywhere.
func objectRefRecord(name string) *storagepb.TableFieldSchema {
	return record(name, opt,
		f("uri", str, opt),
		f("version", str, opt),
		f("authorizer", str, opt),
		f("details", jsn, opt),
	)
}

// LogsTableSchema is the v0alpha2 log contract: 27 columns, grouped by family
// in the same order as LogsDDL.
func LogsTableSchema() *storagepb.TableSchema {
	return &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		// Time family.
		f("timestamp", ts, req),
		f("timestamp_source", str, req),
		f("time_unix_nano", i64, req),
		f("observed_timestamp", ts, opt),
		f("observed_time_unix_nano", i64, req),
		f("receive_time", ts, req),
		f("receive_time_unix_nano", i64, req),

		// Identity and correlation family.
		f("trace_id", str, opt),
		f("span_id", str, opt),
		f("flags", i64, opt),

		// Log payload family.
		f("event_name", str, opt),
		f("severity_number", i64, opt),
		f("severity_text", str, opt),
		f("body", jsn, opt),
		f("attributes", jsn, opt),
		f("attributes_schema", jsn, opt),
		f("dropped_attributes_count", i64, opt),

		// Resource and instrumentation scope family.
		f("service_name", str, opt),
		resourceRecord(),
		f("resource_schema_url", str, opt),
		scopeRecord(),
		f("scope_schema_url", str, opt),

		// Multimodal and oversized payload offloading.
		payloadRefsRecord(),

		// Dedup identity and contract family.
		f("log_name", str, req),
		f("insert_id", str, req),
		f("record_fingerprint", str, opt),
		f("schema_version", str, req),
	}}
}

// MetricsTableSchema is the v0alpha2 metric data-point contract: 40 columns,
// grouped by family in the same order as MetricsDDL.
func MetricsTableSchema() *storagepb.TableSchema {
	return &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		// Time family.
		f("timestamp", ts, req),
		f("time_unix_nano", i64, req),
		f("start_time", ts, opt),
		f("start_time_unix_nano", i64, opt),
		f("receive_time", ts, req),
		f("receive_time_unix_nano", i64, req),

		// Metric identity and metadata.
		f("service_name", str, opt),
		f("metric_name", str, req),
		f("metric_description", str, opt),
		f("metric_unit", str, opt),
		f("metric_type", str, req),
		f("aggregation_temporality", str, opt),
		f("is_monotonic", bl, opt),
		f("flags", i64, opt),
		f("attributes", jsn, opt),
		f("attributes_schema", jsn, opt),
		f("metric_metadata", jsn, opt),
		f("metric_metadata_schema", jsn, opt),

		// Scalar values (GAUGE / SUM).
		f("value_type", str, opt),
		f("value_double", f64, opt),
		f("value_int64", i64, opt),

		// Distribution values (HISTOGRAM / EXPONENTIAL_HISTOGRAM / SUMMARY).
		f("count", i64, opt),
		f("sum", f64, opt),
		f("min", f64, opt),
		f("max", f64, opt),
		f("explicit_bounds", f64, rep),
		f("bucket_counts", i64, rep),
		f("exp_scale", i64, opt),
		f("exp_zero_count", i64, opt),
		f("exp_zero_threshold", f64, opt),
		record("exp_positive", opt,
			f("offset", i64, opt),
			f("bucket_counts", i64, rep),
		),
		record("exp_negative", opt,
			f("offset", i64, opt),
			f("bucket_counts", i64, rep),
		),
		record("summary_quantiles", rep,
			f("quantile", f64, opt),
			f("value", f64, opt),
		),

		// Exemplars (trace/span correlation).
		record("exemplars", rep,
			f("time", ts, opt),
			f("time_unix_nano", i64, opt),
			f("value_double", f64, opt),
			f("value_int64", i64, opt),
			f("span_id", str, opt),
			f("trace_id", str, opt),
			f("filtered_attributes", jsn, opt),
			f("filtered_attributes_schema", jsn, opt),
		),

		// Resource and instrumentation scope family.
		resourceRecord(),
		f("resource_schema_url", str, opt),
		scopeRecord(),
		f("scope_schema_url", str, opt),

		// Dedup and contract identity.
		f("point_fingerprint", str, req),
		f("schema_version", str, req),
	}}
}

// TimestampSource values for the log timestamp_source column, recording which
// fallback produced the partition key.
const (
	TimestampSourceEvent    = "event"
	TimestampSourceObserved = "observed"
	TimestampSourceExporter = "exporter"
)

// QuarantineTableSchema is the opt-in dead-letter contract: 12 columns, in the
// same order as QuarantineDDL. A quarantined row records why a span, log, or
// metric record was rejected, enough identity to find it again, and the record
// itself serialized as OTLP JSON.
func QuarantineTableSchema() *storagepb.TableSchema {
	return &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		f("ingest_time", ts, req),
		f("signal", str, req),
		f("error_code", str, req),
		f("error_message", str, req),
		f("service_name", str, opt),
		f("trace_id", str, opt),
		f("span_id", str, opt),
		f("raw_payload", str, opt),
		f("raw_attributes", jsn, opt),
		objectRefRecord("payload_ref"),
		f("exporter_instance", str, opt),
		f("schema_version", str, req),
	}}
}

// Signal values for the quarantine signal column, naming the table the
// rejected record was bound for.
const (
	SignalSpans   = "spans"
	SignalLogs    = "logs"
	SignalMetrics = "metrics"
)

// FieldNames lists the top-level column names of a schema, in order.
func FieldNames(s *storagepb.TableSchema) []string {
	out := make([]string, 0, len(s.GetFields()))
	for _, fld := range s.GetFields() {
		out = append(out, fld.GetName())
	}
	return out
}
