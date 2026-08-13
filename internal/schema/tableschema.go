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

// SpansTableSchema is the §7.1 span contract.
func SpansTableSchema() *storagepb.TableSchema {
	return &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		f("start_timestamp", ts, req),
		f("start_time_unix_nano", str, req),
		f("end_timestamp", ts, opt),
		f("end_time_unix_nano", str, opt),
		f("export_received_timestamp", ts, req),
		f("duration_ns", i64, opt),

		f("trace_id", str, req),
		f("span_id", str, req),
		f("parent_span_id", str, opt),
		f("trace_state", str, opt),
		f("flags", i64, opt),

		f("name", str, req),
		f("kind", str, opt),
		f("status_code", str, opt),
		f("status_message", str, opt),

		f("service_name", str, opt),
		f("resource_attributes", jsn, opt),
		f("resource_schema_url", str, opt),
		f("resource_dropped_attributes_count", i64, opt),

		f("scope_name", str, opt),
		f("scope_version", str, opt),
		f("scope_attributes", jsn, opt),
		f("scope_schema_url", str, opt),
		f("scope_dropped_attributes_count", i64, opt),

		f("schema_version", str, req),

		f("span_attributes", jsn, opt),
		f("dropped_attributes_count", i64, opt),
		f("dropped_events_count", i64, opt),
		f("dropped_links_count", i64, opt),

		record("events", rep,
			f("name", str, opt),
			f("timestamp", ts, opt),
			f("time_unix_nano", str, opt),
			f("attributes", jsn, opt),
			f("dropped_attributes_count", i64, opt),
		),
		record("links", rep,
			f("trace_id", str, opt),
			f("span_id", str, opt),
			f("trace_state", str, opt),
			f("flags", i64, opt),
			f("attributes", jsn, opt),
			f("dropped_attributes_count", i64, opt),
		),
	}}
}

// LogsTableSchema is the §7.2 log contract.
func LogsTableSchema() *storagepb.TableSchema {
	return &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		f("timestamp", ts, req),
		f("timestamp_source", str, opt),
		f("time_unix_nano", str, req),
		f("observed_timestamp", ts, opt),
		f("observed_time_unix_nano", str, req),
		f("export_received_timestamp", ts, req),

		f("trace_id", str, opt),
		f("span_id", str, opt),
		f("flags", i64, opt),

		f("event_name", str, opt),
		f("severity_number", i64, opt),
		f("severity_text", str, opt),
		f("body", jsn, opt),

		f("service_name", str, opt),
		f("resource_attributes", jsn, opt),
		f("resource_schema_url", str, opt),
		f("resource_dropped_attributes_count", i64, opt),

		f("scope_name", str, opt),
		f("scope_version", str, opt),
		f("scope_attributes", jsn, opt),
		f("scope_schema_url", str, opt),
		f("scope_dropped_attributes_count", i64, opt),

		f("log_attributes", jsn, opt),
		f("dropped_attributes_count", i64, opt),

		f("schema_version", str, req),

		f("source_record_namespace", str, opt),
		f("source_record_id", str, opt),
		f("record_fingerprint", str, req),
	}}
}

// TimestampSource values for the log timestamp_source column, recording which
// fallback produced the partition key.
const (
	TimestampSourceEvent    = "event"
	TimestampSourceObserved = "observed"
	TimestampSourceExporter = "exporter"
)

// FieldNames lists the top-level column names of a schema, in order.
func FieldNames(s *storagepb.TableSchema) []string {
	out := make([]string, 0, len(s.GetFields()))
	for _, fld := range s.GetFields() {
		out = append(out, fld.GetName())
	}
	return out
}
