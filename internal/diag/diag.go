// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package diag builds exporter diagnostics that carry no telemetry content.
//
// FR13 is absolute: raw trace IDs, span IDs, source record IDs and record
// fingerprints are prohibited in normal logs, along with attribute keys,
// values and log bodies. That includes the stable cross-system correlators —
// a trace ID in a log line is exactly as identifying as the span it names, and
// is the thing an operator is most tempted to log.
//
// What is left is still enough to debug with: the destination, an error code,
// a row index, a sanitized field path, a serialized size, and a random
// operation ID that ties log lines to each other without tying them to data.
package diag // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/diag"

import (
	"crypto/rand"
	"encoding/hex"
	"strings"

	"go.uber.org/zap"
)

// OperationID returns a random identifier for one append operation.
//
// Random rather than derived: a hash of the payload would be a stable
// correlator across systems, which is the thing FR13 prohibits. This ID
// correlates log lines within one process lifetime and means nothing outside
// it.
func OperationID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A failed CSPRNG read must not take down the export path. An empty
		// ID degrades correlation; it does not lose data.
		return ""
	}
	return hex.EncodeToString(b[:])
}

// Fields builds the base structured-log fields for an append operation.
func Fields(table, operationID string) []zap.Field {
	return []zap.Field{
		zap.String("destination", table),
		zap.String("append_operation_id", operationID),
	}
}

// RowError builds fields for a single rejected row.
//
// The service's row-error message can name a field, and sometimes echoes part
// of a value, so it is sanitized to a field path before it reaches a log.
func RowError(table, operationID string, index int64, code, rawMessage string) []zap.Field {
	f := Fields(table, operationID)
	return append(f,
		zap.Int64("row_index", index),
		zap.String("error_code", code),
		zap.String("field_path", SanitizeFieldPath(rawMessage)),
	)
}

// Rejection builds fields for a row dropped by the exporter itself.
func Rejection(table, operationID string, index int, reason, fieldPath string, limit, actual int) []zap.Field {
	f := Fields(table, operationID)
	return append(f,
		zap.Int("row_index", index),
		zap.String("reason", reason),
		zap.String("field_path", fieldPath),
		zap.Int("limit", limit),
		zap.Int("actual", actual),
	)
}

// knownFieldTokens are the column and structural names that may legitimately
// appear in a diagnostic path. Anything outside this set is dropped rather
// than passed through, because the only way to be sure a token is not
// telemetry content is to recognize it.
var knownFieldTokens = map[string]bool{
	"start_timestamp": true, "start_time_unix_nano": true,
	"end_timestamp": true, "end_time_unix_nano": true,
	"export_received_timestamp": true, "duration_ns": true,
	"trace_id": true, "span_id": true, "parent_span_id": true,
	"trace_state": true, "flags": true, "name": true, "kind": true,
	"status_code": true, "status_message": true, "service_name": true,
	"resource_attributes": true, "resource_schema_url": true,
	"resource_dropped_attributes_count": true,
	"scope_name":                        true, "scope_version": true, "scope_attributes": true,
	"scope_schema_url": true, "scope_dropped_attributes_count": true,
	"schema_version": true, "span_attributes": true,
	"dropped_attributes_count": true, "dropped_events_count": true,
	"dropped_links_count": true, "events": true, "links": true,
	"timestamp": true, "timestamp_source": true, "time_unix_nano": true,
	"observed_timestamp": true, "observed_time_unix_nano": true,
	"event_name": true, "severity_number": true, "severity_text": true,
	"body": true, "log_attributes": true,
	"source_record_namespace": true, "source_record_id": true,
	"record_fingerprint": true, "attributes": true,
}

// SanitizeFieldPath reduces a service message to the recognized column names
// it mentions.
//
// Allowlist rather than denylist: a redaction pass that tries to strip values
// will eventually miss one, whereas a pass that only emits names we already
// know cannot leak by construction. The cost is a less specific diagnostic,
// which is the correct trade for a component whose payload is customer data.
func SanitizeFieldPath(message string) string {
	if message == "" {
		return ""
	}
	var found []string
	seen := map[string]bool{}
	for _, token := range strings.FieldsFunc(message, isSeparator) {
		token = strings.ToLower(token)
		if knownFieldTokens[token] && !seen[token] {
			seen[token] = true
			found = append(found, token)
		}
	}
	if len(found) == 0 {
		return "unknown"
	}
	return strings.Join(found, ",")
}

func isSeparator(r rune) bool {
	return !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'))
}
