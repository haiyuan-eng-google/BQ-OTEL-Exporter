// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/otlpjson"
)

// FingerprintPrefix versions the fingerprint format, so a future change to the
// canonical form is detectable in the data rather than silently producing
// different hashes for the same record.
const FingerprintPrefix = "v1:"

// LogFingerprint computes the §7.4 record fingerprint:
//
//	"v1:" + lowercase_hex(SHA256(canonical_log_record))
//
// The canonical form covers every non-derived OTLP source field. Object keys
// are sorted by UTF-8 byte order, array order is preserved, values use
// otlp-protojson-v1, and raw *_unix_nano values are used rather than the
// truncated microsecond timestamps.
//
// export_received_timestamp is deliberately excluded. It is ingestion-local
// and derived: including it would give every retry of the same record a
// different fingerprint, which would defeat the entire mechanism.
func LogFingerprint(
	lr plog.LogRecord,
	resource pcommon.Resource,
	resourceSchemaURL string,
	scope pcommon.InstrumentationScope,
	scopeSchemaURL string,
) string {
	var b strings.Builder

	// A field-tagged, length-free encoding would be ambiguous across field
	// boundaries, so every component is written with an explicit name and a
	// separator that cannot appear unescaped in the JSON-encoded parts.
	writeField(&b, "version", FingerprintPrefix)
	writeField(&b, "time_unix_nano", strconv.FormatUint(uint64(lr.Timestamp()), 10))
	writeField(&b, "observed_time_unix_nano", strconv.FormatUint(uint64(lr.ObservedTimestamp()), 10))

	writeField(&b, "trace_id", traceIDString(lr.TraceID()))
	writeField(&b, "span_id", spanIDString(lr.SpanID()))
	writeField(&b, "flags", strconv.FormatUint(uint64(lr.Flags()), 10))

	writeField(&b, "event_name", lr.EventName())
	writeField(&b, "severity_number", strconv.FormatInt(int64(lr.SeverityNumber()), 10))
	writeField(&b, "severity_text", lr.SeverityText())
	writeField(&b, "body", otlpjson.EncodeValue(lr.Body()))

	writeField(&b, "log_attributes", otlpjson.EncodeMap(lr.Attributes()))
	writeField(&b, "dropped_attributes_count", strconv.FormatUint(uint64(lr.DroppedAttributesCount()), 10))

	writeField(&b, "resource_attributes", otlpjson.EncodeMap(resource.Attributes()))
	writeField(&b, "resource_schema_url", resourceSchemaURL)
	writeField(&b, "resource_dropped_attributes_count", strconv.FormatUint(uint64(resource.DroppedAttributesCount()), 10))

	writeField(&b, "scope_name", scope.Name())
	writeField(&b, "scope_version", scope.Version())
	writeField(&b, "scope_attributes", otlpjson.EncodeMap(scope.Attributes()))
	writeField(&b, "scope_schema_url", scopeSchemaURL)
	writeField(&b, "scope_dropped_attributes_count", strconv.FormatUint(uint64(scope.DroppedAttributesCount()), 10))

	sum := sha256.Sum256([]byte(b.String()))
	return FingerprintPrefix + hex.EncodeToString(sum[:])
}

// writeField appends one length-prefixed component. The length prefix is what
// makes the encoding unambiguous: without it, moving a character from the end
// of one field to the start of the next would produce the same byte stream.
func writeField(b *strings.Builder, name, value string) {
	b.WriteString(name)
	b.WriteByte('=')
	b.WriteString(strconv.Itoa(len(value)))
	b.WriteByte(':')
	b.WriteString(value)
	b.WriteByte('\n')
}

// traceIDString renders a trace ID as lowercase hex, or empty when unset.
// Empty is distinct from a zero ID: OTLP uses an all-zero ID to mean absent.
func traceIDString(id pcommon.TraceID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

func spanIDString(id pcommon.SpanID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}
