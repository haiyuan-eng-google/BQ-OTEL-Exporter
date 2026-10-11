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

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// LogFingerprint computes the record_fingerprint of a log record:
//
//	"fp1:" + lowercase_hex(SHA-256(canonical_log_record))
//
// canonical is the record's canonical form from canonicalLogRecord, and
// ordinal is its intra_batch_ordinal: the 0-based count of records with the
// same canonical form before it in the same export batch. The canonical log
// record that is hashed is canonical followed by the ordinal. The ordinal is
// what keeps identical records within one batch apart, while a retry of the
// batch, which repeats its records in the same order, reproduces every
// fingerprint.
func LogFingerprint(canonical string, ordinal int) string {
	var b strings.Builder
	b.WriteString(canonical)
	writeField(&b, "intra_batch_ordinal", strconv.Itoa(ordinal))
	sum := sha256.Sum256([]byte(b.String()))
	return schema.FingerprintPrefix + hex.EncodeToString(sum[:])
}

// fingerprinter assigns fingerprints across one export batch, counting the
// records seen so far per canonical form to derive each intra_batch_ordinal.
// It is keyed by the ordinal-0 fingerprint rather than the canonical form, so
// memory stays bounded by the record count, not the payload size, and a
// record seen for the first time costs a single hash.
type fingerprinter map[string]int

// next returns the fingerprint of the next record with the given canonical
// form.
func (f fingerprinter) next(canonical string) string {
	first := LogFingerprint(canonical, 0)
	ordinal := f[first]
	f[first] = ordinal + 1
	if ordinal == 0 {
		return first
	}
	return LogFingerprint(canonical, ordinal)
}

// canonicalLogRecord renders the part of a log record's canonical form that
// is specific to the record, followed by origin, the part its resource and
// scope contribute, from canonicalOrigin.
//
// It covers every non-derived OTLP field. Values are the plain JSON the row
// stores together with their type schemas, since plain JSON alone would give
// an int 42 and a string "42" the same fingerprint. Times are the raw OTLP
// nanoseconds rather than the truncated microsecond timestamps.
//
// The receive time is deliberately excluded. It is ingestion-local and
// derived: including it would give every retry of the same record a different
// fingerprint, which would defeat the entire mechanism.
func canonicalLogRecord(lr plog.LogRecord, attrs encodedBag, body, bodySchema, origin string) string {
	var b strings.Builder

	// A field-tagged, length-free encoding would be ambiguous across field
	// boundaries, so every component is written with an explicit name and a
	// separator that cannot appear unescaped in the JSON-encoded parts.
	writeField(&b, "version", schema.FingerprintPrefix)
	writeField(&b, "time_unix_nano", strconv.FormatUint(uint64(lr.Timestamp()), 10))
	writeField(&b, "observed_time_unix_nano", strconv.FormatUint(uint64(lr.ObservedTimestamp()), 10))

	writeField(&b, "trace_id", traceIDString(lr.TraceID()))
	writeField(&b, "span_id", spanIDString(lr.SpanID()))
	writeField(&b, "flags", strconv.FormatUint(uint64(lr.Flags()), 10))

	writeField(&b, "event_name", lr.EventName())
	writeField(&b, "severity_number", strconv.FormatInt(int64(lr.SeverityNumber()), 10))
	writeField(&b, "severity_text", lr.SeverityText())
	writeField(&b, "body", body)
	writeField(&b, "body_schema", bodySchema)

	writeField(&b, "attributes", attrs.attributes)
	writeField(&b, "attributes_schema", attrs.schema)
	writeField(&b, "dropped_attributes_count", strconv.FormatInt(attrs.dropped, 10))

	b.WriteString(origin)
	return b.String()
}

// canonicalOrigin renders the part of the canonical form that a resource and
// scope contribute, once for all the log records under them.
func canonicalOrigin(
	res encodedBag,
	resSchemaURL string,
	scope pcommon.InstrumentationScope,
	scopeBag encodedBag,
	scopeSchemaURL string,
) string {
	var b strings.Builder
	writeField(&b, "resource.attributes", res.attributes)
	writeField(&b, "resource.attributes_schema", res.schema)
	writeField(&b, "resource.dropped_attributes_count", strconv.FormatInt(res.dropped, 10))
	writeField(&b, "resource_schema_url", resSchemaURL)

	writeField(&b, "instrumentation_scope.name", scope.Name())
	writeField(&b, "instrumentation_scope.version", scope.Version())
	writeField(&b, "instrumentation_scope.attributes", scopeBag.attributes)
	writeField(&b, "instrumentation_scope.attributes_schema", scopeBag.schema)
	writeField(&b, "instrumentation_scope.dropped_attributes_count", strconv.FormatInt(scopeBag.dropped, 10))
	writeField(&b, "scope_schema_url", scopeSchemaURL)
	return b.String()
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
