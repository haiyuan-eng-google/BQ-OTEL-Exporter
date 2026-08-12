// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package otlpjson implements otlp-protojson-v1, the type-preserving encoding
// for OTLP AnyValue defined in §7.3 of the design doc.
//
// Plain JSON cannot carry OTLP's type information: it cannot distinguish int64
// from double at the scale boundaries, it has no representation for bytes, and
// it conflates empty with null. This encoding follows the canonical AnyValue
// oneof and ProtoJSON scalar rules instead, so the type survives the round trip
// into BigQuery.
package otlpjson // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/otlpjson"

import (
	"encoding/base64"
	"math"
	"sort"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Version is the encoding version published alongside the table schema.
const Version = "otlp-protojson-v1"

// Oneof field names, in the original snake_case proto names. These are the
// type tags; renaming one is a breaking schema change.
const (
	tagString = "string_value"
	tagBool   = "bool_value"
	tagInt    = "int_value"
	tagDouble = "double_value"
	tagBytes  = "bytes_value"
	tagArray  = "array_value"
	tagKvlist = "kvlist_value"
)

// EncodeValue renders a single AnyValue as its otlp-protojson-v1 JSON form.
//
// An explicitly empty AnyValue encodes as `{}`. SQL NULL is reserved for an
// absent column and is never produced here.
func EncodeValue(v pcommon.Value) string {
	var b strings.Builder
	writeValue(&b, v)
	return b.String()
}

// EncodeMap renders an attribute bag as a JSON object mapping attribute keys
// to encoded AnyValues.
//
// Keys are emitted in sorted order. OTLP requires unique attribute keys, so
// sorting is purely for determinism: the fingerprint in §7.4 hashes this
// output, and an unstable key order would give the same logical record
// different fingerprints on different runs.
func EncodeMap(m pcommon.Map) string {
	var b strings.Builder
	writeMap(&b, m)
	return b.String()
}

func writeValue(b *strings.Builder, v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		b.WriteString("{}")

	case pcommon.ValueTypeStr:
		b.WriteByte('{')
		writeJSONString(b, tagString)
		b.WriteByte(':')
		writeJSONString(b, v.Str())
		b.WriteByte('}')

	case pcommon.ValueTypeBool:
		b.WriteByte('{')
		writeJSONString(b, tagBool)
		b.WriteByte(':')
		b.WriteString(strconv.FormatBool(v.Bool()))
		b.WriteByte('}')

	case pcommon.ValueTypeInt:
		// ProtoJSON renders int64 as a decimal string: JSON numbers cannot
		// represent the full int64 range without precision loss.
		b.WriteByte('{')
		writeJSONString(b, tagInt)
		b.WriteByte(':')
		writeJSONString(b, strconv.FormatInt(v.Int(), 10))
		b.WriteByte('}')

	case pcommon.ValueTypeDouble:
		b.WriteByte('{')
		writeJSONString(b, tagDouble)
		b.WriteByte(':')
		writeDouble(b, v.Double())
		b.WriteByte('}')

	case pcommon.ValueTypeBytes:
		b.WriteByte('{')
		writeJSONString(b, tagBytes)
		b.WriteByte(':')
		writeJSONString(b, base64.StdEncoding.EncodeToString(v.Bytes().AsRaw()))
		b.WriteByte('}')

	case pcommon.ValueTypeSlice:
		b.WriteByte('{')
		writeJSONString(b, tagArray)
		b.WriteString(`:{"values":[`)
		s := v.Slice()
		for i := 0; i < s.Len(); i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			// Array order is meaningful and is preserved.
			writeValue(b, s.At(i))
		}
		b.WriteString("]}}")

	case pcommon.ValueTypeMap:
		b.WriteByte('{')
		writeJSONString(b, tagKvlist)
		b.WriteString(`:{"values":[`)
		writeKvlistEntries(b, v.Map())
		b.WriteString("]}}")

	default:
		// pcommon exposes only the compiled value kinds, so this is
		// unreachable for any payload that got through the receiver. Encoding
		// it as empty keeps a future pdata addition from producing invalid
		// JSON in a table column.
		b.WriteString("{}")
	}
}

func writeKvlistEntries(b *strings.Builder, m pcommon.Map) {
	for i, k := range sortedKeys(m) {
		if i > 0 {
			b.WriteByte(',')
		}
		v, _ := m.Get(k)
		b.WriteString(`{"key":`)
		writeJSONString(b, k)
		b.WriteString(`,"value":`)
		writeValue(b, v)
		b.WriteByte('}')
	}
}

func writeMap(b *strings.Builder, m pcommon.Map) {
	b.WriteByte('{')
	for i, k := range sortedKeys(m) {
		if i > 0 {
			b.WriteByte(',')
		}
		v, _ := m.Get(k)
		writeJSONString(b, k)
		b.WriteByte(':')
		writeValue(b, v)
	}
	b.WriteByte('}')
}

// sortedKeys returns the map's keys ordered by UTF-8 byte order, which is what
// Go's string comparison already gives.
func sortedKeys(m pcommon.Map) []string {
	keys := make([]string, 0, m.Len())
	for k := range m.All() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// writeDouble applies the ProtoJSON rules for non-finite doubles, which JSON
// itself cannot express.
func writeDouble(b *strings.Builder, f float64) {
	switch {
	case math.IsNaN(f):
		b.WriteString(`"NaN"`)
	case math.IsInf(f, 1):
		b.WriteString(`"Infinity"`)
	case math.IsInf(f, -1):
		b.WriteString(`"-Infinity"`)
	default:
		b.WriteString(strconv.FormatFloat(f, 'g', -1, 64))
	}
}

const hexDigits = "0123456789abcdef"

// writeJSONString emits a JSON string literal.
//
// Written by hand rather than via encoding/json because that package escapes
// <, > and & for HTML safety. Those escapes are legal JSON but they would make
// the fingerprint in §7.4 depend on an encoder policy rather than on the
// record's content.
func writeJSONString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			if r < 0x20 {
				b.WriteString(`\u00`)
				b.WriteByte(hexDigits[(r>>4)&0xF])
				b.WriteByte(hexDigits[r&0xF])
				continue
			}
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
}
