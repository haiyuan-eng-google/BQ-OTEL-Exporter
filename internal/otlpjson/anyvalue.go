// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package otlpjson renders OTLP AnyValues and attribute bags as JSON for
// BigQuery JSON columns.
//
// Plain JSON on its own does not carry OTLP's type information: it has no
// representation for bytes, and an int64 has to travel as a decimal string to
// keep its precision, which makes it look like any other string. The package
// offers two encodings that recover the type in different ways:
//
//   - otlp-plainjson-v2 (PlainVersion) writes every value as ordinary JSON, so
//     the column reads naturally with BigQuery's JSON functions, and records
//     the OTLP type of each leaf in a companion attributes_schema object
//     produced by EncodeMapWithSchema.
//   - otlp-protojson-v1 (Version) is the type-preserving encoding defined in
//     §7.3 of the design doc. It follows the canonical AnyValue oneof and
//     ProtoJSON scalar rules, so the type survives the round trip into
//     BigQuery inside the value itself. It is kept until its remaining callers
//     move to otlp-plainjson-v2.
package otlpjson // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/otlpjson"

import (
	"encoding/base64"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Version is the encoding version published alongside the table schema.
const Version = "otlp-protojson-v1"

// PlainVersion is the version of the plain JSON encoding produced by
// EncodePlainValue, EncodePlainMap and EncodeMapWithSchema.
const PlainVersion = "otlp-plainjson-v2"

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

// Leaf type names recorded in an attributes_schema object. Renaming one is a
// breaking schema change.
const (
	schemaString = "string"
	schemaBool   = "bool"
	schemaInt    = "int64"
	schemaDouble = "double"
	schemaBytes  = "bytes"
	schemaEmpty  = "empty"
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

// EncodePlainValue renders a single AnyValue as otlp-plainjson-v2 JSON.
//
// Strings, booleans, arrays and maps become their JSON counterparts. An int64
// becomes a decimal string, so values beyond 2^53 keep every digit in JSON
// parsers that read numbers as doubles. A double becomes a JSON number, except
// NaN and the infinities, which take the ProtoJSON string forms "NaN",
// "Infinity" and "-Infinity". Bytes become a standard base64 string. An
// explicitly empty AnyValue becomes JSON null.
//
// Invalid UTF-8 in strings and map keys is replaced with U+FFFD, so the result
// is always valid JSON.
func EncodePlainValue(v pcommon.Value) string {
	var b strings.Builder
	plainWriter{attrs: &b}.value(v)
	return b.String()
}

// EncodePlainMap renders an attribute bag as an otlp-plainjson-v2 JSON object.
// An empty bag encodes as {}.
//
// Keys are emitted in sorted order, so one logical record always encodes to
// the same bytes. Keys that are equal, or become equal once invalid UTF-8 is
// replaced, are emitted once, with the value of the first in payload order:
// the entry pcommon.Map.Get returns.
func EncodePlainMap(m pcommon.Map) string {
	var b strings.Builder
	plainWriter{attrs: &b}.object(m)
	return b.String()
}

// EncodeMapWithSchema renders an attribute bag as otlp-plainjson-v2 together
// with its companion attributes_schema object.
//
// attrsJSON is exactly EncodePlainMap(m). schemaJSON mirrors its shape: every
// leaf value is replaced by the name of its OTLP type ("string", "bool",
// "int64", "double", "bytes", or "empty" for an empty AnyValue), a nested map
// becomes a nested object of the same keys, and an array becomes an array of
// its elements' schemas. The schema is what tells an int64 from a numeric
// string, or bytes from a string that happens to look like base64. An empty
// bag encodes as {} and {}.
func EncodeMapWithSchema(m pcommon.Map) (attrsJSON, schemaJSON string) {
	var attrs, schema strings.Builder
	plainWriter{attrs: &attrs, schema: &schema}.object(m)
	return attrs.String(), schema.String()
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

// plainWriter emits otlp-plainjson-v2 to attrs and, when schema is non-nil,
// the matching attributes_schema to schema. Both come out of a single walk, so
// they cannot disagree about structure, key order, or which of two duplicate
// keys was kept.
type plainWriter struct {
	attrs  *strings.Builder
	schema *strings.Builder
}

func (w plainWriter) value(v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		writeJSONString(w.attrs, strings.ToValidUTF8(v.Str(), "\uFFFD"))
		w.leaf(schemaString)

	case pcommon.ValueTypeBool:
		w.attrs.WriteString(strconv.FormatBool(v.Bool()))
		w.leaf(schemaBool)

	case pcommon.ValueTypeInt:
		// A decimal string, as in ProtoJSON: a JSON number would lose
		// precision beyond 2^53 in parsers that read numbers as doubles.
		writeJSONString(w.attrs, strconv.FormatInt(v.Int(), 10))
		w.leaf(schemaInt)

	case pcommon.ValueTypeDouble:
		writePlainDouble(w.attrs, v.Double())
		w.leaf(schemaDouble)

	case pcommon.ValueTypeBytes:
		writeJSONString(w.attrs, base64.StdEncoding.EncodeToString(v.Bytes().AsRaw()))
		w.leaf(schemaBytes)

	case pcommon.ValueTypeSlice:
		w.punct('[')
		s := v.Slice()
		for i := 0; i < s.Len(); i++ {
			if i > 0 {
				w.punct(',')
			}
			// Array order is meaningful and is preserved.
			w.value(s.At(i))
		}
		w.punct(']')

	case pcommon.ValueTypeMap:
		w.object(v.Map())

	default:
		// ValueTypeEmpty. pcommon exposes only the compiled value kinds, so
		// nothing else reaches here from a decoded payload; a future pdata
		// addition would still produce valid JSON in the column.
		w.attrs.WriteString("null")
		w.leaf(schemaEmpty)
	}
}

// object writes m as a JSON object with sorted, unique keys.
func (w plainWriter) object(m pcommon.Map) {
	w.punct('{')
	for i, e := range plainEntries(m) {
		if i > 0 {
			w.punct(',')
		}
		w.key(e.key)
		w.value(e.value)
	}
	w.punct('}')
}

// key writes an object key and its colon to both outputs.
func (w plainWriter) key(k string) {
	writeJSONString(w.attrs, k)
	w.attrs.WriteByte(':')
	if w.schema != nil {
		writeJSONString(w.schema, k)
		w.schema.WriteByte(':')
	}
}

// punct writes JSON structure that both outputs share.
func (w plainWriter) punct(c byte) {
	w.attrs.WriteByte(c)
	if w.schema != nil {
		w.schema.WriteByte(c)
	}
}

// leaf records the OTLP type of the leaf value just written to attrs.
func (w plainWriter) leaf(typ string) {
	if w.schema != nil {
		writeJSONString(w.schema, typ)
	}
}

type plainEntry struct {
	key   string
	value pcommon.Value
}

// plainEntries returns m's entries with their keys made valid UTF-8 and sorted
// by UTF-8 byte order, keeping only the first entry in payload order for each
// key.
//
// OTLP requires unique keys, but nothing on the wire enforces it and
// pcommon.Map keeps every entry it decodes; repairing invalid UTF-8 can also
// turn two distinct keys into one. Keeping the first matches pcommon.Map.Get,
// and emitting each key once avoids duplicate object names, which RFC 8259
// leaves to each JSON reader to resolve as it sees fit.
func plainEntries(m pcommon.Map) []plainEntry {
	entries := make([]plainEntry, 0, m.Len())
	for k, v := range m.All() {
		entries = append(entries, plainEntry{key: strings.ToValidUTF8(k, "\uFFFD"), value: v})
	}
	// The stable sort keeps equal keys in payload order, so compacting keeps
	// the first of each.
	slices.SortStableFunc(entries, func(a, b plainEntry) int {
		return strings.Compare(a.key, b.key)
	})
	return slices.CompactFunc(entries, func(a, b plainEntry) bool {
		return a.key == b.key
	})
}

// writePlainDouble writes f as a JSON number formatted the way encoding/json
// formats a float64: the shortest digits that round-trip, in plain decimal
// notation unless the magnitude is below 1e-6 or at least 1e21. NaN and the
// infinities, which a JSON number cannot express, take the ProtoJSON string
// forms.
func writePlainDouble(b *strings.Builder, f float64) {
	switch {
	case math.IsNaN(f):
		b.WriteString(`"NaN"`)
	case math.IsInf(f, 1):
		b.WriteString(`"Infinity"`)
	case math.IsInf(f, -1):
		b.WriteString(`"-Infinity"`)
	default:
		format := byte('f')
		if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
			format = 'e'
		}
		s := strconv.FormatFloat(f, format, -1, 64)
		// Drop the padding zero of a two-digit negative exponent, e-07 to
		// e-7, as encoding/json does.
		if n := len(s); format == 'e' && n >= 4 && s[n-4] == 'e' && s[n-3] == '-' && s[n-2] == '0' {
			s = s[:n-2] + s[n-1:]
		}
		b.WriteString(s)
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
