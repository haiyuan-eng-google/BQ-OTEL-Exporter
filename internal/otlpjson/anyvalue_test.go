// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package otlpjson

import (
	"encoding/json"
	"math"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestEncodeValue(t *testing.T) {
	tests := []struct {
		name  string
		build func(pcommon.Value)
		want  string
	}{
		{"empty", func(pcommon.Value) {}, `{}`},
		{"string", func(v pcommon.Value) { v.SetStr("hello") }, `{"string_value":"hello"}`},
		{"bool", func(v pcommon.Value) { v.SetBool(true) }, `{"bool_value":true}`},
		{"double", func(v pcommon.Value) { v.SetDouble(1.5) }, `{"double_value":1.5}`},

		// The doc's worked examples.
		{"int max", func(v pcommon.Value) { v.SetInt(math.MaxInt64) }, `{"int_value":"9223372036854775807"}`},
		{"bytes", func(v pcommon.Value) { v.SetEmptyBytes().Append(1, 2, 3, 4) }, `{"bytes_value":"AQIDBA=="}`},
		{"array", func(v pcommon.Value) {
			s := v.SetEmptySlice()
			s.AppendEmpty().SetStr("one")
			s.AppendEmpty().SetInt(2)
		}, `{"array_value":{"values":[{"string_value":"one"},{"int_value":"2"}]}}`},
		{"kvlist", func(v pcommon.Value) {
			v.SetEmptyMap().PutBool("enabled", true)
		}, `{"kvlist_value":{"values":[{"key":"enabled","value":{"bool_value":true}}]}}`},

		// ProtoJSON rules for values JSON cannot express natively.
		{"nan", func(v pcommon.Value) { v.SetDouble(math.NaN()) }, `{"double_value":"NaN"}`},
		{"inf", func(v pcommon.Value) { v.SetDouble(math.Inf(1)) }, `{"double_value":"Infinity"}`},
		{"-inf", func(v pcommon.Value) { v.SetDouble(math.Inf(-1)) }, `{"double_value":"-Infinity"}`},

		{"escapes", func(v pcommon.Value) { v.SetStr("a\"b\\c\nd\te") }, `{"string_value":"a\"b\\c\nd\te"}`},
		{"control char", func(v pcommon.Value) { v.SetStr("x\x01y") }, `{"string_value":"x\u0001y"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := pcommon.NewValueEmpty()
			tc.build(v)
			got := EncodeValue(v)
			if got != tc.want {
				t.Fatalf("EncodeValue()\n got: %s\nwant: %s", got, tc.want)
			}
			if !json.Valid([]byte(got)) {
				t.Fatalf("output is not valid JSON: %s", got)
			}
		})
	}
}

// int64 at the scale boundary is the case plain JSON gets wrong, and the whole
// reason this encoding exists. A JSON number would round it.
func TestIntPrecisionSurvives(t *testing.T) {
	v := pcommon.NewValueEmpty()
	v.SetInt(math.MaxInt64)

	var decoded map[string]any
	if err := json.Unmarshal([]byte(EncodeValue(v)), &decoded); err != nil {
		t.Fatal(err)
	}
	if got := decoded["int_value"]; got != "9223372036854775807" {
		t.Fatalf("int_value = %v (%T), want the exact decimal string", got, got)
	}
}

// int and double must stay distinguishable even when the double happens to
// hold a whole number.
func TestIntAndDoubleAreDistinguishable(t *testing.T) {
	i := pcommon.NewValueEmpty()
	i.SetInt(42)
	d := pcommon.NewValueEmpty()
	d.SetDouble(42)

	if EncodeValue(i) == EncodeValue(d) {
		t.Fatal("int 42 and double 42 must not encode identically")
	}
}

// Empty encodes as {}; NULL is reserved for an absent column.
func TestEmptyIsNotNull(t *testing.T) {
	if got := EncodeValue(pcommon.NewValueEmpty()); got != "{}" {
		t.Fatalf("empty AnyValue = %s, want {}", got)
	}
}

func TestEncodeMap(t *testing.T) {
	m := pcommon.NewMap()
	m.PutInt("http.status_code", 200)
	m.PutBool("cache.hit", true)

	want := `{"cache.hit":{"bool_value":true},"http.status_code":{"int_value":"200"}}`
	if got := EncodeMap(m); got != want {
		t.Fatalf("EncodeMap()\n got: %s\nwant: %s", got, want)
	}
}

func TestEncodeMapEmpty(t *testing.T) {
	if got := EncodeMap(pcommon.NewMap()); got != "{}" {
		t.Fatalf("empty map = %s, want {}", got)
	}
}

// Key order must not depend on Go's map iteration order: the §7.4 fingerprint
// hashes this output, so an unstable order would give one logical record
// different fingerprints across runs and break deduplication entirely.
func TestEncodeMapIsDeterministic(t *testing.T) {
	build := func() pcommon.Map {
		m := pcommon.NewMap()
		for _, k := range []string{"z", "a", "m", "b", "y", "c", "n", "d"} {
			m.PutStr(k, "v")
		}
		return m
	}
	first := EncodeMap(build())
	for i := 0; i < 200; i++ {
		if got := EncodeMap(build()); got != first {
			t.Fatalf("encoding is not deterministic:\n%s\n%s", first, got)
		}
	}
}

func TestNestedStructures(t *testing.T) {
	v := pcommon.NewValueEmpty()
	outer := v.SetEmptyMap()
	inner := outer.PutEmptySlice("items")
	inner.AppendEmpty().SetInt(1)
	nested := inner.AppendEmpty().SetEmptyMap()
	nested.PutStr("k", "v")

	got := EncodeValue(v)
	if !json.Valid([]byte(got)) {
		t.Fatalf("nested output is not valid JSON: %s", got)
	}
	want := `{"kvlist_value":{"values":[{"key":"items","value":{"array_value":{"values":[` +
		`{"int_value":"1"},{"kvlist_value":{"values":[{"key":"k","value":{"string_value":"v"}}]}}]}}}]}}`
	if got != want {
		t.Fatalf("nested encoding\n got: %s\nwant: %s", got, want)
	}
}

// Slice order carries meaning and must be preserved, unlike map keys.
func TestSliceOrderPreserved(t *testing.T) {
	v := pcommon.NewValueEmpty()
	s := v.SetEmptySlice()
	for _, x := range []string{"c", "a", "b"} {
		s.AppendEmpty().SetStr(x)
	}
	want := `{"array_value":{"values":[{"string_value":"c"},{"string_value":"a"},{"string_value":"b"}]}}`
	if got := EncodeValue(v); got != want {
		t.Fatalf("slice order not preserved\n got: %s\nwant: %s", got, want)
	}
}

// encoding/json escapes <, > and & for HTML safety. Those escapes are legal
// JSON but would make the fingerprint depend on encoder policy, so this
// encoder must leave them alone.
func TestNoHTMLEscaping(t *testing.T) {
	v := pcommon.NewValueEmpty()
	v.SetStr("<a href='x'>&</a>")
	want := `{"string_value":"<a href='x'>&</a>"}`
	if got := EncodeValue(v); got != want {
		t.Fatalf("HTML characters were escaped\n got: %s\nwant: %s", got, want)
	}
}
