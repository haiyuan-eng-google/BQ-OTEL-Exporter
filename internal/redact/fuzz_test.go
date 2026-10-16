// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"bytes"
	"encoding/json"
	"math"
	"regexp"
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/otlpjson"
)

// fuzzSecretPatterns open every FuzzRedact rule set, ahead of any fuzzed
// pattern. Each fully matches one of fuzzKnownSecrets wherever it appears:
// none is anchored, none matches the empty string, and none can match inside
// or across one of fuzzMasks.
var fuzzSecretPatterns = []string{
	`sk-ant-[A-Za-z0-9_-]{8,}`,
	`Bearer [A-Za-z0-9._~+/=-]+`,
	`AIza[0-9A-Za-z_-]{35}`,
}

// fuzzSecretRE finds text that any of fuzzSecretPatterns matches. No string
// value may still contain such text once it has been redacted.
var fuzzSecretRE = regexp.MustCompile(strings.Join(fuzzSecretPatterns, "|"))

// fuzzKnownSecrets are the credentials FuzzRedact plants into its inputs: a
// provider API key, a bearer JWT, and a cloud API key. They are assembled from
// pieces so that secret scanners do not mistake this file for a leak.
var fuzzKnownSecrets = []string{
	"sk-ant-" + "api03-" + strings.Repeat("Fz9_q", 8),
	"Bearer " + "eyJ" + "hbGciOiJIUzI1NiJ9.eyJzdWIiOiJmdXp6In0.c2lnbmF0dXJl",
	"AIza" + "Sy" + strings.Repeat("Fuzz0-", 5) + "Q12",
}

// fuzzSensitiveKeys are blocked by DefaultBlockedKeys in each spelling the
// redactor normalizes: exact names, upper case, padding, and suffix forms.
var fuzzSensitiveKeys = []string{
	"api_key", "client_secret", "authorization", "password",
	"API_KEY", " Authorization ", "http.request.header.authorization",
	"X-Api-Key", "db.password", "refresh_token",
}

// fuzzKeys is the dictionary fuzzAttrs draws keys from as an alternative to
// raw fuzz bytes, so blocked keys turn up at every depth.
var fuzzKeys = append(slices.Clone(fuzzSensitiveKeys),
	"gen_ai.prompt", "gen_ai.usage.input_tokens", "gen_ai.token.type", "note", "")

// fuzzMasks are the replacements FuzzRedact configures. "" selects
// DefaultMaskReplacement.
var fuzzMasks = []string{"", DefaultMaskReplacement, "***", "<masked>"}

const (
	// fuzzCarrierKey holds the planted secret, wrapped in fuzzed text.
	fuzzCarrierKey = "fuzz.carrier"
	// fuzzMaxDepth bounds the nesting of maps and slices fuzzAttrs builds.
	fuzzMaxDepth = 4
	// fuzzMaxEntries bounds the values fuzzAttrs builds for one map tree.
	fuzzMaxEntries = 64
)

// FuzzRedact feeds arbitrary attribute trees and rule sets through the
// copy-on-write and in-place redaction of every signal. Whatever the input,
// redaction must not panic, must be deterministic, must leave its input
// untouched, must mask every blocked key, and must leave no string value
// that the secret patterns still match, so a planted credential can never
// survive.
//
// raw is an opcode stream for fuzzAttrs. carrier is arbitrary text that wraps
// the planted secret and also appears as a value. customKey and customPattern
// are an operator's extra blocked key and value pattern. sel picks the planted
// secret (sel%3), the planted sensitive key (sel>>2) and the mask (bits 4-5);
// bit 6 turns planting off and bit 7 turns log body redaction off.
func FuzzRedact(f *testing.F) {
	for i := range fuzzKnownSecrets {
		f.Add([]byte(nil), "", "", "", uint8(i))
	}
	// A nested map and a slice carrying blocked keys below the top level,
	// plus an int and a NaN double that must pass through untouched.
	f.Add([]byte{
		0x86, 13, // "note": map
		0x81, 1, //   "client_secret": carrier
		0xc0, 10, 5, 'h', 'e', 'l', 'l', 'o', //   "gen_ai.prompt": "hello", close
		0x07, 4, 'l', 'i', 's', 't', // "list": slice
		0x01,                      //   carrier
		0x46,                      //   map, close the slice after it
		0xc0, 2, 3, 'a', 'b', 'c', //     "authorization": "abc", close
		0x02, 3, 'n', '_', 'i', 0, 0, 0, 0, 0, 0, 0x30, 0x39, // "n_i": 12345
		0x03, 3, 'n', '_', 'd', 0x7f, 0xf8, 0, 0, 0, 0, 0, 1, // "n_d": NaN
	}, "token=", "", "", uint8(0x15))
	// An operator key and pattern on top of the defaults, with a mask that
	// differs from the default.
	f.Add([]byte{0x00, 4, 'u', 's', 'e', 'r', 6, 'S', 'S', 'N', ' ', '1', '2'}, " and ", "session.cookie_id", `\d{3}-\d{2}-\d{4}`, uint8(0x29))

	f.Fuzz(func(t *testing.T, raw []byte, carrier, customKey, customPattern string, sel uint8) {
		h := newFuzzRedactHarness(t, customKey, customPattern, sel)
		in := fuzzRedactInput{
			raw:       raw,
			carrier:   carrier,
			secret:    fuzzKnownSecrets[int(sel)%len(fuzzKnownSecrets)],
			sensitive: fuzzSensitiveKeys[int(sel>>2)%len(fuzzSensitiveKeys)],
			plant:     sel&0x40 == 0,
		}

		logs := fuzzSignal[plog.Logs]{
			name:    "logs",
			build:   in.logs,
			redact:  h.r.Logs,
			inPlace: h.r.RedactLogs,
			marshal: (&plog.ProtoMarshaler{}).MarshalLogs,
			maps: func(ld plog.Logs) []pcommon.Map {
				rl := ld.ResourceLogs().At(0)
				sl := rl.ScopeLogs().At(0)
				return []pcommon.Map{rl.Resource().Attributes(), sl.Scope().Attributes(), sl.LogRecords().At(0).Attributes()}
			},
		}
		out := logs.check(t, h, in)
		h.checkBody(t, in, out.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body())

		fuzzSignal[ptrace.Traces]{
			name:    "traces",
			build:   in.traces,
			redact:  h.r.Traces,
			inPlace: h.r.RedactTraces,
			marshal: (&ptrace.ProtoMarshaler{}).MarshalTraces,
			maps: func(td ptrace.Traces) []pcommon.Map {
				rs := td.ResourceSpans().At(0)
				ss := rs.ScopeSpans().At(0)
				span := ss.Spans().At(0)
				return []pcommon.Map{
					rs.Resource().Attributes(), ss.Scope().Attributes(), span.Attributes(),
					span.Events().At(0).Attributes(), span.Links().At(0).Attributes(),
				}
			},
		}.check(t, h, in)

		fuzzSignal[pmetric.Metrics]{
			name:    "metrics",
			build:   in.metrics,
			redact:  h.r.Metrics,
			inPlace: h.r.RedactMetrics,
			marshal: (&pmetric.ProtoMarshaler{}).MarshalMetrics,
			maps: func(md pmetric.Metrics) []pcommon.Map {
				rm := md.ResourceMetrics().At(0)
				sm := rm.ScopeMetrics().At(0)
				gauge := sm.Metrics().At(0)
				dp := gauge.Gauge().DataPoints().At(0)
				return []pcommon.Map{
					rm.Resource().Attributes(), sm.Scope().Attributes(), gauge.Metadata(),
					dp.Attributes(), dp.Exemplars().At(0).FilteredAttributes(),
					sm.Metrics().At(1).Summary().DataPoints().At(0).Attributes(),
				}
			},
		}.check(t, h, in)
	})
}

// fuzzRedactHarness is the Redactor under test and what its configuration
// promises.
type fuzzRedactHarness struct {
	r       *Redactor
	mask    string
	logBody bool
	// idempotent is false when a fuzzed value pattern is in force: such a
	// pattern may match the mask, so a second pass may legitimately change
	// the output.
	idempotent bool
	// keys and values accumulate what OnRedact reported.
	keys, values int
}

func newFuzzRedactHarness(t *testing.T, customKey, customPattern string, sel uint8) *fuzzRedactHarness {
	t.Helper()
	h := &fuzzRedactHarness{logBody: sel&0x80 == 0, idempotent: true}
	cfg := Config{
		BlockedKeys:          DefaultBlockedKeys(),
		BlockedValuePatterns: slices.Clone(fuzzSecretPatterns),
		MaskReplacement:      fuzzMasks[int(sel>>4)%len(fuzzMasks)],
		RedactLogBody:        h.logBody,
		OnRedact: func(matchType string, n int) {
			switch matchType {
			case MatchKey:
				h.keys += n
			case MatchValue:
				h.values += n
			}
		},
	}
	if strings.TrimSpace(customKey) != "" {
		cfg.BlockedKeys = append(cfg.BlockedKeys, customKey)
	}
	if strings.TrimSpace(customPattern) != "" && len(customPattern) <= 64 {
		// New rejects a pattern that does not compile alone or once folded
		// into the alternation (an unterminated \Q, for one). Fall back to
		// the fixed rules then, so the rest of the input is still exercised.
		withCustom := cfg
		withCustom.BlockedValuePatterns = append(slices.Clone(cfg.BlockedValuePatterns), customPattern)
		if r, err := New(withCustom); err == nil {
			h.r, h.idempotent = r, false
		}
	}
	if h.r == nil {
		r, err := New(cfg)
		if err != nil {
			t.Fatalf("New rejected the fixed rule set: %v", err)
		}
		h.r = r
	}
	h.mask = cfg.MaskReplacement
	if h.mask == "" {
		h.mask = DefaultMaskReplacement
	}
	return h
}

// checkMap fails if a redacted map still holds a secret: a blocked key whose
// value is not the mask, at any depth, or a string value the secret patterns
// match. Planted entries must be masked as well. Keys and bytes values are
// never scanned, by design, so they are not checked.
func (h *fuzzRedactHarness) checkMap(t *testing.T, where string, in fuzzRedactInput, m pcommon.Map) {
	t.Helper()
	h.checkTree(t, where, m)
	if attrs, schema := otlpjson.EncodeMapWithSchema(m); !json.Valid([]byte(attrs)) || !json.Valid([]byte(schema)) {
		t.Fatalf("%s: redacted attributes no longer encode as JSON: %q, %q", where, attrs, schema)
	}
	if !in.plant {
		return
	}
	if v, ok := m.Get(in.sensitive); !ok || v.Str() != h.mask {
		t.Fatalf("%s: planted key %q = %q, want the mask %q", where, in.sensitive, v.AsString(), h.mask)
	}
	v, _ := m.Get(fuzzCarrierKey)
	if s := v.Str(); strings.Contains(s, in.secret) || !strings.Contains(s, h.mask) {
		t.Fatalf("%s: planted secret was not masked: %q", where, s)
	}
}

func (h *fuzzRedactHarness) checkTree(t *testing.T, where string, m pcommon.Map) {
	t.Helper()
	for k, v := range m.All() {
		if h.r.KeyBlocked(k) {
			if v.Type() != pcommon.ValueTypeStr || v.Str() != h.mask {
				t.Fatalf("%s: blocked key %q kept a %s value, want the mask %q", where, k, v.Type(), h.mask)
			}
			continue
		}
		h.checkValue(t, where, v)
	}
}

func (h *fuzzRedactHarness) checkValue(t *testing.T, where string, v pcommon.Value) {
	t.Helper()
	switch v.Type() {
	case pcommon.ValueTypeStr:
		if loc := fuzzSecretRE.FindStringIndex(v.Str()); loc != nil {
			t.Fatalf("%s: %q survived redaction in %q", where, v.Str()[loc[0]:loc[1]], v.Str())
		}
	case pcommon.ValueTypeMap:
		h.checkTree(t, where, v.Map())
	case pcommon.ValueTypeSlice:
		for _, el := range v.Slice().All() {
			h.checkValue(t, where, el)
		}
	}
}

// checkBody checks the log body, which is redacted only when RedactLogBody
// is set.
func (h *fuzzRedactHarness) checkBody(t *testing.T, in fuzzRedactInput, body pcommon.Value) {
	t.Helper()
	switch {
	case in.plant && h.logBody:
		if s := body.Str(); strings.Contains(s, in.secret) || !strings.Contains(s, h.mask) {
			t.Fatalf("log body secret was not masked: %q", s)
		}
		h.checkValue(t, "log body", body)
	case in.plant:
		if want := in.carrier + in.secret + in.carrier; body.Str() != want {
			t.Fatalf("log body changed although RedactLogBody is off: %q, want %q", body.Str(), want)
		}
	case h.logBody:
		h.checkValue(t, "log body", body)
	}
}

// fuzzSignal is one OTLP signal type as FuzzRedact drives it.
type fuzzSignal[T comparable] struct {
	name    string
	build   func() T
	redact  func(T) T
	inPlace func(T) (keys, values int)
	marshal func(T) ([]byte, error)
	maps    func(T) []pcommon.Map
}

// check redacts a freshly built batch and returns the result, failing if the
// copy-on-write and in-place paths disagree, if either is not deterministic or
// mutates what it must not, or if any attribute map of the result still holds
// a secret.
func (s fuzzSignal[T]) check(t *testing.T, h *fuzzRedactHarness, in fuzzRedactInput) T {
	t.Helper()
	encode := func(v T) []byte {
		t.Helper()
		b, err := s.marshal(v)
		if err != nil {
			t.Fatalf("%s: marshaling: %v", s.name, err)
		}
		return b
	}

	src := s.build()
	before := encode(src)
	h.keys, h.values = 0, 0
	out := s.redact(src)
	reportedKeys, reportedValues := h.keys, h.values
	if !bytes.Equal(encode(src), before) {
		t.Fatalf("%s: copy-on-write redaction mutated its input", s.name)
	}
	got := encode(out)

	if again := encode(s.redact(s.build())); !bytes.Equal(again, got) {
		t.Fatalf("%s: redacting the same batch twice gave different results", s.name)
	}

	cp := s.build()
	keys, values := s.inPlace(cp)
	if !bytes.Equal(encode(cp), got) {
		t.Fatalf("%s: in-place redaction disagrees with the copy-on-write result", s.name)
	}
	if keys != reportedKeys || values != reportedValues {
		t.Fatalf("%s: OnRedact reported %d keys and %d values, the in-place pass replaced %d and %d",
			s.name, reportedKeys, reportedValues, keys, values)
	}
	if out == src && (keys != 0 || values != 0) {
		t.Fatalf("%s: the scan found nothing to redact, yet the in-place pass replaced %d keys and %d values", s.name, keys, values)
	}

	for _, m := range s.maps(out) {
		h.checkMap(t, s.name, in, m)
	}

	// With only the fixed patterns, redacting a redacted batch is a no-op:
	// blocked keys already hold the mask and no pattern matches the mask.
	if h.idempotent {
		if twice := encode(s.redact(out)); !bytes.Equal(twice, got) {
			t.Fatalf("%s: redacting a redacted batch changed it", s.name)
		}
	}
	return out
}

// fuzzRedactInput is one FuzzRedact case: the fuzzed attribute tree, and
// what the harness plants into it.
type fuzzRedactInput struct {
	raw     []byte
	carrier string
	// secret is planted under fuzzCarrierKey, wrapped in carrier.
	secret string
	// sensitive is a blocked key planted with carrier as its value.
	sensitive string
	plant     bool
}

// attrs builds the attribute tree raw describes and plants the secret and
// the sensitive key into it.
func (in fuzzRedactInput) attrs() pcommon.Map {
	m := pcommon.NewMap()
	(&fuzzAttrs{data: in.raw, carrier: in.carrier, budget: fuzzMaxEntries}).fill(m, 0)
	if in.plant {
		m.PutStr(fuzzCarrierKey, in.carrier+in.secret+in.carrier)
		m.PutStr(in.sensitive, in.carrier)
	}
	return m
}

// logs places the tree on the resource, the scope and the record. The body
// carries the wrapped secret when planting, and an unplanted copy of the tree
// otherwise.
func (in fuzzRedactInput) logs() plog.Logs {
	m := in.attrs()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	m.CopyTo(rl.Resource().Attributes())
	sl := rl.ScopeLogs().AppendEmpty()
	m.CopyTo(sl.Scope().Attributes())
	lr := sl.LogRecords().AppendEmpty()
	m.CopyTo(lr.Attributes())
	if in.plant {
		lr.Body().SetStr(in.carrier + in.secret + in.carrier)
	} else {
		m.CopyTo(lr.Body().SetEmptyMap())
	}
	return ld
}

// traces places the tree on the resource, the scope, a span, and the span's
// event and link.
func (in fuzzRedactInput) traces() ptrace.Traces {
	m := in.attrs()
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	m.CopyTo(rs.Resource().Attributes())
	ss := rs.ScopeSpans().AppendEmpty()
	m.CopyTo(ss.Scope().Attributes())
	span := ss.Spans().AppendEmpty()
	m.CopyTo(span.Attributes())
	m.CopyTo(span.Events().AppendEmpty().Attributes())
	m.CopyTo(span.Links().AppendEmpty().Attributes())
	return td
}

// metrics places the tree on the resource, the scope, a gauge's metadata,
// point and exemplar, and a summary point, which has no exemplars.
func (in fuzzRedactInput) metrics() pmetric.Metrics {
	m := in.attrs()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	m.CopyTo(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	m.CopyTo(sm.Scope().Attributes())
	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("fuzz.gauge")
	m.CopyTo(gauge.Metadata())
	dp := gauge.SetEmptyGauge().DataPoints().AppendEmpty()
	m.CopyTo(dp.Attributes())
	m.CopyTo(dp.Exemplars().AppendEmpty().FilteredAttributes())
	summary := sm.Metrics().AppendEmpty()
	summary.SetName("fuzz.summary")
	m.CopyTo(summary.SetEmptySummary().DataPoints().AppendEmpty().Attributes())
	return md
}

// fuzzAttrs grows a nested attribute tree from fuzz bytes. Each value starts
// with an opcode byte: the low three bits pick its type, bit 7 draws a map
// key from fuzzKeys instead of raw bytes, and bit 6 closes the enclosing
// nested map or slice after the value. Strings are raw bytes and need not be
// valid UTF-8.
type fuzzAttrs struct {
	data    []byte
	carrier string
	budget  int
}

func (b *fuzzAttrs) next() byte {
	if len(b.data) == 0 {
		return 0
	}
	c := b.data[0]
	b.data = b.data[1:]
	return c
}

// text consumes a length byte and then up to that many bytes.
func (b *fuzzAttrs) text() string {
	n := min(int(b.next())%32, len(b.data))
	s := string(b.data[:n])
	b.data = b.data[n:]
	return s
}

func (b *fuzzAttrs) bits() uint64 {
	var u uint64
	for range 8 {
		u = u<<8 | uint64(b.next())
	}
	return u
}

func (b *fuzzAttrs) fill(m pcommon.Map, depth int) {
	for len(b.data) > 0 && b.budget > 0 {
		b.budget--
		op := b.next()
		var k string
		if op&0x80 != 0 {
			k = fuzzKeys[int(b.next())%len(fuzzKeys)]
		} else {
			k = b.text()
		}
		b.set(m.PutEmpty(k), op, depth)
		if depth > 0 && op&0x40 != 0 {
			return
		}
	}
}

func (b *fuzzAttrs) fillSlice(s pcommon.Slice, depth int) {
	for len(b.data) > 0 && b.budget > 0 {
		b.budget--
		op := b.next()
		b.set(s.AppendEmpty(), op, depth)
		if op&0x40 != 0 {
			return
		}
	}
}

// set gives v the type and value op selects. At fuzzMaxDepth a map or slice
// opcode leaves v empty instead.
func (b *fuzzAttrs) set(v pcommon.Value, op byte, depth int) {
	switch op & 0x07 {
	case 0:
		v.SetStr(b.text())
	case 1:
		v.SetStr(b.carrier)
	case 2:
		v.SetInt(int64(b.bits()))
	case 3:
		v.SetDouble(math.Float64frombits(b.bits()))
	case 4:
		v.SetBool(op&0x08 != 0)
	case 5:
		v.SetEmptyBytes().FromRaw([]byte(b.text()))
	case 6:
		if depth < fuzzMaxDepth {
			b.fill(v.SetEmptyMap(), depth+1)
		}
	default:
		if depth < fuzzMaxDepth {
			b.fillSlice(v.SetEmptySlice(), depth+1)
		}
	}
}
