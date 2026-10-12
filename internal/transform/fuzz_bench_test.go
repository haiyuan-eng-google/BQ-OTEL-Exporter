// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"bytes"
	"math"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/redact"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

func FuzzSpanRows(f *testing.F) {
	f.Add("invoke_agent", "gcp.vertex.agent", "gemini-2.5-pro", int64(120), int64(45), uint8(2), int64(1700000000000000000), int64(1700000001000000000))
	f.Add("", "", "", int64(-10), int64(0), uint8(0), int64(1700000005000000000), int64(1700000001000000000))
	f.Add("call_llm\x00\xff", "openai", "gpt-4o", int64(999999), int64(888888), uint8(5), int64(0), int64(0))

	f.Fuzz(func(t *testing.T, name, system, model string, inTok, outTok int64, kind uint8, startNano, endNano int64) {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "fuzz-service")
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("fuzz.scope")
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
		sp.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
		sp.SetName(name)
		sp.SetKind(ptrace.SpanKind(kind % 7))
		if startNano > 0 {
			sp.SetStartTimestamp(pcommon.Timestamp(uint64(startNano)))
		}
		if endNano > 0 {
			sp.SetEndTimestamp(pcommon.Timestamp(uint64(endNano)))
		}
		sp.Attributes().PutStr("gen_ai.system", system)
		sp.Attributes().PutStr("gen_ai.request.model", model)
		sp.Attributes().PutInt("gen_ai.usage.input_tokens", inTok)
		sp.Attributes().PutInt("gen_ai.usage.output_tokens", outTok)

		rows, rejects := SpanRows(td, Options{
			Limits: DefaultLimits(),
			Now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		})

		// Conservation property: every input span produces either a Row or a Rejection.
		if len(rows)+len(rejects) != 1 {
			t.Fatalf("conservation violated: len(rows)=%d + len(rejects)=%d != 1", len(rows), len(rejects))
		}
		for _, r := range rows {
			if d, ok := r["duration_nano"].(int64); ok && d < 0 {
				t.Fatalf("negative duration_nano: %d", d)
			}
			for col, v := range r {
				if s, ok := v.(string); ok && !utf8.ValidString(s) {
					t.Fatalf("column %s is not valid UTF-8", col)
				}
			}
		}

		// Redaction pass: the blocked key forces the copy-on-write branch on
		// every input, the value pattern exercises substring replacement on
		// fuzzed text, and the caller's batch must come back untouched.
		sp.Attributes().PutStr("x-api-key", system)
		before, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
		if err != nil {
			t.Fatal(err)
		}
		rrows, rrejects := SpanRows(td, Options{
			Limits:   DefaultLimits(),
			Now:      func() time.Time { return time.Unix(1700000000, 0).UTC() },
			Redactor: fuzzRedactor(t),
		})
		if len(rrows)+len(rrejects) != 1 {
			t.Fatalf("redaction broke conservation: rows=%d rejects=%d", len(rrows), len(rrejects))
		}
		after, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) {
			t.Fatal("SpanRows with a Redactor mutated the caller's batch")
		}
	})
}

// fuzzRedactor is the redactor shared by the fuzz targets: default keys plus
// value patterns broad enough to fire on fuzzed strings.
func fuzzRedactor(t testing.TB) *redact.Redactor {
	t.Helper()
	r, err := redact.New(redact.Config{
		BlockedKeys:          redact.DefaultBlockedKeys(),
		BlockedValuePatterns: []string{`(?i)secret[a-z0-9_-]*`, `\d{3}-\d{2}-\d{4}`},
		RedactLogBody:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func FuzzLogRows(f *testing.F) {
	f.Add("gen_ai.user.message", "Hello agent", "sess-1", "inv-1", int64(1700000000000000000), int64(1700000001000000000))
	f.Add("", "\xff\xfeinvalid_utf8", "", "", int64(0), int64(0))
	f.Add("gen_ai.choice", `{"role":"assistant","content":"hi"}`, "sess-2", "inv-2", int64(1700000002000000000), int64(0))

	f.Fuzz(func(t *testing.T, eventName, bodyStr, sessID, invID string, tsNano, obsNano int64) {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "fuzz-log-svc")
		sl := rl.ScopeLogs().AppendEmpty()
		lr := sl.LogRecords().AppendEmpty()
		if tsNano > 0 {
			lr.SetTimestamp(pcommon.Timestamp(uint64(tsNano)))
		}
		if obsNano > 0 {
			lr.SetObservedTimestamp(pcommon.Timestamp(uint64(obsNano)))
		}
		lr.SetEventName(eventName)
		lr.Body().SetStr(bodyStr)
		lr.Attributes().PutStr("gen_ai.conversation.id", sessID)
		lr.Attributes().PutStr("gcp.vertex.agent.invocation_id", invID)

		rows, rejects := LogRows(ld, Options{
			Limits: DefaultLimits(),
			Now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		})

		// Conservation property: every input log record produces either a Row or a Rejection.
		if len(rows)+len(rejects) != 1 {
			t.Fatalf("conservation violated: len(rows)=%d + len(rejects)=%d != 1", len(rows), len(rejects))
		}
		for _, r := range rows {
			id, ok := r["insert_id"].(string)
			if !ok || len(id) != 68 || !strings.HasPrefix(id, "fp1:") {
				t.Fatalf("expected 68-char fp1: insert_id, got %q", id)
			}
		}

		// Redaction pass over the same record, including the body path.
		lr.Attributes().PutStr("session_token", sessID)
		before, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
		if err != nil {
			t.Fatal(err)
		}
		rrows, rrejects := LogRows(ld, Options{
			Limits:   DefaultLimits(),
			Now:      func() time.Time { return time.Unix(1700000000, 0).UTC() },
			Redactor: fuzzRedactor(t),
		})
		if len(rrows)+len(rrejects) != 1 {
			t.Fatalf("redaction broke conservation: rows=%d rejects=%d", len(rrows), len(rrejects))
		}
		after, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) {
			t.Fatal("LogRows with a Redactor mutated the caller's batch")
		}
		for _, r := range rrows {
			id, ok := r["insert_id"].(string)
			if !ok || len(id) != 68 || !strings.HasPrefix(id, "fp1:") {
				t.Fatalf("redacted record lost its fp1: insert_id, got %q", id)
			}
		}
	})
}

func FuzzLogFingerprint(f *testing.F) {
	f.Add("canonical-log-record-1", 0)
	f.Add("", 1)
	f.Add("duplicate-record", 42)

	f.Fuzz(func(t *testing.T, canonical string, ordinal int) {
		fp1 := LogFingerprint(canonical, ordinal)
		fp2 := LogFingerprint(canonical, ordinal)
		if fp1 != fp2 {
			t.Fatalf("LogFingerprint not deterministic: %q != %q", fp1, fp2)
		}
		if len(fp1) != 68 || !strings.HasPrefix(fp1, "fp1:") {
			t.Fatalf("expected 68-char fp1: digest, got len=%d (%q)", len(fp1), fp1)
		}
		fpNext := LogFingerprint(canonical, ordinal+1)
		if fp1 == fpNext {
			t.Fatalf("expected distinct fingerprints for different ordinals (%d vs %d)", ordinal, ordinal+1)
		}
	})
}

func TestTransformConservationProperty(t *testing.T) {
	const totalSpans = 25
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()

	for i := 0; i < totalSpans; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID([16]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, byte(i + 1)}))
		sp.SetSpanID(pcommon.SpanID([8]byte{2, 2, 2, 2, 2, 2, 2, byte(i + 1)}))
		sp.SetName("span")
		sp.SetStartTimestamp(pcommon.Timestamp(1700000000000000000))
		sp.SetEndTimestamp(pcommon.Timestamp(1700000001000000000))
		if i%5 == 0 {
			// Exceed attribute count limit to force rejection.
			for k := 0; k < 10; k++ {
				sp.Attributes().PutInt(strings.Repeat("k", k+1), int64(k))
			}
		}
	}

	limits := DefaultLimits()
	limits.MaxAttributeCount = 5
	rows, rejects := SpanRows(td, Options{Limits: limits})
	if len(rows)+len(rejects) != totalSpans {
		t.Fatalf("SpanRows conservation violated: rows=%d + rejects=%d != %d", len(rows), len(rejects), totalSpans)
	}
	if len(rejects) != 5 {
		t.Fatalf("expected 5 rejected spans, got %d", len(rejects))
	}
}

func BenchmarkSpanRows(b *testing.B) {
	td := benchTraces(false)
	opts := Options{
		Limits: DefaultLimits(),
		Now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, rejects := SpanRows(td, opts)
		if len(rows) != 10 || len(rejects) != 0 {
			b.Fatalf("unexpected result: rows=%d rejects=%d", len(rows), len(rejects))
		}
	}
}

// BenchmarkSpanRowsRedactionEnabled measures the redaction step on the same
// fixture as BenchmarkSpanRows. "nomatch" is the hot path that must stay
// allocation-neutral relative to the baseline; "match" pays one batch clone.
func BenchmarkSpanRowsRedactionEnabled(b *testing.B) {
	for _, tc := range []struct {
		name       string
		withSecret bool
	}{
		{"nomatch", false},
		{"match", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			td := benchTraces(tc.withSecret)
			opts := Options{
				Limits:   DefaultLimits(),
				Now:      func() time.Time { return time.Unix(1700000000, 0).UTC() },
				Redactor: fuzzRedactor(b),
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, rejects := SpanRows(td, opts)
				if len(rows) != 10 || len(rejects) != 0 {
					b.Fatalf("unexpected result: rows=%d rejects=%d", len(rows), len(rejects))
				}
			}
		})
	}
}

// benchTraces is the ten-span GenAI batch the span benchmarks share. With
// withSecret each span also carries a blocked key and a pattern-matching
// value so the redaction clone path is exercised on every record.
func benchTraces(withSecret bool) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "bench-agent")
	rs.Resource().Attributes().PutStr("service.version", "1.0.0")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("opentelemetry.instrumentation.genai")

	for i := 0; i < 10; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, byte(i + 1)}))
		sp.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, byte(i + 1)}))
		sp.SetName("invoke_agent")
		sp.SetKind(ptrace.SpanKindClient)
		sp.SetStartTimestamp(pcommon.Timestamp(1700000000000000000))
		sp.SetEndTimestamp(pcommon.Timestamp(1700000000500000000))
		sp.Attributes().PutStr("gen_ai.system", "gcp.vertex.agent")
		sp.Attributes().PutStr("gen_ai.operation.name", "invoke_agent")
		sp.Attributes().PutStr("gen_ai.request.model", "gemini-2.5-pro")
		sp.Attributes().PutInt("gen_ai.usage.input_tokens", 512)
		sp.Attributes().PutInt("gen_ai.usage.output_tokens", 256)
		sp.Attributes().PutStr("gen_ai.conversation.id", "conv-bench-1")
		if withSecret {
			sp.Attributes().PutStr("http.request.header.authorization", "Bearer secret-bench-token")
			sp.Attributes().PutStr("api_key", "k-bench")
		}
	}
	return td
}

func BenchmarkLogRows(b *testing.B) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "bench-agent")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("opentelemetry.instrumentation.genai")

	for i := 0; i < 10; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.Timestamp(1700000000000000000 + uint64(i)*1000))
		lr.SetObservedTimestamp(pcommon.Timestamp(1700000000100000000 + uint64(i)*1000))
		lr.SetEventName("gen_ai.user.message")
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.SetSeverityText("INFO")
		lr.Body().SetStr("Summarize the quarterly metrics for Q3.")
		lr.Attributes().PutStr("gen_ai.system", "gcp.vertex.agent")
		lr.Attributes().PutStr("gen_ai.conversation.id", "conv-bench-1")
	}

	opts := Options{
		Limits: DefaultLimits(),
		Now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, rejects := LogRows(ld, opts)
		if len(rows) != 10 || len(rejects) != 0 {
			b.Fatalf("unexpected result: rows=%d rejects=%d", len(rows), len(rejects))
		}
	}
}

// metricFuzzInput holds the values FuzzMetricRows spreads over one batch that
// carries every metric data type.
type metricFuzzInput struct {
	name, unit, key, val string
	intVal               int64
	dblVal               float64
	count                uint64
	ts, start            int64
	flags, buckets       uint8
	scale, offset        int32
	// withSecret adds blocked-key attributes to the resource and to every
	// data point and exemplar, so a Redactor has something to scrub.
	withSecret bool
}

// build returns six data points: two identical gauge points first, then one
// sum, histogram, exponential histogram and summary point. The identical pair
// exercises the intra-batch ordinal that keeps their fingerprints apart.
func (in metricFuzzInput) build() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "fuzz-metric-svc")
	if in.withSecret {
		rm.Resource().Attributes().PutStr("session_token", in.val)
	}
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("fuzz.scope")

	at := pcommon.Timestamp(uint64(in.ts))
	since := pcommon.Timestamp(uint64(in.start))
	flags := pmetric.DataPointFlags(in.flags)
	n := int(in.buckets % 8)
	counts := make([]uint64, n)
	bounds := make([]float64, n)
	for i := range n {
		counts[i] = in.count + uint64(i)
		bounds[i] = in.dblVal + float64(i)
	}

	attrs := func(m pcommon.Map) {
		m.PutStr(in.key, in.val)
		if in.withSecret {
			m.PutStr("x-api-key", in.val)
		}
	}
	exemplar := func(es pmetric.ExemplarSlice) {
		ex := es.AppendEmpty()
		ex.SetTimestamp(at)
		ex.SetDoubleValue(in.dblVal)
		ex.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
		ex.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
		attrs(ex.FilteredAttributes())
	}
	next := func() pmetric.Metric {
		m := sm.Metrics().AppendEmpty()
		m.SetName(in.name)
		m.SetDescription(in.val)
		m.SetUnit(in.unit)
		return m
	}

	gauge := next().SetEmptyGauge()
	for range 2 {
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(at)
		dp.SetStartTimestamp(since)
		dp.SetFlags(flags)
		dp.SetIntValue(in.intVal)
		attrs(dp.Attributes())
		exemplar(dp.Exemplars())
	}

	total := next().SetEmptySum()
	total.SetIsMonotonic(in.buckets%2 == 0)
	total.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sdp := total.DataPoints().AppendEmpty()
	sdp.SetTimestamp(at)
	sdp.SetStartTimestamp(since)
	sdp.SetFlags(flags)
	sdp.SetDoubleValue(in.dblVal)
	attrs(sdp.Attributes())
	exemplar(sdp.Exemplars())

	hist := next().SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	hdp := hist.DataPoints().AppendEmpty()
	hdp.SetTimestamp(at)
	hdp.SetStartTimestamp(since)
	hdp.SetFlags(flags)
	hdp.SetCount(in.count)
	if in.buckets%2 == 0 {
		hdp.SetSum(in.dblVal)
	}
	if in.buckets%3 == 0 {
		hdp.SetMin(in.dblVal)
		hdp.SetMax(in.dblVal)
	}
	hdp.ExplicitBounds().FromRaw(bounds)
	hdp.BucketCounts().FromRaw(append(slices.Clone(counts), in.count))
	attrs(hdp.Attributes())
	exemplar(hdp.Exemplars())

	expo := next().SetEmptyExponentialHistogram()
	expo.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	edp := expo.DataPoints().AppendEmpty()
	edp.SetTimestamp(at)
	edp.SetStartTimestamp(since)
	edp.SetFlags(flags)
	edp.SetCount(in.count)
	edp.SetSum(in.dblVal)
	edp.SetScale(in.scale)
	edp.SetZeroCount(uint64(in.buckets))
	edp.SetZeroThreshold(in.dblVal)
	edp.Positive().SetOffset(in.offset)
	edp.Positive().BucketCounts().FromRaw(counts)
	edp.Negative().SetOffset(-in.offset)
	edp.Negative().BucketCounts().FromRaw(counts[:n/2])
	attrs(edp.Attributes())
	exemplar(edp.Exemplars())

	summary := next().SetEmptySummary()
	qdp := summary.DataPoints().AppendEmpty()
	qdp.SetTimestamp(at)
	qdp.SetStartTimestamp(since)
	qdp.SetFlags(flags)
	qdp.SetCount(in.count)
	qdp.SetSum(in.dblVal)
	qv := qdp.QuantileValues().AppendEmpty()
	qv.SetQuantile(in.dblVal)
	qv.SetValue(in.dblVal)
	attrs(qdp.Attributes())

	return md
}

// requireMetricRows checks what every MetricRows result must satisfy whatever
// the input: each data point becomes a row or a rejection, rejection indices
// are in range and in walk order, and each row has a well-formed fingerprint,
// a positive timestamp and only valid UTF-8 text.
func requireMetricRows(t *testing.T, label string, points int, rows []Row, rejects []Rejection) {
	t.Helper()
	if len(rows)+len(rejects) != points {
		t.Fatalf("%s: conservation violated: rows=%d + rejects=%d != %d data points", label, len(rows), len(rejects), points)
	}
	prev := -1
	for _, rj := range rejects {
		if rj.Index <= prev || rj.Index >= points {
			t.Fatalf("%s: rejection index %d is out of order or range (previous %d, %d data points)", label, rj.Index, prev, points)
		}
		prev = rj.Index
	}
	for i, r := range rows {
		fp, ok := r["point_fingerprint"].(string)
		if !ok || len(fp) != 68 || !strings.HasPrefix(fp, schema.FingerprintPrefix) {
			t.Fatalf("%s: row %d point_fingerprint = %v, want a 68-char %q digest", label, i, r["point_fingerprint"], schema.FingerprintPrefix)
		}
		if ns, _ := r["time_unix_nano"].(int64); ns <= 0 {
			t.Fatalf("%s: row %d time_unix_nano = %v, want a positive value", label, i, r["time_unix_nano"])
		}
		requireValidUTF8(t, label, r)
	}
}

// requireValidUTF8 walks a row, including nested and repeated structs, and
// fails on any string column that is not valid UTF-8.
func requireValidUTF8(t *testing.T, path string, v any) {
	t.Helper()
	switch x := v.(type) {
	case string:
		if !utf8.ValidString(x) {
			t.Fatalf("%s is not valid UTF-8", path)
		}
	case Row:
		for col, e := range x {
			requireValidUTF8(t, path+"."+col, e)
		}
	case []Row:
		for _, e := range x {
			requireValidUTF8(t, path+"[]", e)
		}
	}
}

func metricFingerprints(rows []Row) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i], _ = r["point_fingerprint"].(string)
	}
	return out
}

func FuzzMetricRows(f *testing.F) {
	f.Add("gen_ai.client.token.usage", "{token}", "gen_ai.token.type", "input", int64(120), 0.5, uint64(3), int64(1700000000000000000), int64(1699999990000000000), uint8(0), uint8(4), int32(2), int32(-1))
	f.Add("", "", "", "", int64(-10), math.NaN(), uint64(0), int64(0), int64(0), uint8(1), uint8(0), int32(0), int32(0))
	f.Add("m\x00\xff", "\xffu", "k\xff", "secret-token 123-45-6789", int64(math.MaxInt64), math.Inf(1), uint64(math.MaxUint64), int64(1700000005000000000), int64(-1), uint8(255), uint8(255), int32(math.MinInt32), int32(math.MaxInt32))

	f.Fuzz(func(t *testing.T, name, unit, key, val string, intVal int64, dblVal float64, count uint64, ts, start int64, flags, buckets uint8, scale, offset int32) {
		in := metricFuzzInput{
			name: name, unit: unit, key: key, val: val,
			intVal: intVal, dblVal: dblVal, count: count,
			ts: ts, start: start, flags: flags, buckets: buckets,
			scale: scale, offset: offset,
		}
		md := in.build()
		points := md.DataPointCount()
		opts := Options{
			Limits: DefaultLimits(),
			Now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		}

		rows, rejects := MetricRows(md, opts)
		requireMetricRows(t, "plain", points, rows, rejects)

		// A fingerprint is a pure function of the point and its position
		// among identical points, so a second pass reproduces it.
		again, _ := MetricRows(md, opts)
		if !slices.Equal(metricFingerprints(rows), metricFingerprints(again)) {
			t.Fatal("a second MetricRows over the same batch produced different fingerprints")
		}

		// The two identical gauge points lead the batch. Unless both were
		// rejected, their ordinals must keep the fingerprints apart.
		if len(rejects) == 0 || rejects[0].Index >= 2 {
			if fps := metricFingerprints(rows); fps[0] == fps[1] {
				t.Fatalf("identical gauge points share the fingerprint %q", fps[0])
			}
		}

		// Every rejected point has exactly one quarantine row.
		if len(rejects) > 0 {
			entries := make([]QuarantineEntry, len(rejects))
			for i, rj := range rejects {
				entries[i] = QuarantineEntry{Rejection: rj}
			}
			quarantined := MetricQuarantineRows(md, entries, opts)
			if len(quarantined) != len(rejects) {
				t.Fatalf("got %d quarantine rows for %d rejections", len(quarantined), len(rejects))
			}
			for _, q := range quarantined {
				if q["signal"] != schema.SignalMetrics {
					t.Fatalf("quarantine signal = %v, want %q", q["signal"], schema.SignalMetrics)
				}
				requireValidUTF8(t, "quarantine", q)
			}
		}

		// Redaction pass: the blocked keys force the copy-on-write branch on
		// every input, and the caller's batch must come back untouched.
		in.withSecret = true
		secret := in.build()
		before, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(secret)
		if err != nil {
			t.Fatal(err)
		}
		opts.Redactor = fuzzRedactor(t)
		rrows, rrejects := MetricRows(secret, opts)
		requireMetricRows(t, "redacted", secret.DataPointCount(), rrows, rrejects)
		after, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(secret)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) {
			t.Fatal("MetricRows with a Redactor mutated the caller's batch")
		}
	})
}

func BenchmarkMetricRows(b *testing.B) {
	md := benchMetrics()
	opts := Options{
		Limits: DefaultLimits(),
		Now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, rejects := MetricRows(md, opts)
		if len(rows) != 10 || len(rejects) != 0 {
			b.Fatalf("unexpected result: rows=%d rejects=%d", len(rows), len(rejects))
		}
	}
}

// benchMetrics is the ten-point GenAI batch the metric benchmark shares:
// token-usage and operation-duration histograms that carry exemplars, plus one
// point of each other metric type.
func benchMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "bench-agent")
	rm.Resource().Attributes().PutStr("service.version", "1.0.0")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("opentelemetry.instrumentation.genai")

	const base = pcommon.Timestamp(1700000000000000000)
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	spanID := pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	bounds := []float64{1, 4, 16, 64, 256, 1024, 4096, 16384, 65536}
	counts := []uint64{0, 1, 3, 9, 20, 11, 4, 2, 1, 0}
	tokenTypes := []string{"input", "output"}
	modelAttrs := func(m pcommon.Map, i int) {
		m.PutStr("gen_ai.operation.name", "chat")
		m.PutStr("gen_ai.request.model", "gemini-2.5-pro")
		m.PutStr("gen_ai.token.type", tokenTypes[i%2])
	}

	histogram := func(name, unit string, points int) {
		m := sm.Metrics().AppendEmpty()
		m.SetName(name)
		m.SetUnit(unit)
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for i := range points {
			dp := h.DataPoints().AppendEmpty()
			dp.SetStartTimestamp(base)
			dp.SetTimestamp(base + pcommon.Timestamp(i+1)*1_000_000_000)
			dp.SetCount(51)
			dp.SetSum(4217)
			dp.SetMin(1)
			dp.SetMax(40000)
			dp.ExplicitBounds().FromRaw(bounds)
			dp.BucketCounts().FromRaw(counts)
			modelAttrs(dp.Attributes(), i)
			ex := dp.Exemplars().AppendEmpty()
			ex.SetTimestamp(dp.Timestamp())
			ex.SetDoubleValue(512)
			ex.SetTraceID(traceID)
			ex.SetSpanID(spanID)
		}
	}
	histogram("gen_ai.client.token.usage", "{token}", 4)
	histogram("gen_ai.client.operation.duration", "s", 2)

	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("gen_ai.client.active_sessions")
	gdp := gauge.SetEmptyGauge().DataPoints().AppendEmpty()
	gdp.SetTimestamp(base + 1_000_000_000)
	gdp.SetIntValue(7)
	modelAttrs(gdp.Attributes(), 0)

	total := sm.Metrics().AppendEmpty()
	total.SetName("gen_ai.client.requests")
	sum := total.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sdp := sum.DataPoints().AppendEmpty()
	sdp.SetStartTimestamp(base)
	sdp.SetTimestamp(base + 1_000_000_000)
	sdp.SetIntValue(42)
	modelAttrs(sdp.Attributes(), 0)

	expo := sm.Metrics().AppendEmpty()
	expo.SetName("gen_ai.client.latency.drift")
	eh := expo.SetEmptyExponentialHistogram()
	eh.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	edp := eh.DataPoints().AppendEmpty()
	edp.SetStartTimestamp(base)
	edp.SetTimestamp(base + 1_000_000_000)
	edp.SetCount(6)
	edp.SetSum(12.5)
	edp.SetScale(2)
	edp.SetZeroCount(1)
	edp.Positive().SetOffset(-2)
	edp.Positive().BucketCounts().FromRaw([]uint64{1, 2, 1})
	edp.Negative().SetOffset(1)
	edp.Negative().BucketCounts().FromRaw([]uint64{1})
	modelAttrs(edp.Attributes(), 0)

	summary := sm.Metrics().AppendEmpty()
	summary.SetName("gen_ai.client.ttft")
	qdp := summary.SetEmptySummary().DataPoints().AppendEmpty()
	qdp.SetStartTimestamp(base)
	qdp.SetTimestamp(base + 1_000_000_000)
	qdp.SetCount(9)
	qdp.SetSum(18.5)
	for _, q := range []struct{ quantile, value float64 }{{0.5, 1}, {0.99, 2.25}} {
		qv := qdp.QuantileValues().AppendEmpty()
		qv.SetQuantile(q.quantile)
		qv.SetValue(q.value)
	}
	modelAttrs(qdp.Attributes(), 0)

	return md
}
