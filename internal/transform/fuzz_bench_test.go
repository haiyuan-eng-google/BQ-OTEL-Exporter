// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"bytes"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/redact"
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
