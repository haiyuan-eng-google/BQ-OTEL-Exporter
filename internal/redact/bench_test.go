// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func benchBatch() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "bench-agent")
	rs.Resource().Attributes().PutStr("service.version", "1.0.0")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("opentelemetry.instrumentation.genai")
	for i := 0; i < 10; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID([16]byte{byte(i + 1)}))
		sp.SetSpanID(pcommon.SpanID([8]byte{byte(i + 1)}))
		sp.SetName("invoke_agent")
		sp.Attributes().PutStr("gen_ai.system", "gcp.vertex.agent")
		sp.Attributes().PutStr("gen_ai.operation.name", "invoke_agent")
		sp.Attributes().PutStr("gen_ai.request.model", "gemini-2.5-pro")
		sp.Attributes().PutInt("gen_ai.usage.input_tokens", 512)
		sp.Attributes().PutInt("gen_ai.usage.output_tokens", 256)
		sp.Attributes().PutStr("gen_ai.conversation.id", "conv-bench-1")
	}
	return td
}

// BenchmarkTracesNoMatch isolates the dry-run scan that every batch pays
// when redaction is enabled, split by what the scan has to do.
func BenchmarkTracesNoMatch(b *testing.B) {
	td := benchBatch()
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"keys-only", Config{BlockedKeys: DefaultBlockedKeys()}},
		{"patterns-only", Config{BlockedValuePatterns: []string{`(?i)secret[a-z0-9_-]*`, `\d{3}-\d{2}-\d{4}`}}},
		{"keys+patterns", Config{BlockedKeys: DefaultBlockedKeys(), BlockedValuePatterns: []string{`(?i)secret[a-z0-9_-]*`, `\d{3}-\d{2}-\d{4}`}}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			r, err := New(tc.cfg)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if out := r.Traces(td); out != td {
					b.Fatal("unexpected clone")
				}
			}
		})
	}
}
