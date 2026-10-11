// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func tightLimits() Limits {
	return Limits{
		MaxAttributeCount:    2,
		MaxEventCount:        2,
		MaxLinkCount:         2,
		MaxNestingDepth:      3,
		MaxCollectionSize:    2,
		MaxAttributeKeyBytes: 32,
		MaxValueBytes:        16,
	}
}

func TestLimitsValidate(t *testing.T) {
	if err := DefaultLimits().Validate(); err != nil {
		t.Fatalf("defaults should validate: %v", err)
	}
	l := DefaultLimits()
	l.MaxValueBytes = 0
	if err := l.Validate(); err == nil {
		t.Fatal("a zero limit can never accept a record and must be rejected")
	}
	lKey := DefaultLimits()
	lKey.MaxAttributeKeyBytes = 0
	if err := lKey.Validate(); err == nil {
		t.Fatal("a zero max_attribute_key_bytes must be rejected")
	}
}

// A breach drops only the offending record, never the batch around it.
func TestStructuralLimitsDropOnlyTheOffender(t *testing.T) {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()

	good := validSpan(ss.Spans().AppendEmpty())
	good.SetName("fine")

	bad := validSpan(ss.Spans().AppendEmpty())
	bad.SetName("too many attributes")
	for i := 0; i < 10; i++ {
		bad.Attributes().PutStr(string(rune('a'+i)), "v")
	}

	alsoGood := validSpan(ss.Spans().AppendEmpty())
	alsoGood.SetName("also fine")

	rows, rejects := SpanRows(td, Options{Limits: tightLimits()})
	if len(rows) != 2 {
		t.Fatalf("kept %d rows, want the 2 valid ones", len(rows))
	}
	if len(rejects) != 1 {
		t.Fatalf("got %d rejections, want 1", len(rejects))
	}
	if rejects[0].Reason != ReasonAttributeCount {
		t.Errorf("reason = %s, want %s", rejects[0].Reason, ReasonAttributeCount)
	}
	// The index must locate the record in the caller's batch.
	if rejects[0].Index != 1 {
		t.Errorf("index = %d, want 1", rejects[0].Index)
	}
}

func TestEachLimitIsEnforced(t *testing.T) {
	tests := map[string]struct {
		build func(ptrace.Span)
		want  Reason
	}{
		"attribute count": {func(s ptrace.Span) {
			for i := 0; i < 5; i++ {
				s.Attributes().PutStr(string(rune('a'+i)), "v")
			}
		}, ReasonAttributeCount},

		"attribute key size": {func(s ptrace.Span) {
			s.Attributes().PutStr("this.attribute.key.is.longer.than.thirty.two.bytes", "v")
		}, ReasonAttributeKeySize},

		"nested map key size": {func(s ptrace.Span) {
			m := s.Attributes().PutEmptyMap("nested")
			m.PutStr("this.nested.map.key.is.longer.than.thirty.two.bytes", "v")
		}, ReasonAttributeKeySize},

		"event count": {func(s ptrace.Span) {
			for i := 0; i < 5; i++ {
				s.Events().AppendEmpty()
			}
		}, ReasonEventCount},

		"link count": {func(s ptrace.Span) {
			for i := 0; i < 5; i++ {
				s.Links().AppendEmpty()
			}
		}, ReasonLinkCount},

		"value size": {func(s ptrace.Span) {
			s.Attributes().PutStr("k", "this string is definitely longer than sixteen bytes")
		}, ReasonValueSize},

		"collection size": {func(s ptrace.Span) {
			sl := s.Attributes().PutEmptySlice("k")
			for i := 0; i < 5; i++ {
				sl.AppendEmpty().SetInt(int64(i))
			}
		}, ReasonCollectionSize},

		"nesting depth": {func(s ptrace.Span) {
			m := s.Attributes().PutEmptyMap("k")
			for i := 0; i < 6; i++ {
				m = m.PutEmptyMap("deeper")
			}
		}, ReasonNestingDepth},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			td := ptrace.NewTraces()
			span := validSpan(td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty())
			tc.build(span)

			rows, rejects := SpanRows(td, Options{Limits: tightLimits()})
			if len(rows) != 0 {
				t.Fatalf("expected the record to be dropped, kept %d rows", len(rows))
			}
			if len(rejects) != 1 || rejects[0].Reason != tc.want {
				t.Fatalf("got %+v, want reason %s", rejects, tc.want)
			}
		})
	}
}

// The field path names the bag, never the attribute key inside it: keys are
// telemetry content and diagnostics end up in logs.
func TestViolationPathCarriesNoAttributeKey(t *testing.T) {
	td := ptrace.NewTraces()
	span := validSpan(td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty())
	span.Attributes().PutStr("customer.email.address", "this string is definitely longer than sixteen bytes")

	_, rejects := SpanRows(td, Options{Limits: tightLimits()})
	if len(rejects) != 1 {
		t.Fatalf("expected one rejection, got %d", len(rejects))
	}
	if rejects[0].FieldPath != "attributes" {
		t.Fatalf("field path = %q, want the bag name only", rejects[0].FieldPath)
	}
}

// Each bag's path is the column, or nested field, the bag is written to, so
// an operator can find it in the table.
func TestViolationPathNamesTheBagColumn(t *testing.T) {
	const long = "this string is definitely longer than sixteen bytes"
	for want, build := range map[string]func(ptrace.ResourceSpans, ptrace.Span){
		"resource.attributes": func(rs ptrace.ResourceSpans, _ ptrace.Span) {
			rs.Resource().Attributes().PutStr("k", long)
		},
		"instrumentation_scope.attributes": func(rs ptrace.ResourceSpans, _ ptrace.Span) {
			rs.ScopeSpans().At(0).Scope().Attributes().PutStr("k", long)
		},
		"attributes": func(_ ptrace.ResourceSpans, s ptrace.Span) {
			s.Attributes().PutStr("k", long)
		},
		"events.attributes": func(_ ptrace.ResourceSpans, s ptrace.Span) {
			s.Events().AppendEmpty().Attributes().PutStr("k", long)
		},
		"links.attributes": func(_ ptrace.ResourceSpans, s ptrace.Span) {
			s.Links().AppendEmpty().Attributes().PutStr("k", long)
		},
	} {
		t.Run(want, func(t *testing.T) {
			td := ptrace.NewTraces()
			rs := td.ResourceSpans().AppendEmpty()
			span := validSpan(rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty())
			build(rs, span)

			_, rejects := SpanRows(td, Options{Limits: tightLimits()})
			if len(rejects) != 1 || rejects[0].FieldPath != want {
				t.Fatalf("rejections = %+v, want field path %q", rejects, want)
			}
		})
	}
}

// A contract violation breaches no configured limit, so its message must not
// claim one.
func TestViolationError(t *testing.T) {
	for _, tc := range []struct {
		v    Violation
		want string
	}{
		{Violation{ReasonAttributeCount, "attributes", 2, 5}, "attribute_count: attributes exceeds limit (5 > 2)"},
		{Violation{Reason: ReasonInvalidID, FieldPath: "trace_id"}, "invalid_id: trace_id"},
	} {
		if got := tc.v.Error(); got != tc.want {
			t.Errorf("Error() = %q, want %q", got, tc.want)
		}
	}
}

func TestLogBodyLimit(t *testing.T) {
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.Timestamp(1))
	lr.Body().SetStr("this body is definitely longer than sixteen bytes")

	rows, rejects := LogRows(ld, Options{Limits: tightLimits()})
	if len(rows) != 0 || len(rejects) != 1 {
		t.Fatalf("rows=%d rejects=%+v, want the record dropped", len(rows), rejects)
	}
	if rejects[0].FieldPath != "body" {
		t.Errorf("field path = %q, want body", rejects[0].FieldPath)
	}
}

// A log record's bags and body report the column, or nested field, they are
// written to, as span bags do.
func TestLogViolationPathNamesTheColumn(t *testing.T) {
	const long = "this string is definitely longer than sixteen bytes"
	for want, build := range map[string]func(plog.ResourceLogs, plog.LogRecord){
		"resource.attributes": func(rl plog.ResourceLogs, _ plog.LogRecord) {
			rl.Resource().Attributes().PutStr("k", long)
		},
		"instrumentation_scope.attributes": func(rl plog.ResourceLogs, _ plog.LogRecord) {
			rl.ScopeLogs().At(0).Scope().Attributes().PutStr("k", long)
		},
		"attributes": func(_ plog.ResourceLogs, lr plog.LogRecord) {
			lr.Attributes().PutStr("k", long)
		},
		"body": func(_ plog.ResourceLogs, lr plog.LogRecord) {
			lr.Body().SetStr(long)
		},
	} {
		t.Run(want, func(t *testing.T) {
			ld := plog.NewLogs()
			rl := ld.ResourceLogs().AppendEmpty()
			lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			build(rl, lr)

			_, rejects := LogRows(ld, Options{Limits: tightLimits()})
			if len(rejects) != 1 || rejects[0].FieldPath != want {
				t.Fatalf("rejections = %+v, want field path %q", rejects, want)
			}
		})
	}
}
