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
		MaxAttributeCount: 2,
		MaxEventCount:     2,
		MaxLinkCount:      2,
		MaxNestingDepth:   3,
		MaxCollectionSize: 2,
		MaxValueBytes:     16,
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
}

// A breach drops only the offending record, never the batch around it.
func TestStructuralLimitsDropOnlyTheOffender(t *testing.T) {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()

	good := ss.Spans().AppendEmpty()
	good.SetName("fine")

	bad := ss.Spans().AppendEmpty()
	bad.SetName("too many attributes")
	for i := 0; i < 10; i++ {
		bad.Attributes().PutStr(string(rune('a'+i)), "v")
	}

	alsoGood := ss.Spans().AppendEmpty()
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
			span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
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
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.Attributes().PutStr("customer.email.address", "this string is definitely longer than sixteen bytes")

	_, rejects := SpanRows(td, Options{Limits: tightLimits()})
	if len(rejects) != 1 {
		t.Fatalf("expected one rejection, got %d", len(rejects))
	}
	if rejects[0].FieldPath != "span_attributes" {
		t.Fatalf("field path = %q, want the bag name only", rejects[0].FieldPath)
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
