// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Two resources with two spans each, so the flattened index order and the
// resource grouping can both be checked.
func multiResourceTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	for r := 0; r < 2; r++ {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", []string{"a", "b"}[r])
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName([]string{"scope-a", "scope-b"}[r])
		for s := 0; s < 2; s++ {
			ss.Spans().AppendEmpty().SetName([]string{"s0", "s1", "s2", "s3"}[r*2+s])
		}
	}
	return td
}

func TestSelectSpans(t *testing.T) {
	td := multiResourceTraces()

	got := SelectSpans(td, []int{1, 2})
	if got.SpanCount() != 2 {
		t.Fatalf("selected %d spans, want 2", got.SpanCount())
	}

	var names []string
	for _, rs := range got.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, s := range ss.Spans().All() {
				names = append(names, s.Name())
			}
		}
	}
	if len(names) != 2 || names[0] != "s1" || names[1] != "s2" {
		t.Fatalf("selected %v, want [s1 s2]", names)
	}
}

// Resource and scope must travel with the span. Flattening them would change
// the resource attributes attached to a retried span, which would change its
// row and its fingerprint.
func TestSelectSpansPreservesResourceGrouping(t *testing.T) {
	got := SelectSpans(multiResourceTraces(), []int{1, 2})
	if got.ResourceSpans().Len() != 2 {
		t.Fatalf("got %d resource groups, want 2", got.ResourceSpans().Len())
	}
	first, _ := got.ResourceSpans().At(0).Resource().Attributes().Get("service.name")
	second, _ := got.ResourceSpans().At(1).Resource().Attributes().Get("service.name")
	if first.Str() != "a" || second.Str() != "b" {
		t.Fatalf("resource attributes not preserved: %s, %s", first.Str(), second.Str())
	}
	if got.ResourceSpans().At(0).ScopeSpans().At(0).Scope().Name() != "scope-a" {
		t.Error("scope not preserved")
	}
}

func TestSelectSpansEmpty(t *testing.T) {
	if got := SelectSpans(multiResourceTraces(), nil); got.SpanCount() != 0 {
		t.Fatalf("selecting nothing produced %d spans", got.SpanCount())
	}
}

func TestSelectSpansAll(t *testing.T) {
	td := multiResourceTraces()
	if got := SelectSpans(td, []int{0, 1, 2, 3}); got.SpanCount() != td.SpanCount() {
		t.Fatalf("selecting everything produced %d of %d spans", got.SpanCount(), td.SpanCount())
	}
}

// The indices SelectSpans takes must mean the same thing as the indices
// SpanRows reports, or a retry payload would contain the wrong records.
func TestSelectSpansAgreesWithRowOrder(t *testing.T) {
	td := multiResourceTraces()
	rows, rejects := SpanRows(td, Options{Limits: DefaultLimits()})
	if len(rejects) != 0 || len(rows) != 4 {
		t.Fatalf("setup: rows=%d rejects=%d", len(rows), len(rejects))
	}

	// Row 2 should be span "s2"; selecting index 2 must yield the same span.
	if rows[2]["name"] != "s2" {
		t.Fatalf("row 2 is %v, want s2", rows[2]["name"])
	}
	got := SelectSpans(td, []int{2})
	name := got.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name()
	if name != "s2" {
		t.Fatalf("SelectSpans(2) gave %q, want s2", name)
	}
}

func TestSelectLogs(t *testing.T) {
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for _, body := range []string{"l0", "l1", "l2"} {
		sl.LogRecords().AppendEmpty().Body().SetStr(body)
	}

	got := SelectLogs(ld, []int{0, 2})
	if got.LogRecordCount() != 2 {
		t.Fatalf("selected %d records, want 2", got.LogRecordCount())
	}
	recs := got.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if recs.At(0).Body().Str() != "l0" || recs.At(1).Body().Str() != "l2" {
		t.Fatalf("selected the wrong records: %q, %q",
			recs.At(0).Body().Str(), recs.At(1).Body().Str())
	}
}
