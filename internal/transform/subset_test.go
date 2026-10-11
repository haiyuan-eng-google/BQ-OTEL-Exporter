// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"slices"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
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
			validSpan(ss.Spans().AppendEmpty()).SetName([]string{"s0", "s1", "s2", "s3"}[r*2+s])
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

// ordinalBatch builds six log records under two resources and three scopes.
// Records are numbered by their flattened position, and groups names, for each
// of them, the records it is identical to: the same body under the same
// resource and scope.
//
//	0  A  r0 / s0
//	1  B  r0 / s0
//	2  A  r0 / s0  identical to 0
//	3  A  r0 / s1  same body as 0 under another scope: a different record
//	4  A  r0 / s1  identical to 3
//	5  B  r1 / s0  same body as 1 under another resource: a different record
func ordinalBatch() (plog.Logs, []string) {
	ld := plog.NewLogs()
	var groups []string
	add := func(sl plog.ScopeLogs, group, body string) {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		lr.Body().SetStr(body)
		groups = append(groups, group)
	}

	r0 := ld.ResourceLogs().AppendEmpty()
	r0.Resource().Attributes().PutStr("service.name", "r0")
	s0 := r0.ScopeLogs().AppendEmpty()
	s0.Scope().SetName("s0")
	add(s0, "r0/s0/A", "A")
	add(s0, "r0/s0/B", "B")
	add(s0, "r0/s0/A", "A")
	s1 := r0.ScopeLogs().AppendEmpty()
	s1.Scope().SetName("s1")
	add(s1, "r0/s1/A", "A")
	add(s1, "r0/s1/A", "A")

	r1 := ld.ResourceLogs().AppendEmpty()
	r1.Resource().Attributes().PutStr("service.name", "r1")
	s2 := r1.ScopeLogs().AppendEmpty()
	s2.Scope().SetName("s0")
	add(s2, "r1/s0/B", "B")
	return ld, groups
}

// A retry payload is fingerprinted from scratch, so an acknowledged record
// that is identical to a retried one has to travel with it. Records that are
// not identical to a retried one, and records that were never sent, have no
// reason to.
func TestPreserveLogOrdinalPeers(t *testing.T) {
	ld, _ := ordinalBatch()
	all := []int{0, 1, 2, 3, 4, 5}

	tests := []struct {
		name       string
		candidates []int
		retryable  []int
		want       []int
	}{
		{"nothing to retry", all, nil, nil},
		{"everything retried", all, all, all},
		{"a distinct acknowledged record stays out", all, []int{1}, []int{1}},
		{"an identical acknowledged record before a retried one travels with it", all, []int{2}, []int{0, 2}},
		{"an identical acknowledged record after a retried one travels with it too", all, []int{0}, []int{0, 2}},
		{"the same body under another scope is not an identical record", all, []int{3}, []int{3, 4}},
		{"the same body under another resource is not an identical record", all, []int{5}, []int{5}},
		{"each group is widened on its own", all, []int{2, 4}, []int{0, 2, 3, 4}},
		{"a record that was not sent never comes back", []int{1, 2, 3}, []int{2}, []int{2}},
		{"candidates in any order give ascending indices", []int{5, 4, 3, 2, 1, 0}, []int{4}, []int{3, 4}},
		{"a retried record missing from the candidates is kept", []int{0, 2}, []int{1}, []int{1}},
		{"a retried peer missing from candidates widens candidates in same group", []int{2}, []int{0}, []int{0, 2}},
		{"offload failed retryable index alongside accepted peer candidate", []int{0, 2}, []int{0}, []int{0, 2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := PreserveLogOrdinalPeers(ld, tc.candidates, tc.retryable)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("PreserveLogOrdinalPeers(candidates %v, retryable %v) = %v, want %v",
					tc.candidates, tc.retryable, got, tc.want)
			}
		})
	}
}

// Whichever records are retried, the payload built from the result must give
// every record it carries the fingerprint, and so the insert_id, it had in the
// original batch. Checked for every retried subset of the batch short of all of
// it, against an oracle that knows which records are identical by construction
// rather than by fingerprinting.
func TestPreserveLogOrdinalPeersReproducesFingerprints(t *testing.T) {
	ld, groups := ordinalBatch()
	original, rejects := LogRows(ld, testOpts())
	if len(rejects) != 0 || len(original) != len(groups) {
		t.Fatalf("setup: %d rows and %d rejections for %d records", len(original), len(rejects), len(groups))
	}
	seen := make(map[any]bool, len(original))
	for _, row := range original {
		if seen[row["insert_id"]] {
			t.Fatalf("setup: insert_id %v is shared by two records of the batch", row["insert_id"])
		}
		seen[row["insert_id"]] = true
	}
	all := make([]int, len(groups))
	for i := range all {
		all[i] = i
	}

	full := 1<<len(all) - 1
	for mask := 1; mask < full; mask++ {
		var retry []int
		retriedGroups := make(map[string]bool)
		for i := range all {
			if mask&(1<<i) != 0 {
				retry = append(retry, i)
				retriedGroups[groups[i]] = true
			}
		}
		// The retried records, and every record identical to one of them.
		var want []int
		for i := range all {
			if retriedGroups[groups[i]] {
				want = append(want, i)
			}
		}

		got := PreserveLogOrdinalPeers(ld, all, retry)
		if !slices.Equal(got, want) {
			t.Fatalf("retrying %v: got %v, want %v", retry, got, want)
		}

		replay, rejects := LogRows(SelectLogs(ld, got), testOpts())
		if len(rejects) != 0 || len(replay) != len(got) {
			t.Fatalf("retrying %v: payload gave %d rows and %d rejections, want %d rows", retry, len(replay), len(rejects), len(got))
		}
		for pos, idx := range got {
			for _, col := range []string{"record_fingerprint", "insert_id"} {
				if replay[pos][col] != original[idx][col] {
					t.Errorf("retrying %v: record %d has %s %v in the payload, want its original %v",
						retry, idx, col, replay[pos][col], original[idx][col])
				}
			}
		}
	}
}
