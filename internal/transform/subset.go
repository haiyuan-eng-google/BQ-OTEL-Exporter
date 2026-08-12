// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// SelectSpans rebuilds a traces payload containing only the spans at the given
// flattened indices.
//
// The indices match the walk order of SpanRows, so a caller can turn "these
// rows still need delivering" into a payload to hand back to exporterhelper.
// Resource and scope grouping is preserved, since dropping it would change the
// resource attributes attached to a retried span.
func SelectSpans(td ptrace.Traces, indices []int) ptrace.Traces {
	keep := indexSet(indices)
	out := ptrace.NewTraces()
	idx := -1

	for _, rs := range td.ResourceSpans().All() {
		var destRS ptrace.ResourceSpans
		rsStarted := false

		for _, ss := range rs.ScopeSpans().All() {
			var destSS ptrace.ScopeSpans
			ssStarted := false

			for _, span := range ss.Spans().All() {
				idx++
				if !keep[idx] {
					continue
				}
				if !rsStarted {
					destRS = out.ResourceSpans().AppendEmpty()
					rs.Resource().CopyTo(destRS.Resource())
					destRS.SetSchemaUrl(rs.SchemaUrl())
					rsStarted = true
				}
				if !ssStarted {
					destSS = destRS.ScopeSpans().AppendEmpty()
					ss.Scope().CopyTo(destSS.Scope())
					destSS.SetSchemaUrl(ss.SchemaUrl())
					ssStarted = true
				}
				span.CopyTo(destSS.Spans().AppendEmpty())
			}
		}
	}
	return out
}

// SelectLogs rebuilds a logs payload containing only the records at the given
// flattened indices.
func SelectLogs(ld plog.Logs, indices []int) plog.Logs {
	keep := indexSet(indices)
	out := plog.NewLogs()
	idx := -1

	for _, rl := range ld.ResourceLogs().All() {
		var destRL plog.ResourceLogs
		rlStarted := false

		for _, sl := range rl.ScopeLogs().All() {
			var destSL plog.ScopeLogs
			slStarted := false

			for _, lr := range sl.LogRecords().All() {
				idx++
				if !keep[idx] {
					continue
				}
				if !rlStarted {
					destRL = out.ResourceLogs().AppendEmpty()
					rl.Resource().CopyTo(destRL.Resource())
					destRL.SetSchemaUrl(rl.SchemaUrl())
					rlStarted = true
				}
				if !slStarted {
					destSL = destRL.ScopeLogs().AppendEmpty()
					sl.Scope().CopyTo(destSL.Scope())
					destSL.SetSchemaUrl(sl.SchemaUrl())
					slStarted = true
				}
				lr.CopyTo(destSL.LogRecords().AppendEmpty())
			}
		}
	}
	return out
}

func indexSet(indices []int) map[int]bool {
	s := make(map[int]bool, len(indices))
	for _, i := range indices {
		s[i] = true
	}
	return s
}
