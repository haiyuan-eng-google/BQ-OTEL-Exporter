// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

var trajectoryBaseCols = []string{
	"start_time", "end_time", "duration_nano", "duration_ms",
	"trace_id", "span_id", "parent_span_id", "service_name",
	"span_name", "span_kind", "status_code", "status_message",
	"gen_ai_operation_name", "gen_ai_system", "gen_ai_provider_name",
	"gen_ai_request_model", "gen_ai_response_model",
	"gen_ai_agent_id", "gen_ai_agent_name", "gen_ai_agent_version",
	"gen_ai_main_agent_id", "gen_ai_main_agent_name",
	"gen_ai_tool_name", "gen_ai_tool_call_id",
	"gen_ai_usage_input_tokens", "gen_ai_usage_output_tokens", "gen_ai_usage_reasoning_tokens",
	"adk_session_id", "adk_invocation_id", "adk_user_id",
	"error_type",
}

const (
	trajectoryMaxDepth       = 16
	trajectoryLookbackMicros = int64(24 * time.Hour / time.Microsecond)
	trajectorySkewMicros     = int64(time.Hour / time.Microsecond)
)

type trajectoryWorkItem struct {
	row     map[string]any
	depth   int64
	visited []string
}

func equalNullableString(a, b any) bool {
	sa, oka := a.(string)
	sb, okb := b.(string)
	if !oka && !okb {
		return true
	}
	return oka && okb && sa == sb
}

func isAgentOperation(op string) bool {
	return op == "invoke_agent" || op == "create_agent"
}

func copyTrajectoryBaseCols(src map[string]any) map[string]any {
	dst := make(map[string]any, len(trajectoryBaseCols)+9)
	for _, col := range trajectoryBaseCols {
		if v, ok := src[col]; ok && v != nil {
			dst[col] = v
		}
	}
	return dst
}

// declaredAgentName is declared_agent_name in the spans CTE of
// v_gen_ai_agent_trajectory: the agent name a span's own attributes declare,
// gen_ai.agent.name and then gcp.vertex.agent.name, without the service_name
// fallback that gen_ai_agent_name has.
func declaredAgentName(t testing.TB, s map[string]any) (string, bool) {
	t.Helper()
	a := parseAttrs(t, s)
	if name, ok := a.jsonValue("gen_ai.agent.name"); ok {
		return name, true
	}
	return a.jsonValue("gcp.vertex.agent.name")
}

// agentPathName is agent_path_name in the spans CTE: the declared agent name,
// or gen_ai_agent_name for an invoke_agent or create_agent span that declares
// none.
func agentPathName(t testing.TB, s map[string]any) (string, bool) {
	t.Helper()
	if name, ok := declaredAgentName(t, s); ok {
		return name, true
	}
	if op, _ := colString(s, "gen_ai_operation_name"); isAgentOperation(op) {
		return colString(s, "gen_ai_agent_name")
	}
	return "", false
}

// reportsTokens matches the token-bearing test of v_gen_ai_agent_trajectory:
// an input, output, or reasoning token count that is not NULL, an explicit 0
// included.
func reportsTokens(s map[string]any) bool {
	_, hasIn := colInt(s, "gen_ai_usage_input_tokens")
	_, hasOut := colInt(s, "gen_ai_usage_output_tokens")
	_, hasReasoning := colInt(s, "gen_ai_usage_reasoning_tokens")
	return hasIn || hasOut || hasReasoning
}

// tokenCoverage holds the LOGICAL_OR per-family descendant token coverage for
// a parent span in token_parent_coverage.
type tokenCoverage struct {
	hasInput     bool
	hasOutput    bool
	hasReasoning bool
}

func recordTokenCoverage(cov map[string]tokenCoverage, key string, child map[string]any) {
	_, hasIn := colInt(child, "gen_ai_usage_input_tokens")
	_, hasOut := colInt(child, "gen_ai_usage_output_tokens")
	_, hasReas := colInt(child, "gen_ai_usage_reasoning_tokens")
	if !hasIn && !hasOut && !hasReas {
		return
	}
	cur := cov[key]
	cur.hasInput = cur.hasInput || hasIn
	cur.hasOutput = cur.hasOutput || hasOut
	cur.hasReasoning = cur.hasReasoning || hasReas
	cov[key] = cur
}

// canonicalSpanBetter reports whether row a wins over row b in the spans CTE's
// QUALIFY ROW_NUMBER() OVER (PARTITION BY trace_id, service_name, span_id
// ORDER BY start_time ASC, end_time DESC, span_name ASC, duration_nano DESC).
// In GoogleSQL, ASC orders NULLS FIRST and DESC orders NULLS LAST.
func canonicalSpanBetter(a, b map[string]any) bool {
	aStart, aHasStart := colInt(a, "start_time")
	bStart, bHasStart := colInt(b, "start_time")
	if aHasStart != bHasStart {
		return !aHasStart
	}
	if aStart != bStart {
		return aStart < bStart
	}
	aEnd, aHasEnd := colInt(a, "end_time")
	bEnd, bHasEnd := colInt(b, "end_time")
	if aHasEnd != bHasEnd {
		return aHasEnd
	}
	if aEnd != bEnd {
		return aEnd > bEnd
	}
	aName, aHasName := colString(a, "span_name")
	bName, bHasName := colString(b, "span_name")
	if aHasName != bHasName {
		return !aHasName
	}
	if aName != bName {
		return aName < bName
	}
	aDur, aHasDur := colInt(a, "duration_nano")
	bDur, bHasDur := colInt(b, "duration_nano")
	if aHasDur != bHasDur {
		return aHasDur
	}
	return aDur > bDur
}

func canonicalizeTrajectorySpans(spans []map[string]any) []map[string]any {
	winners := make(map[string]map[string]any, len(spans))
	var order []string
	for _, s := range spans {
		k := groupKey(s["trace_id"], s["service_name"], s["span_id"])
		cur, seen := winners[k]
		if !seen {
			order = append(order, k)
		}
		if !seen || canonicalSpanBetter(s, cur) {
			winners[k] = s
		}
	}
	out := make([]map[string]any, 0, len(order))
	for _, k := range order {
		out = append(out, winners[k])
	}
	return out
}

// trajectorySpans is v_gen_ai_spans as v_gen_ai_agent_trajectory reads it:
// the genAISpansView rows together with the attributes column that the view
// passes through and genAISpansView leaves out, which declared_agent_name is
// read from. Each deduplicated span goes through genAISpansView on its own, so
// the rows and their order are those of genAISpansView over all of them.
func trajectorySpans(t *testing.T, rawSpans []map[string]any) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, s := range spansDedup.apply(rawSpans) {
		for _, row := range genAISpansView(t, []map[string]any{s}) {
			if a, ok := s["attributes"]; ok {
				row["attributes"] = a
			}
			out = append(out, row)
		}
	}
	return out
}

// genAIAgentTrajectoryView evaluates v_gen_ai_agent_trajectory over the rows
// of v_gen_ai_spans, attributes column included.
func genAIAgentTrajectoryView(t testing.TB, spans []map[string]any) []map[string]any {
	t.Helper()
	spans = canonicalizeTrajectorySpans(spans)
	parentIDs := make(map[string]bool)
	tokenParentCoverage := make(map[string]tokenCoverage)
	for _, s := range spans {
		parent, hasParent := colString(s, "parent_span_id")
		if !hasParent || parent == "" {
			continue
		}
		key := groupKey(s["trace_id"], s["service_name"], parent)
		parentIDs[key] = true
	}

	var emitted []map[string]any
	var frontier []trajectoryWorkItem

	// Anchor member: true roots and orphan roots whose parent_span_id is absent
	// within [-24h, +1h] for the same (trace_id, service_name).
	for _, s := range spans {
		parent, hasParent := colString(s, "parent_span_id")
		sStart, _ := colInt(s, "start_time")
		hasMatchingParent := false
		if hasParent && parent != "" {
			for _, p := range spans {
				if p["trace_id"] != s["trace_id"] {
					continue
				}
				pSpanID, _ := colString(p, "span_id")
				if pSpanID != parent {
					continue
				}
				if !equalNullableString(s["service_name"], p["service_name"]) {
					continue
				}
				pStart, _ := colInt(p, "start_time")
				if pStart >= sStart-trajectoryLookbackMicros && pStart <= sStart+trajectorySkewMicros {
					hasMatchingParent = true
					break
				}
			}
		}
		if hasParent && parent != "" && hasMatchingParent {
			continue
		}

		row := copyTrajectoryBaseCols(s)
		spanID, _ := colString(s, "span_id")
		op, _ := colString(s, "gen_ai_operation_name")
		declared, hasDeclared := declaredAgentName(t, s)

		row["depth"] = int64(0)
		row["root_span_id"] = spanID
		if isAgentOperation(op) || hasDeclared {
			row["nearest_agent_span_id"] = spanID
			if hasDeclared {
				row["nearest_agent_name"] = declared
			} else if name, ok := colString(s, "gen_ai_agent_name"); ok {
				row["nearest_agent_name"] = name
			}
		}
		if name, ok := agentPathName(t, s); ok {
			row["agent_path"] = []string{name}
		} else {
			row["agent_path"] = []string{}
		}
		row["is_orphan"] = hasParent && parent != ""

		item := trajectoryWorkItem{
			row:     row,
			depth:   0,
			visited: []string{spanID},
		}
		emitted = append(emitted, row)
		frontier = append(frontier, item)
	}

	// Recursive member: breadth-first traversal up to depth < 16 with cycle guard.
	for len(frontier) > 0 {
		var nextFrontier []trajectoryWorkItem
		for _, tItem := range frontier {
			if tItem.depth >= trajectoryMaxDepth {
				continue
			}
			tRow := tItem.row
			tSpanID, _ := colString(tRow, "span_id")
			tStart, _ := colInt(tRow, "start_time")

			for _, c := range spans {
				if c["trace_id"] != tRow["trace_id"] {
					continue
				}
				cParent, hasParent := colString(c, "parent_span_id")
				if !hasParent || cParent != tSpanID {
					continue
				}
				if !equalNullableString(c["service_name"], tRow["service_name"]) {
					continue
				}
				cStart, _ := colInt(c, "start_time")
				if cStart < tStart-trajectorySkewMicros || cStart > tStart+trajectoryLookbackMicros {
					continue
				}
				cSpanID, _ := colString(c, "span_id")
				if slices.Contains(tItem.visited, cSpanID) {
					continue
				}

				row := copyTrajectoryBaseCols(c)
				cOp, _ := colString(c, "gen_ai_operation_name")
				cDeclared, cHasDeclared := declaredAgentName(t, c)

				row["depth"] = tItem.depth + 1
				row["root_span_id"] = tRow["root_span_id"]

				if isAgentOperation(cOp) || (tRow["nearest_agent_span_id"] == nil && cHasDeclared) {
					row["nearest_agent_span_id"] = cSpanID
					if cHasDeclared {
						row["nearest_agent_name"] = cDeclared
					} else if name, ok := colString(c, "gen_ai_agent_name"); ok {
						row["nearest_agent_name"] = name
					}
				} else {
					for _, col := range []string{"nearest_agent_span_id", "nearest_agent_name"} {
						if v := tRow[col]; v != nil {
							row[col] = v
						}
					}
				}

				prevPath := tRow["agent_path"].([]string)
				nextPath := slices.Clone(prevPath)
				if name, ok := agentPathName(t, c); ok && (len(nextPath) == 0 || nextPath[len(nextPath)-1] != name) {
					nextPath = append(nextPath, name)
				}
				row["agent_path"] = nextPath
				row["is_orphan"] = tRow["is_orphan"]

				nextVisited := append(slices.Clone(tItem.visited), cSpanID)
				item := trajectoryWorkItem{
					row:     row,
					depth:   tItem.depth + 1,
					visited: nextVisited,
				}
				emitted = append(emitted, row)
				nextFrontier = append(nextFrontier, item)
			}
		}
		frontier = nextFrontier
	}

	// emitted_ancestors: walk upward along parent_span_id -> span_id across all
	// emitted rows in trajectory (up to 32 hops, cycle-guarded by path_span_ids).
	type ancestorHop struct {
		descendant map[string]any
		descSpanID string
		descStart  int64
		ancestor   map[string]any
		ancSpanID  string
		ancParent  string
		ancStart   int64
		hop        int
		path       []string
	}
	var allAncestorHops []ancestorHop
	var ancFrontier []ancestorHop
	reachable := make(map[string]bool)

	for _, c := range emitted {
		cParent, hasParent := colString(c, "parent_span_id")
		if !hasParent || cParent == "" {
			continue
		}
		cSpanID, _ := colString(c, "span_id")
		cStart, _ := colInt(c, "start_time")
		for _, p := range emitted {
			if p["trace_id"] != c["trace_id"] || !equalNullableString(p["service_name"], c["service_name"]) {
				continue
			}
			pSpanID, _ := colString(p, "span_id")
			if pSpanID != cParent || pSpanID == cSpanID {
				continue
			}
			pParent, _ := colString(p, "parent_span_id")
			pStart, _ := colInt(p, "start_time")
			h := ancestorHop{
				descendant: c,
				descSpanID: cSpanID,
				descStart:  cStart,
				ancestor:   p,
				ancSpanID:  pSpanID,
				ancParent:  pParent,
				ancStart:   pStart,
				hop:        1,
				path:       []string{cSpanID, pSpanID},
			}
			allAncestorHops = append(allAncestorHops, h)
			ancFrontier = append(ancFrontier, h)
			reachable[groupKey(c["trace_id"], c["service_name"], cSpanID, pSpanID)] = true
		}
	}
	for len(ancFrontier) > 0 {
		var nextAnc []ancestorHop
		for _, ea := range ancFrontier {
			if ea.hop >= 32 || ea.ancParent == "" {
				continue
			}
			for _, p := range emitted {
				if p["trace_id"] != ea.descendant["trace_id"] || !equalNullableString(p["service_name"], ea.descendant["service_name"]) {
					continue
				}
				pSpanID, _ := colString(p, "span_id")
				if pSpanID != ea.ancParent || slices.Contains(ea.path, pSpanID) {
					continue
				}
				pParent, _ := colString(p, "parent_span_id")
				pStart, _ := colInt(p, "start_time")
				h := ancestorHop{
					descendant: ea.descendant,
					descSpanID: ea.descSpanID,
					descStart:  ea.descStart,
					ancestor:   p,
					ancSpanID:  pSpanID,
					ancParent:  pParent,
					ancStart:   pStart,
					hop:        ea.hop + 1,
					path:       append(slices.Clone(ea.path), pSpanID),
				}
				allAncestorHops = append(allAncestorHops, h)
				nextAnc = append(nextAnc, h)
				reachable[groupKey(ea.descendant["trace_id"], ea.descendant["service_name"], ea.descSpanID, pSpanID)] = true
			}
		}
		ancFrontier = nextAnc
	}

	// token_parent_coverage: aggregate per-family descendant token coverage per
	// ancestor, using reverse_path mutual reachability so only true cycle pairs
	// apply the (start_time, span_id) tie-breaker.
	for _, ea := range allAncestorHops {
		if !reportsTokens(ea.descendant) {
			continue
		}
		hasReversePath := reachable[groupKey(ea.descendant["trace_id"], ea.descendant["service_name"], ea.ancSpanID, ea.descSpanID)]
		if hasReversePath && (ea.descStart < ea.ancStart || (ea.descStart == ea.ancStart && ea.descSpanID <= ea.ancSpanID)) {
			continue
		}
		key := groupKey(ea.descendant["trace_id"], ea.descendant["service_name"], ea.ancSpanID)
		recordTokenCoverage(tokenParentCoverage, key, ea.descendant)
	}

	// Outer SELECT: partial-overlap token family suppression, step_index,
	// is_leaf, and is_token_leaf per (trace_id, service_name).
	partitions := make(map[string][]map[string]any)
	var partitionOrder []string
	for _, row := range emitted {
		pk := groupKey(row["trace_id"], row["service_name"])
		if _, ok := partitions[pk]; !ok {
			partitionOrder = append(partitionOrder, pk)
		}
		partitions[pk] = append(partitions[pk], row)
	}

	var out []map[string]any
	for _, pk := range partitionOrder {
		part := partitions[pk]
		sort.SliceStable(part, func(i, j int) bool {
			si, _ := colInt(part[i], "start_time")
			sj, _ := colInt(part[j], "start_time")
			if si != sj {
				return si < sj
			}
			idi, _ := colString(part[i], "span_id")
			idj, _ := colString(part[j], "span_id")
			return idi < idj
		})
		for idx, row := range part {
			row["step_index"] = int64(idx + 1)
			lk := groupKey(row["trace_id"], row["service_name"], row["span_id"])
			row["is_leaf"] = !parentIDs[lk]
			cov := tokenParentCoverage[lk]
			_, hasIn := colInt(row, "gen_ai_usage_input_tokens")
			_, hasOut := colInt(row, "gen_ai_usage_output_tokens")
			_, hasReas := colInt(row, "gen_ai_usage_reasoning_tokens")
			uncovIn := hasIn && !cov.hasInput
			uncovOut := hasOut && !cov.hasOutput
			uncovReas := hasReas && !cov.hasReasoning
			if cov.hasInput && (uncovOut || uncovReas) {
				delete(row, "gen_ai_usage_input_tokens")
			}
			if cov.hasOutput && (uncovIn || uncovReas) {
				delete(row, "gen_ai_usage_output_tokens")
			}
			if cov.hasReasoning && (uncovIn || uncovOut) {
				delete(row, "gen_ai_usage_reasoning_tokens")
			}
			row["is_token_leaf"] = uncovIn || uncovOut || uncovReas
			out = append(out, row)
		}
	}
	return out
}

// genAIAgentTrajectoryTVF evaluates tvf_gen_ai_agent_trajectory(start_ts, end_ts)
// over the rows of v_gen_ai_spans using a 1-to-1 CTE-by-CTE transliteration of
// schema.GenAIAgentTrajectoryTVFQuery:
//   - spans: [-24h, +1h] horizon scan + canonicalization
//   - parent_ids: distinct (trace_id, service_name, parent_span_id)
//   - hop0..hop32: 6-step binary-lifting pointer jumping (up to 64 nodes / 63 hops)
//     carrying per-node in_tree_window flags
//   - rooted_chains: tree_start_idx = MAX(idx) WHERE NOT n.in_tree_window
//   - trajectory: depth <= 16 segment projection + pre-filter step_index
//   - emitted_ancestors: 1..32-hop simple paths in t.chain to emitted ancestors
//   - token_parent_coverage: mutual-reachability reverse_path join
//   - outer SELECT: WHERE t.start_time >= startTS AND t.start_time < endTS
func genAIAgentTrajectoryTVF(t testing.TB, rawViewSpans []map[string]any, startTS, endTS int64) []map[string]any {
	t.Helper()
	var windowSpans []map[string]any
	for _, s := range rawViewSpans {
		st, _ := colInt(s, "start_time")
		if st >= startTS-trajectoryLookbackMicros && st <= endTS+trajectorySkewMicros {
			windowSpans = append(windowSpans, s)
		}
	}
	spans := canonicalizeTrajectorySpans(windowSpans)

	parentIDs := make(map[string]bool)
	for _, s := range spans {
		parent, hasParent := colString(s, "parent_span_id")
		if !hasParent || parent == "" {
			continue
		}
		parentIDs[groupKey(s["trace_id"], s["service_name"], parent)] = true
	}

	type chainNode struct {
		span         map[string]any
		inTreeWindow bool
	}
	type hopRow struct {
		span        map[string]any
		topParentID string
		chain       []chainNode
	}

	// hop0: 1-hop parent lookup in spans; in_tree_window records whether p is in [-24h, +1h].
	hops := make([]hopRow, 0, len(spans))
	for _, s := range spans {
		parent, hasParent := colString(s, "parent_span_id")
		sStart, _ := colInt(s, "start_time")
		var matchedP map[string]any
		if hasParent && parent != "" {
			for _, p := range spans {
				if p["trace_id"] != s["trace_id"] || !equalNullableString(s["service_name"], p["service_name"]) {
					continue
				}
				pSpanID, _ := colString(p, "span_id")
				if pSpanID == parent {
					matchedP = p
					break
				}
			}
		}
		topParentID := ""
		inTreeWin := false
		if matchedP != nil {
			topParentID, _ = colString(matchedP, "span_id")
			pStart, _ := colInt(matchedP, "start_time")
			inTreeWin = pStart >= sStart-trajectoryLookbackMicros && pStart <= sStart+trajectorySkewMicros
		}
		hops = append(hops, hopRow{
			span:        s,
			topParentID: topParentID,
			chain:       []chainNode{{span: s, inTreeWindow: inTreeWin}},
		})
	}

	// hop1, hop2, hop4, hop8, hop16, hop32: 6 doubling steps (up to 64 nodes / 63 ancestor hops).
	for range 6 {
		nextHops := make([]hopRow, len(hops))
		for i, h := range hops {
			if h.topParentID == "" {
				nextHops[i] = h
				continue
			}
			var matchedAnc *hopRow
			for j := range hops {
				anc := &hops[j]
				ancSpanID, _ := colString(anc.span, "span_id")
				if anc.span["trace_id"] == h.span["trace_id"] &&
					equalNullableString(anc.span["service_name"], h.span["service_name"]) &&
					ancSpanID == h.topParentID {
					matchedAnc = anc
					break
				}
			}
			if matchedAnc != nil {
				merged := make([]chainNode, 0, len(matchedAnc.chain)+len(h.chain))
				merged = append(merged, matchedAnc.chain...)
				merged = append(merged, h.chain...)
				nextHops[i] = hopRow{
					span:        h.span,
					topParentID: matchedAnc.topParentID,
					chain:       merged,
				}
			} else {
				nextHops[i] = h
			}
		}
		hops = nextHops
	}

	// rooted_chains + trajectory CTE: tree_start_idx = MAX(idx) WHERE NOT n.in_tree_window,
	// filtered to tree_start_idx IS NOT NULL AND (ARRAY_LENGTH(chain) - 1 - tree_start_idx) <= 16.
	type trajectoryEntry struct {
		row          map[string]any
		chain        []chainNode
		treeStartIdx int
	}
	var trajectory []trajectoryEntry
	for _, h := range hops {
		treeStartIdx := -1
		for idx, n := range h.chain {
			if !n.inTreeWindow {
				treeStartIdx = idx
			}
		}
		if treeStartIdx < 0 || (len(h.chain)-1-treeStartIdx) > 16 {
			continue
		}
		row := copyTrajectoryBaseCols(h.span)
		row["depth"] = int64(len(h.chain) - 1 - treeStartIdx)
		rootSpan := h.chain[treeStartIdx].span
		rootSpanID, _ := colString(rootSpan, "span_id")
		row["root_span_id"] = rootSpanID

		lastAgentOpIdx := -1
		firstDeclaredIdx := -1
		var firstDeclaredName string
		for idx := treeStartIdx; idx < len(h.chain); idx++ {
			node := h.chain[idx].span
			if op, _ := colString(node, "gen_ai_operation_name"); isAgentOperation(op) {
				lastAgentOpIdx = idx
			}
			if firstDeclaredIdx < 0 {
				if dec, ok := declaredAgentName(t, node); ok {
					firstDeclaredIdx = idx
					firstDeclaredName = dec
				}
			}
		}
		if lastAgentOpIdx >= 0 {
			agNode := h.chain[lastAgentOpIdx].span
			agSpanID, _ := colString(agNode, "span_id")
			row["nearest_agent_span_id"] = agSpanID
			if dec, ok := declaredAgentName(t, agNode); ok {
				row["nearest_agent_name"] = dec
			} else if name, ok := colString(agNode, "gen_ai_agent_name"); ok {
				row["nearest_agent_name"] = name
			}
		} else if firstDeclaredIdx >= 0 {
			decNode := h.chain[firstDeclaredIdx].span
			decSpanID, _ := colString(decNode, "span_id")
			row["nearest_agent_span_id"] = decSpanID
			row["nearest_agent_name"] = firstDeclaredName
		}

		agentPath := []string{}
		for pos := treeStartIdx; pos < len(h.chain); pos++ {
			name, ok := agentPathName(t, h.chain[pos].span)
			if !ok {
				continue
			}
			dupPrev := false
			for prevPos := treeStartIdx; prevPos < pos; prevPos++ {
				prevName, prevOK := agentPathName(t, h.chain[prevPos].span)
				if !prevOK {
					continue
				}
				hasMid := false
				for midPos := prevPos + 1; midPos < pos; midPos++ {
					if _, midOK := agentPathName(t, h.chain[midPos].span); midOK {
						hasMid = true
						break
					}
				}
				if !hasMid && prevName == name {
					dupPrev = true
					break
				}
			}
			if !dupPrev {
				agentPath = append(agentPath, name)
			}
		}
		row["agent_path"] = agentPath

		rootParent, hasRootParent := colString(rootSpan, "parent_span_id")
		row["is_orphan"] = hasRootParent && rootParent != ""
		trajectory = append(trajectory, trajectoryEntry{
			row:          row,
			chain:        h.chain,
			treeStartIdx: treeStartIdx,
		})
	}

	// Pre-filter step_index inside trajectory CTE:
	// ROW_NUMBER() OVER (PARTITION BY h.trace_id, h.service_name ORDER BY h.start_time ASC, h.span_id ASC).
	trajPartitions := make(map[string][]int)
	var trajPartOrder []string
	for i, te := range trajectory {
		pk := groupKey(te.row["trace_id"], te.row["service_name"])
		if _, ok := trajPartitions[pk]; !ok {
			trajPartOrder = append(trajPartOrder, pk)
		}
		trajPartitions[pk] = append(trajPartitions[pk], i)
	}
	for _, pk := range trajPartOrder {
		idxs := trajPartitions[pk]
		sort.SliceStable(idxs, func(i, j int) bool {
			ri, rj := trajectory[idxs[i]].row, trajectory[idxs[j]].row
			si, _ := colInt(ri, "start_time")
			sj, _ := colInt(rj, "start_time")
			if si != sj {
				return si < sj
			}
			idi, _ := colString(ri, "span_id")
			idj, _ := colString(rj, "span_id")
			return idi < idj
		})
		for rank, idx := range idxs {
			trajectory[idx].row["step_index"] = int64(rank + 1)
		}
	}

	// emitted_ancestors CTE: unnest t.chain for 1..32 hops on simple paths to emitted ancestors.
	type emittedAncPair struct {
		desc      map[string]any
		descID    string
		descStart int64
		ancID     string
		ancStart  int64
	}
	var pairs []emittedAncPair
	reachable := make(map[string]bool)
	for _, te := range trajectory {
		descID, _ := colString(te.row, "span_id")
		descStart, _ := colInt(te.row, "start_time")
		seenAnc := make(map[string]bool)
		minIdx := max(len(te.chain)-33, 0)
		for aIdx := minIdx; aIdx < len(te.chain)-1; aIdx++ {
			ancSpan := te.chain[aIdx].span
			ancID, _ := colString(ancSpan, "span_id")
			dupLater := false
			for xIdx := aIdx + 1; xIdx < len(te.chain); xIdx++ {
				xID, _ := colString(te.chain[xIdx].span, "span_id")
				if xID == ancID {
					dupLater = true
					break
				}
			}
			if dupLater {
				continue
			}
			if aIdx < te.treeStartIdx {
				ancRootIdx := -1
				for i2 := 0; i2 <= aIdx; i2++ {
					if !te.chain[i2].inTreeWindow {
						ancRootIdx = i2
					}
				}
				if ancRootIdx < 0 || (aIdx-ancRootIdx) > 16 {
					continue
				}
			}
			if seenAnc[ancID] {
				continue
			}
			seenAnc[ancID] = true
			ancStart, _ := colInt(ancSpan, "start_time")
			pairs = append(pairs, emittedAncPair{
				desc:      te.row,
				descID:    descID,
				descStart: descStart,
				ancID:     ancID,
				ancStart:  ancStart,
			})
			reachable[groupKey(te.row["trace_id"], te.row["service_name"], descID, ancID)] = true
		}
	}

	tokenParentCoverage := make(map[string]tokenCoverage)
	for _, ea := range pairs {
		if !reportsTokens(ea.desc) {
			continue
		}
		hasReversePath := reachable[groupKey(ea.desc["trace_id"], ea.desc["service_name"], ea.ancID, ea.descID)]
		if hasReversePath && (ea.descStart < ea.ancStart || (ea.descStart == ea.ancStart && ea.descID <= ea.ancID)) {
			continue
		}
		key := groupKey(ea.desc["trace_id"], ea.desc["service_name"], ea.ancID)
		recordTokenCoverage(tokenParentCoverage, key, ea.desc)
	}

	// Outer SELECT: filter by WHERE t.start_time >= startTS AND t.start_time < endTS,
	// retaining pre-filter t.step_index, and computing is_leaf, partial-overlap token
	// suppression, and is_token_leaf.
	var out []map[string]any
	for _, pk := range trajPartOrder {
		for _, idx := range trajPartitions[pk] {
			row := trajectory[idx].row
			st, _ := colInt(row, "start_time")
			if st < startTS || st >= endTS {
				continue
			}
			lk := groupKey(row["trace_id"], row["service_name"], row["span_id"])
			row["is_leaf"] = !parentIDs[lk]
			cov := tokenParentCoverage[lk]
			_, hasIn := colInt(row, "gen_ai_usage_input_tokens")
			_, hasOut := colInt(row, "gen_ai_usage_output_tokens")
			_, hasReas := colInt(row, "gen_ai_usage_reasoning_tokens")
			uncovIn := hasIn && !cov.hasInput
			uncovOut := hasOut && !cov.hasOutput
			uncovReas := hasReas && !cov.hasReasoning
			if cov.hasInput && (uncovOut || uncovReas) {
				delete(row, "gen_ai_usage_input_tokens")
			}
			if cov.hasOutput && (uncovIn || uncovReas) {
				delete(row, "gen_ai_usage_output_tokens")
			}
			if cov.hasReasoning && (uncovIn || uncovOut) {
				delete(row, "gen_ai_usage_reasoning_tokens")
			}
			row["is_token_leaf"] = uncovIn || uncovOut || uncovReas
			out = append(out, row)
		}
	}
	return out
}

func TestScenarioMultiAgentTrajectoryView(t *testing.T) {
	rig := newConformanceRig(t)
	rig.pushTraces(t, loadTraces(t, "multi_agent/traces.json"))

	rawSpans := rig.srv.Rows("otel_spans")
	spans := trajectorySpans(t, rawSpans)
	traj := genAIAgentTrajectoryView(t, spans)
	tvfTraj := genAIAgentTrajectoryTVF(t, spans, 0, int64(1)<<60)
	if !reflect.DeepEqual(tvfTraj, traj) {
		t.Fatalf("tvf_gen_ai_agent_trajectory rows differed from v_gen_ai_agent_trajectory:\ngot:  %#v\nwant: %#v", tvfTraj, traj)
	}

	if len(traj) != 8 {
		t.Fatalf("v_gen_ai_agent_trajectory rows = %d, want 8", len(traj))
	}

	// Trace 1: 4bf92f3577b34da6a3ce929d0e0e4736 (7 spans).
	requireCols(t, "span_01 (root planner)", onlyRow(t, "span_01", traj, "span_id", "00f067aa0ba902b7"), map[string]any{
		"trace_id":              "4bf92f3577b34da6a3ce929d0e0e4736",
		"parent_span_id":        nil,
		"service_name":          "multi-agent-planner",
		"gen_ai_operation_name": "invoke_agent",
		"gen_ai_agent_name":     "planner",
		"gen_ai_main_agent_id":  "agent-planner-1",
		"depth":                 int64(0),
		"root_span_id":          "00f067aa0ba902b7",
		"nearest_agent_span_id": "00f067aa0ba902b7",
		"nearest_agent_name":    "planner",
		"agent_path":            []string{"planner"},
		"is_orphan":             false,
		"step_index":            int64(1),
		"is_leaf":               false,
		"is_token_leaf":         false,
	})

	requireCols(t, "span_02 (planner chat)", onlyRow(t, "span_02", traj, "span_id", "00f067aa0ba902b8"), map[string]any{
		"parent_span_id":            "00f067aa0ba902b7",
		"gen_ai_operation_name":     "chat",
		"gen_ai_agent_name":         "planner",
		"gen_ai_usage_input_tokens": int64(200),
		"depth":                     int64(1),
		"root_span_id":              "00f067aa0ba902b7",
		"nearest_agent_span_id":     "00f067aa0ba902b7",
		"nearest_agent_name":        "planner",
		"agent_path":                []string{"planner"},
		"is_orphan":                 false,
		"step_index":                int64(2),
		"is_leaf":                   true,
		"is_token_leaf":             true,
	})

	requireCols(t, "span_03 (delegated researcher)", onlyRow(t, "span_03", traj, "span_id", "00f067aa0ba902b9"), map[string]any{
		"parent_span_id":        "00f067aa0ba902b7",
		"gen_ai_operation_name": "invoke_agent",
		"gen_ai_agent_name":     "researcher",
		"depth":                 int64(1),
		"root_span_id":          "00f067aa0ba902b7",
		"nearest_agent_span_id": "00f067aa0ba902b9",
		"nearest_agent_name":    "researcher",
		"agent_path":            []string{"planner", "researcher"},
		"is_orphan":             false,
		"step_index":            int64(3),
		"is_leaf":               false,
		"is_token_leaf":         false,
	})

	requireCols(t, "span_04b (failed chat, rate_limited)", onlyRow(t, "span_04b", traj, "span_id", "00f067aa0ba902bb"), map[string]any{
		"parent_span_id":        "00f067aa0ba902b9",
		"gen_ai_operation_name": "chat",
		"gen_ai_agent_name":     "researcher",
		"error_type":            "rate_limited",
		"depth":                 int64(2),
		"root_span_id":          "00f067aa0ba902b7",
		"nearest_agent_span_id": "00f067aa0ba902b9",
		"nearest_agent_name":    "researcher",
		"agent_path":            []string{"planner", "researcher"},
		"is_orphan":             false,
		"step_index":            int64(4),
		"is_leaf":               true,
		"is_token_leaf":         false,
	})

	requireCols(t, "span_04 (retry chat)", onlyRow(t, "span_04", traj, "span_id", "00f067aa0ba902ba"), map[string]any{
		"parent_span_id":            "00f067aa0ba902b9",
		"gen_ai_operation_name":     "chat",
		"gen_ai_agent_name":         "researcher",
		"gen_ai_usage_input_tokens": int64(300),
		"depth":                     int64(2),
		"root_span_id":              "00f067aa0ba902b7",
		"nearest_agent_span_id":     "00f067aa0ba902b9",
		"nearest_agent_name":        "researcher",
		"agent_path":                []string{"planner", "researcher"},
		"is_orphan":                 false,
		"step_index":                int64(5),
		"is_leaf":                   true,
		"is_token_leaf":             true,
	})

	requireCols(t, "span_05 (parallel execute_sql)", onlyRow(t, "span_05", traj, "span_id", "00f067aa0ba902bc"), map[string]any{
		"parent_span_id":        "00f067aa0ba902b9",
		"gen_ai_operation_name": "execute_tool",
		"gen_ai_tool_name":      "execute_sql",
		"depth":                 int64(2),
		"root_span_id":          "00f067aa0ba902b7",
		"nearest_agent_span_id": "00f067aa0ba902b9",
		"nearest_agent_name":    "researcher",
		"agent_path":            []string{"planner", "researcher"},
		"is_orphan":             false,
		"step_index":            int64(6),
		"is_leaf":               true,
		"is_token_leaf":         false,
	})

	requireCols(t, "span_06 (parallel search_docs timeout)", onlyRow(t, "span_06", traj, "span_id", "00f067aa0ba902bd"), map[string]any{
		"parent_span_id":        "00f067aa0ba902b9",
		"gen_ai_operation_name": "execute_tool",
		"gen_ai_tool_name":      "search_docs",
		"error_type":            "ToolExecutionTimeout",
		"depth":                 int64(2),
		"root_span_id":          "00f067aa0ba902b7",
		"nearest_agent_span_id": "00f067aa0ba902b9",
		"nearest_agent_name":    "researcher",
		"agent_path":            []string{"planner", "researcher"},
		"is_orphan":             false,
		"step_index":            int64(7),
		"is_leaf":               true,
		"is_token_leaf":         false,
	})

	// Summing tokens over is_token_leaf == true in Trace 1 reproduces the root
	// totals (500 input, 300 output, 120 reasoning) without double counting.
	var leafInput, leafOutput, leafReasoning int64
	for _, r := range traj {
		if r["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
			continue
		}
		if leaf, _ := r["is_token_leaf"].(bool); leaf {
			leafInput += intOrZero(r, "gen_ai_usage_input_tokens")
			leafOutput += intOrZero(r, "gen_ai_usage_output_tokens")
			leafReasoning += intOrZero(r, "gen_ai_usage_reasoning_tokens")
		}
	}
	if leafInput != 500 || leafOutput != 300 || leafReasoning != 120 {
		t.Errorf("is_token_leaf sums = (in=%d, out=%d, reasoning=%d), want (500, 300, 120)",
			leafInput, leafOutput, leafReasoning)
	}

	// Trace 2: 7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d (1 orphan span).
	requireCols(t, "span_orphan (missing parent)", onlyRow(t, "span_orphan", traj, "span_id", "1122334455667788"), map[string]any{
		"trace_id":              "7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d",
		"parent_span_id":        "deadbeefcafebabe",
		"gen_ai_operation_name": "invoke_agent",
		"gen_ai_agent_name":     "late_worker",
		"depth":                 int64(0),
		"root_span_id":          "1122334455667788",
		"nearest_agent_span_id": "1122334455667788",
		"nearest_agent_name":    "late_worker",
		"agent_path":            []string{"late_worker"},
		"is_orphan":             true,
		"step_index":            int64(1),
		"is_leaf":               true,
		"is_token_leaf":         false,
	})
}

func TestAgentTrajectoryViewEdgeCases(t *testing.T) {
	baseTime := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC).UnixMicro()
	// attributes renders a v_gen_ai_spans attributes column.
	attributes := func(kv map[string]string) string {
		raw, err := json.Marshal(kv)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	// makeSpan builds a v_gen_ai_spans row. A non-empty agent is declared as
	// gen_ai.agent.name in attributes; without one, gen_ai_agent_name falls
	// back to service_name as it does in v_gen_ai_spans.
	makeSpan := func(traceID, spanID, parentID string, service any, op, agent string, offsetMicros, inTokens int64) map[string]any {
		row := map[string]any{
			"start_time":            baseTime + offsetMicros,
			"end_time":              baseTime + offsetMicros + 1000,
			"duration_nano":         int64(1_000_000),
			"duration_ms":           float64(1.0),
			"trace_id":              traceID,
			"span_id":               spanID,
			"span_name":             op + " " + spanID,
			"span_kind":             int64(1),
			"status_code":           int64(1),
			"gen_ai_operation_name": op,
		}
		if service != nil {
			row["service_name"] = service
		}
		if parentID != "" {
			row["parent_span_id"] = parentID
		}
		if agent != "" {
			row["gen_ai_agent_name"] = agent
			row["attributes"] = attributes(map[string]string{"gen_ai.agent.name": agent})
		} else if service != nil {
			row["gen_ai_agent_name"] = service
		}
		if inTokens > 0 {
			row["gen_ai_usage_input_tokens"] = inTokens
		}
		return row
	}
	evalView := genAIAgentTrajectoryView
	genAIAgentTrajectoryView := func(t *testing.T, spans []map[string]any) []map[string]any {
		t.Helper()
		gotView := evalView(t, spans)
		gotTVF := genAIAgentTrajectoryTVF(t, spans,
			baseTime-48*int64(time.Hour/time.Microsecond),
			baseTime+48*int64(time.Hour/time.Microsecond),
		)
		if !reflect.DeepEqual(gotTVF, gotView) {
			t.Fatalf("tvf_gen_ai_agent_trajectory differed from v_gen_ai_agent_trajectory:\ngot TVF:  %#v\nwant view: %#v", gotTVF, gotView)
		}
		return gotView
	}

	t.Run("self_parented_span_and_descendant_excluded", func(t *testing.T) {
		spans := []map[string]any{
			makeSpan("t-self-loop", "s1", "s1", "svc", "invoke_agent", "agent-a", 0, 10),
			makeSpan("t-self-loop", "s2", "s1", "svc", "chat", "agent-a", 10, 10),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 0 {
			t.Fatalf("self-parented cycle rows = %d, want 0", len(got))
		}
	})

	t.Run("two_node_cycle_disconnected", func(t *testing.T) {
		spans := []map[string]any{
			makeSpan("t-cycle-2", "span-a", "span-b", "svc", "invoke_agent", "agent-a", 0, 10),
			makeSpan("t-cycle-2", "span-b", "span-a", "svc", "invoke_agent", "agent-b", 10, 10),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 0 {
			t.Fatalf("disconnected 2-node cycle rows = %d, want 0", len(got))
		}
	})

	t.Run("two_node_cycle_rooted", func(t *testing.T) {
		// Corrupted telemetry where span-r appears once as root and once with
		// parent span-b, creating a cycle span-r -> span-b -> span-r.
		spans := []map[string]any{
			makeSpan("t-cycle-2r", "span-r", "", "svc", "invoke_agent", "root", 0, 10),
			makeSpan("t-cycle-2r", "span-b", "span-r", "svc", "invoke_agent", "child", 10, 10),
			makeSpan("t-cycle-2r", "span-r", "span-b", "svc", "invoke_agent", "root", 20, 10),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 2 {
			t.Fatalf("rooted 2-node cycle rows = %d, want 2 (cycle stopped by visited_span_ids)", len(got))
		}
		if got[0]["depth"] != int64(0) || got[1]["depth"] != int64(1) {
			t.Errorf("depths = [%v, %v], want [0, 1]", got[0]["depth"], got[1]["depth"])
		}
	})

	t.Run("three_node_cycle_rooted", func(t *testing.T) {
		// span-r -> span-a -> span-b -> span-a (corrupted duplicate of span-a).
		spans := []map[string]any{
			makeSpan("t-cycle-3r", "span-r", "", "svc", "invoke_agent", "root", 0, 0),
			makeSpan("t-cycle-3r", "span-a", "span-r", "svc", "invoke_agent", "a", 10, 0),
			makeSpan("t-cycle-3r", "span-b", "span-a", "svc", "invoke_agent", "b", 20, 0),
			makeSpan("t-cycle-3r", "span-a", "span-b", "svc", "invoke_agent", "a", 30, 0),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 3 {
			t.Fatalf("rooted 3-node cycle rows = %d, want 3 (cycle stopped by visited_span_ids)", len(got))
		}
		requireCols(t, "span-b at depth 2", onlyRow(t, "span-b", got, "span_id", "span-b"), map[string]any{
			"depth":      int64(2),
			"agent_path": []string{"root", "a", "b"},
		})
	})

	t.Run("depth_cap_16", func(t *testing.T) {
		var spans []map[string]any
		for i := 0; i < 20; i++ {
			id := fmt.Sprintf("s%02d", i)
			parent := ""
			if i > 0 {
				parent = fmt.Sprintf("s%02d", i-1)
			}
			spans = append(spans, makeSpan("t-deep", id, parent, "svc", "invoke_agent", id, int64(i)*100, 1))
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 17 {
			t.Fatalf("20-level chain rows = %d, want 17 (depths 0..16)", len(got))
		}
		var maxDepth int64
		for _, r := range got {
			if d := r["depth"].(int64); d > maxDepth {
				maxDepth = d
			}
		}
		if maxDepth != 16 {
			t.Errorf("max depth = %d, want 16", maxDepth)
		}
		requireCols(t, "s16 at depth cap", onlyRow(t, "s16", got, "span_id", "s16"), map[string]any{
			"depth":        int64(16),
			"root_span_id": "s00",
			"step_index":   int64(17),
		})
	})

	t.Run("null_service_name_is_not_distinct_from", func(t *testing.T) {
		spans := []map[string]any{
			makeSpan("t-null-svc", "s1", "", nil, "invoke_agent", "root", 0, 50),
			makeSpan("t-null-svc", "s2", "s1", nil, "chat", "root", 10, 50),
			makeSpan("t-null-svc", "s3", "s1", "other-svc", "chat", "other", 20, 50),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 3 {
			t.Fatalf("null_service_name rows = %d, want 3", len(got))
		}
		requireCols(t, "s1 (nil service_name root)", onlyRow(t, "s1", got, "span_id", "s1"), map[string]any{
			"service_name":  nil,
			"depth":         int64(0),
			"is_orphan":     false,
			"is_leaf":       false,
			"is_token_leaf": false,
			"step_index":    int64(1),
		})
		requireCols(t, "s2 (nil service_name child)", onlyRow(t, "s2", got, "span_id", "s2"), map[string]any{
			"service_name":          nil,
			"depth":                 int64(1),
			"root_span_id":          "s1",
			"nearest_agent_span_id": "s1",
			"is_orphan":             false,
			"is_leaf":               true,
			"is_token_leaf":         true,
			"step_index":            int64(2),
		})
		requireCols(t, "s3 (different service_name does not join nil)", onlyRow(t, "s3", got, "span_id", "s3"), map[string]any{
			"service_name": "other-svc",
			"depth":        int64(0),
			"root_span_id": "s3",
			"is_orphan":    true,
			"step_index":   int64(1),
		})
	})

	t.Run("descendant_of_orphan_inherits_orphan_flag", func(t *testing.T) {
		spans := []map[string]any{
			makeSpan("t-orphan-tree", "o1", "missing-parent", "svc", "invoke_agent", "worker_a", 0, 100),
			makeSpan("t-orphan-tree", "o2", "o1", "svc", "invoke_agent", "worker_b", 10, 100),
			makeSpan("t-orphan-tree", "o3", "o2", "svc", "chat", "worker_b", 20, 100),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 3 {
			t.Fatalf("orphan tree rows = %d, want 3", len(got))
		}
		requireCols(t, "o1 (orphan root)", onlyRow(t, "o1", got, "span_id", "o1"), map[string]any{
			"depth":         int64(0),
			"root_span_id":  "o1",
			"is_orphan":     true,
			"is_leaf":       false,
			"is_token_leaf": false,
			"agent_path":    []string{"worker_a"},
		})
		requireCols(t, "o2 (orphan child)", onlyRow(t, "o2", got, "span_id", "o2"), map[string]any{
			"depth":                 int64(1),
			"root_span_id":          "o1",
			"nearest_agent_span_id": "o2",
			"nearest_agent_name":    "worker_b",
			"is_orphan":             true,
			"is_leaf":               false,
			"is_token_leaf":         false,
			"agent_path":            []string{"worker_a", "worker_b"},
		})
		requireCols(t, "o3 (orphan grandchild)", onlyRow(t, "o3", got, "span_id", "o3"), map[string]any{
			"depth":                 int64(2),
			"root_span_id":          "o1",
			"nearest_agent_span_id": "o2",
			"nearest_agent_name":    "worker_b",
			"is_orphan":             true,
			"is_leaf":               true,
			"is_token_leaf":         true,
			"agent_path":            []string{"worker_a", "worker_b"},
		})
	})

	t.Run("temporal_window_bounds", func(t *testing.T) {
		spans := []map[string]any{
			makeSpan("t-window", "root", "", "svc", "invoke_agent", "planner", 0, 0),
			// 25 hours after root -> outside +24h window -> becomes orphan root.
			makeSpan("t-window", "too-late", "root", "svc", "chat", "planner", int64(25*time.Hour/time.Microsecond), 10),
			// 2 hours before root -> outside -1h skew window -> becomes orphan root.
			makeSpan("t-window", "too-early", "root", "svc", "chat", "planner", -int64(2*time.Hour/time.Microsecond), 10),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 3 {
			t.Fatalf("temporal_window_bounds rows = %d, want 3", len(got))
		}
		requireCols(t, "too-late becomes orphan root", onlyRow(t, "too-late", got, "span_id", "too-late"), map[string]any{
			"depth":        int64(0),
			"root_span_id": "too-late",
			"is_orphan":    true,
		})
		requireCols(t, "too-early becomes orphan root", onlyRow(t, "too-early", got, "span_id", "too-early"), map[string]any{
			"depth":        int64(0),
			"root_span_id": "too-early",
			"is_orphan":    true,
		})
	})

	// gen_ai_agent_name names every span, because v_gen_ai_spans falls back to
	// service_name, so the agent columns read the declared agent name instead:
	// a chat or tool span that declares no agent keeps its agent's path and
	// nearest agent rather than adding the service name.
	t.Run("undeclared_children_inherit_the_agent", func(t *testing.T) {
		spans := []map[string]any{
			makeSpan("t-undeclared", "planner", "", "my-service", "invoke_agent", "planner", 0, 0),
			makeSpan("t-undeclared", "chat", "planner", "my-service", "chat", "", 10, 20),
			makeSpan("t-undeclared", "tool", "planner", "my-service", "execute_tool", "", 20, 0),
			makeSpan("t-undeclared", "researcher", "planner", "my-service", "invoke_agent", "researcher", 30, 0),
			makeSpan("t-undeclared", "research-chat", "researcher", "my-service", "chat", "", 40, 30),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 5 {
			t.Fatalf("rows = %d, want 5", len(got))
		}
		for _, id := range []string{"chat", "tool"} {
			requireCols(t, id+" under planner without an agent attribute", onlyRow(t, id, got, "span_id", id), map[string]any{
				"gen_ai_agent_name":     "my-service",
				"nearest_agent_span_id": "planner",
				"nearest_agent_name":    "planner",
				"agent_path":            []string{"planner"},
			})
		}
		requireCols(t, "research-chat under researcher without an agent attribute", onlyRow(t, "research-chat", got, "span_id", "research-chat"), map[string]any{
			"gen_ai_agent_name":     "my-service",
			"nearest_agent_span_id": "researcher",
			"nearest_agent_name":    "researcher",
			"agent_path":            []string{"planner", "researcher"},
		})
	})

	// Without an invoke_agent span, a root that declares no agent has no
	// nearest agent and an empty path, and so do its undeclared descendants,
	// until a span declares an agent name (gen_ai.agent.name, or else
	// gcp.vertex.agent.name) and becomes the nearest agent of its subtree.
	t.Run("declared_name_starts_an_agent_only_below_no_agent", func(t *testing.T) {
		vertexRoot := makeSpan("t-vertex", "vertex-root", "", "svc", "chat", "", 0, 0)
		vertexRoot["gen_ai_agent_name"] = "solo"
		vertexRoot["attributes"] = attributes(map[string]string{"gcp.vertex.agent.name": "solo"})
		spans := []map[string]any{
			makeSpan("t-no-agent", "root-chat", "", "svc", "chat", "", 0, 0),
			makeSpan("t-no-agent", "root-tool", "root-chat", "svc", "execute_tool", "", 10, 0),
			makeSpan("t-no-agent", "late-chat", "root-tool", "svc", "chat", "late", 20, 0),
			vertexRoot,
			makeSpan("t-vertex", "vertex-tool", "vertex-root", "svc", "execute_tool", "", 10, 0),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 5 {
			t.Fatalf("rows = %d, want 5", len(got))
		}
		for _, id := range []string{"root-chat", "root-tool"} {
			requireCols(t, id+" without any agent", onlyRow(t, id, got, "span_id", id), map[string]any{
				"gen_ai_agent_name":     "svc",
				"nearest_agent_span_id": nil,
				"nearest_agent_name":    nil,
				"agent_path":            []string{},
			})
		}
		requireCols(t, "late-chat declares the first agent", onlyRow(t, "late-chat", got, "span_id", "late-chat"), map[string]any{
			"nearest_agent_span_id": "late-chat",
			"nearest_agent_name":    "late",
			"agent_path":            []string{"late"},
		})
		requireCols(t, "vertex-root declares gcp.vertex.agent.name only", onlyRow(t, "vertex-root", got, "span_id", "vertex-root"), map[string]any{
			"nearest_agent_span_id": "vertex-root",
			"nearest_agent_name":    "solo",
			"agent_path":            []string{"solo"},
		})
		requireCols(t, "vertex-tool under vertex-root", onlyRow(t, "vertex-tool", got, "span_id", "vertex-tool"), map[string]any{
			"nearest_agent_span_id": "vertex-root",
			"nearest_agent_name":    "solo",
			"agent_path":            []string{"solo"},
		})
	})

	// A token count of 0 is reported, not missing, as in the rollup views: the
	// child is the token leaf and its parent is not. A child whose counts are
	// NULL is not a token leaf and leaves its parent one.
	t.Run("explicit_zero_token_child_is_the_token_leaf", func(t *testing.T) {
		zero := makeSpan("t-zero-tokens", "zero-chat", "agent", "svc", "chat", "", 10, 0)
		zero["gen_ai_usage_input_tokens"] = int64(0)
		zero["gen_ai_usage_output_tokens"] = int64(0)
		spans := []map[string]any{
			makeSpan("t-zero-tokens", "agent", "", "svc", "invoke_agent", "worker", 0, 25),
			zero,
			makeSpan("t-zero-tokens", "other-agent", "", "svc", "invoke_agent", "other", 20, 5),
			makeSpan("t-zero-tokens", "no-tokens", "other-agent", "svc", "execute_tool", "", 30, 0),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 4 {
			t.Fatalf("rows = %d, want 4", len(got))
		}
		requireCols(t, "zero-chat reports 0 tokens", onlyRow(t, "zero-chat", got, "span_id", "zero-chat"), map[string]any{
			"gen_ai_usage_input_tokens":  int64(0),
			"gen_ai_usage_output_tokens": int64(0),
			"is_token_leaf":              true,
		})
		requireCols(t, "agent is the parent of a 0-token child", onlyRow(t, "agent", got, "span_id", "agent"), map[string]any{
			"gen_ai_usage_input_tokens": int64(25),
			"is_token_leaf":             false,
		})
		requireCols(t, "no-tokens reports no tokens", onlyRow(t, "no-tokens", got, "span_id", "no-tokens"), map[string]any{
			"gen_ai_usage_input_tokens": nil,
			"is_token_leaf":             false,
		})
		requireCols(t, "other-agent has only a child without tokens", onlyRow(t, "other-agent", got, "span_id", "other-agent"), map[string]any{
			"gen_ai_usage_input_tokens": int64(5),
			"is_token_leaf":             true,
		})
	})

	t.Run("duplicate_span_id_with_different_name_canonicalized_without_child_fanout", func(t *testing.T) {
		// Two rows for root "p" with different span_name survive otel_spans_dedup;
		// canonicalization in spans picks the earlier/longer/alphabetically first
		// row so child "c" is emitted once rather than fanned out twice.
		rootLaterName := makeSpan("t-dup-parent", "p", "", "svc", "invoke_agent", "planner", 0, 100)
		rootLaterName["span_name"] = "invoke_agent z-name"
		rootEarlierName := makeSpan("t-dup-parent", "p", "", "svc", "invoke_agent", "planner", 0, 100)
		rootEarlierName["span_name"] = "invoke_agent a-name"
		child := makeSpan("t-dup-parent", "c", "p", "svc", "chat", "planner", 10, 100)

		got := genAIAgentTrajectoryView(t, []map[string]any{rootLaterName, rootEarlierName, child})
		if len(got) != 2 {
			t.Fatalf("duplicate parent trajectory rows = %d, want 2 (1 root + 1 child)", len(got))
		}
		requireCols(t, "canonical root p", onlyRow(t, "p", got, "span_id", "p"), map[string]any{
			"span_name":     "invoke_agent a-name",
			"depth":         int64(0),
			"step_index":    int64(1),
			"is_leaf":       false,
			"is_token_leaf": false,
		})
		requireCols(t, "single child c", onlyRow(t, "c", got, "span_id", "c"), map[string]any{
			"parent_span_id":            "p",
			"depth":                     int64(1),
			"step_index":                int64(2),
			"is_leaf":                   true,
			"is_token_leaf":             true,
			"gen_ai_usage_input_tokens": int64(100),
		})
	})

	t.Run("reasoning_only_leaf_is_token_leaf", func(t *testing.T) {
		parent := makeSpan("t-reasoning-only", "p", "", "svc", "invoke_agent", "planner", 0, 0)
		parent["gen_ai_usage_reasoning_tokens"] = int64(7)
		child := makeSpan("t-reasoning-only", "c", "p", "svc", "chat", "planner", 10, 0)
		child["gen_ai_usage_reasoning_tokens"] = int64(7)

		got := genAIAgentTrajectoryView(t, []map[string]any{parent, child})
		if len(got) != 2 {
			t.Fatalf("reasoning_only_leaf rows = %d, want 2", len(got))
		}
		requireCols(t, "parent with reasoning-only child", onlyRow(t, "p", got, "span_id", "p"), map[string]any{
			"gen_ai_usage_reasoning_tokens": int64(7),
			"is_token_leaf":                 false,
		})
		requireCols(t, "reasoning-only leaf child", onlyRow(t, "c", got, "span_id", "c"), map[string]any{
			"gen_ai_usage_reasoning_tokens": int64(7),
			"is_token_leaf":                 true,
		})
	})

	t.Run("tokenless_intermediate_span_suppresses_ancestor_is_token_leaf", func(t *testing.T) {
		root := makeSpan("t-tokenless-mid", "r", "", "svc", "invoke_agent", "planner", 0, 100)
		tool := makeSpan("t-tokenless-mid", "tool", "r", "svc", "execute_tool", "", 10, 0)
		chat := makeSpan("t-tokenless-mid", "chat", "tool", "svc", "chat", "", 20, 100)

		got := genAIAgentTrajectoryView(t, []map[string]any{root, tool, chat})
		if len(got) != 3 {
			t.Fatalf("tokenless_intermediate_span rows = %d, want 3", len(got))
		}
		requireCols(t, "root suppressed across tokenless intermediate", onlyRow(t, "r", got, "span_id", "r"), map[string]any{
			"gen_ai_usage_input_tokens": int64(100),
			"is_token_leaf":             false,
		})
		requireCols(t, "tokenless intermediate tool", onlyRow(t, "tool", got, "span_id", "tool"), map[string]any{
			"gen_ai_usage_input_tokens": nil,
			"is_token_leaf":             false,
		})
		requireCols(t, "grandchild chat is sole token leaf", onlyRow(t, "chat", got, "span_id", "chat"), map[string]any{
			"gen_ai_usage_input_tokens": int64(100),
			"is_token_leaf":             true,
		})
	})

	t.Run("depth_cap_18_node_rollup_chain_retains_depth_16_token_leaf", func(t *testing.T) {
		var spans []map[string]any
		for i := 0; i < 18; i++ {
			id := fmt.Sprintf("a%02d", i)
			parent := ""
			if i > 0 {
				parent = fmt.Sprintf("a%02d", i-1)
			}
			s := makeSpan("t-deep-rollup", id, parent, "svc", "invoke_agent", id, int64(i)*100, 100)
			s["gen_ai_usage_output_tokens"] = int64(40)
			s["gen_ai_usage_reasoning_tokens"] = int64(10)
			spans = append(spans, s)
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 17 {
			t.Fatalf("18-node rollup chain rows = %d, want 17 (depths 0..16)", len(got))
		}
		var sumIn, sumOut, sumReas int64
		for i := 0; i <= 16; i++ {
			id := fmt.Sprintf("a%02d", i)
			row := onlyRow(t, id, got, "span_id", id)
			wantTokenLeaf := i == 16
			requireCols(t, id, row, map[string]any{
				"depth":         int64(i),
				"is_token_leaf": wantTokenLeaf,
			})
			if row["is_token_leaf"] == true {
				if v, ok := colInt(row, "gen_ai_usage_input_tokens"); ok {
					sumIn += v
				}
				if v, ok := colInt(row, "gen_ai_usage_output_tokens"); ok {
					sumOut += v
				}
				if v, ok := colInt(row, "gen_ai_usage_reasoning_tokens"); ok {
					sumReas += v
				}
			}
		}
		if sumIn != 100 || sumOut != 40 || sumReas != 10 {
			t.Fatalf("token leaf sums = (%d, %d, %d), want (100, 40, 10)", sumIn, sumOut, sumReas)
		}
	})

	t.Run("mixed_token_family_parent_input_child_reasoning_preserves_both", func(t *testing.T) {
		parent := makeSpan("t-mixed-family", "p", "", "svc", "invoke_agent", "planner", 0, 100)
		delete(parent, "gen_ai_usage_output_tokens")
		child := makeSpan("t-mixed-family", "c", "p", "svc", "chat", "planner", 10, 0)
		child["gen_ai_usage_reasoning_tokens"] = int64(7)

		got := genAIAgentTrajectoryView(t, []map[string]any{parent, child})
		if len(got) != 2 {
			t.Fatalf("mixed_token_family rows = %d, want 2", len(got))
		}
		requireCols(t, "parent p with input_tokens only", onlyRow(t, "p", got, "span_id", "p"), map[string]any{
			"gen_ai_usage_input_tokens":     int64(100),
			"gen_ai_usage_output_tokens":    nil,
			"gen_ai_usage_reasoning_tokens": nil,
			"is_token_leaf":                 true,
		})
		requireCols(t, "child c with reasoning_tokens only", onlyRow(t, "c", got, "span_id", "c"), map[string]any{
			"gen_ai_usage_input_tokens":     nil,
			"gen_ai_usage_output_tokens":    nil,
			"gen_ai_usage_reasoning_tokens": int64(7),
			"is_token_leaf":                 true,
		})
		var sumIn, sumOut, sumReas int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				if v, ok := colInt(row, "gen_ai_usage_input_tokens"); ok {
					sumIn += v
				}
				if v, ok := colInt(row, "gen_ai_usage_output_tokens"); ok {
					sumOut += v
				}
				if v, ok := colInt(row, "gen_ai_usage_reasoning_tokens"); ok {
					sumReas += v
				}
			}
		}
		if sumIn != 100 || sumOut != 0 || sumReas != 7 {
			t.Fatalf("mixed token family leaf sums = (%d, %d, %d), want (100, 0, 7)", sumIn, sumOut, sumReas)
		}
	})

	t.Run("partial_token_family_overlap_suppresses_covered_families_and_retains_uncovered", func(t *testing.T) {
		parent := makeSpan("t-partial-overlap", "p", "", "svc", "chat", "planner", 0, 100)
		parent["gen_ai_usage_output_tokens"] = int64(40)
		parent["gen_ai_usage_reasoning_tokens"] = int64(7)
		child := makeSpan("t-partial-overlap", "c", "p", "svc", "chat", "planner", 10, 100)
		child["gen_ai_usage_output_tokens"] = int64(40)

		got := genAIAgentTrajectoryView(t, []map[string]any{parent, child})
		if len(got) != 2 {
			t.Fatalf("partial_token_family_overlap rows = %d, want 2", len(got))
		}
		requireCols(t, "parent p retains only uncovered reasoning_tokens", onlyRow(t, "p", got, "span_id", "p"), map[string]any{
			"gen_ai_usage_input_tokens":     nil,
			"gen_ai_usage_output_tokens":    nil,
			"gen_ai_usage_reasoning_tokens": int64(7),
			"is_token_leaf":                 true,
		})
		requireCols(t, "child c retains input and output tokens", onlyRow(t, "c", got, "span_id", "c"), map[string]any{
			"gen_ai_usage_input_tokens":     int64(100),
			"gen_ai_usage_output_tokens":    int64(40),
			"gen_ai_usage_reasoning_tokens": nil,
			"is_token_leaf":                 true,
		})
		var sumIn, sumOut, sumReas int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				sumIn += intOrZero(row, "gen_ai_usage_input_tokens")
				sumOut += intOrZero(row, "gen_ai_usage_output_tokens")
				sumReas += intOrZero(row, "gen_ai_usage_reasoning_tokens")
			}
		}
		if sumIn != 100 || sumOut != 40 || sumReas != 7 {
			t.Fatalf("partial overlap leaf sums = (%d, %d, %d), want (100, 40, 7)", sumIn, sumOut, sumReas)
		}
	})

	t.Run("split_token_families_across_multiple_children_suppresses_parent", func(t *testing.T) {
		parent := makeSpan("t-split-children", "p", "", "svc", "invoke_agent", "planner", 0, 100)
		parent["gen_ai_usage_output_tokens"] = int64(40)
		childIn := makeSpan("t-split-children", "c1", "p", "svc", "chat", "planner", 10, 100)
		childOut := makeSpan("t-split-children", "c2", "p", "svc", "chat", "planner", 20, 0)
		childOut["gen_ai_usage_output_tokens"] = int64(40)

		got := genAIAgentTrajectoryView(t, []map[string]any{parent, childIn, childOut})
		if len(got) != 3 {
			t.Fatalf("split_token_families rows = %d, want 3", len(got))
		}
		requireCols(t, "parent p covered across c1 and c2", onlyRow(t, "p", got, "span_id", "p"), map[string]any{
			"gen_ai_usage_input_tokens":  int64(100),
			"gen_ai_usage_output_tokens": int64(40),
			"is_token_leaf":              false,
		})
		requireCols(t, "child c1 input leaf", onlyRow(t, "c1", got, "span_id", "c1"), map[string]any{
			"gen_ai_usage_input_tokens": int64(100),
			"is_token_leaf":             true,
		})
		requireCols(t, "child c2 output leaf", onlyRow(t, "c2", got, "span_id", "c2"), map[string]any{
			"gen_ai_usage_output_tokens": int64(40),
			"is_token_leaf":              true,
		})
		var sumIn, sumOut, sumReas int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				sumIn += intOrZero(row, "gen_ai_usage_input_tokens")
				sumOut += intOrZero(row, "gen_ai_usage_output_tokens")
				sumReas += intOrZero(row, "gen_ai_usage_reasoning_tokens")
			}
		}
		if sumIn != 100 || sumOut != 40 || sumReas != 0 {
			t.Fatalf("split children leaf sums = (%d, %d, %d), want (100, 40, 0)", sumIn, sumOut, sumReas)
		}
	})

	t.Run("depth_cap_18_node_chain_with_emitted_orphan_child_suppresses_depth_16", func(t *testing.T) {
		var spans []map[string]any
		for i := 0; i <= 16; i++ {
			id := fmt.Sprintf("s%02d", i)
			parent := ""
			if i > 0 {
				parent = fmt.Sprintf("s%02d", i-1)
			}
			spans = append(spans, makeSpan("t-deep-orphan-child", id, parent, "svc", "invoke_agent", id, int64(i)*1_000_000, 100))
		}
		// s17 is a child of s16 starting 25 hours after s00, so it is outside
		// s16's [-1h, +24h] window and emitted as an orphan root at depth 0.
		spans = append(spans, makeSpan("t-deep-orphan-child", "s17", "s16", "svc", "chat", "s17", int64(25*time.Hour/time.Microsecond), 100))

		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 18 {
			t.Fatalf("18-node chain with out-of-window s17 rows = %d, want 18", len(got))
		}
		requireCols(t, "s16 suppressed by emitted orphan child s17", onlyRow(t, "s16", got, "span_id", "s16"), map[string]any{
			"depth":         int64(16),
			"is_token_leaf": false,
		})
		requireCols(t, "s17 emitted as orphan root and sole token leaf", onlyRow(t, "s17", got, "span_id", "s17"), map[string]any{
			"depth":         int64(0),
			"is_orphan":     true,
			"is_token_leaf": true,
		})
		var sumIn int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				sumIn += intOrZero(row, "gen_ai_usage_input_tokens")
			}
		}
		if sumIn != 100 {
			t.Fatalf("token leaf sum = %d, want 100", sumIn)
		}
	})

	t.Run("time_window_broken_cycle_retains_token_leaf", func(t *testing.T) {
		// a starts at 10:00 with parent b (12:00, outside a's [-24h, +1h]
		// parent window), so a is an orphan root at depth 0 and b is emitted as
		// a's child at depth 1. The cycle edge a.parent_span_id = b must not
		// suppress b.
		spanA := makeSpan("t-window-cycle", "a", "b", "svc", "invoke_agent", "agent-a", 0, 100)
		spanB := makeSpan("t-window-cycle", "b", "a", "svc", "invoke_agent", "agent-b", int64(2*time.Hour/time.Microsecond), 100)

		got := genAIAgentTrajectoryView(t, []map[string]any{spanA, spanB})
		if len(got) != 2 {
			t.Fatalf("time_window_broken_cycle rows = %d, want 2", len(got))
		}
		requireCols(t, "a (orphan root suppressed by descendant b)", onlyRow(t, "a", got, "span_id", "a"), map[string]any{
			"depth":         int64(0),
			"is_orphan":     true,
			"is_token_leaf": false,
		})
		requireCols(t, "b (depth 1 child retained as token leaf)", onlyRow(t, "b", got, "span_id", "b"), map[string]any{
			"depth":         int64(1),
			"is_orphan":     true,
			"is_token_leaf": true,
		})
		var sumIn int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				sumIn += intOrZero(row, "gen_ai_usage_input_tokens")
			}
		}
		if sumIn != 100 {
			t.Fatalf("time_window_broken_cycle token leaf sum = %d, want 100", sumIn)
		}
	})

	t.Run("acyclic_earlier_child_of_orphan_parent_suppresses_parent", func(t *testing.T) {
		// p is an orphan root at +0h (its parent "missing" is not exported), and
		// c is an acyclic child of p whose clock was skewed to -2h (outside the
		// [-1h, +24h] child window, so c is also emitted as an orphan root).
		// Because there is no reverse path from p to c, c covers p even though
		// p.is_orphan is true and c.start_time < p.start_time.
		parent := makeSpan("t-orphan-early-child", "p", "missing", "svc", "invoke_agent", "planner", 0, 100)
		child := makeSpan("t-orphan-early-child", "c", "p", "svc", "chat", "planner", -int64(2*time.Hour/time.Microsecond), 100)

		got := genAIAgentTrajectoryView(t, []map[string]any{parent, child})
		if len(got) != 2 {
			t.Fatalf("acyclic_earlier_child_of_orphan_parent rows = %d, want 2", len(got))
		}
		requireCols(t, "orphan parent p suppressed by acyclic earlier child c", onlyRow(t, "p", got, "span_id", "p"), map[string]any{
			"depth":         int64(0),
			"is_orphan":     true,
			"is_token_leaf": false,
		})
		requireCols(t, "earlier child c is sole token leaf", onlyRow(t, "c", got, "span_id", "c"), map[string]any{
			"depth":         int64(0),
			"is_orphan":     true,
			"is_token_leaf": true,
		})
		var sumIn int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				sumIn += intOrZero(row, "gen_ai_usage_input_tokens")
			}
		}
		if sumIn != 100 {
			t.Fatalf("acyclic_earlier_child_of_orphan_parent token leaf sum = %d, want 100", sumIn)
		}
	})

	t.Run("orphan_rooted_depth_16_chain_with_earlier_emitted_child_suppresses_depth_16", func(t *testing.T) {
		// 17-node orphan-rooted chain s00..s16 (s00 names "missing" as parent, so
		// is_orphan = true is inherited by s00..s16) plus child s17 of s16 at
		// s16.start_time - 2h (outside s16's [-1h, +24h] child window, so s17 is
		// emitted as an orphan root at depth 0). Because there is no cycle, s17
		// covers s16..s00 and is the sole token leaf.
		var spans []map[string]any
		for i := 0; i <= 16; i++ {
			id := fmt.Sprintf("s%02d", i)
			parent := "missing"
			if i > 0 {
				parent = fmt.Sprintf("s%02d", i-1)
			}
			spans = append(spans, makeSpan("t-orphan-deep-early-child", id, parent, "svc", "invoke_agent", id, int64(i)*1_000_000, 100))
		}
		s16Offset := int64(16) * 1_000_000
		spans = append(spans, makeSpan("t-orphan-deep-early-child", "s17", "s16", "svc", "chat", "s17", s16Offset-int64(2*time.Hour/time.Microsecond), 100))

		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 18 {
			t.Fatalf("orphan_rooted_depth_16_chain_with_earlier_emitted_child rows = %d, want 18", len(got))
		}
		requireCols(t, "s16 in orphan tree suppressed by earlier emitted child s17", onlyRow(t, "s16", got, "span_id", "s16"), map[string]any{
			"depth":         int64(16),
			"is_orphan":     true,
			"is_token_leaf": false,
		})
		requireCols(t, "s17 emitted as orphan root and sole token leaf", onlyRow(t, "s17", got, "span_id", "s17"), map[string]any{
			"depth":         int64(0),
			"is_orphan":     true,
			"is_token_leaf": true,
		})
		var sumIn int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				sumIn += intOrZero(row, "gen_ai_usage_input_tokens")
			}
		}
		if sumIn != 100 {
			t.Fatalf("orphan_rooted_depth_16_chain_with_earlier_emitted_child token leaf sum = %d, want 100", sumIn)
		}
	})

	t.Run("tokenless_intermediate_span_across_parent_to_tool_window_break_suppresses_parent", func(t *testing.T) {
		// p (+0h, 100 tokens) -> tool (+25h, NULL tokens, outside p's +24h child
		// window so tool is emitted as an orphan root at depth 0) -> c (+25h01m,
		// 100 tokens, emitted at depth 1 under tool). emitted_ancestors walks
		// c -> tool -> p across both trajectory segments so c suppresses p.
		parent := makeSpan("t-window-break-p-tool", "p", "", "svc", "invoke_agent", "planner", 0, 100)
		toolOffset := int64(25 * time.Hour / time.Microsecond)
		tool := makeSpan("t-window-break-p-tool", "tool", "p", "svc", "execute_tool", "", toolOffset, 0)
		child := makeSpan("t-window-break-p-tool", "c", "tool", "svc", "chat", "planner", toolOffset+int64(time.Minute/time.Microsecond), 100)

		got := genAIAgentTrajectoryView(t, []map[string]any{parent, tool, child})
		if len(got) != 3 {
			t.Fatalf("tokenless_intermediate_span_across_parent_to_tool_window_break rows = %d, want 3", len(got))
		}
		requireCols(t, "root p suppressed across window break and tokenless tool", onlyRow(t, "p", got, "span_id", "p"), map[string]any{
			"depth":         int64(0),
			"is_orphan":     false,
			"is_token_leaf": false,
		})
		requireCols(t, "tokenless tool orphan root", onlyRow(t, "tool", got, "span_id", "tool"), map[string]any{
			"depth":         int64(0),
			"is_orphan":     true,
			"is_token_leaf": false,
		})
		requireCols(t, "child c is sole token leaf", onlyRow(t, "c", got, "span_id", "c"), map[string]any{
			"depth":         int64(1),
			"is_orphan":     true,
			"is_token_leaf": true,
		})
		var sumIn int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				sumIn += intOrZero(row, "gen_ai_usage_input_tokens")
			}
		}
		if sumIn != 100 {
			t.Fatalf("tokenless_intermediate_span_across_parent_to_tool_window_break token leaf sum = %d, want 100", sumIn)
		}
	})

	t.Run("tokenless_intermediate_span_across_tool_to_child_window_break_suppresses_parent", func(t *testing.T) {
		// p (+0h, 100 tokens) -> tool (+1m, NULL tokens, emitted at depth 1 under
		// p) -> c (+26h, 100 tokens, outside tool's +24h child window so c is
		// emitted as an orphan root at depth 0). emitted_ancestors walks
		// c -> tool -> p so c suppresses p.
		parent := makeSpan("t-window-break-tool-c", "p", "", "svc", "invoke_agent", "planner", 0, 100)
		tool := makeSpan("t-window-break-tool-c", "tool", "p", "svc", "execute_tool", "", int64(time.Minute/time.Microsecond), 0)
		child := makeSpan("t-window-break-tool-c", "c", "tool", "svc", "chat", "planner", int64(26*time.Hour/time.Microsecond), 100)

		got := genAIAgentTrajectoryView(t, []map[string]any{parent, tool, child})
		if len(got) != 3 {
			t.Fatalf("tokenless_intermediate_span_across_tool_to_child_window_break rows = %d, want 3", len(got))
		}
		requireCols(t, "root p suppressed across tokenless tool and window break", onlyRow(t, "p", got, "span_id", "p"), map[string]any{
			"depth":         int64(0),
			"is_orphan":     false,
			"is_token_leaf": false,
		})
		requireCols(t, "tokenless tool at depth 1", onlyRow(t, "tool", got, "span_id", "tool"), map[string]any{
			"depth":         int64(1),
			"is_orphan":     false,
			"is_token_leaf": false,
		})
		requireCols(t, "child c orphan root is sole token leaf", onlyRow(t, "c", got, "span_id", "c"), map[string]any{
			"depth":         int64(0),
			"is_orphan":     true,
			"is_token_leaf": true,
		})
		var sumIn int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				sumIn += intOrZero(row, "gen_ai_usage_input_tokens")
			}
		}
		if sumIn != 100 {
			t.Fatalf("tokenless_intermediate_span_across_tool_to_child_window_break token leaf sum = %d, want 100", sumIn)
		}
	})

	t.Run("four_node_time_window_broken_cycle_retains_token_leaf", func(t *testing.T) {
		// 4-node cycle across +1.5h steps:
		// a(0h, parent=b) <- b(+1.5h, parent=c) <- c(+3.0h, parent=d) <- d(+4.5h, parent=a).
		// a, b, and c each see their parent +1.5h in the future (outside [-24h, +1h]),
		// so they are emitted as orphan roots at depth 0; d(+4.5h) sees parent a(0h)
		// at -4.5h (inside [-24h, +1h]), so d is emitted at depth 1 under a.
		// Mutual-reachability tie-breaking retains only d (+4.5h) as is_token_leaf = true.
		step := int64(90 * time.Minute / time.Microsecond)
		spans := []map[string]any{
			makeSpan("t-cycle-4-window", "a", "b", "svc", "invoke_agent", "agent-a", 0, 100),
			makeSpan("t-cycle-4-window", "b", "c", "svc", "invoke_agent", "agent-b", step, 100),
			makeSpan("t-cycle-4-window", "c", "d", "svc", "invoke_agent", "agent-c", 2*step, 100),
			makeSpan("t-cycle-4-window", "d", "a", "svc", "invoke_agent", "agent-d", 3*step, 100),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 4 {
			t.Fatalf("four_node_time_window_broken_cycle rows = %d, want 4", len(got))
		}
		for _, id := range []string{"a", "b", "c"} {
			requireCols(t, id+" suppressed in 4-node broken cycle", onlyRow(t, id, got, "span_id", id), map[string]any{
				"depth":         int64(0),
				"is_orphan":     true,
				"is_token_leaf": false,
			})
		}
		requireCols(t, "d retained as sole token leaf in 4-node broken cycle", onlyRow(t, "d", got, "span_id", "d"), map[string]any{
			"depth":         int64(1),
			"root_span_id":  "a",
			"is_orphan":     true,
			"is_token_leaf": true,
		})
		var sumIn int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				sumIn += intOrZero(row, "gen_ai_usage_input_tokens")
			}
		}
		if sumIn != 100 {
			t.Fatalf("four_node_time_window_broken_cycle token leaf sum = %d, want 100", sumIn)
		}
	})

	t.Run("two_node_25h_broken_cycle_retains_token_leaf", func(t *testing.T) {
		// a(0h, parent=b) and b(+25h, parent=a): both edges are outside [-24h, +1h],
		// so both a and b are emitted as orphan roots at depth 0. Mutual reachability
		// across segments retains b (+25h) as the sole token leaf.
		spans := []map[string]any{
			makeSpan("t-cycle-2-25h", "a", "b", "svc", "invoke_agent", "agent-a", 0, 100),
			makeSpan("t-cycle-2-25h", "b", "a", "svc", "invoke_agent", "agent-b", int64(25*time.Hour/time.Microsecond), 100),
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 2 {
			t.Fatalf("two_node_25h_broken_cycle rows = %d, want 2", len(got))
		}
		requireCols(t, "a suppressed by later cycle peer b", onlyRow(t, "a", got, "span_id", "a"), map[string]any{
			"depth":         int64(0),
			"is_orphan":     true,
			"is_token_leaf": false,
		})
		requireCols(t, "b retained as sole token leaf", onlyRow(t, "b", got, "span_id", "b"), map[string]any{
			"depth":         int64(0),
			"is_orphan":     true,
			"is_token_leaf": true,
		})
		var sumIn int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				sumIn += intOrZero(row, "gen_ai_usage_input_tokens")
			}
		}
		if sumIn != 100 {
			t.Fatalf("two_node_25h_broken_cycle token leaf sum = %d, want 100", sumIn)
		}
	})

	t.Run("two_segment_33_node_chain_suppresses_root_across_32_hops", func(t *testing.T) {
		// Segment 1: s00..s15 at 0h..+15s (depths 0..15, s00 reports 100 tokens).
		// Segment 2: s16..s32 at +25h..+25h16s (s16 has parent s15 across a +25h
		// gap so s16 is an orphan root at depth 0, and s32 at depth 16 is 32 hops
		// below s00 and reports 100 tokens).
		var spans []map[string]any
		for i := 0; i <= 15; i++ {
			id := fmt.Sprintf("s%02d", i)
			parent := ""
			if i > 0 {
				parent = fmt.Sprintf("s%02d", i-1)
			}
			tok := int64(0)
			if i == 0 {
				tok = 100
			}
			spans = append(spans, makeSpan("t-33-node-two-seg", id, parent, "svc", "invoke_agent", id, int64(i)*1_000_000, tok))
		}
		seg2Base := int64(25 * time.Hour / time.Microsecond)
		for i := 16; i <= 32; i++ {
			id := fmt.Sprintf("s%02d", i)
			parent := fmt.Sprintf("s%02d", i-1)
			tok := int64(0)
			if i == 32 {
				tok = 100
			}
			spans = append(spans, makeSpan("t-33-node-two-seg", id, parent, "svc", "invoke_agent", id, seg2Base+int64(i-16)*1_000_000, tok))
		}
		got := genAIAgentTrajectoryView(t, spans)
		if len(got) != 33 {
			t.Fatalf("two_segment_33_node_chain rows = %d, want 33", len(got))
		}
		requireCols(t, "s00 suppressed by 32-hop descendant s32", onlyRow(t, "s00", got, "span_id", "s00"), map[string]any{
			"depth":         int64(0),
			"root_span_id":  "s00",
			"is_orphan":     false,
			"is_token_leaf": false,
		})
		requireCols(t, "s32 at depth 16 in second segment is sole token leaf", onlyRow(t, "s32", got, "span_id", "s32"), map[string]any{
			"depth":         int64(16),
			"root_span_id":  "s16",
			"is_orphan":     true,
			"is_token_leaf": true,
		})
		var sumIn int64
		for _, row := range got {
			if row["is_token_leaf"] == true {
				sumIn += intOrZero(row, "gen_ai_usage_input_tokens")
			}
		}
		if sumIn != 100 {
			t.Fatalf("two_segment_33_node_chain token leaf sum = %d, want 100", sumIn)
		}
	})
}

func TestAgentTrajectoryTVFTiledWindowSemantics(t *testing.T) {
	t0 := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC).UnixMicro()
	t1 := t0 + int64(time.Hour/time.Microsecond)
	t2 := t1 + int64(time.Hour/time.Microsecond)

	attrsJSON := func(agent string) string {
		raw, err := json.Marshal(map[string]string{"gen_ai.agent.name": agent})
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	makeSpanAt := func(traceID, spanID, parentID, op, agent string, startMicros, inTokens int64) map[string]any {
		row := map[string]any{
			"start_time":            startMicros,
			"end_time":              startMicros + 1000,
			"duration_nano":         int64(1_000_000),
			"duration_ms":           float64(1.0),
			"trace_id":              traceID,
			"span_id":               spanID,
			"span_name":             op + " " + spanID,
			"span_kind":             int64(1),
			"status_code":           int64(1),
			"service_name":          "tiled-svc",
			"gen_ai_operation_name": op,
			"gen_ai_agent_name":     agent,
			"attributes":            attrsJSON(agent),
		}
		if parentID != "" {
			row["parent_span_id"] = parentID
		}
		if inTokens > 0 {
			row["gen_ai_usage_input_tokens"] = inTokens
		}
		return row
	}

	// 1. Boundary span at start_time == t1 is emitted only in [t1, t2), never in [t0, t1).
	// 2. Cross-window trace: root parent at t1 - 1s (in [t0, t1), 250 tokens) ->
	//    tokenless tool at t1 - 500ms (in [t0, t1)) ->
	//    worker child at t1 + 1s (in [t1, t2), 250 tokens).
	//    Because tvf_gen_ai_agent_trajectory scans spans in [start_ts - 24h, end_ts + 1h]
	//    and computes step_index in the trajectory CTE before applying the outer
	//    WHERE t.start_time >= start_ts AND t.start_time < end_ts:
	//    - In [t0, t1): parent (step_index = 1) and tool (step_index = 2) see child
	//      at t1 + 1s, so parent has is_leaf = false and is_token_leaf = false.
	//    - In [t1, t2): child sees parent and tool in [start_ts - 24h, end_ts + 1h],
	//      so child has depth = 2, root_span_id = "cross-parent", is_orphan = false,
	//      step_index = 3, agent_path = ["planner", "worker"], is_leaf = true,
	//      and is_token_leaf = true.
	//    - Across tiled windows [t0, t1) U [t1, t2), every span is emitted exactly
	//      once and summing tokens where is_token_leaf = true equals 250 + 40 = 290
	//      with zero double-counting.
	spans := []map[string]any{
		makeSpanAt("t-boundary", "boundary-span", "", "chat", "planner", t1, 40),
		makeSpanAt("t-cross", "cross-parent", "", "invoke_agent", "planner", t1-int64(time.Second/time.Microsecond), 250),
		makeSpanAt("t-cross", "cross-tool", "cross-parent", "execute_tool", "planner", t1-int64(500*time.Millisecond/time.Microsecond), 0),
		makeSpanAt("t-cross", "cross-child", "cross-tool", "invoke_agent", "worker", t1+int64(time.Second/time.Microsecond), 250),
	}

	win1 := genAIAgentTrajectoryTVF(t, spans, t0, t1)
	win2 := genAIAgentTrajectoryTVF(t, spans, t1, t2)

	if len(win1) != 2 {
		t.Fatalf("[t0, t1) rows = %d, want 2 (cross-parent and cross-tool); got %#v", len(win1), win1)
	}
	if len(win2) != 2 {
		t.Fatalf("[t1, t2) rows = %d, want 2 (boundary-span and cross-child); got %#v", len(win2), win2)
	}

	requireCols(t, "cross-parent in [t0, t1)", onlyRow(t, "cross-parent", win1, "span_id", "cross-parent"), map[string]any{
		"depth":         int64(0),
		"root_span_id":  "cross-parent",
		"is_orphan":     false,
		"step_index":    int64(1),
		"is_leaf":       false,
		"is_token_leaf": false,
		"agent_path":    []string{"planner"},
	})
	requireCols(t, "cross-tool in [t0, t1)", onlyRow(t, "cross-tool", win1, "span_id", "cross-tool"), map[string]any{
		"depth":         int64(1),
		"root_span_id":  "cross-parent",
		"is_orphan":     false,
		"step_index":    int64(2),
		"is_leaf":       false,
		"is_token_leaf": false,
		"agent_path":    []string{"planner"},
	})
	requireCols(t, "boundary-span in [t1, t2)", onlyRow(t, "boundary-span", win2, "span_id", "boundary-span"), map[string]any{
		"start_time":    t1,
		"depth":         int64(0),
		"is_orphan":     false,
		"step_index":    int64(1),
		"is_leaf":       true,
		"is_token_leaf": true,
	})
	requireCols(t, "cross-child in [t1, t2)", onlyRow(t, "cross-child", win2, "span_id", "cross-child"), map[string]any{
		"depth":                 int64(2),
		"root_span_id":          "cross-parent",
		"nearest_agent_span_id": "cross-child",
		"nearest_agent_name":    "worker",
		"is_orphan":             false,
		"step_index":            int64(3),
		"is_leaf":               true,
		"is_token_leaf":         true,
		"agent_path":            []string{"planner", "worker"},
	})

	var tiledTokenSum int64
	for _, r := range append(slices.Clone(win1), win2...) {
		if r["is_token_leaf"] == true {
			tiledTokenSum += intOrZero(r, "gen_ai_usage_input_tokens")
		}
	}
	if tiledTokenSum != 290 {
		t.Fatalf("tiled [t0, t1) U [t1, t2) token leaf sum = %d, want 290 (250 + 40)", tiledTokenSum)
	}
}

func TestTrajectoryViewModelMatchesViewSQL(t *testing.T) {
	sql := fmt.Sprintf(schema.GenAIAgentTrajectoryViewQuery, confProject, confDataset)
	if strings.Contains(sql, "[OFFSET(ARRAY_LENGTH(t.agent_path)") {
		t.Fatalf("v_gen_ai_agent_trajectory must use SAFE_OFFSET on t.agent_path, got:\n%s", sql)
	}
	requireSQL(t, "v_gen_ai_agent_trajectory", sql,
		"WITH RECURSIVE spans AS (",
		// declaredAgentName and agentPathName.
		`error_type, COALESCE( JSON_VALUE(attributes, '$."gen_ai.agent.name"'), JSON_VALUE(attributes, '$."gcp.vertex.agent.name"') ) AS declared_agent_name, `+
			`COALESCE( JSON_VALUE(attributes, '$."gen_ai.agent.name"'), JSON_VALUE(attributes, '$."gcp.vertex.agent.name"'), `+
			`IF(gen_ai_operation_name IN ('invoke_agent', 'create_agent'), gen_ai_agent_name, NULL) ) AS agent_path_name FROM`,
		"FROM `"+confProject+"."+confDataset+".v_gen_ai_spans` "+
			"QUALIFY ROW_NUMBER() OVER ( PARTITION BY trace_id, service_name, span_id ORDER BY start_time ASC, end_time DESC, span_name ASC, duration_nano DESC ) = 1",
		"parent_ids AS ( SELECT DISTINCT trace_id, service_name, parent_span_id AS span_id FROM spans WHERE parent_span_id IS NOT NULL AND parent_span_id != '' )",
		"trajectory AS (",
		"0 AS depth, s.span_id AS root_span_id, "+
			"CASE WHEN s.gen_ai_operation_name IN ('invoke_agent', 'create_agent') OR s.declared_agent_name IS NOT NULL THEN s.span_id ELSE CAST(NULL AS STRING) END AS nearest_agent_span_id, "+
			"CASE WHEN s.gen_ai_operation_name IN ('invoke_agent', 'create_agent') OR s.declared_agent_name IS NOT NULL THEN COALESCE(s.declared_agent_name, s.gen_ai_agent_name) ELSE CAST(NULL AS STRING) END AS nearest_agent_name, "+
			"CASE WHEN s.agent_path_name IS NOT NULL THEN [s.agent_path_name] ELSE ARRAY<STRING>[] END AS agent_path, "+
			"(s.parent_span_id IS NOT NULL AND s.parent_span_id != '') AS is_orphan, [s.span_id] AS visited_span_ids",
		"FROM spans AS s LEFT JOIN spans AS p ON s.trace_id = p.trace_id AND s.parent_span_id = p.span_id AND s.service_name IS NOT DISTINCT FROM p.service_name AND p.start_time BETWEEN TIMESTAMP_SUB(s.start_time, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(s.start_time, INTERVAL 1 HOUR) WHERE s.parent_span_id IS NULL OR s.parent_span_id = '' OR p.span_id IS NULL",
		"UNION ALL",
		"t.depth + 1 AS depth, t.root_span_id AS root_span_id, "+
			"CASE WHEN c.gen_ai_operation_name IN ('invoke_agent', 'create_agent') OR (t.nearest_agent_span_id IS NULL AND c.declared_agent_name IS NOT NULL) THEN c.span_id ELSE t.nearest_agent_span_id END AS nearest_agent_span_id, "+
			"CASE WHEN c.gen_ai_operation_name IN ('invoke_agent', 'create_agent') OR (t.nearest_agent_span_id IS NULL AND c.declared_agent_name IS NOT NULL) THEN COALESCE(c.declared_agent_name, c.gen_ai_agent_name) ELSE t.nearest_agent_name END AS nearest_agent_name, "+
			"CASE WHEN c.agent_path_name IS NOT NULL AND ( ARRAY_LENGTH(t.agent_path) = 0 OR t.agent_path[SAFE_OFFSET(ARRAY_LENGTH(t.agent_path) - 1)] != c.agent_path_name ) THEN ARRAY_CONCAT(t.agent_path, [c.agent_path_name]) ELSE t.agent_path END AS agent_path, "+
			"t.is_orphan AS is_orphan, ARRAY_CONCAT(t.visited_span_ids, [c.span_id]) AS visited_span_ids",
		"FROM trajectory AS t JOIN spans AS c ON c.trace_id = t.trace_id AND c.parent_span_id = t.span_id AND c.service_name IS NOT DISTINCT FROM t.service_name AND c.start_time BETWEEN TIMESTAMP_SUB(t.start_time, INTERVAL 1 HOUR) AND TIMESTAMP_ADD(t.start_time, INTERVAL 24 HOUR) WHERE t.depth < 16 AND c.span_id NOT IN UNNEST(t.visited_span_ids)",
		// emitted_ancestors + mutual-reachability token_parent_coverage.
		"emitted_ancestors AS ( SELECT c.trace_id, c.service_name, c.span_id AS descendant_span_id, c.start_time AS descendant_start_time, c.gen_ai_usage_input_tokens IS NOT NULL AS has_input, c.gen_ai_usage_output_tokens IS NOT NULL AS has_output, c.gen_ai_usage_reasoning_tokens IS NOT NULL AS has_reasoning, p.span_id AS ancestor_span_id, p.parent_span_id AS ancestor_parent_span_id, p.start_time AS ancestor_start_time, 1 AS hop, [c.span_id, p.span_id] AS path_span_ids FROM trajectory AS c JOIN trajectory AS p ON p.trace_id = c.trace_id AND p.span_id = c.parent_span_id AND p.service_name IS NOT DISTINCT FROM c.service_name WHERE p.span_id != c.span_id UNION ALL SELECT ea.trace_id, ea.service_name, ea.descendant_span_id, ea.descendant_start_time, ea.has_input, ea.has_output, ea.has_reasoning, p.span_id AS ancestor_span_id, p.parent_span_id AS ancestor_parent_span_id, p.start_time AS ancestor_start_time, ea.hop + 1 AS hop, ARRAY_CONCAT(ea.path_span_ids, [p.span_id]) AS path_span_ids FROM emitted_ancestors AS ea JOIN trajectory AS p ON p.trace_id = ea.trace_id AND p.span_id = ea.ancestor_parent_span_id AND p.service_name IS NOT DISTINCT FROM ea.service_name WHERE ea.hop < 32 AND p.span_id NOT IN UNNEST(ea.path_span_ids) )",
		"token_parent_coverage AS ( SELECT ea.trace_id, ea.service_name, ea.ancestor_span_id AS span_id, LOGICAL_OR(ea.has_input) AS has_input, LOGICAL_OR(ea.has_output) AS has_output, LOGICAL_OR(ea.has_reasoning) AS has_reasoning FROM emitted_ancestors AS ea LEFT JOIN emitted_ancestors AS reverse_path ON reverse_path.trace_id = ea.trace_id AND reverse_path.service_name IS NOT DISTINCT FROM ea.service_name AND reverse_path.descendant_span_id = ea.ancestor_span_id AND reverse_path.ancestor_span_id = ea.descendant_span_id WHERE (ea.has_input OR ea.has_output OR ea.has_reasoning) AND ( reverse_path.ancestor_span_id IS NULL OR ea.descendant_start_time > ea.ancestor_start_time OR ( ea.descendant_start_time = ea.ancestor_start_time AND ea.descendant_span_id > ea.ancestor_span_id ) ) GROUP BY ea.trace_id, ea.service_name, ea.ancestor_span_id )",
		"IF( COALESCE(token_leaf_check.has_input, FALSE) AND ( (t.gen_ai_usage_output_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_output, FALSE)) OR (t.gen_ai_usage_reasoning_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_reasoning, FALSE)) ), CAST(NULL AS INT64), t.gen_ai_usage_input_tokens ) AS gen_ai_usage_input_tokens,",
		"IF( COALESCE(token_leaf_check.has_output, FALSE) AND ( (t.gen_ai_usage_input_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_input, FALSE)) OR (t.gen_ai_usage_reasoning_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_reasoning, FALSE)) ), CAST(NULL AS INT64), t.gen_ai_usage_output_tokens ) AS gen_ai_usage_output_tokens,",
		"IF( COALESCE(token_leaf_check.has_reasoning, FALSE) AND ( (t.gen_ai_usage_input_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_input, FALSE)) OR (t.gen_ai_usage_output_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_output, FALSE)) ), CAST(NULL AS INT64), t.gen_ai_usage_reasoning_tokens ) AS gen_ai_usage_reasoning_tokens,",
		"ROW_NUMBER() OVER ( PARTITION BY t.trace_id, t.service_name ORDER BY t.start_time ASC, t.span_id ASC ) AS step_index,",
		"(leaf_check.span_id IS NULL) AS is_leaf,",
		"( (t.gen_ai_usage_input_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_input, FALSE)) OR (t.gen_ai_usage_output_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_output, FALSE)) OR (t.gen_ai_usage_reasoning_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_reasoning, FALSE)) ) AS is_token_leaf",
		"FROM trajectory AS t LEFT JOIN parent_ids AS leaf_check ON t.trace_id = leaf_check.trace_id AND t.span_id = leaf_check.span_id AND t.service_name IS NOT DISTINCT FROM leaf_check.service_name LEFT JOIN token_parent_coverage AS token_leaf_check ON t.trace_id = token_leaf_check.trace_id AND t.span_id = token_leaf_check.span_id AND t.service_name IS NOT DISTINCT FROM token_leaf_check.service_name",
	)

	tvfSQL := fmt.Sprintf(schema.GenAIAgentTrajectoryTVFDDL, confProject, confDataset)
	requireSQL(t, "tvf_gen_ai_agent_trajectory", tvfSQL,
		"WHERE start_time >= TIMESTAMP_SUB(start_ts, INTERVAL 24 HOUR) AND start_time <= TIMESTAMP_ADD(end_ts, INTERVAL 1 HOUR)",
		"AND p.start_time BETWEEN TIMESTAMP_SUB(s.start_time, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(s.start_time, INTERVAL 1 HOUR) ) AS in_tree_window,",
		"FROM spans AS s LEFT JOIN spans AS p ON p.trace_id = s.trace_id AND p.span_id = s.parent_span_id AND p.service_name IS NOT DISTINCT FROM s.service_name",
		"hop1 AS ( SELECT h.* EXCEPT (top_parent_id, chain), IF(anc.span_id IS NOT NULL, anc.top_parent_id, h.top_parent_id) AS top_parent_id, IF(anc.span_id IS NOT NULL, ARRAY_CONCAT(anc.chain, h.chain), h.chain) AS chain FROM hop0 AS h LEFT JOIN hop0 AS anc ON h.top_parent_id IS NOT NULL AND anc.trace_id = h.trace_id AND anc.service_name IS NOT DISTINCT FROM h.service_name AND anc.span_id = h.top_parent_id )",
		"hop2 AS ( SELECT h.* EXCEPT (top_parent_id, chain), IF(anc.span_id IS NOT NULL, anc.top_parent_id, h.top_parent_id) AS top_parent_id, IF(anc.span_id IS NOT NULL, ARRAY_CONCAT(anc.chain, h.chain), h.chain) AS chain FROM hop1 AS h LEFT JOIN hop1 AS anc ON h.top_parent_id IS NOT NULL AND anc.trace_id = h.trace_id AND anc.service_name IS NOT DISTINCT FROM h.service_name AND anc.span_id = h.top_parent_id )",
		"hop4 AS ( SELECT h.* EXCEPT (top_parent_id, chain), IF(anc.span_id IS NOT NULL, anc.top_parent_id, h.top_parent_id) AS top_parent_id, IF(anc.span_id IS NOT NULL, ARRAY_CONCAT(anc.chain, h.chain), h.chain) AS chain FROM hop2 AS h LEFT JOIN hop2 AS anc ON h.top_parent_id IS NOT NULL AND anc.trace_id = h.trace_id AND anc.service_name IS NOT DISTINCT FROM h.service_name AND anc.span_id = h.top_parent_id )",
		"hop8 AS ( SELECT h.* EXCEPT (top_parent_id, chain), IF(anc.span_id IS NOT NULL, anc.top_parent_id, h.top_parent_id) AS top_parent_id, IF(anc.span_id IS NOT NULL, ARRAY_CONCAT(anc.chain, h.chain), h.chain) AS chain FROM hop4 AS h LEFT JOIN hop4 AS anc ON h.top_parent_id IS NOT NULL AND anc.trace_id = h.trace_id AND anc.service_name IS NOT DISTINCT FROM h.service_name AND anc.span_id = h.top_parent_id )",
		"hop16 AS ( SELECT h.* EXCEPT (top_parent_id, chain), IF(anc.span_id IS NOT NULL, anc.top_parent_id, h.top_parent_id) AS top_parent_id, IF(anc.span_id IS NOT NULL, ARRAY_CONCAT(anc.chain, h.chain), h.chain) AS chain FROM hop8 AS h LEFT JOIN hop8 AS anc ON h.top_parent_id IS NOT NULL AND anc.trace_id = h.trace_id AND anc.service_name IS NOT DISTINCT FROM h.service_name AND anc.span_id = h.top_parent_id )",
		"hop32 AS ( SELECT h.* EXCEPT (top_parent_id, chain), IF(anc.span_id IS NOT NULL, anc.top_parent_id, h.top_parent_id) AS top_parent_id, IF(anc.span_id IS NOT NULL, ARRAY_CONCAT(anc.chain, h.chain), h.chain) AS chain FROM hop16 AS h LEFT JOIN hop16 AS anc ON h.top_parent_id IS NOT NULL AND anc.trace_id = h.trace_id AND anc.service_name IS NOT DISTINCT FROM h.service_name AND anc.span_id = h.top_parent_id )",
		"rooted_chains AS ( SELECT h.*, ( SELECT MAX(idx) FROM UNNEST(h.chain) AS n WITH OFFSET AS idx WHERE NOT n.in_tree_window ) AS tree_start_idx FROM hop32 AS h )",
		"(ARRAY_LENGTH(h.chain) - 1 - h.tree_start_idx) AS depth, h.chain[OFFSET(h.tree_start_idx)].span_id AS root_span_id,",
		"h.chain[OFFSET(h.tree_start_idx)].is_orphan AS is_orphan, ROW_NUMBER() OVER ( PARTITION BY h.trace_id, h.service_name ORDER BY h.start_time ASC, h.span_id ASC ) AS step_index, h.tree_start_idx AS tree_start_idx, h.chain AS chain FROM rooted_chains AS h WHERE h.tree_start_idx IS NOT NULL AND (ARRAY_LENGTH(h.chain) - 1 - h.tree_start_idx) <= 16",
		"emitted_ancestors AS ( SELECT DISTINCT t.trace_id, t.service_name, t.span_id AS descendant_span_id, t.start_time AS descendant_start_time, t.gen_ai_usage_input_tokens IS NOT NULL AS has_input, t.gen_ai_usage_output_tokens IS NOT NULL AS has_output, t.gen_ai_usage_reasoning_tokens IS NOT NULL AS has_reasoning, anc.span_id AS ancestor_span_id, anc.start_time AS ancestor_start_time FROM trajectory AS t INNER JOIN UNNEST(t.chain) AS anc WITH OFFSET AS a_idx WHERE a_idx < ARRAY_LENGTH(t.chain) - 1 AND a_idx >= ARRAY_LENGTH(t.chain) - 33 AND NOT EXISTS ( SELECT 1 FROM UNNEST(t.chain) AS x WITH OFFSET AS x_idx WHERE x_idx > a_idx AND x.span_id = anc.span_id ) AND ( a_idx >= t.tree_start_idx OR ( a_idx - ( SELECT MAX(i2) FROM UNNEST(t.chain) AS n2 WITH OFFSET AS i2 WHERE i2 <= a_idx AND NOT n2.in_tree_window ) ) <= 16 ) )",
		"token_parent_coverage AS ( SELECT ea.trace_id, ea.service_name, ea.ancestor_span_id AS span_id, LOGICAL_OR(ea.has_input) AS has_input, LOGICAL_OR(ea.has_output) AS has_output, LOGICAL_OR(ea.has_reasoning) AS has_reasoning FROM emitted_ancestors AS ea LEFT JOIN emitted_ancestors AS reverse_path ON reverse_path.trace_id = ea.trace_id AND reverse_path.service_name IS NOT DISTINCT FROM ea.service_name AND reverse_path.descendant_span_id = ea.ancestor_span_id AND reverse_path.ancestor_span_id = ea.descendant_span_id WHERE (ea.has_input OR ea.has_output OR ea.has_reasoning) AND ( reverse_path.ancestor_span_id IS NULL OR ea.descendant_start_time > ea.ancestor_start_time OR ( ea.descendant_start_time = ea.ancestor_start_time AND ea.descendant_span_id > ea.ancestor_span_id ) ) GROUP BY ea.trace_id, ea.service_name, ea.ancestor_span_id )",
		"t.depth, t.root_span_id, t.nearest_agent_span_id, t.nearest_agent_name, t.agent_path, t.is_orphan, t.step_index,",
		"WHERE t.start_time >= start_ts AND t.start_time < end_ts",
	)
}
