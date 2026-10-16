// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"fmt"
	"slices"
	"sort"
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

// genAIAgentTrajectoryView evaluates v_gen_ai_agent_trajectory over the rows
// of v_gen_ai_spans.
func genAIAgentTrajectoryView(spans []map[string]any) []map[string]any {
	parentIDs := make(map[string]bool)
	tokenParentIDs := make(map[string]bool)
	for _, s := range spans {
		parent, hasParent := colString(s, "parent_span_id")
		if !hasParent || parent == "" {
			continue
		}
		key := groupKey(s["trace_id"], s["service_name"], parent)
		parentIDs[key] = true
		if intOrZero(s, "gen_ai_usage_input_tokens")+intOrZero(s, "gen_ai_usage_output_tokens") > 0 {
			tokenParentIDs[key] = true
		}
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
		agentName, hasAgentName := colString(s, "gen_ai_agent_name")

		row["depth"] = int64(0)
		row["root_span_id"] = spanID
		if isAgentOperation(op) || hasAgentName {
			row["nearest_agent_span_id"] = spanID
			if hasAgentName {
				row["nearest_agent_name"] = agentName
			}
		}
		if hasAgentName {
			row["agent_path"] = []string{agentName}
		} else {
			row["agent_path"] = []string{}
		}
		row["is_orphan"] = hasParent && parent != ""

		emitted = append(emitted, row)
		frontier = append(frontier, trajectoryWorkItem{
			row:     row,
			depth:   0,
			visited: []string{spanID},
		})
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
				cAgentName, cHasAgentName := colString(c, "gen_ai_agent_name")

				row["depth"] = tItem.depth + 1
				row["root_span_id"] = tRow["root_span_id"]

				if isAgentOperation(cOp) || (tRow["nearest_agent_span_id"] == nil && cHasAgentName) {
					row["nearest_agent_span_id"] = cSpanID
				} else if tRow["nearest_agent_span_id"] != nil {
					row["nearest_agent_span_id"] = tRow["nearest_agent_span_id"]
				}

				switch {
				case isAgentOperation(cOp):
					if cHasAgentName {
						row["nearest_agent_name"] = cAgentName
					} else if tRow["nearest_agent_name"] != nil {
						row["nearest_agent_name"] = tRow["nearest_agent_name"]
					}
				case tRow["nearest_agent_name"] == nil:
					if cHasAgentName {
						row["nearest_agent_name"] = cAgentName
					}
				default:
					row["nearest_agent_name"] = tRow["nearest_agent_name"]
				}

				prevPath := tRow["agent_path"].([]string)
				nextPath := slices.Clone(prevPath)
				if cHasAgentName && (len(nextPath) == 0 || nextPath[len(nextPath)-1] != cAgentName) {
					nextPath = append(nextPath, cAgentName)
				}
				row["agent_path"] = nextPath
				row["is_orphan"] = tRow["is_orphan"]

				nextVisited := append(slices.Clone(tItem.visited), cSpanID)
				emitted = append(emitted, row)
				nextFrontier = append(nextFrontier, trajectoryWorkItem{
					row:     row,
					depth:   tItem.depth + 1,
					visited: nextVisited,
				})
			}
		}
		frontier = nextFrontier
	}

	// Outer SELECT: step_index, is_leaf, and is_token_leaf per (trace_id, service_name).
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
			hasTokens := intOrZero(row, "gen_ai_usage_input_tokens")+intOrZero(row, "gen_ai_usage_output_tokens") > 0
			row["is_token_leaf"] = hasTokens && !tokenParentIDs[lk]
			out = append(out, row)
		}
	}
	return out
}

func TestScenarioMultiAgentTrajectoryView(t *testing.T) {
	rig := newConformanceRig(t)
	rig.pushTraces(t, loadTraces(t, "multi_agent/traces.json"))

	rawSpans := rig.srv.Rows("otel_spans")
	spans := genAISpansView(t, rawSpans)
	traj := genAIAgentTrajectoryView(spans)

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
		}
		if inTokens > 0 {
			row["gen_ai_usage_input_tokens"] = inTokens
		}
		return row
	}

	t.Run("two_node_cycle_disconnected", func(t *testing.T) {
		spans := []map[string]any{
			makeSpan("t-cycle-2", "span-a", "span-b", "svc", "invoke_agent", "agent-a", 0, 10),
			makeSpan("t-cycle-2", "span-b", "span-a", "svc", "invoke_agent", "agent-b", 10, 10),
		}
		got := genAIAgentTrajectoryView(spans)
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
		got := genAIAgentTrajectoryView(spans)
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
		got := genAIAgentTrajectoryView(spans)
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
		got := genAIAgentTrajectoryView(spans)
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
		got := genAIAgentTrajectoryView(spans)
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
		got := genAIAgentTrajectoryView(spans)
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
		got := genAIAgentTrajectoryView(spans)
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
}

func TestTrajectoryViewModelMatchesViewSQL(t *testing.T) {
	sql := fmt.Sprintf(schema.GenAIAgentTrajectoryViewQuery, confProject, confDataset)
	requireSQL(t, "v_gen_ai_agent_trajectory", sql,
		"WITH RECURSIVE spans AS (",
		"FROM `"+confProject+"."+confDataset+".v_gen_ai_spans`",
		"parent_ids AS ( SELECT DISTINCT trace_id, service_name, parent_span_id AS span_id FROM spans WHERE parent_span_id IS NOT NULL AND parent_span_id != '' )",
		"token_parent_ids AS ( SELECT DISTINCT trace_id, service_name, parent_span_id AS span_id FROM spans WHERE parent_span_id IS NOT NULL AND parent_span_id != '' AND (COALESCE(gen_ai_usage_input_tokens, 0) + COALESCE(gen_ai_usage_output_tokens, 0) > 0) )",
		"trajectory AS (",
		"0 AS depth, s.span_id AS root_span_id,",
		"(s.parent_span_id IS NOT NULL AND s.parent_span_id != '') AS is_orphan, [s.span_id] AS visited_span_ids",
		"FROM spans AS s LEFT JOIN spans AS p ON s.trace_id = p.trace_id AND s.parent_span_id = p.span_id AND s.service_name IS NOT DISTINCT FROM p.service_name AND p.start_time BETWEEN TIMESTAMP_SUB(s.start_time, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(s.start_time, INTERVAL 1 HOUR) WHERE s.parent_span_id IS NULL OR s.parent_span_id = '' OR p.span_id IS NULL",
		"UNION ALL",
		"t.depth + 1 AS depth, t.root_span_id AS root_span_id,",
		"t.is_orphan AS is_orphan, ARRAY_CONCAT(t.visited_span_ids, [c.span_id]) AS visited_span_ids",
		"FROM trajectory AS t JOIN spans AS c ON c.trace_id = t.trace_id AND c.parent_span_id = t.span_id AND c.service_name IS NOT DISTINCT FROM t.service_name AND c.start_time BETWEEN TIMESTAMP_SUB(t.start_time, INTERVAL 1 HOUR) AND TIMESTAMP_ADD(t.start_time, INTERVAL 24 HOUR) WHERE t.depth < 16 AND c.span_id NOT IN UNNEST(t.visited_span_ids)",
		"ROW_NUMBER() OVER ( PARTITION BY t.trace_id, t.service_name ORDER BY t.start_time ASC, t.span_id ASC ) AS step_index,",
		"(leaf_check.span_id IS NULL) AS is_leaf,",
		"( (COALESCE(t.gen_ai_usage_input_tokens, 0) + COALESCE(t.gen_ai_usage_output_tokens, 0) > 0) AND token_leaf_check.span_id IS NULL ) AS is_token_leaf",
		"FROM trajectory AS t LEFT JOIN parent_ids AS leaf_check ON t.trace_id = leaf_check.trace_id AND t.span_id = leaf_check.span_id AND t.service_name IS NOT DISTINCT FROM leaf_check.service_name LEFT JOIN token_parent_ids AS token_leaf_check ON t.trace_id = token_leaf_check.trace_id AND t.span_id = token_leaf_check.span_id AND t.service_name IS NOT DISTINCT FROM token_leaf_check.service_name",
	)
}
