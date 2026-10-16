// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"fmt"
	"testing"
	"time"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// toolTypeChain extracts gen_ai.tool.type as v_gen_ai_spans projects it for
// v_gen_ai_tool_usage.
var toolTypeChain = attrChain{alias: "gen_ai_tool_type", keys: []string{"gen_ai.tool.type"}}

// toolUsageView evaluates v_gen_ai_tool_usage over the rows of the span table,
// excluding approximate percentile columns.
func toolUsageView(t testing.TB, rawSpans []map[string]any) []map[string]any {
	t.Helper()
	deduped := spansDedup.apply(rawSpans)
	toolTypeBySpan := make(map[string]string, len(deduped))
	for _, s := range deduped {
		sid, _ := colString(s, "span_id")
		a := parseAttrs(t, s)
		if v, ok := toolTypeChain.eval(s, a).(string); ok {
			toolTypeBySpan[sid] = v
		}
	}

	spans := genAISpansView(t, rawSpans)
	groups := make(map[string]map[string]any)
	var order []string
	for _, s := range spans {
		toolName, hasToolName := colString(s, "gen_ai_tool_name")
		op, _ := colString(s, "gen_ai_operation_name")
		if !hasToolName && op != "execute_tool" {
			continue
		}
		start, _ := colInt(s, "start_time")
		hour := start - start%time.Hour.Microseconds()
		sid, _ := colString(s, "span_id")
		toolType, hasToolType := toolTypeBySpan[sid]

		key := groupKey(
			hour,
			s["service_name"],
			s["gen_ai_agent_name"],
			nilUnless(hasToolName, toolName),
			nilUnless(hasToolType, toolType),
		)
		g, ok := groups[key]
		if !ok {
			g = map[string]any{"usage_hour": hour}
			for _, col := range []string{"service_name", "gen_ai_agent_name"} {
				if v, ok := s[col]; ok {
					g[col] = v
				}
			}
			if hasToolName {
				g["gen_ai_tool_name"] = toolName
			}
			if hasToolType {
				g["gen_ai_tool_type"] = toolType
			}
			g["call_count"] = int64(0)
			g["error_count"] = int64(0)
			groups[key] = g
			order = append(order, key)
		}
		g["call_count"] = g["call_count"].(int64) + 1
		if code, _ := colInt(s, "status_code"); code == 2 {
			g["error_count"] = g["error_count"].(int64) + 1
		}
	}

	out := make([]map[string]any, 0, len(order))
	for _, k := range order {
		g := groups[k]
		calls := g["call_count"].(int64)
		errs := g["error_count"].(int64)
		if calls > 0 {
			g["error_rate"] = float64(errs) / float64(calls)
		}
		out = append(out, g)
	}
	return out
}

func TestToolUsageViewModelMatchesViewSQL(t *testing.T) {
	render := fmt.Sprintf
	requireSQL(t, "v_gen_ai_spans (tool_type)", render(schema.GenAISpansViewQuery, confProject, confDataset, confSpans),
		toolTypeChain.sql(""),
	)
	requireSQL(t, "v_gen_ai_tool_usage", render(schema.GenAIToolUsageViewQuery, confProject, confDataset),
		"SELECT TIMESTAMP_TRUNC(start_time, HOUR) AS usage_hour,",
		"service_name, gen_ai_agent_name, gen_ai_tool_name, gen_ai_tool_type,",
		"COUNT(*) AS call_count,",
		"COUNTIF(status_code = 2) AS error_count,",
		"SAFE_DIVIDE(COUNTIF(status_code = 2), COUNT(*)) AS error_rate,",
		"FROM `"+confProject+"."+confDataset+".v_gen_ai_spans`",
		"WHERE gen_ai_tool_name IS NOT NULL OR gen_ai_operation_name = 'execute_tool'",
		"GROUP BY 1, 2, 3, 4, 5",
	)
}

func TestScenarioMultiAgentViews(t *testing.T) {
	rig := newConformanceRig(t)
	rig.pushTraces(t, loadTraces(t, "multi_agent/traces.json"))
	rig.pushLogs(t, loadLogs(t, "multi_agent/logs.json"))
	rig.pushMetrics(t, loadMetrics(t, "multi_agent/metrics.json"))

	rawSpans := rig.srv.Rows("otel_spans")
	rawLogs := rig.srv.Rows("otel_logs")
	rawMetrics := rig.srv.Rows("otel_metrics")

	if len(rawSpans) != 8 {
		t.Fatalf("otel_spans rows = %d, want 8", len(rawSpans))
	}

	// 1. v_gen_ai_spans: all 8 multi_agent spans carry gen_ai.* attributes.
	spans := genAISpansView(t, rawSpans)
	if len(spans) != 8 {
		t.Fatalf("v_gen_ai_spans rows = %d, want 8", len(spans))
	}
	requireCols(t, "span_01 (root planner)", onlyRow(t, "span_01", spans, "span_id", "00f067aa0ba902b7"), map[string]any{
		"parent_span_id":                nil,
		"gen_ai_operation_name":         "invoke_agent",
		"gen_ai_agent_name":             "planner",
		"gen_ai_conversation_id":        "conv-multi-01",
		"gen_ai_usage_input_tokens":     int64(500),
		"gen_ai_usage_output_tokens":    int64(300),
		"gen_ai_usage_reasoning_tokens": int64(120),
		"status_code":                   int64(1),
		"error_type":                    nil,
	})
	requireCols(t, "span_02 (planner chat, v1.40 reasoning)", onlyRow(t, "span_02", spans, "span_id", "00f067aa0ba902b8"), map[string]any{
		"parent_span_id":                "00f067aa0ba902b7",
		"gen_ai_operation_name":         "chat",
		"gen_ai_agent_name":             "planner",
		"gen_ai_request_model":          "gemini-2.5-pro",
		"gen_ai_response_model":         "gemini-2.5-pro-001",
		"gen_ai_usage_input_tokens":     int64(200),
		"gen_ai_usage_output_tokens":    int64(100),
		"gen_ai_usage_reasoning_tokens": int64(50),
		"status_code":                   int64(1),
		"error_type":                    nil,
	})
	requireCols(t, "span_03 (delegated researcher)", onlyRow(t, "span_03", spans, "span_id", "00f067aa0ba902b9"), map[string]any{
		"parent_span_id":                "00f067aa0ba902b7",
		"gen_ai_operation_name":         "invoke_agent",
		"gen_ai_agent_name":             "researcher",
		"gen_ai_usage_input_tokens":     int64(300),
		"gen_ai_usage_output_tokens":    int64(200),
		"gen_ai_usage_reasoning_tokens": int64(70),
		"status_code":                   int64(1),
	})
	requireCols(t, "span_04b (failed chat, rate_limited)", onlyRow(t, "span_04b", spans, "span_id", "00f067aa0ba902bb"), map[string]any{
		"parent_span_id":             "00f067aa0ba902b9",
		"gen_ai_operation_name":      "chat",
		"gen_ai_agent_name":          "researcher",
		"gen_ai_request_model":       "gemini-2.5-flash",
		"gen_ai_response_model":      nil,
		"gen_ai_usage_input_tokens":  nil,
		"gen_ai_usage_output_tokens": nil,
		"status_code":                int64(2),
		"error_type":                 "rate_limited",
	})
	requireCols(t, "span_04 (retry chat, legacy reasoning)", onlyRow(t, "span_04", spans, "span_id", "00f067aa0ba902ba"), map[string]any{
		"parent_span_id":                "00f067aa0ba902b9",
		"gen_ai_operation_name":         "chat",
		"gen_ai_agent_name":             "researcher",
		"gen_ai_request_model":          "gemini-2.5-flash",
		"gen_ai_response_model":         "gemini-2.5-flash-001",
		"gen_ai_usage_input_tokens":     int64(300),
		"gen_ai_usage_output_tokens":    int64(200),
		"gen_ai_usage_reasoning_tokens": int64(70),
		"status_code":                   int64(1),
		"error_type":                    nil,
	})
	requireCols(t, "span_05 (parallel execute_sql)", onlyRow(t, "span_05", spans, "span_id", "00f067aa0ba902bc"), map[string]any{
		"parent_span_id":        "00f067aa0ba902b9",
		"gen_ai_operation_name": "execute_tool",
		"gen_ai_tool_name":      "execute_sql",
		"status_code":           int64(1),
		"error_type":            nil,
	})
	requireCols(t, "span_06 (parallel search_docs timeout)", onlyRow(t, "span_06", spans, "span_id", "00f067aa0ba902bd"), map[string]any{
		"parent_span_id":        "00f067aa0ba902b9",
		"gen_ai_operation_name": "execute_tool",
		"gen_ai_tool_name":      "search_docs",
		"status_code":           int64(2),
		"error_type":            "ToolExecutionTimeout",
	})
	requireCols(t, "span_orphan (missing parent)", onlyRow(t, "span_orphan", spans, "span_id", "1122334455667788"), map[string]any{
		"parent_span_id":         "deadbeefcafebabe",
		"gen_ai_operation_name":  "invoke_agent",
		"gen_ai_agent_name":      "late_worker",
		"gen_ai_conversation_id": "conv-multi-02",
	})

	// 2. v_gen_ai_model_costs: counts only the 3 leaf chat calls (span_02,
	// span_04b, span_04). Parent invoke_agent spans (span_01 and span_03) have
	// token-reporting direct children, so their aggregate tokens are not
	// double-counted.
	costs := modelCostsView(spans)
	if len(costs) != 3 {
		t.Fatalf("v_gen_ai_model_costs rows = %d, want 3: %v", len(costs), costs)
	}
	if got := sumCol(costs, "call_count"); got != 3 {
		t.Errorf("total call_count = %d, want 3", got)
	}
	if got := sumCol(costs, "error_count"); got != 1 {
		t.Errorf("total error_count = %d, want 1", got)
	}
	if got := sumCol(costs, "total_input_tokens"); got != 500 {
		t.Errorf("total_input_tokens = %d, want 500 (200 + 300, no double counting)", got)
	}
	if got := sumCol(costs, "total_output_tokens"); got != 300 {
		t.Errorf("total_output_tokens = %d, want 300 (100 + 200, no double counting)", got)
	}
	if got := sumCol(costs, "total_reasoning_tokens"); got != 120 {
		t.Errorf("total_reasoning_tokens = %d, want 120 (50 + 70, no double counting)", got)
	}
	requireCols(t, "costs (planner gemini-2.5-pro-001)", onlyRow(t, "pro-001", costs, "model", "gemini-2.5-pro-001"), map[string]any{
		"usage_hour":             hourMicros,
		"service_name":           "multi-agent-planner",
		"gen_ai_agent_name":      "planner",
		"provider":               "gcp.vertex.agent",
		"call_count":             int64(1),
		"error_count":            int64(0),
		"total_input_tokens":     int64(200),
		"total_output_tokens":    int64(100),
		"total_reasoning_tokens": int64(50),
	})
	requireCols(t, "costs (researcher failed gemini-2.5-flash)", onlyRow(t, "flash-failed", costs, "model", "gemini-2.5-flash"), map[string]any{
		"usage_hour":             hourMicros,
		"service_name":           "multi-agent-planner",
		"gen_ai_agent_name":      "researcher",
		"provider":               "gcp.vertex.agent",
		"call_count":             int64(1),
		"error_count":            int64(1),
		"total_input_tokens":     int64(0),
		"total_output_tokens":    int64(0),
		"total_reasoning_tokens": int64(0),
	})
	requireCols(t, "costs (researcher retry gemini-2.5-flash-001)", onlyRow(t, "flash-001", costs, "model", "gemini-2.5-flash-001"), map[string]any{
		"usage_hour":             hourMicros,
		"service_name":           "multi-agent-planner",
		"gen_ai_agent_name":      "researcher",
		"provider":               "gcp.vertex.agent",
		"call_count":             int64(1),
		"error_count":            int64(0),
		"total_input_tokens":     int64(300),
		"total_output_tokens":    int64(200),
		"total_reasoning_tokens": int64(70),
	})

	// 3. v_gen_ai_sessions: 2 sessions (conv-multi-01 and conv-multi-02).
	sessions := sessionsView(spans)
	if len(sessions) != 2 {
		t.Fatalf("v_gen_ai_sessions rows = %d, want 2", len(sessions))
	}
	requireCols(t, "session conv-multi-01", onlyRow(t, "conv-multi-01", sessions, "session_id", "conv-multi-01"), map[string]any{
		"service_name":           "multi-agent-planner",
		"session_start_date":     "2024-08-12",
		"turn_count":             int64(1),
		"invocation_count":       int64(0),
		"tool_call_count":        int64(2),
		"error_count":            int64(2),
		"llm_call_count":         int64(3),
		"agent_invocation_count": int64(2),
		"total_input_tokens":     int64(500),
		"total_output_tokens":    int64(300),
		"total_reasoning_tokens": int64(120),
		"session_duration_ms":    int64(4000),
		"agent_name_candidates":  []string{"planner", "researcher"},
	})
	requireCols(t, "session conv-multi-02 (orphan)", onlyRow(t, "conv-multi-02", sessions, "session_id", "conv-multi-02"), map[string]any{
		"service_name":           "multi-agent-planner",
		"session_start_date":     "2024-08-12",
		"turn_count":             int64(1),
		"tool_call_count":        int64(0),
		"error_count":            int64(0),
		"llm_call_count":         int64(0),
		"agent_invocation_count": int64(1),
		"total_input_tokens":     int64(0),
		"total_output_tokens":    int64(0),
		"total_reasoning_tokens": int64(0),
		"session_duration_ms":    int64(500),
		"agent_name_candidates":  []string{"late_worker"},
	})

	// 4. v_gen_ai_tool_usage: 2 tool rows (execute_sql succeeds, search_docs fails).
	tools := toolUsageView(t, rawSpans)
	if len(tools) != 2 {
		t.Fatalf("v_gen_ai_tool_usage rows = %d, want 2", len(tools))
	}
	requireCols(t, "tool execute_sql", onlyRow(t, "execute_sql", tools, "gen_ai_tool_name", "execute_sql"), map[string]any{
		"usage_hour":        hourMicros,
		"service_name":      "multi-agent-planner",
		"gen_ai_agent_name": "researcher",
		"gen_ai_tool_type":  nil,
		"call_count":        int64(1),
		"error_count":       int64(0),
		"error_rate":        0.0,
	})
	requireCols(t, "tool search_docs", onlyRow(t, "search_docs", tools, "gen_ai_tool_name", "search_docs"), map[string]any{
		"usage_hour":        hourMicros,
		"service_name":      "multi-agent-planner",
		"gen_ai_agent_name": "researcher",
		"gen_ai_tool_type":  nil,
		"call_count":        int64(1),
		"error_count":       int64(1),
		"error_rate":        1.0,
	})

	// 5. v_gen_ai_logs: both records join their chat spans.
	logs := genAILogsView(t, rawLogs, rawSpans)
	if len(logs) != 2 {
		t.Fatalf("v_gen_ai_logs rows = %d, want 2", len(logs))
	}
	requireCols(t, "log choice", onlyRow(t, "gen_ai.choice", logs, "event_name", "gen_ai.choice"), map[string]any{
		"span_id":               "00f067aa0ba902b8",
		"gen_ai_operation_name": "chat",
		"gen_ai_agent_name":     "planner",
		"adk_session_id":        "conv-multi-01",
	})
	requireCols(t, "log error details", onlyRow(t, "operation.details", logs, "event_name", "gen_ai.client.inference.operation.details"), map[string]any{
		"span_id":               "00f067aa0ba902bb",
		"gen_ai_operation_name": "chat",
		"gen_ai_agent_name":     "researcher",
		"adk_session_id":        "conv-multi-01",
	})

	// 6. v_gen_ai_metrics: all 12 histogram points match LIKE 'gen_ai.%'.
	metrics := genAIMetricsView(t, rawMetrics)
	if len(metrics) != 12 {
		t.Fatalf("v_gen_ai_metrics rows = %d, want 12", len(metrics))
	}
}

func TestScenarioMCPViews(t *testing.T) {
	rig := newConformanceRig(t)
	rig.pushTraces(t, loadTraces(t, "mcp/traces.json"))

	rawSpans := rig.srv.Rows("otel_spans")
	if len(rawSpans) != 5 {
		t.Fatalf("otel_spans rows = %d, want 5", len(rawSpans))
	}

	// Only the dual-tagged execute_tool span carries gen_ai.* attributes; the
	// four pure MCP spans (initialize, tools/list, failed pure tools/call,
	// resources/read) stay in otel_spans and out of v_gen_ai_spans.
	spans := genAISpansView(t, rawSpans)
	if len(spans) != 1 {
		t.Fatalf("v_gen_ai_spans rows = %d, want 1 (pure MCP spans excluded)", len(spans))
	}
	requireCols(t, "dual-tagged MCP tool span", spans[0], map[string]any{
		"span_id":               "9f8e7d6c5b4a0003",
		"parent_span_id":        "9f8e7d6c5b4a0001",
		"service_name":          "mcp-bigquery-gateway",
		"gen_ai_operation_name": "execute_tool",
		"gen_ai_tool_name":      "query_bigquery",
		"gen_ai_system":         "gcp.vertex.agent",
		"gen_ai_agent_name":     "mcp-bigquery-gateway",
		"status_code":           int64(1),
	})

	// Pure tool/MCP spans contribute 0 LLM cost rows and 0 GenAI session rows.
	if costs := modelCostsView(spans); len(costs) != 0 {
		t.Errorf("v_gen_ai_model_costs rows = %d, want 0", len(costs))
	}
	if sessions := sessionsView(spans); len(sessions) != 0 {
		t.Errorf("v_gen_ai_sessions rows = %d, want 0", len(sessions))
	}

	// v_gen_ai_tool_usage picks up the dual-tagged MCP tool execution.
	tools := toolUsageView(t, rawSpans)
	if len(tools) != 1 {
		t.Fatalf("v_gen_ai_tool_usage rows = %d, want 1", len(tools))
	}
	requireCols(t, "MCP dual-tagged tool usage", tools[0], map[string]any{
		"usage_hour":        hourMicros,
		"service_name":      "mcp-bigquery-gateway",
		"gen_ai_agent_name": "mcp-bigquery-gateway",
		"gen_ai_tool_name":  "query_bigquery",
		"gen_ai_tool_type":  nil,
		"call_count":        int64(1),
		"error_count":       int64(0),
		"error_rate":        0.0,
	})
}
