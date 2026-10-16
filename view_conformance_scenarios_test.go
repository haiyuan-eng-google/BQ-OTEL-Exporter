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
	errCounts := make(map[string]map[string]int64)
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
			g["typed_error_count"] = int64(0)
			groups[key] = g
			errCounts[key] = make(map[string]int64)
			order = append(order, key)
		}
		g["call_count"] = g["call_count"].(int64) + 1
		if code, _ := colInt(s, "status_code"); code == 2 {
			g["error_count"] = g["error_count"].(int64) + 1
		}
		if et, ok := colString(s, "error_type"); ok {
			g["typed_error_count"] = g["typed_error_count"].(int64) + 1
			errCounts[key][et]++
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
		g["top_error_types"] = topErrorTypes(errCounts[k], 5)
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
		"WITH tool_agg AS ( SELECT TIMESTAMP_TRUNC(start_time, HOUR) AS usage_hour,",
		"service_name, gen_ai_agent_name, gen_ai_tool_name, gen_ai_tool_type,",
		"COUNT(*) AS call_count,",
		"COUNTIF(status_code = 2) AS error_count,",
		"SAFE_DIVIDE(COUNTIF(status_code = 2), COUNT(*)) AS error_rate,",
		"COUNTIF(error_type IS NOT NULL) AS typed_error_count,",
		"APPROX_TOP_COUNT(error_type, 6) AS raw_top_error_types",
		"FROM `"+confProject+"."+confDataset+".v_gen_ai_spans`",
		"WHERE gen_ai_tool_name IS NOT NULL OR gen_ai_operation_name = 'execute_tool'",
		"GROUP BY 1, 2, 3, 4, 5 )",
		"SELECT * EXCEPT (raw_top_error_types), ARRAY( SELECT AS STRUCT x.value, x.count FROM UNNEST(raw_top_error_types) AS x WHERE x.value IS NOT NULL ORDER BY x.count DESC, x.value ASC LIMIT 5 ) AS top_error_types FROM tool_agg",
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
		"gen_ai_agent_version":          "1.2.0",
		"gen_ai_main_agent_id":          "agent-planner-1",
		"gen_ai_main_agent_name":        "planner",
		"gen_ai_main_agent_description": "Top-level planner coordinating researcher and tool workers",
		"gen_ai_conversation_id":        "conv-multi-01",
		"gen_ai_usage_input_tokens":     int64(500),
		"gen_ai_usage_output_tokens":    int64(300),
		"gen_ai_usage_reasoning_tokens": int64(120),
		"status_code":                   int64(1),
		"error_type":                    nil,
	})
	requireCols(t, "span_02 (planner chat, v1.41 reasoning)", onlyRow(t, "span_02", spans, "span_id", "00f067aa0ba902b8"), map[string]any{
		"parent_span_id":                "00f067aa0ba902b7",
		"gen_ai_operation_name":         "chat",
		"gen_ai_agent_name":             "planner",
		"gen_ai_main_agent_id":          "agent-planner-1",
		"gen_ai_main_agent_name":        "planner",
		"gen_ai_main_agent_description": "Top-level planner coordinating researcher and tool workers",
		"gen_ai_request_model":          "gemini-2.5-pro",
		"gen_ai_response_model":         "gemini-2.5-pro-001",
		"gen_ai_response_id":            "resp-chat-01",
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
		"gen_ai_agent_version":          "1.0.4",
		"gen_ai_main_agent_id":          "agent-planner-1",
		"gen_ai_main_agent_name":        "planner",
		"gen_ai_main_agent_description": "Top-level planner coordinating researcher and tool workers",
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
		"gen_ai_response_id":            "resp-chat-02",
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
		"typed_error_count":      int64(0),
		"top_error_types":        []map[string]any{},
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
		"typed_error_count":      int64(1),
		"top_error_types":        []map[string]any{{"value": "rate_limited", "count": int64(1)}},
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
		"typed_error_count":      int64(0),
		"top_error_types":        []map[string]any{},
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
		"typed_error_count":      int64(2),
		"top_error_types":        []map[string]any{{"value": "ToolExecutionTimeout", "count": int64(1)}, {"value": "rate_limited", "count": int64(1)}},
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
		"typed_error_count":      int64(0),
		"top_error_types":        []map[string]any{},
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
		"typed_error_count": int64(0),
		"top_error_types":   []map[string]any{},
	})
	requireCols(t, "tool search_docs", onlyRow(t, "search_docs", tools, "gen_ai_tool_name", "search_docs"), map[string]any{
		"usage_hour":        hourMicros,
		"service_name":      "multi-agent-planner",
		"gen_ai_agent_name": "researcher",
		"gen_ai_tool_type":  nil,
		"call_count":        int64(1),
		"error_count":       int64(1),
		"error_rate":        1.0,
		"typed_error_count": int64(1),
		"top_error_types":   []map[string]any{{"value": "ToolExecutionTimeout", "count": int64(1)}},
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
		"error_type":            nil,
	})
	requireCols(t, "log error details", onlyRow(t, "operation.details", logs, "event_name", "gen_ai.client.inference.operation.details"), map[string]any{
		"span_id":               "00f067aa0ba902bb",
		"gen_ai_operation_name": "chat",
		"gen_ai_agent_name":     "researcher",
		"adk_session_id":        "conv-multi-01",
		"error_type":            "rate_limited",
	})

	// 6. v_gen_ai_metrics: all 12 points (6 legacy/duration histograms + 6
	// monotonic delta Sum inference counters) match LIKE 'gen_ai.%'.
	metrics := genAIMetricsView(t, rawMetrics)
	if len(metrics) != 12 {
		t.Fatalf("v_gen_ai_metrics rows = %d, want 12", len(metrics))
	}
	inferenceInput := rowsWhere(metrics, "metric_name", "gen_ai.client.inference.usage.input_tokens")
	if len(inferenceInput) != 2 {
		t.Fatalf("gen_ai.client.inference.usage.input_tokens rows = %d, want 2", len(inferenceInput))
	}
	requireCols(t, "semantic-conventions-genai@f3c6d0c9 input_tokens planner", onlyRow(t, "planner input", inferenceInput, "gen_ai_agent_name", "planner"), map[string]any{
		"metric_type":                      "SUM",
		"is_monotonic":                     true,
		"aggregation_temporality":          "DELTA",
		"gen_ai_provider_name":             "gcp.vertex.agent",
		"gen_ai_system":                    "gcp.vertex.agent",
		"gen_ai_token_modality":            "text",
		"gen_ai_inference_usage_dimension": "input",
		"value_int64":                      int64(200),
		"sum":                              nil,
	})
	inferenceReasoning := rowsWhere(metrics, "metric_name", "gen_ai.client.inference.usage.reasoning.output_tokens")
	requireCols(t, "semantic-conventions-genai@f3c6d0c9 reasoning_output researcher", onlyRow(t, "researcher reasoning", inferenceReasoning, "gen_ai_agent_name", "researcher"), map[string]any{
		"metric_type":                      "SUM",
		"is_monotonic":                     true,
		"aggregation_temporality":          "DELTA",
		"gen_ai_provider_name":             "gcp.vertex.agent",
		"gen_ai_system":                    "gcp.vertex.agent",
		"gen_ai_token_modality":            "text",
		"gen_ai_inference_usage_dimension": "reasoning_output",
		"value_int64":                      int64(70),
		"sum":                              nil,
	})
	legacyToken := rowsWhere(metrics, "metric_name", "gen_ai.client.token.usage")
	for _, r := range legacyToken {
		requireCols(t, "legacy token.usage", r, map[string]any{
			"gen_ai_inference_usage_dimension": r["gen_ai_token_type"],
		})
	}
	opDuration := rowsWhere(metrics, "metric_name", "gen_ai.client.operation.duration")
	for _, r := range opDuration {
		requireCols(t, "operation.duration", r, map[string]any{
			"gen_ai_inference_usage_dimension": nil,
		})
	}

	// 7. v_gen_ai_metric_exemplars: all 12 metric points carry 1 exemplar each
	// (6 pointing to planner chat 00f067aa0ba902b8, 6 to researcher retry chat
	// 00f067aa0ba902ba).
	exemplarRows := genAIMetricExemplarsView(t, rawMetrics, rawSpans)
	if len(exemplarRows) != 12 {
		t.Fatalf("v_gen_ai_metric_exemplars rows = %d, want 12", len(exemplarRows))
	}
	plannerEx := rowsWhere(exemplarRows, "span_id", "00f067aa0ba902b8")
	if len(plannerEx) != 6 {
		t.Fatalf("planner exemplar rows = %d, want 6", len(plannerEx))
	}
	requireCols(t, "planner duration exemplar", onlyRow(t, "planner duration", plannerEx, "metric_name", "gen_ai.client.operation.duration"), map[string]any{
		"service_name":           "multi-agent-planner",
		"metric_unit":            "s",
		"metric_type":            "HISTOGRAM",
		"gen_ai_operation_name":  "chat",
		"gen_ai_system":          "gcp.vertex.agent",
		"gen_ai_request_model":   "gemini-2.5-pro",
		"gen_ai_response_model":  "gemini-2.5-pro-001",
		"gen_ai_agent_name":      "planner",
		"exemplar_value_double":  1.0,
		"exemplar_value_int64":   nil,
		"trace_id":               "4bf92f3577b34da6a3ce929d0e0e4736",
		"span_name":              "chat gemini-2.5-pro",
		"span_duration_ms":       1000.0,
		"span_status_code":       int64(1),
		"span_gen_ai_agent_name": "planner",
		"adk_session_id":         "conv-multi-01",
		"span_error_type":        nil,
	})
	researcherEx := rowsWhere(exemplarRows, "span_id", "00f067aa0ba902ba")
	if len(researcherEx) != 6 {
		t.Fatalf("researcher exemplar rows = %d, want 6", len(researcherEx))
	}
	requireCols(t, "researcher reasoning exemplar", onlyRow(t, "researcher reasoning", researcherEx, "metric_name", "gen_ai.client.inference.usage.reasoning.output_tokens"), map[string]any{
		"service_name":           "multi-agent-planner",
		"metric_unit":            "{token}",
		"metric_type":            "SUM",
		"gen_ai_operation_name":  "chat",
		"gen_ai_system":          "gcp.vertex.agent",
		"gen_ai_request_model":   "gemini-2.5-flash",
		"gen_ai_response_model":  "gemini-2.5-flash-001",
		"gen_ai_agent_name":      "researcher",
		"gen_ai_token_modality":  "text",
		"exemplar_value_int64":   int64(70),
		"exemplar_value_double":  nil,
		"trace_id":               "4bf92f3577b34da6a3ce929d0e0e4736",
		"span_name":              "chat gemini-2.5-flash",
		"span_duration_ms":       400.0,
		"span_status_code":       int64(1),
		"span_gen_ai_agent_name": "researcher",
		"adk_session_id":         "conv-multi-01",
		"span_error_type":        nil,
	})
}

func TestScenarioMCPViews(t *testing.T) {
	rig := newConformanceRig(t)
	rig.pushTraces(t, loadTraces(t, "mcp/traces.json"))

	rawSpans := rig.srv.Rows("otel_spans")
	if len(rawSpans) != 5 {
		t.Fatalf("otel_spans rows = %d, want 5", len(rawSpans))
	}

	// Only the dual-tagged execute_tool span passes the v_gen_ai_spans WHERE
	// filter (via gen_ai.operation.name and gen_ai.provider.name). Failed
	// tools/call span 9f8e7d6c5b4a0004 carries gen_ai.tool.name = "run_untrusted_query"
	// and error.type = "-32603", but no gen_ai.{operation.name,provider.name,system}
	// or gen_ai.main_agent.*, so it stays in otel_spans alongside initialize,
	// tools/list, and resources/read.
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
		"gen_ai_provider_name":  "gcp.vertex.agent",
		"gen_ai_system":         "gcp.vertex.agent",
		"gen_ai_agent_name":     "mcp-bigquery-gateway",
		"status_code":           int64(1),
	})

	// Verify failed pure MCP tools/call span 9f8e7d6c5b4a0004 kept its tool
	// name and JSON-RPC error code in otel_spans even though v_gen_ai_spans
	// excluded it.
	rawErrSpan := onlyRow(t, "otel_spans 9f8e7d6c5b4a0004", rawSpans, "span_id", "9f8e7d6c5b4a0004")
	rawErrAttrs := parseAttrs(t, rawErrSpan)
	if got, _ := rawErrAttrs.jsonValue("gen_ai.tool.name"); got != "run_untrusted_query" {
		t.Errorf("span 9f8e7d6c5b4a0004 gen_ai.tool.name = %q, want run_untrusted_query", got)
	}
	if got, _ := rawErrAttrs.jsonValue("error.type"); got != "-32603" {
		t.Errorf("span 9f8e7d6c5b4a0004 error.type = %q, want -32603", got)
	}

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
		"typed_error_count": int64(0),
		"top_error_types":   []map[string]any{},
	})
}

func TestGenAIViewNewColumnsAndMainAgentOnlyFilter(t *testing.T) {
	rig := newConformanceRig(t)
	at := time.Date(2024, 8, 12, 3, 0, 0, 0, time.UTC)

	rig.pushTraces(t, buildTraces(t, "main-agent-svc",
		// Span 1 carries only gen_ai.main_agent.* and time_to_first_chunk.
		specSpan{
			trace: traceHex(1), id: spanHex(1), name: "root main agent",
			start: at, end: at.Add(time.Second),
			attrs: map[string]any{
				"gen_ai.main_agent.name":              "orchestrator",
				"gen_ai.main_agent.description":       "Coordinates workers",
				"gen_ai.response.time_to_first_chunk": 0.125,
			},
		},
		// Span 2 carries only gen_ai.main_agent.id.
		specSpan{
			trace: traceHex(2), id: spanHex(2), name: "main agent id only",
			start: at, end: at.Add(time.Second),
			attrs: map[string]any{
				"gen_ai.main_agent.id": "main-agent-99",
			},
		},
		// Span 3 carries error.type, joined by a log that lacks error.type.
		specSpan{
			trace: traceHex(3), id: spanHex(3), name: "chat failed",
			start: at, end: at.Add(time.Second), failed: true,
			attrs: map[string]any{
				"gen_ai.operation.name": "chat",
				"gen_ai.system":         "openai",
				"error.type":            "span_only_error",
			},
		},
	))
	rig.pushLogs(t, buildLogs(t, "main-agent-svc",
		specLog{
			trace: traceHex(3), span: spanHex(3), at: at.Add(10 * time.Millisecond),
			event: "gen_ai.choice",
			attrs: map[string]any{"gen_ai.system": "openai"},
		},
	))
	rig.pushMetrics(t, buildGaugeMetrics("main-agent-svc", at,
		"gen_ai.client.inference.usage.cache_read.input_tokens",
		"gen_ai.client.inference.usage.cache_write.input_tokens",
	))

	// Push a second resource whose gen_ai.main_agent.* and gen_ai.agent.version
	// live on resource.attributes rather than span/point attributes. Span 4 is a
	// GenAI chat span and inherits the resource attributes; Span 5 is a plain
	// HTTP span on the same process and must NOT be admitted to v_gen_ai_spans.
	resTraces := buildTraces(t, "res-agent-svc",
		specSpan{
			trace: traceHex(4), id: spanHex(4), name: "resource main agent chat",
			start: at, end: at.Add(time.Second),
			attrs: map[string]any{
				"gen_ai.operation.name": "chat",
				"gen_ai.provider.name":  "gcp.vertex.agent",
			},
		},
		specSpan{
			trace: traceHex(5), id: spanHex(5), name: "non-genai http span",
			start: at, end: at.Add(time.Second),
			attrs: map[string]any{"http.request.method": "POST"},
		},
	)
	resAttrs := resTraces.ResourceSpans().At(0).Resource().Attributes()
	resAttrs.PutStr("gen_ai.main_agent.id", "res-main-1")
	resAttrs.PutStr("gen_ai.main_agent.name", "res-orchestrator")
	resAttrs.PutStr("gen_ai.main_agent.description", "Resource-scoped orchestrator")
	resAttrs.PutStr("gen_ai.agent.version", "2.0.0")
	rig.pushTraces(t, resTraces)

	resMetrics := buildGaugeMetrics("res-agent-svc", at, "gen_ai.client.inference.usage.input_tokens")
	resMetrics.ResourceMetrics().At(0).Resource().Attributes().PutStr("gen_ai.main_agent.name", "res-orchestrator")
	rig.pushMetrics(t, resMetrics)

	spans := genAISpansView(t, rig.srv.Rows(confSpans))
	if len(spans) != 4 {
		t.Fatalf("v_gen_ai_spans rows = %d, want 4 (span main_agent.* admits spans; resource main_agent.* alone does not)", len(spans))
	}
	if nonGenAI := rowsWhere(spans, "span_id", spanHex(5)); len(nonGenAI) != 0 {
		t.Errorf("non-GenAI span on resource with gen_ai.main_agent.name was admitted to v_gen_ai_spans: %v", nonGenAI)
	}
	requireCols(t, "span 1 (main_agent.name + ttfc)", onlyRow(t, "span 1", spans, "span_id", spanHex(1)), map[string]any{
		"gen_ai_main_agent_name":              "orchestrator",
		"gen_ai_main_agent_description":       "Coordinates workers",
		"gen_ai_response_time_to_first_chunk": 0.125,
		"gen_ai_operation_name":               nil,
	})
	requireCols(t, "span 2 (main_agent.id)", onlyRow(t, "span 2", spans, "span_id", spanHex(2)), map[string]any{
		"gen_ai_main_agent_id": "main-agent-99",
	})
	requireCols(t, "span 4 (resource.attributes main_agent.* + agent.version)", onlyRow(t, "span 4", spans, "span_id", spanHex(4)), map[string]any{
		"gen_ai_main_agent_id":          "res-main-1",
		"gen_ai_main_agent_name":        "res-orchestrator",
		"gen_ai_main_agent_description": "Resource-scoped orchestrator",
		"gen_ai_agent_version":          "2.0.0",
		"gen_ai_operation_name":         "chat",
		"gen_ai_provider_name":          "gcp.vertex.agent",
	})

	// v_gen_ai_logs reads error_type from l.attributes only, never s.attributes.
	logs := genAILogsView(t, rig.srv.Rows(confLogs), rig.srv.Rows(confSpans))
	if len(logs) != 1 {
		t.Fatalf("v_gen_ai_logs rows = %d, want 1", len(logs))
	}
	requireCols(t, "log joined to errored span", logs[0], map[string]any{
		"gen_ai_operation_name": "chat",
		"error_type":            nil,
	})

	metrics := genAIMetricsView(t, rig.srv.Rows(confMetrics))
	requireCols(t, "cache_read_input dim", onlyRow(t, "cache_read", metrics, "metric_name", "gen_ai.client.inference.usage.cache_read.input_tokens"), map[string]any{
		"gen_ai_inference_usage_dimension": "cache_read_input",
	})
	requireCols(t, "cache_write_input dim", onlyRow(t, "cache_write", metrics, "metric_name", "gen_ai.client.inference.usage.cache_write.input_tokens"), map[string]any{
		"gen_ai_inference_usage_dimension": "cache_write_input",
	})
	requireCols(t, "resource main_agent.name fallback on metrics", onlyRow(t, "res_metric", metrics, "service_name", "res-agent-svc"), map[string]any{
		"gen_ai_agent_name":                "res-orchestrator",
		"gen_ai_inference_usage_dimension": "input",
	})
}
