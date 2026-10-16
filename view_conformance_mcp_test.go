// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"fmt"
	"math"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// mcpSpanChains are the columns v_mcp_spans promotes from attributes.
var mcpSpanChains = []attrChain{
	{alias: "mcp_method_name", keys: []string{"mcp.method.name"}},
	{alias: "mcp_protocol_version", keys: []string{"mcp.protocol.version"}},
	{alias: "mcp_session_id", keys: []string{"mcp.session.id"}},
	{alias: "mcp_resource_uri", keys: []string{"mcp.resource.uri"}},
	{alias: "jsonrpc_request_id", keys: []string{"jsonrpc.request.id"}},
	{alias: "rpc_response_status_code", keys: []string{"rpc.response.status_code"}},
	{alias: "gen_ai_operation_name", keys: []string{"gen_ai.operation.name"}},
	{alias: "gen_ai_system", keys: []string{"gen_ai.system", "gen_ai.provider.name"}},
	{alias: "gen_ai_tool_name", keys: []string{"gen_ai.tool.name"}},
	{alias: "gen_ai_tool_call_id", keys: []string{"gen_ai.tool.call.id"}},
	{alias: "gen_ai_prompt_name", keys: []string{"gen_ai.prompt.name"}},
	{alias: "gen_ai_agent_name", keys: []string{"gen_ai.agent.name", "gcp.vertex.agent.name", "gen_ai.main_agent.name"}},
	{alias: "error_type", keys: []string{"error.type"}},
	{alias: "server_address", keys: []string{"server.address"}},
	{alias: "server_port", keys: []string{"server.port"}, asInt: true},
	{alias: "network_transport", keys: []string{"network.transport"}},
}

// mcpSpansView evaluates v_mcp_spans over the rows of the span table.
func mcpSpansView(t testing.TB, rawSpans []map[string]any) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, s := range spansDedup.apply(rawSpans) {
		a := parseAttrs(t, s)
		_, hasMethod := a.jsonValue("mcp.method.name")
		_, hasSession := a.jsonValue("mcp.session.id")
		_, hasResourceURI := a.jsonValue("mcp.resource.uri")
		if !hasMethod && !hasSession && !hasResourceURI {
			continue
		}

		row := make(map[string]any)
		for _, col := range []string{
			"start_time", "end_time", "duration_nano", "trace_id", "span_id",
			"parent_span_id", "service_name", "payload_refs", "attributes",
			"resource", "instrumentation_scope",
		} {
			if v, ok := s[col]; ok && v != nil {
				row[col] = v
			}
		}
		if name, ok := s["name"]; ok && name != nil {
			row["span_name"] = name
		}
		if kind, ok := s["kind"]; ok && kind != nil {
			row["span_kind"] = kind
		}
		if st, ok := s["status"].(map[string]any); ok {
			if code, ok := st["code"]; ok && code != nil {
				row["status_code"] = code
			}
			if msg, ok := st["message"]; ok && msg != nil {
				row["status_message"] = msg
			}
		}
		if ns, ok := colInt(s, "duration_nano"); ok {
			row["duration_ms"] = math.Round(float64(ns)/1e6*1000) / 1000
		}

		for _, c := range mcpSpanChains {
			if v := c.eval(s, a); v != nil {
				row[c.alias] = v
			}
		}

		method, ok := colString(row, "mcp_method_name")
		row["is_tool_call"] = ok && method == "tools/call"

		_, hasOp := a.jsonValue("gen_ai.operation.name")
		_, hasSys := a.jsonValue("gen_ai.system")
		_, hasProv := a.jsonValue("gen_ai.provider.name")
		_, hasInv := a.jsonValue("gcp.vertex.agent.invocation_id")
		_, hasMainName := a.jsonValue("gen_ai.main_agent.name")
		_, hasMainID := a.jsonValue("gen_ai.main_agent.id")
		row["also_in_gen_ai_view"] = hasOp || hasSys || hasProv || hasInv || hasMainName || hasMainID

		out = append(out, row)
	}
	return out
}

// mcpTargetName is COALESCE(gen_ai_tool_name, mcp_resource_uri, gen_ai_prompt_name).
func mcpTargetName(s map[string]any) (string, bool) {
	for _, col := range []string{"gen_ai_tool_name", "mcp_resource_uri", "gen_ai_prompt_name"} {
		if v, ok := colString(s, col); ok {
			return v, true
		}
	}
	return "", false
}

// mcpToolUsageView evaluates v_mcp_tool_usage over the rows of v_mcp_spans,
// without its approximate percentile columns.
func mcpToolUsageView(spans []map[string]any) []map[string]any {
	groups := make(map[string]map[string]any)
	errCounts := make(map[string]map[string]int64)
	durationSums := make(map[string]float64)
	var order []string

	for _, s := range spans {
		start, _ := colInt(s, "start_time")
		hour := start - start%time.Hour.Microseconds()
		target, hasTarget := mcpTargetName(s)

		key := groupKey(
			hour,
			s["service_name"],
			s["mcp_method_name"],
			s["mcp_protocol_version"],
			s["gen_ai_tool_name"],
			nilUnless(hasTarget, target),
			s["span_kind"],
			s["server_address"],
		)

		g, ok := groups[key]
		if !ok {
			g = map[string]any{
				"usage_hour":               hour,
				"call_count":               int64(0),
				"gen_ai_tagged_call_count": int64(0),
				"error_count":              int64(0),
				"typed_error_count":        int64(0),
			}
			for _, col := range []string{
				"service_name", "mcp_method_name", "mcp_protocol_version",
				"gen_ai_tool_name", "span_kind", "server_address",
			} {
				if v, ok := s[col]; ok && v != nil {
					g[col] = v
				}
			}
			if hasTarget {
				g["mcp_target_name"] = target
			}
			groups[key] = g
			errCounts[key] = make(map[string]int64)
			order = append(order, key)
		}

		g["call_count"] = g["call_count"].(int64) + 1
		if alsoInGenAI, _ := s["also_in_gen_ai_view"].(bool); alsoInGenAI {
			g["gen_ai_tagged_call_count"] = g["gen_ai_tagged_call_count"].(int64) + 1
		}
		code, _ := colInt(s, "status_code")
		et, hasET := colString(s, "error_type")
		if code == 2 || hasET {
			g["error_count"] = g["error_count"].(int64) + 1
		}
		if hasET {
			g["typed_error_count"] = g["typed_error_count"].(int64) + 1
			errCounts[key][et]++
		}
		if d, ok := s["duration_ms"].(float64); ok {
			durationSums[key] += d
		}
	}

	out := make([]map[string]any, 0, len(order))
	for _, k := range order {
		g := groups[k]
		calls := g["call_count"].(int64)
		errs := g["error_count"].(int64)
		g["top_error_types"] = topErrorTypes(errCounts[k], 5)
		g["error_rate"] = float64(errs) / float64(calls)
		g["avg_duration_ms"] = math.Round(durationSums[k]/float64(calls)*1000) / 1000
		out = append(out, g)
	}
	return out
}

func TestMCPCompanionViews(t *testing.T) {
	rig := newConformanceRig(t)
	td := loadTraces(t, "mcp/traces.json")
	rig.pushTraces(t, td)
	rig.pushTraces(t, td) // retry duplicate batch; dedup view keeps 5 spans

	raw := rig.srv.Rows(confSpans)
	if len(raw) != 10 {
		t.Fatalf("span table rows = %d, want 10 (two pushes of 5 spans)", len(raw))
	}

	mcpSpans := mcpSpansView(t, raw)
	if len(mcpSpans) != 5 {
		t.Fatalf("v_mcp_spans rows = %d, want 5", len(mcpSpans))
	}

	// Only the dual-tagged tools/call span appears in v_gen_ai_spans.
	genAISpans := genAISpansView(t, raw)
	if len(genAISpans) != 1 {
		t.Fatalf("v_gen_ai_spans rows = %d, want 1", len(genAISpans))
	}
	if got := genAISpans[0]["span_id"]; got != "9f8e7d6c5b4a0003" {
		t.Errorf("v_gen_ai_spans span_id = %v, want 9f8e7d6c5b4a0003", got)
	}

	requireCols(t, "initialize", onlyRow(t, "initialize", mcpSpans, "span_id", "9f8e7d6c5b4a0001"), map[string]any{
		"service_name":             "mcp-bigquery-gateway",
		"span_name":                "initialize",
		"span_kind":                int64(3),
		"status_code":              int64(1),
		"duration_ms":              float64(100),
		"mcp_method_name":          "initialize",
		"mcp_protocol_version":     "2025-06-18",
		"mcp_session_id":           "mcp-sess-100",
		"mcp_resource_uri":         nil,
		"jsonrpc_request_id":       "req-1",
		"rpc_response_status_code": "OK",
		"gen_ai_operation_name":    nil,
		"gen_ai_system":            nil,
		"gen_ai_tool_name":         nil,
		"is_tool_call":             false,
		"also_in_gen_ai_view":      false,
	})

	requireCols(t, "tools/list", onlyRow(t, "tools/list", mcpSpans, "span_id", "9f8e7d6c5b4a0002"), map[string]any{
		"span_name":                "tools/list",
		"span_kind":                int64(3),
		"duration_ms":              float64(100),
		"mcp_method_name":          "tools/list",
		"mcp_session_id":           "mcp-sess-100",
		"jsonrpc_request_id":       "req-2",
		"rpc_response_status_code": nil,
		"is_tool_call":             false,
		"also_in_gen_ai_view":      false,
	})

	requireCols(t, "dual-tagged tools/call", onlyRow(t, "tools/call ok", mcpSpans, "span_id", "9f8e7d6c5b4a0003"), map[string]any{
		"span_name":             "execute_tool query_bigquery",
		"span_kind":             int64(3),
		"duration_ms":           float64(500),
		"mcp_method_name":       "tools/call",
		"mcp_protocol_version":  "2025-06-18",
		"mcp_session_id":        "mcp-sess-100",
		"jsonrpc_request_id":    "req-3",
		"gen_ai_operation_name": "execute_tool",
		"gen_ai_system":         "gcp.vertex.agent",
		"gen_ai_tool_name":      "query_bigquery",
		"gen_ai_tool_call_id":   "call-mcp-03",
		"error_type":            nil,
		"is_tool_call":          true,
		"also_in_gen_ai_view":   true,
	})

	requireCols(t, "failed pure-MCP tools/call", onlyRow(t, "tools/call error", mcpSpans, "span_id", "9f8e7d6c5b4a0004"), map[string]any{
		"span_name":                "tools/call",
		"span_kind":                int64(3),
		"status_code":              int64(2),
		"status_message":           "internal tool error",
		"duration_ms":              float64(300),
		"mcp_method_name":          "tools/call",
		"gen_ai_operation_name":    nil,
		"gen_ai_system":            nil,
		"gen_ai_tool_name":         "run_untrusted_query",
		"error_type":               "-32603",
		"rpc_response_status_code": "-32603",
		"is_tool_call":             true,
		"also_in_gen_ai_view":      false,
	})

	requireCols(t, "resources/read SERVER", onlyRow(t, "resources/read", mcpSpans, "span_id", "9f8e7d6c5b4a0005"), map[string]any{
		"span_name":           "resources/read",
		"span_kind":           int64(2),
		"duration_ms":         float64(150),
		"mcp_method_name":     "resources/read",
		"mcp_resource_uri":    "bq://analytics-prod/otel/schema",
		"is_tool_call":        false,
		"also_in_gen_ai_view": false,
	})

	usage := mcpToolUsageView(mcpSpans)
	if len(usage) != 5 {
		t.Fatalf("v_mcp_tool_usage rows = %d, want 5: %v", len(usage), usage)
	}

	requireCols(t, "usage initialize", onlyRow(t, "usage initialize", usage, "mcp_method_name", "initialize"), map[string]any{
		"usage_hour":               hourMicros,
		"service_name":             "mcp-bigquery-gateway",
		"mcp_protocol_version":     "2025-06-18",
		"gen_ai_tool_name":         nil,
		"mcp_target_name":          nil,
		"span_kind":                int64(3),
		"server_address":           nil,
		"call_count":               int64(1),
		"gen_ai_tagged_call_count": int64(0),
		"error_count":              int64(0),
		"typed_error_count":        int64(0),
		"top_error_types":          []map[string]any{},
		"error_rate":               float64(0),
		"avg_duration_ms":          float64(100),
	})

	requireCols(t, "usage query_bigquery", onlyRow(t, "usage query_bigquery", usage, "gen_ai_tool_name", "query_bigquery"), map[string]any{
		"mcp_method_name":          "tools/call",
		"mcp_target_name":          "query_bigquery",
		"call_count":               int64(1),
		"gen_ai_tagged_call_count": int64(1),
		"error_count":              int64(0),
		"typed_error_count":        int64(0),
		"top_error_types":          []map[string]any{},
		"error_rate":               float64(0),
		"avg_duration_ms":          float64(500),
	})

	requireCols(t, "usage run_untrusted_query", onlyRow(t, "usage run_untrusted_query", usage, "gen_ai_tool_name", "run_untrusted_query"), map[string]any{
		"mcp_method_name":          "tools/call",
		"mcp_target_name":          "run_untrusted_query",
		"call_count":               int64(1),
		"gen_ai_tagged_call_count": int64(0),
		"error_count":              int64(1),
		"typed_error_count":        int64(1),
		"top_error_types":          []map[string]any{{"value": "-32603", "count": int64(1)}},
		"error_rate":               float64(1),
		"avg_duration_ms":          float64(300),
	})

	requireCols(t, "usage resources/read", onlyRow(t, "usage resources/read", usage, "mcp_method_name", "resources/read"), map[string]any{
		"gen_ai_tool_name":         nil,
		"mcp_target_name":          "bq://analytics-prod/otel/schema",
		"span_kind":                int64(2),
		"call_count":               int64(1),
		"gen_ai_tagged_call_count": int64(0),
		"error_count":              int64(0),
		"avg_duration_ms":          float64(150),
	})
}

func TestMultiAgentMCPCompanionViews(t *testing.T) {
	rig := newConformanceRig(t)
	td := loadTraces(t, "multi_agent/traces.json")
	rig.pushTraces(t, td)

	raw := rig.srv.Rows(confSpans)
	if len(raw) != 8 {
		t.Fatalf("multi_agent span table rows = %d, want 8", len(raw))
	}

	mcpSpans := mcpSpansView(t, raw)
	if len(mcpSpans) != 2 {
		t.Fatalf("multi_agent v_mcp_spans rows = %d, want 2 (only the two MCP tool spans)", len(mcpSpans))
	}

	requireCols(t, "execute_sql", onlyRow(t, "execute_sql", mcpSpans, "span_id", "00f067aa0ba902bc"), map[string]any{
		"service_name":          "multi-agent-planner",
		"mcp_method_name":       "tools/call",
		"mcp_session_id":        "mcp-sess-99",
		"gen_ai_operation_name": "execute_tool",
		"gen_ai_system":         "gcp.vertex.agent",
		"gen_ai_tool_name":      "execute_sql",
		"gen_ai_tool_call_id":   "call-01",
		"gen_ai_agent_name":     "researcher",
		"error_type":            nil,
		"is_tool_call":          true,
		"also_in_gen_ai_view":   true,
	})

	requireCols(t, "search_docs", onlyRow(t, "search_docs", mcpSpans, "span_id", "00f067aa0ba902bd"), map[string]any{
		"service_name":          "multi-agent-planner",
		"mcp_method_name":       "tools/call",
		"mcp_session_id":        "mcp-sess-99",
		"gen_ai_operation_name": "execute_tool",
		"gen_ai_system":         "gcp.vertex.agent",
		"gen_ai_tool_name":      "search_docs",
		"gen_ai_tool_call_id":   "call-02",
		"gen_ai_agent_name":     "researcher",
		"error_type":            "ToolExecutionTimeout",
		"status_code":           int64(2),
		"is_tool_call":          true,
		"also_in_gen_ai_view":   true,
	})

	usage := mcpToolUsageView(mcpSpans)
	if len(usage) != 2 {
		t.Fatalf("multi_agent v_mcp_tool_usage rows = %d, want 2", len(usage))
	}
	requireCols(t, "usage search_docs", onlyRow(t, "usage search_docs", usage, "gen_ai_tool_name", "search_docs"), map[string]any{
		"call_count":               int64(1),
		"gen_ai_tagged_call_count": int64(1),
		"error_count":              int64(1),
		"typed_error_count":        int64(1),
		"top_error_types":          []map[string]any{{"value": "ToolExecutionTimeout", "count": int64(1)}},
		"error_rate":               float64(1),
		"avg_duration_ms":          float64(700),
	})
}

func TestMCPViewsEdgeCases(t *testing.T) {
	rig := newConformanceRig(t)
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "mcp-edge-svc")
	ss := rs.ScopeSpans().AppendEmpty()
	baseTime := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

	// 1. Session-only MCP span (no mcp.method.name, no mcp.resource.uri).
	s1 := ss.Spans().AppendEmpty()
	s1.SetTraceID(pcommon.TraceID{1})
	s1.SetSpanID(pcommon.SpanID{1})
	s1.SetName("mcp.session.heartbeat")
	s1.SetKind(ptrace.SpanKindInternal)
	s1.SetStartTimestamp(pcommon.NewTimestampFromTime(baseTime))
	s1.SetEndTimestamp(pcommon.NewTimestampFromTime(baseTime.Add(20 * time.Millisecond)))
	s1.Attributes().PutStr("mcp.session.id", "sess-only-1")
	s1.Attributes().PutStr("gcp.vertex.agent.name", "vertex-worker")
	s1.Attributes().PutStr("server.address", "mcp.internal")
	s1.Attributes().PutInt("server.port", 8443)
	s1.Attributes().PutStr("network.transport", "tcp")

	// 2. Prompt-only MCP span (mcp_target_name falls back to gen_ai.prompt.name)
	// with status_code = 2 but no error.type (error_count increments, typed_error_count does not).
	s2 := ss.Spans().AppendEmpty()
	s2.SetTraceID(pcommon.TraceID{1})
	s2.SetSpanID(pcommon.SpanID{2})
	s2.SetName("prompts/get")
	s2.SetKind(ptrace.SpanKindClient)
	s2.SetStartTimestamp(pcommon.NewTimestampFromTime(baseTime.Add(50 * time.Millisecond)))
	s2.SetEndTimestamp(pcommon.NewTimestampFromTime(baseTime.Add(150 * time.Millisecond)))
	s2.Status().SetCode(ptrace.StatusCodeError)
	s2.Attributes().PutStr("mcp.method.name", "prompts/get")
	s2.Attributes().PutStr("gen_ai.prompt.name", "sql_review_prompt")
	s2.Attributes().PutStr("gen_ai.main_agent.name", "supervisor")

	// 3. Non-MCP span in the same trace must be excluded.
	s3 := ss.Spans().AppendEmpty()
	s3.SetTraceID(pcommon.TraceID{1})
	s3.SetSpanID(pcommon.SpanID{3})
	s3.SetName("http.request")
	s3.SetKind(ptrace.SpanKindClient)
	s3.SetStartTimestamp(pcommon.NewTimestampFromTime(baseTime))
	s3.SetEndTimestamp(pcommon.NewTimestampFromTime(baseTime.Add(10 * time.Millisecond)))
	s3.Attributes().PutStr("http.request.method", "POST")

	rig.pushTraces(t, td)
	mcpSpans := mcpSpansView(t, rig.srv.Rows(confSpans))
	if len(mcpSpans) != 2 {
		t.Fatalf("edge case v_mcp_spans rows = %d, want 2", len(mcpSpans))
	}

	requireCols(t, "session-only span", onlyRow(t, "session-only", mcpSpans, "span_name", "mcp.session.heartbeat"), map[string]any{
		"mcp_method_name":     nil,
		"mcp_session_id":      "sess-only-1",
		"gen_ai_agent_name":   "vertex-worker",
		"server_address":      "mcp.internal",
		"server_port":         int64(8443),
		"network_transport":   "tcp",
		"is_tool_call":        false,
		"also_in_gen_ai_view": false,
	})

	requireCols(t, "prompts/get span", onlyRow(t, "prompts/get", mcpSpans, "span_name", "prompts/get"), map[string]any{
		"mcp_method_name":     "prompts/get",
		"gen_ai_prompt_name":  "sql_review_prompt",
		"gen_ai_agent_name":   "supervisor",
		"is_tool_call":        false,
		"also_in_gen_ai_view": true, // gen_ai.main_agent.name satisfies v_gen_ai_spans WHERE
	})

	usage := mcpToolUsageView(mcpSpans)
	requireCols(t, "prompts/get usage", onlyRow(t, "prompts/get usage", usage, "mcp_method_name", "prompts/get"), map[string]any{
		"mcp_target_name":          "sql_review_prompt",
		"call_count":               int64(1),
		"gen_ai_tagged_call_count": int64(1),
		"error_count":              int64(1),
		"typed_error_count":        int64(0),
		"top_error_types":          []map[string]any{},
		"error_rate":               float64(1),
		"avg_duration_ms":          float64(100),
	})
}

func TestMCPViewModelsMatchViewSQL(t *testing.T) {
	spansSQL := fmt.Sprintf(schema.MCPSpansViewQuery, confProject, confDataset, confSpans)
	spanFragments := []string{
		"ROUND(duration_nano / 1e6, 3) AS duration_ms",
		"name AS span_name",
		"kind AS span_kind",
		"status.code AS status_code",
		"status.message AS status_message",
		"COALESCE(JSON_VALUE(attributes, '$.\"mcp.method.name\"') = 'tools/call', FALSE) AS is_tool_call",
		"( JSON_VALUE(attributes, '$.\"gen_ai.operation.name\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"gen_ai.system\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"gen_ai.provider.name\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"gcp.vertex.agent.invocation_id\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"gen_ai.main_agent.name\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"gen_ai.main_agent.id\"') IS NOT NULL ) AS also_in_gen_ai_view",
		"FROM `" + confProject + "." + confDataset + "." + confSpans + "_dedup`",
		"WHERE JSON_VALUE(attributes, '$.\"mcp.method.name\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"mcp.session.id\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"mcp.resource.uri\"') IS NOT NULL",
	}
	for _, c := range mcpSpanChains {
		spanFragments = append(spanFragments, c.sql(""))
	}
	requireSQL(t, "v_mcp_spans", spansSQL, spanFragments...)

	usageSQL := fmt.Sprintf(schema.MCPToolUsageViewQuery, confProject, confDataset)
	requireSQL(t, "v_mcp_tool_usage", usageSQL,
		"WITH mcp_tool_agg AS (",
		"TIMESTAMP_TRUNC(start_time, HOUR) AS usage_hour",
		"COALESCE(gen_ai_tool_name, mcp_resource_uri, gen_ai_prompt_name) AS mcp_target_name",
		"COUNT(*) AS call_count",
		"COUNTIF(also_in_gen_ai_view) AS gen_ai_tagged_call_count",
		"COUNTIF(status_code = 2 OR error_type IS NOT NULL) AS error_count",
		"COUNTIF(error_type IS NOT NULL) AS typed_error_count",
		"APPROX_TOP_COUNT(error_type, 6) AS raw_top_error_types",
		"SAFE_DIVIDE(COUNTIF(status_code = 2 OR error_type IS NOT NULL), COUNT(*)) AS error_rate",
		"ROUND(AVG(duration_ms), 3) AS avg_duration_ms",
		"APPROX_QUANTILES(duration_ms, 100)[OFFSET(50)] AS p50_duration_ms",
		"APPROX_QUANTILES(duration_ms, 100)[OFFSET(95)] AS p95_duration_ms",
		"APPROX_QUANTILES(duration_ms, 100)[OFFSET(99)] AS p99_duration_ms",
		"FROM `"+confProject+"."+confDataset+".v_mcp_spans`",
		"GROUP BY 1, 2, 3, 4, 5, 6, 7, 8",
		"ARRAY( SELECT AS STRUCT x.value, x.count FROM UNNEST(raw_top_error_types) AS x WHERE x.value IS NOT NULL ORDER BY x.count DESC, x.value ASC LIMIT 5 ) AS top_error_types",
		"FROM mcp_tool_agg",
	)
}
