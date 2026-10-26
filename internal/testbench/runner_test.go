// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package testbench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/offload"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/protoenc"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/api/option"
)

func TestGeneratorAllSevenFrameworksAndMCP(t *testing.T) {
	// Verify DefaultFrameworks matches testdata/otlp/manifest.yaml and fixture
	// scope/service conventions 1-to-1 so any mutation to ScopeName/ServiceName fails.
	wantFrameworks := map[string]FrameworkSpec{
		"google_adk": {
			Framework:     "google_adk",
			System:        "gcp.vertex.agent",
			Model:         "gemini-2.5-pro",
			ResponseModel: "gemini-2.5-pro-001",
			AgentName:     "weather_coordinator",
			AgentID:       "agent-adk-01",
			ToolName:      "get_forecast",
			ScopeName:     "gcp.vertex.agent",
			ScopeVer:      "2.6.3",
			ServiceName:   "adk-weather-agent",
			ServiceVer:    "2.6.3",
		},
		"langgraph": {
			Framework:     "langgraph",
			System:        "langsmith",
			Model:         "gpt-4o",
			ResponseModel: "gpt-4o",
			AgentName:     "research_supervisor",
			AgentID:       "agent-lg-01",
			ToolName:      "vector_store_lookup",
			ScopeName:     "langsmith.otel",
			ScopeVer:      "0.10.18",
			ServiceName:   "langgraph-research-graph",
			ServiceVer:    "1.2.0",
		},
		"openai_agents": {
			Framework:     "openai_agents",
			Provider:      "openai",
			Model:         "gpt-4o",
			ResponseModel: "gpt-4o-2024-08-06",
			AgentName:     "billing_triage",
			AgentID:       "agent-oai-01",
			ToolName:      "fetch_customer_ledger",
			ScopeName:     "opentelemetry.instrumentation.openai_agents",
			ScopeVer:      "1.0b0",
			ServiceName:   "openai-triage-agent",
			ServiceVer:    "0.20.0",
		},
		"anthropic_agent_sdk": {
			Framework:     "anthropic_agent_sdk",
			System:        "anthropic",
			Model:         "anthropic-v4-default",
			ResponseModel: "anthropic-v4-default-20260514",
			AgentName:     "code-assistant",
			AgentID:       "agent-anth-01",
			ToolName:      "sandbox_bash_exec",
			ScopeName:     "com.anthropic.agent.sdk",
			ScopeVer:      "2.1.229",
			ServiceName:   "anthropic-agent-cli",
			ServiceVer:    "2.1.229",
		},
		"crewai": {
			Framework:     "crewai",
			Model:         "gpt-4o-mini",
			ResponseModel: "gpt-4o-mini",
			AgentName:     "crew_financial_analyst",
			AgentID:       "agent-crew-01",
			ToolName:      "sec_filing_extractor",
			ScopeName:     "openinference.instrumentation.crewai",
			ScopeVer:      "1.1.12",
			ServiceName:   "crewai-analyst-crew",
			ServiceVer:    "1.15.15",
		},
		"aws_bedrock": {
			Framework:     "aws_bedrock",
			System:        "aws.bedrock",
			Model:         "amazon.nova-pro-v1:0",
			ResponseModel: "amazon.nova-pro-v1:0",
			AgentName:     "bedrock_router",
			AgentID:       "agent-bdrk-01",
			ToolName:      "retrieve_and_generate",
			ScopeName:     "opentelemetry.instrumentation.botocore.bedrock-runtime",
			ScopeVer:      "0.65b0",
			ServiceName:   "bedrock-converse-worker",
			ServiceVer:    "0.65b0",
		},
		"microsoft_agent_framework": {
			Framework:     "microsoft_agent_framework",
			Provider:      "microsoft.agent_framework",
			Model:         "gpt-4o",
			ResponseModel: "gpt-4o",
			AgentName:     "support_orchestrator",
			AgentID:       "maf-agent-09",
			ToolName:      "graph_calendar_query",
			ScopeName:     "agent_framework.observability",
			ScopeVer:      "1.12.0",
			ServiceName:   "maf-orchestrator",
			ServiceVer:    "1.12.0",
		},
	}
	gotFrameworks := DefaultFrameworks()
	if len(gotFrameworks) != 7 {
		t.Fatalf("len(DefaultFrameworks()) = %d, want 7", len(gotFrameworks))
	}
	for _, fw := range gotFrameworks {
		want, ok := wantFrameworks[fw.Framework]
		if !ok {
			t.Fatalf("unexpected framework %q", fw.Framework)
		}
		if fw != want {
			t.Fatalf("DefaultFrameworks()[%q] = %+v, want %+v", fw.Framework, fw, want)
		}
	}

	genCfg := GeneratorConfig{
		PayloadBytes:   8192,
		PlantSecrets:   true,
		UniquePayloads: true,
		Seed:           7,
		BaseTime:       time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC),
	}
	gen := NewGenerator(genCfg)
	gen2 := NewGenerator(genCfg)

	// 28 spans = 7 frameworks * 4-span tree (invoke_agent, chat, execute_tool, mcp).
	td := gen.TracesBatch(28)
	td2 := gen2.TracesBatch(28)
	if got := td.SpanCount(); got != 28 {
		t.Fatalf("TracesBatch(28) SpanCount = %d, want 28", got)
	}
	traceBytes1, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatalf("MarshalTraces(td) error: %v", err)
	}
	traceBytes2, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td2)
	if err != nil {
		t.Fatalf("MarshalTraces(td2) error: %v", err)
	}
	if !bytes.Equal(traceBytes1, traceBytes2) {
		t.Fatal("TracesBatch protobuf bytes differ across two fresh NewGenerator instances with identical config")
	}

	seenFrameworks := make(map[string]bool)
	seenOps := make(map[string]int)
	seenOpenInferenceKinds := make(map[string]int)
	seenMCP := make(map[string]bool)
	spanServiceByPair := make(map[string]string)
	var secretSpans int

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		fwAttr, ok := rs.Resource().Attributes().Get("gen_ai.framework")
		if !ok {
			t.Fatalf("resource %d missing gen_ai.framework", i)
		}
		fwName := fwAttr.Str()
		seenFrameworks[fwName] = true
		wantFW := wantFrameworks[fwName]

		svcAttr, _ := rs.Resource().Attributes().Get("service.name")
		if svcAttr.Str() != wantFW.ServiceName {
			t.Fatalf("framework %s resource service.name = %q, want %q", fwName, svcAttr.Str(), wantFW.ServiceName)
		}
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			scope := sss.At(j).Scope()
			if scope.Name() != wantFW.ScopeName || scope.Version() != wantFW.ScopeVer {
				t.Fatalf("framework %s scope = (%q, %q), want (%q, %q)",
					fwName, scope.Name(), scope.Version(), wantFW.ScopeName, wantFW.ScopeVer)
			}
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				pairKey := sp.TraceID().String() + "/" + sp.SpanID().String()
				spanServiceByPair[pairKey] = svcAttr.Str()

				if op, ok := sp.Attributes().Get("gen_ai.operation.name"); ok {
					seenOps[op.Str()]++
				}
				if oiKind, ok := sp.Attributes().Get("openinference.span.kind"); ok {
					seenOpenInferenceKinds[oiKind.Str()]++
					if fwName != "crewai" {
						t.Errorf("unexpected openinference.span.kind on framework %s", fwName)
					}
					if _, hasGenAIOp := sp.Attributes().Get("gen_ai.operation.name"); hasGenAIOp {
						t.Errorf("CrewAI OpenInference span (%s) should not carry gen_ai.operation.name", oiKind.Str())
					}
				}
				if fwName == "google_adk" {
					for _, adkKey := range []string{
						"gcp.vertex.agent.invocation_id",
						"gcp.vertex.agent.session_id",
						"gcp.vertex.agent.event_id",
						"user.id",
					} {
						if v, ok := sp.Attributes().Get(adkKey); !ok || v.Str() == "" {
							t.Errorf("google_adk span missing %s", adkKey)
						}
					}
				}
				if fwName == "langgraph" {
					if v, ok := sp.Attributes().Get("langsmith.metadata.graph_node"); !ok || v.Str() == "" {
						t.Error("langgraph span missing langsmith.metadata.graph_node")
					}
				}
				if mcpMethod, ok := sp.Attributes().Get("mcp.method.name"); ok {
					m := mcpMethod.Str()
					seenMCP[m] = true
					if _, ok := sp.Attributes().Get("server.address"); !ok {
						t.Errorf("MCP span (%s) missing server.address", m)
					}
					if _, ok := sp.Attributes().Get("jsonrpc.request.id"); !ok {
						t.Errorf("MCP span (%s) missing jsonrpc.request.id", m)
					}
					switch m {
					case "tools/call":
						if _, ok := sp.Attributes().Get("gen_ai.tool.name"); !ok {
							t.Error("MCP tools/call span missing gen_ai.tool.name")
						}
						if callID, ok := sp.Attributes().Get("gen_ai.tool.call.id"); !ok || callID.Str() == "" {
							t.Error("MCP tools/call span missing non-empty gen_ai.tool.call.id")
						}
					case "resources/read":
						if _, ok := sp.Attributes().Get("mcp.resource.uri"); !ok {
							t.Error("MCP resources/read span missing mcp.resource.uri")
						}
					case "prompts/get":
						if _, ok := sp.Attributes().Get("gen_ai.prompt.name"); !ok {
							t.Error("MCP prompts/get span missing gen_ai.prompt.name")
						}
					}
				}
				if _, ok := sp.Attributes().Get("client.api_key"); ok {
					secretSpans++
				}
				for _, payloadKey := range []string{"gen_ai.input.messages", "gen_ai.output.messages", "gen_ai.tool.call.result", "mcp.payload.content", "input.value", "output.value"} {
					if msg, ok := sp.Attributes().Get(payloadKey); ok {
						if len(msg.Str()) != 8192 {
							t.Fatalf("%s len = %d, want 8192", payloadKey, len(msg.Str()))
						}
						if !json.Valid([]byte(msg.Str())) {
							t.Fatalf("%s is not valid JSON: %.120s", payloadKey, msg.Str())
						}
					}
				}
			}
		}
	}

	if len(seenFrameworks) != 7 {
		t.Fatalf("seenFrameworks = %d (%v), want all 7 frameworks", len(seenFrameworks), seenFrameworks)
	}
	if seenOps["invoke_agent"] != 6 || seenOps["chat"] != 6 || seenOps["execute_tool"] != 12 {
		t.Fatalf("unexpected gen_ai.operation.name counts across 6 GenAI frameworks: %v, want invoke_agent=6, chat=6, execute_tool=12", seenOps)
	}
	if seenOpenInferenceKinds["AGENT"] != 1 || seenOpenInferenceKinds["LLM"] != 1 || seenOpenInferenceKinds["TOOL"] != 2 {
		t.Fatalf("unexpected CrewAI OpenInference span kinds: %v, want AGENT=1, LLM=1, TOOL=2", seenOpenInferenceKinds)
	}
	for _, wantMethod := range []string{"tools/call", "resources/read", "prompts/get"} {
		if !seenMCP[wantMethod] {
			t.Fatalf("missing MCP method %q in %v", wantMethod, seenMCP)
		}
	}
	if secretSpans == 0 {
		t.Fatal("expected planted secret attributes when PlantSecrets=true")
	}

	ld := gen.LogsBatch(28)
	ld2 := gen2.LogsBatch(28)
	if got := ld.LogRecordCount(); got != 28 {
		t.Fatalf("LogsBatch(28) LogRecordCount = %d, want 28", got)
	}
	logBytes1, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	if err != nil {
		t.Fatalf("MarshalLogs(ld) error: %v", err)
	}
	logBytes2, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld2)
	if err != nil {
		t.Fatalf("MarshalLogs(ld2) error: %v", err)
	}
	if !bytes.Equal(logBytes1, logBytes2) {
		t.Fatal("LogsBatch protobuf bytes differ across two fresh NewGenerator instances with identical config")
	}
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		svc, _ := rl.Resource().Attributes().Get("service.name")
		fwAttr, _ := rl.Resource().Attributes().Get("gen_ai.framework")
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				lr := lrs.At(k)
				key := lr.TraceID().String() + "/" + lr.SpanID().String()
				wantSvc, ok := spanServiceByPair[key]
				if !ok {
					t.Fatalf("log record correlated pair %s not found in emitted TracesBatch(28)", key)
				}
				if svc.Str() != wantSvc {
					t.Fatalf("log record pair %s service.name = %q, want %q (matching span service.name)", key, svc.Str(), wantSvc)
				}
				if fwAttr.Str() == "google_adk" {
					for _, adkKey := range []string{
						"gcp.vertex.agent.invocation_id",
						"gcp.vertex.agent.session_id",
						"gcp.vertex.agent.event_id",
						"user.id",
					} {
						if v, ok := lr.Attributes().Get(adkKey); !ok || v.Str() == "" {
							t.Errorf("google_adk log missing %s", adkKey)
						}
					}
				}
			}
		}
	}

	md := gen.MetricsBatch(28)
	md2 := gen2.MetricsBatch(28)
	if got := md.DataPointCount(); got != 28 {
		t.Fatalf("MetricsBatch(28) DataPointCount = %d, want 28", got)
	}
	metricBytes1, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	if err != nil {
		t.Fatalf("MarshalMetrics(md) error: %v", err)
	}
	metricBytes2, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md2)
	if err != nil {
		t.Fatalf("MarshalMetrics(md2) error: %v", err)
	}
	if !bytes.Equal(metricBytes1, metricBytes2) {
		t.Fatal("MetricsBatch protobuf bytes differ across two fresh NewGenerator instances with identical config")
	}

	seenMetricNames := make(map[string]int)
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		svc, _ := rm.Resource().Attributes().Get("service.name")
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				seenMetricNames[m.Name()] += m.Histogram().DataPoints().Len()
				dps := m.Histogram().DataPoints()
				for d := 0; d < dps.Len(); d++ {
					dp := dps.At(d)
					bounds := dp.ExplicitBounds().AsRaw()
					buckets := dp.BucketCounts().AsRaw()
					if len(buckets) != len(bounds)+1 {
						t.Fatalf("metric %s dp[%d] len(buckets)=%d, want len(bounds)+1=%d", m.Name(), d, len(buckets), len(bounds)+1)
					}
					var bucketSum uint64
					firstNonEmpty, lastNonEmpty := -1, -1
					for bIdx, c := range buckets {
						bucketSum += c
						if c > 0 {
							if firstNonEmpty == -1 {
								firstNonEmpty = bIdx
							}
							lastNonEmpty = bIdx
						}
					}
					if bucketSum != dp.Count() {
						t.Fatalf("metric %s dp[%d] sum(buckets)=%d, want Count=%d", m.Name(), d, bucketSum, dp.Count())
					}
					if !dp.HasMin() || !dp.HasMax() || dp.Min() > dp.Max() {
						t.Fatalf("metric %s dp[%d] invalid min/max: hasMin=%t hasMax=%t min=%v max=%v",
							m.Name(), d, dp.HasMin(), dp.HasMax(), dp.Min(), dp.Max())
					}
					if firstNonEmpty < 0 || lastNonEmpty < 0 {
						t.Fatalf("metric %s dp[%d] has 0 non-empty buckets", m.Name(), d)
					}
					if firstNonEmpty > 0 && dp.Min() <= bounds[firstNonEmpty-1] {
						t.Fatalf("metric %s dp[%d] Min=%v <= lower bound %v of first non-empty bucket %d",
							m.Name(), d, dp.Min(), bounds[firstNonEmpty-1], firstNonEmpty)
					}
					if firstNonEmpty < len(bounds) && dp.Min() > bounds[firstNonEmpty] {
						t.Fatalf("metric %s dp[%d] Min=%v > upper bound %v of first non-empty bucket %d",
							m.Name(), d, dp.Min(), bounds[firstNonEmpty], firstNonEmpty)
					}
					if lastNonEmpty > 0 && dp.Max() <= bounds[lastNonEmpty-1] {
						t.Fatalf("metric %s dp[%d] Max=%v <= lower bound %v of last non-empty bucket %d",
							m.Name(), d, dp.Max(), bounds[lastNonEmpty-1], lastNonEmpty)
					}
					if lastNonEmpty < len(bounds) && dp.Max() > bounds[lastNonEmpty] {
						t.Fatalf("metric %s dp[%d] Max=%v > upper bound %v of last non-empty bucket %d",
							m.Name(), d, dp.Max(), bounds[lastNonEmpty], lastNonEmpty)
					}
					var impliedMinSum, impliedMaxSum float64
					for bIdx, c := range buckets {
						if c == 0 {
							continue
						}
						lo := dp.Min()
						if bIdx > 0 && bounds[bIdx-1] > lo {
							lo = bounds[bIdx-1]
						}
						hi := dp.Max()
						if bIdx < len(bounds) && bounds[bIdx] < hi {
							hi = bounds[bIdx]
						}
						switch {
						case bIdx == firstNonEmpty && firstNonEmpty == lastNonEmpty:
							if c == 1 {
								impliedMinSum += dp.Min()
								impliedMaxSum += dp.Max()
							} else {
								impliedMinSum += dp.Min() + dp.Max() + float64(c-2)*lo
								impliedMaxSum += dp.Min() + dp.Max() + float64(c-2)*hi
							}
						case bIdx == firstNonEmpty:
							impliedMinSum += dp.Min() + float64(c-1)*lo
							impliedMaxSum += dp.Min() + float64(c-1)*hi
						case bIdx == lastNonEmpty:
							impliedMinSum += dp.Max() + float64(c-1)*lo
							impliedMaxSum += dp.Max() + float64(c-1)*hi
						default:
							impliedMinSum += float64(c) * lo
							impliedMaxSum += float64(c) * hi
						}
					}
					if dp.Sum() < impliedMinSum-1e-9 || dp.Sum() > impliedMaxSum+1e-9 {
						t.Fatalf("metric %s dp[%d] Sum=%v outside bucket/min/max implied range [%v, %v]",
							m.Name(), d, dp.Sum(), impliedMinSum, impliedMaxSum)
					}

					ex := dp.Exemplars().At(0)
					key := ex.TraceID().String() + "/" + ex.SpanID().String()
					wantSvc, ok := spanServiceByPair[key]
					if !ok {
						t.Fatalf("metric exemplar correlated pair %s not found in emitted TracesBatch(28)", key)
					}
					if svc.Str() != wantSvc {
						t.Fatalf("metric exemplar pair %s service.name = %q, want %q (required by v_gen_ai_metric_exemplars join)", key, svc.Str(), wantSvc)
					}
				}
			}
		}
	}
	if seenMetricNames["gen_ai.client.token.usage"] != 14 || seenMetricNames["gen_ai.client.operation.duration"] != 14 {
		t.Fatalf("unexpected metric breakdown in MetricsBatch(28): %v", seenMetricNames)
	}

	// Transform all 3 batches through internal/transform and verify row counts and
	// cross-signal trace_id/span_id/service_name join keys.
	tOpts := transform.Options{Limits: transform.DefaultLimits()}
	spanRows, spanErrs := transform.SpanRows(td, tOpts)
	if len(spanErrs) != 0 || len(spanRows) != 28 {
		t.Fatalf("transform.SpanRows: len=%d, errs=%v", len(spanRows), spanErrs)
	}
	logRows, logErrs := transform.LogRows(ld, tOpts)
	if len(logErrs) != 0 || len(logRows) != 28 {
		t.Fatalf("transform.LogRows: len=%d, errs=%v", len(logRows), logErrs)
	}
	metricRows, metricErrs := transform.MetricRows(md, tOpts)
	if len(metricErrs) != 0 || len(metricRows) != 28 {
		t.Fatalf("transform.MetricRows: len=%d, errs=%v", len(metricRows), metricErrs)
	}
	spanRowService := make(map[string]string, len(spanRows))
	for _, sr := range spanRows {
		key := sr["trace_id"].(string) + "/" + sr["span_id"].(string)
		spanRowService[key] = sr["service_name"].(string)
	}
	for _, lr := range logRows {
		key := lr["trace_id"].(string) + "/" + lr["span_id"].(string)
		if spanRowService[key] != lr["service_name"].(string) {
			t.Fatalf("transformed LogRow %s service_name = %q, want %q", key, lr["service_name"], spanRowService[key])
		}
	}
	for _, mr := range metricRows {
		exs, ok := mr["exemplars"].([]transform.Row)
		if !ok || len(exs) != 1 {
			t.Fatalf("transformed MetricRow exemplars = %T (len %d), want []transform.Row len 1", mr["exemplars"], len(exs))
		}
		key := exs[0]["trace_id"].(string) + "/" + exs[0]["span_id"].(string)
		if spanRowService[key] != mr["service_name"].(string) {
			t.Fatalf("transformed MetricRow %s service_name = %q, want %q", key, mr["service_name"], spanRowService[key])
		}
	}

	// Evaluate transformed SpanRows, LogRows, and MetricRows against the exact SQL
	// view filter predicates and parent-child trajectory invariants from
	// internal/schema/schema.go (v_gen_ai_spans, v_gen_ai_agent_trajectory,
	// v_mcp_spans, v_gen_ai_logs, v_gen_ai_metrics, v_gen_ai_metric_exemplars).
	parseAttrs := func(raw any) map[string]any {
		s, _ := raw.(string)
		if s == "" {
			return nil
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			t.Fatalf("invalid attributes JSON %q: %v", s, err)
		}
		return m
	}
	hasNonEmptyAttr := func(attrs map[string]any, keys ...string) bool {
		for _, k := range keys {
			if v, ok := attrs[k]; ok && v != nil && v != "" {
				return true
			}
		}
		return false
	}
	matchesGenAISpansView := func(attrs map[string]any) bool {
		return hasNonEmptyAttr(attrs,
			"gen_ai.operation.name",
			"gen_ai.system",
			"gen_ai.provider.name",
			"gcp.vertex.agent.invocation_id",
			"gen_ai.main_agent.name",
			"gen_ai.main_agent.id",
		)
	}
	matchesMCPSpansView := func(attrs map[string]any) bool {
		return hasNonEmptyAttr(attrs,
			"mcp.method.name",
			"mcp.session.id",
			"mcp.resource.uri",
		)
	}

	genAISpanSet := make(map[string]bool)
	var (
		genAISpanCount       int
		crewAIGenAISpanCount int
		mcpSpanCount         int
		mcpAlsoInGenAICount  int
		mcpCrewAIOnlyCount   int
	)
	for _, sr := range spanRows {
		attrs := parseAttrs(sr["attributes"])
		svc, _ := sr["service_name"].(string)
		traceID, _ := sr["trace_id"].(string)
		spanID, _ := sr["span_id"].(string)
		inGenAI := matchesGenAISpansView(attrs)
		if inGenAI {
			genAISpanCount++
			genAISpanSet[traceID+"/"+spanID+"/"+svc] = true
			if svc == "crewai-analyst-crew" {
				crewAIGenAISpanCount++
			}
		}
		if matchesMCPSpansView(attrs) {
			mcpSpanCount++
			if inGenAI {
				mcpAlsoInGenAICount++
			} else if svc == "crewai-analyst-crew" {
				mcpCrewAIOnlyCount++
			}
		}
	}
	if genAISpanCount != 24 || crewAIGenAISpanCount != 0 {
		t.Fatalf("v_gen_ai_spans matched %d/28 spans (crewai=%d), want 24/28 (crewai=0)", genAISpanCount, crewAIGenAISpanCount)
	}
	if mcpSpanCount != 7 || mcpAlsoInGenAICount != 6 || mcpCrewAIOnlyCount != 1 {
		t.Fatalf("v_mcp_spans matched %d/28 spans (also_in_gen_ai_view=%d, crewai_only=%d), want 7 (6 and 1)",
			mcpSpanCount, mcpAlsoInGenAICount, mcpCrewAIOnlyCount)
	}

	// Verify v_gen_ai_agent_trajectory parent-child linkage: every non-root span
	// in v_gen_ai_spans must have its (trace_id, parent_span_id, service_name)
	// present in v_gen_ai_spans so is_orphan is FALSE for all 24 GenAI spans.
	var (
		trajectoryRoots    int
		trajectoryChildren int
		trajectoryOrphans  int
	)
	for _, sr := range spanRows {
		attrs := parseAttrs(sr["attributes"])
		if !matchesGenAISpansView(attrs) {
			continue
		}
		svc, _ := sr["service_name"].(string)
		traceID, _ := sr["trace_id"].(string)
		parentSpanID, _ := sr["parent_span_id"].(string)
		if parentSpanID == "" {
			trajectoryRoots++
			continue
		}
		if genAISpanSet[traceID+"/"+parentSpanID+"/"+svc] {
			trajectoryChildren++
		} else {
			trajectoryOrphans++
		}
	}
	if trajectoryRoots != 6 || trajectoryChildren != 18 || trajectoryOrphans != 0 {
		t.Fatalf("v_gen_ai_agent_trajectory roots=%d, children=%d, orphans=%d, want 6, 18, 0",
			trajectoryRoots, trajectoryChildren, trajectoryOrphans)
	}

	// Verify v_gen_ai_logs and v_gen_ai_metrics view predicates across all 28 rows.
	for i, lr := range logRows {
		attrs := parseAttrs(lr["attributes"])
		evName, _ := lr["event_name"].(string)
		if evName == "" {
			if v, ok := attrs["event.name"].(string); ok {
				evName = v
			}
		}
		inGenAILogs := strings.HasPrefix(evName, "gen_ai.") ||
			hasNonEmptyAttr(attrs, "gen_ai.system", "gen_ai.provider.name", "gcp.vertex.agent.invocation_id")
		if !inGenAILogs {
			t.Fatalf("logRow[%d] did not satisfy v_gen_ai_logs WHERE predicate: event_name=%q attrs=%v", i, evName, attrs)
		}
	}
	for i, mr := range metricRows {
		mName, _ := mr["metric_name"].(string)
		if !strings.HasPrefix(mName, "gen_ai.") {
			t.Fatalf("metricRow[%d] metric_name = %q, want prefix 'gen_ai.' for v_gen_ai_metrics", i, mName)
		}
	}

	// Verify payloadFor produces valid JSON across all UniquePayloads x PlantSecrets
	// combinations at 1024 B and across every small PayloadBytes value in 1..128
	// (covering compact JSON fallbacks and exact prefix+suffix boundary sizes).
	for _, unique := range []bool{false, true} {
		for _, secrets := range []bool{false, true} {
			gCombo := NewGenerator(GeneratorConfig{
				PayloadBytes:   1024,
				UniquePayloads: unique,
				PlantSecrets:   secrets,
			})
			for seq := uint64(0); seq < 16; seq++ {
				p := gCombo.payloadFor(seq)
				if len(p) != 1024 {
					t.Fatalf("payloadFor(seq=%d, unique=%t, secrets=%t) len = %d, want 1024", seq, unique, secrets, len(p))
				}
				if !json.Valid([]byte(p)) {
					t.Fatalf("payloadFor(seq=%d, unique=%t, secrets=%t) invalid JSON: %s", seq, unique, secrets, p)
				}
			}

			for payloadBytes := 1; payloadBytes <= 128; payloadBytes++ {
				gSmall := NewGenerator(GeneratorConfig{
					PayloadBytes:   payloadBytes,
					UniquePayloads: unique,
					PlantSecrets:   secrets,
					Seed:           3,
				})
				for seq := uint64(0); seq < 16; seq++ {
					p := gSmall.payloadFor(seq)
					if !json.Valid([]byte(p)) {
						t.Fatalf("payloadFor(payloadBytes=%d, seq=%d, unique=%t, secrets=%t) invalid JSON: %s",
							payloadBytes, seq, unique, secrets, p)
					}
					if !unique && !secrets && payloadBytes >= 18 && len(p) != payloadBytes {
						t.Fatalf("payloadFor(payloadBytes=%d, seq=%d, unique=false, secrets=false) len = %d, want %d",
							payloadBytes, seq, len(p), payloadBytes)
					}
				}
				smallTraces := gSmall.TracesBatch(8)
				for i := 0; i < smallTraces.ResourceSpans().Len(); i++ {
					sss := smallTraces.ResourceSpans().At(i).ScopeSpans()
					for j := 0; j < sss.Len(); j++ {
						spans := sss.At(j).Spans()
						for k := 0; k < spans.Len(); k++ {
							sp := spans.At(k)
							for _, key := range []string{"gen_ai.input.messages", "gen_ai.output.messages", "gen_ai.tool.call.result", "mcp.payload.content", "input.value", "output.value"} {
								if v, ok := sp.Attributes().Get(key); ok && !json.Valid([]byte(v.Str())) {
									t.Fatalf("TracesBatch(payloadBytes=%d, unique=%t, secrets=%t) attr %s invalid JSON: %s",
										payloadBytes, unique, secrets, key, v.Str())
								}
							}
						}
					}
				}
				smallLogs := gSmall.LogsBatch(4)
				for i := 0; i < smallLogs.ResourceLogs().Len(); i++ {
					sls := smallLogs.ResourceLogs().At(i).ScopeLogs()
					for j := 0; j < sls.Len(); j++ {
						lrs := sls.At(j).LogRecords()
						for k := 0; k < lrs.Len(); k++ {
							if v, ok := lrs.At(k).Body().Map().Get("content"); ok && !json.Valid([]byte(v.Str())) {
								t.Fatalf("LogsBatch(payloadBytes=%d, unique=%t, secrets=%t) body.content invalid JSON: %s",
									payloadBytes, unique, secrets, v.Str())
							}
						}
					}
				}
				if got := gSmall.MetricsBatch(4).DataPointCount(); got != 4 {
					t.Fatalf("MetricsBatch(4) DataPointCount = %d, want 4", got)
				}
			}
		}
	}
}

func TestRunBQFakeAllSignalsAndRedaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	cfg := RunnerConfig{
		Mode:               ModeBQFake,
		Signals:            []string{"traces", "logs", "metrics"},
		TargetRate:         2000,
		Duration:           250 * time.Millisecond,
		BatchSize:          50,
		Workers:            2,
		StreamPoolSize:     2,
		ConnectionPoolSize: 2,
		Redaction:          true,
		PayloadBytes:       1024,
	}

	report, err := Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}
	if len(report.Results) != 3 {
		t.Fatalf("len(report.Results) = %d, want 3", len(report.Results))
	}

	for _, res := range report.Results {
		if res.Telemetry.AcknowledgedRows <= 0 {
			t.Errorf("signal %s AcknowledgedRows = %d, want > 0", res.Signal, res.Telemetry.AcknowledgedRows)
		}
		if res.Telemetry.AppendBytesTotal <= 0 {
			t.Errorf("signal %s AppendBytesTotal = %d, want > 0", res.Signal, res.Telemetry.AppendBytesTotal)
		}
		if res.AchievedRowsPerSec <= 0 {
			t.Errorf("signal %s AchievedRowsPerSec = %f, want > 0", res.Signal, res.AchievedRowsPerSec)
		}
		if res.Latency.Samples <= 0 {
			t.Errorf("signal %s Latency.Samples = %d, want > 0", res.Signal, res.Latency.Samples)
		}
		if res.Telemetry.RedactedValuesTotal <= 0 {
			t.Errorf("signal %s RedactedValuesTotal = %d, want > 0 when Redaction=true", res.Signal, res.Telemetry.RedactedValuesTotal)
		}
		if res.Telemetry.RedactedByMatchType["key"] <= 0 || res.Telemetry.RedactedByMatchType["value"] <= 0 {
			t.Errorf("signal %s RedactedByMatchType = %v, want key > 0 and value > 0", res.Signal, res.Telemetry.RedactedByMatchType)
		}
	}

	rawJSON, err := FormatJSON(report)
	if err != nil {
		t.Fatalf("FormatJSON() error: %v", err)
	}
	var decoded BenchmarkReport
	if err := json.Unmarshal([]byte(rawJSON), &decoded); err != nil {
		t.Fatalf("json.Unmarshal(FormatJSON) error: %v", err)
	}
	if len(decoded.Results) != 3 {
		t.Fatalf("decoded results = %d, want 3", len(decoded.Results))
	}

	md := FormatMarkdown(report)
	for _, want := range []string{"| `traces` |", "| `logs` |", "| `metrics` |", "Achieved Rows/s"} {
		if !strings.Contains(md, want) {
			t.Errorf("FormatMarkdown missing %q:\n%s", want, md)
		}
	}
}

func TestRunBQFakeBackpressureAndLocalOffload(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	offloadDir := t.TempDir()
	cfg := RunnerConfig{
		Mode:                  ModeBQFake,
		Signals:               []string{"traces", "logs"},
		Duration:              300 * time.Millisecond,
		BatchSize:             32,
		Workers:               2,
		StreamPoolSize:        2,
		ConnectionPoolSize:    2,
		Redaction:             true,
		Backpressure:          true,
		SimulateAckDelay:      15 * time.Millisecond,
		SimulateThrottleEvery: 2,
		PayloadBytes:          4096,
		OffloadDir:            offloadDir,
		OffloadThresholdBytes: 2048,
	}

	report, err := Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run() with backpressure and offload error: %v", err)
	}
	for _, res := range report.Results {
		if res.Telemetry.OffloadedPayloads <= 0 {
			t.Errorf("signal %s OffloadedPayloads = %d, want > 0", res.Signal, res.Telemetry.OffloadedPayloads)
		}
		if res.Telemetry.OffloadedBytes <= 0 {
			t.Errorf("signal %s OffloadedBytes = %d, want > 0", res.Signal, res.Telemetry.OffloadedBytes)
		}
		if res.Telemetry.OffloadDedupHits <= 0 {
			t.Errorf("signal %s OffloadDedupHits = %d, want > 0", res.Signal, res.Telemetry.OffloadDedupHits)
		}
		if res.Telemetry.RedactedValuesTotal <= 0 {
			t.Errorf("signal %s RedactedValuesTotal = %d, want > 0 when Redaction=true with local offload", res.Signal, res.Telemetry.RedactedValuesTotal)
		}
		if len(res.Telemetry.BackpressureEvents) == 0 {
			t.Errorf("signal %s expected BackpressureEvents to be recorded", res.Signal)
		}
		if res.Telemetry.BackpressureLevel <= 0 {
			t.Errorf("signal %s BackpressureLevel = %f, want > 0 under simulated ack delay and throttling", res.Signal, res.Telemetry.BackpressureLevel)
		}
		if res.Telemetry.Retries <= 0 {
			t.Errorf("signal %s Retries = %d, want > 0 under SimulateThrottleEvery=%d", res.Signal, res.Telemetry.Retries, cfg.SimulateThrottleEvery)
		}
	}

	// Verify Step-0 redaction ran before local-dir offload so no cleartext
	// secret patterns ever reached disk, and at least one offloaded blob on
	// disk contains the "[REDACTED]" mask replacement (non-vacuous proof that
	// planted secrets inside >= OffloadThresholdBytes payloads were redacted
	// prior to offload).
	var (
		totalBlobs    int
		redactedBlobs int
	)
	err = filepath.WalkDir(offloadDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		totalBlobs++
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		content := string(raw)
		for _, forbidden := range []string{"sk-proj-", "ya29.", "AKIA", "secret-key-"} {
			if strings.Contains(content, forbidden) {
				t.Errorf("offloaded file %s contains unredacted secret substring %q", path, forbidden)
			}
		}
		if strings.Contains(content, "[REDACTED]") {
			redactedBlobs++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking offloadDir: %v", err)
	}
	if totalBlobs == 0 {
		t.Fatal("expected offloaded files on disk in offloadDir, found 0")
	}
	if redactedBlobs == 0 {
		t.Fatalf("expected >= 1 offloaded file in %s to contain [REDACTED] placeholder, found 0 across %d files", offloadDir, totalBlobs)
	}
}

func TestRunPerBatchCallTimeoutDoesNotExitWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	// 1. Default AllowPartialFailures=false: when per-batch CallTimeout < SimulateAckDelay,
	// worker continues past delayed batches, and Run returns a non-nil error joined
	// with the underlying context.DeadlineExceeded while still returning the report.
	cfgStrict := RunnerConfig{
		Mode:               ModeBQFake,
		Signals:            []string{"traces"},
		Duration:           350 * time.Millisecond,
		BatchSize:          10,
		Workers:            1,
		StreamPoolSize:     1,
		ConnectionPoolSize: 1,
		SimulateAckDelay:   120 * time.Millisecond,
		CallTimeout:        40 * time.Millisecond,
	}
	reportStrict, errStrict := Run(ctx, cfgStrict)
	if errStrict == nil {
		t.Fatal("Run(AllowPartialFailures=false) with BatchesFailed > 0 succeeded, want non-nil error")
	}
	if !errors.Is(errStrict, context.DeadlineExceeded) && !strings.Contains(errStrict.Error(), "deadline exceeded") {
		t.Fatalf("Run(AllowPartialFailures=false) error = %v, want underlying deadline exceeded error", errStrict)
	}
	if reportStrict == nil || len(reportStrict.Results) != 1 {
		t.Fatalf("expected 1 result on strict report, got %+v", reportStrict)
	}
	if reportStrict.Results[0].BatchesFailed <= 0 || reportStrict.Results[0].BatchesSucceeded <= 0 {
		t.Fatalf("strict result BatchesFailed=%d, BatchesSucceeded=%d, want both > 0",
			reportStrict.Results[0].BatchesFailed, reportStrict.Results[0].BatchesSucceeded)
	}

	// 2. AllowPartialFailures=true permits non-zero BatchesFailed when BatchesSucceeded > 0.
	cfgAllow := cfgStrict
	cfgAllow.AllowPartialFailures = true
	report, err := Run(ctx, cfgAllow)
	if err != nil {
		t.Fatalf("Run(AllowPartialFailures=true) error: %v", err)
	}
	if len(report.Results) != 1 {
		t.Fatalf("len(report.Results) = %d, want 1", len(report.Results))
	}
	res := report.Results[0]
	if res.BatchesFailed <= 0 {
		t.Fatalf("BatchesFailed = %d, want > 0 when per-batch CallTimeout < SimulateAckDelay", res.BatchesFailed)
	}
	if res.BatchesSucceeded <= 0 {
		t.Fatalf("BatchesSucceeded = %d, want > 0 (worker must continue after per-batch CallTimeout)", res.BatchesSucceeded)
	}
	if res.BatchesAttempted != res.BatchesSucceeded+res.BatchesFailed {
		t.Fatalf("BatchesAttempted = %d, want %d+%d", res.BatchesAttempted, res.BatchesSucceeded, res.BatchesFailed)
	}
}

func TestRunUniquePayloadsAndZeroAckErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// 1. UniquePayloads=true generates fresh batches beyond 8 batches with 0 dedup hits.
	offloadDir := t.TempDir()
	uniqueCfg := RunnerConfig{
		Mode:                  ModeBQFake,
		Signals:               []string{"traces"},
		Duration:              180 * time.Millisecond,
		BatchSize:             4,
		Workers:               2,
		StreamPoolSize:        2,
		ConnectionPoolSize:    2,
		PayloadBytes:          1024,
		UniquePayloads:        true,
		OffloadDir:            offloadDir,
		OffloadThresholdBytes: 512,
	}
	uniqueRep, err := Run(ctx, uniqueCfg)
	if err != nil {
		t.Fatalf("Run(UniquePayloads=true) error: %v", err)
	}
	if uniqueRep.Results[0].BatchesSucceeded <= 8 {
		t.Fatalf("BatchesSucceeded = %d, want > 8", uniqueRep.Results[0].BatchesSucceeded)
	}
	if uniqueRep.Results[0].Telemetry.OffloadDedupHits != 0 {
		t.Fatalf("OffloadDedupHits = %d, want 0 when UniquePayloads=true", uniqueRep.Results[0].Telemetry.OffloadDedupHits)
	}

	// 2. All-rejected run (payload exceeds 256 KiB limits.max_value_bytes without offload)
	// must report AchievedRowsPerSec == 0 and return a non-nil error.
	rejectCfg := RunnerConfig{
		Mode:               ModeBQFake,
		Signals:            []string{"traces"},
		Duration:           120 * time.Millisecond,
		BatchSize:          8,
		Workers:            1,
		StreamPoolSize:     1,
		ConnectionPoolSize: 1,
		PayloadBytes:       2 * 1024 * 1024,
	}
	rejectRep, err := Run(ctx, rejectCfg)
	if err == nil {
		t.Fatal("Run() with 100% rejected rows succeeded, want non-nil error")
	}
	if rejectRep == nil || len(rejectRep.Results) != 1 {
		t.Fatalf("expected 1 result on rejected report, got %+v", rejectRep)
	}
	if rejectRep.Results[0].AchievedRowsPerSec != 0 {
		t.Fatalf("AchievedRowsPerSec = %f, want 0 when AcknowledgedRows == 0", rejectRep.Results[0].AchievedRowsPerSec)
	}
	if rejectRep.Results[0].Telemetry.RejectedRows <= 0 {
		t.Fatalf("RejectedRows = %d, want > 0", rejectRep.Results[0].Telemetry.RejectedRows)
	}

	// 3. Parent context cancellation during run must return a non-nil error.
	shortCtx, cancelShort := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancelShort()
	if _, err := Run(shortCtx, RunnerConfig{
		Mode:             ModeBQFake,
		Signals:          []string{"traces"},
		Duration:         2 * time.Second,
		BatchSize:        20,
		Workers:          1,
		SimulateAckDelay: 100 * time.Millisecond,
	}); err == nil {
		t.Fatal("Run() with canceled parent context succeeded, want non-nil error")
	}
}

func TestRunStreamPoolSweepAndValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	cfg := RunnerConfig{
		Mode:      ModeBQFake,
		Signals:   []string{"traces"},
		Duration:  150 * time.Millisecond,
		BatchSize: 40,
		Workers:   2,
	}
	report, err := RunStreamPoolSweep(ctx, cfg, []int{1, 2})
	if err != nil {
		t.Fatalf("RunStreamPoolSweep error: %v", err)
	}
	if len(report.Results) != 2 {
		t.Fatalf("len(report.Results) = %d, want 2", len(report.Results))
	}
	if report.Results[0].StreamPoolSize != 1 || report.Results[1].StreamPoolSize != 2 {
		t.Fatalf("unexpected stream pool sizes: %d, %d", report.Results[0].StreamPoolSize, report.Results[1].StreamPoolSize)
	}

	// Verify Normalize auto-generates otel_bench_<timestamp>_<random_hex> when
	// EphemeralDataset=true and DatasetID is empty, produces 1,000 collision-free
	// IDs across 1,000 tight-loop calls in the same millisecond, and allows
	// subsequent Normalize calls on the generated config.
	dsPattern := regexp.MustCompile(`^otel_bench_[0-9]+_[0-9a-f]{16}$`)
	seenDatasetIDs := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		ephemeralCfg := RunnerConfig{
			Mode:             ModeLive,
			ProjectID:        "test-project",
			EphemeralDataset: true,
		}
		if err := ephemeralCfg.Normalize(); err != nil {
			t.Fatalf("Normalize(ephemeralCfg #%d) unexpected error: %v", i, err)
		}
		if !dsPattern.MatchString(ephemeralCfg.DatasetID) {
			t.Fatalf("ephemeralCfg.DatasetID = %q, want match %s", ephemeralCfg.DatasetID, dsPattern)
		}
		if seenDatasetIDs[ephemeralCfg.DatasetID] {
			t.Fatalf("duplicate ephemeral DatasetID %q at iteration %d", ephemeralCfg.DatasetID, i)
		}
		seenDatasetIDs[ephemeralCfg.DatasetID] = true
		if err := ephemeralCfg.Normalize(); err != nil {
			t.Fatalf("second Normalize(ephemeralCfg #%d) unexpected error: %v", i, err)
		}
	}

	validMaxInflightCfg := RunnerConfig{
		Mode:                ModeBQFake,
		MaxInflightRequests: MaxBenchmarkInflightRequests,
	}
	if err := validMaxInflightCfg.Normalize(); err != nil {
		t.Fatalf("Normalize(MaxInflightRequests=%d) unexpected error: %v", MaxBenchmarkInflightRequests, err)
	}

	invalidCases := []RunnerConfig{
		{Mode: "invalid"},
		{Mode: ModeBQFake, Signals: []string{"profiles"}},
		{Mode: ModeBQFake, StreamPoolSize: 32},
		{Mode: ModeBQFake, ConnectionPoolSize: 32},
		{Mode: ModeLive, ProjectID: ""},
		{Mode: ModeLive, ProjectID: "test-project", DatasetID: ""},
		{Mode: ModeLive, ProjectID: "test-project", DatasetID: "prod_otel", EphemeralDataset: true},
		{Mode: ModeBQFake, DatasetID: "prod_otel", EphemeralDataset: true},
		{Mode: ModeBQFake, OffloadDir: "/tmp/a", OffloadGCSBucket: "bkt"},
		{Mode: ModeBQFake, OffloadGCSBucket: "bkt"},
		{Mode: ModeBQFake, Duration: -time.Second},
		{Mode: ModeBQFake, BatchSize: -1},
		{Mode: ModeBQFake, BatchSize: MaxBenchmarkBatchSize + 1},
		{Mode: ModeBQFake, Workers: -1},
		{Mode: ModeBQFake, Workers: MaxBenchmarkWorkers + 1},
		{Mode: ModeBQFake, StreamPoolSize: -1},
		{Mode: ModeBQFake, ConnectionPoolSize: -1},
		{Mode: ModeBQFake, MaxInflightRequests: -1},
		{Mode: ModeBQFake, MaxInflightRequests: MaxBenchmarkInflightRequests + 1},
		{Mode: ModeBQFake, MaxInflightRequests: math.MaxInt},
		{Mode: ModeBQFake, PayloadBytes: -1},
		{Mode: ModeBQFake, PayloadBytes: math.MaxInt},
		{Mode: ModeBQFake, BatchSize: 1000, PayloadBytes: 512 * 1024},
		{Mode: ModeBQFake, TargetRate: -1},
		{Mode: ModeBQFake, SimulateThrottleEvery: -1},
		{Mode: ModeBQFake, SimulateAckDelay: -time.Millisecond},
		{Mode: ModeBQFake, CallTimeout: -time.Millisecond},
	}
	for _, tc := range invalidCases {
		if err := tc.Normalize(); err == nil {
			t.Errorf("Normalize(%+v) succeeded, want error", tc)
		}
	}
}

func TestDeleteDatasetWithContentsAndDatasetExistsHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	var (
		getCalls           atomic.Int32
		postCalls          atomic.Int32
		deleteCalls        atomic.Int32
		postStatus         atomic.Int32
		delStatus          atomic.Int32
		commitOnPostError  atomic.Bool
		postWaitCancel     atomic.Bool
		postCancelRelease  = make(chan struct{})
		getCommittedStatus atomic.Int32
		committedMu        sync.Mutex
		committedLabels    = make(map[string]map[string]string)
	)
	defer close(postCancelRelease)
	postStatus.Store(http.StatusOK)
	delStatus.Store(http.StatusNoContent)
	getCommittedStatus.Store(http.StatusOK)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getCalls.Add(1)
			switch {
			case strings.HasSuffix(r.URL.Path, "/datasets/existing_ds"):
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"datasetReference":{"projectId":"test-proj","datasetId":"existing_ds"}}`))
				return
			case strings.HasSuffix(r.URL.Path, "/datasets/forbidden_ds"):
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":{"code":403,"message":"Access Denied"}}`))
				return
			case strings.HasSuffix(r.URL.Path, "/datasets/error_ds"):
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"code":500,"message":"Internal Server Error"}}`))
				return
			}
			parts := strings.Split(r.URL.Path, "/datasets/")
			if len(parts) == 2 && parts[1] != "" {
				dsID := parts[1]
				committedMu.Lock()
				labels, ok := committedLabels[dsID]
				committedMu.Unlock()
				if ok {
					status := int(getCommittedStatus.Load())
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(status)
					if status >= 400 {
						_, _ = fmt.Fprintf(w, `{"error":{"code":%d,"message":"Metadata Check Error"}}`, status)
						return
					}
					resp := map[string]any{
						"datasetReference": map[string]string{
							"projectId": "test-proj",
							"datasetId": dsID,
						},
						"labels": labels,
					}
					_ = json.NewEncoder(w).Encode(resp)
					return
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":404,"message":"Not found"}}`))
		case http.MethodPost:
			postCalls.Add(1)
			var body struct {
				DatasetReference struct {
					DatasetID string `json:"datasetId"`
				} `json:"datasetReference"`
				Labels map[string]string `json:"labels"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			dsID := body.DatasetReference.DatasetID
			if dsID == "" {
				dsID = "created_ds"
			}
			code := int(postStatus.Load())
			if code == http.StatusOK || commitOnPostError.Load() || postWaitCancel.Load() {
				committedMu.Lock()
				if _, exists := committedLabels[dsID]; !exists {
					copied := make(map[string]string, len(body.Labels))
					for k, v := range body.Labels {
						copied[k] = v
					}
					if dsID == "post_500_other_owner_ds" {
						copied[ephemeralOwnerLabelKey] = "foreign-owner-token"
					}
					committedLabels[dsID] = copied
				}
				committedMu.Unlock()
			}
			if postWaitCancel.Load() {
				select {
				case <-r.Context().Done():
				case <-postCancelRelease:
				case <-time.After(80 * time.Millisecond):
				}
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			if code == http.StatusConflict {
				_, _ = w.Write([]byte(`{"error":{"code":409,"message":"Already Exists"}}`))
				return
			}
			if code >= 400 {
				_, _ = fmt.Fprintf(w, `{"error":{"code":%d,"message":"Post-Commit Server Error"}}`, code)
				return
			}
			_, _ = fmt.Fprintf(w, `{"datasetReference":{"projectId":"test-proj","datasetId":%q}}`, dsID)
		case http.MethodDelete:
			deleteCalls.Add(1)
			if r.URL.Query().Get("deleteContents") != "true" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			code := int(delStatus.Load())
			if code < 400 {
				parts := strings.Split(r.URL.Path, "/datasets/")
				if len(parts) == 2 {
					committedMu.Lock()
					delete(committedLabels, parts[1])
					committedMu.Unlock()
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			if code >= 400 {
				_, _ = w.Write([]byte(`{"error":{"code":403,"message":"Delete forbidden"}}`))
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	opts := []option.ClientOption{
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	}

	exists, err := datasetExists(ctx, "test-proj", "existing_ds", opts...)
	if err != nil {
		t.Fatalf("datasetExists(existing_ds) error: %v", err)
	}
	if !exists {
		t.Fatal("datasetExists(existing_ds) = false, want true")
	}

	exists, err = datasetExists(ctx, "test-proj", "otel_bench_absent", opts...)
	if err != nil {
		t.Fatalf("datasetExists(otel_bench_absent) error: %v", err)
	}
	if exists {
		t.Fatal("datasetExists(otel_bench_absent) = true, want false")
	}

	// Verify datasetExists fails closed on non-404 HTTP errors (403 Forbidden and 500 Internal Server Error).
	if _, err := datasetExists(ctx, "test-proj", "forbidden_ds", opts...); err == nil {
		t.Fatal("datasetExists(forbidden_ds) succeeded, want non-nil error (fail closed)")
	}
	ctx500, cancel500 := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel500()
	if _, err := datasetExists(ctx500, "test-proj", "error_ds", opts...); err == nil {
		t.Fatal("datasetExists(error_ds) succeeded, want non-nil error (fail closed)")
	}

	if err := DeleteDatasetWithContents(ctx, "test-proj", "otel_bench_absent", opts...); err != nil {
		t.Fatalf("DeleteDatasetWithContents error: %v", err)
	}
	if deleteCalls.Load() != 1 {
		t.Fatalf("deleteCalls = %d, want 1", deleteCalls.Load())
	}

	// Verify createEphemeralDataset and Run/RunStreamPoolSweep ownership & pre-validation guards:
	// 1. Pre-existing dataset (GET -> 200): 0 POSTs, 0 DELETEs.
	postCalls.Store(0)
	deleteCalls.Store(0)
	if err := createEphemeralDataset(ctx, "test-proj", "existing_ds", "US", opts...); err == nil {
		t.Fatal("createEphemeralDataset(existing_ds) succeeded, want error")
	}
	if postCalls.Load() != 0 || deleteCalls.Load() != 0 {
		t.Fatalf("existing_ds triggered post=%d, delete=%d, want 0, 0", postCalls.Load(), deleteCalls.Load())
	}

	// 2. Concurrent creator race (GET -> 404, POST -> 409 Conflict): Run and RunStreamPoolSweep
	// must abort before setting owned=true and must NEVER call DELETE on the concurrently created dataset.
	postStatus.Store(http.StatusConflict)
	for _, isSweep := range []bool{false, true} {
		getCalls.Store(0)
		postCalls.Store(0)
		deleteCalls.Store(0)
		raceCfg := RunnerConfig{
			Mode:             ModeLive,
			ProjectID:        "test-proj",
			EphemeralDataset: true,
			ClientOptions:    opts,
		}
		var raceErr error
		if isSweep {
			_, raceErr = RunStreamPoolSweep(ctx, raceCfg, []int{1, 2})
		} else {
			_, raceErr = Run(ctx, raceCfg)
		}
		if raceErr == nil || !strings.Contains(raceErr.Error(), "creating ephemeral dataset") {
			t.Fatalf("isSweep=%t concurrent 409 error = %v, want creating ephemeral dataset error", isSweep, raceErr)
		}
		if postCalls.Load() != 1 || deleteCalls.Load() != 0 {
			t.Fatalf("isSweep=%t concurrent 409 post=%d, delete=%d, want 1, 0", isSweep, postCalls.Load(), deleteCalls.Load())
		}
	}

	// 3. Pre-validation failure (invalid streamPoolSizes [1, 99] or pre-canceled ctx)
	// must fail BEFORE any dataset GET/POST/DELETE HTTP request is issued.
	postStatus.Store(http.StatusOK)
	getCalls.Store(0)
	postCalls.Store(0)
	deleteCalls.Store(0)
	if _, err := RunStreamPoolSweep(ctx, RunnerConfig{
		Mode:             ModeLive,
		ProjectID:        "test-proj",
		EphemeralDataset: true,
		ClientOptions:    opts,
	}, []int{1, 99}); err == nil {
		t.Fatal("RunStreamPoolSweep([1, 99]) succeeded, want validation error")
	}
	if getCalls.Load() != 0 || postCalls.Load() != 0 || deleteCalls.Load() != 0 {
		t.Fatalf("invalid sweep pool sizes made HTTP calls get=%d, post=%d, delete=%d, want 0, 0, 0",
			getCalls.Load(), postCalls.Load(), deleteCalls.Load())
	}

	canceledCtx, cancelImmediate := context.WithCancel(ctx)
	cancelImmediate()
	if _, err := Run(canceledCtx, RunnerConfig{
		Mode:             ModeLive,
		ProjectID:        "test-proj",
		EphemeralDataset: true,
		ClientOptions:    opts,
	}); err == nil {
		t.Fatal("Run(canceledCtx) succeeded, want context.Canceled")
	}
	if getCalls.Load() != 0 || postCalls.Load() != 0 || deleteCalls.Load() != 0 {
		t.Fatalf("pre-canceled ctx made HTTP calls get=%d, post=%d, delete=%d, want 0, 0, 0",
			getCalls.Load(), postCalls.Load(), deleteCalls.Load())
	}

	// 4. Owned dataset cleanup failure (GET -> 404, POST -> 200, then exporter Start fails locally on missing ADC file, and DELETE -> 403):
	// Run must execute DELETE because it created the dataset, and must join the DELETE error
	// into the returned error while leaving EphemeralDatasetDeleted == false.
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing-adc.json"))
	delStatus.Store(http.StatusForbidden)
	getCalls.Store(0)
	postCalls.Store(0)
	deleteCalls.Store(0)
	delFailRep, delFailErr := Run(ctx, RunnerConfig{
		Mode:             ModeLive,
		ProjectID:        "test-proj",
		EphemeralDataset: true,
		Duration:         10 * time.Millisecond,
		CallTimeout:      10 * time.Millisecond,
		ClientOptions:    opts,
	})
	if delFailErr == nil || !strings.Contains(delFailErr.Error(), "deleting dataset") {
		t.Fatalf("Run() with failing DELETE error = %v, want joined 'deleting dataset' error", delFailErr)
	}
	if postCalls.Load() != 1 || deleteCalls.Load() != 1 {
		t.Fatalf("owned dataset cleanup failure post=%d, delete=%d, want 1, 1", postCalls.Load(), deleteCalls.Load())
	}
	if delFailRep != nil && delFailRep.EphemeralDatasetDeleted {
		t.Fatal("EphemeralDatasetDeleted = true when DELETE returned 403, want false")
	}

	// 5. Ambiguous dataset Create failure cleanup via ownership label verification:
	// 5a. Post-commit 500 where dataset was created with our bq_otel_testbench_owner label:
	// createEphemeralDataset must verify the label via GET and delete the leaked dataset immediately.
	// Use a short caller deadline because the BigQuery client retries 500 responses until context expiry,
	// while createEphemeralDataset runs its metadata verification + DELETE on an independent background context.
	commitOnPostError.Store(true)
	postStatus.Store(http.StatusInternalServerError)
	delStatus.Store(http.StatusNoContent)
	getCommittedStatus.Store(http.StatusOK)
	getCalls.Store(0)
	postCalls.Store(0)
	deleteCalls.Store(0)
	ctxPost500, cancelPost500 := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancelPost500()
	post500Err := createEphemeralDataset(ctxPost500, "test-proj", "post_500_owned_ds", "US", opts...)
	if post500Err == nil || !strings.Contains(post500Err.Error(), "creating ephemeral dataset test-proj.post_500_owned_ds") {
		t.Fatalf("createEphemeralDataset(post_500_owned_ds) error = %v, want creating ephemeral dataset error", post500Err)
	}
	if postCalls.Load() < 1 || deleteCalls.Load() != 1 {
		t.Fatalf("post-commit 500 with matching owner label: post=%d, delete=%d, want >=1, 1",
			postCalls.Load(), deleteCalls.Load())
	}

	// 5b. Post-commit client timeout / context cancellation during POST:
	// createEphemeralDataset must use its bounded background cleanup context to verify the
	// bq_otel_testbench_owner label and delete the dataset even though the caller ctx expired.
	commitOnPostError.Store(false)
	postWaitCancel.Store(true)
	postStatus.Store(http.StatusOK)
	delStatus.Store(http.StatusNoContent)
	getCalls.Store(0)
	postCalls.Store(0)
	deleteCalls.Store(0)
	postTimeoutCtx, cancelPostTimeout := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancelPostTimeout()
	timeoutCreateErr := createEphemeralDataset(postTimeoutCtx, "test-proj", "post_timeout_owned_ds", "US", opts...)
	if timeoutCreateErr == nil {
		t.Fatal("createEphemeralDataset(post_timeout_owned_ds) succeeded on timed-out ctx, want error")
	}
	if postCalls.Load() != 1 || deleteCalls.Load() != 1 {
		t.Fatalf("post-commit timeout with matching owner label: post=%d, delete=%d, want 1, 1",
			postCalls.Load(), deleteCalls.Load())
	}
	postWaitCancel.Store(false)

	// 5c. Ambiguous POST failure where follow-up GET fails (403) or owner label belongs to another creator:
	// createEphemeralDataset must NOT delete the dataset and must report the dataset ID when unverified.
	commitOnPostError.Store(true)
	postStatus.Store(http.StatusInternalServerError)
	getCommittedStatus.Store(http.StatusForbidden)
	getCalls.Store(0)
	postCalls.Store(0)
	deleteCalls.Store(0)
	ctxUnverified, cancelUnverified := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancelUnverified()
	unverifiedErr := createEphemeralDataset(ctxUnverified, "test-proj", "post_500_unverified_ds", "US", opts...)
	if unverifiedErr == nil || !strings.Contains(unverifiedErr.Error(), "was left behind") {
		t.Fatalf("createEphemeralDataset(post_500_unverified_ds) error = %v, want 'was left behind' notice", unverifiedErr)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("unverified metadata DELETE calls = %d, want 0", deleteCalls.Load())
	}

	getCommittedStatus.Store(http.StatusOK)
	commitOnPostError.Store(true)
	postStatus.Store(http.StatusInternalServerError)
	getCalls.Store(0)
	postCalls.Store(0)
	deleteCalls.Store(0)
	ctxOtherOwner, cancelOtherOwner := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancelOtherOwner()
	otherOwnerErr := createEphemeralDataset(ctxOtherOwner, "test-proj", "post_500_other_owner_ds", "US", opts...)
	if otherOwnerErr == nil || !strings.Contains(otherOwnerErr.Error(), "refusing to delete") {
		t.Fatalf("createEphemeralDataset(post_500_other_owner_ds) error = %v, want 'refusing to delete'", otherOwnerErr)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("foreign owner label DELETE calls = %d, want 0", deleteCalls.Load())
	}
	commitOnPostError.Store(false)
	postStatus.Store(http.StatusOK)
	delStatus.Store(http.StatusNoContent)
}

func TestRunPartialFailureEmptyWindowInvalidOffloadAndPanicRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// 1. Empty measurement window (Duration=1ns -> 0 samples / 0 acked rows) must fail.
	if _, err := Run(ctx, RunnerConfig{
		Mode:             ModeBQFake,
		Signals:          []string{"traces"},
		Duration:         time.Nanosecond,
		BatchSize:        10,
		Workers:          1,
		TargetRate:       10,
		SimulateAckDelay: 50 * time.Millisecond,
	}); err == nil {
		t.Fatal("Run(Duration=1ns) with 0 completed batches succeeded, want non-nil error")
	}

	// 2. Regular file passed as OffloadDir fails pre-validation with "not a directory".
	tmpFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(tmpFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", tmpFile, err)
	}
	if _, err := Run(ctx, RunnerConfig{
		Mode:       ModeBQFake,
		Signals:    []string{"traces"},
		Duration:   100 * time.Millisecond,
		BatchSize:  4,
		Workers:    1,
		OffloadDir: tmpFile,
	}); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("Run(OffloadDir=regular file) error = %v, want 'not a directory'", err)
	}

	// 3. Underlying offload filesystem error during worker execution is preserved via errors.Join.
	offloadRoot := t.TempDir()
	// Create a regular file at "<offloadRoot>/local-offload-bench" so validateRunConfig
	// sees offloadRoot as a valid directory, but WriteObject's MkdirAll("<offloadRoot>/local-offload-bench/...")
	// fails with ENOTDIR ("not a directory") on every batch.
	blockerFile := filepath.Join(offloadRoot, "local-offload-bench")
	if err := os.WriteFile(blockerFile, []byte("blocker"), 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", blockerFile, err)
	}
	_, offloadErr := Run(ctx, RunnerConfig{
		Mode:                  ModeBQFake,
		Signals:               []string{"traces"},
		Duration:              120 * time.Millisecond,
		BatchSize:             4,
		Workers:               1,
		PayloadBytes:          1024,
		OffloadDir:            offloadRoot,
		OffloadThresholdBytes: 512,
	})
	if offloadErr == nil || !strings.Contains(offloadErr.Error(), "not a directory") {
		t.Fatalf("Run() with failing offload WriteObject error = %v, want underlying 'not a directory' cause preserved", offloadErr)
	}

	// 4. Worker panic is recovered into a structured error and exporter is shut down cleanly (0 leaked goroutines).
	_, panicErr := Run(ctx, RunnerConfig{
		Mode:      ModeBQFake,
		Signals:   []string{"traces"},
		Duration:  200 * time.Millisecond,
		BatchSize: 8,
		Workers:   2,
		workerHook: func(_, seq int) {
			if seq == 1 {
				panic("simulated worker fault")
			}
		},
	})
	if panicErr == nil || !strings.Contains(panicErr.Error(), "panicked: simulated worker fault") {
		t.Fatalf("Run() with panicking worker error = %v, want recovered panic error", panicErr)
	}

	// 5. Large >4 MiB ProtoRows batch (500 spans x 16 KiB = ~8 MiB request) succeeds against bqfake.
	largeRep, err := Run(ctx, RunnerConfig{
		Mode:               ModeBQFake,
		Signals:            []string{"traces"},
		Duration:           200 * time.Millisecond,
		BatchSize:          500,
		Workers:            1,
		StreamPoolSize:     1,
		ConnectionPoolSize: 1,
		PayloadBytes:       16384,
	})
	if err != nil {
		t.Fatalf("Run(500 x 16 KiB >4 MiB batch) error: %v", err)
	}
	if largeRep.Results[0].BatchesFailed != 0 || largeRep.Results[0].Telemetry.AcknowledgedRows < 500 {
		t.Fatalf("large batch result: failed=%d, acked=%d, want failed=0, acked>=500",
			largeRep.Results[0].BatchesFailed, largeRep.Results[0].Telemetry.AcknowledgedRows)
	}
	if largeRep.Results[0].BytesPerRow <= 16384 {
		t.Fatalf("large batch BytesPerRow = %f, want > 16384", largeRep.Results[0].BytesPerRow)
	}

	// 6. Retry-uninflated BytesPerRow vs AttemptedBytesPerAckedRow under SimulateThrottleEvery=2.
	baseRep, err := Run(ctx, RunnerConfig{
		Mode:      ModeBQFake,
		Signals:   []string{"traces"},
		Duration:  150 * time.Millisecond,
		BatchSize: 28,
		Workers:   1,
	})
	if err != nil {
		t.Fatalf("Run(baseline) error: %v", err)
	}
	throttleRep, err := Run(ctx, RunnerConfig{
		Mode:                  ModeBQFake,
		Signals:               []string{"traces"},
		Duration:              200 * time.Millisecond,
		BatchSize:             28,
		Workers:               1,
		SimulateThrottleEvery: 2,
	})
	if err != nil {
		t.Fatalf("Run(throttled) error: %v", err)
	}
	if throttleRep.Results[0].Telemetry.Retries <= 0 {
		t.Fatalf("throttled Retries = %d, want > 0", throttleRep.Results[0].Telemetry.Retries)
	}
	if math.Abs(throttleRep.Results[0].BytesPerRow-baseRep.Results[0].BytesPerRow) > 5.0 {
		t.Fatalf("throttled BytesPerRow = %.2f vs baseline %.2f, want retry-uninflated match",
			throttleRep.Results[0].BytesPerRow, baseRep.Results[0].BytesPerRow)
	}
	if throttleRep.Results[0].AttemptedBytesPerAckedRow <= throttleRep.Results[0].BytesPerRow {
		t.Fatalf("throttled AttemptedBytesPerAckedRow (%.2f) must exceed single-append BytesPerRow (%.2f)",
			throttleRep.Results[0].AttemptedBytesPerAckedRow, throttleRep.Results[0].BytesPerRow)
	}

	// 6b. Uneven split-batch retry (500 spans x 16 KiB = ~9.13 MiB -> split into 9 uneven
	// AppendRows chunks: eight 57-row chunks and one 44-row tail chunk under 1 MiB max_request_bytes)
	// with SimulateThrottleEvery=1 must preserve exact uninflated BytesPerRow.
	splitThrottleRep, err := Run(ctx, RunnerConfig{
		Mode:                  ModeBQFake,
		Signals:               []string{"traces"},
		Duration:              250 * time.Millisecond,
		BatchSize:             500,
		Workers:               1,
		StreamPoolSize:        1,
		ConnectionPoolSize:    1,
		PayloadBytes:          16384,
		SimulateThrottleEvery: 1,
	})
	if err != nil {
		t.Fatalf("Run(split 500 x 16 KiB throttled) error: %v", err)
	}
	if splitThrottleRep.Results[0].Telemetry.Retries <= 0 {
		t.Fatalf("split throttled Retries = %d, want > 0", splitThrottleRep.Results[0].Telemetry.Retries)
	}
	if math.Abs(splitThrottleRep.Results[0].BytesPerRow-largeRep.Results[0].BytesPerRow) > 1.0 {
		t.Fatalf("split throttled BytesPerRow = %.2f vs unthrottled %.2f, want exact match within 1 B",
			splitThrottleRep.Results[0].BytesPerRow, largeRep.Results[0].BytesPerRow)
	}
	if splitThrottleRep.Results[0].AttemptedBytesPerAckedRow <= splitThrottleRep.Results[0].BytesPerRow {
		t.Fatalf("split throttled AttemptedBytesPerAckedRow (%.2f) must exceed BytesPerRow (%.2f)",
			splitThrottleRep.Results[0].AttemptedBytesPerAckedRow, splitThrottleRep.Results[0].BytesPerRow)
	}
	// Also verify measureSingleAttemptBytesPerRow (used in live mode when Retries > 0) matches
	// bqfake AckedBytes()/AcknowledgedRows across all three signals.
	for _, sig := range []string{"traces", "logs", "metrics"} {
		sampleCfg := RunnerConfig{
			Mode:         ModeBQFake,
			Signals:      []string{sig},
			Duration:     100 * time.Millisecond,
			BatchSize:    28,
			Workers:      1,
			PayloadBytes: 16384,
			Redaction:    true,
		}
		if err := sampleCfg.Normalize(); err != nil {
			t.Fatalf("Normalize(%s): %v", sig, err)
		}
		singleBPR, err := measureSingleAttemptBytesPerRow(sampleCfg, sig)
		if err != nil || singleBPR <= 0 {
			t.Fatalf("measureSingleAttemptBytesPerRow(%s) = %f, err = %v, want > 0, nil", sig, singleBPR, err)
		}
	}

	// 7. Pre-append offload failures under AllowPartialFailures=true must not
	// dilute BytesPerRow (since ConsumeTraces was not dispatched for those batches).
	cleanOffloadDir := t.TempDir()
	cleanOffloadRep, err := Run(ctx, RunnerConfig{
		Mode:                  ModeBQFake,
		Signals:               []string{"traces"},
		Duration:              120 * time.Millisecond,
		BatchSize:             28,
		Workers:               1,
		PayloadBytes:          1024,
		UniquePayloads:        true,
		OffloadDir:            cleanOffloadDir,
		OffloadThresholdBytes: 512,
	})
	if err != nil {
		t.Fatalf("Run(cleanOffload) error: %v", err)
	}
	flakyOffloadRoot := t.TempDir()
	flakyBucketPath := filepath.Join(flakyOffloadRoot, "local-offload-bench")
	partialOffloadRep, err := Run(ctx, RunnerConfig{
		Mode:                  ModeBQFake,
		Signals:               []string{"traces"},
		Duration:              150 * time.Millisecond,
		BatchSize:             28,
		Workers:               1,
		PayloadBytes:          1024,
		UniquePayloads:        true,
		OffloadDir:            flakyOffloadRoot,
		OffloadThresholdBytes: 512,
		AllowPartialFailures:  true,
		workerHook: func(_, seq int) {
			switch seq {
			case 1:
				_ = os.RemoveAll(flakyBucketPath)
				_ = os.WriteFile(flakyBucketPath, []byte("blocker"), 0o600)
			case 2:
				_ = os.Remove(flakyBucketPath)
			}
		},
	})
	if err != nil {
		t.Fatalf("Run(partialOffload, AllowPartialFailures=true) error: %v", err)
	}
	if partialOffloadRep.Results[0].BatchesFailed <= 0 || partialOffloadRep.Results[0].BatchesSucceeded <= 0 {
		t.Fatalf("partialOffload BatchesFailed=%d, BatchesSucceeded=%d, want both > 0",
			partialOffloadRep.Results[0].BatchesFailed, partialOffloadRep.Results[0].BatchesSucceeded)
	}
	if math.Abs(partialOffloadRep.Results[0].BytesPerRow-cleanOffloadRep.Results[0].BytesPerRow) > 5.0 {
		t.Fatalf("partialOffload BytesPerRow = %.2f vs cleanOffload %.2f, want pre-append offload failures excluded from BytesPerRow denominator",
			partialOffloadRep.Results[0].BytesPerRow, cleanOffloadRep.Results[0].BytesPerRow)
	}
}

func TestDocsCostModelOffloadSizesAndBenchmarkReportJSON(t *testing.T) {
	// 1. Verify exact single-row ProtoRows serialized sizes (inline vs ObjectRef offload)
	// and content-addressed GCS object key format (no .json extension) documented in docs/cost-estimation.md.
	gen := NewGenerator(GeneratorConfig{
		PayloadBytes: 16384,
	})
	tdInline := gen.TracesBatch(28)
	ldInline := gen.LogsBatch(28)
	mdInline := gen.MetricsBatch(28)

	tOpts := transform.Options{
		Limits: transform.DefaultLimits(),
	}
	spanEnc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatalf("protoenc.New(SpansTableSchema): %v", err)
	}
	logEnc, err := protoenc.New(schema.LogsTableSchema())
	if err != nil {
		t.Fatalf("protoenc.New(LogsTableSchema): %v", err)
	}
	metricEnc, err := protoenc.New(schema.MetricsTableSchema())
	if err != nil {
		t.Fatalf("protoenc.New(MetricsTableSchema): %v", err)
	}
	encodeSumBytes := func(enc *protoenc.Encoder, rows []transform.Row) int {
		var sum int
		for _, r := range rows {
			b, encErr := enc.Encode(r)
			if encErr != nil {
				t.Fatalf("enc.Encode: %v", encErr)
			}
			sum += len(b)
		}
		return sum
	}

	inlineSpanRows, _ := transform.SpanRows(tdInline, tOpts)
	inlineSpanBytes := encodeSumBytes(spanEnc, inlineSpanRows)
	if math.Abs(float64(inlineSpanBytes)/28.0-18256.0) > 10.0 {
		t.Fatalf("inlineSpanBytes/28 = %.2f (%d B), want ~18256 B/span", float64(inlineSpanBytes)/28.0, inlineSpanBytes)
	}

	inlineLogRows, _ := transform.LogRows(ldInline, tOpts)
	inlineLogBytes := encodeSumBytes(logEnc, inlineLogRows)
	if inlineLogBytes <= 400000 {
		t.Fatalf("inlineLogBytes = %d, want > 400000", inlineLogBytes)
	}

	inlineMetricRows, _ := transform.MetricRows(mdInline, tOpts)
	inlineMetricBytes := encodeSumBytes(metricEnc, inlineMetricRows)
	if int(math.Round(float64(inlineMetricBytes)/28.0)) != 1252 {
		t.Fatalf("inlineMetricBytes = %d (%.2f B/point), want 1252 B/point", inlineMetricBytes, float64(inlineMetricBytes)/28.0)
	}

	// Offload via internal/offload with MinBytes=4096 and verify exact offloaded row sizes and object keys.
	offloadRoot := t.TempDir()
	store := &localDirBlobStore{rootDir: offloadRoot}
	offloader, err := offload.New(store, offload.Options{
		Bucket:         "local-offload-bench",
		Prefix:         "otel-offload",
		MinBytes:       4096,
		MaxObjectBytes: 64 * 1024 * 1024,
		MaxConcurrency: 8,
		CacheSize:      10000,
		Timeout:        5 * time.Second,
	})
	if err != nil {
		t.Fatalf("offload.New: %v", err)
	}

	genOff := NewGenerator(GeneratorConfig{
		PayloadBytes: 16384,
	})
	tdOff := genOff.TracesBatch(28)
	ldOff := genOff.LogsBatch(28)

	offOpts := transform.Options{
		Limits:           transform.DefaultLimits(),
		PayloadOffloader: offloader,
	}
	offSpanRows, spanOffErrs := transform.SpanRows(tdOff, offOpts)
	if len(spanOffErrs) != 0 {
		t.Fatalf("transform.SpanRows(offOpts) errs: %v", spanOffErrs)
	}
	offSpanBytes := encodeSumBytes(spanEnc, offSpanRows)
	if math.Abs(float64(offSpanBytes)/28.0-2402.0) > 1.0 {
		t.Fatalf("offSpanBytes = %d (%.2f B/span), want ~2402 B/span", offSpanBytes, float64(offSpanBytes)/28.0)
	}

	offLogRows, logOffErrs := transform.LogRows(ldOff, offOpts)
	if len(logOffErrs) != 0 {
		t.Fatalf("transform.LogRows(offOpts) errs: %v", logOffErrs)
	}
	offLogBytes := encodeSumBytes(logEnc, offLogRows)
	if math.Abs(float64(offLogBytes)/28.0-1872.0) > 1.0 {
		t.Fatalf("offLogBytes = %d (%.2f B/log), want ~1872 B/log", offLogBytes, float64(offLogBytes)/28.0)
	}

	turnBytesOffload := int(math.Round((float64(offSpanBytes)*4 + float64(offLogBytes)*2 + float64(inlineMetricBytes)*2) / 28.0))
	if math.Abs(float64(turnBytesOffload)-15855.0) > 5.0 {
		t.Fatalf("turnBytesOffload = %d, want ~15855", turnBytesOffload)
	}

	keyPattern := regexp.MustCompile(`^otel-offload/(spans|logs)/\d{4}-\d{2}-\d{2}/[0-9a-f]{64}$`)
	bucketDir := filepath.Join(offloadRoot, "local-offload-bench")
	var objectCount int
	err = filepath.WalkDir(bucketDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		rel, relErr := filepath.Rel(bucketDir, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if !keyPattern.MatchString(rel) {
			t.Fatalf("offloaded object key %q does not match %s (must not have .json suffix)", rel, keyPattern)
		}
		objectCount++
		return nil
	})
	if err != nil || objectCount == 0 {
		t.Fatalf("walking offloaded objects: count=%d err=%v", objectCount, err)
	}

	// 2. Verify docs/cost-estimation.md contains the exact verified figures and no stale pricing/sizes.
	costDocRaw, err := os.ReadFile(filepath.Join("..", "..", "docs", "cost-estimation.md"))
	if err != nil {
		t.Fatalf("ReadFile(docs/cost-estimation.md): %v", err)
	}
	costDoc := string(costDocRaw)
	for _, wantSubstr := range []string{
		"2,402 B",
		"1,872 B",
		"15,855 B / turn",
		"`gs://<bucket>/<prefix>/<signal>/YYYY-MM-DD/<sha256>`",
		"`$0.010` / 1,000 ops",
		"$36.00",
		"$360.00",
		"$3,600.00",
		"$187.14",
		"$1,871.41",
		"$18,714.11",
		"$19.56",
		"$78.26",
		"$313.02",
		"$19.68 / mo",
		"$83.01 / mo",
		"$141.29 / mo",
		"$456.47 / mo",
		"$455.92 / mo",
		"$1,456.22 / mo",
		"$4,163.40 / mo",
		"~98.6%",
		"~97.2%",
		"~94.6%",
		"~95.1%",
		"~90.5%",
		"26.7%",
		"51.1%",
		"63.3%",
		"46.9%",
		"64.6%",
		"73.5%",
		"`10 MiB` per-query minimum",
		"0.137 TiB/mo",
	} {
		if !strings.Contains(costDoc, wantSubstr) {
			t.Errorf("docs/cost-estimation.md missing expected substring %q", wantSubstr)
		}
	}
	for _, forbiddenSubstr := range []string{
		"<sha256>.json",
		"| **GCS Standard Class A Ops (`PUT`)** | `$0.005` / 1,000 ops |",
		"$24 / vCPU-mo",
		"$58.60",
		"$234.40",
		"2,228 B",
		"1,717 B",
		"14,850 B",
	} {
		if strings.Contains(costDoc, forbiddenSubstr) {
			t.Errorf("docs/cost-estimation.md contains stale substring %q", forbiddenSubstr)
		}
	}

	// 3. Verify docs/benchmarks/v0.1.0-performance-envelope.md Section 5 JSON snippet
	// unmarshals cleanly into BenchmarkReport with DisallowUnknownFields() and matches Table 5.1.
	benchDocRaw, err := os.ReadFile(filepath.Join("..", "..", "docs", "benchmarks", "v0.1.0-performance-envelope.md"))
	if err != nil {
		t.Fatalf("ReadFile(docs/benchmarks/v0.1.0-performance-envelope.md): %v", err)
	}
	benchDoc := string(benchDocRaw)
	for _, wantBenchSubstr := range []string{
		"Encoded-Row MiB/s",
		"**`14 of 18` Go packages with tests**",
		"+48.5%` to `+56.1%",
		"+47.0%` to `+48.3%",
		"+19.8%` to `+28.0%",
	} {
		if !strings.Contains(benchDoc, wantBenchSubstr) {
			t.Errorf("docs/benchmarks/v0.1.0-performance-envelope.md missing %q", wantBenchSubstr)
		}
	}

	jsonStart := strings.Index(benchDoc, "```json\n")
	if jsonStart < 0 {
		t.Fatal("missing ```json block in docs/benchmarks/v0.1.0-performance-envelope.md")
	}
	jsonBody := benchDoc[jsonStart+len("```json\n"):]
	jsonEnd := strings.Index(jsonBody, "\n```")
	if jsonEnd < 0 {
		t.Fatal("unterminated ```json block in docs/benchmarks/v0.1.0-performance-envelope.md")
	}
	jsonSnippet := jsonBody[:jsonEnd]

	dec := json.NewDecoder(strings.NewReader(jsonSnippet))
	dec.DisallowUnknownFields()
	var docReport BenchmarkReport
	if err := dec.Decode(&docReport); err != nil {
		t.Fatalf("BenchmarkReport JSON snippet in docs/benchmarks/v0.1.0-performance-envelope.md failed strict schema decode: %v", err)
	}
	if docReport.Timestamp != "2026-10-25T23:14:06Z" || !docReport.EphemeralDatasetDeleted || docReport.Config.Mode != ModeLive {
		t.Fatalf("unexpected docReport header fields: timestamp=%q deleted=%t mode=%q",
			docReport.Timestamp, docReport.EphemeralDatasetDeleted, docReport.Config.Mode)
	}
	if len(docReport.Results) != 3 {
		t.Fatalf("len(docReport.Results) = %d, want 3", len(docReport.Results))
	}
	wantRates := map[string]float64{
		"traces":  10404.2,
		"logs":    9593.6,
		"metrics": 9185.0,
	}
	for _, r := range docReport.Results {
		if r.AchievedRowsPerSec != wantRates[r.Signal] {
			t.Errorf("docReport signal %s AchievedRowsPerSec = %v, want %v", r.Signal, r.AchievedRowsPerSec, wantRates[r.Signal])
		}
		if r.Telemetry.AcknowledgedRows <= 0 || r.Latency.Samples <= 0 {
			t.Errorf("docReport signal %s has empty telemetry/latency: %+v", r.Signal, r)
		}
	}
}
