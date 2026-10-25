// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package testbench provides a synthetic OpenTelemetry GenAI and MCP workload
// generator and configurable load benchmark runner for bigqueryexporter.
package testbench

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// FrameworkSpec describes one of the 7 supported GenAI agent frameworks
// profiled by the workload generator.
type FrameworkSpec struct {
	Framework   string
	Provider    string
	System      string
	Model       string
	AgentName   string
	ToolName    string
	ScopeName   string
	ScopeVer    string
	ServiceName string
}

// DefaultFrameworks returns the 7 canonical GenAI agent frameworks generated
// across synthetic trace, log, and metric batches.
func DefaultFrameworks() []FrameworkSpec {
	return []FrameworkSpec{
		{
			Framework:   "google_adk",
			Provider:    "gcp.vertex_ai",
			System:      "vertex_ai",
			Model:       "gemini-2.5-pro",
			AgentName:   "bq_analytics_coordinator",
			ToolName:    "execute_bigquery_sql",
			ScopeName:   "google.adk.telemetry",
			ScopeVer:    "1.4.0",
			ServiceName: "adk-analytics-agent",
		},
		{
			Framework:   "langgraph",
			Provider:    "openai",
			System:      "openai",
			Model:       "gpt-4o-2026-05",
			AgentName:   "langgraph_research_supervisor",
			ToolName:    "vector_store_lookup",
			ScopeName:   "openinference.instrumentation.langchain",
			ScopeVer:    "0.3.2",
			ServiceName: "langgraph-router",
		},
		{
			Framework:   "openai_agents",
			Provider:    "openai",
			System:      "openai",
			Model:       "o3-mini",
			AgentName:   "triage_handoff_agent",
			ToolName:    "fetch_customer_ledger",
			ScopeName:   "openai.agents.tracing",
			ScopeVer:    "0.2.1",
			ServiceName: "openai-agents-gateway",
		},
		{
			Framework:   "anthropic_agent_sdk",
			Provider:    "anthropic",
			System:      "anthropic",
			Model:       "claude-sonnet-4-20250514",
			AgentName:   "code_verification_agent",
			ToolName:    "sandbox_bash_exec",
			ScopeName:   "anthropic.agent.telemetry",
			ScopeVer:    "0.9.0",
			ServiceName: "anthropic-agent-worker",
		},
		{
			Framework:   "crewai",
			Provider:    "gcp.gemini",
			System:      "gemini",
			Model:       "gemini-2.5-flash",
			AgentName:   "crew_financial_analyst",
			ToolName:    "sec_filing_extractor",
			ScopeName:   "openlit.otel.crewai",
			ScopeVer:    "1.12.0",
			ServiceName: "crewai-finops-pipeline",
		},
		{
			Framework:   "aws_bedrock",
			Provider:    "aws.bedrock",
			System:      "aws.bedrock",
			Model:       "anthropic.claude-3-7-sonnet-20250219-v1:0",
			AgentName:   "bedrock_knowledge_orchestrator",
			ToolName:    "retrieve_and_generate",
			ScopeName:   "aws.bedrock.agent.otel",
			ScopeVer:    "1.1.0",
			ServiceName: "bedrock-enterprise-agent",
		},
		{
			Framework:   "microsoft_agent_framework",
			Provider:    "azure.ai.openai",
			System:      "az.ai.inference",
			Model:       "gpt-4.1",
			AgentName:   "semantic_kernel_planner",
			ToolName:    "graph_calendar_query",
			ScopeName:   "Microsoft.Extensions.AI.OpenTelemetry",
			ScopeVer:    "9.3.0",
			ServiceName: "msft-agent-orchestrator",
		},
	}
}

var mcpMethods = []struct {
	Method     string
	ServerName string
	ToolOrURI  string
}{
	{Method: "tools/call", ServerName: "bigquery-mcp-server", ToolOrURI: "run_sql_query"},
	{Method: "resources/read", ServerName: "gcs-artifact-mcp", ToolOrURI: "gs://agent-context/schemas/orders.json"},
	{Method: "prompts/get", ServerName: "prompt-registry-mcp", ToolOrURI: "incident_root_cause_triage"},
}

// DefaultBaseTime is the deterministic reference UTC timestamp used by
// Generator when GeneratorConfig.BaseTime is zero.
var DefaultBaseTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// GeneratorConfig controls the shape and payload size of synthetic OTLP
// traces, logs, and metrics produced by Generator.
type GeneratorConfig struct {
	// PayloadBytes is the target byte length of synthetic prompt/response/tool
	// payload attributes attached to each record (e.g. 1024 to 524288).
	// Defaults to 1024 bytes when <= 0.
	PayloadBytes int
	// PlantSecrets injects synthetic sensitive attribute keys ("api_key",
	// "authorization") and secret value patterns ("Bearer sk-...", "ya29....")
	// on every 4th record so Step-0 redaction cost and counters can be measured.
	PlantSecrets bool
	// UniquePayloads controls whether large payloads embed a per-record sequence
	// header. When false, payloads cycle over a small 16-entry corpus to exercise
	// SHA-256 deduplication in the GCS payload offloader.
	UniquePayloads bool
	// Seed initializes the deterministic per-signal sequence counters (default 0).
	Seed uint64
	// BaseTime sets the deterministic base timestamp for generated records.
	// Defaults to DefaultBaseTime (2026-01-01T00:00:00Z) when zero.
	BaseTime time.Time
}

// Generator builds deterministic, allocation-conscious OTLP trace, log, and
// metric batches for throughput and latency benchmarking.
type Generator struct {
	cfg           GeneratorConfig
	baseTime      time.Time
	frameworks    []FrameworkSpec
	payloadCorpus []string
	traceSeq      atomic.Uint64
	logSeq        atomic.Uint64
	metricSeq     atomic.Uint64
}

// NewGenerator constructs a workload Generator from cfg.
func NewGenerator(cfg GeneratorConfig) *Generator {
	if cfg.PayloadBytes <= 0 {
		cfg.PayloadBytes = 1024
	}
	baseTime := cfg.BaseTime.UTC()
	if cfg.BaseTime.IsZero() {
		baseTime = DefaultBaseTime
	}
	const corpusSize = 16
	corpus := make([]string, corpusSize)
	for i := range corpus {
		corpus[i] = buildStructuredPayload(i, cfg.PayloadBytes)
	}
	g := &Generator{
		cfg:           cfg,
		baseTime:      baseTime,
		frameworks:    DefaultFrameworks(),
		payloadCorpus: corpus,
	}
	initialSeq := cfg.Seed * 4
	g.traceSeq.Store(initialSeq)
	g.logSeq.Store(initialSeq)
	g.metricSeq.Store(initialSeq)
	return g
}

func buildStructuredPayload(seed int, targetBytes int) string {
	prefix := fmt.Sprintf(
		`{"turn":%d,"context_window":"enterprise_rag","documents":[{"doc_id":"doc-%02d","score":0.984,"snippet":"`,
		seed, seed,
	)
	suffix := `"}]}`
	padLen := targetBytes - len(prefix) - len(suffix)
	if padLen == 0 {
		return prefix + suffix
	}
	if padLen < 0 {
		shortPrefix := fmt.Sprintf(`{"turn":%d,"s":"`, seed)
		const shortSuffix = `"}`
		if targetBytes >= len(shortPrefix)+len(shortSuffix) {
			return shortPrefix + strings.Repeat("x", targetBytes-len(shortPrefix)-len(shortSuffix)) + shortSuffix
		}
		return fmt.Sprintf(`{"turn":%d}`, seed)
	}
	chunk := "OpenTelemetry GenAI semantic conventions v1.40 agent trajectory context block; "
	repeats := (padLen / len(chunk)) + 1
	body := strings.Repeat(chunk, repeats)[:padLen]
	return prefix + body + suffix
}

func (g *Generator) payloadFor(seq uint64) string {
	out := g.payloadCorpus[seq%uint64(len(g.payloadCorpus))]
	plantSecret := g.cfg.PlantSecrets && seq%4 == 0
	if !g.cfg.UniquePayloads && !plantSecret {
		return out
	}

	var inject strings.Builder
	if g.cfg.UniquePayloads {
		_, _ = fmt.Fprintf(&inject, "seq=%012d; ", seq)
	}
	if plantSecret {
		_, _ = fmt.Fprintf(&inject, "auth=%s; ", secretValueTokenFor(seq))
	}
	tag := inject.String()

	const marker = `"snippet":"`
	idx := strings.Index(out, marker)
	const suffixLen = len(`"}]}`)
	if idx < 0 || len(out) < idx+len(marker)+suffixLen {
		if g.cfg.UniquePayloads && plantSecret {
			return fmt.Sprintf(`{"seq":"%012d","auth":%q}`, seq, secretValueTokenFor(seq))
		}
		if g.cfg.UniquePayloads {
			return fmt.Sprintf(`{"seq":"%012d"}`, seq)
		}
		return fmt.Sprintf(`{"auth":%q}`, secretValueTokenFor(seq))
	}
	bodyStart := idx + len(marker)
	bodyEnd := len(out) - suffixLen
	if bodyEnd-bodyStart >= len(tag) {
		return out[:bodyStart] + tag + out[bodyStart+len(tag):]
	}
	return out[:bodyStart] + tag + out[bodyEnd:]
}

// TracesBatch generates a ptrace.Traces batch with exactly spanCount spans
// distributed across the 7 GenAI agent frameworks and MCP child spans.
// Spans are organized into 4-span agent execution trees:
//  1. invoke_agent (root span)
//  2. chat (LLM completion child span)
//  3. execute_tool (framework tool child span)
//  4. mcp.client.operation (MCP tools/call, resources/read, or prompts/get child span)
func (g *Generator) TracesBatch(spanCount int) ptrace.Traces {
	if spanCount <= 0 {
		spanCount = 1
	}
	td := ptrace.NewTraces()
	baseSeq := g.traceSeq.Add(uint64(spanCount)) - uint64(spanCount)

	// Group spans by framework resource to mirror multi-service collector batches.
	numFrameworks := len(g.frameworks)
	rsByFramework := make([]ptrace.SpanSlice, numFrameworks)
	hasFramework := make([]bool, numFrameworks)
	for idx := 0; idx < spanCount; idx++ {
		seq := baseSeq + uint64(idx)
		treeID := seq / 4
		stepInTree := seq % 4
		fwIdx := int(treeID % uint64(numFrameworks))
		fw := g.frameworks[fwIdx]

		if !hasFramework[fwIdx] {
			rs := td.ResourceSpans().AppendEmpty()
			resAttrs := rs.Resource().Attributes()
			resAttrs.PutStr("service.name", fw.ServiceName)
			resAttrs.PutStr("service.version", "0.1.0")
			resAttrs.PutStr("deployment.environment.name", "benchmark")
			resAttrs.PutStr("cloud.provider", "gcp")
			resAttrs.PutStr("cloud.region", "us-central1")
			resAttrs.PutStr("gen_ai.framework", fw.Framework)

			ss := rs.ScopeSpans().AppendEmpty()
			ss.Scope().SetName(fw.ScopeName)
			ss.Scope().SetVersion(fw.ScopeVer)
			rsByFramework[fwIdx] = ss.Spans()
			hasFramework[fwIdx] = true
		}

		sp := rsByFramework[fwIdx].AppendEmpty()
		traceID := traceIDFromTree(treeID)
		rootSpanID := spanIDFromPair(treeID, 0)
		curSpanID := spanIDFromPair(treeID, stepInTree)
		sp.SetTraceID(traceID)
		sp.SetSpanID(curSpanID)
		sp.TraceState().FromRaw("ot=p:8;r:62")
		sp.SetFlags(1)

		startTs := g.baseTime.Add(time.Duration(seq) * 100 * time.Microsecond)
		endTs := startTs.Add(42 * time.Millisecond)
		sp.SetStartTimestamp(pcommon.NewTimestampFromTime(startTs))
		sp.SetEndTimestamp(pcommon.NewTimestampFromTime(endTs))
		sp.Status().SetCode(ptrace.StatusCodeOk)

		attrs := sp.Attributes()
		attrs.PutStr("gen_ai.framework", fw.Framework)
		attrs.PutStr("gen_ai.provider.name", fw.Provider)
		attrs.PutStr("gen_ai.system", fw.System)
		attrs.PutStr("gen_ai.request.model", fw.Model)
		attrs.PutStr("gen_ai.response.model", fw.Model)
		attrs.PutStr("gen_ai.agent.name", fw.AgentName)
		attrs.PutStr("gen_ai.agent.id", fmt.Sprintf("agent-%s-%04d", fw.Framework, treeID%1000))
		attrs.PutStr("gen_ai.conversation.id", fmt.Sprintf("conv-%08x", treeID%100000))
		attrs.PutStr("session.id", fmt.Sprintf("sess-%08x", treeID%100000))

		switch stepInTree {
		case 0:
			// Root invoke_agent span.
			sp.SetName(fmt.Sprintf("invoke_agent %s", fw.AgentName))
			sp.SetKind(ptrace.SpanKindInternal)
			attrs.PutStr("gen_ai.operation.name", "invoke_agent")
			attrs.PutInt("gen_ai.usage.input_tokens", 1420)
			attrs.PutInt("gen_ai.usage.output_tokens", 380)
			attrs.PutStr("gen_ai.input.messages", g.payloadFor(seq))
		case 1:
			// Child LLM chat completion span.
			sp.SetParentSpanID(rootSpanID)
			sp.SetName(fmt.Sprintf("chat %s", fw.Model))
			sp.SetKind(ptrace.SpanKindClient)
			attrs.PutStr("gen_ai.operation.name", "chat")
			attrs.PutDouble("gen_ai.request.temperature", 0.2)
			attrs.PutInt("gen_ai.request.max_tokens", 4096)
			attrs.PutInt("gen_ai.usage.input_tokens", 980)
			attrs.PutInt("gen_ai.usage.output_tokens", 310)
			attrs.PutStr("gen_ai.response.id", fmt.Sprintf("chatcmpl-%012x", seq))
			finish := attrs.PutEmptySlice("gen_ai.response.finish_reasons")
			finish.AppendEmpty().SetStr("tool_calls")
			attrs.PutStr("gen_ai.output.messages", g.payloadFor(seq))
		case 2:
			// Child tool execution span.
			sp.SetParentSpanID(rootSpanID)
			sp.SetName(fmt.Sprintf("execute_tool %s", fw.ToolName))
			sp.SetKind(ptrace.SpanKindInternal)
			attrs.PutStr("gen_ai.operation.name", "execute_tool")
			attrs.PutStr("gen_ai.tool.name", fw.ToolName)
			attrs.PutStr("gen_ai.tool.type", "function")
			attrs.PutStr("gen_ai.tool.call.id", fmt.Sprintf("call_%012x", seq))
			attrs.PutStr("gen_ai.tool.call.arguments", `{"dataset":"analytics_prod","limit":100}`)
			attrs.PutStr("gen_ai.tool.call.result", g.payloadFor(seq))
		case 3:
			// Child MCP client span (tools/call, resources/read, or prompts/get).
			mcp := mcpMethods[treeID%uint64(len(mcpMethods))]
			sp.SetParentSpanID(spanIDFromPair(treeID, 2))
			sp.SetName(fmt.Sprintf("mcp %s %s", mcp.Method, mcp.ToolOrURI))
			sp.SetKind(ptrace.SpanKindClient)
			attrs.PutStr("gen_ai.operation.name", "execute_tool")
			attrs.PutStr("mcp.method.name", mcp.Method)
			attrs.PutStr("mcp.server.name", mcp.ServerName)
			attrs.PutStr("server.address", mcp.ServerName)
			attrs.PutStr("mcp.session.id", fmt.Sprintf("mcp-sess-%08x", treeID%10000))
			attrs.PutStr("mcp.protocol.version", "2025-06-18")
			attrs.PutStr("rpc.system", "jsonrpc")
			attrs.PutStr("rpc.jsonrpc.version", "2.0")
			reqID := fmt.Sprintf("req-%d", seq)
			attrs.PutStr("rpc.jsonrpc.request_id", reqID)
			attrs.PutStr("jsonrpc.request.id", reqID)
			switch mcp.Method {
			case "resources/read":
				attrs.PutStr("mcp.resource.uri", mcp.ToolOrURI)
			case "prompts/get":
				attrs.PutStr("gen_ai.prompt.name", mcp.ToolOrURI)
			default:
				attrs.PutStr("gen_ai.tool.name", mcp.ToolOrURI)
			}
			attrs.PutStr("mcp.payload.content", g.payloadFor(seq))
		}

		// Attach an event to exercise repeated RECORD serialization.
		ev := sp.Events().AppendEmpty()
		ev.SetName("gen_ai.content.completion.chunk")
		ev.SetTimestamp(pcommon.NewTimestampFromTime(startTs.Add(20 * time.Millisecond)))
		ev.Attributes().PutInt("chunk.index", int64(stepInTree))

		if g.cfg.PlantSecrets && seq%4 == 0 {
			plantSecretKeyAttribute(attrs, seq)
		}
	}
	return td
}

// LogsBatch generates a plog.Logs batch with exactly logCount GenAI event log
// records distributed across the 7 GenAI frameworks.
func (g *Generator) LogsBatch(logCount int) plog.Logs {
	if logCount <= 0 {
		logCount = 1
	}
	ld := plog.NewLogs()
	baseSeq := g.logSeq.Add(uint64(logCount)) - uint64(logCount)

	numFrameworks := len(g.frameworks)
	lrByFramework := make([]plog.LogRecordSlice, numFrameworks)
	hasFramework := make([]bool, numFrameworks)
	eventNames := []string{
		"gen_ai.user.message",
		"gen_ai.choice",
		"gen_ai.tool.message",
		"gen_ai.system.instructions",
	}

	for idx := 0; idx < logCount; idx++ {
		seq := baseSeq + uint64(idx)
		fwIdx := int(seq % uint64(numFrameworks))
		fw := g.frameworks[fwIdx]

		if !hasFramework[fwIdx] {
			rl := ld.ResourceLogs().AppendEmpty()
			resAttrs := rl.Resource().Attributes()
			resAttrs.PutStr("service.name", fw.ServiceName)
			resAttrs.PutStr("service.version", "0.1.0")
			resAttrs.PutStr("deployment.environment.name", "benchmark")
			resAttrs.PutStr("gen_ai.framework", fw.Framework)

			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName(fw.ScopeName)
			sl.Scope().SetVersion(fw.ScopeVer)
			lrByFramework[fwIdx] = sl.LogRecords()
			hasFramework[fwIdx] = true
		}

		lr := lrByFramework[fwIdx].AppendEmpty()
		ts := g.baseTime.Add(time.Duration(seq) * 100 * time.Microsecond)
		lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(ts.Add(time.Millisecond)))
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.SetSeverityText("INFO")

		evName := eventNames[seq%uint64(len(eventNames))]
		lr.SetEventName(evName)
		treeID := seq / 4
		lr.SetTraceID(traceIDFromTree(treeID))
		lr.SetSpanID(spanIDFromPair(treeID, seq%4))
		lr.SetFlags(1)

		bodyMap := lr.Body().SetEmptyMap()
		bodyMap.PutStr("role", "assistant")
		bodyMap.PutStr("model", fw.Model)
		bodyMap.PutStr("content", g.payloadFor(seq))

		attrs := lr.Attributes()
		attrs.PutStr("gen_ai.framework", fw.Framework)
		attrs.PutStr("gen_ai.provider.name", fw.Provider)
		attrs.PutStr("gen_ai.system", fw.System)
		attrs.PutStr("gen_ai.request.model", fw.Model)
		attrs.PutStr("gen_ai.agent.name", fw.AgentName)
		attrs.PutStr("gen_ai.conversation.id", fmt.Sprintf("conv-%08x", treeID%100000))
		attrs.PutStr("event.name", evName)

		if g.cfg.PlantSecrets && seq%4 == 0 {
			plantSecretKeyAttribute(attrs, seq)
		}
	}
	return ld
}

// MetricsBatch generates a pmetric.Metrics batch with exactly pointCount
// Histogram data points (gen_ai.client.token.usage with trace exemplars)
// distributed across the 7 GenAI frameworks.
func (g *Generator) MetricsBatch(pointCount int) pmetric.Metrics {
	if pointCount <= 0 {
		pointCount = 1
	}
	md := pmetric.NewMetrics()
	baseSeq := g.metricSeq.Add(uint64(pointCount)) - uint64(pointCount)

	rm := md.ResourceMetrics().AppendEmpty()
	resAttrs := rm.Resource().Attributes()
	resAttrs.PutStr("service.name", "genai-fleet-collector")
	resAttrs.PutStr("service.version", "0.1.0")
	resAttrs.PutStr("deployment.environment.name", "benchmark")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("opentelemetry.instrumentation.genai")
	sm.Scope().SetVersion("0.1.0")

	tokenMetric := sm.Metrics().AppendEmpty()
	tokenMetric.SetName("gen_ai.client.token.usage")
	tokenMetric.SetDescription("Number of input and output tokens processed by GenAI invocations")
	tokenMetric.SetUnit("{token}")
	hist := tokenMetric.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	hPoints := hist.DataPoints()

	for idx := 0; idx < pointCount; idx++ {
		seq := baseSeq + uint64(idx)
		fw := g.frameworks[seq%uint64(len(g.frameworks))]
		dp := hPoints.AppendEmpty()
		pointTs := g.baseTime.Add(time.Duration(seq) * 100 * time.Microsecond)
		dp.SetStartTimestamp(pcommon.NewTimestampFromTime(pointTs.Add(-10 * time.Second)))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(pointTs))
		dp.SetCount(12)
		dp.SetSum(float64(1420 + (seq % 500)))
		dp.SetMin(64)
		dp.SetMax(2048)
		dp.ExplicitBounds().FromRaw([]float64{64, 256, 512, 1024, 2048, 4096})
		dp.BucketCounts().FromRaw([]uint64{1, 2, 4, 3, 2, 0, 0})

		attrs := dp.Attributes()
		attrs.PutStr("gen_ai.framework", fw.Framework)
		attrs.PutStr("gen_ai.provider.name", fw.Provider)
		attrs.PutStr("gen_ai.system", fw.System)
		attrs.PutStr("gen_ai.request.model", fw.Model)
		attrs.PutStr("gen_ai.operation.name", "chat")
		tokenType := "input"
		if seq%2 == 1 {
			tokenType = "output"
		}
		attrs.PutStr("gen_ai.token.type", tokenType)
		if g.cfg.PayloadBytes > 1024 {
			attrs.PutStr("gen_ai.baggage.context", g.payloadFor(seq))
		}
		if g.cfg.PlantSecrets && seq%4 == 0 {
			plantSecretKeyAttribute(attrs, seq)
			if g.cfg.PayloadBytes <= 1024 {
				attrs.PutStr("gen_ai.debug.auth_header", secretValueTokenFor(seq))
			}
		}

		ex := dp.Exemplars().AppendEmpty()
		ex.SetTimestamp(pcommon.NewTimestampFromTime(pointTs))
		ex.SetIntValue(int64(256 + (seq % 256)))
		treeID := seq / 4
		ex.SetTraceID(traceIDFromTree(treeID))
		ex.SetSpanID(spanIDFromPair(treeID, 1))
		ex.FilteredAttributes().PutStr("gen_ai.agent.name", fw.AgentName)
	}
	return md
}

func plantSecretKeyAttribute(attrs pcommon.Map, seq uint64) {
	// Exercises key-glob matching ("client.api_key") in the Step-0 redaction engine.
	attrs.PutStr("client.api_key", fmt.Sprintf("secret-key-%d", seq))
}

func secretValueTokenFor(seq uint64) string {
	// Rotates across all three default RE2 value-regex branches ("Bearer sk-...",
	// "ya29....", "AKIA...") in the Step-0 redaction engine. Synthetic tokens are
	// assembled via strings.Repeat so pre-commit secret scanners are not tripped
	// by literal token strings in source code.
	switch (seq / 4) % 3 {
	case 0:
		return "Bearer sk-proj-" + strings.Repeat("9f8e7d", 6)
	case 1:
		return "token=ya29." + strings.Repeat("a1B2", 6)
	default:
		return "aws_access_key=AKIA" + strings.Repeat("B2C3", 4)
	}
}

func traceIDFromTree(treeID uint64) pcommon.TraceID {
	var tid [16]byte
	binary.BigEndian.PutUint64(tid[0:8], 0x0102030405060708^(treeID+1))
	binary.BigEndian.PutUint64(tid[8:16], treeID+1)
	return pcommon.TraceID(tid)
}

func spanIDFromPair(treeID, step uint64) pcommon.SpanID {
	var sid [8]byte
	binary.BigEndian.PutUint64(sid[0:8], ((treeID+1)<<4)|(step&0x0f))
	return pcommon.SpanID(sid)
}
