// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// TestAutoCreateLiveBigQuery proves the complete control-plane lifecycle with
// the same ADC identity used for row writes. It is intentionally opt-in: the
// test creates billable Google Cloud resources and requires BigQuery IAM.
func TestAutoCreateLiveBigQuery(t *testing.T) {
	project := os.Getenv("BQ_PROJECT")
	if project == "" {
		if os.Getenv("REQUIRE_LIVE_BQ") == "1" {
			t.Fatal("REQUIRE_LIVE_BQ=1 is set but BQ_PROJECT is empty")
		}
		t.Skip("BQ_PROJECT is unset; skipping live BigQuery auto-create integration test")
	}
	location := os.Getenv("BQ_LOCATION")
	if location == "" {
		location = "US"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	// Register cancel first so t.Cleanup's LIFO order runs dataset deletion
	// and admin.Close before canceling ctx.
	t.Cleanup(cancel)
	dataset := uniqueEphemeralDatasetID("otel_exporter_it")

	httpClient, _, closeHTTP, err := googlehttp.NewClient(ctx, []string{bigqueryapi.BigqueryScope})
	if err != nil {
		t.Fatalf("create cleanup/query HTTP client: %v", err)
	}
	admin, err := bq.NewClient(ctx, project, option.WithHTTPClient(httpClient))
	if err != nil {
		closeHTTP()
		t.Fatalf("create cleanup/query client: %v", err)
	}
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close cleanup/query client: %v", err)
		}
		closeHTTP()
	})

	cfg := validConfig()
	cfg.Project = project
	cfg.Dataset = dataset
	cfg.Location = location
	// Leave cfg.TimeoutSettings.Timeout at the 5s default from
	// createDefaultConfig() so cold-dataset provisioning exercises per-call
	// metadata timeouts rather than an artificially raised budget.
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	cfg.AutoCreate.SearchIndexes = true
	cfg.AutoCreate.CompanionViews = true
	cfg.Quarantine.Enabled = true
	settings := component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}

	traces, err := newTracesExporter(settings, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !traces.createdDataset.Load() {
			return
		}
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanCancel()
		if err := admin.Dataset(dataset).DeleteWithContents(cleanCtx); err != nil {
			t.Errorf("delete ephemeral dataset %s: %v", dataset, err)
		}
	})
	if err := traces.start(ctx, nil); err != nil {
		t.Fatalf("first trace startup: %v", err)
	}
	if !traces.createdDataset.Load() {
		t.Fatalf("first trace startup did not cold-create dataset %s.%s (createdDataset=false)", project, dataset)
	}
	defer func() { _ = traces.shutdown(context.Background()) }()

	if err := assertIntegrationTrajectoryDryRunAndPartitionFilter(ctx, admin, project, dataset); err != nil {
		t.Fatalf("trajectory and RPF TVF dry-run verification: %v", err)
	}

	logs, err := newLogsExporter(settings, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := logs.start(ctx, nil); err != nil {
		t.Fatalf("first log startup: %v", err)
	}
	defer func() { _ = logs.shutdown(context.Background()) }()

	metrics, err := newMetricsExporter(settings, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := metrics.start(ctx, nil); err != nil {
		t.Fatalf("first metric startup: %v", err)
	}
	defer func() { _ = metrics.shutdown(context.Background()) }()

	assertIntegrationViewDryRuns(ctx, t, admin, project, dataset, cfg.Traces.Table, cfg.Logs.Table)
	if err := assertIntegrationSearchIndexes(ctx, admin, project, dataset, cfg.Traces.Table, cfg.Logs.Table, cfg.Metrics.Table); err != nil {
		t.Fatalf("search index verification: %v", err)
	}

	if err := traces.pushTraceData(ctx, oneIntegrationTrace()); err != nil {
		t.Fatalf("write trace: %v", err)
	}
	if err := logs.pushLogData(ctx, oneIntegrationLog()); err != nil {
		t.Fatalf("write log: %v", err)
	}
	if err := metrics.pushMetricsData(ctx, oneIntegrationMetrics()); err != nil {
		t.Fatalf("write metric: %v", err)
	}

	// A second set of exporters exercises the dataset and table AlreadyExists
	// paths and the reconciliation of existing views and search indexes against
	// the real metadata API.
	secondTraces, err := newTracesExporter(settings, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := secondTraces.start(ctx, nil); err != nil {
		t.Fatalf("second trace startup was not idempotent: %v", err)
	}
	if secondTraces.createdDataset.Load() {
		t.Fatalf("second trace startup against existing dataset %s.%s set createdDataset=true, want false", project, dataset)
	}
	if err := secondTraces.shutdown(ctx); err != nil {
		t.Fatalf("second trace shutdown: %v", err)
	}

	secondLogs, err := newLogsExporter(settings, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := secondLogs.start(ctx, nil); err != nil {
		t.Fatalf("second log startup was not idempotent: %v", err)
	}
	if err := secondLogs.shutdown(ctx); err != nil {
		t.Fatalf("second log shutdown: %v", err)
	}

	secondMetrics, err := newMetricsExporter(settings, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := secondMetrics.start(ctx, nil); err != nil {
		t.Fatalf("second metric startup was not idempotent: %v", err)
	}
	if err := secondMetrics.shutdown(ctx); err != nil {
		t.Fatalf("second metric shutdown: %v", err)
	}

	for _, table := range []string{cfg.Traces.Table, cfg.Logs.Table, cfg.Metrics.Table} {
		if err := waitForRows(ctx, admin, project, dataset, table); err != nil {
			t.Error(err)
		}
		if err := waitForRows(ctx, admin, project, dataset, table+"_dedup"); err != nil {
			t.Error(err)
		}
	}
	if err := waitForRows(ctx, admin, project, dataset, "otel_spans_as_allspans"); err != nil {
		t.Error(err)
	}
	for _, name := range []string{
		cfg.Quarantine.Table,
		"v_gen_ai_spans",
		"v_gen_ai_logs",
		"v_gen_ai_metrics",
		"v_gen_ai_metric_exemplars",
		"v_gen_ai_model_costs",
		"v_gen_ai_sessions",
		"v_gen_ai_tool_usage",
		"v_gen_ai_agent_trajectory",
		"v_mcp_spans",
		"v_mcp_tool_usage",
	} {
		if _, err := admin.Dataset(dataset).Table(name).Metadata(ctx); err != nil {
			t.Errorf("resource %s metadata: %v", name, err)
		}
	}

	assertIntegrationViewColumns(ctx, t, admin, project, dataset)
	if err := assertIntegrationTrajectoryTVFLiveExecution(ctx, t, admin, project, dataset); err != nil {
		t.Fatalf("live trajectory TVF execution: %v", err)
	}
}

func oneIntegrationTrace() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "it-agent")
	now := time.Now()
	spans := rs.ScopeSpans().AppendEmpty().Spans()

	s := spans.AppendEmpty()
	s.SetName("chat gemini-2.5-pro")
	s.SetTraceID(pcommon.TraceID{1})
	s.SetSpanID(pcommon.SpanID{1})
	s.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-time.Second)))
	s.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
	s.Status().SetCode(ptrace.StatusCodeError)
	attrs := s.Attributes()
	attrs.PutStr("gen_ai.operation.name", "chat")
	attrs.PutStr("gen_ai.system", "gcp.vertex.agent")
	attrs.PutStr("gen_ai.request.model", "gemini-2.5-pro")
	attrs.PutStr("gen_ai.conversation.id", "sess-it-1")
	attrs.PutStr("gen_ai.tool.name", "search")
	attrs.PutStr("mcp.method.name", "tools/call")
	attrs.PutStr("gen_ai.main_agent.id", "main-1")
	attrs.PutStr("gen_ai.main_agent.name", "planner")
	attrs.PutStr("gen_ai.main_agent.description", "root planner")
	attrs.PutStr("gen_ai.agent.version", "1.0.0")
	attrs.PutStr("gen_ai.response.id", "resp-1")
	attrs.PutDouble("gen_ai.response.time_to_first_chunk", 0.25)
	attrs.PutInt("gen_ai.usage.input_tokens", 10)
	attrs.PutInt("gen_ai.usage.output_tokens", 5)
	attrs.PutStr("error.type", "rate_limited")

	// A root invoke_agent span and its healthy child chat span in TraceID{2}
	// exercise both top_error_types NULL exclusion and recursive depth-1
	// traversal + ancestor token-leaf suppression in v_gen_ai_agent_trajectory.
	rootAgent := spans.AppendEmpty()
	rootAgent.SetName("invoke_agent planner")
	rootAgent.SetTraceID(pcommon.TraceID{2})
	rootAgent.SetSpanID(pcommon.SpanID{3})
	rootAgent.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-600 * time.Millisecond)))
	rootAgent.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
	rootAgent.Status().SetCode(ptrace.StatusCodeOk)
	rootAttrs := rootAgent.Attributes()
	rootAttrs.PutStr("gen_ai.operation.name", "invoke_agent")
	rootAttrs.PutStr("gen_ai.system", "gcp.vertex.agent")
	rootAttrs.PutStr("gen_ai.agent.name", "planner")
	rootAttrs.PutInt("gen_ai.usage.input_tokens", 8)
	rootAttrs.PutInt("gen_ai.usage.output_tokens", 4)

	okSpan := spans.AppendEmpty()
	okSpan.SetName("chat gemini-2.5-pro ok")
	okSpan.SetTraceID(pcommon.TraceID{2})
	okSpan.SetSpanID(pcommon.SpanID{2})
	okSpan.SetParentSpanID(pcommon.SpanID{3})
	okSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-500 * time.Millisecond)))
	okSpan.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
	okSpan.Status().SetCode(ptrace.StatusCodeOk)
	okAttrs := okSpan.Attributes()
	okAttrs.PutStr("gen_ai.operation.name", "chat")
	okAttrs.PutStr("gen_ai.system", "gcp.vertex.agent")
	okAttrs.PutStr("gen_ai.request.model", "gemini-2.5-pro")
	okAttrs.PutStr("gen_ai.conversation.id", "sess-it-1")
	okAttrs.PutStr("gen_ai.tool.name", "search")
	okAttrs.PutStr("mcp.method.name", "tools/call")
	okAttrs.PutInt("gen_ai.usage.input_tokens", 8)
	okAttrs.PutInt("gen_ai.usage.output_tokens", 4)

	// TraceID{3} in a second ResourceSpans ("it-trajectory-svc") exercises
	// cross-window [-24h, +1h] parent correlation, orphan-flag inheritance, and
	// pre-filter step_index in tvf_gen_ai_agent_trajectory:
	// - SpanID{4} starts at now - 2h (outside a [now - 30m, now + 30m) TVF query
	//   window, but inside [start_ts - 24h, end_ts + 1h]) with missing parent
	//   SpanID{99} (so it is an orphan root at step_index = 1).
	// - SpanID{5} starts at now - 400ms (inside [now - 30m, now + 30m)) with
	//   parent SpanID{4}, so it is emitted with depth = 1, root_span_id = "04...",
	//   nearest_agent_name = "orphan_root", is_orphan = true, step_index = 2,
	//   is_leaf = true, and is_token_leaf = true.
	rsTraj := td.ResourceSpans().AppendEmpty()
	rsTraj.Resource().Attributes().PutStr("service.name", "it-trajectory-svc")
	trajSpans := rsTraj.ScopeSpans().AppendEmpty().Spans()

	orphanRoot := trajSpans.AppendEmpty()
	orphanRoot.SetName("invoke_agent orphan_root")
	orphanRoot.SetTraceID(pcommon.TraceID{3})
	orphanRoot.SetSpanID(pcommon.SpanID{4})
	orphanRoot.SetParentSpanID(pcommon.SpanID{0x99})
	orphanRoot.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-2 * time.Hour)))
	orphanRoot.SetEndTimestamp(pcommon.NewTimestampFromTime(now.Add(-2*time.Hour + time.Second)))
	orphanRoot.Status().SetCode(ptrace.StatusCodeOk)
	orAttrs := orphanRoot.Attributes()
	orAttrs.PutStr("gen_ai.operation.name", "invoke_agent")
	orAttrs.PutStr("gen_ai.system", "gcp.vertex.agent")
	orAttrs.PutStr("gen_ai.agent.name", "orphan_root")
	orAttrs.PutInt("gen_ai.usage.input_tokens", 20)

	orphanChild := trajSpans.AppendEmpty()
	orphanChild.SetName("chat gemini-2.5-pro orphan-child")
	orphanChild.SetTraceID(pcommon.TraceID{3})
	orphanChild.SetSpanID(pcommon.SpanID{5})
	orphanChild.SetParentSpanID(pcommon.SpanID{4})
	orphanChild.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-400 * time.Millisecond)))
	orphanChild.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
	orphanChild.Status().SetCode(ptrace.StatusCodeOk)
	ocAttrs := orphanChild.Attributes()
	ocAttrs.PutStr("gen_ai.operation.name", "chat")
	ocAttrs.PutStr("gen_ai.system", "gcp.vertex.agent")
	ocAttrs.PutInt("gen_ai.usage.input_tokens", 20)
	return td
}

func oneIntegrationLog() plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "it-agent")
	record := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
	record.SetEventName("gen_ai.choice")
	record.SetTraceID(pcommon.TraceID{1})
	record.SetSpanID(pcommon.SpanID{1})
	record.Attributes().PutStr("gen_ai.system", "gcp.vertex.agent")
	record.Attributes().PutStr("error.type", "rate_limited")
	record.Body().SetStr("auto-create integration test")
	return ld
}

func oneIntegrationMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "it-agent")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("gen_ai.client.inference.usage.input_tokens")
	m.SetUnit("{token}")
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	sum.SetIsMonotonic(true)
	dp := sum.DataPoints().AppendEmpty()
	now := time.Now()
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-time.Second)))
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.SetIntValue(10)
	dp.Attributes().PutStr("gen_ai.system", "gcp.vertex.agent")
	dp.Attributes().PutStr("gen_ai.token.modality", "text")
	dp.Attributes().PutStr("gen_ai.agent.name", "planner")
	ex := dp.Exemplars().AppendEmpty()
	ex.SetTimestamp(pcommon.NewTimestampFromTime(now))
	ex.SetIntValue(10)
	ex.SetTraceID(pcommon.TraceID{1})
	ex.SetSpanID(pcommon.SpanID{1})
	return md
}

func assertIntegrationViewColumns(ctx context.Context, t *testing.T, client *bq.Client, project, dataset string) {
	t.Helper()
	for _, tc := range []struct {
		name string
		sql  string
		want []bq.Value
	}{
		{
			name: "v_gen_ai_spans",
			sql: fmt.Sprintf(
				"SELECT gen_ai_main_agent_id, gen_ai_main_agent_name, gen_ai_main_agent_description, gen_ai_agent_version, gen_ai_response_id, gen_ai_response_time_to_first_chunk FROM `%s.%s.v_gen_ai_spans` WHERE error_type IS NOT NULL LIMIT 1",
				project, dataset,
			),
			want: []bq.Value{"main-1", "planner", "root planner", "1.0.0", "resp-1", float64(0.25)},
		},
		{
			name: "v_gen_ai_logs",
			sql:  fmt.Sprintf("SELECT error_type FROM `%s.%s.v_gen_ai_logs` LIMIT 1", project, dataset),
			want: []bq.Value{"rate_limited"},
		},
		{
			name: "v_gen_ai_metrics",
			sql:  fmt.Sprintf("SELECT gen_ai_token_modality, gen_ai_agent_name, gen_ai_inference_usage_dimension FROM `%s.%s.v_gen_ai_metrics` LIMIT 1", project, dataset),
			want: []bq.Value{"text", "planner", "input"},
		},
		{
			name: "v_gen_ai_metric_exemplars",
			sql:  fmt.Sprintf("SELECT exemplar_value_int64, span_name, span_error_type FROM `%s.%s.v_gen_ai_metric_exemplars` LIMIT 1", project, dataset),
			want: []bq.Value{int64(10), "chat gemini-2.5-pro", "rate_limited"},
		},
		{
			name: "v_gen_ai_model_costs",
			sql:  fmt.Sprintf("SELECT typed_error_count, ARRAY_LENGTH(top_error_types), top_error_types[OFFSET(0)].value FROM `%s.%s.v_gen_ai_model_costs` WHERE service_name = 'it-agent' LIMIT 1", project, dataset),
			want: []bq.Value{int64(1), int64(1), "rate_limited"},
		},
		{
			name: "v_gen_ai_sessions",
			sql:  fmt.Sprintf("SELECT typed_error_count, ARRAY_LENGTH(top_error_types), top_error_types[OFFSET(0)].value FROM `%s.%s.v_gen_ai_sessions` WHERE service_name = 'it-agent' LIMIT 1", project, dataset),
			want: []bq.Value{int64(1), int64(1), "rate_limited"},
		},
		{
			name: "v_gen_ai_tool_usage",
			sql:  fmt.Sprintf("SELECT typed_error_count, ARRAY_LENGTH(top_error_types), top_error_types[OFFSET(0)].value FROM `%s.%s.v_gen_ai_tool_usage` WHERE service_name = 'it-agent' LIMIT 1", project, dataset),
			want: []bq.Value{int64(1), int64(1), "rate_limited"},
		},
		{
			name: "v_gen_ai_agent_trajectory",
			sql:  fmt.Sprintf("SELECT depth, is_orphan, is_leaf, is_token_leaf FROM `%s.%s.v_gen_ai_agent_trajectory` WHERE error_type IS NOT NULL LIMIT 1", project, dataset),
			want: []bq.Value{int64(0), false, true, true},
		},
		{
			name: "v_gen_ai_agent_trajectory (recursive parent suppressed)",
			sql:  fmt.Sprintf("SELECT depth, is_orphan, is_leaf, is_token_leaf FROM `%s.%s.v_gen_ai_agent_trajectory` WHERE span_id = '0300000000000000' LIMIT 1", project, dataset),
			want: []bq.Value{int64(0), false, false, false},
		},
		{
			name: "v_gen_ai_agent_trajectory (recursive depth 1 child)",
			sql:  fmt.Sprintf("SELECT depth, root_span_id, nearest_agent_name, is_orphan, is_leaf, is_token_leaf FROM `%s.%s.v_gen_ai_agent_trajectory` WHERE span_id = '0200000000000000' LIMIT 1", project, dataset),
			want: []bq.Value{int64(1), "0300000000000000", "planner", false, true, true},
		},
		{
			name: "v_mcp_spans",
			sql:  fmt.Sprintf("SELECT mcp_method_name, is_tool_call, also_in_gen_ai_view FROM `%s.%s.v_mcp_spans` WHERE error_type IS NOT NULL LIMIT 1", project, dataset),
			want: []bq.Value{"tools/call", true, true},
		},
		{
			name: "v_mcp_tool_usage",
			sql:  fmt.Sprintf("SELECT call_count, gen_ai_tagged_call_count, typed_error_count, ARRAY_LENGTH(top_error_types), top_error_types[OFFSET(0)].value FROM `%s.%s.v_mcp_tool_usage` LIMIT 1", project, dataset),
			want: []bq.Value{int64(2), int64(2), int64(1), int64(1), "rate_limited"},
		},
	} {
		rows, err := client.Query(tc.sql).Read(ctx)
		if err != nil {
			t.Errorf("%s query: %v", tc.name, err)
			continue
		}
		var got []bq.Value
		if err := rows.Next(&got); err != nil {
			t.Errorf("%s rows.Next: %v", tc.name, err)
			continue
		}
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("%s columns = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func assertIntegrationTrajectoryTVFLiveExecution(ctx context.Context, t *testing.T, client *bq.Client, project, dataset string) error {
	t.Helper()
	sql := fmt.Sprintf(
		"SELECT span_id, depth, root_span_id, nearest_agent_name, is_orphan, step_index, is_leaf, is_token_leaf "+
			"FROM `%s.%s.%s`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR), TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) "+
			"ORDER BY span_id",
		project, dataset, schema.GenAIAgentTrajectoryTVFName,
	)
	job, err := client.Query(sql).Run(ctx)
	if err != nil {
		return fmt.Errorf("live query %s: %w", schema.GenAIAgentTrajectoryTVFName, err)
	}
	st, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("wait live query %s: %w", schema.GenAIAgentTrajectoryTVFName, err)
	}
	if err := st.Err(); err != nil {
		return fmt.Errorf("status live query %s: %w", schema.GenAIAgentTrajectoryTVFName, err)
	}
	if st.Statistics != nil {
		var slotMillis int64
		if st.Statistics.Details != nil {
			if qs, ok := st.Statistics.Details.(*bq.QueryStatistics); ok && qs != nil {
				slotMillis = qs.SlotMillis
			}
		}
		t.Logf("live %s query succeeded (TotalBytesProcessed=%d, SlotMillis=%d)",
			schema.GenAIAgentTrajectoryTVFName, st.Statistics.TotalBytesProcessed, slotMillis)
	}
	it, err := job.Read(ctx)
	if err != nil {
		return fmt.Errorf("read live query %s: %w", schema.GenAIAgentTrajectoryTVFName, err)
	}
	var got [][]bq.Value
	for {
		var row []bq.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("next live query %s: %w", schema.GenAIAgentTrajectoryTVFName, err)
		}
		got = append(got, row)
	}
	want := [][]bq.Value{
		{"0100000000000000", int64(0), "0100000000000000", nil, false, int64(1), true, true},
		{"0200000000000000", int64(1), "0300000000000000", "planner", false, int64(2), true, true},
		{"0300000000000000", int64(0), "0300000000000000", "planner", false, int64(1), false, false},
		{"0500000000000000", int64(1), "0400000000000000", "orphan_root", true, int64(2), true, true},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		return fmt.Errorf("live %s rows = %v, want %v", schema.GenAIAgentTrajectoryTVFName, got, want)
	}
	return nil
}

func assertIntegrationViewDryRuns(ctx context.Context, t *testing.T, client *bq.Client, project, dataset, spansTable, logsTable string) {
	t.Helper()
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "MCPSpansViewQuery",
			sql:  fmt.Sprintf(schema.MCPSpansViewQuery, project, dataset, spansTable),
		},
		{
			name: "MCPToolUsageViewQuery",
			sql:  fmt.Sprintf(schema.MCPToolUsageViewQuery, project, dataset),
		},
	} {
		q := client.Query(tc.sql)
		q.DryRun = true
		job, err := q.Run(ctx)
		if err != nil {
			t.Errorf("%s dry-run failed: %v", tc.name, err)
			continue
		}
		if err := job.LastStatus().Err(); err != nil {
			t.Errorf("%s dry-run status error: %v", tc.name, err)
			continue
		}
		if got := job.LastStatus().Statistics.TotalBytesProcessed; got != 0 {
			t.Errorf("%s dry-run TotalBytesProcessed = %d, want 0 on empty tables", tc.name, got)
		}
	}

	for _, view := range []string{
		schema.SpansAsAllSpansViewName,
		schema.GenAISpansViewName,
		schema.GenAILogsViewName,
		schema.GenAIMetricsViewName,
		schema.GenAIMetricExemplarsViewName,
		schema.GenAIModelCostsViewName,
		schema.GenAISessionsViewName,
		schema.GenAIToolUsageViewName,
		schema.GenAIAgentTrajectoryViewName,
		schema.MCPSpansViewName,
		schema.MCPToolUsageViewName,
	} {
		q := client.Query(fmt.Sprintf("SELECT * FROM `%s.%s.%s` WHERE FALSE", project, dataset, view))
		q.DryRun = true
		job, err := q.Run(ctx)
		if err != nil {
			t.Errorf("view %s WHERE FALSE dry-run failed: %v", view, err)
			continue
		}
		if err := job.LastStatus().Err(); err != nil {
			t.Errorf("view %s WHERE FALSE dry-run status error: %v", view, err)
			continue
		}
		if got := job.LastStatus().Statistics.TotalBytesProcessed; got != 0 {
			t.Errorf("view %s WHERE FALSE dry-run TotalBytesProcessed = %d, want 0", view, got)
		}
	}

	// Provision and dry-run the three bounded-window companion TVFs.
	for tvfName, ddl := range schema.CompanionTVFDDLs(project, dataset, spansTable, logsTable) {
		job, err := client.Query(ddl).Run(ctx)
		if err != nil {
			t.Fatalf("create TVF %s: %v", tvfName, err)
		}
		st, err := job.Wait(ctx)
		if err != nil {
			t.Fatalf("wait TVF %s: %v", tvfName, err)
		}
		if err := st.Err(); err != nil {
			t.Fatalf("TVF %s DDL status: %v", tvfName, err)
		}

		callArgs := "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), CURRENT_TIMESTAMP()"
		if tvfName == schema.GenAIAgentTrajectoryTVFName {
			callArgs = "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR), TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)"
		}
		q := client.Query(fmt.Sprintf(
			"SELECT * FROM `%s.%s.%s`(%s)",
			project, dataset, tvfName, callArgs,
		))
		q.DryRun = true
		dryJob, err := q.Run(ctx)
		if err != nil {
			t.Errorf("TVF %s dry-run failed: %v", tvfName, err)
			continue
		}
		if err := dryJob.LastStatus().Err(); err != nil {
			t.Errorf("TVF %s dry-run status error: %v", tvfName, err)
			continue
		}
		if got := dryJob.LastStatus().Statistics.TotalBytesProcessed; got != 0 {
			t.Errorf("TVF %s dry-run TotalBytesProcessed = %d, want 0 on empty tables", tvfName, got)
		}
	}
}

func assertIntegrationSearchIndexes(
	ctx context.Context, client *bq.Client,
	project, dataset, spansTable, logsTable, metricsTable string,
) error {
	wantIndexes := map[string]string{
		spansTable:   "otel_spans_trace_lookup_idx",
		logsTable:    "otel_logs_trace_lookup_idx",
		metricsTable: "otel_metrics_trace_lookup_idx",
	}
	q := client.Query(fmt.Sprintf(
		"SELECT table_name, index_name, index_status FROM `%s.%s.INFORMATION_SCHEMA.SEARCH_INDEXES`",
		project, dataset,
	))
	rows, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("query INFORMATION_SCHEMA.SEARCH_INDEXES: %w", err)
	}
	found := make(map[string]string)
	statuses := make(map[string]string)
	for {
		var vals []bq.Value
		err := rows.Next(&vals)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("read INFORMATION_SCHEMA.SEARCH_INDEXES: %w", err)
		}
		if len(vals) >= 3 {
			tbl, _ := vals[0].(string)
			idx, _ := vals[1].(string)
			status, _ := vals[2].(string)
			found[tbl] = idx
			statuses[tbl] = status
		}
	}
	for tbl, wantIdx := range wantIndexes {
		if gotIdx := found[tbl]; gotIdx != wantIdx {
			return fmt.Errorf("search index on %s = %q, want %q (all found: %v)", tbl, gotIdx, wantIdx, found)
		}
		status := statuses[tbl]
		if status != "ACTIVE" && status != "PENDING" && status != "TEMPORARILY DISABLED" {
			return fmt.Errorf("search index status on %s = %q, want ACTIVE, PENDING, or TEMPORARILY DISABLED", tbl, status)
		}
	}
	return nil
}

func waitForRows(ctx context.Context, client *bq.Client, project, dataset, table string) error {
	query := client.Query(fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s.%s`", project, dataset, table))
	for {
		rows, err := query.Read(ctx)
		if err == nil {
			var values []bq.Value
			err = rows.Next(&values)
			if err == nil && len(values) == 1 {
				if count, ok := values[0].(int64); ok && count > 0 {
					return nil
				}
			}
			if err != nil && !errors.Is(err, iterator.Done) {
				return fmt.Errorf("query %s.%s.%s: %w", project, dataset, table, err)
			}
		}
		if ctx.Err() != nil {
			return fmt.Errorf("no rows became queryable in %s.%s.%s: %w", project, dataset, table, ctx.Err())
		}
		time.Sleep(time.Second)
	}
}

func assertIntegrationTrajectoryDryRunAndPartitionFilter(ctx context.Context, client *bq.Client, project, dataset string) error {
	dryRunQuery := client.Query(fmt.Sprintf(schema.GenAIAgentTrajectoryViewQuery, project, dataset))
	dryRunQuery.DryRun = true
	dryRunJob, err := dryRunQuery.Run(ctx)
	if err != nil {
		return fmt.Errorf("dry-run GenAIAgentTrajectoryViewQuery: %w", err)
	}
	dryRunStatus := dryRunJob.LastStatus()
	if dryRunStatus.Statistics == nil {
		return errors.New("dry-run GenAIAgentTrajectoryViewQuery returned nil statistics")
	}
	if dryRunStatus.Statistics.TotalBytesProcessed != 0 {
		return fmt.Errorf("dry-run TotalBytesProcessed on empty table = %d, want 0", dryRunStatus.Statistics.TotalBytesProcessed)
	}

	const (
		rpfSpansTable   = "otel_spans_rpf"
		rpfLogsTable    = "otel_logs_rpf"
		rpfMetricsTable = "otel_metrics_rpf"
	)
	for _, tc := range []struct {
		name string
		fn   func() (*bq.TableMetadata, string, error)
	}{
		{rpfSpansTable, func() (*bq.TableMetadata, string, error) {
			return schema.DestinationMetadataWithOptions(
				schema.SpansTableSchema(), project, dataset, rpfSpansTable, schema.DestinationOptions{RequirePartitionFilter: true})
		}},
		{rpfLogsTable, func() (*bq.TableMetadata, string, error) {
			return schema.DestinationMetadataWithOptions(
				schema.LogsTableSchema(), project, dataset, rpfLogsTable, schema.DestinationOptions{RequirePartitionFilter: true})
		}},
		{rpfMetricsTable, func() (*bq.TableMetadata, string, error) {
			return schema.DestinationMetadataWithOptions(
				schema.MetricsTableSchema(), project, dataset, rpfMetricsTable, schema.DestinationOptions{RequirePartitionFilter: true})
		}},
	} {
		md, dedupQuery, err := tc.fn()
		if err != nil {
			return fmt.Errorf("derive %s metadata: %w", tc.name, err)
		}
		if err := client.Dataset(dataset).Table(tc.name).Create(ctx, md); err != nil {
			return fmt.Errorf("create %s: %w", tc.name, err)
		}
		if err := client.Dataset(dataset).Table(tc.name+"_dedup").Create(ctx, &bq.TableMetadata{
			ViewQuery:    dedupQuery,
			UseLegacySQL: false,
		}); err != nil {
			return fmt.Errorf("create %s_dedup: %w", tc.name, err)
		}
	}
	if err := client.Dataset(dataset).Table("v_gen_ai_spans_rpf").Create(ctx, &bq.TableMetadata{
		ViewQuery:    fmt.Sprintf(schema.GenAISpansViewQuery, project, dataset, rpfSpansTable),
		UseLegacySQL: false,
	}); err != nil {
		return fmt.Errorf("create v_gen_ai_spans_rpf: %w", err)
	}
	if err := client.Dataset(dataset).Table("v_gen_ai_metrics_rpf").Create(ctx, &bq.TableMetadata{
		ViewQuery:    fmt.Sprintf(schema.GenAIMetricsViewQuery, project, dataset, rpfMetricsTable),
		UseLegacySQL: false,
	}); err != nil {
		return fmt.Errorf("create v_gen_ai_metrics_rpf: %w", err)
	}
	rpfTrajectoryQuery := strings.ReplaceAll(
		fmt.Sprintf(schema.GenAIAgentTrajectoryViewQuery, project, dataset),
		"`"+project+"."+dataset+"."+schema.GenAISpansViewName+"`",
		"`"+project+"."+dataset+".v_gen_ai_spans_rpf`",
	)
	if err := client.Dataset(dataset).Table("v_gen_ai_agent_trajectory_rpf").Create(ctx, &bq.TableMetadata{
		ViewQuery:    rpfTrajectoryQuery,
		UseLegacySQL: false,
	}); err != nil {
		return fmt.Errorf("create v_gen_ai_agent_trajectory_rpf: %w", err)
	}

	rpfSelect := client.Query(fmt.Sprintf(
		"SELECT * FROM `%s.%s.v_gen_ai_agent_trajectory_rpf` WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)",
		project, dataset,
	))
	rpfSelect.DryRun = true
	_, rpfErr := rpfSelect.Run(ctx)
	if rpfErr == nil {
		return errors.New("dry-run SELECT over v_gen_ai_agent_trajectory_rpf with require_partition_filter=true succeeded, want rejection")
	}
	lowerErr := strings.ToLower(rpfErr.Error())
	if !strings.Contains(lowerErr, "partition") || !strings.Contains(lowerErr, "filter") {
		return fmt.Errorf("dry-run SELECT over v_gen_ai_agent_trajectory_rpf error = %w, want partition filter rejection", rpfErr)
	}

	// Prove that all three bounded-window TVF counterparts succeed against
	// require_partition_filter=true base tables.
	rpfLogsTVFDDL := strings.ReplaceAll(
		fmt.Sprintf(schema.GenAILogsTVFDDL, project, dataset, rpfLogsTable, rpfSpansTable),
		"`"+project+"."+dataset+"."+schema.GenAILogsTVFName+"`",
		"`"+project+"."+dataset+".tvf_gen_ai_logs_rpf`",
	)
	rpfExemplarsTVFDDL := strings.ReplaceAll(
		strings.ReplaceAll(
			fmt.Sprintf(schema.GenAIMetricExemplarsTVFDDL, project, dataset, rpfSpansTable),
			"`"+project+"."+dataset+"."+schema.GenAIMetricExemplarsTVFName+"`",
			"`"+project+"."+dataset+".tvf_gen_ai_metric_exemplars_rpf`",
		),
		"`"+project+"."+dataset+"."+schema.GenAIMetricsViewName+"`",
		"`"+project+"."+dataset+".v_gen_ai_metrics_rpf`",
	)
	rpfTrajectoryTVFDDL := strings.ReplaceAll(
		strings.ReplaceAll(
			fmt.Sprintf(schema.GenAIAgentTrajectoryTVFDDL, project, dataset),
			"`"+project+"."+dataset+"."+schema.GenAIAgentTrajectoryTVFName+"`",
			"`"+project+"."+dataset+".tvf_gen_ai_agent_trajectory_rpf`",
		),
		"`"+project+"."+dataset+"."+schema.GenAISpansViewName+"`",
		"`"+project+"."+dataset+".v_gen_ai_spans_rpf`",
	)

	for _, tc := range []struct {
		name      string
		ddl       string
		narrowSQL string
		wideSQL   string
	}{
		{
			name: "tvf_gen_ai_logs_rpf",
			ddl:  rpfLogsTVFDDL,
			narrowSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_logs_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
			wideSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_logs_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 96 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
		},
		{
			name: "tvf_gen_ai_metric_exemplars_rpf",
			ddl:  rpfExemplarsTVFDDL,
			narrowSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_metric_exemplars_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
			wideSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_metric_exemplars_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 96 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
		},
		{
			name: "tvf_gen_ai_agent_trajectory_rpf",
			ddl:  rpfTrajectoryTVFDDL,
			narrowSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_agent_trajectory_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
			wideSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_agent_trajectory_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 96 HOUR), CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 96 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
		},
	} {
		tvfJob, err := client.Query(tc.ddl).Run(ctx)
		if err != nil {
			return fmt.Errorf("create %s: %w", tc.name, err)
		}
		st, err := tvfJob.Wait(ctx)
		if err != nil {
			return fmt.Errorf("wait %s: %w", tc.name, err)
		}
		if err := st.Err(); err != nil {
			return fmt.Errorf("status %s: %w", tc.name, err)
		}
		rpfTVFSelect := client.Query(tc.narrowSQL)
		rpfTVFSelect.DryRun = true
		rpfTVFJob, err := rpfTVFSelect.Run(ctx)
		if err != nil {
			return fmt.Errorf("dry-run SELECT over %s with require_partition_filter=true failed: %w", tc.name, err)
		}
		if got := rpfTVFJob.LastStatus().Statistics.TotalBytesProcessed; got != 0 {
			return fmt.Errorf("%s empty-table dry-run TotalBytesProcessed = %d, want 0", tc.name, got)
		}
	}

	// Seed two distinct partitions (-72h and -30m) in otel_spans_rpf, otel_logs_rpf,
	// and otel_metrics_rpf via DML INSERT INTO so dry-run TotalBytesProcessed proves
	// nonzero partition pruning (0 < narrowBytes < wideBytes) across all three TVFs.
	seedSQL := fmt.Sprintf(`
		INSERT INTO `+"`%s.%s.%s`"+` (
			start_time, start_time_unix_nano, end_time, end_time_unix_nano,
			receive_time, receive_time_unix_nano, duration_nano,
			trace_id, span_id, name, kind, service_name, attributes, schema_version
		)
		VALUES
			(
				TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 72 HOUR), 1000,
				TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 72 HOUR), 2000,
				CURRENT_TIMESTAMP(), 3000, 1000,
				'0102030405060708090a0b0c0d0e0f10', '0102030405060708',
				'invoke_agent old', 1, 'rpf-svc', JSON '{"gen_ai.operation.name":"invoke_agent","gen_ai.agent.name":"planner"}', 'v0alpha2'
			),
			(
				TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), 4000,
				TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), 5000,
				CURRENT_TIMESTAMP(), 6000, 1000,
				'1112131415161718191a1b1c1d1e1f20', '1112131415161718',
				'invoke_agent recent', 1, 'rpf-svc', JSON '{"gen_ai.operation.name":"invoke_agent","gen_ai.agent.name":"planner"}', 'v0alpha2'
			);
		INSERT INTO `+"`%s.%s.%s`"+` (
			timestamp, timestamp_source, time_unix_nano, observed_time_unix_nano,
			receive_time, receive_time_unix_nano, trace_id, span_id,
			event_name, service_name, log_name, insert_id, schema_version
		)
		VALUES
			(
				TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 72 HOUR), 'event_time', 1000, 1000,
				CURRENT_TIMESTAMP(), 3000, '0102030405060708090a0b0c0d0e0f10', '0102030405060708',
				'gen_ai.choice', 'rpf-svc', 'otel', 'ins-old', 'v0alpha2'
			),
			(
				TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), 'event_time', 4000, 4000,
				CURRENT_TIMESTAMP(), 6000, '1112131415161718191a1b1c1d1e1f20', '1112131415161718',
				'gen_ai.choice', 'rpf-svc', 'otel', 'ins-recent', 'v0alpha2'
			);
		INSERT INTO `+"`%s.%s.%s`"+` (
			timestamp, time_unix_nano, receive_time, receive_time_unix_nano,
			service_name, metric_name, metric_type, value_type, value_int64,
			exemplars, point_fingerprint, schema_version
		)
		VALUES
			(
				TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 72 HOUR), 1000, CURRENT_TIMESTAMP(), 3000,
				'rpf-svc', 'gen_ai.client.token.usage', 'SUM', 'INT64', 100,
				[STRUCT(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 72 HOUR) AS time, 1000 AS time_unix_nano, CAST(NULL AS FLOAT64) AS value_double, 100 AS value_int64, '0102030405060708' AS span_id, '0102030405060708090a0b0c0d0e0f10' AS trace_id, CAST(NULL AS JSON) AS filtered_attributes, CAST(NULL AS JSON) AS filtered_attributes_schema)],
				'fp-old', 'v0alpha2'
			),
			(
				TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), 4000, CURRENT_TIMESTAMP(), 6000,
				'rpf-svc', 'gen_ai.client.token.usage', 'SUM', 'INT64', 200,
				[STRUCT(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE) AS time, 4000 AS time_unix_nano, CAST(NULL AS FLOAT64) AS value_double, 200 AS value_int64, '1112131415161718' AS span_id, '1112131415161718191a1b1c1d1e1f20' AS trace_id, CAST(NULL AS JSON) AS filtered_attributes, CAST(NULL AS JSON) AS filtered_attributes_schema)],
				'fp-recent', 'v0alpha2'
			);
	`, project, dataset, rpfSpansTable, project, dataset, rpfLogsTable, project, dataset, rpfMetricsTable)
	seedJob, err := client.Query(seedSQL).Run(ctx)
	if err != nil {
		return fmt.Errorf("seed RPF partitions: %w", err)
	}
	seedStatus, err := seedJob.Wait(ctx)
	if err != nil {
		return fmt.Errorf("wait seed RPF partitions: %w", err)
	}
	if err := seedStatus.Err(); err != nil {
		return fmt.Errorf("seed RPF partitions status: %w", err)
	}

	dryRunBytes := func(sql string) (int64, error) {
		q := client.Query(sql)
		q.DryRun = true
		j, err := q.Run(ctx)
		if err != nil {
			return 0, err
		}
		st := j.LastStatus()
		if st == nil || st.Statistics == nil {
			return 0, errors.New("dry-run job missing LastStatus().Statistics")
		}
		if err := st.Err(); err != nil {
			return 0, err
		}
		return st.Statistics.TotalBytesProcessed, nil
	}

	for _, tc := range []struct {
		name      string
		narrowSQL string
		wideSQL   string
	}{
		{
			name: "tvf_gen_ai_logs_rpf",
			narrowSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_logs_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
			wideSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_logs_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 96 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
		},
		{
			name: "tvf_gen_ai_metric_exemplars_rpf",
			narrowSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_metric_exemplars_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
			wideSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_metric_exemplars_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 96 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
		},
		{
			name: "tvf_gen_ai_agent_trajectory_rpf",
			narrowSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_agent_trajectory_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
			wideSQL: fmt.Sprintf(
				"SELECT * FROM `%s.%s.tvf_gen_ai_agent_trajectory_rpf`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 96 HOUR), CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 96 HOUR), CURRENT_TIMESTAMP())",
				project, dataset,
			),
		},
	} {
		narrowBytes, err := dryRunBytes(tc.narrowSQL)
		if err != nil {
			return fmt.Errorf("%s narrow dry-run failed: %w", tc.name, err)
		}
		wideBytes, err := dryRunBytes(tc.wideSQL)
		if err != nil {
			return fmt.Errorf("%s wide dry-run failed: %w", tc.name, err)
		}
		if narrowBytes <= 0 || narrowBytes >= wideBytes {
			return fmt.Errorf("%s partition pruning check: narrowBytes=%d, wideBytes=%d; want 0 < narrowBytes < wideBytes", tc.name, narrowBytes, wideBytes)
		}
	}
	return nil
}
