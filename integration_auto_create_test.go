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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	// Register cancel first so t.Cleanup's LIFO order runs dataset deletion
	// and admin.Close before canceling ctx.
	t.Cleanup(cancel)
	dataset := fmt.Sprintf("otel_exporter_it_%d", time.Now().UnixNano())

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
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		if err := admin.Dataset(dataset).DeleteWithContents(cleanupCtx); err != nil {
			t.Errorf("cleanup dataset %s.%s: %v", project, dataset, err)
		}
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
	if err := traces.start(ctx, nil); err != nil {
		t.Fatalf("first trace startup: %v", err)
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
	assertIntegrationSearchIndexes(ctx, t, admin, project, dataset, cfg.Traces.Table, cfg.Logs.Table, cfg.Metrics.Table)

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
	assertIntegrationTrajectoryTVFLiveExecution(ctx, t, admin, project, dataset)
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
			sql:  fmt.Sprintf("SELECT typed_error_count, ARRAY_LENGTH(top_error_types), top_error_types[OFFSET(0)].value FROM `%s.%s.v_gen_ai_model_costs` LIMIT 1", project, dataset),
			want: []bq.Value{int64(1), int64(1), "rate_limited"},
		},
		{
			name: "v_gen_ai_sessions",
			sql:  fmt.Sprintf("SELECT typed_error_count, ARRAY_LENGTH(top_error_types), top_error_types[OFFSET(0)].value FROM `%s.%s.v_gen_ai_sessions` LIMIT 1", project, dataset),
			want: []bq.Value{int64(1), int64(1), "rate_limited"},
		},
		{
			name: "v_gen_ai_tool_usage",
			sql:  fmt.Sprintf("SELECT typed_error_count, ARRAY_LENGTH(top_error_types), top_error_types[OFFSET(0)].value FROM `%s.%s.v_gen_ai_tool_usage` LIMIT 1", project, dataset),
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
			sql:  fmt.Sprintf("SELECT depth, root_span_id, nearest_agent_name, is_orphan, is_leaf, is_token_leaf FROM `%s.%s.v_gen_ai_agent_trajectory` WHERE depth = 1 LIMIT 1", project, dataset),
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

func assertIntegrationTrajectoryTVFLiveExecution(ctx context.Context, t *testing.T, client *bq.Client, project, dataset string) {
	t.Helper()
	sql := fmt.Sprintf(
		"SELECT span_id, depth, root_span_id, nearest_agent_name, is_orphan, is_leaf, is_token_leaf "+
			"FROM `%s.%s.%s`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) "+
			"ORDER BY span_id",
		project, dataset, schema.GenAIAgentTrajectoryTVFName,
	)
	job, err := client.Query(sql).Run(ctx)
	if err != nil {
		t.Fatalf("live query %s: %v", schema.GenAIAgentTrajectoryTVFName, err)
	}
	st, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("wait live query %s: %v", schema.GenAIAgentTrajectoryTVFName, err)
	}
	if err := st.Err(); err != nil {
		t.Fatalf("status live query %s: %v", schema.GenAIAgentTrajectoryTVFName, err)
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
		t.Fatalf("read live query %s: %v", schema.GenAIAgentTrajectoryTVFName, err)
	}
	var got [][]bq.Value
	for {
		var row []bq.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Fatalf("next live query %s: %v", schema.GenAIAgentTrajectoryTVFName, err)
		}
		got = append(got, row)
	}
	want := [][]bq.Value{
		{"0100000000000000", int64(0), "0100000000000000", nil, false, true, true},
		{"0200000000000000", int64(1), "0300000000000000", "planner", false, true, true},
		{"0300000000000000", int64(0), "0300000000000000", "planner", false, false, false},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("live %s rows = %v, want %v", schema.GenAIAgentTrajectoryTVFName, got, want)
	}
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

		q := client.Query(fmt.Sprintf(
			"SELECT * FROM `%s.%s.%s`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), CURRENT_TIMESTAMP())",
			project, dataset, tvfName,
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
	ctx context.Context, t *testing.T, client *bq.Client,
	project, dataset, spansTable, logsTable, metricsTable string,
) {
	t.Helper()
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
		t.Fatalf("query INFORMATION_SCHEMA.SEARCH_INDEXES: %v", err)
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
			t.Fatalf("read INFORMATION_SCHEMA.SEARCH_INDEXES: %v", err)
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
			t.Errorf("search index on %s = %q, want %q (all found: %v)", tbl, gotIdx, wantIdx, found)
		}
		if status := statuses[tbl]; status == "" {
			t.Errorf("search index status on %s is empty", tbl)
		}
	}
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
		return fmt.Errorf("dry-run SELECT over v_gen_ai_agent_trajectory_rpf error = %v, want partition filter rejection", rpfErr)
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
		name string
		ddl  string
	}{
		{"tvf_gen_ai_logs_rpf", rpfLogsTVFDDL},
		{"tvf_gen_ai_metric_exemplars_rpf", rpfExemplarsTVFDDL},
		{"tvf_gen_ai_agent_trajectory_rpf", rpfTrajectoryTVFDDL},
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
		rpfTVFSelect := client.Query(fmt.Sprintf(
			"SELECT * FROM `%s.%s.%s`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), CURRENT_TIMESTAMP())",
			project, dataset, tc.name,
		))
		rpfTVFSelect.DryRun = true
		rpfTVFJob, err := rpfTVFSelect.Run(ctx)
		if err != nil {
			return fmt.Errorf("dry-run SELECT over %s with require_partition_filter=true failed: %w", tc.name, err)
		}
		if got := rpfTVFJob.LastStatus().Statistics.TotalBytesProcessed; got != 0 {
			return fmt.Errorf("%s dry-run TotalBytesProcessed = %d, want 0", tc.name, got)
		}
	}
	return nil
}
