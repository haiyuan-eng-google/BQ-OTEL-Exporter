// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"
	"os"
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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dataset := fmt.Sprintf("otel_exporter_it_%d", time.Now().UnixNano())

	httpClient, httpBase, err := newBigQueryHTTPClient(ctx, option.WithScopes(bigqueryapi.BigqueryScope))
	if err != nil {
		t.Fatalf("create cleanup/query HTTP client: %v", err)
	}
	admin, err := bq.NewClient(ctx, project, option.WithHTTPClient(httpClient))
	if err != nil {
		httpBase.CloseIdleConnections()
		t.Fatalf("create cleanup/query client: %v", err)
	}
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close cleanup/query client: %v", err)
		}
		httpBase.CloseIdleConnections()
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
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
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
	// paths and the reconciliation of existing views against the real metadata
	// API.
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
		"v_gen_ai_model_costs",
		"v_gen_ai_sessions",
		"v_gen_ai_tool_usage",
	} {
		if _, err := admin.Dataset(dataset).Table(name).Metadata(ctx); err != nil {
			t.Errorf("resource %s metadata: %v", name, err)
		}
	}

	assertIntegrationViewColumns(ctx, t, admin, project, dataset)
}

func oneIntegrationTrace() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "it-agent")
	now := time.Now()
	s := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
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
	attrs.PutStr("gen_ai.main_agent.id", "main-1")
	attrs.PutStr("gen_ai.main_agent.name", "planner")
	attrs.PutStr("gen_ai.main_agent.description", "root planner")
	attrs.PutStr("gen_ai.agent.version", "1.0.0")
	attrs.PutStr("gen_ai.response.id", "resp-1")
	attrs.PutDouble("gen_ai.response.time_to_first_chunk", 0.25)
	attrs.PutInt("gen_ai.usage.input_tokens", 10)
	attrs.PutInt("gen_ai.usage.output_tokens", 5)
	attrs.PutStr("error.type", "rate_limited")
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
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
	dp.SetIntValue(10)
	dp.Attributes().PutStr("gen_ai.system", "gcp.vertex.agent")
	dp.Attributes().PutStr("gen_ai.token.modality", "text")
	dp.Attributes().PutStr("gen_ai.agent.name", "planner")
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
				"SELECT gen_ai_main_agent_id, gen_ai_main_agent_name, gen_ai_main_agent_description, gen_ai_agent_version, gen_ai_response_id, gen_ai_response_time_to_first_chunk FROM `%s.%s.v_gen_ai_spans` LIMIT 1",
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
			name: "v_gen_ai_model_costs",
			sql:  fmt.Sprintf("SELECT typed_error_count, ARRAY_LENGTH(top_error_types) FROM `%s.%s.v_gen_ai_model_costs` LIMIT 1", project, dataset),
			want: []bq.Value{int64(1), int64(1)},
		},
		{
			name: "v_gen_ai_sessions",
			sql:  fmt.Sprintf("SELECT typed_error_count, ARRAY_LENGTH(top_error_types) FROM `%s.%s.v_gen_ai_sessions` LIMIT 1", project, dataset),
			want: []bq.Value{int64(1), int64(1)},
		},
		{
			name: "v_gen_ai_tool_usage",
			sql:  fmt.Sprintf("SELECT typed_error_count, ARRAY_LENGTH(top_error_types) FROM `%s.%s.v_gen_ai_tool_usage` LIMIT 1", project, dataset),
			want: []bq.Value{int64(1), int64(1)},
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
