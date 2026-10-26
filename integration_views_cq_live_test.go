// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bigqueryexporter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// TestLiveContinuousQueriesDryRun validates all three Continuous Query SQL
// templates in deploy/continuous-queries/ against a live BigQuery dataset where
// otel_spans enforces require_partition_filter = true and otel_alerts is
// provisioned with the Terraform 7-column alert sink schema.
func TestLiveContinuousQueriesDryRun(t *testing.T) {
	project := os.Getenv("BQ_PROJECT")
	if project == "" {
		if os.Getenv("REQUIRE_LIVE_BQ") == "1" {
			t.Fatal("REQUIRE_LIVE_BQ=1 is set but BQ_PROJECT is empty")
		}
		t.Skip("BQ_PROJECT is unset; skipping live BigQuery Continuous Queries dry-run test")
	}
	location := os.Getenv("BQ_LOCATION")
	if location == "" {
		location = "US"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	dataset := uniqueEphemeralDatasetID("otel_cq_it")

	httpClient, _, closeHTTP, err := googlehttp.NewClient(ctx, []string{bigqueryapi.BigqueryScope})
	if err != nil {
		t.Fatalf("create HTTP client: %v", err)
	}
	admin, err := bq.NewClient(ctx, project, option.WithHTTPClient(httpClient))
	if err != nil {
		closeHTTP()
		t.Fatalf("create BigQuery client: %v", err)
	}
	rawSvc, err := bigqueryapi.NewService(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		_ = admin.Close()
		closeHTTP()
		t.Fatalf("create BigQuery v2 service: %v", err)
	}
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close BigQuery client: %v", err)
		}
		closeHTTP()
	})
	if err := createOwnedEphemeralDataset(ctx, admin, dataset, location, t.Cleanup, func(err error) {
		t.Errorf("%v", err)
	}); err != nil {
		t.Fatalf("create dataset %s.%s: %v", project, dataset, err)
	}

	const spansTable = "otel_spans"
	spansMD, _, err := schema.DestinationMetadataWithOptions(
		schema.SpansTableSchema(), project, dataset, spansTable, schema.DestinationOptions{RequirePartitionFilter: true})
	if err != nil {
		t.Fatalf("derive otel_spans metadata: %v", err)
	}
	if err := admin.Dataset(dataset).Table(spansTable).Create(ctx, spansMD); err != nil {
		t.Fatalf("create otel_spans: %v", err)
	}

	alertsMD := &bq.TableMetadata{
		TimePartitioning: &bq.TimePartitioning{
			Type:  bq.DayPartitioningType,
			Field: "event_time",
		},
		Clustering: &bq.Clustering{
			Fields: []string{"alert_kind", "service_name"},
		},
		Schema: bq.Schema{
			{Name: "alert_id", Type: bq.StringFieldType, Required: true},
			{Name: "alert_kind", Type: bq.StringFieldType, Required: true},
			{Name: "event_time", Type: bq.TimestampFieldType, Required: true},
			{Name: "service_name", Type: bq.StringFieldType},
			{Name: "trace_id", Type: bq.StringFieldType, Required: true},
			{Name: "span_id", Type: bq.StringFieldType, Required: true},
			{Name: "payload", Type: bq.JSONFieldType},
		},
	}
	if err := admin.Dataset(dataset).Table("otel_alerts").Create(ctx, alertsMD); err != nil {
		t.Fatalf("create otel_alerts: %v", err)
	}

	replacer := strings.NewReplacer(
		"${project_id}", project,
		"${dataset_id}", dataset,
		"${spans_table}", spansTable,
		"${pubsub_topic}", "otel-alerts-test",
		"${max_turn_input_tokens}", "32000",
		"${max_turn_output_tokens}", "8192",
		"${max_turn_duration_ms}", "30000",
	)

	templates := []string{
		"tool_error_rate_alert.sql",
		"token_budget_burn.sql",
		"agent_latency_p95.sql",
	}
	for _, name := range templates {
		t.Run(name, func(t *testing.T) {
			subCtx := t.Context()
			raw, err := os.ReadFile(filepath.Join("deploy", "continuous-queries", name))
			if err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
			rendered := replacer.Replace(string(raw))
			innerSelect, insertSQL := extractContinuousQuerySQLVariants(t, name, rendered, project, dataset)

			// 1. Dry-run the inner SELECT ... FROM APPENDS(TABLE ...) query
			// (validates partition filter satisfaction on otel_spans and output JSON shape).
			qSelect := admin.Query(innerSelect)
			qSelect.DryRun = true
			selectJob, err := qSelect.Run(subCtx)
			if err != nil {
				t.Fatalf("%s inner SELECT dry-run failed: %v\nSQL:\n%s", name, err, innerSelect)
			}
			assertDryRunJobSucceeded(t, name+" inner SELECT", selectJob)

			// 2. Dry-run the INSERT INTO `<project>.<dataset>.otel_alerts` table-sink
			// query against the provisioned 7-column otel_alerts table.
			qInsert := admin.Query(insertSQL)
			qInsert.DryRun = true
			insertJob, err := qInsert.Run(subCtx)
			if err != nil {
				t.Fatalf("%s INSERT INTO otel_alerts dry-run failed: %v\nSQL:\n%s", name, err, insertSQL)
			}
			assertDryRunJobSucceeded(t, name+" INSERT INTO otel_alerts", insertJob)

			// 3. Dry-run the Continuous Query EXPORT DATA ... CLOUD_PUBSUB statement
			// with continuous=true on JobConfigurationQuery.
			t.Run("export_data_continuous", func(t *testing.T) {
				exportJob := &bigqueryapi.Job{
					Configuration: &bigqueryapi.JobConfiguration{
						DryRun: true,
						Query: &bigqueryapi.JobConfigurationQuery{
							Query:        rendered,
							UseLegacySql: new(bool),
							Continuous:   true,
						},
					},
				}
				insertedJob, err := rawSvc.Jobs.Insert(project, exportJob).Context(subCtx).Do()
				skipReason, fatalErr := validateContinuousExportDryRunJob(insertedJob, err)
				if fatalErr != nil {
					t.Fatalf("%s EXPORT DATA continuous dry-run failed with unexpected error: %v", name, fatalErr)
				}
				if skipReason != "" {
					t.Skipf("%s EXPORT DATA continuous dry-run not validated (Enterprise Continuous Queries / Pub/Sub entitlement unavailable in test project): %s", name, skipReason)
				}
			})
		})
	}
}

func assertDryRunJobSucceeded(t *testing.T, label string, job *bq.Job) {
	t.Helper()
	if job == nil {
		t.Fatalf("%s dry-run returned nil job", label)
	}
	st := job.LastStatus()
	if st == nil {
		t.Fatalf("%s dry-run returned nil LastStatus()", label)
	}
	if err := st.Err(); err != nil {
		t.Fatalf("%s dry-run job status error: %v", label, err)
	}
	if st.Statistics == nil {
		t.Fatalf("%s dry-run job returned nil Statistics", label)
	}
	if got := st.Statistics.TotalBytesProcessed; got != 0 {
		t.Errorf("%s dry-run TotalBytesProcessed = %d, want 0", label, got)
	}
}

// extractContinuousQuerySQLVariants extracts (1) the inner SELECT ... FROM APPENDS(...)
// query from the EXPORT DATA statement and (2) the INSERT INTO otel_alerts
// table-sink variant matching deploy/continuous-queries/README.md and the
// 7-column otel_alerts table schema in deploy/terraform/main.tf.
func extractContinuousQuerySQLVariants(t *testing.T, name, rendered, project, dataset string) (innerSelect string, insertSQL string) {
	t.Helper()

	_, afterAs, ok := strings.Cut(rendered, "AS (\n")
	if !ok {
		t.Fatalf("%s missing 'AS (' in EXPORT DATA block", name)
	}
	lastParen := strings.LastIndex(afterAs, ");")
	if lastParen < 0 {
		t.Fatalf("%s missing closing ');' in EXPORT DATA block", name)
	}
	innerSelect = strings.TrimSpace(afterAs[:lastParen])

	// Extract the STRUCT(...) body and the FROM APPENDS(...) ... tail so we can
	// dry-run the INSERT INTO `<project>.<dataset>.otel_alerts` variant against
	// the live otel_alerts table schema.
	_, afterStruct, ok := strings.Cut(innerSelect, "STRUCT(\n")
	if !ok {
		t.Fatalf("%s missing 'STRUCT(' in inner SELECT", name)
	}
	structFields, fromTail, ok := strings.Cut(afterStruct, "\n      )\n    ) AS message\n")
	if !ok {
		t.Fatalf("%s missing ') AS message' closing STRUCT wrapper", name)
	}
	const coreHeaderSuffix = "trace_id,\n        span_id,"
	coreCols, payloadCols, ok := strings.Cut(structFields, coreHeaderSuffix)
	if !ok {
		t.Fatalf("%s STRUCT fields missing standard header ending with trace_id, span_id", name)
	}
	insertSQL = fmt.Sprintf(
		"INSERT INTO `%s.%s.otel_alerts` (\n  alert_id, alert_kind, event_time, service_name, trace_id, span_id, payload\n)\nSELECT\n%s%s\n  TO_JSON(STRUCT(\n%s\n  )) AS payload\n%s;",
		project, dataset, coreCols, coreHeaderSuffix, strings.TrimSpace(payloadCols), strings.TrimSpace(fromTail),
	)
	return innerSelect, insertSQL
}
