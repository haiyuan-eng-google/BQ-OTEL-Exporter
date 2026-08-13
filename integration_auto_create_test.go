// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bigqueryexporter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
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

	if err := traces.pushTraceData(ctx, newTestTraces(1)); err != nil {
		t.Fatalf("write trace: %v", err)
	}
	if err := logs.pushLogData(ctx, oneIntegrationLog()); err != nil {
		t.Fatalf("write log: %v", err)
	}

	// A second pair of exporters exercises dataset, table, and create-only view
	// AlreadyExists paths against the real metadata API.
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

	for _, table := range []string{cfg.Traces.Table, cfg.Logs.Table} {
		if err := waitForRows(ctx, admin, project, dataset, table); err != nil {
			t.Error(err)
		}
		if err := waitForRows(ctx, admin, project, dataset, table+"_dedup"); err != nil {
			t.Error(err)
		}
	}
}

func oneIntegrationLog() plog.Logs {
	ld := plog.NewLogs()
	record := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
	record.Body().SetStr("auto-create integration test")
	return ld
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
			if err != nil && err != iterator.Done {
				return fmt.Errorf("query %s.%s.%s: %w", project, dataset, table, err)
			}
		}
		if ctx.Err() != nil {
			return fmt.Errorf("no rows became queryable in %s.%s.%s: %w", project, dataset, table, ctx.Err())
		}
		time.Sleep(time.Second)
	}
}
