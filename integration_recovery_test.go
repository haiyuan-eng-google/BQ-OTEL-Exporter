// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bigqueryexporter

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// TestRecoveryAfterAttemptTimeoutsLiveBigQuery is the live half of issue #3.
// Concurrent bursts across traces and logs run with attempt deadlines short
// enough to expire after dispatch. Every failed batch is then replayed in the
// same exporters, as exporterhelper's retry sender would, without a restart.
// The test requires at least one timeout after dispatch, zero dropped batches,
// both current-value gauges back at zero, and every logical record exactly
// once in the dedup views.
//
// Opt-in like the auto-create test: it creates billable resources. Set
// BQ_RECOVERY_BURST_TIMEOUT (default 25ms) below the project's append latency
// if no attempt times out after dispatch.
func TestRecoveryAfterAttemptTimeoutsLiveBigQuery(t *testing.T) {
	project := os.Getenv("BQ_PROJECT")
	if project == "" {
		t.Skip("BQ_PROJECT is unset; skipping live BigQuery recovery integration test")
	}
	location := os.Getenv("BQ_LOCATION")
	if location == "" {
		location = "US"
	}
	burstTimeout := 25 * time.Millisecond
	if v := os.Getenv("BQ_RECOVERY_BURST_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("BQ_RECOVERY_BURST_TIMEOUT: %v", err)
		}
		burstTimeout = d
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	dataset := fmt.Sprintf("otel_exporter_recovery_it_%d", time.Now().UnixNano())
	admin := newLiveBigQueryAdmin(ctx, t, project, dataset)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	settings := component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: provider}

	cfg := validConfig()
	cfg.Project = project
	cfg.Dataset = dataset
	cfg.Location = location
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true

	traces, err := newTracesExporter(settings, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := traces.start(ctx, nil); err != nil {
		t.Fatalf("trace startup: %v", err)
	}
	logs, err := newLogsExporter(settings, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := logs.start(ctx, nil); err != nil {
		_ = traces.shutdown(context.Background())
		t.Fatalf("log startup: %v", err)
	}
	shutDown := false
	defer func() {
		if !shutDown {
			_ = traces.shutdown(context.Background())
			_ = logs.shutdown(context.Background())
		}
	}()

	// A batch is one exporterhelper request. Replays push the same payload, so
	// span IDs and log fingerprints stay stable across attempts.
	runID := uint64(time.Now().UnixNano())
	var seq atomic.Uint64
	var wantSpans, wantLogs atomic.Int64
	type batch func(context.Context) error
	newBatch := func(signal string) batch {
		if signal == "traces" {
			td := recoveryTraces(runID, &seq, 4)
			wantSpans.Add(int64(td.SpanCount()))
			return func(ctx context.Context) error { return traces.pushTraceData(ctx, td) }
		}
		ld := recoveryLogs(runID, &seq, 4)
		wantLogs.Add(int64(ld.LogRecordCount()))
		return func(ctx context.Context) error { return logs.pushLogData(ctx, ld) }
	}
	attempt := func(b batch, timeout time.Duration) error {
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, timeout)
		defer cancelAttempt()
		return b(attemptCtx)
	}

	var mu sync.Mutex
	var failed []batch
	dropped := 0
	for burst := 0; burst < 3; burst++ {
		// An ordinary attempt per signal first, so the burst dispatches onto
		// open streams rather than timing out while one is created.
		for _, signal := range []string{"traces", "logs"} {
			if b := newBatch(signal); attempt(b, cfg.TimeoutSettings.Timeout) != nil {
				failed = append(failed, b)
			}
		}
		var wg sync.WaitGroup
		for sender := 0; sender < 10; sender++ {
			for _, signal := range []string{"traces", "logs"} {
				b := newBatch(signal)
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := attempt(b, burstTimeout)
					if err == nil {
						return
					}
					mu.Lock()
					defer mu.Unlock()
					if consumererror.IsPermanent(err) {
						dropped++
						t.Errorf("burst batch rejected permanently: %v", err)
						return
					}
					failed = append(failed, b)
				}()
			}
		}
		wg.Wait()
	}
	if n := liveMetricSum(t, reader, "otelcol_exporter_bigquery_timeouts_after_dispatch"); n == 0 {
		t.Fatalf("no attempt timed out after dispatch with a %v burst timeout; lower BQ_RECOVERY_BURST_TIMEOUT", burstTimeout)
	}

	// Replay in the same exporters with the configured attempt timeout. A batch
	// that is rejected permanently or outlives the horizon is a dropped batch.
	horizon := time.Now().Add(3 * time.Minute)
	for _, b := range failed {
		for {
			err := attempt(b, cfg.TimeoutSettings.Timeout)
			if err == nil {
				break
			}
			if consumererror.IsPermanent(err) || time.Now().After(horizon) {
				dropped++
				t.Errorf("replayed batch dropped: %v", err)
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	t.Logf("burst timeout %v: %d batches replayed, %d dropped, %d timeouts after dispatch, %d stream retirements",
		burstTimeout, len(failed), dropped,
		liveMetricSum(t, reader, "otelcol_exporter_bigquery_timeouts_after_dispatch"),
		liveMetricSum(t, reader, "otelcol_exporter_bigquery_stream_retirements"))
	if dropped != 0 {
		t.Fatalf("%d batches dropped, want 0", dropped)
	}

	for _, gauge := range []string{
		"otelcol_exporter_bigquery_inflight_requests",
		"otelcol_exporter_bigquery_unresolved_results",
	} {
		if err := waitForZeroGauge(ctx, t, reader, gauge); err != nil {
			t.Error(err)
		}
	}

	for table, want := range map[string]int64{
		cfg.Traces.Table + "_dedup": wantSpans.Load(),
		cfg.Logs.Table + "_dedup":   wantLogs.Load(),
	} {
		if err := waitForRowCount(ctx, admin, project, dataset, table, want); err != nil {
			t.Error(err)
		}
	}

	shutDown = true
	if err := traces.shutdown(ctx); err != nil {
		t.Errorf("trace shutdown: %v", err)
	}
	if err := logs.shutdown(ctx); err != nil {
		t.Errorf("log shutdown: %v", err)
	}
}

// recoveryTraces builds n spans whose IDs are unique within the run.
func recoveryTraces(runID uint64, seq *atomic.Uint64, n int) ptrace.Traces {
	td := ptrace.NewTraces()
	spans := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	now := time.Now()
	for range n {
		id := seq.Add(1)
		var traceID pcommon.TraceID
		binary.BigEndian.PutUint64(traceID[:8], runID)
		binary.BigEndian.PutUint64(traceID[8:], id)
		var spanID pcommon.SpanID
		binary.BigEndian.PutUint64(spanID[:], id)
		span := spans.AppendEmpty()
		span.SetName("recovery integration test")
		span.SetTraceID(traceID)
		span.SetSpanID(spanID)
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(now.Add(time.Millisecond)))
	}
	return td
}

// recoveryLogs builds n log records whose bodies, and so fingerprints, are
// unique within the run.
func recoveryLogs(runID uint64, seq *atomic.Uint64, n int) plog.Logs {
	ld := plog.NewLogs()
	records := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	now := pcommon.NewTimestampFromTime(time.Now())
	for range n {
		record := records.AppendEmpty()
		record.SetTimestamp(now)
		record.Body().SetStr(fmt.Sprintf("recovery integration test %d/%d", runID, seq.Add(1)))
	}
	return ld
}

func newLiveBigQueryAdmin(ctx context.Context, t *testing.T, project, dataset string) *bq.Client {
	t.Helper()
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
	return admin
}

// liveMetricSum returns the current value of an int64 sum, or zero when the
// instrument has not recorded anything yet.
func liveMetricSum(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == name {
				for _, point := range sum.DataPoints {
					total += point.Value
				}
			}
		}
	}
	return total
}

func waitForZeroGauge(ctx context.Context, t *testing.T, reader *sdkmetric.ManualReader, name string) error {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		value := liveMetricSum(t, reader, name)
		if value == 0 {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("%s = %d after recovery, want 0", name, value)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitForRowCount waits until the table holds exactly want rows. For a dedup
// view that is the number of distinct logical records sent.
func waitForRowCount(ctx context.Context, client *bq.Client, project, dataset, table string, want int64) error {
	query := client.Query(fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s.%s`", project, dataset, table))
	var got int64
	for {
		rows, err := query.Read(ctx)
		if err == nil {
			var values []bq.Value
			err = rows.Next(&values)
			if err == nil && len(values) == 1 {
				got, _ = values[0].(int64)
				if got == want {
					return nil
				}
				if got > want {
					return fmt.Errorf("%s.%s.%s has %d rows, want %d logical records", project, dataset, table, got, want)
				}
			}
			if err != nil && err != iterator.Done {
				return fmt.Errorf("query %s.%s.%s: %w", project, dataset, table, err)
			}
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%s.%s.%s has %d rows, want %d: %w", project, dataset, table, got, want, ctx.Err())
		}
		time.Sleep(time.Second)
	}
}
