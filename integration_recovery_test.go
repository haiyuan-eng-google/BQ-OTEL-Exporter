// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bigqueryexporter

import (
	"context"
	"encoding/binary"
	"errors"
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
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
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
		if os.Getenv("REQUIRE_LIVE_BQ") == "1" {
			t.Fatal("REQUIRE_LIVE_BQ=1 is set but BQ_PROJECT is empty")
		}
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
	dataset := uniqueEphemeralDatasetID("otel_exporter_recovery_it")
	admin := newLiveBigQueryAdmin(ctx, t, project, dataset, location)

	readers := make(map[string]*sdkmetric.ManualReader, 3)
	settingsBySignal := make(map[string]component.TelemetrySettings, 3)
	for _, sig := range []string{"traces", "logs", "metrics"} {
		r := sdkmetric.NewManualReader()
		p := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		readers[sig] = r
		settingsBySignal[sig] = component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: p}
	}

	cfg := validConfig()
	cfg.Project = project
	cfg.Dataset = dataset
	cfg.Location = location
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true

	traces, err := newTracesExporter(settingsBySignal["traces"], cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := traces.start(ctx, nil); err != nil {
		t.Fatalf("trace startup: %v", err)
	}
	logs, err := newLogsExporter(settingsBySignal["logs"], cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := logs.start(ctx, nil); err != nil {
		_ = traces.shutdown(context.Background())
		t.Fatalf("log startup: %v", err)
	}
	metrics, err := newMetricsExporter(settingsBySignal["metrics"], cfg)
	if err != nil {
		_ = traces.shutdown(context.Background())
		_ = logs.shutdown(context.Background())
		t.Fatal(err)
	}
	if err := metrics.start(ctx, nil); err != nil {
		_ = traces.shutdown(context.Background())
		_ = logs.shutdown(context.Background())
		t.Fatalf("metric startup: %v", err)
	}
	shutDown := false
	defer func() {
		if !shutDown {
			_ = traces.shutdown(context.Background())
			_ = logs.shutdown(context.Background())
			_ = metrics.shutdown(context.Background())
		}
	}()

	// A batch is one exporterhelper request. Replays push the same payload, so
	// span IDs, log fingerprints, and metric point fingerprints (including
	// intra_batch_ordinal peer pairs) stay stable across attempts.
	runID := uint64(time.Now().UnixNano())
	var seq atomic.Uint64
	var wantSpans, wantLogs, wantMetrics atomic.Int64
	type batch func(context.Context) error
	newBatch := func(signal string) batch {
		switch signal {
		case "traces":
			td := recoveryTraces(runID, &seq, 4)
			wantSpans.Add(int64(td.SpanCount()))
			return func(ctx context.Context) error { return traces.pushTraceData(ctx, td) }
		case "metrics":
			md := recoveryMetrics(runID, &seq)
			wantMetrics.Add(int64(md.DataPointCount()))
			return func(ctx context.Context) error { return metrics.pushMetricsData(ctx, md) }
		default:
			ld := recoveryLogs(runID, &seq, 4)
			wantLogs.Add(int64(ld.LogRecordCount()))
			return func(ctx context.Context) error { return logs.pushLogData(ctx, ld) }
		}
	}
	attempt := func(b batch, timeout time.Duration) error {
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, timeout)
		defer cancelAttempt()
		return b(attemptCtx)
	}

	type taggedBatch struct {
		signal string
		run    batch
	}
	var mu sync.Mutex
	var failed []taggedBatch
	failedBySignal := map[string]int{}
	dropped := 0
	for burst := 0; burst < 3; burst++ {
		// An ordinary attempt per signal first, so the burst dispatches onto
		// open streams rather than timing out while one is created.
		for _, signal := range []string{"traces", "logs", "metrics"} {
			if b := newBatch(signal); attempt(b, cfg.TimeoutSettings.Timeout) != nil {
				failed = append(failed, taggedBatch{signal: signal, run: b})
				failedBySignal[signal]++
			}
		}
		var wg sync.WaitGroup
		for sender := 0; sender < 10; sender++ {
			for _, signal := range []string{"traces", "logs", "metrics"} {
				b := newBatch(signal)
				wg.Add(1)
				go func(sig string, b batch) {
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
					failed = append(failed, taggedBatch{signal: sig, run: b})
					failedBySignal[sig]++
				}(signal, b)
			}
		}
		wg.Wait()
	}

	// Ensure every signal (traces, logs, and metrics) experienced at least one
	// post-dispatch attempt timeout and queued a failed batch for replay.
	for _, signal := range []string{"traces", "logs", "metrics"} {
		sigReader := readers[signal]
		for round := 0; round < 6 && (failedBySignal[signal] == 0 || liveMetricSum(t, sigReader, "otelcol_exporter_bigquery_timeouts_after_dispatch") == 0); round++ {
			// Sweep candidate post-dispatch timeouts (15ms, 10ms, 20ms, 8ms, 12ms, 25ms)
			// after warming the stream so at least one AppendRows call dispatches and
			// times out while waiting for the server ack.
			candidates := []time.Duration{
				15 * time.Millisecond,
				10 * time.Millisecond,
				20 * time.Millisecond,
				8 * time.Millisecond,
				12 * time.Millisecond,
				burstTimeout,
			}
			tryTimeout := candidates[round%len(candidates)]
			if b := newBatch(signal); attempt(b, cfg.TimeoutSettings.Timeout) != nil {
				failed = append(failed, taggedBatch{signal: signal, run: b})
				failedBySignal[signal]++
			}
			var wg sync.WaitGroup
			for range 10 {
				b := newBatch(signal)
				wg.Add(1)
				go func(b batch) {
					defer wg.Done()
					err := attempt(b, tryTimeout)
					if err == nil {
						return
					}
					mu.Lock()
					defer mu.Unlock()
					if consumererror.IsPermanent(err) {
						dropped++
						t.Errorf("targeted %s burst batch rejected permanently: %v", signal, err)
						return
					}
					failed = append(failed, taggedBatch{signal: signal, run: b})
					failedBySignal[signal]++
				}(b)
			}
			wg.Wait()
		}
		if failedBySignal[signal] == 0 {
			t.Fatalf("no %s batch timed out for replay; lower BQ_RECOVERY_BURST_TIMEOUT", signal)
		}
		if n := liveMetricSum(t, sigReader, "otelcol_exporter_bigquery_timeouts_after_dispatch"); n == 0 {
			t.Fatalf("no %s attempt timed out after dispatch with a %v burst timeout; lower BQ_RECOVERY_BURST_TIMEOUT", signal, burstTimeout)
		}
	}

	// Replay in the same exporters with the configured attempt timeout. A batch
	// that is rejected permanently or outlives the horizon is a dropped batch.
	replayedBySignal := map[string]int{}
	horizon := time.Now().Add(3 * time.Minute)
	for _, tb := range failed {
		for {
			err := attempt(tb.run, cfg.TimeoutSettings.Timeout)
			if err == nil {
				replayedBySignal[tb.signal]++
				break
			}
			if consumererror.IsPermanent(err) || time.Now().After(horizon) {
				dropped++
				t.Errorf("replayed %s batch dropped: %v", tb.signal, err)
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	traceTimeouts := liveMetricSum(t, readers["traces"], "otelcol_exporter_bigquery_timeouts_after_dispatch")
	logTimeouts := liveMetricSum(t, readers["logs"], "otelcol_exporter_bigquery_timeouts_after_dispatch")
	metricTimeouts := liveMetricSum(t, readers["metrics"], "otelcol_exporter_bigquery_timeouts_after_dispatch")
	totalTimeouts := traceTimeouts + logTimeouts + metricTimeouts
	totalRetirements := liveMetricSum(t, readers["traces"], "otelcol_exporter_bigquery_stream_retirements") +
		liveMetricSum(t, readers["logs"], "otelcol_exporter_bigquery_stream_retirements") +
		liveMetricSum(t, readers["metrics"], "otelcol_exporter_bigquery_stream_retirements")
	t.Logf("burst timeout %v: %d batches replayed (traces=%d, logs=%d, metrics=%d), %d dropped, %d timeouts after dispatch (traces=%d, logs=%d, metrics=%d), %d stream retirements",
		burstTimeout, len(failed),
		replayedBySignal["traces"], replayedBySignal["logs"], replayedBySignal["metrics"],
		dropped,
		totalTimeouts, traceTimeouts, logTimeouts, metricTimeouts,
		totalRetirements)
	if dropped != 0 {
		t.Fatalf("%d batches dropped, want 0", dropped)
	}
	for _, signal := range []string{"traces", "logs", "metrics"} {
		if replayedBySignal[signal] == 0 {
			t.Fatalf("replayedBySignal[%q] = 0, want >= 1", signal)
		}
	}

	for _, signal := range []string{"traces", "logs", "metrics"} {
		for _, gauge := range []string{
			"otelcol_exporter_bigquery_inflight_requests",
			"otelcol_exporter_bigquery_unresolved_results",
		} {
			if err := waitForZeroGauge(ctx, t, readers[signal], gauge); err != nil {
				t.Errorf("%s: %v", signal, err)
			}
		}
	}

	for table, want := range map[string]int64{
		cfg.Traces.Table + "_dedup":  wantSpans.Load(),
		cfg.Logs.Table + "_dedup":    wantLogs.Load(),
		cfg.Metrics.Table + "_dedup": wantMetrics.Load(),
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
	if err := metrics.shutdown(ctx); err != nil {
		t.Errorf("metric shutdown: %v", err)
	}
}

// recoveryMetrics builds one batch of OTLP metrics covering all 5 metric data
// types (Gauge with duplicate-ordinal peer points, Sum with exemplar,
// Histogram with exemplar, ExponentialHistogram, and Summary) whose
// point_fingerprint values remain stable across retries.
func recoveryMetrics(runID uint64, seq *atomic.Uint64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "recovery-service")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("recovery-scope")

	batchID := seq.Add(1)
	now := time.Now().UTC()
	ts := pcommon.NewTimestampFromTime(now)
	startTS := pcommon.NewTimestampFromTime(now.Add(-time.Second))

	var traceID pcommon.TraceID
	binary.BigEndian.PutUint64(traceID[:8], runID)
	binary.BigEndian.PutUint64(traceID[8:], batchID)
	var spanID pcommon.SpanID
	binary.BigEndian.PutUint64(spanID[:], batchID)

	// 1. Gauge with two identical peer points so intra_batch_ordinal (0 and 1)
	// and PreserveMetricOrdinalPeers are exercised across timeout retries.
	gaugeMetric := sm.Metrics().AppendEmpty()
	gaugeMetric.SetName("gen_ai.client.active_requests")
	gaugeMetric.SetUnit("{request}")
	gaugePts := gaugeMetric.SetEmptyGauge().DataPoints()
	for range 2 {
		gp := gaugePts.AppendEmpty()
		gp.SetStartTimestamp(startTS)
		gp.SetTimestamp(ts)
		gp.SetIntValue(int64(batchID))
		gp.Attributes().PutStr("gen_ai.system", "vertex_ai")
		gp.Attributes().PutInt("recovery.batch_id", int64(batchID))
	}

	// 2. Sum (monotonic cumulative) with exemplar.
	sumMetric := sm.Metrics().AppendEmpty()
	sumMetric.SetName("gen_ai.client.token.usage")
	sumMetric.SetUnit("{token}")
	sum := sumMetric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)
	sp := sum.DataPoints().AppendEmpty()
	sp.SetStartTimestamp(startTS)
	sp.SetTimestamp(ts)
	sp.SetIntValue(int64(100 + batchID))
	sp.Attributes().PutStr("gen_ai.system", "vertex_ai")
	sp.Attributes().PutStr("gen_ai.token.type", "input")
	sp.Attributes().PutInt("recovery.batch_id", int64(batchID))
	sumEx := sp.Exemplars().AppendEmpty()
	sumEx.SetTimestamp(ts)
	sumEx.SetIntValue(int64(100 + batchID))
	sumEx.SetTraceID(traceID)
	sumEx.SetSpanID(spanID)

	// 3. Histogram with exemplar.
	histMetric := sm.Metrics().AppendEmpty()
	histMetric.SetName("gen_ai.client.operation.duration")
	histMetric.SetUnit("s")
	hist := histMetric.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	hp := hist.DataPoints().AppendEmpty()
	hp.SetStartTimestamp(startTS)
	hp.SetTimestamp(ts)
	hp.SetCount(2)
	hp.SetSum(0.45)
	hp.SetMin(0.15)
	hp.SetMax(0.30)
	hp.ExplicitBounds().FromRaw([]float64{0.1, 0.25, 0.5})
	hp.BucketCounts().FromRaw([]uint64{0, 1, 1, 0})
	hp.Attributes().PutStr("gen_ai.operation.name", "chat")
	hp.Attributes().PutInt("recovery.batch_id", int64(batchID))
	histEx := hp.Exemplars().AppendEmpty()
	histEx.SetTimestamp(ts)
	histEx.SetDoubleValue(0.30)
	histEx.SetTraceID(traceID)
	histEx.SetSpanID(spanID)

	// 4. ExponentialHistogram.
	expMetric := sm.Metrics().AppendEmpty()
	expMetric.SetName("gen_ai.client.time_to_first_token")
	expMetric.SetUnit("s")
	expHist := expMetric.SetEmptyExponentialHistogram()
	expHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	ep := expHist.DataPoints().AppendEmpty()
	ep.SetStartTimestamp(startTS)
	ep.SetTimestamp(ts)
	ep.SetCount(3)
	ep.SetSum(0.6)
	ep.SetScale(2)
	ep.SetZeroCount(1)
	ep.Positive().SetOffset(0)
	ep.Positive().BucketCounts().FromRaw([]uint64{1, 1})
	ep.Attributes().PutInt("recovery.batch_id", int64(batchID))

	// 5. Summary.
	summaryMetric := sm.Metrics().AppendEmpty()
	summaryMetric.SetName("gen_ai.client.summary_latency")
	summaryMetric.SetUnit("s")
	sump := summaryMetric.SetEmptySummary().DataPoints()
	sdp := sump.AppendEmpty()
	sdp.SetStartTimestamp(startTS)
	sdp.SetTimestamp(ts)
	sdp.SetCount(4)
	sdp.SetSum(1.2)
	qv := sdp.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.95)
	qv.SetValue(0.42)
	sdp.Attributes().PutInt("recovery.batch_id", int64(batchID))

	return md
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

func newLiveBigQueryAdmin(ctx context.Context, t *testing.T, project, dataset, location string) *bq.Client {
	t.Helper()
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
	if err := createOwnedEphemeralDataset(ctx, admin, dataset, location, t.Cleanup); err != nil {
		t.Fatalf("create dataset %s.%s: %v", project, dataset, err)
	}
	return admin
}

// liveMetricSum returns the current value of an int64 sum, or zero when the
// instrument has not recorded anything yet.
func liveMetricSum(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	return liveMetricSumBySignal(t, reader, name, "")
}

// liveMetricSumBySignal returns the current value of an int64 sum filtered by
// the "signal" attribute when wantSignal is non-empty.
func liveMetricSumBySignal(t *testing.T, reader *sdkmetric.ManualReader, name, wantSignal string) int64 {
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
					if wantSignal != "" {
						sigVal, ok := point.Attributes.Value("signal")
						if !ok || sigVal.AsString() != wantSignal {
							continue
						}
					}
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
			if err != nil && !errors.Is(err, iterator.Done) {
				return fmt.Errorf("query %s.%s.%s: %w", project, dataset, table, err)
			}
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%s.%s.%s has %d rows, want %d: %w", project, dataset, table, got, want, ctx.Err())
		}
		time.Sleep(time.Second)
	}
}
