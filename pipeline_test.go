// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
)

func TestCollectorPipelineFactoryLifecycleWithBQFake(t *testing.T) {
	srv := bqfake.NewServer(t)
	ctx := context.Background()

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Project = "my-project"
	cfg.Dataset = "otel"
	cfg.Endpoint.URL = srv.Addr()
	cfg.Endpoint.Insecure = true
	cfg.Endpoint.WithoutAuthentication = true
	cfg.Quarantine.Enabled = true
	cfg.BackOffConfig.InitialInterval = 10 * time.Millisecond

	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg.Validate: %v", err)
	}

	tracesExp, err := factory.CreateTraces(ctx, exportertest.NewNopSettings(factory.Type()), cfg)
	if err != nil {
		t.Fatalf("CreateTraces: %v", err)
	}
	logsExp, err := factory.CreateLogs(ctx, exportertest.NewNopSettings(factory.Type()), cfg)
	if err != nil {
		t.Fatalf("CreateLogs: %v", err)
	}
	metricsExp, err := factory.CreateMetrics(ctx, exportertest.NewNopSettings(factory.Type()), cfg)
	if err != nil {
		t.Fatalf("CreateMetrics: %v", err)
	}

	host := componenttest.NewNopHost()
	if err := tracesExp.Start(ctx, host); err != nil {
		t.Fatalf("tracesExp.Start: %v", err)
	}
	if err := logsExp.Start(ctx, host); err != nil {
		_ = tracesExp.Shutdown(ctx)
		t.Fatalf("logsExp.Start: %v", err)
	}
	if err := metricsExp.Start(ctx, host); err != nil {
		_ = tracesExp.Shutdown(ctx)
		_ = logsExp.Shutdown(ctx)
		t.Fatalf("metricsExp.Start: %v", err)
	}

	// Inject one transient Unavailable fault on traces, and one on metrics, to
	// prove exporterhelper's queue and retry pipeline recovers automatically
	// before Shutdown.
	srv.EnqueueFault(bqfake.Fault{
		Mode:  bqfake.FaultUnavailable,
		Table: "otel_spans",
	})
	srv.EnqueueFault(bqfake.Fault{
		Mode:  bqfake.FaultUnavailable,
		Table: "otel_metrics",
	})

	now := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "pipeline-test-svc")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	span.SetName("pipeline_span")
	span.SetKind(ptrace.SpanKindInternal)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(now.Add(10 * time.Millisecond)))

	if err := tracesExp.ConsumeTraces(ctx, td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "pipeline-test-svc")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(now))
	lr.SetEventName("gen_ai.user.message")
	lr.Body().SetStr("pipeline log body")

	if err := logsExp.ConsumeLogs(ctx, ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "pipeline-test-svc")
	ms := rm.ScopeMetrics().AppendEmpty().Metrics()
	depth := ms.AppendEmpty()
	depth.SetName("gen_ai.client.queue.depth")
	depthPoint := depth.SetEmptyGauge().DataPoints().AppendEmpty()
	depthPoint.SetTimestamp(pcommon.NewTimestampFromTime(now))
	depthPoint.SetIntValue(3)
	usage := ms.AppendEmpty()
	usage.SetName("gen_ai.client.token.usage")
	usageHistogram := usage.SetEmptyHistogram()
	usageHistogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	usagePoint := usageHistogram.DataPoints().AppendEmpty()
	usagePoint.SetTimestamp(pcommon.NewTimestampFromTime(now))
	usagePoint.SetCount(2)
	usagePoint.SetSum(300)
	// The exemplar ties the histogram point to the span exported above.
	exemplar := usagePoint.Exemplars().AppendEmpty()
	exemplar.SetTimestamp(pcommon.NewTimestampFromTime(now))
	exemplar.SetDoubleValue(120)
	exemplar.SetTraceID(span.TraceID())
	exemplar.SetSpanID(span.SpanID())

	if err := metricsExp.ConsumeMetrics(ctx, md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	// Wait for asynchronous sending_queue consumers and retry backoff to flush.
	deadline := time.Now().Add(5 * time.Second)
	for (srv.RowCount("otel_spans") < 1 || srv.RowCount("otel_logs") < 1 || srv.RowCount("otel_metrics") < 2) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if err := tracesExp.Shutdown(ctx); err != nil {
		t.Fatalf("tracesExp.Shutdown: %v", err)
	}
	if err := logsExp.Shutdown(ctx); err != nil {
		t.Fatalf("logsExp.Shutdown: %v", err)
	}
	if err := metricsExp.Shutdown(ctx); err != nil {
		t.Fatalf("metricsExp.Shutdown: %v", err)
	}

	// Three deliveries plus the two injected faults: each fault made the
	// pipeline send again, so neither was a silent no-op.
	if _, requests, _ := srv.Counts(); requests != 5 {
		t.Errorf("append requests = %d, want 5 (3 deliveries and 2 retried faults)", requests)
	}

	spans := srv.Rows("otel_spans")
	if len(spans) != 1 || spans[0]["name"] != "pipeline_span" {
		t.Fatalf("otel_spans = %#v, want 1 row with name=pipeline_span", spans)
	}
	logs := srv.Rows("otel_logs")
	if len(logs) != 1 || logs[0]["event_name"] != "gen_ai.user.message" {
		t.Fatalf("otel_logs = %#v, want 1 row with event_name=gen_ai.user.message", logs)
	}

	metricRows := srv.Rows("otel_metrics")
	if len(metricRows) != 2 {
		t.Fatalf("otel_metrics = %#v, want 2 rows", metricRows)
	}
	byName := make(map[string]map[string]any, len(metricRows))
	for _, row := range metricRows {
		name, _ := row["metric_name"].(string)
		byName[name] = row
	}
	if g := byName["gen_ai.client.queue.depth"]; g["metric_type"] != "GAUGE" || g["value_int64"] != int64(3) {
		t.Errorf("gauge row = %#v, want metric_type=GAUGE and value_int64=3", g)
	}
	h := byName["gen_ai.client.token.usage"]
	if h["metric_type"] != "HISTOGRAM" || h["count"] != int64(2) || h["sum"] != float64(300) {
		t.Errorf("histogram row = %#v, want metric_type=HISTOGRAM, count=2 and sum=300", h)
	}
	exemplars, _ := h["exemplars"].([]map[string]any)
	if len(exemplars) != 1 {
		t.Fatalf("histogram exemplars = %#v, want 1", h["exemplars"])
	}
	// The exemplar reaches BigQuery under the IDs of the span row, which is
	// how a metric point joins back to its trace.
	if exemplars[0]["trace_id"] != spans[0]["trace_id"] || exemplars[0]["span_id"] != spans[0]["span_id"] {
		t.Errorf("exemplar trace_id = %v, span_id = %v, want the span row's %v and %v",
			exemplars[0]["trace_id"], exemplars[0]["span_id"], spans[0]["trace_id"], spans[0]["span_id"])
	}
}

// TestCollectorPipelineMetricsProvisioningWithBQFake runs the metrics exporter
// the way a collector does, through the factory with provisioning on, and
// checks that the first start creates the metrics views and that the pipeline
// then delivers the same rows, and the same quarantined point, as the
// end-to-end test that drives the exporter directly.
func TestCollectorPipelineMetricsProvisioningWithBQFake(t *testing.T) {
	srv := bqfake.NewServer(t)
	wireFakeHooks(t, srv)
	ctx := context.Background()

	factory := NewFactory()
	cfg := provisioningConfig()
	cfg.BackOffConfig.InitialInterval = 10 * time.Millisecond
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg.Validate: %v", err)
	}

	exp, err := factory.CreateMetrics(ctx, exportertest.NewNopSettings(factory.Type()), cfg)
	if err != nil {
		t.Fatalf("CreateMetrics: %v", err)
	}
	if err := exp.Start(ctx, componenttest.NewNopHost()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	var once sync.Once
	shutdown := func() { once.Do(func() { _ = exp.Shutdown(ctx) }) }
	t.Cleanup(shutdown)

	assertMetricsProvisioned(t, srv, cfg)

	if err := exp.ConsumeMetrics(ctx, newGenAIMetricsBatch()); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	wantRows := wantGenAIMetricRows()
	deadline := time.Now().Add(5 * time.Second)
	for (srv.RowCount(cfg.Metrics.Table) < len(wantRows) || srv.RowCount(cfg.Quarantine.Table) < 1) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	shutdown()

	rows := srv.Rows(cfg.Metrics.Table)
	if len(rows) != len(wantRows) {
		t.Fatalf("%s rows = %d, want %d", cfg.Metrics.Table, len(rows), len(wantRows))
	}
	for i, want := range wantRows {
		assertMetricRow(t, fmt.Sprintf("row %d (%v)", i, want["metric_name"]), rows[i], want)
	}
	if fps := metricFingerprints(t, rows); !allDistinct(fps) {
		t.Errorf("fingerprints = %v, want all distinct", fps)
	}
	quarantined := srv.Rows(cfg.Quarantine.Table)
	if len(quarantined) != 1 || quarantined[0]["error_code"] != "MISSING_TIMESTAMP" {
		t.Fatalf("%s = %#v, want 1 row with error_code=MISSING_TIMESTAMP", cfg.Quarantine.Table, quarantined)
	}
}

func TestCollectorPipelineStreamPoolWithBQFake(t *testing.T) {
	for _, tc := range []struct {
		name               string
		enableMultiplexing bool
		wantMinStreams     int
	}{
		{name: "multiplexed", enableMultiplexing: true, wantMinStreams: 4},
		{name: "simplex", enableMultiplexing: false, wantMinStreams: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := bqfake.NewServer(t)
			ctx := context.Background()

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig().(*Config)
			cfg.Project = "my-project"
			cfg.Dataset = "otel"
			cfg.Endpoint.URL = srv.Addr()
			cfg.Endpoint.Insecure = true
			cfg.Endpoint.WithoutAuthentication = true
			cfg.Write.StreamPoolSize = 4
			cfg.Write.EnableMultiplexing = tc.enableMultiplexing
			cfg.Write.ConnectionPoolSize = 4
			cfg.Write.MaxRequestBytes = 512
			cfg.Write.MaxRowBytes = 512
			cfg.BackOffConfig.InitialInterval = 10 * time.Millisecond

			if err := cfg.Validate(); err != nil {
				t.Fatalf("cfg.Validate: %v", err)
			}

			exp, err := factory.CreateTraces(ctx, exportertest.NewNopSettings(factory.Type()), cfg)
			if err != nil {
				t.Fatalf("CreateTraces: %v", err)
			}
			if err := exp.Start(ctx, componenttest.NewNopHost()); err != nil {
				t.Fatalf("Start: %v", err)
			}
			var once sync.Once
			shutdown := func() { once.Do(func() { _ = exp.Shutdown(ctx) }) }
			t.Cleanup(shutdown)

			now := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
			const spanCount = 16
			td := ptrace.NewTraces()
			rs := td.ResourceSpans().AppendEmpty()
			rs.Resource().Attributes().PutStr("service.name", "stream-pool-svc")
			spans := rs.ScopeSpans().AppendEmpty().Spans()
			for i := range spanCount {
				sp := spans.AppendEmpty()
				sp.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, byte(i + 1)})
				sp.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, byte(i + 1)})
				sp.SetName(fmt.Sprintf("pool_span_%d", i))
				sp.SetKind(ptrace.SpanKindServer)
				sp.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(time.Duration(i) * time.Millisecond)))
				sp.SetEndTimestamp(pcommon.NewTimestampFromTime(now.Add(time.Duration(i+1) * time.Millisecond)))
				sp.Attributes().PutStr("payload", "padding-bytes-to-split-requests-across-stream-pool-slots-cleanly")
			}

			if err := exp.ConsumeTraces(ctx, td); err != nil {
				t.Fatalf("ConsumeTraces: %v", err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for srv.RowCount(cfg.Traces.Table) < spanCount && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			shutdown()

			if got := srv.RowCount(cfg.Traces.Table); got != spanCount {
				t.Fatalf("%s row count = %d, want %d", cfg.Traces.Table, got, spanCount)
			}
			if streams, requests, _ := srv.Counts(); streams < tc.wantMinStreams || requests < 4 {
				t.Fatalf("bqfake streams=%d requests=%d, want streams>=%d and requests>=4",
					streams, requests, tc.wantMinStreams)
			}
		})
	}
}
