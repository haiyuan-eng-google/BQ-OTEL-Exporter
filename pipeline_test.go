// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
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

	host := componenttest.NewNopHost()
	if err := tracesExp.Start(ctx, host); err != nil {
		t.Fatalf("tracesExp.Start: %v", err)
	}
	if err := logsExp.Start(ctx, host); err != nil {
		_ = tracesExp.Shutdown(ctx)
		t.Fatalf("logsExp.Start: %v", err)
	}

	// Inject one transient Unavailable fault on traces to prove exporterhelper's
	// queue and retry pipeline recovers automatically before Shutdown.
	srv.EnqueueFault(bqfake.Fault{
		Mode:  bqfake.FaultUnavailable,
		Table: "otel_spans",
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

	// Wait for asynchronous sending_queue consumers and retry backoff to flush.
	deadline := time.Now().Add(5 * time.Second)
	for (srv.RowCount("otel_spans") < 1 || srv.RowCount("otel_logs") < 1) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if err := tracesExp.Shutdown(ctx); err != nil {
		t.Fatalf("tracesExp.Shutdown: %v", err)
	}
	if err := logsExp.Shutdown(ctx); err != nil {
		t.Fatalf("logsExp.Shutdown: %v", err)
	}

	spans := srv.Rows("otel_spans")
	if len(spans) != 1 || spans[0]["name"] != "pipeline_span" {
		t.Fatalf("otel_spans = %#v, want 1 row with name=pipeline_span", spans)
	}
	logs := srv.Rows("otel_logs")
	if len(logs) != 1 || logs[0]["event_name"] != "gen_ai.user.message" {
		t.Fatalf("otel_logs = %#v, want 1 row with event_name=gen_ai.user.message", logs)
	}
}
