// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
)

func TestZeroContentLeakageRedactionCanary(t *testing.T) {
	sentinels := []string{
		"SENTINEL_SECRET_KEY_99887766",
		"SENTINEL_PII_PROMPT_SSN_000_11_2222",
		"SENTINEL_BEARER_TOKEN_ABCDEF123456",
		"SENTINEL_EVENT_BODY_CREDIT_CARD_4111",
	}

	srv := bqfake.NewServer(t)
	wireFakeHooks(t, srv)

	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)

	cfg := validConfig()
	cfg.Project = "canary-proj"
	cfg.Dataset = "canary_ds"
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	cfg.AutoCreate.CompanionViews = false
	// Set tight value limit so oversized sentinel strings trigger structural rejection logs.
	cfg.Limits.MaxValueBytes = 64

	settings := component.TelemetrySettings{
		Logger:        logger,
		MeterProvider: noop.NewMeterProvider(),
	}
	tracesExp, err := newTracesExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	logsExp, err := newLogsExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newLogsExporter: %v", err)
	}

	ctx := context.Background()
	if err := tracesExp.start(ctx, nil); err != nil {
		t.Fatalf("tracesExp.start: %v", err)
	}
	t.Cleanup(func() { _ = tracesExp.shutdown(context.Background()) })
	if err := logsExp.start(ctx, nil); err != nil {
		t.Fatalf("logsExp.start: %v", err)
	}
	t.Cleanup(func() { _ = logsExp.shutdown(context.Background()) })

	// Inject a server-side RowError whose raw server message echoes a sentinel field name,
	// plus a second span that triggers a client-side MaxValueBytes rejection.
	srv.EnqueueFault(bqfake.Fault{
		Mode:  bqfake.FaultRowErrors,
		Table: "otel_spans",
		RowErrors: []*storagepb.RowError{
			{
				Index:   0,
				Code:    storagepb.RowError_FIELDS_ERROR,
				Message: fmt.Sprintf("field %q contains invalid value %q", sentinels[0], sentinels[1]),
			},
		},
	})

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "canary-svc")
	ss := rs.ScopeSpans().AppendEmpty()

	// Span 0: passes structural limits, hits server RowError containing sentinels.
	sp0 := ss.Spans().AppendEmpty()
	sp0.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 1}))
	sp0.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 1}))
	sp0.SetName(sentinels[0])
	sp0.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000000, 0)))
	sp0.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000001, 0)))
	sp0.Attributes().PutStr(sentinels[0], sentinels[1])

	// Span 1: exceeds MaxValueBytes (64 bytes) with sentinel payload.
	sp1 := ss.Spans().AppendEmpty()
	sp1.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 2}))
	sp1.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 2}))
	sp1.SetName("oversize-span")
	sp1.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000000, 0)))
	sp1.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000001, 0)))
	sp1.Attributes().PutStr(sentinels[2], strings.Repeat(sentinels[1], 4))

	traceErr := tracesExp.pushTraceData(ctx, td)

	// Log batch: one record with oversized body containing sentinel, one hitting server RowError.
	srv.EnqueueFault(bqfake.Fault{
		Mode:  bqfake.FaultRowErrors,
		Table: "otel_logs",
		RowErrors: []*storagepb.RowError{
			{
				Index:   0,
				Code:    storagepb.RowError_FIELDS_ERROR,
				Message: fmt.Sprintf("invalid body payload %q in column '%s'", sentinels[3], sentinels[2]),
			},
		},
	})

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "canary-svc")
	sl := rl.ScopeLogs().AppendEmpty()

	lr0 := sl.LogRecords().AppendEmpty()
	lr0.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000000, 0)))
	lr0.Body().SetStr(sentinels[3])
	lr0.Attributes().PutStr(sentinels[0], sentinels[2])

	lr1 := sl.LogRecords().AppendEmpty()
	lr1.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000000, 0)))
	lr1.Body().SetStr(strings.Repeat(sentinels[3], 4))

	logErr := logsExp.pushLogData(ctx, ld)

	// Verify diagnostic logs were actually emitted.
	entries := recorded.All()
	if len(entries) == 0 {
		t.Fatal("expected diagnostic log entries to be emitted during rejection and row errors")
	}

	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	for i, entry := range entries {
		buf, err := encoder.EncodeEntry(entry.Entry, entry.Context)
		if err != nil {
			t.Fatalf("EncodeEntry[%d]: %v", i, err)
		}
		serialized := buf.String()
		for _, s := range sentinels {
			if strings.Contains(serialized, s) {
				t.Fatalf("sentinel %q leaked into diagnostic log entry[%d]: %s", s, i, serialized)
			}
		}
	}

	for _, errVal := range []error{traceErr, logErr} {
		if errVal == nil {
			continue
		}
		errMsg := errVal.Error()
		for _, s := range sentinels {
			if strings.Contains(errMsg, s) {
				t.Fatalf("sentinel %q leaked into returned error string: %s", s, errMsg)
			}
		}
	}
}
