// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/diag"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/metadata"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

type tracesExporter struct {
	*signalExporter
}

func newTracesExporter(set component.TelemetrySettings, cfg *Config) (*tracesExporter, error) {
	tel, err := metadata.NewTelemetry(set.MeterProvider)
	if err != nil {
		return nil, err
	}
	se := newSignalExporter(set, cfg, cfg.Traces.Table, schema.SpansTableSchema())
	se.tel = tel
	return &tracesExporter{se}, nil
}

func (e *tracesExporter) start(ctx context.Context, host component.Host) error {
	return e.signalExporter.start(ctx, host)
}

func (e *tracesExporter) shutdown(ctx context.Context) error {
	return e.signalExporter.shutdown(ctx)
}

// pushTraceData maps spans onto rows, appends the valid ones, and returns only
// telemetry that is both retryable and still unacknowledged.
//
// Permanently invalid spans are counted and dropped here. They are never
// routed back through consumererror, because exporterhelper treats a returned
// payload as retryable and would replay them forever.
func (e *tracesExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	opID := diag.OperationID()

	rows, rejects := transform.SpanRows(td, e.opts)
	e.recordRejections(ctx, rejects, opID)

	encoded, keptIndex, _ := e.encodeRows(ctx, rows, opID)

	// keptIndex is expressed in row positions; map it back to span positions
	// so a retry payload can be rebuilt from the original batch.
	spanIndex := make([]int, len(keptIndex))
	rowToSpan := rowPositionToRecordIndex(len(rows), rejects)
	for i, ri := range keptIndex {
		spanIndex[i] = rowToSpan[ri]
	}

	decision := e.appendEncoded(ctx, encoded, spanIndex, opID)
	if decision.err == nil {
		return nil
	}
	if decision.permanent {
		return consumererror.NewPermanent(decision.err)
	}
	if len(decision.retryableIndices) == 0 {
		return decision.err
	}
	return consumererror.NewTraces(decision.err, transform.SelectSpans(td, decision.retryableIndices))
}
