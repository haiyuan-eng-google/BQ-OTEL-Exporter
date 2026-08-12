// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/diag"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/metadata"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

type logsExporter struct {
	*signalExporter
}

func newLogsExporter(set component.TelemetrySettings, cfg *Config) (*logsExporter, error) {
	tel, err := metadata.NewTelemetry(set.MeterProvider)
	if err != nil {
		return nil, err
	}
	se := newSignalExporter(set, cfg, cfg.Logs.Table, schema.LogsTableSchema())
	se.tel = tel
	return &logsExporter{se}, nil
}

func (e *logsExporter) start(ctx context.Context, host component.Host) error {
	return e.signalExporter.start(ctx, host)
}

func (e *logsExporter) shutdown(ctx context.Context) error {
	return e.signalExporter.shutdown(ctx)
}

func (e *logsExporter) pushLogData(ctx context.Context, ld plog.Logs) error {
	opID := diag.OperationID()

	rows, rejects := transform.LogRows(ld, e.opts)
	e.recordRejections(ctx, rejects, opID)

	encoded, keptIndex, _ := e.encodeRows(ctx, rows, opID)

	recordIndex := make([]int, len(keptIndex))
	rowToRecord := rowPositionToRecordIndex(len(rows), rejects)
	for i, ri := range keptIndex {
		recordIndex[i] = rowToRecord[ri]
	}

	decision := e.appendEncoded(ctx, encoded, recordIndex, opID)
	if decision.err == nil {
		return nil
	}
	if decision.permanent {
		return consumererror.NewPermanent(decision.err)
	}
	if len(decision.retryableIndices) == 0 {
		return decision.err
	}
	return consumererror.NewLogs(decision.err, transform.SelectLogs(ld, decision.retryableIndices))
}
