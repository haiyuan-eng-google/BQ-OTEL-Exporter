// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/diag"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/metadata"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

type metricsExporter struct {
	*signalExporter
}

func newMetricsExporter(set component.TelemetrySettings, cfg *Config) (*metricsExporter, error) {
	tel, err := metadata.NewTelemetry(set.MeterProvider)
	if err != nil {
		return nil, err
	}
	se := newSignalExporter(set, cfg, cfg.Metrics.Table, schema.MetricsTableSchema())
	se.tel = tel
	return &metricsExporter{se}, nil
}

func (e *metricsExporter) start(ctx context.Context, host component.Host) error {
	return e.signalExporter.start(ctx, host)
}

func (e *metricsExporter) shutdown(ctx context.Context) error {
	return e.signalExporter.shutdown(ctx)
}

func (e *metricsExporter) pushMetricsData(ctx context.Context, md pmetric.Metrics) error {
	opID := diag.OperationID()

	rows, rejects := transform.MetricRows(md, e.opts)
	e.recordRejections(ctx, rejects, opID)

	encoded, keptIndex, drops := e.encodeRows(ctx, rows, opID)

	pointIndex := make([]int, len(keptIndex))
	rowToPoint := rowPositionToRecordIndex(len(rows), rejects)
	for i, ri := range keptIndex {
		pointIndex[i] = rowToPoint[ri]
	}

	decision := e.appendEncoded(ctx, encoded, pointIndex, opID)
	if e.quarantine != nil {
		entries := quarantineEntries(rejects, drops, rowToPoint, decision.rowErrors)
		if len(entries) > 0 {
			qCtx, cancel := e.quarantineContext(ctx)
			qOpts := e.opts
			qOpts.Context = qCtx
			e.writeQuarantine(qCtx, entries, transform.MetricQuarantineRows(md, entries, qOpts), opID)
			cancel()
		}
	}
	if decision.err == nil {
		return nil
	}
	if decision.permanent {
		return consumererror.NewPermanent(decision.err)
	}
	if len(decision.retryableIndices) == 0 {
		return decision.err
	}
	candidates := validRecordIndices(pointIndex, decision.rowErrors)
	retry := transform.PreserveMetricOrdinalPeers(md, candidates, decision.retryableIndices)
	return consumererror.NewMetrics(decision.err, transform.SelectMetrics(md, retry))
}
