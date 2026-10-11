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

	encoded, keptIndex, drops := e.encodeRows(ctx, rows, opID)

	recordIndex := make([]int, len(keptIndex))
	rowToRecord := rowPositionToRecordIndex(len(rows), rejects)
	for i, ri := range keptIndex {
		recordIndex[i] = rowToRecord[ri]
	}

	decision := e.appendEncoded(ctx, encoded, recordIndex, opID)
	if e.quarantine != nil {
		entries := quarantineEntries(rejects, drops, rowToRecord, decision.rowErrors)
		e.quarantineRows(ctx, transform.LogQuarantineRows(ld, entries, e.opts), opID)
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
	// Acknowledged records normally stay out of the retry payload. The
	// exception is one identical to a retried record: the payload is
	// fingerprinted afresh, so a retried record keeps its intra_batch_ordinal,
	// and with it its insert_id, only if the identical records ahead of it
	// travel along.
	retry := transform.PreserveLogOrdinalPeers(ld,
		validRecordIndices(recordIndex, decision.rowErrors), decision.retryableIndices)
	return consumererror.NewLogs(decision.err, transform.SelectLogs(ld, retry))
}
