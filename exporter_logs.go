// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"
)

// writeAPITraceID identifies this client to the Storage Write API backend.
const writeAPITraceID = "otel-bigqueryexporter"

type logsExporter struct {
	cfg    *Config
	logger *zap.Logger
	opts   bigquery.WriterOptions
}

func newLogsExporter(set component.TelemetrySettings, cfg *Config) *logsExporter {
	return &logsExporter{
		cfg:    cfg,
		logger: set.Logger,
		opts: bigquery.WriterOptions{
			Project:             cfg.Project,
			Dataset:             cfg.Dataset,
			Table:               cfg.LogsTable,
			Mode:                bigquery.DefaultStream,
			MaxRequestBytes:     cfg.Write.MaxRequestBytes,
			MaxInflightRequests: cfg.Write.MaxInflightRequests,
			MaxInflightBytes:    cfg.Write.MaxInflightBytes,
			TraceID:             writeAPITraceID,
		},
	}
}

func (e *logsExporter) start(_ context.Context, _ component.Host) error {
	e.logger.Info("starting BigQuery logs exporter",
		zap.String("destination", e.opts.TableRef()))
	return nil
}

func (e *logsExporter) shutdown(_ context.Context) error {
	return nil
}

func (e *logsExporter) pushLogData(_ context.Context, ld plog.Logs) error {
	e.logger.Debug("would append log records",
		zap.Int("records", ld.LogRecordCount()),
		zap.String("destination", e.opts.TableRef()))
	return errNotImplemented
}
