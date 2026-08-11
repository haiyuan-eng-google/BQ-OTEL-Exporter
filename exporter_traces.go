// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"
)

// errNotImplemented is returned by the transport paths that land in M1. It is
// deliberately explicit: an exporter that silently accepts and discards
// telemetry is worse than one that fails loudly.
var errNotImplemented = errors.New("bigqueryexporter: write path not implemented yet (M1)")

type tracesExporter struct {
	cfg    *Config
	logger *zap.Logger
	opts   bigquery.WriterOptions
}

func newTracesExporter(set component.TelemetrySettings, cfg *Config) *tracesExporter {
	return &tracesExporter{
		cfg:    cfg,
		logger: set.Logger,
		opts: bigquery.WriterOptions{
			Project:             cfg.Project,
			Dataset:             cfg.Dataset,
			Table:               cfg.SpansTable,
			Mode:                bigquery.DefaultStream,
			MaxRequestBytes:     cfg.Write.MaxRequestBytes,
			MaxInflightRequests: cfg.Write.MaxInflightRequests,
			MaxInflightBytes:    cfg.Write.MaxInflightBytes,
			TraceID:             writeAPITraceID,
		},
	}
}

func (e *tracesExporter) start(_ context.Context, _ component.Host) error {
	e.logger.Info("starting BigQuery traces exporter",
		zap.String("destination", e.opts.TableRef()))
	return nil
}

func (e *tracesExporter) shutdown(_ context.Context) error {
	return nil
}

func (e *tracesExporter) pushTraceData(_ context.Context, td ptrace.Traces) error {
	e.logger.Debug("would append spans",
		zap.Int("spans", td.SpanCount()),
		zap.String("destination", e.opts.TableRef()))
	return errNotImplemented
}
