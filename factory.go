// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:generate make mdatagen

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/metadata"
)

const (
	defaultSpansTable = "otel_spans"
	defaultLogsTable  = "otel_logs"

	// defaultMaxRequestBytes leaves roughly a megabyte of headroom under the
	// Storage Write API per-request limit. This is our threshold, not the
	// API's; the gap absorbs protobuf framing overhead we do not model.
	defaultMaxRequestBytes = 9 * 1024 * 1024
	// defaultMaxRowBytes bounds a single serialized row.
	defaultMaxRowBytes = 1024 * 1024

	defaultMaxInflightRequests = 1000
	defaultMaxInflightBytes    = 100 * 1024 * 1024
)

// NewFactory creates a factory for the BigQuery exporter.
//
// Traces and logs only. The OTLP metrics signal is an independent future
// proposal (N1) rather than a missing feature.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutConfig(),
		QueueSettings:   configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		BackOffConfig:   configretry.NewDefaultBackOffConfig(),
		SpansTable:      defaultSpansTable,
		LogsTable:       defaultLogsTable,
		Write: WriteConfig{
			MaxRequestBytes:     defaultMaxRequestBytes,
			MaxRowBytes:         defaultMaxRowBytes,
			MaxInflightRequests: defaultMaxInflightRequests,
			MaxInflightBytes:    defaultMaxInflightBytes,
		},
	}
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*Config)
	exp := newTracesExporter(set.TelemetrySettings, c)

	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		exp.pushTraceData,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	c := cfg.(*Config)
	exp := newLogsExporter(set.TelemetrySettings, c)

	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exp.pushLogData,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}
