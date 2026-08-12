// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:generate make mdatagen

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/metadata"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

// schemaVersion is the contract version this build writes.
const schemaVersion = schema.Version

const (
	defaultSpansTable = "otel_spans"
	defaultLogsTable  = "otel_logs"

	// defaultMaxRequestBytes leaves headroom under the Storage Write API
	// per-request limit. This is our threshold, not the API's; the gap
	// absorbs protobuf framing overhead we do not model.
	defaultMaxRequestBytes = 8 * 1024 * 1024
	// defaultMaxRowBytes bounds a single serialized row.
	defaultMaxRowBytes = 1024 * 1024

	// Flow control uses managedwriter's native in-flight limits rather than a
	// bespoke window. These match its documented defaults.
	defaultMaxInflightRequests = 8
	defaultMaxInflightBytes    = 64 * 1024 * 1024

	// defaultMaxElapsedTime sits deliberately above the ten-minute outage the
	// reliability journey exercises. Horizon expiry is terminal — the
	// persistent queue deletes the item — so a sub-outage horizon would break
	// the no-loss promise, while an unbounded one would make the duplicate
	// bound meaningless.
	defaultMaxElapsedTime = 900 * time.Second
)

// NewFactory creates a factory for the BigQuery exporter.
//
// Traces and logs only. The OTLP metrics signal is an independent future
// proposal rather than a missing feature: it needs its own relational design,
// and guessing at one would freeze the guess into a versioned contract.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	backoff := configretry.NewDefaultBackOffConfig()
	backoff.MaxElapsedTime = defaultMaxElapsedTime

	limits := transform.DefaultLimits()

	return &Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutConfig(),
		QueueSettings:   configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		BackOffConfig:   backoff,
		Traces:          SignalConfig{Table: defaultSpansTable},
		Logs:            LogsConfig{Table: defaultLogsTable},
		Write: WriteConfig{
			MaxRequestBytes:     defaultMaxRequestBytes,
			MaxRowBytes:         defaultMaxRowBytes,
			MaxInflightRequests: defaultMaxInflightRequests,
			MaxInflightBytes:    defaultMaxInflightBytes,
		},
		Limits: LimitsConfig{
			MaxAttributeCount: limits.MaxAttributeCount,
			MaxEventCount:     limits.MaxEventCount,
			MaxLinkCount:      limits.MaxLinkCount,
			MaxNestingDepth:   limits.MaxNestingDepth,
			MaxCollectionSize: limits.MaxCollectionSize,
			MaxValueBytes:     limits.MaxValueBytes,
		},
	}
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*Config)
	exp, err := newTracesExporter(set.TelemetrySettings, c)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		exp.pushTraceData,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		// exporterhelper is the single owner of append replay. managedwriter's
		// own write retries are disabled, so attempts are never multiplied.
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	c := cfg.(*Config)
	exp, err := newLogsExporter(set.TelemetrySettings, c)
	if err != nil {
		return nil, err
	}

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
