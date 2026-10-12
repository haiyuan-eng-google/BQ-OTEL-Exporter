// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

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
	defaultSpansTable   = "otel_spans"
	defaultLogsTable    = "otel_logs"
	defaultMetricsTable = "otel_metrics"
	// defaultQuarantineTable is used only once quarantine.enabled is set.
	defaultQuarantineTable               = "otel_quarantine"
	defaultQuarantinePartitionExpiryDays = 30
	defaultQuarantineMaxRawPayloadBytes  = 8 * 1024 * 1024
	defaultQuarantineTimeout             = 5 * time.Second

	// Defaults for optional GCS payload offloading.
	defaultOffloadPrefix         = "otel-offload"
	defaultOffloadMinBytes       = 256 * 1024
	defaultOffloadMaxObjectBytes = 64 * 1024 * 1024
	defaultOffloadMaxConcurrency = 8
	defaultOffloadTimeout        = 10 * time.Second

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

	// Multiplexing shares a pool of gRPC connections across default write
	// streams instead of opening a dedicated connection per stream.
	defaultEnableMultiplexing = true
	defaultConnectionPoolSize = 4

	// defaultMaxElapsedTime sits deliberately above the ten-minute outage the
	// reliability journey exercises. Horizon expiry is terminal — the
	// persistent queue deletes the item — so a sub-outage horizon would break
	// the no-loss promise, while an unbounded one would make the duplicate
	// bound meaningless.
	defaultMaxElapsedTime = 900 * time.Second
)

// NewFactory creates a factory for the BigQuery exporter supporting traces,
// logs, and metrics.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	backoff := configretry.NewDefaultBackOffConfig()
	backoff.MaxElapsedTime = defaultMaxElapsedTime
	queue := exporterhelper.NewDefaultQueueConfig()
	// Keep exporterhelper's sender concurrency within managedwriter's request
	// window. Higher values only add blocked attempt contexts under pressure.
	queue.NumConsumers = defaultMaxInflightRequests

	limits := transform.DefaultLimits()

	return &Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutConfig(),
		QueueSettings:   configoptional.Some(queue),
		BackOffConfig:   backoff,
		Traces:          SignalConfig{Table: defaultSpansTable},
		Logs:            LogsConfig{Table: defaultLogsTable},
		Metrics:         SignalConfig{Table: defaultMetricsTable},
		Write: WriteConfig{
			MaxRequestBytes:     defaultMaxRequestBytes,
			MaxRowBytes:         defaultMaxRowBytes,
			MaxInflightRequests: defaultMaxInflightRequests,
			MaxInflightBytes:    defaultMaxInflightBytes,
			EnableMultiplexing:  defaultEnableMultiplexing,
			ConnectionPoolSize:  defaultConnectionPoolSize,
		},
		Limits: LimitsConfig{
			MaxAttributeCount:    limits.MaxAttributeCount,
			MaxEventCount:        limits.MaxEventCount,
			MaxLinkCount:         limits.MaxLinkCount,
			MaxNestingDepth:      limits.MaxNestingDepth,
			MaxCollectionSize:    limits.MaxCollectionSize,
			MaxAttributeKeyBytes: limits.MaxAttributeKeyBytes,
			MaxValueBytes:        limits.MaxValueBytes,
		},
		Quarantine: QuarantineConfig{
			Table:                   defaultQuarantineTable,
			PartitionExpirationDays: defaultQuarantinePartitionExpiryDays,
			MaxRawPayloadBytes:      defaultQuarantineMaxRawPayloadBytes,
			Timeout:                 defaultQuarantineTimeout,
		},
		Offload: OffloadConfig{
			Prefix:         defaultOffloadPrefix,
			MinBytes:       defaultOffloadMinBytes,
			MaxObjectBytes: defaultOffloadMaxObjectBytes,
			MaxConcurrency: defaultOffloadMaxConcurrency,
			Timeout:        defaultOffloadTimeout,
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

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)
	exp, err := newMetricsExporter(set.TelemetrySettings, c)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		exp.pushMetricsData,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}
