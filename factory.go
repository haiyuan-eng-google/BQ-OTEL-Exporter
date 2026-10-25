// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/metadata"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/redact"
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
	defaultOffloadCacheSize      = 4096
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
	defaultStreamPoolSize      = 1

	// Multiplexing shares a pool of gRPC connections across default write
	// streams instead of opening a dedicated connection per stream.
	defaultEnableMultiplexing = true
	defaultConnectionPoolSize = 4

	// Defaults for optional adaptive AIMD write backpressure.
	defaultBackpressureMinRequestBytes     = 1024 * 1024
	defaultBackpressureMinInflightRequests = 1
	defaultBackpressureDecreaseFactor      = 0.5
	defaultBackpressureRecoveryInterval    = 10 * time.Second
	defaultBackpressureLatencyThreshold    = 2 * time.Second

	// defaultMaxElapsedTime sits deliberately above the ten-minute outage the
	// reliability journey exercises. Horizon expiry is terminal — the
	// persistent queue deletes the item — so a sub-outage horizon would break
	// the no-loss promise, while an unbounded one would make the duplicate
	// bound meaningless.
	defaultMaxElapsedTime = 900 * time.Second
)

// errUnexpectedConfigType is returned when an exporter constructor receives a
// nil or non-*Config component.Config value.
var errUnexpectedConfigType = errors.New("invalid config: expected *bigqueryexporter.Config")

// NewFactory creates a factory for the BigQuery exporter supporting traces,
// logs, and metrics.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		CreateDefaultConfig,
		exporter.WithTraces(CreateTracesExporter, metadata.TracesStability),
		exporter.WithLogs(CreateLogsExporter, metadata.LogsStability),
		exporter.WithMetrics(CreateMetricsExporter, metadata.MetricsStability),
	)
}

// CreateDefaultConfig returns the default configuration for the BigQuery exporter.
func CreateDefaultConfig() component.Config {
	return createDefaultConfig()
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
			StreamPoolSize:      defaultStreamPoolSize,
			EnableMultiplexing:  defaultEnableMultiplexing,
			ConnectionPoolSize:  defaultConnectionPoolSize,
			Backpressure: BackpressureConfig{
				MinRequestBytes:     defaultBackpressureMinRequestBytes,
				MinInflightRequests: defaultBackpressureMinInflightRequests,
				DecreaseFactor:      defaultBackpressureDecreaseFactor,
				RecoveryInterval:    defaultBackpressureRecoveryInterval,
				LatencyThreshold:    defaultBackpressureLatencyThreshold,
			},
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
			CacheSize:      defaultOffloadCacheSize,
			Timeout:        defaultOffloadTimeout,
		},
		Redaction: RedactionConfig{
			MaskReplacement: redact.DefaultMaskReplacement,
		},
	}
}

// CreateTracesExporter creates a traces exporter based on the provided configuration.
// It matches exporter.CreateTracesFunc so NewFactory and upstream Collector distribution
// wrappers can wire it directly with their own exporter.Settings.
func CreateTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	c, ok := cfg.(*Config)
	if !ok || c == nil {
		return nil, errUnexpectedConfigType
	}
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

// CreateLogsExporter creates a logs exporter based on the provided configuration.
// It matches exporter.CreateLogsFunc so NewFactory and upstream Collector distribution
// wrappers can wire it directly with their own exporter.Settings.
func CreateLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	c, ok := cfg.(*Config)
	if !ok || c == nil {
		return nil, errUnexpectedConfigType
	}
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

// CreateMetricsExporter creates a metrics exporter based on the provided configuration.
// It matches exporter.CreateMetricsFunc so NewFactory and upstream Collector distribution
// wrappers can wire it directly with their own exporter.Settings.
func CreateMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c, ok := cfg.(*Config)
	if !ok || c == nil {
		return nil, errUnexpectedConfigType
	}
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
