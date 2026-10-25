// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package testbench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/bigquery"
	bigqueryexporter "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/offload"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

const (
	// ModeBQFake selects the hermetic in-process gRPC Storage Write API fake.
	ModeBQFake = "bqfake"
	// ModeLive selects a live BigQuery dataset via Storage Write API.
	ModeLive = "live"
)

// RunnerConfig configures a single load benchmark execution.
type RunnerConfig struct {
	// Mode is "bqfake" (default) or "live".
	Mode string `json:"mode"`
	// Signals lists the OTLP signals to benchmark ("traces", "logs", "metrics").
	// Defaults to ["traces"] when empty.
	Signals []string `json:"signals"`
	// TargetRate is the target total records/sec across workers per signal.
	// When <= 0, workers push batches without rate-limiter sleeps (max throughput).
	TargetRate int `json:"target_rate"`
	// Duration is the benchmark measurement window per signal (default 2s).
	Duration time.Duration `json:"duration"`
	// BatchSize is the number of spans, log records, or metric data points per batch (default 500).
	BatchSize int `json:"batch_size"`
	// Workers is the number of concurrent producer goroutines per signal (default 4).
	Workers int `json:"workers"`
	// StreamPoolSize sets write.stream_pool_size (default 4, valid 1..16).
	StreamPoolSize int `json:"stream_pool_size"`
	// ConnectionPoolSize sets write.connection_pool_size (default 4, valid 1..16).
	ConnectionPoolSize int `json:"connection_pool_size"`
	// MaxInflightRequests sets write.max_inflight_requests (default 32).
	MaxInflightRequests int `json:"max_inflight_requests"`
	// MaxRequestBytes sets write.max_request_bytes (default 8 MiB).
	MaxRequestBytes int `json:"max_request_bytes"`
	// Compression sets write.compression ("" or "gzip").
	Compression string `json:"compression,omitempty"`
	// Redaction enables Step-0 sensitive-data redaction and plants synthetic secrets.
	Redaction bool `json:"redaction"`
	// PlantSecrets forces synthetic secret injection even when Redaction is false.
	PlantSecrets bool `json:"plant_secrets"`
	// Backpressure enables write.backpressure (AIMD concurrency & batch regulator).
	Backpressure bool `json:"backpressure"`
	// PayloadBytes sets the synthetic attribute payload size in bytes (default 1024).
	PayloadBytes int `json:"payload_bytes"`
	// UniquePayloads embeds a unique sequence prefix into every payload when true.
	UniquePayloads bool `json:"unique_payloads"`
	// OffloadDir enables local-directory content-addressed SHA-256 payload offload.
	OffloadDir string `json:"offload_dir,omitempty"`
	// OffloadThresholdBytes sets the byte threshold for local-directory offload (default 4096 when OffloadDir is set).
	OffloadThresholdBytes int `json:"offload_threshold_bytes,omitempty"`
	// SimulateAckDelay injects FaultDelayedAck into bqfake on every 3rd batch when > 0.
	SimulateAckDelay time.Duration `json:"simulate_ack_delay,omitempty"`
	// SimulateThrottleEvery injects FaultResourceExhausted into bqfake every N batches when > 0.
	SimulateThrottleEvery int `json:"simulate_throttle_every,omitempty"`
	// ProjectID is the target GCP project ("bench-project" in bqfake mode).
	ProjectID string `json:"project_id"`
	// DatasetID is the target BigQuery dataset ("otel_bench" in bqfake mode).
	DatasetID string `json:"dataset_id"`
	// Location is the BigQuery dataset location (default "US").
	Location string `json:"location"`
	// EphemeralDataset provisions the dataset and tables automatically in live mode
	// and deletes the dataset with all contents on teardown.
	EphemeralDataset bool `json:"ephemeral_dataset"`
	// KeepEphemeralDataset suppresses automatic deletion when EphemeralDataset is
	// managed by an outer sweep loop.
	KeepEphemeralDataset bool `json:"-"`
}

// Normalize applies defaults and validates c.
func (c *RunnerConfig) Normalize() error {
	if c.Mode == "" {
		c.Mode = ModeBQFake
	}
	if c.Mode != ModeBQFake && c.Mode != ModeLive {
		return fmt.Errorf("unsupported mode %q: must be %q or %q", c.Mode, ModeBQFake, ModeLive)
	}
	if len(c.Signals) == 0 {
		c.Signals = []string{"traces"}
	}
	for _, sig := range c.Signals {
		switch sig {
		case "traces", "logs", "metrics":
		default:
			return fmt.Errorf("unsupported signal %q: must be traces, logs, or metrics", sig)
		}
	}
	if c.Duration <= 0 {
		c.Duration = 2 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.StreamPoolSize <= 0 {
		c.StreamPoolSize = 4
	}
	if c.StreamPoolSize > 16 {
		return fmt.Errorf("stream_pool_size %d exceeds maximum 16", c.StreamPoolSize)
	}
	if c.ConnectionPoolSize <= 0 {
		c.ConnectionPoolSize = 4
	}
	if c.ConnectionPoolSize > 16 {
		return fmt.Errorf("connection_pool_size %d exceeds maximum 16", c.ConnectionPoolSize)
	}
	if c.MaxInflightRequests <= 0 {
		c.MaxInflightRequests = 32
	}
	if c.MaxRequestBytes <= 0 {
		c.MaxRequestBytes = 8 * 1024 * 1024
	}
	if c.PayloadBytes <= 0 {
		c.PayloadBytes = 1024
	}
	if c.OffloadDir != "" && c.OffloadThresholdBytes <= 0 {
		c.OffloadThresholdBytes = 4096
	}
	if c.Location == "" {
		c.Location = "US"
	}
	if c.Mode == ModeBQFake {
		if c.ProjectID == "" {
			c.ProjectID = "bench-project"
		}
		if c.DatasetID == "" {
			c.DatasetID = "otel_bench"
		}
	} else {
		if strings.TrimSpace(c.ProjectID) == "" {
			return errors.New("project_id is required when mode is \"live\"")
		}
		if strings.TrimSpace(c.DatasetID) == "" {
			c.DatasetID = fmt.Sprintf("otel_bench_%d", time.Now().UTC().UnixNano()/1e6)
		}
	}
	return nil
}

// LatencySummary reports batch append latency distribution in milliseconds.
type LatencySummary struct {
	Samples int     `json:"samples"`
	MinMs   float64 `json:"min_ms"`
	P50Ms   float64 `json:"p50_ms"`
	P95Ms   float64 `json:"p95_ms"`
	P99Ms   float64 `json:"p99_ms"`
	MaxMs   float64 `json:"max_ms"`
	MeanMs  float64 `json:"mean_ms"`
}

// MemorySummary reports runtime memory and CPU deltas captured across the run.
type MemorySummary struct {
	HeapAllocBytes       uint64  `json:"heap_alloc_bytes"`
	HeapAllocMB          float64 `json:"heap_alloc_mb"`
	TotalAllocDeltaBytes uint64  `json:"total_alloc_delta_bytes"`
	TotalAllocDeltaMB    float64 `json:"total_alloc_delta_mb"`
	MallocsDelta         uint64  `json:"mallocs_delta"`
	NumGCDelta           uint32  `json:"num_gc_delta"`
	SysMB                float64 `json:"sys_mb"`
	PeakRSSMB            float64 `json:"peak_rss_mb"`
	Goroutines           int     `json:"goroutines"`
	CPUSeconds           float64 `json:"cpu_seconds"`
	CPUCoresUsed         float64 `json:"cpu_cores_used"`
	CPUCoresPer10kRowsS  float64 `json:"cpu_cores_per_10k_rows_s"`
}

// TelemetrySnapshot holds the exporter's OpenTelemetry self-telemetry metrics
// collected at the end of a benchmark run.
type TelemetrySnapshot struct {
	AcknowledgedRows      int64            `json:"acknowledged_rows"`
	RejectedRows          int64            `json:"rejected_rows"`
	Retries               int64            `json:"retries"`
	QuarantinedRows       int64            `json:"quarantined_rows"`
	AppendWaitCount       uint64           `json:"append_result_wait_count"`
	AppendWaitSumSeconds  float64          `json:"append_result_wait_sum_s"`
	AppendWaitMeanMs      float64          `json:"append_result_wait_mean_ms"`
	AppendBytesTotal      int64            `json:"append_bytes_total"`
	RedactedValuesTotal   int64            `json:"redacted_values_total"`
	RedactedByMatchType   map[string]int64 `json:"redacted_by_match_type,omitempty"`
	BackpressureEvents    map[string]int64 `json:"backpressure_events,omitempty"`
	BackpressureLevel     int64            `json:"backpressure_level"`
	EffectiveRequestBytes int64            `json:"effective_request_bytes"`
	OffloadedPayloads     int64            `json:"offloaded_payloads"`
	OffloadedBytes        int64            `json:"offloaded_bytes"`
	OffloadDedupHits      int64            `json:"offload_dedup_hits"`
}

// SignalResult captures the benchmark measurements for one signal or combined run.
type SignalResult struct {
	Signal             string            `json:"signal"`
	Mode               string            `json:"mode"`
	StreamPoolSize     int               `json:"stream_pool_size"`
	ConnectionPoolSize int               `json:"connection_pool_size"`
	Workers            int               `json:"workers"`
	BatchSize          int               `json:"batch_size"`
	PayloadBytes       int               `json:"payload_bytes"`
	Redaction          bool              `json:"redaction"`
	Backpressure       bool              `json:"backpressure"`
	ElapsedSeconds     float64           `json:"elapsed_seconds"`
	BatchesSucceeded   int64             `json:"batches_succeeded"`
	BatchesFailed      int64             `json:"batches_failed"`
	RecordsSubmitted   int64             `json:"records_submitted"`
	AchievedRowsPerSec float64           `json:"achieved_rows_per_sec"`
	WireMBPerSec       float64           `json:"wire_mb_per_sec"`
	BytesPerRow        float64           `json:"bytes_per_row"`
	Latency            LatencySummary    `json:"latency"`
	Memory             MemorySummary     `json:"memory"`
	Telemetry          TelemetrySnapshot `json:"telemetry"`
}

// BenchmarkReport aggregates one or more SignalResults from a run or sweep.
type BenchmarkReport struct {
	Timestamp               string         `json:"timestamp"`
	GoVersion               string         `json:"go_version"`
	GOOS                    string         `json:"goos"`
	GOARCH                  string         `json:"goarch"`
	NumCPU                  int            `json:"num_cpu"`
	Config                  RunnerConfig   `json:"config"`
	EphemeralDatasetDeleted bool           `json:"ephemeral_dataset_deleted,omitempty"`
	Results                 []SignalResult `json:"results"`
}

// Run executes the benchmark specified by cfg and returns a structured report.
func Run(ctx context.Context, cfg RunnerConfig) (*BenchmarkReport, error) {
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}

	report := &BenchmarkReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
		Config:    cfg,
	}

	if cfg.Mode == ModeLive && cfg.EphemeralDataset && !cfg.KeepEphemeralDataset {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := DeleteDatasetWithContents(cleanupCtx, cfg.ProjectID, cfg.DatasetID); err == nil {
				report.EphemeralDatasetDeleted = true
			}
		}()
	}

	for _, sig := range cfg.Signals {
		res, err := runSingleSignal(ctx, cfg, sig)
		if err != nil {
			return nil, fmt.Errorf("benchmark signal %s: %w", sig, err)
		}
		report.Results = append(report.Results, res)
	}
	return report, nil
}

// RunStreamPoolSweep executes cfg across each streamPoolSizes value (e.g. 1, 2, 4, 8, 16)
// and returns a combined BenchmarkReport. In live mode with EphemeralDataset, a single
// ephemeral dataset is shared across the sweep and deleted upon completion.
func RunStreamPoolSweep(ctx context.Context, cfg RunnerConfig, streamPoolSizes []int) (*BenchmarkReport, error) {
	if len(streamPoolSizes) == 0 {
		streamPoolSizes = []int{1, 2, 4, 8, 16}
	}
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}

	combined := &BenchmarkReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
		Config:    cfg,
	}

	if cfg.Mode == ModeLive && cfg.EphemeralDataset {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := DeleteDatasetWithContents(cleanupCtx, cfg.ProjectID, cfg.DatasetID); err == nil {
				combined.EphemeralDatasetDeleted = true
			}
		}()
	}

	for _, poolSize := range streamPoolSizes {
		stepCfg := cfg
		stepCfg.StreamPoolSize = poolSize
		stepCfg.KeepEphemeralDataset = true
		stepReport, err := Run(ctx, stepCfg)
		if err != nil {
			return nil, err
		}
		combined.Results = append(combined.Results, stepReport.Results...)
	}
	return combined, nil
}

// DeleteDatasetWithContents removes a BigQuery dataset and all tables inside it.
func DeleteDatasetWithContents(ctx context.Context, projectID, datasetID string) error {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("creating bigquery client for cleanup: %w", err)
	}
	defer func() { _ = client.Close() }()
	if err := client.Dataset(datasetID).DeleteWithContents(ctx); err != nil {
		return fmt.Errorf("deleting dataset %s.%s: %w", projectID, datasetID, err)
	}
	return nil
}

func runSingleSignal(ctx context.Context, cfg RunnerConfig, signal string) (SignalResult, error) {
	var fakeSrv *bqfake.Server
	if cfg.Mode == ModeBQFake {
		var err error
		fakeSrv, err = bqfake.New()
		if err != nil {
			return SignalResult{}, fmt.Errorf("starting bqfake server: %w", err)
		}
		defer fakeSrv.Close()
	}

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	factory := bigqueryexporter.NewFactory()
	expCfg, ok := factory.CreateDefaultConfig().(*bigqueryexporter.Config)
	if !ok {
		return SignalResult{}, errors.New("unexpected exporter config type")
	}

	// Disable the asynchronous sending_queue wrapper so ConsumeTraces/Logs/Metrics
	// measures synchronous end-to-end pipeline + Storage Write API AppendRows
	// acknowledgement latency directly instead of buffering in memory.
	expCfg.QueueSettings = configoptional.None[exporterhelper.QueueBatchConfig]()
	expCfg.Project = cfg.ProjectID
	expCfg.Dataset = cfg.DatasetID
	expCfg.Location = cfg.Location
	expCfg.Write.StreamPoolSize = cfg.StreamPoolSize
	expCfg.Write.ConnectionPoolSize = cfg.ConnectionPoolSize
	expCfg.Write.MaxInflightRequests = cfg.MaxInflightRequests
	expCfg.Write.MaxRequestBytes = cfg.MaxRequestBytes
	expCfg.Write.Compression = cfg.Compression
	expCfg.Redaction.Enabled = cfg.Redaction
	if cfg.Redaction {
		expCfg.Redaction.RedactLogBody = true
		expCfg.Redaction.BlockedValuePatterns = []string{
			`Bearer\s+sk-[A-Za-z0-9_-]{16,}`,
			`ya29\.[0-9A-Za-z_-]{20,}`,
			`AKIA[0-9A-Z]{16}`,
		}
	}
	if cfg.Backpressure {
		expCfg.Write.Backpressure.Enabled = true
		if cfg.SimulateAckDelay > 0 {
			expCfg.Write.Backpressure.LatencyThreshold = cfg.SimulateAckDelay / 2
			if expCfg.Write.Backpressure.LatencyThreshold < time.Millisecond {
				expCfg.Write.Backpressure.LatencyThreshold = time.Millisecond
			}
		}
	}

	if cfg.Mode == ModeBQFake {
		expCfg.Endpoint.URL = fakeSrv.Addr()
		expCfg.Endpoint.Insecure = true
		expCfg.Endpoint.WithoutAuthentication = true
		expCfg.TimeoutSettings.Timeout = 10 * time.Second
		expCfg.BackOffConfig.InitialInterval = 10 * time.Millisecond
		expCfg.BackOffConfig.MaxInterval = 50 * time.Millisecond
	} else {
		expCfg.TimeoutSettings.Timeout = 30 * time.Second
		if cfg.EphemeralDataset {
			expCfg.AutoCreate.Dataset = true
			expCfg.AutoCreate.Tables = true
			expCfg.Quarantine.Enabled = true
		}
	}

	if err := expCfg.Validate(); err != nil {
		return SignalResult{}, fmt.Errorf("validating exporter config: %w", err)
	}

	set := exporter.Settings{
		ID: component.NewID(component.MustNewType("bigquery")),
		TelemetrySettings: component.TelemetrySettings{
			Logger:         zap.NewNop(),
			TracerProvider: tracenoop.NewTracerProvider(),
			MeterProvider:  mp,
		},
	}

	var (
		tracesExp  exporter.Traces
		logsExp    exporter.Logs
		metricsExp exporter.Metrics
		err        error
	)

	switch signal {
	case "traces":
		tracesExp, err = factory.CreateTraces(ctx, set, expCfg)
	case "logs":
		logsExp, err = factory.CreateLogs(ctx, set, expCfg)
	case "metrics":
		metricsExp, err = factory.CreateMetrics(ctx, set, expCfg)
	}
	if err != nil {
		return SignalResult{}, fmt.Errorf("creating %s exporter: %w", signal, err)
	}

	host := componenttest.NewNopHost()
	startExp := func(c component.Component) error {
		return c.Start(ctx, host)
	}
	stopExp := func(c component.Component) error {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return c.Shutdown(shutdownCtx)
	}

	var activeComp component.Component
	switch signal {
	case "traces":
		activeComp = tracesExp
	case "logs":
		activeComp = logsExp
	case "metrics":
		activeComp = metricsExp
	}
	if err := startExp(activeComp); err != nil {
		return SignalResult{}, fmt.Errorf("starting %s exporter: %w", signal, err)
	}

	// Optional local-directory content-addressed payload offloader.
	var (
		localOffloader  *offload.Offloader
		offloadPayloads atomic.Int64
		offloadBytes    atomic.Int64
		offloadDedup    atomic.Int64
	)
	if cfg.OffloadDir != "" {
		store := &localDirBlobStore{rootDir: cfg.OffloadDir}
		localOffloader, err = offload.New(store, offload.Options{
			Bucket:         "local-offload-bench",
			Prefix:         "otel-offload",
			MinBytes:       cfg.OffloadThresholdBytes,
			MaxObjectBytes: 64 * 1024 * 1024,
			MaxConcurrency: 8,
			CacheSize:      10000,
			Timeout:        5 * time.Second,
			OnOffload: func(payloads int, b int64, errClass string) {
				if errClass == "" {
					offloadPayloads.Add(int64(payloads))
					offloadBytes.Add(b)
				}
			},
			OnDedupHit: func() {
				offloadDedup.Add(1)
			},
		})
		if err != nil {
			_ = stopExp(activeComp)
			return SignalResult{}, fmt.Errorf("creating local offloader: %w", err)
		}
	}

	gen := NewGenerator(GeneratorConfig{
		PayloadBytes:   cfg.PayloadBytes,
		PlantSecrets:   cfg.Redaction || cfg.PlantSecrets,
		UniquePayloads: cfg.UniquePayloads,
	})

	// Pre-build a pool of batches per worker so the benchmark measures exporter
	// pipeline (redaction, ProtoRows serialization, stream pool routing, AIMD
	// backpressure, gRPC Write API I/O) rather than synthetic string building.
	// When Redaction is enabled, each consume call clones a fresh pdata batch so
	// Step-0 in-place mutation re-runs on every batch.
	const prebuiltCount = 8
	prebuiltTraces := make([]ptrace.Traces, prebuiltCount)
	prebuiltLogs := make([]plog.Logs, prebuiltCount)
	prebuiltMetrics := make([]pmetric.Metrics, prebuiltCount)
	for i := 0; i < prebuiltCount; i++ {
		switch signal {
		case "traces":
			prebuiltTraces[i] = gen.TracesBatch(cfg.BatchSize)
		case "logs":
			prebuiltLogs[i] = gen.LogsBatch(cfg.BatchSize)
		case "metrics":
			prebuiltMetrics[i] = gen.MetricsBatch(cfg.BatchSize)
		}
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	cpuBefore, _ := readProcessCPUAndRSS()

	var (
		batchesSucceeded atomic.Int64
		batchesFailed    atomic.Int64
		recordsSubmitted atomic.Int64
		batchSeq         atomic.Int64
		latMu            sync.Mutex
		latenciesMs      = make([]float64, 0, 1024)
	)

	targetTable := fmt.Sprintf("projects/%s/datasets/%s/tables/otel_%s", cfg.ProjectID, cfg.DatasetID, signal)
	if signal == "traces" {
		targetTable = fmt.Sprintf("projects/%s/datasets/%s/tables/otel_spans", cfg.ProjectID, cfg.DatasetID)
	}

	runCtx, cancelRun := context.WithTimeout(ctx, cfg.Duration)
	defer cancelRun()

	var perWorkerInterval time.Duration
	if cfg.TargetRate > 0 {
		batchesPerSec := float64(cfg.TargetRate) / float64(cfg.BatchSize)
		perWorkerBatchesPerSec := batchesPerSec / float64(cfg.Workers)
		if perWorkerBatchesPerSec > 0 {
			perWorkerInterval = time.Duration(float64(time.Second) / perWorkerBatchesPerSec)
		}
	}

	startWall := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var nextTick time.Time
			if perWorkerInterval > 0 {
				nextTick = time.Now()
			}
			for {
				if runCtx.Err() != nil || time.Since(startWall) >= cfg.Duration {
					return
				}
				if perWorkerInterval > 0 {
					now := time.Now()
					if now.Before(nextTick) {
						select {
						case <-runCtx.Done():
							return
						case <-time.After(nextTick.Sub(now)):
						}
					}
					nextTick = nextTick.Add(perWorkerInterval)
				}

				seq := int(batchSeq.Add(1))
				if fakeSrv != nil {
					if cfg.SimulateThrottleEvery > 0 && seq%cfg.SimulateThrottleEvery == 0 {
						fakeSrv.EnqueueFault(bqfake.Fault{
							Mode:  bqfake.FaultResourceExhausted,
							Table: targetTable,
						})
					} else if cfg.SimulateAckDelay > 0 && seq%3 == 0 {
						fakeSrv.EnqueueFault(bqfake.Fault{
							Mode:  bqfake.FaultDelayedAck,
							Delay: cfg.SimulateAckDelay,
							Table: targetTable,
						})
					}
				}

				idx := (workerID + seq) % prebuiltCount
				t0 := time.Now()
				var consumeErr error
				switch signal {
				case "traces":
					td := prebuiltTraces[idx]
					if localOffloader != nil {
						cloned := ptrace.NewTraces()
						prebuiltTraces[idx].CopyTo(cloned)
						offloadTraceBatch(ctx, localOffloader, cloned)
						td = cloned
					}
					consumeErr = tracesExp.ConsumeTraces(ctx, td)
				case "logs":
					ld := prebuiltLogs[idx]
					if localOffloader != nil {
						cloned := plog.NewLogs()
						prebuiltLogs[idx].CopyTo(cloned)
						offloadLogBatch(ctx, localOffloader, cloned)
						ld = cloned
					}
					consumeErr = logsExp.ConsumeLogs(ctx, ld)
				case "metrics":
					md := prebuiltMetrics[idx]
					consumeErr = metricsExp.ConsumeMetrics(ctx, md)
				}
				elapsedMs := float64(time.Since(t0).Microseconds()) / 1000.0

				if consumeErr != nil {
					if errors.Is(consumeErr, context.DeadlineExceeded) || errors.Is(consumeErr, context.Canceled) {
						return
					}
					batchesFailed.Add(1)
					continue
				}

				batchesSucceeded.Add(1)
				recordsSubmitted.Add(int64(cfg.BatchSize))
				latMu.Lock()
				latenciesMs = append(latenciesMs, elapsedMs)
				latMu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(startWall)
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}

	goroutinesDuring := runtime.NumGoroutine()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	cpuAfter, peakRSSMB := readProcessCPUAndRSS()

	if err := stopExp(activeComp); err != nil {
		return SignalResult{}, fmt.Errorf("shutting down %s exporter: %w", signal, err)
	}

	var rm metricdata.ResourceMetrics
	_ = reader.Collect(context.Background(), &rm)
	telem := extractTelemetrySnapshot(rm)
	if localOffloader != nil {
		telem.OffloadedPayloads += offloadPayloads.Load()
		telem.OffloadedBytes += offloadBytes.Load()
		telem.OffloadDedupHits += offloadDedup.Load()
	}

	elapsedSec := elapsed.Seconds()
	ackedRows := telem.AcknowledgedRows
	if ackedRows <= 0 {
		ackedRows = recordsSubmitted.Load()
	}
	rowsPerSec := float64(ackedRows) / elapsedSec
	wireMBPerSec := (float64(telem.AppendBytesTotal) / (1024.0 * 1024.0)) / elapsedSec
	var bytesPerRow float64
	if ackedRows > 0 {
		bytesPerRow = float64(telem.AppendBytesTotal) / float64(ackedRows)
	}

	cpuDeltaSec := cpuAfter - cpuBefore
	if cpuDeltaSec < 0 {
		cpuDeltaSec = 0
	}
	cpuCoresUsed := cpuDeltaSec / elapsedSec
	var cpuCoresPer10k float64
	if rowsPerSec > 0 {
		cpuCoresPer10k = cpuCoresUsed / (rowsPerSec / 10000.0)
	}

	var totalAllocDelta uint64
	if memAfter.TotalAlloc >= memBefore.TotalAlloc {
		totalAllocDelta = memAfter.TotalAlloc - memBefore.TotalAlloc
	}
	var mallocsDelta uint64
	if memAfter.Mallocs >= memBefore.Mallocs {
		mallocsDelta = memAfter.Mallocs - memBefore.Mallocs
	}
	var numGCDelta uint32
	if memAfter.NumGC >= memBefore.NumGC {
		numGCDelta = memAfter.NumGC - memBefore.NumGC
	}

	return SignalResult{
		Signal:             signal,
		Mode:               cfg.Mode,
		StreamPoolSize:     cfg.StreamPoolSize,
		ConnectionPoolSize: cfg.ConnectionPoolSize,
		Workers:            cfg.Workers,
		BatchSize:          cfg.BatchSize,
		PayloadBytes:       cfg.PayloadBytes,
		Redaction:          cfg.Redaction,
		Backpressure:       cfg.Backpressure,
		ElapsedSeconds:     round2(elapsedSec),
		BatchesSucceeded:   batchesSucceeded.Load(),
		BatchesFailed:      batchesFailed.Load(),
		RecordsSubmitted:   recordsSubmitted.Load(),
		AchievedRowsPerSec: round2(rowsPerSec),
		WireMBPerSec:       round2(wireMBPerSec),
		BytesPerRow:        round2(bytesPerRow),
		Latency:            computeLatencySummary(latenciesMs),
		Memory: MemorySummary{
			HeapAllocBytes:       memAfter.HeapAlloc,
			HeapAllocMB:          round2(float64(memAfter.HeapAlloc) / (1024.0 * 1024.0)),
			TotalAllocDeltaBytes: totalAllocDelta,
			TotalAllocDeltaMB:    round2(float64(totalAllocDelta) / (1024.0 * 1024.0)),
			MallocsDelta:         mallocsDelta,
			NumGCDelta:           numGCDelta,
			SysMB:                round2(float64(memAfter.Sys) / (1024.0 * 1024.0)),
			PeakRSSMB:            round2(peakRSSMB),
			Goroutines:           goroutinesDuring,
			CPUSeconds:           round2(cpuDeltaSec),
			CPUCoresUsed:         round2(cpuCoresUsed),
			CPUCoresPer10kRowsS:  round2(cpuCoresPer10k),
		},
		Telemetry: telem,
	}, nil
}

type localDirBlobStore struct {
	rootDir string
	seq     atomic.Int64
}

func (s *localDirBlobStore) WriteObject(ctx context.Context, bucket, object, _ string, data []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	target := filepath.Join(s.rootDir, bucket, filepath.FromSlash(object))
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return "", err
	}
	if err := os.WriteFile(target, data, 0o600); err != nil {
		return "", err
	}
	ver := fmt.Sprintf("%d", s.seq.Add(1))
	return ver, nil
}

func offloadTraceBatch(ctx context.Context, o *offload.Offloader, td ptrace.Traces) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		res := rss.At(i).Resource()
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			scope := sss.At(j).Scope()
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				_, _ = o.OffloadSpan(ctx, spans.At(k), res, scope)
			}
		}
	}
}

func offloadLogBatch(ctx context.Context, o *offload.Offloader, ld plog.Logs) {
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		res := rls.At(i).Resource()
		sls := rls.At(i).ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			scope := sls.At(j).Scope()
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				_, _ = o.OffloadLog(ctx, lrs.At(k), res, scope)
			}
		}
	}
}

func computeLatencySummary(samples []float64) LatencySummary {
	if len(samples) == 0 {
		return LatencySummary{}
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	var sum float64
	for _, v := range sorted {
		sum += v
	}
	return LatencySummary{
		Samples: len(sorted),
		MinMs:   round2(sorted[0]),
		P50Ms:   round2(percentile(sorted, 0.50)),
		P95Ms:   round2(percentile(sorted, 0.95)),
		P99Ms:   round2(percentile(sorted, 0.99)),
		MaxMs:   round2(sorted[len(sorted)-1]),
		MeanMs:  round2(sum / float64(len(sorted))),
	}
}

func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	pos := q * float64(len(sorted)-1)
	lower := int(math.Floor(pos))
	upper := int(math.Ceil(pos))
	if lower == upper {
		return sorted[lower]
	}
	weight := pos - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

func extractTelemetrySnapshot(rm metricdata.ResourceMetrics) TelemetrySnapshot {
	snap := TelemetrySnapshot{
		RedactedByMatchType: make(map[string]int64),
		BackpressureEvents:  make(map[string]int64),
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "otelcol_exporter_bigquery_acknowledged_rows":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						snap.AcknowledgedRows += dp.Value
					}
				}
			case "otelcol_exporter_bigquery_rejected_rows":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						snap.RejectedRows += dp.Value
					}
				}
			case "otelcol_exporter_bigquery_retries":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						snap.Retries += dp.Value
					}
				}
			case "otelcol_exporter_bigquery_quarantined_rows":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						snap.QuarantinedRows += dp.Value
					}
				}
			case "otelcol_exporter_bigquery_append_result_wait":
				if hist, ok := m.Data.(metricdata.Histogram[float64]); ok {
					for _, dp := range hist.DataPoints {
						snap.AppendWaitCount += dp.Count
						snap.AppendWaitSumSeconds += dp.Sum
					}
				}
			case "otelcol_exporter_bigquery_append_bytes":
				switch d := m.Data.(type) {
				case metricdata.Sum[int64]:
					for _, dp := range d.DataPoints {
						snap.AppendBytesTotal += dp.Value
					}
				case metricdata.Histogram[int64]:
					for _, dp := range d.DataPoints {
						snap.AppendBytesTotal += dp.Sum
					}
				}
			case "otelcol_exporter_bigquery_redacted_values":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						snap.RedactedValuesTotal += dp.Value
						if mt, ok := attrString(dp.Attributes, "match_type"); ok {
							snap.RedactedByMatchType[mt] += dp.Value
						}
					}
				}
			case "otelcol_exporter_bigquery_backpressure_events":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						trig, _ := attrString(dp.Attributes, "trigger")
						if trig == "" {
							trig = "unknown"
						}
						snap.BackpressureEvents[trig] += dp.Value
					}
				}
			case "otelcol_exporter_bigquery_backpressure_level":
				if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok {
					for _, dp := range gauge.DataPoints {
						snap.BackpressureLevel = dp.Value
					}
				}
			case "otelcol_exporter_bigquery_effective_request_bytes":
				if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok {
					for _, dp := range gauge.DataPoints {
						snap.EffectiveRequestBytes = dp.Value
					}
				}
			case "otelcol_exporter_bigquery_offloaded_payloads":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						snap.OffloadedPayloads += dp.Value
					}
				}
			case "otelcol_exporter_bigquery_offloaded_bytes":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						snap.OffloadedBytes += dp.Value
					}
				}
			case "otelcol_exporter_bigquery_offload_dedup_hits":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						snap.OffloadDedupHits += dp.Value
					}
				}
			}
		}
	}
	if snap.AppendWaitCount > 0 {
		snap.AppendWaitMeanMs = round2((snap.AppendWaitSumSeconds * 1000.0) / float64(snap.AppendWaitCount))
	}
	snap.AppendWaitSumSeconds = round2(snap.AppendWaitSumSeconds)
	if len(snap.RedactedByMatchType) == 0 {
		snap.RedactedByMatchType = nil
	}
	if len(snap.BackpressureEvents) == 0 {
		snap.BackpressureEvents = nil
	}
	return snap
}

func attrString(set attribute.Set, key string) (string, bool) {
	val, ok := set.Value(attribute.Key(key))
	if !ok {
		return "", false
	}
	return val.AsString(), true
}

func readProcessCPUAndRSS() (cpuSeconds float64, peakRSSMB float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	userSec := float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	sysSec := float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	// On Linux, Maxrss is reported in kilobytes.
	rssMB := float64(ru.Maxrss) / 1024.0
	return userSec + sysSec, rssMB
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

// FormatJSON renders report as indented JSON.
func FormatJSON(report *BenchmarkReport) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// FormatMarkdown renders report as a Markdown summary table with self-telemetry
// and resource utilization columns.
func FormatMarkdown(report *BenchmarkReport) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "### Benchmark Summary (`mode=%s`, Go `%s`, `%s/%s`, `%d` vCPU)\n\n",
		report.Config.Mode, report.GoVersion, report.GOOS, report.GOARCH, report.NumCPU)
	sb.WriteString("| Signal | Stream Pool | Redaction | Backpressure | Achieved Rows/s | Wire MB/s | Bytes/Row | p50 (ms) | p95 (ms) | p99 (ms) | CPU Cores / 10k rows/s | Heap Alloc (MB) | Peak RSS (MB) | Acked Rows | Redacted Values |\n")
	sb.WriteString("|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range report.Results {
		fmt.Fprintf(&sb, "| `%s` | %d | `%t` | `%t` | %.1f | %.2f | %.0f | %.2f | %.2f | %.2f | %.2f | %.2f | %.1f | %d | %d |\n",
			r.Signal,
			r.StreamPoolSize,
			r.Redaction,
			r.Backpressure,
			r.AchievedRowsPerSec,
			r.WireMBPerSec,
			r.BytesPerRow,
			r.Latency.P50Ms,
			r.Latency.P95Ms,
			r.Latency.P99Ms,
			r.Memory.CPUCoresPer10kRowsS,
			r.Memory.HeapAllocMB,
			r.Memory.PeakRSSMB,
			r.Telemetry.AcknowledgedRows,
			r.Telemetry.RedactedValuesTotal,
		)
	}
	return sb.String()
}
