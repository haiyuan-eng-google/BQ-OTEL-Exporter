// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package testbench

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	bigqueryexporter "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/offload"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/protoenc"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/redact"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
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
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const (
	// ModeBQFake selects the hermetic in-process gRPC Storage Write API fake.
	ModeBQFake = "bqfake"
	// ModeLive selects a live BigQuery dataset via Storage Write API.
	ModeLive = "live"

	// MaxBenchmarkPayloadBytes bounds synthetic payload_bytes (16 MiB) so extreme
	// flag values fail closed during validation rather than panicking in make.
	MaxBenchmarkPayloadBytes = 16 * 1024 * 1024
	// MaxBenchmarkBatchSize bounds synthetic batch_size (100,000 records).
	MaxBenchmarkBatchSize = 100_000
	// MaxBenchmarkWorkers bounds concurrent benchmark workers (256 goroutines).
	MaxBenchmarkWorkers = 256
	// MaxBenchmarkInflightRequests bounds write.max_inflight_requests (65,536) so
	// extreme flag values fail closed during validation rather than allocating a
	// multi-gigabyte flow-control semaphore slice in managedwriter.
	MaxBenchmarkInflightRequests = 65536
	// MaxBatchPayloadBudgetBytes bounds int64(batch_size)*int64(payload_bytes)
	// (256 MiB per batch) to prevent pre-allocation OOM panics.
	MaxBatchPayloadBudgetBytes = 256 * 1024 * 1024

	ephemeralOwnerLabelKey = "bq_otel_testbench_owner"
)

var blockedSecretPatterns = []string{
	`Bearer\s+sk-[A-Za-z0-9_-]{16,}`,
	`ya29\.[0-9A-Za-z_-]{20,}`,
	`AKIA[0-9A-Z]{16}`,
}

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
	// OffloadGCSBucket enables native exporter GCS payload offloading (payload_refs ObjectRef) in live mode.
	OffloadGCSBucket string `json:"offload_gcs_bucket,omitempty"`
	// OffloadThresholdBytes sets the byte threshold for payload offload (default 4096 when OffloadDir or OffloadGCSBucket is set).
	OffloadThresholdBytes int `json:"offload_threshold_bytes,omitempty"`
	// SimulateAckDelay injects FaultDelayedAck into bqfake on batch 1 and every 3rd batch when > 0.
	SimulateAckDelay time.Duration `json:"simulate_ack_delay,omitempty"`
	// SimulateThrottleEvery injects FaultResourceExhausted into bqfake every N batches when > 0.
	SimulateThrottleEvery int `json:"simulate_throttle_every,omitempty"`
	// CallTimeout overrides the per-batch Consume* timeout when > 0.
	CallTimeout time.Duration `json:"call_timeout,omitempty"`
	// AllowPartialFailures permits Run to succeed when BatchesFailed > 0 as long
	// as at least one batch succeeded and rows were acknowledged (used for
	// fault-injection tests that intentionally trigger per-batch timeouts).
	AllowPartialFailures bool `json:"allow_partial_failures,omitempty"`
	// ProjectID is the target GCP project ("bench-project" in bqfake mode).
	ProjectID string `json:"project_id"`
	// DatasetID is the target BigQuery dataset ("otel_bench" in bqfake mode).
	DatasetID string `json:"dataset_id"`
	// Location is the BigQuery dataset location (default "US").
	Location string `json:"location"`
	// EphemeralDataset provisions a unique otel_bench_<timestamp>_<random_hex>
	// dataset and tables automatically in live mode and deletes the dataset with
	// all contents on teardown. Must not be combined with an explicit DatasetID.
	EphemeralDataset bool `json:"ephemeral_dataset"`
	// KeepEphemeralDataset suppresses automatic deletion when EphemeralDataset is
	// managed by an outer sweep loop.
	KeepEphemeralDataset bool `json:"-"`
	// ClientOptions injects optional Google API client options for BigQuery REST
	// dataset lifecycle calls in tests.
	ClientOptions []option.ClientOption `json:"-"`

	ephemeralDatasetGenerated bool
	workerHook                func(workerID, seq int)
}

func generateEphemeralDatasetID() (string, error) {
	var randBytes [8]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		return "", fmt.Errorf("generating cryptographic random suffix for ephemeral dataset: %w", err)
	}
	return fmt.Sprintf("otel_bench_%d_%s", time.Now().UTC().UnixMilli(), hex.EncodeToString(randBytes[:])), nil
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
	if c.TargetRate < 0 {
		return fmt.Errorf("target_rate %d must be >= 0", c.TargetRate)
	}
	if c.Duration < 0 {
		return fmt.Errorf("duration %s must be > 0", c.Duration)
	}
	if c.Duration == 0 {
		c.Duration = 2 * time.Second
	}
	if c.BatchSize < 0 {
		return fmt.Errorf("batch_size %d must be > 0", c.BatchSize)
	}
	if c.BatchSize == 0 {
		c.BatchSize = 500
	}
	if c.BatchSize > MaxBenchmarkBatchSize {
		return fmt.Errorf("batch_size %d exceeds maximum %d", c.BatchSize, MaxBenchmarkBatchSize)
	}
	if c.Workers < 0 {
		return fmt.Errorf("workers %d must be > 0", c.Workers)
	}
	if c.Workers == 0 {
		c.Workers = 4
	}
	if c.Workers > MaxBenchmarkWorkers {
		return fmt.Errorf("workers %d exceeds maximum %d", c.Workers, MaxBenchmarkWorkers)
	}
	if c.StreamPoolSize < 0 || c.StreamPoolSize > 16 {
		return fmt.Errorf("stream_pool_size %d must be between 1 and 16", c.StreamPoolSize)
	}
	if c.StreamPoolSize == 0 {
		c.StreamPoolSize = 4
	}
	if c.ConnectionPoolSize < 0 || c.ConnectionPoolSize > 16 {
		return fmt.Errorf("connection_pool_size %d must be between 1 and 16", c.ConnectionPoolSize)
	}
	if c.ConnectionPoolSize == 0 {
		c.ConnectionPoolSize = 4
	}
	if c.MaxInflightRequests < 0 {
		return fmt.Errorf("max_inflight_requests %d must be > 0", c.MaxInflightRequests)
	}
	if c.MaxInflightRequests == 0 {
		c.MaxInflightRequests = 32
	}
	if c.MaxInflightRequests > MaxBenchmarkInflightRequests {
		return fmt.Errorf("max_inflight_requests %d exceeds maximum %d", c.MaxInflightRequests, MaxBenchmarkInflightRequests)
	}
	if c.MaxRequestBytes < 0 {
		return fmt.Errorf("max_request_bytes %d must be > 0", c.MaxRequestBytes)
	}
	if c.MaxRequestBytes == 0 {
		c.MaxRequestBytes = 8 * 1024 * 1024
	}
	if c.PayloadBytes < 0 {
		return fmt.Errorf("payload_bytes %d must be > 0", c.PayloadBytes)
	}
	if c.PayloadBytes == 0 {
		c.PayloadBytes = 1024
	}
	if c.PayloadBytes > MaxBenchmarkPayloadBytes {
		return fmt.Errorf("payload_bytes %d exceeds maximum %d", c.PayloadBytes, MaxBenchmarkPayloadBytes)
	}
	if int64(c.BatchSize)*int64(c.PayloadBytes) > MaxBatchPayloadBudgetBytes {
		return fmt.Errorf("batch payload budget (%d * %d = %d bytes) exceeds maximum %d bytes",
			c.BatchSize, c.PayloadBytes, int64(c.BatchSize)*int64(c.PayloadBytes), MaxBatchPayloadBudgetBytes)
	}
	if c.OffloadThresholdBytes < 0 {
		return fmt.Errorf("offload_threshold_bytes %d must be > 0", c.OffloadThresholdBytes)
	}
	if c.SimulateAckDelay < 0 {
		return fmt.Errorf("simulate_ack_delay %s must be >= 0", c.SimulateAckDelay)
	}
	if c.SimulateThrottleEvery < 0 {
		return fmt.Errorf("simulate_throttle_every %d must be >= 0", c.SimulateThrottleEvery)
	}
	if c.CallTimeout < 0 {
		return fmt.Errorf("call_timeout %s must be >= 0", c.CallTimeout)
	}
	if c.OffloadDir != "" && c.OffloadGCSBucket != "" {
		return errors.New("offload_dir and offload_gcs_bucket are mutually exclusive")
	}
	if c.Mode != ModeLive && strings.TrimSpace(c.OffloadGCSBucket) != "" {
		return errors.New("offload_gcs_bucket requires mode \"live\"")
	}
	if (c.OffloadDir != "" || c.OffloadGCSBucket != "") && c.OffloadThresholdBytes <= 0 {
		c.OffloadThresholdBytes = 4096
	}
	if c.Location == "" {
		c.Location = "US"
	}
	if c.EphemeralDataset && !c.ephemeralDatasetGenerated && strings.TrimSpace(c.DatasetID) != "" {
		return errors.New("dataset_id must not be set when ephemeral_dataset is true; a unique otel_bench_<timestamp>_<random_hex> dataset is generated automatically")
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
		if c.EphemeralDataset {
			if strings.TrimSpace(c.DatasetID) == "" {
				dsID, err := generateEphemeralDatasetID()
				if err != nil {
					return err
				}
				c.DatasetID = dsID
				c.ephemeralDatasetGenerated = true
			}
		} else if strings.TrimSpace(c.DatasetID) == "" {
			return errors.New("dataset_id is required when mode is \"live\" and ephemeral_dataset is false")
		}
	}
	return nil
}

func buildExporterConfig(cfg RunnerConfig, signal, fakeAddr string) (*bigqueryexporter.Config, error) {
	factory := bigqueryexporter.NewFactory()
	expCfg, ok := factory.CreateDefaultConfig().(*bigqueryexporter.Config)
	if !ok {
		return nil, errors.New("unexpected exporter config type")
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

	useLocalOffload := cfg.OffloadDir != "" && (signal == "traces" || signal == "logs")
	expCfg.Redaction.Enabled = cfg.Redaction && !useLocalOffload
	if expCfg.Redaction.Enabled {
		expCfg.Redaction.RedactLogBody = true
		expCfg.Redaction.BlockedValuePatterns = blockedSecretPatterns
	}
	if cfg.OffloadGCSBucket != "" {
		expCfg.Offload.Enabled = true
		expCfg.Offload.Bucket = cfg.OffloadGCSBucket
		if cfg.OffloadThresholdBytes > 0 {
			expCfg.Offload.MinBytes = cfg.OffloadThresholdBytes
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
		expCfg.Endpoint.URL = fakeAddr
		expCfg.Endpoint.Insecure = true
		expCfg.Endpoint.WithoutAuthentication = true
		expCfg.TimeoutSettings.Timeout = 10 * time.Second
		expCfg.BackOffConfig.InitialInterval = 10 * time.Millisecond
		expCfg.BackOffConfig.MaxInterval = 50 * time.Millisecond
	} else {
		expCfg.TimeoutSettings.Timeout = 30 * time.Second
		if cfg.EphemeralDataset {
			// Ephemeral datasets are explicitly created and ownership-verified
			// by createEphemeralDataset before the exporter starts, so the
			// exporter verifies dataset existence (AutoCreate.Dataset = false)
			// and auto-provisions tables + views inside the owned dataset.
			expCfg.AutoCreate.Dataset = false
			expCfg.AutoCreate.Tables = true
			expCfg.Quarantine.Enabled = true
		}
	}
	if cfg.CallTimeout > 0 {
		expCfg.TimeoutSettings.Timeout = cfg.CallTimeout
	}
	if err := expCfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating exporter config: %w", err)
	}
	return expCfg, nil
}

func validateRunConfig(ctx context.Context, cfg *RunnerConfig, poolSizes []int) error {
	if err := cfg.Normalize(); err != nil {
		return err
	}
	for _, ps := range poolSizes {
		if ps < 1 || ps > 16 {
			return fmt.Errorf("stream_pool_size %d must be between 1 and 16", ps)
		}
	}
	if cfg.OffloadDir != "" {
		if info, err := os.Stat(cfg.OffloadDir); err == nil && !info.IsDir() {
			return fmt.Errorf("offload_dir %q is not a directory", cfg.OffloadDir)
		}
		if err := os.MkdirAll(cfg.OffloadDir, 0o750); err != nil {
			return fmt.Errorf("validating offload_dir %q: %w", cfg.OffloadDir, err)
		}
	}
	checkPools := poolSizes
	if len(checkPools) == 0 {
		checkPools = []int{cfg.StreamPoolSize}
	}
	for _, ps := range checkPools {
		stepCfg := *cfg
		stepCfg.StreamPoolSize = ps
		for _, sig := range stepCfg.Signals {
			if _, err := buildExporterConfig(stepCfg, sig, "127.0.0.1:1"); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
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
	BackpressureLevel     float64          `json:"backpressure_level"`
	EffectiveRequestBytes int64            `json:"effective_request_bytes"`
	OffloadedPayloads     int64            `json:"offloaded_payloads"`
	OffloadedBytes        int64            `json:"offloaded_bytes"`
	OffloadDedupHits      int64            `json:"offload_dedup_hits"`
}

// SignalResult captures the benchmark measurements for one signal or combined run.
type SignalResult struct {
	Signal                    string            `json:"signal"`
	Mode                      string            `json:"mode"`
	StreamPoolSize            int               `json:"stream_pool_size"`
	ConnectionPoolSize        int               `json:"connection_pool_size"`
	Workers                   int               `json:"workers"`
	BatchSize                 int               `json:"batch_size"`
	PayloadBytes              int               `json:"payload_bytes"`
	Redaction                 bool              `json:"redaction"`
	Backpressure              bool              `json:"backpressure"`
	ElapsedSeconds            float64           `json:"elapsed_seconds"`
	BatchesAttempted          int64             `json:"batches_attempted"`
	BatchesSucceeded          int64             `json:"batches_succeeded"`
	BatchesFailed             int64             `json:"batches_failed"`
	RecordsAttempted          int64             `json:"records_attempted"`
	RecordsSubmitted          int64             `json:"records_submitted"`
	AchievedRowsPerSec        float64           `json:"achieved_rows_per_sec"`
	WireMBPerSec              float64           `json:"wire_mb_per_sec"`
	BytesPerRow               float64           `json:"bytes_per_row"`
	AttemptedBytesPerAckedRow float64           `json:"attempted_bytes_per_acked_row,omitempty"`
	Latency                   LatencySummary    `json:"latency"`
	Memory                    MemorySummary     `json:"memory"`
	Telemetry                 TelemetrySnapshot `json:"telemetry"`
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
func Run(ctx context.Context, cfg RunnerConfig) (report *BenchmarkReport, retErr error) {
	if err := validateRunConfig(ctx, &cfg, nil); err != nil {
		return nil, err
	}

	report = &BenchmarkReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
		Config:    cfg,
	}

	if cfg.Mode == ModeLive && cfg.EphemeralDataset && !cfg.KeepEphemeralDataset {
		if err := createEphemeralDataset(ctx, cfg.ProjectID, cfg.DatasetID, cfg.Location, cfg.ClientOptions...); err != nil {
			return nil, err
		}
		owned := true
		if owned {
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				if delErr := DeleteDatasetWithContents(cleanupCtx, cfg.ProjectID, cfg.DatasetID, cfg.ClientOptions...); delErr != nil {
					_, _ = fmt.Fprintf(os.Stderr, "bq-otel-testbench: failed to delete ephemeral dataset %s.%s: %v\n", cfg.ProjectID, cfg.DatasetID, delErr)
					retErr = errors.Join(retErr, delErr)
				} else if report != nil {
					report.EphemeralDatasetDeleted = true
				}
			}()
		}
	}

	var runErr error
	for _, sig := range cfg.Signals {
		res, firstConsumeErr, err := runSingleSignal(ctx, cfg, sig)
		if err != nil {
			return report, fmt.Errorf("benchmark signal %s: %w", sig, err)
		}
		report.Results = append(report.Results, res)
		if ctx.Err() != nil {
			return report, fmt.Errorf("benchmark signal %s interrupted before completion: %w", sig, ctx.Err())
		}
		if res.Latency.Samples == 0 || res.BatchesSucceeded == 0 || res.RecordsSubmitted == 0 || res.Telemetry.AcknowledgedRows == 0 {
			sigErr := fmt.Errorf("benchmark signal %s acknowledged 0 rows or completed 0 batches (samples=%d, batches_attempted=%d, batches_succeeded=%d, batches_failed=%d, submitted=%d, rejected=%d, acked=%d)",
				sig, res.Latency.Samples, res.BatchesAttempted, res.BatchesSucceeded, res.BatchesFailed, res.RecordsSubmitted, res.Telemetry.RejectedRows, res.Telemetry.AcknowledgedRows)
			if firstConsumeErr != nil {
				sigErr = errors.Join(sigErr, firstConsumeErr)
			}
			if runErr == nil {
				runErr = sigErr
			}
			continue
		}
		if res.BatchesFailed > 0 && !cfg.AllowPartialFailures {
			sigErr := fmt.Errorf("benchmark signal %s had %d failed batches out of %d attempted (succeeded=%d, acked=%d)",
				sig, res.BatchesFailed, res.BatchesAttempted, res.BatchesSucceeded, res.Telemetry.AcknowledgedRows)
			if firstConsumeErr != nil {
				sigErr = errors.Join(sigErr, firstConsumeErr)
			}
			if runErr == nil {
				runErr = sigErr
			}
		}
	}
	if runErr != nil {
		return report, runErr
	}
	return report, nil
}

// RunStreamPoolSweep executes cfg across each streamPoolSizes value (e.g. 1, 2, 4, 8, 16)
// and returns a combined BenchmarkReport. In live mode with EphemeralDataset, a single
// ephemeral dataset is created upfront, shared across the sweep, and deleted upon completion.
func RunStreamPoolSweep(ctx context.Context, cfg RunnerConfig, streamPoolSizes []int) (combined *BenchmarkReport, retErr error) {
	if len(streamPoolSizes) == 0 {
		streamPoolSizes = []int{1, 2, 4, 8, 16}
	}
	if err := validateRunConfig(ctx, &cfg, streamPoolSizes); err != nil {
		return nil, err
	}

	combined = &BenchmarkReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
		Config:    cfg,
	}

	if cfg.Mode == ModeLive && cfg.EphemeralDataset && !cfg.KeepEphemeralDataset {
		if err := createEphemeralDataset(ctx, cfg.ProjectID, cfg.DatasetID, cfg.Location, cfg.ClientOptions...); err != nil {
			return nil, err
		}
		owned := true
		if owned {
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				if delErr := DeleteDatasetWithContents(cleanupCtx, cfg.ProjectID, cfg.DatasetID, cfg.ClientOptions...); delErr != nil {
					_, _ = fmt.Fprintf(os.Stderr, "bq-otel-testbench: failed to delete ephemeral dataset %s.%s: %v\n", cfg.ProjectID, cfg.DatasetID, delErr)
					retErr = errors.Join(retErr, delErr)
				} else if combined != nil {
					combined.EphemeralDatasetDeleted = true
				}
			}()
		}
	}

	for _, poolSize := range streamPoolSizes {
		stepCfg := cfg
		stepCfg.StreamPoolSize = poolSize
		stepCfg.KeepEphemeralDataset = true
		stepReport, err := Run(ctx, stepCfg)
		if stepReport != nil {
			combined.Results = append(combined.Results, stepReport.Results...)
		}
		if err != nil {
			return combined, err
		}
	}
	return combined, nil
}

func newBigQueryClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*bigquery.Client, func(), error) {
	httpClient, endpoint, cleanup, err := googlehttp.NewClient(ctx, []string{bigquery.Scope}, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("creating bigquery HTTP client: %w", err)
	}
	bqOpts := []option.ClientOption{option.WithHTTPClient(httpClient)}
	if endpoint != "" {
		bqOpts = append(bqOpts, option.WithEndpoint(endpoint))
	}
	client, err := bigquery.NewClient(ctx, projectID, bqOpts...)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("creating bigquery client: %w", err)
	}
	closeAll := func() {
		_ = client.Close()
		cleanup()
	}
	return client, closeAll, nil
}

func datasetExists(ctx context.Context, projectID, datasetID string, opts ...option.ClientOption) (bool, error) {
	client, closeAll, err := newBigQueryClient(ctx, projectID, opts...)
	if err != nil {
		return false, err
	}
	defer closeAll()
	_, err = client.Dataset(datasetID).Metadata(ctx)
	if err == nil {
		return true, nil
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("checking whether dataset %s.%s exists: %w", projectID, datasetID, err)
}

func generateEphemeralOwnerToken() (string, error) {
	var randBytes [16]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		return "", fmt.Errorf("generating cryptographic ownership token for ephemeral dataset: %w", err)
	}
	return hex.EncodeToString(randBytes[:]), nil
}

func createEphemeralDataset(ctx context.Context, projectID, datasetID, location string, opts ...option.ClientOption) error {
	exists, err := datasetExists(ctx, projectID, datasetID, opts...)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("refusing to use existing dataset %s.%s with ephemeral_dataset=true", projectID, datasetID)
	}
	ownerToken, err := generateEphemeralOwnerToken()
	if err != nil {
		return err
	}
	client, closeAll, err := newBigQueryClient(ctx, projectID, opts...)
	if err != nil {
		return fmt.Errorf("creating bigquery client for ephemeral dataset provisioning: %w", err)
	}
	defer closeAll()

	createErr := client.Dataset(datasetID).Create(ctx, &bigquery.DatasetMetadata{
		Location:    location,
		Description: "bq-otel-testbench ephemeral dataset owner=" + ownerToken,
		Labels: map[string]string{
			ephemeralOwnerLabelKey: ownerToken,
		},
	})
	if createErr == nil {
		return nil
	}

	var apiErr *googleapi.Error
	if errors.As(createErr, &apiErr) && apiErr.Code == http.StatusConflict {
		return fmt.Errorf("creating ephemeral dataset %s.%s: %w", projectID, datasetID, createErr)
	}

	// A non-409 failure (e.g. HTTP 500, client timeout, or context cancellation)
	// may occur after the server already committed the dataset. Inspect the
	// dataset metadata on a bounded background context and delete it only if
	// the ownership label matches our unique per-call token.
	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelVerify()
	verifyClient, closeVerify, verifyClientErr := newBigQueryClient(verifyCtx, projectID, opts...)
	if verifyClientErr != nil {
		return fmt.Errorf("creating ephemeral dataset %s.%s (ambiguous create outcome; verify whether dataset %s.%s was left behind: %v): %w",
			projectID, datasetID, projectID, datasetID, verifyClientErr, createErr)
	}
	defer closeVerify()

	md, metaErr := verifyClient.Dataset(datasetID).Metadata(verifyCtx)
	if metaErr != nil {
		var metaAPIErr *googleapi.Error
		if errors.As(metaErr, &metaAPIErr) && metaAPIErr.Code == http.StatusNotFound {
			return fmt.Errorf("creating ephemeral dataset %s.%s: %w", projectID, datasetID, createErr)
		}
		return fmt.Errorf("creating ephemeral dataset %s.%s (ambiguous create outcome; verify whether dataset %s.%s was left behind: %v): %w",
			projectID, datasetID, projectID, datasetID, metaErr, createErr)
	}
	if md != nil && md.Labels[ephemeralOwnerLabelKey] == ownerToken {
		if delErr := verifyClient.Dataset(datasetID).DeleteWithContents(verifyCtx); delErr != nil {
			return errors.Join(
				fmt.Errorf("creating ephemeral dataset %s.%s (committed before error; cleanup failed for dataset %s.%s): %w",
					projectID, datasetID, projectID, datasetID, createErr),
				fmt.Errorf("deleting dataset %s.%s: %w", projectID, datasetID, delErr),
			)
		}
		return fmt.Errorf("creating ephemeral dataset %s.%s (committed dataset %s.%s was cleaned up after create error): %w",
			projectID, datasetID, projectID, datasetID, createErr)
	}
	return fmt.Errorf("creating ephemeral dataset %s.%s (dataset %s.%s exists without matching ownership token; refusing to delete): %w",
		projectID, datasetID, projectID, datasetID, createErr)
}

// DeleteDatasetWithContents removes a BigQuery dataset and all tables inside it.
func DeleteDatasetWithContents(ctx context.Context, projectID, datasetID string, opts ...option.ClientOption) error {
	client, closeAll, err := newBigQueryClient(ctx, projectID, opts...)
	if err != nil {
		return fmt.Errorf("creating bigquery client for cleanup: %w", err)
	}
	defer closeAll()
	if err := client.Dataset(datasetID).DeleteWithContents(ctx); err != nil {
		return fmt.Errorf("deleting dataset %s.%s: %w", projectID, datasetID, err)
	}
	return nil
}

func runSingleSignal(ctx context.Context, cfg RunnerConfig, signal string) (SignalResult, error, error) {
	var fakeSrv *bqfake.Server
	var fakeAddr string
	if cfg.Mode == ModeBQFake {
		var err error
		fakeSrv, err = bqfake.New()
		if err != nil {
			return SignalResult{}, nil, fmt.Errorf("starting bqfake server: %w", err)
		}
		defer func() { _ = fakeSrv.Close() }()
		fakeAddr = fakeSrv.Addr()
	}

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	expCfg, err := buildExporterConfig(cfg, signal, fakeAddr)
	if err != nil {
		return SignalResult{}, nil, err
	}

	// Optional local-directory content-addressed payload offloader and generator
	// are constructed BEFORE starting the exporter so any initialization error or
	// allocation panic cannot leak started exporter background goroutines.
	var (
		localOffloader      *offload.Offloader
		localRedactor       *redact.Redactor
		offloadPayloads     atomic.Int64
		offloadBytes        atomic.Int64
		offloadDedup        atomic.Int64
		localRedactedKeys   atomic.Int64
		localRedactedValues atomic.Int64
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
			return SignalResult{}, nil, fmt.Errorf("creating local offloader: %w", err)
		}
		if cfg.Redaction {
			localRedactor, err = redact.New(redact.Config{
				BlockedKeys:          redact.DefaultBlockedKeys(),
				BlockedValuePatterns: blockedSecretPatterns,
				MaskReplacement:      redact.DefaultMaskReplacement,
				RedactLogBody:        true,
			})
			if err != nil {
				return SignalResult{}, nil, fmt.Errorf("creating local redactor: %w", err)
			}
		}
	}

	gen := NewGenerator(GeneratorConfig{
		PayloadBytes:   cfg.PayloadBytes,
		PlantSecrets:   cfg.Redaction || cfg.PlantSecrets,
		UniquePayloads: cfg.UniquePayloads,
	})

	// Pre-build a pool of batches per worker when UniquePayloads is false so the
	// benchmark measures exporter pipeline (redaction, ProtoRows serialization,
	// stream pool routing, AIMD backpressure, gRPC Write API I/O) rather than
	// synthetic string building. When UniquePayloads is true, workers generate
	// fresh batches per dispatch so every record carries a distinct payload.
	const prebuiltCount = 8
	prebuiltTraces := make([]ptrace.Traces, prebuiltCount)
	prebuiltLogs := make([]plog.Logs, prebuiltCount)
	prebuiltMetrics := make([]pmetric.Metrics, prebuiltCount)
	if !cfg.UniquePayloads {
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
	}

	factory := bigqueryexporter.NewFactory()
	telSet := componenttest.NewNopTelemetrySettings()
	telSet.Logger = zap.NewNop()
	telSet.MeterProvider = mp
	set := exporter.Settings{
		ID:                component.NewID(component.MustNewType("bigquery")),
		TelemetrySettings: telSet,
	}

	var (
		tracesExp  exporter.Traces
		logsExp    exporter.Logs
		metricsExp exporter.Metrics
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
		return SignalResult{}, nil, fmt.Errorf("creating %s exporter: %w", signal, err)
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
		return SignalResult{}, nil, fmt.Errorf("starting %s exporter: %w", signal, err)
	}

	var (
		stopOnce sync.Once
		stopErr  error
	)
	shutdownExporter := func() error {
		stopOnce.Do(func() {
			stopErr = stopExp(activeComp)
		})
		return stopErr
	}
	defer func() {
		_ = shutdownExporter()
	}()

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	cpuBefore, _ := readProcessCPUAndRSS()

	var (
		batchesAttempted     atomic.Int64
		batchesSucceeded     atomic.Int64
		batchesFailed        atomic.Int64
		consumeBatchesFailed atomic.Int64
		recordsAttempted     atomic.Int64
		recordsSubmitted     atomic.Int64
		batchSeq             atomic.Int64
		latMu                sync.Mutex
		latenciesMs          = make([]float64, 0, 1024)
		errMu                sync.Mutex
		firstConsumeErr      error
		panicErr             error
	)

	targetTable := "otel_" + signal
	if signal == "traces" {
		targetTable = "otel_spans"
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
			defer func() {
				if r := recover(); r != nil {
					errMu.Lock()
					if panicErr == nil {
						panicErr = fmt.Errorf("worker %d panicked: %v", workerID, r)
					}
					errMu.Unlock()
					cancelRun()
				}
			}()
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
				if cfg.workerHook != nil {
					cfg.workerHook(workerID, seq)
				}
				if fakeSrv != nil {
					if cfg.SimulateThrottleEvery > 0 && seq%cfg.SimulateThrottleEvery == 0 {
						fakeSrv.EnqueueFault(bqfake.Fault{
							Mode:  bqfake.FaultResourceExhausted,
							Table: targetTable,
						})
					} else if cfg.SimulateAckDelay > 0 && (seq == 1 || seq%3 == 0) {
						fakeSrv.EnqueueFault(bqfake.Fault{
							Mode:  bqfake.FaultDelayedAck,
							Delay: cfg.SimulateAckDelay,
							Table: targetTable,
						})
					}
				}

				idx := (workerID + seq) % prebuiltCount
				t0 := time.Now()
				callCtx, cancelCall := context.WithTimeout(ctx, expCfg.TimeoutSettings.Timeout)
				var (
					consumeErr        error
					consumeDispatched bool
				)
				switch signal {
				case "traces":
					var td ptrace.Traces
					if cfg.UniquePayloads {
						td = gen.TracesBatch(cfg.BatchSize)
					} else {
						td = prebuiltTraces[idx]
					}
					if localOffloader != nil {
						cloned := td
						if !cfg.UniquePayloads {
							cloned = ptrace.NewTraces()
							prebuiltTraces[idx].CopyTo(cloned)
						}
						// Step-0 redaction runs before content-addressed offload so
						// cleartext secrets are never hashed or written to disk.
						if localRedactor != nil {
							keys, vals := localRedactor.RedactTraces(cloned)
							localRedactedKeys.Add(int64(keys))
							localRedactedValues.Add(int64(vals))
						}
						consumeErr = offloadTraceBatch(callCtx, localOffloader, cloned)
						td = cloned
					}
					if consumeErr == nil {
						consumeDispatched = true
						consumeErr = tracesExp.ConsumeTraces(callCtx, td)
					}
				case "logs":
					var ld plog.Logs
					if cfg.UniquePayloads {
						ld = gen.LogsBatch(cfg.BatchSize)
					} else {
						ld = prebuiltLogs[idx]
					}
					if localOffloader != nil {
						cloned := ld
						if !cfg.UniquePayloads {
							cloned = plog.NewLogs()
							prebuiltLogs[idx].CopyTo(cloned)
						}
						if localRedactor != nil {
							keys, vals := localRedactor.RedactLogs(cloned)
							localRedactedKeys.Add(int64(keys))
							localRedactedValues.Add(int64(vals))
						}
						consumeErr = offloadLogBatch(callCtx, localOffloader, cloned)
						ld = cloned
					}
					if consumeErr == nil {
						consumeDispatched = true
						consumeErr = logsExp.ConsumeLogs(callCtx, ld)
					}
				case "metrics":
					var md pmetric.Metrics
					if cfg.UniquePayloads {
						md = gen.MetricsBatch(cfg.BatchSize)
					} else {
						md = prebuiltMetrics[idx]
					}
					consumeDispatched = true
					consumeErr = metricsExp.ConsumeMetrics(callCtx, md)
				}
				cancelCall()
				elapsedMs := float64(time.Since(t0).Microseconds()) / 1000.0

				if consumeErr != nil {
					if ctx.Err() != nil && (errors.Is(consumeErr, context.DeadlineExceeded) || errors.Is(consumeErr, context.Canceled)) {
						return
					}
					batchesAttempted.Add(1)
					recordsAttempted.Add(int64(cfg.BatchSize))
					batchesFailed.Add(1)
					if consumeDispatched {
						consumeBatchesFailed.Add(1)
					}
					errMu.Lock()
					if firstConsumeErr == nil {
						firstConsumeErr = consumeErr
					}
					errMu.Unlock()
					continue
				}

				batchesAttempted.Add(1)
				recordsAttempted.Add(int64(cfg.BatchSize))
				batchesSucceeded.Add(1)
				recordsSubmitted.Add(int64(cfg.BatchSize))
				latMu.Lock()
				latenciesMs = append(latenciesMs, elapsedMs)
				latMu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	if panicErr != nil {
		_ = shutdownExporter()
		return SignalResult{}, firstConsumeErr, panicErr
	}
	elapsed := time.Since(startWall)
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}

	goroutinesDuring := runtime.NumGoroutine()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	cpuAfter, peakRSSMB := readProcessCPUAndRSS()

	if err := shutdownExporter(); err != nil {
		return SignalResult{}, firstConsumeErr, fmt.Errorf("shutting down %s exporter: %w", signal, err)
	}

	var rm metricdata.ResourceMetrics
	_ = reader.Collect(context.Background(), &rm)
	telem := extractTelemetrySnapshot(rm)
	if localOffloader != nil {
		telem.OffloadedPayloads += offloadPayloads.Load()
		telem.OffloadedBytes += offloadBytes.Load()
		telem.OffloadDedupHits += offloadDedup.Load()
		rk := localRedactedKeys.Load()
		rv := localRedactedValues.Load()
		if rk > 0 || rv > 0 {
			if telem.RedactedByMatchType == nil {
				telem.RedactedByMatchType = make(map[string]int64)
			}
			if rk > 0 {
				telem.RedactedValuesTotal += rk
				telem.RedactedByMatchType[redact.MatchKey] += rk
			}
			if rv > 0 {
				telem.RedactedValuesTotal += rv
				telem.RedactedByMatchType[redact.MatchValue] += rv
			}
		}
	}

	elapsedSec := elapsed.Seconds()
	ackedRows := telem.AcknowledgedRows
	rowsPerSec := float64(ackedRows) / elapsedSec
	wireMBPerSec := (float64(telem.AppendBytesTotal) / (1024.0 * 1024.0)) / elapsedSec
	var (
		bytesPerRow               float64
		attemptedBytesPerAckedRow float64
	)
	if ackedRows > 0 {
		attemptedBytesPerAckedRow = float64(telem.AppendBytesTotal) / float64(ackedRows)
		bytesPerRow = attemptedBytesPerAckedRow
		if fakeSrv != nil && fakeSrv.AckedBytes() > 0 {
			// In bqfake mode, fakeSrv.AckedBytes() sums the exact serialized
			// ProtoRows byte length of every acknowledged AppendRows chunk,
			// remaining invariant under whole-batch or uneven split-chunk retries.
			bytesPerRow = float64(fakeSrv.AckedBytes()) / float64(ackedRows)
		} else if telem.Retries > 0 || consumeBatchesFailed.Load() > 0 {
			if singleBPR, err := measureSingleAttemptBytesPerRow(cfg, signal); err == nil && singleBPR > 0 {
				bytesPerRow = singleBPR
			}
		}
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
		Signal:                    signal,
		Mode:                      cfg.Mode,
		StreamPoolSize:            cfg.StreamPoolSize,
		ConnectionPoolSize:        cfg.ConnectionPoolSize,
		Workers:                   cfg.Workers,
		BatchSize:                 cfg.BatchSize,
		PayloadBytes:              cfg.PayloadBytes,
		Redaction:                 cfg.Redaction,
		Backpressure:              cfg.Backpressure,
		ElapsedSeconds:            round2(elapsedSec),
		BatchesAttempted:          batchesAttempted.Load(),
		BatchesSucceeded:          batchesSucceeded.Load(),
		BatchesFailed:             batchesFailed.Load(),
		RecordsAttempted:          recordsAttempted.Load(),
		RecordsSubmitted:          recordsSubmitted.Load(),
		AchievedRowsPerSec:        round2(rowsPerSec),
		WireMBPerSec:              round2(wireMBPerSec),
		BytesPerRow:               round2(bytesPerRow),
		AttemptedBytesPerAckedRow: round2(attemptedBytesPerAckedRow),
		Latency:                   computeLatencySummary(latenciesMs),
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
	}, firstConsumeErr, nil
}

func measureSingleAttemptBytesPerRow(cfg RunnerConfig, signal string) (float64, error) {
	const refRecords = 28
	refGen := NewGenerator(GeneratorConfig{
		PayloadBytes:   cfg.PayloadBytes,
		PlantSecrets:   cfg.Redaction || cfg.PlantSecrets,
		UniquePayloads: cfg.UniquePayloads,
	})
	tOpts := transform.Options{Limits: transform.DefaultLimits()}
	if cfg.Redaction {
		rd, err := redact.New(redact.Config{
			BlockedKeys:          redact.DefaultBlockedKeys(),
			BlockedValuePatterns: blockedSecretPatterns,
			MaskReplacement:      redact.DefaultMaskReplacement,
			RedactLogBody:        true,
		})
		if err != nil {
			return 0, err
		}
		tOpts.Redactor = rd
	}
	var (
		rows    []transform.Row
		enc     *protoenc.Encoder
		encErr  error
		offload *offload.Offloader
	)
	if cfg.OffloadDir != "" && (signal == "traces" || signal == "logs") {
		offload, encErr = newMemReferenceOffloader(cfg.OffloadThresholdBytes, false)
		if encErr != nil {
			return 0, encErr
		}
	} else if cfg.OffloadGCSBucket != "" && (signal == "traces" || signal == "logs") {
		offload, encErr = newMemReferenceOffloader(cfg.OffloadThresholdBytes, true)
		if encErr != nil {
			return 0, encErr
		}
		tOpts.PayloadOffloader = offload
	}

	switch signal {
	case "traces":
		td := refGen.TracesBatch(refRecords)
		if tOpts.Redactor != nil {
			tOpts.Redactor.RedactTraces(td)
			tOpts.Redactor = nil
		}
		if offload != nil && tOpts.PayloadOffloader == nil {
			if err := offloadTraceBatch(context.Background(), offload, td); err != nil {
				return 0, err
			}
		}
		rows, _ = transform.SpanRows(td, tOpts)
		enc, encErr = protoenc.New(schema.SpansTableSchema())
	case "logs":
		ld := refGen.LogsBatch(refRecords)
		if tOpts.Redactor != nil {
			tOpts.Redactor.RedactLogs(ld)
			tOpts.Redactor = nil
		}
		if offload != nil && tOpts.PayloadOffloader == nil {
			if err := offloadLogBatch(context.Background(), offload, ld); err != nil {
				return 0, err
			}
		}
		rows, _ = transform.LogRows(ld, tOpts)
		enc, encErr = protoenc.New(schema.LogsTableSchema())
	case "metrics":
		md := refGen.MetricsBatch(refRecords)
		rows, _ = transform.MetricRows(md, tOpts)
		enc, encErr = protoenc.New(schema.MetricsTableSchema())
	default:
		return 0, fmt.Errorf("unsupported signal %q", signal)
	}
	if encErr != nil || len(rows) == 0 {
		return 0, encErr
	}
	var totalBytes int64
	var encodedCount int64
	for _, r := range rows {
		b, err := enc.Encode(r)
		if err != nil {
			continue
		}
		totalBytes += int64(len(b))
		encodedCount++
	}
	if encodedCount == 0 {
		return 0, nil
	}
	return float64(totalBytes) / float64(encodedCount), nil
}

type noopBlobStore struct {
	seq atomic.Int64
}

func (s *noopBlobStore) WriteObject(ctx context.Context, _, _, _ string, _ []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", s.seq.Add(1)), nil
}

func newMemReferenceOffloader(thresholdBytes int, _ bool) (*offload.Offloader, error) {
	if thresholdBytes <= 0 {
		thresholdBytes = 4096
	}
	return offload.New(&noopBlobStore{}, offload.Options{
		Bucket:         "local-offload-bench",
		Prefix:         "otel-offload",
		MinBytes:       thresholdBytes,
		MaxObjectBytes: 64 * 1024 * 1024,
		MaxConcurrency: 8,
		CacheSize:      10000,
		Timeout:        5 * time.Second,
	})
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

func offloadTraceBatch(ctx context.Context, o *offload.Offloader, td ptrace.Traces) error {
	ctx = transform.WithBatchUploadCache(ctx)
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		res := rss.At(i).Resource()
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			scope := sss.At(j).Scope()
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				if _, err := o.OffloadSpan(ctx, spans.At(k), res, scope); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func offloadLogBatch(ctx context.Context, o *offload.Offloader, ld plog.Logs) error {
	ctx = transform.WithBatchUploadCache(ctx)
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		res := rls.At(i).Resource()
		sls := rls.At(i).ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			scope := sls.At(j).Scope()
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				if _, err := o.OffloadLog(ctx, lrs.At(k), res, scope); err != nil {
					return err
				}
			}
		}
	}
	return nil
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
				switch g := m.Data.(type) {
				case metricdata.Gauge[float64]:
					for _, dp := range g.DataPoints {
						snap.BackpressureLevel = round2(dp.Value)
					}
				case metricdata.Gauge[int64]:
					for _, dp := range g.DataPoints {
						snap.BackpressureLevel = float64(dp.Value)
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
	sb.WriteString("| Signal | Stream Pool | Redaction | Backpressure | Samples | Achieved Rows/s | Encoded-Row MiB/s | Bytes/Row | p50 (ms) | p95 (ms) | p99 (ms) | CPU Cores / 10k rows/s | Heap Alloc Snapshot (MiB) | Process Peak RSS (MiB) | Acked Rows | Rejected Rows | Batches Failed | Redacted Values |\n")
	sb.WriteString("|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range report.Results {
		fmt.Fprintf(&sb, "| `%s` | %d | `%t` | `%t` | %d | %.1f | %.2f | %.0f | %.2f | %.2f | %.2f | %.2f | %.2f | %.1f | %d | %d | %d | %d |\n",
			r.Signal,
			r.StreamPoolSize,
			r.Redaction,
			r.Backpressure,
			r.Latency.Samples,
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
			r.Telemetry.RejectedRows,
			r.BatchesFailed,
			r.Telemetry.RedactedValuesTotal,
		)
	}
	return sb.String()
}
