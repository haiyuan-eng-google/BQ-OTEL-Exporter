// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Command bq-otel-testbench runs synthetic OpenTelemetry GenAI and MCP load
// benchmarks against bigqueryexporter using either the in-process bqfake
// Storage Write API server or a live BigQuery dataset.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testbench"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "bq-otel-testbench: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("bq-otel-testbench", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		cfg        testbench.RunnerConfig
		signalsCSV string
		sweepCSV   string
		format     string
		jsonOut    string
	)

	fs.StringVar(&cfg.Mode, "mode", testbench.ModeBQFake, "Sink mode: bqfake or live")
	fs.StringVar(&signalsCSV, "signals", "traces", "Comma-separated OTLP signals to benchmark (traces,logs,metrics)")
	fs.IntVar(&cfg.TargetRate, "rate", 0, "Target records/sec per signal (0 = unthrottled max throughput)")
	fs.DurationVar(&cfg.Duration, "duration", 2*time.Second, "Benchmark duration per signal")
	fs.IntVar(&cfg.BatchSize, "batch-size", 500, "Records per batch")
	fs.IntVar(&cfg.Workers, "workers", 4, "Concurrent producer workers")
	fs.IntVar(&cfg.StreamPoolSize, "stream-pool-size", 4, "BigQuery Write API stream_pool_size (1..16)")
	fs.StringVar(&sweepCSV, "stream-pool-sweep", "", "Optional comma-separated stream_pool_size sweep (e.g. 1,2,4,8,16)")
	fs.IntVar(&cfg.ConnectionPoolSize, "connection-pool-size", 4, "BigQuery Write API gRPC connection_pool_size (1..16)")
	fs.IntVar(&cfg.MaxInflightRequests, "max-inflight-requests", 32, "Maximum in-flight AppendRows requests per stream")
	fs.IntVar(&cfg.MaxRequestBytes, "max-request-bytes", 8*1024*1024, "Maximum serialized ProtoRows payload bytes per request")
	fs.StringVar(&cfg.Compression, "compression", "", "Optional gRPC compressor (empty or gzip)")
	fs.BoolVar(&cfg.Redaction, "redaction", false, "Enable Step-0 sensitive-data redaction and plant synthetic secrets")
	fs.BoolVar(&cfg.PlantSecrets, "plant-secrets", false, "Plant synthetic secret attributes even when redaction is disabled")
	fs.BoolVar(&cfg.Backpressure, "backpressure", false, "Enable AIMD adaptive write backpressure")
	fs.IntVar(&cfg.PayloadBytes, "payload-bytes", 1024, "Synthetic prompt/response/tool payload byte length (e.g. 1024..524288)")
	fs.BoolVar(&cfg.UniquePayloads, "unique-payloads", false, "Generate unique sequence prefixes per payload (disables dedup hits)")
	fs.StringVar(&cfg.OffloadDir, "offload-dir", "", "Optional local directory sink to benchmark SHA-256 content-addressed offload")
	fs.IntVar(&cfg.OffloadThresholdBytes, "offload-threshold-bytes", 4096, "Byte threshold when --offload-dir is enabled")
	fs.DurationVar(&cfg.SimulateAckDelay, "simulate-ack-delay", 0, "Inject FaultDelayedAck into bqfake on every 3rd batch")
	fs.IntVar(&cfg.SimulateThrottleEvery, "simulate-throttle-every", 0, "Inject FaultResourceExhausted into bqfake every N batches")
	fs.StringVar(&cfg.ProjectID, "project", "", "GCP project ID (required when --mode=live)")
	fs.StringVar(&cfg.DatasetID, "dataset", "", "BigQuery dataset ID (auto-generated when --mode=live and --ephemeral-dataset)")
	fs.StringVar(&cfg.Location, "location", "US", "BigQuery dataset location")
	fs.BoolVar(&cfg.EphemeralDataset, "ephemeral-dataset", false, "Auto-create and delete the live BigQuery dataset on teardown")
	fs.StringVar(&format, "format", "both", "Output format: markdown, json, or both")
	fs.StringVar(&jsonOut, "json-out", "", "Optional file path to write JSON benchmark report")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}

	cfg.Signals = splitNonEmptyCSV(signalsCSV)
	format = strings.ToLower(strings.TrimSpace(format))
	switch format {
	case "markdown", "json", "both":
	default:
		return fmt.Errorf("unsupported --format %q: must be markdown, json, or both", format)
	}

	var (
		report *testbench.BenchmarkReport
		err    error
	)
	if strings.TrimSpace(sweepCSV) != "" {
		poolSizes, parseErr := parseIntCSV(sweepCSV)
		if parseErr != nil {
			return fmt.Errorf("parsing --stream-pool-sweep: %w", parseErr)
		}
		report, err = testbench.RunStreamPoolSweep(ctx, cfg, poolSizes)
	} else {
		report, err = testbench.Run(ctx, cfg)
	}
	if err != nil {
		return err
	}

	jsonStr, err := testbench.FormatJSON(report)
	if err != nil {
		return err
	}

	if jsonOut != "" {
		if err := os.WriteFile(jsonOut, []byte(jsonStr), 0o600); err != nil {
			return fmt.Errorf("writing --json-out %q: %w", jsonOut, err)
		}
	}

	switch format {
	case "markdown":
		_, err = io.WriteString(stdout, testbench.FormatMarkdown(report))
	case "json":
		_, err = io.WriteString(stdout, jsonStr)
	case "both":
		if _, err = io.WriteString(stdout, testbench.FormatMarkdown(report)+"\n"); err == nil {
			_, err = io.WriteString(stdout, jsonStr)
		}
	}
	return err
}

func splitNonEmptyCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func parseIntCSV(raw string) ([]int, error) {
	parts := splitNonEmptyCSV(raw)
	if len(parts) == 0 {
		return nil, errors.New("empty integer list")
	}
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.Atoi(p)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("invalid positive integer %q", p)
		}
		out = append(out, v)
	}
	return out, nil
}
