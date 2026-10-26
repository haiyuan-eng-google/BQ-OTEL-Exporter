// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Command bq-quarantine-replay reads quarantined rows from the BigQuery
// otel_quarantine table, either exported to a file (JSONL or JSON array,
// -input) or straight from the table (-bq-project and -bq-dataset), filters
// them by signal, error_code, and time window, reconstructs OTLP traces,
// logs, and metrics, and writes replayed OTLP JSON envelopes to an output
// stream or file, or sends them to an OTLP/HTTP receiver (-otlp-endpoint).
// -rate-limit paces the replay, and -checkpoint-file records its progress
// so that a crashed run can be rerun from where it stopped (or, for legacy,
// empty-key, or boundary-group-mutated BigQuery cursors whose saved
// (canonical_row_key, group_count, prefix_sha256) no longer matches, replay the
// current (ingest_time, row_fp) group at least once while keeping earlier
// groups excluded and later groups proceeding normally).
//
// Exit status: 0 when every record was handled without error, 1 when the
// run failed or some records failed, and 2 for invalid flags.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/quarantinereplay"
)

// bigQueryClientOptions returns extra client options for the BigQuery
// source. Tests point it at a fake server.
var bigQueryClientOptions = func() []option.ClientOption { return nil }

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bq-quarantine-replay", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		inputPath      = fs.String("input", "-", "Path to quarantine JSONL or JSON array file ('-' for stdin)")
		outputPath     = fs.String("output", "-", "Path to write replayed OTLP JSONL ('-' for stdout)")
		signal         = fs.String("signal", "", "Optional comma-separated signal filter ('spans', 'logs', 'metrics')")
		errorCodes     = fs.String("error-code", "", "Optional comma-separated error_code filter (for example, 'OVERSIZE_ROW,ROW_ERROR')")
		sinceStr       = fs.String("since", "", "Optional inclusive lower bound on ingest_time (RFC3339)")
		untilStr       = fs.String("until", "", "Optional inclusive upper bound on ingest_time (RFC3339)")
		fetchGCS       = fs.Bool("fetch-gcs", false, "Fetch offloaded payload_ref objects from gs:// when raw_payload is empty")
		dryRun         = fs.Bool("dry-run", false, "Validate and count matching records without emitting output")
		bqProject      = fs.String("bq-project", "", "Read rows straight from BigQuery instead of -input: the project that holds -bq-dataset and runs the query job")
		bqDataset      = fs.String("bq-dataset", "", "BigQuery dataset that holds the quarantine table (with -bq-project)")
		bqTable        = fs.String("bq-table", quarantinereplay.DefaultQuarantineTable, "BigQuery quarantine table (with -bq-project and -bq-dataset)")
		otlpEndpoint   = fs.String("otlp-endpoint", "", "Send replayed batches to this OTLP/HTTP receiver instead of -output, for example 'http://localhost:4318'")
		rateLimit      = fs.Float64("rate-limit", 0, "Maximum records replayed per second (0 means no limit)")
		checkpointPath = fs.String("checkpoint-file", "", "File that records the run's progress; a rerun with the same file, source, and filters resumes after the last handled record (or replays the current (ingest_time, row_fp) BigQuery group at least once when a legacy, empty-key, or boundary-group-mutated cursor whose saved (canonical_row_key, group_count, prefix_sha256) no longer matches is rewound, while earlier groups stay excluded and later groups proceed normally)")
	)

	if err := fs.Parse(args); err != nil {
		return 2
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	usage := func(format string, a ...any) int {
		_, _ = fmt.Fprintf(stderr, format+"\n", a...)
		return 2
	}

	if strings.TrimSpace(*signal) != "" {
		if _, err := quarantinereplay.ParseSignals(*signal); err != nil {
			_, _ = fmt.Fprintf(stderr, "invalid -signal %q: %v\n", *signal, err)
			return 2
		}
	}
	filter := quarantinereplay.Filter{
		Signal: strings.TrimSpace(*signal),
	}
	if strings.TrimSpace(*errorCodes) != "" {
		for _, part := range strings.Split(*errorCodes, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				filter.ErrorCodes = append(filter.ErrorCodes, trimmed)
			}
		}
	}
	if *sinceStr != "" {
		t, err := time.Parse(time.RFC3339, *sinceStr)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "invalid -since %q: %v\n", *sinceStr, err)
			return 2
		}
		filter.Since = t
	}
	if *untilStr != "" {
		t, err := time.Parse(time.RFC3339, *untilStr)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "invalid -until %q: %v\n", *untilStr, err)
			return 2
		}
		filter.Until = t
	}

	bqMode := *bqProject != "" || *bqDataset != ""
	switch {
	case bqMode && (*bqProject == "" || *bqDataset == ""):
		return usage("-bq-project and -bq-dataset must be set together")
	case bqMode && set["input"]:
		return usage("-input cannot be combined with -bq-project and -bq-dataset: choose one source")
	case !bqMode && set["bq-table"]:
		return usage("-bq-table needs -bq-project and -bq-dataset")
	case *otlpEndpoint != "" && set["output"]:
		return usage("-output cannot be combined with -otlp-endpoint: choose one destination")
	case *otlpEndpoint != "" && *dryRun:
		return usage("-dry-run cannot be combined with -otlp-endpoint")
	case *checkpointPath != "" && *dryRun:
		return usage("-dry-run cannot be combined with -checkpoint-file: it would record records as handled without replaying them")
	case *rateLimit < 0 || math.IsNaN(*rateLimit) || math.IsInf(*rateLimit, 0):
		return usage("invalid -rate-limit %v: want a non-negative, finite number of records per second", *rateLimit)
	}
	src := quarantinereplay.BigQuerySource{Project: *bqProject, Dataset: *bqDataset, Table: *bqTable}
	if bqMode {
		if err := src.Validate(); err != nil {
			return usage("invalid BigQuery source: %v", err)
		}
	}

	opts := quarantinereplay.Options{Filter: filter}
	if *rateLimit > 0 {
		limiter, err := quarantinereplay.NewRateLimiter(*rateLimit)
		if err != nil {
			return usage("invalid -rate-limit: %v", err)
		}
		opts.RateLimiter = limiter
	}

	var otlpSink *quarantinereplay.OTLPHTTPSink
	if *otlpEndpoint != "" {
		s, err := quarantinereplay.NewOTLPHTTPSink(*otlpEndpoint, quarantinereplay.OTLPHTTPSinkOptions{})
		if err != nil {
			return usage("invalid -otlp-endpoint: %v", err)
		}
		defer s.Close()
		otlpSink = s
	}

	in := stdin
	source := "stdin"
	if !bqMode && *inputPath != "-" && *inputPath != "" {
		f, err := os.Open(*inputPath)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "opening input %q: %v\n", *inputPath, err)
			return 1
		}
		defer func() { _ = f.Close() }()
		in = f
		if abs, err := filepath.Abs(*inputPath); err == nil {
			source = "file:" + abs
		} else {
			source = "file:" + *inputPath
		}
	}
	if *checkpointPath != "" {
		opts.Checkpoint = &quarantinereplay.CheckpointConfig{Path: *checkpointPath}
		if !bqMode {
			// ReplayBigQuery names its source after the table itself.
			opts.Checkpoint.Source = source
		}
	}

	var (
		sink    quarantinereplay.Sink
		outFile *os.File
	)
	switch {
	case *dryRun:
		sink = quarantinereplay.NewNopSink()
	case otlpSink != nil:
		sink = otlpSink
	default:
		out := stdout
		if *outputPath != "-" && *outputPath != "" {
			flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
			if opts.Checkpoint != nil {
				// A resumed run adds to what the earlier runs wrote.
				flags = os.O_WRONLY | os.O_CREATE | os.O_APPEND
			}
			f, err := os.OpenFile(*outputPath, flags, 0o666)
			if err != nil {
				_, _ = fmt.Fprintf(stderr, "creating output %q: %v\n", *outputPath, err)
				return 1
			}
			outFile = f
			out = f
		}
		sink = quarantinereplay.NewJSONWriterSink(out)
	}

	ctx := context.Background()
	if *fetchGCS {
		fetcher, cleanup, err := quarantinereplay.NewGCSObjectFetcher(ctx)
		if err != nil {
			if outFile != nil {
				_ = outFile.Close()
			}
			_, _ = fmt.Fprintf(stderr, "initializing GCS object fetcher: %v\n", err)
			return 1
		}
		defer cleanup()
		opts.ObjectFetcher = fetcher
	}

	var (
		sum quarantinereplay.Summary
		err error
	)
	if bqMode {
		sum, err = quarantinereplay.ReplayBigQuery(ctx, src, sink, opts, bigQueryClientOptions()...)
	} else {
		sum, err = quarantinereplay.Replay(ctx, in, sink, opts)
	}
	if outFile != nil {
		if cerr := outFile.Close(); cerr != nil && err == nil {
			_, _ = fmt.Fprintf(stderr, "closing output %q: %v\n", *outputPath, cerr)
			return 1
		}
	}
	_, _ = fmt.Fprintf(
		stderr,
		"quarantine replay summary: total_read=%d matched=%d replayed_spans=%d replayed_logs=%d replayed_metrics=%d skipped_filtered=%d skipped_no_payload=%d failed_unmarshal=%d failed_export=%d skipped_checkpointed=%d\n",
		sum.TotalRead, sum.Matched, sum.ReplayedSpans, sum.ReplayedLogs, sum.ReplayedMetrics,
		sum.SkippedFiltered, sum.SkippedNoPayload, sum.FailedUnmarshal, sum.FailedExport, sum.SkippedCheckpointed,
	)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "quarantine replay completed with errors: %v\n", err)
		return 1
	}
	return 0
}
