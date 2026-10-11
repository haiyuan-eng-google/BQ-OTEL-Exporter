// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Command bq-quarantine-replay reads quarantined rows exported from the
// BigQuery otel_quarantine table (JSONL or JSON array), filters them by
// signal, error_code, and time window, reconstructs OTLP traces and logs, and
// writes replayed OTLP JSON envelopes to an output stream or file.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/quarantinereplay"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bq-quarantine-replay", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		inputPath  = fs.String("input", "-", "Path to quarantine JSONL or JSON array file ('-' for stdin)")
		outputPath = fs.String("output", "-", "Path to write replayed OTLP JSONL ('-' for stdout)")
		signal     = fs.String("signal", "", "Optional signal filter ('spans' or 'logs')")
		errorCodes = fs.String("error-code", "", "Optional comma-separated error_code filter (for example, 'OVERSIZE_ROW,ROW_ERROR')")
		sinceStr   = fs.String("since", "", "Optional inclusive lower bound on ingest_time (RFC3339)")
		untilStr   = fs.String("until", "", "Optional inclusive upper bound on ingest_time (RFC3339)")
		fetchGCS   = fs.Bool("fetch-gcs", false, "Fetch offloaded payload_ref objects from gs:// when raw_payload is empty")
		dryRun     = fs.Bool("dry-run", false, "Validate and count matching records without emitting output")
	)

	if err := fs.Parse(args); err != nil {
		return 2
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

	in := stdin
	if *inputPath != "-" && *inputPath != "" {
		f, err := os.Open(*inputPath)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "opening input %q: %v\n", *inputPath, err)
			return 1
		}
		defer func() { _ = f.Close() }()
		in = f
	}

	var sink quarantinereplay.Sink
	if *dryRun {
		sink = quarantinereplay.NewNopSink()
	} else {
		out := stdout
		if *outputPath != "-" && *outputPath != "" {
			f, err := os.Create(*outputPath)
			if err != nil {
				_, _ = fmt.Fprintf(stderr, "creating output %q: %v\n", *outputPath, err)
				return 1
			}
			defer func() { _ = f.Close() }()
			out = f
		}
		sink = quarantinereplay.NewJSONWriterSink(out)
	}

	ctx := context.Background()
	opts := quarantinereplay.Options{Filter: filter}
	if *fetchGCS {
		fetcher, cleanup, err := quarantinereplay.NewGCSObjectFetcher(ctx)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "initializing GCS object fetcher: %v\n", err)
			return 1
		}
		defer cleanup()
		opts.ObjectFetcher = fetcher
	}

	sum, err := quarantinereplay.Replay(ctx, in, sink, opts)
	_, _ = fmt.Fprintf(
		stderr,
		"quarantine replay summary: total_read=%d matched=%d replayed_spans=%d replayed_logs=%d skipped_filtered=%d skipped_no_payload=%d failed_unmarshal=%d failed_export=%d\n",
		sum.TotalRead, sum.Matched, sum.ReplayedSpans, sum.ReplayedLogs,
		sum.SkippedFiltered, sum.SkippedNoPayload, sum.FailedUnmarshal, sum.FailedExport,
	)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "quarantine replay completed with errors: %v\n", err)
		return 1
	}
	return 0
}
