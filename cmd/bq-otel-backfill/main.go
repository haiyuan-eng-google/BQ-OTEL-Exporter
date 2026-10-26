// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Command bq-otel-backfill appends historical OTLP JSON archives held in
// Cloud Storage to the exporter's BigQuery tables.
//
//	bq-otel-backfill -source-gcs-prefix=gs://BUCKET/PREFIX/ -project=PROJECT -dataset=DATASET
//
// It reads every object under the prefix whose name ends in .jsonl or
// .jsonl.gz, one OTLP/JSON export request per line, which is what the
// Collector's file exporter writes. Records go through the exporter's own
// pipeline: redaction first, then the row transform with the default
// structural limits, then the Storage Write API default stream. Rows keep
// the times the telemetry carries, so they land in the historical
// partitions those times select.
//
// Before a backfill, run bq-otel-validate against the dataset (the tables
// must already exist) and check each table's partition expiration: rows
// older than it fall in partitions that have already expired, and BigQuery
// does not keep them. Pass the deployment's redaction and log identity
// settings so backfilled rows match exported ones. Delivery is
// at-least-once and running a backfill again appends its rows again; the
// _dedup views collapse the copies.
//
// The identity running the command needs storage.objects.list and
// storage.objects.get on the bucket, and bigquery.tables.updateData on the
// tables.
//
// Exit status: 0 when every object was read to the end and every row that
// could be built was appended, 1 when there were failures or the run could
// not start or stopped early, and 2 for invalid flags.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/backfill"
)

// storageClientOptions and writeClientOptions return extra options for the
// Cloud Storage client and the Storage Write API client. Tests point them
// at fake servers.
var (
	storageClientOptions = func() []option.ClientOption { return nil }
	writeClientOptions   = func() []option.ClientOption { return nil }
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// patterns collects a repeated flag.
type patterns []string

func (p *patterns) String() string { return strings.Join(*p, ",") }

func (p *patterns) Set(v string) error {
	*p = append(*p, v)
	return nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bq-otel-backfill", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var valuePatterns patterns
	var (
		source                   = fs.String("source-gcs-prefix", "", "gs://bucket/prefix whose .jsonl and .jsonl.gz objects are appended; end it with / to select one folder (required)")
		project                  = fs.String("project", "", "BigQuery project that holds -dataset (required)")
		dataset                  = fs.String("dataset", "", "BigQuery dataset the exporter writes to (required)")
		signalName               = fs.String("signal", backfill.SignalAuto, "Signal to append: traces, logs, metrics, or auto for every signal found")
		batchSize                = fs.Int("batch-size", backfill.DefaultBatchSize, "Rows per append")
		maxWorkers               = fs.Int("max-workers", backfill.DefaultMaxWorkers, "Objects read and appended at once")
		tracesTable              = fs.String("traces-table", backfill.DefaultTracesTable, "Span table (the exporter's traces.table)")
		logsTable                = fs.String("logs-table", backfill.DefaultLogsTable, "Log table (the exporter's logs.table)")
		metricsTable             = fs.String("metrics-table", backfill.DefaultMetricsTable, "Metric table (the exporter's metrics.table)")
		redactEnabled            = fs.Bool("redact", false, "Enable client-side redaction (the exporter's redaction.enabled); when omitted, enabled implicitly if any other -redact-* flag is set")
		redactKeys               = fs.String("redact-keys", "", "Comma-separated attribute keys masked in addition to the defaults (the exporter's redaction.blocked_keys)")
		redactDisableDefaultKeys = fs.Bool("redact-disable-default-keys", false, "Disable the built-in blocked key list so only -redact-keys apply (the exporter's redaction.disable_default_keys)")
		redactMaskReplacement    = fs.String("redact-mask-replacement", "", "Replacement text for redacted values (the exporter's redaction.mask_replacement; defaults to [REDACTED])")
		redactLogBody            = fs.Bool("redact-log-body", false, "Apply the value patterns to log bodies (the exporter's redaction.redact_log_body)")
		logNameAttribute         = fs.String("log-name-attribute", "", "The exporter's logs.log_name_attribute")
		insertIDAttribute        = fs.String("insert-id-attribute", "", "The exporter's logs.insert_id_attribute")
		maxAttempts              = fs.Int("max-attempts", backfill.DefaultMaxAttempts, "Attempts at one append or one Cloud Storage request before the object fails")
		maxLineBytes             = fs.Int("max-line-bytes", backfill.DefaultMaxLineBytes, "Longest line read; a longer line is reported and skipped")
		format                   = fs.String("format", "text", "Report format: text or json")
		progress                 = fs.Bool("progress", true, "Print a line to stderr as each object finishes")
	)
	fs.Var(&valuePatterns, "redact-value-pattern", "RE2 expression whose matches are masked inside string values (the exporter's redaction.blocked_value_patterns); repeat for more")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	usage := func(format string, a ...any) int {
		_, _ = fmt.Fprintf(stderr, "bq-otel-backfill: "+format+"\n", a...)
		return 2
	}
	if fs.NArg() > 0 {
		return usage("unexpected arguments %q", fs.Args())
	}
	if *source == "" || *project == "" || *dataset == "" {
		return usage("-source-gcs-prefix, -project, and -dataset are required")
	}
	if *format != "text" && *format != "json" {
		return usage("invalid -format %q: want text or json", *format)
	}
	for _, n := range []struct {
		name  string
		value int
	}{
		{"-batch-size", *batchSize}, {"-max-workers", *maxWorkers}, {"-max-attempts", *maxAttempts}, {"-max-line-bytes", *maxLineBytes},
	} {
		// Zero would select the library default, which the flag already shows.
		if n.value <= 0 {
			return usage("invalid %s %d: want a positive number", n.name, n.value)
		}
	}
	var keys []string
	for _, k := range strings.Split(*redactKeys, ",") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	visited := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})
	redactOn := *redactEnabled
	if !visited["redact"] {
		redactOn = len(keys) > 0 ||
			*redactDisableDefaultKeys ||
			len(valuePatterns) > 0 ||
			*redactMaskReplacement != "" ||
			*redactLogBody
	}
	cfg := backfill.Config{
		Source:               *source,
		Project:              *project,
		Dataset:              *dataset,
		Signal:               *signalName,
		TracesTable:          *tracesTable,
		LogsTable:            *logsTable,
		MetricsTable:         *metricsTable,
		BatchSize:            *batchSize,
		MaxWorkers:           *maxWorkers,
		Redact:               redactOn,
		BlockedKeys:          keys,
		DisableDefaultKeys:   *redactDisableDefaultKeys,
		BlockedValuePatterns: valuePatterns,
		MaskReplacement:      *redactMaskReplacement,
		RedactLogBody:        *redactLogBody,
		LogNameAttribute:     *logNameAttribute,
		InsertIDAttribute:    *insertIDAttribute,
		MaxAttempts:          *maxAttempts,
		MaxLineBytes:         *maxLineBytes,
		StorageClientOptions: storageClientOptions(),
		WriteClientOptions:   writeClientOptions(),
	}
	if err := cfg.Validate(); err != nil {
		return usage("%v", err)
	}
	if *progress {
		cfg.OnObject = func(r backfill.ObjectResult) {
			status := "done"
			switch {
			case r.Err != nil:
				status = "stopped: " + r.Err.Error()
			case r.Failures > 0:
				status = count(int64(r.Failures), "failure")
			}
			_, _ = fmt.Fprintf(stderr, "bq-otel-backfill: %s: %s, %s appended, %s\n",
				r.Object, count(r.Lines, "line"), count(r.Appended, "row"), status)
		}
	}

	report, err := backfill.Run(ctx, cfg)
	if report == nil {
		// The run could not start, so there is nothing to report.
		_, _ = fmt.Fprintf(stderr, "bq-otel-backfill: %v\n", err)
		return 1
	}
	write := report.WriteText
	if *format == "json" {
		write = report.WriteJSON
	}
	if werr := write(stdout); werr != nil {
		_, _ = fmt.Fprintf(stderr, "bq-otel-backfill: writing the report: %v\n", werr)
		return 1
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "bq-otel-backfill: the run did not finish: %v\n", err)
		return 1
	}
	if !report.Complete() {
		return 1
	}
	return 0
}

// count formats n with noun, made plural unless n is 1.
func count(n int64, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
