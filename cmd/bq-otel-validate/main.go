// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Command bq-otel-validate checks, before production traffic is routed to
// an exporter deployment, that the BigQuery dataset it writes to is ready:
// the span, log, metric, and quarantine tables match the current schema
// contract, the 11 companion views compile in a dry run with a partition
// filter (which finds require_partition_filter conflicts), the identity
// running the command has bigquery.datasets.get, bigquery.tables.get, and
// bigquery.tables.updateData, and the Storage Write API gRPC endpoint is
// reachable. Every failed check comes with a remediation. The command never
// writes to BigQuery: the view checks are dry runs, which process no data.
//
// The permission checks test the identity running the command, so run it as
// the collector's identity, for example after
//
//	gcloud auth application-default login --impersonate-service-account=SA_EMAIL
//
// Exit status: 0 when the dataset conforms (warnings allowed), 1 when a check
// failed or the run could not finish, and 2 for invalid flags.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/validate"
)

// bigQueryClientOptions and writeClientOptions return extra options for the
// BigQuery REST client and the Storage Write API client. Tests point them at
// fake servers.
var (
	bigQueryClientOptions = func() []option.ClientOption { return nil }
	writeClientOptions    = func() []option.ClientOption { return nil }
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bq-otel-validate", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		project         = fs.String("project", "", "BigQuery project that holds -dataset; the view dry runs run as query jobs in it (required)")
		dataset         = fs.String("dataset", "", "BigQuery dataset the exporter writes to (required)")
		signals         = fs.String("signals", "traces,logs,metrics", "Comma-separated signals whose tables and companion views are checked: traces, logs, metrics")
		quarantine      = fs.Bool("quarantine", true, "Check the quarantine table (-quarantine=false when the deployment does not enable quarantine)")
		tracesTable     = fs.String("traces-table", validate.DefaultTracesTable, "Span table (the exporter's traces.table)")
		logsTable       = fs.String("logs-table", validate.DefaultLogsTable, "Log table (the exporter's logs.table)")
		metricsTable    = fs.String("metrics-table", validate.DefaultMetricsTable, "Metric table (the exporter's metrics.table)")
		quarantineTable = fs.String("quarantine-table", validate.DefaultQuarantineTable, "Quarantine table (the exporter's quarantine.table)")
		checks          = fs.String("checks", "schema,views,iam,storage-write", "Comma-separated checks to run: schema, views, iam, storage-write")
		format          = fs.String("format", "text", "Report format: text or json")
		timeout         = fs.Duration("timeout", 5*time.Minute, "Time limit for the whole run")
		probeTimeout    = fs.Duration("probe-timeout", validate.DefaultProbeTimeout, "Time limit for the Storage Write API reachability probe")
	)

	if err := fs.Parse(args); err != nil {
		return 2
	}
	usage := func(format string, a ...any) int {
		_, _ = fmt.Fprintf(stderr, "bq-otel-validate: "+format+"\n", a...)
		return 2
	}
	if fs.NArg() > 0 {
		return usage("unexpected arguments %q", fs.Args())
	}
	if *project == "" || *dataset == "" {
		return usage("-project and -dataset are required")
	}
	sigs, err := validate.ParseSignals(*signals)
	if err != nil {
		return usage("invalid -signals: %v", err)
	}
	cats, err := validate.ParseCategories(*checks)
	if err != nil {
		return usage("invalid -checks: %v", err)
	}
	if *format != "text" && *format != "json" {
		return usage("invalid -format %q: want text or json", *format)
	}
	if *timeout <= 0 {
		return usage("invalid -timeout %v: want a positive duration", *timeout)
	}
	if *probeTimeout <= 0 {
		return usage("invalid -probe-timeout %v: want a positive duration", *probeTimeout)
	}
	cfg := validate.Config{
		Project:            *project,
		Dataset:            *dataset,
		Signals:            sigs,
		SkipQuarantine:     !*quarantine,
		TracesTable:        *tracesTable,
		LogsTable:          *logsTable,
		MetricsTable:       *metricsTable,
		QuarantineTable:    *quarantineTable,
		Checks:             cats,
		ClientOptions:      bigQueryClientOptions(),
		WriteClientOptions: writeClientOptions(),
		ProbeTimeout:       *probeTimeout,
	}
	if err := cfg.Validate(); err != nil {
		return usage("%v", err)
	}

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	report, err := validate.Run(ctx, cfg)
	if report != nil {
		write := report.WriteText
		if *format == "json" {
			write = report.WriteJSON
		}
		if werr := write(stdout); werr != nil {
			_, _ = fmt.Fprintf(stderr, "bq-otel-validate: writing the report: %v\n", werr)
			return 1
		}
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "bq-otel-validate: the run did not finish: %v\n", err)
		return 1
	}
	if !report.Conforms {
		return 1
	}
	return 0
}
