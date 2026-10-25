// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package validate runs the preflight checks of the bq-otel-validate command
// against the BigQuery dataset an exporter deployment writes to, before
// production traffic is routed to it:
//
//   - schema: the dataset exists, and the span, log, metric, and quarantine
//     tables can take rows of the current schema contract;
//   - views: a dry run of each of the 11 companion views, with a filter on the
//     column that prunes its partitions, compiles, so a view that BigQuery
//     rejects because a table it reads requires a partition filter is found;
//   - iam: the identity running the checks has bigquery.datasets.get on the
//     dataset and bigquery.tables.get and bigquery.tables.updateData on each
//     table, plus bigquery.jobs.create on the project when the view dry runs
//     run;
//   - storage_write: the Storage Write API gRPC endpoint answers a
//     GetWriteStream call for a table's _default stream.
//
// Every failed check carries a remediation. Dry runs process no data, and no
// check writes to BigQuery.
package validate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/auth/credentials"
	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// Default table names, matching the exporter's traces.table, logs.table,
// metrics.table, and quarantine.table defaults.
const (
	DefaultTracesTable     = "otel_spans"
	DefaultLogsTable       = "otel_logs"
	DefaultMetricsTable    = "otel_metrics"
	DefaultQuarantineTable = "otel_quarantine"
)

// DefaultProbeTimeout bounds the Storage Write API reachability probe when
// Config.ProbeTimeout is zero.
const DefaultProbeTimeout = 30 * time.Second

// Signal names a telemetry signal whose table and companion views are
// checked.
type Signal string

// The signals Config.Signals accepts.
const (
	SignalTraces  Signal = "traces"
	SignalLogs    Signal = "logs"
	SignalMetrics Signal = "metrics"
)

var allSignals = []Signal{SignalTraces, SignalLogs, SignalMetrics}

// Category groups related checks.
type Category string

// The check categories, in the order a Report lists them.
const (
	CategorySchema       Category = "schema"
	CategoryViews        Category = "views"
	CategoryIAM          Category = "iam"
	CategoryStorageWrite Category = "storage_write"
)

var allCategories = []Category{CategorySchema, CategoryViews, CategoryIAM, CategoryStorageWrite}

// Status is the outcome of one check. Only StatusFail makes a report
// non-conforming.
type Status string

// The check outcomes.
const (
	StatusPass Status = "pass"
	StatusWarn Status = "warn"
	StatusFail Status = "fail"
	StatusSkip Status = "skip"
)

const (
	permDatasetsGet      = "bigquery.datasets.get"
	permTablesGet        = "bigquery.tables.get"
	permTablesUpdateData = "bigquery.tables.updateData"
	permJobsCreate       = "bigquery.jobs.create"
)

var (
	// projectIDPattern accepts project IDs, including domain-scoped ones
	// such as "example.com:my-project".
	projectIDPattern = regexp.MustCompile(`^([a-z0-9.-]+:)?[a-z0-9][a-z0-9-]*$`)
	datasetIDPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	// tableIDPattern is BigQuery's table name character set: letters,
	// marks, numbers, connectors (including "_"), dashes, and spaces.
	tableIDPattern = regexp.MustCompile(`^[\p{L}\p{M}\p{N}\p{Pc}\p{Pd}\p{Zs}]+$`)
)

// Config selects what Run checks and how it reaches BigQuery.
type Config struct {
	// Project holds the dataset. The view dry runs run as query jobs in it.
	Project string
	// Dataset is the dataset the exporter writes to.
	Dataset string
	// Signals lists the signals whose tables and companion views are
	// checked. Empty means all three.
	Signals []Signal
	// SkipQuarantine leaves the quarantine table out, for deployments that
	// do not enable quarantine.
	SkipQuarantine bool
	// TracesTable, LogsTable, MetricsTable, and QuarantineTable name the
	// tables. Empty means the exporter's default name.
	TracesTable     string
	LogsTable       string
	MetricsTable    string
	QuarantineTable string
	// Checks lists the categories to run. Empty means all of them.
	Checks []Category
	// ClientOptions configure the BigQuery REST client. With none, it uses
	// Application Default Credentials.
	ClientOptions []option.ClientOption
	// WriteClientOptions configure the Storage Write API client. Nil means
	// Application Default Credentials and the default endpoint.
	WriteClientOptions []option.ClientOption
	// ProbeTimeout bounds the Storage Write API probe. Zero means
	// DefaultProbeTimeout.
	ProbeTimeout time.Duration
}

// Check is the outcome of one preflight check.
type Check struct {
	Category Category `json:"category"`
	// Name identifies the check within its category, such as "logs table"
	// or "v_gen_ai_spans".
	Name string `json:"name"`
	// Resource is the project, dataset, table, or view the check is about.
	Resource string `json:"resource"`
	Status   Status `json:"status"`
	Detail   string `json:"detail"`
	// Remediation says how to fix a failed check, or what a warning means.
	Remediation string `json:"remediation,omitempty"`
}

// Summary counts the checks of a report by status.
type Summary struct {
	Pass int `json:"pass"`
	Warn int `json:"warn"`
	Fail int `json:"fail"`
	Skip int `json:"skip"`
}

// Report is the result of Run.
type Report struct {
	Project       string `json:"project"`
	Dataset       string `json:"dataset"`
	SchemaVersion string `json:"schema_version"`
	// Conforms is true when no check failed.
	Conforms bool    `json:"conforms"`
	Summary  Summary `json:"summary"`
	Checks   []Check `json:"checks"`
}

// ParseSignals parses a comma-separated list of signals.
func ParseSignals(s string) ([]Signal, error) {
	var out []Signal
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		sig := Signal(part)
		if !slices.Contains(allSignals, sig) {
			return nil, fmt.Errorf("unknown signal %q: want traces, logs, or metrics", part)
		}
		if !slices.Contains(out, sig) {
			out = append(out, sig)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no signal given")
	}
	return out, nil
}

// ParseCategories parses a comma-separated list of check categories.
// "storage-write" is accepted for CategoryStorageWrite.
func ParseCategories(s string) ([]Category, error) {
	var out []Category
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		cat := Category(strings.ReplaceAll(part, "-", "_"))
		if !slices.Contains(allCategories, cat) {
			return nil, fmt.Errorf("unknown check %q: want schema, views, iam, or storage-write", part)
		}
		if !slices.Contains(out, cat) {
			out = append(out, cat)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no check given")
	}
	return out, nil
}

// tableSpec is one table Run checks, with its metadata once read.
type tableSpec struct {
	label  string // "traces table"
	signal Signal // empty for the quarantine table
	id     string
	want   *storagepb.TableSchema
	ddl    string // the internal/schema constants that create it
	md     *bq.TableMetadata
	err    error
}

// companionView describes how to dry-run one companion view.
type companionView struct {
	name string
	// filterColumn is the view column whose filter reaches the partitioned
	// tables the view reads (README, "Filtering the GenAI views by time").
	filterColumn string
	// filterIsDate is true when filterColumn is a DATE, not a TIMESTAMP.
	filterIsDate bool
	// unprunable marks the views that join spans on a time window that
	// depends on each row. BigQuery cannot use it to skip span partitions,
	// so it rejects every query over them while the spans table requires a
	// partition filter, and the exporter then skips them unless
	// auto_create.allow_unprunable_views is set.
	unprunable bool
	// signals lists the signals whose tables the view reads.
	signals []Signal
}

// companionViews lists the 11 companion views in the order the exporter
// creates them.
var companionViews = []companionView{
	{name: schema.SpansAsAllSpansViewName, filterColumn: "start_time", signals: []Signal{SignalTraces}},
	{name: schema.GenAISpansViewName, filterColumn: "start_time", signals: []Signal{SignalTraces}},
	{name: schema.GenAIModelCostsViewName, filterColumn: "usage_hour", signals: []Signal{SignalTraces}},
	{name: schema.GenAISessionsViewName, filterColumn: "session_start_date", filterIsDate: true, signals: []Signal{SignalTraces}},
	{name: schema.GenAIToolUsageViewName, filterColumn: "usage_hour", signals: []Signal{SignalTraces}},
	{name: schema.GenAIAgentTrajectoryViewName, filterColumn: "start_time", unprunable: true, signals: []Signal{SignalTraces}},
	{name: schema.MCPSpansViewName, filterColumn: "start_time", signals: []Signal{SignalTraces}},
	{name: schema.MCPToolUsageViewName, filterColumn: "usage_hour", signals: []Signal{SignalTraces}},
	{name: schema.GenAILogsViewName, filterColumn: "timestamp", unprunable: true, signals: []Signal{SignalLogs, SignalTraces}},
	{name: schema.GenAIMetricsViewName, filterColumn: "timestamp", signals: []Signal{SignalMetrics}},
	{name: schema.GenAIMetricExemplarsViewName, filterColumn: "metric_timestamp", unprunable: true, signals: []Signal{SignalMetrics, SignalTraces}},
}

// dryRunSQL is the query a view's dry run compiles: every column of the view,
// with a filter on filterColumn that BigQuery can use for partition
// elimination, so that a table that requires a partition filter accepts it
// whenever the view passes the filter down.
func dryRunSQL(project, dataset string, v companionView) string {
	bound := "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)"
	if v.filterIsDate {
		bound = "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"
	}
	return fmt.Sprintf("SELECT * FROM `%s.%s.%s` WHERE `%s` >= %s", project, dataset, v.name, v.filterColumn, bound)
}

// jobsCreateProbeSQL is the dry run that shows whether the identity may run
// query jobs in the project at all.
const jobsCreateProbeSQL = "SELECT 1"

// Run checks the dataset cfg names and reports every check's outcome. It
// returns an error, and no report, when cfg is invalid or the BigQuery client
// cannot be created. When ctx ends early it returns the checks made so far
// together with ctx's error.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	hc, endpoint, cleanup, err := googlehttp.NewClient(ctx, []string{bigqueryapi.BigqueryScope}, cfg.ClientOptions...)
	if err != nil {
		return nil, fmt.Errorf("creating BigQuery HTTP client: %w", err)
	}
	defer cleanup()
	opts := []option.ClientOption{option.WithHTTPClient(hc)}
	if endpoint != "" {
		opts = append(opts, option.WithEndpoint(endpoint))
	}
	client, err := bq.NewClient(ctx, cfg.Project, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating BigQuery client: %w", err)
	}
	defer func() { _ = client.Close() }()

	v := &validator{cfg: cfg, client: client, checks: map[Category][]Check{}}
	v.tables = v.tableSpecs()
	v.run(ctx)
	return v.report(), ctx.Err()
}

// Validate reports, without contacting BigQuery, the error Run returns for a
// configuration it cannot run.
func (c Config) Validate() error {
	return c.normalize()
}

func (c *Config) normalize() error {
	if !projectIDPattern.MatchString(c.Project) {
		return fmt.Errorf("invalid BigQuery project ID %q", c.Project)
	}
	if !datasetIDPattern.MatchString(c.Dataset) {
		return fmt.Errorf("invalid BigQuery dataset ID %q", c.Dataset)
	}
	for _, t := range []struct {
		name *string
		def  string
	}{
		{&c.TracesTable, DefaultTracesTable},
		{&c.LogsTable, DefaultLogsTable},
		{&c.MetricsTable, DefaultMetricsTable},
		{&c.QuarantineTable, DefaultQuarantineTable},
	} {
		if *t.name == "" {
			*t.name = t.def
		}
		if !tableIDPattern.MatchString(*t.name) {
			return fmt.Errorf("invalid BigQuery table ID %q", *t.name)
		}
	}
	if len(c.Signals) == 0 {
		c.Signals = slices.Clone(allSignals)
	}
	for _, s := range c.Signals {
		if !slices.Contains(allSignals, s) {
			return fmt.Errorf("unknown signal %q", s)
		}
	}
	if len(c.Checks) == 0 {
		c.Checks = slices.Clone(allCategories)
	}
	for _, cat := range c.Checks {
		if !slices.Contains(allCategories, cat) {
			return fmt.Errorf("unknown check category %q", cat)
		}
	}
	if c.ProbeTimeout < 0 {
		return fmt.Errorf("probe timeout must not be negative, got %v", c.ProbeTimeout)
	}
	if c.ProbeTimeout == 0 {
		c.ProbeTimeout = DefaultProbeTimeout
	}
	return nil
}

type validator struct {
	cfg    Config
	client *bq.Client
	tables []*tableSpec
	checks map[Category][]Check
}

func (v *validator) tableSpecs() []*tableSpec {
	var out []*tableSpec
	if v.signal(SignalTraces) {
		out = append(out, &tableSpec{label: "traces table", signal: SignalTraces, id: v.cfg.TracesTable,
			want: schema.SpansTableSchema(), ddl: "SpansDDL and SpansDedupViewDDL"})
	}
	if v.signal(SignalLogs) {
		out = append(out, &tableSpec{label: "logs table", signal: SignalLogs, id: v.cfg.LogsTable,
			want: schema.LogsTableSchema(), ddl: "LogsDDL and LogsDedupViewDDL"})
	}
	if v.signal(SignalMetrics) {
		out = append(out, &tableSpec{label: "metrics table", signal: SignalMetrics, id: v.cfg.MetricsTable,
			want: schema.MetricsTableSchema(), ddl: "MetricsDDL and MetricsDedupViewDDL"})
	}
	if !v.cfg.SkipQuarantine {
		out = append(out, &tableSpec{label: "quarantine table", id: v.cfg.QuarantineTable,
			want: schema.QuarantineTableSchema(), ddl: "QuarantineDDL"})
	}
	return out
}

func (v *validator) signal(s Signal) bool { return slices.Contains(v.cfg.Signals, s) }

func (v *validator) enabled(c Category) bool { return slices.Contains(v.cfg.Checks, c) }

func (v *validator) qualify(id string) string {
	return v.cfg.Project + "." + v.cfg.Dataset + "." + id
}

func (v *validator) add(c Check) { v.checks[c.Category] = append(v.checks[c.Category], c) }

func (v *validator) run(ctx context.Context) {
	if v.checkDataset(ctx) {
		if v.enabled(CategorySchema) || v.enabled(CategoryViews) || v.enabled(CategoryIAM) {
			for _, t := range v.tables {
				t.md, t.err = v.client.Dataset(v.cfg.Dataset).Table(t.id).Metadata(ctx)
			}
		}
		if v.enabled(CategorySchema) {
			for _, t := range v.tables {
				v.checkTableSchema(t)
			}
		}
		if v.enabled(CategoryViews) {
			v.checkViews(ctx)
		}
		if v.enabled(CategoryIAM) {
			for _, t := range v.tables {
				v.checkTablePermissions(ctx, t)
			}
		}
	}
	if v.enabled(CategoryStorageWrite) {
		v.checkStorageWrite(ctx)
	}
}

// checkDataset reads the dataset's metadata and reports whether the table,
// view, and permission checks can go on. A missing dataset always fails the
// report, whichever categories run.
func (v *validator) checkDataset(ctx context.Context) bool {
	id := v.cfg.Project + "." + v.cfg.Dataset
	_, err := v.client.Dataset(v.cfg.Dataset).Metadata(ctx)
	e := describe(err)
	switch {
	case err == nil:
		if v.enabled(CategorySchema) {
			v.add(Check{Category: CategorySchema, Name: "dataset", Resource: id, Status: StatusPass, Detail: "dataset exists"})
		}
		if v.enabled(CategoryIAM) {
			v.add(Check{Category: CategoryIAM, Name: permDatasetsGet, Resource: id, Status: StatusPass,
				Detail: "dataset metadata is readable"})
		}
		return true
	case e.code == http.StatusNotFound:
		v.add(Check{Category: CategorySchema, Name: "dataset", Resource: id, Status: StatusFail,
			Detail:      "dataset not found: " + e.message,
			Remediation: v.createDatasetRemediation()})
		return false
	case e.code == http.StatusForbidden:
		if v.enabled(CategoryIAM) {
			v.add(Check{Category: CategoryIAM, Name: permDatasetsGet, Resource: id, Status: StatusFail,
				Detail:      "permission denied reading dataset metadata: " + e.message,
				Remediation: v.iamRemediation([]string{permDatasetsGet})})
		} else {
			v.add(Check{Category: CategorySchema, Name: "dataset", Resource: id, Status: StatusFail,
				Detail:      "permission denied reading dataset metadata: " + e.message,
				Remediation: v.iamRemediation([]string{permDatasetsGet})})
		}
		// Table-level grants may still allow the table checks.
		return true
	default:
		v.add(Check{Category: CategorySchema, Name: "dataset", Resource: id, Status: StatusFail,
			Detail: "could not read dataset metadata: " + e.message})
		return true
	}
}

func (v *validator) checkTableSchema(t *tableSpec) {
	id := v.qualify(t.id)
	add := func(status Status, detail, remediation string) {
		v.add(Check{Category: CategorySchema, Name: t.label, Resource: id, Status: status, Detail: detail, Remediation: remediation})
	}
	if t.err != nil {
		e := describe(t.err)
		switch e.code {
		case http.StatusNotFound:
			add(StatusFail, "table not found", v.createTableRemediation(t))
		case http.StatusForbidden:
			add(StatusFail, "permission denied reading table metadata: "+e.message,
				v.iamRemediation([]string{permTablesGet}))
		default:
			add(StatusFail, "could not read table metadata: "+e.message, "")
		}
		return
	}
	if t.md.Type != bq.RegularTable {
		add(StatusFail, fmt.Sprintf("%s is a %s, not a table", id, t.md.Type),
			fmt.Sprintf("The exporter appends rows to a regular table. Point the exporter at a table created from the current DDL (internal/schema %s).", t.ddl))
		return
	}
	have, err := schema.FromBigQuery(t.md.Schema)
	if err != nil {
		add(StatusFail, "unreadable table schema: "+err.Error(), v.migrateRemediation(t))
		return
	}
	if diffs := schema.CompatibilityDifferences(t.want, have); len(diffs) > 0 {
		add(StatusFail, fmt.Sprintf("does not match schema %s: %s", schema.Version, summarize(diffs, 10)),
			v.migrateRemediation(t))
		return
	}
	detail := "matches schema " + schema.Version
	if requiresPartitionFilter(t.md) {
		detail += "; queries must filter on its partitioning column (require_partition_filter)"
	}
	add(StatusPass, detail, "")
}

// summarize joins at most limit items and counts the rest.
func summarize(items []string, limit int) string {
	if len(items) <= limit {
		return strings.Join(items, "; ")
	}
	return fmt.Sprintf("%s; and %d more", strings.Join(items[:limit], "; "), len(items)-limit)
}

// requiresPartitionFilter reports whether a table's metadata, in either of
// the two places BigQuery reports it, requires a partition filter on
// queries.
func requiresPartitionFilter(md *bq.TableMetadata) bool {
	return md != nil && (md.RequirePartitionFilter ||
		(md.TimePartitioning != nil && md.TimePartitioning.RequirePartitionFilter))
}

func (v *validator) table(s Signal) *tableSpec {
	for _, t := range v.tables {
		if t.signal == s && s != "" {
			return t
		}
	}
	return nil
}

func (v *validator) checkViews(ctx context.Context) {
	var applicable []companionView
	for _, cv := range companionViews {
		if missing := v.missingSignals(cv.signals); len(missing) > 0 {
			v.add(Check{Category: CategoryViews, Name: cv.name, Resource: v.qualify(cv.name), Status: StatusSkip,
				Detail: fmt.Sprintf("not checked: the view reads the %s tables, and this run leaves out %s",
					joinSignals(cv.signals), joinSignals(missing))})
			continue
		}
		applicable = append(applicable, cv)
	}
	if len(applicable) == 0 {
		return
	}
	if !v.checkJobsCreate(ctx) {
		for _, cv := range applicable {
			v.add(Check{Category: CategoryViews, Name: cv.name, Resource: v.qualify(cv.name), Status: StatusSkip,
				Detail: "not checked: dry runs need " + permJobsCreate + " (see the iam checks)"})
		}
		return
	}
	spansRequireFilter := false
	if t := v.table(SignalTraces); t != nil && t.err == nil {
		spansRequireFilter = requiresPartitionFilter(t.md)
	}
	for _, cv := range applicable {
		v.checkView(ctx, cv, spansRequireFilter)
	}
}

func (v *validator) missingSignals(need []Signal) []Signal {
	var missing []Signal
	for _, s := range need {
		if !v.signal(s) {
			missing = append(missing, s)
		}
	}
	return missing
}

func joinSignals(s []Signal) string {
	parts := make([]string, len(s))
	for i, sig := range s {
		parts[i] = string(sig)
	}
	return strings.Join(parts, " and ")
}

// checkJobsCreate dry-runs a trivial query, which needs only
// bigquery.jobs.create, and reports whether the view dry runs can run.
func (v *validator) checkJobsCreate(ctx context.Context) bool {
	id := v.cfg.Project
	_, err := v.dryRun(ctx, jobsCreateProbeSQL)
	if err == nil {
		v.add(Check{Category: CategoryIAM, Name: permJobsCreate, Resource: id, Status: StatusPass,
			Detail: "dry-run query jobs are accepted"})
		return true
	}
	e := describe(err)
	if e.code == http.StatusForbidden {
		v.add(Check{Category: CategoryIAM, Name: permJobsCreate, Resource: id, Status: StatusFail,
			Detail:      "dry-run query jobs are denied: " + e.message,
			Remediation: v.jobsCreateRemediation()})
		return false
	}
	// Anything else does not show the permission is missing; the view dry
	// runs report what they find.
	v.add(Check{Category: CategoryIAM, Name: permJobsCreate, Resource: id, Status: StatusFail,
		Detail: "could not run a dry-run query job: " + e.message})
	return true
}

func (v *validator) checkView(ctx context.Context, cv companionView, spansRequireFilter bool) {
	id := v.qualify(cv.name)
	add := func(status Status, detail, remediation string) {
		v.add(Check{Category: CategoryViews, Name: cv.name, Resource: id, Status: status, Detail: detail, Remediation: remediation})
	}
	md, err := v.client.Dataset(v.cfg.Dataset).Table(cv.name).Metadata(ctx)
	if err != nil {
		e := describe(err)
		switch {
		case e.code == http.StatusNotFound && cv.unprunable && spansRequireFilter:
			add(StatusWarn, fmt.Sprintf("not created, as expected while %s requires a partition filter",
				v.qualify(v.cfg.TracesTable)), v.unprunableSkippedRemediation())
			return
		case e.code == http.StatusNotFound:
			add(StatusFail, "view not found", createViewsRemediation)
			return
		case e.code == http.StatusForbidden:
			// The dry run still shows whether the view can be queried.
		default:
			add(StatusFail, "could not read view metadata: "+e.message, "")
			return
		}
	} else if md.Type != bq.ViewTable {
		add(StatusFail, fmt.Sprintf("%s is a %s, not a view", id, md.Type),
			"Drop it and let the exporter (auto_create.companion_views) or deploy/terraform create the view.")
		return
	}

	bytes, err := v.dryRun(ctx, dryRunSQL(v.cfg.Project, v.cfg.Dataset, cv))
	if err == nil {
		add(StatusPass, fmt.Sprintf("dry run with a filter on %s compiles (estimated %d bytes)", cv.filterColumn, bytes), "")
		return
	}
	e := describe(err)
	switch {
	case isPartitionFilterError(e) && cv.unprunable:
		add(StatusFail, "require_partition_filter conflict: "+e.message, v.unprunableConflictRemediation(cv.name))
	case isPartitionFilterError(e):
		add(StatusFail, fmt.Sprintf("require_partition_filter conflict: a filter on %s does not reach the partitioned table: %s",
			cv.filterColumn, e.message), recreateViewRemediation)
	case e.code == http.StatusForbidden:
		add(StatusFail, "dry run denied: "+e.message, dryRunAccessRemediation)
	case e.code == http.StatusNotFound:
		add(StatusFail, "the view reads a table or view that does not exist: "+e.message, recreateViewRemediation)
	case e.code == http.StatusBadRequest:
		add(StatusFail, "the view does not compile: "+e.message, recreateViewRemediation)
	default:
		add(StatusFail, "dry run failed: "+e.message, "")
	}
}

// dryRun compiles sql as a dry-run query job, which processes no data, and
// returns BigQuery's estimate of the bytes a real run would process.
func (v *validator) dryRun(ctx context.Context, sql string) (int64, error) {
	q := v.client.Query(sql)
	q.DryRun = true
	job, err := q.Run(ctx)
	if err != nil {
		return 0, err
	}
	st := job.LastStatus()
	if st == nil {
		return 0, nil
	}
	if err := st.Err(); err != nil {
		return 0, err
	}
	if st.Statistics == nil {
		return 0, nil
	}
	return st.Statistics.TotalBytesProcessed, nil
}

func (v *validator) checkTablePermissions(ctx context.Context, t *tableSpec) {
	id := v.qualify(t.id)
	name := t.label + " permissions"
	if t.err != nil && describe(t.err).code == http.StatusNotFound {
		v.add(Check{Category: CategoryIAM, Name: name, Resource: id, Status: StatusSkip, Detail: "not checked: table not found"})
		return
	}
	want := []string{permTablesGet, permTablesUpdateData}
	granted, err := v.client.Dataset(v.cfg.Dataset).Table(t.id).IAM().TestPermissions(ctx, want)
	if err != nil {
		e := describe(err)
		if e.code == http.StatusNotFound {
			v.add(Check{Category: CategoryIAM, Name: name, Resource: id, Status: StatusSkip, Detail: "not checked: table not found"})
			return
		}
		v.add(Check{Category: CategoryIAM, Name: name, Resource: id, Status: StatusFail,
			Detail: "could not test permissions: " + e.message})
		return
	}
	var missing []string
	for _, p := range want {
		if !slices.Contains(granted, p) {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		v.add(Check{Category: CategoryIAM, Name: name, Resource: id, Status: StatusFail,
			Detail:      "missing " + strings.Join(missing, " and "),
			Remediation: v.iamRemediation(missing)})
		return
	}
	v.add(Check{Category: CategoryIAM, Name: name, Resource: id, Status: StatusPass,
		Detail: "has " + strings.Join(want, " and ")})
}

// checkStorageWrite calls GetWriteStream on the _default stream of the first
// table that exists (or of the first table, when none does, because a
// NotFound answer still shows the endpoint is reachable).
func (v *validator) checkStorageWrite(ctx context.Context) {
	var probe *tableSpec
	for _, t := range v.tables {
		if t.md != nil && t.err == nil {
			probe = t
			break
		}
	}
	if probe == nil {
		probe = v.tables[0]
	}
	streamName := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/_default", v.cfg.Project, v.cfg.Dataset, probe.id)
	add := func(status Status, detail, remediation string) {
		v.add(Check{Category: CategoryStorageWrite, Name: "default stream", Resource: v.qualify(probe.id),
			Status: status, Detail: detail, Remediation: remediation})
	}

	opts := v.cfg.WriteClientOptions
	if opts == nil {
		creds, cleanup, err := googlehttp.Credentials(credentials.DetectOptions{
			Scopes:           []string{bigqueryapi.BigqueryInsertdataScope},
			UseSelfSignedJWT: true,
		}, nil)
		if err != nil {
			add(StatusFail, "could not load credentials: "+err.Error(), unauthenticatedRemediation)
			return
		}
		defer cleanup()
		opts = []option.ClientOption{option.WithAuthCredentials(creds)}
	}
	// One connection is enough for one call; later options win.
	opts = append(slices.Clone(opts), option.WithGRPCConnectionPool(1))

	probeCtx, cancel := context.WithTimeout(ctx, v.cfg.ProbeTimeout)
	defer cancel()
	client, err := managedwriter.NewClient(probeCtx, v.cfg.Project, opts...)
	if err != nil {
		add(StatusFail, "could not create the Storage Write API client: "+err.Error(), unreachableRemediation(v.cfg.Project))
		return
	}
	defer func() { _ = client.Close() }()
	ws, err := client.GetWriteStream(probeCtx, &storagepb.GetWriteStreamRequest{
		Name: streamName,
		View: storagepb.WriteStreamView_BASIC,
	})
	switch code := status.Code(err); {
	case err == nil:
		add(StatusPass, fmt.Sprintf("GetWriteStream answered for %s (type %s, location %s)",
			streamName, ws.GetType(), ws.GetLocation()), "")
	case code == codes.NotFound:
		add(StatusPass, "endpoint reachable; the table does not exist yet (see the schema checks): "+status.Convert(err).Message(), "")
	case code == codes.PermissionDenied:
		add(StatusFail, "endpoint reachable, but GetWriteStream was denied: "+status.Convert(err).Message(),
			v.iamRemediation([]string{permTablesUpdateData}))
	case code == codes.Unauthenticated:
		add(StatusFail, "credentials were rejected: "+status.Convert(err).Message(), unauthenticatedRemediation)
	default:
		add(StatusFail, "Storage Write API endpoint unreachable: "+err.Error(), unreachableRemediation(v.cfg.Project))
	}
}

func (v *validator) report() *Report {
	r := &Report{
		Project:       v.cfg.Project,
		Dataset:       v.cfg.Dataset,
		SchemaVersion: schema.Version,
		Checks:        []Check{},
	}
	for _, cat := range allCategories {
		r.Checks = append(r.Checks, v.checks[cat]...)
	}
	for _, c := range r.Checks {
		switch c.Status {
		case StatusPass:
			r.Summary.Pass++
		case StatusWarn:
			r.Summary.Warn++
		case StatusFail:
			r.Summary.Fail++
		case StatusSkip:
			r.Summary.Skip++
		}
	}
	r.Conforms = r.Summary.Fail == 0
	return r
}

// WriteJSON writes r as indented JSON.
func (r *Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteText writes r for people: one line per check, each failed check
// followed by its remediation, and a closing verdict.
func (r *Report) WriteText(w io.Writer) error {
	var b strings.Builder
	fmt.Fprintf(&b, "bq-otel-validate: project %s, dataset %s, schema %s\n", r.Project, r.Dataset, r.SchemaVersion)
	b.WriteString("Permissions are those of the identity running this command; run it as the collector's identity.\n\n")
	for _, c := range r.Checks {
		fmt.Fprintf(&b, "%-4s  %-13s  %s (%s): %s\n", strings.ToUpper(string(c.Status)), c.Category, c.Name, c.Resource, c.Detail)
		if c.Remediation != "" {
			fmt.Fprintf(&b, "      fix: %s\n", c.Remediation)
		}
	}
	verdict := "CONFORMANT"
	if !r.Conforms {
		verdict = "NOT CONFORMANT"
	}
	fmt.Fprintf(&b, "\n%s: %d passed, %d warnings, %d failed, %d skipped\n",
		verdict, r.Summary.Pass, r.Summary.Warn, r.Summary.Fail, r.Summary.Skip)
	_, err := io.WriteString(w, b.String())
	return err
}

// apiError is what Run needs to know about an error from a BigQuery call.
type apiError struct {
	code    int // HTTP status, or 0 when unknown
	message string
}

func describe(err error) apiError {
	if err == nil {
		return apiError{}
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		e := apiError{code: gerr.Code, message: gerr.Message}
		if e.message == "" && len(gerr.Errors) > 0 {
			e.message = gerr.Errors[0].Message
		}
		if e.message == "" {
			e.message = err.Error()
		}
		return e
	}
	var jobErr *bq.Error
	if errors.As(err, &jobErr) {
		e := apiError{message: jobErr.Message}
		switch jobErr.Reason {
		case "notFound":
			e.code = http.StatusNotFound
		case "accessDenied":
			e.code = http.StatusForbidden
		case "invalid", "invalidQuery":
			e.code = http.StatusBadRequest
		}
		if e.message == "" {
			e.message = err.Error()
		}
		return e
	}
	return apiError{message: err.Error()}
}

// isPartitionFilterError reports whether BigQuery rejected a query because a
// table it reads requires a partition filter the query does not give it:
// "Cannot query over table '...' without a filter over column(s) '...' that
// can be used for partition elimination".
func isPartitionFilterError(e apiError) bool {
	msg := strings.ToLower(e.message)
	return strings.Contains(msg, "partition elimination") || strings.Contains(msg, "without a filter over column")
}

func (v *validator) iamRemediation(missing []string) string {
	return fmt.Sprintf("Grant the collector's identity %s on dataset %s.%s: either the least-privilege custom role with "+
		"%s, %s, and %s (deploy/terraform grants it as otelCollectorWriter; to create it yourself: "+
		"gcloud iam roles create otelCollectorWriter --project=%s --permissions=%s,%s,%s), "+
		"or the broader predefined role roles/bigquery.dataEditor. Permissions are probed for the identity running "+
		"this command, so run it as the collector's identity.",
		strings.Join(missing, " and "), v.cfg.Project, v.cfg.Dataset,
		permDatasetsGet, permTablesGet, permTablesUpdateData,
		v.cfg.Project, permDatasetsGet, permTablesGet, permTablesUpdateData)
}

func (v *validator) jobsCreateRemediation() string {
	return fmt.Sprintf("The companion view dry runs run as query jobs in project %s, so the identity running this "+
		"command needs %s there (for example roles/bigquery.jobUser). The collector needs it only for "+
		"auto_create.search_indexes. Rerun as an identity that has it, or leave the view checks out "+
		"(--checks=schema,iam,storage-write).", v.cfg.Project, permJobsCreate)
}

func (v *validator) createDatasetRemediation() string {
	return fmt.Sprintf("Create the dataset: apply deploy/terraform, run bq --location=LOCATION mk --dataset %s:%s, "+
		"or start the exporter with auto_create.dataset: true and location set.", v.cfg.Project, v.cfg.Dataset)
}

func (v *validator) createTableRemediation(t *tableSpec) string {
	return fmt.Sprintf("Create the table before routing traffic: apply deploy/terraform, run the DDL in internal/schema "+
		"(%s), or start the exporter with auto_create.tables: true (needs bigquery.tables.create). %s",
		t.ddl, "Leave a table this deployment does not use out of the run (--signals, --quarantine=false).")
}

func (v *validator) migrateRemediation(t *tableSpec) string {
	return fmt.Sprintf("Missing NULLABLE or REPEATED columns are added by the exporter at startup when "+
		"auto_create.tables is true (needs bigquery.tables.update), or by ALTER TABLE ... ADD COLUMN for "+
		"top-level columns. Type, mode, and REQUIRED-column differences cannot be changed in place: create a "+
		"new table from the current DDL (internal/schema %s), copy the rows, and point the exporter at it.", t.ddl)
}

func (v *validator) unprunableConflictRemediation(view string) string {
	spans := v.qualify(v.cfg.TracesTable)
	return fmt.Sprintf("BigQuery rejects every query over this view while %[1]s requires a partition filter, because "+
		"the view joins spans on a time window that depends on each row. Either stop requiring the filter "+
		"(ALTER TABLE `%[1]s` SET OPTIONS (require_partition_filter = false)), or drop the view "+
		"(DROP VIEW `%[2]s`) and keep auto_create.allow_unprunable_views off so the exporter leaves it out; "+
		"to correlate ad hoc, join a start_time-bounded %[3]s_dedup subquery directly.",
		spans, v.qualify(view), v.cfg.TracesTable)
}

func (v *validator) unprunableSkippedRemediation() string {
	spans := v.qualify(v.cfg.TracesTable)
	return fmt.Sprintf("The exporter skips this view while %[1]s requires a partition filter (unless "+
		"auto_create.allow_unprunable_views is set) because BigQuery would reject every query over it. To use it, "+
		"stop requiring the filter (ALTER TABLE `%[1]s` SET OPTIONS (require_partition_filter = false)) and "+
		"restart the exporter with auto_create.companion_views; otherwise join a start_time-bounded %[2]s_dedup "+
		"subquery directly.", spans, v.cfg.TracesTable)
}

const createViewsRemediation = "Create the companion views: start the exporter with auto_create.tables and " +
	"auto_create.companion_views set to true (needs bigquery.tables.create and bigquery.tables.update), or apply " +
	"deploy/terraform. If this deployment does not use companion views, leave the view checks out " +
	"(--checks=schema,iam,storage-write)."

const recreateViewRemediation = "Recreate the view from the current definition: restart the exporter with " +
	"auto_create.companion_views, which reconciles existing views to the current query, or re-apply " +
	"deploy/terraform. The tables and _dedup views it reads must exist and match schema " + schema.Version + "."

const dryRunAccessRemediation = "Dry runs need bigquery.tables.getData on every table and view the query reads, " +
	"for the identity running this command (for example roles/bigquery.dataViewer on the dataset). The collector " +
	"itself does not need it."

const unauthenticatedRemediation = "Check the credentials of the identity running this command: run gcloud auth " +
	"application-default login, or set GOOGLE_APPLICATION_CREDENTIALS to a valid credential file."

func unreachableRemediation(project string) string {
	return fmt.Sprintf("Allow gRPC (HTTP/2 over TLS) egress to bigquerystorage.googleapis.com:443: check firewall "+
		"and proxy rules, Private Google Access on private subnets, and any VPC Service Controls perimeter around "+
		"project %s.", project)
}
