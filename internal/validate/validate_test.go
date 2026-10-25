// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
)

const (
	testProject = "test-project"
	testDataset = "telemetry"
)

// apiFailure is how the fake fails a call.
type apiFailure struct {
	// code is the HTTP status. Zero fails a dry run as a finished job with
	// an error result rather than as a failed call.
	code    int
	reason  string
	message string
}

func notFound(id string) apiFailure {
	return apiFailure{code: http.StatusNotFound, reason: "notFound",
		message: fmt.Sprintf("Not found: Table %s:%s.%s", testProject, testDataset, id)}
}

// partitionFilterFailure is BigQuery's answer to a query that reads the spans
// table without a filter it can use to skip partitions.
func partitionFilterFailure() apiFailure {
	return apiFailure{code: http.StatusBadRequest, reason: "invalidQuery",
		message: "Cannot query over table 'test-project.telemetry.otel_spans' without a filter over column(s) " +
			"'start_time' that can be used for partition elimination"}
}

// fakeBigQuery serves the REST calls Run makes: datasets.get, tables.get,
// tables.testIamPermissions, and jobs.insert for dry-run queries. It never
// answers with a 5xx status, which the client retries until its context
// ends, and it fails the test on any job that is not a dry run. Its bqfake
// server answers the Storage Write API probe.
type fakeBigQuery struct {
	t     *testing.T
	srv   *httptest.Server
	write *bqfake.Server

	mu sync.Mutex
	// datasetFailure, when set, fails datasets.get.
	datasetFailure *apiFailure
	// objects holds the tables and views of the dataset by ID.
	objects map[string]*bigqueryapi.Table
	// getFailures fail tables.get by ID.
	getFailures map[string]apiFailure
	// granted answers testIamPermissions by table ID. A table without an
	// entry has every permission asked for.
	granted map[string][]string
	// iamFailures fail testIamPermissions by table ID.
	iamFailures map[string]apiFailure
	// probeFailure, when set, fails the dry run that probes
	// bigquery.jobs.create.
	probeFailure *apiFailure
	// dryRunFailures fail the dry run of a view by view ID.
	dryRunFailures map[string]apiFailure
	// requests records "METHOD path" for every call.
	requests []string
	// queries records the SQL of every job.
	queries []string
}

// newFake returns a fake whose dataset conforms: the four tables with the
// current schemas and the 11 companion views.
func newFake(t *testing.T) *fakeBigQuery {
	t.Helper()
	f := &fakeBigQuery{
		t:              t,
		write:          bqfake.NewServer(t),
		objects:        map[string]*bigqueryapi.Table{},
		getFailures:    map[string]apiFailure{},
		granted:        map[string][]string{},
		iamFailures:    map[string]apiFailure{},
		dryRunFailures: map[string]apiFailure{},
	}
	f.addTable(DefaultTracesTable, schema.SpansTableSchema())
	f.addTable(DefaultLogsTable, schema.LogsTableSchema())
	f.addTable(DefaultMetricsTable, schema.MetricsTableSchema())
	f.addTable(DefaultQuarantineTable, schema.QuarantineTableSchema())
	for _, cv := range companionViews {
		f.addView(cv.name)
	}
	f.srv = httptest.NewServer(f)
	t.Cleanup(f.srv.Close)
	return f
}

// addTable adds a table with the given schema, converted to its REST form by
// bqfake the way the exporter's tests seed tables, and sets its expected time
// partitioning and clustering from internal/schema.
func (f *fakeBigQuery) addTable(id string, ts *storagepb.TableSchema) {
	f.t.Helper()
	if err := f.write.SeedTable(testProject, testDataset, id, ts); err != nil {
		f.t.Fatalf("SeedTable(%s): %v", id, err)
	}
	tbl, ok := f.write.Table(testProject, testDataset, id)
	if !ok {
		f.t.Fatalf("bqfake has no table %s", id)
	}
	applyTableLayout(tbl, ts)
	f.edit(func() { f.objects[id] = tbl })
}

func applyTableLayout(tbl *bigqueryapi.Table, ts *storagepb.TableSchema) {
	var md *bq.TableMetadata
	switch ts {
	case nil:
		return
	default:
		switch {
		case len(ts.GetFields()) > 0 && ts.GetFields()[0].GetName() == "ingest_time":
			md = mustQuarantineDestinationMetadata()
		case len(ts.GetFields()) > 0 && ts.GetFields()[0].GetName() == "trace_id":
			md = mustDestinationMetadata(ts, testProject, testDataset, DefaultTracesTable)
		case len(ts.GetFields()) > 1 && ts.GetFields()[1].GetName() == "metric_name":
			md = mustDestinationMetadata(ts, testProject, testDataset, DefaultMetricsTable)
		default:
			md = mustDestinationMetadata(ts, testProject, testDataset, DefaultLogsTable)
		}
	}
	if md.TimePartitioning != nil {
		tbl.TimePartitioning = &bigqueryapi.TimePartitioning{
			Type:  string(md.TimePartitioning.Type),
			Field: md.TimePartitioning.Field,
		}
	}
	if md.Clustering != nil {
		tbl.Clustering = &bigqueryapi.Clustering{
			Fields: slices.Clone(md.Clustering.Fields),
		}
	}
}

func (f *fakeBigQuery) addView(id string) {
	f.edit(func() {
		f.objects[id] = &bigqueryapi.Table{
			TableReference: &bigqueryapi.TableReference{ProjectId: testProject, DatasetId: testDataset, TableId: id},
			Type:           "VIEW",
			View:           &bigqueryapi.ViewDefinition{Query: "SELECT 1"},
		}
	})
}

// edit changes the fake under its lock.
func (f *fakeBigQuery) edit(change func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change()
}

// recorded returns the calls and the SQL the fake received.
func (f *fakeBigQuery) recorded() (requests, queries []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests), slices.Clone(f.queries)
}

// config returns a Config that reaches the fake.
func (f *fakeBigQuery) config() Config {
	return Config{
		Project: testProject,
		Dataset: testDataset,
		ClientOptions: []option.ClientOption{
			option.WithEndpoint(f.srv.URL + "/bigquery/v2/"),
			option.WithoutAuthentication(),
		},
		WriteClientOptions: f.write.WriteClientOptions(),
	}
}

func (f *fakeBigQuery) run(cfg Config) *Report {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	report, err := Run(ctx, cfg)
	if err != nil {
		f.t.Fatalf("Run() error = %v", err)
	}
	return report
}

const (
	datasetPath = "/bigquery/v2/projects/" + testProject + "/datasets/" + testDataset
	tablesPath  = datasetPath + "/tables/"
	jobsPath    = "/bigquery/v2/projects/" + testProject + "/jobs"
)

func (f *fakeBigQuery) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.Path)
	path := r.URL.Path
	switch {
	case r.Method == http.MethodPost && path == jobsPath:
		f.insertJob(w, r)
	case r.Method == http.MethodGet && path == datasetPath:
		if f.datasetFailure != nil {
			writeFailure(w, *f.datasetFailure)
			return
		}
		writeJSON(w, &bigqueryapi.Dataset{
			DatasetReference: &bigqueryapi.DatasetReference{ProjectId: testProject, DatasetId: testDataset},
			Location:         "US",
		})
	case r.Method == http.MethodPost && strings.HasPrefix(path, tablesPath) && strings.HasSuffix(path, ":testIamPermissions"):
		f.testPermissions(w, r, strings.TrimSuffix(strings.TrimPrefix(path, tablesPath), ":testIamPermissions"))
	case r.Method == http.MethodGet && strings.HasPrefix(path, tablesPath):
		id := strings.TrimPrefix(path, tablesPath)
		if failure, ok := f.getFailures[id]; ok {
			writeFailure(w, failure)
			return
		}
		tbl, ok := f.objects[id]
		if !ok {
			writeFailure(w, notFound(id))
			return
		}
		writeJSON(w, tbl)
	default:
		f.t.Errorf("unexpected request %s %s", r.Method, r.URL)
		writeFailure(w, apiFailure{code: http.StatusBadRequest, reason: "invalid", message: "unexpected request"})
	}
}

// viewInQuery finds the view a dry run reads.
var viewInQuery = regexp.MustCompile("FROM `" + regexp.QuoteMeta(testProject+"."+testDataset+".") + "([^`]+)`")

func (f *fakeBigQuery) insertJob(w http.ResponseWriter, r *http.Request) {
	var job bigqueryapi.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil || job.Configuration == nil || job.Configuration.Query == nil {
		f.t.Errorf("jobs.insert body is not a query job (decoding error: %v)", err)
		writeFailure(w, apiFailure{code: http.StatusBadRequest, reason: "invalid", message: "not a query job"})
		return
	}
	sql := job.Configuration.Query.Query
	f.queries = append(f.queries, sql)
	if !job.Configuration.DryRun {
		f.t.Errorf("job is not a dry run: %s", sql)
		writeFailure(w, apiFailure{code: http.StatusBadRequest, reason: "invalid", message: "not a dry run"})
		return
	}
	if legacy := job.Configuration.Query.UseLegacySql; legacy == nil || *legacy {
		f.t.Errorf("dry run does not ask for GoogleSQL: %s", sql)
	}
	failure := f.probeFailure
	if m := viewInQuery.FindStringSubmatch(sql); m != nil {
		failure = nil
		if df, ok := f.dryRunFailures[m[1]]; ok {
			failure = &df
		}
	}
	if failure != nil && failure.code != 0 {
		writeFailure(w, *failure)
		return
	}
	job.Status = &bigqueryapi.JobStatus{State: "DONE"}
	if failure != nil {
		job.Status.ErrorResult = &bigqueryapi.ErrorProto{Reason: failure.reason, Message: failure.message}
	} else {
		job.Statistics = &bigqueryapi.JobStatistics{TotalBytesProcessed: 2048}
	}
	writeJSON(w, &job)
}

func (f *fakeBigQuery) testPermissions(w http.ResponseWriter, r *http.Request, id string) {
	var req bigqueryapi.TestIamPermissionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		f.t.Errorf("decoding testIamPermissions body: %v", err)
	}
	if failure, ok := f.iamFailures[id]; ok {
		writeFailure(w, failure)
		return
	}
	if _, ok := f.objects[id]; !ok {
		writeFailure(w, notFound(id))
		return
	}
	held, limited := f.granted[id]
	var granted []string
	for _, p := range req.Permissions {
		if !limited || slices.Contains(held, p) {
			granted = append(granted, p)
		}
	}
	writeJSON(w, &bigqueryapi.TestIamPermissionsResponse{Permissions: granted})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeFailure(w http.ResponseWriter, failure apiFailure) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(failure.code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code":    failure.code,
		"message": failure.message,
		"errors":  []map[string]string{{"domain": "global", "reason": failure.reason, "message": failure.message}},
	}})
}

// writeServer is a Storage Write API server whose GetWriteStream answers
// with err, or with a stream when err is nil.
type writeServer struct {
	storagepb.UnimplementedBigQueryWriteServer
	err error

	mu    sync.Mutex
	names []string
}

func (s *writeServer) GetWriteStream(_ context.Context, req *storagepb.GetWriteStreamRequest) (*storagepb.WriteStream, error) {
	s.mu.Lock()
	s.names = append(s.names, req.GetName())
	s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	return &storagepb.WriteStream{Name: req.GetName(), Type: storagepb.WriteStream_COMMITTED, Location: "EU"}, nil
}

func (s *writeServer) requested() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.names)
}

func startWriteServer(t *testing.T, err error) (*writeServer, []option.ClientOption) {
	t.Helper()
	lis, lerr := net.Listen("tcp", "127.0.0.1:0")
	if lerr != nil {
		t.Fatal(lerr)
	}
	ws := &writeServer{err: err}
	gs := grpc.NewServer()
	storagepb.RegisterBigQueryWriteServer(gs, ws)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = gs.Serve(lis)
	}()
	t.Cleanup(func() {
		gs.Stop()
		<-served
	})
	return ws, insecureWriteOptions(lis.Addr().String())
}

func insecureWriteOptions(addr string) []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(addr),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}

// findCheck returns the check of a category with the given name.
func findCheck(t *testing.T, r *Report, cat Category, name string) Check {
	t.Helper()
	for _, c := range r.Checks {
		if c.Category == cat && c.Name == name {
			return c
		}
	}
	t.Fatalf("report has no %s check %q; checks:%s", cat, name, listChecks(r))
	return Check{}
}

// expectCheck asserts a check's status, and that its detail and remediation
// together contain each of the given strings.
func expectCheck(t *testing.T, r *Report, cat Category, name string, want Status, contains ...string) Check {
	t.Helper()
	c := findCheck(t, r, cat, name)
	if c.Status != want {
		t.Errorf("%s check %q status = %s, want %s (detail %q)", cat, name, c.Status, want, c.Detail)
	}
	text := c.Detail + "\n" + c.Remediation
	for _, s := range contains {
		if !strings.Contains(text, s) {
			t.Errorf("%s check %q:\n detail %q\n remediation %q\nwant it to contain %q", cat, name, c.Detail, c.Remediation, s)
		}
	}
	return c
}

func hasCheck(r *Report, cat Category, name string) bool {
	return slices.ContainsFunc(r.Checks, func(c Check) bool { return c.Category == cat && c.Name == name })
}

func listChecks(r *Report) string {
	var b strings.Builder
	for _, c := range r.Checks {
		fmt.Fprintf(&b, "\n  %s %s %q: %s", c.Status, c.Category, c.Name, c.Detail)
	}
	return b.String()
}

func viewByName(t *testing.T, name string) companionView {
	t.Helper()
	for _, cv := range companionViews {
		if cv.name == name {
			return cv
		}
	}
	t.Fatalf("no companion view %q", name)
	return companionView{}
}

var unprunableViews = []string{
	schema.GenAIAgentTrajectoryViewName,
	schema.GenAILogsViewName,
	schema.GenAIMetricExemplarsViewName,
}

var tableLabels = []string{"traces table", "logs table", "metrics table", "quarantine table"}

func TestRunConformantDataset(t *testing.T) {
	f := newFake(t)
	r := f.run(f.config())
	// 5 schema checks (dataset and 4 tables), 11 views, 6 iam checks
	// (datasets.get, jobs.create, and 4 tables), and the write probe.
	if !r.Conforms || r.Summary != (Summary{Pass: 23}) {
		t.Fatalf("Conforms = %v, Summary = %+v, want a conformant report of 23 passing checks:%s",
			r.Conforms, r.Summary, listChecks(r))
	}
	if r.Project != testProject || r.Dataset != testDataset || r.SchemaVersion != schema.Version {
		t.Errorf("report is for %s.%s at %s, want %s.%s at %s",
			r.Project, r.Dataset, r.SchemaVersion, testProject, testDataset, schema.Version)
	}
	var order []Category
	for _, c := range r.Checks {
		if len(order) == 0 || order[len(order)-1] != c.Category {
			order = append(order, c.Category)
		}
	}
	if !slices.Equal(order, allCategories) {
		t.Errorf("categories appear in order %v, want %v", order, allCategories)
	}
	expectCheck(t, r, CategorySchema, "dataset", StatusPass, "dataset exists")
	expectCheck(t, r, CategoryIAM, "bigquery.datasets.get", StatusPass)
	expectCheck(t, r, CategoryIAM, "bigquery.jobs.create", StatusPass)
	for _, label := range tableLabels {
		c := expectCheck(t, r, CategorySchema, label, StatusPass, "matches schema "+schema.Version)
		if strings.Contains(c.Detail, "require_partition_filter") {
			t.Errorf("%s detail = %q, but the table does not require a partition filter", label, c.Detail)
		}
		expectCheck(t, r, CategoryIAM, label+" permissions", StatusPass, "has bigquery.tables.get and bigquery.tables.updateData")
	}
	expectCheck(t, r, CategoryStorageWrite, "default stream", StatusPass,
		"GetWriteStream answered for projects/test-project/datasets/telemetry/tables/otel_spans/streams/_default (type COMMITTED, location US)")

	requests, queries := f.recorded()
	wantQueries := []string{jobsCreateProbeSQL}
	for _, cv := range companionViews {
		wantQueries = append(wantQueries, dryRunSQL(testProject, testDataset, cv))
		c := expectCheck(t, r, CategoryViews, cv.name, StatusPass,
			fmt.Sprintf("dry run with a filter on %s compiles (estimated 2048 bytes)", cv.filterColumn))
		if c.Resource != testProject+"."+testDataset+"."+cv.name {
			t.Errorf("view %s resource = %q", cv.name, c.Resource)
		}
	}
	if !slices.Equal(queries, wantQueries) {
		t.Errorf("queries = %q, want %q", queries, wantQueries)
	}
	// No check writes: every call reads metadata, tests permissions, or is
	// a dry run, which the fake insists on.
	for _, req := range requests {
		if !strings.HasPrefix(req, "GET ") && req != "POST "+jobsPath && !strings.HasSuffix(req, ":testIamPermissions") {
			t.Errorf("unexpected call %s", req)
		}
	}
}

func TestDryRunSQL(t *testing.T) {
	for _, tc := range []struct {
		view string
		want string
	}{
		{schema.GenAISpansViewName,
			"SELECT * FROM `p.d.v_gen_ai_spans` WHERE `start_time` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)"},
		{schema.GenAIModelCostsViewName,
			"SELECT * FROM `p.d.v_gen_ai_model_costs` WHERE `usage_hour` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)"},
		{schema.GenAISessionsViewName,
			"SELECT * FROM `p.d.v_gen_ai_sessions` WHERE `session_start_date` >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"},
		{schema.GenAIMetricExemplarsViewName,
			"SELECT * FROM `p.d.v_gen_ai_metric_exemplars` WHERE `metric_timestamp` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)"},
	} {
		if got := dryRunSQL("p", "d", viewByName(t, tc.view)); got != tc.want {
			t.Errorf("dryRunSQL(%s) = %q, want %q", tc.view, got, tc.want)
		}
	}
}

// TestCompanionViewsMatchSchema keeps the views Run dry-runs in step with
// the views the exporter creates.
func TestCompanionViewsMatchSchema(t *testing.T) {
	queries := map[string]string{}
	for _, views := range []map[string]*bq.TableMetadata{
		schema.CompanionViewsMetadata(testProject, testDataset, DefaultTracesTable, DefaultLogsTable, true),
		schema.CompanionViewsMetadata(testProject, testDataset, DefaultTracesTable, DefaultLogsTable, false),
		schema.MetricsCompanionViewsMetadata(testProject, testDataset, DefaultMetricsTable),
		{schema.GenAIMetricExemplarsViewName: schema.MetricExemplarsCompanionViewMetadata(testProject, testDataset, DefaultTracesTable)},
	} {
		for name, md := range views {
			queries[name] = md.ViewQuery
		}
	}
	var names []string
	for _, cv := range companionViews {
		names = append(names, cv.name)
	}
	if got, want := slices.Sorted(slices.Values(names)), slices.Sorted(maps.Keys(queries)); !slices.Equal(got, want) {
		t.Fatalf("companion views = %v, want the exporter's %v", got, want)
	}
	if !slices.Equal(names[:len(schema.SpanCompanionViewOrder)], schema.SpanCompanionViewOrder) {
		t.Errorf("span views are listed as %v, want the exporter's order %v", names[:len(schema.SpanCompanionViewOrder)], schema.SpanCompanionViewOrder)
	}
	for _, cv := range companionViews {
		query := queries[cv.name]
		if !regexp.MustCompile(`\b` + regexp.QuoteMeta(cv.filterColumn) + `\b`).MatchString(query) {
			t.Errorf("view %s does not mention its filter column %s", cv.name, cv.filterColumn)
		}
		readsSpans := strings.Contains(query, DefaultTracesTable) ||
			strings.Contains(query, schema.GenAISpansViewName) || strings.Contains(query, schema.MCPSpansViewName)
		readsLogs := strings.Contains(query, DefaultLogsTable)
		readsMetrics := strings.Contains(query, DefaultMetricsTable) || strings.Contains(query, schema.GenAIMetricsViewName)
		for _, s := range []struct {
			signal Signal
			reads  bool
		}{{SignalTraces, readsSpans}, {SignalLogs, readsLogs}, {SignalMetrics, readsMetrics}} {
			if slices.Contains(cv.signals, s.signal) != s.reads {
				t.Errorf("view %s: lists signals %v, but reads the %s tables = %v", cv.name, cv.signals, s.signal, s.reads)
			}
		}
		if cv.unprunable != slices.Contains(unprunableViews, cv.name) {
			t.Errorf("view %s: unprunable = %v, want %v", cv.name, cv.unprunable, !cv.unprunable)
		}
	}
}

func TestRunMissingDataset(t *testing.T) {
	f := newFake(t)
	f.edit(func() {
		f.datasetFailure = &apiFailure{code: http.StatusNotFound, reason: "notFound", message: "Not found: Dataset test-project:telemetry"}
	})
	r := f.run(f.config())
	if r.Conforms || len(r.Checks) != 2 {
		t.Fatalf("Conforms = %v with %d checks, want a failing report of the dataset and write probe checks:%s",
			r.Conforms, len(r.Checks), listChecks(r))
	}
	expectCheck(t, r, CategorySchema, "dataset", StatusFail, "dataset not found: Not found: Dataset test-project:telemetry",
		"bq --location=LOCATION mk --dataset test-project:telemetry", "auto_create.dataset: true", "deploy/terraform")
	expectCheck(t, r, CategoryStorageWrite, "default stream", StatusPass, "otel_spans/streams/_default")
	if requests, _ := f.recorded(); len(requests) != 1 {
		t.Errorf("calls = %q, want only datasets.get", requests)
	}
}

func TestRunDatasetUnreadable(t *testing.T) {
	denied := apiFailure{code: http.StatusForbidden, reason: "accessDenied",
		message: "Access Denied: Dataset test-project:telemetry: Permission bigquery.datasets.get denied"}
	for _, tc := range []struct {
		name     string
		failure  apiFailure
		checks   []Category
		cat      Category
		check    string
		contains []string
	}{
		{"denied", denied, nil, CategoryIAM, "bigquery.datasets.get",
			[]string{"permission denied reading dataset metadata: Access Denied", "otelCollectorWriter", "roles/bigquery.dataEditor"}},
		{"denied without the iam checks", denied, []Category{CategorySchema}, CategorySchema, "dataset",
			[]string{"permission denied reading dataset metadata", "bigquery.datasets.get on dataset test-project.telemetry"}},
		{"other error", apiFailure{code: http.StatusBadRequest, reason: "invalid", message: "Invalid dataset ID"},
			nil, CategorySchema, "dataset", []string{"could not read dataset metadata: Invalid dataset ID"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			f.edit(func() { f.datasetFailure = &tc.failure })
			cfg := f.config()
			cfg.Checks = tc.checks
			r := f.run(cfg)
			expectCheck(t, r, tc.cat, tc.check, StatusFail, tc.contains...)
			if r.Conforms || r.Summary.Fail != 1 {
				t.Errorf("Conforms = %v, Summary = %+v, want exactly one failure:%s", r.Conforms, r.Summary, listChecks(r))
			}
			// Table-level grants can still allow the table checks.
			expectCheck(t, r, CategorySchema, "traces table", StatusPass)
		})
	}
}

func TestRunMissingTable(t *testing.T) {
	f := newFake(t)
	f.edit(func() {
		delete(f.objects, DefaultLogsTable)
		f.dryRunFailures[schema.GenAILogsViewName] = notFound(DefaultLogsTable + "_dedup")
	})
	r := f.run(f.config())
	expectCheck(t, r, CategorySchema, "logs table", StatusFail, "table not found",
		"LogsDDL and LogsDedupViewDDL", "auto_create.tables: true", "--signals")
	expectCheck(t, r, CategoryIAM, "logs table permissions", StatusSkip, "not checked: table not found")
	expectCheck(t, r, CategoryViews, schema.GenAILogsViewName, StatusFail,
		"the view reads a table or view that does not exist: Not found: Table test-project:telemetry.otel_logs_dedup")
	if r.Conforms || r.Summary.Fail != 2 || r.Summary.Skip != 1 {
		t.Errorf("Conforms = %v, Summary = %+v, want 2 failures and 1 skip:%s", r.Conforms, r.Summary, listChecks(r))
	}
}

func TestRunSchemaMismatch(t *testing.T) {
	t.Run("a few differences", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			tbl := f.objects[DefaultTracesTable]
			var fields []*bigqueryapi.TableFieldSchema
			for _, field := range tbl.Schema.Fields {
				switch field.Name {
				case "end_time":
					continue
				case "duration_nano":
					field.Type = "STRING"
				case "trace_id":
					field.Mode = "NULLABLE"
				}
				fields = append(fields, field)
			}
			tbl.Schema.Fields = fields
		})
		r := f.run(f.config())
		expectCheck(t, r, CategorySchema, "traces table", StatusFail, "does not match schema "+schema.Version,
			"end_time is missing", "duration_nano has type STRING, want INT64", "trace_id has mode NULLABLE, want REQUIRED",
			"ALTER TABLE ... ADD COLUMN", "SpansDDL and SpansDedupViewDDL")
	})
	t.Run("many differences", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			f.objects[DefaultQuarantineTable].Schema.Fields = []*bigqueryapi.TableFieldSchema{{Name: "unrelated", Type: "STRING"}}
		})
		r := f.run(f.config())
		c := expectCheck(t, r, CategorySchema, "quarantine table", StatusFail, "QuarantineDDL")
		if !regexp.MustCompile(`; and \d+ more$`).MatchString(c.Detail) || strings.Count(c.Detail, " is missing") != 10 {
			t.Errorf("detail = %q, want the first 10 differences and a count of the rest", c.Detail)
		}
	})
	t.Run("unreadable schema", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() { f.objects[DefaultMetricsTable].Schema = nil })
		r := f.run(f.config())
		expectCheck(t, r, CategorySchema, "metrics table", StatusFail, "unreadable table schema: destination table reports no columns")
	})
	t.Run("missing partitioning and clustering", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			tbl := f.objects[DefaultTracesTable]
			tbl.TimePartitioning = nil
			tbl.Clustering = nil
		})
		r := f.run(f.config())
		expectCheck(t, r, CategorySchema, "traces table", StatusFail,
			"time partitioning is missing, want HOUR partitioning on start_time",
			"clustering is missing, want (service_name, trace_id, name)",
			"SpansDDL and SpansDedupViewDDL")
		if r.Conforms {
			t.Error("Conforms = true, want false when partitioning and clustering are missing")
		}
	})
	t.Run("wrong partitioning and clustering", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			tbl := f.objects[DefaultTracesTable]
			tbl.TimePartitioning = &bigqueryapi.TimePartitioning{Type: "DAY", Field: "receive_time"}
			tbl.Clustering = &bigqueryapi.Clustering{Fields: []string{"span_id"}}
		})
		r := f.run(f.config())
		expectCheck(t, r, CategorySchema, "traces table", StatusFail,
			"time partitioning has type DAY, want HOUR",
			"time partitioning is on receive_time, want start_time",
			"clustering is (span_id), want (service_name, trace_id, name)")
		if r.Conforms {
			t.Error("Conforms = true, want false when partitioning and clustering are wrong")
		}
	})
}

func TestRunTableProblems(t *testing.T) {
	t.Run("not a table", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() { f.objects[DefaultMetricsTable].Type = "VIEW" })
		r := f.run(f.config())
		expectCheck(t, r, CategorySchema, "metrics table", StatusFail,
			"test-project.telemetry.otel_metrics is a VIEW, not a table", "MetricsDDL and MetricsDedupViewDDL")
	})
	t.Run("metadata denied", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			f.getFailures[DefaultTracesTable] = apiFailure{code: http.StatusForbidden, reason: "accessDenied",
				message: "Access Denied: Table test-project:telemetry.otel_spans"}
			f.granted[DefaultTracesTable] = []string{permTablesUpdateData}
		})
		r := f.run(f.config())
		expectCheck(t, r, CategorySchema, "traces table", StatusFail,
			"permission denied reading table metadata: Access Denied", "Grant the collector's identity bigquery.tables.get")
		// The permission check still runs, and names what is missing.
		expectCheck(t, r, CategoryIAM, "traces table permissions", StatusFail, "missing bigquery.tables.get")
	})
	t.Run("metadata unreadable", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			f.getFailures[DefaultQuarantineTable] = apiFailure{code: http.StatusBadRequest, reason: "invalid", message: "Invalid table ID"}
		})
		r := f.run(f.config())
		expectCheck(t, r, CategorySchema, "quarantine table", StatusFail, "could not read table metadata: Invalid table ID")
	})
}

func TestRunRequirePartitionFilter(t *testing.T) {
	for _, tc := range []struct {
		name    string
		require func(*bigqueryapi.Table)
	}{
		{"on the table", func(tbl *bigqueryapi.Table) { tbl.RequirePartitionFilter = true }},
		{"on its time partitioning", func(tbl *bigqueryapi.Table) {
			tbl.TimePartitioning = &bigqueryapi.TimePartitioning{Type: "HOUR", Field: "start_time", RequirePartitionFilter: true}
		}},
	} {
		t.Run(tc.name+", unprunable views skipped", func(t *testing.T) {
			f := newFake(t)
			f.edit(func() {
				tc.require(f.objects[DefaultTracesTable])
				for _, v := range unprunableViews {
					delete(f.objects, v)
				}
			})
			r := f.run(f.config())
			if !r.Conforms || r.Summary.Warn != len(unprunableViews) {
				t.Errorf("Conforms = %v, Summary = %+v, want a conformant report with %d warnings:%s",
					r.Conforms, r.Summary, len(unprunableViews), listChecks(r))
			}
			expectCheck(t, r, CategorySchema, "traces table", StatusPass,
				"queries must filter on its partitioning column (require_partition_filter)")
			for _, v := range unprunableViews {
				expectCheck(t, r, CategoryViews, v, StatusWarn,
					"not created, as expected while test-project.telemetry.otel_spans requires a partition filter",
					"auto_create.allow_unprunable_views", "require_partition_filter = false")
			}
			// Views that do not exist are not dry-run.
			if _, queries := f.recorded(); len(queries) != 1+len(companionViews)-len(unprunableViews) {
				t.Errorf("queries = %q, want the probe and one dry run per existing view", queries)
			}
		})
	}
	t.Run("unprunable views present", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			f.objects[DefaultTracesTable].RequirePartitionFilter = true
			for _, v := range unprunableViews {
				f.dryRunFailures[v] = partitionFilterFailure()
			}
		})
		r := f.run(f.config())
		if r.Conforms || r.Summary.Fail != len(unprunableViews) {
			t.Errorf("Conforms = %v, Summary = %+v, want %d failures:%s", r.Conforms, r.Summary, len(unprunableViews), listChecks(r))
		}
		for _, v := range unprunableViews {
			expectCheck(t, r, CategoryViews, v, StatusFail,
				"require_partition_filter conflict: Cannot query over table 'test-project.telemetry.otel_spans'",
				"ALTER TABLE `test-project.telemetry.otel_spans` SET OPTIONS (require_partition_filter = false)",
				"DROP VIEW `test-project.telemetry."+v+"`", "auto_create.allow_unprunable_views off")
		}
	})
	t.Run("unprunable view missing without the filter", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() { delete(f.objects, schema.GenAILogsViewName) })
		r := f.run(f.config())
		expectCheck(t, r, CategoryViews, schema.GenAILogsViewName, StatusFail, "view not found", "auto_create.companion_views")
	})
}

func TestRunCustomTableNames(t *testing.T) {
	f := newFake(t)
	for _, tbl := range []struct {
		old, renamed string
		ts           *storagepb.TableSchema
	}{
		{DefaultTracesTable, "spans_v2", schema.SpansTableSchema()},
		{DefaultLogsTable, "logs_v2", schema.LogsTableSchema()},
		{DefaultMetricsTable, "metrics_v2", schema.MetricsTableSchema()},
		{DefaultQuarantineTable, "dead_letter", schema.QuarantineTableSchema()},
	} {
		f.edit(func() { delete(f.objects, tbl.old) })
		f.addTable(tbl.renamed, tbl.ts)
	}
	f.edit(func() {
		f.objects["spans_v2"].RequirePartitionFilter = true
		delete(f.objects, schema.GenAIAgentTrajectoryViewName)
	})
	cfg := f.config()
	cfg.TracesTable, cfg.LogsTable, cfg.MetricsTable, cfg.QuarantineTable = "spans_v2", "logs_v2", "metrics_v2", "dead_letter"
	r := f.run(cfg)
	for label, id := range map[string]string{
		"traces table": "spans_v2", "logs table": "logs_v2", "metrics table": "metrics_v2", "quarantine table": "dead_letter",
	} {
		if c := expectCheck(t, r, CategorySchema, label, StatusPass); c.Resource != testProject+"."+testDataset+"."+id {
			t.Errorf("%s resource = %q, want %s", label, c.Resource, id)
		}
	}
	expectCheck(t, r, CategoryViews, schema.GenAIAgentTrajectoryViewName, StatusWarn,
		"test-project.telemetry.spans_v2 requires a partition filter", "start_time-bounded spans_v2_dedup subquery")
	expectCheck(t, r, CategoryStorageWrite, "default stream", StatusPass, "tables/spans_v2/streams/_default")
}

func TestRunViewDryRunFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		view     string
		failure  apiFailure
		contains []string
	}{
		{"filter does not reach the partitioned table", schema.GenAIModelCostsViewName, partitionFilterFailure(),
			[]string{"require_partition_filter conflict: a filter on usage_hour does not reach the partitioned table", "auto_create.companion_views"}},
		{"does not compile", schema.GenAISpansViewName,
			apiFailure{code: http.StatusBadRequest, reason: "invalidQuery", message: "Unrecognized name: gen_ai_system at [3:5]"},
			[]string{"the view does not compile: Unrecognized name: gen_ai_system", "Recreate the view"}},
		{"reads a missing table", schema.MCPSpansViewName, notFound(DefaultTracesTable + "_dedup"),
			[]string{"the view reads a table or view that does not exist", "otel_spans_dedup"}},
		{"denied", schema.GenAIMetricsViewName,
			apiFailure{code: http.StatusForbidden, reason: "accessDenied", message: "Access Denied: Table test-project:telemetry.otel_metrics"},
			[]string{"dry run denied: Access Denied", "bigquery.tables.getData", "roles/bigquery.dataViewer"}},
		{"job error: invalid query", schema.GenAIToolUsageViewName,
			apiFailure{reason: "invalidQuery", message: "Syntax error: Unexpected keyword"},
			[]string{"the view does not compile: Syntax error: Unexpected keyword"}},
		{"job error: not found", schema.MCPToolUsageViewName,
			apiFailure{reason: "notFound", message: "Not found: Table test-project:telemetry.v_mcp_spans"},
			[]string{"the view reads a table or view that does not exist"}},
		{"job error: access denied", schema.SpansAsAllSpansViewName,
			apiFailure{reason: "accessDenied", message: "Access Denied: Table test-project:telemetry.otel_spans_dedup"},
			[]string{"dry run denied", "bigquery.tables.getData"}},
		{"job error: partition filter", schema.GenAIMetricExemplarsViewName,
			apiFailure{reason: "invalidQuery", message: partitionFilterFailure().message},
			[]string{"require_partition_filter conflict", "DROP VIEW `test-project.telemetry.v_gen_ai_metric_exemplars`"}},
		{"other failure", schema.GenAISessionsViewName,
			apiFailure{reason: "resourcesExceeded", message: "Resources exceeded during query execution"},
			[]string{"dry run failed: Resources exceeded during query execution"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			f.edit(func() { f.dryRunFailures[tc.view] = tc.failure })
			r := f.run(f.config())
			expectCheck(t, r, CategoryViews, tc.view, StatusFail, tc.contains...)
			if r.Conforms || r.Summary.Fail != 1 {
				t.Errorf("Conforms = %v, Summary = %+v, want exactly one failure:%s", r.Conforms, r.Summary, listChecks(r))
			}
		})
	}
}

func TestRunViewMetadataProblems(t *testing.T) {
	t.Run("view missing", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() { delete(f.objects, schema.MCPSpansViewName) })
		r := f.run(f.config())
		expectCheck(t, r, CategoryViews, schema.MCPSpansViewName, StatusFail, "view not found",
			"auto_create.companion_views set to true", "--checks=schema,iam,storage-write")
	})
	t.Run("not a view", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() { f.objects[schema.GenAISpansViewName].Type = "TABLE" })
		r := f.run(f.config())
		expectCheck(t, r, CategoryViews, schema.GenAISpansViewName, StatusFail,
			"test-project.telemetry.v_gen_ai_spans is a TABLE, not a view", "Drop it")
	})
	t.Run("metadata unreadable", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			f.getFailures[schema.GenAISpansViewName] = apiFailure{code: http.StatusBadRequest, reason: "invalid", message: "Invalid table ID"}
		})
		r := f.run(f.config())
		expectCheck(t, r, CategoryViews, schema.GenAISpansViewName, StatusFail, "could not read view metadata: Invalid table ID")
	})
	t.Run("metadata denied, dry run allowed", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			f.getFailures[schema.GenAISpansViewName] = apiFailure{code: http.StatusForbidden, reason: "accessDenied", message: "Access Denied"}
		})
		r := f.run(f.config())
		expectCheck(t, r, CategoryViews, schema.GenAISpansViewName, StatusPass, "dry run with a filter on start_time compiles")
	})
}

func TestRunJobsCreateProbe(t *testing.T) {
	t.Run("denied", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			f.probeFailure = &apiFailure{code: http.StatusForbidden, reason: "accessDenied",
				message: "Access Denied: Project test-project: User does not have bigquery.jobs.create permission in project test-project."}
		})
		r := f.run(f.config())
		expectCheck(t, r, CategoryIAM, "bigquery.jobs.create", StatusFail, "dry-run query jobs are denied: Access Denied",
			"roles/bigquery.jobUser", "--checks=schema,iam,storage-write")
		for _, cv := range companionViews {
			expectCheck(t, r, CategoryViews, cv.name, StatusSkip, "not checked: dry runs need bigquery.jobs.create")
		}
		if _, queries := f.recorded(); !slices.Equal(queries, []string{jobsCreateProbeSQL}) {
			t.Errorf("queries = %q, want only the probe", queries)
		}
		if r.Summary.Fail != 1 || r.Summary.Skip != len(companionViews) {
			t.Errorf("Summary = %+v, want 1 failure and %d skips", r.Summary, len(companionViews))
		}
	})
	t.Run("other failure", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			f.probeFailure = &apiFailure{code: http.StatusBadRequest, reason: "invalid", message: "Invalid project ID"}
		})
		r := f.run(f.config())
		expectCheck(t, r, CategoryIAM, "bigquery.jobs.create", StatusFail, "could not run a dry-run query job: Invalid project ID")
		// The view dry runs still report what they find.
		expectCheck(t, r, CategoryViews, schema.GenAISpansViewName, StatusPass)
	})
}

func TestRunTablePermissions(t *testing.T) {
	t.Run("missing updateData", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() { f.granted[DefaultTracesTable] = []string{permTablesGet} })
		r := f.run(f.config())
		expectCheck(t, r, CategoryIAM, "traces table permissions", StatusFail, "missing bigquery.tables.updateData",
			"Grant the collector's identity bigquery.tables.updateData on dataset test-project.telemetry",
			"gcloud iam roles create otelCollectorWriter --project=test-project "+
				"--permissions=bigquery.datasets.get,bigquery.tables.get,bigquery.tables.updateData",
			"roles/bigquery.dataEditor", "run it as the collector's identity")
		if r.Conforms || r.Summary.Fail != 1 {
			t.Errorf("Conforms = %v, Summary = %+v, want exactly one failure", r.Conforms, r.Summary)
		}
	})
	t.Run("missing both", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() { f.granted[DefaultLogsTable] = nil })
		r := f.run(f.config())
		expectCheck(t, r, CategoryIAM, "logs table permissions", StatusFail,
			"missing bigquery.tables.get and bigquery.tables.updateData")
	})
	t.Run("test fails", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			f.iamFailures[DefaultMetricsTable] = apiFailure{code: http.StatusBadRequest, reason: "invalid", message: "Invalid permission"}
		})
		r := f.run(f.config())
		expectCheck(t, r, CategoryIAM, "metrics table permissions", StatusFail, "could not test permissions: Invalid permission")
	})
	t.Run("table gone", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() { f.iamFailures[DefaultMetricsTable] = notFound(DefaultMetricsTable) })
		r := f.run(f.config())
		expectCheck(t, r, CategoryIAM, "metrics table permissions", StatusFail, "cannot test permissions: table not found")
		if r.Conforms {
			t.Error("Conforms = true when testIamPermissions returned 404")
		}
	})
	t.Run("iam-only with missing tables fails", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			for _, id := range []string{DefaultTracesTable, DefaultLogsTable, DefaultMetricsTable, DefaultQuarantineTable} {
				delete(f.objects, id)
			}
		})
		cfg := f.config()
		cfg.Checks = []Category{CategoryIAM}
		r := f.run(cfg)
		if r.Conforms || r.Summary.Fail != 4 || r.Summary.Skip != 0 {
			t.Errorf("Conforms = %v, Summary = %+v, want 4 failures and 0 skips:%s", r.Conforms, r.Summary, listChecks(r))
		}
		expectCheck(t, r, CategoryIAM, "traces table permissions", StatusFail, "cannot test permissions: table not found")
	})
	t.Run("iam-only with testIamPermissions 404 fails", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() { f.iamFailures[DefaultTracesTable] = notFound(DefaultTracesTable) })
		cfg := f.config()
		cfg.Checks = []Category{CategoryIAM}
		r := f.run(cfg)
		if r.Conforms || r.Summary.Fail != 1 {
			t.Errorf("Conforms = %v, Summary = %+v, want 1 failure:%s", r.Conforms, r.Summary, listChecks(r))
		}
		expectCheck(t, r, CategoryIAM, "traces table permissions", StatusFail, "cannot test permissions: table not found")
	})
}

func TestRunStorageWriteProbe(t *testing.T) {
	const stream = "projects/test-project/datasets/telemetry/tables/otel_spans/streams/_default"
	for _, tc := range []struct {
		name     string
		err      error
		want     Status
		contains []string
	}{
		{"answers", nil, StatusPass,
			[]string{"GetWriteStream answered for " + stream + " (type COMMITTED, location EU)"}},
		{"table not found", status.Error(codes.NotFound, "Requested entity was not found."), StatusPass,
			[]string{"endpoint reachable; the table does not exist yet (see the schema checks): Requested entity was not found."}},
		{"permission denied", status.Error(codes.PermissionDenied, "Permission 'TABLES_UPDATE_DATA' denied on resource"), StatusFail,
			[]string{"endpoint reachable, but GetWriteStream was denied: Permission 'TABLES_UPDATE_DATA'",
				"Grant the collector's identity bigquery.tables.updateData", "otelCollectorWriter"}},
		{"unauthenticated", status.Error(codes.Unauthenticated, "Request had invalid authentication credentials."), StatusFail,
			[]string{"credentials were rejected: Request had invalid authentication credentials.",
				"gcloud auth application-default login", "GOOGLE_APPLICATION_CREDENTIALS"}},
		{"other error", status.Error(codes.FailedPrecondition, "precondition failed"), StatusFail,
			[]string{"Storage Write API endpoint unreachable", "precondition failed", "bigquerystorage.googleapis.com:443"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			ws, opts := startWriteServer(t, tc.err)
			cfg := f.config()
			cfg.WriteClientOptions = opts
			r := f.run(cfg)
			c := expectCheck(t, r, CategoryStorageWrite, "default stream", tc.want, tc.contains...)
			if c.Resource != "test-project.telemetry.otel_spans" {
				t.Errorf("resource = %q, want the traces table", c.Resource)
			}
			if got := ws.requested(); !slices.Equal(got, []string{stream}) {
				t.Errorf("GetWriteStream calls = %q, want one for %s", got, stream)
			}
		})
	}
	t.Run("probes the first table that exists", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() { delete(f.objects, DefaultTracesTable) })
		ws, opts := startWriteServer(t, nil)
		cfg := f.config()
		cfg.WriteClientOptions = opts
		f.run(cfg)
		want := []string{"projects/test-project/datasets/telemetry/tables/otel_logs/streams/_default"}
		if got := ws.requested(); !slices.Equal(got, want) {
			t.Errorf("GetWriteStream calls = %q, want %q", got, want)
		}
	})
	t.Run("unreachable", func(t *testing.T) {
		f := newFake(t)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := lis.Addr().String()
		_ = lis.Close()
		cfg := f.config()
		cfg.WriteClientOptions = insecureWriteOptions(addr)
		cfg.ProbeTimeout = 300 * time.Millisecond
		start := time.Now()
		r := f.run(cfg)
		if elapsed := time.Since(start); elapsed > 20*time.Second {
			t.Errorf("Run took %v; the probe timeout should bound it", elapsed)
		}
		expectCheck(t, r, CategoryStorageWrite, "default stream", StatusFail, "Storage Write API endpoint unreachable",
			"bigquerystorage.googleapis.com:443", "Private Google Access", "VPC Service Controls perimeter around project test-project")
	})
}

func TestRunSignalSubsets(t *testing.T) {
	t.Run("logs only", func(t *testing.T) {
		f := newFake(t)
		ws, opts := startWriteServer(t, nil)
		cfg := f.config()
		cfg.Signals = []Signal{SignalLogs}
		cfg.WriteClientOptions = opts
		r := f.run(cfg)
		for _, label := range []string{"traces table", "metrics table"} {
			if hasCheck(r, CategorySchema, label) {
				t.Errorf("logs-only run checked the %s", label)
			}
		}
		expectCheck(t, r, CategorySchema, "logs table", StatusPass)
		expectCheck(t, r, CategorySchema, "quarantine table", StatusPass)
		expectCheck(t, r, CategoryViews, schema.GenAILogsViewName, StatusSkip,
			"not checked: the view reads the logs and traces tables, and this run leaves out traces")
		expectCheck(t, r, CategoryViews, schema.GenAIMetricExemplarsViewName, StatusSkip, "this run leaves out metrics and traces")
		if r.Summary.Skip != len(companionViews) || hasCheck(r, CategoryIAM, "bigquery.jobs.create") {
			t.Errorf("Summary = %+v, want every view skipped and no jobs.create probe:%s", r.Summary, listChecks(r))
		}
		if _, queries := f.recorded(); len(queries) != 0 {
			t.Errorf("queries = %q, want none", queries)
		}
		want := []string{"projects/test-project/datasets/telemetry/tables/otel_logs/streams/_default"}
		if got := ws.requested(); !slices.Equal(got, want) {
			t.Errorf("GetWriteStream calls = %q, want %q", got, want)
		}
	})
	t.Run("traces and metrics without quarantine", func(t *testing.T) {
		f := newFake(t)
		cfg := f.config()
		cfg.Signals = []Signal{SignalTraces, SignalMetrics}
		cfg.SkipQuarantine = true
		r := f.run(cfg)
		for _, label := range []string{"logs table", "quarantine table"} {
			if hasCheck(r, CategorySchema, label) || hasCheck(r, CategoryIAM, label+" permissions") {
				t.Errorf("run checked the %s", label)
			}
		}
		expectCheck(t, r, CategoryViews, schema.GenAILogsViewName, StatusSkip, "this run leaves out logs")
		expectCheck(t, r, CategoryViews, schema.GenAIMetricExemplarsViewName, StatusPass)
		if r.Summary != (Summary{Pass: 18, Skip: 1}) {
			t.Errorf("Summary = %+v, want 18 passes and 1 skip:%s", r.Summary, listChecks(r))
		}
	})
}

func TestRunCheckSubsets(t *testing.T) {
	t.Run("storage write only", func(t *testing.T) {
		f := newFake(t)
		cfg := f.config()
		cfg.Checks = []Category{CategoryStorageWrite}
		r := f.run(cfg)
		if len(r.Checks) != 1 || r.Checks[0].Category != CategoryStorageWrite || !r.Conforms {
			t.Errorf("checks:%s\nwant only a passing write probe", listChecks(r))
		}
		if requests, _ := f.recorded(); len(requests) != 1 {
			t.Errorf("calls = %q, want only datasets.get", requests)
		}
	})
	t.Run("storage write only, dataset missing", func(t *testing.T) {
		f := newFake(t)
		f.edit(func() {
			f.datasetFailure = &apiFailure{code: http.StatusNotFound, reason: "notFound", message: "Not found"}
		})
		cfg := f.config()
		cfg.Checks = []Category{CategoryStorageWrite}
		r := f.run(cfg)
		// A missing dataset fails the report whichever checks run.
		expectCheck(t, r, CategorySchema, "dataset", StatusFail, "dataset not found")
		if r.Conforms || len(r.Checks) != 2 {
			t.Errorf("Conforms = %v, checks:%s\nwant the dataset failure and the write probe", r.Conforms, listChecks(r))
		}
	})
	t.Run("schema only", func(t *testing.T) {
		f := newFake(t)
		cfg := f.config()
		cfg.Checks = []Category{CategorySchema}
		r := f.run(cfg)
		if r.Summary != (Summary{Pass: 5}) {
			t.Errorf("Summary = %+v, want the 5 schema checks:%s", r.Summary, listChecks(r))
		}
		requests, queries := f.recorded()
		if len(queries) != 0 || slices.ContainsFunc(requests, func(req string) bool { return strings.HasPrefix(req, "POST ") }) {
			t.Errorf("schema-only run made calls %q", requests)
		}
	})
	t.Run("views only", func(t *testing.T) {
		f := newFake(t)
		cfg := f.config()
		cfg.Checks = []Category{CategoryViews}
		r := f.run(cfg)
		// The jobs.create probe the dry runs depend on is reported too.
		if r.Summary != (Summary{Pass: len(companionViews) + 1}) || !hasCheck(r, CategoryIAM, "bigquery.jobs.create") {
			t.Errorf("Summary = %+v, want the view checks and the jobs.create probe:%s", r.Summary, listChecks(r))
		}
	})
}

func TestRunReturnsPartialReportWhenContextEnds(t *testing.T) {
	f := newFake(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := Run(ctx, f.config())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if r == nil || r.Conforms || len(r.Checks) == 0 {
		t.Fatalf("Run() report = %+v, want the failed checks made so far", r)
	}
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"project", func(c *Config) { c.Project = "Bad Project" }, `invalid BigQuery project ID "Bad Project"`},
		{"empty project", func(c *Config) { c.Project = "" }, `invalid BigQuery project ID ""`},
		{"dataset", func(c *Config) { c.Dataset = "my-dataset" }, `invalid BigQuery dataset ID "my-dataset"`},
		{"table", func(c *Config) { c.LogsTable = "logs.v2" }, `invalid BigQuery table ID "logs.v2"`},
		{"signal", func(c *Config) { c.Signals = []Signal{"profiles"} }, `unknown signal "profiles"`},
		{"category", func(c *Config) { c.Checks = []Category{"quota"} }, `unknown check category "quota"`},
		{"probe timeout", func(c *Config) { c.ProbeTimeout = -time.Second }, "probe timeout must not be negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Project: testProject, Dataset: testDataset}
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() = %v, want an error containing %q", err, tc.want)
			}
			r, err := Run(context.Background(), cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) || r != nil {
				t.Errorf("Run() = %v, %v; want no report and an error containing %q", r, err, tc.want)
			}
		})
	}
}

func TestConfigValidateLeavesConfigUnchanged(t *testing.T) {
	cfg := Config{Project: testProject, Dataset: testDataset}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	if !reflect.DeepEqual(cfg, Config{Project: testProject, Dataset: testDataset}) {
		t.Errorf("Validate() changed the config to %+v", cfg)
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{Project: "example.com:my-project", Dataset: testDataset}
	if err := cfg.normalize(); err != nil {
		t.Fatalf("normalize() = %v", err)
	}
	want := Config{
		Project:         "example.com:my-project",
		Dataset:         testDataset,
		Signals:         []Signal{SignalTraces, SignalLogs, SignalMetrics},
		TracesTable:     "otel_spans",
		LogsTable:       "otel_logs",
		MetricsTable:    "otel_metrics",
		QuarantineTable: "otel_quarantine",
		Checks:          []Category{CategorySchema, CategoryViews, CategoryIAM, CategoryStorageWrite},
		ProbeTimeout:    DefaultProbeTimeout,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("normalized config = %+v, want %+v", cfg, want)
	}
}

func TestParseSignals(t *testing.T) {
	got, err := ParseSignals(" logs, traces,logs ,")
	if want := []Signal{SignalLogs, SignalTraces}; err != nil || !slices.Equal(got, want) {
		t.Errorf("ParseSignals() = %v, %v; want %v", got, err, want)
	}
	for _, bad := range []string{"", " , ", "traces,profiles"} {
		if got, err := ParseSignals(bad); err == nil {
			t.Errorf("ParseSignals(%q) = %v, want an error", bad, got)
		}
	}
}

func TestParseCategories(t *testing.T) {
	got, err := ParseCategories("storage-write, schema,storage_write,views")
	if want := []Category{CategoryStorageWrite, CategorySchema, CategoryViews}; err != nil || !slices.Equal(got, want) {
		t.Errorf("ParseCategories() = %v, %v; want %v", got, err, want)
	}
	for _, bad := range []string{"", ",", "quota", "iam,quota"} {
		if got, err := ParseCategories(bad); err == nil {
			t.Errorf("ParseCategories(%q) = %v, want an error", bad, got)
		}
	}
}

func TestReportWriters(t *testing.T) {
	r := &Report{
		Project:       "p",
		Dataset:       "d",
		SchemaVersion: schema.Version,
		Summary:       Summary{Pass: 1, Warn: 1, Fail: 1, Skip: 1},
		Checks: []Check{
			{Category: CategorySchema, Name: "dataset", Resource: "p.d", Status: StatusPass, Detail: "dataset exists"},
			{Category: CategoryViews, Name: "v_gen_ai_logs", Resource: "p.d.v_gen_ai_logs", Status: StatusWarn,
				Detail: "not created", Remediation: "restart"},
			{Category: CategoryIAM, Name: "traces table permissions", Resource: "p.d.otel_spans", Status: StatusFail,
				Detail: "missing bigquery.tables.updateData", Remediation: "grant <role> & more"},
			{Category: CategoryStorageWrite, Name: "default stream", Resource: "p.d.otel_spans", Status: StatusSkip, Detail: "skipped"},
		},
	}
	var text bytes.Buffer
	if err := r.WriteText(&text); err != nil {
		t.Fatal(err)
	}
	want := "bq-otel-validate: project p, dataset d, schema " + schema.Version + "\n" +
		"Permissions are those of the identity running this command; run it as the collector's identity.\n\n" +
		"PASS  schema" + strings.Repeat(" ", 9) + "dataset (p.d): dataset exists\n" +
		"WARN  views" + strings.Repeat(" ", 10) + "v_gen_ai_logs (p.d.v_gen_ai_logs): not created\n" +
		"      fix: restart\n" +
		"FAIL  iam" + strings.Repeat(" ", 12) + "traces table permissions (p.d.otel_spans): missing bigquery.tables.updateData\n" +
		"      fix: grant <role> & more\n" +
		"SKIP  storage_write  default stream (p.d.otel_spans): skipped\n" +
		"\nNOT CONFORMANT: 1 passed, 1 warnings, 1 failed, 1 skipped\n"
	if text.String() != want {
		t.Errorf("WriteText() =\n%s\nwant\n%s", text.String(), want)
	}

	var raw bytes.Buffer
	if err := r.WriteJSON(&raw); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{`"conforms": false`, `"remediation": "grant <role> & more"`, `"storage_write"`} {
		if !strings.Contains(raw.String(), s) {
			t.Errorf("WriteJSON() = %s, want it to contain %s", raw.String(), s)
		}
	}
	var decoded struct {
		Report
		Checks []map[string]any `json:"checks"`
	}
	if err := json.Unmarshal(raw.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded.Checks[0]["remediation"]; ok {
		t.Errorf("a check without a remediation has a remediation key: %v", decoded.Checks[0])
	}
	var back Report
	if err := json.Unmarshal(raw.Bytes(), &back); err != nil || !reflect.DeepEqual(&back, r) {
		t.Errorf("WriteJSON() does not round-trip: %+v, %v", back, err)
	}

	r.Conforms = true
	text.Reset()
	if err := r.WriteText(&text); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(text.String(), "\nCONFORMANT: 1 passed, 1 warnings, 1 failed, 1 skipped\n") {
		t.Errorf("WriteText() of a conformant report ends %q", text.String()[strings.LastIndex(text.String(), "\n\n"):])
	}
}
