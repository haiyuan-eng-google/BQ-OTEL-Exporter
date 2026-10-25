// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
)

const (
	testProject = "test-project"
	testDataset = "telemetry"
)

var viewNames = []string{
	schema.SpansAsAllSpansViewName, schema.GenAISpansViewName, schema.GenAIModelCostsViewName,
	schema.GenAISessionsViewName, schema.GenAIToolUsageViewName, schema.GenAIAgentTrajectoryViewName,
	schema.MCPSpansViewName, schema.MCPToolUsageViewName, schema.GenAILogsViewName,
	schema.GenAIMetricsViewName, schema.GenAIMetricExemplarsViewName,
}

// fakeBigQuery answers the metadata, permission, and dry-run calls the
// command makes, for a dataset that holds objects. Every permission asked
// for is granted, and every dry run compiles.
type fakeBigQuery struct {
	mu         sync.Mutex
	objects    map[string]*bigqueryapi.Table
	dryRuns    int
	unexpected []string
}

// startFake starts a fake of a conformant dataset and points the command's
// client seams at it and at a bqfake Storage Write API server.
func startFake(t *testing.T) *fakeBigQuery {
	t.Helper()
	write := bqfake.NewServer(t)
	f := &fakeBigQuery{objects: map[string]*bigqueryapi.Table{}}
	for id, ts := range map[string]*storagepb.TableSchema{
		"otel_spans":      schema.SpansTableSchema(),
		"otel_logs":       schema.LogsTableSchema(),
		"otel_metrics":    schema.MetricsTableSchema(),
		"otel_quarantine": schema.QuarantineTableSchema(),
	} {
		if err := write.SeedTable(testProject, testDataset, id, ts); err != nil {
			t.Fatal(err)
		}
		tbl, ok := write.Table(testProject, testDataset, id)
		if !ok {
			t.Fatalf("bqfake has no table %s", id)
		}
		md, _, err := schema.DestinationMetadata(ts, testProject, testDataset, id)
		if id == "otel_quarantine" {
			md, err = schema.QuarantineDestinationMetadata()
		}
		if err != nil {
			t.Fatal(err)
		}
		if md.TimePartitioning != nil {
			tbl.TimePartitioning = &bigqueryapi.TimePartitioning{
				Type:  string(md.TimePartitioning.Type),
				Field: md.TimePartitioning.Field,
			}
		}
		if md.Clustering != nil {
			tbl.Clustering = &bigqueryapi.Clustering{Fields: md.Clustering.Fields}
		}
		f.objects[id] = tbl
	}
	for _, v := range viewNames {
		f.objects[v] = &bigqueryapi.Table{Type: "VIEW", View: &bigqueryapi.ViewDefinition{Query: "SELECT 1"}}
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	oldBigQuery, oldWrite := bigQueryClientOptions, writeClientOptions
	bigQueryClientOptions = func() []option.ClientOption {
		return []option.ClientOption{option.WithEndpoint(srv.URL + "/bigquery/v2/"), option.WithoutAuthentication()}
	}
	writeClientOptions = write.WriteClientOptions
	t.Cleanup(func() { bigQueryClientOptions, writeClientOptions = oldBigQuery, oldWrite })
	return f
}

func (f *fakeBigQuery) edit(change func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change()
}

// verify checks that the fake saw only the calls it serves, and the
// expected number of dry runs.
func (f *fakeBigQuery) verify(t *testing.T, wantDryRuns int) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.unexpected) > 0 {
		t.Errorf("unexpected calls: %q", f.unexpected)
	}
	if f.dryRuns != wantDryRuns {
		t.Errorf("dry runs = %d, want %d", f.dryRuns, wantDryRuns)
	}
}

func (f *fakeBigQuery) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	const (
		datasetPath = "/bigquery/v2/projects/" + testProject + "/datasets/" + testDataset
		tablesPath  = datasetPath + "/tables/"
	)
	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && path == datasetPath:
		writeJSON(w, http.StatusOK, &bigqueryapi.Dataset{
			DatasetReference: &bigqueryapi.DatasetReference{ProjectId: testProject, DatasetId: testDataset},
		})
	case r.Method == http.MethodPost && strings.HasPrefix(path, tablesPath) && strings.HasSuffix(path, ":testIamPermissions"):
		var req bigqueryapi.TestIamPermissionsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			f.unexpected = append(f.unexpected, "undecodable testIamPermissions body")
		}
		writeJSON(w, http.StatusOK, &bigqueryapi.TestIamPermissionsResponse{Permissions: req.Permissions})
	case r.Method == http.MethodGet && strings.HasPrefix(path, tablesPath):
		tbl, ok := f.objects[strings.TrimPrefix(path, tablesPath)]
		if !ok {
			writeError(w, http.StatusNotFound, "notFound", "Not found: Table "+strings.TrimPrefix(path, tablesPath))
			return
		}
		writeJSON(w, http.StatusOK, tbl)
	case r.Method == http.MethodPost && path == "/bigquery/v2/projects/"+testProject+"/jobs":
		var job bigqueryapi.Job
		if err := json.NewDecoder(r.Body).Decode(&job); err != nil || job.Configuration == nil || !job.Configuration.DryRun {
			f.unexpected = append(f.unexpected, "a job that is not a dry run")
			writeError(w, http.StatusBadRequest, "invalid", "only dry runs are expected")
			return
		}
		f.dryRuns++
		job.Status = &bigqueryapi.JobStatus{State: "DONE"}
		writeJSON(w, http.StatusOK, &job)
	default:
		f.unexpected = append(f.unexpected, r.Method+" "+path)
		writeError(w, http.StatusBadRequest, "invalid", "unexpected request")
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, reason, message string) {
	writeJSON(w, code, map[string]any{"error": map[string]any{
		"code":    code,
		"message": message,
		"errors":  []map[string]string{{"reason": reason, "message": message}},
	}})
}

func runCommand(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(context.Background(), args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestConformantDataset(t *testing.T) {
	f := startFake(t)
	code, stdout, stderr := runCommand(t, "--project="+testProject, "--dataset="+testDataset)
	if code != 0 || stderr != "" {
		t.Fatalf("exit %d, stderr %q, stdout:\n%s", code, stderr, stdout)
	}
	for _, want := range []string{
		"bq-otel-validate: project test-project, dataset telemetry, schema " + schema.Version + "\n",
		"PASS  schema         traces table (test-project.telemetry.otel_spans): matches schema " + schema.Version + "\n",
		"PASS  views          v_gen_ai_sessions (test-project.telemetry.v_gen_ai_sessions): dry run with a filter on session_start_date compiles",
		"PASS  iam            traces table permissions (test-project.telemetry.otel_spans): has bigquery.tables.get and bigquery.tables.updateData\n",
		"PASS  storage_write  default stream (test-project.telemetry.otel_spans): GetWriteStream answered",
		"\nCONFORMANT: 23 passed, 0 warnings, 0 failed, 0 skipped\n",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, stdout)
		}
	}
	// The jobs.create probe and one dry run per view.
	f.verify(t, 1+len(viewNames))
}

func TestJSONReport(t *testing.T) {
	f := startFake(t)
	code, stdout, stderr := runCommand(t, "-project", testProject, "-dataset", testDataset,
		"-format=json", "-signals=traces", "-quarantine=false")
	if code != 0 || stderr != "" {
		t.Fatalf("exit %d, stderr %q, stdout:\n%s", code, stderr, stdout)
	}
	var report struct {
		Conforms bool `json:"conforms"`
		Summary  struct {
			Pass int `json:"pass"`
			Skip int `json:"skip"`
		} `json:"summary"`
		Checks []struct {
			Category string `json:"category"`
			Name     string `json:"name"`
			Status   string `json:"status"`
		} `json:"checks"`
	}
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("stdout is not a JSON report: %v\n%s", err, stdout)
	}
	// dataset and traces table, 8 trace views (3 views read logs or
	// metrics and are skipped), datasets.get, jobs.create, and traces
	// table permissions, and the write probe.
	if !report.Conforms || report.Summary.Pass != 14 || report.Summary.Skip != 3 {
		t.Errorf("report = %+v, want a conformant report of 14 passes and 3 skips", report)
	}
	for _, c := range report.Checks {
		if c.Name == "logs table" || c.Name == "metrics table" || c.Name == "quarantine table" {
			t.Errorf("traces-only run without quarantine checked the %s", c.Name)
		}
	}
	f.verify(t, 1+8)
}

func TestNonConformantDataset(t *testing.T) {
	f := startFake(t)
	f.edit(func() { delete(f.objects, "otel_logs") })
	code, stdout, stderr := runCommand(t, "-project="+testProject, "-dataset="+testDataset, "-checks=schema,iam")
	if code != 1 || stderr != "" {
		t.Fatalf("exit %d, stderr %q, want exit 1 with the failures on stdout:\n%s", code, stderr, stdout)
	}
	for _, want := range []string{
		"FAIL  schema         logs table (test-project.telemetry.otel_logs): table not found\n",
		"      fix: Create the table before routing traffic",
		"SKIP  iam            logs table permissions (test-project.telemetry.otel_logs): not checked: table not found\n",
		"\nNOT CONFORMANT: 8 passed, 0 warnings, 1 failed, 1 skipped\n",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, stdout)
		}
	}
	f.verify(t, 0)

	// An IAM-only run when tables do not exist must fail (exit 1, conforms=false)
	// rather than reporting conformant with skipped checks.
	f.edit(func() {
		for _, id := range []string{"otel_spans", "otel_logs", "otel_metrics", "otel_quarantine"} {
			delete(f.objects, id)
		}
	})
	code, stdout, stderr = runCommand(t, "-project="+testProject, "-dataset="+testDataset, "-checks=iam", "-format=json")
	if code != 1 || stderr != "" {
		t.Fatalf("IAM-only missing tables: exit %d, stderr %q, want exit 1:\n%s", code, stderr, stdout)
	}
	var iamReport struct {
		Conforms bool `json:"conforms"`
		Summary  struct {
			Pass int `json:"pass"`
			Fail int `json:"fail"`
			Skip int `json:"skip"`
		} `json:"summary"`
	}
	if err := json.Unmarshal([]byte(stdout), &iamReport); err != nil {
		t.Fatalf("IAM-only JSON decode: %v\n%s", err, stdout)
	}
	if iamReport.Conforms || iamReport.Summary.Fail != 4 || iamReport.Summary.Skip != 0 {
		t.Errorf("IAM-only report = %+v, want conforms=false fail=4 skip=0", iamReport)
	}
}

func TestCustomTableNames(t *testing.T) {
	f := startFake(t)
	f.edit(func() {
		f.objects["spans_v2"] = f.objects["otel_spans"]
		delete(f.objects, "otel_spans")
	})
	code, stdout, stderr := runCommand(t, "-project="+testProject, "-dataset="+testDataset,
		"-traces-table=spans_v2", "-signals=traces", "-quarantine=false", "-checks=schema")
	if code != 0 || stderr != "" {
		t.Fatalf("exit %d, stderr %q, stdout:\n%s", code, stderr, stdout)
	}
	for _, want := range []string{
		"traces table (test-project.telemetry.spans_v2): matches schema",
		"\nCONFORMANT: 2 passed, 0 warnings, 0 failed, 0 skipped\n",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, stdout)
		}
	}
	f.verify(t, 0)
}

func TestRunThatDoesNotFinish(t *testing.T) {
	startFake(t)
	code, stdout, stderr := runCommand(t, "-project="+testProject, "-dataset="+testDataset, "-timeout=1ns")
	if code != 1 || !strings.Contains(stderr, "the run did not finish") || !strings.Contains(stderr, "deadline exceeded") {
		t.Errorf("exit %d, stderr %q, want exit 1 and the deadline", code, stderr)
	}
	if !strings.Contains(stdout, "NOT CONFORMANT") {
		t.Errorf("stdout does not report the checks made so far:\n%s", stdout)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no flags", nil, "-project and -dataset are required"},
		{"no dataset", []string{"-project=p"}, "-project and -dataset are required"},
		{"stray argument", []string{"-project=p", "-dataset=d", "extra"}, `unexpected arguments ["extra"]`},
		{"unknown flag", []string{"-bogus"}, "flag provided but not defined: -bogus"},
		{"signals", []string{"-project=p", "-dataset=d", "-signals=profiles"}, `invalid -signals: unknown signal "profiles"`},
		{"checks", []string{"-project=p", "-dataset=d", "-checks=quota"}, `invalid -checks: unknown check "quota"`},
		{"format", []string{"-project=p", "-dataset=d", "-format=yaml"}, `invalid -format "yaml": want text or json`},
		{"timeout", []string{"-project=p", "-dataset=d", "-timeout=0s"}, "invalid -timeout 0s"},
		{"probe timeout", []string{"-project=p", "-dataset=d", "-probe-timeout=-1s"}, "invalid -probe-timeout -1s"},
		{"project ID", []string{"-project=Bad Project", "-dataset=d"}, `invalid BigQuery project ID "Bad Project"`},
		{"table ID", []string{"-project=p", "-dataset=d", "-logs-table=a.b"}, `invalid BigQuery table ID "a.b"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runCommand(t, tc.args...)
			if code != 2 || stdout != "" || !strings.Contains(stderr, tc.want) {
				t.Errorf("exit %d, stdout %q, stderr %q; want exit 2 and %q", code, stdout, stderr, tc.want)
			}
		})
	}
}
