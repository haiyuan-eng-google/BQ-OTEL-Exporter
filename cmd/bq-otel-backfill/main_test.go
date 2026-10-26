// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/redact"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
)

const (
	testProject    = "test-project"
	testDataset    = "telemetry"
	testBucket     = "archive-bucket"
	testSource     = "gs://" + testBucket + "/exports/"
	testGeneration = "1700000000000000"
)

// Archive lines as the Collector's file exporter writes them, one OTLP/JSON
// export request per line, recorded in March 2025.
const (
	// spansLine holds two spans of service checkout. The first carries
	// values that only the redaction flags mask.
	spansLine = `{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeSpans":[{"spans":[` +
		`{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","name":"POST /cart","kind":2,` +
		`"startTimeUnixNano":"1741944413000000000","endTimeUnixNano":"1741944414000000000","attributes":[` +
		`{"key":"x-session","value":{"stringValue":"session-cleartext"}},` +
		`{"key":"x-tenant-key","value":{"stringValue":"tenant-cleartext"}},` +
		`{"key":"note","value":{"stringValue":"card 4111-1111-1111-1111 on file"}}]},` +
		`{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b175","parentSpanId":"eee19b7ec3c1b174","name":"SELECT cart","kind":3,` +
		`"startTimeUnixNano":"1741944413100000000","endTimeUnixNano":"1741944413900000000"}]}]}]}`
	// logsLine holds one log record whose attributes carry a log name and
	// an upstream record ID that only the log identity flags select.
	logsLine = `{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeLogs":[{"logRecords":[` +
		`{"timeUnixNano":"1742022000000000000","severityNumber":9,"severityText":"INFO","body":{"stringValue":"login token hunter2-body accepted"},` +
		`"attributes":[{"key":"app.log","value":{"stringValue":"audit"}},{"key":"app.record_id","value":{"stringValue":"rec-42"}}]}]}]}]}`
	// metricsLine holds one sum data point.
	metricsLine = `{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeMetrics":[{"metrics":[` +
		`{"name":"http.server.request.count","sum":{"aggregationTemporality":2,"isMonotonic":true,` +
		`"dataPoints":[{"timeUnixNano":"1742149800000000000","asInt":"3"}]}}]}]}]}`
)

func jsonl(lines ...string) []byte {
	return []byte(strings.Join(lines, "\n") + "\n")
}

func gzipped(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// fakeGCS serves the Cloud Storage JSON API calls a backfill makes: an
// objects.list of a single page, and media downloads of the listed
// generation.
type fakeGCS struct {
	mu         sync.Mutex
	objects    map[string][]byte
	listStatus int
	unexpected []string
}

func (f *fakeGCS) put(name string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[name] = data
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	const listPath = "/storage/v1/b/" + testBucket + "/o"
	path, q := r.URL.EscapedPath(), r.URL.Query()
	switch {
	case r.Method == http.MethodGet && path == listPath && f.listStatus != 0:
		writeGCSError(w, f.listStatus, "list refused")
	case r.Method == http.MethodGet && path == listPath:
		type item struct {
			Name       string `json:"name"`
			Generation string `json:"generation"`
			Size       string `json:"size"`
		}
		var page struct {
			Items []item `json:"items"`
		}
		for _, name := range slices.Sorted(maps.Keys(f.objects)) {
			if strings.HasPrefix(name, q.Get("prefix")) {
				page.Items = append(page.Items, item{name, testGeneration, strconv.Itoa(len(f.objects[name]))})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)
	case r.Method == http.MethodGet && strings.HasPrefix(path, listPath+"/") &&
		q.Get("alt") == "media" && q.Get("generation") == testGeneration:
		name, err := url.PathUnescape(strings.TrimPrefix(path, listPath+"/"))
		data, ok := f.objects[name]
		if err != nil || !ok {
			f.unexpected = append(f.unexpected, "download of an unlisted object "+path)
			writeGCSError(w, http.StatusNotFound, "no such object")
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	default:
		f.unexpected = append(f.unexpected, r.Method+" "+r.URL.String())
		writeGCSError(w, http.StatusBadRequest, "unexpected request")
	}
}

func writeGCSError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": message}})
}

// testEnv is an archive bucket and a Storage Write API to append to.
type testEnv struct {
	gcs *fakeGCS
	bq  *bqfake.Server
}

// startEnv starts the fakes and points the command's client seams at them.
func startEnv(t *testing.T) *testEnv {
	t.Helper()
	e := &testEnv{gcs: &fakeGCS{objects: map[string][]byte{}}, bq: bqfake.NewServer(t)}
	srv := httptest.NewServer(e.gcs)
	t.Cleanup(func() {
		srv.Close()
		e.gcs.mu.Lock()
		defer e.gcs.mu.Unlock()
		if len(e.gcs.unexpected) > 0 {
			t.Errorf("unexpected Cloud Storage requests: %q", e.gcs.unexpected)
		}
	})
	oldStorage, oldWrite := storageClientOptions, writeClientOptions
	storageClientOptions = func() []option.ClientOption {
		return []option.ClientOption{option.WithEndpoint(srv.URL), option.WithoutAuthentication()}
	}
	writeClientOptions = e.bq.WriteClientOptions
	t.Cleanup(func() { storageClientOptions, writeClientOptions = oldStorage, oldWrite })
	return e
}

func runCommand(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(context.Background(), args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// required are the flags every run needs, for the test source and dataset.
var required = []string{"-source-gcs-prefix=" + testSource, "-project=" + testProject, "-dataset=" + testDataset}

func withRequired(args ...string) []string {
	return append(slices.Clone(required), args...)
}

// column returns one column of rows, sorted.
func column(rows []map[string]any, name string) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, fmt.Sprint(row[name]))
	}
	slices.Sort(out)
	return out
}

func TestBackfillArchive(t *testing.T) {
	e := startEnv(t)
	e.gcs.put("exports/2025-03-14.jsonl", jsonl(spansLine, logsLine, metricsLine))
	e.gcs.put("exports/2025-03-15.jsonl.gz", gzipped(t, jsonl(logsLine)))
	e.gcs.put("exports/README.md", []byte("not an archive\n"))
	e.gcs.put("staging/2025-03-14.jsonl", jsonl(spansLine))

	code, stdout, stderr := runCommand(t, required...)
	if code != 0 {
		t.Fatalf("exit %d, stderr %q, stdout:\n%s", code, stderr, stdout)
	}
	for _, want := range []string{
		"backfill gs://archive-bucket/exports/ -> test-project.telemetry\n",
		"objects  2 archives, 1 other objects skipped, 0 with failures\n",
		"lines    4 read, 0 malformed, 0 of other signals\n",
		"traces   otel_spans: 2 spans, 2 appended, 0 rejected, 0 unwritten\n",
		"logs     otel_logs: 2 log records, 2 appended, 0 rejected, 0 unwritten\n",
		"metrics  otel_metrics: 1 data points, 1 appended, 0 rejected, 0 unwritten\n",
		"\nCOMPLETE: 5 rows appended\n",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, stdout)
		}
	}
	// The objects are read at once, so they can finish in either order.
	progress := strings.SplitAfter(stderr, "\n")
	slices.Sort(progress)
	if want := []string{
		"",
		"bq-otel-backfill: gs://archive-bucket/exports/2025-03-14.jsonl: 3 lines, 4 rows appended, done\n",
		"bq-otel-backfill: gs://archive-bucket/exports/2025-03-15.jsonl.gz: 1 line, 1 row appended, done\n",
	}; !slices.Equal(progress, want) {
		t.Errorf("stderr = %q, want a progress line per archive", stderr)
	}

	for table, want := range map[string]int{"otel_spans": 2, "otel_logs": 2, "otel_metrics": 1} {
		if got := e.bq.RowCount(table); got != want {
			t.Errorf("%s holds %d rows, want %d", table, got, want)
		}
	}
	// Without the redaction and log identity flags, the exporter's defaults
	// apply: x-session is not a blocked key, and the log name falls back to
	// the service name.
	if dump := fmt.Sprint(e.bq.Rows("otel_spans")); !strings.Contains(dump, "session-cleartext") {
		t.Errorf("the default redaction masked x-session: %s", dump)
	}
	logs := e.bq.Rows("otel_logs")
	if got := column(logs, "log_name"); !slices.Equal(got, []string{"checkout", "checkout"}) {
		t.Errorf("log_name = %q, want the service name", got)
	}
	if got := column(logs, "insert_id"); slices.Contains(got, "rec-42") {
		t.Errorf("insert_id = %q, want the record fingerprint", got)
	}
}

func TestRedactionAndLogIdentityFlags(t *testing.T) {
	e := startEnv(t)
	e.gcs.put("exports/2025-03-14.jsonl", jsonl(spansLine, logsLine))

	code, stdout, stderr := runCommand(t, withRequired(
		"-redact-keys=x-session, X-Tenant-Key",
		`-redact-value-pattern=\d{4}-\d{4}-\d{4}-\d{4}`,
		"-redact-value-pattern=hunter2-[a-z]+",
		"-redact-log-body",
		"-log-name-attribute=app.log",
		"-insert-id-attribute=app.record_id",
	)...)
	if code != 0 {
		t.Fatalf("exit %d, stderr %q, stdout:\n%s", code, stderr, stdout)
	}

	spans, logs := fmt.Sprint(e.bq.Rows("otel_spans")), fmt.Sprint(e.bq.Rows("otel_logs"))
	for _, secret := range []string{"session-cleartext", "tenant-cleartext", "4111-1111-1111-1111", "hunter2-body"} {
		if strings.Contains(spans, secret) || strings.Contains(logs, secret) {
			t.Errorf("a row holds %q in the clear:\nspans %s\nlogs %s", secret, spans, logs)
		}
	}
	if want := "card " + redact.DefaultMaskReplacement + " on file"; !strings.Contains(spans, want) {
		t.Errorf("span rows do not hold %q: %s", want, spans)
	}
	if want := "login token " + redact.DefaultMaskReplacement + " accepted"; !strings.Contains(logs, want) {
		t.Errorf("log rows do not hold %q: %s", want, logs)
	}
	rows := e.bq.Rows("otel_logs")
	if got := column(rows, "log_name"); !slices.Equal(got, []string{"audit"}) {
		t.Errorf("log_name = %q, want the -log-name-attribute value", got)
	}
	if got := column(rows, "insert_id"); !slices.Equal(got, []string{"rec-42"}) {
		t.Errorf("insert_id = %q, want the -insert-id-attribute value", got)
	}

	// Verify -redact, -redact-disable-default-keys, and -redact-mask-replacement.
	e2 := startEnv(t)
	e2.gcs.put("exports/2025-03-14.jsonl", jsonl(spansLine))
	code, stdout, stderr = runCommand(t, withRequired(
		"-redact",
		"-redact-disable-default-keys",
		"-redact-keys=x-session",
		"-redact-mask-replacement=<MASKED>",
	)...)
	if code != 0 {
		t.Fatalf("exit %d, stderr %q, stdout:\n%s", code, stderr, stdout)
	}
	spans2 := fmt.Sprint(e2.bq.Rows("otel_spans"))
	if strings.Contains(spans2, "session-cleartext") || !strings.Contains(spans2, "<MASKED>") {
		t.Errorf("custom mask replacement not applied to x-session: %s", spans2)
	}

	// Explicit -redact=false is authoritative even when other -redact-* flags
	// (such as -redact-mask-replacement=[REDACTED] or -redact-keys) are passed.
	e3 := startEnv(t)
	e3.gcs.put("exports/2025-03-14.jsonl", jsonl(spansLine))
	code, stdout, stderr = runCommand(t, withRequired(
		"-redact=false",
		"-redact-mask-replacement=[REDACTED]",
		"-redact-keys=x-session",
	)...)
	if code != 0 {
		t.Fatalf("exit %d, stderr %q, stdout:\n%s", code, stderr, stdout)
	}
	spans3 := fmt.Sprint(e3.bq.Rows("otel_spans"))
	if !strings.Contains(spans3, "session-cleartext") || strings.Contains(spans3, "[REDACTED]") {
		t.Errorf("explicit -redact=false still redacted spans: %s", spans3)
	}
}

func TestSignalTableAndBatchFlags(t *testing.T) {
	e := startEnv(t)
	e.gcs.put("exports/2025-03-14.jsonl", jsonl(spansLine, logsLine, logsLine, metricsLine))

	code, stdout, stderr := runCommand(t, withRequired(
		"-signal=logs", "-logs-table=archive_logs", "-batch-size=1", "-max-workers=1", "-format=json",
	)...)
	if code != 0 {
		t.Fatalf("exit %d, stderr %q, stdout:\n%s", code, stderr, stdout)
	}
	var report struct {
		Lines            int `json:"lines"`
		OtherSignalLines int `json:"other_signal_lines"`
		FailureCount     int `json:"failure_count"`
		Signals          []struct {
			Signal   string `json:"signal"`
			Table    string `json:"table"`
			Appended int    `json:"appended"`
		} `json:"signals"`
	}
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("stdout is not a JSON report: %v\n%s", err, stdout)
	}
	if report.Lines != 4 || report.OtherSignalLines != 2 || report.FailureCount != 0 || len(report.Signals) != 1 ||
		report.Signals[0].Signal != "logs" || report.Signals[0].Table != "archive_logs" || report.Signals[0].Appended != 2 {
		t.Errorf("report = %+v, want 2 logs appended to archive_logs and 2 lines of other signals", report)
	}
	for table, want := range map[string]int{"archive_logs": 2, "otel_logs": 0, "otel_spans": 0, "otel_metrics": 0} {
		if got := e.bq.RowCount(table); got != want {
			t.Errorf("%s holds %d rows, want %d", table, got, want)
		}
	}
	// One append per row.
	if _, requests, _ := e.bq.Counts(); requests != 2 {
		t.Errorf("append requests = %d, want 2 at -batch-size=1", requests)
	}
}

func TestMalformedLine(t *testing.T) {
	e := startEnv(t)
	e.gcs.put("exports/bad.jsonl", jsonl(spansLine, "not json"))

	code, stdout, stderr := runCommand(t, required...)
	if code != 1 {
		t.Fatalf("exit %d, stderr %q, want exit 1; stdout:\n%s", code, stderr, stdout)
	}
	for _, want := range []string{
		"lines    2 read, 1 malformed, 0 of other signals\n",
		"FAIL     gs://archive-bucket/exports/bad.jsonl line 2: ",
		"\nINCOMPLETE: 2 rows appended, 1 failures\n",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, stdout)
		}
	}
	if want := "bq-otel-backfill: gs://archive-bucket/exports/bad.jsonl: 2 lines, 2 rows appended, 1 failure\n"; stderr != want {
		t.Errorf("stderr = %q, want %q", stderr, want)
	}
}

func TestRunThatDoesNotFinish(t *testing.T) {
	e := startEnv(t)
	e.gcs.put("exports/2025-03-14.jsonl", jsonl(spansLine))
	e.gcs.mu.Lock()
	e.gcs.listStatus = http.StatusForbidden
	e.gcs.mu.Unlock()

	code, stdout, stderr := runCommand(t, required...)
	if want := "bq-otel-backfill: the run did not finish: listing gs://archive-bucket/exports/: HTTP 403: list refused\n"; code != 1 || stderr != want {
		t.Errorf("exit %d, stderr %q; want exit 1 and %q", code, stderr, want)
	}
	for _, want := range []string{
		"STOPPED  listing gs://archive-bucket/exports/: HTTP 403: list refused\n",
		"\nINCOMPLETE: 0 rows appended, 0 failures\n",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, stdout)
		}
	}
	if n := e.bq.RowCount("otel_spans"); n != 0 {
		t.Errorf("otel_spans holds %d rows, want 0", n)
	}
}

func TestRunThatCannotStart(t *testing.T) {
	startEnv(t)
	storageClientOptions = func() []option.ClientOption {
		return []option.ClientOption{option.WithoutAuthentication(), option.WithAPIKey("key")}
	}
	code, stdout, stderr := runCommand(t, required...)
	if code != 1 || stdout != "" || !strings.HasPrefix(stderr, "bq-otel-backfill: creating the Cloud Storage client: ") {
		t.Errorf("exit %d, stdout %q, stderr %q; want exit 1 and the client error alone", code, stdout, stderr)
	}
}

func TestNoProgress(t *testing.T) {
	e := startEnv(t)
	e.gcs.put("exports/2025-03-14.jsonl", jsonl(metricsLine))

	code, stdout, stderr := runCommand(t, withRequired("-progress=false")...)
	if code != 0 || stderr != "" || !strings.Contains(stdout, "\nCOMPLETE: 1 rows appended\n") {
		t.Errorf("exit %d, stderr %q, stdout:\n%s", code, stderr, stdout)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no flags", nil, "-source-gcs-prefix, -project, and -dataset are required"},
		{"no dataset", []string{"-source-gcs-prefix=" + testSource, "-project=p"}, "-source-gcs-prefix, -project, and -dataset are required"},
		{"stray argument", withRequired("extra"), `unexpected arguments ["extra"]`},
		{"unknown flag", []string{"-bogus"}, "flag provided but not defined: -bogus"},
		{"format", withRequired("-format=yaml"), `invalid -format "yaml": want text or json`},
		{"batch size", withRequired("-batch-size=0"), "invalid -batch-size 0: want a positive number"},
		{"max workers", withRequired("-max-workers=-1"), "invalid -max-workers -1: want a positive number"},
		{"max attempts", withRequired("-max-attempts=0"), "invalid -max-attempts 0: want a positive number"},
		{"max line bytes", withRequired("-max-line-bytes=0"), "invalid -max-line-bytes 0: want a positive number"},
		{"signal", withRequired("-signal=profiles"), `signal "profiles": want traces, logs, metrics, or auto`},
		{"source scheme", []string{"-source-gcs-prefix=s3://bucket/x", "-project=p", "-dataset=d"}, `source "s3://bucket/x": want gs://bucket/prefix`},
		{"source bucket", []string{"-source-gcs-prefix=gs:///x", "-project=p", "-dataset=d"}, `source "gs:///x" names no bucket`},
		{"value pattern", withRequired("-redact-value-pattern=("), "redaction: "},
		{"blocked insert_id_attribute", withRequired("-redact", "-insert-id-attribute=authorization"), `insert_id_attribute "authorization" matches a blocked redaction key`},
		{"blocked log_name_attribute", withRequired("-redact", "-redact-disable-default-keys", "-redact-keys=session_*", "-log-name-attribute=session_stream"), `log_name_attribute "session_stream" matches a blocked redaction key`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runCommand(t, tc.args...)
			if code != 2 || stdout != "" || !strings.Contains(stderr, tc.want) {
				t.Errorf("exit %d, stdout %q, stderr %q; want exit 2 and %q", code, stdout, stderr, tc.want)
			}
		})
	}
}

func TestCount(t *testing.T) {
	for n, want := range map[int64]string{0: "0 lines", 1: "1 line", 2: "2 lines"} {
		if got := count(n, "line"); got != want {
			t.Errorf("count(%d) = %q, want %q", n, got, want)
		}
	}
}
