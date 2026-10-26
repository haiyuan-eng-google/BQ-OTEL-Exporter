// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/api/option"

	bigqueryexporter "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"
	bqi "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/protoenc"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/redact"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
)

const (
	testProject = "test-project"
	testDataset = "telemetry"
	testBucket  = "archive-bucket"
	testSource  = "gs://" + testBucket + "/exports/"
)

// The archive holds telemetry recorded long before the run, so the tests
// can tell the record times a row must keep from the time it was appended.
var (
	spanStart   = time.Date(2025, time.March, 14, 9, 26, 53, 589793000, time.UTC)
	logTime     = time.Date(2025, time.March, 15, 7, 0, 0, 123456000, time.UTC)
	metricTime  = time.Date(2025, time.March, 16, 18, 30, 0, 0, time.UTC)
	receiveTime = time.Date(2026, time.October, 25, 12, 0, 0, 0, time.UTC)
)

// newSpans returns traces of service checkout holding n spans, the i-th
// named prefix-i and starting i seconds after start.
func newSpans(n int, prefix string, start time.Time) (ptrace.Traces, ptrace.SpanSlice) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for i := range n {
		sp := spans.AppendEmpty()
		sp.SetName(fmt.Sprintf("%s-%d", prefix, i))
		sp.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		sp.SetSpanID(pcommon.SpanID{1, 0, 0, 0, 0, 0, byte(i >> 8), byte(i)})
		begin := start.Add(time.Duration(i) * time.Second)
		sp.SetStartTimestamp(pcommon.NewTimestampFromTime(begin))
		sp.SetEndTimestamp(pcommon.NewTimestampFromTime(begin.Add(250 * time.Millisecond)))
	}
	return td, spans
}

func tracesJSON(t *testing.T, td ptrace.Traces) string {
	t.Helper()
	raw, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func spansLine(t *testing.T, n int, prefix string, start time.Time) string {
	t.Helper()
	td, _ := newSpans(n, prefix, start)
	return tracesJSON(t, td)
}

// newLogs returns logs of service checkout holding n records, the i-th
// with body "body i" and stamped i seconds after ts.
func newLogs(n int, body string, ts time.Time) (plog.Logs, plog.LogRecordSlice) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "checkout")
	records := rl.ScopeLogs().AppendEmpty().LogRecords()
	for i := range n {
		lr := records.AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(ts.Add(time.Duration(i) * time.Second)))
		lr.SetSeverityText("INFO")
		lr.Body().SetStr(fmt.Sprintf("%s %d", body, i))
	}
	return ld, records
}

func logsJSON(t *testing.T, ld plog.Logs) string {
	t.Helper()
	raw, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func logsLine(t *testing.T, n int, body string, ts time.Time) string {
	t.Helper()
	ld, _ := newLogs(n, body, ts)
	return logsJSON(t, ld)
}

// newMetrics returns one gauge of service checkout with n data points, the
// i-th stamped i minutes after ts.
func newMetrics(n int, ts time.Time) (pmetric.Metrics, pmetric.NumberDataPointSlice) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "checkout")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("queue.depth")
	points := m.SetEmptyGauge().DataPoints()
	for i := range n {
		dp := points.AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(ts.Add(time.Duration(i) * time.Minute)))
		dp.SetIntValue(int64(i))
	}
	return md, points
}

func metricsJSON(t *testing.T, md pmetric.Metrics) string {
	t.Helper()
	raw, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(md)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func metricsLine(t *testing.T, n int, ts time.Time) string {
	t.Helper()
	md, _ := newMetrics(n, ts)
	return metricsJSON(t, md)
}

// jsonl joins lines into a JSON Lines body.
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

// fakeObject is one object in fakeGCS.
type fakeObject struct {
	stored     []byte
	generation int64
	// transcode marks an object stored gzip-compressed with
	// Content-Encoding: gzip. Cloud Storage serves it decompressed unless
	// the request accepts gzip; plain holds the decompressed bytes.
	transcode bool
	plain     []byte
	// failures answers that many downloads with 503 before serving it.
	failures int
	// status, when nonzero, answers every download with it.
	status int
}

func transcoded(t *testing.T) func(*fakeObject) {
	return func(o *fakeObject) {
		o.plain = o.stored
		o.stored = gzipped(t, o.stored)
		o.transcode = true
	}
}

func failingFirst(n int) func(*fakeObject) {
	return func(o *fakeObject) { o.failures = n }
}

func answering(status int) func(*fakeObject) {
	return func(o *fakeObject) { o.status = status }
}

// fakeGCS serves the two Cloud Storage JSON API calls a run makes:
// objects.list, two objects to a page, and media downloads.
type fakeGCS struct {
	srv *httptest.Server

	mu          sync.Mutex
	objects     map[string]*fakeObject
	listStatus  int
	listQueries []url.Values
	downloads   map[string]int
	// barrier makes each download wait, for up to five seconds, until two
	// downloads have been in progress at once.
	barrier     bool
	inflight    int
	maxInflight int
	unexpected  []string
}

func newFakeGCS(t *testing.T) *fakeGCS {
	t.Helper()
	f := &fakeGCS{objects: map[string]*fakeObject{}, downloads: map[string]int{}}
	f.srv = httptest.NewServer(f)
	t.Cleanup(func() {
		f.srv.Close()
		f.mu.Lock()
		defer f.mu.Unlock()
		if len(f.unexpected) > 0 {
			t.Errorf("unexpected Cloud Storage requests: %q", f.unexpected)
		}
	})
	return f
}

func (f *fakeGCS) put(name string, data []byte, opts ...func(*fakeObject)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj := &fakeObject{stored: data, generation: int64(1000 + len(f.objects))}
	for _, opt := range opts {
		opt(obj)
	}
	f.objects[name] = obj
}

func (f *fakeGCS) edit(change func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change()
}

func (f *fakeGCS) downloadCount(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.downloads[name]
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const listPath = "/storage/v1/b/" + testBucket + "/o"
	path := r.URL.EscapedPath()
	switch {
	case r.Method == http.MethodGet && path == listPath:
		f.list(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(path, listPath+"/") && r.URL.Query().Get("alt") == "media":
		escaped := strings.TrimPrefix(path, listPath+"/")
		name, err := url.PathUnescape(escaped)
		// The JSON API takes the object name as one path segment.
		if err != nil || strings.Contains(escaped, "/") {
			f.unexpect(w, r)
			return
		}
		f.download(w, r, name)
	default:
		f.unexpect(w, r)
	}
}

func (f *fakeGCS) unexpect(w http.ResponseWriter, r *http.Request) {
	f.edit(func() { f.unexpected = append(f.unexpected, r.Method+" "+r.URL.String()) })
	writeGCSError(w, http.StatusBadRequest, "unexpected request")
}

func (f *fakeGCS) list(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query()
	f.listQueries = append(f.listQueries, q)
	if f.listStatus != 0 {
		writeGCSError(w, f.listStatus, "list refused")
		return
	}
	var names []string
	for name := range f.objects {
		if strings.HasPrefix(name, q.Get("prefix")) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	start := 0
	if tok := q.Get("pageToken"); tok != "" {
		var err error
		if start, err = strconv.Atoi(tok); err != nil || start > len(names) {
			writeGCSError(w, http.StatusBadRequest, "bad page token")
			return
		}
	}
	end := min(start+2, len(names))
	type item struct {
		Name       string `json:"name"`
		Generation string `json:"generation"`
		Size       string `json:"size"`
	}
	var page struct {
		Items         []item `json:"items,omitempty"`
		NextPageToken string `json:"nextPageToken,omitempty"`
	}
	for _, name := range names[start:end] {
		obj := f.objects[name]
		page.Items = append(page.Items, item{name, strconv.FormatInt(obj.generation, 10), strconv.Itoa(len(obj.stored))})
	}
	if end < len(names) {
		page.NextPageToken = strconv.Itoa(end)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(page)
}

func (f *fakeGCS) download(w http.ResponseWriter, r *http.Request, name string) {
	f.mu.Lock()
	f.downloads[name]++
	f.inflight++
	f.maxInflight = max(f.maxInflight, f.inflight)
	barrier := f.barrier
	status, encoding := http.StatusOK, ""
	var body []byte
	obj, ok := f.objects[name]
	switch {
	case !ok:
		status = http.StatusNotFound
	case r.URL.Query().Get("generation") != strconv.FormatInt(obj.generation, 10):
		f.unexpected = append(f.unexpected, "download of "+name+" without its listed generation")
		status = http.StatusNotFound
	case obj.status != 0:
		status = obj.status
	case obj.failures > 0:
		obj.failures--
		status = http.StatusServiceUnavailable
	case obj.transcode && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip"):
		body, encoding = obj.stored, "gzip"
	case obj.transcode:
		body = obj.plain
	default:
		body = obj.stored
	}
	f.mu.Unlock()
	defer f.edit(func() { f.inflight-- })

	if barrier {
		f.awaitOverlap()
	}
	if status != http.StatusOK {
		writeGCSError(w, status, fmt.Sprintf("object %s: %s", name, http.StatusText(status)))
		return
	}
	if encoding != "" {
		w.Header().Set("Content-Encoding", encoding)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(body)
}

func (f *fakeGCS) awaitOverlap() {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		f.mu.Lock()
		overlapped := f.maxInflight >= 2
		f.mu.Unlock()
		if overlapped {
			return
		}
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

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	return &testEnv{gcs: newFakeGCS(t), bq: bqfake.NewServer(t)}
}

func (e *testEnv) config() Config {
	return Config{
		Source:               testSource,
		Project:              testProject,
		Dataset:              testDataset,
		StorageClientOptions: []option.ClientOption{option.WithEndpoint(e.gcs.srv.URL), option.WithoutAuthentication()},
		WriteClientOptions:   e.bq.WriteClientOptions(),
		RetryBackoff:         time.Millisecond,
		RetryMaxBackoff:      4 * time.Millisecond,
		Now:                  func() time.Time { return receiveTime },
	}
}

func (e *testEnv) requests() int {
	_, requests, _ := e.bq.Counts()
	return requests
}

func mustRun(t *testing.T, cfg Config) *Report {
	t.Helper()
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return rep
}

func signalReport(t *testing.T, rep *Report, signal string) SignalReport {
	t.Helper()
	for _, s := range rep.Signals {
		if s.Signal == signal {
			return s
		}
	}
	t.Fatalf("the report has no %s entry: %+v", signal, rep.Signals)
	return SignalReport{}
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

// micros returns the TIMESTAMP values the rows must hold, as column
// returns them.
func micros(times ...time.Time) []string {
	out := make([]string, 0, len(times))
	for _, ts := range times {
		out = append(out, strconv.FormatInt(ts.UnixMicro(), 10))
	}
	slices.Sort(out)
	return out
}

func TestRunAppendsArchive(t *testing.T) {
	e := newTestEnv(t)
	e.gcs.put("exports/2025-03-14/spans.jsonl", jsonl(
		spansLine(t, 3, "early", spanStart),
		spansLine(t, 2, "late", spanStart.Add(time.Hour)),
	))
	e.gcs.put("exports/2025-03-15/logs.jsonl.gz", gzipped(t, jsonl(
		logsLine(t, 2, "first", logTime),
		logsLine(t, 1, "second", logTime.Add(time.Hour)),
	)))
	e.gcs.put("exports/2025-03-16/metrics.jsonl.gz", jsonl(metricsLine(t, 4, metricTime)), transcoded(t))
	// CRLF endings, blank lines, and a last line without a newline.
	e.gcs.put("exports/mixed.jsonl", []byte(spansLine(t, 1, "mixed", spanStart.Add(2*time.Hour))+"\r\n\n   \n"+
		logsLine(t, 1, "mixed", logTime)+"\n"+metricsLine(t, 2, metricTime)))
	e.gcs.put("exports/README.md", []byte("# archive\n"))
	e.gcs.put("exports/dir/", nil)
	e.gcs.put("other/spans.jsonl", jsonl(spansLine(t, 1, "outside", spanStart)))

	var finished []ObjectResult
	cfg := e.config()
	cfg.OnObject = func(r ObjectResult) { finished = append(finished, r) }
	rep := mustRun(t, cfg)

	if !rep.Complete() || rep.Error != "" {
		t.Errorf("Complete() = false, failures %+v, error %q", rep.Failures, rep.Error)
	}
	if rep.Objects != 4 || rep.Skipped != 2 || rep.FailedObjects != 0 {
		t.Errorf("objects %d, skipped %d, failed %d; want 4, 2, 0", rep.Objects, rep.Skipped, rep.FailedObjects)
	}
	if rep.Lines != 8 || rep.MalformedLines != 0 || rep.OtherSignalLines != 0 {
		t.Errorf("lines %d, malformed %d, other %d; want 8, 0, 0", rep.Lines, rep.MalformedLines, rep.OtherSignalLines)
	}
	for _, want := range []SignalReport{
		{Signal: SignalTraces, Table: DefaultTracesTable, Records: 6, Appended: 6},
		{Signal: SignalLogs, Table: DefaultLogsTable, Records: 4, Appended: 4},
		{Signal: SignalMetrics, Table: DefaultMetricsTable, Records: 6, Appended: 6},
	} {
		if got := signalReport(t, rep, want.Signal); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s report = %+v, want %+v", want.Signal, got, want)
		}
	}

	// Rows keep the times the telemetry carries, so they land in the
	// historical partitions; receive_time is when they were appended.
	spans := e.bq.Rows(DefaultTracesTable)
	wantStarts := micros(spanStart, spanStart.Add(time.Second), spanStart.Add(2*time.Second),
		spanStart.Add(time.Hour), spanStart.Add(time.Hour+time.Second), spanStart.Add(2*time.Hour))
	if got := column(spans, "start_time"); !slices.Equal(got, wantStarts) {
		t.Errorf("span start_time = %v, want %v", got, wantStarts)
	}
	logs := e.bq.Rows(DefaultLogsTable)
	wantLogTimes := micros(logTime, logTime.Add(time.Second), logTime.Add(time.Hour), logTime)
	if got := column(logs, "timestamp"); !slices.Equal(got, wantLogTimes) {
		t.Errorf("log timestamp = %v, want %v", got, wantLogTimes)
	}
	if distinct := slices.Compact(column(logs, "timestamp_source")); len(distinct) != 1 || distinct[0] != schema.TimestampSourceEvent {
		t.Errorf("log timestamp_source = %v, want only %q", distinct, schema.TimestampSourceEvent)
	}
	metrics := e.bq.Rows(DefaultMetricsTable)
	wantPoints := micros(metricTime, metricTime.Add(time.Minute), metricTime.Add(2*time.Minute),
		metricTime.Add(3*time.Minute), metricTime, metricTime.Add(time.Minute))
	if got := column(metrics, "timestamp"); !slices.Equal(got, wantPoints) {
		t.Errorf("metric timestamp = %v, want %v", got, wantPoints)
	}
	for table, rows := range map[string][]map[string]any{"spans": spans, "logs": logs, "metrics": metrics} {
		for _, got := range column(rows, "receive_time") {
			if want := micros(receiveTime)[0]; got != want {
				t.Errorf("%s receive_time = %s, want %s", table, got, want)
			}
		}
	}

	for name, want := range map[string]int{
		"exports/2025-03-14/spans.jsonl":      1,
		"exports/2025-03-15/logs.jsonl.gz":    1,
		"exports/2025-03-16/metrics.jsonl.gz": 1,
		"exports/mixed.jsonl":                 1,
		"exports/README.md":                   0,
		"exports/dir/":                        0,
		"other/spans.jsonl":                   0,
	} {
		if got := e.gcs.downloadCount(name); got != want {
			t.Errorf("%s downloaded %d times, want %d", name, got, want)
		}
	}
	e.gcs.edit(func() {
		var tokens []string
		for _, q := range e.gcs.listQueries {
			if q.Get("prefix") != "exports/" {
				t.Errorf("listed with prefix %q, want exports/", q.Get("prefix"))
			}
			tokens = append(tokens, q.Get("pageToken"))
		}
		if want := []string{"", "2", "4"}; !slices.Equal(tokens, want) {
			t.Errorf("page tokens = %q, want %q", tokens, want)
		}
	})

	var urls []string
	var appended int64
	for _, r := range finished {
		urls = append(urls, r.Object)
		appended += r.Appended
		if r.Failures != 0 || r.Err != nil {
			t.Errorf("OnObject(%+v): want no failures", r)
		}
	}
	slices.Sort(urls)
	wantURLs := []string{
		"gs://archive-bucket/exports/2025-03-14/spans.jsonl",
		"gs://archive-bucket/exports/2025-03-15/logs.jsonl.gz",
		"gs://archive-bucket/exports/2025-03-16/metrics.jsonl.gz",
		"gs://archive-bucket/exports/mixed.jsonl",
	}
	if !slices.Equal(urls, wantURLs) || appended != 16 {
		t.Errorf("OnObject saw %q with %d rows appended, want %q with 16", urls, appended, wantURLs)
	}
}

func TestRunRedactsBeforeAppending(t *testing.T) {
	e := newTestEnv(t)
	td, spans := newSpans(1, "secret", spanStart)
	td.ResourceSpans().At(0).Resource().Attributes().PutStr("client_secret", "resource-cleartext")
	attrs := spans.At(0).Attributes()
	attrs.PutStr("password", "span-cleartext")
	attrs.PutStr("X-Session", "session-cleartext")
	attrs.PutStr("note", "card 4111-1111-1111-1111 on file")
	attrs.PutStr("http.route", "/checkout")
	ld, records := newLogs(1, "login token hunter2-body accepted", logTime)
	records.At(0).Attributes().PutStr("api_key", "log-cleartext")
	md, points := newMetrics(1, metricTime)
	points.At(0).Attributes().PutStr("token", "metric-cleartext")
	e.gcs.put("exports/secrets.jsonl.gz", gzipped(t, jsonl(tracesJSON(t, td), logsJSON(t, ld), metricsJSON(t, md))))

	cfg := e.config()
	cfg.Redact = true
	cfg.BlockedKeys = []string{"x-session"}
	cfg.BlockedValuePatterns = []string{`\d{4}-\d{4}-\d{4}-\d{4}`, `hunter2-[a-z]+`}
	cfg.RedactLogBody = true
	if rep := mustRun(t, cfg); !rep.Complete() {
		t.Fatalf("Complete() = false: %+v", rep.Failures)
	}

	kept := map[string][]string{
		DefaultTracesTable:  {"card " + redact.DefaultMaskReplacement + " on file", "/checkout"},
		DefaultLogsTable:    {"login token " + redact.DefaultMaskReplacement + " accepted"},
		DefaultMetricsTable: {redact.DefaultMaskReplacement},
	}
	for table, want := range kept {
		rows := e.bq.Rows(table)
		if len(rows) != 1 {
			t.Fatalf("%s holds %d rows, want 1", table, len(rows))
		}
		dump := fmt.Sprint(rows)
		for _, secret := range []string{
			"resource-cleartext", "span-cleartext", "session-cleartext", "4111-1111-1111-1111",
			"hunter2-body", "log-cleartext", "metric-cleartext",
		} {
			if strings.Contains(dump, secret) {
				t.Errorf("%s row holds %q in the clear: %s", table, secret, dump)
			}
		}
		for _, w := range want {
			if !strings.Contains(dump, w) {
				t.Errorf("%s row does not hold %q: %s", table, w, dump)
			}
		}
	}
}

func TestRunRedactionParityWithLiveExporter(t *testing.T) {
	makeInputs := func() (ptrace.Traces, plog.Logs) {
		td, spans := newSpans(1, "parity", spanStart)
		sAttrs := spans.At(0).Attributes()
		sAttrs.PutStr("authorization", "Bearer secret")
		sAttrs.PutStr("x-custom", "custom value")
		sAttrs.PutStr("note", "card 4111-1111-1111-1111 on file")

		ld, records := newLogs(1, "login token hunter2-body accepted", logTime)
		lAttrs := records.At(0).Attributes()
		lAttrs.PutStr("authorization", "Bearer secret")
		lAttrs.PutStr("x-custom", "custom value")
		lAttrs.PutStr("note", "card 4111-1111-1111-1111 on file")
		return td, ld
	}

	cases := []struct {
		name         string
		configure    func(*bigqueryexporter.RedactionConfig)
		wantAuthKept bool
		wantCustKept bool
	}{
		{
			name: "materialized factory defaults with Enabled=false and MaskReplacement=[REDACTED]",
			configure: func(_ *bigqueryexporter.RedactionConfig) {
				// Leave factory defaults untouched: Enabled=false, MaskReplacement="[REDACTED]".
			},
			wantAuthKept: true,
			wantCustKept: true,
		},
		{
			name: "disabled custom rules with Enabled=false",
			configure: func(rc *bigqueryexporter.RedactionConfig) {
				rc.Enabled = false
				rc.DisableDefaultKeys = true
				rc.BlockedKeys = []string{"x-custom"}
				rc.BlockedValuePatterns = []string{`\d{4}-\d{4}-\d{4}-\d{4}`, `hunter2-[a-z]+`}
				rc.MaskReplacement = "<CUSTOM_MASK>"
				rc.RedactLogBody = true
			},
			wantAuthKept: true,
			wantCustKept: true,
		},
		{
			name: "enabled defaults",
			configure: func(rc *bigqueryexporter.RedactionConfig) {
				rc.Enabled = true
			},
			wantAuthKept: false,
			wantCustKept: true,
		},
		{
			name: "enabled with disabled built-ins",
			configure: func(rc *bigqueryexporter.RedactionConfig) {
				rc.Enabled = true
				rc.DisableDefaultKeys = true
				rc.BlockedKeys = []string{"x-custom"}
			},
			wantAuthKept: true,
			wantCustKept: false,
		},
		{
			name: "enabled custom masks and patterns",
			configure: func(rc *bigqueryexporter.RedactionConfig) {
				rc.Enabled = true
				rc.BlockedKeys = []string{"x-custom"}
				rc.BlockedValuePatterns = []string{`\d{4}-\d{4}-\d{4}-\d{4}`, `hunter2-[a-z]+`}
				rc.MaskReplacement = "<CUSTOM_MASK>"
				rc.RedactLogBody = true
			},
			wantAuthKept: false,
			wantCustKept: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			liveSrv := bqfake.NewServer(t)
			factory := bigqueryexporter.NewFactory()
			expCfg := factory.CreateDefaultConfig().(*bigqueryexporter.Config)
			expCfg.Project = testProject
			expCfg.Dataset = testDataset
			expCfg.Endpoint.URL = liveSrv.Addr()
			expCfg.Endpoint.Insecure = true
			expCfg.Endpoint.WithoutAuthentication = true
			expCfg.BackOffConfig.Enabled = false
			tc.configure(&expCfg.Redaction)
			if err := expCfg.Validate(); err != nil {
				t.Fatalf("expCfg.Validate() = %v", err)
			}

			set := exportertest.NewNopSettings(factory.Type())
			tracesExp, err := factory.CreateTraces(ctx, set, expCfg)
			if err != nil {
				t.Fatalf("CreateTraces: %v", err)
			}
			logsExp, err := factory.CreateLogs(ctx, set, expCfg)
			if err != nil {
				t.Fatalf("CreateLogs: %v", err)
			}
			if err := tracesExp.Start(ctx, componenttest.NewNopHost()); err != nil {
				t.Fatalf("tracesExp.Start: %v", err)
			}
			if err := logsExp.Start(ctx, componenttest.NewNopHost()); err != nil {
				_ = tracesExp.Shutdown(ctx)
				t.Fatalf("logsExp.Start: %v", err)
			}

			tdLive, ldLive := makeInputs()
			if err := tracesExp.ConsumeTraces(ctx, tdLive); err != nil {
				_ = tracesExp.Shutdown(ctx)
				_ = logsExp.Shutdown(ctx)
				t.Fatalf("ConsumeTraces: %v", err)
			}
			if err := logsExp.ConsumeLogs(ctx, ldLive); err != nil {
				_ = tracesExp.Shutdown(ctx)
				_ = logsExp.Shutdown(ctx)
				t.Fatalf("ConsumeLogs: %v", err)
			}
			if err := tracesExp.Shutdown(ctx); err != nil {
				_ = logsExp.Shutdown(ctx)
				t.Fatalf("tracesExp.Shutdown: %v", err)
			}
			if err := logsExp.Shutdown(ctx); err != nil {
				t.Fatalf("logsExp.Shutdown: %v", err)
			}

			// Copy all redaction fields from expCfg.Redaction into backfill.Config.
			e := newTestEnv(t)
			tdArchive, ldArchive := makeInputs()
			e.gcs.put("exports/parity.jsonl", jsonl(tracesJSON(t, tdArchive), logsJSON(t, ldArchive)))
			bfCfg := e.config()
			bfCfg.Redact = expCfg.Redaction.Enabled
			bfCfg.BlockedKeys = slices.Clone(expCfg.Redaction.BlockedKeys)
			bfCfg.DisableDefaultKeys = expCfg.Redaction.DisableDefaultKeys
			bfCfg.BlockedValuePatterns = slices.Clone(expCfg.Redaction.BlockedValuePatterns)
			bfCfg.MaskReplacement = expCfg.Redaction.MaskReplacement
			bfCfg.RedactLogBody = expCfg.Redaction.RedactLogBody
			bfCfg.LogNameAttribute = expCfg.Logs.LogNameAttribute
			bfCfg.InsertIDAttribute = expCfg.Logs.InsertIDAttribute

			rep := mustRun(t, bfCfg)
			if !rep.Complete() {
				t.Fatalf("backfill Complete() = false: %+v", rep.Failures)
			}

			liveSpans := liveSrv.Rows(DefaultTracesTable)
			bfSpans := e.bq.Rows(DefaultTracesTable)
			if len(liveSpans) != 1 || len(bfSpans) != 1 {
				t.Fatalf("spans rows: live=%d backfill=%d, want 1 each", len(liveSpans), len(bfSpans))
			}
			delete(liveSpans[0], "receive_time")
			delete(bfSpans[0], "receive_time")
			delete(liveSpans[0], "receive_time_unix_nano")
			delete(bfSpans[0], "receive_time_unix_nano")
			if !reflect.DeepEqual(liveSpans[0], bfSpans[0]) {
				t.Errorf("span row mismatch:\nlive:     %+v\nbackfill: %+v", liveSpans[0], bfSpans[0])
			}

			liveLogs := liveSrv.Rows(DefaultLogsTable)
			bfLogs := e.bq.Rows(DefaultLogsTable)
			if len(liveLogs) != 1 || len(bfLogs) != 1 {
				t.Fatalf("logs rows: live=%d backfill=%d, want 1 each", len(liveLogs), len(bfLogs))
			}
			liveFP := fmt.Sprint(liveLogs[0]["insert_id"])
			bfFP := fmt.Sprint(bfLogs[0]["insert_id"])
			if !strings.HasPrefix(liveFP, "fp1:") || liveFP != bfFP {
				t.Errorf("log insert_id fingerprint mismatch: live=%q backfill=%q", liveFP, bfFP)
			}
			delete(liveLogs[0], "receive_time")
			delete(bfLogs[0], "receive_time")
			delete(liveLogs[0], "receive_time_unix_nano")
			delete(bfLogs[0], "receive_time_unix_nano")
			if !reflect.DeepEqual(liveLogs[0], bfLogs[0]) {
				t.Errorf("log row mismatch:\nlive:     %+v\nbackfill: %+v", liveLogs[0], bfLogs[0])
			}

			dump := fmt.Sprint(bfLogs[0])
			if got := strings.Contains(dump, "Bearer secret"); got != tc.wantAuthKept {
				t.Errorf("authorization cleartext present = %v, want %v (row=%s)", got, tc.wantAuthKept, dump)
			}
			if got := strings.Contains(dump, "custom value"); got != tc.wantCustKept {
				t.Errorf("x-custom cleartext present = %v, want %v (row=%s)", got, tc.wantCustKept, dump)
			}
		})
	}
}

func TestRedactionRejectsBlockedLogIdentityAttributes(t *testing.T) {
	base := Config{Source: testSource, Project: testProject, Dataset: testDataset, Redact: true}
	for _, tc := range []struct {
		name string
		edit func(*Config)
		want string
	}{
		{
			name: "insert_id_attribute matches built-in exact blocked key",
			edit: func(c *Config) { c.InsertIDAttribute = "authorization" },
			want: `redaction: insert_id_attribute "authorization" matches a blocked redaction key`,
		},
		{
			name: "insert_id_attribute matches built-in glob blocked key",
			edit: func(c *Config) { c.InsertIDAttribute = "user_secret_id" },
			want: `redaction: insert_id_attribute "user_secret_id" matches a blocked redaction key`,
		},
		{
			name: "insert_id_attribute matches custom exact key with DisableDefaultKeys",
			edit: func(c *Config) {
				c.DisableDefaultKeys = true
				c.BlockedKeys = []string{"session_token"}
				c.InsertIDAttribute = "session_token"
			},
			want: `redaction: insert_id_attribute "session_token" matches a blocked redaction key`,
		},
		{
			name: "insert_id_attribute matches custom glob key with DisableDefaultKeys",
			edit: func(c *Config) {
				c.DisableDefaultKeys = true
				c.BlockedKeys = []string{"session_*"}
				c.InsertIDAttribute = "session_token"
			},
			want: `redaction: insert_id_attribute "session_token" matches a blocked redaction key`,
		},
		{
			name: "log_name_attribute matches built-in exact blocked key",
			edit: func(c *Config) { c.LogNameAttribute = "api_key" },
			want: `redaction: log_name_attribute "api_key" matches a blocked redaction key`,
		},
		{
			name: "log_name_attribute matches built-in glob blocked key",
			edit: func(c *Config) { c.LogNameAttribute = "oauth_token" },
			want: `redaction: log_name_attribute "oauth_token" matches a blocked redaction key`,
		},
		{
			name: "log_name_attribute matches custom glob key with DisableDefaultKeys",
			edit: func(c *Config) {
				c.DisableDefaultKeys = true
				c.BlockedKeys = []string{"tenant.*.secret"}
				c.LogNameAttribute = "tenant.prod.secret"
			},
			want: `redaction: log_name_attribute "tenant.prod.secret" matches a blocked redaction key`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.edit(&c)
			if err := c.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() = %v, want %q", err, tc.want)
			}
			e := newTestEnv(t)
			ld, records := newLogs(2, "event", logTime)
			records.At(0).Attributes().PutStr(c.InsertIDAttribute, "id-1")
			records.At(1).Attributes().PutStr(c.InsertIDAttribute, "id-2")
			e.gcs.put("exports/logs.jsonl", jsonl(logsJSON(t, ld)))
			runCfg := e.config()
			runCfg.Redact = c.Redact
			runCfg.DisableDefaultKeys = c.DisableDefaultKeys
			runCfg.BlockedKeys = c.BlockedKeys
			runCfg.InsertIDAttribute = c.InsertIDAttribute
			runCfg.LogNameAttribute = c.LogNameAttribute
			if _, err := Run(context.Background(), runCfg); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Run() error = %v, want %q", err, tc.want)
			}
			if got := e.bq.RowCount(DefaultLogsTable); got != 0 {
				t.Errorf("otel_logs holds %d rows after rejected identity config, want 0", got)
			}
		})
	}

	// Row-level check: when InsertIDAttribute and LogNameAttribute do NOT match
	// built-in or custom globs, distinct records keep distinct insert_id and
	// log_name values while sibling blocked attributes are masked.
	e := newTestEnv(t)
	ld, records := newLogs(2, "audit event", logTime)
	records.At(0).Attributes().PutStr("app.record_id", "rec-1")
	records.At(0).Attributes().PutStr("app.log", "audit-a")
	records.At(0).Attributes().PutStr("session_token", "tok-1")
	records.At(1).Attributes().PutStr("app.record_id", "rec-2")
	records.At(1).Attributes().PutStr("app.log", "audit-b")
	records.At(1).Attributes().PutStr("session_token", "tok-2")
	e.gcs.put("exports/logs.jsonl", jsonl(logsJSON(t, ld)))

	cfg := e.config()
	cfg.Redact = true
	cfg.BlockedKeys = []string{"session_*"}
	cfg.InsertIDAttribute = "app.record_id"
	cfg.LogNameAttribute = "app.log"
	rep := mustRun(t, cfg)
	if !rep.Complete() {
		t.Fatalf("Complete() = false: %+v", rep.Failures)
	}
	rows := e.bq.Rows(DefaultLogsTable)
	if len(rows) != 2 {
		t.Fatalf("otel_logs holds %d rows, want 2", len(rows))
	}
	if got := []string{fmt.Sprint(rows[0]["insert_id"]), fmt.Sprint(rows[1]["insert_id"])}; !slices.Equal(got, []string{"rec-1", "rec-2"}) {
		t.Errorf("insert_id values = %v, want [rec-1 rec-2]", got)
	}
	if got := []string{fmt.Sprint(rows[0]["log_name"]), fmt.Sprint(rows[1]["log_name"])}; !slices.Equal(got, []string{"audit-a", "audit-b"}) {
		t.Errorf("log_name values = %v, want [audit-a audit-b]", got)
	}
	if dump := fmt.Sprint(rows); strings.Contains(dump, "tok-1") || strings.Contains(dump, "tok-2") {
		t.Errorf("session_token leaked in rows: %s", dump)
	}
}

func TestRunSignalSkipsOtherSignals(t *testing.T) {
	e := newTestEnv(t)
	e.gcs.put("exports/mixed.jsonl", jsonl(
		spansLine(t, 1, "skipped", spanStart),
		logsLine(t, 2, "kept", logTime),
		metricsLine(t, 3, metricTime),
	))
	cfg := e.config()
	cfg.Signal = SignalLogs
	cfg.LogsTable = "archive_logs"
	rep := mustRun(t, cfg)

	if rep.Lines != 3 || rep.OtherSignalLines != 2 || !rep.Complete() {
		t.Errorf("lines %d, other %d, complete %v; want 3, 2, true", rep.Lines, rep.OtherSignalLines, rep.Complete())
	}
	want := []SignalReport{{Signal: SignalLogs, Table: "archive_logs", Records: 2, Appended: 2}}
	if fmt.Sprint(rep.Signals) != fmt.Sprint(want) {
		t.Errorf("Signals = %+v, want %+v", rep.Signals, want)
	}
	for table, want := range map[string]int{"archive_logs": 2, DefaultLogsTable: 0, DefaultTracesTable: 0, DefaultMetricsTable: 0} {
		if got := e.bq.RowCount(table); got != want {
			t.Errorf("%s holds %d rows, want %d", table, got, want)
		}
	}
}

func TestRunBatchesAppends(t *testing.T) {
	e := newTestEnv(t)
	e.gcs.put("exports/spans.jsonl", jsonl(
		spansLine(t, 4, "a", spanStart),
		spansLine(t, 3, "b", spanStart.Add(time.Hour)),
	))
	cfg := e.config()
	cfg.BatchSize = 3
	cfg.MaxWorkers = 1
	rep := mustRun(t, cfg)

	if got := signalReport(t, rep, SignalTraces).Appended; got != 7 {
		t.Errorf("appended %d spans, want 7", got)
	}
	// Batches fill across lines: 3, 3, then the last 1.
	if got := e.requests(); got != 3 {
		t.Errorf("%d append requests, want 3", got)
	}
}

func TestRunWorkers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		workers int
		check   func(maxInflight int) bool
		want    string
	}{
		{"objects are read concurrently", 4, func(n int) bool { return n >= 2 }, "at least 2"},
		{"max workers caps concurrency", 1, func(n int) bool { return n == 1 }, "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t)
			for i := range 4 {
				e.gcs.put(fmt.Sprintf("exports/part-%d.jsonl", i), jsonl(spansLine(t, 1, fmt.Sprintf("part%d", i), spanStart)))
			}
			e.gcs.edit(func() { e.gcs.barrier = tc.workers > 1 })
			cfg := e.config()
			cfg.MaxWorkers = tc.workers
			rep := mustRun(t, cfg)

			if got := signalReport(t, rep, SignalTraces).Appended; got != 4 || !rep.Complete() {
				t.Errorf("appended %d spans, complete %v; want 4, true", got, rep.Complete())
			}
			e.gcs.edit(func() {
				if !tc.check(e.gcs.maxInflight) {
					t.Errorf("%d downloads at once, want %s", e.gcs.maxInflight, tc.want)
				}
			})
		})
	}
}

func TestRunReportsLineProblems(t *testing.T) {
	e := newTestEnv(t)
	e.gcs.put("exports/bad.jsonl", jsonl(
		spansLine(t, 1, "good", spanStart),
		"not json",
		`{"hello":"world"}`,
		`[1,2,3]`,
		`{"resourceSpans":"nope"}`,
		strings.Repeat("x", 3000),
		"",
		logsLine(t, 1, "good", logTime),
	))
	cfg := e.config()
	cfg.MaxLineBytes = 2048
	var results []ObjectResult
	cfg.OnObject = func(r ObjectResult) { results = append(results, r) }
	rep := mustRun(t, cfg)

	if rep.Complete() {
		t.Error("Complete() = true, want false")
	}
	if rep.Lines != 7 || rep.MalformedLines != 5 || rep.FailedObjects != 1 || rep.FailureCount != 5 {
		t.Errorf("lines %d, malformed %d, failed objects %d, failures %d; want 7, 5, 1, 5",
			rep.Lines, rep.MalformedLines, rep.FailedObjects, rep.FailureCount)
	}
	wantErrs := map[int64]string{
		2: "not valid JSON",
		3: errNotExportRequest.Error(),
		4: "not a JSON object",
		5: "decoding OTLP JSON traces",
		6: "line longer than 2048 bytes",
	}
	for _, f := range rep.Failures {
		want, ok := wantErrs[f.Line]
		if !ok || !strings.Contains(f.Error, want) || f.Object != "gs://archive-bucket/exports/bad.jsonl" {
			t.Errorf("unexpected failure %+v", f)
		}
		delete(wantErrs, f.Line)
	}
	if len(wantErrs) > 0 {
		t.Errorf("no failures reported for lines %v", wantErrs)
	}
	if signalReport(t, rep, SignalTraces).Appended != 1 || signalReport(t, rep, SignalLogs).Appended != 1 {
		t.Errorf("the good lines were not appended: %+v", rep.Signals)
	}
	if len(results) != 1 || results[0].Lines != 7 || results[0].Failures != 5 || results[0].Err != nil {
		t.Errorf("OnObject saw %+v", results)
	}
}

func TestRunRetriesTransientAppendFailures(t *testing.T) {
	for _, mode := range []bqfake.FaultMode{bqfake.FaultUnavailable, bqfake.FaultResourceExhausted, bqfake.FaultStreamClosed} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			e := newTestEnv(t)
			e.gcs.put("exports/spans.jsonl", jsonl(spansLine(t, 2, "retried", spanStart)))
			e.bq.EnqueueFault(bqfake.Fault{Mode: mode, Table: DefaultTracesTable}, bqfake.Fault{Mode: mode, Table: DefaultTracesTable})
			rep := mustRun(t, e.config())

			if got := signalReport(t, rep, SignalTraces); got.Appended != 2 || !rep.Complete() {
				t.Errorf("traces %+v, failures %+v; want 2 appended", got, rep.Failures)
			}
			if got := e.bq.RowCount(DefaultTracesTable); got != 2 {
				t.Errorf("otel_spans holds %d rows, want 2", got)
			}
		})
	}
}

func TestRunGivesUpAfterMaxAttempts(t *testing.T) {
	e := newTestEnv(t)
	e.gcs.put("exports/spans.jsonl", jsonl(spansLine(t, 2, "lost", spanStart)))
	e.gcs.put("exports/later.jsonl", jsonl(spansLine(t, 1, "later", spanStart)))
	for range 3 {
		e.bq.EnqueueFault(bqfake.Fault{Mode: bqfake.FaultUnavailable, Table: DefaultTracesTable})
	}
	cfg := e.config()
	cfg.MaxAttempts = 3
	cfg.MaxWorkers = 1
	rep := mustRun(t, cfg)

	if rep.Complete() || rep.FailedObjects != 1 || len(rep.Failures) != 1 {
		t.Fatalf("complete %v, failed objects %d, failures %+v; want one failure", rep.Complete(), rep.FailedObjects, rep.Failures)
	}
	// Objects are read in name order, so later.jsonl meets the faults.
	if f := rep.Failures[0]; !strings.Contains(f.Error, "appending traces rows read by line 1 to otel_spans: giving up after 3 attempts: ") ||
		f.Object != "gs://archive-bucket/exports/later.jsonl" {
		t.Errorf("failure = %+v", f)
	}
	// The object that failed keeps its rows unwritten; the run went on.
	if got := signalReport(t, rep, SignalTraces); got.Appended != 2 || got.Unwritten != 1 {
		t.Errorf("traces %+v, want 2 appended and 1 unwritten", got)
	}
}

func TestRunResendsValidRowsAfterRowErrors(t *testing.T) {
	e := newTestEnv(t)
	e.gcs.put("exports/spans.jsonl", jsonl(spansLine(t, 3, "row", spanStart)))
	e.bq.EnqueueFault(bqfake.Fault{
		Mode:      bqfake.FaultRowErrors,
		Table:     DefaultTracesTable,
		RowErrors: []*storagepb.RowError{{Index: 1, Code: storagepb.RowError_FIELDS_ERROR, Message: "bad value"}},
	})
	rep := mustRun(t, e.config())

	want := SignalReport{Signal: SignalTraces, Table: DefaultTracesTable, Records: 3, Appended: 2,
		Rejected: map[string]int64{"row_error": 1}}
	if got := signalReport(t, rep, SignalTraces); fmt.Sprint(got) != fmt.Sprint(want) || !rep.Complete() {
		t.Errorf("traces %+v, complete %v; want %+v", got, rep.Complete(), want)
	}
	if got, want := column(e.bq.Rows(DefaultTracesTable), "name"), []string{"row-0", "row-2"}; !slices.Equal(got, want) {
		t.Errorf("appended spans %q, want %q", got, want)
	}
	if got := e.requests(); got != 2 {
		t.Errorf("%d append requests, want 2", got)
	}
}

func TestRunStopsOnPermanentAppendFailure(t *testing.T) {
	for _, tc := range []struct {
		name         string
		faults       []bqfake.Fault
		label        string
		wantRequests int
	}{
		{
			name:         "permission denied",
			faults:       []bqfake.Fault{{Mode: bqfake.FaultPermissionDenied, Table: DefaultTracesTable}},
			label:        bqi.LabelPermission,
			wantRequests: 1,
		},
		{
			name: "schema mismatch on a rebuilt stream",
			faults: []bqfake.Fault{
				{Mode: bqfake.FaultInvalidArgument, Table: DefaultTracesTable, Message: "schema mismatch: unknown field"},
				{Mode: bqfake.FaultInvalidArgument, Table: DefaultTracesTable, Message: "schema mismatch: unknown field"},
			},
			label:        bqi.LabelSchemaMismatch,
			wantRequests: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t)
			e.gcs.put("exports/spans.jsonl", jsonl(spansLine(t, 2, "denied", spanStart)))
			e.bq.EnqueueFault(tc.faults...)
			cfg := e.config()
			cfg.MaxWorkers = 1
			rep, err := Run(context.Background(), cfg)

			var permanent *PermanentError
			if !errors.As(err, &permanent) || permanent.Label != tc.label || permanent.Table != DefaultTracesTable {
				t.Fatalf("Run error = %v, want a *PermanentError labeled %s", err, tc.label)
			}
			if rep == nil || rep.Complete() || rep.Error != err.Error() || rep.FailureCount != 0 {
				t.Fatalf("report %+v: want the run's error and no object failures", rep)
			}
			if got := signalReport(t, rep, SignalTraces); got.Appended != 0 || got.Unwritten != 2 {
				t.Errorf("traces %+v, want 2 unwritten", got)
			}
			if got := e.requests(); got != tc.wantRequests {
				t.Errorf("%d append requests, want %d", got, tc.wantRequests)
			}
		})
	}

	t.Run("split batch with row errors and permanent failure aborts", func(t *testing.T) {
		e := newTestEnv(t)
		cfg := e.config()
		if err := cfg.normalize(); err != nil {
			t.Fatal(err)
		}
		lifetimeCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client, err := managedwriter.NewClient(lifetimeCtx, cfg.Project, cfg.WriteClientOptions...)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		ts, label := tableSchema(SignalTraces)
		enc, err := protoenc.New(ts)
		if err != nil {
			t.Fatal(err)
		}
		sk := &sink{
			table: DefaultTracesTable,
			enc:   enc,
			writer: bqi.NewWriter(lifetimeCtx, client, bqi.WriterOptions{
				Project:             cfg.Project,
				Dataset:             cfg.Dataset,
				Table:               DefaultTracesTable,
				Signal:              label,
				Descriptor:          enc.Descriptor(),
				MaxRequestBytes:     64,
				MaxInflightRequests: maxInflightRequests,
				MaxInflightBytes:    maxInflightBytes,
				StreamPoolSize:      1,
				TraceID:             writeTraceID,
			}),
		}
		defer func() { _ = sk.writer.Close() }()

		// Build 2 rows of 128 bytes each (> 64-byte MaxRequestBytes) so a single
		// Append call splits into two sub-requests.
		row := bytes.Repeat([]byte{0x08, 0x01}, 64)
		batch := [][]byte{row, row}
		e.bq.EnqueueFault(
			bqfake.Fault{
				Mode:      bqfake.FaultRowErrors,
				Table:     DefaultTracesTable,
				RowErrors: []*storagepb.RowError{{Index: 0, Code: storagepb.RowError_FIELDS_ERROR, Message: "bad row"}},
			},
			bqfake.Fault{
				Mode:  bqfake.FaultPermissionDenied,
				Table: DefaultTracesTable,
			},
		)
		r := &run{cfg: cfg}
		acked, refused, err := r.appendRows(context.Background(), sk, batch)

		var permanent *PermanentError
		if !errors.As(err, &permanent) || permanent.Label != bqi.LabelPermission || permanent.Table != DefaultTracesTable {
			t.Fatalf("appendRows error = %v, want *PermanentError labeled %s", err, bqi.LabelPermission)
		}
		if acked != 0 || refused != 1 {
			t.Errorf("acked=%d refused=%d, want acked=0 refused=1", acked, refused)
		}
		if got := e.requests(); got != 2 {
			t.Errorf("%d append requests, want 2 (no retry after permanent failure)", got)
		}
	})

	t.Run("final batch via batcher.flushAll with row errors and permanent failure aborts", func(t *testing.T) {
		e := newTestEnv(t)
		cfg := e.config()
		if err := cfg.normalize(); err != nil {
			t.Fatal(err)
		}
		lifetimeCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client, err := managedwriter.NewClient(lifetimeCtx, cfg.Project, cfg.WriteClientOptions...)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		ts, label := tableSchema(SignalTraces)
		enc, err := protoenc.New(ts)
		if err != nil {
			t.Fatal(err)
		}
		sk := &sink{
			table: DefaultTracesTable,
			enc:   enc,
			writer: bqi.NewWriter(lifetimeCtx, client, bqi.WriterOptions{
				Project:             cfg.Project,
				Dataset:             cfg.Dataset,
				Table:               DefaultTracesTable,
				Signal:              label,
				Descriptor:          enc.Descriptor(),
				MaxRequestBytes:     64,
				MaxInflightRequests: maxInflightRequests,
				MaxInflightBytes:    maxInflightBytes,
				StreamPoolSize:      1,
				TraceID:             writeTraceID,
			}),
		}
		defer func() { _ = sk.writer.Close() }()

		row := bytes.Repeat([]byte{0x08, 0x01}, 64)
		e.bq.EnqueueFault(
			bqfake.Fault{
				Mode:      bqfake.FaultRowErrors,
				Table:     DefaultTracesTable,
				RowErrors: []*storagepb.RowError{{Index: 0, Code: storagepb.RowError_FIELDS_ERROR, Message: "bad row"}},
			},
			bqfake.Fault{
				Mode:  bqfake.FaultPermissionDenied,
				Table: DefaultTracesTable,
			},
		)
		r := &run{
			cfg:    cfg,
			sinks:  map[string]*sink{SignalTraces: sk},
			totals: map[string]*signalStats{},
		}
		st := &objectStats{
			url:     "gs://" + testBucket + "/exports/final.jsonl",
			signals: map[string]*signalStats{SignalTraces: {records: 2, encoded: 2}},
		}
		b := &batcher{
			run:  r,
			st:   st,
			rows: map[string][][]byte{SignalTraces: {row, row}},
		}
		err = b.flushAll(context.Background(), 1)

		var permanent *PermanentError
		if !errors.As(err, &permanent) || permanent.Label != bqi.LabelPermission || permanent.Table != DefaultTracesTable {
			t.Fatalf("flushAll error = %v, want *PermanentError labeled %s", err, bqi.LabelPermission)
		}
		s := st.signal(SignalTraces)
		if s.appended != 0 || s.rejected["row_error"] != 1 || s.encoded-s.appended-s.rejected["row_error"] != 1 {
			t.Errorf("signal stats = %+v, want appended=0 rejected[row_error]=1 unwritten=1", s)
		}
		if got := e.requests(); got != 2 {
			t.Errorf("%d append requests, want 2 (no retry after permanent failure)", got)
		}
	})
}

func TestRunRebuildsStreamOnce(t *testing.T) {
	e := newTestEnv(t)
	e.gcs.put("exports/spans.jsonl", jsonl(spansLine(t, 2, "rebuilt", spanStart)))
	e.bq.EnqueueFault(bqfake.Fault{Mode: bqfake.FaultInvalidArgument, Table: DefaultTracesTable, Message: "schema mismatch: unknown field"})
	rep := mustRun(t, e.config())

	if got := signalReport(t, rep, SignalTraces); got.Appended != 2 || !rep.Complete() {
		t.Errorf("traces %+v, failures %+v; want 2 appended", got, rep.Failures)
	}
	if connections, _, _ := e.bq.Counts(); connections < 2 {
		t.Errorf("%d append connections, want a second one for the rebuilt stream", connections)
	}
}

func TestRunRecordsUnreadableObjects(t *testing.T) {
	e := newTestEnv(t)
	e.gcs.put("exports/a-missing.jsonl", nil, answering(http.StatusNotFound))
	e.gcs.put("exports/b-flaky.jsonl", jsonl(spansLine(t, 1, "flaky", spanStart)), failingFirst(2))
	e.gcs.put("exports/c-down.jsonl", jsonl(spansLine(t, 1, "down", spanStart)), answering(http.StatusServiceUnavailable))
	e.gcs.put("exports/d-good.jsonl", jsonl(spansLine(t, 2, "good", spanStart)))
	cfg := e.config()
	cfg.MaxAttempts = 3
	rep := mustRun(t, cfg)

	if rep.Complete() || rep.FailedObjects != 2 || rep.FailureCount != 2 {
		t.Fatalf("complete %v, failed objects %d, failures %+v; want 2", rep.Complete(), rep.FailedObjects, rep.Failures)
	}
	got := map[string]Failure{}
	for _, f := range rep.Failures {
		got[f.Object] = f
	}
	for object, want := range map[string]string{
		"gs://archive-bucket/exports/a-missing.jsonl": "opening the object: HTTP 404: object exports/a-missing.jsonl: Not Found",
		"gs://archive-bucket/exports/c-down.jsonl":    "opening the object: HTTP 503: object exports/c-down.jsonl: Service Unavailable",
	} {
		if f := got[object]; f.Error != want || f.Line != 0 {
			t.Errorf("failure for %s = %+v, want %q", object, f, want)
		}
	}
	for name, want := range map[string]int{
		"exports/a-missing.jsonl": 1,
		"exports/b-flaky.jsonl":   3,
		"exports/c-down.jsonl":    3,
		"exports/d-good.jsonl":    1,
	} {
		if got := e.gcs.downloadCount(name); got != want {
			t.Errorf("%s downloaded %d times, want %d", name, got, want)
		}
	}
	if got := signalReport(t, rep, SignalTraces).Appended; got != 3 {
		t.Errorf("appended %d spans, want 3", got)
	}
}

func TestRunListFailure(t *testing.T) {
	for _, tc := range []struct {
		status      int
		want        string
		wantQueries int
	}{
		{http.StatusForbidden, "listing gs://archive-bucket/exports/: HTTP 403: list refused", 1},
		{http.StatusServiceUnavailable, "listing gs://archive-bucket/exports/: HTTP 503: list refused", 2},
	} {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			e := newTestEnv(t)
			e.gcs.edit(func() { e.gcs.listStatus = tc.status })
			cfg := e.config()
			cfg.MaxAttempts = 2
			rep, err := Run(context.Background(), cfg)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("Run error = %v, want %q", err, tc.want)
			}
			if rep == nil || rep.Objects != 0 || rep.Error != tc.want || rep.Complete() {
				t.Errorf("report = %+v", rep)
			}
			e.gcs.edit(func() {
				if len(e.gcs.listQueries) != tc.wantQueries {
					t.Errorf("%d list requests, want %d", len(e.gcs.listQueries), tc.wantQueries)
				}
			})
		})
	}
}

func TestRunCountsRejectedRecords(t *testing.T) {
	e := newTestEnv(t)
	td, spans := newSpans(3, "rejected", spanStart)
	spans.At(1).SetTraceID(pcommon.TraceID{})
	// Each value is under the 256 KiB value limit, but the row is over the
	// 1 MiB row limit.
	big := strings.Repeat("v", 240<<10)
	for i := range 5 {
		spans.At(2).Attributes().PutStr(fmt.Sprintf("blob.%d", i), big)
	}
	e.gcs.put("exports/spans.jsonl", jsonl(tracesJSON(t, td)))
	rep := mustRun(t, e.config())

	want := SignalReport{Signal: SignalTraces, Table: DefaultTracesTable, Records: 3, Appended: 1,
		Rejected: map[string]int64{"invalid_id": 1, "row_bytes": 1}}
	if got := signalReport(t, rep, SignalTraces); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("traces %+v, want %+v", got, want)
	}
	// The exporter rejects the same records; they are not failures.
	if !rep.Complete() {
		t.Errorf("Complete() = false: %+v", rep.Failures)
	}
}

func TestRunStopsWhenContextEnds(t *testing.T) {
	t.Run("during the run", func(t *testing.T) {
		e := newTestEnv(t)
		for i := range 3 {
			e.gcs.put(fmt.Sprintf("exports/part-%d.jsonl", i), jsonl(spansLine(t, 1, "part", spanStart)))
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cfg := e.config()
		cfg.MaxWorkers = 1
		cfg.OnObject = func(ObjectResult) { cancel() }
		rep, err := Run(ctx, cfg)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
		// Stopping is not an object failure; the report says why it stopped.
		if rep.FailureCount != 0 || rep.Complete() || rep.Error != context.Canceled.Error() {
			t.Errorf("report = %+v", rep)
		}
		if got := signalReport(t, rep, SignalTraces).Appended; got < 1 {
			t.Errorf("appended %d spans, want the first object's", got)
		}
	})
	t.Run("before the run", func(t *testing.T) {
		e := newTestEnv(t)
		e.gcs.put("exports/spans.jsonl", jsonl(spansLine(t, 1, "never", spanStart)))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := Run(ctx, e.config()); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
		if got := e.bq.RowCount(DefaultTracesTable); got != 0 {
			t.Errorf("otel_spans holds %d rows, want 0", got)
		}
	})
}

func TestRunCapsListedFailures(t *testing.T) {
	e := newTestEnv(t)
	lines := make([]string, maxListedFailures+5)
	for i := range lines {
		lines[i] = "garbage"
	}
	e.gcs.put("exports/garbage.jsonl", jsonl(lines...))
	rep := mustRun(t, e.config())
	if len(rep.Failures) != maxListedFailures || rep.FailureCount != maxListedFailures+5 || rep.MalformedLines != maxListedFailures+5 {
		t.Errorf("%d failures listed, %d counted, %d malformed; want %d, %d, %d", len(rep.Failures), rep.FailureCount,
			rep.MalformedLines, maxListedFailures, maxListedFailures+5, maxListedFailures+5)
	}
	if last := rep.Failures[len(rep.Failures)-1]; last.Line != maxListedFailures {
		t.Errorf("the last listed failure is line %d, want line %d", last.Line, maxListedFailures)
	}

	// Verify that readObject itself caps per-object failure slice growth at
	// maxListedFailures even on an archive with 100,000 malformed lines.
	const hugeLines = 100_000
	huge := bytes.Repeat([]byte("garbage\n"), hugeLines)
	r := &run{
		cfg:    Config{MaxLineBytes: DefaultMaxLineBytes},
		bucket: testBucket,
		store:  &memStore{objects: map[string][]byte{"exports/huge.jsonl": huge}},
	}
	st := &objectStats{
		url:     "gs://" + testBucket + "/exports/huge.jsonl",
		signals: map[string]*signalStats{},
	}
	if err := r.readObject(context.Background(), Object{Name: "exports/huge.jsonl", Size: int64(len(huge))}, st); err != nil {
		t.Fatalf("readObject(huge): %v", err)
	}
	if len(st.failures) != maxListedFailures || st.failureCount != hugeLines || st.malformed != hugeLines {
		t.Errorf("len(st.failures)=%d st.failureCount=%d st.malformed=%d, want %d, %d, %d",
			len(st.failures), st.failureCount, st.malformed, maxListedFailures, hugeLines, hugeLines)
	}
}

// memStore is an ObjectStore over in-memory objects. readErr makes an
// object's reader fail after its bytes.
type memStore struct {
	objects map[string][]byte
	readErr map[string]error
}

func (m *memStore) List(_ context.Context, _, prefix string, fn func(Object) error) error {
	var names []string
	for name := range m.objects {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	for _, name := range names {
		if err := fn(Object{Name: name, Size: int64(len(m.objects[name]))}); err != nil {
			return err
		}
	}
	return nil
}

func (m *memStore) Open(_ context.Context, _ string, obj Object) (io.ReadCloser, error) {
	r := io.Reader(bytes.NewReader(m.objects[obj.Name]))
	if err := m.readErr[obj.Name]; err != nil {
		r = io.MultiReader(r, &errReader{err})
	}
	return io.NopCloser(r), nil
}

type errReader struct{ err error }

func (r *errReader) Read([]byte) (int, error) { return 0, r.err }

func TestRunRecordsUnreadableContent(t *testing.T) {
	bq := bqfake.NewServer(t)
	good := jsonl(spansLine(t, 2, "good", spanStart))
	compressed := gzipped(t, good)
	store := &memStore{
		objects: map[string][]byte{
			"exports/bad-header.jsonl.gz": {0x1f, 0x8b, 0x00},
			"exports/truncated.jsonl.gz":  compressed[:len(compressed)/2],
			"exports/reset.jsonl":         good,
			"exports/good.jsonl.gz":       compressed,
		},
		readErr: map[string]error{"exports/reset.jsonl": errors.New("connection reset by peer")},
	}
	rep := mustRun(t, Config{
		Source:             testSource,
		Project:            testProject,
		Dataset:            testDataset,
		Signal:             SignalTraces,
		Store:              store,
		WriteClientOptions: bq.WriteClientOptions(),
	})

	got := map[string]string{}
	for _, f := range rep.Failures {
		got[strings.TrimPrefix(f.Object, "gs://"+testBucket+"/")] = f.Error
	}
	for object, want := range map[string]string{
		"exports/bad-header.jsonl.gz": "reading the gzip header",
		"exports/truncated.jsonl.gz":  "reading line 1: unexpected EOF",
		"exports/reset.jsonl":         "reading line 2: connection reset by peer",
	} {
		if !strings.Contains(got[object], want) {
			t.Errorf("failure for %s = %q, want %q", object, got[object], want)
		}
	}
	// The reset object's line was read but its object failed before the
	// rows were appended.
	if s := signalReport(t, rep, SignalTraces); s.Appended != 2 || s.Unwritten != 2 || rep.FailedObjects != 3 {
		t.Errorf("traces %+v, failed objects %d; want 2 appended, 2 unwritten, 3 failed", s, rep.FailedObjects)
	}
}

func TestConfigValidate(t *testing.T) {
	valid := Config{Source: testSource, Project: testProject, Dataset: testDataset}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	if valid.Signal != "" || valid.BatchSize != 0 || valid.Now != nil {
		t.Errorf("Validate changed the config: %+v", valid)
	}
	for _, signal := range []string{"", SignalTraces, SignalLogs, SignalMetrics, SignalAuto} {
		c := valid
		c.Signal = signal
		if err := c.Validate(); err != nil {
			t.Errorf("signal %q: Validate() = %v", signal, err)
		}
	}
	for _, tc := range []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"not a gs URL", func(c *Config) { c.Source = "s3://bucket/x" }, `source "s3://bucket/x": want gs://bucket/prefix`},
		{"no bucket", func(c *Config) { c.Source = "gs:///x" }, `source "gs:///x" names no bucket`},
		{"no project", func(c *Config) { c.Project = "" }, "project is required"},
		{"no dataset", func(c *Config) { c.Dataset = "" }, "dataset is required"},
		{"signal", func(c *Config) { c.Signal = "spans" }, `signal "spans": want traces, logs, metrics, or auto`},
		{"batch size", func(c *Config) { c.BatchSize = -1 }, "batch size must be positive, got -1"},
		{"workers", func(c *Config) { c.MaxWorkers = -2 }, "max workers must be positive, got -2"},
		{"line bytes", func(c *Config) { c.MaxLineBytes = -3 }, "max line bytes must be positive, got -3"},
		{"attempts", func(c *Config) { c.MaxAttempts = -4 }, "max attempts must be positive, got -4"},
		{"backoff", func(c *Config) { c.RetryBackoff = -time.Second }, "retry backoff must be positive, got -1s"},
		{"max backoff", func(c *Config) { c.RetryMaxBackoff = -time.Second }, "retry max backoff must be positive, got -1s"},
		{"pattern", func(c *Config) { c.BlockedValuePatterns = []string{"("} }, "redaction: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			tc.edit(&c)
			if err := c.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	c := Config{Source: testSource, Project: testProject, Dataset: testDataset, RetryBackoff: time.Minute}
	if err := c.normalize(); err != nil {
		t.Fatal(err)
	}
	got := fmt.Sprint(c.Signal, c.TracesTable, c.LogsTable, c.MetricsTable, c.BatchSize, c.MaxWorkers,
		c.MaxLineBytes, c.MaxAttempts, c.RetryBackoff, c.RetryMaxBackoff, c.Now != nil)
	want := fmt.Sprint(SignalAuto, "otel_spans", "otel_logs", "otel_metrics", 500, 4,
		64<<20, 5, time.Minute, time.Minute, true)
	if got != want {
		t.Errorf("defaults = %s, want %s", got, want)
	}
}

func TestParseSource(t *testing.T) {
	for _, tc := range []struct {
		source, bucket, prefix, err string
	}{
		{source: "gs://b", bucket: "b"},
		{source: "gs://b/", bucket: "b"},
		{source: "gs://b/exports/2025/", bucket: "b", prefix: "exports/2025/"},
		{source: "gs://b/day=1", bucket: "b", prefix: "day=1"},
		{source: "b/exports", err: "want gs://bucket/prefix"},
		{source: "gs://", err: "names no bucket"},
	} {
		bucket, prefix, err := ParseSource(tc.source)
		switch {
		case tc.err != "":
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("ParseSource(%q) error = %v, want %q", tc.source, err, tc.err)
			}
		case err != nil || bucket != tc.bucket || prefix != tc.prefix:
			t.Errorf("ParseSource(%q) = %q, %q, %v; want %q, %q", tc.source, bucket, prefix, err, tc.bucket, tc.prefix)
		}
	}
}

func TestDetectSignal(t *testing.T) {
	for _, tc := range []struct {
		line, signal, err string
	}{
		{line: `{"resourceSpans":[]}`, signal: SignalTraces},
		{line: `{"resource_logs":[]}`, signal: SignalLogs},
		{line: `{"resourceMetrics":[]}`, signal: SignalMetrics},
		{line: `{"resource_spans":[]}`, signal: SignalTraces},
		{line: `{"resourceLogs":[]}`, signal: SignalLogs},
		{line: `{"resource_metrics":[]}`, signal: SignalMetrics},
		// Only top-level keys count.
		{line: `{"meta":{"resourceSpans":[]},"resourceLogs":[]}`, signal: SignalLogs},
		{line: `{"meta":{"resourceSpans":[]}}`, err: errNotExportRequest.Error()},
		{line: `{}`, err: errNotExportRequest.Error()},
		{line: `[]`, err: "not a JSON object"},
		{line: `"resourceSpans"`, err: "not a JSON object"},
		{line: `not json`, err: "not valid JSON"},
		{line: `{"a":`, err: "not valid JSON"},
		{line: `{"a" 1}`, err: "not valid JSON"},
	} {
		signal, err := detectSignal([]byte(tc.line))
		switch {
		case tc.err != "":
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("detectSignal(%s) error = %v, want %q", tc.line, err, tc.err)
			}
		case err != nil || signal != tc.signal:
			t.Errorf("detectSignal(%s) = %q, %v; want %q", tc.line, signal, err, tc.signal)
		}
	}
}

func TestLineReader(t *testing.T) {
	type step struct {
		line string
		err  error
		n    int64
	}
	for _, tc := range []struct {
		input string
		want  []step
	}{
		{
			// A 16-byte buffer splits the long line into fragments.
			input: "short\nexactly10!\nthis line is far too long\n\ncrlf\r\ntail",
			want: []step{
				{"short\n", nil, 1},
				{"exactly10!\n", nil, 2},
				{"", errLineTooLong, 3},
				{"\n", nil, 4},
				{"crlf\r\n", nil, 5},
				{"tail", nil, 6},
				{"", io.EOF, 6},
			},
		},
		{input: "0123456789ABCDEFGHIJ", want: []step{{"", errLineTooLong, 1}, {"", io.EOF, 1}}},
		{input: "", want: []step{{"", io.EOF, 0}}},
	} {
		lr := &lineReader{r: bufio.NewReaderSize(strings.NewReader(tc.input), 16), max: 10}
		for i, want := range tc.want {
			line, err := lr.next()
			if string(line) != want.line || !errors.Is(err, want.err) || lr.n != want.n {
				t.Errorf("%q step %d: next() = %q, %v at line %d; want %q, %v at line %d",
					tc.input, i, line, err, lr.n, want.line, want.err, want.n)
			}
		}
	}
}

func TestReportText(t *testing.T) {
	rep := &Report{
		Source: testSource, Project: testProject, Dataset: testDataset,
		Objects: 3, Skipped: 1, FailedObjects: 1,
		Lines: 10, MalformedLines: 2, OtherSignalLines: 1,
		Signals: []SignalReport{
			{Signal: SignalTraces, Table: "otel_spans", Records: 7, Appended: 4, Rejected: map[string]int64{"row_bytes": 1, "invalid_id": 2}},
			{Signal: SignalLogs, Table: "otel_logs", Records: 2, Appended: 1, Unwritten: 1},
			{Signal: SignalMetrics, Table: "otel_metrics"},
		},
		Failures: []Failure{
			{Object: "gs://b/x.jsonl", Line: 3, Error: "not a JSON object"},
			{Object: "gs://b/y.jsonl", Error: "opening the object: HTTP 404"},
		},
		FailureCount: 4,
		Error:        "listing gs://b/: HTTP 403",
	}
	var buf bytes.Buffer
	if err := rep.WriteText(&buf); err != nil {
		t.Fatal(err)
	}
	want := `backfill gs://archive-bucket/exports/ -> test-project.telemetry
objects  3 archives, 1 other objects skipped, 1 with failures
lines    10 read, 2 malformed, 1 of other signals
traces   otel_spans: 7 spans, 4 appended, 3 rejected (invalid_id 2, row_bytes 1), 0 unwritten
logs     otel_logs: 2 log records, 1 appended, 0 rejected, 1 unwritten
metrics  otel_metrics: 0 data points, 0 appended, 0 rejected, 0 unwritten
FAIL     gs://b/x.jsonl line 3: not a JSON object
FAIL     gs://b/y.jsonl: opening the object: HTTP 404
FAIL     ... and 2 more
STOPPED  listing gs://b/: HTTP 403
INCOMPLETE: 5 rows appended, 4 failures
`
	if got := buf.String(); got != want {
		t.Errorf("WriteText:\n%s\nwant:\n%s", got, want)
	}

	complete := &Report{Source: testSource, Project: testProject, Dataset: testDataset,
		Signals: []SignalReport{{Signal: SignalLogs, Table: "otel_logs", Records: 2, Appended: 2}}}
	buf.Reset()
	if err := complete.WriteText(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(buf.String(), "\nCOMPLETE: 2 rows appended\n") {
		t.Errorf("WriteText:\n%s", buf.String())
	}
}

func TestReportJSON(t *testing.T) {
	e := newTestEnv(t)
	e.gcs.put("exports/mixed.jsonl", jsonl(spansLine(t, 2, "json", spanStart), "oops"))
	rep := mustRun(t, e.config())
	var buf bytes.Buffer
	if err := rep.WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Source       string `json:"source"`
		Lines        int    `json:"lines"`
		Malformed    int    `json:"malformed_lines"`
		FailureCount int    `json:"failure_count"`
		Failures     []struct {
			Object string `json:"object"`
			Line   int    `json:"line"`
		} `json:"failures"`
		Signals []struct {
			Signal   string           `json:"signal"`
			Appended int              `json:"appended"`
			Rejected map[string]int64 `json:"rejected"`
		} `json:"signals"`
		Error *string `json:"error"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("WriteJSON wrote %s: %v", buf.String(), err)
	}
	if got.Source != testSource || got.Lines != 2 || got.Malformed != 1 || got.FailureCount != 1 ||
		len(got.Failures) != 1 || got.Failures[0].Line != 2 || got.Error != nil {
		t.Errorf("WriteJSON wrote %s", buf.String())
	}
	if len(got.Signals) != 3 || got.Signals[0].Signal != SignalTraces || got.Signals[0].Appended != 2 || got.Signals[0].Rejected != nil {
		t.Errorf("signals = %+v", got.Signals)
	}
}

func TestPermanentErrorMessage(t *testing.T) {
	cause := errors.New("rpc error: code = InvalidArgument")
	schemaErr := &PermanentError{Table: "otel_spans", Label: bqi.LabelSchemaMismatch, Err: cause}
	if got, want := schemaErr.Error(), "append to otel_spans failed permanently (schema_mismatch): rpc error: code = InvalidArgument"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(schemaErr, cause) {
		t.Error("errors.Is(PermanentError, cause) = false")
	}
	permErr := &PermanentError{Table: "otel_logs", Label: bqi.LabelPermission, Err: cause}
	if got, want := permErr.Error(), bqi.PermissionHint("otel_logs")+": "+cause.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestNewGCSStoreEndpoint(t *testing.T) {
	for _, tc := range []struct {
		endpoint, want string
	}{
		{"", "https://storage.googleapis.com"},
		{"storage.example.com/", "https://storage.example.com"},
		{"http://127.0.0.1:9/", "http://127.0.0.1:9"},
	} {
		opts := []option.ClientOption{option.WithoutAuthentication()}
		if tc.endpoint != "" {
			opts = append(opts, option.WithEndpoint(tc.endpoint))
		}
		s, err := newGCSStore(context.Background(), Config{MaxAttempts: 1}, opts...)
		if err != nil {
			t.Fatalf("newGCSStore(%q): %v", tc.endpoint, err)
		}
		if s.baseURL != tc.want {
			t.Errorf("newGCSStore(%q) base URL = %q, want %q", tc.endpoint, s.baseURL, tc.want)
		}
		s.close()
	}
}
