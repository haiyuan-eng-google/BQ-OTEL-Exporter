// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/api/option"
)

func TestCLIRun(t *testing.T) {
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName("cli-span")
	sp.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	sp.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	raw, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}

	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "quarantine.jsonl")
	outPath := filepath.Join(tmpDir, "replayed.jsonl")
	line := `{"ingest_time":"2026-10-11T10:00:00Z","signal":"spans","error_code":"OVERSIZE_ROW","raw_payload":` +
		strconvQuote(string(raw)) + "}\n"
	if err := os.WriteFile(inPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-input", inPath,
		"-output", outPath,
		"-signal", "spans",
		"-error-code", "OVERSIZE_ROW,ROW_ERROR",
		"-since", "2026-10-11T09:00:00Z",
		"-until", "2026-10-11T11:00:00Z",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run() = %d, stderr=%s", code, stderr.String())
	}
	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(outBytes), `"signal":"spans"`) || !strings.Contains(string(outBytes), "cli-span") {
		t.Errorf("replayed output = %s", string(outBytes))
	}

	// Dry-run from stdin
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"-dry-run", "-signal", "traces,metrics"}, strings.NewReader(line), &stdout, &stderr)
	if code != 0 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "replayed_spans=1") || !strings.Contains(stderr.String(), "replayed_metrics=0") {
		t.Errorf("dry-run code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}

	// Invalid -signal flag returns 2
	if got := run([]string{"-signal", "unknown"}, strings.NewReader(""), &stdout, &stderr); got != 2 {
		t.Errorf("bad -signal flag returned %d, want 2", got)
	}

	// Invalid timestamp flag returns 2
	if got := run([]string{"-since", "bad-time"}, strings.NewReader(""), &stdout, &stderr); got != 2 {
		t.Errorf("bad -since flag returned %d, want 2", got)
	}
}

func strconvQuote(s string) string {
	var b bytes.Buffer
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteByte(s[i])
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteByte(s[i])
		}
	}
	b.WriteByte('"')
	return b.String()
}

// spanPayload returns OTLP JSON traces holding one span named name.
func spanPayload(t *testing.T, name string) string {
	t.Helper()
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName(name)
	sp.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	sp.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	raw, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func quarantineLine(t *testing.T, spanName string) string {
	t.Helper()
	return `{"ingest_time":"2026-10-11T10:00:00Z","signal":"spans","error_code":"OVERSIZE_ROW","raw_payload":` +
		strconvQuote(spanPayload(t, spanName)) + "}\n"
}

func TestCLIFlagErrors(t *testing.T) {
	for _, args := range [][]string{
		{"-bq-project", "proj"},
		{"-bq-dataset", "ds"},
		{"-bq-project", "proj", "-bq-dataset", "ds", "-input", "rows.jsonl"},
		{"-bq-table", "dead_letters"},
		{"-bq-project", "Bad Project", "-bq-dataset", "ds"},
		{"-bq-project", "proj", "-bq-dataset", "ds", "-bq-table", "t`x"},
		{"-otlp-endpoint", "http://localhost:4318", "-output", "out.jsonl"},
		{"-otlp-endpoint", "http://localhost:4318", "-dry-run"},
		{"-otlp-endpoint", "ftp://localhost:4318"},
		{"-dry-run", "-checkpoint-file", "replay.ckpt"},
		{"-rate-limit", "-1"},
		{"-rate-limit", "NaN"},
		{"-rate-limit", "fast"},
	} {
		var stdout, stderr bytes.Buffer
		if got := run(args, strings.NewReader(""), &stdout, &stderr); got != 2 {
			t.Errorf("run(%q) = %d, want 2 (stderr=%q)", args, got, stderr.String())
		}
	}
}

func TestCLICheckpointResumeAppendsOutput(t *testing.T) {
	dir := t.TempDir()
	inPath := filepath.Join(dir, "quarantine.jsonl")
	outPath := filepath.Join(dir, "replayed.jsonl")
	ckptPath := filepath.Join(dir, "replay.ckpt")
	if err := os.WriteFile(inPath, []byte(quarantineLine(t, "first")+quarantineLine(t, "second")), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"-input", inPath, "-output", outPath, "-checkpoint-file", ckptPath}
	var stdout, stderr bytes.Buffer
	if code := run(args, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("first run = %d, stderr=%s", code, stderr.String())
	}

	// More rows land in the same export. The rerun replays only those and
	// keeps what the first run wrote.
	f, err := os.OpenFile(inPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(quarantineLine(t, "third")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	stderr.Reset()
	if code := run(args, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("resumed run = %d, stderr=%s", code, stderr.String())
	}
	if got := stderr.String(); !strings.Contains(got, "total_read=1 ") || !strings.Contains(got, "replayed_spans=1 ") || !strings.Contains(got, "skipped_checkpointed=2\n") {
		t.Errorf("resumed run summary = %q, want one record replayed and two skipped", got)
	}
	out, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 3 || !strings.Contains(lines[0], "first") || !strings.Contains(lines[1], "second") || !strings.Contains(lines[2], "third") {
		t.Errorf("output after the resumed run = %q, want first, second, third", lines)
	}
	ckpt, err := os.ReadFile(ckptPath)
	if err != nil {
		t.Fatal(err)
	}
	if want := `"source": "file:` + inPath + `"`; !strings.Contains(string(ckpt), want) {
		t.Errorf("checkpoint = %s, want it to name the input as %s", ckpt, want)
	}

	// Without a checkpoint the output is replaced, as before.
	if code := run([]string{"-input", inPath, "-output", outPath}, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("run without a checkpoint = %d, stderr=%s", code, stderr.String())
	}
	if out, err := os.ReadFile(outPath); err != nil || strings.Count(string(out), "\n") != 3 {
		t.Errorf("output without a checkpoint = %q, %v; want the three records once", out, err)
	}
}

// fakeBigQuery answers jobs.query for project "proj" with every row at once.
// It ignores the resume cursor, so a resumed run has to skip the rows it
// handled before.
type fakeBigQuery struct {
	mu      sync.Mutex
	rows    [][]any
	queries []string
}

func (f *fakeBigQuery) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
	}
	if r.Method != http.MethodPost || r.URL.Path != "/bigquery/v2/projects/proj/queries" || json.NewDecoder(r.Body).Decode(&req) != nil {
		http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, req.Query)
	var fields []map[string]string
	for _, name := range []string{"ingest_time_us", "signal", "error_code", "raw_payload", "row_fp", "row_ord", "row_key"} {
		typ := "STRING"
		if name == "ingest_time_us" || name == "row_fp" || name == "row_ord" {
			typ = "INTEGER"
		}
		fields = append(fields, map[string]string{"name": name, "type": typ})
	}
	var rows []map[string]any
	for _, row := range f.rows {
		var cells []map[string]any
		for _, v := range row {
			cells = append(cells, map[string]any{"v": v})
		}
		rows = append(rows, map[string]any{"f": cells})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jobReference": map[string]string{"projectId": "proj", "jobId": "job-1", "location": "US"},
		"jobComplete":  true,
		"schema":       map[string]any{"fields": fields},
		"rows":         rows,
	})
}

// traceReceiver is an OTLP/HTTP receiver for traces that records span names.
type traceReceiver struct {
	mu    sync.Mutex
	names []string
}

func (rc *traceReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	req := ptraceotlp.NewExportRequest()
	if err != nil || r.URL.Path != "/v1/traces" || req.UnmarshalProto(body) != nil {
		http.Error(w, "unexpected request", http.StatusBadRequest)
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rss := req.Traces().ResourceSpans()
	for i := range rss.Len() {
		sss := rss.At(i).ScopeSpans()
		for j := range sss.Len() {
			spans := sss.At(j).Spans()
			for k := range spans.Len() {
				rc.names = append(rc.names, spans.At(k).Name())
			}
		}
	}
}

func TestCLIBigQueryToOTLP(t *testing.T) {
	micros := strconv.FormatInt(time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC).UnixMicro(), 10)
	spanKey := `{"signal":"spans","error_code":"OVERSIZE_ROW","raw_payload":"` + spanPayload(t, "from-bigquery") + `"}`
	logKey := `{"signal":"logs","error_code":"ROW_ERROR","raw_payload":""}`
	bq := &fakeBigQuery{rows: [][]any{
		{micros, "spans", "OVERSIZE_ROW", spanPayload(t, "from-bigquery"), "42", "0", spanKey},
		{micros, "logs", "ROW_ERROR", "", "43", "0", logKey},
	}}
	bqSrv := httptest.NewServer(bq)
	defer bqSrv.Close()
	prev := bigQueryClientOptions
	bigQueryClientOptions = func() []option.ClientOption {
		return []option.ClientOption{option.WithEndpoint(bqSrv.URL + "/bigquery/v2/"), option.WithoutAuthentication()}
	}
	defer func() { bigQueryClientOptions = prev }()
	rc := &traceReceiver{}
	otlpSrv := httptest.NewServer(rc)
	defer otlpSrv.Close()

	args := []string{
		"-bq-project", "proj", "-bq-dataset", "ds", "-bq-table", "dead_letters", "-signal", "spans",
		"-otlp-endpoint", otlpSrv.URL, "-rate-limit", "1000", "-checkpoint-file", filepath.Join(t.TempDir(), "bq.ckpt"),
	}
	var stdout, stderr bytes.Buffer
	if code := run(args, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("run = %d, stderr=%s", code, stderr.String())
	}
	if want := "total_read=2 matched=1 replayed_spans=1 replayed_logs=0 replayed_metrics=0 skipped_filtered=1 "; !strings.Contains(stderr.String(), want) {
		t.Errorf("summary = %q, want %q", stderr.String(), want)
	}
	if stdout.Len() != 0 {
		t.Errorf("an OTLP replay wrote %q to stdout", stdout.String())
	}
	if len(bq.queries) != 1 || !strings.Contains(bq.queries[0], "FROM `proj.ds.dead_letters`") || !strings.Contains(bq.queries[0], "signal IN UNNEST(@signals)") {
		t.Errorf("queries = %q", bq.queries)
	}

	// The rerun resumes after both rows, so nothing is sent twice.
	stderr.Reset()
	if code := run(args, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("rerun = %d, stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "total_read=0 ") || !strings.Contains(stderr.String(), "skipped_checkpointed=2\n") {
		t.Errorf("rerun summary = %q, want both rows skipped", stderr.String())
	}
	if len(bq.queries) != 2 || !strings.Contains(bq.queries[1], "@cursor_time") {
		t.Errorf("the rerun did not resume from the cursor: %q", bq.queries)
	}
	if want := []string{"from-bigquery"}; len(rc.names) != 1 || rc.names[0] != want[0] {
		t.Errorf("receiver got spans %v, want %v", rc.names, want)
	}
}
