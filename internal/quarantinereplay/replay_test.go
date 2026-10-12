// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package quarantinereplay

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/api/option"
)

func sampleSpanJSON(t *testing.T, name string) string {
	t.Helper()
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName(name)
	sp.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	sp.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	raw, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func sampleLogJSON(t *testing.T, body string) string {
	t.Helper()
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr(body)
	raw, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestReplayJSONLAndFilters(t *testing.T) {
	spanPayload := sampleSpanJSON(t, "GET /orders")
	logPayload := sampleLogJSON(t, "replayed log body")
	offloadedSpanPayload := sampleSpanJSON(t, "offloaded-span")

	records := []Record{
		{
			IngestTime: "2026-10-11T10:00:00Z",
			Signal:     "spans",
			ErrorCode:  "OVERSIZE_ROW",
			RawPayload: spanPayload,
		},
		{
			IngestTime: "2026-10-11T11:00:00Z",
			Signal:     "logs",
			ErrorCode:  "ROW_ERROR",
			RawPayload: logPayload,
		},
		{
			IngestTime: "2026-10-11T12:00:00Z",
			Signal:     "spans",
			ErrorCode:  "OVERSIZE_ROW",
			PayloadRef: &ObjectRef{URI: "gs://q-bucket/quarantine/spans/obj1.json"},
		},
		{
			// metadata_only row: neither raw_payload nor payload_ref
			IngestTime: "2026-10-11T12:30:00Z",
			Signal:     "spans",
			ErrorCode:  "OVERSIZE_ROW",
		},
	}

	var buf bytes.Buffer
	for _, r := range records {
		line, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}

	sink := NewMemorySink()
	fetcher := func(_ context.Context, uri string) ([]byte, error) {
		if uri == "gs://q-bucket/quarantine/spans/obj1.json" {
			return []byte(offloadedSpanPayload), nil
		}
		t.Fatalf("unexpected URI %q", uri)
		return nil, nil
	}

	sum, err := Replay(context.Background(), bytes.NewReader(buf.Bytes()), sink, Options{
		Filter: Filter{
			Signal:     "spans",
			ErrorCodes: []string{"OVERSIZE_ROW"},
			Since:      time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC),
			Until:      time.Date(2026, 10, 11, 13, 0, 0, 0, time.UTC),
		},
		ObjectFetcher: fetcher,
	})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if sum.TotalRead != 4 || sum.Matched != 3 || sum.SkippedFiltered != 1 || sum.SkippedNoPayload != 1 || sum.ReplayedSpans != 2 {
		t.Fatalf("Summary = %+v, want TotalRead=4 Matched=3 SkippedFiltered=1 SkippedNoPayload=1 ReplayedSpans=2", sum)
	}
	if len(sink.Traces) != 2 || len(sink.Logs) != 0 {
		t.Fatalf("sink Traces=%d Logs=%d, want 2 and 0", len(sink.Traces), len(sink.Logs))
	}
}

func TestReplayJSONArrayAndJSONWriterSink(t *testing.T) {
	records := []Record{
		{
			IngestTime: int64(1_760_000_000_000_000),
			Signal:     "logs",
			ErrorCode:  "ROW_ERROR",
			RawPayload: sampleLogJSON(t, "array-log"),
		},
	}
	arrBytes, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	sink := NewJSONWriterSink(&out)
	sum, err := Replay(context.Background(), bytes.NewReader(arrBytes), sink, Options{})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if sum.ReplayedLogs != 1 {
		t.Fatalf("ReplayedLogs = %d, want 1", sum.ReplayedLogs)
	}
	if !strings.Contains(out.String(), `"signal":"logs"`) || !strings.Contains(out.String(), "array-log") {
		t.Errorf("JSONWriterSink output = %q", out.String())
	}
}

func TestGCSObjectFetcher(t *testing.T) {
	livePayload := sampleSpanJSON(t, "gcs-live-span")
	genPayload := sampleSpanJSON(t, "gcs-gen-42-span")
	var lastQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastQuery = r.URL.RawQuery
		if r.URL.Path != "/storage/v1/b/my-q-bucket/o/path/to/span.json" {
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		switch r.URL.Query().Get("generation") {
		case "":
			_, _ = w.Write([]byte(livePayload))
		case "42":
			_, _ = w.Write([]byte(genPayload))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	fetcher, cleanup, err := NewGCSObjectFetcher(
		context.Background(),
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("NewGCSObjectFetcher() error = %v", err)
	}
	defer cleanup()

	got, err := fetcher(context.Background(), "gs://my-q-bucket/path/to/span.json")
	if err != nil {
		t.Fatalf("fetcher() error = %v", err)
	}
	if string(got) != livePayload {
		t.Errorf("fetched payload = %q, want %q", string(got), livePayload)
	}

	gotGen, err := fetcher(context.Background(), "gs://my-q-bucket/path/to/span.json?generation=42")
	if err != nil {
		t.Fatalf("fetcher(generation=42) error = %v", err)
	}
	if string(gotGen) != genPayload || !strings.Contains(lastQuery, "generation=42") {
		t.Errorf("fetched generation payload = %q (query=%q), want %q with generation=42", string(gotGen), lastQuery, genPayload)
	}

	// Missing generation must fail rather than reading the live object.
	if _, err := fetcher(context.Background(), "gs://my-q-bucket/path/to/span.json?generation=999"); err == nil {
		t.Error("expected error when requested generation returns HTTP 404")
	}

	if _, err := fetcher(context.Background(), "https://not-gs/bucket/obj"); err == nil {
		t.Error("expected error for non-gs:// URI")
	}
	if _, err := fetcher(context.Background(), "gs://bucket-only/"); err == nil {
		t.Error("expected error for gs:// URI without object key")
	}
}

func TestReplayForwardsPayloadRefGeneration(t *testing.T) {
	genPayload := sampleSpanJSON(t, "pinned-generation-span")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/storage/v1/b/q-bucket/o/quarantine/spans/item.json" && r.URL.Query().Get("generation") == "777" {
			_, _ = w.Write([]byte(genPayload))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	fetcher, cleanup, err := NewGCSObjectFetcher(
		context.Background(),
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	records := []Record{
		{
			IngestTime: "2026-10-11T12:00:00Z",
			Signal:     "spans",
			ErrorCode:  "OVERSIZE_ROW",
			PayloadRef: &ObjectRef{
				URI:     "gs://q-bucket/quarantine/spans/item.json",
				Version: "777",
			},
		},
		{
			IngestTime: "2026-10-11T12:01:00Z",
			Signal:     "spans",
			ErrorCode:  "OVERSIZE_ROW",
			PayloadRef: &ObjectRef{
				URI:     "gs://q-bucket/quarantine/spans/item.json",
				Version: "888-deleted",
			},
		},
	}
	rawInput, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}

	sink := NewMemorySink()
	sum, err := Replay(context.Background(), bytes.NewReader(rawInput), sink, Options{
		ObjectFetcher: fetcher,
	})
	if err == nil {
		t.Fatal("expected error for unavailable generation 888-deleted")
	}
	if sum.ReplayedSpans != 1 || sum.FailedUnmarshal != 1 || len(sink.Traces) != 1 {
		t.Fatalf("Summary = %+v, sink.Traces = %d, want ReplayedSpans=1 FailedUnmarshal=1", sum, len(sink.Traces))
	}
	gotName := sink.Traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name()
	if gotName != "pinned-generation-span" {
		t.Errorf("replayed span name = %q, want pinned-generation-span", gotName)
	}
}

func TestParsedIngestTimeAndEdgeCases(t *testing.T) {
	cases := []any{
		"2026-10-11 12:00:00.123456 UTC",
		"1760184000123456",
		"1760184000.5",
		json.Number("1760184000123456"),
		json.Number("1760184000.5"),
		float64(1760184000123456),
		float64(1760184000.5),
		int64(1760184000),
	}
	for _, in := range cases {
		rec := Record{IngestTime: in}
		if ts, ok := rec.ParsedIngestTime(); !ok || ts.IsZero() {
			t.Errorf("ParsedIngestTime(%#v) = (%v, %v), want valid time", in, ts, ok)
		}
	}
	for _, invalid := range []any{nil, "", "not-a-timestamp", struct{}{}} {
		rec := Record{IngestTime: invalid}
		if _, ok := rec.ParsedIngestTime(); ok {
			t.Errorf("ParsedIngestTime(%#v) succeeded unexpectedly", invalid)
		}
	}

	// Empty input returns empty summary without error.
	if sum, err := Replay(context.Background(), strings.NewReader("   \n"), NewNopSink(), Options{}); err != nil || sum.TotalRead != 0 {
		t.Errorf("empty Replay() = (%+v, %v), want (0, nil)", sum, err)
	}

	// Nil sink returns error.
	if _, err := Replay(context.Background(), strings.NewReader(""), nil, Options{}); err == nil {
		t.Error("expected error when sink is nil")
	}

	// Malformed JSONL, missing ObjectFetcher, bad OTLP JSON, and unsupported signal.
	badRecords := strings.Join([]string{
		`{"signal":"spans","error_code":"ROW_ERROR","raw_payload":"{not-valid-otlp"}`,
		`{"signal":"logs","error_code":"ROW_ERROR","raw_payload":"{not-valid-otlp"}`,
		`{"signal":"metrics","error_code":"ROW_ERROR","raw_payload":"{not-valid-otlp"}`,
		`{"signal":"unknown","error_code":"ROW_ERROR","raw_payload":"{}"}`,
		`{"signal":"spans","error_code":"ROW_ERROR","payload_ref":{"uri":"gs://b/k"}}`,
	}, "\n")
	sum, err := Replay(context.Background(), strings.NewReader(badRecords), NewNopSink(), Options{})
	if err == nil || sum.FailedUnmarshal != 5 {
		t.Errorf("Replay(badRecords) = (%+v, %v), want FailedUnmarshal=5 and joined error", sum, err)
	}
}

func sampleMetricJSON(t *testing.T, name string) string {
	t.Helper()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "billing")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(1760184000000000000))
	dp.SetDoubleValue(42.5)
	raw, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(md)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestReplayMetricsAndSignalFilterAndBadTimestamp(t *testing.T) {
	inlineMetric := sampleMetricJSON(t, "inline.gauge")
	offloadedMetric := sampleMetricJSON(t, "offloaded.gauge")
	inlineSpan := sampleSpanJSON(t, "trace-span")
	inlineLog := sampleLogJSON(t, "skip-log")

	records := []Record{
		{
			IngestTime: "2026-10-11T10:00:00Z",
			Signal:     "metrics",
			ErrorCode:  "ROW_ERROR",
			RawPayload: inlineMetric,
		},
		{
			IngestTime: "2026-10-11T10:01:00Z",
			Signal:     "otel_metrics",
			ErrorCode:  "OVERSIZE_ROW",
			PayloadRef: &ObjectRef{URI: "gs://q-bucket/metrics/m1.json", Version: "101"},
		},
		{
			IngestTime: "2026-10-11T10:02:00Z",
			Signal:     "spans",
			ErrorCode:  "ROW_ERROR",
			RawPayload: inlineSpan,
		},
		{
			IngestTime: "2026-10-11T10:03:00Z",
			Signal:     "logs",
			ErrorCode:  "ROW_ERROR",
			RawPayload: inlineLog,
		},
		{
			IngestTime: "corrupt-timestamp",
			Signal:     "metrics",
			ErrorCode:  "ROW_ERROR",
			RawPayload: inlineMetric,
		},
	}

	var jsonl bytes.Buffer
	for _, r := range records {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		jsonl.Write(b)
		jsonl.WriteByte('\n')
	}

	sink := NewMemorySink()
	since, _ := time.Parse(time.RFC3339, "2026-10-11T09:00:00Z")
	sum, err := Replay(context.Background(), &jsonl, sink, Options{
		Filter: Filter{
			Signal: "traces,metrics",
			Since:  since,
		},
		ObjectFetcher: func(_ context.Context, uri string) ([]byte, error) {
			if strings.Contains(uri, "metrics/m1.json") {
				return []byte(offloadedMetric), nil
			}
			return nil, context.DeadlineExceeded
		},
	})
	if err == nil {
		t.Fatal("expected error for corrupt ingest_time when Since is set")
	}
	if sum.TotalRead != 5 || sum.ReplayedMetrics != 2 || sum.ReplayedSpans != 1 || sum.SkippedFiltered != 1 || sum.FailedUnmarshal != 1 {
		t.Fatalf("Summary = %+v, want TotalRead=5 ReplayedMetrics=2 ReplayedSpans=1 SkippedFiltered=1 FailedUnmarshal=1", sum)
	}
	if len(sink.Metrics) != 2 {
		t.Fatalf("len(sink.Metrics) = %d, want 2", len(sink.Metrics))
	}

	var outBuf bytes.Buffer
	jw := NewJSONWriterSink(&outBuf)
	if err := jw.ExportMetrics(context.Background(), sink.Metrics[0]); err != nil {
		t.Fatalf("JSONWriterSink.ExportMetrics() error = %v", err)
	}
	if !strings.Contains(outBuf.String(), `"signal":"metrics"`) || !strings.Contains(outBuf.String(), "inline.gauge") {
		t.Errorf("JSONWriterSink output = %s", outBuf.String())
	}
}
