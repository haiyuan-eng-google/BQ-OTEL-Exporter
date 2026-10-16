// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	stdhttp "net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

type testPayloadOffloader struct{}

func (testPayloadOffloader) OffloadSpan(_ context.Context, span ptrace.Span, _ pcommon.Resource, _ pcommon.InstrumentationScope) ([]transform.PayloadRef, error) {
	v, ok := span.Attributes().Get("gen_ai.prompt.large")
	if !ok {
		return nil, nil
	}
	origLen := int64(len(v.Str()))
	span.Attributes().PutStr("gen_ai.prompt.large", "<offloaded:gs://test-bucket/spans/prompt-001.json>")
	return []transform.PayloadRef{
		{
			AttributePath: "attributes.gen_ai.prompt.large",
			PartIndex:     0,
			MIMEType:      "application/json",
			StorageMode:   "gcs_object_ref",
			ByteSize:      origLen,
			SHA256:        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			ObjectRef: transform.ObjectRef{
				URI:        "gs://test-bucket/spans/prompt-001.json",
				Version:    "1",
				Authorizer: "projects/my-project/locations/us/connections/gcs-conn",
				Details:    `{"content_type":"application/json"}`,
			},
		},
	}, nil
}

func (testPayloadOffloader) OffloadLog(_ context.Context, lr plog.LogRecord, _ pcommon.Resource, _ pcommon.InstrumentationScope) ([]transform.PayloadRef, error) {
	if lr.Body().Type() != pcommon.ValueTypeStr || !strings.HasPrefix(lr.Body().Str(), "OFFLOAD:") {
		return nil, nil
	}
	origLen := int64(len(lr.Body().Str()))
	lr.Body().SetStr("<offloaded:gs://test-bucket/logs/body-001.txt>")
	return []transform.PayloadRef{
		{
			AttributePath: "body",
			PartIndex:     0,
			MIMEType:      "text/plain",
			StorageMode:   "gcs_object_ref",
			ByteSize:      origLen,
			SHA256:        "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e",
			ObjectRef: transform.ObjectRef{
				URI:     "gs://test-bucket/logs/body-001.txt",
				Version: "1",
			},
		},
	}, nil
}

func wireFakeHooks(t *testing.T, srv *bqfake.Server) {
	t.Helper()
	origHTTP := newBigQueryHTTPClient
	origWriter := newManagedWriterClient
	t.Cleanup(func() {
		newBigQueryHTTPClient = origHTTP
		newManagedWriterClient = origWriter
	})

	newBigQueryHTTPClient = func(_ context.Context, _ ...option.ClientOption) (*stdhttp.Client, *stdhttp.Transport, error) {
		c, tr := srv.NewHTTPClient()
		return c, tr, nil
	}
	newManagedWriterClient = func(ctx context.Context, projectID string, _ ...option.ClientOption) (*managedwriter.Client, error) {
		return managedwriter.NewClient(ctx, projectID, srv.WriteClientOptions()...)
	}
}

func TestVerticalSliceEndToEndTracesLogsAndQuarantine(t *testing.T) {
	srv := bqfake.NewServer(t)
	wireFakeHooks(t, srv)

	cfg := validConfig()
	cfg.Project = "my-project"
	cfg.Dataset = "otel"
	cfg.Location = "US"
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	cfg.AutoCreate.CompanionViews = true
	cfg.Quarantine.Enabled = true

	res := pcommon.NewResource()
	res.Attributes().PutStr("service.instance.id", "collector-pod-1")
	settings := component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
		Resource:      res,
	}

	ctx := context.Background()
	tracesExp, err := newTracesExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	tracesExp.opts.PayloadOffloader = testPayloadOffloader{}
	if err := tracesExp.start(ctx, nil); err != nil {
		t.Fatalf("tracesExporter.start: %v", err)
	}
	t.Cleanup(func() { _ = tracesExp.shutdown(context.Background()) })

	logsExp, err := newLogsExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newLogsExporter: %v", err)
	}
	logsExp.opts.PayloadOffloader = testPayloadOffloader{}
	if err := logsExp.start(ctx, nil); err != nil {
		t.Fatalf("logsExporter.start: %v", err)
	}
	t.Cleanup(func() { _ = logsExp.shutdown(context.Background()) })

	// Verify control-plane auto-creation of dataset, tables, dedup views, and companion views.
	if _, ok := srv.Dataset("my-project", "otel"); !ok {
		t.Fatal("expected dataset my-project.otel to be created via REST v2")
	}
	for _, name := range []string{
		"otel_spans",
		"otel_spans_dedup",
		"otel_logs",
		"otel_logs_dedup",
		"otel_quarantine",
		schema.SpansAsAllSpansViewName,
		schema.GenAISpansViewName,
		schema.GenAILogsViewName,
	} {
		if _, ok := srv.Table("my-project", "otel", name); !ok {
			t.Errorf("expected REST table/view %q to be created", name)
		}
	}

	// Build a trace batch with 1 rich valid span and 1 invalid span (empty span_id -> quarantined).
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.SetSchemaUrl("https://opentelemetry.io/schemas/1.26.0")
	rs.Resource().Attributes().PutStr("service.name", "vertex-agent-svc")
	rs.Resource().Attributes().PutInt("host.cpu.count", 8)
	rs.Resource().SetDroppedAttributesCount(1)

	ss := rs.ScopeSpans().AppendEmpty()
	ss.SetSchemaUrl("https://opentelemetry.io/schemas/1.26.0")
	ss.Scope().SetName("google.adk.tracer")
	ss.Scope().SetVersion("2.6.3")
	ss.Scope().Attributes().PutStr("telemetry.sdk.language", "python")

	validSpan := ss.Spans().AppendEmpty()
	traceID := pcommon.TraceID{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0, 0xf0, 0x01}
	spanID := pcommon.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	parentID := pcommon.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}
	startTime := time.Date(2026, 8, 12, 15, 42, 19, 123456000, time.UTC)
	endTime := startTime.Add(150 * time.Millisecond)

	validSpan.SetTraceID(traceID)
	validSpan.SetSpanID(spanID)
	validSpan.SetParentSpanID(parentID)
	validSpan.SetName("call_llm")
	validSpan.SetKind(ptrace.SpanKindClient)
	validSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(startTime))
	validSpan.SetEndTimestamp(pcommon.NewTimestampFromTime(endTime))
	validSpan.Status().SetCode(ptrace.StatusCodeOk)
	validSpan.Status().SetMessage("completed")
	validSpan.Attributes().PutStr("gen_ai.system", "gcp.vertex.agent")
	validSpan.Attributes().PutStr("gen_ai.operation.name", "call_llm")
	validSpan.Attributes().PutInt("gen_ai.usage.input_tokens", 512)
	validSpan.Attributes().PutInt("gen_ai.usage.output_tokens", 128)
	validSpan.Attributes().PutStr("gen_ai.prompt.large", "large-inline-prompt-payload")

	ev := validSpan.Events().AppendEmpty()
	ev.SetName("gen_ai.content.prompt")
	ev.SetTimestamp(pcommon.NewTimestampFromTime(startTime.Add(10 * time.Millisecond)))
	ev.Attributes().PutStr("gen_ai.prompt", "Summarize quarterly revenue")

	link := validSpan.Links().AppendEmpty()
	link.SetTraceID(pcommon.TraceID{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99})
	link.SetSpanID(pcommon.SpanID{0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22})
	link.Attributes().PutStr("link.role", "parent_invocation")

	// Second span in the same batch has an empty span_id and must be routed to otel_quarantine.
	invalidSpan := ss.Spans().AppendEmpty()
	invalidSpan.SetTraceID(traceID)
	invalidSpan.SetName("broken_span")
	invalidSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(startTime))
	invalidSpan.SetEndTimestamp(pcommon.NewTimestampFromTime(endTime))

	if err := tracesExp.pushTraceData(ctx, td); err != nil {
		t.Fatalf("pushTraceData: %v", err)
	}

	spanRows := srv.Rows("otel_spans")
	if len(spanRows) != 1 {
		t.Fatalf("otel_spans rows = %d, want 1", len(spanRows))
	}
	gotSpan := spanRows[0]

	if gotSpan["trace_id"] != "102030405060708090a0b0c0d0e0f001" {
		t.Errorf("trace_id = %v, want 102030405060708090a0b0c0d0e0f001", gotSpan["trace_id"])
	}
	if gotSpan["span_id"] != "0102030405060708" {
		t.Errorf("span_id = %v, want 0102030405060708", gotSpan["span_id"])
	}
	if gotSpan["parent_span_id"] != "1112131415161718" {
		t.Errorf("parent_span_id = %v, want 1112131415161718", gotSpan["parent_span_id"])
	}
	startHour, ok := bqfake.StartTimeHour(gotSpan)
	if !ok || !startHour.Equal(time.Date(2026, 8, 12, 15, 0, 0, 0, time.UTC)) {
		t.Errorf("start_time_hour = (%v, %v), want 2026-08-12T15:00:00Z", startHour, ok)
	}
	if gotSpan["service_name"] != "vertex-agent-svc" {
		t.Errorf("service_name = %v, want vertex-agent-svc", gotSpan["service_name"])
	}
	if gotSpan["schema_version"] != schema.Version {
		t.Errorf("schema_version = %v, want %s", gotSpan["schema_version"], schema.Version)
	}

	// Verify plain JSON attributes and attributes_schema sidecar.
	var spanAttrs map[string]any
	if err := json.Unmarshal([]byte(gotSpan["attributes"].(string)), &spanAttrs); err != nil {
		t.Fatalf("unmarshal span attributes: %v", err)
	}
	if spanAttrs["gen_ai.system"] != "gcp.vertex.agent" {
		t.Errorf("attributes[gen_ai.system] = %v, want gcp.vertex.agent", spanAttrs["gen_ai.system"])
	}
	if spanAttrs["gen_ai.usage.input_tokens"] != "512" {
		t.Errorf("attributes[gen_ai.usage.input_tokens] = %v, want \"512\"", spanAttrs["gen_ai.usage.input_tokens"])
	}
	if spanAttrs["gen_ai.prompt.large"] != "<offloaded:gs://test-bucket/spans/prompt-001.json>" {
		t.Errorf("attributes[gen_ai.prompt.large] = %v, want offloaded placeholder", spanAttrs["gen_ai.prompt.large"])
	}

	var spanSchema map[string]any
	if err := json.Unmarshal([]byte(gotSpan["attributes_schema"].(string)), &spanSchema); err != nil {
		t.Fatalf("unmarshal span attributes_schema: %v", err)
	}
	if spanSchema["gen_ai.usage.input_tokens"] != "int64" {
		t.Errorf("attributes_schema[gen_ai.usage.input_tokens] = %v, want int64", spanSchema["gen_ai.usage.input_tokens"])
	}

	// Verify resource, instrumentation_scope, status, events, links, and payload_refs.
	resMap, ok := gotSpan["resource"].(map[string]any)
	if !ok || resMap["dropped_attributes_count"] != int64(1) {
		t.Errorf("resource = %#v, want dropped_attributes_count=1", gotSpan["resource"])
	}
	scopeMap, ok := gotSpan["instrumentation_scope"].(map[string]any)
	if !ok || scopeMap["name"] != "google.adk.tracer" || scopeMap["version"] != "2.6.3" {
		t.Errorf("instrumentation_scope = %#v, want google.adk.tracer@2.6.3", gotSpan["instrumentation_scope"])
	}
	statusMap, ok := gotSpan["status"].(map[string]any)
	if !ok || statusMap["code_name"] != "STATUS_CODE_OK" || statusMap["message"] != "completed" {
		t.Errorf("status = %#v, want STATUS_CODE_OK / completed", gotSpan["status"])
	}
	eventsSlice, ok := gotSpan["events"].([]map[string]any)
	if !ok || len(eventsSlice) != 1 || eventsSlice[0]["name"] != "gen_ai.content.prompt" {
		t.Errorf("events = %#v, want 1 gen_ai.content.prompt event", gotSpan["events"])
	}
	linksSlice, ok := gotSpan["links"].([]map[string]any)
	if !ok || len(linksSlice) != 1 || linksSlice[0]["span_id"] != "9988776655443322" {
		t.Errorf("links = %#v, want 1 link with span_id 9988776655443322", gotSpan["links"])
	}
	refsSlice, ok := gotSpan["payload_refs"].([]map[string]any)
	if !ok || len(refsSlice) != 1 {
		t.Fatalf("payload_refs = %#v, want 1 entry", gotSpan["payload_refs"])
	}
	objRef, ok := refsSlice[0]["object_ref"].(map[string]any)
	if !ok || objRef["uri"] != "gs://test-bucket/spans/prompt-001.json" {
		t.Errorf("payload_refs[0].object_ref = %#v, want gs://test-bucket/spans/prompt-001.json", refsSlice[0]["object_ref"])
	}

	// Verify quarantine row for the invalid span.
	qRows := srv.Rows("otel_quarantine")
	if len(qRows) != 1 {
		t.Fatalf("otel_quarantine rows = %d, want 1", len(qRows))
	}
	if qRows[0]["signal"] != schema.SignalSpans || qRows[0]["error_code"] != "INVALID_SPAN_ID" {
		t.Errorf("quarantine row = %#v, want signal=spans error_code=INVALID_SPAN_ID", qRows[0])
	}
	if qRows[0]["exporter_instance"] != "collector-pod-1" {
		t.Errorf("quarantine exporter_instance = %v, want collector-pod-1", qRows[0]["exporter_instance"])
	}

	// Now push a logs batch with 2 identical records (exercising intra_batch_ordinal fingerprint)
	// and 1 offloaded log body.
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "vertex-agent-svc")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("google.adk.logger")

	for i := 0; i < 2; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(startTime))
		lr.SetTraceID(traceID)
		lr.SetSpanID(spanID)
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.SetSeverityText("INFO")
		lr.Attributes().PutStr("event.name", "gen_ai.choice")
		lr.Attributes().PutStr("gen_ai.system", "gcp.vertex.agent")
		lr.Body().SetStr("OFFLOAD:large-completion-body")
	}

	if err := logsExp.pushLogData(ctx, ld); err != nil {
		t.Fatalf("pushLogData: %v", err)
	}

	logRows := srv.Rows("otel_logs")
	if len(logRows) != 2 {
		t.Fatalf("otel_logs rows = %d, want 2", len(logRows))
	}
	fp0, _ := logRows[0]["record_fingerprint"].(string)
	fp1, _ := logRows[1]["record_fingerprint"].(string)
	if !strings.HasPrefix(fp0, schema.FingerprintPrefix) || !strings.HasPrefix(fp1, schema.FingerprintPrefix) {
		t.Errorf("fingerprints = (%q, %q), want %q prefix", fp0, fp1, schema.FingerprintPrefix)
	}
	if fp0 == fp1 {
		t.Errorf("identical records in same batch produced identical fingerprint %q; intra_batch_ordinal did not separate them", fp0)
	}
	if logRows[0]["event_name"] != "gen_ai.choice" {
		t.Errorf("logRows[0][event_name] = %v, want gen_ai.choice", logRows[0]["event_name"])
	}
	if logRows[0]["body"] != `"<offloaded:gs://test-bucket/logs/body-001.txt>"` {
		t.Errorf("logRows[0][body] = %v, want offloaded placeholder JSON string", logRows[0]["body"])
	}
}

func TestVerticalSliceServiceRowErrorsAndTransientRecovery(t *testing.T) {
	srv := bqfake.NewServer(t)
	wireFakeHooks(t, srv)

	cfg := validConfig()
	cfg.Project = "my-project"
	cfg.Dataset = "otel"
	cfg.Location = "US"
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	cfg.Quarantine.Enabled = true

	settings := component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}
	ctx := context.Background()

	tracesExp, err := newTracesExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	if err := tracesExp.start(ctx, nil); err != nil {
		t.Fatalf("tracesExporter.start: %v", err)
	}
	t.Cleanup(func() { _ = tracesExp.shutdown(context.Background()) })

	// Inject:
	// 1. RowError on row index 1 of a 3-span batch.
	// 2. Unavailable on the first retry of the 2 valid spans.
	// 3. DelayedAck on the second retry (succeeds).
	srv.EnqueueFault(
		bqfake.Fault{
			Mode:  bqfake.FaultRowErrors,
			Table: "otel_spans",
			RowErrors: []*storagepb.RowError{
				{Index: 1, Code: storagepb.RowError_FIELDS_ERROR, Message: "invalid value in attributes"},
			},
		},
		bqfake.Fault{Mode: bqfake.FaultUnavailable, Table: "otel_spans"},
		bqfake.Fault{Mode: bqfake.FaultDelayedAck, Table: "otel_spans", Delay: 5 * time.Millisecond},
	)

	batch := newTestTraces(3)
	err = tracesExp.pushTraceData(ctx, batch)
	var traceErr consumererror.Traces
	if !errors.As(err, &traceErr) {
		t.Fatalf("push #1 error = %T (%v), want consumererror.Traces", err, err)
	}
	if traceErr.Data().SpanCount() != 2 {
		t.Fatalf("retryable span count = %d, want 2", traceErr.Data().SpanCount())
	}
	// The rejected row must already be in otel_quarantine.
	if srv.RowCount("otel_quarantine") != 1 {
		t.Fatalf("otel_quarantine count = %d, want 1", srv.RowCount("otel_quarantine"))
	}

	// Retry #1 hits FaultUnavailable -> returns retryable 2 spans.
	err = tracesExp.pushTraceData(ctx, traceErr.Data())
	if !errors.As(err, &traceErr) || traceErr.Data().SpanCount() != 2 {
		t.Fatalf("retry #1 error = %v, want 2 retryable spans", err)
	}

	// Retry #2 waits for DelayedAck and succeeds.
	if err := tracesExp.pushTraceData(ctx, traceErr.Data()); err != nil {
		t.Fatalf("retry #2 failed: %v", err)
	}

	gotSpans := srv.Rows("otel_spans")
	if len(gotSpans) != 2 {
		t.Fatalf("otel_spans count = %d, want 2", len(gotSpans))
	}
	if gotSpans[0]["name"] != "a" || gotSpans[1]["name"] != "c" {
		t.Errorf("delivered span names = [%v, %v], want [a, c]", gotSpans[0]["name"], gotSpans[1]["name"])
	}
}

// metricsE2EBase is the timestamp of the first data point in the metrics batch
// the end-to-end tests push. It falls on a whole second, so the expected
// microsecond and nanosecond columns read naturally.
var metricsE2EBase = time.Date(2026, 8, 12, 15, 42, 19, 0, time.UTC)

const (
	metricsE2ESchemaURL = "https://opentelemetry.io/schemas/1.37.0"

	// The exemplars of the metrics batch link to the span that the traces
	// end-to-end test exports, so a trace and its metrics can be joined.
	metricsE2ETraceHex = "102030405060708090a0b0c0d0e0f001"
	metricsE2ESpanHex  = "0102030405060708"
)

func metricsE2EAt(offset time.Duration) pcommon.Timestamp {
	return pcommon.NewTimestampFromTime(metricsE2EBase.Add(offset))
}

func metricsE2EMicros(offset time.Duration) int64 {
	return metricsE2EBase.Add(offset).UnixMicro()
}

func metricsE2ENanos(offset time.Duration) int64 {
	return metricsE2EBase.Add(offset).UnixNano()
}

// linkedExemplar appends an exemplar recorded offset after metricsE2EBase that
// links to the span of the traces end-to-end test. The caller sets its value.
func linkedExemplar(exemplars pmetric.ExemplarSlice, offset time.Duration) pmetric.Exemplar {
	ex := exemplars.AppendEmpty()
	ex.SetTimestamp(metricsE2EAt(offset))
	ex.SetTraceID(pcommon.TraceID{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0, 0xf0, 0x01})
	ex.SetSpanID(pcommon.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08})
	return ex
}

// newGenAIMetricsBatch builds the batch that the metrics end-to-end tests
// push: one resource and scope that carry every OTLP metric type, exemplars
// that link to a span, and one data point with no timestamp, which breaks the
// metric contract and must be quarantined. A data point's position, in the
// order the exporter walks the batch, is:
//
//	0  gauge                  gen_ai.client.queue.depth         int value, exemplar
//	1  gauge                  gen_ai.client.queue.depth         no timestamp, quarantined
//	2  gauge                  gen_ai.client.queue.depth         double value
//	3  sum                    gen_ai.client.request.count       cumulative and monotonic, exemplar
//	4  histogram              gen_ai.client.token.usage         input tokens, exemplar
//	5  histogram              gen_ai.client.token.usage         output tokens, exemplar
//	6  histogram              gen_ai.client.operation.duration  error.type, exemplar
//	7  exponential histogram  gen_ai.client.latency.drift       zero, positive and negative buckets, exemplar
//	8  summary                gen_ai.client.latency.legacy      quantiles
//
// Every call builds a new batch, so a test can push equal batches more than
// once.
func newGenAIMetricsBatch() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl(metricsE2ESchemaURL)
	rm.Resource().Attributes().PutStr("service.name", "vertex-agent-svc")
	rm.Resource().Attributes().PutStr("deployment.environment.name", "staging")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.SetSchemaUrl(metricsE2ESchemaURL)
	sm.Scope().SetName("google.adk.meter")
	sm.Scope().SetVersion("2.6.3")
	sm.Scope().Attributes().PutStr("telemetry.sdk.language", "python")

	// Points 0 to 2: gauge.
	queue := sm.Metrics().AppendEmpty()
	queue.SetName("gen_ai.client.queue.depth")
	queue.SetUnit("{request}")
	queue.SetDescription("Requests waiting for a model slot")
	gauge := queue.SetEmptyGauge().DataPoints()

	gdp := gauge.AppendEmpty()
	gdp.SetTimestamp(metricsE2EAt(0))
	gdp.SetIntValue(7)
	gdp.Attributes().PutStr("gen_ai.provider.name", "gcp.vertex_ai")
	gex := linkedExemplar(gdp.Exemplars(), 0)
	gex.SetIntValue(7)
	gex.FilteredAttributes().PutStr("request.id", "req-1")

	// A data point with no timestamp breaks the metric contract. It is
	// quarantined, and the points around it are not affected.
	gdp = gauge.AppendEmpty()
	gdp.SetDoubleValue(1)
	gdp.Attributes().PutStr("gen_ai.provider.name", "gcp.vertex_ai")

	gdp = gauge.AppendEmpty()
	gdp.SetTimestamp(metricsE2EAt(time.Second))
	gdp.SetDoubleValue(3.5)
	gdp.Attributes().PutStr("gen_ai.provider.name", "gcp.vertex_ai")

	// Point 3: sum.
	requests := sm.Metrics().AppendEmpty()
	requests.SetName("gen_ai.client.request.count")
	requests.SetUnit("{request}")
	requests.SetDescription("GenAI requests issued")
	total := requests.SetEmptySum()
	total.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	total.SetIsMonotonic(true)
	sdp := total.DataPoints().AppendEmpty()
	sdp.SetStartTimestamp(metricsE2EAt(-time.Hour))
	sdp.SetTimestamp(metricsE2EAt(time.Second))
	sdp.SetIntValue(42)
	sdp.Attributes().PutStr("gen_ai.operation.name", "chat")
	linkedExemplar(sdp.Exemplars(), 900*time.Millisecond).SetDoubleValue(1)

	// Points 4 and 5: histogram of input and output tokens.
	tokens := sm.Metrics().AppendEmpty()
	tokens.SetName("gen_ai.client.token.usage")
	tokens.SetUnit("{token}")
	tokens.SetDescription("Number of input and output tokens used")
	tokenHistogram := tokens.SetEmptyHistogram()
	tokenHistogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	for _, p := range []struct {
		tokenType     string
		sum, lo, hi   float64
		buckets       []uint64
		exemplarAt    time.Duration
		exemplarValue float64
	}{
		{"input", 1536, 128, 900, []uint64{0, 0, 0, 0, 1, 2, 0}, 1500 * time.Millisecond, 508},
		{"output", 384, 32, 200, []uint64{0, 0, 0, 1, 2, 0, 0}, 1600 * time.Millisecond, 152},
	} {
		hdp := tokenHistogram.DataPoints().AppendEmpty()
		hdp.SetStartTimestamp(metricsE2EAt(time.Second))
		hdp.SetTimestamp(metricsE2EAt(2 * time.Second))
		hdp.SetCount(3)
		hdp.SetSum(p.sum)
		hdp.SetMin(p.lo)
		hdp.SetMax(p.hi)
		hdp.ExplicitBounds().FromRaw([]float64{1, 4, 16, 64, 256, 1024})
		hdp.BucketCounts().FromRaw(p.buckets)
		hdp.Attributes().PutStr("gen_ai.operation.name", "chat")
		hdp.Attributes().PutStr("gen_ai.provider.name", "gcp.vertex_ai")
		hdp.Attributes().PutStr("gen_ai.request.model", "gemini-2.5-pro")
		hdp.Attributes().PutStr("gen_ai.token.type", p.tokenType)
		hex := linkedExemplar(hdp.Exemplars(), p.exemplarAt)
		hex.SetDoubleValue(p.exemplarValue)
		hex.FilteredAttributes().PutStr("session.id", "sess-42")
	}

	// Point 6: histogram of operation durations.
	duration := sm.Metrics().AppendEmpty()
	duration.SetName("gen_ai.client.operation.duration")
	duration.SetUnit("s")
	duration.SetDescription("GenAI operation duration")
	durationHistogram := duration.SetEmptyHistogram()
	durationHistogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	ddp := durationHistogram.DataPoints().AppendEmpty()
	ddp.SetStartTimestamp(metricsE2EAt(2 * time.Second))
	ddp.SetTimestamp(metricsE2EAt(3 * time.Second))
	ddp.SetCount(2)
	ddp.SetSum(3.5)
	ddp.SetMin(1)
	ddp.SetMax(2.5)
	ddp.ExplicitBounds().FromRaw([]float64{0.25, 0.5, 1, 2, 4})
	ddp.BucketCounts().FromRaw([]uint64{0, 0, 1, 0, 1, 0})
	ddp.Attributes().PutStr("error.type", "timeout")
	ddp.Attributes().PutStr("gen_ai.operation.name", "chat")
	ddp.Attributes().PutStr("gen_ai.provider.name", "gcp.vertex_ai")
	ddp.Attributes().PutStr("gen_ai.request.model", "gemini-2.5-pro")
	linkedExemplar(ddp.Exemplars(), 2900*time.Millisecond).SetDoubleValue(2.5)

	// Point 7: exponential histogram. The metric is a signed drift, so it
	// fills the negative buckets as well as the positive ones, and puts the
	// STRUCT columns of both on the wire.
	drift := sm.Metrics().AppendEmpty()
	drift.SetName("gen_ai.client.latency.drift")
	drift.SetUnit("s")
	drift.SetDescription("Signed drift of the observed latency from its baseline")
	driftHistogram := drift.SetEmptyExponentialHistogram()
	driftHistogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	edp := driftHistogram.DataPoints().AppendEmpty()
	edp.SetStartTimestamp(metricsE2EAt(2 * time.Second))
	edp.SetTimestamp(metricsE2EAt(3 * time.Second))
	edp.SetScale(2)
	edp.SetCount(6)
	edp.SetZeroCount(1)
	edp.SetZeroThreshold(0.001)
	edp.SetSum(1.25)
	edp.SetMin(-1.5)
	edp.SetMax(2)
	edp.Positive().SetOffset(-2)
	edp.Positive().BucketCounts().FromRaw([]uint64{1, 2, 1})
	edp.Negative().SetOffset(1)
	edp.Negative().BucketCounts().FromRaw([]uint64{1})
	edp.Attributes().PutStr("gen_ai.operation.name", "chat")
	edp.Attributes().PutStr("gen_ai.request.model", "gemini-2.5-pro")
	linkedExemplar(edp.Exemplars(), 2500*time.Millisecond).SetIntValue(2)

	// Point 8: summary.
	legacy := sm.Metrics().AppendEmpty()
	legacy.SetName("gen_ai.client.latency.legacy")
	legacy.SetUnit("s")
	legacy.SetDescription("Latency quantiles reported by a legacy summary instrument")
	ydp := legacy.SetEmptySummary().DataPoints().AppendEmpty()
	ydp.SetStartTimestamp(metricsE2EAt(0))
	ydp.SetTimestamp(metricsE2EAt(4 * time.Second))
	ydp.SetCount(10)
	ydp.SetSum(12.5)
	for _, q := range [][2]float64{{0.5, 1}, {0.99, 2.25}, {1, 3}} {
		qv := ydp.QuantileValues().AppendEmpty()
		qv.SetQuantile(q[0])
		qv.SetValue(q[1])
	}
	ydp.Attributes().PutStr("gen_ai.provider.name", "gcp.vertex_ai")
	return md
}

// newIdenticalPointsBatch builds a batch whose first two data points are
// identical and whose third differs by value.
func newIdenticalPointsBatch() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "vertex-agent-svc")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("gen_ai.client.queue.depth")
	gauge := m.SetEmptyGauge().DataPoints()
	for _, v := range []int64{5, 5, 6} {
		dp := gauge.AppendEmpty()
		dp.SetTimestamp(metricsE2EAt(0))
		dp.SetIntValue(v)
	}
	return md
}

// decodedFloats and decodedInts build the repeated-scalar columns of a decoded
// row, which bqfake returns as []any of float64 and int64.
func decodedFloats(vs ...float64) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = v
	}
	return out
}

func decodedInts(vs ...int64) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = v
	}
	return out
}

// wantMetricRow returns the columns that every data point of
// newGenAIMetricsBatch shares, overlaid with the columns of one point.
func wantMetricRow(point map[string]any) map[string]any {
	want := map[string]any{
		"service_name":        "vertex-agent-svc",
		"flags":               int64(0),
		"schema_version":      schema.Version,
		"resource_schema_url": metricsE2ESchemaURL,
		"scope_schema_url":    metricsE2ESchemaURL,
		"resource": map[string]any{
			"attributes":               `{"deployment.environment.name":"staging","service.name":"vertex-agent-svc"}`,
			"attributes_schema":        `{"deployment.environment.name":"string","service.name":"string"}`,
			"dropped_attributes_count": int64(0),
		},
		"instrumentation_scope": map[string]any{
			"name":                     "google.adk.meter",
			"version":                  "2.6.3",
			"attributes":               `{"telemetry.sdk.language":"python"}`,
			"attributes_schema":        `{"telemetry.sdk.language":"string"}`,
			"dropped_attributes_count": int64(0),
		},
	}
	maps.Copy(want, point)
	return want
}

// wantGenAIMetricRows lists the decoded otel_metrics rows that
// newGenAIMetricsBatch produces, in the order they are written: positions 0 and
// 2 to 8 of the batch, since position 1 is quarantined. The columns that vary
// from run to run (receive_time, receive_time_unix_nano and point_fingerprint)
// are left to the checks that cover them.
func wantGenAIMetricRows() []map[string]any {
	const (
		providerAttrs  = `{"gen_ai.provider.name":"gcp.vertex_ai"}`
		providerSchema = `{"gen_ai.provider.name":"string"}`
		sessionFilter  = `{"session.id":"sess-42"}`
		sessionSchema  = `{"session.id":"string"}`
		noFilter       = `{}`
	)
	exemplar := func(offset time.Duration, valueColumn string, value any, filtered, filteredSchema string) map[string]any {
		return map[string]any{
			"time":                       metricsE2EMicros(offset),
			"time_unix_nano":             metricsE2ENanos(offset),
			valueColumn:                  value,
			"trace_id":                   metricsE2ETraceHex,
			"span_id":                    metricsE2ESpanHex,
			"filtered_attributes":        filtered,
			"filtered_attributes_schema": filteredSchema,
		}
	}

	return []map[string]any{
		// Position 0: gauge with an int value and an exemplar.
		wantMetricRow(map[string]any{
			"timestamp":          metricsE2EMicros(0),
			"time_unix_nano":     metricsE2ENanos(0),
			"metric_name":        "gen_ai.client.queue.depth",
			"metric_type":        transform.MetricTypeGauge,
			"metric_unit":        "{request}",
			"metric_description": "Requests waiting for a model slot",
			"value_type":         transform.ScalarValueTypeInt64,
			"value_int64":        int64(7),
			"attributes":         providerAttrs,
			"attributes_schema":  providerSchema,
			"exemplars": []map[string]any{
				exemplar(0, "value_int64", int64(7), `{"request.id":"req-1"}`, `{"request.id":"string"}`),
			},
		}),
		// Position 2: gauge with a double value.
		wantMetricRow(map[string]any{
			"timestamp":          metricsE2EMicros(time.Second),
			"time_unix_nano":     metricsE2ENanos(time.Second),
			"metric_name":        "gen_ai.client.queue.depth",
			"metric_type":        transform.MetricTypeGauge,
			"metric_unit":        "{request}",
			"metric_description": "Requests waiting for a model slot",
			"value_type":         transform.ScalarValueTypeDouble,
			"value_double":       3.5,
			"attributes":         providerAttrs,
			"attributes_schema":  providerSchema,
		}),
		// Position 3: cumulative, monotonic sum.
		wantMetricRow(map[string]any{
			"timestamp":               metricsE2EMicros(time.Second),
			"time_unix_nano":          metricsE2ENanos(time.Second),
			"start_time":              metricsE2EMicros(-time.Hour),
			"start_time_unix_nano":    metricsE2ENanos(-time.Hour),
			"metric_name":             "gen_ai.client.request.count",
			"metric_type":             transform.MetricTypeSum,
			"metric_unit":             "{request}",
			"metric_description":      "GenAI requests issued",
			"aggregation_temporality": transform.TemporalityCumulative,
			"is_monotonic":            true,
			"value_type":              transform.ScalarValueTypeInt64,
			"value_int64":             int64(42),
			"attributes":              `{"gen_ai.operation.name":"chat"}`,
			"attributes_schema":       `{"gen_ai.operation.name":"string"}`,
			"exemplars": []map[string]any{
				exemplar(900*time.Millisecond, "value_double", 1.0, noFilter, noFilter),
			},
		}),
		// Position 4: input token histogram.
		wantMetricRow(map[string]any{
			"timestamp":               metricsE2EMicros(2 * time.Second),
			"time_unix_nano":          metricsE2ENanos(2 * time.Second),
			"start_time":              metricsE2EMicros(time.Second),
			"start_time_unix_nano":    metricsE2ENanos(time.Second),
			"metric_name":             "gen_ai.client.token.usage",
			"metric_type":             transform.MetricTypeHistogram,
			"metric_unit":             "{token}",
			"metric_description":      "Number of input and output tokens used",
			"aggregation_temporality": transform.TemporalityDelta,
			"count":                   int64(3),
			"sum":                     1536.0,
			"min":                     128.0,
			"max":                     900.0,
			"explicit_bounds":         decodedFloats(1, 4, 16, 64, 256, 1024),
			"bucket_counts":           decodedInts(0, 0, 0, 0, 1, 2, 0),
			"attributes":              `{"gen_ai.operation.name":"chat","gen_ai.provider.name":"gcp.vertex_ai","gen_ai.request.model":"gemini-2.5-pro","gen_ai.token.type":"input"}`,
			"attributes_schema":       `{"gen_ai.operation.name":"string","gen_ai.provider.name":"string","gen_ai.request.model":"string","gen_ai.token.type":"string"}`,
			"exemplars": []map[string]any{
				exemplar(1500*time.Millisecond, "value_double", 508.0, sessionFilter, sessionSchema),
			},
		}),
		// Position 5: output token histogram.
		wantMetricRow(map[string]any{
			"timestamp":               metricsE2EMicros(2 * time.Second),
			"time_unix_nano":          metricsE2ENanos(2 * time.Second),
			"start_time":              metricsE2EMicros(time.Second),
			"start_time_unix_nano":    metricsE2ENanos(time.Second),
			"metric_name":             "gen_ai.client.token.usage",
			"metric_type":             transform.MetricTypeHistogram,
			"metric_unit":             "{token}",
			"metric_description":      "Number of input and output tokens used",
			"aggregation_temporality": transform.TemporalityDelta,
			"count":                   int64(3),
			"sum":                     384.0,
			"min":                     32.0,
			"max":                     200.0,
			"explicit_bounds":         decodedFloats(1, 4, 16, 64, 256, 1024),
			"bucket_counts":           decodedInts(0, 0, 0, 1, 2, 0, 0),
			"attributes":              `{"gen_ai.operation.name":"chat","gen_ai.provider.name":"gcp.vertex_ai","gen_ai.request.model":"gemini-2.5-pro","gen_ai.token.type":"output"}`,
			"attributes_schema":       `{"gen_ai.operation.name":"string","gen_ai.provider.name":"string","gen_ai.request.model":"string","gen_ai.token.type":"string"}`,
			"exemplars": []map[string]any{
				exemplar(1600*time.Millisecond, "value_double", 152.0, sessionFilter, sessionSchema),
			},
		}),
		// Position 6: operation duration histogram with an error type.
		wantMetricRow(map[string]any{
			"timestamp":               metricsE2EMicros(3 * time.Second),
			"time_unix_nano":          metricsE2ENanos(3 * time.Second),
			"start_time":              metricsE2EMicros(2 * time.Second),
			"start_time_unix_nano":    metricsE2ENanos(2 * time.Second),
			"metric_name":             "gen_ai.client.operation.duration",
			"metric_type":             transform.MetricTypeHistogram,
			"metric_unit":             "s",
			"metric_description":      "GenAI operation duration",
			"aggregation_temporality": transform.TemporalityDelta,
			"count":                   int64(2),
			"sum":                     3.5,
			"min":                     1.0,
			"max":                     2.5,
			"explicit_bounds":         decodedFloats(0.25, 0.5, 1, 2, 4),
			"bucket_counts":           decodedInts(0, 0, 1, 0, 1, 0),
			"attributes":              `{"error.type":"timeout","gen_ai.operation.name":"chat","gen_ai.provider.name":"gcp.vertex_ai","gen_ai.request.model":"gemini-2.5-pro"}`,
			"attributes_schema":       `{"error.type":"string","gen_ai.operation.name":"string","gen_ai.provider.name":"string","gen_ai.request.model":"string"}`,
			"exemplars": []map[string]any{
				exemplar(2900*time.Millisecond, "value_double", 2.5, noFilter, noFilter),
			},
		}),
		// Position 7: exponential histogram.
		wantMetricRow(map[string]any{
			"timestamp":               metricsE2EMicros(3 * time.Second),
			"time_unix_nano":          metricsE2ENanos(3 * time.Second),
			"start_time":              metricsE2EMicros(2 * time.Second),
			"start_time_unix_nano":    metricsE2ENanos(2 * time.Second),
			"metric_name":             "gen_ai.client.latency.drift",
			"metric_type":             transform.MetricTypeExponentialHistogram,
			"metric_unit":             "s",
			"metric_description":      "Signed drift of the observed latency from its baseline",
			"aggregation_temporality": transform.TemporalityDelta,
			"count":                   int64(6),
			"sum":                     1.25,
			"min":                     -1.5,
			"max":                     2.0,
			"exp_scale":               int64(2),
			"exp_zero_count":          int64(1),
			"exp_zero_threshold":      0.001,
			"exp_positive": map[string]any{
				"offset":        int64(-2),
				"bucket_counts": decodedInts(1, 2, 1),
			},
			"exp_negative": map[string]any{
				"offset":        int64(1),
				"bucket_counts": decodedInts(1),
			},
			"attributes":        `{"gen_ai.operation.name":"chat","gen_ai.request.model":"gemini-2.5-pro"}`,
			"attributes_schema": `{"gen_ai.operation.name":"string","gen_ai.request.model":"string"}`,
			"exemplars": []map[string]any{
				exemplar(2500*time.Millisecond, "value_int64", int64(2), noFilter, noFilter),
			},
		}),
		// Position 8: summary.
		wantMetricRow(map[string]any{
			"timestamp":            metricsE2EMicros(4 * time.Second),
			"time_unix_nano":       metricsE2ENanos(4 * time.Second),
			"start_time":           metricsE2EMicros(0),
			"start_time_unix_nano": metricsE2ENanos(0),
			"metric_name":          "gen_ai.client.latency.legacy",
			"metric_type":          transform.MetricTypeSummary,
			"metric_unit":          "s",
			"metric_description":   "Latency quantiles reported by a legacy summary instrument",
			"count":                int64(10),
			"sum":                  12.5,
			"summary_quantiles": []map[string]any{
				{"quantile": 0.5, "value": 1.0},
				{"quantile": 0.99, "value": 2.25},
				{"quantile": 1.0, "value": 3.0},
			},
			"attributes":        providerAttrs,
			"attributes_schema": providerSchema,
		}),
	}
}

// volatileMetricColumns vary from run to run, so a row comparison leaves them
// out: receive_time and its nanosecond twin are the clock, and point_fingerprint
// is checked for determinism rather than for a value.
var volatileMetricColumns = []string{"receive_time", "receive_time_unix_nano", "point_fingerprint"}

// assertMetricRow compares a decoded otel_metrics row with the columns it
// must have, in both directions: a missing, wrong, or unexpected column is an
// error. The clock columns are only required to be set.
func assertMetricRow(t *testing.T, label string, got, want map[string]any) {
	t.Helper()
	for _, col := range []string{"receive_time", "receive_time_unix_nano"} {
		if v, ok := got[col].(int64); !ok || v <= 0 {
			t.Errorf("%s: %s = %#v, want a positive clock reading", label, col, got[col])
		}
	}
	got = maps.Clone(got)
	for _, col := range volatileMetricColumns {
		delete(got, col)
	}
	for _, col := range slices.Sorted(maps.Keys(want)) {
		if !reflect.DeepEqual(got[col], want[col]) {
			t.Errorf("%s: %s = %#v, want %#v", label, col, got[col], want[col])
		}
	}
	for _, col := range slices.Sorted(maps.Keys(got)) {
		if _, ok := want[col]; !ok {
			t.Errorf("%s: unexpected column %s = %#v", label, col, got[col])
		}
	}
}

// metricFingerprints returns the point_fingerprint of each row, failing the
// test when one is missing or lacks the fingerprint prefix.
func metricFingerprints(t *testing.T, rows []map[string]any) []string {
	t.Helper()
	out := make([]string, len(rows))
	for i, row := range rows {
		fp, _ := row["point_fingerprint"].(string)
		if !strings.HasPrefix(fp, schema.FingerprintPrefix) {
			t.Fatalf("row %d: point_fingerprint = %q, want the %q prefix", i, fp, schema.FingerprintPrefix)
		}
		out[i] = fp
	}
	return out
}

func allDistinct(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, v := range values {
		seen[v] = struct{}{}
	}
	return len(seen) == len(values)
}

// provisioningConfig returns a config under which every signal exporter
// creates the dataset, its tables, and its views, and quarantines bad data.
func provisioningConfig() *Config {
	cfg := validConfig()
	cfg.Project = "my-project"
	cfg.Dataset = "otel"
	cfg.Location = "US"
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	cfg.AutoCreate.CompanionViews = true
	cfg.Quarantine.Enabled = true
	return cfg
}

func e2eSettings() component.TelemetrySettings {
	res := pcommon.NewResource()
	res.Attributes().PutStr("service.instance.id", "collector-pod-1")
	return component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
		Resource:      res,
	}
}

// e2eExporter is the lifecycle that the three signal exporters share.
type e2eExporter interface {
	start(context.Context, component.Host) error
	shutdown(context.Context) error
}

// startE2EExporter starts exp and returns a function that shuts it down. The
// shutdown runs once, whether the test calls the function or the test ends.
func startE2EExporter(t *testing.T, exp e2eExporter) (stop func()) {
	t.Helper()
	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	var once sync.Once
	stop = func() { once.Do(func() { _ = exp.shutdown(context.Background()) }) }
	t.Cleanup(stop)
	return stop
}

func startMetricsExporter(t *testing.T, cfg *Config) (*metricsExporter, func()) {
	t.Helper()
	exp, err := newMetricsExporter(e2eSettings(), cfg)
	if err != nil {
		t.Fatalf("newMetricsExporter: %v", err)
	}
	return exp, startE2EExporter(t, exp)
}

// startAllSignals starts a traces, a logs, and a metrics exporter over cfg, in
// that order, as one collector does, and returns a function that shuts them
// down.
func startAllSignals(t *testing.T, cfg *Config) (stop func()) {
	t.Helper()
	settings := e2eSettings()
	traces, err := newTracesExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	logs, err := newLogsExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newLogsExporter: %v", err)
	}
	metrics, err := newMetricsExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newMetricsExporter: %v", err)
	}
	stops := []func(){
		startE2EExporter(t, traces),
		startE2EExporter(t, logs),
		startE2EExporter(t, metrics),
	}
	return func() {
		for i := len(stops) - 1; i >= 0; i-- {
			stops[i]()
		}
	}
}

// assertMetricsProvisioned checks what a metrics exporter provisions under
// provisioningConfig: the metrics table with its layout, the dedup view, the
// v_gen_ai_metrics companion view, and the quarantine table, and nothing that
// belongs to the other signals.
func assertMetricsProvisioned(t *testing.T, srv *bqfake.Server, cfg *Config) {
	t.Helper()
	project, dataset := cfg.Project, cfg.Dataset
	if _, ok := srv.Dataset(project, dataset); !ok {
		t.Fatalf("dataset %s.%s was not created", project, dataset)
	}

	for id, wantType := range map[string]string{
		cfg.Metrics.Table:            "TABLE",
		cfg.Metrics.Table + "_dedup": "VIEW",
		schema.GenAIMetricsViewName:  "VIEW",
		cfg.Quarantine.Table:         "TABLE",
	} {
		tbl, ok := srv.Table(project, dataset, id)
		if !ok {
			t.Errorf("%s was not created", id)
			continue
		}
		if tbl.Type != wantType {
			t.Errorf("%s type = %q, want %q", id, tbl.Type, wantType)
		}
	}

	for _, id := range []string{
		cfg.Traces.Table, cfg.Traces.Table + "_dedup",
		cfg.Logs.Table, cfg.Logs.Table + "_dedup",
		schema.SpansAsAllSpansViewName, schema.GenAISpansViewName,
		schema.GenAIModelCostsViewName, schema.GenAISessionsViewName,
		schema.GenAIToolUsageViewName, schema.GenAILogsViewName,
		schema.GenAIMetricExemplarsViewName,
	} {
		if _, ok := srv.Table(project, dataset, id); ok {
			t.Errorf("the metrics exporter created %s, which belongs to another signal", id)
		}
	}

	for id, wantQuery := range map[string]string{
		cfg.Metrics.Table + "_dedup": fmt.Sprintf(schema.MetricsDedupViewQuery, project, dataset, cfg.Metrics.Table),
		schema.GenAIMetricsViewName:  fmt.Sprintf(schema.GenAIMetricsViewQuery, project, dataset, cfg.Metrics.Table),
	} {
		tbl, ok := srv.Table(project, dataset, id)
		if !ok || tbl.View == nil {
			continue // already reported above
		}
		if tbl.View.Query != wantQuery {
			t.Errorf("%s query =\n%s\nwant\n%s", id, tbl.View.Query, wantQuery)
		}
		if tbl.View.UseLegacySql {
			t.Errorf("%s uses legacy SQL, want standard SQL", id)
		}
	}

	if tbl, ok := srv.Table(project, dataset, cfg.Metrics.Table); ok {
		if tp := tbl.TimePartitioning; tp == nil || tp.Type != "DAY" || tp.Field != "timestamp" {
			t.Errorf("%s partitioning = %+v, want DAY on timestamp", cfg.Metrics.Table, tp)
		}
		if c := tbl.Clustering; c == nil || !slices.Equal(c.Fields, []string{"service_name", "metric_name", "metric_type"}) {
			t.Errorf("%s clustering = %+v, want service_name, metric_name, metric_type", cfg.Metrics.Table, c)
		}
	}
}

func TestVerticalSliceEndToEndMetrics(t *testing.T) {
	srv := bqfake.NewServer(t)
	wireFakeHooks(t, srv)
	cfg := provisioningConfig()
	ctx := context.Background()

	exp, stop := startMetricsExporter(t, cfg)
	assertMetricsProvisioned(t, srv, cfg)

	// Every OTLP metric type, with its exemplars, arrives as one decoded row
	// per data point.
	if err := exp.pushMetricsData(ctx, newGenAIMetricsBatch()); err != nil {
		t.Fatalf("pushMetricsData: %v", err)
	}
	wantRows := wantGenAIMetricRows()
	rows := srv.Rows("otel_metrics")
	if len(rows) != len(wantRows) {
		t.Fatalf("otel_metrics rows = %d, want %d", len(rows), len(wantRows))
	}
	for i, want := range wantRows {
		assertMetricRow(t, fmt.Sprintf("row %d (%v)", i, want["metric_name"]), rows[i], want)
	}

	// The data point without a timestamp is quarantined with its payload, and
	// the points around it are delivered.
	qRows := srv.Rows("otel_quarantine")
	if len(qRows) != 1 {
		t.Fatalf("otel_quarantine rows = %d, want 1", len(qRows))
	}
	q := qRows[0]
	if q["signal"] != schema.SignalMetrics || q["error_code"] != "MISSING_TIMESTAMP" {
		t.Errorf("quarantine row signal = %v, error_code = %v, want %s / MISSING_TIMESTAMP",
			q["signal"], q["error_code"], schema.SignalMetrics)
	}
	if q["service_name"] != "vertex-agent-svc" || q["exporter_instance"] != "collector-pod-1" {
		t.Errorf("quarantine row service_name = %v, exporter_instance = %v, want vertex-agent-svc / collector-pod-1",
			q["service_name"], q["exporter_instance"])
	}
	if raw, _ := q["raw_payload"].(string); !strings.Contains(raw, "gen_ai.client.queue.depth") {
		t.Errorf("quarantine raw_payload = %q, want the metric of the rejected data point", raw)
	}

	// Fingerprints separate the points of a batch, and they are a function of
	// the data point alone: pushing an equal batch again, or from a new
	// exporter as after a collector restart, reproduces every one.
	first := metricFingerprints(t, rows)
	if !allDistinct(first) {
		t.Errorf("fingerprints of one batch = %v, want all distinct", first)
	}
	if err := exp.pushMetricsData(ctx, newGenAIMetricsBatch()); err != nil {
		t.Fatalf("second pushMetricsData: %v", err)
	}
	rows = srv.Rows("otel_metrics")
	if len(rows) != 2*len(wantRows) {
		t.Fatalf("otel_metrics rows after a second push = %d, want %d", len(rows), 2*len(wantRows))
	}
	if got := metricFingerprints(t, rows[len(wantRows):]); !slices.Equal(got, first) {
		t.Errorf("fingerprints of a repeated push = %v, want %v", got, first)
	}

	stop()
	exp, _ = startMetricsExporter(t, cfg)
	if err := exp.pushMetricsData(ctx, newGenAIMetricsBatch()); err != nil {
		t.Fatalf("pushMetricsData after a restart: %v", err)
	}
	rows = srv.Rows("otel_metrics")
	if len(rows) != 3*len(wantRows) {
		t.Fatalf("otel_metrics rows after a restart = %d, want %d", len(rows), 3*len(wantRows))
	}
	if got := metricFingerprints(t, rows[2*len(wantRows):]); !slices.Equal(got, first) {
		t.Errorf("fingerprints after a restart = %v, want %v", got, first)
	}

	// Identical data points in one batch are told apart by their position in
	// it, and a repeat of the batch reproduces the whole set.
	srv.ResetRows()
	if err := exp.pushMetricsData(ctx, newIdenticalPointsBatch()); err != nil {
		t.Fatalf("pushMetricsData of identical points: %v", err)
	}
	identical := metricFingerprints(t, srv.Rows("otel_metrics"))
	if len(identical) != 3 || !allDistinct(identical) {
		t.Fatalf("fingerprints of two identical points and a third = %v, want 3 distinct", identical)
	}
	if err := exp.pushMetricsData(ctx, newIdenticalPointsBatch()); err != nil {
		t.Fatalf("second pushMetricsData of identical points: %v", err)
	}
	if got := metricFingerprints(t, srv.Rows("otel_metrics")[3:]); !slices.Equal(got, identical) {
		t.Errorf("fingerprints of repeated identical points = %v, want %v", got, identical)
	}
}

// exporterOwnedViews returns the definition of every view that the three
// signal exporters keep, by view ID, as this release defines them.
func exporterOwnedViews(cfg *Config) map[string]string {
	p, d := cfg.Project, cfg.Dataset
	views := map[string]string{
		cfg.Traces.Table + "_dedup":  fmt.Sprintf(schema.SpansDedupViewQuery, p, d, cfg.Traces.Table),
		cfg.Logs.Table + "_dedup":    fmt.Sprintf(schema.LogsDedupViewQuery, p, d, cfg.Logs.Table),
		cfg.Metrics.Table + "_dedup": fmt.Sprintf(schema.MetricsDedupViewQuery, p, d, cfg.Metrics.Table),
	}
	for _, isSpans := range []bool{true, false} {
		for id, md := range schema.CompanionViewsMetadata(p, d, cfg.Traces.Table, cfg.Logs.Table, isSpans) {
			views[id] = md.ViewQuery
		}
	}
	for id, md := range schema.MetricsCompanionViewsMetadata(p, d, cfg.Metrics.Table) {
		views[id] = md.ViewQuery
	}
	views[schema.GenAIMetricExemplarsViewName] = schema.MetricExemplarsCompanionViewMetadata(p, d, cfg.Traces.Table).ViewQuery
	return views
}

func TestVerticalSliceViewsReconcileOnRestart(t *testing.T) {
	srv := bqfake.NewServer(t)
	wireFakeHooks(t, srv)
	cfg := provisioningConfig()
	ctx := context.Background()
	project, dataset := cfg.Project, cfg.Dataset

	wantViews := exporterOwnedViews(cfg)
	tableIDs := []string{cfg.Traces.Table, cfg.Logs.Table, cfg.Metrics.Table, cfg.Quarantine.Table}

	type objectState struct{ query, etag string }
	snapshot := func() map[string]objectState {
		t.Helper()
		out := make(map[string]objectState)
		for _, id := range append(slices.Collect(maps.Keys(wantViews)), tableIDs...) {
			tbl, ok := srv.Table(project, dataset, id)
			if !ok {
				t.Fatalf("%s.%s.%s does not exist", project, dataset, id)
			}
			state := objectState{etag: tbl.Etag}
			if tbl.View != nil {
				state.query = tbl.View.Query
			}
			out[id] = state
		}
		return out
	}

	// The first start creates every table and view.
	stop := startAllSignals(t, cfg)
	stop()
	created := snapshot()
	for id, want := range wantViews {
		if created[id].query != want {
			t.Errorf("view %s was created as\n%s\nwant\n%s", id, created[id].query, want)
		}
	}

	// A view left behind by an older release holds the older definition.
	admin, err := newDestinationAdmin(ctx, project, dataset)
	if err != nil {
		t.Fatalf("newDestinationAdmin: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	const stale = "SELECT 1 AS stale"
	for id := range wantViews {
		if err := admin.UpdateTable(ctx, id, bq.TableMetadataToUpdate{ViewQuery: stale}, ""); err != nil {
			t.Fatalf("making view %s stale: %v", id, err)
		}
	}
	staled := snapshot()
	for id := range wantViews {
		if staled[id].query != stale {
			t.Fatalf("view %s query = %q after the update, want %q", id, staled[id].query, stale)
		}
	}

	// Restarting replaces every stale definition with this release's, writes
	// through the ETag it read, and leaves the tables alone.
	stop = startAllSignals(t, cfg)
	stop()
	reconciled := snapshot()
	for id, want := range wantViews {
		if reconciled[id].query != want {
			t.Errorf("view %s after a restart =\n%s\nwant\n%s", id, reconciled[id].query, want)
		}
		if reconciled[id].etag == staled[id].etag {
			t.Errorf("view %s kept ETag %s, want it rewritten", id, staled[id].etag)
		}
	}
	for _, id := range tableIDs {
		if reconciled[id] != created[id] {
			t.Errorf("table %s changed on restart: %+v, want %+v", id, reconciled[id], created[id])
		}
	}

	// A start that finds every view current writes nothing.
	stop = startAllSignals(t, cfg)
	stop()
	for id, state := range snapshot() {
		if state != reconciled[id] {
			t.Errorf("%s was written although it was current: %+v, want %+v", id, state, reconciled[id])
		}
	}
}
