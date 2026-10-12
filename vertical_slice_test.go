// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
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
