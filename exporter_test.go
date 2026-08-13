// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bqi "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/metadata"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/protoenc"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

type fakeAppender struct {
	outcome  bqi.AppendOutcome
	gotRows  int
	appended int
	closed   int
}

func (f *fakeAppender) Append(_ context.Context, rows [][]byte) bqi.AppendOutcome {
	f.gotRows = len(rows)
	f.appended++
	return f.outcome
}

func (f *fakeAppender) Close() error { f.closed++; return nil }

func newTestTraces(spans int) ptrace.Traces {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for i := 0; i < spans; i++ {
		s := ss.Spans().AppendEmpty()
		s.SetName(string(rune('a' + i)))
		s.SetTraceID(pcommon.TraceID{byte(i + 1)})
		s.SetSpanID(pcommon.SpanID{byte(i + 1)})
		s.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		s.SetEndTimestamp(pcommon.Timestamp(1_700_000_001_000_000_000))
	}
	return td
}

func newTestTracesExporter(t *testing.T, out bqi.AppendOutcome) (*tracesExporter, *fakeAppender) {
	t.Helper()
	cfg := validConfig()
	tel, err := metadata.NewTelemetry(noop.NewMeterProvider())
	if err != nil {
		t.Fatal(err)
	}
	enc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	app := &fakeAppender{outcome: out}

	se := &signalExporter{
		cfg:    cfg,
		table:  cfg.Traces.Table,
		logger: zap.NewNop(),
		tel:    tel,
		writer: app,
		enc:    enc,
		schema: schema.SpansTableSchema(),
		opts:   transform.Options{Limits: cfg.Limits.toTransform()},
	}
	return &tracesExporter{se}, app
}

func TestPushTracesSuccess(t *testing.T) {
	e, app := newTestTracesExporter(t, bqi.AppendOutcome{AcknowledgedRows: 3})

	if err := e.pushTraceData(context.Background(), newTestTraces(3)); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if app.gotRows != 3 {
		t.Fatalf("appended %d rows, want 3", app.gotRows)
	}
}

// FR6: when the service reports row errors, no rows in that request were
// appended. The invalid rows are dropped, and the valid ones that travelled
// with them must come back for re-sending.
func TestPushTracesReturnsValidSubsetOnRowErrors(t *testing.T) {
	e, _ := newTestTracesExporter(t, bqi.AppendOutcome{
		RowErrors: []bqi.RowError{{Index: 1, Code: "FIELDS_ERROR", Message: "bad field body"}},
	})

	err := e.pushTraceData(context.Background(), newTestTraces(3))
	if err == nil {
		t.Fatal("expected an error carrying the retryable subset")
	}

	var traceErr consumererror.Traces
	if !errors.As(err, &traceErr) {
		t.Fatalf("expected a consumererror.Traces, got %T: %v", err, err)
	}

	got := traceErr.Data()
	if got.SpanCount() != 2 {
		t.Fatalf("returned %d spans for retry, want the 2 valid ones", got.SpanCount())
	}

	// The rejected span must not be among them: it is permanently invalid, and
	// routing it back would have exporterhelper replay it forever.
	spans := got.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	for i := 0; i < spans.Len(); i++ {
		if spans.At(i).Name() == "b" {
			t.Fatal("the permanently rejected span was routed back for retry")
		}
	}
}

// A permanent transport failure must be marked permanent so exporterhelper
// drops it rather than retrying to the horizon.
func TestPushTracesPermanentError(t *testing.T) {
	e, _ := newTestTracesExporter(t, bqi.AppendOutcome{
		Verdict: bqi.Classify(status.Error(codes.PermissionDenied, "denied")),
		Err:     status.Error(codes.PermissionDenied, "denied"),
	})

	err := e.pushTraceData(context.Background(), newTestTraces(2))
	if err == nil {
		t.Fatal("expected an error")
	}
	if !consumererror.IsPermanent(err) {
		t.Fatalf("permission denied must be permanent, got %v", err)
	}
}

// A retryable transport failure returns the whole batch: nothing in it was
// acknowledged.
func TestPushTracesRetryableReturnsWholeBatch(t *testing.T) {
	e, _ := newTestTracesExporter(t, bqi.AppendOutcome{
		Verdict: bqi.Classify(status.Error(codes.Unavailable, "down")),
		Err:     status.Error(codes.Unavailable, "down"),
	})

	err := e.pushTraceData(context.Background(), newTestTraces(3))
	if consumererror.IsPermanent(err) {
		t.Fatal("a transient failure must not be permanent")
	}
	var traceErr consumererror.Traces
	if !errors.As(err, &traceErr) {
		t.Fatalf("expected a consumererror.Traces, got %T", err)
	}
	if traceErr.Data().SpanCount() != 3 {
		t.Fatalf("returned %d spans, want the whole batch of 3", traceErr.Data().SpanCount())
	}
}

// Records dropped for breaching a structural limit are never handed back:
// they are permanently invalid, and the surviving records must still be sent.
func TestPushTracesDropsStructurallyInvalidRecords(t *testing.T) {
	e, app := newTestTracesExporter(t, bqi.AppendOutcome{AcknowledgedRows: 2})
	e.opts.Limits.MaxAttributeCount = 1

	td := newTestTraces(3)
	bad := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(1)
	for i := 0; i < 5; i++ {
		bad.Attributes().PutStr(string(rune('x'+i)), "v")
	}

	if err := e.pushTraceData(context.Background(), td); err != nil {
		t.Fatalf("surviving records should still be delivered, got %v", err)
	}
	if app.gotRows != 2 {
		t.Fatalf("appended %d rows, want the 2 that passed the limits", app.gotRows)
	}
}

// An empty batch must not reach the service at all.
func TestPushTracesEmptyBatch(t *testing.T) {
	e, app := newTestTracesExporter(t, bqi.AppendOutcome{})
	if err := e.pushTraceData(context.Background(), ptrace.NewTraces()); err != nil {
		t.Fatalf("empty batch should succeed, got %v", err)
	}
	if app.appended != 0 {
		t.Fatal("empty batch should not have produced an append")
	}
}

func TestPushLogs(t *testing.T) {
	cfg := validConfig()
	tel, err := metadata.NewTelemetry(noop.NewMeterProvider())
	if err != nil {
		t.Fatal(err)
	}
	enc, err := protoenc.New(schema.LogsTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	app := &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 2}}
	e := &logsExporter{&signalExporter{
		cfg: cfg, table: cfg.Logs.Table, logger: zap.NewNop(), tel: tel,
		writer: app, enc: enc, schema: schema.LogsTableSchema(),
		opts: transform.Options{Limits: cfg.Limits.toTransform()},
	}}

	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for _, body := range []string{"one", "two"} {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		lr.Body().SetStr(body)
	}

	if err := e.pushLogData(context.Background(), ld); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if app.gotRows != 2 {
		t.Fatalf("appended %d rows, want 2", app.gotRows)
	}
}

// Collector shutdown can be invoked again after a partial lifecycle failure.
// Resource ownership must be consumed exactly once rather than closing an
// already-closed stream a second time.
func TestSignalExporterShutdownIsIdempotent(t *testing.T) {
	app := &fakeAppender{}
	e := &signalExporter{writer: app}

	if err := e.shutdown(context.Background()); err != nil {
		t.Fatalf("first shutdown failed: %v", err)
	}
	if err := e.shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown failed: %v", err)
	}
	if app.closed != 1 {
		t.Fatalf("writer closed %d times, want exactly once", app.closed)
	}
}

// managedwriter retains its constructor context for background connection
// management. Even a partial Start failure must cancel the exporter-owned
// context instead of leaking that background lifetime.
func TestSignalExporterStartFailureCancelsOwnedLifetime(t *testing.T) {
	original := newManagedWriterClient
	t.Cleanup(func() { newManagedWriterClient = original })

	wantErr := errors.New("client construction failed")
	var retained context.Context
	newManagedWriterClient = func(
		ctx context.Context, _ string, _ ...option.ClientOption,
	) (*managedwriter.Client, error) {
		retained = ctx
		return nil, wantErr
	}

	cfg := validConfig()
	e := newSignalExporter(
		component.TelemetrySettings{Logger: zap.NewNop()},
		cfg,
		cfg.Traces.Table,
		schema.SpansTableSchema(),
	)
	if err := e.start(context.Background(), nil); !errors.Is(err, wantErr) {
		t.Fatalf("start error = %v, want %v", err, wantErr)
	}
	if retained == nil {
		t.Fatal("managedwriter constructor did not receive a context")
	}
	select {
	case <-retained.Done():
	default:
		t.Fatal("partial Start failure left the managedwriter lifetime context live")
	}
}
