// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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

func TestPushTracesMixedOutcomeRetriesOnlyNonRejectedRows(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	e, _ := newTestTracesExporter(t, bqi.AppendOutcome{
		AcknowledgedRows: 1,
		RowErrors:        []bqi.RowError{{Index: 1, Code: "FIELDS_ERROR", Message: "bad field body"}},
		Verdict:          bqi.Classify(status.Error(codes.Unavailable, "partial transport failure")),
		Err:              status.Error(codes.Unavailable, "partial transport failure"),
	})
	tel, err := metadata.NewTelemetry(provider)
	if err != nil {
		t.Fatal(err)
	}
	e.tel = tel

	err = e.pushTraceData(context.Background(), newTestTraces(3))
	var traceErr consumererror.Traces
	if !errors.As(err, &traceErr) {
		t.Fatalf("expected retryable trace subset, got %T: %v", err, err)
	}
	spans := traceErr.Data().ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	if spans.Len() != 2 || spans.At(0).Name() != "a" || spans.At(1).Name() != "c" {
		t.Fatalf("retry subset = %v/%d, want only non-rejected spans a and c", spanNames(spans), spans.Len())
	}
	if got := metricInt64Sum(t, reader, "otelcol_exporter_bigquery_acknowledged_rows"); got != 1 {
		t.Fatalf("acknowledged metric = %d, want confirmed count 1", got)
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
	originalAdmin := newDestinationAdmin
	t.Cleanup(func() {
		newManagedWriterClient = original
		newDestinationAdmin = originalAdmin
	})
	newDestinationAdmin = func(
		context.Context, string, string, ...option.ClientOption,
	) (destinationAdmin, error) {
		return &fakeDestinationAdmin{
			datasetMetadata: &bq.DatasetMetadata{},
			tableMetadata: &bq.TableMetadata{
				Schema: mustBigQuerySchema(t, schema.SpansTableSchema()),
			},
		}, nil
	}

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

func TestSignalExporterStartBoundsBlockingMetadata(t *testing.T) {
	originalAdmin := newDestinationAdmin
	originalWriter := newManagedWriterClient
	t.Cleanup(func() {
		newDestinationAdmin = originalAdmin
		newManagedWriterClient = originalWriter
	})
	admin := &fakeDestinationAdmin{blockDatasetRead: true}
	newDestinationAdmin = func(
		context.Context, string, string, ...option.ClientOption,
	) (destinationAdmin, error) {
		return admin, nil
	}
	newManagedWriterClient = func(
		context.Context, string, ...option.ClientOption,
	) (*managedwriter.Client, error) {
		t.Fatal("writer construction must not run after metadata startup times out")
		return nil, nil
	}

	cfg := validConfig()
	cfg.TimeoutSettings.Timeout = 20 * time.Millisecond
	e := newSignalExporter(component.TelemetrySettings{Logger: zap.NewNop()}, cfg, cfg.Traces.Table, schema.SpansTableSchema())
	started := time.Now()
	err := e.start(context.Background(), nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("start error = %v, want bounded metadata deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocking metadata startup returned after %v, want bounded return", elapsed)
	}
}

func TestSignalExporterStartSkipsMetadataForNoAuthLoopback(t *testing.T) {
	originalAdmin := newDestinationAdmin
	t.Cleanup(func() { newDestinationAdmin = originalAdmin })
	newDestinationAdmin = func(
		context.Context, string, string, ...option.ClientOption,
	) (destinationAdmin, error) {
		t.Fatal("no-auth loopback mode must not construct a metadata client")
		return nil, nil
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing-adc.json"))

	cfg := validConfig()
	cfg.Endpoint.URL = "localhost:19050"
	cfg.Endpoint.Insecure = true
	cfg.Endpoint.WithoutAuthentication = true
	e := newSignalExporter(component.TelemetrySettings{Logger: zap.NewNop()}, cfg, cfg.Traces.Table, schema.SpansTableSchema())
	if err := e.start(context.Background(), nil); err != nil {
		t.Fatalf("no-auth loopback startup must not require ADC or metadata: %v", err)
	}
	if err := e.shutdown(context.Background()); err != nil {
		t.Fatalf("no-auth loopback shutdown failed: %v", err)
	}
}

func TestSignalExporterStartLogsAdminCloseFailureAfterSuccess(t *testing.T) {
	originalAdmin := newDestinationAdmin
	originalWriter := newManagedWriterClient
	t.Cleanup(func() {
		newDestinationAdmin = originalAdmin
		newManagedWriterClient = originalWriter
	})
	closeErr := errors.New("metadata close failed")
	admin := &fakeDestinationAdmin{
		datasetMetadata: &bq.DatasetMetadata{},
		tableMetadata: &bq.TableMetadata{
			Schema: mustBigQuerySchema(t, schema.SpansTableSchema()),
		},
		closeErr: closeErr,
	}
	newDestinationAdmin = func(
		context.Context, string, string, ...option.ClientOption,
	) (destinationAdmin, error) {
		return admin, nil
	}
	newManagedWriterClient = func(
		context.Context, string, ...option.ClientOption,
	) (*managedwriter.Client, error) {
		return &managedwriter.Client{}, nil
	}
	core, logs := observer.New(zap.WarnLevel)
	cfg := validConfig()
	e := newSignalExporter(component.TelemetrySettings{Logger: zap.New(core)}, cfg, cfg.Traces.Table, schema.SpansTableSchema())

	if err := e.start(context.Background(), nil); err != nil {
		t.Fatalf("successful start must not fail because metadata client close failed: %v", err)
	}
	if logs.FilterMessage("closing BigQuery metadata client failed").Len() != 1 {
		t.Fatalf("close warning count = %d, want 1", logs.Len())
	}
	// The zero-value managedwriter client is a constructor test double and
	// cannot be closed. Release the remaining owned resources directly.
	e.client = nil
	if err := e.shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSignalExporterStartJoinsAdminCloseWithExistingFailure(t *testing.T) {
	originalAdmin := newDestinationAdmin
	originalWriter := newManagedWriterClient
	t.Cleanup(func() {
		newDestinationAdmin = originalAdmin
		newManagedWriterClient = originalWriter
	})
	closeErr := errors.New("metadata close failed")
	writerErr := errors.New("writer construction failed")
	newDestinationAdmin = func(
		context.Context, string, string, ...option.ClientOption,
	) (destinationAdmin, error) {
		return &fakeDestinationAdmin{
			datasetMetadata: &bq.DatasetMetadata{},
			tableMetadata: &bq.TableMetadata{
				Schema: mustBigQuerySchema(t, schema.SpansTableSchema()),
			},
			closeErr: closeErr,
		}, nil
	}
	newManagedWriterClient = func(
		context.Context, string, ...option.ClientOption,
	) (*managedwriter.Client, error) {
		return nil, writerErr
	}

	cfg := validConfig()
	e := newSignalExporter(component.TelemetrySettings{Logger: zap.NewNop()}, cfg, cfg.Traces.Table, schema.SpansTableSchema())
	err := e.start(context.Background(), nil)
	if !errors.Is(err, writerErr) || !errors.Is(err, closeErr) {
		t.Fatalf("start error = %v, want both writer and metadata-close failures", err)
	}
}

func spanNames(spans ptrace.SpanSlice) []string {
	names := make([]string, 0, spans.Len())
	for i := 0; i < spans.Len(); i++ {
		names = append(names, spans.At(i).Name())
	}
	return names
}

func metricInt64Sum(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s data = %T, want int64 sum", name, m.Data)
			}
			var total int64
			for _, point := range sum.DataPoints {
				total += point.Value
			}
			return total
		}
	}
	t.Fatalf("metric %s was not collected", name)
	return 0
}
