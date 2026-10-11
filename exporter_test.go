// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
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
	closeErr error
	gotRows  int
	appended int
	closed   int
}

func (f *fakeAppender) Append(_ context.Context, rows [][]byte) bqi.AppendOutcome {
	f.gotRows = len(rows)
	f.appended++
	return f.outcome
}

func (f *fakeAppender) Close() error { f.closed++; return f.closeErr }

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

func TestPushTracesSplitBatchExcludesAckedSubRequestsFromRetry(t *testing.T) {
	e, _ := newTestTracesExporter(t, bqi.AppendOutcome{
		AcknowledgedRows: 2,
		AckedRanges:      [][2]int{{0, 2}},
		Verdict:          bqi.Classify(status.Error(codes.Unavailable, "second sub-request failed")),
		Err:              status.Error(codes.Unavailable, "second sub-request failed"),
	})

	err := e.pushTraceData(context.Background(), newTestTraces(4))
	var traceErr consumererror.Traces
	if !errors.As(err, &traceErr) {
		t.Fatalf("expected retryable trace subset, got %T: %v", err, err)
	}
	spans := traceErr.Data().ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	if got := spanNames(spans); !equalStrings(got, []string{"c", "d"}) {
		t.Fatalf("retry subset = %v, want only unacknowledged spans [c d]", got)
	}
}

// A request refused for row errors appended none of its rows, so its valid
// rows still owe delivery. A permanent rejection of another request in the
// same batch must not drop them with it.
func TestPushTracesPermanentSiblingKeepsRowErrorSubset(t *testing.T) {
	e, _ := newTestTracesExporter(t, bqi.AppendOutcome{
		RowErrors: []bqi.RowError{{Index: 1, Code: "FIELDS_ERROR", Message: "bad field body"}},
		Verdict:   bqi.Classify(status.Error(codes.InvalidArgument, "request rejected")),
		Err:       status.Error(codes.InvalidArgument, "request rejected"),
	})

	err := e.pushTraceData(context.Background(), newTestTraces(3))
	if consumererror.IsPermanent(err) {
		t.Fatalf("batch with valid rows still owed was dropped as permanent: %v", err)
	}
	var traceErr consumererror.Traces
	if !errors.As(err, &traceErr) {
		t.Fatalf("expected a retryable trace subset, got %T: %v", err, err)
	}
	spans := traceErr.Data().ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	if spans.Len() != 2 || spans.At(0).Name() != "a" || spans.At(1).Name() != "c" {
		t.Fatalf("retry subset = %v, want the rows that were not rejected: a and c", spanNames(spans))
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

// Spans that cannot fill the table's required columns (an empty trace ID, a
// missing end time) are dropped and counted before the append. Like limit
// breaches they are permanent, so they are never handed back for retry, and
// the spans around them must still be delivered.
func TestPushTracesDropsSpansViolatingTheContract(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	e, app := newTestTracesExporter(t, bqi.AppendOutcome{AcknowledgedRows: 2})
	tel, err := metadata.NewTelemetry(provider)
	if err != nil {
		t.Fatal(err)
	}
	e.tel = tel

	td := newTestTraces(4)
	spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	spans.At(1).SetTraceID(pcommon.NewTraceIDEmpty())
	spans.At(2).SetEndTimestamp(0)

	if err := e.pushTraceData(context.Background(), td); err != nil {
		t.Fatalf("surviving spans should still be delivered, got %v", err)
	}
	if app.gotRows != 2 {
		t.Fatalf("appended %d rows, want the 2 spans that satisfy the contract", app.gotRows)
	}
	if got := metricInt64Sum(t, reader, "otelcol_exporter_bigquery_rejected_rows"); got != 2 {
		t.Fatalf("rejected metric = %d, want the 2 contract violations", got)
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

func newTestLogsExporter(t *testing.T, out bqi.AppendOutcome) (*logsExporter, *fakeAppender) {
	t.Helper()
	cfg := validConfig()
	tel, err := metadata.NewTelemetry(noop.NewMeterProvider())
	if err != nil {
		t.Fatal(err)
	}
	enc, err := protoenc.New(schema.LogsTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	app := &fakeAppender{outcome: out}
	e := &logsExporter{&signalExporter{
		cfg: cfg, table: cfg.Logs.Table, logger: zap.NewNop(), tel: tel,
		writer: app, enc: enc, schema: schema.LogsTableSchema(),
		opts: transform.Options{Limits: cfg.Limits.toTransform()},
	}}
	return e, app
}

func TestPushLogs(t *testing.T) {
	e, app := newTestLogsExporter(t, bqi.AppendOutcome{AcknowledgedRows: 2})

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

// Log records whose times do not fit the table's INT64 columns are dropped
// and counted before the append. Like limit breaches they are permanent, so
// they are never handed back for retry, and the records around them must
// still be delivered.
func TestPushLogsDropsRecordsViolatingTheContract(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	e, app := newTestLogsExporter(t, bqi.AppendOutcome{AcknowledgedRows: 2})
	tel, err := metadata.NewTelemetry(provider)
	if err != nil {
		t.Fatal(err)
	}
	e.tel = tel

	ld := plog.NewLogs()
	records := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for _, body := range []string{"a", "b", "c", "d"} {
		lr := records.AppendEmpty()
		lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		lr.Body().SetStr(body)
	}
	records.At(1).SetTimestamp(math.MaxInt64 + 1)
	records.At(2).SetObservedTimestamp(math.MaxUint64)

	if err := e.pushLogData(context.Background(), ld); err != nil {
		t.Fatalf("surviving records should still be delivered, got %v", err)
	}
	if app.gotRows != 2 {
		t.Fatalf("appended %d rows, want the 2 records that satisfy the contract", app.gotRows)
	}
	if got := metricInt64Sum(t, reader, "otelcol_exporter_bigquery_rejected_rows"); got != 2 {
		t.Fatalf("rejected metric = %d, want the 2 contract violations", got)
	}
}

// The log identity attributes are configured on the exporter and must reach
// the row builder, or log_name and insert_id would silently ignore them.
func TestNewSignalExporterWiresLogIdentityOptions(t *testing.T) {
	cfg := validConfig()
	cfg.Logs.LogNameAttribute = "log.file.name"
	cfg.Logs.InsertIDAttribute = "log.record.uid"

	e, err := newLogsExporter(component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.opts.LogNameAttribute != "log.file.name" || e.opts.InsertIDAttribute != "log.record.uid" {
		t.Fatalf("log_name attribute = %q, insert_id attribute = %q, want the configured ones",
			e.opts.LogNameAttribute, e.opts.InsertIDAttribute)
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

// recordingEncoder records every row it is asked to encode, then encodes it
// with next.
type recordingEncoder struct {
	next rowEncoder
	rows []transform.Row
}

func (r *recordingEncoder) Encode(row transform.Row) ([]byte, error) {
	r.rows = append(r.rows, row)
	return r.next.Encode(row)
}

// faultyEncoder fails rows whose key column holds fail, and returns a row one
// byte over maxRowBytes for rows whose key column holds oversize. It encodes
// every other row with next.
type faultyEncoder struct {
	next        rowEncoder
	key         string
	fail        string
	oversize    string
	maxRowBytes int
}

func (f faultyEncoder) Encode(row transform.Row) ([]byte, error) {
	switch v := row[f.key]; {
	case f.fail != "" && v == f.fail:
		return nil, errors.New("cannot encode secret-value")
	case f.oversize != "" && v == f.oversize:
		return make([]byte, f.maxRowBytes+1), nil
	}
	return f.next.Encode(row)
}

// withQuarantine enables quarantine on e, recording each quarantine row before
// the real quarantine encoder serializes it, and returns the recorder and the
// quarantine stream.
func withQuarantine(t *testing.T, e *signalExporter, out bqi.AppendOutcome) (*recordingEncoder, *fakeAppender) {
	t.Helper()
	enc, err := protoenc.New(schema.QuarantineTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingEncoder{next: enc}
	stream := &fakeAppender{outcome: out}
	e.cfg.Quarantine.Enabled = true
	e.quarantineEnc = rec
	e.quarantine = stream
	return rec, stream
}

// column returns one string column of rows, with "" where it is unset.
func column(rows []transform.Row, name string) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		s, _ := r[name].(string)
		out = append(out, s)
	}
	return out
}

// logsMention reports whether any log entry carries needle in its message or
// fields.
func logsMention(logs *observer.ObservedLogs, needle string) bool {
	for _, entry := range logs.All() {
		if strings.Contains(entry.Message, needle) || strings.Contains(fmt.Sprint(entry.ContextMap()), needle) {
			return true
		}
	}
	return false
}

// With quarantine enabled, every span the push drops permanently reaches the
// quarantine table once, in batch order and with its error code: a contract
// violation, a row that cannot be serialized, a row over the size limit, and
// a row the service rejected. The push result and the rejected count are what
// they would be without quarantine, and no log carries the content that only
// the quarantine table may hold.
func TestPushTracesQuarantinesEveryDroppedSpan(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	e, app := newTestTracesExporter(t, bqi.AppendOutcome{
		RowErrors: []bqi.RowError{{Index: 1, Code: "FIELDS_ERROR", Message: "bad value secret-value"}},
	})
	tel, err := metadata.NewTelemetry(provider)
	if err != nil {
		t.Fatal(err)
	}
	e.tel = tel
	core, logs := observer.New(zap.DebugLevel)
	e.logger = zap.New(core)
	maxRow := e.cfg.Write.MaxRowBytes
	e.enc = faultyEncoder{next: e.enc, key: "name", fail: "b", oversize: "c", maxRowBytes: maxRow}
	e.opts.ExporterInstance = "collector-1"
	quarantined, stream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 4})

	td := newTestTraces(5)
	td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).SetTraceID(pcommon.NewTraceIDEmpty())

	err = e.pushTraceData(context.Background(), td)
	var traceErr consumererror.Traces
	if !errors.As(err, &traceErr) {
		t.Fatalf("expected the retryable subset, got %T: %v", err, err)
	}
	if got := spanNames(traceErr.Data().ResourceSpans().At(0).ScopeSpans().At(0).Spans()); !equalStrings(got, []string{"d"}) {
		t.Fatalf("retry subset = %v, want only d", got)
	}
	if app.gotRows != 2 {
		t.Fatalf("appended %d signal rows, want d and e", app.gotRows)
	}

	if stream.appended != 1 || stream.gotRows != 4 {
		t.Fatalf("quarantine appends = %d of %d rows, want one append of 4 rows", stream.appended, stream.gotRows)
	}
	wantCodes := []string{"INVALID_TRACE_ID", "SERIALIZATION", "OVERSIZE_ROW", "ROW_ERROR"}
	if got := column(quarantined.rows, "error_code"); !equalStrings(got, wantCodes) {
		t.Errorf("error codes = %v, want %v", got, wantCodes)
	}
	wantSpans := []string{
		pcommon.SpanID{1}.String(), pcommon.SpanID{2}.String(), pcommon.SpanID{3}.String(), pcommon.SpanID{5}.String(),
	}
	if got := column(quarantined.rows, "span_id"); !equalStrings(got, wantSpans) {
		t.Errorf("quarantined span IDs = %v, want those of a, b, c and e: %v", got, wantSpans)
	}
	messages := column(quarantined.rows, "error_message")
	for i, want := range []string{
		"invalid_id: trace_id",
		"serialization: cannot encode secret-value",
		fmt.Sprintf("row_bytes exceeds limit (%d > %d)", maxRow+1, maxRow),
		"row_error: FIELDS_ERROR: bad value secret-value",
	} {
		if i < len(messages) && messages[i] != want {
			t.Errorf("error_message[%d] = %q, want %q", i, messages[i], want)
		}
	}
	for i, r := range quarantined.rows {
		if r["signal"] != schema.SignalSpans || r["exporter_instance"] != "collector-1" {
			t.Errorf("row %d signal = %v, exporter_instance = %v, want %s and collector-1",
				i, r["signal"], r["exporter_instance"], schema.SignalSpans)
		}
	}
	if got := metricInt64Sum(t, reader, "otelcol_exporter_bigquery_rejected_rows"); got != 4 {
		t.Errorf("rejected metric = %d, want each of the 4 dropped spans counted once", got)
	}
	if logsMention(logs, "secret-value") {
		t.Error("a log carries telemetry content that only the quarantine table may hold")
	}
}

// Log records route the same way. The encode drops come after a contract
// violation, so their row positions must be mapped back to record positions.
func TestPushLogsQuarantinesEveryDroppedRecord(t *testing.T) {
	e, app := newTestLogsExporter(t, bqi.AppendOutcome{
		RowErrors: []bqi.RowError{{Index: 1, Code: "FIELDS_ERROR"}},
	})
	e.enc = faultyEncoder{
		next: e.enc, key: "severity_text", fail: "FAIL", oversize: "HUGE", maxRowBytes: e.cfg.Write.MaxRowBytes,
	}
	quarantined, stream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 4})

	ld := plog.NewLogs()
	records := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i, severity := range []string{"INFO", "FAIL", "HUGE", "INFO", "INFO"} {
		lr := records.AppendEmpty()
		lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		lr.SetSeverityText(severity)
		lr.SetSpanID(pcommon.SpanID{byte(i + 1)})
	}
	records.At(0).SetTimestamp(math.MaxInt64 + 1)

	err := e.pushLogData(context.Background(), ld)
	var logsErr consumererror.Logs
	if !errors.As(err, &logsErr) {
		t.Fatalf("expected the retryable subset, got %T: %v", err, err)
	}
	retry := logsErr.Data()
	if retry.LogRecordCount() != 1 ||
		retry.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SpanID() != (pcommon.SpanID{4}) {
		t.Fatalf("retry subset has %d records, want only the fourth", retry.LogRecordCount())
	}
	if app.gotRows != 2 {
		t.Fatalf("appended %d signal rows, want the fourth and fifth records", app.gotRows)
	}

	if stream.appended != 1 || stream.gotRows != 4 {
		t.Fatalf("quarantine appends = %d of %d rows, want one append of 4 rows", stream.appended, stream.gotRows)
	}
	wantCodes := []string{"TIMESTAMP_OVERFLOW", "SERIALIZATION", "OVERSIZE_ROW", "ROW_ERROR"}
	if got := column(quarantined.rows, "error_code"); !equalStrings(got, wantCodes) {
		t.Errorf("error codes = %v, want %v", got, wantCodes)
	}
	wantSpans := []string{
		pcommon.SpanID{1}.String(), pcommon.SpanID{2}.String(), pcommon.SpanID{3}.String(), pcommon.SpanID{5}.String(),
	}
	if got := column(quarantined.rows, "span_id"); !equalStrings(got, wantSpans) {
		t.Errorf("quarantined span IDs = %v, want records 1, 2, 3 and 5: %v", got, wantSpans)
	}
	if got := column(quarantined.rows, "error_message"); len(got) == 4 && got[3] != "row_error: FIELDS_ERROR" {
		t.Errorf("row error message = %q, want the bare code when the service sends no message", got[3])
	}
	for i, r := range quarantined.rows {
		if r["signal"] != schema.SignalLogs {
			t.Errorf("row %d signal = %v, want %s", i, r["signal"], schema.SignalLogs)
		}
	}
}

// Quarantine is best effort: a quarantine append that fails is logged once,
// without telemetry content, and the push result is unchanged.
func TestPushTracesQuarantineFailureLeavesResultUnchanged(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "down")
	for _, tc := range []struct {
		name      string
		outcome   bqi.AppendOutcome
		wantCode  string
		rowErrors int64
	}{
		{
			name:     "transport failure",
			outcome:  bqi.AppendOutcome{Verdict: bqi.Classify(unavailable), Err: unavailable},
			wantCode: bqi.Classify(unavailable).Label,
		},
		{
			name: "row errors",
			outcome: bqi.AppendOutcome{
				RowErrors: []bqi.RowError{{Index: 0, Code: "FIELDS_ERROR", Message: "bad value secret-value"}},
			},
			rowErrors: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, app := newTestTracesExporter(t, bqi.AppendOutcome{AcknowledgedRows: 1})
			core, logs := observer.New(zap.DebugLevel)
			e.logger = zap.New(core)
			_, stream := withQuarantine(t, e.signalExporter, tc.outcome)

			td := newTestTraces(2)
			bad := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(1)
			bad.SetEndTimestamp(0)
			bad.Attributes().PutStr("prompt", "secret-value")

			if err := e.pushTraceData(context.Background(), td); err != nil {
				t.Fatalf("a failed quarantine append changed the push result: %v", err)
			}
			if app.gotRows != 1 || stream.appended != 1 {
				t.Fatalf("signal rows = %d, quarantine appends = %d, want 1 and 1", app.gotRows, stream.appended)
			}
			warned := logs.FilterMessage("quarantine append failed; unacknowledged quarantine rows are dropped").All()
			if len(warned) != 1 {
				t.Fatalf("quarantine failure warnings = %d, want 1", len(warned))
			}
			fields := warned[0].ContextMap()
			if fields["destination"] != quarantineTableID || fields["rows"] != int64(1) || fields["row_errors"] != tc.rowErrors {
				t.Errorf("warning fields = %v, want the quarantine table, 1 row and %d row errors", fields, tc.rowErrors)
			}
			if got, _ := fields["error_code"].(string); got != tc.wantCode {
				t.Errorf("error_code = %q, want %q", got, tc.wantCode)
			}
			if logsMention(logs, "secret-value") {
				t.Error("a log carries telemetry content that only the quarantine table may hold")
			}
		})
	}
}

// Nothing reaches the quarantine table when quarantine is disabled, or when a
// push drops nothing.
func TestPushTracesQuarantinesOnlyDroppedSpansWhenEnabled(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		e, app := newTestTracesExporter(t, bqi.AppendOutcome{AcknowledgedRows: 1})
		enc, err := protoenc.New(schema.QuarantineTableSchema())
		if err != nil {
			t.Fatal(err)
		}
		rec := &recordingEncoder{next: enc}
		e.quarantineEnc = rec

		td := newTestTraces(2)
		td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(1).SetEndTimestamp(0)
		if err := e.pushTraceData(context.Background(), td); err != nil {
			t.Fatal(err)
		}
		if app.gotRows != 1 || len(rec.rows) != 0 {
			t.Fatalf("signal rows = %d, quarantine rows built = %d, want 1 and none", app.gotRows, len(rec.rows))
		}
	})
	t.Run("nothing dropped", func(t *testing.T) {
		e, _ := newTestTracesExporter(t, bqi.AppendOutcome{AcknowledgedRows: 3})
		rec, stream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{})

		if err := e.pushTraceData(context.Background(), newTestTraces(3)); err != nil {
			t.Fatal(err)
		}
		if len(rec.rows) != 0 || stream.appended != 0 {
			t.Fatalf("quarantine rows built = %d, appends = %d, want none", len(rec.rows), stream.appended)
		}
	})
}

// A quarantine row that cannot be serialized, or is still over the row limit,
// is skipped and the rest are appended. When none is left nothing is appended.
func TestQuarantineRowsSkipsRowsItCannotEncode(t *testing.T) {
	for _, tc := range []struct {
		name     string
		spans    int
		wantRows int
	}{
		{name: "some left", spans: 4, wantRows: 1},
		{name: "none left", spans: 3, wantRows: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := newTestTracesExporter(t, bqi.AppendOutcome{AcknowledgedRows: 1})
			core, logs := observer.New(zap.DebugLevel)
			e.logger = zap.New(core)
			_, stream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 1})
			e.quarantineEnc = faultyEncoder{
				next: e.quarantineEnc, key: "error_code",
				fail: "INVALID_TRACE_ID", oversize: "INVALID_SPAN_ID", maxRowBytes: e.cfg.Write.MaxRowBytes,
			}

			td := newTestTraces(tc.spans)
			spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
			spans.At(0).SetTraceID(pcommon.NewTraceIDEmpty())
			spans.At(1).SetSpanID(pcommon.NewSpanIDEmpty())
			if tc.spans > 3 {
				spans.At(3).SetEndTimestamp(0)
			}

			if err := e.pushTraceData(context.Background(), td); err != nil {
				t.Fatal(err)
			}
			wantAppends := min(tc.wantRows, 1)
			if stream.appended != wantAppends || stream.gotRows != tc.wantRows {
				t.Fatalf("quarantine appends = %d of %d rows, want %d of %d", stream.appended, stream.gotRows, wantAppends, tc.wantRows)
			}
			for _, msg := range []string{"dropping quarantine row that failed serialization", "dropping oversized quarantine row"} {
				if got := logs.FilterMessage(msg).Len(); got != 1 {
					t.Errorf("%q logged %d times, want once", msg, got)
				}
			}
		})
	}
}

// Shutdown closes the quarantine stream exactly once and reports its close
// failure.
func TestSignalExporterShutdownClosesQuarantineStream(t *testing.T) {
	closeErr := errors.New("quarantine stream close failed")
	writer := &fakeAppender{}
	quarantine := &fakeAppender{closeErr: closeErr}
	e := &signalExporter{writer: writer, quarantine: quarantine}

	if err := e.shutdown(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("first shutdown error = %v, want %v", err, closeErr)
	}
	if err := e.shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown failed: %v", err)
	}
	if writer.closed != 1 || quarantine.closed != 1 {
		t.Fatalf("closed the writer %d and the quarantine stream %d times, want each exactly once",
			writer.closed, quarantine.closed)
	}
}

// With quarantine enabled, start opens a second stream for the quarantine
// table and names it in the startup log. Without it, no stream is opened.
func TestSignalExporterStartOpensQuarantineStreamOnlyWhenEnabled(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing-adc.json"))
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			originalAdmin := newDestinationAdmin
			t.Cleanup(func() { newDestinationAdmin = originalAdmin })
			newDestinationAdmin = func(
				context.Context, string, string, ...option.ClientOption,
			) (destinationAdmin, error) {
				t.Error("no-auth loopback mode must not construct a metadata client")
				return nil, errors.New("unexpected metadata client")
			}

			cfg := validConfig()
			cfg.Endpoint.URL = "localhost:19050"
			cfg.Endpoint.Insecure = true
			cfg.Endpoint.WithoutAuthentication = true
			cfg.Quarantine.Enabled = enabled
			core, logs := observer.New(zap.InfoLevel)
			e := newSignalExporter(component.TelemetrySettings{Logger: zap.New(core)}, cfg, cfg.Traces.Table, schema.SpansTableSchema())

			if err := e.start(context.Background(), nil); err != nil {
				t.Fatalf("start failed: %v", err)
			}
			if opened := e.quarantine != nil && e.quarantineEnc != nil; opened != enabled {
				t.Errorf("quarantine stream opened = %v, want %v", opened, enabled)
			}
			started := logs.FilterMessage("BigQuery exporter started").All()
			if len(started) != 1 {
				t.Fatalf("startup logs = %d, want 1", len(started))
			}
			want := ""
			if enabled {
				want = quarantineTableID
			}
			if got, _ := started[0].ContextMap()["quarantine"].(string); got != want {
				t.Errorf("startup log quarantine = %q, want %q", got, want)
			}
			if err := e.shutdown(context.Background()); err != nil {
				t.Fatalf("shutdown failed: %v", err)
			}
			if e.quarantine != nil {
				t.Error("shutdown kept the quarantine stream")
			}
		})
	}
}

// Quarantine rows are fitted to the configured row limit and labelled with
// the collector's own service.instance.id, when it has one.
func TestNewSignalExporterWiresQuarantineOptions(t *testing.T) {
	withID := pcommon.NewResource()
	withID.Attributes().PutStr("service.instance.id", "collector-7")
	withoutID := pcommon.NewResource()
	withoutID.Attributes().PutStr("service.name", "otelcol")
	for _, tc := range []struct {
		name     string
		resource pcommon.Resource
		want     string
	}{
		{"service.instance.id", withID, "collector-7"},
		{"no service.instance.id", withoutID, ""},
		{"no resource", pcommon.Resource{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Write.MaxRowBytes = 12345
			e, err := newTracesExporter(component.TelemetrySettings{
				Logger:        zap.NewNop(),
				MeterProvider: noop.NewMeterProvider(),
				Resource:      tc.resource,
			}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if e.opts.ExporterInstance != tc.want || e.opts.MaxRowBytes != 12345 {
				t.Fatalf("exporter instance = %q, max row bytes = %d, want %q and 12345",
					e.opts.ExporterInstance, e.opts.MaxRowBytes, tc.want)
			}
		})
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
