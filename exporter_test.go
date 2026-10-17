// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
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
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/offload"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/protoenc"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

type fakeAppender struct {
	outcome  bqi.AppendOutcome
	appendFn func(context.Context, [][]byte) bqi.AppendOutcome
	closeErr error
	gotRows  int
	appended int
	closed   int
}

func (f *fakeAppender) Append(ctx context.Context, rows [][]byte) bqi.AppendOutcome {
	f.gotRows = len(rows)
	f.appended++
	if f.appendFn != nil {
		return f.appendFn(ctx, rows)
	}
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
			if opened := e.quarantine != nil && e.quarantineEnc != nil && e.quarantineClient != nil; opened != enabled {
				t.Errorf("quarantine stream opened = %v, want %v", opened, enabled)
			}
			if enabled && e.quarantineClient == e.client {
				t.Error("quarantine client must be distinct from the primary writer client")
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
			if e.quarantine != nil || e.quarantineClient != nil {
				t.Error("shutdown kept the quarantine stream or client")
			}
		})
	}
}

// When a split batch acknowledges earlier sub-requests and every remaining row
// in a later sub-request is permanently rejected by the service, retryable is
// empty. Both traces and logs push paths must quarantine the rejected row once
// and return nil so exporterhelper does not replay the whole original batch.
func TestPushSplitBatchAllRemainingRowsRejectedCompletesWithoutRetry(t *testing.T) {
	for _, withTransportErr := range []bool{false, true} {
		t.Run(fmt.Sprintf("withTransportErr=%v", withTransportErr), func(t *testing.T) {
			outcome := bqi.AppendOutcome{
				AcknowledgedRows: 2,
				AckedRanges:      [][2]int{{0, 2}},
				RowErrors:        []bqi.RowError{{Index: 2, Code: "FIELDS_ERROR", Message: "bad row"}},
			}
			if withTransportErr {
				outcome.Verdict = bqi.Classify(status.Error(codes.InvalidArgument, "row rejected"))
				outcome.Err = status.Error(codes.InvalidArgument, "row rejected")
			}

			te, tApp := newTestTracesExporter(t, outcome)
			tQuarantined, tStream := withQuarantine(t, te.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 1})
			if err := te.pushTraceData(context.Background(), newTestTraces(3)); err != nil {
				t.Fatalf("pushTraceData returned %v, want nil when all non-acked rows are permanently rejected", err)
			}
			if tApp.appended != 1 || tStream.appended != 1 || len(tQuarantined.rows) != 1 {
				t.Fatalf("traces appends = %d, quarantine appends = %d (%d rows), want 1, 1, 1",
					tApp.appended, tStream.appended, len(tQuarantined.rows))
			}
			if got := tQuarantined.rows[0]["span_id"]; got != (pcommon.SpanID{3}).String() {
				t.Errorf("quarantined trace span_id = %v, want span 3", got)
			}

			le, lApp := newTestLogsExporter(t, outcome)
			lQuarantined, lStream := withQuarantine(t, le.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 1})
			ld := plog.NewLogs()
			records := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
			for i := range 3 {
				lr := records.AppendEmpty()
				lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
				lr.SetSpanID(pcommon.SpanID{byte(i + 1)})
			}
			if err := le.pushLogData(context.Background(), ld); err != nil {
				t.Fatalf("pushLogData returned %v, want nil when all non-acked rows are permanently rejected", err)
			}
			if lApp.appended != 1 || lStream.appended != 1 || len(lQuarantined.rows) != 1 {
				t.Fatalf("logs appends = %d, quarantine appends = %d (%d rows), want 1, 1, 1",
					lApp.appended, lStream.appended, len(lQuarantined.rows))
			}
			if got := lQuarantined.rows[0]["span_id"]; got != (pcommon.SpanID{3}).String() {
				t.Errorf("quarantined log span_id = %v, want record 3", got)
			}
		})
	}
}

// logsWithBodies builds one scope's log records, one per body, all else equal.
// Records with the same body are identical, so the nth of them takes
// intra_batch_ordinal n. A body of "invalid" also gets a time that does not fit
// the table's INT64 columns, so the transform stage drops that record.
func logsWithBodies(bodies ...string) plog.Logs {
	ld := plog.NewLogs()
	records := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for _, body := range bodies {
		lr := records.AppendEmpty()
		lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		lr.Body().SetStr(body)
		if body == "invalid" {
			lr.SetTimestamp(math.MaxInt64 + 1)
		}
	}
	return ld
}

// A log record's insert_id encodes how many identical records precede it in
// its batch, and a retry payload is fingerprinted from scratch. When a split
// batch has some sub-requests acknowledged and one failed, the payload handed
// back must still give every record it carries the identity it had in the
// original batch. If an acknowledged record were left out ahead of an identical
// retried one, the retried one would be sent under the acknowledged copy's
// insert_id, and the dedup view would collapse two distinct records into one.
func TestPushLogDataSplitBatchPreservesOrdinalForIdenticalRecordsAcrossSplit(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "later sub-request failed")
	// splitAfter acknowledges the given row ranges of the batch sent and fails
	// the rest of it.
	splitAfter := func(acked ...[2]int) bqi.AppendOutcome {
		out := bqi.AppendOutcome{
			AckedRanges: acked,
			Verdict:     bqi.Classify(unavailable),
			Err:         unavailable,
		}
		for _, r := range acked {
			out.AcknowledgedRows += r[1] - r[0]
		}
		return out
	}

	// Row positions are positions among the rows sent, which leave out any
	// record the transform stage dropped.
	tests := []struct {
		name   string
		bodies []string
		out    bqi.AppendOutcome
		// retry lists, by row position, the rows the payload must carry.
		retry []int
	}{
		{
			name:   "identical records split one and one",
			bodies: []string{"same", "same"},
			out:    splitAfter([2]int{0, 1}),
			retry:  []int{0, 1},
		},
		{
			name:   "identical records after a distinct one",
			bodies: []string{"other", "same", "same"},
			out:    splitAfter([2]int{0, 2}),
			retry:  []int{1, 2},
		},
		{
			name:   "identical records over three sub-requests",
			bodies: []string{"same", "same", "same"},
			out:    splitAfter([2]int{0, 1}, [2]int{1, 2}),
			retry:  []int{0, 1, 2},
		},
		{
			name:   "a record the transform stage dropped between identical records",
			bodies: []string{"same", "invalid", "same"},
			out:    splitAfter([2]int{0, 1}),
			retry:  []int{0, 1},
		},
		{
			name:   "distinct acknowledged records stay out of the retry",
			bodies: []string{"a", "b", "c"},
			out:    splitAfter([2]int{0, 2}),
			retry:  []int{2},
		},
		{
			name:   "nothing acknowledged",
			bodies: []string{"same", "same", "other"},
			out:    splitAfter(),
			retry:  []int{0, 1, 2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := newTestLogsExporter(t, tc.out)
			ld := logsWithBodies(tc.bodies...)

			sent, _ := transform.LogRows(ld, e.opts)
			seen := make(map[string]bool, len(sent))
			for _, id := range column(sent, "insert_id") {
				if seen[id] {
					t.Fatalf("setup: insert_id %q is shared by two rows of the batch", id)
				}
				seen[id] = true
			}

			err := e.pushLogData(context.Background(), ld)
			var logsErr consumererror.Logs
			if !errors.As(err, &logsErr) {
				t.Fatalf("expected a retryable consumererror.Logs, got %T: %v", err, err)
			}
			replay, rejects := transform.LogRows(logsErr.Data(), e.opts)
			if len(rejects) != 0 {
				t.Fatalf("the retry payload carries records the transform stage drops: %+v", rejects)
			}
			if len(replay) != len(tc.retry) {
				t.Fatalf("retry payload holds %d records, want %d: rows %v of those sent",
					len(replay), len(tc.retry), tc.retry)
			}
			for i, row := range tc.retry {
				for _, col := range []string{"record_fingerprint", "insert_id"} {
					if got, want := replay[i][col], sent[row][col]; got != want {
						t.Errorf("retry record %d has %s %v, want %v, the value of row %d in the original batch",
							i, col, got, want, row)
					}
				}
			}
		})
	}
}

// A record the service rejected is dropped for good, even when it is identical
// to one that must be retried: returning it would have the service reject it,
// and exporterhelper replay it, over and over.
func TestPushLogDataSplitBatchDoesNotReturnServiceRejectedIdenticalRecord(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "last sub-request failed")
	e, _ := newTestLogsExporter(t, bqi.AppendOutcome{
		AcknowledgedRows: 1,
		AckedRanges:      [][2]int{{0, 1}},
		RowErrors:        []bqi.RowError{{Index: 1, Code: "FIELDS_ERROR", Message: "bad field body"}},
		Verdict:          bqi.Classify(unavailable),
		Err:              unavailable,
	})

	err := e.pushLogData(context.Background(), logsWithBodies("same", "same", "same"))
	var logsErr consumererror.Logs
	if !errors.As(err, &logsErr) {
		t.Fatalf("expected a retryable consumererror.Logs, got %T: %v", err, err)
	}
	// The retried record comes back with its acknowledged predecessor, and the
	// rejected record in between stays out.
	if got := logsErr.Data().LogRecordCount(); got != 2 {
		t.Fatalf("retry payload holds %d records, want 2: the rejected record must not come back", got)
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

func TestNewSignalExporterWiresOffloadAndQuarantinePIIOptions(t *testing.T) {
	cfg := validConfig()
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "otel-offload-bucket"
	cfg.Quarantine.MetadataOnly = true
	cfg.Quarantine.MaxRawPayloadBytes = 4096

	e, err := newTracesExporter(component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.opts.PayloadOffloader == nil || e.opts.QuarantinePayloadOffloader == nil {
		t.Fatal("expected PayloadOffloader and QuarantinePayloadOffloader to be wired when offload.enabled is set")
	}
	if !e.opts.QuarantineMetadataOnly || e.opts.MaxQuarantinePayloadBytes != 4096 {
		t.Fatalf("QuarantineMetadataOnly=%v MaxQuarantinePayloadBytes=%d, want true and 4096",
			e.opts.QuarantineMetadataOnly, e.opts.MaxQuarantinePayloadBytes)
	}
}

func TestPushTracesOffloadsOversizedPayloadEndToEnd(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	cfg := validConfig()
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "otel-offload-bucket"
	cfg.Offload.MinBytes = 256 * 1024
	cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024

	e, err := newTracesExporter(component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: mp,
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	memStore := offload.NewMemoryBlobStore()
	e.offloader.SetStore(memStore)

	enc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingEncoder{next: enc}
	app := &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 1}}
	e.enc = rec
	e.writer = app

	td := newTestTraces(1)
	largePrompt := strings.Repeat("P", 512*1024)
	td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutStr("gen_ai.prompt", largePrompt)

	if err := e.pushTraceData(context.Background(), td); err != nil {
		t.Fatalf("pushTraceData failed on 512KiB prompt with offload enabled: %v", err)
	}
	if app.gotRows != 1 || len(rec.rows) != 1 {
		t.Fatalf("appended %d rows (encoded %d), want 1", app.gotRows, len(rec.rows))
	}
	if memStore.Count() != 1 {
		t.Fatalf("MemoryBlobStore count = %d, want 1 offloaded object", memStore.Count())
	}
	refs, ok := rec.rows[0]["payload_refs"].([]transform.Row)
	if !ok || len(refs) != 1 {
		t.Fatalf("payload_refs = %#v, want 1 entry", rec.rows[0]["payload_refs"])
	}
	if refs[0]["attribute_path"] != "attributes.gen_ai.prompt" {
		t.Errorf("payload_refs[0].attribute_path = %v, want attributes.gen_ai.prompt", refs[0]["attribute_path"])
	}
	if got := metricInt64Sum(t, reader, "otelcol_exporter_bigquery_offloaded_payloads"); got != 1 {
		t.Errorf("offloaded_payloads = %d, want 1", got)
	}
	if got := metricInt64Sum(t, reader, "otelcol_exporter_bigquery_offloaded_bytes"); got != int64(len(largePrompt)) {
		t.Errorf("offloaded_bytes = %d, want %d", got, len(largePrompt))
	}

	// Simulate an offload upload failure on an uncached payload and verify offload_errors increments.
	memStore.SetError(errors.New("gcs write error"))
	tdFail := newTestTraces(1)
	tdFail.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutStr("gen_ai.prompt", strings.Repeat("Q", 512*1024))
	_ = e.pushTraceData(context.Background(), tdFail)
	if got := metricInt64Sum(t, reader, "otelcol_exporter_bigquery_offload_errors"); got != 1 {
		t.Errorf("offload_errors = %d, want 1", got)
	}
}

func TestQuarantineAppendUsesBoundedDetachedContext(t *testing.T) {
	e, app := newTestTracesExporter(t, bqi.AppendOutcome{AcknowledgedRows: 1})
	e.cfg.Quarantine.Timeout = 30 * time.Millisecond

	var sawDeadline bool
	var sawParentCancelled bool
	_, stream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{})
	stream.appendFn = func(qCtx context.Context, _ [][]byte) bqi.AppendOutcome {
		_, sawDeadline = qCtx.Deadline()
		// Even if parent was already cancelled before quarantineRows, qCtx must
		// not inherit its immediate cancellation.
		sawParentCancelled = qCtx.Err() == context.Canceled
		<-qCtx.Done()
		return bqi.AppendOutcome{
			Verdict: bqi.Classify(qCtx.Err()),
			Err:     qCtx.Err(),
		}
	}

	parentCtx, parentCancel := context.WithCancel(context.Background())
	parentCancel() // Cancel caller context before quarantineRows runs; signal fakeAppender ignores ctx.

	td := newTestTraces(2)
	td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(1).SetEndTimestamp(0)

	start := time.Now()
	if err := e.pushTraceData(parentCtx, td); err != nil {
		t.Fatalf("quarantine timeout must not fail signal push: %v", err)
	}
	elapsed := time.Since(start)
	if app.gotRows != 1 {
		t.Fatalf("signal rows = %d, want 1", app.gotRows)
	}
	if !sawDeadline {
		t.Error("quarantine Append context had no deadline")
	}
	if sawParentCancelled {
		t.Error("quarantine Append context inherited caller cancellation instead of detaching via context.WithoutCancel")
	}
	if elapsed > time.Second {
		t.Errorf("blocking quarantine stream stalled pushTraceData for %v, want bounded ~30ms timeout", elapsed)
	}
}

type funcBlobStore struct {
	writeFn func(ctx context.Context, bucket, object, contentType string, data []byte) (string, error)
}

func (f funcBlobStore) WriteObject(ctx context.Context, bucket, object, contentType string, data []byte) (string, error) {
	return f.writeFn(ctx, bucket, object, contentType, data)
}

func TestPushTracesOffloadUploadFailureReturnsRetryableSpansWithoutQuarantining(t *testing.T) {
	t.Run("append succeeds for surviving spans", func(t *testing.T) {
		cfg := validConfig()
		cfg.Offload.Enabled = true
		cfg.Offload.Bucket = "otel-offload-bucket"
		cfg.Offload.MinBytes = 1024
		cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024

		e, err := newTracesExporter(component.TelemetrySettings{
			Logger:        zap.NewNop(),
			MeterProvider: noop.NewMeterProvider(),
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		uploadErr := errors.New("simulated GCS 503")
		e.offloader.SetStore(funcBlobStore{
			writeFn: func(_ context.Context, _, _, _ string, data []byte) (string, error) {
				if strings.HasPrefix(string(data), "FAIL") {
					return "", uploadErr
				}
				return "1", nil
			},
		})

		enc, err := protoenc.New(schema.SpansTableSchema())
		if err != nil {
			t.Fatal(err)
		}
		app := &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 2}}
		e.enc = enc
		e.writer = app
		quarantined, qStream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 1})

		// Span 0 ("a"): normal
		// Span 1 ("b"): 2KiB attribute whose offload upload fails -> must be retryable, NOT quarantined
		// Span 2 ("c"): permanently invalid (EndTimestamp=0) -> must be quarantined, NOT retried
		// Span 3 ("d"): normal
		td := newTestTraces(4)
		spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
		spans.At(1).Attributes().PutStr("gen_ai.prompt", "FAIL"+strings.Repeat("x", 2048))
		spans.At(2).SetEndTimestamp(0)

		pushErr := e.pushTraceData(context.Background(), td)
		if pushErr == nil {
			t.Fatal("expected retryable error for span whose offload upload failed")
		}
		if consumererror.IsPermanent(pushErr) {
			t.Fatalf("offload upload failure must be retryable, got permanent: %v", pushErr)
		}
		var traceErr consumererror.Traces
		if !errors.As(pushErr, &traceErr) {
			t.Fatalf("expected consumererror.Traces, got %T: %v", pushErr, pushErr)
		}
		got := traceErr.Data().ResourceSpans().At(0).ScopeSpans().At(0).Spans()
		if !equalStrings(spanNames(got), []string{"b"}) {
			t.Fatalf("retryable spans = %v, want [b]", spanNames(got))
		}
		if app.gotRows != 2 {
			t.Fatalf("appended signal rows = %d, want 2 (spans a and d)", app.gotRows)
		}
		if qStream.gotRows != 1 || len(quarantined.rows) != 1 {
			t.Fatalf("quarantined rows = %d, want 1 (only span c)", len(quarantined.rows))
		}
		if quarantined.rows[0]["span_id"] != (pcommon.SpanID{3}).String() {
			t.Errorf("quarantined span_id = %v, want span c (%s)", quarantined.rows[0]["span_id"], (pcommon.SpanID{3}).String())
		}
	})

	t.Run("row error index mapping accounts for skipped offload failures", func(t *testing.T) {
		cfg := validConfig()
		cfg.Offload.Enabled = true
		cfg.Offload.Bucket = "otel-offload-bucket"
		cfg.Offload.MinBytes = 1024
		cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024

		e, err := newTracesExporter(component.TelemetrySettings{
			Logger:        zap.NewNop(),
			MeterProvider: noop.NewMeterProvider(),
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		e.offloader.SetStore(funcBlobStore{
			writeFn: func(_ context.Context, _, _, _ string, _ []byte) (string, error) {
				return "", errors.New("simulated GCS 503")
			},
		})

		enc, err := protoenc.New(schema.SpansTableSchema())
		if err != nil {
			t.Fatal(err)
		}
		// Encoded rows sent to BigQuery are [span "a" (idx 0), span "d" (idx 3)].
		// RowError at Index 1 refers to encoded row 1 -> original span "d" (idx 3).
		app := &fakeAppender{outcome: bqi.AppendOutcome{
			RowErrors: []bqi.RowError{{Index: 1, Code: "FIELDS_ERROR", Message: "bad row"}},
		}}
		e.enc = enc
		e.writer = app
		quarantined, _ := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 2})

		td := newTestTraces(4)
		spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
		spans.At(1).Attributes().PutStr("gen_ai.prompt", strings.Repeat("x", 2048)) // idx 1: offload fails
		spans.At(2).SetEndTimestamp(0)                                              // idx 2: pre-offload rejected

		pushErr := e.pushTraceData(context.Background(), td)
		if pushErr == nil || consumererror.IsPermanent(pushErr) {
			t.Fatalf("expected retryable error, got %v", pushErr)
		}
		var traceErr consumererror.Traces
		if !errors.As(pushErr, &traceErr) {
			t.Fatalf("expected consumererror.Traces, got %T: %v", pushErr, pushErr)
		}
		// Span "a" (idx 0, valid row in failed Append) and span "b" (idx 1, offload failed) must be retried.
		got := traceErr.Data().ResourceSpans().At(0).ScopeSpans().At(0).Spans()
		if !equalStrings(spanNames(got), []string{"a", "b"}) {
			t.Fatalf("retryable spans = %v, want [a b]", spanNames(got))
		}
		// Span "c" (idx 2) and span "d" (idx 3) must be quarantined.
		wantQuarantinedSpans := []string{pcommon.SpanID{3}.String(), pcommon.SpanID{4}.String()}
		if gotSpans := column(quarantined.rows, "span_id"); !equalStrings(gotSpans, wantQuarantinedSpans) {
			t.Fatalf("quarantined span_ids = %v, want %v", gotSpans, wantQuarantinedSpans)
		}
	})
}

func TestPushLogsOffloadUploadFailureReturnsRetryableLogsWithoutQuarantining(t *testing.T) {
	cfg := validConfig()
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "otel-offload-bucket"
	cfg.Offload.MinBytes = 1024
	cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024

	e, err := newLogsExporter(component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.offloader.SetStore(funcBlobStore{
		writeFn: func(_ context.Context, _, _, _ string, _ []byte) (string, error) {
			return "", errors.New("simulated GCS 503")
		},
	})

	enc, err := protoenc.New(schema.LogsTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	app := &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 1}}
	e.enc = enc
	e.writer = app
	quarantined, qStream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{})

	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	lr0 := sl.LogRecords().AppendEmpty()
	lr0.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	lr0.Body().SetStr(strings.Repeat("L", 2048)) // offload upload fails
	lr1 := sl.LogRecords().AppendEmpty()
	lr1.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	lr1.Body().SetStr("small-surviving-log")

	pushErr := e.pushLogData(context.Background(), ld)
	if pushErr == nil || consumererror.IsPermanent(pushErr) {
		t.Fatalf("expected retryable consumererror.Logs, got %v", pushErr)
	}
	var logErr consumererror.Logs
	if !errors.As(pushErr, &logErr) {
		t.Fatalf("expected consumererror.Logs, got %T: %v", pushErr, pushErr)
	}
	if logErr.Data().LogRecordCount() != 1 {
		t.Fatalf("retryable log count = %d, want 1", logErr.Data().LogRecordCount())
	}
	if app.gotRows != 1 {
		t.Fatalf("appended log rows = %d, want 1", app.gotRows)
	}
	if qStream.appended != 0 || len(quarantined.rows) != 0 {
		t.Fatalf("quarantine appends = %d (rows = %d), want 0", qStream.appended, len(quarantined.rows))
	}
}

func TestQuarantineOffloadAndAppendShareSingleTimeoutBudget(t *testing.T) {
	cfg := validConfig()
	cfg.Quarantine.Enabled = true
	cfg.Quarantine.Timeout = 60 * time.Millisecond
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "otel-offload-bucket"
	cfg.Offload.MinBytes = 512
	cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024
	cfg.Offload.Timeout = 5 * time.Second

	e, err := newTracesExporter(component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}

	var uploadBudget time.Duration
	e.offloader.SetStore(funcBlobStore{
		writeFn: func(ctx context.Context, _, _, _ string, _ []byte) (string, error) {
			if dl, ok := ctx.Deadline(); ok {
				uploadBudget = time.Until(dl)
			}
			select {
			case <-time.After(25 * time.Millisecond):
				return "1", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
	})

	enc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	e.enc = enc
	e.writer = &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 1}}

	var appendBudget time.Duration
	_, qStream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{})
	qStream.appendFn = func(qCtx context.Context, _ [][]byte) bqi.AppendOutcome {
		if dl, ok := qCtx.Deadline(); ok {
			appendBudget = time.Until(dl)
		}
		<-qCtx.Done()
		return bqi.AppendOutcome{Verdict: bqi.Classify(qCtx.Err()), Err: qCtx.Err()}
	}

	td := newTestTraces(2)
	bad := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(1)
	bad.SetEndTimestamp(0)
	bad.Attributes().PutStr("large", strings.Repeat("Q", 1024))

	start := time.Now()
	if err := e.pushTraceData(context.Background(), td); err != nil {
		t.Fatalf("pushTraceData failed: %v", err)
	}
	elapsed := time.Since(start)

	if uploadBudget <= 0 || uploadBudget > 70*time.Millisecond {
		t.Errorf("quarantine upload context remaining budget = %v, want bounded by ~60ms quarantine.timeout (not 5s offload.timeout)", uploadBudget)
	}
	if appendBudget <= 0 || appendBudget >= uploadBudget {
		t.Errorf("quarantine append remaining budget = %v, want < uploadBudget (%v) under shared deadline", appendBudget, uploadBudget)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("quarantine upload + append took %v, want bounded by single ~60ms quarantine.timeout", elapsed)
	}
}

func TestPushTracesMetadataOnlyQuarantineStripsRowErrorMessageDetail(t *testing.T) {
	e, _ := newTestTracesExporter(t, bqi.AppendOutcome{
		RowErrors: []bqi.RowError{{Index: 0, Code: "FIELDS_ERROR", Message: "invalid value secret-pii-token"}},
	})
	e.opts.QuarantineMetadataOnly = true
	quarantined, _ := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 1})

	_ = e.pushTraceData(context.Background(), newTestTraces(1))
	if len(quarantined.rows) != 1 {
		t.Fatalf("quarantined rows = %d, want 1", len(quarantined.rows))
	}
	gotMsg, _ := quarantined.rows[0]["error_message"].(string)
	if gotMsg != "row_error" {
		t.Errorf("metadata_only error_message = %q, want %q", gotMsg, "row_error")
	}
	if strings.Contains(gotMsg, "secret-pii-token") {
		t.Errorf("metadata_only error_message leaked raw service detail: %q", gotMsg)
	}
}

func TestPushTracesDeduplicatesRepeatedContentAddressedUploadsInPush(t *testing.T) {
	cfg := validConfig()
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "otel-offload-bucket"
	cfg.Offload.MinBytes = 256 * 1024
	cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024
	cfg.Offload.MaxConcurrency = 8

	e, err := newTracesExporter(component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	writesByKey := make(map[string]int)
	e.offloader.SetStore(funcBlobStore{
		writeFn: func(_ context.Context, bucket, object, _ string, _ []byte) (string, error) {
			key := bucket + "/" + object
			mu.Lock()
			writesByKey[key]++
			count := writesByKey[key]
			mu.Unlock()
			if count > 1 {
				return "", fmt.Errorf("duplicate WriteObject call #%d for key %q within a single push", count, key)
			}
			return "gen-1", nil
		},
	})

	enc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingEncoder{next: enc}
	app := &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 3}}
	e.enc = rec
	e.writer = app

	sharedPayload := strings.Repeat("S", 300*1024)
	td := newTestTraces(3)
	spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	for i := 0; i < spans.Len(); i++ {
		spans.At(i).Attributes().PutStr("gen_ai.prompt", sharedPayload)
	}

	if err := e.pushTraceData(context.Background(), td); err != nil {
		t.Fatalf("pushTraceData failed on 3 spans sharing identical 300KiB value: %v", err)
	}
	if app.gotRows != 3 || len(rec.rows) != 3 {
		t.Fatalf("appended %d rows (encoded %d), want 3", app.gotRows, len(rec.rows))
	}
	mu.Lock()
	totalWrites := len(writesByKey)
	mu.Unlock()
	if totalWrites != 1 {
		t.Fatalf("unique keys written = %d, want 1", totalWrites)
	}
}

func TestPushTracesConcurrentOffloadWithinAttemptBudget(t *testing.T) {
	for _, tc := range []struct {
		name  string
		spans int
		attrs int
	}{
		{name: "10_spans_1_attr_each", spans: 10, attrs: 1},
		{name: "1_span_10_attrs", spans: 1, attrs: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Offload.Enabled = true
			cfg.Offload.Bucket = "otel-offload-bucket"
			cfg.Offload.MinBytes = 1024
			cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024
			cfg.Offload.MaxConcurrency = 8

			e, err := newTracesExporter(component.TelemetrySettings{
				Logger:        zap.NewNop(),
				MeterProvider: noop.NewMeterProvider(),
			}, cfg)
			if err != nil {
				t.Fatal(err)
			}

			var inFlight atomic.Int32
			var maxInFlight atomic.Int32
			e.offloader.SetStore(funcBlobStore{
				writeFn: func(ctx context.Context, _, _, _ string, _ []byte) (string, error) {
					cur := inFlight.Add(1)
					for {
						prev := maxInFlight.Load()
						if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
							break
						}
					}
					defer inFlight.Add(-1)
					select {
					case <-time.After(20 * time.Millisecond):
						return "1", nil
					case <-ctx.Done():
						return "", ctx.Err()
					}
				},
			})

			enc, err := protoenc.New(schema.SpansTableSchema())
			if err != nil {
				t.Fatal(err)
			}
			app := &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: tc.spans}}
			e.enc = enc
			e.writer = app

			td := newTestTraces(tc.spans)
			spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
			for i := 0; i < tc.spans; i++ {
				for j := 0; j < tc.attrs; j++ {
					val := fmt.Sprintf("span-%d-attr-%d-%s", i, j, strings.Repeat("Z", 1500))
					spans.At(i).Attributes().PutStr(fmt.Sprintf("blob_%d", j), val)
				}
			}

			// 10 distinct 20ms uploads would take 200ms sequentially and exceed the
			// 110ms context deadline; with MaxConcurrency=8 they finish in 2 waves (~40ms).
			ctx, cancel := context.WithTimeout(context.Background(), 110*time.Millisecond)
			defer cancel()

			start := time.Now()
			if err := e.pushTraceData(ctx, td); err != nil {
				t.Fatalf("pushTraceData timed out or failed (%v); sequential uploads would take ~200ms", err)
			}
			elapsed := time.Since(start)
			if elapsed >= 110*time.Millisecond {
				t.Errorf("pushTraceData took %v, want < 110ms", elapsed)
			}
			if gotMax := maxInFlight.Load(); gotMax <= 1 || gotMax > 8 {
				t.Errorf("max concurrent WriteObject calls = %d, want in (1, 8]", gotMax)
			}
		})
	}
}

func TestPushLogsPreservesOrdinalPeersWhenOffloadFailsOnce(t *testing.T) {
	cfg := validConfig()
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "otel-offload-bucket"
	cfg.Offload.MinBytes = 1024
	cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024
	cfg.Offload.MaxConcurrency = 1

	e, err := newLogsExporter(component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}

	var uploadCount atomic.Int32
	e.offloader.SetStore(funcBlobStore{
		writeFn: func(_ context.Context, _, _, _ string, _ []byte) (string, error) {
			if uploadCount.Add(1) == 1 {
				return "", errors.New("simulated transient upload failure on 1st record")
			}
			return "1", nil
		},
	})

	enc, err := protoenc.New(schema.LogsTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	app := &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 1}}
	e.enc = enc
	e.writer = app

	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	lr0 := sl.LogRecords().AppendEmpty()
	lr0.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	lr0.Body().SetStr(strings.Repeat("IDENTICAL_LOG_PAYLOAD", 100))
	lr1 := sl.LogRecords().AppendEmpty()
	lr1.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	lr1.Body().SetStr(strings.Repeat("IDENTICAL_LOG_PAYLOAD", 100))

	pushErr := e.pushLogData(context.Background(), ld)
	if pushErr == nil || consumererror.IsPermanent(pushErr) {
		t.Fatalf("expected retryable consumererror.Logs, got %v", pushErr)
	}
	var logErr consumererror.Logs
	if !errors.As(pushErr, &logErr) {
		t.Fatalf("expected consumererror.Logs, got %T: %v", pushErr, pushErr)
	}
	// Because lr0 and lr1 are identical logs and lr1 was accepted with ordinal 0,
	// the retry payload MUST preserve ordinal peers and include BOTH logs (not just lr0),
	// preventing insert_id collision in BigQuery dedup view.
	if got := logErr.Data().LogRecordCount(); got != 2 {
		t.Fatalf("retryable log count = %d, want 2 (ordinal peers preserved)", got)
	}
}

func TestQuarantineStopsProcessingOnceBudgetIsExhausted(t *testing.T) {
	cfg := validConfig()
	cfg.Quarantine.Enabled = true
	cfg.Quarantine.Timeout = 10 * time.Millisecond
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "otel-offload-bucket"
	cfg.Offload.MinBytes = 512
	cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024

	e, err := newTracesExporter(component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}

	var callbackCalls atomic.Int32
	e.offloader.SetStore(funcBlobStore{
		writeFn: func(ctx context.Context, _, _, _ string, _ []byte) (string, error) {
			callbackCalls.Add(1)
			// Sleep past the 10ms quarantine timeout so the deadline expires
			select {
			case <-time.After(20 * time.Millisecond):
				return "1", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
	})

	enc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	e.enc = enc
	e.writer = &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 1}}
	_, qStream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{})

	// 32 rejected records with 512 KiB payloads
	const count = 32
	td := newTestTraces(count)
	spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	for i := 0; i < count; i++ {
		spans.At(i).SetEndTimestamp(0) // rejection reason
		spans.At(i).Attributes().PutStr("large", strings.Repeat("X", 2048))
	}

	start := time.Now()
	_ = e.pushTraceData(context.Background(), td)
	elapsed := time.Since(start)

	// Verify that callbacks stopped once deadline expired, rather than running for all 32 records
	if calls := callbackCalls.Load(); calls > 2 {
		t.Errorf("quarantine payload offload callback called %d times, want <= 2 after deadline expiry", calls)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("pushTraceData delayed by %v, want <= 100ms", elapsed)
	}
	_ = qStream
}

// A record whose payload upload fails is withheld from the rows sent and does
// not take an intra_batch_ordinal, so an identical record behind it is written
// under the ordinal, and the insert_id, the withheld one would have had. The
// retries must still leave every record with an insert_id of its own: across
// the first attempt and its retries, the distinct insert_ids written must be
// those a run without upload failures writes. Otherwise the dedup view
// collapses identical records into one.
func TestPushLogsOffloadRetryKeepsDistinctInsertIDs(t *testing.T) {
	payload := func(s string) string { return strings.Repeat(s, 2048) }
	tests := []struct {
		name        string
		bodies      []string
		concurrency int
		// fail reports whether the nth upload, counted from 1, fails.
		fail func(n int) bool
	}{
		{
			name:        "two identical records, the first upload fails once",
			bodies:      []string{payload("A"), payload("A")},
			concurrency: 1,
			fail:        func(n int) bool { return n == 1 },
		},
		{
			name:        "three identical records, the second upload fails once",
			bodies:      []string{payload("A"), payload("A"), payload("A")},
			concurrency: 1,
			fail:        func(n int) bool { return n == 2 },
		},
		{
			name:        "identical records among distinct ones, two uploads fail",
			bodies:      []string{payload("B"), payload("A"), payload("A"), payload("C"), payload("A")},
			concurrency: 1,
			fail:        func(n int) bool { return n == 2 || n == 5 },
		},
		{
			name:        "concurrent uploads, the first fails",
			bodies:      []string{payload("A"), payload("A")},
			concurrency: 8,
			fail:        func(n int) bool { return n == 1 },
		},
		{
			name:        "inline identical records around a failed upload",
			bodies:      []string{"x", "x", payload("Z"), "x"},
			concurrency: 1,
			fail:        func(n int) bool { return n == 1 },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := insertIDsWrittenUntilDelivered(t, tc.concurrency, func(int) bool { return false }, logsWithBodies(tc.bodies...))
			if len(want) != len(tc.bodies) {
				t.Fatalf("setup: a run without upload failures wrote %d distinct insert_ids, want %d", len(want), len(tc.bodies))
			}
			got := insertIDsWrittenUntilDelivered(t, tc.concurrency, tc.fail, logsWithBodies(tc.bodies...))
			if !slices.Equal(got, want) {
				t.Fatalf("distinct insert_ids written = %d %v, want the %d of a run without upload failures %v",
					len(got), got, len(want), want)
			}
		})
	}
}

// insertIDsWrittenUntilDelivered pushes ld through a logs exporter whose
// payload uploads fail as fail says, pushes each retry payload it returns
// until a push succeeds, and returns the sorted distinct insert_ids of every
// row appended along the way. The writer acknowledges every row it is given.
func insertIDsWrittenUntilDelivered(t *testing.T, concurrency int, fail func(n int) bool, ld plog.Logs) []string {
	t.Helper()
	cfg := validConfig()
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "otel-offload-bucket"
	cfg.Offload.MinBytes = 1024
	cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024
	cfg.Offload.MaxConcurrency = concurrency
	e, err := newLogsExporter(component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var uploads atomic.Int32
	e.offloader.SetStore(funcBlobStore{
		writeFn: func(_ context.Context, _, _, _ string, _ []byte) (string, error) {
			if fail(int(uploads.Add(1))) {
				return "", errors.New("simulated transient upload failure")
			}
			return "1", nil
		},
	})
	enc, err := protoenc.New(schema.LogsTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingEncoder{next: enc}
	e.enc = rec
	e.writer = &fakeAppender{appendFn: func(_ context.Context, rows [][]byte) bqi.AppendOutcome {
		return bqi.AppendOutcome{AcknowledgedRows: len(rows), AckedRanges: [][2]int{{0, len(rows)}}}
	}}

	payload := ld
	for range 10 {
		err := e.pushLogData(context.Background(), payload)
		if err == nil {
			ids := column(rec.rows, "insert_id")
			slices.Sort(ids)
			return slices.Compact(ids)
		}
		var logsErr consumererror.Logs
		if !errors.As(err, &logsErr) {
			t.Fatalf("expected a retryable consumererror.Logs, got %T: %v", err, err)
		}
		payload = logsErr.Data()
	}
	t.Fatal("the batch was not delivered within 10 pushes")
	return nil
}

func newTestMetrics(points int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("gen_ai.client.token.usage")
	g := m.SetEmptyGauge()
	for i := 0; i < points; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + uint64(i)*1_000_000_000))
		dp.SetIntValue(int64(i + 1))
	}
	return md
}

func newTestMetricsExporter(t *testing.T, out bqi.AppendOutcome) (*metricsExporter, *fakeAppender) {
	t.Helper()
	cfg := validConfig()
	tel, err := metadata.NewTelemetry(noop.NewMeterProvider())
	if err != nil {
		t.Fatal(err)
	}
	enc, err := protoenc.New(schema.MetricsTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	app := &fakeAppender{outcome: out}

	se := &signalExporter{
		cfg:    cfg,
		table:  cfg.Metrics.Table,
		logger: zap.NewNop(),
		tel:    tel,
		writer: app,
		enc:    enc,
		schema: schema.MetricsTableSchema(),
		opts:   transform.Options{Limits: cfg.Limits.toTransform()},
	}
	return &metricsExporter{se}, app
}

func TestPushMetricsSuccess(t *testing.T) {
	e, app := newTestMetricsExporter(t, bqi.AppendOutcome{AcknowledgedRows: 3})

	if err := e.pushMetricsData(context.Background(), newTestMetrics(3)); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if app.gotRows != 3 {
		t.Fatalf("appended %d rows, want 3", app.gotRows)
	}
}

func TestPushMetricsReturnsValidSubsetOnRowErrors(t *testing.T) {
	e, _ := newTestMetricsExporter(t, bqi.AppendOutcome{
		RowErrors: []bqi.RowError{{Index: 1, Code: "FIELDS_ERROR", Message: "bad field metric_name"}},
	})

	err := e.pushMetricsData(context.Background(), newTestMetrics(3))
	if err == nil || consumererror.IsPermanent(err) {
		t.Fatalf("error = %v, want retryable subset error", err)
	}
	var mdErr consumererror.Metrics
	if !errors.As(err, &mdErr) {
		t.Fatalf("error %T does not carry pmetric.Metrics", err)
	}
	dps := mdErr.Data().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints()
	if dps.Len() != 2 || dps.At(0).IntValue() != 1 || dps.At(1).IntValue() != 3 {
		t.Fatalf("retry subset len=%d, want 2 points with values [1, 3]", dps.Len())
	}
}

func TestPushMetricsQuarantinesEveryDroppedDataPoint(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	tel, err := metadata.NewTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := newTestMetricsExporter(t, bqi.AppendOutcome{
		AcknowledgedRows: 1,
		RowErrors:        []bqi.RowError{{Index: 1, Code: "FIELDS_ERROR", Message: "bad metric row"}},
	})
	e.tel = tel
	e.cfg.Limits.MaxAttributeCount = 1
	e.opts.Limits = e.cfg.Limits.toTransform()
	_, stream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 2})

	md := newTestMetrics(3)
	dps := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints()
	dps.At(0).Attributes().PutStr("a", "1")
	dps.At(0).Attributes().PutStr("b", "2") // exceeds MaxAttributeCount=1

	err = e.pushMetricsData(context.Background(), md)
	if err == nil || consumererror.IsPermanent(err) {
		t.Fatalf("pushMetricsData error = %v, want retryable subset error for surviving point", err)
	}
	if stream.gotRows != 2 {
		t.Fatalf("quarantined %d rows, want 2 (1 limit breach + 1 row rejection)", stream.gotRows)
	}
	if got := metricInt64Sum(t, reader, "otelcol_exporter_bigquery_quarantined_rows"); got != 2 {
		t.Errorf("quarantined_rows = %d, want 2", got)
	}
	if got := metricInt64Sum(t, reader, "otelcol_exporter_bigquery_append_bytes"); got <= 0 {
		t.Errorf("append_bytes = %d, want > 0", got)
	}
}

type fakeStatusHost struct {
	events []*componentstatus.Event
}

func (f *fakeStatusHost) GetExtensions() map[component.ID]component.Component { return nil }

func (f *fakeStatusHost) Report(ev *componentstatus.Event) {
	f.events = append(f.events, ev)
}

func TestComponentStatusReportedAcrossLifecycleAndAppend(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing-adc.json"))
	originalAdmin := newDestinationAdmin
	t.Cleanup(func() { newDestinationAdmin = originalAdmin })
	newDestinationAdmin = func(
		context.Context, string, string, ...option.ClientOption,
	) (destinationAdmin, error) {
		return &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadata: &bq.TableMetadata{Schema: mustBigQuerySchema(t, schema.SpansTableSchema())}}, nil
	}

	cfg := validConfig()
	cfg.Endpoint.URL = "localhost:19050"
	cfg.Endpoint.Insecure = true
	cfg.Endpoint.WithoutAuthentication = true
	e := newSignalExporter(componentTelemetryForTest(), cfg, cfg.Traces.Table, schema.SpansTableSchema())
	host := &fakeStatusHost{}

	if err := e.start(context.Background(), host); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	// Replace writer with fakeAppender to test appendEncoded status transitions.
	_ = e.writer.Close()
	tempErr := status.Error(codes.Unavailable, "temp")
	app := &fakeAppender{outcome: bqi.AppendOutcome{Verdict: bqi.Classify(tempErr), Err: tempErr}}
	e.writer = app

	_ = e.appendEncoded(context.Background(), [][]byte{[]byte("row")}, []int{0}, "op-1")
	app.outcome = bqi.AppendOutcome{AcknowledgedRows: 1}
	_ = e.appendEncoded(context.Background(), [][]byte{[]byte("row")}, []int{0}, "op-2")
	permErr := status.Error(codes.PermissionDenied, "denied")
	app.outcome = bqi.AppendOutcome{Verdict: bqi.Classify(permErr), Err: permErr}
	_ = e.appendEncoded(context.Background(), [][]byte{[]byte("row")}, []int{0}, "op-3")

	if err := e.shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	var gotStatuses []componentstatus.Status
	for _, ev := range host.events {
		gotStatuses = append(gotStatuses, ev.Status())
	}
	wantStatuses := []componentstatus.Status{
		componentstatus.StatusStarting,
		componentstatus.StatusOK,
		componentstatus.StatusRecoverableError,
		componentstatus.StatusOK,
		componentstatus.StatusPermanentError,
		componentstatus.StatusStopping,
		componentstatus.StatusStopped,
	}
	if len(gotStatuses) != len(wantStatuses) {
		t.Fatalf("reported statuses = %v, want %v", gotStatuses, wantStatuses)
	}
	for i := range wantStatuses {
		if gotStatuses[i] != wantStatuses[i] {
			t.Errorf("status[%d] = %v, want %v", i, gotStatuses[i], wantStatuses[i])
		}
	}
}

// The quarantine self-telemetry exists to expose dead-letter loss, so a
// quarantine append that only partly succeeds must count each row by its own
// fate: quarantined when its sub-request was acknowledged and the service did
// not reject it, failed otherwise. A real writer reports AckedRanges alongside
// AcknowledgedRows, and rows outside the acknowledged ranges were not written
// even though another sub-request was.
func TestQuarantineTelemetryCountsEachRowOfAPartiallyFailedAppend(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "later quarantine sub-request failed")
	rejected := status.Error(codes.InvalidArgument, "quarantine row rejected")
	tests := []struct {
		name                   string
		out                    bqi.AppendOutcome
		quarantined, failedRow int64
	}{
		{
			name: "first sub-request acknowledged, the second failed",
			out: bqi.AppendOutcome{
				AcknowledgedRows: 2, AckedRanges: [][2]int{{0, 2}},
				Verdict: bqi.Classify(unavailable), Err: unavailable,
			},
			quarantined: 2, failedRow: 1,
		},
		{
			name: "a row error fails the sub-request it travelled in",
			out: bqi.AppendOutcome{
				AcknowledgedRows: 1, AckedRanges: [][2]int{{0, 1}},
				RowErrors: []bqi.RowError{{Index: 2, Code: "FIELDS_ERROR", Message: "bad quarantine row"}},
				Verdict:   bqi.Classify(rejected), Err: rejected,
			},
			quarantined: 1, failedRow: 2,
		},
		{
			name:        "every sub-request acknowledged",
			out:         bqi.AppendOutcome{AcknowledgedRows: 3, AckedRanges: [][2]int{{0, 2}, {2, 3}}},
			quarantined: 3, failedRow: 0,
		},
		{
			name:        "nothing acknowledged",
			out:         bqi.AppendOutcome{Verdict: bqi.Classify(unavailable), Err: unavailable},
			quarantined: 0, failedRow: 3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
			tel, err := metadata.NewTelemetry(mp)
			if err != nil {
				t.Fatal(err)
			}
			e, app := newTestTracesExporter(t, bqi.AppendOutcome{})
			e.tel = tel
			_, stream := withQuarantine(t, e.signalExporter, tc.out)

			// Three spans without an end time: the transform stage rejects all
			// three, so the signal table receives nothing and the quarantine
			// append carries three rows.
			td := newTestTraces(3)
			spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
			for i := range spans.Len() {
				spans.At(i).SetEndTimestamp(0)
			}
			if err := e.pushTraceData(context.Background(), td); err != nil {
				t.Fatalf("pushTraceData returned %v, want nil: every span was rejected", err)
			}
			if app.appended != 0 || stream.gotRows != 3 {
				t.Fatalf("signal appends = %d, quarantine rows = %d, want 0 and 3", app.appended, stream.gotRows)
			}
			if got := metricInt64SumOrZero(t, reader, "otelcol_exporter_bigquery_quarantined_rows"); got != tc.quarantined {
				t.Errorf("quarantined_rows = %d, want %d", got, tc.quarantined)
			}
			if got := metricInt64SumOrZero(t, reader, "otelcol_exporter_bigquery_quarantine_failed_rows"); got != tc.failedRow {
				t.Errorf("quarantine_failed_rows = %d, want %d", got, tc.failedRow)
			}
		})
	}
}

// metricInt64SumOrZero is metricInt64Sum for a counter that may never have
// been incremented, and so never collected.
func metricInt64SumOrZero(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return metricInt64Sum(t, reader, name)
			}
		}
	}
	return 0
}

func metricInt64ByAttr(t *testing.T, reader *sdkmetric.ManualReader, name, attrKey string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s data = %T, want int64 sum", name, m.Data)
			}
			for _, point := range sum.DataPoints {
				for _, kv := range point.Attributes.ToSlice() {
					if string(kv.Key) == attrKey {
						out[kv.Value.AsString()] += point.Value
					}
				}
			}
		}
	}
	return out
}

// metricFloat64Gauge returns the single data point of a float64 gauge.
func metricFloat64Gauge(t *testing.T, reader *sdkmetric.ManualReader, name string) float64 {
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
			gauge, ok := m.Data.(metricdata.Gauge[float64])
			if !ok {
				t.Fatalf("metric %s data = %T, want float64 gauge", name, m.Data)
			}
			if len(gauge.DataPoints) != 1 {
				t.Fatalf("metric %s has %d data points, want 1", name, len(gauge.DataPoints))
			}
			return gauge.DataPoints[0].Value
		}
	}
	t.Fatalf("metric %s was not recorded", name)
	return 0
}

// slowQuarantineStore is a payload store whose every upload outlasts a 10 ms
// quarantine budget, and that counts the uploads it was asked for.
func slowQuarantineStore(calls *atomic.Int32) funcBlobStore {
	return funcBlobStore{
		writeFn: func(ctx context.Context, _, _, _ string, _ []byte) (string, error) {
			calls.Add(1)
			select {
			case <-time.After(20 * time.Millisecond):
				return "1", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
	}
}

// Metric points dropped for quarantine get the same quarantine budget as spans
// and log records: their payload uploads run under quarantine.timeout rather
// than offload.timeout from a background context, and building rows stops once
// the budget is spent. Every point that never reaches the quarantine table is
// counted in quarantine_failed_rows.
func TestPushMetricsQuarantineStopsOnceBudgetIsExhausted(t *testing.T) {
	cfg := validConfig()
	cfg.Quarantine.Enabled = true
	cfg.Quarantine.Timeout = 10 * time.Millisecond
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "otel-offload-bucket"
	cfg.Offload.MinBytes = 512
	cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024
	cfg.Limits.MaxAttributeCount = 1

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	e, err := newMetricsExporter(component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: mp}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	e.offloader.SetStore(slowQuarantineStore(&calls))
	enc, err := protoenc.New(schema.MetricsTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	e.enc = enc
	e.writer = &fakeAppender{}
	_, stream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 32})

	// Two attributes against a limit of one reject every point, and the large
	// one makes each point's quarantine payload eligible for upload.
	const count = 32
	md := newTestMetrics(count)
	dps := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints()
	for i := range dps.Len() {
		dps.At(i).Attributes().PutStr("large", strings.Repeat("X", 2048))
		dps.At(i).Attributes().PutStr("b", "2")
	}

	start := time.Now()
	if err := e.pushMetricsData(context.Background(), md); err != nil {
		t.Fatalf("pushMetricsData returned %v, want nil: every point was rejected", err)
	}
	elapsed := time.Since(start)

	if got := calls.Load(); got > 2 {
		t.Errorf("quarantine payload uploads = %d, want <= 2 once the 10ms budget is spent", got)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("pushMetricsData took %v, want it bounded by the 10ms quarantine budget", elapsed)
	}
	if stream.appended != 0 {
		t.Errorf("quarantine appends = %d, want 0 after the budget expired", stream.appended)
	}
	quarantined := metricInt64SumOrZero(t, reader, "otelcol_exporter_bigquery_quarantined_rows")
	failed := metricInt64SumOrZero(t, reader, "otelcol_exporter_bigquery_quarantine_failed_rows")
	if quarantined != 0 || failed != count {
		t.Errorf("quarantined_rows = %d, quarantine_failed_rows = %d, want 0 and %d", quarantined, failed, count)
	}
}

// Spans and log records whose quarantine rows are given up on when the budget
// runs out, whether before their row was built or before it was written, are
// counted in quarantine_failed_rows: it covers every row targeted for
// quarantine that never reached the table.
func TestQuarantineBudgetExpiryCountsEveryUnwrittenRow(t *testing.T) {
	const count = 32
	for _, signal := range []string{"traces", "logs"} {
		t.Run(signal, func(t *testing.T) {
			cfg := validConfig()
			cfg.Quarantine.Enabled = true
			cfg.Quarantine.Timeout = 10 * time.Millisecond
			cfg.Offload.Enabled = true
			cfg.Offload.Bucket = "otel-offload-bucket"
			cfg.Offload.MinBytes = 512
			cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024
			cfg.Limits.MaxAttributeCount = 1

			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
			set := component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: mp}
			var calls atomic.Int32

			var push func() error
			switch signal {
			case "traces":
				e, err := newTracesExporter(set, cfg)
				if err != nil {
					t.Fatal(err)
				}
				e.offloader.SetStore(slowQuarantineStore(&calls))
				e.writer = &fakeAppender{}
				withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: count})
				td := newTestTraces(count)
				spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
				for i := range spans.Len() {
					spans.At(i).Attributes().PutStr("large", strings.Repeat("X", 2048))
					spans.At(i).Attributes().PutStr("b", "2")
				}
				push = func() error { return e.pushTraceData(context.Background(), td) }
			case "logs":
				e, err := newLogsExporter(set, cfg)
				if err != nil {
					t.Fatal(err)
				}
				e.offloader.SetStore(slowQuarantineStore(&calls))
				e.writer = &fakeAppender{}
				withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: count})
				ld := logsWithBodies(make([]string, count)...)
				records := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
				for i := range records.Len() {
					records.At(i).Attributes().PutStr("large", strings.Repeat("X", 2048))
					records.At(i).Attributes().PutStr("b", "2")
				}
				push = func() error { return e.pushLogData(context.Background(), ld) }
			}

			if err := push(); err != nil {
				t.Fatalf("push returned %v, want nil: every record was rejected", err)
			}
			quarantined := metricInt64SumOrZero(t, reader, "otelcol_exporter_bigquery_quarantined_rows")
			failed := metricInt64SumOrZero(t, reader, "otelcol_exporter_bigquery_quarantine_failed_rows")
			if quarantined+failed != count || quarantined != 0 {
				t.Errorf("quarantined_rows = %d, quarantine_failed_rows = %d, want 0 and %d", quarantined, failed, count)
			}
		})
	}
}

func TestPushMetricsPreservesRetryIdentityWhenEarlierOrdinalPeerDroppedByEncodeRows(t *testing.T) {
	// Construct 3 otherwise identical delta-sum points whose metric descriptions differ.
	// Point 0 has a long description that makes its proto-encoded row exceed max_row_bytes.
	// Point 1 and Point 2 fit within max_row_bytes and are sent to the writer.
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	addDeltaSum := func(desc string, val int64) {
		m := sm.Metrics().AppendEmpty()
		m.SetName("test.counter")
		m.SetDescription(desc)
		sum := m.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		sum.SetIsMonotonic(true)
		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		dp.SetIntValue(val)
	}

	// 3 identical delta-sum points (same timestamp, value, metric name, scope, resource).
	// Because metric description is excluded from canonicalMetricPoint, all 3 points share
	// the same fingerprint group and consume ordinals 0, 1, 2.
	addDeltaSum(strings.Repeat("X", 2048), 7) // Point 0: oversized, will be dropped by encodeRows
	addDeltaSum("short", 7)                   // Point 1: kept in request 0 (fails)
	addDeltaSum("short", 7)                   // Point 2: kept in request 1 (acknowledged)

	// Verify original row fingerprints: ordinals 0, 1, 2
	rows, rejects := transform.MetricRows(md, transform.Options{Limits: transform.DefaultLimits()})
	if len(rejects) != 0 || len(rows) != 3 {
		t.Fatalf("MetricRows got %d rows, %d rejects; want 3 rows, 0 rejects", len(rows), len(rejects))
	}
	fp0 := rows[0]["point_fingerprint"].(string)
	fp1 := rows[1]["point_fingerprint"].(string)
	fp2 := rows[2]["point_fingerprint"].(string)
	if fp0 == fp1 || fp1 == fp2 || fp0 == fp2 {
		t.Fatalf("expected distinct ordinals/fingerprints; got fps: %s, %s, %s", fp0, fp1, fp2)
	}

	cfg := validConfig()
	cfg.Write.MaxRowBytes = 1024 // point 0 exceeds 1024, points 1 and 2 fit
	tel, err := metadata.NewTelemetry(noop.NewMeterProvider())
	if err != nil {
		t.Fatal(err)
	}
	enc, err := protoenc.New(schema.MetricsTableSchema())
	if err != nil {
		t.Fatal(err)
	}

	// Configure fakeAppender:
	// Points 1 and 2 are passed to Append (2 rows).
	// Request for row 0 (Point 1) fails with retryable error.
	// Request for row 1 (Point 2) is acknowledged: AckedRanges = [][2]int{{1, 2}}, AcknowledgedRows = 1.
	tempErr := status.Error(codes.Unavailable, "request 0 temporary error")
	app := &fakeAppender{
		outcome: bqi.AppendOutcome{
			AcknowledgedRows: 1,
			AckedRanges:      [][2]int{{1, 2}},
			Verdict:          bqi.Classify(tempErr),
			Err:              tempErr,
		},
	}

	se := &signalExporter{
		cfg:    cfg,
		table:  cfg.Metrics.Table,
		logger: zap.NewNop(),
		tel:    tel,
		writer: app,
		enc:    enc,
		schema: schema.MetricsTableSchema(),
		opts:   transform.Options{Limits: cfg.Limits.toTransform()},
	}
	e := &metricsExporter{se}

	pushErr := e.pushMetricsData(context.Background(), md)
	if pushErr == nil || consumererror.IsPermanent(pushErr) {
		t.Fatalf("pushMetricsData returned %v, want retryable error", pushErr)
	}
	var mdErr consumererror.Metrics
	if !errors.As(pushErr, &mdErr) {
		t.Fatalf("error %T does not carry pmetric.Metrics", pushErr)
	}

	// On retry, the payload must include Point 0 so that Point 1 reproduces its original ordinal 1 and fingerprint!
	retryRows, _ := transform.MetricRows(mdErr.Data(), e.opts)
	if len(retryRows) != 3 {
		t.Fatalf("expected 3 retry rows (peer 0, point 1, point 2), got %d", len(retryRows))
	}
	if retryRows[0]["point_fingerprint"].(string) != fp0 {
		t.Errorf("retried point 0 fingerprint = %s, want %s", retryRows[0]["point_fingerprint"], fp0)
	}
	if retryRows[1]["point_fingerprint"].(string) != fp1 {
		t.Errorf("retried point 1 fingerprint = %s, want %s (original ordinal 1 preserved)",
			retryRows[1]["point_fingerprint"], fp1)
	}
	if retryRows[2]["point_fingerprint"].(string) != fp2 {
		t.Errorf("retried point 2 fingerprint = %s, want %s (original ordinal 2 preserved)",
			retryRows[2]["point_fingerprint"], fp2)
	}

	// When pushMetricsData is retried with mdErr.Data(), point 0 is dropped by encodeRows again,
	// and point 1 is sent with its original identity to BigQuery.
	var secondPushRows [][]byte
	app.appendFn = func(_ context.Context, rows [][]byte) bqi.AppendOutcome {
		secondPushRows = rows
		return bqi.AppendOutcome{AcknowledgedRows: len(rows)}
	}
	if err := e.pushMetricsData(context.Background(), mdErr.Data()); err != nil {
		t.Fatalf("retry pushMetricsData failed: %v", err)
	}
	if len(secondPushRows) != 2 {
		t.Fatalf("second push sent %d rows, want 2 (points 1 and 2; point 0 dropped by encoder)", len(secondPushRows))
	}
}

func TestPushDeduplicatesDuplicateRowErrorIndicesForSameRecord(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	tel, err := metadata.NewTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}

	e, _ := newTestTracesExporter(t, bqi.AppendOutcome{
		RowErrors: []bqi.RowError{
			{Index: 0, Code: "FIELDS_ERROR", Message: "field a invalid"},
			{Index: 0, Code: "FIELDS_ERROR", Message: "field b invalid"},
		},
	})
	e.tel = tel
	quarantined, qStream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 1})

	err = e.pushTraceData(context.Background(), newTestTraces(2))
	if err == nil || consumererror.IsPermanent(err) {
		t.Fatalf("pushTraceData error = %v, want retryable subset error for span 1", err)
	}
	if qStream.gotRows != 1 || len(quarantined.rows) != 1 {
		t.Fatalf("quarantined rows = %d (stream got %d), want 1 (duplicate RowError index 0 deduplicated)", len(quarantined.rows), qStream.gotRows)
	}
	if got := metricInt64SumOrZero(t, reader, "otelcol_exporter_bigquery_rejected_rows"); got != 1 {
		t.Errorf("rejected_rows = %d, want 1", got)
	}
}

func TestPushStreamRecreateVerdictReturnsRetryableWithoutQuarantining(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	tel, err := metadata.NewTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}

	st, err := status.New(codes.InvalidArgument, "schema mismatch").WithDetails(&storagepb.StorageError{
		Code:   storagepb.StorageError_SCHEMA_MISMATCH_EXTRA_FIELDS,
		Entity: "projects/my-project/datasets/otel/tables/otel_spans/streams/_default",
	})
	if err != nil {
		t.Fatal(err)
	}
	recreateErr := st.Err()

	e, _ := newTestTracesExporter(t, bqi.AppendOutcome{
		Verdict: bqi.Classify(recreateErr),
		Err:     recreateErr,
	})
	e.tel = tel
	host := &fakeStatusHost{}
	e.host = host
	quarantined, qStream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{})

	pushErr := e.pushTraceData(context.Background(), newTestTraces(2))
	if pushErr == nil || consumererror.IsPermanent(pushErr) {
		t.Fatalf("pushTraceData error = %v, want retryable error when StreamRecreate is true", pushErr)
	}
	var traceErr consumererror.Traces
	if !errors.As(pushErr, &traceErr) || traceErr.Data().SpanCount() != 2 {
		t.Fatalf("retryable spans = %v, want consumererror.Traces carrying 2 spans", pushErr)
	}
	if qStream.appended != 0 || len(quarantined.rows) != 0 {
		t.Errorf("quarantined rows = %d, want 0 on stream recreation", len(quarantined.rows))
	}
	if got := metricInt64SumOrZero(t, reader, "otelcol_exporter_bigquery_retries"); got != 1 {
		t.Errorf("retries = %d, want 1", got)
	}
	if got := metricInt64SumOrZero(t, reader, "otelcol_exporter_bigquery_rejected_rows"); got != 0 {
		t.Errorf("rejected_rows = %d, want 0", got)
	}
	if len(host.events) != 1 || host.events[0].Status() != componentstatus.StatusRecoverableError {
		t.Errorf("reported statuses = %v, want [StatusRecoverableError]", host.events)
	}
}

func TestPushPermanentRequestLevelFailureQuarantinesAndRecordsRejected(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	tel, err := metadata.NewTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}

	permErr := status.Error(codes.PermissionDenied, "iam permission denied on table")
	e, _ := newTestTracesExporter(t, bqi.AppendOutcome{
		Verdict: bqi.Classify(permErr),
		Err:     permErr,
	})
	e.tel = tel
	quarantined, qStream := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{
		AcknowledgedRows: 2,
		AckedRanges:      [][2]int{{0, 2}},
	})

	pushErr := e.pushTraceData(context.Background(), newTestTraces(2))
	if !consumererror.IsPermanent(pushErr) {
		t.Fatalf("pushTraceData error = %v, want permanent error", pushErr)
	}
	if qStream.gotRows != 2 || len(quarantined.rows) != 2 {
		t.Fatalf("quarantined rows = %d (stream got %d), want 2", len(quarantined.rows), qStream.gotRows)
	}
	for i, row := range quarantined.rows {
		if row["error_code"] != "REQUEST_PERMANENT" {
			t.Errorf("quarantined.rows[%d][\"error_code\"] = %v, want REQUEST_PERMANENT", i, row["error_code"])
		}
		if msg, _ := row["error_message"].(string); !strings.Contains(msg, "request_permanent: permission_denied:") {
			t.Errorf("quarantined.rows[%d][\"error_message\"] = %q, want prefix \"request_permanent: permission_denied:\"", i, msg)
		}
	}
	if got := metricInt64ByAttr(t, reader, "otelcol_exporter_bigquery_rejected_rows", "reason")["request_permanent"]; got != 2 {
		t.Errorf("rejected_rows{reason=request_permanent} = %d, want 2", got)
	}
	if got := metricInt64ByAttr(t, reader, "otelcol_exporter_bigquery_quarantined_rows", "reason")["request_permanent"]; got != 2 {
		t.Errorf("quarantined_rows{reason=request_permanent} = %d, want 2", got)
	}
}

func TestNewSignalExporterWiresRedactorPerSignal(t *testing.T) {
	cfg := validConfig()
	cfg.Redaction.Enabled = true

	traces, err := newTracesExporter(component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: noop.NewMeterProvider()}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	logs, err := newLogsExporter(component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: noop.NewMeterProvider()}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := newMetricsExporter(component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: noop.NewMeterProvider()}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for name, e := range map[string]*signalExporter{"traces": traces.signalExporter, "logs": logs.signalExporter, "metrics": metrics.signalExporter} {
		if e.opts.Redactor == nil {
			t.Errorf("%s: Redactor not wired when redaction.enabled is set", name)
		}
		if e.startErr != nil {
			t.Errorf("%s: unexpected startErr %v", name, e.startErr)
		}
	}
	if got := signalForSchema(schema.SpansTableSchema()); got != schema.SignalSpans {
		t.Errorf("signalForSchema(spans) = %q", got)
	}
	if got := signalForSchema(schema.LogsTableSchema()); got != schema.SignalLogs {
		t.Errorf("signalForSchema(logs) = %q", got)
	}
	if got := signalForSchema(schema.MetricsTableSchema()); got != schema.SignalMetrics {
		t.Errorf("signalForSchema(metrics) = %q", got)
	}

	off := validConfig()
	e, err := newTracesExporter(component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: noop.NewMeterProvider()}, off)
	if err != nil {
		t.Fatal(err)
	}
	if e.opts.Redactor != nil {
		t.Fatal("Redactor must stay nil when redaction is disabled")
	}
}

// TestPushTracesRedactsBeforeOffloadEndToEnd runs the real offloader against
// an in-memory blob store: the stored object must be the redacted bytes, its
// key must be their digest, the appended row must carry no cleartext, and
// the redacted_values counter must reflect the batch.
func TestPushTracesRedactsBeforeOffloadEndToEnd(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	cfg := validConfig()
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "otel-offload-bucket"
	cfg.Offload.MinBytes = 1024
	cfg.Offload.MaxObjectBytes = 64 * 1024 * 1024
	cfg.Redaction.Enabled = true
	cfg.Redaction.BlockedValuePatterns = []string{`sk-live-[0-9a-f]+`}

	e, err := newTracesExporter(component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: mp}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	memStore := offload.NewMemoryBlobStore()
	e.offloader.SetStore(memStore)

	enc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingEncoder{next: enc}
	app := &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 1}}
	e.enc = rec
	e.writer = app

	const secret = "sk-live-0123456789abcdef0123456789abcdef"
	prompt := strings.Repeat("context ", 200) + secret + strings.Repeat(" more", 100)
	td := newTestTraces(1)
	span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	span.Attributes().PutStr("gen_ai.prompt", prompt)
	span.Attributes().PutStr("authorization", "Bearer "+strings.Repeat("t", 2048)) // oversized, but key-redacted first
	before, _ := (&ptrace.JSONMarshaler{}).MarshalTraces(td)

	if err := e.pushTraceData(context.Background(), td); err != nil {
		t.Fatalf("pushTraceData: %v", err)
	}
	if after, _ := (&ptrace.JSONMarshaler{}).MarshalTraces(td); string(after) != string(before) {
		t.Fatal("pushTraceData mutated the caller's batch")
	}
	if app.gotRows != 1 || len(rec.rows) != 1 {
		t.Fatalf("appended %d rows (encoded %d), want 1", app.gotRows, len(rec.rows))
	}
	if memStore.Count() != 1 {
		t.Fatalf("blob store holds %d objects, want 1 (the authorization value must shrink below min_bytes)", memStore.Count())
	}

	wantData := strings.ReplaceAll(prompt, secret, "[REDACTED]")
	wantSum := sha256.Sum256([]byte(wantData))
	wantHex := hex.EncodeToString(wantSum[:])
	refs, _ := rec.rows[0]["payload_refs"].([]transform.Row)
	if len(refs) != 1 {
		t.Fatalf("payload_refs = %#v, want 1", rec.rows[0]["payload_refs"])
	}
	objRef, _ := refs[0]["object_ref"].(transform.Row)
	uri, _ := objRef["uri"].(string)
	if !strings.Contains(uri, wantHex) {
		t.Fatalf("object uri %q does not carry the digest of the redacted bytes %s", uri, wantHex)
	}
	object := strings.TrimPrefix(uri, "gs://"+cfg.Offload.Bucket+"/")
	stored, ok := memStore.Get(cfg.Offload.Bucket, object)
	if !ok {
		t.Fatalf("object %q not found in the blob store", object)
	}
	if strings.Contains(string(stored.Data), secret) || !strings.Contains(string(stored.Data), "[REDACTED]") {
		t.Fatalf("stored object leaks the secret or lacks the mask")
	}
	attrs, _ := rec.rows[0]["attributes"].(string)
	if strings.Contains(attrs, secret) || strings.Contains(attrs, "Bearer") || !strings.Contains(attrs, `"authorization":"[REDACTED]"`) {
		t.Fatalf("attributes column leaks cleartext: %s", attrs)
	}

	keyCount := metricInt64SumWithAttr(t, reader, "otelcol_exporter_bigquery_redacted_values", metadata.AttrMatchType, "key")
	valueCount := metricInt64SumWithAttr(t, reader, "otelcol_exporter_bigquery_redacted_values", metadata.AttrMatchType, "value")
	if keyCount != 1 || valueCount != 1 {
		t.Fatalf("redacted_values key=%d value=%d, want 1 and 1", keyCount, valueCount)
	}
	if sig := metricInt64SumWithAttr(t, reader, "otelcol_exporter_bigquery_redacted_values", metadata.AttrSignal, schema.SignalSpans); sig != 2 {
		t.Fatalf("redacted_values{signal=spans} = %d, want 2", sig)
	}
}

// TestPushTracesQuarantineRowsAreRedacted drives a rejected span through the
// exporter's quarantine path and checks the DLQ row content.
func TestPushTracesQuarantineRowsAreRedacted(t *testing.T) {
	cfg := validConfig()
	cfg.Redaction.Enabled = true
	e, err := newTracesExporter(component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: noop.NewMeterProvider()}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	e.enc = enc
	e.writer = &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 1}}
	quarantined, _ := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 1})

	td := newTestTraces(2)
	bad := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(1)
	bad.SetEndTimestamp(0)
	bad.Attributes().PutStr("password", "hunter2")
	bad.Attributes().PutStr("x-api-key", "AKIA-not-really")

	if err := e.pushTraceData(context.Background(), td); err != nil {
		t.Fatalf("pushTraceData: %v", err)
	}
	if len(quarantined.rows) != 1 {
		t.Fatalf("quarantine rows = %d, want 1", len(quarantined.rows))
	}
	row := quarantined.rows[0]
	payload, _ := row["raw_payload"].(string)
	attrs, _ := row["raw_attributes"].(string)
	for _, leak := range []string{"hunter2", "AKIA-not-really"} {
		if strings.Contains(payload, leak) || strings.Contains(attrs, leak) {
			t.Fatalf("quarantine row leaks %q: payload=%s attrs=%s", leak, payload, attrs)
		}
	}
	if !strings.Contains(payload, "[REDACTED]") || !strings.Contains(attrs, "[REDACTED]") {
		t.Fatalf("quarantine row is missing the mask: payload=%s attrs=%s", payload, attrs)
	}
}

func TestStartFailsClosedWhenRedactorCannotBeBuilt(t *testing.T) {
	cfg := validConfig()
	e := newSignalExporter(component.TelemetrySettings{Logger: zap.NewNop()}, cfg, cfg.Traces.Table, schema.SpansTableSchema())
	e.startErr = errors.New("building redactor: synthetic")
	err := e.start(context.Background(), componenttest.NewNopHost())
	if err == nil || !strings.Contains(err.Error(), "building redactor") {
		t.Fatalf("start error = %v, want the construction error", err)
	}
	if e.client != nil {
		t.Fatal("start must not open a client after a construction failure")
	}
}

// metricInt64SumWithAttr sums the data points of an int64 counter whose
// attribute key equals value.
func metricInt64SumWithAttr(t *testing.T, reader *sdkmetric.ManualReader, name, key, value string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want Sum[int64]", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key(key)); ok && v.AsString() == value {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func TestPermanentOffloadErrorRoutesToQuarantineWithoutRetry(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	cfg := validConfig()
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "offload-bucket"
	cfg.Offload.MinBytes = 64
	cfg.Offload.MaxConcurrency = 1
	cfg.Quarantine.Enabled = true

	e, err := newTracesExporter(component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: mp}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var callNum atomic.Int32
	e.offloader.SetStore(&funcBlobStore{
		writeFn: func(_ context.Context, _, object, _ string, _ []byte) (string, error) {
			n := callNum.Add(1)
			// First span offload succeeds; second span offload fails permanently with HTTP 403;
			// quarantine offload for the second span also returns HTTP 403 and falls back to inline raw_payload.
			if n == 1 && !strings.Contains(object, "quarantine/") {
				return "gen-1", nil
			}
			return "", &offload.GCSHTTPError{
				StatusCode: 403,
				Status:     "403 Forbidden",
				Body:       "storage.objects.create access denied",
			}
		},
	})
	enc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	e.enc = enc
	primary := &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 1}}
	e.writer = primary
	quarantined, _ := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 1})

	td := newTestTraces(2)
	spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	spans.At(0).Attributes().PutStr("large", strings.Repeat("A", 256))
	spans.At(1).Attributes().PutStr("large", strings.Repeat("B", 256))

	if err := e.pushTraceData(context.Background(), td); err != nil {
		t.Fatalf("pushTraceData returned %v, want nil (permanent offload error must quarantine rather than retry)", err)
	}
	if primary.appended != 1 {
		t.Fatalf("primary appends = %d, want 1", primary.appended)
	}
	if len(quarantined.rows) != 1 {
		t.Fatalf("quarantine rows = %d, want 1", len(quarantined.rows))
	}
	if got := quarantined.rows[0]["error_code"]; got != "OFFLOAD_PERMANENT" {
		t.Errorf("quarantine error_code = %v, want OFFLOAD_PERMANENT", got)
	}
	if got := metricInt64SumWithAttr(t, reader, "otelcol_exporter_bigquery_rejected_rows", "reason", "offload_permanent"); got != 1 {
		t.Errorf("rejected_rows{reason=offload_permanent} = %d, want 1", got)
	}
	if got := metricInt64SumWithAttr(t, reader, "otelcol_exporter_bigquery_offload_errors", "class", "http_403"); got < 1 {
		t.Errorf("offload_errors{class=http_403} = %d, want >= 1", got)
	}
	if got := metricInt64SumWithAttr(t, reader, "otelcol_exporter_bigquery_quarantined_rows", "reason", "offload_permanent"); got != 1 {
		t.Errorf("quarantined_rows{reason=offload_permanent} = %d, want 1", got)
	}
}

func TestQuarantineOversizedPayloadsOffloadConcurrently(t *testing.T) {
	cfg := validConfig()
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "offload-bucket"
	cfg.Offload.MinBytes = 64
	cfg.Offload.MaxConcurrency = 4
	cfg.Quarantine.Enabled = true
	cfg.Quarantine.MaxRawPayloadBytes = 512

	e, err := newTracesExporter(component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: noop.NewMeterProvider()}, cfg)
	if err != nil {
		t.Fatal(err)
	}

	var inFlight, maxInFlight atomic.Int32
	e.offloader.SetStore(&funcBlobStore{
		writeFn: func(_ context.Context, _, _, _ string, _ []byte) (string, error) {
			cur := inFlight.Add(1)
			for {
				prev := maxInFlight.Load()
				if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(25 * time.Millisecond)
			inFlight.Add(-1)
			return "gen-q", nil
		},
	})
	enc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	e.enc = enc
	e.writer = &fakeAppender{}
	quarantined, _ := withQuarantine(t, e.signalExporter, bqi.AppendOutcome{AcknowledgedRows: 4})

	td := newTestTraces(4)
	spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	for i := range 4 {
		spans.At(i).SetEndTimestamp(0)
		spans.At(i).Attributes().PutStr("payload", strings.Repeat(string(rune('A'+i)), 1024))
	}

	if err := e.pushTraceData(context.Background(), td); err != nil {
		t.Fatalf("pushTraceData: %v", err)
	}
	if peak := maxInFlight.Load(); peak < 2 {
		t.Errorf("peak concurrent quarantine offload uploads = %d, want >= 2", peak)
	}
	if len(quarantined.rows) != 4 {
		t.Fatalf("quarantine rows = %d, want 4", len(quarantined.rows))
	}
	for i, row := range quarantined.rows {
		if row["payload_ref"] == nil {
			t.Errorf("quarantine row %d missing payload_ref", i)
		}
	}
}

func TestOffloadDedupHitsSelfTelemetry(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	cfg := validConfig()
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = "offload-bucket"
	cfg.Offload.MinBytes = 64
	cfg.Offload.CacheSize = 16

	e, err := newTracesExporter(component.TelemetrySettings{Logger: zap.NewNop(), MeterProvider: mp}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.offloader.SetStore(offload.NewMemoryBlobStore())
	enc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	e.enc = enc
	e.writer = &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 2}}

	sharedPayload := strings.Repeat("D", 512)
	td1 := newTestTraces(2)
	spans1 := td1.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	spans1.At(0).Attributes().PutStr("blob", sharedPayload)
	spans1.At(1).Attributes().PutStr("blob", sharedPayload)
	if err := e.pushTraceData(context.Background(), td1); err != nil {
		t.Fatal(err)
	}

	e.writer = &fakeAppender{outcome: bqi.AppendOutcome{AcknowledgedRows: 1}}
	td2 := newTestTraces(1)
	td2.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutStr("blob", sharedPayload)
	if err := e.pushTraceData(context.Background(), td2); err != nil {
		t.Fatal(err)
	}

	if got := metricInt64SumOrZero(t, reader, "otelcol_exporter_bigquery_offloaded_payloads"); got != 1 {
		t.Errorf("offloaded_payloads = %d, want 1", got)
	}
	if got := metricInt64SumWithAttr(t, reader, "otelcol_exporter_bigquery_offload_dedup_hits", "signal", "spans"); got != 2 {
		t.Errorf("offload_dedup_hits{signal=spans} = %d, want 2 (1 intra-batch + 1 cross-batch)", got)
	}
}

func TestShutdownFlushWithReducedWindow(t *testing.T) {
	tests := []struct {
		name              string
		backpressure      bool
		maxInflight       int
		retriesEnabled    bool
		stepWindowToFloor bool
	}{
		{
			name:           "backpressure_off",
			backpressure:   false,
			maxInflight:    8,
			retriesEnabled: true,
		},
		{
			name:           "backpressure_off_static_window_1",
			backpressure:   false,
			maxInflight:    1,
			retriesEnabled: true,
		},
		{
			name:              "backpressure_on_window_reduced",
			backpressure:      true,
			maxInflight:       8,
			retriesEnabled:    true,
			stepWindowToFloor: true,
		},
		{
			name:              "backpressure_on_window_reduced_retries_disabled",
			backpressure:      true,
			maxInflight:       8,
			retriesEnabled:    false,
			stepWindowToFloor: true,
		},
	}

	makeOneSpanBatch := func(seq int) ptrace.Traces {
		now := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "shutdown-flush-test")
		sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, byte(seq + 1)})
		sp.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, byte(seq + 1)})
		sp.SetName(fmt.Sprintf("span-%d", seq))
		sp.SetKind(ptrace.SpanKindInternal)
		sp.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
		sp.SetEndTimestamp(pcommon.NewTimestampFromTime(now.Add(5 * time.Millisecond)))
		return td
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := bqfake.NewServer(t)
			ctx := context.Background()

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig().(*Config)
			cfg.Project = "my-project"
			cfg.Dataset = "otel"
			cfg.Endpoint.URL = srv.Addr()
			cfg.Endpoint.Insecure = true
			cfg.Endpoint.WithoutAuthentication = true
			cfg.TimeoutSettings.Timeout = 5 * time.Second
			cfg.Write.MaxInflightRequests = tc.maxInflight
			cfg.QueueSettings.Get().NumConsumers = 8
			cfg.QueueSettings.Get().QueueSize = 1000
			cfg.BackOffConfig.Enabled = tc.retriesEnabled
			cfg.BackOffConfig.InitialInterval = 10 * time.Millisecond
			cfg.BackOffConfig.MaxInterval = 50 * time.Millisecond
			cfg.Write.Backpressure.Enabled = tc.backpressure
			cfg.Write.Backpressure.MinInflightRequests = 1
			cfg.Write.Backpressure.DecreaseFactor = 0.5
			cfg.Write.Backpressure.RecoveryInterval = time.Minute
			cfg.Write.Backpressure.LatencyThreshold = 2 * time.Second
			// backpressure_level reports the larger of the byte and in-flight
			// pressures. A byte floor below the 1 MiB threshold that three
			// step-downs reach keeps the byte pressure under 1, so the gauge
			// reads 1 exactly when EffectiveInflightRequests() is at its floor
			// of 1. The byte threshold itself is the same 1 MiB as with the
			// default floor.
			cfg.Write.Backpressure.MinRequestBytes = 256 * 1024

			if err := cfg.Validate(); err != nil {
				t.Fatalf("cfg.Validate: %v", err)
			}

			reader := sdkmetric.NewManualReader()
			set := exportertest.NewNopSettings(factory.Type())
			set.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			exp, err := factory.CreateTraces(ctx, set, cfg)
			if err != nil {
				t.Fatalf("CreateTraces: %v", err)
			}
			if err := exp.Start(ctx, componenttest.NewNopHost()); err != nil {
				t.Fatalf("Start: %v", err)
			}

			if tc.stepWindowToFloor {
				// Step the adaptive window down 8 -> 4 -> 2 -> 1 using three
				// quota faults spaced past the 250ms throttle debounce.
				for step := range 3 {
					_, beforeReqs, _ := srv.Counts()
					srv.EnqueueFault(bqfake.Fault{
						Mode:  bqfake.FaultResourceExhausted,
						Table: "otel_spans",
					})
					_ = exp.ConsumeTraces(ctx, makeOneSpanBatch(100+step))
					waitDeadline := time.Now().Add(2 * time.Second)
					for {
						_, reqs, _ := srv.Counts()
						if reqs > beforeReqs {
							break
						}
						if time.Now().After(waitDeadline) {
							t.Fatalf("step %d: timed out waiting for quota fault request", step)
						}
						time.Sleep(5 * time.Millisecond)
					}
					if tc.retriesEnabled {
						// Wait for the retry of the step-down span to flush before next step.
						for srv.RowCount("otel_spans") < step+1 && time.Now().Before(waitDeadline) {
							time.Sleep(5 * time.Millisecond)
						}
					}
					time.Sleep(275 * time.Millisecond)
				}
				srv.ResetRows()
			}

			const totalSpans = 48
			faults := make([]bqfake.Fault, totalSpans)
			for i := range faults {
				faults[i] = bqfake.Fault{
					Mode:  bqfake.FaultDelayedAck,
					Table: "otel_spans",
					Delay: 20 * time.Millisecond,
				}
			}
			srv.EnqueueFault(faults...)

			for i := range totalSpans {
				if err := exp.ConsumeTraces(ctx, makeOneSpanBatch(i)); err != nil {
					t.Fatalf("ConsumeTraces(%d): %v", i, err)
				}
			}

			if tc.stepWindowToFloor {
				// Recovery needs a clean acknowledgement a minute after the
				// last step-down, so the window is still at its floor while
				// the queue drains.
				if got := metricFloat64Gauge(t, reader, "otelcol_exporter_bigquery_backpressure_level"); got != 1 {
					t.Fatalf("backpressure_level before Shutdown = %v, want 1 (EffectiveInflightRequests() at its floor of 1)", got)
				}
			}

			// A generous budget: under CPU starvation 48 delayed acks through a
			// window of 1 take several times their nominal second.
			shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if err := exp.Shutdown(shutdownCtx); err != nil {
				t.Fatalf("Shutdown: %v", err)
			}

			if got := srv.RowCount("otel_spans"); got != totalSpans {
				t.Fatalf("flushed spans at Shutdown = %d, want %d (all queued spans must drain without premature shedding)", got, totalSpans)
			}
		})
	}
}

func TestSlowAckThenFastServiceRowConservation(t *testing.T) {
	makeSpanBatch := func(startSeq, count int, padBytes int) ptrace.Traces {
		now := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "slow-ack-conservation-test")
		spans := rs.ScopeSpans().AppendEmpty().Spans()
		pad := ""
		if padBytes > 0 {
			pad = strings.Repeat("x", padBytes)
		}
		for i := range count {
			seq := startSeq + i
			sp := spans.AppendEmpty()
			sp.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, byte(seq >> 8), byte(seq + 1)})
			sp.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, byte(seq >> 8), byte(seq + 1)})
			sp.SetName(fmt.Sprintf("span-%d", seq))
			sp.SetKind(ptrace.SpanKindInternal)
			sp.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
			sp.SetEndTimestamp(pcommon.NewTimestampFromTime(now.Add(5 * time.Millisecond)))
			if pad != "" {
				sp.Attributes().PutStr("payload.pad", pad)
			}
		}
		return td
	}

	waitForRows := func(t *testing.T, srv *bqfake.Server, want int, timeout time.Duration) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for srv.RowCount("otel_spans") < want {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %d rows in otel_spans; got %d", want, srv.RowCount("otel_spans"))
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Tripwire for the removed early shed, which failed batches with
	// error_code=backpressure before their timeout expired. No code produces
	// that label any more, so it is matched as a literal.
	const removedShedLabel = "backpressure"
	assertZeroBackpressureLogs := func(t *testing.T, logs *observer.ObservedLogs) {
		t.Helper()
		var bpErrors int
		for _, entry := range logs.FilterMessage("append failed").All() {
			if entry.ContextMap()["error_code"] == removedShedLabel {
				bpErrors++
			}
		}
		if bpErrors != 0 {
			t.Fatalf("captured %d append failed log entries with error_code=%s, want 0", bpErrors, removedShedLabel)
		}
	}

	t.Run("slow_ack_warmup_48_spans_20ms", func(t *testing.T) {
		for _, retriesEnabled := range []bool{true, false} {
			name := "retries_enabled_shutdown"
			if !retriesEnabled {
				name = "retries_disabled_running_queue"
			}
			t.Run(name, func(t *testing.T) {
				srv := bqfake.NewServer(t)
				ctx := context.Background()
				factory := NewFactory()
				cfg := factory.CreateDefaultConfig().(*Config)
				cfg.Project = "my-project"
				cfg.Dataset = "otel"
				cfg.Endpoint.URL = srv.Addr()
				cfg.Endpoint.Insecure = true
				cfg.Endpoint.WithoutAuthentication = true
				cfg.TimeoutSettings.Timeout = 3 * time.Second
				cfg.Write.MaxInflightRequests = 2
				cfg.QueueSettings.Get().NumConsumers = 8
				cfg.QueueSettings.Get().QueueSize = 1000
				cfg.BackOffConfig.Enabled = retriesEnabled
				cfg.BackOffConfig.InitialInterval = 10 * time.Millisecond
				cfg.BackOffConfig.MaxInterval = 50 * time.Millisecond
				cfg.Write.Backpressure.Enabled = true
				cfg.Write.Backpressure.MinInflightRequests = 1
				cfg.Write.Backpressure.DecreaseFactor = 0.5
				cfg.Write.Backpressure.RecoveryInterval = time.Minute
				cfg.Write.Backpressure.LatencyThreshold = 500 * time.Millisecond

				if err := cfg.Validate(); err != nil {
					t.Fatalf("cfg.Validate: %v", err)
				}
				set := exportertest.NewNopSettings(factory.Type())
				core, logs := observer.New(zap.DebugLevel)
				set.Logger = zap.New(core)

				exp, err := factory.CreateTraces(ctx, set, cfg)
				if err != nil {
					t.Fatalf("CreateTraces: %v", err)
				}
				if err := exp.Start(ctx, componenttest.NewNopHost()); err != nil {
					t.Fatalf("Start: %v", err)
				}
				var shutDown bool
				t.Cleanup(func() {
					if shutDown {
						return
					}
					cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cCancel()
					_ = exp.Shutdown(cCtx)
				})

				// The 2.2s warmup ack exceeds the 500ms latency threshold and
				// steps the window down. The 48 spans that follow, acknowledged
				// in 20ms, must all be stored within the 3s attempt timeout.
				srv.EnqueueFault(bqfake.Fault{
					Mode:  bqfake.FaultDelayedAck,
					Table: "otel_spans",
					Delay: 2200 * time.Millisecond,
				})
				if err := exp.ConsumeTraces(ctx, makeSpanBatch(900, 1, 0)); err != nil {
					t.Fatalf("warmup ConsumeTraces: %v", err)
				}
				waitForRows(t, srv, 1, 5*time.Second)
				time.Sleep(20 * time.Millisecond)
				srv.ResetRows()

				const totalSpans = 48
				faults := make([]bqfake.Fault, totalSpans)
				for i := range faults {
					faults[i] = bqfake.Fault{
						Mode:  bqfake.FaultDelayedAck,
						Table: "otel_spans",
						Delay: 20 * time.Millisecond,
					}
				}
				srv.EnqueueFault(faults...)

				for i := range totalSpans {
					if err := exp.ConsumeTraces(ctx, makeSpanBatch(i, 1, 0)); err != nil {
						t.Fatalf("ConsumeTraces(%d): %v", i, err)
					}
				}

				if !retriesEnabled {
					waitForRows(t, srv, totalSpans, 5*time.Second)
				}

				shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				shutDown = true
				if err := exp.Shutdown(shutdownCtx); err != nil {
					t.Fatalf("Shutdown: %v", err)
				}

				if got := srv.RowCount("otel_spans"); got != totalSpans {
					t.Fatalf("stored spans = %d, want %d", got, totalSpans)
				}
				assertZeroBackpressureLogs(t, logs)
			})
		}
	})

	t.Run("slow_ack_3400ms_warmup_100_span_split_batch", func(t *testing.T) {
		for _, retriesEnabled := range []bool{false, true} {
			name := "retries_disabled"
			if retriesEnabled {
				name = "default_retries_shutdown"
			}
			t.Run(name, func(t *testing.T) {
				srv := bqfake.NewServer(t)
				ctx := context.Background()
				factory := NewFactory()
				cfg := factory.CreateDefaultConfig().(*Config)
				cfg.Project = "my-project"
				cfg.Dataset = "otel"
				cfg.Endpoint.URL = srv.Addr()
				cfg.Endpoint.Insecure = true
				cfg.Endpoint.WithoutAuthentication = true
				cfg.Write.MaxRequestBytes = 16 * 1024
				cfg.Write.MaxRowBytes = 8 * 1024
				cfg.BackOffConfig.Enabled = retriesEnabled
				cfg.Write.Backpressure.Enabled = true
				cfg.Write.Backpressure.MinRequestBytes = 4 * 1024

				if err := cfg.Validate(); err != nil {
					t.Fatalf("cfg.Validate: %v", err)
				}
				set := exportertest.NewNopSettings(factory.Type())
				core, logs := observer.New(zap.DebugLevel)
				set.Logger = zap.New(core)

				exp, err := factory.CreateTraces(ctx, set, cfg)
				if err != nil {
					t.Fatalf("CreateTraces: %v", err)
				}
				if err := exp.Start(ctx, componenttest.NewNopHost()); err != nil {
					t.Fatalf("Start: %v", err)
				}
				var shutDown bool
				t.Cleanup(func() {
					if shutDown {
						return
					}
					cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cCancel()
					_ = exp.Shutdown(cCtx)
				})

				srv.EnqueueFault(bqfake.Fault{
					Mode:  bqfake.FaultDelayedAck,
					Table: "otel_spans",
					Delay: 3400 * time.Millisecond,
				})
				if err := exp.ConsumeTraces(ctx, makeSpanBatch(0, 1, 0)); err != nil {
					t.Fatalf("warmup ConsumeTraces: %v", err)
				}
				waitForRows(t, srv, 1, 6*time.Second)

				if err := exp.ConsumeTraces(ctx, makeSpanBatch(1000, 100, 300)); err != nil {
					t.Fatalf("batch ConsumeTraces: %v", err)
				}

				if !retriesEnabled {
					waitForRows(t, srv, 101, 5*time.Second)
				}

				shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				shutDown = true
				if err := exp.Shutdown(shutdownCtx); err != nil {
					t.Fatalf("Shutdown: %v", err)
				}

				if got := srv.RowCount("otel_spans"); got != 101 {
					t.Fatalf("stored spans = %d, want 101", got)
				}
				assertZeroBackpressureLogs(t, logs)
			})
		}
	})

	t.Run("slow_ack_3400ms_warmup_eight_1_span_batches", func(t *testing.T) {
		for _, retriesEnabled := range []bool{false, true} {
			name := "retries_disabled"
			if retriesEnabled {
				name = "default_retries_shutdown"
			}
			t.Run(name, func(t *testing.T) {
				srv := bqfake.NewServer(t)
				ctx := context.Background()
				factory := NewFactory()
				cfg := factory.CreateDefaultConfig().(*Config)
				cfg.Project = "my-project"
				cfg.Dataset = "otel"
				cfg.Endpoint.URL = srv.Addr()
				cfg.Endpoint.Insecure = true
				cfg.Endpoint.WithoutAuthentication = true
				cfg.BackOffConfig.Enabled = retriesEnabled
				cfg.Write.Backpressure.Enabled = true

				if err := cfg.Validate(); err != nil {
					t.Fatalf("cfg.Validate: %v", err)
				}
				set := exportertest.NewNopSettings(factory.Type())
				core, logs := observer.New(zap.DebugLevel)
				set.Logger = zap.New(core)

				exp, err := factory.CreateTraces(ctx, set, cfg)
				if err != nil {
					t.Fatalf("CreateTraces: %v", err)
				}
				if err := exp.Start(ctx, componenttest.NewNopHost()); err != nil {
					t.Fatalf("Start: %v", err)
				}
				var shutDown bool
				t.Cleanup(func() {
					if shutDown {
						return
					}
					cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cCancel()
					_ = exp.Shutdown(cCtx)
				})

				srv.EnqueueFault(bqfake.Fault{
					Mode:  bqfake.FaultDelayedAck,
					Table: "otel_spans",
					Delay: 3400 * time.Millisecond,
				})
				if err := exp.ConsumeTraces(ctx, makeSpanBatch(0, 1, 0)); err != nil {
					t.Fatalf("warmup ConsumeTraces: %v", err)
				}
				waitForRows(t, srv, 1, 6*time.Second)

				for range 4 {
					srv.EnqueueFault(bqfake.Fault{
						Mode:  bqfake.FaultDelayedAck,
						Table: "otel_spans",
						Delay: 300 * time.Millisecond,
					})
				}
				for i := range 8 {
					if err := exp.ConsumeTraces(ctx, makeSpanBatch(100+i, 1, 0)); err != nil {
						t.Fatalf("ConsumeTraces(%d): %v", i, err)
					}
				}

				if !retriesEnabled {
					waitForRows(t, srv, 9, 5*time.Second)
				}

				shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				shutDown = true
				if err := exp.Shutdown(shutdownCtx); err != nil {
					t.Fatalf("Shutdown: %v", err)
				}

				if got := srv.RowCount("otel_spans"); got != 9 {
					t.Fatalf("stored spans = %d, want 9", got)
				}
				assertZeroBackpressureLogs(t, logs)
			})
		}
	})
}
