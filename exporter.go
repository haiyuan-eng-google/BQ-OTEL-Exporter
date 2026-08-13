// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	bqi "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/diag"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/metadata"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/protoenc"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

// writeAPITraceID identifies this client to the Storage Write API backend.
const writeAPITraceID = "otel-bigqueryexporter"

// scopeRead is sufficient for existence and schema validation when startup is
// not authorized to create control-plane resources.
const scopeRead = "https://www.googleapis.com/auth/bigquery.readonly"

// newManagedWriterClient is the external constructor seam. Keeping it narrow
// lets lifecycle tests prove which context is retained without opening a real
// Storage Write API connection.
var newManagedWriterClient = managedwriter.NewClient

// rowAppender is the append surface the push paths depend on. Narrowing it to
// an interface is what lets the FR6 subset logic be tested without a live
// BigQuery client, which is the part most worth testing.
type rowAppender interface {
	Append(ctx context.Context, rows [][]byte) bqi.AppendOutcome
	Close() error
}

// rowEncoder serializes one row.
type rowEncoder interface {
	Encode(row transform.Row) ([]byte, error)
}

// signalExporter holds everything both pipelines share. The traces and logs
// exporters differ only in which table they target and how they build rows.
type signalExporter struct {
	cfg    *Config
	table  string
	logger *zap.Logger
	tel    *metadata.Telemetry

	lifetimeCancel context.CancelFunc

	client *managedwriter.Client
	writer rowAppender
	enc    rowEncoder

	schema *storagepb.TableSchema
	opts   transform.Options
}

func newSignalExporter(
	set component.TelemetrySettings,
	cfg *Config,
	table string,
	tableSchema *storagepb.TableSchema,
) *signalExporter {
	return &signalExporter{
		cfg:    cfg,
		table:  table,
		logger: set.Logger,
		schema: tableSchema,
		opts: transform.Options{
			Limits:                  cfg.Limits.toTransform(),
			SourceRecordNamespace:   cfg.Logs.SourceRecordNamespace,
			SourceRecordIDAttribute: cfg.Logs.SourceRecordIDAttribute,
		},
	}
}

// authenticationOptions renders the configured identity for either BigQuery
// client. Service endpoint overrides remain a Storage Write-only concern.
func (e *signalExporter) authenticationOptions(
	ctx context.Context, scopes []string,
) ([]option.ClientOption, error) {
	var opts []option.ClientOption

	switch {
	case e.cfg.Credentials.File != "":
		opts = append(opts, option.WithCredentialsFile(e.cfg.Credentials.File))

	case e.cfg.Credentials.ImpersonateServiceAccount != "":
		ts, err := impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
			TargetPrincipal: e.cfg.Credentials.ImpersonateServiceAccount,
			Scopes:          scopes,
		})
		if err != nil {
			return nil, fmt.Errorf("configuring impersonation of %s: %w",
				e.cfg.Credentials.ImpersonateServiceAccount, err)
		}
		opts = append(opts, option.WithTokenSource(ts))
	}

	return opts, nil
}

func (e *signalExporter) metadataClientOptions(ctx context.Context) ([]option.ClientOption, error) {
	return e.authenticationOptions(ctx, e.metadataScopes())
}

func (e *signalExporter) metadataScopes() []string {
	if e.cfg.AutoCreate.Dataset || e.cfg.AutoCreate.Tables {
		return []string{bigqueryapi.BigqueryScope}
	}
	return []string{scopeRead}
}

func (e *signalExporter) writerClientOptions(ctx context.Context) ([]option.ClientOption, error) {
	opts, err := e.authenticationOptions(ctx, []string{bigqueryapi.BigqueryInsertdataScope})
	if err != nil {
		return nil, err
	}
	if url := e.cfg.Endpoint.URL; url != "" {
		opts = append(opts, option.WithEndpoint(url))
		if e.cfg.Endpoint.Insecure {
			// Validation has already confirmed this is a loopback endpoint.
			opts = append(opts, option.WithGRPCDialOption(
				grpc.WithTransportCredentials(insecure.NewCredentials())))
		}
		if e.cfg.Endpoint.WithoutAuthentication {
			opts = append(opts, option.WithoutAuthentication())
		}
	}

	return opts, nil
}

func (e *signalExporter) start(ctx context.Context, _ component.Host) (err error) {
	// Start's context is scoped to component startup. managedwriter retains
	// both its client and stream constructor contexts for background work, so
	// give them an exporter-owned lifetime that ends only during cleanup.
	lifetimeCtx, lifetimeCancel := context.WithCancel(context.Background())
	e.lifetimeCancel = lifetimeCancel
	defer func() {
		if err != nil {
			err = errors.Join(err, e.shutdown(context.Background()))
		}
	}()

	metadataOpts, err := e.metadataClientOptions(ctx)
	if err != nil {
		return err
	}
	admin, err := newDestinationAdmin(ctx, e.cfg.Project, e.cfg.Dataset, metadataOpts...)
	if err != nil {
		return fmt.Errorf("creating BigQuery metadata client: %w", err)
	}
	defer func() { err = errors.Join(err, admin.Close()) }()
	if err := e.prepareDestination(ctx, admin); err != nil {
		return err
	}

	writerOpts, err := e.writerClientOptions(lifetimeCtx)
	if err != nil {
		return err
	}
	client, err := newManagedWriterClient(lifetimeCtx, e.cfg.Project, writerOpts...)
	if err != nil {
		return fmt.Errorf("creating BigQuery Storage Write client: %w", err)
	}
	e.client = client

	enc, err := protoenc.New(e.schema)
	if err != nil {
		return err
	}
	e.enc = enc

	e.writer = bqi.NewWriter(lifetimeCtx, client, bqi.WriterOptions{
		Project:             e.cfg.Project,
		Dataset:             e.cfg.Dataset,
		Table:               e.table,
		Descriptor:          enc.Descriptor(),
		MaxRequestBytes:     e.cfg.Write.MaxRequestBytes,
		MaxInflightRequests: e.cfg.Write.MaxInflightRequests,
		MaxInflightBytes:    e.cfg.Write.MaxInflightBytes,
		TraceID:             writeAPITraceID,
		Observer:            e.tel,
	})

	e.logger.Info("BigQuery exporter started",
		zap.String("destination", e.tableID()),
		zap.String("delivery", "at-least-once (default stream) — appends may duplicate data"),
		zap.String("schema_version", schemaVersion),
	)
	return nil
}

func (e *signalExporter) tableID() string {
	return fmt.Sprintf("%s.%s.%s", e.cfg.Project, e.cfg.Dataset, e.table)
}

func (e *signalExporter) shutdown(_ context.Context) error {
	// In-flight appends drain against the caller's context deadline; whatever
	// has not been acknowledged fails back to the persistent queue and is
	// replayed after restart.
	var errs []error
	if e.writer != nil {
		if err := e.writer.Close(); err != nil {
			errs = append(errs, err)
		}
		e.writer = nil
	}
	if e.client != nil {
		if err := e.client.Close(); err != nil {
			errs = append(errs, err)
		}
		e.client = nil
	}
	if e.lifetimeCancel != nil {
		e.lifetimeCancel()
		e.lifetimeCancel = nil
	}
	return errors.Join(errs...)
}

// encodeRows serializes rows, dropping any that cannot be encoded or that
// exceed the per-row byte limit.
//
// Both are permanent, row-scoped failures: retrying them produces the same
// result. They are counted and dropped rather than failing the batch, because
// one pathological row must not block the telemetry around it.
func (e *signalExporter) encodeRows(
	ctx context.Context, rows []transform.Row, opID string,
) (encoded [][]byte, keptIndex []int, dropped int) {
	encoded = make([][]byte, 0, len(rows))
	keptIndex = make([]int, 0, len(rows))

	for i, row := range rows {
		b, err := e.enc.Encode(row)
		if err != nil {
			e.tel.RecordRejected(ctx, string(transform.ReasonSerialization), 1)
			e.logger.Debug("dropping row that failed serialization",
				diag.Rejection(e.tableID(), opID, i, string(transform.ReasonSerialization), "unknown", 0, 0)...)
			dropped++
			continue
		}
		if len(b) > e.cfg.Write.MaxRowBytes {
			e.tel.RecordRejected(ctx, string(transform.ReasonRowBytes), 1)
			e.logger.Debug("dropping oversized row",
				diag.Rejection(e.tableID(), opID, i, string(transform.ReasonRowBytes), "unknown",
					e.cfg.Write.MaxRowBytes, len(b))...)
			dropped++
			continue
		}
		encoded = append(encoded, b)
		keptIndex = append(keptIndex, i)
	}
	return encoded, keptIndex, dropped
}

// recordRejections counts and logs records the transform stage dropped for
// breaching a structural limit.
func (e *signalExporter) recordRejections(ctx context.Context, rejects []transform.Rejection, opID string) {
	for _, r := range rejects {
		e.tel.RecordRejected(ctx, string(r.Reason), 1)
		e.logger.Debug("dropping record that breached a structural limit",
			diag.Rejection(e.tableID(), opID, r.Index, string(r.Reason), r.FieldPath, r.Limit, r.Actual)...)
	}
}

// appendResult describes what the caller should do after an append.
type appendDecision struct {
	// retryableIndices lists positions in the original row slice whose
	// telemetry was neither acknowledged nor permanently rejected.
	retryableIndices []int
	// err is a transport-level failure to surface, if any.
	err error
	// permanent reports whether err is permanent.
	permanent bool
}

// appendEncoded sends encoded rows and interprets the outcome.
//
// The rule that shapes this: when AppendRows reports row errors, no rows in
// that request were appended. So a row error is not just "that row failed" —
// every row that shared the request needs re-sending, minus the invalid ones.
func (e *signalExporter) appendEncoded(
	ctx context.Context, encoded [][]byte, keptIndex []int, opID string,
) appendDecision {
	if len(encoded) == 0 {
		return appendDecision{}
	}

	out := e.writer.Append(ctx, encoded)

	if out.Err != nil {
		v := out.Verdict
		switch v.Class {
		case bqi.Uncertain:
			// The append may have been applied. Replaying can duplicate,
			// which the at-least-once contract permits and the dedup SQL
			// resolves.
			e.tel.RecordUncertainAckReplay(ctx)
			e.tel.RecordRetry(ctx, v.Label)
		case bqi.Retryable:
			e.tel.RecordRetry(ctx, v.Label)
		}

		if v.Label == bqi.LabelPermission {
			e.logger.Error(bqi.PermissionHint(e.tableID()),
				append(diag.Fields(e.tableID(), opID), zap.String("error_code", v.Label))...)
		} else {
			e.logger.Warn("append failed",
				append(diag.Fields(e.tableID(), opID),
					zap.String("error_code", v.Label),
					zap.String("class", v.Class.String()),
					zap.String("owner", v.Owner.String()),
					zap.Int("rows", len(encoded)),
				)...)
		}

		if v.Class == bqi.Permanent {
			return appendDecision{err: out.Err, permanent: true}
		}
		// Everything in this append is still unacknowledged.
		return appendDecision{retryableIndices: keptIndex, err: out.Err}
	}

	if len(out.RowErrors) > 0 {
		invalid := make(map[int]bool, len(out.RowErrors))
		for _, re := range out.RowErrors {
			pos := int(re.Index)
			if pos < 0 || pos >= len(keptIndex) {
				continue
			}
			invalid[pos] = true
			e.tel.RecordRejected(ctx, string(transform.ReasonRowError), 1)
			e.logger.Debug("service rejected a row",
				diag.RowError(e.tableID(), opID, re.Index, re.Code, re.Message)...)
		}

		// The whole request was refused, so the valid rows that travelled with
		// the invalid ones still need delivering. Permanently invalid rows are
		// dropped here and never routed back through consumererror.
		var retry []int
		for pos, origIdx := range keptIndex {
			if !invalid[pos] {
				retry = append(retry, origIdx)
			}
		}
		return appendDecision{
			retryableIndices: retry,
			err:              fmt.Errorf("%d row(s) rejected by the service; re-sending the valid subset", len(invalid)),
		}
	}

	e.tel.RecordAcknowledged(ctx, out.AcknowledgedRows)
	return appendDecision{}
}
