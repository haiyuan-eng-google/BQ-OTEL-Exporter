// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
	"golang.org/x/oauth2/google"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"

	bqi "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/diag"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/metadata"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/offload"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/protoenc"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

// writeAPITraceID identifies this client to the Storage Write API backend.
const writeAPITraceID = "otel-bigqueryexporter"

// scopeRead is sufficient for existence and schema validation when startup is
// not authorized to create control-plane resources.
const scopeRead = "https://www.googleapis.com/auth/bigquery.readonly"

// scopeStorageReadWrite authorizes uploading offloaded payloads to GCS.
const scopeStorageReadWrite = "https://www.googleapis.com/auth/devstorage.read_write"

// newManagedWriterClient is the external constructor seam. Keeping it narrow
// lets lifecycle tests prove which context is retained without opening a real
// Storage Write API connection.
var newManagedWriterClient = managedwriter.NewClient

// newBlobStore constructs the GCS blob store used when offload.enabled is set.
var newBlobStore = func(ctx context.Context, opts ...option.ClientOption) (offload.BlobStore, error) {
	return offload.NewGCSBlobStore(ctx, opts...)
}

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

	// quarantine, quarantineClient, and quarantineEnc write the opt-in
	// quarantine table on a client and stream of its own so an idle quarantine
	// stream never holds open the primary writer's multiplexed connection. All
	// stay nil unless quarantine is enabled.
	quarantineClient *managedwriter.Client
	quarantine       rowAppender
	quarantineEnc    rowEncoder

	// offloader offloads oversized span/log values and quarantine payloads
	// when offload.enabled is set.
	offloader *offload.Offloader

	schema *storagepb.TableSchema
	opts   transform.Options
}

func newSignalExporter(
	set component.TelemetrySettings,
	cfg *Config,
	table string,
	tableSchema *storagepb.TableSchema,
) *signalExporter {
	se := &signalExporter{
		cfg:    cfg,
		table:  table,
		logger: set.Logger,
		schema: tableSchema,
		opts: transform.Options{
			Limits:                    cfg.Limits.toTransform(),
			LogNameAttribute:          cfg.Logs.LogNameAttribute,
			InsertIDAttribute:         cfg.Logs.InsertIDAttribute,
			ExporterInstance:          exporterInstance(set.Resource),
			MaxRowBytes:               cfg.Write.MaxRowBytes,
			QuarantineMetadataOnly:    cfg.Quarantine.MetadataOnly,
			MaxQuarantinePayloadBytes: cfg.Quarantine.MaxRawPayloadBytes,
		},
	}
	if cfg.Offload.Enabled {
		off, err := offload.New(nil, offload.Options{
			Bucket:         cfg.Offload.Bucket,
			Prefix:         cfg.Offload.Prefix,
			Authorizer:     cfg.Offload.Authorizer,
			MinBytes:       cfg.Offload.MinBytes,
			MaxObjectBytes: cfg.Offload.MaxObjectBytes,
			Timeout:        cfg.Offload.Timeout,
		})
		if err == nil {
			se.offloader = off
			se.opts.PayloadOffloader = off
			se.opts.QuarantinePayloadOffloader = off.OffloadQuarantine
		}
	}
	return se
}

// exporterInstance labels quarantine rows with the service.instance.id of the
// collector's own telemetry resource, or nothing when the host sets none.
func exporterInstance(res pcommon.Resource) string {
	if res == (pcommon.Resource{}) {
		return ""
	}
	if v, ok := res.Attributes().Get("service.instance.id"); ok {
		return v.AsString()
	}
	return ""
}

// authenticationOptions renders the configured identity for either BigQuery
// client. Service endpoint overrides remain a Storage Write-only concern.
func (e *signalExporter) authenticationOptions(
	ctx context.Context, scopes []string,
) ([]option.ClientOption, error) {
	var opts []option.ClientOption

	switch {
	case e.cfg.Credentials.File != "":
		credOpt, err := credentialsFileOption(ctx, e.cfg.Credentials.File, scopes)
		if err != nil {
			return nil, err
		}
		// The scopes are repeated as a client option so the client library
		// sees the same explicit scope list on every authentication path.
		opts = append(opts,
			credOpt,
			option.WithScopes(scopes...),
		)

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

	default:
		opts = append(opts, option.WithScopes(scopes...))
	}

	return opts, nil
}

// credentialFileTypes lists the credential file formats that credentials.file
// accepts. The file path comes from the collector's own configuration; the
// declared type is still checked so that a file of an unexpected kind is
// rejected with a clear error instead of being interpreted implicitly.
var credentialFileTypes = []google.CredentialsType{
	google.ServiceAccount,
	google.AuthorizedUser,
	google.ExternalAccount,
	google.ExternalAccountAuthorizedUser,
	google.ImpersonatedServiceAccount,
}

// credentialsFileOption loads the credentials file named by credentials.file,
// validates it at startup so malformed files fail fast, and returns a client
// option that uses the auth library's credentials path with the already-read
// bytes.
//
// option.WithAuthCredentialsJSON is used instead of option.WithCredentials
// for three reasons:
//
//  1. Per-call context cancellation: WithCredentials wraps an oauth2.TokenSource
//     whose Token() method takes no context, binding token refresh to the
//     exporter's lifetimeCtx. WithAuthCredentialsJSON uses cloud.google.com/go/auth,
//     whose Credentials.Token(ctx) honors the per-RPC stream context that
//     Writer.Close() cancels, so shutdown completes promptly even when a
//     token endpoint stalls.
//
//  2. mTLS transport preservation: WithCredentials sets DialSettings.Credentials,
//     which bypasses GetOAuth2Configuration on the REST metadata client path,
//     dropping the oauth2.mtls.googleapis.com/token endpoint and the TLS client
//     certificate callback. WithAuthCredentialsJSON sets
//     DialSettings.AuthCredentialsJSON, which goes through
//     GetOAuth2Configuration and preserves mTLS.
//
//  3. Local JWT signing for service_account keys: WithAuthCredentialsJSON
//     passes the validated bytes through cloud.google.com/go/auth, which
//     signs JWTs locally for service_account keys (0 token_uri calls) instead
//     of exchanging a refresh token over the network. It also avoids a
//     second disk read (no TOCTOU).
func credentialsFileOption(
	ctx context.Context, path string, scopes []string,
) (option.ClientOption, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading credentials file: %w", err)
	}

	var declared struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &declared); err != nil {
		return nil, fmt.Errorf("parsing credentials file %q: %w", path, err)
	}

	credType := google.CredentialsType(declared.Type)
	if !slices.Contains(credentialFileTypes, credType) {
		return nil, fmt.Errorf("credentials file %q has unsupported type %q; supported types: %v",
			path, declared.Type, credentialFileTypes)
	}

	// Validate the file at startup so malformed credentials fail fast
	// rather than on the first export. The token is not fetched here;
	// CredentialsFromJSONWithType only parses and constructs the token source.
	if _, err := google.CredentialsFromJSONWithType(ctx, data, credType, scopes...); err != nil {
		return nil, fmt.Errorf("loading credentials file %q: %w", path, err)
	}

	return option.WithAuthCredentialsJSON(option.CredentialsType(declared.Type), data), nil
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
	var opts []option.ClientOption
	if !e.cfg.Endpoint.WithoutAuthentication {
		var err error
		opts, err = e.authenticationOptions(ctx, []string{bigqueryapi.BigqueryInsertdataScope})
		if err != nil {
			return nil, err
		}
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
	if e.cfg.Write.EnableMultiplexing {
		opts = append(opts,
			managedwriter.WithMultiplexing(),
			managedwriter.WithDefaultInflightRequests(e.cfg.Write.MaxInflightRequests),
			managedwriter.WithDefaultInflightBytes(e.cfg.Write.MaxInflightBytes),
		)
	}
	if e.cfg.Write.ConnectionPoolSize > 0 {
		opts = append(opts, option.WithGRPCConnectionPool(e.cfg.Write.ConnectionPoolSize))
	}
	if ka := e.cfg.Write.Keepalive; ka.Time > 0 || ka.Timeout > 0 || ka.PermitWithoutStream {
		opts = append(opts, option.WithGRPCDialOption(grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                ka.Time,
			Timeout:             ka.Timeout,
			PermitWithoutStream: ka.PermitWithoutStream,
		})))
	}
	if e.cfg.Write.Compression == "gzip" {
		opts = append(opts, option.WithGRPCDialOption(
			grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name))))
	}

	return opts, nil
}

func (e *signalExporter) offloadClientOptions(ctx context.Context) ([]option.ClientOption, error) {
	if e.cfg.Endpoint.WithoutAuthentication {
		return []option.ClientOption{option.WithoutAuthentication()}, nil
	}
	return e.authenticationOptions(ctx, []string{scopeStorageReadWrite})
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

	if !e.cfg.Endpoint.WithoutAuthentication {
		startupCtx, startupCancel := context.WithTimeout(ctx, e.cfg.TimeoutSettings.Timeout)
		defer startupCancel()

		metadataOpts, metadataErr := e.metadataClientOptions(startupCtx)
		if metadataErr != nil {
			return metadataErr
		}
		admin, adminErr := newDestinationAdmin(
			startupCtx, e.cfg.Project, e.cfg.Dataset, metadataOpts...)
		if adminErr != nil {
			return fmt.Errorf("creating BigQuery metadata client: %w", adminErr)
		}
		defer func() {
			closeErr := admin.Close()
			if closeErr == nil {
				return
			}
			closeErr = fmt.Errorf("closing BigQuery metadata client: %w", closeErr)
			if err != nil {
				err = errors.Join(err, closeErr)
				return
			}
			e.logger.Warn("closing BigQuery metadata client failed", zap.Error(closeErr))
		}()
		if prepareErr := e.prepareDestination(startupCtx, admin); prepareErr != nil {
			return prepareErr
		}
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

	startedFields := []zap.Field{
		zap.String("destination", e.tableID()),
		zap.String("delivery", "at-least-once (default stream) — appends may duplicate data"),
		zap.String("schema_version", schemaVersion),
	}
	if e.cfg.Quarantine.Enabled {
		qenc, qerr := protoenc.New(schema.QuarantineTableSchema())
		if qerr != nil {
			return qerr
		}
		e.quarantineEnc = qenc
		// The quarantine table gets a client and stream of its own, so a
		// quarantine append that fails or stalls can never fail a signal
		// append, and an idle quarantine stream never holds open a shared
		// multiplexed connection when the signal writer retires its stream.
		qClient, qClientErr := newManagedWriterClient(lifetimeCtx, e.cfg.Project, writerOpts...)
		if qClientErr != nil {
			return fmt.Errorf("creating BigQuery quarantine Storage Write client: %w", qClientErr)
		}
		e.quarantineClient = qClient
		e.quarantine = bqi.NewWriter(lifetimeCtx, qClient, bqi.WriterOptions{
			Project:             e.cfg.Project,
			Dataset:             e.cfg.Dataset,
			Table:               e.cfg.Quarantine.Table,
			Descriptor:          qenc.Descriptor(),
			MaxRequestBytes:     e.cfg.Write.MaxRequestBytes,
			MaxInflightRequests: e.cfg.Write.MaxInflightRequests,
			MaxInflightBytes:    e.cfg.Write.MaxInflightBytes,
			TraceID:             writeAPITraceID,
			Observer:            e.tel,
		})
		startedFields = append(startedFields, zap.String("quarantine", e.quarantineTableID()))
	}
	if e.cfg.Offload.Enabled && e.offloader != nil {
		if e.offloader.Store() == nil {
			offloadOpts, oerr := e.offloadClientOptions(lifetimeCtx)
			if oerr != nil {
				return oerr
			}
			store, serr := newBlobStore(lifetimeCtx, offloadOpts...)
			if serr != nil {
				return fmt.Errorf("creating GCS payload offloader: %w", serr)
			}
			e.offloader.SetStore(store)
		}
		startedFields = append(startedFields, zap.String("offload_bucket", e.cfg.Offload.Bucket))
	}

	e.logger.Info("BigQuery exporter started", startedFields...)
	return nil
}

func (e *signalExporter) tableID() string {
	return fmt.Sprintf("%s.%s.%s", e.cfg.Project, e.cfg.Dataset, e.table)
}

func (e *signalExporter) quarantineTableID() string {
	return fmt.Sprintf("%s.%s.%s", e.cfg.Project, e.cfg.Dataset, e.cfg.Quarantine.Table)
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
	if e.quarantine != nil {
		if err := e.quarantine.Close(); err != nil {
			errs = append(errs, err)
		}
		e.quarantine = nil
	}
	if e.quarantineClient != nil {
		if err := e.quarantineClient.Close(); err != nil {
			errs = append(errs, err)
		}
		e.quarantineClient = nil
	}
	if e.offloader != nil {
		if err := e.offloader.Close(); err != nil {
			errs = append(errs, err)
		}
		e.offloader.SetStore(nil)
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
// one pathological row must not block the telemetry around it. drops
// describes each dropped row for the quarantine table, indexed by row
// position.
func (e *signalExporter) encodeRows(
	ctx context.Context, rows []transform.Row, opID string,
) (encoded [][]byte, keptIndex []int, drops []transform.QuarantineEntry) {
	encoded = make([][]byte, 0, len(rows))
	keptIndex = make([]int, 0, len(rows))

	for i, row := range rows {
		b, err := e.enc.Encode(row)
		if err != nil {
			e.tel.RecordRejected(ctx, string(transform.ReasonSerialization), 1)
			e.logger.Debug("dropping row that failed serialization",
				diag.Rejection(e.tableID(), opID, i, string(transform.ReasonSerialization), "unknown", 0, 0)...)
			drops = append(drops, transform.QuarantineEntry{
				Rejection: transform.Rejection{Index: i, Reason: transform.ReasonSerialization},
				Detail:    err.Error(),
			})
			continue
		}
		if len(b) > e.cfg.Write.MaxRowBytes {
			e.tel.RecordRejected(ctx, string(transform.ReasonRowBytes), 1)
			e.logger.Debug("dropping oversized row",
				diag.Rejection(e.tableID(), opID, i, string(transform.ReasonRowBytes), "unknown",
					e.cfg.Write.MaxRowBytes, len(b))...)
			drops = append(drops, transform.QuarantineEntry{
				Rejection: transform.Rejection{
					Index: i, Reason: transform.ReasonRowBytes,
					Limit: e.cfg.Write.MaxRowBytes, Actual: len(b),
				},
			})
			continue
		}
		encoded = append(encoded, b)
		keptIndex = append(keptIndex, i)
	}
	return encoded, keptIndex, drops
}

// recordRejections counts and logs records the transform stage dropped for
// violating the table contract or breaching a structural limit.
func (e *signalExporter) recordRejections(ctx context.Context, rejects []transform.Rejection, opID string) {
	for _, r := range rejects {
		e.tel.RecordRejected(ctx, string(r.Reason), 1)
		e.logger.Debug("dropping record that violated the table contract or breached a structural limit",
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
	// rowErrors describes each record the service rejected, once per record
	// and indexed like retryableIndices, for the quarantine table.
	rowErrors []transform.QuarantineEntry
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
	e.tel.RecordAcknowledged(ctx, out.AcknowledgedRows)

	invalid := make(map[int]bool, len(out.RowErrors))
	var rowErrors []transform.QuarantineEntry
	for _, re := range out.RowErrors {
		pos := int(re.Index)
		if pos < 0 || pos >= len(keptIndex) {
			continue
		}
		if !invalid[pos] {
			rowErrors = append(rowErrors, transform.QuarantineEntry{
				Rejection: transform.Rejection{Index: keptIndex[pos], Reason: transform.ReasonRowError},
				Detail:    rowErrorDetail(re),
			})
		}
		invalid[pos] = true
		e.tel.RecordRejected(ctx, string(transform.ReasonRowError), 1)
		e.logger.Debug("service rejected a row",
			diag.RowError(e.tableID(), opID, re.Index, re.Code, re.Message)...)
	}

	retryable := make([]int, 0, len(keptIndex)-len(invalid))
	for pos, origIdx := range keptIndex {
		if !invalid[pos] && !out.IsAcked(pos) {
			retryable = append(retryable, origIdx)
		}
	}

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

		// A permanent verdict means every request that failed outright was
		// rejected permanently. A request refused for row errors appended none
		// of its rows, though, and still owes its valid ones, so only a batch
		// without row errors is dropped.
		if v.Class == bqi.Permanent && len(out.RowErrors) == 0 {
			return appendDecision{err: out.Err, permanent: true}
		}
		if len(retryable) == 0 {
			return appendDecision{rowErrors: rowErrors}
		}
		// Rows in already-acknowledged sub-requests are excluded via
		// out.IsAcked; retry every remaining row not permanently rejected.
		return appendDecision{retryableIndices: retryable, err: out.Err, rowErrors: rowErrors}
	}

	if len(out.RowErrors) > 0 {
		if len(retryable) == 0 {
			return appendDecision{rowErrors: rowErrors}
		}
		// The whole request was refused, so the valid rows that travelled with
		// the invalid ones still need delivering. Permanently invalid rows are
		// dropped here and never routed back through consumererror.
		return appendDecision{
			retryableIndices: retryable,
			err:              fmt.Errorf("%d row(s) rejected by the service; re-sending the valid subset", len(invalid)),
			rowErrors:        rowErrors,
		}
	}

	return appendDecision{}
}

// rowErrorDetail renders a service row error for the quarantine table.
func rowErrorDetail(re bqi.RowError) string {
	if re.Message == "" {
		return re.Code
	}
	return re.Code + ": " + re.Message
}

// quarantineEntries lists every record a push drops permanently, indexed by
// record position: the transform stage's rejections, the rows encodeRows
// dropped, mapped from row positions through rowToRecord, and the rows the
// service rejected.
func quarantineEntries(
	rejects []transform.Rejection,
	drops []transform.QuarantineEntry,
	rowToRecord []int,
	rowErrors []transform.QuarantineEntry,
) []transform.QuarantineEntry {
	entries := make([]transform.QuarantineEntry, 0, len(rejects)+len(drops)+len(rowErrors))
	for _, r := range rejects {
		entries = append(entries, transform.QuarantineEntry{Rejection: r})
	}
	for _, d := range drops {
		d.Index = rowToRecord[d.Index]
		entries = append(entries, d)
	}
	return append(entries, rowErrors...)
}

// quarantineRows appends rows to the quarantine table on its dedicated stream.
//
// Quarantine is best effort and never changes the outcome of the push that
// produced the rows. They are appended once and never retried; when the
// append fails, the failure is logged without any row content and the rows
// are dropped, exactly as they would be with quarantine disabled.
func (e *signalExporter) quarantineRows(ctx context.Context, rows []transform.Row, opID string) {
	if len(rows) == 0 {
		return
	}
	qid := e.quarantineTableID()
	encoded := make([][]byte, 0, len(rows))
	for i, row := range rows {
		b, err := e.quarantineEnc.Encode(row)
		if err != nil {
			e.logger.Debug("dropping quarantine row that failed serialization",
				diag.Rejection(qid, opID, i, string(transform.ReasonSerialization), "unknown", 0, 0)...)
			continue
		}
		if len(b) > e.cfg.Write.MaxRowBytes {
			e.logger.Debug("dropping oversized quarantine row",
				diag.Rejection(qid, opID, i, string(transform.ReasonRowBytes), "unknown",
					e.cfg.Write.MaxRowBytes, len(b))...)
			continue
		}
		encoded = append(encoded, b)
	}
	if len(encoded) == 0 {
		return
	}

	timeout := e.cfg.Quarantine.Timeout
	if timeout <= 0 {
		timeout = defaultQuarantineTimeout
	}
	qCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	out := e.quarantine.Append(qCtx, encoded)
	if out.Err == nil && len(out.RowErrors) == 0 {
		return
	}
	for _, re := range out.RowErrors {
		e.logger.Debug("service rejected a quarantine row",
			diag.RowError(qid, opID, re.Index, re.Code, re.Message)...)
	}
	fields := append(diag.Fields(qid, opID),
		zap.Int("rows", len(encoded)),
		zap.Int("acknowledged", out.AcknowledgedRows),
		zap.Int("row_errors", len(out.RowErrors)),
	)
	if out.Err != nil {
		fields = append(fields,
			zap.String("error_code", out.Verdict.Label),
			zap.String("class", out.Verdict.Class.String()),
		)
	}
	e.logger.Warn("quarantine append failed; unacknowledged quarantine rows are dropped", fields...)
}
