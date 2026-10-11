// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"
	"maps"
	stdhttp "net/http"
	"reflect"
	"slices"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/googleapis/gax-go/v2/apierror"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// destinationAdmin is the narrow control-plane boundary used during Start.
// It deliberately excludes row writes so provisioning cannot accidentally
// become part of the data path.
type destinationAdmin interface {
	DatasetMetadata(context.Context) (*bq.DatasetMetadata, error)
	CreateDataset(context.Context, *bq.DatasetMetadata) error
	TableMetadata(context.Context, string) (*bq.TableMetadata, error)
	CreateTable(context.Context, string, *bq.TableMetadata) error
	UpdateTable(context.Context, string, bq.TableMetadataToUpdate, string) error
	RunDDL(context.Context, string) error
	Close() error
}

type bigQueryDestinationAdmin struct {
	client   *bq.Client
	httpBase *stdhttp.Transport
	dataset  *bq.Dataset
}

func (a *bigQueryDestinationAdmin) DatasetMetadata(ctx context.Context) (*bq.DatasetMetadata, error) {
	return a.dataset.Metadata(ctx)
}

func (a *bigQueryDestinationAdmin) CreateDataset(ctx context.Context, md *bq.DatasetMetadata) error {
	return a.dataset.Create(ctx, md)
}

func (a *bigQueryDestinationAdmin) TableMetadata(ctx context.Context, id string) (*bq.TableMetadata, error) {
	return a.dataset.Table(id).Metadata(ctx)
}

func (a *bigQueryDestinationAdmin) CreateTable(ctx context.Context, id string, md *bq.TableMetadata) error {
	return a.dataset.Table(id).Create(ctx, md)
}

func (a *bigQueryDestinationAdmin) UpdateTable(
	ctx context.Context, id string, md bq.TableMetadataToUpdate, etag string,
) error {
	_, err := a.dataset.Table(id).Update(ctx, md, etag)
	return err
}

func (a *bigQueryDestinationAdmin) RunDDL(ctx context.Context, sql string) error {
	job, err := a.client.Query(sql).Run(ctx)
	if err != nil {
		return err
	}
	st, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	return st.Err()
}

func (a *bigQueryDestinationAdmin) Close() error {
	err := a.client.Close()
	a.httpBase.CloseIdleConnections()
	return err
}

var newBigQueryHTTPClient = func(
	ctx context.Context, opts ...option.ClientOption,
) (*stdhttp.Client, *stdhttp.Transport, error) {
	defaultTransport, ok := stdhttp.DefaultTransport.(*stdhttp.Transport)
	if !ok {
		return nil, nil, errors.New("default HTTP transport is not configurable")
	}
	base := defaultTransport.Clone()
	authenticated, err := htransport.NewTransport(ctx, base, opts...)
	if err != nil {
		base.CloseIdleConnections()
		return nil, nil, err
	}
	return &stdhttp.Client{Transport: authenticated}, base, nil
}

var newDestinationAdmin = func(
	ctx context.Context, project, dataset string, opts ...option.ClientOption,
) (destinationAdmin, error) {
	httpClient, httpBase, err := newBigQueryHTTPClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	bqOpts := []option.ClientOption{option.WithHTTPClient(httpClient)}
	for _, opt := range opts {
		if rt := reflect.TypeOf(opt); rt != nil {
			name := rt.Name()
			if name == "withEndpoint" || name == "withUniverseDomain" {
				bqOpts = append(bqOpts, opt)
			}
		}
	}
	client, err := bq.NewClient(ctx, project, bqOpts...)
	if err != nil {
		httpBase.CloseIdleConnections()
		return nil, err
	}
	return &bigQueryDestinationAdmin{
		client: client, httpBase: httpBase, dataset: client.Dataset(dataset),
	}, nil
}

// prepareDestination checks, and where enabled creates, everything this
// exporter writes to: the dataset, the signal table and its views, and the
// quarantine table when quarantine is enabled.
func (e *signalExporter) prepareDestination(ctx context.Context, admin destinationAdmin) error {
	if err := e.prepareSignalDestination(ctx, admin); err != nil {
		return err
	}
	if !e.cfg.Quarantine.Enabled {
		return nil
	}
	return e.prepareQuarantineTable(ctx, admin)
}

func (e *signalExporter) destinationOptions() schema.DestinationOptions {
	switch {
	case proto.Equal(e.schema, schema.SpansTableSchema()):
		return schema.DestinationOptions{
			PartitionExpirationDays: e.cfg.Traces.PartitionExpirationDays,
			RequirePartitionFilter:  e.cfg.Traces.RequirePartitionFilter,
		}
	case proto.Equal(e.schema, schema.LogsTableSchema()):
		return schema.DestinationOptions{
			PartitionExpirationDays: e.cfg.Logs.PartitionExpirationDays,
			RequirePartitionFilter:  e.cfg.Logs.RequirePartitionFilter,
		}
	case proto.Equal(e.schema, schema.MetricsTableSchema()):
		return schema.DestinationOptions{
			PartitionExpirationDays: e.cfg.Metrics.PartitionExpirationDays,
			RequirePartitionFilter:  e.cfg.Metrics.RequirePartitionFilter,
		}
	default:
		return schema.DestinationOptions{}
	}
}

func (e *signalExporter) prepareSignalDestination(ctx context.Context, admin destinationAdmin) error {
	if err := e.ensureDataset(ctx, admin); err != nil {
		return err
	}

	tableMD, tableErr := admin.TableMetadata(ctx, e.table)
	if tableErr != nil && isContextError(tableErr) {
		return fmt.Errorf("reading destination table metadata for %s: %w", e.tableID(), tableErr)
	}
	createTable := false
	switch {
	case tableErr == nil:
		if e.cfg.AutoCreate.Tables {
			reconciled, err := e.reconcileTableSchema(ctx, admin, e.table, tableMD, e.schema, "destination table "+e.tableID())
			if err != nil {
				return err
			}
			tableMD = reconciled
		}
		if err := e.validateTableMetadata(tableMD); err != nil {
			return err
		}
	case isNotFound(tableErr):
		if !e.cfg.AutoCreate.Tables {
			return fmt.Errorf(
				"destination table %s does not exist; create it or enable auto_create.tables",
				e.tableID())
		}
		createTable = true
	default:
		if !e.cfg.AutoCreate.Tables {
			e.logger.Warn("table metadata is unreadable; deferring authorization to the real append",
				zap.String("destination", e.tableID()), zap.Error(tableErr))
			return nil
		}
		e.logger.Warn("table metadata is unreadable; attempting idempotent creation because auto_create.tables is enabled",
			zap.String("destination", e.tableID()), zap.Error(tableErr))
		createTable = true
	}

	if !e.cfg.AutoCreate.Tables {
		return nil
	}

	createMD, viewQuery, err := schema.DestinationMetadataWithOptions(
		e.schema, e.cfg.Project, e.cfg.Dataset, e.table, e.destinationOptions())
	if err != nil {
		return fmt.Errorf("deriving destination metadata: %w", err)
	}
	if createTable {
		if err := e.createDestinationTable(ctx, admin, createMD); err != nil {
			return err
		}
	}
	if err := e.createDedupView(ctx, admin, viewQuery); err != nil {
		return err
	}
	if e.cfg.AutoCreate.SearchIndexes {
		if err := e.createSearchIndex(ctx, admin); err != nil {
			return err
		}
	}
	if !e.cfg.AutoCreate.CompanionViews {
		return nil
	}
	return e.createCompanionViews(ctx, admin)
}

func (e *signalExporter) reconcileTableSchema(
	ctx context.Context,
	admin destinationAdmin,
	table string,
	md *bq.TableMetadata,
	want *storagepb.TableSchema,
	subject string,
) (*bq.TableMetadata, error) {
	if md == nil {
		return nil, nil
	}
	merged, changed, err := schema.ReconcileAdditiveSchema(md.Schema, want)
	if err != nil || !changed {
		return md, nil
	}
	haveMerged, err := schema.FromBigQuery(merged)
	if err != nil || len(schema.CompatibilityDifferences(want, haveMerged)) > 0 {
		return md, nil
	}
	if err := admin.UpdateTable(ctx, table, bq.TableMetadataToUpdate{Schema: merged}, md.ETag); err != nil {
		return nil, fmt.Errorf("updating %s schema: %w", subject, err)
	}
	updated := *md
	updated.Schema = merged
	return &updated, nil
}

func (e *signalExporter) createSearchIndex(ctx context.Context, admin destinationAdmin) error {
	var ddl string
	switch {
	case proto.Equal(e.schema, schema.SpansTableSchema()):
		ddl = fmt.Sprintf(schema.SpansSearchIndexDDL, e.cfg.Project, e.cfg.Dataset, e.table)
	case proto.Equal(e.schema, schema.LogsTableSchema()):
		ddl = fmt.Sprintf(schema.LogsSearchIndexDDL, e.cfg.Project, e.cfg.Dataset, e.table)
	default:
		return nil
	}
	if err := admin.RunDDL(ctx, ddl); err != nil {
		return fmt.Errorf("creating search index on %s: %w", e.tableID(), err)
	}
	return nil
}

func (e *signalExporter) ensureDataset(ctx context.Context, admin destinationAdmin) error {
	_, datasetErr := admin.DatasetMetadata(ctx)
	if datasetErr != nil && isContextError(datasetErr) {
		return fmt.Errorf("reading destination dataset metadata for %s.%s: %w",
			e.cfg.Project, e.cfg.Dataset, datasetErr)
	}
	switch {
	case datasetErr == nil:
		return nil
	case isNotFound(datasetErr):
		if !e.cfg.AutoCreate.Dataset {
			return fmt.Errorf(
				"destination dataset %s.%s does not exist; create it or enable auto_create.dataset",
				e.cfg.Project, e.cfg.Dataset)
		}
		if err := admin.CreateDataset(ctx, &bq.DatasetMetadata{Location: e.cfg.Location}); err != nil && !isAlreadyExists(err) {
			return fmt.Errorf("creating destination dataset %s.%s: %w", e.cfg.Project, e.cfg.Dataset, err)
		}
		return nil
	default:
		e.logger.Warn("dataset metadata is unreadable; deferring authorization to the real append",
			zap.String("dataset", e.cfg.Project+"."+e.cfg.Dataset), zap.Error(datasetErr))
		return nil
	}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (e *signalExporter) createDestinationTable(
	ctx context.Context, admin destinationAdmin, md *bq.TableMetadata,
) error {
	if err := admin.CreateTable(ctx, e.table, md); err != nil {
		if !isAlreadyExists(err) {
			return fmt.Errorf("creating destination table %s: %w", e.tableID(), err)
		}
		winner, metadataErr := admin.TableMetadata(ctx, e.table)
		if metadataErr != nil {
			return fmt.Errorf(
				"validating concurrently created destination table %s: %w", e.tableID(), metadataErr)
		}
		return e.validateTableMetadata(winner)
	}
	return nil
}

func (e *signalExporter) createDedupView(
	ctx context.Context, admin destinationAdmin, query string,
) error {
	viewID := e.table + "_dedup"
	md := &bq.TableMetadata{ViewQuery: query, UseLegacySQL: false}
	err := admin.CreateTable(ctx, viewID, md)
	if err == nil || isAlreadyExists(err) {
		return nil
	}
	return fmt.Errorf("creating deduplication view %s.%s.%s: %w",
		e.cfg.Project, e.cfg.Dataset, viewID, err)
}

// createCompanionViews creates the optional companion views of the active
// signal when enabled. Like the dedup view they are create-only: an existing
// view produces AlreadyExists and is left as it is.
//
// v_gen_ai_logs joins the log and span dedup views, which belong to separate
// signal exporters, so whichever exporter starts second creates v_gen_ai_logs
// once the peer signal's dedup view exists. When the peer dedup view does not
// exist yet (or in a single-signal deployment), v_gen_ai_logs is skipped with
// a notice rather than failing startup, and a later start creates it. An
// unreadable peer dedup view is skipped with a warning because companion views
// are a convenience and must not block export.
func (e *signalExporter) createCompanionViews(ctx context.Context, admin destinationAdmin) error {
	if proto.Equal(e.schema, schema.MetricsTableSchema()) {
		return nil
	}
	isSpans := proto.Equal(e.schema, schema.SpansTableSchema())
	spansTable, logsTable := e.table, e.cfg.Logs.Table
	if !isSpans {
		spansTable, logsTable = e.cfg.Traces.Table, e.table
		ok, err := e.probePeerDedupView(ctx, admin, "span", spansTable)
		if err != nil || !ok {
			return err
		}
	}

	views := schema.CompanionViewsMetadata(e.cfg.Project, e.cfg.Dataset, spansTable, logsTable, isSpans)
	order := slices.Sorted(maps.Keys(views))
	if isSpans {
		order = schema.SpanCompanionViewOrder
	}
	var companionViewsPreserved bool
	for _, id := range order {
		md, ok := views[id]
		if !ok {
			continue
		}
		err := admin.CreateTable(ctx, id, md)
		if isAlreadyExists(err) {
			companionViewsPreserved = true
			continue
		}
		if err != nil {
			if isRollupView(id) && companionViewsPreserved {
				e.logger.Warn("skipping rollup view creation over preserved companion view",
					zap.String("view", e.cfg.Project+"."+e.cfg.Dataset+"."+id),
					zap.Error(err))
				continue
			}
			return fmt.Errorf("creating companion view %s.%s.%s: %w",
				e.cfg.Project, e.cfg.Dataset, id, err)
		}
	}

	if isSpans {
		ok, err := e.probePeerDedupView(ctx, admin, "log", logsTable)
		if err != nil || !ok {
			return err
		}
		logViews := schema.CompanionViewsMetadata(e.cfg.Project, e.cfg.Dataset, spansTable, logsTable, false)
		for _, id := range slices.Sorted(maps.Keys(logViews)) {
			if err := admin.CreateTable(ctx, id, logViews[id]); err != nil && !isAlreadyExists(err) {
				return fmt.Errorf("creating companion view %s.%s.%s: %w",
					e.cfg.Project, e.cfg.Dataset, id, err)
			}
		}
	}
	return nil
}

func isRollupView(id string) bool {
	switch id {
	case schema.GenAIModelCostsViewName,
		schema.GenAISessionsViewName,
		schema.GenAIToolUsageViewName:
		return true
	default:
		return false
	}
}

func (e *signalExporter) probePeerDedupView(
	ctx context.Context, admin destinationAdmin, peerSignal, peerTable string,
) (bool, error) {
	peerView := e.cfg.Project + "." + e.cfg.Dataset + "." + peerTable + "_dedup"
	logsView := e.cfg.Project + "." + e.cfg.Dataset + "." + schema.GenAILogsViewName
	if _, err := admin.TableMetadata(ctx, peerTable+"_dedup"); err != nil {
		switch {
		case isContextError(err):
			return false, fmt.Errorf("reading %s deduplication view metadata for %s: %w", peerSignal, peerView, err)
		case isNotFound(err):
			e.logger.Info(peerSignal+" deduplication view does not exist; skipping companion view until a later start",
				zap.String("view", logsView), zap.String("requires", peerView))
		default:
			e.logger.Warn(peerSignal+" deduplication view metadata is unreadable; skipping companion view",
				zap.String("view", logsView), zap.String("requires", peerView), zap.Error(err))
		}
		return false, nil
	}
	return true, nil
}

// prepareQuarantineTable checks the quarantine table, and creates it when
// auto_create.tables is enabled. The rules match the signal table's: a
// missing table needs auto_create.tables, a table that cannot take quarantine
// rows fails startup, and unreadable metadata defers to the first quarantine
// append unless creation is enabled. All signal exporters share the table, so
// a concurrent creation by another signal exporter is validated like any
// existing table.
func (e *signalExporter) prepareQuarantineTable(ctx context.Context, admin destinationAdmin) error {
	table := e.cfg.Quarantine.Table
	subject := "quarantine table " + e.quarantineTableID()
	want := schema.QuarantineTableSchema()

	md, err := admin.TableMetadata(ctx, table)
	switch {
	case err == nil:
		if e.cfg.AutoCreate.Tables {
			md, err = e.reconcileTableSchema(ctx, admin, table, md, want, subject)
			if err != nil {
				return err
			}
		}
		return validateSchemaOf(md, want, subject)
	case isContextError(err):
		return fmt.Errorf("reading %s metadata: %w", subject, err)
	case isNotFound(err):
		if !e.cfg.AutoCreate.Tables {
			return fmt.Errorf("%s does not exist; create it or enable auto_create.tables", subject)
		}
	default:
		if !e.cfg.AutoCreate.Tables {
			e.logger.Warn("quarantine table metadata is unreadable; deferring authorization to the first quarantine append",
				zap.String("destination", e.quarantineTableID()), zap.Error(err))
			return nil
		}
		e.logger.Warn("quarantine table metadata is unreadable; attempting idempotent creation because auto_create.tables is enabled",
			zap.String("destination", e.quarantineTableID()), zap.Error(err))
	}

	createMD, err := schema.QuarantineDestinationMetadataWithOptions(schema.DestinationOptions{
		PartitionExpirationDays: e.cfg.Quarantine.PartitionExpirationDays,
		RequirePartitionFilter:  e.cfg.Quarantine.RequirePartitionFilter,
	})
	if err != nil {
		return fmt.Errorf("deriving quarantine table metadata: %w", err)
	}
	if err := admin.CreateTable(ctx, table, createMD); err != nil {
		if !isAlreadyExists(err) {
			return fmt.Errorf("creating %s: %w", subject, err)
		}
		winner, readErr := admin.TableMetadata(ctx, table)
		if readErr != nil {
			return fmt.Errorf("validating concurrently created %s: %w", subject, readErr)
		}
		return validateSchemaOf(winner, want, subject)
	}
	return nil
}

func (e *signalExporter) validateTableMetadata(md *bq.TableMetadata) error {
	return validateSchemaOf(md, e.schema, "destination table "+e.tableID())
}

// validateSchemaOf checks that a table described by md can take rows of the
// want schema. subject names the table in errors.
func validateSchemaOf(md *bq.TableMetadata, want *storagepb.TableSchema, subject string) error {
	if md == nil {
		return fmt.Errorf("%s returned nil metadata", subject)
	}
	have, err := schema.FromBigQuery(md.Schema)
	if err != nil {
		return fmt.Errorf("%s has an unreadable schema: %w", subject, err)
	}
	if differences := schema.CompatibilityDifferences(want, have); len(differences) > 0 {
		return fmt.Errorf(
			"%s does not match schema %s: %v — create or migrate the table before starting",
			subject, schemaVersion, differences)
	}
	return nil
}

func isNotFound(err error) bool {
	return errorCodeIs(err, stdhttp.StatusNotFound, codes.NotFound)
}

func isAlreadyExists(err error) bool {
	return errorCodeIs(err, stdhttp.StatusConflict, codes.AlreadyExists)
}

func errorCodeIs(err error, httpCode int, grpcCode codes.Code) bool {
	var googleErr *googleapi.Error
	if errors.As(err, &googleErr) && googleErr.Code == httpCode {
		return true
	}
	if status.Code(err) == grpcCode {
		return true
	}
	apiErr, ok := apierror.FromError(err)
	return ok && (apiErr.HTTPCode() == httpCode || apiErr.GRPCStatus().Code() == grpcCode)
}
