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
	"strings"
	"time"

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
	// The API client's auth middleware does not forward pool cleanup.
	a.httpBase.CloseIdleConnections()
	return err
}

// timedDestinationAdmin bounds each individual BigQuery metadata or DDL RPC
// with the configured timeout while still honoring cancellation of the parent
// Start context. That prevents a cold-start provisioning sequence (dataset,
// signal table, dedup view, companion views, quarantine table, search index)
// from exhausting a single shared 5s deadline across 14+ sequential calls.
type timedDestinationAdmin struct {
	inner   destinationAdmin
	timeout time.Duration
}

func withPerCallTimeout(admin destinationAdmin, timeout time.Duration) destinationAdmin {
	if timeout <= 0 {
		return admin
	}
	return &timedDestinationAdmin{inner: admin, timeout: timeout}
}

func (a *timedDestinationAdmin) DatasetMetadata(ctx context.Context) (*bq.DatasetMetadata, error) {
	callCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	return a.inner.DatasetMetadata(callCtx)
}

func (a *timedDestinationAdmin) CreateDataset(ctx context.Context, md *bq.DatasetMetadata) error {
	callCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	return a.inner.CreateDataset(callCtx, md)
}

func (a *timedDestinationAdmin) TableMetadata(ctx context.Context, id string) (*bq.TableMetadata, error) {
	callCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	return a.inner.TableMetadata(callCtx, id)
}

func (a *timedDestinationAdmin) CreateTable(ctx context.Context, id string, md *bq.TableMetadata) error {
	callCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	return a.inner.CreateTable(callCtx, id, md)
}

func (a *timedDestinationAdmin) UpdateTable(
	ctx context.Context, id string, md bq.TableMetadataToUpdate, etag string,
) error {
	callCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	return a.inner.UpdateTable(callCtx, id, md, etag)
}

func (a *timedDestinationAdmin) RunDDL(ctx context.Context, sql string) error {
	callCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	return a.inner.RunDDL(callCtx, sql)
}

func (a *timedDestinationAdmin) Close() error {
	return a.inner.Close()
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

func (e *signalExporter) effectiveKMSKeyName(override string) string {
	if override != "" {
		return override
	}
	return e.cfg.KMSKeyName
}

func (e *signalExporter) effectiveLabels(override map[string]string) map[string]string {
	return mergeLabels(e.cfg.Labels, override)
}

func mergeLabels(base, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(override))
	maps.Copy(out, base)
	maps.Copy(out, override)
	return out
}

func mustParseDurationOrZero(raw string) time.Duration {
	if raw == "" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0
	}
	return d
}

func (e *signalExporter) destinationOptions() schema.DestinationOptions {
	switch {
	case proto.Equal(e.schema, schema.SpansTableSchema()):
		return schema.DestinationOptions{
			PartitionExpirationDays: e.cfg.Traces.PartitionExpirationDays,
			RequirePartitionFilter:  e.cfg.Traces.RequirePartitionFilter,
			KMSKeyName:              e.effectiveKMSKeyName(e.cfg.Traces.KMSKeyName),
			Labels:                  e.effectiveLabels(e.cfg.Traces.Labels),
			MaxStaleness:            mustParseDurationOrZero(e.cfg.Traces.MaxStaleness),
		}
	case proto.Equal(e.schema, schema.LogsTableSchema()):
		return schema.DestinationOptions{
			PartitionExpirationDays: e.cfg.Logs.PartitionExpirationDays,
			RequirePartitionFilter:  e.cfg.Logs.RequirePartitionFilter,
			KMSKeyName:              e.effectiveKMSKeyName(e.cfg.Logs.KMSKeyName),
			Labels:                  e.effectiveLabels(e.cfg.Logs.Labels),
			MaxStaleness:            mustParseDurationOrZero(e.cfg.Logs.MaxStaleness),
		}
	case proto.Equal(e.schema, schema.MetricsTableSchema()):
		return schema.DestinationOptions{
			PartitionExpirationDays: e.cfg.Metrics.PartitionExpirationDays,
			RequirePartitionFilter:  e.cfg.Metrics.RequirePartitionFilter,
			KMSKeyName:              e.effectiveKMSKeyName(e.cfg.Metrics.KMSKeyName),
			Labels:                  e.effectiveLabels(e.cfg.Metrics.Labels),
			MaxStaleness:            mustParseDurationOrZero(e.cfg.Metrics.MaxStaleness),
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
	effectiveTableMD := tableMD
	if createTable {
		createdMD, err := e.createDestinationTable(ctx, admin, createMD)
		if err != nil {
			return err
		}
		effectiveTableMD = createdMD
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
	return e.createCompanionViews(ctx, admin, effectiveTableMD)
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
	for attempt := range 2 {
		merged, changed, err := schema.ReconcileAdditiveSchema(md.Schema, want)
		if err != nil {
			e.logger.Warn("additive schema reconciliation skipped due to incompatible existing schema",
				zap.String("table", table), zap.Error(err))
			return md, nil
		}
		if !changed {
			return md, nil
		}
		haveMerged, err := schema.FromBigQuery(merged)
		if err != nil {
			e.logger.Warn("additive schema reconciliation skipped due to incompatible existing schema",
				zap.String("table", table), zap.Error(err))
			return md, nil
		}
		if diffs := schema.CompatibilityDifferences(want, haveMerged); len(diffs) > 0 {
			e.logger.Warn("additive schema reconciliation skipped due to incompatible existing schema",
				zap.String("table", table), zap.Strings("differences", diffs))
			return md, nil
		}
		err = admin.UpdateTable(ctx, table, bq.TableMetadataToUpdate{Schema: merged}, md.ETag)
		if err == nil {
			updated := *md
			updated.Schema = merged
			return &updated, nil
		}
		if attempt == 0 && isPreconditionFailed(err) {
			freshMD, readErr := admin.TableMetadata(ctx, table)
			if readErr != nil {
				return nil, fmt.Errorf("re-reading %s metadata after ETag precondition failure: %w", subject, readErr)
			}
			if freshMD == nil {
				return nil, fmt.Errorf("re-reading %s metadata after ETag precondition failure returned nil metadata", subject)
			}
			md = freshMD
			continue
		}
		return nil, fmt.Errorf("updating %s schema: %w", subject, err)
	}
	return md, nil
}

func (e *signalExporter) createSearchIndex(ctx context.Context, admin destinationAdmin) error {
	var ddl string
	switch {
	case proto.Equal(e.schema, schema.SpansTableSchema()):
		ddl = fmt.Sprintf(schema.SpansSearchIndexDDL, e.cfg.Project, e.cfg.Dataset, e.table)
	case proto.Equal(e.schema, schema.LogsTableSchema()):
		ddl = fmt.Sprintf(schema.LogsSearchIndexDDL, e.cfg.Project, e.cfg.Dataset, e.table)
	case proto.Equal(e.schema, schema.MetricsTableSchema()):
		ddl = fmt.Sprintf(schema.MetricsSearchIndexDDL, e.cfg.Project, e.cfg.Dataset, e.table)
	default:
		return nil
	}
	if err := admin.RunDDL(ctx, ddl); err != nil {
		if isSearchIndexAlreadyExists(err) {
			return nil
		}
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
		dsMD := &bq.DatasetMetadata{Location: e.cfg.Location}
		if e.cfg.KMSKeyName != "" {
			dsMD.DefaultEncryptionConfig = &bq.EncryptionConfig{KMSKeyName: e.cfg.KMSKeyName}
		}
		if len(e.cfg.Labels) > 0 {
			dsMD.Labels = maps.Clone(e.cfg.Labels)
		}
		if err := admin.CreateDataset(ctx, dsMD); err != nil && !isAlreadyExists(err) {
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
) (*bq.TableMetadata, error) {
	if err := admin.CreateTable(ctx, e.table, md); err != nil {
		if !isAlreadyExists(err) {
			return nil, fmt.Errorf("creating destination table %s: %w", e.tableID(), err)
		}
		winner, metadataErr := admin.TableMetadata(ctx, e.table)
		if metadataErr != nil {
			return nil, fmt.Errorf(
				"validating concurrently created destination table %s: %w", e.tableID(), metadataErr)
		}
		if err := e.validateTableMetadata(winner); err != nil {
			return nil, err
		}
		return winner, nil
	}
	return md, nil
}

// ensureView creates the view id, or reconciles an existing one to the wanted
// definition in md, so a collector upgraded to a release with newer view SQL
// replaces the stale definition on its next start. kind names the view in
// errors, as in "creating deduplication view".
//
// Only the query is reconciled; labels and every other property of an
// existing view are left alone, and an existing view that has the wanted query
// is not touched. Reconciliation is best effort: a view that cannot be read, is
// not a view, or cannot be updated (for lack of permission, or because its
// query no longer validates) is kept as it is with a warning, and ensureView
// reports it as preserved, because a view is a convenience and must not block
// export. A context error always fails, like every other metadata call, and so
// does any error from creating a view that does not yet exist.
func (e *signalExporter) ensureView(
	ctx context.Context, admin destinationAdmin, kind, id string, md *bq.TableMetadata,
) (preserved bool, err error) {
	name := e.cfg.Project + "." + e.cfg.Dataset + "." + id
	createErr := admin.CreateTable(ctx, id, md)
	if createErr == nil {
		return false, nil
	}
	if !isAlreadyExists(createErr) {
		return false, fmt.Errorf("creating %s %s: %w", kind, name, createErr)
	}

	// The view exists. A concurrent collector may reconcile it at the same
	// moment, in which case the first update fails its ETag precondition and
	// the retry finds the definition already current.
	for attempt := range 2 {
		existing, readErr := admin.TableMetadata(ctx, id)
		switch {
		case readErr != nil && isContextError(readErr):
			return false, fmt.Errorf("reading existing %s %s: %w", kind, name, readErr)
		case readErr != nil:
			e.logger.Warn("existing view is unreadable; leaving it as it is",
				zap.String("view", name), zap.Error(readErr))
			return true, nil
		case existing == nil || existing.ViewQuery == "":
			e.logger.Warn("existing object is not a view; leaving it as it is",
				zap.String("view", name))
			return true, nil
		case !existing.UseLegacySQL && sameViewQuery(existing.ViewQuery, md.ViewQuery):
			return false, nil
		}

		updateErr := admin.UpdateTable(ctx, id, bq.TableMetadataToUpdate{
			ViewQuery:    md.ViewQuery,
			UseLegacySQL: false,
		}, existing.ETag)
		switch {
		case updateErr == nil:
			e.logger.Info("replaced the stale view definition with this release's",
				zap.String("view", name))
			return false, nil
		case isContextError(updateErr):
			return false, fmt.Errorf("updating existing %s %s: %w", kind, name, updateErr)
		case attempt == 0 && isPreconditionFailed(updateErr):
			continue
		default:
			e.logger.Warn("view definition differs from this release's but could not be updated; leaving it as it is",
				zap.String("view", name), zap.Error(updateErr))
			return true, nil
		}
	}
	return true, nil
}

// sameViewQuery reports whether two view queries differ at most in whitespace.
func sameViewQuery(a, b string) bool {
	return strings.Join(strings.Fields(a), " ") == strings.Join(strings.Fields(b), " ")
}

// createDedupView creates the deduplication view of the active signal, or
// reconciles an existing one to the current definition. See ensureView.
func (e *signalExporter) createDedupView(
	ctx context.Context, admin destinationAdmin, query string,
) error {
	md := &bq.TableMetadata{ViewQuery: query, UseLegacySQL: false}
	if labels := e.destinationOptions().Labels; len(labels) > 0 {
		md.Labels = maps.Clone(labels)
	}
	_, err := e.ensureView(ctx, admin, "deduplication view", e.table+"_dedup", md)
	return err
}

// createCompanionViews creates the optional companion views of the active
// signal when enabled. Like the dedup view, an existing companion view is
// reconciled to the current definition rather than left stale: when its query
// differs it is updated, and when it already matches nothing is written. See
// ensureView for how a view that cannot be read or updated is handled. With
// this option on, the exporter therefore owns these fixed-name views, and a
// customized definition under one of their names is replaced.
//
// v_gen_ai_logs joins the log and span dedup views, and
// v_gen_ai_metric_exemplars joins v_gen_ai_metrics with the span dedup view.
// Their inputs belong to separate signal exporters, so whichever exporter
// starts second creates the cross-signal view once the peer signal's dedup view
// exists. When a peer dedup view does not exist yet (or in a single-signal
// deployment), the cross-signal view is skipped with a notice rather than
// failing startup, and a later start creates it. An unreadable peer dedup view
// is skipped with a warning because companion views are a convenience and must
// not block export.
//
// The cross-signal views join spans on a window that depends on each row, and
// v_gen_ai_agent_trajectory joins each span to its parent and child spans on
// such a window, so BigQuery cannot use it to skip span partitions and rejects
// every query over these views when the spans table requires a partition
// filter. Unless auto_create.allow_unprunable_views is set, these three views
// are therefore skipped with a warning when the spans table in effect requires
// one: for the traces exporter the existing table, the table it created, or
// the winner of a concurrent creation; for the logs and metrics exporters the
// table's metadata, or traces.require_partition_filter while the table does
// not exist yet. The other companion views are created either way, and this
// never fails startup.
func (e *signalExporter) createCompanionViews(
	ctx context.Context, admin destinationAdmin, effectiveTableMD *bq.TableMetadata,
) error {
	if proto.Equal(e.schema, schema.MetricsTableSchema()) {
		views := schema.MetricsCompanionViewsMetadata(e.cfg.Project, e.cfg.Dataset, e.table)
		preserved, err := e.ensureCompanionViews(ctx, admin,
			slices.Sorted(maps.Keys(views)), views, e.destinationOptions().Labels, false)
		if err != nil {
			return err
		}
		ok, err := e.probePeerDedupView(ctx, admin, "span", e.cfg.Traces.Table, schema.GenAIMetricExemplarsViewName)
		if err != nil || !ok {
			return err
		}
		ok, err = e.peerSpanTableAllowsCrossSignalView(ctx, admin, e.cfg.Traces.Table, schema.GenAIMetricExemplarsViewName)
		if err != nil || !ok {
			return err
		}
		exemplarViews := map[string]*bq.TableMetadata{
			schema.GenAIMetricExemplarsViewName: schema.MetricExemplarsCompanionViewMetadata(
				e.cfg.Project, e.cfg.Dataset, e.cfg.Traces.Table),
		}
		_, err = e.ensureCompanionViews(ctx, admin,
			[]string{schema.GenAIMetricExemplarsViewName}, exemplarViews, e.destinationOptions().Labels, preserved)
		return err
	}
	isSpans := proto.Equal(e.schema, schema.SpansTableSchema())
	spansTable, logsTable := e.table, e.cfg.Logs.Table
	if !isSpans {
		spansTable, logsTable = e.cfg.Traces.Table, e.table
		ok, err := e.probePeerDedupView(ctx, admin, "span", spansTable, schema.GenAILogsViewName)
		if err != nil || !ok {
			return err
		}
		ok, err = e.peerSpanTableAllowsCrossSignalView(ctx, admin, spansTable, schema.GenAILogsViewName)
		if err != nil || !ok {
			return err
		}
	}

	views := schema.CompanionViewsMetadata(e.cfg.Project, e.cfg.Dataset, spansTable, logsTable, isSpans)
	order := slices.Sorted(maps.Keys(views))
	spansRequireFilter := false
	if isSpans {
		order = schema.SpanCompanionViewOrder
		spansRequireFilter = tableRequiresPartitionFilter(effectiveTableMD)
		// ensureCompanionViews passes over an id without metadata, so the other
		// span views are still created when the trajectory view is skipped.
		if e.skipCrossSignalView(spansTable, schema.GenAIAgentTrajectoryViewName, spansRequireFilter) {
			delete(views, schema.GenAIAgentTrajectoryViewName)
		}
	}
	if _, err := e.ensureCompanionViews(ctx, admin, order, views, e.destinationOptions().Labels, false); err != nil {
		return err
	}

	if isSpans {
		ok, err := e.probePeerDedupView(ctx, admin, "log", logsTable, schema.GenAILogsViewName)
		if err != nil {
			return err
		}
		if ok && !e.skipCrossSignalView(spansTable, schema.GenAILogsViewName, spansRequireFilter) {
			logViews := schema.CompanionViewsMetadata(e.cfg.Project, e.cfg.Dataset, spansTable, logsTable, false)
			if _, err := e.ensureCompanionViews(ctx, admin,
				slices.Sorted(maps.Keys(logViews)), logViews, e.effectiveLabels(e.cfg.Logs.Labels), false); err != nil {
				return err
			}
		}

		ok, err = e.probePeerDedupView(ctx, admin, "metric", e.cfg.Metrics.Table, schema.GenAIMetricExemplarsViewName)
		if err != nil || !ok {
			return err
		}
		// v_gen_ai_metrics reads only the metric dedup view, so it is ensured
		// whatever the spans table requires; only the exemplar view joins spans.
		metricViews := schema.MetricsCompanionViewsMetadata(e.cfg.Project, e.cfg.Dataset, e.cfg.Metrics.Table)
		order := []string{schema.GenAIMetricsViewName}
		if !e.skipCrossSignalView(spansTable, schema.GenAIMetricExemplarsViewName, spansRequireFilter) {
			metricViews[schema.GenAIMetricExemplarsViewName] = schema.MetricExemplarsCompanionViewMetadata(
				e.cfg.Project, e.cfg.Dataset, spansTable)
			order = append(order, schema.GenAIMetricExemplarsViewName)
		}
		_, err = e.ensureCompanionViews(ctx, admin, order, metricViews, e.effectiveLabels(e.cfg.Metrics.Labels), false)
		return err
	}
	return nil
}

// tableRequiresPartitionFilter reports whether a table's metadata, in either of
// the two places BigQuery reports it, requires a partition filter on queries.
func tableRequiresPartitionFilter(md *bq.TableMetadata) bool {
	return md != nil && (md.RequirePartitionFilter ||
		(md.TimePartitioning != nil && md.TimePartitioning.RequirePartitionFilter))
}

// skipCrossSignalView reports whether a companion view that joins spansTable on
// a window that depends on each row (a cross-signal view or
// v_gen_ai_agent_trajectory) is skipped: when that table requires a partition
// filter and auto_create.allow_unprunable_views is off, BigQuery would reject
// every query over the view, so it is left out with a warning instead of
// failing startup.
func (e *signalExporter) skipCrossSignalView(spansTable, view string, spansRequireFilter bool) bool {
	if !spansRequireFilter || e.cfg.AutoCreate.AllowUnprunableViews {
		return false
	}
	e.logger.Warn(crossSignalViewSkippedMessage,
		zap.String("view", e.cfg.Project+"."+e.cfg.Dataset+"."+view),
		zap.String("spans_table", e.cfg.Project+"."+e.cfg.Dataset+"."+spansTable))
	return true
}

// crossSignalViewSkippedMessage is the warning logged when a companion view
// that BigQuery cannot prune by span partition (see skipCrossSignalView) is
// skipped because the spans table requires a partition filter.
const crossSignalViewSkippedMessage = "skipping unprunable companion view because the spans table requires a partition filter; " +
	"set auto_create.allow_unprunable_views: true to create it anyway"

// peerSpanTableAllowsCrossSignalView reads the spans table that a cross-signal
// view of the logs or metrics exporter joins, and reports whether to create the
// view. The table's metadata decides whether it requires a partition filter;
// traces.require_partition_filter decides only while the table does not exist
// yet, because it applies only when the table is created. A view over a table
// that requires one is skipped with a warning (see skipCrossSignalView), and so
// is a view over a table whose metadata cannot be read, which is never taken to
// require no filter. Only a context error fails startup.
func (e *signalExporter) peerSpanTableAllowsCrossSignalView(
	ctx context.Context, admin destinationAdmin, peerSpanTable, dependentView string,
) (bool, error) {
	if e.cfg.AutoCreate.AllowUnprunableViews {
		return true, nil
	}
	spanTableID := e.cfg.Project + "." + e.cfg.Dataset + "." + peerSpanTable
	viewName := e.cfg.Project + "." + e.cfg.Dataset + "." + dependentView
	spanMD, err := admin.TableMetadata(ctx, peerSpanTable)
	switch {
	case err == nil:
		return !e.skipCrossSignalView(peerSpanTable, dependentView, tableRequiresPartitionFilter(spanMD)), nil
	case isContextError(err):
		return false, fmt.Errorf("reading span table metadata for %s: %w", spanTableID, err)
	case isNotFound(err):
		return !e.skipCrossSignalView(peerSpanTable, dependentView, e.cfg.Traces.RequirePartitionFilter), nil
	default:
		e.logger.Warn("span table metadata is unreadable; skipping companion view",
			zap.String("view", viewName), zap.String("requires", spanTableID), zap.Error(err))
		return false, nil
	}
}

// ensureCompanionViews creates or reconciles the companion views in order,
// giving each view it creates the labels. A rollup view whose creation fails
// after an earlier companion view was preserved is skipped with a warning
// instead of failing startup: the preserved view may lack a column the rollup
// needs, and a later start can retry.
func (e *signalExporter) ensureCompanionViews(
	ctx context.Context,
	admin destinationAdmin,
	order []string,
	views map[string]*bq.TableMetadata,
	labels map[string]string,
	companionViewsPreserved bool,
) (bool, error) {
	for _, id := range order {
		md, ok := views[id]
		if !ok {
			continue
		}
		if len(labels) > 0 {
			md.Labels = maps.Clone(labels)
		}
		preserved, err := e.ensureView(ctx, admin, "companion view", id, md)
		if err != nil {
			if isRollupView(id) && companionViewsPreserved && !isContextError(err) {
				e.logger.Warn("skipping rollup view creation over preserved companion view",
					zap.String("view", e.cfg.Project+"."+e.cfg.Dataset+"."+id),
					zap.Error(err))
				continue
			}
			return companionViewsPreserved, err
		}
		if preserved {
			companionViewsPreserved = true
		}
	}
	return companionViewsPreserved, nil
}

func isRollupView(id string) bool {
	switch id {
	case schema.GenAIModelCostsViewName,
		schema.GenAISessionsViewName,
		schema.GenAIToolUsageViewName,
		schema.GenAIAgentTrajectoryViewName,
		schema.GenAIMetricExemplarsViewName,
		schema.MCPToolUsageViewName:
		return true
	default:
		return false
	}
}

func (e *signalExporter) probePeerDedupView(
	ctx context.Context, admin destinationAdmin, peerSignal, peerTable, dependentView string,
) (bool, error) {
	peerView := e.cfg.Project + "." + e.cfg.Dataset + "." + peerTable + "_dedup"
	viewName := e.cfg.Project + "." + e.cfg.Dataset + "." + dependentView
	if _, err := admin.TableMetadata(ctx, peerTable+"_dedup"); err != nil {
		switch {
		case isContextError(err):
			return false, fmt.Errorf("reading %s deduplication view metadata for %s: %w", peerSignal, peerView, err)
		case isNotFound(err):
			e.logger.Info(peerSignal+" deduplication view does not exist; skipping companion view until a later start",
				zap.String("view", viewName), zap.String("requires", peerView))
		default:
			e.logger.Warn(peerSignal+" deduplication view metadata is unreadable; skipping companion view",
				zap.String("view", viewName), zap.String("requires", peerView), zap.Error(err))
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
		KMSKeyName:              e.effectiveKMSKeyName(e.cfg.Quarantine.KMSKeyName),
		Labels:                  e.effectiveLabels(e.cfg.Quarantine.Labels),
		MaxStaleness:            mustParseDurationOrZero(e.cfg.Quarantine.MaxStaleness),
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

func isPreconditionFailed(err error) bool {
	return errorCodeIs(err, stdhttp.StatusPreconditionFailed, codes.FailedPrecondition)
}

func isSearchIndexAlreadyExists(err error) bool {
	if isAlreadyExists(err) {
		return true
	}
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate search index")
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
