// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	stdhttp "net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
)

type fakeDestinationAdmin struct {
	datasetMetadata    *bq.DatasetMetadata
	datasetMetadataErr error
	tableMetadata      *bq.TableMetadata
	tableMetadataErr   error
	createDatasetErr   error
	createTableErr     error
	createViewErr      error
	updateTableErr     error
	runDDLErr          error
	closeErr           error
	blockDatasetRead   bool
	// tableMetadataByID answers reads of the listed IDs ahead of, and
	// without consuming, the shared tableResponses sequence.
	tableMetadataByID map[string]fakeTableMetadataResponse
	// tableSequenceByID answers successive reads of the listed IDs with the
	// next response in order, repeating the last one once it is the only one
	// left. It is consulted ahead of tableMetadataByID.
	tableSequenceByID map[string][]fakeTableMetadataResponse
	// createErrByID overrides the creation result for the listed IDs.
	createErrByID map[string]error
	// updateErrByID overrides the update result for the listed IDs.
	updateErrByID map[string]error
	// updateSequenceByID answers successive updates of the listed IDs with the
	// next result in order, repeating the last one once it is the only one
	// left. It is consulted ahead of updateErrByID.
	updateSequenceByID map[string][]error

	createdDataset    *bq.DatasetMetadata
	createdTables     map[string]*bq.TableMetadata
	createdTableOrder []string
	updatedTables     map[string]bq.TableMetadataToUpdate
	updatedETags      map[string]string
	updates           []fakeUpdateCall
	ranDDLs           []string
	tableResponses    []fakeTableMetadataResponse
	tableReads        int
	tableReadIDs      []string
}

type fakeTableMetadataResponse struct {
	metadata *bq.TableMetadata
	err      error
}

// fakeUpdateCall records one UpdateTable call.
type fakeUpdateCall struct {
	id   string
	etag string
	md   bq.TableMetadataToUpdate
}

func (f *fakeDestinationAdmin) DatasetMetadata(ctx context.Context) (*bq.DatasetMetadata, error) {
	if f.blockDatasetRead {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.datasetMetadata, f.datasetMetadataErr
}

func (f *fakeDestinationAdmin) CreateDataset(_ context.Context, md *bq.DatasetMetadata) error {
	f.createdDataset = md
	return f.createDatasetErr
}

func (f *fakeDestinationAdmin) TableMetadata(_ context.Context, id string) (*bq.TableMetadata, error) {
	f.tableReadIDs = append(f.tableReadIDs, id)
	if sequence := f.tableSequenceByID[id]; len(sequence) > 0 {
		response := sequence[0]
		if len(sequence) > 1 {
			f.tableSequenceByID[id] = sequence[1:]
		}
		return response.metadata, response.err
	}
	if response, ok := f.tableMetadataByID[id]; ok {
		return response.metadata, response.err
	}
	if f.tableReads < len(f.tableResponses) {
		response := f.tableResponses[f.tableReads]
		f.tableReads++
		return response.metadata, response.err
	}
	f.tableReads++
	return f.tableMetadata, f.tableMetadataErr
}

func (f *fakeDestinationAdmin) CreateTable(_ context.Context, id string, md *bq.TableMetadata) error {
	if f.createdTables == nil {
		f.createdTables = make(map[string]*bq.TableMetadata)
	}
	f.createdTables[id] = md
	f.createdTableOrder = append(f.createdTableOrder, id)
	if err, ok := f.createErrByID[id]; ok {
		return err
	}
	if strings.HasSuffix(id, "_dedup") {
		return f.createViewErr
	}
	return f.createTableErr
}

func (f *fakeDestinationAdmin) UpdateTable(_ context.Context, id string, md bq.TableMetadataToUpdate, etag string) error {
	if f.updatedTables == nil {
		f.updatedTables = make(map[string]bq.TableMetadataToUpdate)
	}
	if f.updatedETags == nil {
		f.updatedETags = make(map[string]string)
	}
	f.updatedTables[id] = md
	f.updatedETags[id] = etag
	f.updates = append(f.updates, fakeUpdateCall{id: id, etag: etag, md: md})
	if sequence := f.updateSequenceByID[id]; len(sequence) > 0 {
		err := sequence[0]
		if len(sequence) > 1 {
			f.updateSequenceByID[id] = sequence[1:]
		}
		return err
	}
	if err, ok := f.updateErrByID[id]; ok {
		return err
	}
	return f.updateTableErr
}

func (f *fakeDestinationAdmin) RunDDL(_ context.Context, sql string) error {
	f.ranDDLs = append(f.ranDDLs, sql)
	return f.runDDLErr
}

func (f *fakeDestinationAdmin) Close() error { return f.closeErr }

func newTestSignalExporter(cfg *Config, table string, tableSchema *storagepb.TableSchema) *signalExporter {
	return newSignalExporter(componentTelemetryForTest(), cfg, table, tableSchema)
}

func componentTelemetryForTest() component.TelemetrySettings {
	return component.TelemetrySettings{Logger: zap.NewNop()}
}

func notFound() error      { return &googleapi.Error{Code: 404, Message: "not found"} }
func alreadyExists() error { return &googleapi.Error{Code: 409, Message: "already exists"} }
func forbidden() error     { return &googleapi.Error{Code: 403, Message: "forbidden"} }

func TestPrepareDestinationMissingDatasetRequiresAutoCreate(t *testing.T) {
	cfg := validConfig()
	admin := &fakeDestinationAdmin{datasetMetadataErr: notFound()}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	err := e.prepareDestination(context.Background(), admin)
	if err == nil || !strings.Contains(err.Error(), "auto_create.dataset") {
		t.Fatalf("error = %v, want clear auto_create.dataset instruction", err)
	}
	if admin.createdDataset != nil {
		t.Fatal("dataset creation must stay disabled")
	}
}

func TestPrepareDestinationCreatesDatasetInConfiguredLocation(t *testing.T) {
	cfg := validConfig()
	cfg.Location = "EU"
	cfg.AutoCreate.Dataset = true
	admin := &fakeDestinationAdmin{
		datasetMetadataErr: notFound(),
		tableMetadata:      &bq.TableMetadata{Schema: mustBigQuerySchema(t, schema.SpansTableSchema())},
	}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	if admin.createdDataset == nil || admin.createdDataset.Location != "EU" {
		t.Fatalf("created dataset = %#v, want Location EU", admin.createdDataset)
	}
}

func TestPrepareDestinationTreatsAlreadyExistsAsIdempotent(t *testing.T) {
	cfg := validConfig()
	cfg.Location = "US"
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	admin := &fakeDestinationAdmin{
		datasetMetadataErr: notFound(),
		tableResponses: []fakeTableMetadataResponse{
			{err: notFound()},
			{metadata: &bq.TableMetadata{Schema: mustBigQuerySchema(t, schema.LogsTableSchema())}},
		},
		createDatasetErr: alreadyExists(),
		createTableErr:   alreadyExists(),
		createViewErr:    alreadyExists(),
	}
	e := newTestSignalExporter(cfg, cfg.Logs.Table, schema.LogsTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatalf("idempotent create returned %v", err)
	}
}

func TestPrepareDestinationValidatesConcurrentCreateWinner(t *testing.T) {
	cfg := validConfig()
	cfg.AutoCreate.Tables = true
	wrong := mustBigQuerySchema(t, schema.SpansTableSchema())
	wrong[0].Type = bq.StringFieldType
	admin := &fakeDestinationAdmin{
		datasetMetadata: &bq.DatasetMetadata{},
		tableResponses: []fakeTableMetadataResponse{
			{err: notFound()},
			{metadata: &bq.TableMetadata{Schema: wrong}},
		},
		createTableErr: alreadyExists(),
	}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	err := e.prepareDestination(context.Background(), admin)
	if err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("error = %v, want concurrent winner schema mismatch", err)
	}
	if admin.tableReads != 2 {
		t.Fatalf("table metadata reads = %d, want initial probe and winner validation", admin.tableReads)
	}
}

func TestPrepareDestinationMissingTableRequiresAutoCreate(t *testing.T) {
	cfg := validConfig()
	admin := &fakeDestinationAdmin{
		datasetMetadata:  &bq.DatasetMetadata{},
		tableMetadataErr: notFound(),
	}
	e := newTestSignalExporter(cfg, cfg.Logs.Table, schema.LogsTableSchema())

	err := e.prepareDestination(context.Background(), admin)
	if err == nil || !strings.Contains(err.Error(), "auto_create.tables") {
		t.Fatalf("error = %v, want clear auto_create.tables instruction", err)
	}
}

func TestPrepareDestinationCreatesOnlyActiveSignalTableAndView(t *testing.T) {
	tests := []struct {
		name          string
		table         string
		tableSchema   *storagepb.TableSchema
		otherTable    string
		partitionType bq.TimePartitioningType
		partition     string
		cluster       []string
	}{
		{"traces", defaultSpansTable, schema.SpansTableSchema(), defaultLogsTable, bq.HourPartitioningType, "start_time", []string{"service_name", "trace_id", "name"}},
		{"logs", defaultLogsTable, schema.LogsTableSchema(), defaultSpansTable, bq.DayPartitioningType, "timestamp", []string{"service_name", "trace_id", "severity_number"}},
		{"metrics", defaultMetricsTable, schema.MetricsTableSchema(), defaultSpansTable, bq.DayPartitioningType, "timestamp", []string{"service_name", "metric_name", "metric_type"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.AutoCreate.Tables = true
			admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataErr: notFound()}
			e := newTestSignalExporter(cfg, tc.table, tc.tableSchema)

			if err := e.prepareDestination(context.Background(), admin); err != nil {
				t.Fatal(err)
			}
			tableMD := admin.createdTables[tc.table]
			if tableMD == nil {
				t.Fatalf("active table %q was not created", tc.table)
			}
			if _, exists := admin.createdTables[tc.otherTable]; exists {
				t.Fatalf("inactive table %q was created", tc.otherTable)
			}
			if tp := tableMD.TimePartitioning; tp == nil || tp.Field != tc.partition || tp.Type != tc.partitionType {
				t.Fatalf("partitioning = %#v, want type %q on field %q", tp, tc.partitionType, tc.partition)
			}
			if got := clusteringFields(tableMD); !equalStrings(got, tc.cluster) {
				t.Fatalf("clustering = %v, want %v", got, tc.cluster)
			}
			viewMD := admin.createdTables[tc.table+"_dedup"]
			if viewMD == nil || !strings.Contains(viewMD.ViewQuery, "`my-project.otel."+tc.table+"`") {
				t.Fatalf("dedup view = %#v, want canonical query for active table", viewMD)
			}
		})
	}
}

// An existing table is never replaced, and an existing object under a view's
// name that is not a view (the fake answers every read with the table's
// schema-only metadata) is never updated. Stale views are reconciled instead;
// see TestPrepareDestinationReconcilesExistingViews.
func TestPrepareDestinationDoesNotReplaceExistingTableOrNonViewObject(t *testing.T) {
	cfg := validConfig()
	cfg.AutoCreate.Tables = true
	admin := &fakeDestinationAdmin{
		datasetMetadata: &bq.DatasetMetadata{},
		tableMetadata:   &bq.TableMetadata{Schema: mustBigQuerySchema(t, schema.SpansTableSchema())},
		createViewErr:   alreadyExists(),
	}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	if _, ok := admin.createdTables[cfg.Traces.Table]; ok {
		t.Fatal("existing table must not be replaced")
	}
	if len(admin.updatedTables) > 0 {
		t.Fatalf("an existing object that is not a view must not be updated, updated = %#v", admin.updatedTables)
	}
}

func TestPrepareDestinationRejectsTypeModeAndNestedSchemaDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(bq.Schema)
	}{
		{"type", func(s bq.Schema) { s[0].Type = bq.StringFieldType }},
		{"mode", func(s bq.Schema) { s[0].Required = false }},
		{"nested", mutateField("events", func(f *bq.FieldSchema) { f.Schema[0].Type = bq.IntegerFieldType })},
		{"nested struct", mutateField("status", func(f *bq.FieldSchema) { f.Schema[1].Type = bq.IntegerFieldType })},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			actual := mustBigQuerySchema(t, schema.SpansTableSchema())
			tc.mutate(actual)
			admin := &fakeDestinationAdmin{
				datasetMetadata: &bq.DatasetMetadata{},
				tableMetadata:   &bq.TableMetadata{Schema: actual},
			}
			e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

			err := e.prepareDestination(context.Background(), admin)
			if err == nil || !strings.Contains(err.Error(), "schema") {
				t.Fatalf("error = %v, want schema mismatch", err)
			}
		})
	}
}

func TestPrepareDestinationPermissionAmbiguityDefersWhenCreationIsDisabled(t *testing.T) {
	cfg := validConfig()
	admin := &fakeDestinationAdmin{datasetMetadataErr: forbidden(), tableMetadataErr: forbidden()}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatalf("ambiguous metadata permission must defer to real append: %v", err)
	}
	if admin.createdDataset != nil || len(admin.createdTables) != 0 {
		t.Fatal("permission ambiguity must not trigger creation when creation is disabled")
	}
}

func TestPrepareDestinationUnreadableTableAttemptsEnabledCreation(t *testing.T) {
	cfg := validConfig()
	cfg.AutoCreate.Tables = true
	createErr := status.Error(codes.PermissionDenied, "cannot create")
	admin := &fakeDestinationAdmin{
		datasetMetadata:  &bq.DatasetMetadata{},
		tableMetadataErr: forbidden(),
		createTableErr:   createErr,
	}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	err := e.prepareDestination(context.Background(), admin)
	if !errors.Is(err, createErr) {
		t.Fatalf("error = %v, want table creation failure %v", err, createErr)
	}
	if admin.createdTables[cfg.Traces.Table] == nil {
		t.Fatal("enabled idempotent table creation was not attempted after an unreadable metadata probe")
	}
}

func TestPrepareDestinationReturnsNonConflictCreationFailure(t *testing.T) {
	cfg := validConfig()
	cfg.AutoCreate.Tables = true
	createErr := status.Error(codes.Unavailable, "control plane unavailable")
	admin := &fakeDestinationAdmin{
		datasetMetadata:  &bq.DatasetMetadata{},
		tableMetadataErr: notFound(),
		createTableErr:   createErr,
	}
	e := newTestSignalExporter(cfg, cfg.Logs.Table, schema.LogsTableSchema())

	err := e.prepareDestination(context.Background(), admin)
	if !errors.Is(err, createErr) {
		t.Fatalf("error = %v, want non-conflict creation error %v", err, createErr)
	}
}

func TestPrepareDestinationRecognizesGRPCProvisioningCodes(t *testing.T) {
	cfg := validConfig()
	cfg.AutoCreate.Tables = true
	admin := &fakeDestinationAdmin{
		datasetMetadata: &bq.DatasetMetadata{},
		tableResponses: []fakeTableMetadataResponse{
			{err: status.Error(codes.NotFound, "missing")},
			{metadata: &bq.TableMetadata{Schema: mustBigQuerySchema(t, schema.SpansTableSchema())}},
		},
		createTableErr: status.Error(codes.AlreadyExists, "concurrent winner"),
		createViewErr:  status.Error(codes.AlreadyExists, "existing view"),
	}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatalf("gRPC NotFound/AlreadyExists provisioning should be idempotent: %v", err)
	}
	// Three reads: the initial gRPC NotFound, the concurrent-winner
	// validation, and the read of the existing dedup view that startup
	// performs so a stale definition can be reconciled.
	if admin.tableReads != 3 {
		t.Fatalf("table reads = %d, want initial gRPC NotFound, concurrent-winner validation, and the existing dedup view read", admin.tableReads)
	}
}

func companionViewConfig() *Config {
	cfg := validConfig()
	cfg.AutoCreate.Tables = true
	cfg.AutoCreate.CompanionViews = true
	return cfg
}

// Without auto_create.companion_views, startup creates only the table and its
// dedup view, and never probes the other signal's dedup view.
func TestPrepareDestinationCompanionViewsAreOptIn(t *testing.T) {
	for _, tc := range []struct {
		name        string
		table       string
		tableSchema *storagepb.TableSchema
	}{
		{"traces", defaultSpansTable, schema.SpansTableSchema()},
		{"logs", defaultLogsTable, schema.LogsTableSchema()},
		{"metrics", defaultMetricsTable, schema.MetricsTableSchema()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.AutoCreate.Tables = true
			admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataErr: notFound()}
			e := newTestSignalExporter(cfg, tc.table, tc.tableSchema)

			if err := e.prepareDestination(context.Background(), admin); err != nil {
				t.Fatal(err)
			}
			if got := slices.Sorted(maps.Keys(admin.createdTables)); !equalStrings(got, []string{tc.table, tc.table + "_dedup"}) {
				t.Errorf("created %v, want only the table and its dedup view", got)
			}
			if !equalStrings(admin.tableReadIDs, []string{tc.table}) {
				t.Errorf("table reads = %v, want only the destination table probe", admin.tableReadIDs)
			}
		})
	}
}

func TestPrepareDestinationCreatesSpanCompanionViews(t *testing.T) {
	cfg := companionViewConfig()
	admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataErr: notFound()}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{
		"otel_spans_as_allspans": fmt.Sprintf(schema.SpansAsAllSpansViewQuery, "my-project", "otel", cfg.Traces.Table),
		"v_gen_ai_spans":         fmt.Sprintf(schema.GenAISpansViewQuery, "my-project", "otel", cfg.Traces.Table),
		"v_gen_ai_model_costs":   fmt.Sprintf(schema.GenAIModelCostsViewQuery, "my-project", "otel"),
		"v_gen_ai_sessions":      fmt.Sprintf(schema.GenAISessionsViewQuery, "my-project", "otel"),
		"v_gen_ai_tool_usage":    fmt.Sprintf(schema.GenAIToolUsageViewQuery, "my-project", "otel"),
	} {
		if md := admin.createdTables[id]; md == nil || md.ViewQuery != want {
			t.Errorf("companion view %s = %#v, want expected view query", id, md)
		}
	}
	if got := len(admin.createdTables); got != 7 {
		t.Errorf("created %d tables and views, want the table, its dedup view and five span companion views", got)
	}
	spansIdx := slices.Index(admin.createdTableOrder, "v_gen_ai_spans")
	for _, rollup := range []string{"v_gen_ai_model_costs", "v_gen_ai_sessions", "v_gen_ai_tool_usage"} {
		if idx := slices.Index(admin.createdTableOrder, rollup); spansIdx < 0 || idx < spansIdx {
			t.Errorf("createdTableOrder = %v, want v_gen_ai_spans before %s", admin.createdTableOrder, rollup)
		}
	}
	if !equalStrings(admin.tableReadIDs, []string{cfg.Traces.Table, cfg.Logs.Table + "_dedup", cfg.Metrics.Table + "_dedup"}) {
		t.Errorf("table reads = %v, want the destination table probe, the log dedup view probe, and the metric dedup view probe", admin.tableReadIDs)
	}
}

func TestPrepareDestinationCreatesLogsCompanionViewOverSpanDedupView(t *testing.T) {
	cfg := companionViewConfig()
	admin := &fakeDestinationAdmin{
		datasetMetadata:  &bq.DatasetMetadata{},
		tableMetadataErr: notFound(),
		tableMetadataByID: map[string]fakeTableMetadataResponse{
			cfg.Traces.Table + "_dedup": {metadata: &bq.TableMetadata{Type: bq.ViewTable}},
		},
	}
	e := newTestSignalExporter(cfg, cfg.Logs.Table, schema.LogsTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(schema.GenAILogsViewQuery, "my-project", "otel", cfg.Logs.Table, cfg.Traces.Table)
	if md := admin.createdTables["v_gen_ai_logs"]; md == nil || md.ViewQuery != want {
		t.Fatalf("v_gen_ai_logs = %#v, want the join of the log and span dedup views", md)
	}
	if got := len(admin.createdTables); got != 3 {
		t.Errorf("created %d tables and views, want the table, its dedup view and v_gen_ai_logs", got)
	}
	if !slices.Contains(admin.tableReadIDs, cfg.Traces.Table+"_dedup") {
		t.Errorf("table reads = %v, want a probe of the span dedup view", admin.tableReadIDs)
	}
}

func TestPrepareDestinationCreatesLogsCompanionViewWhenTracesStartsSecond(t *testing.T) {
	cfg := companionViewConfig()
	admin := &fakeDestinationAdmin{
		datasetMetadata:  &bq.DatasetMetadata{},
		tableMetadataErr: notFound(),
		tableMetadataByID: map[string]fakeTableMetadataResponse{
			cfg.Logs.Table + "_dedup": {metadata: &bq.TableMetadata{Type: bq.ViewTable}},
		},
	}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(schema.GenAILogsViewQuery, "my-project", "otel", cfg.Logs.Table, cfg.Traces.Table)
	if md := admin.createdTables["v_gen_ai_logs"]; md == nil || md.ViewQuery != want {
		t.Fatalf("v_gen_ai_logs = %#v, want traces startup to create v_gen_ai_logs when log dedup view already exists", md)
	}
	if got := len(admin.createdTables); got != 8 {
		t.Errorf("created %d tables and views, want the table, its dedup view, five span companion views, and v_gen_ai_logs", got)
	}
	if !slices.Contains(admin.tableReadIDs, cfg.Logs.Table+"_dedup") {
		t.Errorf("table reads = %v, want a probe of the log dedup view", admin.tableReadIDs)
	}
}

// v_gen_ai_logs joins the span and log dedup views. Whichever exporter starts
// first and cannot see the peer dedup view skips v_gen_ai_logs and still
// starts, unless the probe ran out of time, which fails startup like every
// other metadata read.
func TestPrepareDestinationLogsCompanionViewNeedsSpanDedupView(t *testing.T) {
	for _, tc := range []struct {
		name     string
		probeErr error
		wantErr  error
	}{
		{name: "absent", probeErr: notFound()},
		{name: "unreadable", probeErr: forbidden()},
		{name: "deadline", probeErr: context.DeadlineExceeded, wantErr: context.DeadlineExceeded},
	} {
		t.Run("logs_"+tc.name, func(t *testing.T) {
			cfg := companionViewConfig()
			admin := &fakeDestinationAdmin{
				datasetMetadata:  &bq.DatasetMetadata{},
				tableMetadataErr: notFound(),
				tableMetadataByID: map[string]fakeTableMetadataResponse{
					cfg.Traces.Table + "_dedup": {err: tc.probeErr},
				},
			}
			e := newTestSignalExporter(cfg, cfg.Logs.Table, schema.LogsTableSchema())

			err := e.prepareDestination(context.Background(), admin)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if _, ok := admin.createdTables["v_gen_ai_logs"]; ok {
				t.Error("v_gen_ai_logs must not be created without a readable span dedup view")
			}
			if admin.createdTables[cfg.Logs.Table+"_dedup"] == nil {
				t.Error("the log dedup view must still be created")
			}
		})
		t.Run("traces_"+tc.name, func(t *testing.T) {
			cfg := companionViewConfig()
			admin := &fakeDestinationAdmin{
				datasetMetadata:  &bq.DatasetMetadata{},
				tableMetadataErr: notFound(),
				tableMetadataByID: map[string]fakeTableMetadataResponse{
					cfg.Logs.Table + "_dedup": {err: tc.probeErr},
				},
			}
			e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

			err := e.prepareDestination(context.Background(), admin)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if _, ok := admin.createdTables["v_gen_ai_logs"]; ok {
				t.Error("v_gen_ai_logs must not be created without a readable log dedup view")
			}
			if admin.createdTables["v_gen_ai_spans"] == nil {
				t.Error("span companion views must still be created")
			}
		})
	}
}

// An existing companion object that is not a view (the fake answers every read
// with schema-only metadata) is preserved as-is and never updated; existing
// views are reconciled in TestPrepareDestinationReconcilesExistingViews. Any
// other creation failure fails startup with the view named.
func TestPrepareDestinationCompanionViewCreationResults(t *testing.T) {
	createErr := status.Error(codes.PermissionDenied, "cannot create view")
	for _, tc := range []struct {
		name       string
		createErrs map[string]error
		wantErr    error
		wantView   string
	}{
		{
			name: "already exists preserved",
			createErrs: map[string]error{
				"otel_spans_as_allspans": alreadyExists(),
				"v_gen_ai_spans":         status.Error(codes.AlreadyExists, "existing view"),
				"v_gen_ai_model_costs":   alreadyExists(),
				"v_gen_ai_sessions":      alreadyExists(),
				"v_gen_ai_tool_usage":    alreadyExists(),
				"v_gen_ai_logs":          alreadyExists(),
			},
		},
		{
			name: "preserved companion view allows rollup view failure",
			createErrs: map[string]error{
				"v_gen_ai_spans":       alreadyExists(),
				"v_gen_ai_model_costs": status.Error(codes.InvalidArgument, "column not found in underlying view"),
			},
		},
		{
			name:       "span view fails",
			createErrs: map[string]error{"v_gen_ai_spans": createErr},
			wantErr:    createErr,
			wantView:   "my-project.otel.v_gen_ai_spans",
		},
		{
			name:       "logs view fails on traces startup",
			createErrs: map[string]error{"v_gen_ai_logs": createErr},
			wantErr:    createErr,
			wantView:   "my-project.otel.v_gen_ai_logs",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := companionViewConfig()
			admin := &fakeDestinationAdmin{
				datasetMetadata: &bq.DatasetMetadata{},
				tableMetadata:   &bq.TableMetadata{Schema: mustBigQuerySchema(t, schema.SpansTableSchema())},
				createViewErr:   alreadyExists(),
				createErrByID:   tc.createErrs,
			}
			e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

			err := e.prepareDestination(context.Background(), admin)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != nil && !strings.Contains(err.Error(), tc.wantView) {
				t.Errorf("error %q must name the companion view %q", err, tc.wantView)
			}
			if tc.wantErr == nil {
				if len(admin.updatedTables) > 0 {
					t.Errorf("an existing object that is not a view must not be updated, updated = %#v", admin.updatedTables)
				}
			}
		})
	}
}

// A metrics exporter with auto_create.companion_views creates v_gen_ai_metrics
// over the metric dedup view, after the table and that view. It reads no other
// signal's view, so it needs no peer probe.
func TestPrepareDestinationCreatesMetricsCompanionView(t *testing.T) {
	cfg := companionViewConfig()
	cfg.Labels = map[string]string{"env": "prod"}
	cfg.Metrics.Labels = map[string]string{"signal": "metrics"}
	admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataErr: notFound()}
	e := newTestSignalExporter(cfg, cfg.Metrics.Table, schema.MetricsTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{cfg.Metrics.Table, cfg.Metrics.Table + "_dedup", schema.GenAIMetricsViewName}
	if !equalStrings(admin.createdTableOrder, wantOrder) {
		t.Errorf("created %v, want %v", admin.createdTableOrder, wantOrder)
	}
	want := fmt.Sprintf(schema.GenAIMetricsViewQuery, "my-project", "otel", cfg.Metrics.Table)
	md := admin.createdTables[schema.GenAIMetricsViewName]
	if md == nil || md.ViewQuery != want || md.UseLegacySQL {
		t.Fatalf("v_gen_ai_metrics = %#v, want the standard-SQL view over the metric dedup view", md)
	}
	if !strings.Contains(md.ViewQuery, "`my-project.otel."+cfg.Metrics.Table+"_dedup`") {
		t.Errorf("v_gen_ai_metrics must read the metric dedup view, got %q", md.ViewQuery)
	}
	if wantLabels := map[string]string{"env": "prod", "signal": "metrics"}; !maps.Equal(md.Labels, wantLabels) {
		t.Errorf("v_gen_ai_metrics labels = %v, want %v", md.Labels, wantLabels)
	}
	if !equalStrings(admin.tableReadIDs, []string{cfg.Metrics.Table, cfg.Traces.Table + "_dedup"}) {
		t.Errorf("table reads = %v, want the destination table probe and the span dedup view probe", admin.tableReadIDs)
	}
}

func TestPrepareDestinationCreatesMetricExemplarsCompanionViewOverSpanDedupView(t *testing.T) {
	cfg := companionViewConfig()
	cfg.Labels = map[string]string{"env": "prod"}
	cfg.Metrics.Labels = map[string]string{"signal": "metrics"}
	admin := &fakeDestinationAdmin{
		datasetMetadata:  &bq.DatasetMetadata{},
		tableMetadataErr: notFound(),
		tableMetadataByID: map[string]fakeTableMetadataResponse{
			cfg.Traces.Table + "_dedup": {metadata: &bq.TableMetadata{Type: bq.ViewTable}},
		},
	}
	e := newTestSignalExporter(cfg, cfg.Metrics.Table, schema.MetricsTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{
		cfg.Metrics.Table,
		cfg.Metrics.Table + "_dedup",
		schema.GenAIMetricsViewName,
		schema.GenAIMetricExemplarsViewName,
	}
	if !equalStrings(admin.createdTableOrder, wantOrder) {
		t.Errorf("created %v, want %v", admin.createdTableOrder, wantOrder)
	}
	want := fmt.Sprintf(schema.GenAIMetricExemplarsViewQuery, "my-project", "otel", cfg.Traces.Table)
	md := admin.createdTables[schema.GenAIMetricExemplarsViewName]
	if md == nil || md.ViewQuery != want || md.UseLegacySQL {
		t.Fatalf("v_gen_ai_metric_exemplars = %#v, want the join of v_gen_ai_metrics and the span dedup view", md)
	}
	if wantLabels := map[string]string{"env": "prod", "signal": "metrics"}; !maps.Equal(md.Labels, wantLabels) {
		t.Errorf("v_gen_ai_metric_exemplars labels = %v, want %v", md.Labels, wantLabels)
	}
}

func TestPrepareDestinationCreatesMetricExemplarsCompanionViewWhenTracesStartsSecond(t *testing.T) {
	cfg := companionViewConfig()
	cfg.Labels = map[string]string{"env": "prod"}
	cfg.Metrics.Labels = map[string]string{"signal": "metrics"}
	admin := &fakeDestinationAdmin{
		datasetMetadata:  &bq.DatasetMetadata{},
		tableMetadataErr: notFound(),
		tableMetadataByID: map[string]fakeTableMetadataResponse{
			cfg.Metrics.Table + "_dedup": {metadata: &bq.TableMetadata{Type: bq.ViewTable}},
		},
	}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	wantMetrics := fmt.Sprintf(schema.GenAIMetricsViewQuery, "my-project", "otel", cfg.Metrics.Table)
	if md := admin.createdTables[schema.GenAIMetricsViewName]; md == nil || md.ViewQuery != wantMetrics {
		t.Fatalf("v_gen_ai_metrics = %#v, want traces startup to ensure v_gen_ai_metrics before v_gen_ai_metric_exemplars", md)
	}
	wantExemplars := fmt.Sprintf(schema.GenAIMetricExemplarsViewQuery, "my-project", "otel", cfg.Traces.Table)
	md := admin.createdTables[schema.GenAIMetricExemplarsViewName]
	if md == nil || md.ViewQuery != wantExemplars {
		t.Fatalf("v_gen_ai_metric_exemplars = %#v, want traces startup to create v_gen_ai_metric_exemplars when metric dedup view already exists", md)
	}
	metricsIdx := slices.Index(admin.createdTableOrder, schema.GenAIMetricsViewName)
	exemplarsIdx := slices.Index(admin.createdTableOrder, schema.GenAIMetricExemplarsViewName)
	if metricsIdx < 0 || exemplarsIdx < 0 || exemplarsIdx < metricsIdx {
		t.Errorf("createdTableOrder = %v, want %s before %s",
			admin.createdTableOrder, schema.GenAIMetricsViewName, schema.GenAIMetricExemplarsViewName)
	}
	if wantLabels := map[string]string{"env": "prod", "signal": "metrics"}; !maps.Equal(md.Labels, wantLabels) {
		t.Errorf("v_gen_ai_metric_exemplars labels = %v, want %v", md.Labels, wantLabels)
	}
	if _, ok := admin.createdTables[schema.GenAILogsViewName]; ok {
		t.Error("v_gen_ai_logs must not be created when the log dedup view does not exist")
	}
}

func TestPrepareDestinationMetricExemplarsCompanionViewNeedsPeerDedupView(t *testing.T) {
	for _, tc := range []struct {
		name     string
		probeErr error
		wantErr  error
	}{
		{name: "absent", probeErr: notFound()},
		{name: "unreadable", probeErr: forbidden()},
		{name: "deadline", probeErr: context.DeadlineExceeded, wantErr: context.DeadlineExceeded},
	} {
		t.Run("metrics_"+tc.name, func(t *testing.T) {
			cfg := companionViewConfig()
			admin := &fakeDestinationAdmin{
				datasetMetadata:  &bq.DatasetMetadata{},
				tableMetadataErr: notFound(),
				tableMetadataByID: map[string]fakeTableMetadataResponse{
					cfg.Traces.Table + "_dedup": {err: tc.probeErr},
				},
			}
			e := newTestSignalExporter(cfg, cfg.Metrics.Table, schema.MetricsTableSchema())

			err := e.prepareDestination(context.Background(), admin)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if _, ok := admin.createdTables[schema.GenAIMetricExemplarsViewName]; ok {
				t.Error("v_gen_ai_metric_exemplars must not be created without a readable span dedup view")
			}
			if admin.createdTables[schema.GenAIMetricsViewName] == nil {
				t.Error("v_gen_ai_metrics must still be created")
			}
		})
		t.Run("traces_"+tc.name, func(t *testing.T) {
			cfg := companionViewConfig()
			admin := &fakeDestinationAdmin{
				datasetMetadata:  &bq.DatasetMetadata{},
				tableMetadataErr: notFound(),
				tableMetadataByID: map[string]fakeTableMetadataResponse{
					cfg.Metrics.Table + "_dedup": {err: tc.probeErr},
				},
			}
			e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

			err := e.prepareDestination(context.Background(), admin)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if _, ok := admin.createdTables[schema.GenAIMetricExemplarsViewName]; ok {
				t.Error("v_gen_ai_metric_exemplars must not be created without a readable metric dedup view")
			}
			if admin.createdTables[schema.GenAISpansViewName] == nil {
				t.Error("span companion views must still be created")
			}
		})
	}
}

// reconcileTarget is a view that startup may find already present: a signal's
// deduplication view or one of its companion views.
type reconcileTarget struct {
	name        string
	table       string
	tableSchema *storagepb.TableSchema
	viewID      string
	// query is the definition this release wants for viewID.
	query string
	// peers answers the reads an exporter must pass before it reaches viewID,
	// such as the span dedup view that v_gen_ai_logs joins.
	peers map[string]fakeTableMetadataResponse
}

func reconcileTargets() []reconcileTarget {
	const project, dataset = "my-project", "otel"
	return []reconcileTarget{
		{
			name: "span dedup view", table: defaultSpansTable, tableSchema: schema.SpansTableSchema(),
			viewID: defaultSpansTable + "_dedup",
			query:  fmt.Sprintf(schema.SpansDedupViewQuery, project, dataset, defaultSpansTable),
		},
		{
			name: "log dedup view", table: defaultLogsTable, tableSchema: schema.LogsTableSchema(),
			viewID: defaultLogsTable + "_dedup",
			query:  fmt.Sprintf(schema.LogsDedupViewQuery, project, dataset, defaultLogsTable),
		},
		{
			name: "metric dedup view", table: defaultMetricsTable, tableSchema: schema.MetricsTableSchema(),
			viewID: defaultMetricsTable + "_dedup",
			query:  fmt.Sprintf(schema.MetricsDedupViewQuery, project, dataset, defaultMetricsTable),
		},
		{
			name: "span companion view", table: defaultSpansTable, tableSchema: schema.SpansTableSchema(),
			viewID: schema.GenAISpansViewName,
			query:  fmt.Sprintf(schema.GenAISpansViewQuery, project, dataset, defaultSpansTable),
		},
		{
			name: "log companion view", table: defaultLogsTable, tableSchema: schema.LogsTableSchema(),
			viewID: schema.GenAILogsViewName,
			query:  fmt.Sprintf(schema.GenAILogsViewQuery, project, dataset, defaultLogsTable, defaultSpansTable),
			peers: map[string]fakeTableMetadataResponse{
				defaultSpansTable + "_dedup": {metadata: &bq.TableMetadata{Type: bq.ViewTable}},
			},
		},
		{
			name: "metric companion view", table: defaultMetricsTable, tableSchema: schema.MetricsTableSchema(),
			viewID: schema.GenAIMetricsViewName,
			query:  fmt.Sprintf(schema.GenAIMetricsViewQuery, project, dataset, defaultMetricsTable),
		},
		{
			name: "metric exemplars companion view", table: defaultMetricsTable, tableSchema: schema.MetricsTableSchema(),
			viewID: schema.GenAIMetricExemplarsViewName,
			query:  fmt.Sprintf(schema.GenAIMetricExemplarsViewQuery, project, dataset, defaultSpansTable),
			peers: map[string]fakeTableMetadataResponse{
				defaultSpansTable + "_dedup": {metadata: &bq.TableMetadata{Type: bq.ViewTable}},
			},
		},
	}
}

// A view that already exists is reconciled to the current definition, so a
// collector upgraded to a release with newer view SQL replaces the stale
// definition on its next start. Only the query changes, and nothing is written
// when the view is already current. A view that cannot be read, is not a view,
// or cannot be updated is kept as it is with a warning, since a view must not
// block export; a context error fails startup like every other metadata call.
func TestPrepareDestinationReconcilesExistingViews(t *testing.T) {
	const (
		msgReplaced   = "replaced the stale view definition with this release's"
		msgNotAView   = "existing object is not a view; leaving it as it is"
		msgUnreadable = "existing view is unreadable; leaving it as it is"
		msgNotUpdated = "view definition differs from this release's but could not be updated; leaving it as it is"
		stale         = "SELECT 1 AS stale"
		concurrent    = "SELECT 2 AS changed_concurrently"
	)
	view := func(query, etag string) fakeTableMetadataResponse {
		return fakeTableMetadataResponse{metadata: &bq.TableMetadata{
			Type: bq.ViewTable, ViewQuery: query, ETag: etag,
		}}
	}
	readFails := func(err error) fakeTableMetadataResponse { return fakeTableMetadataResponse{err: err} }
	reads := func(responses ...fakeTableMetadataResponse) func(string) []fakeTableMetadataResponse {
		return func(string) []fakeTableMetadataResponse { return responses }
	}
	preconditionFailed := &googleapi.Error{Code: stdhttp.StatusPreconditionFailed, Message: "Precondition Failed"}
	rejectedQuery := &googleapi.Error{Code: stdhttp.StatusBadRequest, Message: "Unrecognized name: removed_column"}

	scenarios := []struct {
		name string
		// reads are the successive answers to reading the existing view; the
		// wanted definition is passed in for the ones that depend on it.
		reads func(want string) []fakeTableMetadataResponse
		// updates are the results of successive updates of the view.
		updates []error
		// wantETags are the ETags the updates must carry, in order.
		wantETags    []string
		wantErr      error
		wantReplaced bool
		wantWarn     string
	}{
		{
			name: "current definition is left alone",
			reads: func(want string) []fakeTableMetadataResponse {
				return []fakeTableMetadataResponse{view(want, "etag-1")}
			},
		},
		{
			name: "whitespace-only difference is left alone",
			reads: func(want string) []fakeTableMetadataResponse {
				return []fakeTableMetadataResponse{view(strings.Join(strings.Fields(want), "\n"), "etag-1")}
			},
		},
		{
			name:         "stale definition is replaced",
			reads:        reads(view(stale, "etag-1")),
			wantETags:    []string{"etag-1"},
			wantReplaced: true,
		},
		{
			name: "legacy SQL view with the current text is replaced",
			reads: func(want string) []fakeTableMetadataResponse {
				legacy := view(want, "etag-1")
				legacy.metadata.UseLegacySQL = true
				return []fakeTableMetadataResponse{legacy}
			},
			wantETags:    []string{"etag-1"},
			wantReplaced: true,
		},
		{
			name: "object that is not a view is left alone",
			reads: reads(fakeTableMetadataResponse{
				metadata: &bq.TableMetadata{Type: bq.RegularTable, ETag: "etag-1"},
			}),
			wantWarn: msgNotAView,
		},
		{
			name:     "unreadable view is left alone",
			reads:    reads(readFails(forbidden())),
			wantWarn: msgUnreadable,
		},
		{
			name:    "read that runs out of time fails startup",
			reads:   reads(readFails(context.DeadlineExceeded)),
			wantErr: context.DeadlineExceeded,
		},
		{
			name:      "update that is refused is tolerated",
			reads:     reads(view(stale, "etag-1")),
			updates:   []error{forbidden()},
			wantETags: []string{"etag-1"},
			wantWarn:  msgNotUpdated,
		},
		{
			name:      "update that rejects the query is tolerated",
			reads:     reads(view(stale, "etag-1")),
			updates:   []error{rejectedQuery},
			wantETags: []string{"etag-1"},
			wantWarn:  msgNotUpdated,
		},
		{
			name:      "update that is canceled fails startup",
			reads:     reads(view(stale, "etag-1")),
			updates:   []error{context.Canceled},
			wantETags: []string{"etag-1"},
			wantErr:   context.Canceled,
		},
		{
			name: "concurrent collector that already replaced it wins",
			reads: func(want string) []fakeTableMetadataResponse {
				return []fakeTableMetadataResponse{view(stale, "etag-1"), view(want, "etag-2")}
			},
			updates:   []error{preconditionFailed},
			wantETags: []string{"etag-1"},
		},
		{
			name:         "concurrent change is retried once with the fresh ETag",
			reads:        reads(view(stale, "etag-1"), view(concurrent, "etag-2")),
			updates:      []error{preconditionFailed, nil},
			wantETags:    []string{"etag-1", "etag-2"},
			wantReplaced: true,
		},
		{
			name:      "second precondition failure is tolerated",
			reads:     reads(view(stale, "etag-1"), view(concurrent, "etag-2")),
			updates:   []error{preconditionFailed, preconditionFailed},
			wantETags: []string{"etag-1", "etag-2"},
			wantWarn:  msgNotUpdated,
		},
		{
			name:      "read that fails after a precondition failure is tolerated",
			reads:     reads(view(stale, "etag-1"), readFails(forbidden())),
			updates:   []error{preconditionFailed},
			wantETags: []string{"etag-1"},
			wantWarn:  msgUnreadable,
		},
	}

	for _, target := range reconcileTargets() {
		for _, sc := range scenarios {
			t.Run(target.name+"/"+sc.name, func(t *testing.T) {
				cfg := companionViewConfig()
				core, logs := observer.New(zap.InfoLevel)
				e := newSignalExporter(
					component.TelemetrySettings{Logger: zap.New(core)}, cfg, target.table, target.tableSchema)
				admin := &fakeDestinationAdmin{
					datasetMetadata:    &bq.DatasetMetadata{},
					tableMetadataErr:   notFound(),
					tableMetadataByID:  maps.Clone(target.peers),
					tableSequenceByID:  map[string][]fakeTableMetadataResponse{target.viewID: sc.reads(target.query)},
					createErrByID:      map[string]error{target.viewID: alreadyExists()},
					updateSequenceByID: map[string][]error{target.viewID: sc.updates},
				}
				viewName := "my-project.otel." + target.viewID

				err := e.prepareDestination(context.Background(), admin)
				switch {
				case sc.wantErr == nil && err != nil:
					t.Fatalf("prepareDestination = %v, want startup to succeed", err)
				case sc.wantErr != nil && !errors.Is(err, sc.wantErr):
					t.Fatalf("prepareDestination = %v, want %v", err, sc.wantErr)
				case sc.wantErr != nil && !strings.Contains(err.Error(), viewName):
					t.Errorf("error %q must name the view %q", err, viewName)
				}

				wantUpdate := bq.TableMetadataToUpdate{ViewQuery: target.query, UseLegacySQL: false}
				var etags []string
				for _, u := range admin.updates {
					etags = append(etags, u.etag)
					if u.id != target.viewID || !reflect.DeepEqual(u.md, wantUpdate) {
						t.Errorf("update of %s = %#v, want only the query and standard SQL of %s", u.id, u.md, target.viewID)
					}
				}
				if !equalStrings(etags, sc.wantETags) {
					t.Errorf("update ETags = %v, want %v", etags, sc.wantETags)
				}

				if got := logs.FilterMessage(msgReplaced).Len(); (got == 1) != sc.wantReplaced || got > 1 {
					t.Errorf("%d %q notices, want one: %v", got, msgReplaced, sc.wantReplaced)
				}
				warnings := logs.FilterLevelExact(zap.WarnLevel).All()
				switch {
				case sc.wantWarn == "" && len(warnings) != 0:
					t.Errorf("warnings = %v, want none", warnings)
				case sc.wantWarn != "" && (len(warnings) != 1 ||
					warnings[0].Message != sc.wantWarn || warnings[0].ContextMap()["view"] != viewName):
					t.Errorf("warnings = %v, want one %q about %s", warnings, sc.wantWarn, viewName)
				}
			})
		}
	}
}

// A rollup view whose creation fails is tolerated only after a companion view
// it may depend on was preserved as it is: one that could not be read or
// updated, or that is not a view. A view reconciled to the current definition
// is not preserved, so a rollup failure after it fails startup, and a context
// error fails startup either way.
func TestPrepareDestinationRollupFailureToleranceFollowsPreservedViews(t *testing.T) {
	rollupErr := status.Error(codes.InvalidArgument, "column not found in underlying view")
	spansQuery := fmt.Sprintf(schema.GenAISpansViewQuery, "my-project", "otel", defaultSpansTable)
	view := func(query string) fakeTableMetadataResponse {
		return fakeTableMetadataResponse{metadata: &bq.TableMetadata{Type: bq.ViewTable, ViewQuery: query, ETag: "etag-1"}}
	}
	for _, tc := range []struct {
		name        string
		spansView   fakeTableMetadataResponse
		updateErr   error
		rollupErr   error
		wantErr     error
		wantSkipped bool
	}{
		{
			name:        "stale span view that cannot be updated",
			spansView:   view("SELECT 1 AS stale"),
			updateErr:   forbidden(),
			rollupErr:   rollupErr,
			wantSkipped: true,
		},
		{
			name: "span object that is not a view",
			spansView: fakeTableMetadataResponse{
				metadata: &bq.TableMetadata{Type: bq.RegularTable},
			},
			rollupErr:   rollupErr,
			wantSkipped: true,
		},
		{
			name:      "stale span view that is replaced",
			spansView: view("SELECT 1 AS stale"),
			rollupErr: rollupErr,
			wantErr:   rollupErr,
		},
		{
			name:      "current span view",
			spansView: view(spansQuery),
			rollupErr: rollupErr,
			wantErr:   rollupErr,
		},
		{
			name: "context error is never tolerated",
			spansView: fakeTableMetadataResponse{
				metadata: &bq.TableMetadata{Type: bq.RegularTable},
			},
			rollupErr: context.DeadlineExceeded,
			wantErr:   context.DeadlineExceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := companionViewConfig()
			core, logs := observer.New(zap.WarnLevel)
			e := newSignalExporter(
				component.TelemetrySettings{Logger: zap.New(core)}, cfg, cfg.Traces.Table, schema.SpansTableSchema())
			admin := &fakeDestinationAdmin{
				datasetMetadata:   &bq.DatasetMetadata{},
				tableMetadataErr:  notFound(),
				tableMetadataByID: map[string]fakeTableMetadataResponse{schema.GenAISpansViewName: tc.spansView},
				createErrByID: map[string]error{
					schema.GenAISpansViewName:      alreadyExists(),
					schema.GenAIModelCostsViewName: tc.rollupErr,
				},
				updateErrByID: map[string]error{schema.GenAISpansViewName: tc.updateErr},
			}

			err := e.prepareDestination(context.Background(), admin)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			rollupName := "my-project.otel." + schema.GenAIModelCostsViewName
			if tc.wantErr != nil && !strings.Contains(err.Error(), rollupName) {
				t.Errorf("error %q must name the rollup view %q", err, rollupName)
			}
			skipped := logs.FilterMessage("skipping rollup view creation over preserved companion view").Len()
			if (skipped == 1) != tc.wantSkipped || skipped > 1 {
				t.Errorf("%d skipped-rollup warnings, want one: %v", skipped, tc.wantSkipped)
			}
			if tc.wantSkipped && admin.createdTables[schema.GenAISessionsViewName] == nil {
				t.Error("the rollup views after the skipped one must still be created")
			}
		})
	}
}

func quarantineDestinationConfig() *Config {
	cfg := validConfig()
	cfg.Quarantine.Enabled = true
	return cfg
}

// existingSignalTable answers reads of the signal table with a valid table,
// so a test reaches the quarantine table without the signal table's own
// creation path.
func existingSignalTable(
	t *testing.T, table string, tableSchema *storagepb.TableSchema,
) map[string]fakeTableMetadataResponse {
	t.Helper()
	return map[string]fakeTableMetadataResponse{
		table: {metadata: &bq.TableMetadata{Schema: mustBigQuerySchema(t, tableSchema)}},
	}
}

const quarantineTableID = "my-project.otel.otel_quarantine"

// Without quarantine.enabled, startup neither probes nor creates the
// quarantine table, even when it may create tables.
func TestPrepareDestinationQuarantineTableIsOptIn(t *testing.T) {
	cfg := validConfig()
	cfg.AutoCreate.Tables = true
	admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataErr: notFound()}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(admin.tableReadIDs, defaultQuarantineTable) {
		t.Errorf("table reads = %v, want no quarantine table probe", admin.tableReadIDs)
	}
	if _, ok := admin.createdTables[defaultQuarantineTable]; ok {
		t.Error("the quarantine table was created with quarantine disabled")
	}
}

// An existing quarantine table is validated like a signal table and never
// replaced: one that cannot take quarantine rows fails startup by name.
func TestPrepareDestinationValidatesExistingQuarantineTable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(bq.Schema)
		nilMD   bool
		wantErr string
	}{
		{name: "matches"},
		{
			name:    "type drift",
			mutate:  mutateField("error_code", func(f *bq.FieldSchema) { f.Type = bq.IntegerFieldType }),
			wantErr: "quarantine table " + quarantineTableID + " does not match schema",
		},
		{
			name:    "nested drift",
			mutate:  mutateField("payload_ref", func(f *bq.FieldSchema) { f.Schema[0].Type = bq.IntegerFieldType }),
			wantErr: "quarantine table " + quarantineTableID + " does not match schema",
		},
		{name: "nil metadata", nilMD: true, wantErr: "quarantine table " + quarantineTableID + " returned nil metadata"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := quarantineDestinationConfig()
			byID := existingSignalTable(t, cfg.Traces.Table, schema.SpansTableSchema())
			var md *bq.TableMetadata
			if !tc.nilMD {
				actual := mustBigQuerySchema(t, schema.QuarantineTableSchema())
				if tc.mutate != nil {
					tc.mutate(actual)
				}
				md = &bq.TableMetadata{Schema: actual}
			}
			byID[defaultQuarantineTable] = fakeTableMetadataResponse{metadata: md}
			admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataByID: byID}
			e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

			err := e.prepareDestination(context.Background(), admin)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("valid quarantine table rejected: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
			if len(admin.createdTables) != 0 {
				t.Errorf("created %v, want nothing created", slices.Sorted(maps.Keys(admin.createdTables)))
			}
			if !equalStrings(admin.tableReadIDs, []string{cfg.Traces.Table, defaultQuarantineTable}) {
				t.Errorf("table reads = %v, want the signal table then the quarantine table", admin.tableReadIDs)
			}
		})
	}
}

func TestPrepareDestinationMissingQuarantineTableRequiresAutoCreate(t *testing.T) {
	cfg := quarantineDestinationConfig()
	admin := &fakeDestinationAdmin{
		datasetMetadata:   &bq.DatasetMetadata{},
		tableMetadataByID: existingSignalTable(t, cfg.Logs.Table, schema.LogsTableSchema()),
		tableMetadataErr:  notFound(),
	}
	e := newTestSignalExporter(cfg, cfg.Logs.Table, schema.LogsTableSchema())

	err := e.prepareDestination(context.Background(), admin)
	want := "quarantine table " + quarantineTableID + " does not exist; create it or enable auto_create.tables"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
	if len(admin.createdTables) != 0 {
		t.Errorf("created %v with table creation disabled", slices.Sorted(maps.Keys(admin.createdTables)))
	}
}

// Either exporter may create the shared quarantine table: day partitions on
// ingest_time that expire after 30 days, clustered for triage, with no dedup
// view of its own.
func TestPrepareDestinationCreatesQuarantineTable(t *testing.T) {
	for _, tc := range []struct {
		name            string
		table           string
		tableSchema     *storagepb.TableSchema
		quarantineTable string
	}{
		{"traces", defaultSpansTable, schema.SpansTableSchema(), defaultQuarantineTable},
		{"logs", defaultLogsTable, schema.LogsTableSchema(), defaultQuarantineTable},
		{"custom table", defaultSpansTable, schema.SpansTableSchema(), "dead_letters"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := quarantineDestinationConfig()
			cfg.AutoCreate.Tables = true
			cfg.Quarantine.Table = tc.quarantineTable
			admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataErr: notFound()}
			e := newTestSignalExporter(cfg, tc.table, tc.tableSchema)

			if err := e.prepareDestination(context.Background(), admin); err != nil {
				t.Fatal(err)
			}
			md := admin.createdTables[tc.quarantineTable]
			if md == nil {
				t.Fatalf("created %v, want the quarantine table %q", slices.Sorted(maps.Keys(admin.createdTables)), tc.quarantineTable)
			}
			if tp := md.TimePartitioning; tp == nil || tp.Type != bq.DayPartitioningType ||
				tp.Field != "ingest_time" || tp.Expiration != 30*24*time.Hour {
				t.Errorf("partitioning = %#v, want daily on ingest_time expiring after 30 days", tp)
			}
			if got := clusteringFields(md); !equalStrings(got, []string{"signal", "error_code", "service_name"}) {
				t.Errorf("clustering = %v, want [signal error_code service_name]", got)
			}
			if err := validateSchemaOf(md, schema.QuarantineTableSchema(), "created table"); err != nil {
				t.Errorf("created quarantine table fails startup validation: %v", err)
			}
			want := []string{tc.table, tc.table + "_dedup", tc.quarantineTable}
			if got := slices.Sorted(maps.Keys(admin.createdTables)); !equalStrings(got, slices.Sorted(slices.Values(want))) {
				t.Errorf("created %v, want %v and no quarantine dedup view", got, want)
			}
		})
	}
}

// The traces and logs exporters share the quarantine table, so the other one
// can win a creation race. The winner is validated like any existing table.
func TestPrepareDestinationValidatesConcurrentQuarantineCreateWinner(t *testing.T) {
	wrong := mustBigQuerySchema(t, schema.QuarantineTableSchema())
	mutateField("signal", func(f *bq.FieldSchema) { f.Type = bq.IntegerFieldType })(wrong)
	for _, tc := range []struct {
		name    string
		winner  fakeTableMetadataResponse
		wantErr string
		wrapped error
	}{
		{name: "matches", winner: fakeTableMetadataResponse{
			metadata: &bq.TableMetadata{Schema: mustBigQuerySchema(t, schema.QuarantineTableSchema())},
		}},
		{
			name:    "drifted",
			winner:  fakeTableMetadataResponse{metadata: &bq.TableMetadata{Schema: wrong}},
			wantErr: "quarantine table " + quarantineTableID + " does not match schema",
		},
		{
			name:    "unreadable",
			winner:  fakeTableMetadataResponse{err: forbidden()},
			wantErr: "validating concurrently created quarantine table " + quarantineTableID,
			wrapped: forbidden(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := quarantineDestinationConfig()
			cfg.AutoCreate.Tables = true
			admin := &fakeDestinationAdmin{
				datasetMetadata:   &bq.DatasetMetadata{},
				tableMetadataByID: existingSignalTable(t, cfg.Traces.Table, schema.SpansTableSchema()),
				tableResponses:    []fakeTableMetadataResponse{{err: notFound()}, tc.winner},
				createErrByID:     map[string]error{defaultQuarantineTable: alreadyExists()},
			}
			e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

			err := e.prepareDestination(context.Background(), admin)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("valid concurrent winner rejected: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
			if tc.wrapped != nil && !strings.Contains(err.Error(), tc.wrapped.Error()) {
				t.Errorf("error = %v, want it to carry the read failure %v", err, tc.wrapped)
			}
			if admin.tableReads != 2 {
				t.Errorf("quarantine table reads = %d, want the initial probe and the winner validation", admin.tableReads)
			}
		})
	}
}

// Unreadable quarantine metadata defers to the first quarantine append when
// creation is disabled, and otherwise attempts the idempotent creation.
func TestPrepareDestinationUnreadableQuarantineTable(t *testing.T) {
	for _, tc := range []struct {
		name       string
		autoCreate bool
		wantLog    string
	}{
		{
			name:    "creation disabled",
			wantLog: "quarantine table metadata is unreadable; deferring authorization to the first quarantine append",
		},
		{
			name:       "creation enabled",
			autoCreate: true,
			wantLog:    "quarantine table metadata is unreadable; attempting idempotent creation because auto_create.tables is enabled",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := quarantineDestinationConfig()
			cfg.AutoCreate.Tables = tc.autoCreate
			byID := existingSignalTable(t, cfg.Traces.Table, schema.SpansTableSchema())
			byID[defaultQuarantineTable] = fakeTableMetadataResponse{err: forbidden()}
			admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataByID: byID}
			core, logs := observer.New(zap.WarnLevel)
			e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())
			e.logger = zap.New(core)

			if err := e.prepareDestination(context.Background(), admin); err != nil {
				t.Fatalf("unreadable quarantine metadata must not fail startup: %v", err)
			}
			if _, created := admin.createdTables[defaultQuarantineTable]; created != tc.autoCreate {
				t.Errorf("quarantine table created = %v, want %v", created, tc.autoCreate)
			}
			if got := logs.FilterMessage(tc.wantLog).Len(); got != 1 {
				t.Errorf("warnings %q = %d, want 1", tc.wantLog, got)
			}
		})
	}
}

func TestPrepareDestinationQuarantineMetadataDeadlineFailsStartup(t *testing.T) {
	cfg := quarantineDestinationConfig()
	cfg.AutoCreate.Tables = true
	byID := existingSignalTable(t, cfg.Traces.Table, schema.SpansTableSchema())
	byID[defaultQuarantineTable] = fakeTableMetadataResponse{err: context.DeadlineExceeded}
	admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataByID: byID}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	err := e.prepareDestination(context.Background(), admin)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want %v", err, context.DeadlineExceeded)
	}
	if !strings.Contains(err.Error(), "reading quarantine table "+quarantineTableID+" metadata") {
		t.Errorf("error %q must name the quarantine table", err)
	}
	if _, ok := admin.createdTables[defaultQuarantineTable]; ok {
		t.Error("creation was attempted after the metadata read ran out of time")
	}
}

func TestPrepareDestinationReturnsQuarantineCreationFailure(t *testing.T) {
	cfg := quarantineDestinationConfig()
	cfg.AutoCreate.Tables = true
	createErr := status.Error(codes.Unavailable, "control plane unavailable")
	admin := &fakeDestinationAdmin{
		datasetMetadata:  &bq.DatasetMetadata{},
		tableMetadataErr: notFound(),
		createErrByID:    map[string]error{defaultQuarantineTable: createErr},
	}
	e := newTestSignalExporter(cfg, cfg.Logs.Table, schema.LogsTableSchema())

	err := e.prepareDestination(context.Background(), admin)
	if !errors.Is(err, createErr) {
		t.Fatalf("error = %v, want %v", err, createErr)
	}
	if !strings.Contains(err.Error(), "creating quarantine table "+quarantineTableID) {
		t.Errorf("error %q must name the quarantine table", err)
	}
}

// The signal destination is prepared first: when it fails, the quarantine
// table is not touched.
func TestPrepareDestinationSignalFailureSkipsQuarantine(t *testing.T) {
	cfg := quarantineDestinationConfig()
	admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataErr: notFound()}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	err := e.prepareDestination(context.Background(), admin)
	if err == nil || !strings.Contains(err.Error(), "destination table my-project.otel.otel_spans does not exist") {
		t.Fatalf("error = %v, want the missing signal table", err)
	}
	if !equalStrings(admin.tableReadIDs, []string{cfg.Traces.Table}) {
		t.Errorf("table reads = %v, want only the signal table", admin.tableReadIDs)
	}
}

func TestClientOptionsSeparateMetadataFromStorageEndpoint(t *testing.T) {
	cfg := validConfig()
	cfg.Credentials.File = writeCredentialsFile(t, authorizedUserFile)
	cfg.Endpoint.URL = "localhost:9050"
	cfg.Endpoint.Insecure = true
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	metadataOpts, err := e.metadataClientOptions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	writerOpts, err := e.writerClientOptions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(metadataOpts) != 2 {
		t.Fatalf("metadata options = %d, want configured credential and explicit scope", len(metadataOpts))
	}
	if len(writerOpts) != 8 {
		t.Fatalf("writer options = %d, want credential, explicit scope, endpoint, transport, multiplexing, default inflight requests/bytes, and pool size", len(writerOpts))
	}
}

func TestAuthenticationOptionsApplyScopesToADC(t *testing.T) {
	cfg := validConfig()
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	metadataOpts, err := e.metadataClientOptions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	writerOpts, err := e.writerClientOptions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(metadataOpts) != 1 || len(writerOpts) != 5 {
		t.Fatalf("ADC options metadata=%d writer=%d, want 1 metadata scope and 5 writer options (scope, multiplexing, default inflight requests/bytes, pool)", len(metadataOpts), len(writerOpts))
	}
}

func TestWriterClientOptionsWithoutAuthenticationSkipAuthOptions(t *testing.T) {
	cfg := validConfig()
	cfg.Credentials.File = "/credentials-that-must-not-be-used.json"
	cfg.Endpoint.URL = "localhost:9050"
	cfg.Endpoint.Insecure = true
	cfg.Endpoint.WithoutAuthentication = true
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	opts, err := e.writerClientOptions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(opts) != 7 {
		t.Fatalf("no-auth writer options = %d, want endpoint, transport, WithoutAuthentication, multiplexing, default inflight requests/bytes, and pool size", len(opts))
	}
}

func TestWriterClientOptionsMultiplexingKeepaliveAndCompression(t *testing.T) {
	cfg := validConfig()
	cfg.Write.EnableMultiplexing = false
	cfg.Write.ConnectionPoolSize = 0
	cfg.Write.Compression = "gzip"
	cfg.Write.Keepalive = KeepaliveConfig{
		Time:                30 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	opts, err := e.writerClientOptions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// 1 ADC scope + 1 keepalive dial option + 1 gzip compressor dial option = 3.
	if len(opts) != 3 {
		t.Fatalf("writer options = %d, want 3 (ADC scope, keepalive, gzip)", len(opts))
	}
}

func TestMetadataScopesAreLeastPrivilege(t *testing.T) {
	cfg := validConfig()
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())
	if got := e.metadataScopes(); !equalStrings(got, []string{scopeRead}) {
		t.Fatalf("creation disabled scopes = %v, want readonly", got)
	}

	cfg.AutoCreate.Dataset = true
	if got := e.metadataScopes(); !equalStrings(got, []string{bigqueryapi.BigqueryScope}) {
		t.Fatalf("dataset creation scopes = %v, want admin", got)
	}
	cfg.AutoCreate.Dataset = false
	cfg.AutoCreate.Tables = true
	if got := e.metadataScopes(); !equalStrings(got, []string{bigqueryapi.BigqueryScope}) {
		t.Fatalf("table creation scopes = %v, want admin", got)
	}
	cfg.AutoCreate.Tables = false
	cfg.AutoCreate.SearchIndexes = true
	if got := e.metadataScopes(); !equalStrings(got, []string{bigqueryapi.BigqueryScope}) {
		t.Fatalf("search index creation scopes = %v, want admin", got)
	}
}

func TestPrepareDestinationAppliesPartitionLifecycleOptions(t *testing.T) {
	cfg := validConfig()
	cfg.AutoCreate.Tables = true
	cfg.Traces.PartitionExpirationDays = 14
	cfg.Traces.RequirePartitionFilter = true
	cfg.Quarantine.Enabled = true
	cfg.Quarantine.PartitionExpirationDays = 7
	cfg.Quarantine.RequirePartitionFilter = true
	admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataErr: notFound()}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	spansMD := admin.createdTables[cfg.Traces.Table]
	if spansMD == nil || spansMD.TimePartitioning == nil {
		t.Fatalf("spans table metadata = %#v, want TimePartitioning", spansMD)
	}
	if spansMD.TimePartitioning.Expiration != 14*24*time.Hour || !spansMD.RequirePartitionFilter {
		t.Errorf("spans TimePartitioning = %#v, RequirePartitionFilter = %v, want 14d expiration and true", spansMD.TimePartitioning, spansMD.RequirePartitionFilter)
	}
	quarantineMD := admin.createdTables[cfg.Quarantine.Table]
	if quarantineMD == nil || quarantineMD.TimePartitioning == nil {
		t.Fatalf("quarantine table metadata = %#v, want TimePartitioning", quarantineMD)
	}
	if quarantineMD.TimePartitioning.Expiration != 7*24*time.Hour || !quarantineMD.RequirePartitionFilter {
		t.Errorf("quarantine TimePartitioning = %#v, RequirePartitionFilter = %v, want 7d expiration and true", quarantineMD.TimePartitioning, quarantineMD.RequirePartitionFilter)
	}
}

func TestPrepareDestinationReconcilesAdditiveSchemaWithETag(t *testing.T) {
	full := mustBigQuerySchema(t, schema.SpansTableSchema())
	var withoutPayloadRefs bq.Schema
	for _, f := range full {
		if f.Name == "payload_refs" {
			continue
		}
		withoutPayloadRefs = append(withoutPayloadRefs, f)
	}

	t.Run("auto_create.tables enabled patches schema in place with ETag", func(t *testing.T) {
		cfg := validConfig()
		cfg.AutoCreate.Tables = true
		admin := &fakeDestinationAdmin{
			datasetMetadata: &bq.DatasetMetadata{},
			tableMetadata:   &bq.TableMetadata{Schema: withoutPayloadRefs, ETag: "etag-42"},
			createViewErr:   alreadyExists(),
		}
		e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

		if err := e.prepareDestination(context.Background(), admin); err != nil {
			t.Fatalf("prepareDestination failed: %v", err)
		}
		upd, ok := admin.updatedTables[cfg.Traces.Table]
		if !ok || upd.Schema == nil {
			t.Fatalf("updatedTables[%q] = %#v, want additive schema update", cfg.Traces.Table, upd)
		}
		if got := admin.updatedETags[cfg.Traces.Table]; got != "etag-42" {
			t.Errorf("updatedETags[%q] = %q, want etag-42", cfg.Traces.Table, got)
		}
		if err := validateSchemaOf(&bq.TableMetadata{Schema: upd.Schema}, schema.SpansTableSchema(), "reconciled"); err != nil {
			t.Errorf("reconciled schema fails full validation: %v", err)
		}
	})

	t.Run("auto_create.tables disabled fails with clear instruction", func(t *testing.T) {
		cfg := validConfig()
		admin := &fakeDestinationAdmin{
			datasetMetadata: &bq.DatasetMetadata{},
			tableMetadata:   &bq.TableMetadata{Schema: withoutPayloadRefs, ETag: "etag-42"},
		}
		e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

		err := e.prepareDestination(context.Background(), admin)
		if err == nil || !strings.Contains(err.Error(), "payload_refs is missing") {
			t.Fatalf("error = %v, want missing column reported when auto_create.tables is disabled", err)
		}
		if _, ok := admin.updatedTables[cfg.Traces.Table]; ok {
			t.Error("table must not be patched when auto_create.tables is disabled")
		}
	})
}

// TestPrepareDestinationKeepsPolicyTagsThroughSchemaReconcile runs the
// additive schema update over the REST wire against the fake, which stores
// an update's schema exactly as sent. A table that lacks a column gets it
// with every existing policy tag sent back, and a restart against the
// completed schema writes nothing.
func TestPrepareDestinationKeepsPolicyTagsThroughSchemaReconcile(t *testing.T) {
	const tag = "projects/my-project/locations/us/taxonomies/1/policyTags/2"
	defaults := validConfig()
	for _, tc := range []struct {
		name         string
		table        string
		tableSchema  *storagepb.TableSchema
		signalTable  string
		signalSchema *storagepb.TableSchema
		quarantine   bool
		drop         string
		tagged       []string
	}{
		{
			name:         "otel_spans",
			table:        defaults.Traces.Table,
			tableSchema:  schema.SpansTableSchema(),
			signalTable:  defaults.Traces.Table,
			signalSchema: schema.SpansTableSchema(),
			drop:         "payload_refs.object_ref.details",
			tagged:       []string{"status.message", "payload_refs.object_ref.uri"},
		},
		{
			name:         "otel_logs",
			table:        defaults.Logs.Table,
			tableSchema:  schema.LogsTableSchema(),
			signalTable:  defaults.Logs.Table,
			signalSchema: schema.LogsTableSchema(),
			drop:         "record_fingerprint",
			tagged:       []string{"severity_text"},
		},
		{
			name:         "otel_metrics",
			table:        defaults.Metrics.Table,
			tableSchema:  schema.MetricsTableSchema(),
			signalTable:  defaults.Metrics.Table,
			signalSchema: schema.MetricsTableSchema(),
			drop:         "scope_schema_url",
			tagged:       []string{"metric_description"},
		},
		{
			// Whichever signal starts prepares the quarantine table. Here the
			// spans exporter does, after creating its own table.
			name:         "otel_quarantine",
			table:        defaults.Quarantine.Table,
			tableSchema:  schema.QuarantineTableSchema(),
			signalTable:  defaults.Traces.Table,
			signalSchema: schema.SpansTableSchema(),
			quarantine:   true,
			drop:         "exporter_instance",
			tagged:       []string{"raw_payload", "error_message"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := bqfake.NewServer(t)
			wireFakeHooks(t, srv)
			ctx := context.Background()
			cfg := validConfig()
			cfg.AutoCreate.Tables = true
			cfg.Quarantine.Enabled = tc.quarantine
			admin, err := newDestinationAdmin(ctx, cfg.Project, cfg.Dataset)
			if err != nil {
				t.Fatalf("newDestinationAdmin: %v", err)
			}
			t.Cleanup(func() { _ = admin.Close() })

			// The table predates one column and has policy tags on some of
			// the columns it does have.
			srv.SeedDataset(cfg.Project, cfg.Dataset, "US")
			seeded := withoutBigQueryField(t, mustBigQuerySchema(t, tc.tableSchema), tc.drop)
			wantTags := make(map[string][]string, len(tc.tagged))
			for _, path := range tc.tagged {
				bigQueryFieldAt(t, seeded, path).PolicyTags = &bq.PolicyTagList{Names: []string{tag}}
				wantTags[path] = []string{tag}
			}
			if err := admin.CreateTable(ctx, tc.table, &bq.TableMetadata{Schema: seeded}); err != nil {
				t.Fatalf("creating the tagged %s: %v", tc.table, err)
			}
			fakeTable := func() *bigqueryapi.Table {
				t.Helper()
				tbl, ok := srv.Table(cfg.Project, cfg.Dataset, tc.table)
				if !ok {
					t.Fatalf("%s does not exist", tc.table)
				}
				return tbl
			}
			start := func() {
				t.Helper()
				e := newTestSignalExporter(cfg, tc.signalTable, tc.signalSchema)
				if err := e.prepareDestination(ctx, admin); err != nil {
					t.Fatalf("prepareDestination: %v", err)
				}
			}
			seededETag := fakeTable().Etag

			// The first start adds the missing column and sends every
			// existing policy tag back with the rest of the schema.
			start()
			updated := fakeTable()
			if updated.Etag == seededETag {
				t.Fatalf("%s kept ETag %s, want the missing column %s added", tc.table, seededETag, tc.drop)
			}
			if restFieldAt(updated.Schema.Fields, tc.drop) == nil {
				t.Errorf("%s still lacks %s after the schema update", tc.table, tc.drop)
			}
			if got := restPolicyTagsByPath(updated.Schema.Fields); !maps.EqualFunc(got, wantTags, slices.Equal) {
				t.Errorf("policy tags after the schema update = %v, want %v", got, wantTags)
			}

			// A restart against the completed schema writes nothing, so
			// policy tags that want lacks never cause an update.
			start()
			restarted := fakeTable()
			if restarted.Etag != updated.Etag {
				t.Errorf("restart rewrote %s: ETag %s, want %s", tc.table, restarted.Etag, updated.Etag)
			}
			if got := restPolicyTagsByPath(restarted.Schema.Fields); !maps.EqualFunc(got, wantTags, slices.Equal) {
				t.Errorf("policy tags after a restart = %v, want %v", got, wantTags)
			}
		})
	}
}

// withoutBigQueryField returns s without the field at the dotted path, which
// must exist. The records on the way to it are copied, so s is unchanged.
func withoutBigQueryField(t *testing.T, s bq.Schema, path string) bq.Schema {
	t.Helper()
	name, rest, nested := strings.Cut(path, ".")
	out := make(bq.Schema, 0, len(s))
	found := false
	for _, f := range s {
		switch {
		case f.Name != name:
			out = append(out, f)
		case nested:
			copied := *f
			copied.Schema = withoutBigQueryField(t, f.Schema, rest)
			out = append(out, &copied)
			found = true
		default:
			found = true
		}
	}
	if !found {
		t.Fatalf("schema has no field %s", path)
	}
	return out
}

// bigQueryFieldAt returns the field at the dotted path of s, which must
// exist.
func bigQueryFieldAt(t *testing.T, s bq.Schema, path string) *bq.FieldSchema {
	t.Helper()
	name, rest, nested := strings.Cut(path, ".")
	for _, f := range s {
		if f.Name != name {
			continue
		}
		if !nested {
			return f
		}
		return bigQueryFieldAt(t, f.Schema, rest)
	}
	t.Fatalf("schema has no field %s", path)
	return nil
}

// restFieldAt returns the field at the dotted path of fields, or nil if
// there is none.
func restFieldAt(fields []*bigqueryapi.TableFieldSchema, path string) *bigqueryapi.TableFieldSchema {
	name, rest, nested := strings.Cut(path, ".")
	for _, f := range fields {
		if f.Name != name {
			continue
		}
		if !nested {
			return f
		}
		return restFieldAt(f.Fields, rest)
	}
	return nil
}

// restPolicyTagsByPath maps the dotted path of every field that carries
// policy tags, nested fields included, to the tag names.
func restPolicyTagsByPath(fields []*bigqueryapi.TableFieldSchema) map[string][]string {
	out := make(map[string][]string)
	var walk func(prefix string, fields []*bigqueryapi.TableFieldSchema)
	walk = func(prefix string, fields []*bigqueryapi.TableFieldSchema) {
		for _, f := range fields {
			if f.PolicyTags != nil {
				out[prefix+f.Name] = f.PolicyTags.Names
			}
			walk(prefix+f.Name+".", f.Fields)
		}
	}
	walk("", fields)
	return out
}

func TestPrepareDestinationCreatesSearchIndexesWhenEnabled(t *testing.T) {
	for _, tc := range []struct {
		name        string
		table       string
		tableSchema *storagepb.TableSchema
		wantDDL     string
	}{
		{
			name:        "traces",
			table:       defaultSpansTable,
			tableSchema: schema.SpansTableSchema(),
			wantDDL:     fmt.Sprintf(schema.SpansSearchIndexDDL, "my-project", "otel", defaultSpansTable),
		},
		{
			name:        "logs",
			table:       defaultLogsTable,
			tableSchema: schema.LogsTableSchema(),
			wantDDL:     fmt.Sprintf(schema.LogsSearchIndexDDL, "my-project", "otel", defaultLogsTable),
		},
		{
			name:        "metrics",
			table:       defaultMetricsTable,
			tableSchema: schema.MetricsTableSchema(),
			wantDDL:     fmt.Sprintf(schema.MetricsSearchIndexDDL, "my-project", "otel", defaultMetricsTable),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.AutoCreate.Tables = true
			cfg.AutoCreate.SearchIndexes = true
			admin := &fakeDestinationAdmin{datasetMetadata: &bq.DatasetMetadata{}, tableMetadataErr: notFound()}
			e := newTestSignalExporter(cfg, tc.table, tc.tableSchema)

			if err := e.prepareDestination(context.Background(), admin); err != nil {
				t.Fatalf("prepareDestination failed: %v", err)
			}
			if !equalStrings(admin.ranDDLs, []string{tc.wantDDL}) {
				t.Fatalf("ranDDLs = %v, want [%q]", admin.ranDDLs, tc.wantDDL)
			}
		})
	}

	t.Run("search index already exists is treated as idempotent", func(t *testing.T) {
		cfg := validConfig()
		cfg.AutoCreate.Tables = true
		cfg.AutoCreate.SearchIndexes = true
		admin := &fakeDestinationAdmin{
			datasetMetadata:  &bq.DatasetMetadata{},
			tableMetadataErr: notFound(),
			runDDLErr:        &googleapi.Error{Code: 400, Message: "Already exists: Search index idx_otel_metrics_search"},
		}
		e := newTestSignalExporter(cfg, defaultMetricsTable, schema.MetricsTableSchema())
		if err := e.prepareDestination(context.Background(), admin); err != nil {
			t.Fatalf("prepareDestination failed on idempotent search index error: %v", err)
		}
	})
}

func TestReconcileTableSchemaRetriesOnPreconditionFailedAndLogsWarningOnIncompatibleSchema(t *testing.T) {
	fullSpans := mustBigQuerySchema(t, schema.SpansTableSchema())
	var withoutPayloadRefs bq.Schema
	for _, f := range fullSpans {
		if f.Name == "payload_refs" {
			continue
		}
		withoutPayloadRefs = append(withoutPayloadRefs, f)
	}

	t.Run("retries once on HTTP 412 Precondition Failed with refreshed ETag", func(t *testing.T) {
		cfg := validConfig()
		cfg.AutoCreate.Tables = true
		admin := &fakeDestinationAdmin{
			datasetMetadata: &bq.DatasetMetadata{},
			tableResponses: []fakeTableMetadataResponse{
				{metadata: &bq.TableMetadata{Schema: withoutPayloadRefs, ETag: "etag-stale"}},
				{metadata: &bq.TableMetadata{Schema: withoutPayloadRefs, ETag: "etag-fresh"}},
			},
			createViewErr: alreadyExists(),
		}
		var updateCalls int
		admin.updateTableErr = &googleapi.Error{Code: stdhttp.StatusPreconditionFailed, Message: "Precondition Failed"}
		e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

		// Wrap admin so first UpdateTable returns 412 and second succeeds.
		retryingAdmin := &conditionalUpdateAdmin{
			fakeDestinationAdmin: admin,
			onUpdate: func(call int, etag string) error {
				updateCalls = call
				if call == 1 {
					if etag != "etag-stale" {
						t.Errorf("first UpdateTable etag = %q, want etag-stale", etag)
					}
					return &googleapi.Error{Code: stdhttp.StatusPreconditionFailed, Message: "Precondition Failed"}
				}
				if etag != "etag-fresh" {
					t.Errorf("second UpdateTable etag = %q, want etag-fresh", etag)
				}
				return nil
			},
		}

		if err := e.prepareDestination(context.Background(), retryingAdmin); err != nil {
			t.Fatalf("prepareDestination failed after 412 retry: %v", err)
		}
		if updateCalls != 2 {
			t.Fatalf("UpdateTable calls = %d, want 2", updateCalls)
		}
	})

	t.Run("logs warning when additive schema reconciliation is skipped due to incompatible type", func(t *testing.T) {
		cfg := validConfig()
		cfg.AutoCreate.Tables = true
		incompatible := make(bq.Schema, len(withoutPayloadRefs))
		for i, f := range withoutPayloadRefs {
			cp := *f
			incompatible[i] = &cp
		}
		incompatible[0].Type = bq.StringFieldType

		core, recorded := observer.New(zap.WarnLevel)
		set := component.TelemetrySettings{Logger: zap.New(core)}
		e := newSignalExporter(set, cfg, cfg.Traces.Table, schema.SpansTableSchema())
		admin := &fakeDestinationAdmin{
			datasetMetadata: &bq.DatasetMetadata{},
			tableMetadata:   &bq.TableMetadata{Schema: incompatible, ETag: "etag-1"},
		}
		if err := e.prepareDestination(context.Background(), admin); err == nil {
			t.Fatal("expected error for incompatible schema")
		}
		if recorded.FilterMessage("additive schema reconciliation skipped due to incompatible existing schema").Len() != 1 {
			t.Fatalf("expected warning log for skipped additive schema reconciliation, got %v", recorded.All())
		}
	})
}

type conditionalUpdateAdmin struct {
	*fakeDestinationAdmin
	calls    int
	onUpdate func(call int, etag string) error
}

func (c *conditionalUpdateAdmin) UpdateTable(ctx context.Context, id string, md bq.TableMetadataToUpdate, etag string) error {
	c.calls++
	_ = c.fakeDestinationAdmin.UpdateTable(ctx, id, md, etag)
	return c.onUpdate(c.calls, etag)
}

func TestPrepareDestinationAppliesGovernanceOptionsToDatasetTablesAndViews(t *testing.T) {
	cfg := validConfig()
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	cfg.AutoCreate.CompanionViews = true
	cfg.Quarantine.Enabled = true
	cfg.KMSKeyName = "projects/my-project/locations/us/keyRings/ring/cryptoKeys/default-key"
	cfg.Labels = map[string]string{
		"env":   "prod",
		"owner": "platform",
	}
	cfg.Logs.KMSKeyName = "projects/my-project/locations/us/keyRings/ring/cryptoKeys/logs-key"
	cfg.Logs.Labels = map[string]string{
		"owner":  "observability",
		"signal": "logs",
	}
	cfg.Logs.MaxStaleness = "15m"
	cfg.Quarantine.Labels = map[string]string{
		"tier": "quarantine",
	}
	cfg.Quarantine.MaxStaleness = "30m"

	admin := &fakeDestinationAdmin{
		datasetMetadataErr: notFound(),
		tableMetadataErr:   notFound(),
		tableMetadataByID: map[string]fakeTableMetadataResponse{
			defaultSpansTable + "_dedup": {metadata: &bq.TableMetadata{ViewQuery: "SELECT 1"}},
		},
	}
	e := newTestSignalExporter(cfg, cfg.Logs.Table, schema.LogsTableSchema())
	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatalf("prepareDestination failed: %v", err)
	}

	if admin.createdDataset == nil || admin.createdDataset.DefaultEncryptionConfig == nil ||
		admin.createdDataset.DefaultEncryptionConfig.KMSKeyName != cfg.KMSKeyName {
		t.Fatalf("createdDataset DefaultEncryptionConfig = %#v, want KMSKeyName %q", admin.createdDataset, cfg.KMSKeyName)
	}
	if !maps.Equal(admin.createdDataset.Labels, cfg.Labels) {
		t.Errorf("createdDataset.Labels = %v, want %v", admin.createdDataset.Labels, cfg.Labels)
	}

	logsMD := admin.createdTables[cfg.Logs.Table]
	if logsMD == nil || logsMD.EncryptionConfig == nil || logsMD.EncryptionConfig.KMSKeyName != cfg.Logs.KMSKeyName {
		t.Fatalf("logs table EncryptionConfig = %#v, want %q", logsMD, cfg.Logs.KMSKeyName)
	}
	wantLogsLabels := map[string]string{"env": "prod", "owner": "observability", "signal": "logs"}
	if !maps.Equal(logsMD.Labels, wantLogsLabels) {
		t.Errorf("logs table Labels = %v, want %v", logsMD.Labels, wantLogsLabels)
	}
	if logsMD.MaxStaleness == nil || *logsMD.MaxStaleness != *bq.IntervalValueFromDuration(15 * time.Minute) {
		t.Errorf("logs table MaxStaleness = %v, want 15m", logsMD.MaxStaleness)
	}

	dedupMD := admin.createdTables[cfg.Logs.Table+"_dedup"]
	if dedupMD == nil || !maps.Equal(dedupMD.Labels, wantLogsLabels) {
		t.Errorf("dedup view Labels = %v, want %v", dedupMD, wantLogsLabels)
	}
	if dedupMD.EncryptionConfig != nil || dedupMD.MaxStaleness != nil {
		t.Errorf("dedup view must not set EncryptionConfig (%v) or MaxStaleness (%v)", dedupMD.EncryptionConfig, dedupMD.MaxStaleness)
	}
	compMD := admin.createdTables[schema.GenAILogsViewName]
	if compMD == nil || !maps.Equal(compMD.Labels, wantLogsLabels) {
		t.Errorf("companion view Labels = %v, want %v", compMD, wantLogsLabels)
	}
	if compMD.EncryptionConfig != nil || compMD.MaxStaleness != nil {
		t.Errorf("companion view must not set EncryptionConfig (%v) or MaxStaleness (%v)", compMD.EncryptionConfig, compMD.MaxStaleness)
	}

	qMD := admin.createdTables[cfg.Quarantine.Table]
	if qMD == nil || qMD.EncryptionConfig == nil || qMD.EncryptionConfig.KMSKeyName != cfg.KMSKeyName {
		t.Fatalf("quarantine table EncryptionConfig = %#v, want fallback %q", qMD, cfg.KMSKeyName)
	}
	wantQLabels := map[string]string{"env": "prod", "owner": "platform", "tier": "quarantine"}
	if !maps.Equal(qMD.Labels, wantQLabels) {
		t.Errorf("quarantine table Labels = %v, want %v", qMD.Labels, wantQLabels)
	}
	if qMD.MaxStaleness == nil || *qMD.MaxStaleness != *bq.IntervalValueFromDuration(30 * time.Minute) {
		t.Errorf("quarantine table MaxStaleness = %v, want 30m", qMD.MaxStaleness)
	}
}

func TestAuthenticationAndEndpointOptionsSupportEnterpriseCredentials(t *testing.T) {
	t.Run("credentials.json external_account with delegates, quota_project, universe_domain, and admin_url", func(t *testing.T) {
		cfg := validConfig()
		cfg.Credentials.JSON = `{
			"type": "external_account",
			"audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/p/providers/pr",
			"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
			"token_url": "https://sts.googleapis.com/v1/token",
			"credential_source": {"file": "/var/run/secrets/token"}
		}`
		cfg.Credentials.ImpersonateServiceAccount = "writer@my-project.iam.gserviceaccount.com"
		cfg.Credentials.ImpersonateDelegates = []string{"delegate-1@my-project.iam.gserviceaccount.com"}
		cfg.Credentials.QuotaProject = "billing-project"
		cfg.Credentials.UniverseDomain = "apis-tpclp.goog"
		cfg.Endpoint.URL = "localhost:9050"
		cfg.Endpoint.AdminURL = "http://localhost:9051/bigquery/v2/"
		cfg.Endpoint.Insecure = true
		e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

		metaOpts, err := e.metadataClientOptions(context.Background())
		if err != nil {
			t.Fatalf("metadataClientOptions failed: %v", err)
		}
		writerOpts, err := e.writerClientOptions(context.Background())
		if err != nil {
			t.Fatalf("writerClientOptions failed: %v", err)
		}
		// auth: TokenSource + Scopes + QuotaProject + UniverseDomain = 4; metadata adds AdminURL = 5; writer adds URL + transport + multiplexing + inflight requests + inflight bytes + pool = 10.
		if len(metaOpts) != 5 {
			t.Errorf("len(metaOpts) = %d, want 5", len(metaOpts))
		}
		if len(writerOpts) != 10 {
			t.Errorf("len(writerOpts) = %d, want 10", len(writerOpts))
		}
	})

	t.Run("credentials.json authorized_user and impersonated_service_account without target principal", func(t *testing.T) {
		for _, credJSON := range []string{
			`{"type":"authorized_user","client_id":"cid","client_secret":"csec","refresh_token":"rtok"}`,
			`{"type":"impersonated_service_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@p.iam.gserviceaccount.com:generateAccessToken","source_credentials":{"type":"authorized_user","client_id":"cid","client_secret":"csec","refresh_token":"rtok"}}`,
		} {
			cfg := validConfig()
			cfg.Credentials.JSON = configopaque.String(credJSON)
			e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())
			opts, err := e.writerClientOptions(context.Background())
			if err != nil {
				t.Fatalf("writerClientOptions failed: %v", err)
			}
			if len(opts) != 6 {
				t.Errorf("len(opts) = %d, want 6 (TokenSource + Scopes + multiplexing + inflight requests + inflight bytes + pool)", len(opts))
			}
		}
	})

	t.Run("credentials.json rejects unknown credential type", func(t *testing.T) {
		cfg := validConfig()
		cfg.Credentials.JSON = `{"type":"unknown_custom_type"}`
		e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())
		if _, err := e.writerClientOptions(context.Background()); err == nil {
			t.Fatal("expected error for unknown credential type, got nil")
		}
	})
}

func mustBigQuerySchema(t *testing.T, in *storagepb.TableSchema) bq.Schema {
	t.Helper()
	out, err := schema.ToBigQuery(in)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func clusteringFields(md *bq.TableMetadata) []string {
	if md.Clustering == nil {
		return nil
	}
	return md.Clustering.Fields
}

// mutateField returns a schema mutation that applies fn to the top-level
// field called name. If no field has that name the schema is left unchanged,
// so a drift test using it fails rather than passing vacuously.
func mutateField(name string, fn func(*bq.FieldSchema)) func(bq.Schema) {
	return func(s bq.Schema) {
		for _, f := range s {
			if f.Name == name {
				fn(f)
			}
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type roundTripFunc func(*stdhttp.Request) (*stdhttp.Response, error)

func (f roundTripFunc) RoundTrip(req *stdhttp.Request) (*stdhttp.Response, error) {
	return f(req)
}

func TestNewDestinationAdminAppliesAdminURL(t *testing.T) {
	var gotURL string
	origHTTP := newBigQueryHTTPClient
	t.Cleanup(func() { newBigQueryHTTPClient = origHTTP })
	newBigQueryHTTPClient = func(context.Context, ...option.ClientOption) (*stdhttp.Client, *stdhttp.Transport, error) {
		client := &stdhttp.Client{
			Transport: roundTripFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
				gotURL = req.URL.String()
				return &stdhttp.Response{
					StatusCode: stdhttp.StatusOK,
					Header:     stdhttp.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{}`)),
				}, nil
			}),
		}
		return client, &stdhttp.Transport{}, nil
	}

	cfg := validConfig()
	cfg.Endpoint.AdminURL = "https://private.example.test/bigquery/v2/"
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())
	metaOpts, err := e.metadataClientOptions(context.Background())
	if err != nil {
		t.Fatalf("metadataClientOptions() error = %v", err)
	}

	admin, err := newDestinationAdmin(context.Background(), cfg.Project, cfg.Dataset, metaOpts...)
	if err != nil {
		t.Fatalf("newDestinationAdmin() error = %v", err)
	}
	defer func() { _ = admin.Close() }()

	if _, err := admin.DatasetMetadata(context.Background()); err != nil {
		t.Fatalf("DatasetMetadata() error = %v", err)
	}
	wantPrefix := "https://private.example.test/bigquery/v2/projects/my-project/datasets/otel"
	if !strings.HasPrefix(gotURL, wantPrefix) {
		t.Fatalf("DatasetMetadata request URL = %q, want prefix %q", gotURL, wantPrefix)
	}
}

func TestNewDestinationAdminAppliesUniverseDomain(t *testing.T) {
	origHTTP := newBigQueryHTTPClient
	t.Cleanup(func() { newBigQueryHTTPClient = origHTTP })
	newBigQueryHTTPClient = func(context.Context, ...option.ClientOption) (*stdhttp.Client, *stdhttp.Transport, error) {
		client := &stdhttp.Client{
			Transport: roundTripFunc(func(_ *stdhttp.Request) (*stdhttp.Response, error) {
				return &stdhttp.Response{
					StatusCode: stdhttp.StatusOK,
					Header:     stdhttp.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{}`)),
				}, nil
			}),
		}
		return client, &stdhttp.Transport{}, nil
	}

	cfg := validConfig()
	cfg.Credentials.UniverseDomain = "my-universe.goog"
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())
	metaOpts, err := e.metadataClientOptions(context.Background())
	if err != nil {
		t.Fatalf("metadataClientOptions() error = %v", err)
	}

	admin, err := newDestinationAdmin(context.Background(), cfg.Project, cfg.Dataset, metaOpts...)
	if err != nil {
		t.Fatalf("newDestinationAdmin() error = %v", err)
	}
	defer func() { _ = admin.Close() }()

	if _, err := admin.DatasetMetadata(context.Background()); err != nil {
		t.Fatalf("DatasetMetadata() error = %v", err)
	}
}
