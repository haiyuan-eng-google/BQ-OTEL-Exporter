// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

type fakeDestinationAdmin struct {
	datasetMetadata    *bq.DatasetMetadata
	datasetMetadataErr error
	tableMetadata      *bq.TableMetadata
	tableMetadataErr   error
	createDatasetErr   error
	createTableErr     error
	createViewErr      error
	closeErr           error
	blockDatasetRead   bool
	// tableMetadataByID answers reads of the listed IDs ahead of, and
	// without consuming, the shared tableResponses sequence.
	tableMetadataByID map[string]fakeTableMetadataResponse
	// createErrByID overrides the creation result for the listed IDs.
	createErrByID map[string]error

	createdDataset *bq.DatasetMetadata
	createdTables  map[string]*bq.TableMetadata
	tableResponses []fakeTableMetadataResponse
	tableReads     int
	tableReadIDs   []string
}

type fakeTableMetadataResponse struct {
	metadata *bq.TableMetadata
	err      error
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
	if err, ok := f.createErrByID[id]; ok {
		return err
	}
	if strings.HasSuffix(id, "_dedup") {
		return f.createViewErr
	}
	return f.createTableErr
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

func TestPrepareDestinationDoesNotReplaceExistingView(t *testing.T) {
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
	if admin.tableReads != 2 {
		t.Fatalf("table reads = %d, want initial gRPC NotFound and concurrent-winner validation", admin.tableReads)
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
	for id, tmpl := range map[string]string{
		"otel_spans_as_allspans": schema.SpansAsAllSpansViewQuery,
		"v_gen_ai_spans":         schema.GenAISpansViewQuery,
	} {
		want := fmt.Sprintf(tmpl, "my-project", "otel", cfg.Traces.Table)
		if md := admin.createdTables[id]; md == nil || md.ViewQuery != want {
			t.Errorf("companion view %s = %#v, want a view over the span dedup view", id, md)
		}
	}
	if got := len(admin.createdTables); got != 4 {
		t.Errorf("created %d tables and views, want the table, its dedup view and two companion views", got)
	}
	if !equalStrings(admin.tableReadIDs, []string{cfg.Traces.Table, cfg.Logs.Table + "_dedup"}) {
		t.Errorf("table reads = %v, want the destination table probe and the log dedup view probe", admin.tableReadIDs)
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
	if got := len(admin.createdTables); got != 5 {
		t.Errorf("created %d tables and views, want the table, its dedup view, two span companion views, and v_gen_ai_logs", got)
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

// Like the dedup view, companion views are create-only: one that already
// exists is left alone, and any other creation failure fails startup with the
// view named.
func TestPrepareDestinationCompanionViewCreationResults(t *testing.T) {
	createErr := status.Error(codes.PermissionDenied, "cannot create view")
	for _, tc := range []struct {
		name       string
		createErrs map[string]error
		wantErr    error
		wantView   string
	}{
		{
			name: "already exists",
			createErrs: map[string]error{
				"otel_spans_as_allspans": alreadyExists(),
				"v_gen_ai_spans":         status.Error(codes.AlreadyExists, "existing view"),
				"v_gen_ai_logs":          alreadyExists(),
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
