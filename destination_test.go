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

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
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
	if !equalStrings(admin.tableReadIDs, []string{cfg.Traces.Table}) {
		t.Errorf("table reads = %v, want only the destination table probe", admin.tableReadIDs)
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

// v_gen_ai_logs joins the span dedup view. A logs exporter that cannot see
// that view skips the companion view and still starts, unless the probe ran
// out of time, which fails startup like every other metadata read.
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
		t.Run(tc.name, func(t *testing.T) {
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
	}{
		{
			name: "already exists",
			createErrs: map[string]error{
				"otel_spans_as_allspans": alreadyExists(),
				"v_gen_ai_spans":         status.Error(codes.AlreadyExists, "existing view"),
			},
		},
		{
			name:       "fails",
			createErrs: map[string]error{"v_gen_ai_spans": createErr},
			wantErr:    createErr,
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
			if tc.wantErr != nil && !strings.Contains(err.Error(), "my-project.otel.v_gen_ai_spans") {
				t.Errorf("error %q must name the companion view", err)
			}
		})
	}
}

func TestClientOptionsSeparateMetadataFromStorageEndpoint(t *testing.T) {
	cfg := validConfig()
	cfg.Credentials.File = "/tmp/identity.json"
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
	if len(writerOpts) != 4 {
		t.Fatalf("writer options = %d, want credential, explicit scope, endpoint, and transport", len(writerOpts))
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
	if len(metadataOpts) != 1 || len(writerOpts) != 1 {
		t.Fatalf("ADC options metadata=%d writer=%d, want one explicit scope for each", len(metadataOpts), len(writerOpts))
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
	if len(opts) != 3 {
		t.Fatalf("no-auth writer options = %d, want only endpoint, transport, and WithoutAuthentication", len(opts))
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
