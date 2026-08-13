// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"

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

	createdDataset *bq.DatasetMetadata
	createdTables  map[string]*bq.TableMetadata
	tableResponses []fakeTableMetadataResponse
	tableReads     int
}

type fakeTableMetadataResponse struct {
	metadata *bq.TableMetadata
	err      error
}

func (f *fakeDestinationAdmin) DatasetMetadata(context.Context) (*bq.DatasetMetadata, error) {
	return f.datasetMetadata, f.datasetMetadataErr
}

func (f *fakeDestinationAdmin) CreateDataset(_ context.Context, md *bq.DatasetMetadata) error {
	f.createdDataset = md
	return f.createDatasetErr
}

func (f *fakeDestinationAdmin) TableMetadata(context.Context, string) (*bq.TableMetadata, error) {
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
	return f.createTableErr
}

func (f *fakeDestinationAdmin) CreateView(_ context.Context, id, query string) error {
	if f.createdTables == nil {
		f.createdTables = make(map[string]*bq.TableMetadata)
	}
	f.createdTables[id] = &bq.TableMetadata{ViewQuery: query}
	return f.createViewErr
}

func (f *fakeDestinationAdmin) Close() error { return nil }

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
		name        string
		table       string
		tableSchema *storagepb.TableSchema
		otherTable  string
		partition   string
		cluster     []string
	}{
		{"traces", defaultSpansTable, schema.SpansTableSchema(), defaultLogsTable, "start_timestamp", []string{"service_name", "name"}},
		{"logs", defaultLogsTable, schema.LogsTableSchema(), defaultSpansTable, "timestamp", nil},
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
			if tableMD.TimePartitioning == nil || tableMD.TimePartitioning.Field != tc.partition {
				t.Fatalf("partitioning = %#v, want field %q", tableMD.TimePartitioning, tc.partition)
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
		{"nested", func(s bq.Schema) { s[len(s)-1].Schema[0].Type = bq.IntegerFieldType }},
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

func TestPrepareDestinationPermissionAmbiguityDoesNotBlockWritesOrCreate(t *testing.T) {
	cfg := validConfig()
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	cfg.Location = "US"
	admin := &fakeDestinationAdmin{datasetMetadataErr: forbidden(), tableMetadataErr: forbidden()}
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())

	if err := e.prepareDestination(context.Background(), admin); err != nil {
		t.Fatalf("ambiguous metadata permission must defer to real append: %v", err)
	}
	if admin.createdDataset != nil || len(admin.createdTables) != 0 {
		t.Fatal("permission ambiguity must not be mistaken for a missing resource")
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
	if len(metadataOpts) != 1 {
		t.Fatalf("metadata options = %d, want only the configured credential", len(metadataOpts))
	}
	if len(writerOpts) != 3 {
		t.Fatalf("writer options = %d, want credential plus endpoint and transport", len(writerOpts))
	}
}

func TestMetadataScopesAreLeastPrivilege(t *testing.T) {
	cfg := validConfig()
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())
	if got := e.metadataScopes(); !equalStrings(got, []string{scopeRead}) {
		t.Fatalf("creation disabled scopes = %v, want readonly", got)
	}

	cfg.AutoCreate.Dataset = true
	if got := e.metadataScopes(); !equalStrings(got, []string{scopeAdmin}) {
		t.Fatalf("dataset creation scopes = %v, want admin", got)
	}
	cfg.AutoCreate.Dataset = false
	cfg.AutoCreate.Tables = true
	if got := e.metadataScopes(); !equalStrings(got, []string{scopeAdmin}) {
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
