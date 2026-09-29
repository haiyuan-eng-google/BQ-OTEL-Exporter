// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"os"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"

	bqi "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// TestReadmeQuotesStartupErrors keeps the README's startup-error table tied to
// the messages the exporter actually returns. Credential and Compose messages
// come from other code and are proven by the collector-example CI job instead.
func TestReadmeQuotesStartupErrors(t *testing.T) {
	raw, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	readme := string(raw)
	ctx := context.Background()

	cfg := validConfig()
	e := newTestSignalExporter(cfg, cfg.Traces.Table, schema.SpansTableSchema())
	missingDataset := e.prepareDestination(ctx, &fakeDestinationAdmin{datasetMetadataErr: notFound()})
	missingTable := e.prepareDestination(ctx, &fakeDestinationAdmin{
		datasetMetadata:  &bq.DatasetMetadata{},
		tableMetadataErr: notFound(),
	})

	createCfg := validConfig()
	createCfg.AutoCreate.Tables = true
	denied := forbidden()
	createDenied := newTestSignalExporter(createCfg, createCfg.Traces.Table, schema.SpansTableSchema()).
		prepareDestination(ctx, &fakeDestinationAdmin{
			datasetMetadata:  &bq.DatasetMetadata{},
			tableMetadataErr: notFound(),
			createTableErr:   denied,
		})

	for _, err := range []error{missingDataset, missingTable, createDenied} {
		if err == nil {
			t.Fatal("prepareDestination succeeded; want a startup error")
		}
	}
	for name, want := range map[string]string{
		"missing dataset": missingDataset.Error(),
		"missing table":   missingTable.Error(),
		// The README quotes the exporter's prefix; BigQuery supplies the rest.
		"create denied": strings.TrimSpace(strings.TrimSuffix(createDenied.Error(), denied.Error())),
		"append denied": bqi.PermissionHint(e.tableID()),
	} {
		if !strings.Contains(readme, "`"+want+"`") {
			t.Errorf("README does not quote the %s error %q", name, want)
		}
	}
}
