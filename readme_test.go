// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"fmt"
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

func TestReadmeReflectsV0Alpha2Contract(t *testing.T) {
	raw, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	readme := string(raw)

	spansIndexDDL := strings.TrimSpace(fmt.Sprintf(schema.SpansSearchIndexDDL, "my-project", "otel", "otel_spans"))
	logsIndexDDL := strings.TrimSpace(fmt.Sprintf(schema.LogsSearchIndexDDL, "my-project", "otel", "otel_logs"))

	for _, required := range []string{
		"logs.log_name_attribute",
		"logs.insert_id_attribute",
		"metrics.table",
		"otel_metrics",
		"otel_metrics_dedup",
		"auto_create.companion_views",
		"auto_create.search_indexes",
		"quarantine.enabled",
		"quarantine.table",
		"require_partition_filter",
		"credentials.json",
		"credentials.impersonate_delegates",
		"credentials.quota_project",
		"credentials.universe_domain",
		"endpoint.admin_url",
		"attributes_schema",
		"payload_refs",
		"otel_spans_as_allspans",
		"v_gen_ai_spans",
		"v_gen_ai_model_costs",
		"v_gen_ai_sessions",
		"v_gen_ai_tool_usage",
		"v_gen_ai_logs",
		"otel_quarantine",
		"otelcol_exporter_bigquery_quarantined_rows",
		"otelcol_exporter_bigquery_append_bytes",
		"redaction.enabled",
		"redaction.blocked_keys",
		"redaction.blocked_value_patterns",
		"otelcol_exporter_bigquery_redacted_values",
		"offload.cache_size",
		"otelcol_exporter_bigquery_offload_dedup_hits",
		"v0alpha2",
		"Operating v0alpha2 tables",
		"TIMESTAMP_TRUNC(start_time, HOUR)",
		"10,000",
		"416",
		"partition_expiration_days",
		"ALTER TABLE",
		"CREATE SEARCH INDEX",
		"otel_spans_trace_lookup_idx",
		"otel_logs_trace_lookup_idx",
		"otel_metrics_trace_lookup_idx",
		"NO_OP_ANALYZER",
		"bigquery.jobs.create",
		spansIndexDDL,
		logsIndexDDL,
	} {
		if !strings.Contains(readme, required) {
			t.Errorf("README.md is missing v0alpha2 contract token %q", required)
		}
	}

	for _, retired := range []string{
		"source_record_namespace",
		"source_record_id_attribute",
		"otlp-protojson-v1",
		"attributes_fingerprint",
	} {
		if strings.Contains(readme, retired) {
			t.Errorf("README.md still references retired v0alpha1 token %q", retired)
		}
	}
}
