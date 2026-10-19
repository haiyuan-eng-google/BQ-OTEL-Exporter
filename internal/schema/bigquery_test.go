// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestToBigQueryPreservesCompleteSchema(t *testing.T) {
	got, err := ToBigQuery(&storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		{Name: "required_string", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_REQUIRED},
		{Name: "optional_json", Type: storagepb.TableFieldSchema_JSON, Mode: storagepb.TableFieldSchema_NULLABLE},
		{
			Name: "nested", Type: storagepb.TableFieldSchema_STRUCT, Mode: storagepb.TableFieldSchema_REPEATED,
			Fields: []*storagepb.TableFieldSchema{
				{Name: "at", Type: storagepb.TableFieldSchema_TIMESTAMP, Mode: storagepb.TableFieldSchema_NULLABLE},
				{Name: "count", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_REQUIRED},
			},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}

	want := bq.Schema{
		{Name: "required_string", Type: bq.StringFieldType, Required: true},
		{Name: "optional_json", Type: bq.JSONFieldType},
		{
			Name: "nested", Type: bq.RecordFieldType, Repeated: true,
			Schema: bq.Schema{
				{Name: "at", Type: bq.TimestampFieldType},
				{Name: "count", Type: bq.IntegerFieldType, Required: true},
			},
		},
	}
	assertBigQuerySchemaEqual(t, got, want)
}

func TestBigQuerySchemaRoundTripPreservesCanonicalContracts(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   *storagepb.TableSchema
	}{
		{name: "spans", in: SpansTableSchema()},
		{name: "logs", in: LogsTableSchema()},
		{name: "metrics", in: MetricsTableSchema()},
		{name: "quarantine", in: QuarantineTableSchema()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bqs, err := ToBigQuery(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			got, err := FromBigQuery(bqs)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(got, tc.in) {
				t.Fatalf("schema changed across conversion\ngot:  %v\nwant: %v", got, tc.in)
			}
		})
	}
}

// DestinationMetadata is what auto-create applies, so the partitioning,
// clustering and view it returns are the physical layout of every table the
// exporter creates.
func TestDestinationMetadata(t *testing.T) {
	for _, tc := range []struct {
		name          string
		in            *storagepb.TableSchema
		partitionType bq.TimePartitioningType
		partition     string
		cluster       []string
		viewQuery     string
	}{
		{
			name:          "spans",
			in:            SpansTableSchema(),
			partitionType: bq.HourPartitioningType,
			partition:     "start_time",
			cluster:       []string{"service_name", "trace_id", "name"},
			viewQuery:     SpansDedupViewQuery,
		},
		{
			name:          "logs",
			in:            LogsTableSchema(),
			partitionType: bq.DayPartitioningType,
			partition:     "timestamp",
			cluster:       []string{"service_name", "trace_id", "severity_number"},
			viewQuery:     LogsDedupViewQuery,
		},
		{
			name:          "metrics",
			in:            MetricsTableSchema(),
			partitionType: bq.DayPartitioningType,
			partition:     "timestamp",
			cluster:       []string{"service_name", "metric_name", "metric_type"},
			viewQuery:     MetricsDedupViewQuery,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			md, view, err := DestinationMetadata(tc.in, "proj", "ds", "tbl")
			if err != nil {
				t.Fatal(err)
			}
			if tp := md.TimePartitioning; tp == nil || tp.Type != tc.partitionType || tp.Field != tc.partition {
				t.Errorf("partitioning = %+v, want type %q on %q", tp, tc.partitionType, tc.partition)
			}
			var cluster []string
			if md.Clustering != nil {
				cluster = md.Clustering.Fields
			}
			if !slices.Equal(cluster, tc.cluster) {
				t.Errorf("clustering = %v, want %v", cluster, tc.cluster)
			}
			wantSchema, err := ToBigQuery(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			assertBigQuerySchemaEqual(t, md.Schema, wantSchema)
			if want := fmt.Sprintf(tc.viewQuery, "proj", "ds", "tbl"); view != want {
				t.Errorf("view query = %q, want %q", view, want)
			}
			if !strings.Contains(view, "`proj.ds.tbl`") {
				t.Errorf("view query %q does not reference the base table", view)
			}
		})
	}

	if _, _, err := DestinationMetadata(&storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		f("not_a_contract", str, opt),
	}}, "proj", "ds", "tbl"); err == nil {
		t.Error("a schema that is not a canonical contract must be rejected")
	}
	if _, _, err := DestinationMetadata(QuarantineTableSchema(), "proj", "ds", "tbl"); err == nil {
		t.Error("the quarantine contract has no dedup view and must go through QuarantineDestinationMetadata")
	}
}

func TestDestinationMetadataWithOptions(t *testing.T) {
	const kmsKey = "projects/proj/locations/us/keyRings/ring/cryptoKeys/key"
	labels := map[string]string{"env": "prod", "team": "observability"}
	md, _, err := DestinationMetadataWithOptions(SpansTableSchema(), "proj", "ds", "tbl", DestinationOptions{
		PartitionExpirationDays: 90,
		RequirePartitionFilter:  true,
		KMSKeyName:              kmsKey,
		Labels:                  labels,
		MaxStaleness:            15 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if md.TimePartitioning.Expiration != 90*24*time.Hour {
		t.Fatalf("partition expiration = %v, want 90 days", md.TimePartitioning.Expiration)
	}
	if !md.RequirePartitionFilter {
		t.Fatal("RequirePartitionFilter = false, want true")
	}
	if md.EncryptionConfig == nil || md.EncryptionConfig.KMSKeyName != kmsKey {
		t.Fatalf("EncryptionConfig = %+v, want KMSKeyName %q", md.EncryptionConfig, kmsKey)
	}
	if !maps.Equal(md.Labels, labels) {
		t.Fatalf("Labels = %v, want %v", md.Labels, labels)
	}
	if md.MaxStaleness == nil || md.MaxStaleness.SubSecondNanos != 0 || md.MaxStaleness.Hours != 0 || md.MaxStaleness.Minutes != 15 {
		t.Fatalf("MaxStaleness = %+v, want 15m interval", md.MaxStaleness)
	}
}

// QuarantineDestinationMetadata is what auto-create applies for the opt-in
// quarantine table, so it must carry the layout QuarantineDDL declares: daily
// partitions on ingest_time that expire after 30 days, clustering on
// (signal, error_code, service_name), and no view.
func TestQuarantineDestinationMetadata(t *testing.T) {
	md, err := QuarantineDestinationMetadata()
	if err != nil {
		t.Fatal(err)
	}
	tp := md.TimePartitioning
	if tp == nil || tp.Type != bq.DayPartitioningType || tp.Field != "ingest_time" {
		t.Fatalf("partitioning = %+v, want daily on ingest_time", tp)
	}
	if tp.Expiration != 30*24*time.Hour {
		t.Errorf("partition expiration = %v, want 30 days", tp.Expiration)
	}
	days := int(tp.Expiration / (24 * time.Hour))
	if !strings.Contains(QuarantineDDL, fmt.Sprintf("partition_expiration_days = %d\n", days)) {
		t.Errorf("metadata expires partitions after %d days, but QuarantineDDL disagrees", days)
	}
	var cluster []string
	if md.Clustering != nil {
		cluster = md.Clustering.Fields
	}
	if want := []string{"signal", "error_code", "service_name"}; !slices.Equal(cluster, want) {
		t.Errorf("clustering = %v, want %v", cluster, want)
	}
	wantSchema, err := ToBigQuery(QuarantineTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	assertBigQuerySchemaEqual(t, md.Schema, wantSchema)
	if md.ViewQuery != "" {
		t.Errorf("quarantine table metadata must not define a view, got %q", md.ViewQuery)
	}

	const qKMS = "projects/proj/locations/us/keyRings/ring/cryptoKeys/qkey"
	customMD, err := QuarantineDestinationMetadataWithOptions(DestinationOptions{
		PartitionExpirationDays: 14,
		RequirePartitionFilter:  true,
		KMSKeyName:              qKMS,
		Labels:                  map[string]string{"tier": "quarantine"},
		MaxStaleness:            5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if customMD.TimePartitioning.Expiration != 14*24*time.Hour || !customMD.RequirePartitionFilter {
		t.Fatalf("custom quarantine options = %+v, want 14d expiration and RequirePartitionFilter=true", customMD)
	}
	if customMD.EncryptionConfig == nil || customMD.EncryptionConfig.KMSKeyName != qKMS {
		t.Fatalf("quarantine EncryptionConfig = %+v, want %q", customMD.EncryptionConfig, qKMS)
	}
	if !maps.Equal(customMD.Labels, map[string]string{"tier": "quarantine"}) {
		t.Fatalf("quarantine Labels = %v, want tier=quarantine", customMD.Labels)
	}
	if customMD.MaxStaleness == nil || customMD.MaxStaleness.Minutes != 5 {
		t.Fatalf("quarantine MaxStaleness = %+v, want 5m", customMD.MaxStaleness)
	}
}

// CompanionViewsMetadata is what auto-create applies for the optional
// companion views, so each signal must get exactly its own views, under their
// published names, as plain standard SQL views over the configured tables.
func TestCompanionViewsMetadata(t *testing.T) {
	for _, tc := range []struct {
		name    string
		isSpans bool
		want    map[string]string
	}{
		{
			name:    "spans",
			isSpans: true,
			want: map[string]string{
				"otel_spans_as_allspans":    fmt.Sprintf(SpansAsAllSpansViewQuery, "proj", "ds", "spans_tbl"),
				"v_gen_ai_spans":            fmt.Sprintf(GenAISpansViewQuery, "proj", "ds", "spans_tbl"),
				"v_gen_ai_model_costs":      fmt.Sprintf(GenAIModelCostsViewQuery, "proj", "ds"),
				"v_gen_ai_sessions":         fmt.Sprintf(GenAISessionsViewQuery, "proj", "ds"),
				"v_gen_ai_tool_usage":       fmt.Sprintf(GenAIToolUsageViewQuery, "proj", "ds"),
				"v_gen_ai_agent_trajectory": fmt.Sprintf(GenAIAgentTrajectoryViewQuery, "proj", "ds"),
			},
		},
		{
			name: "logs",
			want: map[string]string{
				"v_gen_ai_logs": fmt.Sprintf(GenAILogsViewQuery, "proj", "ds", "logs_tbl", "spans_tbl"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := CompanionViewsMetadata("proj", "ds", "spans_tbl", "logs_tbl", tc.isSpans)
			gotNames, wantNames := slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(tc.want))
			if !slices.Equal(gotNames, wantNames) {
				t.Fatalf("views = %v, want %v", gotNames, wantNames)
			}
			for name, query := range tc.want {
				md := got[name]
				if md.ViewQuery != query {
					t.Errorf("%s query = %q, want %q", name, md.ViewQuery, query)
				}
				if md.UseLegacySQL || md.Schema != nil || md.TimePartitioning != nil || md.Clustering != nil {
					t.Errorf("%s must be a plain standard SQL view, got %+v", name, md)
				}
			}
		})
	}
}

// The metrics signal has one companion view, v_gen_ai_metrics. It reads the
// metric dedup view of the configured metrics table, so a custom table name
// must reach the query, and it must be a plain standard SQL view like the
// others. None of the span or log companion views is created for metrics, and
// v_gen_ai_metrics is not created for them.
func TestMetricsCompanionViewsMetadata(t *testing.T) {
	got := MetricsCompanionViewsMetadata("proj", "ds", "metrics_tbl")
	if names := slices.Sorted(maps.Keys(got)); !slices.Equal(names, []string{"v_gen_ai_metrics"}) {
		t.Fatalf("views = %v, want [v_gen_ai_metrics]", names)
	}
	md := got[GenAIMetricsViewName]
	if want := fmt.Sprintf(GenAIMetricsViewQuery, "proj", "ds", "metrics_tbl"); md.ViewQuery != want {
		t.Errorf("v_gen_ai_metrics query = %q, want %q", md.ViewQuery, want)
	}
	if !strings.Contains(md.ViewQuery, "FROM `proj.ds.metrics_tbl_dedup`") {
		t.Errorf("v_gen_ai_metrics must read the metric dedup view, got %q", md.ViewQuery)
	}
	if md.UseLegacySQL || md.Schema != nil || md.TimePartitioning != nil || md.Clustering != nil {
		t.Errorf("v_gen_ai_metrics must be a plain standard SQL view, got %+v", md)
	}
	for _, isSpans := range []bool{true, false} {
		if _, ok := CompanionViewsMetadata("proj", "ds", "spans_tbl", "logs_tbl", isSpans)[GenAIMetricsViewName]; ok {
			t.Errorf("CompanionViewsMetadata(isSpans=%v) must not include v_gen_ai_metrics", isSpans)
		}
	}
}

// v_gen_ai_metric_exemplars joins v_gen_ai_metrics with the span dedup view, so
// its metadata helper takes the spans table name and stays separate from the
// single-signal companion view maps.
func TestMetricExemplarsCompanionViewMetadata(t *testing.T) {
	md := MetricExemplarsCompanionViewMetadata("proj", "ds", "spans_tbl")
	if want := fmt.Sprintf(GenAIMetricExemplarsViewQuery, "proj", "ds", "spans_tbl"); md.ViewQuery != want {
		t.Errorf("v_gen_ai_metric_exemplars query = %q, want %q", md.ViewQuery, want)
	}
	if !strings.Contains(md.ViewQuery, "FROM `proj.ds.v_gen_ai_metrics` m,\nUNNEST(m.exemplars) AS ex") {
		t.Errorf("v_gen_ai_metric_exemplars must unnest v_gen_ai_metrics.exemplars, got %q", md.ViewQuery)
	}
	if !strings.Contains(md.ViewQuery, "LEFT JOIN `proj.ds.spans_tbl_dedup` s") {
		t.Errorf("v_gen_ai_metric_exemplars must left join the span dedup view, got %q", md.ViewQuery)
	}
	if md.UseLegacySQL || md.Schema != nil || md.TimePartitioning != nil || md.Clustering != nil {
		t.Errorf("v_gen_ai_metric_exemplars must be a plain standard SQL view, got %+v", md)
	}
	for _, isSpans := range []bool{true, false} {
		if _, ok := CompanionViewsMetadata("proj", "ds", "spans_tbl", "logs_tbl", isSpans)[GenAIMetricExemplarsViewName]; ok {
			t.Errorf("CompanionViewsMetadata(isSpans=%v) must not include v_gen_ai_metric_exemplars", isSpans)
		}
	}
	if _, ok := MetricsCompanionViewsMetadata("proj", "ds", "metrics_tbl")[GenAIMetricExemplarsViewName]; ok {
		t.Errorf("MetricsCompanionViewsMetadata must not include v_gen_ai_metric_exemplars")
	}
}

func TestReconcileAdditiveSchema(t *testing.T) {
	want := SpansTableSchema()
	fullBQ, err := ToBigQuery(want)
	if err != nil {
		t.Fatal(err)
	}
	// Drop a top-level repeated field (payload_refs) and a nested nullable field (status.code_name).
	var existing bq.Schema
	for _, f := range fullBQ {
		if f.Name == "payload_refs" {
			continue
		}
		copyF := *f
		if copyF.Name == "status" {
			copyF.Schema = bq.Schema{copyF.Schema[0], copyF.Schema[2]}
		}
		existing = append(existing, &copyF)
	}

	merged, changed, err := ReconcileAdditiveSchema(existing, want)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected ReconcileAdditiveSchema to detect missing additive fields")
	}
	haveProto, err := FromBigQuery(merged)
	if err != nil {
		t.Fatal(err)
	}
	if diffs := CompatibilityDifferences(want, haveProto); len(diffs) != 0 {
		t.Fatalf("reconciled schema still has differences: %v", diffs)
	}
}

const testPolicyTag = "projects/p/locations/us/taxonomies/1/policyTags/2"

// The schema update sends the whole schema back, so the reconciled schema
// carries every policy tag the live table has, as a copy: on a top-level
// column, on a field of a RECORD that gains a field, and on a field of a
// RECORD that gains nothing.
func TestReconcileAdditiveSchemaKeepsPolicyTags(t *testing.T) {
	for _, tc := range []struct {
		name string
		want *storagepb.TableSchema
		// drop names the fields the live table lacks, so that reconciliation
		// adds them, and tagged the fields the live table tags.
		drop, tagged []string
	}{
		{
			name:   "quarantine",
			want:   QuarantineTableSchema(),
			drop:   []string{"exporter_instance"},
			tagged: []string{"raw_payload", "error_message"},
		},
		{
			name:   "spans",
			want:   SpansTableSchema(),
			drop:   []string{"scope_schema_url", "payload_refs.object_ref.details"},
			tagged: []string{"status.message", "payload_refs.object_ref.uri"},
		},
		{
			name:   "logs",
			want:   LogsTableSchema(),
			drop:   []string{"record_fingerprint"},
			tagged: []string{"severity_text", "payload_refs.object_ref.uri"},
		},
		{
			name:   "metrics",
			want:   MetricsTableSchema(),
			drop:   []string{"scope_schema_url"},
			tagged: []string{"metric_description", "exemplars.trace_id"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live := liveSchema(t, tc.want, tc.drop, tc.tagged)
			merged, changed, err := ReconcileAdditiveSchema(live, tc.want)
			if err != nil {
				t.Fatal(err)
			}
			if !changed {
				t.Fatalf("live schema lacks %v, but reconciliation reports no change", tc.drop)
			}
			for _, path := range tc.drop {
				if fieldAt(merged, path) == nil {
					t.Errorf("reconciled schema lacks %s", path)
				}
			}
			assertPolicyTagsCopied(t, merged, live, tc.tagged)
		})
	}
}

// A RECORD the exporter's schema does not have, such as a column an operator
// added, is copied with its nested policy tags.
func TestReconcileAdditiveSchemaKeepsPolicyTagsOfUnknownRecords(t *testing.T) {
	live := liveSchema(t, QuarantineTableSchema(), []string{"exporter_instance"}, nil)
	live = append(live, &bq.FieldSchema{Name: "operator_notes", Type: bq.RecordFieldType, Schema: bq.Schema{
		{Name: "text", Type: bq.StringFieldType, PolicyTags: &bq.PolicyTagList{Names: []string{testPolicyTag}}},
	}})
	merged, changed, err := ReconcileAdditiveSchema(live, QuarantineTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("live schema lacks exporter_instance, but reconciliation reports no change")
	}
	assertPolicyTagsCopied(t, merged, live, []string{"operator_notes.text"})
}

// A live table whose schema differs from the exporter's only in its policy
// tags needs no update, and the schema reconciliation returns still carries
// the tags.
func TestReconcileAdditiveSchemaIgnoresPolicyTagOnlyDifferences(t *testing.T) {
	for name, tc := range map[string]struct {
		want   *storagepb.TableSchema
		tagged []string
	}{
		"quarantine": {QuarantineTableSchema(), []string{"raw_payload", "error_message"}},
		"spans":      {SpansTableSchema(), []string{"status.message", "payload_refs.object_ref.uri"}},
		"logs":       {LogsTableSchema(), []string{"severity_text"}},
		"metrics":    {MetricsTableSchema(), []string{"metric_description"}},
	} {
		t.Run(name, func(t *testing.T) {
			live := liveSchema(t, tc.want, nil, tc.tagged)
			merged, changed, err := ReconcileAdditiveSchema(live, tc.want)
			if err != nil {
				t.Fatal(err)
			}
			if changed {
				t.Error("live schema differs only in policy tags, but reconciliation reports a change")
			}
			assertPolicyTagsCopied(t, merged, live, tc.tagged)
		})
	}
}

// ReconcileAdditiveSchema relies on the exporter's own schemas carrying no
// policy tags: reconciliation keeps the live table's tags and never sets any.
func TestToBigQuerySetsNoPolicyTags(t *testing.T) {
	for name, in := range map[string]*storagepb.TableSchema{
		"spans":      SpansTableSchema(),
		"logs":       LogsTableSchema(),
		"metrics":    MetricsTableSchema(),
		"quarantine": QuarantineTableSchema(),
	} {
		got, err := ToBigQuery(in)
		if err != nil {
			t.Fatal(err)
		}
		if tagged := taggedFields(got, ""); len(tagged) != 0 {
			t.Errorf("%s: ToBigQuery sets policy tags on %v", name, tagged)
		}
	}
}

// liveSchema returns the BigQuery schema of want as a live table might return
// it: without the fields at the drop paths, and with testPolicyTag on the
// fields at the tagged paths. Paths are dotted, for example status.message.
func liveSchema(t *testing.T, want *storagepb.TableSchema, drop, tagged []string) bq.Schema {
	t.Helper()
	live, err := ToBigQuery(want)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range drop {
		fields := &live
		name := path
		if i := strings.LastIndex(path, "."); i >= 0 {
			parent := fieldAt(live, path[:i])
			if parent == nil {
				t.Fatalf("schema has no field %s", path[:i])
			}
			fields, name = &parent.Schema, path[i+1:]
		}
		before := len(*fields)
		*fields = slices.DeleteFunc(*fields, func(f *bq.FieldSchema) bool { return f.Name == name })
		if len(*fields) != before-1 {
			t.Fatalf("schema has no field %s to drop", path)
		}
	}
	for _, path := range tagged {
		f := fieldAt(live, path)
		if f == nil {
			t.Fatalf("schema has no field %s to tag", path)
		}
		f.PolicyTags = &bq.PolicyTagList{Names: []string{testPolicyTag}}
	}
	return live
}

// assertPolicyTagsCopied fails unless exactly the fields at the tagged paths
// of merged carry testPolicyTag, each in a list that shares no memory with the
// list the same field has in live.
func assertPolicyTagsCopied(t *testing.T, merged, live bq.Schema, tagged []string) {
	t.Helper()
	if got := taggedFields(merged, ""); !slices.Equal(got, slices.Sorted(slices.Values(tagged))) {
		t.Errorf("reconciled schema tags %v, want %v", got, slices.Sorted(slices.Values(tagged)))
	}
	for _, path := range tagged {
		got, was := fieldAt(merged, path), fieldAt(live, path)
		if got == nil || got.PolicyTags == nil || !slices.Equal(got.PolicyTags.Names, []string{testPolicyTag}) {
			t.Errorf("reconciled %s policy tags = %+v, want [%s]", path, got, testPolicyTag)
			continue
		}
		if got.PolicyTags == was.PolicyTags || &got.PolicyTags.Names[0] == &was.PolicyTags.Names[0] {
			t.Errorf("reconciled %s shares its policy tags with the live schema", path)
		}
	}
}

// fieldAt returns the field at the dotted path in s, or nil when there is none.
func fieldAt(s bq.Schema, path string) *bq.FieldSchema {
	name, rest, nested := strings.Cut(path, ".")
	for _, f := range s {
		if f == nil || f.Name != name {
			continue
		}
		if !nested {
			return f
		}
		return fieldAt(f.Schema, rest)
	}
	return nil
}

// taggedFields returns the sorted dotted paths of the fields in s that carry
// policy tags, each prefixed with prefix.
func taggedFields(s bq.Schema, prefix string) []string {
	var paths []string
	for _, f := range s {
		if f == nil {
			continue
		}
		if f.PolicyTags != nil {
			paths = append(paths, prefix+f.Name)
		}
		paths = append(paths, taggedFields(f.Schema, prefix+f.Name+".")...)
	}
	slices.Sort(paths)
	return paths
}

func TestToBigQueryRejectsUnsupportedTypeAndMode(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field *storagepb.TableFieldSchema
	}{
		{
			name:  "type",
			field: &storagepb.TableFieldSchema{Name: "bad", Type: storagepb.TableFieldSchema_TYPE_UNSPECIFIED, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
		{
			name:  "mode",
			field: &storagepb.TableFieldSchema{Name: "bad", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_MODE_UNSPECIFIED},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ToBigQuery(&storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{tc.field}}); err == nil {
				t.Fatal("expected unsupported schema entry to fail")
			}
		})
	}
}

func TestCompatibilityDifferencesAllowsOnlyAdditiveNullableFields(t *testing.T) {
	want := &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		{Name: "original", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_REQUIRED},
	}}
	withNullable := proto.Clone(want).(*storagepb.TableSchema)
	withNullable.Fields = append(withNullable.Fields, &storagepb.TableFieldSchema{
		Name: "additive_nullable", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE,
	})
	if got := CompatibilityDifferences(want, withNullable); len(got) != 0 {
		t.Fatalf("extra nullable field must be compatible, got %v", got)
	}

	withRequired := proto.Clone(want).(*storagepb.TableSchema)
	withRequired.Fields = append(withRequired.Fields, &storagepb.TableFieldSchema{
		Name: "breaking_required", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_REQUIRED,
	})
	if got := CompatibilityDifferences(want, withRequired); len(got) != 1 {
		t.Fatalf("extra required field differences = %v, want one incompatibility", got)
	}
}

func TestCompatibilityDifferencesRejectsNestedConstraintDrift(t *testing.T) {
	want := &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		{
			Name: "nested", Type: storagepb.TableFieldSchema_STRUCT, Mode: storagepb.TableFieldSchema_NULLABLE,
			Fields: []*storagepb.TableFieldSchema{
				{
					Name: "limited", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE,
					MaxLength: 64, DefaultValueExpression: "'unknown'",
				},
				{
					Name: "decimal", Type: storagepb.TableFieldSchema_NUMERIC, Mode: storagepb.TableFieldSchema_NULLABLE,
					Precision: 38, Scale: 9,
				},
				{
					Name: "occurred_at", Type: storagepb.TableFieldSchema_TIMESTAMP, Mode: storagepb.TableFieldSchema_NULLABLE,
					TimestampPrecision: wrapperspb.Int64(6),
				},
				{
					Name: "window", Type: storagepb.TableFieldSchema_RANGE, Mode: storagepb.TableFieldSchema_NULLABLE,
					RangeElementType: &storagepb.TableFieldSchema_FieldElementType{Type: storagepb.TableFieldSchema_DATE},
				},
			},
		},
	}}

	tests := []struct {
		name       string
		mutate     func(*storagepb.TableSchema)
		wantDetail string
	}{
		{
			name: "max length",
			mutate: func(s *storagepb.TableSchema) {
				s.Fields[0].Fields[0].MaxLength = 32
			},
			wantDetail: "nested.limited has max length 32, want 64",
		},
		{
			name: "default expression",
			mutate: func(s *storagepb.TableSchema) {
				s.Fields[0].Fields[0].DefaultValueExpression = "'other'"
			},
			wantDetail: `nested.limited has default value expression "'other'", want "'unknown'"`,
		},
		{
			name: "precision",
			mutate: func(s *storagepb.TableSchema) {
				s.Fields[0].Fields[1].Precision = 20
			},
			wantDetail: "nested.decimal has precision 20, want 38",
		},
		{
			name: "scale",
			mutate: func(s *storagepb.TableSchema) {
				s.Fields[0].Fields[1].Scale = 4
			},
			wantDetail: "nested.decimal has scale 4, want 9",
		},
		{
			name: "timestamp precision",
			mutate: func(s *storagepb.TableSchema) {
				s.Fields[0].Fields[2].TimestampPrecision = wrapperspb.Int64(3)
			},
			wantDetail: "nested.occurred_at has timestamp precision 3, want 6",
		},
		{
			name: "range element type",
			mutate: func(s *storagepb.TableSchema) {
				s.Fields[0].Fields[3].RangeElementType.Type = storagepb.TableFieldSchema_DATETIME
			},
			wantDetail: "nested.window has range element type DATETIME, want DATE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			have := proto.Clone(want).(*storagepb.TableSchema)
			tc.mutate(have)
			differences := CompatibilityDifferences(want, have)
			if len(differences) != 1 || !strings.Contains(differences[0], tc.wantDetail) {
				t.Fatalf("differences = %v, want one containing %q", differences, tc.wantDetail)
			}
		})
	}
}

func assertBigQuerySchemaEqual(t *testing.T, got, want bq.Schema) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("schema length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Name != w.Name || g.Type != w.Type || g.Required != w.Required || g.Repeated != w.Repeated {
			t.Errorf("field %d = {%q %s required=%v repeated=%v}, want {%q %s required=%v repeated=%v}",
				i, g.Name, g.Type, g.Required, g.Repeated, w.Name, w.Type, w.Required, w.Repeated)
		}
		assertBigQuerySchemaEqual(t, g.Schema, w.Schema)
	}
}
