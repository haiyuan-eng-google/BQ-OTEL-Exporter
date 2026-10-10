// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

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
				"otel_spans_as_allspans": fmt.Sprintf(SpansAsAllSpansViewQuery, "proj", "ds", "spans_tbl"),
				"v_gen_ai_spans":         fmt.Sprintf(GenAISpansViewQuery, "proj", "ds", "spans_tbl"),
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
