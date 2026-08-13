// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"testing"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/protobuf/proto"
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
