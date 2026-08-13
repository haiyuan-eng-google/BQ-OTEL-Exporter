// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// DestinationMetadata builds the physical table and logical deduplication
// view metadata for one of the exporter's canonical signal contracts.
func DestinationMetadata(
	in *storagepb.TableSchema, project, dataset, table string,
) (*bq.TableMetadata, string, error) {
	bqs, err := ToBigQuery(in)
	if err != nil {
		return nil, "", err
	}
	md := &bq.TableMetadata{Schema: bqs}
	var queryTemplate string
	switch {
	case proto.Equal(in, SpansTableSchema()):
		md.TimePartitioning = &bq.TimePartitioning{Field: "start_timestamp"}
		md.Clustering = &bq.Clustering{Fields: []string{"service_name", "name"}}
		queryTemplate = SpansDedupViewQuery
	case proto.Equal(in, LogsTableSchema()):
		md.TimePartitioning = &bq.TimePartitioning{Field: "timestamp"}
		queryTemplate = LogsDedupViewQuery
	default:
		return nil, "", fmt.Errorf("schema is not a canonical trace or log contract")
	}
	return md, fmt.Sprintf(queryTemplate, project, dataset, table), nil
}

var fieldTypePairs = []struct {
	storage  storagepb.TableFieldSchema_Type
	bigQuery bq.FieldType
}{
	{storagepb.TableFieldSchema_STRING, bq.StringFieldType},
	{storagepb.TableFieldSchema_INT64, bq.IntegerFieldType},
	{storagepb.TableFieldSchema_DOUBLE, bq.FloatFieldType},
	{storagepb.TableFieldSchema_STRUCT, bq.RecordFieldType},
	{storagepb.TableFieldSchema_BYTES, bq.BytesFieldType},
	{storagepb.TableFieldSchema_BOOL, bq.BooleanFieldType},
	{storagepb.TableFieldSchema_TIMESTAMP, bq.TimestampFieldType},
	{storagepb.TableFieldSchema_DATE, bq.DateFieldType},
	{storagepb.TableFieldSchema_TIME, bq.TimeFieldType},
	{storagepb.TableFieldSchema_DATETIME, bq.DateTimeFieldType},
	{storagepb.TableFieldSchema_GEOGRAPHY, bq.GeographyFieldType},
	{storagepb.TableFieldSchema_NUMERIC, bq.NumericFieldType},
	{storagepb.TableFieldSchema_BIGNUMERIC, bq.BigNumericFieldType},
	{storagepb.TableFieldSchema_INTERVAL, bq.IntervalFieldType},
	{storagepb.TableFieldSchema_JSON, bq.JSONFieldType},
	{storagepb.TableFieldSchema_RANGE, bq.RangeFieldType},
}

var storageToBigQueryType, bigQueryToStorageType = func() (
	map[storagepb.TableFieldSchema_Type]bq.FieldType,
	map[bq.FieldType]storagepb.TableFieldSchema_Type,
) {
	toBigQuery := make(map[storagepb.TableFieldSchema_Type]bq.FieldType, len(fieldTypePairs))
	toStorage := make(map[bq.FieldType]storagepb.TableFieldSchema_Type, len(fieldTypePairs))
	for _, pair := range fieldTypePairs {
		toBigQuery[pair.storage] = pair.bigQuery
		toStorage[pair.bigQuery] = pair.storage
	}
	return toBigQuery, toStorage
}()

// ToBigQuery derives the metadata API schema from the Storage Write API
// schema. The storagepb value is the single typed contract used for both table
// creation and row encoding.
func ToBigQuery(in *storagepb.TableSchema) (bq.Schema, error) {
	if in == nil {
		return nil, fmt.Errorf("storage schema is nil")
	}
	return fieldsToBigQuery(in.GetFields())
}

func fieldsToBigQuery(in []*storagepb.TableFieldSchema) (bq.Schema, error) {
	out := make(bq.Schema, 0, len(in))
	for _, field := range in {
		if field == nil {
			return nil, fmt.Errorf("schema contains a nil field")
		}
		typ, ok := storageToBigQueryType[field.GetType()]
		if !ok {
			return nil, fmt.Errorf("field %q has unsupported type %s", field.GetName(), field.GetType())
		}
		converted := &bq.FieldSchema{
			Name:                   field.GetName(),
			Description:            field.GetDescription(),
			Type:                   typ,
			MaxLength:              field.GetMaxLength(),
			Precision:              field.GetPrecision(),
			Scale:                  field.GetScale(),
			DefaultValueExpression: field.GetDefaultValueExpression(),
		}
		switch field.GetMode() {
		case storagepb.TableFieldSchema_NULLABLE:
		case storagepb.TableFieldSchema_REQUIRED:
			converted.Required = true
		case storagepb.TableFieldSchema_REPEATED:
			converted.Repeated = true
		default:
			return nil, fmt.Errorf("field %q has unsupported mode %s", field.GetName(), field.GetMode())
		}
		if precision := field.GetTimestampPrecision(); precision != nil {
			converted.TimestampPrecision = precision.GetValue()
		}
		if rangeType := field.GetRangeElementType(); rangeType != nil {
			convertedType, ok := storageToBigQueryType[rangeType.GetType()]
			if !ok {
				return nil, fmt.Errorf("field %q has unsupported range element type %s", field.GetName(), rangeType.GetType())
			}
			converted.RangeElementType = &bq.RangeElementType{Type: convertedType}
		}
		var err error
		converted.Schema, err = fieldsToBigQuery(field.GetFields())
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.GetName(), err)
		}
		out = append(out, converted)
	}
	return out, nil
}

// FromBigQuery converts metadata returned by BigQuery into the canonical
// Storage Write API form so startup validation compares types, modes, and
// nested fields rather than names alone.
func FromBigQuery(in bq.Schema) (*storagepb.TableSchema, error) {
	fields, err := fieldsFromBigQuery(in)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("destination table reports no columns")
	}
	return &storagepb.TableSchema{Fields: fields}, nil
}

func fieldsFromBigQuery(in bq.Schema) ([]*storagepb.TableFieldSchema, error) {
	out := make([]*storagepb.TableFieldSchema, 0, len(in))
	for _, field := range in {
		if field == nil {
			return nil, fmt.Errorf("schema contains a nil field")
		}
		typ, ok := bigQueryToStorageType[field.Type]
		if !ok {
			return nil, fmt.Errorf("field %q has unsupported type %s", field.Name, field.Type)
		}
		if field.Required && field.Repeated {
			return nil, fmt.Errorf("field %q cannot be both required and repeated", field.Name)
		}
		mode := storagepb.TableFieldSchema_NULLABLE
		if field.Required {
			mode = storagepb.TableFieldSchema_REQUIRED
		} else if field.Repeated {
			mode = storagepb.TableFieldSchema_REPEATED
		}
		converted := &storagepb.TableFieldSchema{
			Name:                   field.Name,
			Description:            field.Description,
			Type:                   typ,
			Mode:                   mode,
			MaxLength:              field.MaxLength,
			Precision:              field.Precision,
			Scale:                  field.Scale,
			DefaultValueExpression: field.DefaultValueExpression,
		}
		if field.TimestampPrecision != 0 {
			converted.TimestampPrecision = wrapperspb.Int64(field.TimestampPrecision)
		}
		if field.RangeElementType != nil {
			rangeType, ok := bigQueryToStorageType[field.RangeElementType.Type]
			if !ok {
				return nil, fmt.Errorf("field %q has unsupported range element type %s", field.Name, field.RangeElementType.Type)
			}
			converted.RangeElementType = &storagepb.TableFieldSchema_FieldElementType{Type: rangeType}
		}
		var err error
		converted.Fields, err = fieldsFromBigQuery(field.Schema)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.Name, err)
		}
		out = append(out, converted)
	}
	return out, nil
}

// CompatibilityDifferences reports missing or incompatible fields in have.
// Extra destination fields are allowed because older exporters leave additive
// nullable columns unset.
func CompatibilityDifferences(want, have *storagepb.TableSchema) []string {
	if want == nil {
		return nil
	}
	if have == nil {
		return []string{"destination schema is nil"}
	}
	return fieldDifferences("", want.GetFields(), have.GetFields())
}

func fieldDifferences(prefix string, want, have []*storagepb.TableFieldSchema) []string {
	byName := make(map[string]*storagepb.TableFieldSchema, len(have))
	for _, field := range have {
		if field != nil {
			byName[field.GetName()] = field
		}
	}
	wantedNames := make(map[string]bool, len(want))
	var differences []string
	for _, expected := range want {
		if expected == nil {
			continue
		}
		wantedNames[expected.GetName()] = true
		path := strings.TrimPrefix(prefix+"."+expected.GetName(), ".")
		actual, ok := byName[expected.GetName()]
		if !ok {
			differences = append(differences, path+" is missing")
			continue
		}
		if actual.GetType() != expected.GetType() {
			differences = append(differences, fmt.Sprintf(
				"%s has type %s, want %s", path, actual.GetType(), expected.GetType()))
		}
		if actual.GetMode() != expected.GetMode() {
			differences = append(differences, fmt.Sprintf(
				"%s has mode %s, want %s", path, actual.GetMode(), expected.GetMode()))
		}
		differences = append(differences,
			fieldDifferences(path, expected.GetFields(), actual.GetFields())...)
	}
	for _, actual := range have {
		if actual == nil || wantedNames[actual.GetName()] || actual.GetMode() != storagepb.TableFieldSchema_REQUIRED {
			continue
		}
		path := strings.TrimPrefix(prefix+"."+actual.GetName(), ".")
		differences = append(differences, path+" is an extra REQUIRED field")
	}
	return differences
}
