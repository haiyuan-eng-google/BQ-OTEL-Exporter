// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
	"strings"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// DestinationOptions configures table-level partitioning options when
// creating a destination table via the BigQuery metadata API.
type DestinationOptions struct {
	PartitionExpirationDays int
	RequirePartitionFilter  bool
}

// DestinationMetadata builds the physical table and logical deduplication
// view metadata for one of the exporter's canonical signal contracts using
// default DestinationOptions.
func DestinationMetadata(
	in *storagepb.TableSchema, project, dataset, table string,
) (*bq.TableMetadata, string, error) {
	return DestinationMetadataWithOptions(in, project, dataset, table, DestinationOptions{})
}

// DestinationMetadataWithOptions builds the physical table and logical
// deduplication view metadata for one of the exporter's canonical signal
// contracts with explicit partitioning options.
func DestinationMetadataWithOptions(
	in *storagepb.TableSchema, project, dataset, table string, opts DestinationOptions,
) (*bq.TableMetadata, string, error) {
	bqs, err := ToBigQuery(in)
	if err != nil {
		return nil, "", err
	}
	md := &bq.TableMetadata{
		Schema:                 bqs,
		RequirePartitionFilter: opts.RequirePartitionFilter,
	}
	var queryTemplate string
	switch {
	case proto.Equal(in, SpansTableSchema()):
		md.TimePartitioning = &bq.TimePartitioning{Type: bq.HourPartitioningType, Field: "start_time"}
		md.Clustering = &bq.Clustering{Fields: []string{"service_name", "trace_id", "name"}}
		queryTemplate = SpansDedupViewQuery
	case proto.Equal(in, LogsTableSchema()):
		md.TimePartitioning = &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "timestamp"}
		md.Clustering = &bq.Clustering{Fields: []string{"service_name", "trace_id", "severity_number"}}
		queryTemplate = LogsDedupViewQuery
	case proto.Equal(in, MetricsTableSchema()):
		md.TimePartitioning = &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "timestamp"}
		md.Clustering = &bq.Clustering{Fields: []string{"service_name", "metric_name", "metric_type"}}
		queryTemplate = MetricsDedupViewQuery
	default:
		return nil, "", fmt.Errorf("schema is not a canonical trace, log, or metric contract")
	}
	if opts.PartitionExpirationDays > 0 {
		md.TimePartitioning.Expiration = time.Duration(opts.PartitionExpirationDays) * 24 * time.Hour
	}
	return md, fmt.Sprintf(queryTemplate, project, dataset, table), nil
}

// quarantinePartitionExpiration matches partition_expiration_days in
// QuarantineDDL.
const quarantinePartitionExpiration = 30 * 24 * time.Hour

// QuarantineDestinationMetadata builds the physical table metadata of the
// opt-in quarantine table, matching QuarantineDDL. Unlike the signal tables,
// it has no deduplication view.
func QuarantineDestinationMetadata() (*bq.TableMetadata, error) {
	return QuarantineDestinationMetadataWithOptions(DestinationOptions{})
}

// QuarantineDestinationMetadataWithOptions builds the physical table metadata
// of the opt-in quarantine table with explicit partitioning options. When
// opts.PartitionExpirationDays is 0, the default 30-day expiration from
// QuarantineDDL is applied.
func QuarantineDestinationMetadataWithOptions(opts DestinationOptions) (*bq.TableMetadata, error) {
	bqs, err := ToBigQuery(QuarantineTableSchema())
	if err != nil {
		return nil, err
	}
	expiration := quarantinePartitionExpiration
	if opts.PartitionExpirationDays > 0 {
		expiration = time.Duration(opts.PartitionExpirationDays) * 24 * time.Hour
	}
	return &bq.TableMetadata{
		Schema:                 bqs,
		RequirePartitionFilter: opts.RequirePartitionFilter,
		TimePartitioning: &bq.TimePartitioning{
			Type:       bq.DayPartitioningType,
			Field:      "ingest_time",
			Expiration: expiration,
		},
		Clustering: &bq.Clustering{Fields: []string{"signal", "error_code", "service_name"}},
	}, nil
}

// SpanCompanionViewOrder lists the span companion views in dependency order so
// v_gen_ai_spans is created before the rollup views that query it.
var SpanCompanionViewOrder = []string{
	SpansAsAllSpansViewName,
	GenAISpansViewName,
	GenAIModelCostsViewName,
	GenAISessionsViewName,
	GenAIToolUsageViewName,
}

// CompanionViewsMetadata builds the optional companion views of one signal,
// keyed by view name: otel_spans_as_allspans, v_gen_ai_spans,
// v_gen_ai_model_costs, v_gen_ai_sessions, and v_gen_ai_tool_usage for traces,
// and v_gen_ai_logs for logs. The views read the dedup views, never the raw
// tables, and v_gen_ai_logs reads the span dedup view as well as the log one,
// so it can only be created once the span dedup view exists.
func CompanionViewsMetadata(
	project, dataset, spansTable, logsTable string, isSpans bool,
) map[string]*bq.TableMetadata {
	view := func(query string) *bq.TableMetadata {
		return &bq.TableMetadata{ViewQuery: query, UseLegacySQL: false}
	}
	if isSpans {
		return map[string]*bq.TableMetadata{
			SpansAsAllSpansViewName: view(fmt.Sprintf(SpansAsAllSpansViewQuery, project, dataset, spansTable)),
			GenAISpansViewName:      view(fmt.Sprintf(GenAISpansViewQuery, project, dataset, spansTable)),
			GenAIModelCostsViewName: view(fmt.Sprintf(GenAIModelCostsViewQuery, project, dataset)),
			GenAISessionsViewName:   view(fmt.Sprintf(GenAISessionsViewQuery, project, dataset)),
			GenAIToolUsageViewName:  view(fmt.Sprintf(GenAIToolUsageViewQuery, project, dataset)),
		}
	}
	return map[string]*bq.TableMetadata{
		GenAILogsViewName: view(fmt.Sprintf(GenAILogsViewQuery, project, dataset, logsTable, spansTable)),
	}
}

// ReconcileAdditiveSchema appends any NULLABLE or REPEATED fields present in
// want but missing from existing (both at top level and inside RECORD fields).
// Required fields missing from existing are not added because BigQuery does
// not permit adding REQUIRED columns to an existing table.
func ReconcileAdditiveSchema(existing bq.Schema, want *storagepb.TableSchema) (bq.Schema, bool, error) {
	if want == nil {
		return existing, false, nil
	}
	wantBQ, err := ToBigQuery(want)
	if err != nil {
		return nil, false, err
	}
	merged, changed := reconcileBigQueryFields(existing, wantBQ)
	return merged, changed, nil
}

func reconcileBigQueryFields(existing, want bq.Schema) (bq.Schema, bool) {
	wantByName := make(map[string]*bq.FieldSchema, len(want))
	for _, f := range want {
		if f != nil {
			wantByName[f.Name] = f
		}
	}
	existingByName := make(map[string]bool, len(existing))
	merged := make(bq.Schema, len(existing))
	changed := false
	for i, ef := range existing {
		if ef == nil {
			continue
		}
		existingByName[ef.Name] = true
		copyField := *ef
		if wf, ok := wantByName[ef.Name]; ok && ef.Type == bq.RecordFieldType && wf.Type == bq.RecordFieldType &&
			ef.Required == wf.Required && ef.Repeated == wf.Repeated {
			nestedMerged, nestedChanged := reconcileBigQueryFields(ef.Schema, wf.Schema)
			if nestedChanged {
				copyField.Schema = nestedMerged
				changed = true
			}
		}
		merged[i] = &copyField
	}
	for _, wf := range want {
		if wf == nil || existingByName[wf.Name] || wf.Required {
			continue
		}
		copyField := *wf
		merged = append(merged, &copyField)
		changed = true
	}
	return merged, changed
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
		if actual.GetMaxLength() != expected.GetMaxLength() {
			differences = append(differences, fmt.Sprintf(
				"%s has max length %d, want %d", path, actual.GetMaxLength(), expected.GetMaxLength()))
		}
		if actual.GetPrecision() != expected.GetPrecision() {
			differences = append(differences, fmt.Sprintf(
				"%s has precision %d, want %d", path, actual.GetPrecision(), expected.GetPrecision()))
		}
		if actual.GetScale() != expected.GetScale() {
			differences = append(differences, fmt.Sprintf(
				"%s has scale %d, want %d", path, actual.GetScale(), expected.GetScale()))
		}
		if actual.GetDefaultValueExpression() != expected.GetDefaultValueExpression() {
			differences = append(differences, fmt.Sprintf(
				"%s has default value expression %q, want %q", path,
				actual.GetDefaultValueExpression(), expected.GetDefaultValueExpression()))
		}
		if actual.GetTimestampPrecision().GetValue() != expected.GetTimestampPrecision().GetValue() {
			differences = append(differences, fmt.Sprintf(
				"%s has timestamp precision %d, want %d", path,
				actual.GetTimestampPrecision().GetValue(), expected.GetTimestampPrecision().GetValue()))
		}
		actualRangeType, expectedRangeType := actual.GetRangeElementType(), expected.GetRangeElementType()
		if (actualRangeType == nil) != (expectedRangeType == nil) ||
			actualRangeType.GetType() != expectedRangeType.GetType() {
			differences = append(differences, fmt.Sprintf(
				"%s has range element type %s, want %s", path,
				actualRangeType.GetType(), expectedRangeType.GetType()))
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
