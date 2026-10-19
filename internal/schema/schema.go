// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package schema holds the versioned BigQuery table contract and the canonical
// deduplication SQL that goes with it. docs/schema-rationale.md explains the
// reasoning behind both.
//
// The DDL here is the operator-facing representation of the typed TableSchema
// contract in tableschema.go. It must track that contract column for column:
// downstream queries and dbt models pin against it, so divergence between the
// two representations is a bug.
package schema // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"

// Version is the per-row schema_version value.
//
// The contract was published as v0alpha1 at M2, during which documented
// breaking corrections are allowed; v0alpha2 is the first such correction. It
// freezes as v1 only at M4, after at least two independent design partners
// complete representative workflows. Do not bump this to v1 without that gate.
const Version = "v0alpha2"

// SpansDDL creates the v0alpha2 span table.
//
// Every timestamp is stored twice on purpose: a microsecond-precision
// TIMESTAMP for querying and partitioning, and the exact OTLP nanoseconds as
// INT64, because BigQuery TIMESTAMP cannot hold nanoseconds. A span whose
// nanoseconds do not fit INT64 is rejected before a row is built.
//
// Partitioning is hourly on start_time, and clustering on (service_name,
// trace_id, name) keeps per-service scans and trace lookups pruned.
const SpansDDL = `
CREATE TABLE IF NOT EXISTS ` + "`%[1]s.%[2]s.%[3]s`" + ` (
  -- Time family
  start_time                         TIMESTAMP NOT NULL,
  start_time_unix_nano               INT64     NOT NULL,
  end_time                           TIMESTAMP NOT NULL,
  end_time_unix_nano                 INT64     NOT NULL,
  receive_time                       TIMESTAMP NOT NULL,
  receive_time_unix_nano             INT64     NOT NULL,
  duration_nano                      INT64     NOT NULL,
  -- Identity family
  trace_id                           STRING    NOT NULL,
  span_id                            STRING    NOT NULL,
  parent_span_id                     STRING,
  trace_state                        STRING,
  flags                              INT64,
  -- Span payload family (Plain JSON + attributes_schema sidecar)
  name                               STRING    NOT NULL,
  kind                               INT64     NOT NULL,
  kind_name                          STRING,
  status STRUCT<
    code                             INT64,
    code_name                        STRING,
    message                          STRING
  >,
  attributes                         JSON,
  attributes_schema                  JSON,
  dropped_attributes_count           INT64,
  -- Resource & Instrumentation Scope family
  service_name                       STRING,
  resource STRUCT<
    attributes                       JSON,
    attributes_schema                JSON,
    dropped_attributes_count         INT64
  >,
  resource_schema_url                STRING,
  instrumentation_scope STRUCT<
    name                             STRING,
    version                          STRING,
    attributes                       JSON,
    attributes_schema                JSON,
    dropped_attributes_count         INT64
  >,
  scope_schema_url                   STRING,
  -- Signal detail: Inline Events & Links
  events ARRAY<STRUCT<
    time                             TIMESTAMP,
    time_unix_nano                   INT64,
    name                             STRING,
    attributes                       JSON,
    attributes_schema                JSON,
    dropped_attributes_count         INT64
  >>,
  dropped_events_count               INT64,
  links ARRAY<STRUCT<
    trace_id                         STRING,
    span_id                          STRING,
    trace_state                      STRING,
    flags                            INT64,
    attributes                       JSON,
    attributes_schema                JSON,
    dropped_attributes_count         INT64
  >>,
  dropped_links_count                INT64,
  -- Multimodal & Oversized Payload Offloading via BigQuery ObjectRef
  payload_refs ARRAY<STRUCT<
    attribute_path                   STRING,
    part_index                       INT64,
    mime_type                        STRING,
    storage_mode                     STRING,
    byte_size                        INT64,
    sha256                           STRING,
    object_ref STRUCT<
      uri                            STRING,
      version                        STRING,
      authorizer                     STRING,
      details                        JSON
    >
  >>,
  -- Contract metadata
  schema_version                     STRING    NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(start_time, HOUR)
CLUSTER BY service_name, trace_id, name;
`

// LogsDDL creates the v0alpha2 log table.
//
// As with spans, every timestamp is stored twice: a microsecond-precision
// TIMESTAMP and the exact OTLP nanoseconds as INT64. A record whose
// nanoseconds do not fit INT64 is rejected before a row is built. timestamp
// is never NULL: it falls back from the event time to the observed time to
// the exporter receive time, and timestamp_source records which one won.
//
// Partitioning is daily on timestamp, and clustering on (service_name,
// trace_id, severity_number) keeps per-service scans, trace correlation and
// severity filters pruned.
const LogsDDL = `
CREATE TABLE IF NOT EXISTS ` + "`%[1]s.%[2]s.%[3]s`" + ` (
  -- Time family
  timestamp                          TIMESTAMP NOT NULL,
  timestamp_source                   STRING    NOT NULL,
  time_unix_nano                     INT64     NOT NULL,
  observed_timestamp                 TIMESTAMP,
  observed_time_unix_nano            INT64     NOT NULL,
  receive_time                       TIMESTAMP NOT NULL,
  receive_time_unix_nano             INT64     NOT NULL,
  -- Identity & Correlation family
  trace_id                           STRING,
  span_id                            STRING,
  flags                              INT64,
  -- Log payload family (Plain JSON body & attributes + sidecar)
  event_name                         STRING,
  severity_number                    INT64,
  severity_text                      STRING,
  body                               JSON,
  attributes                         JSON,
  attributes_schema                  JSON,
  dropped_attributes_count           INT64,
  -- Resource & Instrumentation Scope family
  service_name                       STRING,
  resource STRUCT<
    attributes                       JSON,
    attributes_schema                JSON,
    dropped_attributes_count         INT64
  >,
  resource_schema_url                STRING,
  instrumentation_scope STRUCT<
    name                             STRING,
    version                          STRING,
    attributes                       JSON,
    attributes_schema                JSON,
    dropped_attributes_count         INT64
  >,
  scope_schema_url                   STRING,
  -- Multimodal & Oversized Payload Offloading via BigQuery ObjectRef
  payload_refs ARRAY<STRUCT<
    attribute_path                   STRING,
    part_index                       INT64,
    mime_type                        STRING,
    storage_mode                     STRING,
    byte_size                        INT64,
    sha256                           STRING,
    object_ref STRUCT<
      uri                            STRING,
      version                        STRING,
      authorizer                     STRING,
      details                        JSON
    >
  >>,
  -- Dedup Identity & Contract family (with intra_batch_ordinal in record_fingerprint)
  log_name                           STRING    NOT NULL,
  insert_id                          STRING    NOT NULL,
  record_fingerprint                 STRING,
  schema_version                     STRING    NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY service_name, trace_id, severity_number;
`

// MetricsDDL creates the v0alpha2 metric data-point table.
//
// Partitioning is daily on timestamp, and clustering on (service_name,
// metric_name, metric_type) keeps per-service and per-instrument queries
// pruned across all five OTLP metric types (Gauge, Sum, Histogram,
// ExponentialHistogram, Summary).
const MetricsDDL = `
CREATE TABLE IF NOT EXISTS ` + "`%[1]s.%[2]s.%[3]s`" + ` (
  -- Time family
  timestamp                          TIMESTAMP NOT NULL,
  time_unix_nano                     INT64     NOT NULL,
  start_time                         TIMESTAMP,
  start_time_unix_nano               INT64,
  receive_time                       TIMESTAMP NOT NULL,
  receive_time_unix_nano             INT64     NOT NULL,
  -- Metric identity & metadata
  service_name                       STRING,
  metric_name                        STRING    NOT NULL,
  metric_description                 STRING,
  metric_unit                        STRING,
  metric_type                        STRING    NOT NULL,
  aggregation_temporality            STRING,
  is_monotonic                       BOOL,
  flags                              INT64,
  attributes                         JSON,
  attributes_schema                  JSON,
  metric_metadata                    JSON,
  metric_metadata_schema             JSON,
  -- Scalar values (GAUGE / SUM)
  value_type                         STRING,
  value_double                       FLOAT64,
  value_int64                        INT64,
  -- Distribution values (HISTOGRAM / EXPONENTIAL_HISTOGRAM / SUMMARY)
  count                              INT64,
  sum                                FLOAT64,
  min                                FLOAT64,
  max                                FLOAT64,
  explicit_bounds                    ARRAY<FLOAT64>,
  bucket_counts                      ARRAY<INT64>,
  exp_scale                          INT64,
  exp_zero_count                     INT64,
  exp_zero_threshold                 FLOAT64,
  exp_positive STRUCT<
    offset                           INT64,
    bucket_counts                    ARRAY<INT64>
  >,
  exp_negative STRUCT<
    offset                           INT64,
    bucket_counts                    ARRAY<INT64>
  >,
  summary_quantiles ARRAY<STRUCT<
    quantile                         FLOAT64,
    value                            FLOAT64
  >>,
  -- Exemplars (trace/span correlation)
  exemplars ARRAY<STRUCT<
    time                             TIMESTAMP,
    time_unix_nano                   INT64,
    value_double                     FLOAT64,
    value_int64                      INT64,
    span_id                          STRING,
    trace_id                         STRING,
    filtered_attributes              JSON,
    filtered_attributes_schema       JSON
  >>,
  -- Resource & Instrumentation Scope family
  resource STRUCT<
    attributes                       JSON,
    attributes_schema                JSON,
    dropped_attributes_count         INT64
  >,
  resource_schema_url                STRING,
  instrumentation_scope STRUCT<
    name                             STRING,
    version                          STRING,
    attributes                       JSON,
    attributes_schema                JSON,
    dropped_attributes_count         INT64
  >,
  scope_schema_url                   STRING,
  -- Dedup & Contract identity
  point_fingerprint                  STRING    NOT NULL,
  schema_version                     STRING    NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY service_name, metric_name, metric_type;
`

// QuarantineDDL creates the opt-in quarantine (dead-letter) table, which the
// exporter writes only when quarantine.enabled is set. A span, log, or metric
// record that cannot be written to its signal table, because of an invalid ID,
// a missing or overflowing timestamp, a value over a configured limit, a row
// over the row size limit, or a row BigQuery itself rejects, lands here
// instead of being dropped, with the reason in error_code and error_message
// and the record serialized as OTLP JSON in raw_payload.
//
// Partitioning is daily on ingest_time and partitions expire after 30 days by
// default, because the table is a triage queue rather than an archive;
// clustering on (signal, error_code, service_name) keeps "what is failing, and
// where" queries pruned. There is no dedup view: a record whose whole batch is
// retried can be quarantined more than once.
const QuarantineDDL = `
CREATE TABLE IF NOT EXISTS ` + "`%[1]s.%[2]s.%[3]s`" + ` (
  ingest_time                        TIMESTAMP NOT NULL,
  signal                             STRING    NOT NULL, -- 'spans' | 'logs' | 'metrics'
  error_code                         STRING    NOT NULL, -- e.g. 'INVALID_SPAN_ID', 'MISSING_END_TIME', 'OVERSIZE_ROW'
  error_message                      STRING    NOT NULL,
  service_name                       STRING,
  trace_id                           STRING,
  span_id                            STRING,
  raw_payload                        STRING,             -- serialized OTLP JSON/proto up to 8 MiB
  raw_attributes                     JSON,
  payload_ref STRUCT<
    uri                              STRING,
    version                          STRING,
    authorizer                       STRING,
    details                          JSON
  >,                                                     -- populated when raw_payload > 8 MiB is offloaded to GCS
  exporter_instance                  STRING,
  schema_version                     STRING    NOT NULL
)
PARTITION BY DATE(ingest_time)
CLUSTER BY signal, error_code, service_name
OPTIONS (
  partition_expiration_days = 30
);
`

// SpansDedupViewDDL is the canonical span deduplication view.
//
// Identity is (start_time, service_name, trace_id, span_id). Every retry of a
// span carries the same start time and service, so this is the span identity
// with the partitioning column and the leading clustering columns added.
// name, the last clustering column, is added too: it is fixed for a span, so
// it never splits the retries of one. BigQuery pushes a filter below a window
// function only when the filter refers to PARTITION BY columns, so the window
// partitions on every column the base table is partitioned, clustered or
// search-indexed on. A filter on any of them then reaches the table scan, and
// a query over the view still prunes partitions and clusters. Ordering is on
// the raw nanos rather than end_time, because microsecond precision alone can
// tie across retries; receive_time_unix_nano breaks any remaining tie.
//
// Planned: once truncate-and-flag lands, an is_truncated ASC term goes first
// in the ORDER BY, so a truncated copy can never outrank the full copy it
// duplicates. Not applied here because the column is not yet part of the span
// contract.
const SpansDedupViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s.%[3]s_dedup`" + ` AS
` + SpansDedupViewQuery + `;
`

// SpansDedupViewQuery is shared by operator-run DDL and metadata-API view
// creation so their deduplication behavior cannot drift.
const SpansDedupViewQuery = `
SELECT *
FROM ` + "`%[1]s.%[2]s.%[3]s`" + `
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY
    start_time,
    service_name,
    trace_id,
    span_id,
    name
  ORDER BY
    end_time_unix_nano DESC,
    receive_time_unix_nano DESC
) = 1
`

// LogsDedupViewDDL is the canonical log deduplication view.
//
// Identity is (timestamp, time_unix_nano, service_name, log_name, insert_id),
// aligned with the Cloud Logging (log_name, insert_id) identity. insert_id is
// the upstream record ID when instrumentation supplies one and the record
// fingerprint otherwise. The window also partitions on trace_id, span_id and
// severity_number. They are part of what the record fingerprint covers, so
// retries of one record agree on them, and records that share an insert_id but
// differ in any of them are not retries of each other. With the partitioning
// column and service_name they cover every column the base table is
// partitioned, clustered or search-indexed on. BigQuery pushes a filter below
// a window function only when the filter refers to PARTITION BY columns, so a
// filter on any of them reaches the table scan and a query over the view still
// prunes partitions and clusters; time_unix_nano keeps apart records that
// share a microsecond timestamp. The most recently received copy wins.
//
// Scope of the promise, deliberately narrow: this removes retry duplicates.
// The fingerprint separates identical records only within one export batch,
// through its intra-batch ordinal, so identical records delivered in separate
// batches collapse to one row. A record with neither an event nor an observed
// time is stamped with the exporter receive time, which differs on every
// retry, so retries of such a record are not collapsed.
const LogsDedupViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s.%[3]s_dedup`" + ` AS
` + LogsDedupViewQuery + `;
`

// LogsDedupViewQuery is shared by operator-run DDL and metadata-API view
// creation so their deduplication behavior cannot drift.
const LogsDedupViewQuery = `
SELECT *
FROM ` + "`%[1]s.%[2]s.%[3]s`" + `
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY
    timestamp,
    time_unix_nano,
    service_name,
    trace_id,
    span_id,
    severity_number,
    log_name,
    insert_id
  ORDER BY
    receive_time_unix_nano DESC
) = 1
`

// MetricsDedupViewDDL is the canonical metric deduplication view.
//
// Identity is (timestamp, time_unix_nano, service_name, metric_name,
// point_fingerprint). The window also partitions on metric_type, the last
// clustering column of the base table: the fingerprint covers it, so retries
// of one point agree on it. BigQuery pushes a filter below a window function
// only when the filter refers to PARTITION BY columns, so partitioning on the
// partitioning column and every clustering column lets filters on them prune
// partitions and clusters before the window function runs.
// receive_time_unix_nano selects the most recently received copy.
//
// The search index covers all columns, which adds nothing to the window: the
// exemplar trace_id and span_id it serves sit inside a repeated field, so
// they cannot be PARTITION BY keys.
const MetricsDedupViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s.%[3]s_dedup`" + ` AS
` + MetricsDedupViewQuery + `;
`

// MetricsDedupViewQuery is shared by operator-run DDL and metadata-API view
// creation so their deduplication behavior cannot drift.
const MetricsDedupViewQuery = `
SELECT *
FROM ` + "`%[1]s.%[2]s.%[3]s`" + `
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY
    timestamp,
    time_unix_nano,
    service_name,
    metric_name,
    metric_type,
    point_fingerprint
  ORDER BY
    receive_time_unix_nano DESC
) = 1
`

// SpansSearchIndexDDL creates the trace lookup search index on the span
// table. NO_OP_ANALYZER indexes every trace_id and span_id value whole,
// without tokenizing it, which suits exact-match lookups of one trace or one
// span.
//
// The exporter runs this statement during startup only when
// auto_create.search_indexes is enabled (which requires bigquery.jobs.create);
// otherwise operators run it themselves or manage it with their
// infrastructure-as-code tooling. The index name is derived from the table
// name, so the default otel_spans table gets otel_spans_trace_lookup_idx.
const SpansSearchIndexDDL = `
CREATE SEARCH INDEX IF NOT EXISTS ` + "`%[3]s_trace_lookup_idx`" + `
ON ` + "`%[1]s.%[2]s.%[3]s`" + ` (trace_id, span_id) OPTIONS (analyzer = 'NO_OP_ANALYZER');
`

// LogsSearchIndexDDL creates the trace lookup search index on the log table,
// so looking up the records correlated with one trace or one span can use the
// index. As with SpansSearchIndexDDL, the exporter runs it only when
// auto_create.search_indexes is enabled, and the default otel_logs table gets
// otel_logs_trace_lookup_idx.
const LogsSearchIndexDDL = `
CREATE SEARCH INDEX IF NOT EXISTS ` + "`%[3]s_trace_lookup_idx`" + `
ON ` + "`%[1]s.%[2]s.%[3]s`" + ` (trace_id, span_id) OPTIONS (analyzer = 'NO_OP_ANALYZER');
`

// MetricsSearchIndexDDL creates a search index across all columns on the
// metrics table to accelerate point lookups by exemplar trace_id, span_id, and
// JSON attributes across partitions when auto_create.search_indexes is enabled.
const MetricsSearchIndexDDL = `
CREATE SEARCH INDEX IF NOT EXISTS ` + "`%[3]s_trace_lookup_idx`" + `
ON ` + "`%[1]s.%[2]s.%[3]s`" + ` (ALL COLUMNS) OPTIONS (analyzer = 'NO_OP_ANALYZER');
`

// Companion view names. The companion views are optional conveniences over
// the dedup views, created only when auto_create.companion_views is enabled.
// Their names are fixed rather than derived from the table names, so one
// dataset holds one set of them.
const (
	SpansAsAllSpansViewName      = "otel_spans_as_allspans"
	GenAISpansViewName           = "v_gen_ai_spans"
	GenAILogsViewName            = "v_gen_ai_logs"
	GenAIMetricsViewName         = "v_gen_ai_metrics"
	GenAIMetricExemplarsViewName = "v_gen_ai_metric_exemplars"
	GenAIModelCostsViewName      = "v_gen_ai_model_costs"
	GenAISessionsViewName        = "v_gen_ai_sessions"
	GenAIToolUsageViewName       = "v_gen_ai_tool_usage"
	GenAIAgentTrajectoryViewName = "v_gen_ai_agent_trajectory"
)

// SpansAsAllSpansViewDDL creates otel_spans_as_allspans, which presents the
// deduplicated spans in the 25-column layout of the Cloud Trace _AllSpans
// view. Every column passes through unchanged except status, which is
// narrowed to its code and message.
const SpansAsAllSpansViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s." + SpansAsAllSpansViewName + "`" + ` AS
` + SpansAsAllSpansViewQuery + `;
`

// SpansAsAllSpansViewQuery is shared by operator-run DDL and metadata-API
// view creation. It takes (project, dataset, spans table) and reads the span
// dedup view, never the raw table.
const SpansAsAllSpansViewQuery = `
SELECT
  trace_id,
  span_id,
  trace_state,
  parent_span_id,
  name,
  kind,
  start_time,
  start_time_unix_nano,
  end_time,
  end_time_unix_nano,
  receive_time,
  receive_time_unix_nano,
  duration_nano,
  attributes,
  attributes_schema,
  dropped_attributes_count,
  events,
  dropped_events_count,
  links,
  dropped_links_count,
  STRUCT(status.code AS code, status.message AS message) AS status,
  resource,
  resource_schema_url,
  instrumentation_scope,
  scope_schema_url
FROM ` + "`%[1]s.%[2]s.%[3]s_dedup`" + `
`

// GenAISpansViewDDL creates v_gen_ai_spans: the spans that carry
// gen_ai.operation.name, gen_ai.system, gen_ai.provider.name,
// gcp.vertex.agent.invocation_id, gen_ai.main_agent.name or
// gen_ai.main_agent.id, with the OpenTelemetry GenAI semantic
// convention attributes (including v1.37.0+ and v1.40.0 provider, cache/reasoning
// token, output, tool, agent, and data source fields) and the Agent
// Development Kit gcp.vertex.agent.* attributes promoted out of the plain
// JSON attributes into typed top-level columns. Where the same fact can arrive
// under more than one key, such as gen_ai.system and its successor
// gen_ai.provider.name, or the reasoning token count under
// gen_ai.usage.reasoning.output_tokens and its earlier name
// gen_ai.usage.reasoning_tokens, the keys are coalesced in a fixed order, the
// current key first. The error.type attribute is promoted as error_type,
// followed by the main-agent (semantic-conventions-genai@f3c6d0c9),
// agent-version and response-id (v1.40.0), and time-to-first-chunk (v1.41.0)
// attributes (coalescing span attributes first and resource.attributes second
// for the agent-entity fields), appended at the end so the position of every
// earlier column is unchanged. The raw attribute, resource, scope, event and
// link columns and payload_refs are kept for anything not promoted.
const GenAISpansViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s." + GenAISpansViewName + "`" + ` AS
` + GenAISpansViewQuery + `;
`

// GenAISpansViewQuery is shared by operator-run DDL and metadata-API view
// creation. It takes (project, dataset, spans table) and reads the span dedup
// view, never the raw table.
const GenAISpansViewQuery = `
SELECT
  start_time,
  end_time,
  duration_nano,
  ROUND(duration_nano / 1e6, 3) AS duration_ms,
  trace_id,
  span_id,
  parent_span_id,
  service_name,
  name AS span_name,
  kind AS span_kind,
  status.code AS status_code,
  status.message AS status_message,
  -- Promoted OTel GenAI semantic convention fields (Plain JSON)
  JSON_VALUE(attributes, '$."gen_ai.operation.name"') AS gen_ai_operation_name,
  COALESCE(
    JSON_VALUE(attributes, '$."gen_ai.system"'),
    JSON_VALUE(attributes, '$."gen_ai.provider.name"')
  ) AS gen_ai_system,
  JSON_VALUE(attributes, '$."gen_ai.request.model"') AS gen_ai_request_model,
  JSON_VALUE(attributes, '$."gen_ai.response.model"') AS gen_ai_response_model,
  COALESCE(
    JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
    JSON_VALUE(attributes, '$."gcp.vertex.agent.name"'),
    service_name
  ) AS gen_ai_agent_name,
  JSON_VALUE(attributes, '$."gen_ai.agent.id"') AS gen_ai_agent_id,
  COALESCE(
    JSON_VALUE(attributes, '$."gen_ai.conversation.id"'),
    JSON_VALUE(attributes, '$."gcp.vertex.agent.session_id"')
  ) AS gen_ai_conversation_id,
  LAX_INT64(attributes['gen_ai.usage.input_tokens']) AS gen_ai_usage_input_tokens,
  LAX_INT64(attributes['gen_ai.usage.output_tokens']) AS gen_ai_usage_output_tokens,
  LAX_FLOAT64(attributes['gen_ai.request.temperature']) AS gen_ai_request_temperature,
  JSON_VALUE(attributes, '$."gen_ai.response.finish_reasons"[0]') AS gen_ai_response_finish_reason,
  JSON_VALUE(attributes, '$."gen_ai.tool.name"') AS gen_ai_tool_name,
  JSON_VALUE(attributes, '$."gen_ai.tool.call.id"') AS gen_ai_tool_call_id,
  -- ADK / Vertex Agent Analytics attributes
  JSON_VALUE(attributes, '$."gcp.vertex.agent.event_id"') AS adk_event_id,
  COALESCE(
    JSON_VALUE(attributes, '$."gcp.vertex.agent.session_id"'),
    JSON_VALUE(attributes, '$."gen_ai.conversation.id"')
  ) AS adk_session_id,
  JSON_VALUE(attributes, '$."gcp.vertex.agent.invocation_id"') AS adk_invocation_id,
  COALESCE(
    JSON_VALUE(attributes, '$."user.id"'),
    JSON_VALUE(attributes, '$."gen_ai.user.id"')
  ) AS adk_user_id,
  -- Multimodal ObjectRef array and raw bags
  payload_refs,
  attributes,
  resource,
  instrumentation_scope,
  events,
  links,
  -- Promoted OTel GenAI v1.37.0+, v1.40.0, v1.41.0, and semantic-conventions-genai@f3c6d0c9 fields
  JSON_VALUE(attributes, '$."gen_ai.provider.name"') AS gen_ai_provider_name,
  COALESCE(
    LAX_INT64(attributes['gen_ai.usage.cache_read.input_tokens']),
    LAX_INT64(attributes['gen_ai.usage.cache_read_input_tokens'])
  ) AS gen_ai_usage_cache_read_input_tokens,
  COALESCE(
    LAX_INT64(attributes['gen_ai.usage.cache_creation.input_tokens']),
    LAX_INT64(attributes['gen_ai.usage.cache_creation_input_tokens'])
  ) AS gen_ai_usage_cache_creation_input_tokens,
  COALESCE(
    LAX_INT64(attributes['gen_ai.usage.reasoning.output_tokens']),
    LAX_INT64(attributes['gen_ai.usage.reasoning_tokens'])
  ) AS gen_ai_usage_reasoning_tokens,
  LAX_FLOAT64(attributes['gen_ai.request.top_p']) AS gen_ai_request_top_p,
  LAX_INT64(attributes['gen_ai.request.top_k']) AS gen_ai_request_top_k,
  LAX_INT64(attributes['gen_ai.request.max_tokens']) AS gen_ai_request_max_tokens,
  JSON_VALUE(attributes, '$."gen_ai.output.type"') AS gen_ai_output_type,
  JSON_VALUE(attributes, '$."gen_ai.tool.type"') AS gen_ai_tool_type,
  JSON_VALUE(attributes, '$."gen_ai.tool.description"') AS gen_ai_tool_description,
  JSON_VALUE(attributes, '$."gen_ai.agent.description"') AS gen_ai_agent_description,
  JSON_VALUE(attributes, '$."gen_ai.agent.parent.id"') AS gen_ai_agent_parent_id,
  JSON_VALUE(attributes, '$."gen_ai.data_source.id"') AS gen_ai_data_source_id,
  JSON_VALUE(attributes, '$."gen_ai.evaluation.name"') AS gen_ai_evaluation_name,
  COALESCE(
    LAX_FLOAT64(attributes['gen_ai.evaluation.score.value']),
    LAX_FLOAT64(attributes['gen_ai.evaluation.score'])
  ) AS gen_ai_evaluation_score,
  JSON_VALUE(attributes, '$."error.type"') AS error_type,
  COALESCE(
    JSON_VALUE(attributes, '$."gen_ai.main_agent.id"'),
    JSON_VALUE(resource.attributes, '$."gen_ai.main_agent.id"')
  ) AS gen_ai_main_agent_id,
  COALESCE(
    JSON_VALUE(attributes, '$."gen_ai.main_agent.name"'),
    JSON_VALUE(resource.attributes, '$."gen_ai.main_agent.name"')
  ) AS gen_ai_main_agent_name,
  COALESCE(
    JSON_VALUE(attributes, '$."gen_ai.main_agent.description"'),
    JSON_VALUE(resource.attributes, '$."gen_ai.main_agent.description"')
  ) AS gen_ai_main_agent_description,
  COALESCE(
    JSON_VALUE(attributes, '$."gen_ai.agent.version"'),
    JSON_VALUE(resource.attributes, '$."gen_ai.agent.version"')
  ) AS gen_ai_agent_version,
  JSON_VALUE(attributes, '$."gen_ai.response.id"') AS gen_ai_response_id,
  LAX_FLOAT64(attributes['gen_ai.response.time_to_first_chunk']) AS gen_ai_response_time_to_first_chunk
FROM ` + "`%[1]s.%[2]s.%[3]s_dedup`" + `
WHERE JSON_VALUE(attributes, '$."gen_ai.operation.name"') IS NOT NULL
   OR JSON_VALUE(attributes, '$."gen_ai.system"') IS NOT NULL
   OR JSON_VALUE(attributes, '$."gen_ai.provider.name"') IS NOT NULL
   OR JSON_VALUE(attributes, '$."gcp.vertex.agent.invocation_id"') IS NOT NULL
   OR JSON_VALUE(attributes, '$."gen_ai.main_agent.name"') IS NOT NULL
   OR JSON_VALUE(attributes, '$."gen_ai.main_agent.id"') IS NOT NULL
`

// GenAILogsViewDDL creates v_gen_ai_logs: the log records whose event name
// starts with "gen_ai.", or that carry gen_ai.system, gen_ai.provider.name or
// gcp.vertex.agent.invocation_id, with the GenAI and agent fields promoted
// from the record's own attributes and, where the record lacks one, from the
// attributes of the span it belongs to. The event name falls back from the
// event_name column to the event.name attribute. The agent name, session ID
// and user ID are coalesced over the same attribute keys, in the same order,
// as the columns of v_gen_ai_spans with those names, the record's attributes
// first and the span's second. Unlike the span view, the agent name does not
// fall back to service_name.
const GenAILogsViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s." + GenAILogsViewName + "`" + ` AS
` + GenAILogsViewQuery + `;
`

// GenAILogsViewQuery is shared by operator-run DDL and metadata-API view
// creation. It takes (project, dataset, logs table, spans table) and left-joins
// the log dedup view to the span dedup view, so the span dedup view must exist
// before this view can be created.
//
// The join matches on trace_id and span_id, and on service_name with IS NOT
// DISTINCT FROM so that a record and a span that both lack a service name
// still match, which a plain equality or a USING clause would not do. The join
// also carries a range predicate on the span table's partitioning column: only
// a span that started from 24 hours before to 1 hour after the record's
// timestamp enriches the record. The range compares start_time with each
// record's own timestamp, which is not known when the query starts, and
// BigQuery does not use such a predicate to skip span partitions: in dry runs,
// a filter on timestamp left the estimated bytes of the span side as large as
// with no filter at all, and a query over this view is rejected when the span
// table requires a partition filter. The predicate says which spans enrich a
// record; it does not bound what the query scans. A query that has to bound the
// span scan should join the log dedup view to a start_time-bounded span dedup
// subquery directly. A span outside that window (a span that began more than a
// day before one of its log records, or more than an hour after it) does not
// enrich the record, whose own attributes are still projected. The LIKE
// pattern's percent sign is doubled because every template here goes through
// fmt.Sprintf.
const GenAILogsViewQuery = `
SELECT
  l.timestamp,
  l.receive_time,
  l.trace_id,
  l.span_id,
  l.service_name,
  COALESCE(NULLIF(l.event_name, ''), JSON_VALUE(l.attributes, '$."event.name"')) AS event_name,
  l.severity_number,
  l.severity_text,
  COALESCE(JSON_VALUE(l.attributes, '$."gen_ai.operation.name"'), JSON_VALUE(s.attributes, '$."gen_ai.operation.name"')) AS gen_ai_operation_name,
  COALESCE(
    JSON_VALUE(l.attributes, '$."gen_ai.system"'),
    JSON_VALUE(l.attributes, '$."gen_ai.provider.name"'),
    JSON_VALUE(s.attributes, '$."gen_ai.system"'),
    JSON_VALUE(s.attributes, '$."gen_ai.provider.name"')
  ) AS gen_ai_system,
  COALESCE(
    JSON_VALUE(l.attributes, '$."gen_ai.agent.name"'),
    JSON_VALUE(l.attributes, '$."gcp.vertex.agent.name"'),
    JSON_VALUE(s.attributes, '$."gen_ai.agent.name"'),
    JSON_VALUE(s.attributes, '$."gcp.vertex.agent.name"')
  ) AS gen_ai_agent_name,
  COALESCE(
    JSON_VALUE(l.attributes, '$."gcp.vertex.agent.session_id"'),
    JSON_VALUE(l.attributes, '$."gen_ai.conversation.id"'),
    JSON_VALUE(s.attributes, '$."gcp.vertex.agent.session_id"'),
    JSON_VALUE(s.attributes, '$."gen_ai.conversation.id"')
  ) AS adk_session_id,
  COALESCE(JSON_VALUE(l.attributes, '$."gcp.vertex.agent.invocation_id"'), JSON_VALUE(s.attributes, '$."gcp.vertex.agent.invocation_id"')) AS adk_invocation_id,
  COALESCE(JSON_VALUE(l.attributes, '$."gcp.vertex.agent.event_id"'), JSON_VALUE(s.attributes, '$."gcp.vertex.agent.event_id"')) AS adk_event_id,
  COALESCE(
    JSON_VALUE(l.attributes, '$."user.id"'),
    JSON_VALUE(l.attributes, '$."gen_ai.user.id"'),
    JSON_VALUE(s.attributes, '$."user.id"'),
    JSON_VALUE(s.attributes, '$."gen_ai.user.id"')
  ) AS adk_user_id,
  l.body,
  l.payload_refs,
  l.attributes,
  l.resource,
  l.instrumentation_scope,
  JSON_VALUE(l.attributes, '$."error.type"') AS error_type
FROM ` + "`%[1]s.%[2]s.%[3]s_dedup`" + ` l
LEFT JOIN ` + "`%[1]s.%[2]s.%[4]s_dedup`" + ` s
  ON l.trace_id = s.trace_id
  AND l.span_id = s.span_id
  AND l.service_name IS NOT DISTINCT FROM s.service_name
  AND s.start_time BETWEEN TIMESTAMP_SUB(l.timestamp, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(l.timestamp, INTERVAL 1 HOUR)
WHERE COALESCE(NULLIF(l.event_name, ''), JSON_VALUE(l.attributes, '$."event.name"')) LIKE 'gen_ai.%%'
   OR JSON_VALUE(l.attributes, '$."gen_ai.system"') IS NOT NULL
   OR JSON_VALUE(l.attributes, '$."gen_ai.provider.name"') IS NOT NULL
   OR JSON_VALUE(l.attributes, '$."gcp.vertex.agent.invocation_id"') IS NOT NULL
`

// GenAIMetricsViewDDL creates v_gen_ai_metrics: the metric points whose name
// starts with "gen_ai.", such as gen_ai.client.token.usage,
// gen_ai.client.operation.duration, and gen_ai.client.inference.usage.*, with
// the OpenTelemetry GenAI semantic convention attributes promoted out of the
// plain JSON attributes into typed columns. The system is coalesced from
// gen_ai.system and its successor gen_ai.provider.name, the current key second
// as in v_gen_ai_spans, and the token type (input or output) of a token usage
// point is promoted as gen_ai_token_type. Every point-value column of the
// metrics table passes through unchanged, so a histogram keeps its count, sum,
// bounds and bucket counts, an exponential histogram its buckets, and each
// point its exemplars, whose trace_id and span_id link a point to a trace. The
// raw attribute, resource and scope columns are kept for anything not promoted,
// followed by gen_ai_token_modality, gen_ai_agent_name, and
// gen_ai_inference_usage_dimension (semantic-conventions-genai@f3c6d0c9)
// appended at the end, and then by time_unix_nano and point_fingerprint, which
// with timestamp, service_name, metric_name and metric_type make up the
// identity of a point in the metric deduplication view (see
// MetricsDedupViewDDL), so two points that share a microsecond timestamp stay
// distinguishable.
const GenAIMetricsViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s." + GenAIMetricsViewName + "`" + ` AS
` + GenAIMetricsViewQuery + `;
`

// GenAIMetricsViewQuery is shared by operator-run DDL and metadata-API view
// creation. It takes (project, dataset, metrics table) and reads the metric
// dedup view, never the raw table. The LIKE pattern's percent sign is doubled
// because every template here goes through fmt.Sprintf.
const GenAIMetricsViewQuery = `
SELECT
  timestamp,
  start_time,
  receive_time,
  service_name,
  metric_name,
  metric_description,
  metric_unit,
  metric_type,
  aggregation_temporality,
  is_monotonic,
  -- Promoted OTel GenAI semantic convention fields (Plain JSON)
  JSON_VALUE(attributes, '$."gen_ai.operation.name"') AS gen_ai_operation_name,
  COALESCE(
    JSON_VALUE(attributes, '$."gen_ai.system"'),
    JSON_VALUE(attributes, '$."gen_ai.provider.name"')
  ) AS gen_ai_system,
  JSON_VALUE(attributes, '$."gen_ai.provider.name"') AS gen_ai_provider_name,
  JSON_VALUE(attributes, '$."gen_ai.request.model"') AS gen_ai_request_model,
  JSON_VALUE(attributes, '$."gen_ai.response.model"') AS gen_ai_response_model,
  JSON_VALUE(attributes, '$."gen_ai.token.type"') AS gen_ai_token_type,
  JSON_VALUE(attributes, '$."error.type"') AS error_type,
  -- Point values
  value_type,
  value_double,
  value_int64,
  count,
  sum,
  min,
  max,
  explicit_bounds,
  bucket_counts,
  exp_scale,
  exp_zero_count,
  exp_zero_threshold,
  exp_positive,
  exp_negative,
  summary_quantiles,
  exemplars,
  -- Raw bags
  attributes,
  resource,
  instrumentation_scope,
  JSON_VALUE(attributes, '$."gen_ai.token.modality"') AS gen_ai_token_modality,
  COALESCE(
    JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
    JSON_VALUE(attributes, '$."gen_ai.main_agent.name"'),
    JSON_VALUE(resource.attributes, '$."gen_ai.main_agent.name"')
  ) AS gen_ai_agent_name,
  CASE
    WHEN metric_name = 'gen_ai.client.inference.usage.input_tokens' THEN 'input'
    WHEN metric_name = 'gen_ai.client.inference.usage.output_tokens' THEN 'output'
    WHEN metric_name = 'gen_ai.client.inference.usage.cache_read.input_tokens' THEN 'cache_read_input'
    WHEN metric_name = 'gen_ai.client.inference.usage.cache_write.input_tokens' THEN 'cache_write_input'
    WHEN metric_name = 'gen_ai.client.inference.usage.reasoning.output_tokens' THEN 'reasoning_output'
    ELSE JSON_VALUE(attributes, '$."gen_ai.token.type"')
  END AS gen_ai_inference_usage_dimension,
  time_unix_nano,
  point_fingerprint
FROM ` + "`%[1]s.%[2]s.%[3]s_dedup`" + `
WHERE metric_name LIKE 'gen_ai.%%'
`

// GenAIMetricExemplarsViewDDL creates v_gen_ai_metric_exemplars: one row per
// exemplar on a GenAI metric point that carries a non-empty trace_id and
// span_id, left-joined to the span deduplication view within a 25-hour window
// around the metric point timestamp. When multiple span deduplication rows
// match an exemplar in that window, the latest span (by end_time, start_time,
// receive_time, then name) is selected. An exemplar whose span is absent or
// outside the window stays in the view with NULL span_* columns.
//
// That selection ranks the rows of one exemplar slot: one point of the metric
// deduplication view, identified by the same columns as there (timestamp,
// time_unix_nano, service_name, metric_name, metric_type, point_fingerprint),
// and the exemplar's offset in its exemplars array. Distinct points that share
// a microsecond timestamp, and repeated exemplars of one point, therefore each
// keep their row. m.timestamp stays among the PARTITION BY columns because
// BigQuery pushes a filter below a window function only when the filter refers
// to PARTITION BY columns (see MetricsDedupViewDDL); this keeps that path open
// for a filter on metric_timestamp.
const GenAIMetricExemplarsViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s." + GenAIMetricExemplarsViewName + "`" + ` AS
` + GenAIMetricExemplarsViewQuery + `;
`

// GenAIMetricExemplarsViewQuery is shared by operator-run DDL and metadata-API
// view creation. It takes (project, dataset, spans table), reads
// v_gen_ai_metrics, unnests exemplars, and left-joins the span dedup view.
const GenAIMetricExemplarsViewQuery = `
SELECT
  m.timestamp AS metric_timestamp,
  m.service_name,
  m.metric_name,
  m.metric_unit,
  m.metric_type,
  m.gen_ai_operation_name,
  m.gen_ai_system,
  m.gen_ai_request_model,
  m.gen_ai_response_model,
  m.gen_ai_agent_name,
  m.gen_ai_token_type,
  m.gen_ai_token_modality,
  m.error_type AS metric_error_type,
  ex.time AS exemplar_timestamp,
  ex.value_double AS exemplar_value_double,
  ex.value_int64 AS exemplar_value_int64,
  ex.trace_id AS trace_id,
  ex.span_id AS span_id,
  ex.filtered_attributes AS exemplar_filtered_attributes,
  s.start_time AS span_start_time,
  s.end_time AS span_end_time,
  s.duration_nano AS span_duration_nano,
  ROUND(s.duration_nano / 1e6, 3) AS span_duration_ms,
  s.name AS span_name,
  s.status.code AS span_status_code,
  s.status.message AS span_status_message,
  COALESCE(
    JSON_VALUE(s.attributes, '$."gen_ai.agent.name"'),
    JSON_VALUE(s.attributes, '$."gcp.vertex.agent.name"'),
    JSON_VALUE(s.resource.attributes, '$."gen_ai.agent.name"'),
    JSON_VALUE(s.attributes, '$."gen_ai.main_agent.name"'),
    JSON_VALUE(s.resource.attributes, '$."gen_ai.main_agent.name"')
  ) AS span_gen_ai_agent_name,
  JSON_VALUE(s.attributes, '$."gen_ai.tool.name"') AS span_gen_ai_tool_name,
  COALESCE(
    JSON_VALUE(s.attributes, '$."gcp.vertex.agent.session_id"'),
    JSON_VALUE(s.attributes, '$."gen_ai.conversation.id"')
  ) AS adk_session_id,
  JSON_VALUE(s.attributes, '$."error.type"') AS span_error_type
FROM ` + "`%[1]s.%[2]s." + GenAIMetricsViewName + "`" + ` m,
UNNEST(m.exemplars) AS ex WITH OFFSET AS exemplar_ordinal
LEFT JOIN ` + "`%[1]s.%[2]s.%[3]s_dedup`" + ` s
  ON s.trace_id = ex.trace_id
  AND s.span_id = ex.span_id
  AND s.service_name IS NOT DISTINCT FROM m.service_name
  AND s.start_time BETWEEN TIMESTAMP_SUB(m.timestamp, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(m.timestamp, INTERVAL 1 HOUR)
WHERE ex.trace_id IS NOT NULL
  AND ex.span_id IS NOT NULL
  AND ex.trace_id != ''
  AND ex.span_id != ''
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY m.timestamp, m.time_unix_nano, m.service_name, m.metric_name, m.metric_type, m.point_fingerprint, exemplar_ordinal
  ORDER BY s.end_time DESC, s.start_time DESC, s.receive_time DESC, s.name ASC
) = 1
`

// GenAIModelCostsViewDDL creates v_gen_ai_model_costs: hourly token and
// latency rollup per service, agent, provider, and model over v_gen_ai_spans.
// Each LLM call is counted once: a span whose direct children report token
// usage is assumed to report their sum and is left out, and so is an agent or
// tool span that reports no tokens of its own. A span is matched with its
// children only within one trace, service, and usage hour; see
// genAITokenParentsAgg. See genAILLMCallPredicate for the exact rule.
const GenAIModelCostsViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s." + GenAIModelCostsViewName + "`" + ` AS
` + GenAIModelCostsViewQuery + `;
`

// genAITokenParentsAgg is the window aggregate the two rollup views share to
// find the spans that report the sum of their children's tokens. Over one
// window partition it collects the parent_span_id of every span that has a
// parent and reports input or output tokens, so a span is such a parent when
// its span_id is in the array. A parent is taken to report the sum of its
// children's tokens, so the rollups leave its own tokens out instead of
// counting the same tokens twice. It reads the span as s. It takes the place of
// a second read of v_gen_ai_spans: the window runs over the rows the view
// already scans, so a filter on a column of its PARTITION BY list narrows that
// one scan.
//
// Spans see each other only inside a partition, which makes the PARTITION BY
// list the matching rule. A parent and its child are matched only when both
// fall in the same partition: the same trace_id, service_name, and usage hour
// for v_gen_ai_model_costs, and the same trace_id, service_name, UTC date, and
// session for v_gen_ai_sessions. A pair split across an hour or a date
// boundary, or across two service names, is not matched, and the parent's
// tokens are counted along with its child's. Only direct children are visible:
// a span is listed when one of its own children carries tokens, not when a
// deeper descendant does.
//
// BigQuery's analytic ARRAY_AGG accepts neither IGNORE NULLS nor DISTINCT, so
// the array also holds a NULL for every span that is not a token-reporting
// child. genAITokenLeaf compares by equality, which a NULL never satisfies.
// The array has one element for each span of its partition, so a partition
// with an unusually large number of spans costs more than a typical trace.
const genAITokenParentsAgg = `ARRAY_AGG(
      IF(
        s.parent_span_id IS NOT NULL
        AND (s.gen_ai_usage_input_tokens IS NOT NULL OR s.gen_ai_usage_output_tokens IS NOT NULL),
        s.parent_span_id,
        NULL
      )
    )`

// genAITokenLeaf is true for a span that no token-reporting span of its window
// partition names as its parent: s.span_id is not among the elements of the
// s.token_parent_span_ids array that genAITokenParentsAgg computed.
const genAITokenLeaf = `NOT EXISTS (
    SELECT 1 FROM UNNEST(s.token_parent_span_ids) AS token_parent_span_id
    WHERE token_parent_span_id = s.span_id
  )`

// genAILLMCallPredicate is the condition that selects the spans a rollup view
// counts as one LLM call each. It reads the span as s, needs the
// token_parent_span_ids column that genAITokenParentsAgg computes, and holds
// for a span that:
//   - has no direct child reporting tokens (genAITokenLeaf), so a parent that
//     reports the sum of its children is skipped;
//   - is an inference span: it has an inference operation name or a request
//     model; and
//   - is not an agent or tool span, unless it reports tokens itself, so an
//     invocation span that names a request model and is the only span of its
//     framework to carry tokens still contributes them. An invocation span
//     that reports tokens but names no request model is not an inference span,
//     so it is not counted as a call.
const genAILLMCallPredicate = genAITokenLeaf + `
  AND (
    s.gen_ai_operation_name IN ('chat', 'generate_content', 'text_completion', 'embeddings')
    OR s.gen_ai_request_model IS NOT NULL
  )
  AND (
    COALESCE(s.gen_ai_operation_name, '') NOT IN ('invoke_agent', 'create_agent', 'execute_tool')
    OR s.gen_ai_usage_input_tokens IS NOT NULL
    OR s.gen_ai_usage_output_tokens IS NOT NULL
  )`

// GenAIModelCostsViewQuery is shared by operator-run DDL and metadata-API view
// creation. It takes (project, dataset) and reads v_gen_ai_spans once, in
// cost_spans, which also computes usage_hour as a column of its own. The window
// in cost_windows partitions by that column, trace_id, and service_name, so a
// filter on usage_hour reaches the span scan and a query over a few hours reads
// a few hourly partitions. In BigQuery dry runs, writing the same expression
// inline in PARTITION BY showed no such narrowing.
const GenAIModelCostsViewQuery = `
WITH cost_spans AS (
  SELECT
    TIMESTAMP_TRUNC(s.start_time, HOUR) AS usage_hour,
    s.service_name,
    s.trace_id,
    s.span_id,
    s.parent_span_id,
    s.gen_ai_agent_name,
    s.gen_ai_provider_name,
    s.gen_ai_system,
    s.gen_ai_request_model,
    s.gen_ai_response_model,
    s.gen_ai_operation_name,
    s.status_code,
    s.duration_ms,
    s.gen_ai_usage_input_tokens,
    s.gen_ai_usage_output_tokens,
    s.gen_ai_usage_cache_read_input_tokens,
    s.gen_ai_usage_cache_creation_input_tokens,
    s.gen_ai_usage_reasoning_tokens,
    s.error_type
  FROM ` + "`%[1]s.%[2]s." + GenAISpansViewName + "`" + ` AS s
),
cost_windows AS (
  SELECT
    s.*,
    ` + genAITokenParentsAgg + ` OVER (
      PARTITION BY s.usage_hour, s.service_name, s.trace_id
    ) AS token_parent_span_ids
  FROM cost_spans AS s
),
cost_agg AS (
  SELECT
    s.usage_hour,
    s.service_name,
    s.gen_ai_agent_name,
    COALESCE(s.gen_ai_provider_name, s.gen_ai_system) AS provider,
    COALESCE(s.gen_ai_response_model, s.gen_ai_request_model) AS model,
    COUNT(*) AS call_count,
    COUNTIF(s.status_code = 2) AS error_count,
    SUM(COALESCE(s.gen_ai_usage_input_tokens, 0)) AS total_input_tokens,
    SUM(COALESCE(s.gen_ai_usage_output_tokens, 0)) AS total_output_tokens,
    SUM(COALESCE(s.gen_ai_usage_cache_read_input_tokens, 0)) AS total_cache_read_input_tokens,
    SUM(COALESCE(s.gen_ai_usage_cache_creation_input_tokens, 0)) AS total_cache_creation_input_tokens,
    SUM(COALESCE(s.gen_ai_usage_reasoning_tokens, 0)) AS total_reasoning_tokens,
    APPROX_QUANTILES(s.duration_ms, 100)[OFFSET(50)] AS p50_duration_ms,
    APPROX_QUANTILES(s.duration_ms, 100)[OFFSET(95)] AS p95_duration_ms,
    APPROX_QUANTILES(s.duration_ms, 100)[OFFSET(99)] AS p99_duration_ms,
    COUNTIF(s.error_type IS NOT NULL) AS typed_error_count,
    APPROX_TOP_COUNT(s.error_type, 6) AS raw_top_error_types
  FROM cost_windows AS s
  WHERE ` + genAILLMCallPredicate + `
  GROUP BY 1, 2, 3, 4, 5
)
SELECT
  * EXCEPT (raw_top_error_types),
  ARRAY(
    SELECT AS STRUCT x.value, x.count
    FROM UNNEST(raw_top_error_types) AS x
    WHERE x.value IS NOT NULL
    ORDER BY x.count DESC, x.value ASC
    LIMIT 5
  ) AS top_error_types
FROM cost_agg
`

// GenAISessionsViewDDL creates v_gen_ai_sessions: per-session / conversation
// summary over v_gen_ai_spans. A row is one session within one UTC date of its
// spans' start times, grouped by service_name, session_start_date and
// session_id, so a session that runs across midnight UTC has one row for each
// date, and a filter on service_name or session_start_date applies to grouping
// keys. The token totals sum only the spans whose direct children report no
// tokens, so a parent span that reports the sum of its children is not counted
// with them. Only spans that carry a session id take part, and a span is
// matched with its children only within one session, trace, service, and UTC
// date (see genAITokenParentsAgg): a child without a session id is not seen, so
// its session-bearing parent keeps its own tokens. The totals cover every span
// without a token-reporting child, not only LLM calls, so a span that reports
// tokens but is not an LLM call, such as an invoke_agent span that names no
// request model, adds to them and is not counted by v_gen_ai_model_costs.
// llm_call_count counts the LLM calls by the same rule as
// v_gen_ai_model_costs, and agent_invocation_count counts the invoke_agent
// spans separately. session_start_date, total_cache_creation_input_tokens,
// llm_call_count, agent_invocation_count, typed_error_count and
// top_error_types are appended at the end, so the position of every earlier
// column is unchanged.
const GenAISessionsViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s." + GenAISessionsViewName + "`" + ` AS
` + GenAISessionsViewQuery + `;
`

// GenAISessionsViewQuery is shared by operator-run DDL and metadata-API view
// creation. It takes (project, dataset) and reads v_gen_ai_spans once, in
// session_spans, which keeps only the spans that have a session id before the
// window runs and computes service_name, session_start_date, and session_id as
// columns of their own, so a filter on session_start_date reaches the span scan
// (see GenAIModelCostsViewQuery). session_windows runs the genAITokenParentsAgg
// window over each session of a trace. session_calls marks each span once, as
// is_token_leaf (no direct child reports tokens) and as is_llm_call
// (genAILLMCallPredicate), and the outer query only aggregates.
const GenAISessionsViewQuery = `
WITH session_spans AS (
  SELECT
    s.service_name,
    DATE(s.start_time) AS session_start_date,
    COALESCE(s.adk_session_id, s.gen_ai_conversation_id) AS session_id,
    s.adk_user_id,
    s.gen_ai_agent_name,
    s.start_time,
    s.end_time,
    s.trace_id,
    s.span_id,
    s.parent_span_id,
    s.adk_invocation_id,
    s.gen_ai_operation_name,
    s.gen_ai_tool_name,
    s.gen_ai_request_model,
    s.status_code,
    s.gen_ai_usage_input_tokens,
    s.gen_ai_usage_output_tokens,
    s.gen_ai_usage_cache_read_input_tokens,
    s.gen_ai_usage_cache_creation_input_tokens,
    s.gen_ai_usage_reasoning_tokens,
    s.error_type
  FROM ` + "`%[1]s.%[2]s." + GenAISpansViewName + "`" + ` AS s
  WHERE COALESCE(s.adk_session_id, s.gen_ai_conversation_id) IS NOT NULL
),
session_windows AS (
  SELECT
    s.*,
    ` + genAITokenParentsAgg + ` OVER (
      PARTITION BY s.service_name, s.session_start_date, s.session_id, s.trace_id
    ) AS token_parent_span_ids
  FROM session_spans AS s
),
session_calls AS (
  SELECT
    s.*,
    ` + genAITokenLeaf + ` AS is_token_leaf,
    COALESCE(` + genAILLMCallPredicate + `, FALSE) AS is_llm_call
  FROM session_windows AS s
),
session_agg AS (
  SELECT
    session_id,
    ANY_VALUE(adk_user_id) AS user_id,
    service_name,
    ANY_VALUE(gen_ai_agent_name) AS agent_name,
    MIN(start_time) AS session_start_time,
    MAX(end_time) AS session_end_time,
    TIMESTAMP_DIFF(MAX(end_time), MIN(start_time), MILLISECOND) AS session_duration_ms,
    COUNT(DISTINCT trace_id) AS turn_count,
    COUNT(DISTINCT adk_invocation_id) AS invocation_count,
    COUNTIF(gen_ai_operation_name = 'execute_tool' OR gen_ai_tool_name IS NOT NULL) AS tool_call_count,
    COUNTIF(status_code = 2) AS error_count,
    SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_input_tokens, 0), 0)) AS total_input_tokens,
    SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_output_tokens, 0), 0)) AS total_output_tokens,
    SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_cache_read_input_tokens, 0), 0)) AS total_cache_read_input_tokens,
    SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_reasoning_tokens, 0), 0)) AS total_reasoning_tokens,
    session_start_date,
    SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_cache_creation_input_tokens, 0), 0)) AS total_cache_creation_input_tokens,
    COUNTIF(is_llm_call) AS llm_call_count,
    COUNTIF(gen_ai_operation_name = 'invoke_agent') AS agent_invocation_count,
    COUNTIF(error_type IS NOT NULL) AS typed_error_count,
    APPROX_TOP_COUNT(error_type, 6) AS raw_top_error_types
  FROM session_calls
  GROUP BY service_name, session_start_date, session_id
)
SELECT
  * EXCEPT (raw_top_error_types),
  ARRAY(
    SELECT AS STRUCT x.value, x.count
    FROM UNNEST(raw_top_error_types) AS x
    WHERE x.value IS NOT NULL
    ORDER BY x.count DESC, x.value ASC
    LIMIT 5
  ) AS top_error_types
FROM session_agg
`

// GenAIToolUsageViewDDL creates v_gen_ai_tool_usage: hourly tool invocation
// volume, error rate, and latency percentiles over v_gen_ai_spans.
const GenAIToolUsageViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s." + GenAIToolUsageViewName + "`" + ` AS
` + GenAIToolUsageViewQuery + `;
`

// GenAIToolUsageViewQuery is shared by operator-run DDL and metadata-API view
// creation. It takes (project, dataset) and reads v_gen_ai_spans.
const GenAIToolUsageViewQuery = `
WITH tool_agg AS (
  SELECT
    TIMESTAMP_TRUNC(start_time, HOUR) AS usage_hour,
    service_name,
    gen_ai_agent_name,
    gen_ai_tool_name,
    gen_ai_tool_type,
    COUNT(*) AS call_count,
    COUNTIF(status_code = 2) AS error_count,
    SAFE_DIVIDE(COUNTIF(status_code = 2), COUNT(*)) AS error_rate,
    APPROX_QUANTILES(duration_ms, 100)[OFFSET(50)] AS p50_duration_ms,
    APPROX_QUANTILES(duration_ms, 100)[OFFSET(95)] AS p95_duration_ms,
    COUNTIF(error_type IS NOT NULL) AS typed_error_count,
    APPROX_TOP_COUNT(error_type, 6) AS raw_top_error_types
  FROM ` + "`%[1]s.%[2]s." + GenAISpansViewName + "`" + `
  WHERE gen_ai_tool_name IS NOT NULL OR gen_ai_operation_name = 'execute_tool'
  GROUP BY 1, 2, 3, 4, 5
)
SELECT
  * EXCEPT (raw_top_error_types),
  ARRAY(
    SELECT AS STRUCT x.value, x.count
    FROM UNNEST(raw_top_error_types) AS x
    WHERE x.value IS NOT NULL
    ORDER BY x.count DESC, x.value ASC
    LIMIT 5
  ) AS top_error_types
FROM tool_agg
`

// GenAIAgentTrajectoryViewDDL creates v_gen_ai_agent_trajectory: a recursive
// span-DAG companion view over v_gen_ai_spans that walks parent_span_id links
// up to depth 16, records the nearest agent span and deduplicated agent_path,
// flags orphan roots whose parent_span_id is absent, and computes step_index,
// is_leaf, and is_token_leaf per trace and service.
const GenAIAgentTrajectoryViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s." + GenAIAgentTrajectoryViewName + "`" + ` AS
` + GenAIAgentTrajectoryViewQuery + `;
`

// GenAIAgentTrajectoryViewQuery is shared by operator-run DDL and metadata-API
// view creation. It takes (project, dataset) and reads v_gen_ai_spans.
//
// Anchor rows are true roots (parent_span_id IS NULL or empty) and orphan
// roots whose parent_span_id does not appear in v_gen_ai_spans for the same
// trace_id and null-safe service_name within 24 hours before to 1 hour after
// the child's start_time. Recursive steps traverse child spans within the
// symmetric [-1h, +24h] window up to depth < 16 and guard against cycles with
// visited_span_ids.
//
// The agent columns read declared_agent_name, the agent name a span's own
// attributes declare (gen_ai.agent.name, then gcp.vertex.agent.name), because
// gen_ai_agent_name falls back to service_name and so names every span. An
// invoke_agent or create_agent span always starts a new nearest agent, and a
// span that only declares an agent name starts one when no span above it has
// one. Such a span is its own nearest_agent_span_id, and its
// nearest_agent_name is its declared name, or gen_ai_agent_name for an agent
// span that declares none; every other span inherits both columns from its
// parent. agent_path appends agent_path_name (the declared name or, for an
// agent span without one, gen_ai_agent_name) unless it repeats the last entry,
// so a chat or tool span without an agent attribute adds nothing to it.
//
// is_token_leaf uses the token-parent test of the rollup views (see
// genAITokenParentsAgg): a span reports tokens when its input or output token
// count IS NOT NULL, so an explicit 0 counts. A span that reports tokens is a
// token leaf unless a direct child in the same trace and service reports them
// too.
const GenAIAgentTrajectoryViewQuery = `
WITH RECURSIVE
  spans AS (
    SELECT
      start_time,
      end_time,
      duration_nano,
      duration_ms,
      trace_id,
      span_id,
      parent_span_id,
      service_name,
      span_name,
      span_kind,
      status_code,
      status_message,
      gen_ai_operation_name,
      gen_ai_system,
      gen_ai_provider_name,
      gen_ai_request_model,
      gen_ai_response_model,
      gen_ai_agent_id,
      gen_ai_agent_name,
      gen_ai_agent_version,
      gen_ai_main_agent_id,
      gen_ai_main_agent_name,
      gen_ai_tool_name,
      gen_ai_tool_call_id,
      gen_ai_usage_input_tokens,
      gen_ai_usage_output_tokens,
      gen_ai_usage_reasoning_tokens,
      adk_session_id,
      adk_invocation_id,
      adk_user_id,
      error_type,
      COALESCE(
        JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
        JSON_VALUE(attributes, '$."gcp.vertex.agent.name"')
      ) AS declared_agent_name,
      COALESCE(
        JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
        JSON_VALUE(attributes, '$."gcp.vertex.agent.name"'),
        IF(gen_ai_operation_name IN ('invoke_agent', 'create_agent'), gen_ai_agent_name, NULL)
      ) AS agent_path_name
    FROM ` + "`%[1]s.%[2]s." + GenAISpansViewName + "`" + `
  ),
  parent_ids AS (
    SELECT DISTINCT
      trace_id,
      service_name,
      parent_span_id AS span_id
    FROM spans
    WHERE parent_span_id IS NOT NULL
      AND parent_span_id != ''
  ),
  token_parent_ids AS (
    SELECT DISTINCT
      trace_id,
      service_name,
      parent_span_id AS span_id
    FROM spans
    WHERE parent_span_id IS NOT NULL
      AND parent_span_id != ''
      AND (gen_ai_usage_input_tokens IS NOT NULL OR gen_ai_usage_output_tokens IS NOT NULL)
  ),
  trajectory AS (
    SELECT
      s.start_time,
      s.end_time,
      s.duration_nano,
      s.duration_ms,
      s.trace_id,
      s.span_id,
      s.parent_span_id,
      s.service_name,
      s.span_name,
      s.span_kind,
      s.status_code,
      s.status_message,
      s.gen_ai_operation_name,
      s.gen_ai_system,
      s.gen_ai_provider_name,
      s.gen_ai_request_model,
      s.gen_ai_response_model,
      s.gen_ai_agent_id,
      s.gen_ai_agent_name,
      s.gen_ai_agent_version,
      s.gen_ai_main_agent_id,
      s.gen_ai_main_agent_name,
      s.gen_ai_tool_name,
      s.gen_ai_tool_call_id,
      s.gen_ai_usage_input_tokens,
      s.gen_ai_usage_output_tokens,
      s.gen_ai_usage_reasoning_tokens,
      s.adk_session_id,
      s.adk_invocation_id,
      s.adk_user_id,
      s.error_type,
      0 AS depth,
      s.span_id AS root_span_id,
      CASE
        WHEN s.gen_ai_operation_name IN ('invoke_agent', 'create_agent')
          OR s.declared_agent_name IS NOT NULL
        THEN s.span_id
        ELSE CAST(NULL AS STRING)
      END AS nearest_agent_span_id,
      CASE
        WHEN s.gen_ai_operation_name IN ('invoke_agent', 'create_agent')
          OR s.declared_agent_name IS NOT NULL
        THEN COALESCE(s.declared_agent_name, s.gen_ai_agent_name)
        ELSE CAST(NULL AS STRING)
      END AS nearest_agent_name,
      CASE
        WHEN s.agent_path_name IS NOT NULL THEN [s.agent_path_name]
        ELSE ARRAY<STRING>[]
      END AS agent_path,
      (s.parent_span_id IS NOT NULL AND s.parent_span_id != '') AS is_orphan,
      [s.span_id] AS visited_span_ids
    FROM spans AS s
    LEFT JOIN spans AS p
      ON s.trace_id = p.trace_id
      AND s.parent_span_id = p.span_id
      AND s.service_name IS NOT DISTINCT FROM p.service_name
      AND p.start_time BETWEEN TIMESTAMP_SUB(s.start_time, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(s.start_time, INTERVAL 1 HOUR)
    WHERE s.parent_span_id IS NULL
       OR s.parent_span_id = ''
       OR p.span_id IS NULL

    UNION ALL

    SELECT
      c.start_time,
      c.end_time,
      c.duration_nano,
      c.duration_ms,
      c.trace_id,
      c.span_id,
      c.parent_span_id,
      c.service_name,
      c.span_name,
      c.span_kind,
      c.status_code,
      c.status_message,
      c.gen_ai_operation_name,
      c.gen_ai_system,
      c.gen_ai_provider_name,
      c.gen_ai_request_model,
      c.gen_ai_response_model,
      c.gen_ai_agent_id,
      c.gen_ai_agent_name,
      c.gen_ai_agent_version,
      c.gen_ai_main_agent_id,
      c.gen_ai_main_agent_name,
      c.gen_ai_tool_name,
      c.gen_ai_tool_call_id,
      c.gen_ai_usage_input_tokens,
      c.gen_ai_usage_output_tokens,
      c.gen_ai_usage_reasoning_tokens,
      c.adk_session_id,
      c.adk_invocation_id,
      c.adk_user_id,
      c.error_type,
      t.depth + 1 AS depth,
      t.root_span_id AS root_span_id,
      CASE
        WHEN c.gen_ai_operation_name IN ('invoke_agent', 'create_agent')
          OR (t.nearest_agent_span_id IS NULL AND c.declared_agent_name IS NOT NULL)
        THEN c.span_id
        ELSE t.nearest_agent_span_id
      END AS nearest_agent_span_id,
      CASE
        WHEN c.gen_ai_operation_name IN ('invoke_agent', 'create_agent')
          OR (t.nearest_agent_span_id IS NULL AND c.declared_agent_name IS NOT NULL)
        THEN COALESCE(c.declared_agent_name, c.gen_ai_agent_name)
        ELSE t.nearest_agent_name
      END AS nearest_agent_name,
      CASE
        WHEN c.agent_path_name IS NOT NULL
          AND (
            ARRAY_LENGTH(t.agent_path) = 0
            OR t.agent_path[OFFSET(ARRAY_LENGTH(t.agent_path) - 1)] != c.agent_path_name
          )
        THEN ARRAY_CONCAT(t.agent_path, [c.agent_path_name])
        ELSE t.agent_path
      END AS agent_path,
      t.is_orphan AS is_orphan,
      ARRAY_CONCAT(t.visited_span_ids, [c.span_id]) AS visited_span_ids
    FROM trajectory AS t
    JOIN spans AS c
      ON c.trace_id = t.trace_id
      AND c.parent_span_id = t.span_id
      AND c.service_name IS NOT DISTINCT FROM t.service_name
      AND c.start_time BETWEEN TIMESTAMP_SUB(t.start_time, INTERVAL 1 HOUR) AND TIMESTAMP_ADD(t.start_time, INTERVAL 24 HOUR)
    WHERE t.depth < 16
      AND c.span_id NOT IN UNNEST(t.visited_span_ids)
  )
SELECT
  t.start_time,
  t.end_time,
  t.duration_nano,
  t.duration_ms,
  t.trace_id,
  t.span_id,
  t.parent_span_id,
  t.service_name,
  t.span_name,
  t.span_kind,
  t.status_code,
  t.status_message,
  t.gen_ai_operation_name,
  t.gen_ai_system,
  t.gen_ai_provider_name,
  t.gen_ai_request_model,
  t.gen_ai_response_model,
  t.gen_ai_agent_id,
  t.gen_ai_agent_name,
  t.gen_ai_agent_version,
  t.gen_ai_main_agent_id,
  t.gen_ai_main_agent_name,
  t.gen_ai_tool_name,
  t.gen_ai_tool_call_id,
  t.gen_ai_usage_input_tokens,
  t.gen_ai_usage_output_tokens,
  t.gen_ai_usage_reasoning_tokens,
  t.adk_session_id,
  t.adk_invocation_id,
  t.adk_user_id,
  t.error_type,
  t.depth,
  t.root_span_id,
  t.nearest_agent_span_id,
  t.nearest_agent_name,
  t.agent_path,
  t.is_orphan,
  ROW_NUMBER() OVER (
    PARTITION BY t.trace_id, t.service_name
    ORDER BY t.start_time ASC, t.span_id ASC
  ) AS step_index,
  (leaf_check.span_id IS NULL) AS is_leaf,
  (
    (t.gen_ai_usage_input_tokens IS NOT NULL OR t.gen_ai_usage_output_tokens IS NOT NULL)
    AND token_leaf_check.span_id IS NULL
  ) AS is_token_leaf
FROM trajectory AS t
LEFT JOIN parent_ids AS leaf_check
  ON t.trace_id = leaf_check.trace_id
  AND t.span_id = leaf_check.span_id
  AND t.service_name IS NOT DISTINCT FROM leaf_check.service_name
LEFT JOIN token_parent_ids AS token_leaf_check
  ON t.trace_id = token_leaf_check.trace_id
  AND t.span_id = token_leaf_check.span_id
  AND t.service_name IS NOT DISTINCT FROM token_leaf_check.service_name
`

// FingerprintPrefix is the version tag on record_fingerprint and
// point_fingerprint values, which are formatted as
// "fp1:" + lowercase_hex(SHA-256(canonical_record)).
//
// The canonical form covers every non-derived OTLP field of the record, its
// resource and its scope, encodes values as plain JSON together with their
// type sidecars, and ends with the record's intra_batch_ordinal: the 0-based
// count of identical canonical records before it in the same export batch.
// Receive time is ingestion-local and derived, so it is excluded: including
// it would give every retry a distinct fingerprint and defeat the whole
// mechanism.
const FingerprintPrefix = "fp1:"
