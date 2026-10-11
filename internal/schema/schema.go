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

// QuarantineDDL creates the opt-in quarantine (dead-letter) table, which the
// exporter writes only when quarantine.enabled is set. A span or log record
// that cannot be written to its signal table, because of an invalid ID, a
// missing or overflowing timestamp, a value over a configured limit, a row
// over the row size limit, or a row BigQuery itself rejects, lands here
// instead of being dropped, with the reason in error_code and error_message
// and the record serialized as OTLP JSON in raw_payload.
//
// Partitioning is daily on ingest_time and partitions expire after 30 days,
// because the table is a triage queue rather than an archive; clustering on
// (signal, error_code, service_name) keeps "what is failing, and where"
// queries pruned. There is no dedup view: a record whose whole batch is
// retried can be quarantined more than once.
const QuarantineDDL = `
CREATE TABLE IF NOT EXISTS ` + "`%[1]s.%[2]s.%[3]s`" + ` (
  ingest_time                        TIMESTAMP NOT NULL,
  signal                             STRING    NOT NULL, -- 'spans' | 'logs'
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
// with the partitioning column and the leading clustering columns added: a
// filter on any of them can be pushed below the window function, and a query
// over the view still prunes partitions and clusters. Ordering is on the raw
// nanos rather than end_time, because microsecond precision alone can tie
// across retries; receive_time_unix_nano breaks any remaining tie.
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
    span_id
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
// fingerprint otherwise. The partitioning column and the leading clustering
// column come first, so a filter on either can be pushed below the window
// function and a query over the view still prunes partitions and clusters;
// time_unix_nano keeps apart records that share a microsecond timestamp. The
// most recently received copy wins.
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
    log_name,
    insert_id
  ORDER BY
    receive_time_unix_nano DESC
) = 1
`

// SpansSearchIndexDDL creates the trace lookup search index on the span
// table. NO_OP_ANALYZER indexes every trace_id and span_id value whole,
// without tokenizing it, which suits exact-match lookups of one trace or one
// span.
//
// The exporter never runs this statement. Auto-create goes through the table
// metadata API only, while a search index is created by a SQL query job,
// which needs bigquery.jobs.create; operators run it themselves or manage it
// with their infrastructure-as-code tooling. The index name is derived from
// the table name, so the default otel_spans table gets
// otel_spans_trace_lookup_idx.
const SpansSearchIndexDDL = `
CREATE SEARCH INDEX IF NOT EXISTS ` + "`%[3]s_trace_lookup_idx`" + `
ON ` + "`%[1]s.%[2]s.%[3]s`" + ` (trace_id, span_id) OPTIONS (analyzer = 'NO_OP_ANALYZER');
`

// LogsSearchIndexDDL creates the trace lookup search index on the log table,
// so looking up the records correlated with one trace or one span can use the
// index. As with SpansSearchIndexDDL, the exporter never runs it, and the
// default otel_logs table gets otel_logs_trace_lookup_idx.
const LogsSearchIndexDDL = `
CREATE SEARCH INDEX IF NOT EXISTS ` + "`%[3]s_trace_lookup_idx`" + `
ON ` + "`%[1]s.%[2]s.%[3]s`" + ` (trace_id, span_id) OPTIONS (analyzer = 'NO_OP_ANALYZER');
`

// Companion view names. The companion views are optional conveniences over
// the dedup views, created only when auto_create.companion_views is enabled.
// Their names are fixed rather than derived from the table names, so one
// dataset holds one set of them.
const (
	SpansAsAllSpansViewName = "otel_spans_as_allspans"
	GenAISpansViewName      = "v_gen_ai_spans"
	GenAILogsViewName       = "v_gen_ai_logs"
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
// gen_ai.operation.name, gen_ai.system or gcp.vertex.agent.invocation_id,
// with the OpenTelemetry GenAI semantic convention attributes and the Agent
// Development Kit gcp.vertex.agent.* attributes promoted out of the plain
// JSON attributes into typed top-level columns. Where the same fact can arrive
// under more than one key, such as gen_ai.system and its successor
// gen_ai.provider.name, the keys are coalesced in a fixed order. The raw
// attribute, resource, scope, event and link columns and payload_refs are
// kept for anything not promoted.
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
  links
FROM ` + "`%[1]s.%[2]s.%[3]s_dedup`" + `
WHERE JSON_VALUE(attributes, '$."gen_ai.operation.name"') IS NOT NULL
   OR JSON_VALUE(attributes, '$."gen_ai.system"') IS NOT NULL
   OR JSON_VALUE(attributes, '$."gcp.vertex.agent.invocation_id"') IS NOT NULL
`

// GenAILogsViewDDL creates v_gen_ai_logs: the log records whose event name
// starts with "gen_ai.", or that carry gen_ai.system or
// gcp.vertex.agent.invocation_id, with the GenAI and agent fields promoted
// from the record's own attributes and, where the record lacks one, from the
// attributes of the span it belongs to. The event name falls back from the
// event_name column to the event.name attribute.
const GenAILogsViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s." + GenAILogsViewName + "`" + ` AS
` + GenAILogsViewQuery + `;
`

// GenAILogsViewQuery is shared by operator-run DDL and metadata-API view
// creation. It takes (project, dataset, logs table, spans table) and joins
// the log dedup view to the span dedup view on (service_name, trace_id,
// span_id), so the span dedup view must exist before this view can be
// created. The LIKE pattern's percent sign is doubled because every template
// here goes through fmt.Sprintf.
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
  COALESCE(JSON_VALUE(l.attributes, '$."gen_ai.system"'), JSON_VALUE(s.attributes, '$."gen_ai.system"')) AS gen_ai_system,
  COALESCE(JSON_VALUE(l.attributes, '$."gen_ai.agent.name"'), JSON_VALUE(s.attributes, '$."gen_ai.agent.name"')) AS gen_ai_agent_name,
  COALESCE(JSON_VALUE(l.attributes, '$."gen_ai.conversation.id"'), JSON_VALUE(s.attributes, '$."gen_ai.conversation.id"'), JSON_VALUE(s.attributes, '$."gcp.vertex.agent.session_id"')) AS adk_session_id,
  COALESCE(JSON_VALUE(l.attributes, '$."gcp.vertex.agent.invocation_id"'), JSON_VALUE(s.attributes, '$."gcp.vertex.agent.invocation_id"')) AS adk_invocation_id,
  COALESCE(JSON_VALUE(l.attributes, '$."gcp.vertex.agent.event_id"'), JSON_VALUE(s.attributes, '$."gcp.vertex.agent.event_id"')) AS adk_event_id,
  COALESCE(JSON_VALUE(l.attributes, '$."user.id"'), JSON_VALUE(s.attributes, '$."user.id"')) AS adk_user_id,
  l.body,
  l.payload_refs,
  l.attributes,
  l.resource,
  l.instrumentation_scope
FROM ` + "`%[1]s.%[2]s.%[3]s_dedup`" + ` l
LEFT JOIN ` + "`%[1]s.%[2]s.%[4]s_dedup`" + ` s
  USING (service_name, trace_id, span_id)
WHERE COALESCE(NULLIF(l.event_name, ''), JSON_VALUE(l.attributes, '$."event.name"')) LIKE 'gen_ai.%%'
   OR JSON_VALUE(l.attributes, '$."gen_ai.system"') IS NOT NULL
   OR JSON_VALUE(l.attributes, '$."gcp.vertex.agent.invocation_id"') IS NOT NULL
`

// FingerprintPrefix is the version tag on record_fingerprint values, which are
// formatted as "fp1:" + lowercase_hex(SHA-256(canonical_log_record)).
//
// The canonical form covers every non-derived OTLP field of the record, its
// resource and its scope, encodes values as plain JSON together with their
// type sidecars, and ends with the record's intra_batch_ordinal: the 0-based
// count of identical canonical records before it in the same export batch.
// Receive time is ingestion-local and derived, so it is excluded: including
// it would give every retry a distinct fingerprint and defeat the whole
// mechanism.
const FingerprintPrefix = "fp1:"
