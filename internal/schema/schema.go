// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package schema holds the versioned BigQuery table contract (§7 of the design
// doc) and the canonical deduplication SQL that goes with it (§7.4).
//
// The DDL here is the source of truth for the table shape. It must track §7.1
// and §7.2 column for column: downstream queries and dbt models pin against
// this contract, so a divergence between the doc and this file is a bug in
// whichever one is wrong.
package schema // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"

// Version is the per-row schema_version value.
//
// The contract is published as v0alpha1 at M2, during which documented
// breaking corrections are allowed. It freezes as v1 only at M4, after at
// least two independent design partners complete representative workflows.
// Do not bump this to v1 without that gate.
const Version = "v0alpha1"

// SpansDDL creates the span table per §7.1.
//
// Timestamps are stored twice on purpose: a microsecond-precision TIMESTAMP
// for querying and partitioning, and the raw OTLP value as a decimal STRING,
// because BigQuery TIMESTAMP cannot hold nanoseconds and the unsigned fixed64
// domain does not fit INT64.
//
// Clustering on (service_name, name) is the M3 benchmark candidate, not a
// frozen default.
const SpansDDL = `
CREATE TABLE IF NOT EXISTS ` + "`%[1]s.%[2]s.%[3]s`" + ` (
  start_timestamp                    TIMESTAMP NOT NULL,
  start_time_unix_nano               STRING    NOT NULL,
  end_timestamp                      TIMESTAMP,
  end_time_unix_nano                 STRING,
  export_received_timestamp          TIMESTAMP NOT NULL,
  duration_ns                        INT64,

  trace_id                           STRING    NOT NULL,
  span_id                            STRING    NOT NULL,
  parent_span_id                     STRING,
  trace_state                        STRING,
  flags                              INT64,

  name                               STRING    NOT NULL,
  kind                               STRING,
  status_code                        STRING,
  status_message                     STRING,

  service_name                       STRING,
  resource_attributes                JSON,
  resource_schema_url                STRING,
  resource_dropped_attributes_count  INT64,

  scope_name                         STRING,
  scope_version                      STRING,
  scope_attributes                   JSON,
  scope_schema_url                   STRING,
  scope_dropped_attributes_count     INT64,

  schema_version                     STRING    NOT NULL,

  span_attributes                    JSON,
  dropped_attributes_count           INT64,
  dropped_events_count               INT64,
  dropped_links_count                INT64,

  events ARRAY<STRUCT<
    name                     STRING,
    timestamp                TIMESTAMP,
    time_unix_nano           STRING,
    attributes               JSON,
    dropped_attributes_count INT64
  >>,

  links ARRAY<STRUCT<
    trace_id                 STRING,
    span_id                  STRING,
    trace_state              STRING,
    flags                    INT64,
    attributes               JSON,
    dropped_attributes_count INT64
  >>
)
PARTITION BY DATE(start_timestamp)
CLUSTER BY service_name, name;
`

// LogsDDL creates the log table per §7.2.
//
// Logs get no default clustering in v1. This is final rather than pending:
// log query patterns vary too widely for a default to be useful.
const LogsDDL = `
CREATE TABLE IF NOT EXISTS ` + "`%[1]s.%[2]s.%[3]s`" + ` (
  timestamp                          TIMESTAMP NOT NULL,
  timestamp_source                   STRING,
  time_unix_nano                     STRING    NOT NULL,
  observed_timestamp                 TIMESTAMP,
  observed_time_unix_nano            STRING    NOT NULL,
  export_received_timestamp          TIMESTAMP NOT NULL,

  trace_id                           STRING,
  span_id                            STRING,
  flags                              INT64,

  event_name                         STRING,
  severity_number                    INT64,
  severity_text                      STRING,
  body                               JSON,

  service_name                       STRING,
  resource_attributes                JSON,
  resource_schema_url                STRING,
  resource_dropped_attributes_count  INT64,

  scope_name                         STRING,
  scope_version                      STRING,
  scope_attributes                   JSON,
  scope_schema_url                   STRING,
  scope_dropped_attributes_count     INT64,

  log_attributes                     JSON,
  dropped_attributes_count           INT64,

  schema_version                     STRING    NOT NULL,

  source_record_namespace            STRING,
  source_record_id                   STRING,
  record_fingerprint                 STRING    NOT NULL
)
PARTITION BY DATE(timestamp);
`

// SpansDedupViewDDL is the canonical span deduplication view (§7.4).
//
// Identity is (trace_id, span_id). Ordering is on the raw nanos rather than
// end_timestamp, because microsecond precision alone can tie across retries;
// export_received_timestamp breaks any remaining tie.
//
// Pending amendment B.2: once truncate-and-flag lands, an is_truncated ASC
// term goes first in the ORDER BY, so a truncated copy can never outrank the
// full copy it duplicates. Not applied here because the column is not yet part
// of the approved §7.1 contract.
const SpansDedupViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s.%[3]s_dedup`" + ` AS
SELECT * FROM ` + "`%[1]s.%[2]s.%[3]s`" + `
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY trace_id, span_id
  ORDER BY CAST(end_time_unix_nano AS BIGNUMERIC) DESC,
           export_received_timestamp DESC
) = 1;
`

// LogsDedupViewDDL is the canonical log deduplication view (§7.4).
//
// Two-tier identity. Exact identity is the typed tuple
// (source_record_namespace, source_record_id) when instrumentation supplies
// one; otherwise identity falls back to record_fingerprint.
//
// The PARTITION BY deliberately uses three separate expressions and never
// string concatenation. Flattening to CONCAT(namespace, '/', id) would collide
// ('a/b','c') with ('a','b/c') and would mix source IDs and fingerprints into
// a single string domain. The leading boolean keeps the two identity kinds in
// disjoint partitions.
//
// Scope of the promise, deliberately narrow: this removes content-equivalent
// retry duplicates. A fingerprint cannot separate two legitimately emitted log
// records whose complete contents are identical — exact record identity is
// available only through the trusted namespace/ID tuple.
const LogsDedupViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s.%[3]s_dedup`" + ` AS
SELECT * FROM ` + "`%[1]s.%[2]s.%[3]s`" + `
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY
    (source_record_namespace IS NOT NULL AND source_record_id IS NOT NULL),
    IFNULL(source_record_namespace, ''),
    IF(source_record_namespace IS NOT NULL AND source_record_id IS NOT NULL,
       source_record_id, record_fingerprint)
  ORDER BY export_received_timestamp DESC
) = 1;
`

// FingerprintPrefix is the version tag on record_fingerprint values, which are
// formatted as "v1:" + lowercase_hex(SHA256(canonical_log_record)).
//
// The canonical form covers all non-derived OTLP source fields with object
// keys sorted by UTF-8 byte order and array order preserved.
// export_received_timestamp is ingestion-local and derived, so it is excluded:
// including it would give every retry a distinct fingerprint and defeat the
// whole mechanism.
const FingerprintPrefix = "v1:"
