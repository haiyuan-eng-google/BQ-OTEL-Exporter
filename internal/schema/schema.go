// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package schema holds the versioned BigQuery table contract and the canonical
// deduplication SQL that goes with it.
package schema // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"

// Version is the schema contract version. Additive, nullable columns are a
// minor bump; anything that could break a pinned query is a major bump.
const Version = "v1"

// SpansDDL creates the span table. Day-partitioned on start time and clustered
// for the common trace lookup, so ordinary queries prune both.
const SpansDDL = `
CREATE TABLE IF NOT EXISTS ` + "`%[1]s.%[2]s.%[3]s`" + ` (
  trace_id             STRING     NOT NULL,
  span_id              STRING     NOT NULL,
  parent_span_id       STRING,
  trace_state          STRING,
  name                 STRING     NOT NULL,
  kind                 STRING     NOT NULL,
  start_time           TIMESTAMP  NOT NULL,
  end_time             TIMESTAMP  NOT NULL,
  duration_nanos       INT64      NOT NULL,
  status_code          STRING,
  status_message       STRING,
  resource_attributes  JSON,
  scope_name           STRING,
  scope_version        STRING,
  scope_attributes     JSON,
  attributes           JSON,
  events               JSON,
  links                JSON,
  is_truncated         BOOL       NOT NULL,
  record_fingerprint   BYTES      NOT NULL,
  schema_version       STRING     NOT NULL
)
PARTITION BY DATE(start_time)
CLUSTER BY trace_id, name;
`

// LogsDDL creates the log table.
const LogsDDL = `
CREATE TABLE IF NOT EXISTS ` + "`%[1]s.%[2]s.%[3]s`" + ` (
  timestamp               TIMESTAMP  NOT NULL,
  observed_timestamp      TIMESTAMP,
  trace_id                STRING,
  span_id                 STRING,
  severity_text           STRING,
  severity_number         INT64,
  body                    JSON,
  resource_attributes     JSON,
  scope_name              STRING,
  scope_version           STRING,
  scope_attributes        JSON,
  attributes              JSON,
  source_record_namespace STRING,
  source_record_id        STRING,
  is_truncated            BOOL       NOT NULL,
  record_fingerprint      BYTES      NOT NULL,
  schema_version          STRING     NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY severity_number, trace_id;
`

// SpansDedupViewDDL is the canonical span deduplication view.
//
// Delivery is at-least-once, so a retried append can land the same logical span
// twice. Identity is (trace_id, span_id). Ordering prefers an untruncated copy
// over a truncated one, then the latest end time, so a truncated span can never
// outrank the full one it duplicates.
const SpansDedupViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s.%[3]s_dedup`" + ` AS
SELECT * EXCEPT(_rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY trace_id, span_id
    ORDER BY is_truncated ASC, end_time DESC
  ) AS _rn
  FROM ` + "`%[1]s.%[2]s.%[3]s`" + `
)
WHERE _rn = 1;
`

// LogsDedupViewDDL is the canonical log deduplication view.
//
// Logs have no natural primary key. Where a producer supplies a trusted
// (source_record_namespace, source_record_id) tuple that is the identity;
// otherwise identity falls back to the content fingerprint, which collapses
// two byte-identical but genuinely distinct records into one. That limit is
// inherent to fingerprinting and is stated rather than hidden.
const LogsDedupViewDDL = `
CREATE OR REPLACE VIEW ` + "`%[1]s.%[2]s.%[3]s_dedup`" + ` AS
SELECT * EXCEPT(_rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY
      COALESCE(source_record_namespace, ''),
      COALESCE(source_record_id, TO_HEX(record_fingerprint))
    ORDER BY is_truncated ASC, observed_timestamp DESC
  ) AS _rn
  FROM ` + "`%[1]s.%[2]s.%[3]s`" + `
)
WHERE _rn = 1;
`
