# Schema and delivery rationale

This page explains why the BigQuery tables, the deduplication views and the
write path look the way they do. It describes the `v0alpha2` contract
(`schema_version = "v0alpha2"`). The definitions themselves live in code, so
this page does not repeat them:

- table DDL, deduplication SQL and companion views:
  [`internal/schema/schema.go`](../internal/schema/schema.go)
- the typed contract the exporter writes against:
  [`internal/schema/tableschema.go`](../internal/schema/tableschema.go)
- how append failures are classified:
  [`internal/bigquery/errors.go`](../internal/bigquery/errors.go)

Code comments that need this background point to this page instead of to an
external document, so each section is written to stand on its own.

> **The contract is alpha.** Documented breaking corrections are allowed while
> the version starts with `v0alpha`; `v0alpha2` is the first such correction.
> The contract freezes as `v1` only after at least two independent design
> partners have completed representative workflows against a released
> artifact. See [What is not settled](#what-is-not-settled).

## The design principle

The tables are a **versioned relational projection of the supported, validated
OTLP trace and log fields**. Application semantic conventions are interpreted in
views, not stored in columns, and `service_name` is the one deliberately
promoted attribute.

Keeping application semantics out of the columns matters because GenAI and agent
attribute names are still moving. The shipped views already coalesce
`gen_ai.system` with its successor `gen_ai.provider.name`, and read
`gcp.vertex.agent.*` attributes next to `gen_ai.*` ones. A view absorbs a change
like that with a `CREATE OR REPLACE`; a column set that encoded the conventions
would need a table migration each time.

Preservation is bounded. The contract keeps every field it supports and says
what happens to the rest: a record that cannot fit is rejected and counted (see
[Rejection and structural limits](#rejection-and-structural-limits)). It is not
a promise to reconstruct every conceivable OTLP payload.

## Table contract

Each row of the table below is a decision and the constraint that forced it.

| Decision | What forced it |
| --- | --- |
| **Every timestamp is stored twice**: a `TIMESTAMP` and the exact OTLP nanoseconds as `INT64` (`*_unix_nano`). | BigQuery `TIMESTAMP` has microsecond precision and OTLP carries nanoseconds. The `TIMESTAMP` is for querying and partitioning; the `INT64` keeps the exact value and orders retries whose microsecond times tie. OTLP nanoseconds are an unsigned 64-bit value, so a record whose nanoseconds exceed the `INT64` maximum is rejected (`timestamp_overflow`) instead of being stored wrongly. |
| **Zero is data, not absence.** A raw `*_unix_nano` column keeps a zero exactly as sent; the matching `TIMESTAMP` is `NULL` rather than claiming the Unix epoch. | OTLP lets an event or log record omit its times. Writing 1970-01-01 would look like a real time to every query. |
| `duration_nano` is precomputed as `INT64`. | Duration analysis is common, and the unit belongs in the name. Clock skew between hosts can put a span's end before its start, so a negative value is clamped to zero while the raw times stay as sent. |
| `kind` and `status.code` are stored as the **integer and the enum name** (`kind_name`, `status.code_name`). | The integer is the stable OTLP wire value and survives a number the producer sent outside the known enum. The name makes SQL readable without a join or a `CASE`, and is `NULL` for a value outside the enum. |
| Attributes, resource and scope attributes, event and link attributes, and the log body are **plain JSON plus an `attributes_schema` sidecar**. | Attribute keys are open-ended and vary by signal and framework, so no fixed column set covers them, and a repeated key/value record would force an `UNNEST` into every query. Plain JSON keeps one attribute a path expression (`JSON_VALUE`, `LAX_INT64`). Plain JSON cannot carry OTLP's leaf types on its own: an `int64` has to travel as a decimal string to keep its precision beyond 2^53, which makes it look like any other string, and JSON has no bytes. The sidecar records the OTLP type of each leaf (`string`, `bool`, `int64`, `double`, `bytes`, `empty`, with arrays and maps described structurally), so nothing is lost. Keys are written in sorted order, so one logical record always encodes to the same bytes. |
| `service_name` is copied out of the `service.name` resource attribute into a column. | It appears in nearly every query and is the leading clustering column. The attribute also stays in `resource.attributes`. Nothing else is promoted: a second exception would start the slide into a semantic schema. |
| `events` and `links` are `REPEATED` records on the span row, not side tables. | A span stays atomic: one append, one row, one identity for deduplication. Side tables would each need their own identity, their own deduplication SQL, and a join for the common case. |
| `schema_version` is a per-row `REQUIRED` column. | It keeps a table that holds more than one version legible during a rollout and lets a reader fail fast on a version it does not understand. |
| `receive_time` and `receive_time_unix_nano` are `REQUIRED`. | They record when the exporter received the batch and break ties in deduplication ordering. They are ingestion-local, so they are left out of the log fingerprint. |
| **`REQUIRED` versus `NULLABLE` follows the rejection rules.** A column is `REQUIRED` exactly when a record without it is rejected. | `trace_id`, `span_id` and a span's start and end times are `REQUIRED`, and a span without them is rejected. `parent_span_id` is `NULLABLE` because an absent parent is a valid root span: empty is not the same as `NULL`. A log record may leave both of its times unset, so a log's raw `*_unix_nano` columns are `REQUIRED` but may hold zero, and its partition `timestamp` falls back instead (below). |
| `otel_spans` is partitioned **hourly** on `start_time`; `otel_logs` is partitioned **daily** on `timestamp`. | Time is the universal predicate for trace and log analytics, so the partition column must always be populated. Hourly partitions prune more tightly for narrow time windows, at the cost of BigQuery's limit of 10,000 partitions per table (about 416 days of hourly partitions); the README's "Operating v0alpha2 tables" section covers `partition_expiration_days`. |
| A log's `timestamp` **falls back** from event time to observed time to exporter receive time, and `timestamp_source` (`event`, `observed`, `exporter`) records which one was used. | The partition column cannot be `NULL`, and OTLP permits both record times to be zero. |
| Clustering is `(service_name, trace_id, name)` for spans and `(service_name, trace_id, severity_number)` for logs. | Per-service scans, trace lookup and, for logs, severity filters stay pruned. These choices come from expected query patterns and have not been benchmarked at volume. |
| `payload_refs` is a `REPEATED` record of BigQuery `ObjectRef` values on spans and logs. | Large prompts, completions and multimodal payloads can live in object storage and still be reached from SQL, so a row does not have to hold them inline. |
| `otel_metrics` is a 39-column table storing all five OTLP metric data-point types (`GAUGE`, `SUM`, `HISTOGRAM`, `EXPONENTIAL_HISTOGRAM`, `SUMMARY`) in a single relational table, partitioned daily on `timestamp` and clustered by `(service_name, metric_name, metric_type)`. | A unified table avoids the fan-out and joins of one table per data-point type. `metric_type` is in the clustering key because metrics analytics often filters by type. `point_fingerprint` is the dedup key. |
| `otel_quarantine` is a 12-column dead-letter table partitioned daily on `ingest_time` with 30-day expiration and clustered by `(signal, error_code, service_name)`. | Permanently rejected records are preserved for replay (via `cmd/bq-quarantine-replay`) instead of being silently dropped. The short retention keeps storage bounded. |

## Log identity and the record fingerprint

A log record has no natural unique key in OTLP, so the table borrows the
identity Cloud Logging uses, `(log_name, insert_id)`:

- `log_name` is taken from the configured attribute, then from well-known
  attributes, then from `service_name`, then `"default"`.
- `insert_id` is the upstream record ID when instrumentation supplies one (the
  configured attribute, then well-known attributes), and the record
  fingerprint otherwise.
- `record_fingerprint` is `"fp1:"` followed by the lowercase hex SHA-256 of a
  canonical form of the record. The canonical form covers every non-derived OTLP
  field of the record, its resource and its scope, and ends with the record's
  `intra_batch_ordinal`.

Several choices in the fingerprint matter:

- **Values are fingerprinted as plain JSON together with their type sidecars.**
  Plain JSON alone would give the integer `42` and the string `"42"` the same
  fingerprint.
- **Times are the raw OTLP nanoseconds**, not the truncated microsecond
  `TIMESTAMP` values.
- **Receive time is excluded.** It differs on every retry, so including it
  would defeat the mechanism.
- **The `intra_batch_ordinal` separates identical records within one export
  batch**, while a retry of the batch repeats its records in the same order and
  therefore reproduces every fingerprint. Only kept records consume an ordinal;
  a rejected record is rejected again on every retry, so the ordinals of the
  others still repeat.
- **The JSON writer is hand-written.** `encoding/json` escapes `<`, `>` and `&`
  for HTML safety. Those escapes are legal JSON, but they would make the
  fingerprint depend on an encoder policy instead of on the record's content.

An upstream `insert_id` is trusted as the record's identity. Multi-tenant
collectors that accept untrusted OTLP should strip or namespace those attributes
before export, as the README explains.

## Deduplication

Delivery is at-least-once (see [Write path and delivery](#write-path-and-delivery)),
so every row carries an identity and the shipped `_dedup` views keep one row per
identity.

- **Spans** are identified by `(start_time, service_name, trace_id, span_id)`.
  Every retry of a span carries the same start time and service, so this is the
  span identity plus the partitioning column and the leading clustering columns.
  The window also partitions on `name`, the last clustering column: it is fixed
  for a span, so it never splits the retries of one. A filter on any of them
  can be pushed below the window function, so a query
  over the view still prunes partitions and clusters. Among copies, the one
  with the latest `end_time_unix_nano` wins, then the latest
  `receive_time_unix_nano`. Ordering uses the raw nanoseconds because the
  microsecond `end_time` alone can tie across retries.
- **Logs** are identified by
  `(timestamp, time_unix_nano, service_name, log_name, insert_id)`. The
  partitioning column and leading clustering column come first for the same
  pruning reason, and `time_unix_nano` keeps apart records that share a
  microsecond. The window also partitions on `trace_id`, `span_id`, and
  `severity_number`, the remaining clustering and search-index columns. The
  record fingerprint covers them, so retries of one record agree on them. The
  most recently received copy wins.
- **Metrics** are identified by
  `(timestamp, time_unix_nano, service_name, metric_name, point_fingerprint)`.
  The window also partitions on `metric_type`, the last clustering column, which
  the fingerprint covers. The most recently received copy wins.

The promise is deliberately narrow: **the views remove retry duplicates**.

- Identical log records sent without an upstream `insert_id` in separate batches
  at the same nanosecond collapse into one row, because the fingerprint
  separates identical records only within a batch.
- A log record with neither an event nor an observed time is stamped with the
  receive time, which differs on every retry, so its retries are not collapsed.
- Equal ordering keys can still tie. `ROW_NUMBER` does not order peers
  deterministically, so no deterministic winner is promised among conflicting
  rows that tie on every ordering key.
- Duplicates from outside the retry window (fan-out to several pipelines,
  several collectors exporting the same telemetry, upstream SDK retries) are
  outside what the exporter can bound. The view is the correctness backstop for
  those as well.

## Rejection and structural limits

A record that cannot become a valid row is rejected as a **permanent,
row-scoped failure**. Retrying produces the same result, and one pathological
record must not block the telemetry around it, so the record is counted and
dropped (or quarantined) while the rest of the batch continues.

There are three kinds of rejection, in the order they are checked:

1. **Contract violations**: an all-zero trace or span ID (`invalid_id`), a span
   without a start or end time (`missing_timestamp`), or nanoseconds that do not
   fit `INT64` (`timestamp_overflow`). These records have no valid row at all:
   writing one anyway would fail the whole append or store a value the column
   does not mean.
2. **Structural limits**, enforced **before serialization**: attribute, event
   and link counts, nesting depth, collection size and the size of a single
   string or bytes value (`limits.*` in the configuration). These limits are
   about shape, not encoded size: a payload can be structurally pathological, a
   deeply self-nested attribute or a million events, and still serialize small.
   The defaults are deliberately generous. They exist to stop pathological
   payloads from becoming unqueryable rows or consuming unbounded memory, not to
   shape ordinary telemetry.
3. **Encoded-size limits**, enforced **after serialization**: a row that cannot
   be serialized is rejected (`serialization`), as is a row above
   `write.max_row_bytes` (`row_bytes`), and requests are split to stay under
   `write.max_request_bytes`. Rows the service itself rejects are reported as
   `row_error`.

Every rejection increments `rejected_rows{reason}`. The `reason` domain is
closed and never derived from telemetry content. When `quarantine.enabled` is
`true` the record is also written to `otel_quarantine`, with the reason in
`error_code`, the record as OTLP JSON in `raw_payload`, and a 30-day partition
expiry because the table is a triage queue rather than an archive. The
quarantine table has no deduplication view: a record whose whole batch is
retried can be quarantined more than once.

## Diagnostics carry no telemetry content

Raw trace IDs, span IDs, source record IDs and record fingerprints are kept out
of normal logs, along with attribute keys, attribute values and log bodies. That
includes the stable cross-system correlators: a trace ID in a log line is
exactly as identifying as the span it names, and it is the thing an operator is
most tempted to log.

What is left is enough to debug with: the destination, an error code, a row
index, a sanitized field path (the shape of the path only, never an attribute
key), a serialized size, and a random operation ID that ties log lines to each
other without tying them to data. The operation ID is random rather than
derived, because a hash of the payload would be a stable correlator across
systems.

The same rule bounds metric labels: every label domain is closed, because
content-derived labels would be unbounded cardinality and would leak payload
into the metrics pipeline.

## Write path and delivery

**At-least-once on the default stream.** The exporter writes on the Storage
Write API default stream, which is offset-free and needs no stream-offset
bookkeeping. A retried append can therefore land the same logical record twice.
This is a deliberate choice rather than a limitation being worked around: read
time deduplication is the proportionate answer for most consumers, while
exactly-once would mean committed-mode streams with per-stream offset
management, lower default quotas and materially more server-side orchestration.

**Three failure classes, exactly one owner each.** An append failure is
`permanent` (it will never succeed on retry), `retryable` (transient) or
`uncertain` (the append may or may not have been applied). The exporter handles
permanent failures itself, and returns retryable and uncertain ones so that
`exporterhelper` owns backoff and replay. Exactly one owner per class is the
point: two components retrying the same failure would multiply attempts and make
the duplicate bound below meaningless. The metric label for each failure kind is
drawn from a closed list. `Classify` in `internal/bigquery/errors.go` is the
authoritative table.

**The duplicate bound.** In-process replays are triggered by an uncertain
acknowledgement, for example a deadline that fires after dispatch. `N` such
replays permit at most `N + 1` physical copies of a logical record, and
`uncertain_ack_replays` counts them. Crash replays from a persistent queue are
explicitly out of scope and are not counted. The retry horizon must also be
finite: an unbounded `retry_on_failure.max_elapsed_time` would remove the bound.

**Subset retry on row errors.** When `AppendRows` reports row errors, no rows in
that request were appended. A row error is therefore not just "that row
failed": every other row that shared the request must be sent again, minus the
invalid ones.

## What was rejected and why

- **Typed wrappers around every attribute value.** An earlier pre-alpha revision
  stored attributes in a tagged JSON encoding. It kept type fidelity, but
  queries could not use direct `JSON_VALUE` and `LAX_INT64` expressions on the
  values. `v0alpha2` stores plain JSON and moves the types into
  `attributes_schema`.
- **The Cloud Logging sink schema.** It is log-oriented, not designed for span
  analytics, and adds an intermediate product hop.
- **A `gen_ai.*`-shaped agent-trace table.** The conventions are still moving,
  so semantic opinion belongs in a view that can be revised without a table
  migration.
- **Side tables for events and links.** See the table contract above.
- **Exactly-once delivery.** See the write path above.

## What is not settled

- **The `v1` freeze.** Until at least two independent design partners complete
  representative workflows against a released artifact, the schema stays
  `v0alpha` and each breaking correction is documented.
- **Clustering defaults.** `(service_name, trace_id, name)` for spans and
  `(service_name, trace_id, severity_number)` for logs are chosen from expected
  query patterns, not measured ones.
- **The log fingerprint for records that already carry a trusted `insert_id`.**
  It is computed for every record today. Whether it should be dropped for
  records with a trusted upstream identity is a measured decision that has not
  been made.
- **A total order for conflicting rows that tie on every deduplication ordering
  key.** Today a tie has no deterministic winner.

## Why the schema is the durable artifact

A future Collector, managed endpoint or SDK writer can target the same table
contract if it implements the same versioned mapping, validation, identity and
evolution rules. Transport choice alone does not ensure compatible rows. The
table contract, the deduplication SQL and the views are what survive whichever
transport wins, which is the strongest argument for getting the contract right
and against rushing the `v1` freeze.

## Requirement shorthand in code comments

A few comments in the code use short requirement labels. They mean:

| Label | Meaning |
| --- | --- |
| FR6 | Row-error subset retry: after row errors, resend the other rows of the request, minus the invalid ones (see [Write path and delivery](#write-path-and-delivery)). |
| FR7 | Encoded-size enforcement after serialization (`write.max_row_bytes`, `write.max_request_bytes`). |
| FR8 | Structural limits before serialization (`limits.*`). |
| FR13 | No telemetry content in logs or diagnostics (see [Diagnostics carry no telemetry content](#diagnostics-carry-no-telemetry-content)). |

The architecture review that led to this page is tracked in
[issue #9](https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter/issues/9).
