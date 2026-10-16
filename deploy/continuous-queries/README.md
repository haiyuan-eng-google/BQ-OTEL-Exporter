# BigQuery Continuous Queries Templates for Real-Time Alerting

This directory provides parameterized BigQuery Continuous Query templates that
stream real-time alerts from newly ingested OpenTelemetry spans to Cloud
Pub/Sub (or to an `otel_alerts` BigQuery table).

BigQuery Continuous Queries run continuously against the append change-history
stream (`APPENDS(TABLE ...)`) of a base table, avoiding the polling latency and
repeated partition scans of scheduled queries.

## Templates and Stateless Per-Span Semantics

| File | Alert kind | What it detects | Threshold variables |
|:---|:---|:---|:---|
| `tool_error_rate_alert.sql` | `TOOL_ERROR` | Tool or MCP spans (`gen_ai.operation.name = 'execute_tool'`, `gen_ai.tool.name`, or `mcp.method.name`) with `status.code = 2` or a non-null `error.type` attribute | — |
| `token_budget_burn.sql` | `TOKEN_BUDGET_BURN` | Spans whose single-turn input or output tokens exceed configured thresholds | `max_turn_input_tokens`, `max_turn_output_tokens` |
| `agent_latency_p95.sql` | `AGENT_LATENCY_HIGH` | `invoke_agent`, `chat`, `generate_content`, or `execute_tool` spans slower than the duration threshold | `max_turn_duration_ms` |

Every template accepts `${project_id}`, `${dataset_id}`, `${spans_table}`, and
`${pubsub_topic}` (plus its threshold variables).

All three shipped templates are **stateless per-span filters**—each emits one
Pub/Sub message per matching span row rather than computing a tumbling-window
error rate, tokens-per-minute burn, or rolling p95. Stateful window
aggregations (`TUMBLE`, `HOP`, `SESSION`) in BigQuery Continuous Queries are in
Preview with strict operator restrictions, so the default templates evaluate
each span row individually as it arrives. During an outage, a stateless filter
emits one message per failing span; downstream consumers (Cloud Run, Cloud
Functions, Dataflow, or Pub/Sub subscriptions) or an optional windowed query
(see [Table-Sink and Windowed Variants](#table-sink-and-windowed-variants))
should aggregate or rate-limit alerts.

## Base Tables, Partition Filters, and Column-Level Security

- **Why Continuous Queries read base tables (`APPENDS`)**: BigQuery
  `APPENDS(TABLE ...)` reads the append history of a base table. Logical views
  that use `QUALIFY` or window functions—such as `otel_spans_dedup`,
  `v_gen_ai_spans`, `v_gen_ai_model_costs`, `v_gen_ai_tool_usage`, and
  `v_mcp_spans`—are not eligible as `APPENDS` sources. Each template therefore
  reads `otel_spans` directly and extracts promoted JSON attributes inline.
- **`require_partition_filter` compatibility**: Every template includes
  `WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)` so
  partition pruning succeeds when `require_partition_filter = true` is set on
  `otel_spans`.
- **Column-level security incompatibility**: BigQuery Continuous Queries do not
  support tables with column-level security policy tags or row-level security.
  `tool_error_rate_alert.sql` omits `status.message`, and `deploy/terraform`
  rejects `enable_continuous_queries = true` when
  `enable_column_level_security = true`.

## Delivery, Replay, and Runtime Limits

- **At-least-once delivery and automatic reprocessing**: Duplicate alerts can
  come from exporter retries on the Storage Write API default stream and from
  BigQuery Continuous Query automatic reprocessing during internal maintenance
  or transient sink retries. Every template sets a deterministic `alert_id` of
  the form `<ALERT_KIND>:<trace_id>:<span_id>`. Downstream Pub/Sub subscribers
  or queries over `otel_alerts` should deduplicate on `alert_id`.
- **10-minute lookback on start/restart**: Each template starts at
  `CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE`. Starting or restarting a job
  replays the last 10 minutes of spans, while downtime longer than 10 minutes
  skips older spans unless you widen that interval (within BigQuery's
  time-travel window).
- **48-hour watermark lag and 150-day max runtime**: A Continuous Query fails
  if its processing watermark falls more than 48 hours behind, and a job run as
  a service account stops after 150 days (2 days for a user account). Both
  cases require re-submitting the job.

## Prerequisites and Cost

1. **APIs**: Enable `bigquery.googleapis.com`,
   `bigqueryreservation.googleapis.com`, `pubsub.googleapis.com`, and
   `iam.googleapis.com` in the project.
2. **BigQuery Edition, Reservation, and Slot Cost**: Continuous Queries require
   an `ENTERPRISE` or `ENTERPRISE_PLUS` reservation and a reservation
   assignment with `job_type = "CONTINUOUS"`. Each Continuous Query needs at
   least 10 slots, and a `CONTINUOUS` assignment is capped at 500 slots.
   Setting `continuous_query_slot_capacity = 100` provisions 100 always-on
   baseline Enterprise slots, which incur continuous capacity charges while the
   reservation exists (configure slot autoscaling if you prefer scale-to-zero
   or autoscale capacity).
3. **Service Account and IAM Roles**:
   - **Continuous Query service account**:
     - `roles/bigquery.jobUser` on the project (`bigquery.jobs.create`).
     - `roles/bigquery.dataViewer` on the OTel dataset (`bigquery.tables.export` and `bigquery.tables.getData`).
     - `roles/pubsub.publisher` and `roles/pubsub.viewer` on the destination Pub/Sub topic (for `EXPORT DATA`).
     - `roles/bigquery.dataEditor` on `otel_alerts` (for the `INSERT INTO` variant).
   - **Submitting operator**:
     - `roles/bigquery.jobUser` on the project.
     - `roles/iam.serviceAccountUser` on the Continuous Query service account.
4. **Pub/Sub Subscription**: Terraform creates the alert topic
   (`<dataset>-otel-alerts`) without a subscription. Create a pull or push
   subscription (or Cloud Run / Dataflow trigger) to receive published alerts.

## Provisioning with Terraform

From the repository root, set `enable_continuous_queries=true` in
`deploy/terraform` to provision `otel_alerts`, the Pub/Sub topic, the dedicated
service account (`<service_account_id>-cq`), IAM bindings, and the `CONTINUOUS`
reservation assignment:

```bash
terraform -chdir=deploy/terraform init
terraform -chdir=deploy/terraform apply \
  -var="project_id=my-project" \
  -var="enable_continuous_queries=true"
```

Terraform renders all three SQL templates into the
`continuous_queries_rendered_sql` output and prints ready-to-run `bq query`
commands in `continuous_queries_bq_cli_commands`.

## Running a Continuous Query with `bq`

Pipe the rendered SQL on standard input to `bq query` with `--continuous=true`
and `--connection_property=service_account=...` (piping via stdin avoids `bq`
flag parsing on leading SQL text):

```bash
SA_EMAIL="$(terraform -chdir=deploy/terraform output -raw continuous_queries_service_account_email)"

terraform -chdir=deploy/terraform output -json continuous_queries_rendered_sql \
  | jq -r '.tool_error_rate_alert' \
  | bq query \
      --project_id=my-project \
      --location=US \
      --use_legacy_sql=false \
      --continuous=true \
      --connection_property="service_account=${SA_EMAIL}"
```

### Rendering Without Terraform

If you do not use the Terraform module, substitute the `${...}` placeholders
with `sed` before piping to `bq query`:

```bash
sed \
  -e 's/${project_id}/my-project/g' \
  -e 's/${dataset_id}/otel/g' \
  -e 's/${spans_table}/otel_spans/g' \
  -e 's/${pubsub_topic}/otel-otel-alerts/g' \
  deploy/continuous-queries/tool_error_rate_alert.sql \
  | bq query --project_id=my-project --location=US --use_legacy_sql=false \
      --continuous=true --connection_property="service_account=${SA_EMAIL}"
```

## Table-Sink and Windowed Variants

### Writing to `otel_alerts` (`INSERT INTO`)

To write alerts into the `otel_alerts` table instead of Pub/Sub, replace the
`EXPORT DATA ... AS (SELECT TO_JSON_STRING(STRUCT(...)) AS message ...)`
wrapper with `INSERT INTO` matching the `otel_alerts` columns:

```sql
INSERT INTO `my-project.otel.otel_alerts` (
  alert_id, alert_kind, event_time, service_name, trace_id, span_id, payload
)
SELECT
  CONCAT('TOOL_ERROR:', trace_id, ':', span_id) AS alert_id,
  'TOOL_ERROR' AS alert_kind,
  start_time AS event_time,
  service_name,
  trace_id,
  span_id,
  TO_JSON(STRUCT(name AS span_name, status.code AS status_code)) AS payload
FROM APPENDS(TABLE `my-project.otel.otel_spans`, CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE)
WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND (status.code = 2 OR JSON_VALUE(attributes, '$."error.type"') IS NOT NULL)
  AND (
    JSON_VALUE(attributes, '$."gen_ai.operation.name"') = 'execute_tool'
    OR JSON_VALUE(attributes, '$."gen_ai.tool.name"') IS NOT NULL
    OR JSON_VALUE(attributes, '$."mcp.method.name"') IS NOT NULL
  );
```

### Windowed Aggregation (`TUMBLE`, Preview)

Where BigQuery Continuous Query windowing (Preview) is enabled, you can wrap
`APPENDS` in `TUMBLE` over the `_CHANGE_TIMESTAMP` column to emit one alert per
5-minute window instead of one per span:

```sql
SELECT
  window_start,
  window_end,
  service_name,
  COUNT(*) AS error_spans
FROM TUMBLE(
  (
    SELECT _CHANGE_TIMESTAMP, service_name
    FROM APPENDS(TABLE `my-project.otel.otel_spans`, CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE)
    WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
      AND status.code = 2
  ),
  '_CHANGE_TIMESTAMP',
  INTERVAL 5 MINUTE
)
GROUP BY window_start, window_end, service_name;
```
