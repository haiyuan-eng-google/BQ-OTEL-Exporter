# BigQuery Continuous Queries Templates for Real-Time Alerting

This directory provides parameterized BigQuery Continuous Query templates that
stream real-time alerts from newly ingested OpenTelemetry spans to Cloud
Pub/Sub or to an `otel_alerts` BigQuery table.

BigQuery Continuous Queries run continuously against the append change-history
stream (`APPENDS(TABLE ...)`) of a base table, avoiding the polling latency and
repeated partition scans of scheduled queries.

## Templates

| File | Alert kind | What it detects | Threshold variables |
|:---|:---|:---|:---|
| `tool_error_rate_alert.sql` | `TOOL_ERROR` | GenAI, tool, or MCP spans with `status.code = 2` or a non-null `error.type` attribute | — |
| `token_budget_burn.sql` | `TOKEN_BUDGET_BURN` | Spans whose single-turn input or output tokens exceed configured thresholds | `max_turn_input_tokens`, `max_turn_output_tokens` |
| `agent_latency_p95.sql` | `AGENT_LATENCY_HIGH` | `invoke_agent`, `chat`, `generate_content`, or `execute_tool` spans slower than the duration threshold | `max_turn_duration_ms` |

Every template accepts `${project_id}`, `${dataset_id}`, `${spans_table}`,
`${alerts_table}`, and `${pubsub_topic}`.

## Why Continuous Queries Read Base Tables (`APPENDS`)

BigQuery `APPENDS(TABLE ...)` reads the append history of a base table. Logical
views that use `QUALIFY` or window functions—such as `otel_spans_dedup`,
`v_gen_ai_spans`, `v_gen_ai_model_costs`, `v_gen_ai_tool_usage`, and
`v_mcp_spans`—are not eligible as `APPENDS` sources in Continuous Queries.

For that reason, each template reads the base spans table (`otel_spans`)
directly and extracts promoted JSON attributes (`gen_ai.operation.name`,
`gen_ai.agent.name`, `gen_ai.tool.name`, `mcp.method.name`, `error.type`, and
token counts) inline.

### At-Least-Once Delivery and Deduplication

Because the exporter writes to the Storage Write API default stream with
at-least-once delivery and `APPENDS` runs before `otel_spans_dedup`, a retried
batch can emit the same span alert more than once.

Each template sets a deterministic `alert_id` of the form
`<ALERT_KIND>:<trace_id>:<span_id>`. Downstream Pub/Sub subscribers or queries
over `otel_alerts` should deduplicate on `alert_id` (or `(trace_id, span_id)`)
within their retry window.

## Prerequisites

1. **BigQuery Edition and Reservation**: Continuous Queries require a
   reservation using the `ENTERPRISE` or `ENTERPRISE_PLUS` edition and a
   reservation assignment with `job_type = "CONTINUOUS"`.
2. **Service Account**: Exporting Continuous Query results to Cloud Pub/Sub
   requires running the job as a service account. Jobs run as a service account
   also run for up to 150 days (compared with 2 days for user-account jobs).
3. **IAM Roles**:
   - **Continuous Query service account**:
     - `roles/bigquery.jobUser` on the project (`bigquery.jobs.create`).
     - `roles/bigquery.dataViewer` on the OTel dataset (`bigquery.tables.export` and `bigquery.tables.getData`).
     - `roles/pubsub.publisher` and `roles/pubsub.viewer` on the destination Pub/Sub topic (when using the default `EXPORT DATA` sink).
     - `roles/bigquery.dataEditor` on `otel_alerts` (when using the table-sink `INSERT INTO` variant).
   - **Submitting operator**:
     - `roles/bigquery.jobUser` on the project.
     - `roles/iam.serviceAccountUser` on the Continuous Query service account.

## Provisioning with Terraform

Set `enable_continuous_queries=true` in `deploy/terraform` to provision the
`otel_alerts` table, Pub/Sub topic, dedicated service account, IAM bindings,
and `CONTINUOUS` reservation assignment:

```bash
cd deploy/terraform
terraform init
terraform apply \
  -var="project_id=my-project" \
  -var="enable_continuous_queries=true"
```

Terraform renders all three SQL templates into the
`continuous_queries_rendered_sql` output and prints the ready-to-run `bq query`
commands in `continuous_queries_bq_cli_commands`.

## Running a Continuous Query with `bq`

Render one of the templates (or read it from `terraform output`) and pass
`--continuous=true` and `--connection_property=service_account=...`:

```bash
SA_EMAIL="$(terraform -chdir=deploy/terraform output -raw continuous_queries_service_account_email)"
SQL="$(terraform -chdir=deploy/terraform output -json continuous_queries_rendered_sql | jq -r '.tool_error_rate_alert')"

bq query \
  --project_id=my-project \
  --location=US \
  --use_legacy_sql=false \
  --continuous=true \
  --connection_property="service_account=${SA_EMAIL}" \
  "${SQL}"
```

To write into `otel_alerts` instead of Pub/Sub, uncomment the `INSERT INTO`
statement at the bottom of the template and comment out the `EXPORT DATA`
statement above it.
