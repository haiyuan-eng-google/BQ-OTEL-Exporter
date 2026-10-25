# Looker Studio dashboard

A six-page Looker Studio report over the companion views that the exporter's
Terraform module creates (`v_gen_ai_*`, `v_mcp_tool_usage`, `otel_alerts`). A
report cannot be created from a file, so the report is published once as a
template and every deployment copies it with a one-click URL that the Terraform
module renders.

| Path | Purpose |
|:---|:---|
| [`report_spec.yaml`](report_spec.yaml) | Build sheet for the template report: the six data sources, their calculated fields, the report parameters, and the controls and charts of each page. The conformance tests hold the queries to it. |
| [`queries/`](queries/) | The six custom queries, one per data source. |
| [`../../deploy/terraform`](../../deploy/terraform) | The `looker_studio_create_url` output that copies the template for a deployment. |
| [`../../deploy/dataform`](../../deploy/dataform) | Optional incremental marts to read instead of the views. |

## Pages

| Page | Alias | Query | Reads | Dataform mart | `looker_studio_create_url` binds it |
|:---|:---|:---|:---|:---|:---|
| 1. Executive GenAI Overview and FinOps Cost Control | `ds0` | `executive_summary.sql` | `v_gen_ai_model_costs` | `m_gen_ai_model_costs_hourly` | always |
| 2. Agent Sessions and Multi-Turn Engagement | `ds1` | `agent_sessions.sql` | `v_gen_ai_sessions` | `m_gen_ai_sessions_daily` | always |
| 3. Tool and MCP Server Observability | `ds2` | `mcp_tool_usage.sql` | `v_mcp_tool_usage`, `v_gen_ai_tool_usage` | `m_mcp_tool_usage_hourly` (MCP half only) | always |
| 4. Multi-Agent Trajectory and Execution DAG | `ds3` | `agent_trajectory.sql` | `v_gen_ai_agent_trajectory` | `m_gen_ai_agent_trajectory` | while `require_partition_filter` is `false` |
| 5. Real-Time Security and SLA Alerting | `ds4` | `security_sla_alerts.sql` | `otel_alerts` | none | when `enable_continuous_queries` is `true` |
| 6. Metric Exemplar and Trace Deep-Dive | `ds5` | `metric_exemplars.sql` | `v_gen_ai_metric_exemplars` | none | while `require_partition_filter` is `false` |

What each page shows:

1. **Executive overview and FinOps.** LLM calls, input, output, reasoning, cache
   read and cache creation tokens, error rate, hourly token burn, and p50, p95
   and p99 latency by model and provider. The estimated cost uses two report
   parameters for the unit prices of your own models; the template ships none.
2. **Sessions.** Active users (`user_id`), turns per session, session duration
   against tokens, and a ledger of the sessions that had errors.
3. **Tools and MCP.** MCP calls by JSON-RPC method (`tools/call`,
   `resources/read`, `prompts/get`, `initialize`), the calls that are also
   tagged as GenAI tool calls (`gen_ai_tagged_call_count`), server latency, top
   error types, and the GenAI tool view.
4. **Trajectory.** Call depth (`0` to `16`), `agent_path` flows, orphan
   boundaries (`is_orphan`), and token totals that count each token once
   (`is_token_leaf`, `leaf_*_tokens`).
5. **Alerts.** Alert volume by kind (`TOOL_ERROR`, `TOKEN_BUDGET_BURN`,
   `AGENT_LATENCY_HIGH`, and the optional `TOOL_ERROR_WINDOW`) and the alert
   log with clickable trace links.
6. **Exemplars.** Metric points with the `trace_id` and `span_id` of their
   exemplar, and the span behind each one.

## Get your copy with one click

This needs a published template report (see [Build the template](#build-the-template)
if your organization has none) and three values from whoever published it: the
report ID, and the sandbox project and dataset that the template's queries name.

1. Set the three Terraform variables and apply the module:

   ```hcl
   looker_studio_template_report_id  = "<report ID>"
   looker_studio_template_project_id = "<sandbox project ID>"
   looker_studio_template_dataset_id = "<sandbox dataset ID>"
   ```

2. Print the URL. The output is `null` while `looker_studio_template_report_id`
   is empty.

   ```sh
   terraform output -raw looker_studio_create_url
   ```

3. Open it in a browser while signed in as the person who should own the copy.
   Looker Studio creates the report `GenAI observability <dataset>` with the
   data sources pointed at your project and dataset.

The URL is a Looker Studio
[Linking API](https://developers.google.com/looker-studio/integrate/linking-api)
URL. For each bound data source it sets:

| Parameter | Effect |
|:---|:---|
| `ds.<alias>.sqlReplace` | Rewrites the template query: `<sandbox project>.<sandbox dataset>.` becomes `<your project>.<your dataset>.`, and `project=<sandbox project>` (the Cloud Trace link) becomes `project=<your project>`. A replacement applies only where its pattern matches. |
| `ds.<alias>.billingProjectId` | Runs the queries in your project. |
| `ds.<alias>.keepDatasourceName` | Keeps the template's data source names. |
| `ds.<alias>.refreshFields` | Set to `false`, so the template's field types and calculated fields survive. |

The URL leaves out `ds.<alias>.connector`, which makes it an update of the
template's data sources: everything it does not set, such as the query text,
stays as the template has it. A data source that the URL does not bind keeps the
template's query. That query names the sandbox, so the page fails with an access
or "not found" error instead of showing another project's data. Append
`&c.explain=true` to the URL to see how Looker Studio read it.

Google's Linking API documentation shows the same URL format under the
`datastudio.google.com` host name; the module uses `lookerstudio.google.com`.
Replace the host in the URL if your organization still serves the product from
the older name.

## Build the template

Do this once per organization. Looker Studio gets the fields of a custom query
by running it against BigQuery when you add it, so the query must be valid
then: the template cannot hold the `YOUR_PROJECT_ID` and `YOUR_DATASET_ID`
placeholders of the files in [`queries/`](queries/). Build it against a sandbox
project and dataset instead, and give the Terraform variables those names.

1. **Prepare the sandbox.** Run the Terraform module in a sandbox project with
   `enable_continuous_queries = true` and `require_partition_filter = false`
   (the default), so that the six views and `otel_alerts` all exist. Keep real
   data out of the sandbox: anyone who opens the template itself sees whatever
   the template's data credentials can read (see [Credentials and sharing](#credentials-and-sharing)).
2. **Render the queries** for the sandbox:

   ```sh
   mkdir -p /tmp/looker-queries
   for f in dashboards/looker_studio/queries/*.sql; do
     sed -e 's/YOUR_PROJECT_ID/my-sandbox-project/g' \
         -e 's/YOUR_DATASET_ID/my_sandbox_dataset/g' \
         "$f" > "/tmp/looker-queries/$(basename "$f")"
   done
   ```

3. **Add the data sources.** In a new report, choose Add data, then BigQuery,
   then Custom query. Pick the sandbox project, paste one rendered query, tick
   "Enable date range parameters", and add it. Do this for all six queries, name
   each data source as in [`report_spec.yaml`](report_spec.yaml), and keep them
   embedded in the report (not reusable): an embedded data source inherits view
   access from the report, while a reusable one needs view access granted to
   every user who opens the one-click URL. Then open Resource, Manage added data
   sources, and edit the Alias column to `ds0` to `ds5` as in the
   [Pages](#pages) table.
4. **Add the parameters and calculated fields.** Create the two number
   parameters `input_price_per_million_tokens` and
   `output_price_per_million_tokens` (default `0`), and the calculated fields
   under each data source in [`report_spec.yaml`](report_spec.yaml); the
   formulas can be pasted as they are.
5. **Build the pages.** One page per entry under `pages:` in the spec, with its
   controls and components, and a date range control that defaults to the last
   7 days.
6. **Publish.** Share the report (see [Credentials and sharing](#credentials-and-sharing))
   and note the report ID: the part of its URL between `reporting/` and `/page`.
   Hand the report ID and the two sandbox names to the teams that deploy the
   module.

## Credentials and sharing

A custom query data source reads BigQuery with the data credentials you pick
for it:

| Data credentials | Who needs `roles/bigquery.dataViewer` on the dataset and `roles/bigquery.jobUser` on the billing project |
|:---|:---|
| Owner's (the default) | Only the data source owner. Viewers see the charts without BigQuery access of their own, which also means they see everything the owner can read. |
| Viewer's | Every viewer of the report. |
| Service account | Only the service account. |

The data sources that the one-click URL creates use the credentials of the
person who opens it, so open it as the person who should own the copy and who
can read your dataset.

To share the template with another organization, set the template report's
sharing so that people outside the owning organization can view it (for example
link sharing). The owning organization's Workspace sharing policy must allow
that. The template reads only the sandbox, so share it with Viewer's
credentials or a sandbox that holds no data you would mind exposing.

## Datasets that require a partition filter

With `require_partition_filter` set, every query must filter on a column that
lets BigQuery skip partitions. Pages 1, 2, 3 and 5 keep working: each query puts
a range predicate on the view's partition column (`usage_hour`,
`session_start_date`, `event_time`) from the date range parameters. Pages 4 and
6 read views that join spans on a time window that depends on each row, so
BigQuery rejects every query over them while the spans table requires a
partition filter, and the module creates them only with `allow_unprunable_views`.
The URL therefore leaves `ds3` and `ds5` unbound under `require_partition_filter`.

To restore page 4 there, run the [Dataform package](../../deploy/dataform/README.md)
and edit `ds3` in your copy so that its `FROM` clause reads
`<your project>.<marts dataset>.m_gen_ai_agent_trajectory`. Page 6 has no mart.
The same edit points any other data source at its mart (see the table in
[Pages](#pages)); a mart lags its view by the ingestion delay and the run
interval, as the Dataform README explains.

## Reading the charts

- **Percentiles do not average.** The views hold p50, p95 and p99 per hour (and
  per model or tool). Pages 1 and 3 chart the maximum over the range, the worst
  hour. For an exact percentile over a range, query the spans.
- **Page 3 has two sources in one data source.** The `source` column is `mcp`
  for rows of `v_mcp_tool_usage` and `gen_ai` for rows of `v_gen_ai_tool_usage`.
  A call that carries both an MCP method and a GenAI tool name is in both, so
  filter each chart on `source` and never add `call_count` across the two.
  `gen_ai_tagged_call_count` is the part of an MCP row that is also a GenAI
  tool call, and `mcp_only_call_count` is the rest.
- **Page 2 has one row per UTC date and session.** A session that crosses
  midnight UTC is on two rows, so the page counts distinct `session_id`.
  `user_id` and `agent_name` come from `ANY_VALUE` over the session's spans: a
  session that reports several values, or a mix of values and nulls, shows an
  arbitrary one of them.
- **Page 4 counts tokens once with `leaf_*_tokens`.** A parent span can report
  the sum of its children, so adding `gen_ai_usage_*_tokens` over every span
  counts those tokens twice; `leaf_input_tokens`, `leaf_output_tokens` and
  `leaf_reasoning_tokens` keep the count of token leaves only. The depth stops at
  `16`: deeper spans are not in the view.
- **`is_orphan` marks a missing parent.** It is `true` on a span whose
  `parent_span_id` matches no span within 24 hours before to 1 hour after it, and
  on every span below that one. Filter `depth = 0` as well to list only the
  orphan roots.
- **Page 5 deduplicates alerts.** Continuous queries deliver alerts at least
  once, so the query keeps the earliest row of each `alert_id`. Windowed alerts
  have no trace, so their `trace_id` and link are empty.
- **Trace links** open Cloud Trace Explorer in your project and work when the
  same traces are also exported to Cloud Trace. Edit the link prefix in
  `agent_trajectory.sql`, `security_sla_alerts.sql` and `metric_exemplars.sql`
  for another tracing UI.
- **Status messages are not selected.** With `enable_column_level_security`,
  the span status message sits behind a policy tag, and the Dataform marts leave
  it out. No query selects it; the pages show `status_code` and `error_type`.

## Test a query

Each query takes two string parameters, `DS_START_DATE` and `DS_END_DATE`, in
`YYYYMMDD` form with both ends included; Looker Studio fills them from the
report's date range when "Enable date range parameters" is on. To check a query
against your dataset without running it:

```sh
bq query --dry_run --use_legacy_sql=false \
  --parameter=DS_START_DATE:STRING:20260101 \
  --parameter=DS_END_DATE:STRING:20260107 \
  "$(sed -e 's/YOUR_PROJECT_ID/my-project/g' -e 's/YOUR_DATASET_ID/otel/g' \
      dashboards/looker_studio/queries/executive_summary.sql)"
```
