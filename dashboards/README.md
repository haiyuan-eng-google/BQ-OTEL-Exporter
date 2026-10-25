# Dashboards

Ready-made dashboards over the data that the BigQuery exporter writes. The
BigQuery dashboards read the companion views that the Terraform module in
[`../deploy/terraform`](../deploy/terraform) creates, so deploy the module first
(or create the views some other way).

| Folder | Tool | Contents |
|:---|:---|:---|
| [`looker_studio/`](looker_studio/) | Looker Studio | A six-page report: executive overview and FinOps, sessions, tools and MCP, trajectory, alerts and metric exemplars. It is published once as a template, and Terraform renders a one-click URL that copies it for a deployment. See its [README](looker_studio/README.md). |
| [`grafana/`](grafana/) | Grafana | Two dashboards: GenAI, MCP and sessions over BigQuery, and the exporter's own metrics over Prometheus. Described below. |

The optional Dataform package in [`../deploy/dataform`](../deploy/dataform)
keeps incremental tables with the same columns as the views. Point a dashboard at
them when the views scan more data than you want to pay for; each README says
how.

## Grafana

| File | Dashboard | Data source | Reads |
|:---|:---|:---|:---|
| [`grafana/genai-and-mcp-overview.json`](grafana/genai-and-mcp-overview.json) | GenAI, MCP & Trajectory Analytics | Google BigQuery | `v_gen_ai_model_costs`, `v_gen_ai_sessions`, `v_mcp_tool_usage`, `otel_alerts` |
| [`grafana/collector-self-telemetry.json`](grafana/collector-self-telemetry.json) | Collector BigQuery Exporter Self-Telemetry | Prometheus | The `otelcol_exporter_bigquery_*` metrics that [`metadata.yaml`](../metadata.yaml) declares |

### Requirements

- Grafana 10.4 or later. The files are written in the dashboard JSON format of
  Grafana 10.4 (`schemaVersion` 39); other versions may need panel options
  adjusted.
- For the BigQuery dashboard, the
  [Google BigQuery data source plugin](https://grafana.com/grafana/plugins/grafana-bigquery-datasource/)
  (`grafana-bigquery-datasource`) and a data source that uses it:
  - authentication that can read the exporter dataset and run queries: grant
    `roles/bigquery.dataViewer` on the dataset and `roles/bigquery.jobUser` on the
    project that runs the queries;
  - a default project to run queries in;
  - a processing location equal to the location of the exporter dataset, if the
    data source asks for one.
- For the `Alerts` row, the `otel_alerts` table, which Terraform creates when
  `enable_continuous_queries` is `true`.
- For the self-telemetry dashboard, a Prometheus data source that holds the
  collector's own metrics. Scrape the collector's internal telemetry endpoint, as
  its documentation describes, or get the metrics into Prometheus in any other
  way.

### Import

1. In Grafana choose **Dashboards**, **New**, **Import**, and upload the JSON
   file.
2. Pick the data source. For the BigQuery dashboard also enter `project` and
   `dataset`: the project and the dataset that hold the exporter views.
3. Choose **Import**.

The files declare their inputs in `__inputs`, so import them with the Grafana
import form or with `POST /api/dashboards/import`, passing the inputs
`DS_BIGQUERY` (or `DS_PROMETHEUS`), `VAR_PROJECT` and `VAR_DATASET`. A folder that
Grafana provisions from files does not substitute inputs: replace
`${DS_BIGQUERY}`, `${VAR_PROJECT}` and `${VAR_DATASET}` in the file first.

### GenAI, MCP & Trajectory Analytics

| Row | Reads | Panels |
|:---|:---|:---|
| Cost and usage | `v_gen_ai_model_costs` | LLM calls, error rate, token totals by kind; token burn, calls and errors, error rate and worst p95 latency by hour; tokens by model; top error types; a model ledger with provider, model, calls, errors, every token kind and the worst p50, p95 and p99 |
| Sessions | `v_gen_ai_sessions` | Sessions and active users (`user_id`), turns and duration per session, sessions and users by day, turns per session, invocations, LLM calls and tool calls per session by day, and a ledger of sessions with errors |
| Tools and MCP | `v_mcp_tool_usage` | MCP calls, error rate and the share that is also tagged as a GenAI tool call; calls by JSON-RPC method; worst p95 latency by server; a server and method ledger; top error types |
| Alerts | `otel_alerts` | Alert count, volume by hour and kind, and an alert log whose `trace_id` opens Cloud Trace Explorer |

The dashboard has three variables:

- `service` filters every panel. It lists the services that have GenAI or MCP
  activity in the time range, and **All** selects every listed service. A service
  name that contains a quote, a backslash, a backtick or a control character is
  not listed (Grafana pastes the selected names into the SQL), so panels left on
  **All** leave that service out.
- `project` and `dataset` are constants that hold the values entered at import.

How the queries are written:

- Every query puts a time predicate on the view that BigQuery can use to skip
  partitions: `$__timeFilter(usage_hour)` on the hourly views,
  `$__timeFilter(event_time)` on `otel_alerts`, and a range over
  `session_start_date` on the sessions view. The dashboard therefore works on
  tables that set `require_partition_filter`.
- The sessions panels read whole UTC dates, so a range that starts mid-day still
  counts that day's sessions in full.
- A session that crosses midnight UTC has a row on each date. The stat panels
  count it once and the by-day panels show it on both dates.
- `user_id` and `agent_name` of a session are one arbitrary value of the
  session's spans and can be empty.
- The latency columns are the worst hourly percentile in the range. Percentiles
  cannot be averaged or summed, so the dashboard shows the worst value instead of
  a combined one.
- The views keep the top five error types per hour, so the counts in the error
  type tables are lower bounds.
- Alerts are delivered at least once, so the alert panels count distinct
  `alert_id` values.
- The views hold token counts and no prices, so the dashboard shows tokens, not
  currency.

The title says Trajectory because the Sessions row shows how many agent
invocations, LLM calls and tool calls each session makes. The span-level
execution tree is page 4 of the Looker Studio report. It needs
`v_gen_ai_agent_trajectory`, which BigQuery cannot query when the spans table
requires a partition filter, and so the Grafana dashboard leaves it out.

### Collector BigQuery Exporter Self-Telemetry

The dashboard has three rows: delivery (acknowledged, rejected, quarantined rows
and redacted values), append latency and flow control, and retries, uncertainty
and offload. The `instance` variable filters on the Prometheus `instance` label,
and **All** matches every instance.

| Metric, without the `otelcol_exporter_bigquery_` prefix | Panels |
|:---|:---|
| `acknowledged_rows` | Acknowledged rows per second |
| `rejected_rows` | Rejected rows per second, by `reason` |
| `quarantined_rows`, `quarantine_failed_rows` | Quarantined rows per second, by `reason`, and quarantine writes that failed |
| `redacted_values` | Redacted values per second, by `signal` and `match_type` |
| `append_result_wait` | p50, p95 and p99 of the wait for an append result |
| `inflight_requests`, `unresolved_results` | Requests in flight and results not yet resolved |
| `backpressure_events` | Events per second, by `trigger` (`latency`, `recovery` or `throttle`) |
| `backpressure_level` | Current level, by `signal` and `table` |
| `effective_request_bytes` | Current request size limit, by `signal` and `table` |
| `append_bytes` | Appended bytes per second |
| `retries` | Retries per second, by `class` |
| `timeouts_after_dispatch`, `uncertain_ack_replays`, `stream_retirements` | Timeouts, uncertain acknowledgements replayed and retired streams |
| `offloaded_payloads`, `offloaded_bytes`, `offload_dedup_hits`, `offload_errors` | Offload volume, deduplicated payloads and errors by `class` |

Prometheus can add suffixes to the names that `metadata.yaml` declares: `_total`
on counters, a unit suffix such as `_bytes` or `_seconds`, `_ratio` on a gauge
whose unit is `1`, and `_bucket`, `_sum` and `_count` on a histogram. The queries
match each name with and without those suffixes, for example
`{__name__=~"otelcol_exporter_bigquery_acknowledged_rows(_total)?"}`. If you
rename metrics further (relabeling, or a prefix added on remote write), edit the
selectors.

Delivery is at-least-once (see the [main README](../README.md)). A retry can
acknowledge a row more than once, so the acknowledged-rows panels measure
throughput, not the number of distinct rows in BigQuery.

### Pointing the BigQuery dashboard at the Dataform marts

The marts have the same columns as the views, in a dataset of their own. In the
dashboard's JSON model, replace `${dataset}.v_gen_ai_model_costs` by
`<marts dataset>.m_gen_ai_model_costs_hourly`, and likewise for the sessions and
MCP views. The mapping is in the
[Dataform README](../deploy/dataform/README.md#pointing-the-dashboards-at-the-marts).
`otel_alerts` has no mart.

### Troubleshooting

| Symptom | Cause and fix |
|:---|:---|
| `Not found: Dataset ... was not found in location ...` | The processing location of the data source differs from the location of the dataset |
| `Access Denied` | The data source identity lacks `roles/bigquery.dataViewer` on the dataset or `roles/bigquery.jobUser` on the project that runs the queries |
| The `Alerts` row fails with `Not found: Table ... otel_alerts` | `enable_continuous_queries` is `false`. Enable it, or delete the row |
| Every BigQuery panel is empty and `service` lists nothing | No GenAI or MCP spans in the time range, or the views do not exist yet |
| A line chart reports that the data has no time field | The query's **Format** is wrong. Open the panel's query and set **Format** to **Time series** for line charts and to **Table** for the other panels |
| A Prometheus panel is empty | The collector does not expose the metric under the expected name; look the name up in Prometheus and adjust the selector |
