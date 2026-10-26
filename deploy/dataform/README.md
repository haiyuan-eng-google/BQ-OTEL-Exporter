# Dataform marts for the dashboards

Incremental BigQuery tables that sit on top of the exporter's companion views
(`v_gen_ai_model_costs`, `v_gen_ai_sessions`, `v_mcp_tool_usage`,
`v_gen_ai_agent_trajectory`). A view recomputes its result from the raw spans
every time a dashboard loads. A mart computes it once per run into a table that
is partitioned by day and clustered by `service_name` and its main dimension,
so a dashboard query reads a small, pruned table instead.

The marts are optional. The dashboards in [`../../dashboards`](../../dashboards)
read the views by default and switch to a mart by changing one table name (see
[Pointing the dashboards at the marts](#pointing-the-dashboards-at-the-marts)).
On an exporter dataset that requires a partition filter, the trajectory mart is
the only way to get the trajectory (see
[Datasets that require a partition filter](#datasets-that-require-a-partition-filter)).

## What is in the package

All actions except the declarations carry the tag `otel_marts`.

| Action | Type | Reads | Partition by | Cluster by |
|:---|:---|:---|:---|:---|
| `v_gen_ai_spans`, `v_gen_ai_model_costs`, `v_gen_ai_sessions`, `v_mcp_tool_usage` | declaration | the exporter views (not written) | | |
| `stg_run_window` | table | | | |
| `stg_spans_dedup` | incremental | `stg_run_window`, `v_gen_ai_spans` | `DATE(start_time)` | `service_name`, `trace_id` |
| `m_gen_ai_model_costs_hourly` | incremental | `stg_run_window`, `v_gen_ai_model_costs` | `DATE(usage_hour)` | `service_name`, `model` |
| `m_mcp_tool_usage_hourly` | incremental | `stg_run_window`, `v_mcp_tool_usage` | `DATE(usage_hour)` | `service_name`, `mcp_method_name` |
| `m_gen_ai_sessions_daily` | incremental | `stg_run_window`, `v_gen_ai_sessions` | `session_start_date` | `service_name`, `agent_name` |
| `m_gen_ai_agent_trajectory` | incremental | `stg_run_window`, `stg_spans_dedup` | `DATE(start_time)` | `service_name`, `trace_id` |
| `assert_no_orphan_tokens` | assertion | `stg_run_window`, `stg_spans_dedup`, `m_gen_ai_agent_trajectory` | | |
| `assert_no_unhandled_cycles` | assertion | `stg_run_window`, `stg_spans_dedup`, `m_gen_ai_agent_trajectory` | | |

Every mart has the columns of its view, in the view's order, so a dashboard
query can read either. The one exception is `status_message` of the trajectory
(see [Access and column-level security](#access-and-column-level-security)).
The definitions are in `definitions/`: `sources/` (the declarations),
`staging/`, `marts/` and `assertions/`.

`stg_spans_dedup` holds one canonical row per GenAI span (the typed columns
only, never the attribute, event or link bags). The trajectory mart reads it
instead of re-reading the raw spans through the view chain on every run.

## Set up

1. Create the exporter tables and companion views first (`deploy/terraform`, or
   the collector's `auto_create` option). The marts read the four views above.
2. Install the Dataform CLI (`npm install -g @dataform/cli`) or use a managed
   Dataform repository.
3. Replace the placeholders in [`workflow_settings.yaml`](workflow_settings.yaml):

   | Setting | Meaning |
   |:---|:---|
   | `defaultProject` | Project the marts are written to |
   | `defaultLocation` | Location of the marts datasets. It must equal the location of the exporter dataset: a query cannot read across locations |
   | `defaultDataset`, `defaultAssertionDataset` | Datasets for the marts and the assertion views. Dataform creates them when they do not exist |
   | `vars.source_project`, `vars.source_dataset` | `project_id` and `dataset_id` of the exporter. Only read |
   | `vars.run_timestamp` | Optional ISO-8601 UTC timestamp override for `stg_run_window` (empty uses the run-start `CURRENT_TIMESTAMP()`; an invalid non-empty value fails the `CAST`) |

   Always set `defaultProject` and `defaultLocation` in
   [`workflow_settings.yaml`](workflow_settings.yaml) before running (`--vars`
   only overrides entries under `vars:`, so leaving `defaultProject` at
   `YOUR_PROJECT_ID` would still target `YOUR_PROJECT_ID` for the destination
   tables and assertion views). Once `defaultProject` and `defaultLocation` are
   set, you can either edit `vars.source_project` and `vars.source_dataset` in
   the file or pass them on the command line:
   `dataform run --vars=source_project=my-project,source_dataset=otel`.

   ```yaml
   defaultProject: my-project
   defaultLocation: US
   defaultDataset: otel_marts
   defaultAssertionDataset: otel_marts_assertions

   vars:
     source_project: my-project
     source_dataset: otel
     run_timestamp: ""
   ```
4. Create `.df-credentials.json` with `dataform init-creds bigquery` and keep it
   out of version control (`.gitignore` in this directory lists it).
5. Grant the identity that runs Dataform (the CLI user, or the service account of
   the managed workflow) `roles/bigquery.jobUser` on the project,
   `roles/bigquery.dataViewer` on the exporter dataset and
   `roles/bigquery.dataEditor` on the marts and assertion datasets. Creating
   the datasets also needs `bigquery.datasets.create` (for example
   `roles/bigquery.user`); create them first to avoid that.
6. Check that everything compiles: `dataform compile`.

## Run

```bash
# First run, then every hour. The first run builds every table from all history.
dataform run --tags otel_marts

# Rebuild everything from the views (see "Full refresh and retention").
dataform run --tags otel_marts --full-refresh
```

For an hourly schedule, run the same command from cron, Cloud Scheduler or a
managed Dataform workflow configuration that selects the tag `otel_marts`:
`0 * * * * cd /path/to/deploy/dataform && dataform run --tags otel_marts`.
The tag runs the staging table first, then the marts, then the assertions.
Run it as one workflow: do not start a second run while one is in progress, and
do not run the staging table and the marts as separate workflows.

With a managed Dataform repository, use this directory as the repository root,
set the compilation variables `source_project` and `source_dataset` in the
release configuration and select the tag `otel_marts` in the workflow
configuration.

## Refresh model and freshness

Every incremental run recomputes a rolling window and replaces the rows of that
window in one transaction (`DELETE` then `INSERT`). Let `T` be the start time of
a run. Then

```
window_start = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR), HOUR)
```

so `T - 4h < window_start <= T - 3h`: the lookback `L` is 3 to 4 hours.
`stg_run_window` captures `T` and `window_start` once at the start of the
invocation (or from `vars.run_timestamp` when set), and every incremental
action and assertion reads `stg_run_window.window_start` so the whole DAG
shares a single cutoff even if execution crosses an hour boundary. A first
run or a full refresh uses the Unix epoch as `window_start` and rebuilds
everything. A rerun gives the same rows, and each individual table action is
atomic (`BEGIN TRANSACTION` ... `DELETE` ... `INSERT` ... `COMMIT TRANSACTION`):
if a table action fails, its own transaction rolls back and leaves that table
as it was, while upstream table actions that already committed keep their
refreshed rows until the next run.

| Mart | What one run recomputes | Row is exact when |
|:---|:---|:---|
| `stg_spans_dedup` | spans with `start_time >= window_start` | the span reached the exporter table before the run |
| `m_gen_ai_model_costs_hourly`, `m_mcp_tool_usage_hourly` | hours with `usage_hour >= window_start` | all spans of the hour reached the exporter table before the run |
| `m_gen_ai_sessions_daily` | UTC dates with `session_start_date >= DATE(window_start)` | all spans of the date reached the exporter table before the run |
| `m_gen_ai_agent_trajectory` | every trace with a span at or after `window_start`, over the spans at or after `window_start - 24h` | the trace has no span before `window_start - 24h` (see below) |

### When to run

**Run delay after the window end.** A row is final only after the ingestion
watermark has passed the end of its window: the point after which no span that
starts in the window can still reach the table. A span reaches the table later
than its start by its own duration, the export delay of the collector (batching
and retries) and the `max_staleness` of the table, if you set one. Rows of the
latest hours are therefore provisional, and each later run inside the lookback
replaces them with the then-current result. Do not read the latest 3 to 4 hours
as final.

Hour `H` is rewritten by every run with `T < H + 4h`, that is, until 3 hours
after the hour ends. With runs every `g` hours, a span is reflected in its hour
whenever it is ingested less than `3h - g` after the end of its hour: 2 hours
for hourly runs. A span ingested later is not reflected until the next full
refresh. Choose `g` and the lookback so that the ingestion delay of your deployment
plus `g` stays below 3 hours. To widen the lookback, change `INTERVAL 3 HOUR`
in `stg_run_window` (which `stg_spans_dedup`, the four marts and the two
assertions read); a conformance test fails when they differ.

Run at least every 3 hours: the hours between the window of one run and the
window of the next run are not recomputed, so a gap longer than the lookback
leaves them with rows computed before the gap. Running on an hourly schedule
prevents future lookback gaps; to repair an already-expired gap longer than the
lookback window, run a targeted backfill (`--vars=run_timestamp=...` or a
temporarily widened lookback) or `dataform run --tags otel_marts --full-refresh`.

### Lookback completeness bound

A recomputed row is complete only when everything it depends on lies inside the
range the run reads. For the hourly marts this is the hour itself and for the
session mart the UTC date. The trajectory is different, because the view
connects a span to a parent up to 24 hours before it and to children up to 24
hours after it, and numbers `step_index` over the whole trace. A trace is exact
when the first event of the trace is at or after `window_start - 24h`, that is,
when every one of its spans starts in the range the run reads. Spans older than
that are neither read nor rewritten, so these are corrected only by a full
refresh:

- a parent that arrives after its trace has left the window,
- a child that starts more than 24 hours after its parent,
- the `step_index`, `depth` and `agent_path` of a trace whose first span is
  older than `window_start - 24h`.

Spans that reach the table after their trace has left the window are likewise
not picked up until a full refresh.

### What the refresh does not promise

The refresh is an idempotent recomputation of a window. It is not
exactly-once processing: the exporter delivers spans at least once, the views
deduplicate them, and the marts recompute from the views. Nothing tracks which
spans a previous run has seen. The sessions mart is the view's result for a
UTC date and adds no pre-aggregation of its own, so a session that spans
midnight has one row per date, as in the view.

## Parity with the views

For a closed range that is older than the lookback and the ingestion delay, a
mart equals its view row for row, with two documented exceptions:

- `m_gen_ai_sessions_daily.agent_name` and `.user_id` are `ANY_VALUE` picks of
  the view. They are arbitrary when a session reports more than one value, and
  the view itself can return a different value on two runs.
- `m_gen_ai_agent_trajectory` has no `status_message`.

Compare a mart with its view over a closed range like this (replace the
project, datasets and the range):

```sql
SELECT COUNT(*) AS rows_only_in_mart
FROM (
  SELECT TO_JSON_STRING(m) AS row_json
  FROM `my-project.otel_marts.m_gen_ai_model_costs_hourly` AS m
  WHERE usage_hour >= TIMESTAMP '2026-01-01 00:00:00+00'
    AND usage_hour < TIMESTAMP '2026-01-02 00:00:00+00'
  EXCEPT DISTINCT
  SELECT TO_JSON_STRING(v)
  FROM `my-project.otel.v_gen_ai_model_costs` AS v
  WHERE usage_hour >= TIMESTAMP '2026-01-01 00:00:00+00'
    AND usage_hour < TIMESTAMP '2026-01-02 00:00:00+00'
);
```

Swap the two `SELECT`s to count the rows only in the view. Both counts are 0
when the mart is current for the range.

## Assertions

Both assertions are views in `defaultAssertionDataset`. They return the rows
that break an invariant, and a run fails when either returns a row. They read
the staging table and the trajectory mart over the rolling window, so they
check the run that just finished.

| Assertion | A row is returned when |
|:---|:---|
| `assert_no_orphan_tokens` | a span of the trajectory mart breaks a token invariant. The `violation` column names the rule: `projected_token_differs_from_raw` (a token count is neither `NULL` nor the staged count), `rollup_token_changed` (a span that is not a token leaf has an uncovered count or lost/changed a count, or a partial leaf nulled an uncovered count), `token_leaf_without_tokens` (a token leaf with no count left), `token_double_counted_with_child` (a token leaf whose counts are all covered by emitted descendants up to 32 hops, or a partial leaf that keeps a count an emitted descendant already covers) or `span_missing_from_trajectory` (a staged span that reports tokens is not in the mart) |
| `assert_no_unhandled_cycles` | a staged span appears in the trajectory mart zero times (`span_missing_from_trajectory`: it is in a parent cycle or deeper than 16 levels, which the trajectory does not emit) or more than once (`span_in_trajectory_more_than_once`) |

They check the invariants that make a sum of `gen_ai_usage_*_tokens` over the
rows with `is_token_leaf` count each token once, and that no span is silently
dropped from the trajectory.

Query an assertion view to see the offending rows:

```sql
SELECT * FROM `my-project.otel_marts_assertions.assert_no_orphan_tokens`;
```

## Datasets that require a partition filter

When the exporter tables set `require_partition_filter` (Terraform), the module
leaves out `v_gen_ai_logs`, `v_gen_ai_metric_exemplars` and
`v_gen_ai_agent_trajectory` unless `allow_unprunable_views` is set, because a
query over them cannot skip partitions. This package keeps working there:

- `stg_spans_dedup` and the three aggregate marts put a time predicate on the
  view that BigQuery can use to skip partitions, in the same `SELECT` as the
  deduplication window and before it.
- `m_gen_ai_agent_trajectory` reads `stg_spans_dedup`, an ordinary table of this
  package, not the raw spans. It is therefore the only source of the trajectory
  on such a dataset. The Looker Studio page for the metric exemplars has no
  mart and is unavailable there.

The marts datasets themselves are ordinary tables without a partition-filter
requirement.

## Access and column-level security

The marts are ordinary tables in another dataset. Grant read access to them as
you would on the exporter dataset: they hold the same typed GenAI columns,
including `adk_user_id` and `user_id`.

With `enable_column_level_security`, the span column behind `status_message`
sits behind a policy tag, and a table that Dataform creates carries no policy
tag. The package therefore never stages or materializes `status_message`: the
staging table and the trajectory mart leave it out, so running the package does
not copy that text out of the protection and does not need a reader role on the
policy tag. A dashboard query that selects it must read the view.

## Pointing the dashboards at the marts

Replace the view in the `FROM` clause of a dashboard query by the mart in the
marts dataset. The columns are the same, except `status_message`, which the
dashboard queries do not select:

| View | Mart |
|:---|:---|
| `v_gen_ai_model_costs` | `m_gen_ai_model_costs_hourly` |
| `v_gen_ai_sessions` | `m_gen_ai_sessions_daily` |
| `v_mcp_tool_usage` | `m_mcp_tool_usage_hourly` |
| `v_gen_ai_agent_trajectory` | `m_gen_ai_agent_trajectory` |

`v_gen_ai_tool_usage` and `v_gen_ai_metric_exemplars` have no mart.

## Full refresh and retention

`--full-refresh` replaces each incremental table from the views, which read
the raw spans. It scans all the history the raw tables still hold and rebuilds
only that: if the spans table expires partitions (`spans_partition_expiration_days`),
a full refresh drops the mart rows older than that. To keep long-term history
in a mart, add `protected: true` to its `config` block; Dataform then refuses
to full-refresh it. A change to a mart's columns, partitioning or clustering
also needs a full refresh, because an incremental run only inserts rows.

## Troubleshooting

| Symptom | Cause and fix |
|:---|:---|
| `Not found: Dataset ... was not found in location ...` | `defaultLocation` differs from the location of the exporter dataset |
| `Access Denied: ... bigquery.tables.getData` | The Dataform identity lacks `roles/bigquery.dataViewer` on the exporter dataset |
| `Transaction is aborted due to concurrent update` | Two runs overlapped. One of them is rolled back; rerun it after the other finishes |
| An assertion fails | Query the assertion view for the offending rows; a full refresh clears rows left behind by a completeness bound |
| A mart lags its view | The row is inside the ingestion delay, or its span arrived after its window left the lookback; see [Refresh model and freshness](#refresh-model-and-freshness) |

## Dataform core versions

`workflow_settings.yaml` pins `dataformCoreVersion: 3.1.2`, and the package has
been compiled and graph-validated with that version. Dataform core 3 rejects a project that has both
`workflow_settings.yaml` and `dataform.json`, so this directory has no
`dataform.json`. A project that must stay on Dataform core 2.x needs a
`dataform.json` with the same settings in place of `workflow_settings.yaml`
(`defaultDatabase`, `defaultLocation`, `defaultSchema`, `assertionSchema` and
`vars`); that path is not covered here.
