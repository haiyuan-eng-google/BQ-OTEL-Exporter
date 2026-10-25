/*
 * Copyright 2026 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 *
 * Looker Studio data source ds0 - Page 1, Executive GenAI Overview and FinOps
 * Cost Control.
 *
 * Reads the v_gen_ai_model_costs companion view: one row per hour, service,
 * agent, provider and model. Deployments that materialize the view with the
 * Dataform package can point the FROM clause at m_gen_ai_model_costs_hourly,
 * which has the same columns (see deploy/dataform/README.md).
 *
 * YOUR_PROJECT_ID and YOUR_DATASET_ID are placeholders. Replace them with a
 * sandbox project and dataset when you build the template data source,
 * because Looker Studio checks a custom query when it is saved; the
 * looker_studio_create_url Terraform output then rewrites the sandbox names
 * for each deployment (see README.md in the parent directory).
 * @DS_START_DATE and @DS_END_DATE are the Looker Studio date range parameters
 * (STRING, YYYYMMDD, both ends inclusive); enable them on the data source with
 * "Enable date range parameters". The range predicate on usage_hour lets
 * BigQuery prune partitions, so the query is accepted by tables that set
 * require_partition_filter.
 *
 * top_error_types is flattened to text because Looker Studio cannot read
 * ARRAY or STRUCT columns. Latency percentiles are per hour and model; do not
 * average them across rows when you need an exact percentile.
 */
SELECT
  usage_hour,
  DATE(usage_hour) AS usage_date,
  service_name,
  gen_ai_agent_name,
  provider,
  model,
  call_count,
  error_count,
  typed_error_count,
  total_input_tokens,
  total_output_tokens,
  total_cache_read_input_tokens,
  total_cache_creation_input_tokens,
  total_reasoning_tokens,
  p50_duration_ms,
  p95_duration_ms,
  p99_duration_ms,
  NULLIF(
    ARRAY_TO_STRING(
      ARRAY(
        SELECT CONCAT(COALESCE(e.value, ''), ' (', CAST(e.count AS STRING), ')')
        FROM UNNEST(top_error_types) AS e
      ),
      ', '
    ),
    ''
  ) AS top_error_types
FROM `YOUR_PROJECT_ID.YOUR_DATASET_ID.v_gen_ai_model_costs`
WHERE usage_hour >= TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_START_DATE))
  AND usage_hour < TIMESTAMP(DATE_ADD(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 1 DAY))
