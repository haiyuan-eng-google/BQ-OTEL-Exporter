/*
 * Copyright 2026 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 *
 * Looker Studio data source ds1 - Page 2, Agent Sessions and Multi-Turn
 * Engagement.
 *
 * Reads the v_gen_ai_sessions companion view: one row per service, UTC start
 * date and session id. A session that crosses UTC midnight therefore appears
 * once per date it started activity on. The user identity column of the view
 * is user_id (the value of the adk.user.id attribute); count distinct user_id
 * for active users. Deployments that materialize the view with the Dataform
 * package can point the FROM clause at m_gen_ai_sessions_daily, which has the
 * same columns (see deploy/dataform/README.md).
 *
 * YOUR_PROJECT_ID and YOUR_DATASET_ID are placeholders. Replace them with a
 * sandbox project and dataset when you build the template data source,
 * because Looker Studio checks a custom query when it is saved; the
 * looker_studio_create_url Terraform output then rewrites the sandbox names
 * for each deployment (see README.md in the parent directory).
 * @DS_START_DATE and @DS_END_DATE are the Looker Studio date range parameters
 * (STRING, YYYYMMDD, both ends inclusive); enable them on the data source with
 * "Enable date range parameters". The predicate on session_start_date lets
 * BigQuery prune partitions, so the query is accepted by tables that set
 * require_partition_filter.
 *
 * top_error_types is flattened to text because Looker Studio cannot read
 * ARRAY or STRUCT columns.
 */
SELECT
  session_start_date,
  session_start_time,
  session_end_time,
  session_id,
  user_id,
  service_name,
  agent_name,
  session_duration_ms,
  turn_count,
  invocation_count,
  llm_call_count,
  agent_invocation_count,
  tool_call_count,
  error_count,
  typed_error_count,
  total_input_tokens,
  total_output_tokens,
  total_cache_read_input_tokens,
  total_cache_creation_input_tokens,
  total_reasoning_tokens,
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
FROM `YOUR_PROJECT_ID.YOUR_DATASET_ID.v_gen_ai_sessions`
WHERE session_start_date >= PARSE_DATE('%Y%m%d', @DS_START_DATE)
  AND session_start_date <= PARSE_DATE('%Y%m%d', @DS_END_DATE)
