/*
 * Copyright 2026 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 *
 * Looker Studio data source ds3 - Page 4, Multi-Agent Trajectory and Execution
 * DAG.
 *
 * Reads the v_gen_ai_agent_trajectory companion view: one row per GenAI span
 * of a (trace_id, service_name) trajectory, with the call depth (0 to 16), the
 * agent_path, the is_orphan boundary flag and the is_token_leaf token
 * integrity flag.
 *
 * Two deployment notes:
 *   - The view is not created when the exporter tables set
 *     require_partition_filter (Terraform allow_unprunable_views = false),
 *     because a time predicate cannot prune the recursive span read. In that
 *     case, and for any deployment where scanning every GenAI span is too
 *     expensive, point the FROM clause at the m_gen_ai_agent_trajectory
 *     Dataform mart, which has the same columns and a DATE(start_time)
 *     partition (see deploy/dataform/README.md).
 *   - Sum the leaf_*_tokens columns, not the raw gen_ai_usage_* columns, to
 *     count each token once: parent spans that report a roll-up of their
 *     children keep is_token_leaf = FALSE and contribute 0.
 *
 * agent_path is flattened to text ('root > planner > worker') because Looker
 * Studio cannot read ARRAY columns. status_message is not selected: with
 * enable_column_level_security the span column it comes from sits behind a
 * policy tag, and a viewer who holds neither reader role would get an access
 * denied error for the whole query.
 *
 * YOUR_PROJECT_ID and YOUR_DATASET_ID are placeholders. Replace them with a
 * sandbox project and dataset when you build the template data source,
 * because Looker Studio checks a custom query when it is saved; the
 * looker_studio_create_url Terraform output then rewrites the sandbox names
 * for each deployment (see README.md in the parent directory).
 * @DS_START_DATE and @DS_END_DATE are the Looker Studio date range parameters
 * (STRING, YYYYMMDD, both ends inclusive); enable them on the data source with
 * "Enable date range parameters".
 */
SELECT
  start_time,
  DATE(start_time) AS start_date,
  end_time,
  duration_ms,
  trace_id,
  span_id,
  parent_span_id,
  service_name,
  span_name,
  span_kind,
  status_code,
  gen_ai_operation_name,
  gen_ai_provider_name,
  gen_ai_request_model,
  gen_ai_agent_name,
  gen_ai_main_agent_name,
  gen_ai_tool_name,
  gen_ai_usage_input_tokens,
  gen_ai_usage_output_tokens,
  gen_ai_usage_reasoning_tokens,
  IF(is_token_leaf, COALESCE(gen_ai_usage_input_tokens, 0), 0) AS leaf_input_tokens,
  IF(is_token_leaf, COALESCE(gen_ai_usage_output_tokens, 0), 0) AS leaf_output_tokens,
  IF(is_token_leaf, COALESCE(gen_ai_usage_reasoning_tokens, 0), 0) AS leaf_reasoning_tokens,
  adk_session_id,
  adk_invocation_id,
  adk_user_id,
  error_type,
  depth,
  root_span_id,
  nearest_agent_name,
  ARRAY_TO_STRING(agent_path, ' > ') AS agent_path,
  ARRAY_LENGTH(agent_path) AS agent_path_length,
  is_orphan,
  step_index,
  is_leaf,
  is_token_leaf,
  CONCAT(
    'https://console.cloud.google.com/traces/explorer;traceId=',
    trace_id,
    '?project=YOUR_PROJECT_ID'
  ) AS trace_url
FROM `YOUR_PROJECT_ID.YOUR_DATASET_ID.v_gen_ai_agent_trajectory`
WHERE start_time >= TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_START_DATE))
  AND start_time < TIMESTAMP(DATE_ADD(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 1 DAY))
