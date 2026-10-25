/*
 * Copyright 2026 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 *
 * Looker Studio data source ds2 - Page 3, Tool and MCP Server Observability.
 *
 * One query over two companion views, distinguished by the source column:
 *   source = 'mcp'    v_mcp_tool_usage     one row per hour, service, MCP
 *                                          method, target and server
 *   source = 'gen_ai' v_gen_ai_tool_usage  one row per hour, service, agent
 *                                          and tool
 * A tools/call span that carries both the MCP and the GenAI attributes is
 * counted in both views. gen_ai_tagged_call_count is the part of an MCP row's
 * call_count that is also counted in v_gen_ai_tool_usage, and
 * mcp_only_call_count is the remainder. Never add call_count across the two
 * sources: filter on source, or chart them side by side.
 *
 * Columns that exist in only one view are NULL for the other source. The
 * Dataform package materializes v_mcp_tool_usage as m_mcp_tool_usage_hourly
 * with the same columns (see deploy/dataform/README.md).
 *
 * The views' error_rate column is not selected: the ratio of one row cannot be
 * summed or averaged over a date range, so report_spec.yaml defines error_rate
 * as the calculated field SUM(error_count) / SUM(call_count).
 *
 * YOUR_PROJECT_ID and YOUR_DATASET_ID are placeholders. Replace them with a
 * sandbox project and dataset when you build the template data source,
 * because Looker Studio checks a custom query when it is saved; the
 * looker_studio_create_url Terraform output then rewrites the sandbox names
 * for each deployment (see README.md in the parent directory).
 * @DS_START_DATE and @DS_END_DATE are the Looker Studio date range parameters
 * (STRING, YYYYMMDD, both ends inclusive); enable them on the data source with
 * "Enable date range parameters". The range predicates on usage_hour let
 * BigQuery prune partitions, so the query is accepted by tables that set
 * require_partition_filter.
 *
 * top_error_types is flattened to text because Looker Studio cannot read
 * ARRAY or STRUCT columns.
 */
SELECT
  'mcp' AS source,
  usage_hour,
  DATE(usage_hour) AS usage_date,
  service_name,
  mcp_method_name,
  mcp_protocol_version,
  mcp_target_name,
  gen_ai_tool_name,
  CAST(NULL AS STRING) AS gen_ai_agent_name,
  CAST(NULL AS STRING) AS gen_ai_tool_type,
  server_address,
  span_kind,
  call_count,
  gen_ai_tagged_call_count,
  call_count - COALESCE(gen_ai_tagged_call_count, 0) AS mcp_only_call_count,
  error_count,
  typed_error_count,
  avg_duration_ms,
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
FROM `YOUR_PROJECT_ID.YOUR_DATASET_ID.v_mcp_tool_usage`
WHERE usage_hour >= TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_START_DATE))
  AND usage_hour < TIMESTAMP(DATE_ADD(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 1 DAY))

UNION ALL

SELECT
  'gen_ai' AS source,
  usage_hour,
  DATE(usage_hour) AS usage_date,
  service_name,
  CAST(NULL AS STRING) AS mcp_method_name,
  CAST(NULL AS STRING) AS mcp_protocol_version,
  CAST(NULL AS STRING) AS mcp_target_name,
  gen_ai_tool_name,
  gen_ai_agent_name,
  gen_ai_tool_type,
  CAST(NULL AS STRING) AS server_address,
  CAST(NULL AS INT64) AS span_kind,
  call_count,
  CAST(NULL AS INT64) AS gen_ai_tagged_call_count,
  CAST(NULL AS INT64) AS mcp_only_call_count,
  error_count,
  typed_error_count,
  CAST(NULL AS FLOAT64) AS avg_duration_ms,
  p50_duration_ms,
  p95_duration_ms,
  CAST(NULL AS FLOAT64) AS p99_duration_ms,
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
FROM `YOUR_PROJECT_ID.YOUR_DATASET_ID.v_gen_ai_tool_usage`
WHERE usage_hour >= TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_START_DATE))
  AND usage_hour < TIMESTAMP(DATE_ADD(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 1 DAY))
