/*
 * Copyright 2026 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 *
 * Looker Studio data source ds4 - Page 5, Real-Time Security and SLA Alerting.
 *
 * Reads the otel_alerts sink table written by the Continuous Queries alert
 * templates in deploy/continuous-queries (TOOL_ERROR, TOKEN_BUDGET_BURN,
 * AGENT_LATENCY_HIGH and the optional windowed TOOL_ERROR_WINDOW). The table
 * exists only when the Terraform variable enable_continuous_queries is set.
 *
 * Alerts are delivered at least once, so the query keeps one row per alert_id.
 * The time predicate is applied in the same SELECT block, before the
 * QUALIFY ROW_NUMBER() de-duplication, so it prunes event_time partitions
 * (the table can set require_partition_filter).
 *
 * The payload JSON is flattened into typed columns; a column is NULL for the
 * alert kinds whose payload does not carry the key. Windowed alerts have an
 * empty trace_id and span_id, so trace_id and trace_url are NULL for them.
 * trace_url opens the trace in Cloud Trace Explorer when the same traces are
 * also exported to Cloud Trace; replace the link prefix for another tracing
 * UI.
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
  alert_id,
  alert_kind,
  event_time,
  DATE(event_time) AS event_date,
  service_name,
  NULLIF(trace_id, '') AS trace_id,
  NULLIF(span_id, '') AS span_id,
  IF(
    trace_id != '',
    CONCAT(
      'https://console.cloud.google.com/traces/explorer;traceId=',
      trace_id,
      '?project=YOUR_PROJECT_ID'
    ),
    CAST(NULL AS STRING)
  ) AS trace_url,
  JSON_VALUE(payload, '$.span_name') AS span_name,
  JSON_VALUE(payload, '$.operation_name') AS operation_name,
  JSON_VALUE(payload, '$.agent_name') AS agent_name,
  JSON_VALUE(payload, '$.tool_name') AS tool_name,
  JSON_VALUE(payload, '$.mcp_method_name') AS mcp_method_name,
  JSON_VALUE(payload, '$.error_type') AS error_type,
  SAFE_CAST(JSON_VALUE(payload, '$.status_code') AS INT64) AS status_code,
  JSON_VALUE(payload, '$.session_id') AS session_id,
  JSON_VALUE(payload, '$.request_model') AS request_model,
  JSON_VALUE(payload, '$.response_model') AS response_model,
  SAFE_CAST(JSON_VALUE(payload, '$.input_tokens') AS INT64) AS input_tokens,
  SAFE_CAST(JSON_VALUE(payload, '$.output_tokens') AS INT64) AS output_tokens,
  SAFE_CAST(JSON_VALUE(payload, '$.reasoning_tokens') AS INT64) AS reasoning_tokens,
  SAFE_CAST(JSON_VALUE(payload, '$.duration_ms') AS FLOAT64) AS duration_ms,
  SAFE_CAST(JSON_VALUE(payload, '$.error_spans') AS INT64) AS error_spans,
  TO_JSON_STRING(payload) AS payload_json
FROM `YOUR_PROJECT_ID.YOUR_DATASET_ID.otel_alerts`
WHERE event_time >= TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_START_DATE))
  AND event_time < TIMESTAMP(DATE_ADD(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 1 DAY))
QUALIFY ROW_NUMBER() OVER (PARTITION BY alert_id ORDER BY event_time ASC) = 1
