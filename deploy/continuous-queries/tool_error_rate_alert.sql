-- Copyright 2026 Google LLC
-- SPDX-License-Identifier: Apache-2.0
--
-- Real-time GenAI, tool, and MCP error alert.
-- Reads newly appended rows from the base spans table via APPENDS and routes
-- error spans (status.code = 2 or error.type set) to Cloud Pub/Sub.
--
-- Template variables:
--   project_id   : Google Cloud project ID owning the dataset and Pub/Sub topic
--   dataset_id   : BigQuery dataset ID
--   spans_table  : Base spans table name (for example otel_spans)
--   alerts_table : Alert sink table name (for example otel_alerts)
--   pubsub_topic : Pub/Sub topic name (for example otel-otel-alerts)

EXPORT DATA
  OPTIONS (
    format = 'CLOUD_PUBSUB',
    uri = 'https://pubsub.googleapis.com/projects/${project_id}/topics/${pubsub_topic}'
  )
AS (
  SELECT
    TO_JSON_STRING(
      STRUCT(
        CONCAT('TOOL_ERROR:', trace_id, ':', span_id) AS alert_id,
        'TOOL_ERROR' AS alert_kind,
        start_time AS event_time,
        service_name,
        trace_id,
        span_id,
        name AS span_name,
        JSON_VALUE(attributes, '$."gen_ai.operation.name"') AS operation_name,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
          JSON_VALUE(attributes, '$."gen_ai.main_agent.name"')
        ) AS agent_name,
        JSON_VALUE(attributes, '$."gen_ai.tool.name"') AS tool_name,
        JSON_VALUE(attributes, '$."mcp.method.name"') AS mcp_method_name,
        JSON_VALUE(attributes, '$."error.type"') AS error_type,
        status.code AS status_code,
        status.message AS status_message
      )
    ) AS message
  FROM
    APPENDS(
      TABLE `${project_id}.${dataset_id}.${spans_table}`,
      CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE
    )
  WHERE
    (
      status.code = 2
      OR JSON_VALUE(attributes, '$."error.type"') IS NOT NULL
    )
    AND (
      JSON_VALUE(attributes, '$."gen_ai.operation.name"') IS NOT NULL
      OR JSON_VALUE(attributes, '$."gen_ai.tool.name"') IS NOT NULL
      OR JSON_VALUE(attributes, '$."mcp.method.name"') IS NOT NULL
    )
);

-- Table-sink variant (uncomment and comment out EXPORT DATA above to write
-- alerts into the BigQuery alerts table instead of Pub/Sub):
--
-- INSERT INTO `${project_id}.${dataset_id}.${alerts_table}` (
--   alert_id,
--   alert_kind,
--   event_time,
--   service_name,
--   trace_id,
--   span_id,
--   payload
-- )
-- SELECT
--   CONCAT('TOOL_ERROR:', trace_id, ':', span_id) AS alert_id,
--   'TOOL_ERROR' AS alert_kind,
--   start_time AS event_time,
--   service_name,
--   trace_id,
--   span_id,
--   TO_JSON(
--     STRUCT(
--       name AS span_name,
--       JSON_VALUE(attributes, '$."gen_ai.operation.name"') AS operation_name,
--       COALESCE(
--         JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
--         JSON_VALUE(attributes, '$."gen_ai.main_agent.name"')
--       ) AS agent_name,
--       JSON_VALUE(attributes, '$."gen_ai.tool.name"') AS tool_name,
--       JSON_VALUE(attributes, '$."mcp.method.name"') AS mcp_method_name,
--       JSON_VALUE(attributes, '$."error.type"') AS error_type,
--       status.code AS status_code,
--       status.message AS status_message
--     )
--   ) AS payload
-- FROM
--   APPENDS(
--     TABLE `${project_id}.${dataset_id}.${spans_table}`,
--     CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE
--   )
-- WHERE
--   (
--     status.code = 2
--     OR JSON_VALUE(attributes, '$."error.type"') IS NOT NULL
--   )
--   AND (
--     JSON_VALUE(attributes, '$."gen_ai.operation.name"') IS NOT NULL
--     OR JSON_VALUE(attributes, '$."gen_ai.tool.name"') IS NOT NULL
--     OR JSON_VALUE(attributes, '$."mcp.method.name"') IS NOT NULL
--   );
