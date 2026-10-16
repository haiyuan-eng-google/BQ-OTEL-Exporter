-- Copyright 2026 Google LLC
-- SPDX-License-Identifier: Apache-2.0
--
-- Real-time high-latency agent, model, and tool span alert.
-- Reads newly appended rows from the base spans table via APPENDS and routes
-- spans whose duration exceeds max_turn_duration_ms to Cloud Pub/Sub.
--
-- Template variables:
--   project_id           : Google Cloud project ID owning the dataset and Pub/Sub topic
--   dataset_id           : BigQuery dataset ID
--   spans_table          : Base spans table name (for example otel_spans)
--   alerts_table         : Alert sink table name (for example otel_alerts)
--   pubsub_topic         : Pub/Sub topic name (for example otel-otel-alerts)
--   max_turn_duration_ms : Span duration threshold in milliseconds (for example 30000)

EXPORT DATA
  OPTIONS (
    format = 'CLOUD_PUBSUB',
    uri = 'https://pubsub.googleapis.com/projects/${project_id}/topics/${pubsub_topic}'
  )
AS (
  SELECT
    TO_JSON_STRING(
      STRUCT(
        CONCAT('AGENT_LATENCY_HIGH:', trace_id, ':', span_id) AS alert_id,
        'AGENT_LATENCY_HIGH' AS alert_kind,
        start_time AS event_time,
        service_name,
        trace_id,
        span_id,
        name AS span_name,
        ROUND(duration_nano / 1e6, 3) AS duration_ms,
        JSON_VALUE(attributes, '$."gen_ai.operation.name"') AS operation_name,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
          JSON_VALUE(attributes, '$."gen_ai.main_agent.name"')
        ) AS agent_name,
        JSON_VALUE(attributes, '$."gen_ai.request.model"') AS request_model,
        JSON_VALUE(attributes, '$."gen_ai.tool.name"') AS tool_name
      )
    ) AS message
  FROM
    APPENDS(
      TABLE `${project_id}.${dataset_id}.${spans_table}`,
      CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE
    )
  WHERE
    duration_nano / 1e6 > ${max_turn_duration_ms}
    AND JSON_VALUE(attributes, '$."gen_ai.operation.name"') IN (
      'invoke_agent',
      'chat',
      'generate_content',
      'execute_tool'
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
--   CONCAT('AGENT_LATENCY_HIGH:', trace_id, ':', span_id) AS alert_id,
--   'AGENT_LATENCY_HIGH' AS alert_kind,
--   start_time AS event_time,
--   service_name,
--   trace_id,
--   span_id,
--   TO_JSON(
--     STRUCT(
--       name AS span_name,
--       ROUND(duration_nano / 1e6, 3) AS duration_ms,
--       JSON_VALUE(attributes, '$."gen_ai.operation.name"') AS operation_name,
--       COALESCE(
--         JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
--         JSON_VALUE(attributes, '$."gen_ai.main_agent.name"')
--       ) AS agent_name,
--       JSON_VALUE(attributes, '$."gen_ai.request.model"') AS request_model,
--       JSON_VALUE(attributes, '$."gen_ai.tool.name"') AS tool_name
--     )
--   ) AS payload
-- FROM
--   APPENDS(
--     TABLE `${project_id}.${dataset_id}.${spans_table}`,
--     CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE
--   )
-- WHERE
--   duration_nano / 1e6 > ${max_turn_duration_ms}
--   AND JSON_VALUE(attributes, '$."gen_ai.operation.name"') IN (
--     'invoke_agent',
--     'chat',
--     'generate_content',
--     'execute_tool'
--   );
