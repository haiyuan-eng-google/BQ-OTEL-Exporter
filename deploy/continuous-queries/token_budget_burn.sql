-- Copyright 2026 Google LLC
-- SPDX-License-Identifier: Apache-2.0
--
-- Real-time GenAI token budget circuit-breaker alert.
-- Reads newly appended rows from the base spans table via APPENDS and routes
-- spans exceeding single-turn input or output token thresholds to Cloud Pub/Sub.
--
-- Template variables:
--   project_id             : Google Cloud project ID owning the dataset and Pub/Sub topic
--   dataset_id             : BigQuery dataset ID
--   spans_table            : Base spans table name (for example otel_spans)
--   alerts_table           : Alert sink table name (for example otel_alerts)
--   pubsub_topic           : Pub/Sub topic name (for example otel-otel-alerts)
--   max_turn_input_tokens  : Single-turn input token threshold (for example 100000)
--   max_turn_output_tokens : Single-turn output token threshold (for example 32000)

EXPORT DATA
  OPTIONS (
    format = 'CLOUD_PUBSUB',
    uri = 'https://pubsub.googleapis.com/projects/${project_id}/topics/${pubsub_topic}'
  )
AS (
  SELECT
    TO_JSON_STRING(
      STRUCT(
        CONCAT('TOKEN_BUDGET_BURN:', trace_id, ':', span_id) AS alert_id,
        'TOKEN_BUDGET_BURN' AS alert_kind,
        start_time AS event_time,
        service_name,
        trace_id,
        span_id,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.conversation.id"'),
          JSON_VALUE(attributes, '$."session.id"'),
          JSON_VALUE(attributes, '$."gcp.vertex.agent.session_id"')
        ) AS session_id,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
          JSON_VALUE(attributes, '$."gen_ai.main_agent.name"')
        ) AS agent_name,
        JSON_VALUE(attributes, '$."gen_ai.request.model"') AS request_model,
        JSON_VALUE(attributes, '$."gen_ai.response.model"') AS response_model,
        LAX_INT64(attributes['gen_ai.usage.input_tokens']) AS input_tokens,
        LAX_INT64(attributes['gen_ai.usage.output_tokens']) AS output_tokens,
        COALESCE(
          LAX_INT64(attributes['gen_ai.usage.reasoning.output_tokens']),
          LAX_INT64(attributes['gen_ai.usage.reasoning_tokens'])
        ) AS reasoning_tokens
      )
    ) AS message
  FROM
    APPENDS(
      TABLE `${project_id}.${dataset_id}.${spans_table}`,
      CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE
    )
  WHERE
    COALESCE(LAX_INT64(attributes['gen_ai.usage.input_tokens']), 0) > ${max_turn_input_tokens}
    OR COALESCE(LAX_INT64(attributes['gen_ai.usage.output_tokens']), 0) > ${max_turn_output_tokens}
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
--   CONCAT('TOKEN_BUDGET_BURN:', trace_id, ':', span_id) AS alert_id,
--   'TOKEN_BUDGET_BURN' AS alert_kind,
--   start_time AS event_time,
--   service_name,
--   trace_id,
--   span_id,
--   TO_JSON(
--     STRUCT(
--       COALESCE(
--         JSON_VALUE(attributes, '$."gen_ai.conversation.id"'),
--         JSON_VALUE(attributes, '$."session.id"'),
--         JSON_VALUE(attributes, '$."gcp.vertex.agent.session_id"')
--       ) AS session_id,
--       COALESCE(
--         JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
--         JSON_VALUE(attributes, '$."gen_ai.main_agent.name"')
--       ) AS agent_name,
--       JSON_VALUE(attributes, '$."gen_ai.request.model"') AS request_model,
--       JSON_VALUE(attributes, '$."gen_ai.response.model"') AS response_model,
--       LAX_INT64(attributes['gen_ai.usage.input_tokens']) AS input_tokens,
--       LAX_INT64(attributes['gen_ai.usage.output_tokens']) AS output_tokens,
--       COALESCE(
--         LAX_INT64(attributes['gen_ai.usage.reasoning.output_tokens']),
--         LAX_INT64(attributes['gen_ai.usage.reasoning_tokens'])
--       ) AS reasoning_tokens
--     )
--   ) AS payload
-- FROM
--   APPENDS(
--     TABLE `${project_id}.${dataset_id}.${spans_table}`,
--     CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE
--   )
-- WHERE
--   COALESCE(LAX_INT64(attributes['gen_ai.usage.input_tokens']), 0) > ${max_turn_input_tokens}
--   OR COALESCE(LAX_INT64(attributes['gen_ai.usage.output_tokens']), 0) > ${max_turn_output_tokens};
