/*
 * Copyright 2026 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 *
 * Stateless per-span high-latency agent, model, and tool alert.
 * Reads newly appended rows from the base spans table via APPENDS and routes
 * spans whose duration exceeds max_turn_duration_ms to Cloud Pub/Sub.
 * Stateful window aggregations in Continuous Queries are Preview with strict
 * operator limits, so this template filters each span individually rather than
 * computing a rolling p95; see README.md in this directory for INSERT INTO and
 * TUMBLE window variants.
 *
 * Template variables:
 *   project_id           : Google Cloud project ID owning the dataset and Pub/Sub topic
 *   dataset_id           : BigQuery dataset ID
 *   spans_table          : Base spans table name (for example otel_spans)
 *   pubsub_topic         : Pub/Sub topic name (for example otel-otel-alerts)
 *   max_turn_duration_ms : Span duration threshold in milliseconds (for example 30000)
 */

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
    AND start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
);
