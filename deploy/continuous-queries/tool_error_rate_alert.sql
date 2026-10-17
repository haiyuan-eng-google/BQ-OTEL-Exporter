/*
 * Copyright 2026 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 *
 * Stateless per-span tool and MCP error alert.
 * Reads newly appended rows from the base spans table via APPENDS and routes
 * tool or MCP error spans (status.code = 2 or error.type set) to Cloud Pub/Sub.
 * Stateful window aggregations in Continuous Queries are Preview with strict
 * operator limits, so this template filters each span individually; see
 * README.md in this directory for INSERT INTO and TUMBLE window variants.
 *
 * Template variables:
 *   project_id   : Google Cloud project ID owning the dataset and Pub/Sub topic
 *   dataset_id   : BigQuery dataset ID
 *   spans_table  : Base spans table name (for example otel_spans)
 *   pubsub_topic : Pub/Sub topic name (for example otel-alerts-otel)
 *
 * The start_time >= 1970-01-01 predicate satisfies require_partition_filter on
 * otel_spans without dropping late-arriving or quarantined-replay spans.
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
          JSON_VALUE(attributes, '$."gcp.vertex.agent.name"'),
          JSON_VALUE(attributes, '$."gen_ai.main_agent.name"')
        ) AS agent_name,
        JSON_VALUE(attributes, '$."gen_ai.tool.name"') AS tool_name,
        JSON_VALUE(attributes, '$."mcp.method.name"') AS mcp_method_name,
        JSON_VALUE(attributes, '$."error.type"') AS error_type,
        status.code AS status_code
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
      JSON_VALUE(attributes, '$."gen_ai.operation.name"') = 'execute_tool'
      OR JSON_VALUE(attributes, '$."gen_ai.tool.name"') IS NOT NULL
      OR JSON_VALUE(attributes, '$."mcp.method.name"') IS NOT NULL
    )
    AND start_time >= TIMESTAMP('1970-01-01 00:00:00+00')
);
