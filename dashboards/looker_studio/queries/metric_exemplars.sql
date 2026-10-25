/*
 * Copyright 2026 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 *
 * Looker Studio data source ds5 - Page 6, Metric Exemplar and Trace Deep-Dive.
 *
 * Reads the v_gen_ai_metric_exemplars companion view: one row per exemplar
 * attached to a GenAI metric point, joined to the span the exemplar points at
 * (trace_id and span_id). Use it to jump from a histogram or counter spike to
 * the trace that caused it. The view is not created when the exporter tables
 * set require_partition_filter (Terraform allow_unprunable_views = false);
 * there is no Dataform mart for this page, so it is unavailable in that mode.
 *
 * exemplar_value is the double value when the exemplar has one and the int64
 * value otherwise. exemplar_filtered_attributes is a JSON column; it is
 * flattened to text because Looker Studio cannot read JSON columns.
 * span_status_message is not selected: with enable_column_level_security the
 * span column it comes from sits behind a policy tag, and a viewer who holds
 * neither reader role would get an access denied error for the whole query.
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
  metric_timestamp,
  DATE(metric_timestamp) AS metric_date,
  service_name,
  metric_name,
  metric_unit,
  metric_type,
  gen_ai_operation_name,
  gen_ai_system,
  gen_ai_request_model,
  gen_ai_response_model,
  gen_ai_agent_name,
  gen_ai_token_type,
  gen_ai_token_modality,
  metric_error_type,
  exemplar_timestamp,
  COALESCE(exemplar_value_double, CAST(exemplar_value_int64 AS FLOAT64)) AS exemplar_value,
  trace_id,
  span_id,
  CONCAT(
    'https://console.cloud.google.com/traces/explorer;traceId=',
    trace_id,
    '?project=YOUR_PROJECT_ID'
  ) AS trace_url,
  TO_JSON_STRING(exemplar_filtered_attributes) AS exemplar_filtered_attributes,
  span_start_time,
  span_end_time,
  span_duration_ms,
  span_name,
  span_status_code,
  span_gen_ai_agent_name,
  span_gen_ai_tool_name,
  adk_session_id,
  span_error_type
FROM `YOUR_PROJECT_ID.YOUR_DATASET_ID.v_gen_ai_metric_exemplars`
WHERE metric_timestamp >= TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_START_DATE))
  AND metric_timestamp < TIMESTAMP(DATE_ADD(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 1 DAY))
