// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
	"strings"
)

// Names of the bounded-window Table-Valued Functions (TVFs) that provide
// partition-prunable alternatives to the three cross-signal and recursive
// companion views when require_partition_filter = true is enabled on the
// underlying otel_spans, otel_logs, or otel_metrics tables.
const (
	GenAILogsTVFName            = "tvf_gen_ai_logs"
	GenAIMetricExemplarsTVFName = "tvf_gen_ai_metric_exemplars"
	GenAIAgentTrajectoryTVFName = "tvf_gen_ai_agent_trajectory"
)

// GenAILogsTVFDDL creates tvf_gen_ai_logs(start_ts TIMESTAMP, end_ts TIMESTAMP):
// the bounded-window Table-Valued Function counterpart of v_gen_ai_logs.
//
// Unlike v_gen_ai_logs, which joins otel_spans_dedup on a per-row correlation
// window (s.start_time BETWEEN l.timestamp - 24h AND l.timestamp + 1h) that
// BigQuery's static partition-filter validator cannot prune when
// otel_spans.require_partition_filter = true, tvf_gen_ai_logs pushes static
// partition bounds into both otel_logs_dedup ([start_ts, end_ts]) and
// otel_spans_dedup ([start_ts - 24h, end_ts + 1h]) while preserving the exact
// per-row correlation window and output schema of v_gen_ai_logs.
const GenAILogsTVFDDL = `
CREATE OR REPLACE TABLE FUNCTION ` + "`%[1]s.%[2]s." + GenAILogsTVFName + "`" + `(start_ts TIMESTAMP, end_ts TIMESTAMP) AS (
` + GenAILogsTVFQuery + `
);
`

// GenAILogsTVFQuery is the SELECT body of tvf_gen_ai_logs. It takes
// (project, dataset, logs table, spans table) via fmt.Sprintf.
const GenAILogsTVFQuery = `
SELECT
  l.timestamp,
  l.receive_time,
  l.trace_id,
  l.span_id,
  l.service_name,
  COALESCE(NULLIF(l.event_name, ''), JSON_VALUE(l.attributes, '$."event.name"')) AS event_name,
  l.severity_number,
  l.severity_text,
  COALESCE(JSON_VALUE(l.attributes, '$."gen_ai.operation.name"'), JSON_VALUE(s.attributes, '$."gen_ai.operation.name"')) AS gen_ai_operation_name,
  COALESCE(
    JSON_VALUE(l.attributes, '$."gen_ai.system"'),
    JSON_VALUE(l.attributes, '$."gen_ai.provider.name"'),
    JSON_VALUE(s.attributes, '$."gen_ai.system"'),
    JSON_VALUE(s.attributes, '$."gen_ai.provider.name"')
  ) AS gen_ai_system,
  COALESCE(
    JSON_VALUE(l.attributes, '$."gen_ai.agent.name"'),
    JSON_VALUE(l.attributes, '$."gcp.vertex.agent.name"'),
    JSON_VALUE(s.attributes, '$."gen_ai.agent.name"'),
    JSON_VALUE(s.attributes, '$."gcp.vertex.agent.name"')
  ) AS gen_ai_agent_name,
  COALESCE(
    JSON_VALUE(l.attributes, '$."gcp.vertex.agent.session_id"'),
    JSON_VALUE(l.attributes, '$."gen_ai.conversation.id"'),
    JSON_VALUE(s.attributes, '$."gcp.vertex.agent.session_id"'),
    JSON_VALUE(s.attributes, '$."gen_ai.conversation.id"')
  ) AS adk_session_id,
  COALESCE(JSON_VALUE(l.attributes, '$."gcp.vertex.agent.invocation_id"'), JSON_VALUE(s.attributes, '$."gcp.vertex.agent.invocation_id"')) AS adk_invocation_id,
  COALESCE(JSON_VALUE(l.attributes, '$."gcp.vertex.agent.event_id"'), JSON_VALUE(s.attributes, '$."gcp.vertex.agent.event_id"')) AS adk_event_id,
  COALESCE(
    JSON_VALUE(l.attributes, '$."user.id"'),
    JSON_VALUE(l.attributes, '$."gen_ai.user.id"'),
    JSON_VALUE(s.attributes, '$."user.id"'),
    JSON_VALUE(s.attributes, '$."gen_ai.user.id"')
  ) AS adk_user_id,
  l.body,
  l.payload_refs,
  l.attributes,
  l.resource,
  l.instrumentation_scope,
  JSON_VALUE(l.attributes, '$."error.type"') AS error_type
FROM ` + "`%[1]s.%[2]s.%[3]s_dedup`" + ` l
LEFT JOIN (
  SELECT *
  FROM ` + "`%[1]s.%[2]s.%[4]s_dedup`" + `
  WHERE start_time >= TIMESTAMP_SUB(start_ts, INTERVAL 24 HOUR)
    AND start_time <= TIMESTAMP_ADD(end_ts, INTERVAL 1 HOUR)
) s
  ON l.trace_id = s.trace_id
  AND l.span_id = s.span_id
  AND l.service_name IS NOT DISTINCT FROM s.service_name
  AND s.start_time BETWEEN TIMESTAMP_SUB(l.timestamp, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(l.timestamp, INTERVAL 1 HOUR)
WHERE l.timestamp >= start_ts
  AND l.timestamp <= end_ts
  AND (
    COALESCE(NULLIF(l.event_name, ''), JSON_VALUE(l.attributes, '$."event.name"')) LIKE 'gen_ai.%%'
    OR JSON_VALUE(l.attributes, '$."gen_ai.system"') IS NOT NULL
    OR JSON_VALUE(l.attributes, '$."gen_ai.provider.name"') IS NOT NULL
    OR JSON_VALUE(l.attributes, '$."gcp.vertex.agent.invocation_id"') IS NOT NULL
  )
`

// GenAIMetricExemplarsTVFDDL creates
// tvf_gen_ai_metric_exemplars(start_ts TIMESTAMP, end_ts TIMESTAMP): the
// bounded-window Table-Valued Function counterpart of v_gen_ai_metric_exemplars.
//
// It pushes static partition bounds into both v_gen_ai_metrics ([start_ts, end_ts])
// and otel_spans_dedup ([start_ts - 24h, end_ts + 1h]) so queries succeed when
// require_partition_filter = true is enabled on otel_metrics and otel_spans.
const GenAIMetricExemplarsTVFDDL = `
CREATE OR REPLACE TABLE FUNCTION ` + "`%[1]s.%[2]s." + GenAIMetricExemplarsTVFName + "`" + `(start_ts TIMESTAMP, end_ts TIMESTAMP) AS (
` + GenAIMetricExemplarsTVFQuery + `
);
`

// GenAIMetricExemplarsTVFQuery is the SELECT body of tvf_gen_ai_metric_exemplars.
// It takes (project, dataset, spans table) via fmt.Sprintf.
const GenAIMetricExemplarsTVFQuery = `
SELECT
  m.timestamp AS metric_timestamp,
  m.service_name,
  m.metric_name,
  m.metric_unit,
  m.metric_type,
  m.gen_ai_operation_name,
  m.gen_ai_system,
  m.gen_ai_request_model,
  m.gen_ai_response_model,
  m.gen_ai_agent_name,
  m.gen_ai_token_type,
  m.gen_ai_token_modality,
  m.error_type AS metric_error_type,
  ex.time AS exemplar_timestamp,
  ex.value_double AS exemplar_value_double,
  ex.value_int64 AS exemplar_value_int64,
  ex.trace_id AS trace_id,
  ex.span_id AS span_id,
  ex.filtered_attributes AS exemplar_filtered_attributes,
  s.start_time AS span_start_time,
  s.end_time AS span_end_time,
  s.duration_nano AS span_duration_nano,
  ROUND(s.duration_nano / 1e6, 3) AS span_duration_ms,
  s.name AS span_name,
  s.status.code AS span_status_code,
  s.status.message AS span_status_message,
  COALESCE(
    JSON_VALUE(s.attributes, '$."gen_ai.agent.name"'),
    JSON_VALUE(s.attributes, '$."gcp.vertex.agent.name"'),
    JSON_VALUE(s.resource.attributes, '$."gen_ai.agent.name"'),
    JSON_VALUE(s.attributes, '$."gen_ai.main_agent.name"'),
    JSON_VALUE(s.resource.attributes, '$."gen_ai.main_agent.name"')
  ) AS span_gen_ai_agent_name,
  JSON_VALUE(s.attributes, '$."gen_ai.tool.name"') AS span_gen_ai_tool_name,
  COALESCE(
    JSON_VALUE(s.attributes, '$."gcp.vertex.agent.session_id"'),
    JSON_VALUE(s.attributes, '$."gen_ai.conversation.id"')
  ) AS adk_session_id,
  JSON_VALUE(s.attributes, '$."error.type"') AS span_error_type
FROM ` + "`%[1]s.%[2]s." + GenAIMetricsViewName + "`" + ` m,
UNNEST(m.exemplars) AS ex WITH OFFSET AS exemplar_ordinal
LEFT JOIN (
  SELECT *
  FROM ` + "`%[1]s.%[2]s.%[3]s_dedup`" + `
  WHERE start_time >= TIMESTAMP_SUB(start_ts, INTERVAL 24 HOUR)
    AND start_time <= TIMESTAMP_ADD(end_ts, INTERVAL 1 HOUR)
) s
  ON s.trace_id = ex.trace_id
  AND s.span_id = ex.span_id
  AND s.service_name IS NOT DISTINCT FROM m.service_name
  AND s.start_time BETWEEN TIMESTAMP_SUB(m.timestamp, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(m.timestamp, INTERVAL 1 HOUR)
WHERE m.timestamp >= start_ts
  AND m.timestamp <= end_ts
  AND ex.trace_id IS NOT NULL
  AND ex.span_id IS NOT NULL
  AND ex.trace_id != ''
  AND ex.span_id != ''
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY m.timestamp, m.time_unix_nano, m.service_name, m.metric_name, m.metric_type, m.point_fingerprint, exemplar_ordinal
  ORDER BY s.end_time DESC, s.start_time DESC, s.receive_time DESC, s.name ASC
) = 1
`

// GenAIAgentTrajectoryTVFDDL creates
// tvf_gen_ai_agent_trajectory(start_ts TIMESTAMP, end_ts TIMESTAMP): the
// bounded-window Table-Valued Function counterpart of v_gen_ai_agent_trajectory.
//
// Because v_gen_ai_agent_trajectory uses a recursive CTE (trajectory) and
// self-joins over its spans CTE, an outer WHERE filter on start_time cannot be
// pushed down by BigQuery into every branch of the recursion when
// otel_spans.require_partition_filter = true. Filtering start_time BETWEEN
// start_ts AND end_ts directly inside the spans CTE satisfies the partition
// filter on the single read of v_gen_ai_spans -> otel_spans_dedup -> otel_spans.
const GenAIAgentTrajectoryTVFDDL = `
CREATE OR REPLACE TABLE FUNCTION ` + "`%[1]s.%[2]s." + GenAIAgentTrajectoryTVFName + "`" + `(start_ts TIMESTAMP, end_ts TIMESTAMP) AS (
` + GenAIAgentTrajectoryTVFQuery + `
);
`

// GenAIAgentTrajectoryTVFQuery is the SELECT body of tvf_gen_ai_agent_trajectory.
// It takes (project, dataset) via fmt.Sprintf.
//
// BigQuery GoogleSQL does not permit WITH RECURSIVE inside CREATE TABLE FUNCTION,
// so this TVF resolves parent-child chains up to depth 16 using 5 logarithmic
// pointer-jumping (binary lifting) CTEs (hop0 -> hop1 -> hop2 -> hop4 -> hop8 -> hop16)
// over the unique (trace_id, service_name, span_id) spans within [start_ts, end_ts].
const GenAIAgentTrajectoryTVFQuery = `
WITH
  spans AS (
    SELECT
      start_time,
      end_time,
      duration_nano,
      duration_ms,
      trace_id,
      span_id,
      parent_span_id,
      service_name,
      span_name,
      span_kind,
      status_code,
      status_message,
      gen_ai_operation_name,
      gen_ai_system,
      gen_ai_provider_name,
      gen_ai_request_model,
      gen_ai_response_model,
      gen_ai_agent_id,
      gen_ai_agent_name,
      gen_ai_agent_version,
      gen_ai_main_agent_id,
      gen_ai_main_agent_name,
      gen_ai_tool_name,
      gen_ai_tool_call_id,
      gen_ai_usage_input_tokens,
      gen_ai_usage_output_tokens,
      gen_ai_usage_reasoning_tokens,
      adk_session_id,
      adk_invocation_id,
      adk_user_id,
      error_type,
      COALESCE(
        JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
        JSON_VALUE(attributes, '$."gcp.vertex.agent.name"')
      ) AS declared_agent_name,
      COALESCE(
        JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
        JSON_VALUE(attributes, '$."gcp.vertex.agent.name"'),
        IF(gen_ai_operation_name IN ('invoke_agent', 'create_agent'), gen_ai_agent_name, NULL)
      ) AS agent_path_name
    FROM ` + "`%[1]s.%[2]s." + GenAISpansViewName + "`" + `
    WHERE start_time >= start_ts
      AND start_time <= end_ts
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY trace_id, service_name, span_id
      ORDER BY start_time ASC, end_time DESC, span_name ASC, duration_nano DESC
    ) = 1
  ),
  parent_ids AS (
    SELECT DISTINCT
      trace_id,
      service_name,
      parent_span_id AS span_id
    FROM spans
    WHERE parent_span_id IS NOT NULL
      AND parent_span_id != ''
  ),
  hop0 AS (
    SELECT
      s.*,
      IF(
        s.parent_span_id IS NOT NULL
          AND s.parent_span_id != ''
          AND p.span_id IS NOT NULL
          AND p.span_id != s.span_id,
        p.span_id,
        CAST(NULL AS STRING)
      ) AS top_parent_id,
      [
        STRUCT(
          s.span_id AS span_id,
          (s.parent_span_id IS NOT NULL AND s.parent_span_id != '') AS is_orphan,
          (s.gen_ai_operation_name IN ('invoke_agent', 'create_agent')) AS is_agent_op,
          (s.declared_agent_name IS NOT NULL) AS has_declared_agent,
          COALESCE(s.declared_agent_name, s.gen_ai_agent_name) AS resolved_agent_name,
          s.agent_path_name AS agent_path_name
        )
      ] AS chain
    FROM spans AS s
    LEFT JOIN spans AS p
      ON s.trace_id = p.trace_id
      AND s.parent_span_id = p.span_id
      AND s.service_name IS NOT DISTINCT FROM p.service_name
      AND p.start_time BETWEEN TIMESTAMP_SUB(s.start_time, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(s.start_time, INTERVAL 1 HOUR)
  ),
  hop1 AS (
    SELECT
      h.* EXCEPT (top_parent_id, chain),
      IF(anc.span_id IS NOT NULL, anc.top_parent_id, h.top_parent_id) AS top_parent_id,
      IF(anc.span_id IS NOT NULL, ARRAY_CONCAT(anc.chain, h.chain), h.chain) AS chain
    FROM hop0 AS h
    LEFT JOIN hop0 AS anc
      ON h.top_parent_id IS NOT NULL
      AND anc.trace_id = h.trace_id
      AND anc.service_name IS NOT DISTINCT FROM h.service_name
      AND anc.span_id = h.top_parent_id
  ),
  hop2 AS (
    SELECT
      h.* EXCEPT (top_parent_id, chain),
      IF(anc.span_id IS NOT NULL, anc.top_parent_id, h.top_parent_id) AS top_parent_id,
      IF(anc.span_id IS NOT NULL, ARRAY_CONCAT(anc.chain, h.chain), h.chain) AS chain
    FROM hop1 AS h
    LEFT JOIN hop1 AS anc
      ON h.top_parent_id IS NOT NULL
      AND anc.trace_id = h.trace_id
      AND anc.service_name IS NOT DISTINCT FROM h.service_name
      AND anc.span_id = h.top_parent_id
  ),
  hop4 AS (
    SELECT
      h.* EXCEPT (top_parent_id, chain),
      IF(anc.span_id IS NOT NULL, anc.top_parent_id, h.top_parent_id) AS top_parent_id,
      IF(anc.span_id IS NOT NULL, ARRAY_CONCAT(anc.chain, h.chain), h.chain) AS chain
    FROM hop2 AS h
    LEFT JOIN hop2 AS anc
      ON h.top_parent_id IS NOT NULL
      AND anc.trace_id = h.trace_id
      AND anc.service_name IS NOT DISTINCT FROM h.service_name
      AND anc.span_id = h.top_parent_id
  ),
  hop8 AS (
    SELECT
      h.* EXCEPT (top_parent_id, chain),
      IF(anc.span_id IS NOT NULL, anc.top_parent_id, h.top_parent_id) AS top_parent_id,
      IF(anc.span_id IS NOT NULL, ARRAY_CONCAT(anc.chain, h.chain), h.chain) AS chain
    FROM hop4 AS h
    LEFT JOIN hop4 AS anc
      ON h.top_parent_id IS NOT NULL
      AND anc.trace_id = h.trace_id
      AND anc.service_name IS NOT DISTINCT FROM h.service_name
      AND anc.span_id = h.top_parent_id
  ),
  hop16 AS (
    SELECT
      h.* EXCEPT (top_parent_id, chain),
      IF(anc.span_id IS NOT NULL, anc.top_parent_id, h.top_parent_id) AS top_parent_id,
      IF(anc.span_id IS NOT NULL, ARRAY_CONCAT(anc.chain, h.chain), h.chain) AS chain
    FROM hop8 AS h
    LEFT JOIN hop8 AS anc
      ON h.top_parent_id IS NOT NULL
      AND anc.trace_id = h.trace_id
      AND anc.service_name IS NOT DISTINCT FROM h.service_name
      AND anc.span_id = h.top_parent_id
  ),
  trajectory AS (
    SELECT
      h.start_time,
      h.end_time,
      h.duration_nano,
      h.duration_ms,
      h.trace_id,
      h.span_id,
      h.parent_span_id,
      h.service_name,
      h.span_name,
      h.span_kind,
      h.status_code,
      h.status_message,
      h.gen_ai_operation_name,
      h.gen_ai_system,
      h.gen_ai_provider_name,
      h.gen_ai_request_model,
      h.gen_ai_response_model,
      h.gen_ai_agent_id,
      h.gen_ai_agent_name,
      h.gen_ai_agent_version,
      h.gen_ai_main_agent_id,
      h.gen_ai_main_agent_name,
      h.gen_ai_tool_name,
      h.gen_ai_tool_call_id,
      h.gen_ai_usage_input_tokens,
      h.gen_ai_usage_output_tokens,
      h.gen_ai_usage_reasoning_tokens,
      h.adk_session_id,
      h.adk_invocation_id,
      h.adk_user_id,
      h.error_type,
      ARRAY_LENGTH(h.chain) - 1 AS depth,
      h.chain[OFFSET(0)].span_id AS root_span_id,
      COALESCE(
        (
          SELECT n.span_id
          FROM UNNEST(h.chain) AS n WITH OFFSET AS idx
          WHERE n.is_agent_op
          ORDER BY idx DESC
          LIMIT 1
        ),
        (
          SELECT n.span_id
          FROM UNNEST(h.chain) AS n WITH OFFSET AS idx
          WHERE n.has_declared_agent
          ORDER BY idx ASC
          LIMIT 1
        )
      ) AS nearest_agent_span_id,
      COALESCE(
        (
          SELECT n.resolved_agent_name
          FROM UNNEST(h.chain) AS n WITH OFFSET AS idx
          WHERE n.is_agent_op
          ORDER BY idx DESC
          LIMIT 1
        ),
        (
          SELECT n.resolved_agent_name
          FROM UNNEST(h.chain) AS n WITH OFFSET AS idx
          WHERE n.has_declared_agent
          ORDER BY idx ASC
          LIMIT 1
        )
      ) AS nearest_agent_name,
      ARRAY(
        SELECT n.agent_path_name
        FROM UNNEST(h.chain) AS n WITH OFFSET AS idx
        WHERE n.agent_path_name IS NOT NULL
          AND (
            idx = 0
            OR n.agent_path_name IS DISTINCT FROM (
              SELECT prev.agent_path_name
              FROM UNNEST(h.chain) AS prev WITH OFFSET AS p_idx
              WHERE p_idx < idx
                AND prev.agent_path_name IS NOT NULL
              ORDER BY p_idx DESC
              LIMIT 1
            )
          )
        ORDER BY idx ASC
      ) AS agent_path,
      h.chain[OFFSET(0)].is_orphan AS is_orphan,
      h.chain AS chain
    FROM hop16 AS h
    WHERE h.top_parent_id IS NULL
      AND ARRAY_LENGTH(h.chain) <= 17
  ),
  token_parent_coverage AS (
    SELECT
      c.trace_id,
      c.service_name,
      anc.span_id AS span_id,
      LOGICAL_OR(c.gen_ai_usage_input_tokens IS NOT NULL) AS has_input,
      LOGICAL_OR(c.gen_ai_usage_output_tokens IS NOT NULL) AS has_output,
      LOGICAL_OR(c.gen_ai_usage_reasoning_tokens IS NOT NULL) AS has_reasoning
    FROM trajectory AS c,
    UNNEST(c.chain) AS anc WITH OFFSET AS anc_idx
    WHERE anc_idx < ARRAY_LENGTH(c.chain) - 1
      AND (
        c.gen_ai_usage_input_tokens IS NOT NULL
        OR c.gen_ai_usage_output_tokens IS NOT NULL
        OR c.gen_ai_usage_reasoning_tokens IS NOT NULL
      )
    GROUP BY c.trace_id, c.service_name, anc.span_id
  )
SELECT
  t.start_time,
  t.end_time,
  t.duration_nano,
  t.duration_ms,
  t.trace_id,
  t.span_id,
  t.parent_span_id,
  t.service_name,
  t.span_name,
  t.span_kind,
  t.status_code,
  t.status_message,
  t.gen_ai_operation_name,
  t.gen_ai_system,
  t.gen_ai_provider_name,
  t.gen_ai_request_model,
  t.gen_ai_response_model,
  t.gen_ai_agent_id,
  t.gen_ai_agent_name,
  t.gen_ai_agent_version,
  t.gen_ai_main_agent_id,
  t.gen_ai_main_agent_name,
  t.gen_ai_tool_name,
  t.gen_ai_tool_call_id,
  IF(
    COALESCE(token_leaf_check.has_input, FALSE)
    AND (
      (t.gen_ai_usage_output_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_output, FALSE))
      OR (t.gen_ai_usage_reasoning_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_reasoning, FALSE))
    ),
    CAST(NULL AS INT64),
    t.gen_ai_usage_input_tokens
  ) AS gen_ai_usage_input_tokens,
  IF(
    COALESCE(token_leaf_check.has_output, FALSE)
    AND (
      (t.gen_ai_usage_input_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_input, FALSE))
      OR (t.gen_ai_usage_reasoning_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_reasoning, FALSE))
    ),
    CAST(NULL AS INT64),
    t.gen_ai_usage_output_tokens
  ) AS gen_ai_usage_output_tokens,
  IF(
    COALESCE(token_leaf_check.has_reasoning, FALSE)
    AND (
      (t.gen_ai_usage_input_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_input, FALSE))
      OR (t.gen_ai_usage_output_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_output, FALSE))
    ),
    CAST(NULL AS INT64),
    t.gen_ai_usage_reasoning_tokens
  ) AS gen_ai_usage_reasoning_tokens,
  t.adk_session_id,
  t.adk_invocation_id,
  t.adk_user_id,
  t.error_type,
  t.depth,
  t.root_span_id,
  t.nearest_agent_span_id,
  t.nearest_agent_name,
  t.agent_path,
  t.is_orphan,
  ROW_NUMBER() OVER (
    PARTITION BY t.trace_id, t.service_name
    ORDER BY t.start_time ASC, t.span_id ASC
  ) AS step_index,
  (leaf_check.span_id IS NULL) AS is_leaf,
  (
    (t.gen_ai_usage_input_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_input, FALSE))
    OR (t.gen_ai_usage_output_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_output, FALSE))
    OR (t.gen_ai_usage_reasoning_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_reasoning, FALSE))
  ) AS is_token_leaf
FROM trajectory AS t
LEFT JOIN parent_ids AS leaf_check
  ON t.trace_id = leaf_check.trace_id
  AND t.span_id = leaf_check.span_id
  AND t.service_name IS NOT DISTINCT FROM leaf_check.service_name
LEFT JOIN token_parent_coverage AS token_leaf_check
  ON t.trace_id = token_leaf_check.trace_id
  AND t.span_id = token_leaf_check.span_id
  AND t.service_name IS NOT DISTINCT FROM token_leaf_check.service_name
`

// CompanionTVFDDLs returns the CREATE OR REPLACE TABLE FUNCTION statements for
// all three bounded-window companion TVFs (tvf_gen_ai_logs,
// tvf_gen_ai_metric_exemplars, tvf_gen_ai_agent_trajectory), keyed by routine
// name.
func CompanionTVFDDLs(project, dataset, spansTable, logsTable string) map[string]string {
	return map[string]string{
		GenAILogsTVFName:            strings.TrimSpace(fmt.Sprintf(GenAILogsTVFDDL, project, dataset, logsTable, spansTable)),
		GenAIMetricExemplarsTVFName: strings.TrimSpace(fmt.Sprintf(GenAIMetricExemplarsTVFDDL, project, dataset, spansTable)),
		GenAIAgentTrajectoryTVFName: strings.TrimSpace(fmt.Sprintf(GenAIAgentTrajectoryTVFDDL, project, dataset)),
	}
}

// CompanionTVFQueries returns the SELECT body for all three bounded-window
// companion TVFs, keyed by routine name.
func CompanionTVFQueries(project, dataset, spansTable, logsTable string) map[string]string {
	return map[string]string{
		GenAILogsTVFName:            strings.TrimSpace(fmt.Sprintf(GenAILogsTVFQuery, project, dataset, logsTable, spansTable)),
		GenAIMetricExemplarsTVFName: strings.TrimSpace(fmt.Sprintf(GenAIMetricExemplarsTVFQuery, project, dataset, spansTable)),
		GenAIAgentTrajectoryTVFName: strings.TrimSpace(fmt.Sprintf(GenAIAgentTrajectoryTVFQuery, project, dataset)),
	}
}
