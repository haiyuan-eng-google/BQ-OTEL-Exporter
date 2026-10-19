# Terraform module provisioning the BigQuery destination tables, companion
# views, and least-privilege IAM role for the OTel Collector (v0alpha2).
#
# This module provisions ONLY the storage layer. The collector itself is
# deployed via the Helm chart (deploy/helm/), Cloud Run (deploy/cloudrun/),
# or Docker Compose (example/docker-compose.yml).

provider "google" {
  project = var.project_id
  region  = var.location
}

locals {
  resource_fields = [
    { name = "attributes", type = "JSON", mode = "NULLABLE" },
    { name = "attributes_schema", type = "JSON", mode = "NULLABLE" },
    { name = "dropped_attributes_count", type = "INT64", mode = "NULLABLE" },
  ]
  instrumentation_scope_fields = [
    { name = "name", type = "STRING", mode = "NULLABLE" },
    { name = "version", type = "STRING", mode = "NULLABLE" },
    { name = "attributes", type = "JSON", mode = "NULLABLE" },
    { name = "attributes_schema", type = "JSON", mode = "NULLABLE" },
    { name = "dropped_attributes_count", type = "INT64", mode = "NULLABLE" },
  ]
  object_ref_fields = [
    { name = "uri", type = "STRING", mode = "NULLABLE" },
    { name = "version", type = "STRING", mode = "NULLABLE" },
    { name = "authorizer", type = "STRING", mode = "NULLABLE" },
    { name = "details", type = "JSON", mode = "NULLABLE" },
  ]
  payload_ref_fields = [
    { name = "attribute_path", type = "STRING", mode = "NULLABLE" },
    { name = "part_index", type = "INT64", mode = "NULLABLE" },
    { name = "mime_type", type = "STRING", mode = "NULLABLE" },
    { name = "storage_mode", type = "STRING", mode = "NULLABLE" },
    { name = "byte_size", type = "INT64", mode = "NULLABLE" },
    { name = "sha256", type = "STRING", mode = "NULLABLE" },
    { name = "object_ref", type = "RECORD", mode = "NULLABLE", fields = local.object_ref_fields },
  ]

  # The provider passes max_staleness to the API unchanged, and the API takes
  # only a SQL interval string, so a Go duration is converted to the interval
  # the exporter writes for it: everything in the hours, minutes and seconds,
  # so 15m is 0-0 0 0:15:0 and 48h is 0-0 0 48:0:0. timeadd parses the
  # duration with the exporter's grammar, and formatdate reads the elapsed
  # days, hours, minutes and seconds back from the epoch it was added to,
  # which holds because variables.tf keeps a duration to whole seconds from 1s
  # to 168h. An interval is passed through as given, and an empty value leaves
  # the option unset.
  max_staleness_epoch = "2000-01-01T00:00:00Z"
  max_staleness_end   = try(timeadd(local.max_staleness_epoch, var.max_staleness), local.max_staleness_epoch)
  max_staleness = (
    var.max_staleness == "" ? null :
    startswith(var.max_staleness, "0-0 ") ? var.max_staleness :
    format("0-0 0 %d:%s:%s",
      (tonumber(formatdate("D", local.max_staleness_end)) - 1) * 24 + tonumber(formatdate("h", local.max_staleness_end)),
      formatdate("m", local.max_staleness_end),
      formatdate("s", local.max_staleness_end),
    )
  )
}

# --- Dataset ---

resource "google_bigquery_dataset" "otel" {
  dataset_id = var.dataset_id
  location   = var.location

  dynamic "default_encryption_configuration" {
    for_each = var.kms_key_name == "" ? [] : [1]
    content {
      kms_key_name = var.kms_key_name
    }
  }

  labels = var.labels

  # With CMEK, BigQuery must be allowed to use the key before it creates an
  # encrypted dataset or table, so a first apply grants it first. Without
  # CMEK the grant has no instances and this orders nothing.
  depends_on = [google_kms_crypto_key_iam_member.bq_encryptor]
}

# --- Signal Tables (v0alpha2 contract matching internal/schema/schema.go) ---
#
# Each table takes its partition expiration from its own
# *_partition_expiration_days variable, where 0 leaves partitions without
# expiration. require_partition_filter is set on the table itself, because the
# time_partitioning argument of that name is deprecated.

# otel_spans: 30-column v0alpha2 span table, hourly-partitioned on start_time,
# clustered by (service_name, trace_id, name).
resource "google_bigquery_table" "otel_spans" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = var.spans_table

  time_partitioning {
    type          = "HOUR"
    field         = "start_time"
    expiration_ms = var.spans_partition_expiration_days > 0 ? var.spans_partition_expiration_days * 86400000 : null
  }

  require_partition_filter = var.require_partition_filter

  clustering = ["service_name", "trace_id", "name"]

  max_staleness = local.max_staleness

  dynamic "encryption_configuration" {
    for_each = var.kms_key_name == "" ? [] : [1]
    content {
      kms_key_name = var.kms_key_name
    }
  }

  labels = var.labels

  schema = jsonencode([
    { name = "start_time", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "start_time_unix_nano", type = "INT64", mode = "REQUIRED" },
    { name = "end_time", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "end_time_unix_nano", type = "INT64", mode = "REQUIRED" },
    { name = "receive_time", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "receive_time_unix_nano", type = "INT64", mode = "REQUIRED" },
    { name = "duration_nano", type = "INT64", mode = "REQUIRED" },
    { name = "trace_id", type = "STRING", mode = "REQUIRED" },
    { name = "span_id", type = "STRING", mode = "REQUIRED" },
    { name = "parent_span_id", type = "STRING", mode = "NULLABLE" },
    { name = "trace_state", type = "STRING", mode = "NULLABLE" },
    { name = "flags", type = "INT64", mode = "NULLABLE" },
    { name = "name", type = "STRING", mode = "REQUIRED" },
    { name = "kind", type = "INT64", mode = "REQUIRED" },
    { name = "kind_name", type = "STRING", mode = "NULLABLE" },
    {
      name = "status"
      type = "RECORD"
      mode = "NULLABLE"
      fields = [
        { name = "code", type = "INT64", mode = "NULLABLE" },
        { name = "code_name", type = "STRING", mode = "NULLABLE" },
        merge({ name = "message", type = "STRING", mode = "NULLABLE" }, local.sensitive_column),
      ]
    },
    { name = "attributes", type = "JSON", mode = "NULLABLE" },
    { name = "attributes_schema", type = "JSON", mode = "NULLABLE" },
    { name = "dropped_attributes_count", type = "INT64", mode = "NULLABLE" },
    { name = "service_name", type = "STRING", mode = "NULLABLE" },
    { name = "resource", type = "RECORD", mode = "NULLABLE", fields = local.resource_fields },
    { name = "resource_schema_url", type = "STRING", mode = "NULLABLE" },
    { name = "instrumentation_scope", type = "RECORD", mode = "NULLABLE", fields = local.instrumentation_scope_fields },
    { name = "scope_schema_url", type = "STRING", mode = "NULLABLE" },
    {
      name = "events"
      type = "RECORD"
      mode = "REPEATED"
      fields = [
        { name = "time", type = "TIMESTAMP", mode = "NULLABLE" },
        { name = "time_unix_nano", type = "INT64", mode = "NULLABLE" },
        { name = "name", type = "STRING", mode = "NULLABLE" },
        { name = "attributes", type = "JSON", mode = "NULLABLE" },
        { name = "attributes_schema", type = "JSON", mode = "NULLABLE" },
        { name = "dropped_attributes_count", type = "INT64", mode = "NULLABLE" },
      ]
    },
    { name = "dropped_events_count", type = "INT64", mode = "NULLABLE" },
    {
      name = "links"
      type = "RECORD"
      mode = "REPEATED"
      fields = [
        { name = "trace_id", type = "STRING", mode = "NULLABLE" },
        { name = "span_id", type = "STRING", mode = "NULLABLE" },
        { name = "trace_state", type = "STRING", mode = "NULLABLE" },
        { name = "flags", type = "INT64", mode = "NULLABLE" },
        { name = "attributes", type = "JSON", mode = "NULLABLE" },
        { name = "attributes_schema", type = "JSON", mode = "NULLABLE" },
        { name = "dropped_attributes_count", type = "INT64", mode = "NULLABLE" },
      ]
    },
    { name = "dropped_links_count", type = "INT64", mode = "NULLABLE" },
    { name = "payload_refs", type = "RECORD", mode = "REPEATED", fields = local.payload_ref_fields },
    { name = "schema_version", type = "STRING", mode = "REQUIRED" },
  ])

  depends_on = [google_kms_crypto_key_iam_member.bq_encryptor]
}

# otel_logs: 27-column v0alpha2 log table, daily-partitioned on timestamp,
# clustered by (service_name, trace_id, severity_number).
resource "google_bigquery_table" "otel_logs" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = var.logs_table

  time_partitioning {
    type          = "DAY"
    field         = "timestamp"
    expiration_ms = var.logs_partition_expiration_days > 0 ? var.logs_partition_expiration_days * 86400000 : null
  }

  require_partition_filter = var.require_partition_filter

  clustering = ["service_name", "trace_id", "severity_number"]

  max_staleness = local.max_staleness

  dynamic "encryption_configuration" {
    for_each = var.kms_key_name == "" ? [] : [1]
    content {
      kms_key_name = var.kms_key_name
    }
  }

  labels = var.labels

  schema = jsonencode([
    { name = "timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "timestamp_source", type = "STRING", mode = "REQUIRED" },
    { name = "time_unix_nano", type = "INT64", mode = "REQUIRED" },
    { name = "observed_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "observed_time_unix_nano", type = "INT64", mode = "REQUIRED" },
    { name = "receive_time", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "receive_time_unix_nano", type = "INT64", mode = "REQUIRED" },
    { name = "trace_id", type = "STRING", mode = "NULLABLE" },
    { name = "span_id", type = "STRING", mode = "NULLABLE" },
    { name = "flags", type = "INT64", mode = "NULLABLE" },
    { name = "event_name", type = "STRING", mode = "NULLABLE" },
    { name = "severity_number", type = "INT64", mode = "NULLABLE" },
    merge({ name = "severity_text", type = "STRING", mode = "NULLABLE" }, local.sensitive_column),
    { name = "body", type = "JSON", mode = "NULLABLE" },
    { name = "attributes", type = "JSON", mode = "NULLABLE" },
    { name = "attributes_schema", type = "JSON", mode = "NULLABLE" },
    { name = "dropped_attributes_count", type = "INT64", mode = "NULLABLE" },
    { name = "service_name", type = "STRING", mode = "NULLABLE" },
    { name = "resource", type = "RECORD", mode = "NULLABLE", fields = local.resource_fields },
    { name = "resource_schema_url", type = "STRING", mode = "NULLABLE" },
    { name = "instrumentation_scope", type = "RECORD", mode = "NULLABLE", fields = local.instrumentation_scope_fields },
    { name = "scope_schema_url", type = "STRING", mode = "NULLABLE" },
    { name = "payload_refs", type = "RECORD", mode = "REPEATED", fields = local.payload_ref_fields },
    { name = "log_name", type = "STRING", mode = "REQUIRED" },
    { name = "insert_id", type = "STRING", mode = "REQUIRED" },
    { name = "record_fingerprint", type = "STRING", mode = "NULLABLE" },
    { name = "schema_version", type = "STRING", mode = "REQUIRED" },
  ])

  depends_on = [google_kms_crypto_key_iam_member.bq_encryptor]
}

# otel_metrics: 40-column v0alpha2 metric data-point table, daily-partitioned on
# timestamp, clustered by (service_name, metric_name, metric_type).
resource "google_bigquery_table" "otel_metrics" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = var.metrics_table

  time_partitioning {
    type          = "DAY"
    field         = "timestamp"
    expiration_ms = var.metrics_partition_expiration_days > 0 ? var.metrics_partition_expiration_days * 86400000 : null
  }

  require_partition_filter = var.require_partition_filter

  clustering = ["service_name", "metric_name", "metric_type"]

  max_staleness = local.max_staleness

  dynamic "encryption_configuration" {
    for_each = var.kms_key_name == "" ? [] : [1]
    content {
      kms_key_name = var.kms_key_name
    }
  }

  labels = var.labels

  schema = jsonencode([
    { name = "timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "time_unix_nano", type = "INT64", mode = "REQUIRED" },
    { name = "start_time", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "start_time_unix_nano", type = "INT64", mode = "NULLABLE" },
    { name = "receive_time", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "receive_time_unix_nano", type = "INT64", mode = "REQUIRED" },
    { name = "service_name", type = "STRING", mode = "NULLABLE" },
    { name = "metric_name", type = "STRING", mode = "REQUIRED" },
    { name = "metric_description", type = "STRING", mode = "NULLABLE" },
    { name = "metric_unit", type = "STRING", mode = "NULLABLE" },
    { name = "metric_type", type = "STRING", mode = "REQUIRED" },
    { name = "aggregation_temporality", type = "STRING", mode = "NULLABLE" },
    { name = "is_monotonic", type = "BOOL", mode = "NULLABLE" },
    { name = "flags", type = "INT64", mode = "NULLABLE" },
    { name = "attributes", type = "JSON", mode = "NULLABLE" },
    { name = "attributes_schema", type = "JSON", mode = "NULLABLE" },
    { name = "metric_metadata", type = "JSON", mode = "NULLABLE" },
    { name = "metric_metadata_schema", type = "JSON", mode = "NULLABLE" },
    { name = "value_type", type = "STRING", mode = "NULLABLE" },
    { name = "value_double", type = "FLOAT64", mode = "NULLABLE" },
    { name = "value_int64", type = "INT64", mode = "NULLABLE" },
    { name = "count", type = "INT64", mode = "NULLABLE" },
    { name = "sum", type = "FLOAT64", mode = "NULLABLE" },
    { name = "min", type = "FLOAT64", mode = "NULLABLE" },
    { name = "max", type = "FLOAT64", mode = "NULLABLE" },
    { name = "explicit_bounds", type = "FLOAT64", mode = "REPEATED" },
    { name = "bucket_counts", type = "INT64", mode = "REPEATED" },
    { name = "exp_scale", type = "INT64", mode = "NULLABLE" },
    { name = "exp_zero_count", type = "INT64", mode = "NULLABLE" },
    { name = "exp_zero_threshold", type = "FLOAT64", mode = "NULLABLE" },
    {
      name = "exp_positive"
      type = "RECORD"
      mode = "NULLABLE"
      fields = [
        { name = "offset", type = "INT64", mode = "NULLABLE" },
        { name = "bucket_counts", type = "INT64", mode = "REPEATED" },
      ]
    },
    {
      name = "exp_negative"
      type = "RECORD"
      mode = "NULLABLE"
      fields = [
        { name = "offset", type = "INT64", mode = "NULLABLE" },
        { name = "bucket_counts", type = "INT64", mode = "REPEATED" },
      ]
    },
    {
      name = "summary_quantiles"
      type = "RECORD"
      mode = "REPEATED"
      fields = [
        { name = "quantile", type = "FLOAT64", mode = "NULLABLE" },
        { name = "value", type = "FLOAT64", mode = "NULLABLE" },
      ]
    },
    {
      name = "exemplars"
      type = "RECORD"
      mode = "REPEATED"
      fields = [
        { name = "time", type = "TIMESTAMP", mode = "NULLABLE" },
        { name = "time_unix_nano", type = "INT64", mode = "NULLABLE" },
        { name = "value_double", type = "FLOAT64", mode = "NULLABLE" },
        { name = "value_int64", type = "INT64", mode = "NULLABLE" },
        { name = "span_id", type = "STRING", mode = "NULLABLE" },
        { name = "trace_id", type = "STRING", mode = "NULLABLE" },
        { name = "filtered_attributes", type = "JSON", mode = "NULLABLE" },
        { name = "filtered_attributes_schema", type = "JSON", mode = "NULLABLE" },
      ]
    },
    { name = "resource", type = "RECORD", mode = "NULLABLE", fields = local.resource_fields },
    { name = "resource_schema_url", type = "STRING", mode = "NULLABLE" },
    { name = "instrumentation_scope", type = "RECORD", mode = "NULLABLE", fields = local.instrumentation_scope_fields },
    { name = "scope_schema_url", type = "STRING", mode = "NULLABLE" },
    { name = "point_fingerprint", type = "STRING", mode = "REQUIRED" },
    { name = "schema_version", type = "STRING", mode = "REQUIRED" },
  ])

  depends_on = [google_kms_crypto_key_iam_member.bq_encryptor]
}

# otel_quarantine: 12-column v0alpha2 quarantine (dead-letter) table,
# daily-partitioned on ingest_time with a 30-day partition expiration by
# default, clustered by (signal, error_code, service_name).
resource "google_bigquery_table" "otel_quarantine" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = var.quarantine_table

  time_partitioning {
    type          = "DAY"
    field         = "ingest_time"
    expiration_ms = var.quarantine_partition_expiration_days > 0 ? var.quarantine_partition_expiration_days * 86400000 : null
  }

  require_partition_filter = var.require_partition_filter

  clustering = ["signal", "error_code", "service_name"]

  max_staleness = local.max_staleness

  dynamic "encryption_configuration" {
    for_each = var.kms_key_name == "" ? [] : [1]
    content {
      kms_key_name = var.kms_key_name
    }
  }

  labels = var.labels

  schema = jsonencode([
    { name = "ingest_time", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "signal", type = "STRING", mode = "REQUIRED" },
    { name = "error_code", type = "STRING", mode = "REQUIRED" },
    merge({ name = "error_message", type = "STRING", mode = "REQUIRED" }, local.sensitive_column),
    { name = "service_name", type = "STRING", mode = "NULLABLE" },
    { name = "trace_id", type = "STRING", mode = "NULLABLE" },
    { name = "span_id", type = "STRING", mode = "NULLABLE" },
    merge({ name = "raw_payload", type = "STRING", mode = "NULLABLE" }, local.sensitive_column),
    { name = "raw_attributes", type = "JSON", mode = "NULLABLE" },
    { name = "payload_ref", type = "RECORD", mode = "NULLABLE", fields = local.object_ref_fields },
    { name = "exporter_instance", type = "STRING", mode = "NULLABLE" },
    { name = "schema_version", type = "STRING", mode = "REQUIRED" },
  ])

  depends_on = [google_kms_crypto_key_iam_member.bq_encryptor]
}

# --- Deduplication & Companion Views (matching internal/schema/schema.go) ---

resource "google_bigquery_table" "otel_spans_dedup" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "${var.spans_table}_dedup"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<-EOT
      SELECT *
      FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_spans.table_id}`
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
          start_time,
          service_name,
          trace_id,
          span_id,
          name
        ORDER BY
          end_time_unix_nano DESC,
          receive_time_unix_nano DESC
      ) = 1
    EOT
  }
}

resource "google_bigquery_table" "otel_logs_dedup" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "${var.logs_table}_dedup"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<-EOT
      SELECT *
      FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_logs.table_id}`
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
          timestamp,
          time_unix_nano,
          service_name,
          trace_id,
          span_id,
          severity_number,
          log_name,
          insert_id
        ORDER BY
          receive_time_unix_nano DESC
      ) = 1
    EOT
  }
}

resource "google_bigquery_table" "otel_metrics_dedup" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "${var.metrics_table}_dedup"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<-EOT
      SELECT *
      FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_metrics.table_id}`
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
          timestamp,
          time_unix_nano,
          service_name,
          metric_name,
          metric_type,
          point_fingerprint
        ORDER BY
          receive_time_unix_nano DESC
      ) = 1
    EOT
  }
}

resource "google_bigquery_table" "otel_spans_as_allspans" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "otel_spans_as_allspans"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<-EOT
      SELECT
        trace_id,
        span_id,
        trace_state,
        parent_span_id,
        name,
        kind,
        start_time,
        start_time_unix_nano,
        end_time,
        end_time_unix_nano,
        receive_time,
        receive_time_unix_nano,
        duration_nano,
        attributes,
        attributes_schema,
        dropped_attributes_count,
        events,
        dropped_events_count,
        links,
        dropped_links_count,
        STRUCT(status.code AS code, status.message AS message) AS status,
        resource,
        resource_schema_url,
        instrumentation_scope,
        scope_schema_url
      FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_spans_dedup.table_id}`
    EOT
  }
}

resource "google_bigquery_table" "v_gen_ai_spans" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "v_gen_ai_spans"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<-EOT
      SELECT
        start_time,
        end_time,
        duration_nano,
        ROUND(duration_nano / 1e6, 3) AS duration_ms,
        trace_id,
        span_id,
        parent_span_id,
        service_name,
        name AS span_name,
        kind AS span_kind,
        status.code AS status_code,
        status.message AS status_message,
        JSON_VALUE(attributes, '$."gen_ai.operation.name"') AS gen_ai_operation_name,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.system"'),
          JSON_VALUE(attributes, '$."gen_ai.provider.name"')
        ) AS gen_ai_system,
        JSON_VALUE(attributes, '$."gen_ai.request.model"') AS gen_ai_request_model,
        JSON_VALUE(attributes, '$."gen_ai.response.model"') AS gen_ai_response_model,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
          JSON_VALUE(attributes, '$."gcp.vertex.agent.name"'),
          service_name
        ) AS gen_ai_agent_name,
        JSON_VALUE(attributes, '$."gen_ai.agent.id"') AS gen_ai_agent_id,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.conversation.id"'),
          JSON_VALUE(attributes, '$."gcp.vertex.agent.session_id"')
        ) AS gen_ai_conversation_id,
        LAX_INT64(attributes['gen_ai.usage.input_tokens']) AS gen_ai_usage_input_tokens,
        LAX_INT64(attributes['gen_ai.usage.output_tokens']) AS gen_ai_usage_output_tokens,
        LAX_FLOAT64(attributes['gen_ai.request.temperature']) AS gen_ai_request_temperature,
        JSON_VALUE(attributes, '$."gen_ai.response.finish_reasons"[0]') AS gen_ai_response_finish_reason,
        JSON_VALUE(attributes, '$."gen_ai.tool.name"') AS gen_ai_tool_name,
        JSON_VALUE(attributes, '$."gen_ai.tool.call.id"') AS gen_ai_tool_call_id,
        JSON_VALUE(attributes, '$."gcp.vertex.agent.event_id"') AS adk_event_id,
        COALESCE(
          JSON_VALUE(attributes, '$."gcp.vertex.agent.session_id"'),
          JSON_VALUE(attributes, '$."gen_ai.conversation.id"')
        ) AS adk_session_id,
        JSON_VALUE(attributes, '$."gcp.vertex.agent.invocation_id"') AS adk_invocation_id,
        COALESCE(
          JSON_VALUE(attributes, '$."user.id"'),
          JSON_VALUE(attributes, '$."gen_ai.user.id"')
        ) AS adk_user_id,
        payload_refs,
        attributes,
        resource,
        instrumentation_scope,
        events,
        links,
        JSON_VALUE(attributes, '$."gen_ai.provider.name"') AS gen_ai_provider_name,
        COALESCE(
          LAX_INT64(attributes['gen_ai.usage.cache_read.input_tokens']),
          LAX_INT64(attributes['gen_ai.usage.cache_read_input_tokens'])
        ) AS gen_ai_usage_cache_read_input_tokens,
        COALESCE(
          LAX_INT64(attributes['gen_ai.usage.cache_creation.input_tokens']),
          LAX_INT64(attributes['gen_ai.usage.cache_creation_input_tokens'])
        ) AS gen_ai_usage_cache_creation_input_tokens,
        COALESCE(
          LAX_INT64(attributes['gen_ai.usage.reasoning.output_tokens']),
          LAX_INT64(attributes['gen_ai.usage.reasoning_tokens'])
        ) AS gen_ai_usage_reasoning_tokens,
        LAX_FLOAT64(attributes['gen_ai.request.top_p']) AS gen_ai_request_top_p,
        LAX_INT64(attributes['gen_ai.request.top_k']) AS gen_ai_request_top_k,
        LAX_INT64(attributes['gen_ai.request.max_tokens']) AS gen_ai_request_max_tokens,
        JSON_VALUE(attributes, '$."gen_ai.output.type"') AS gen_ai_output_type,
        JSON_VALUE(attributes, '$."gen_ai.tool.type"') AS gen_ai_tool_type,
        JSON_VALUE(attributes, '$."gen_ai.tool.description"') AS gen_ai_tool_description,
        JSON_VALUE(attributes, '$."gen_ai.agent.description"') AS gen_ai_agent_description,
        JSON_VALUE(attributes, '$."gen_ai.agent.parent.id"') AS gen_ai_agent_parent_id,
        JSON_VALUE(attributes, '$."gen_ai.data_source.id"') AS gen_ai_data_source_id,
        JSON_VALUE(attributes, '$."gen_ai.evaluation.name"') AS gen_ai_evaluation_name,
        COALESCE(
          LAX_FLOAT64(attributes['gen_ai.evaluation.score.value']),
          LAX_FLOAT64(attributes['gen_ai.evaluation.score'])
        ) AS gen_ai_evaluation_score,
        JSON_VALUE(attributes, '$."error.type"') AS error_type,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.main_agent.id"'),
          JSON_VALUE(resource.attributes, '$."gen_ai.main_agent.id"')
        ) AS gen_ai_main_agent_id,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.main_agent.name"'),
          JSON_VALUE(resource.attributes, '$."gen_ai.main_agent.name"')
        ) AS gen_ai_main_agent_name,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.main_agent.description"'),
          JSON_VALUE(resource.attributes, '$."gen_ai.main_agent.description"')
        ) AS gen_ai_main_agent_description,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.agent.version"'),
          JSON_VALUE(resource.attributes, '$."gen_ai.agent.version"')
        ) AS gen_ai_agent_version,
        JSON_VALUE(attributes, '$."gen_ai.response.id"') AS gen_ai_response_id,
        LAX_FLOAT64(attributes['gen_ai.response.time_to_first_chunk']) AS gen_ai_response_time_to_first_chunk
      FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_spans_dedup.table_id}`
      WHERE JSON_VALUE(attributes, '$."gen_ai.operation.name"') IS NOT NULL
         OR JSON_VALUE(attributes, '$."gen_ai.system"') IS NOT NULL
         OR JSON_VALUE(attributes, '$."gen_ai.provider.name"') IS NOT NULL
         OR JSON_VALUE(attributes, '$."gcp.vertex.agent.invocation_id"') IS NOT NULL
         OR JSON_VALUE(attributes, '$."gen_ai.main_agent.name"') IS NOT NULL
         OR JSON_VALUE(attributes, '$."gen_ai.main_agent.id"') IS NOT NULL
    EOT
  }
}

# Earlier releases created v_gen_ai_logs without count; keep that view as
# instance 0 rather than replacing it.
moved {
  from = google_bigquery_table.v_gen_ai_logs
  to   = google_bigquery_table.v_gen_ai_logs[0]
}

resource "google_bigquery_table" "v_gen_ai_logs" {
  # BigQuery rejects every query over this view while otel_spans requires a
  # partition filter, so it is left out then unless allow_unprunable_views.
  count = (!var.require_partition_filter || var.allow_unprunable_views) ? 1 : 0

  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "v_gen_ai_logs"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<-EOT
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
      FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_logs_dedup.table_id}` l
      LEFT JOIN `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_spans_dedup.table_id}` s
        ON l.trace_id = s.trace_id
        AND l.span_id = s.span_id
        AND l.service_name IS NOT DISTINCT FROM s.service_name
        AND s.start_time BETWEEN TIMESTAMP_SUB(l.timestamp, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(l.timestamp, INTERVAL 1 HOUR)
      WHERE COALESCE(NULLIF(l.event_name, ''), JSON_VALUE(l.attributes, '$."event.name"')) LIKE 'gen_ai.%'
         OR JSON_VALUE(l.attributes, '$."gen_ai.system"') IS NOT NULL
         OR JSON_VALUE(l.attributes, '$."gen_ai.provider.name"') IS NOT NULL
         OR JSON_VALUE(l.attributes, '$."gcp.vertex.agent.invocation_id"') IS NOT NULL
    EOT
  }
}

resource "google_bigquery_table" "v_gen_ai_model_costs" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "v_gen_ai_model_costs"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<EOT
      WITH cost_spans AS (
        SELECT
          TIMESTAMP_TRUNC(s.start_time, HOUR) AS usage_hour,
          s.service_name,
          s.trace_id,
          s.span_id,
          s.parent_span_id,
          s.gen_ai_agent_name,
          s.gen_ai_provider_name,
          s.gen_ai_system,
          s.gen_ai_request_model,
          s.gen_ai_response_model,
          s.gen_ai_operation_name,
          s.status_code,
          s.duration_ms,
          s.gen_ai_usage_input_tokens,
          s.gen_ai_usage_output_tokens,
          s.gen_ai_usage_cache_read_input_tokens,
          s.gen_ai_usage_cache_creation_input_tokens,
          s.gen_ai_usage_reasoning_tokens,
          s.error_type
        FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.v_gen_ai_spans.table_id}` AS s
      ),
      cost_windows AS (
        SELECT
          s.*,
          ARRAY_AGG(
            IF(
              s.parent_span_id IS NOT NULL
              AND (s.gen_ai_usage_input_tokens IS NOT NULL OR s.gen_ai_usage_output_tokens IS NOT NULL),
              s.parent_span_id,
              NULL
            )
          ) OVER (
            PARTITION BY s.usage_hour, s.service_name, s.trace_id
          ) AS token_parent_span_ids
        FROM cost_spans AS s
      ),
      cost_agg AS (
        SELECT
          s.usage_hour,
          s.service_name,
          s.gen_ai_agent_name,
          COALESCE(s.gen_ai_provider_name, s.gen_ai_system) AS provider,
          COALESCE(s.gen_ai_response_model, s.gen_ai_request_model) AS model,
          COUNT(*) AS call_count,
          COUNTIF(s.status_code = 2) AS error_count,
          SUM(COALESCE(s.gen_ai_usage_input_tokens, 0)) AS total_input_tokens,
          SUM(COALESCE(s.gen_ai_usage_output_tokens, 0)) AS total_output_tokens,
          SUM(COALESCE(s.gen_ai_usage_cache_read_input_tokens, 0)) AS total_cache_read_input_tokens,
          SUM(COALESCE(s.gen_ai_usage_cache_creation_input_tokens, 0)) AS total_cache_creation_input_tokens,
          SUM(COALESCE(s.gen_ai_usage_reasoning_tokens, 0)) AS total_reasoning_tokens,
          APPROX_QUANTILES(s.duration_ms, 100)[OFFSET(50)] AS p50_duration_ms,
          APPROX_QUANTILES(s.duration_ms, 100)[OFFSET(95)] AS p95_duration_ms,
          APPROX_QUANTILES(s.duration_ms, 100)[OFFSET(99)] AS p99_duration_ms,
          COUNTIF(s.error_type IS NOT NULL) AS typed_error_count,
          APPROX_TOP_COUNT(s.error_type, 6) AS raw_top_error_types
        FROM cost_windows AS s
        WHERE NOT EXISTS (
            SELECT 1 FROM UNNEST(s.token_parent_span_ids) AS token_parent_span_id
            WHERE token_parent_span_id = s.span_id
          )
          AND (
            s.gen_ai_operation_name IN ('chat', 'generate_content', 'text_completion', 'embeddings')
            OR s.gen_ai_request_model IS NOT NULL
          )
          AND (
            COALESCE(s.gen_ai_operation_name, '') NOT IN ('invoke_agent', 'create_agent', 'execute_tool')
            OR s.gen_ai_usage_input_tokens IS NOT NULL
            OR s.gen_ai_usage_output_tokens IS NOT NULL
          )
        GROUP BY 1, 2, 3, 4, 5
      )
      SELECT
        * EXCEPT (raw_top_error_types),
        ARRAY(
          SELECT AS STRUCT x.value, x.count
          FROM UNNEST(raw_top_error_types) AS x
          WHERE x.value IS NOT NULL
          ORDER BY x.count DESC, x.value ASC
          LIMIT 5
        ) AS top_error_types
      FROM cost_agg
    EOT
  }
}

resource "google_bigquery_table" "v_gen_ai_sessions" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "v_gen_ai_sessions"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<EOT
      WITH session_spans AS (
        SELECT
          s.service_name,
          DATE(s.start_time) AS session_start_date,
          COALESCE(s.adk_session_id, s.gen_ai_conversation_id) AS session_id,
          s.adk_user_id,
          s.gen_ai_agent_name,
          s.start_time,
          s.end_time,
          s.trace_id,
          s.span_id,
          s.parent_span_id,
          s.adk_invocation_id,
          s.gen_ai_operation_name,
          s.gen_ai_tool_name,
          s.gen_ai_request_model,
          s.status_code,
          s.gen_ai_usage_input_tokens,
          s.gen_ai_usage_output_tokens,
          s.gen_ai_usage_cache_read_input_tokens,
          s.gen_ai_usage_cache_creation_input_tokens,
          s.gen_ai_usage_reasoning_tokens,
          s.error_type
        FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.v_gen_ai_spans.table_id}` AS s
        WHERE COALESCE(s.adk_session_id, s.gen_ai_conversation_id) IS NOT NULL
      ),
      session_windows AS (
        SELECT
          s.*,
          ARRAY_AGG(
            IF(
              s.parent_span_id IS NOT NULL
              AND (s.gen_ai_usage_input_tokens IS NOT NULL OR s.gen_ai_usage_output_tokens IS NOT NULL),
              s.parent_span_id,
              NULL
            )
          ) OVER (
            PARTITION BY s.service_name, s.session_start_date, s.session_id, s.trace_id
          ) AS token_parent_span_ids
        FROM session_spans AS s
      ),
      session_calls AS (
        SELECT
          s.*,
          NOT EXISTS (
            SELECT 1 FROM UNNEST(s.token_parent_span_ids) AS token_parent_span_id
            WHERE token_parent_span_id = s.span_id
          ) AS is_token_leaf,
          COALESCE(
            NOT EXISTS (
              SELECT 1 FROM UNNEST(s.token_parent_span_ids) AS token_parent_span_id
              WHERE token_parent_span_id = s.span_id
            )
            AND (
              s.gen_ai_operation_name IN ('chat', 'generate_content', 'text_completion', 'embeddings')
              OR s.gen_ai_request_model IS NOT NULL
            )
            AND (
              COALESCE(s.gen_ai_operation_name, '') NOT IN ('invoke_agent', 'create_agent', 'execute_tool')
              OR s.gen_ai_usage_input_tokens IS NOT NULL
              OR s.gen_ai_usage_output_tokens IS NOT NULL
            ), FALSE
          ) AS is_llm_call
        FROM session_windows AS s
      ),
      session_agg AS (
        SELECT
          session_id,
          ANY_VALUE(adk_user_id) AS user_id,
          service_name,
          ANY_VALUE(gen_ai_agent_name) AS agent_name,
          MIN(start_time) AS session_start_time,
          MAX(end_time) AS session_end_time,
          TIMESTAMP_DIFF(MAX(end_time), MIN(start_time), MILLISECOND) AS session_duration_ms,
          COUNT(DISTINCT trace_id) AS turn_count,
          COUNT(DISTINCT adk_invocation_id) AS invocation_count,
          COUNTIF(gen_ai_operation_name = 'execute_tool' OR gen_ai_tool_name IS NOT NULL) AS tool_call_count,
          COUNTIF(status_code = 2) AS error_count,
          SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_input_tokens, 0), 0)) AS total_input_tokens,
          SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_output_tokens, 0), 0)) AS total_output_tokens,
          SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_cache_read_input_tokens, 0), 0)) AS total_cache_read_input_tokens,
          SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_reasoning_tokens, 0), 0)) AS total_reasoning_tokens,
          session_start_date,
          SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_cache_creation_input_tokens, 0), 0)) AS total_cache_creation_input_tokens,
          COUNTIF(is_llm_call) AS llm_call_count,
          COUNTIF(gen_ai_operation_name = 'invoke_agent') AS agent_invocation_count,
          COUNTIF(error_type IS NOT NULL) AS typed_error_count,
          APPROX_TOP_COUNT(error_type, 6) AS raw_top_error_types
        FROM session_calls
        GROUP BY service_name, session_start_date, session_id
      )
      SELECT
        * EXCEPT (raw_top_error_types),
        ARRAY(
          SELECT AS STRUCT x.value, x.count
          FROM UNNEST(raw_top_error_types) AS x
          WHERE x.value IS NOT NULL
          ORDER BY x.count DESC, x.value ASC
          LIMIT 5
        ) AS top_error_types
      FROM session_agg
    EOT
  }
}

resource "google_bigquery_table" "v_gen_ai_metrics" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "v_gen_ai_metrics"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<EOT
      SELECT
        timestamp,
        start_time,
        receive_time,
        service_name,
        metric_name,
        metric_description,
        metric_unit,
        metric_type,
        aggregation_temporality,
        is_monotonic,
        JSON_VALUE(attributes, '$."gen_ai.operation.name"') AS gen_ai_operation_name,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.system"'),
          JSON_VALUE(attributes, '$."gen_ai.provider.name"')
        ) AS gen_ai_system,
        JSON_VALUE(attributes, '$."gen_ai.provider.name"') AS gen_ai_provider_name,
        JSON_VALUE(attributes, '$."gen_ai.request.model"') AS gen_ai_request_model,
        JSON_VALUE(attributes, '$."gen_ai.response.model"') AS gen_ai_response_model,
        JSON_VALUE(attributes, '$."gen_ai.token.type"') AS gen_ai_token_type,
        JSON_VALUE(attributes, '$."error.type"') AS error_type,
        value_type,
        value_double,
        value_int64,
        count,
        sum,
        min,
        max,
        explicit_bounds,
        bucket_counts,
        exp_scale,
        exp_zero_count,
        exp_zero_threshold,
        exp_positive,
        exp_negative,
        summary_quantiles,
        exemplars,
        attributes,
        resource,
        instrumentation_scope,
        JSON_VALUE(attributes, '$."gen_ai.token.modality"') AS gen_ai_token_modality,
        COALESCE(
          JSON_VALUE(attributes, '$."gen_ai.agent.name"'),
          JSON_VALUE(attributes, '$."gen_ai.main_agent.name"'),
          JSON_VALUE(resource.attributes, '$."gen_ai.main_agent.name"')
        ) AS gen_ai_agent_name,
        CASE
          WHEN metric_name = 'gen_ai.client.inference.usage.input_tokens' THEN 'input'
          WHEN metric_name = 'gen_ai.client.inference.usage.output_tokens' THEN 'output'
          WHEN metric_name = 'gen_ai.client.inference.usage.cache_read.input_tokens' THEN 'cache_read_input'
          WHEN metric_name = 'gen_ai.client.inference.usage.cache_write.input_tokens' THEN 'cache_write_input'
          WHEN metric_name = 'gen_ai.client.inference.usage.reasoning.output_tokens' THEN 'reasoning_output'
          ELSE JSON_VALUE(attributes, '$."gen_ai.token.type"')
        END AS gen_ai_inference_usage_dimension,
        time_unix_nano,
        point_fingerprint
      FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_metrics_dedup.table_id}`
      WHERE metric_name LIKE 'gen_ai.%'
    EOT
  }
}

resource "google_bigquery_table" "v_gen_ai_metric_exemplars" {
  # Like v_gen_ai_logs, left out while otel_spans requires a partition filter
  # unless allow_unprunable_views.
  count = (!var.require_partition_filter || var.allow_unprunable_views) ? 1 : 0

  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "v_gen_ai_metric_exemplars"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<EOT
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
      FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.v_gen_ai_metrics.table_id}` m,
      UNNEST(m.exemplars) AS ex WITH OFFSET AS exemplar_ordinal
      LEFT JOIN `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_spans_dedup.table_id}` s
        ON s.trace_id = ex.trace_id
        AND s.span_id = ex.span_id
        AND s.service_name IS NOT DISTINCT FROM m.service_name
        AND s.start_time BETWEEN TIMESTAMP_SUB(m.timestamp, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(m.timestamp, INTERVAL 1 HOUR)
      WHERE ex.trace_id IS NOT NULL
        AND ex.span_id IS NOT NULL
        AND ex.trace_id != ''
        AND ex.span_id != ''
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY m.timestamp, m.time_unix_nano, m.service_name, m.metric_name, m.metric_type, m.point_fingerprint, exemplar_ordinal
        ORDER BY s.end_time DESC, s.start_time DESC, s.receive_time DESC, s.name ASC
      ) = 1
    EOT
  }
}

resource "google_bigquery_table" "v_gen_ai_tool_usage" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "v_gen_ai_tool_usage"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<EOT
      WITH tool_agg AS (
        SELECT
          TIMESTAMP_TRUNC(start_time, HOUR) AS usage_hour,
          service_name,
          gen_ai_agent_name,
          gen_ai_tool_name,
          gen_ai_tool_type,
          COUNT(*) AS call_count,
          COUNTIF(status_code = 2) AS error_count,
          SAFE_DIVIDE(COUNTIF(status_code = 2), COUNT(*)) AS error_rate,
          APPROX_QUANTILES(duration_ms, 100)[OFFSET(50)] AS p50_duration_ms,
          APPROX_QUANTILES(duration_ms, 100)[OFFSET(95)] AS p95_duration_ms,
          COUNTIF(error_type IS NOT NULL) AS typed_error_count,
          APPROX_TOP_COUNT(error_type, 6) AS raw_top_error_types
        FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.v_gen_ai_spans.table_id}`
        WHERE gen_ai_tool_name IS NOT NULL OR gen_ai_operation_name = 'execute_tool'
        GROUP BY 1, 2, 3, 4, 5
      )
      SELECT
        * EXCEPT (raw_top_error_types),
        ARRAY(
          SELECT AS STRUCT x.value, x.count
          FROM UNNEST(raw_top_error_types) AS x
          WHERE x.value IS NOT NULL
          ORDER BY x.count DESC, x.value ASC
          LIMIT 5
        ) AS top_error_types
      FROM tool_agg
    EOT
  }
}

resource "google_bigquery_table" "v_gen_ai_agent_trajectory" {
  # Like v_gen_ai_logs, left out while otel_spans requires a partition filter
  # unless allow_unprunable_views: it joins each span to its parent and child
  # spans on a window that depends on each row.
  count = (!var.require_partition_filter || var.allow_unprunable_views) ? 1 : 0

  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = "v_gen_ai_agent_trajectory"

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<EOT
      WITH RECURSIVE
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
          FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.v_gen_ai_spans.table_id}`
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
        trajectory AS (
          SELECT
            s.start_time,
            s.end_time,
            s.duration_nano,
            s.duration_ms,
            s.trace_id,
            s.span_id,
            s.parent_span_id,
            s.service_name,
            s.span_name,
            s.span_kind,
            s.status_code,
            s.status_message,
            s.gen_ai_operation_name,
            s.gen_ai_system,
            s.gen_ai_provider_name,
            s.gen_ai_request_model,
            s.gen_ai_response_model,
            s.gen_ai_agent_id,
            s.gen_ai_agent_name,
            s.gen_ai_agent_version,
            s.gen_ai_main_agent_id,
            s.gen_ai_main_agent_name,
            s.gen_ai_tool_name,
            s.gen_ai_tool_call_id,
            s.gen_ai_usage_input_tokens,
            s.gen_ai_usage_output_tokens,
            s.gen_ai_usage_reasoning_tokens,
            s.adk_session_id,
            s.adk_invocation_id,
            s.adk_user_id,
            s.error_type,
            0 AS depth,
            s.span_id AS root_span_id,
            CASE
              WHEN s.gen_ai_operation_name IN ('invoke_agent', 'create_agent')
                OR s.declared_agent_name IS NOT NULL
              THEN s.span_id
              ELSE CAST(NULL AS STRING)
            END AS nearest_agent_span_id,
            CASE
              WHEN s.gen_ai_operation_name IN ('invoke_agent', 'create_agent')
                OR s.declared_agent_name IS NOT NULL
              THEN COALESCE(s.declared_agent_name, s.gen_ai_agent_name)
              ELSE CAST(NULL AS STRING)
            END AS nearest_agent_name,
            CASE
              WHEN s.agent_path_name IS NOT NULL THEN [s.agent_path_name]
              ELSE ARRAY<STRING>[]
            END AS agent_path,
            (s.parent_span_id IS NOT NULL AND s.parent_span_id != '') AS is_orphan,
            [s.span_id] AS visited_span_ids
          FROM spans AS s
          LEFT JOIN spans AS p
            ON s.trace_id = p.trace_id
            AND s.parent_span_id = p.span_id
            AND s.service_name IS NOT DISTINCT FROM p.service_name
            AND p.start_time BETWEEN TIMESTAMP_SUB(s.start_time, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(s.start_time, INTERVAL 1 HOUR)
          WHERE s.parent_span_id IS NULL
             OR s.parent_span_id = ''
             OR p.span_id IS NULL

          UNION ALL

          SELECT
            c.start_time,
            c.end_time,
            c.duration_nano,
            c.duration_ms,
            c.trace_id,
            c.span_id,
            c.parent_span_id,
            c.service_name,
            c.span_name,
            c.span_kind,
            c.status_code,
            c.status_message,
            c.gen_ai_operation_name,
            c.gen_ai_system,
            c.gen_ai_provider_name,
            c.gen_ai_request_model,
            c.gen_ai_response_model,
            c.gen_ai_agent_id,
            c.gen_ai_agent_name,
            c.gen_ai_agent_version,
            c.gen_ai_main_agent_id,
            c.gen_ai_main_agent_name,
            c.gen_ai_tool_name,
            c.gen_ai_tool_call_id,
            c.gen_ai_usage_input_tokens,
            c.gen_ai_usage_output_tokens,
            c.gen_ai_usage_reasoning_tokens,
            c.adk_session_id,
            c.adk_invocation_id,
            c.adk_user_id,
            c.error_type,
            t.depth + 1 AS depth,
            t.root_span_id AS root_span_id,
            CASE
              WHEN c.gen_ai_operation_name IN ('invoke_agent', 'create_agent')
                OR (t.nearest_agent_span_id IS NULL AND c.declared_agent_name IS NOT NULL)
              THEN c.span_id
              ELSE t.nearest_agent_span_id
            END AS nearest_agent_span_id,
            CASE
              WHEN c.gen_ai_operation_name IN ('invoke_agent', 'create_agent')
                OR (t.nearest_agent_span_id IS NULL AND c.declared_agent_name IS NOT NULL)
              THEN COALESCE(c.declared_agent_name, c.gen_ai_agent_name)
              ELSE t.nearest_agent_name
            END AS nearest_agent_name,
            CASE
              WHEN c.agent_path_name IS NOT NULL
                AND (
                  ARRAY_LENGTH(t.agent_path) = 0
                  OR t.agent_path[SAFE_OFFSET(ARRAY_LENGTH(t.agent_path) - 1)] != c.agent_path_name
                )
              THEN ARRAY_CONCAT(t.agent_path, [c.agent_path_name])
              ELSE t.agent_path
            END AS agent_path,
            t.is_orphan AS is_orphan,
            ARRAY_CONCAT(t.visited_span_ids, [c.span_id]) AS visited_span_ids
          FROM trajectory AS t
          JOIN spans AS c
            ON c.trace_id = t.trace_id
            AND c.parent_span_id = t.span_id
            AND c.service_name IS NOT DISTINCT FROM t.service_name
            AND c.start_time BETWEEN TIMESTAMP_SUB(t.start_time, INTERVAL 1 HOUR) AND TIMESTAMP_ADD(t.start_time, INTERVAL 24 HOUR)
          WHERE t.depth < 16
            AND c.span_id NOT IN UNNEST(t.visited_span_ids)
        ),
        token_parent_coverage AS (
          SELECT
            trace_id,
            service_name,
            span_id,
            LOGICAL_OR(has_input) AS has_input,
            LOGICAL_OR(has_output) AS has_output,
            LOGICAL_OR(has_reasoning) AS has_reasoning
          FROM (
            SELECT
              d.trace_id,
              d.service_name,
              anc_span_id AS span_id,
              d.gen_ai_usage_input_tokens IS NOT NULL AS has_input,
              d.gen_ai_usage_output_tokens IS NOT NULL AS has_output,
              d.gen_ai_usage_reasoning_tokens IS NOT NULL AS has_reasoning
            FROM trajectory AS d
            CROSS JOIN UNNEST(d.visited_span_ids) AS anc_span_id WITH OFFSET AS anc_idx
            WHERE anc_idx < ARRAY_LENGTH(d.visited_span_ids) - 1
              AND (
                d.gen_ai_usage_input_tokens IS NOT NULL
                OR d.gen_ai_usage_output_tokens IS NOT NULL
                OR d.gen_ai_usage_reasoning_tokens IS NOT NULL
              )
            UNION ALL
            SELECT
              p.trace_id,
              p.service_name,
              p.span_id AS span_id,
              c.gen_ai_usage_input_tokens IS NOT NULL AS has_input,
              c.gen_ai_usage_output_tokens IS NOT NULL AS has_output,
              c.gen_ai_usage_reasoning_tokens IS NOT NULL AS has_reasoning
            FROM trajectory AS p
            JOIN trajectory AS c
              ON c.trace_id = p.trace_id
              AND c.parent_span_id = p.span_id
              AND c.service_name IS NOT DISTINCT FROM p.service_name
            WHERE c.span_id NOT IN UNNEST(p.visited_span_ids)
              AND (
                NOT p.is_orphan
                OR c.start_time > p.start_time
                OR (c.start_time = p.start_time AND c.span_id > p.span_id)
              )
              AND (
                c.gen_ai_usage_input_tokens IS NOT NULL
                OR c.gen_ai_usage_output_tokens IS NOT NULL
                OR c.gen_ai_usage_reasoning_tokens IS NOT NULL
              )
          )
          GROUP BY trace_id, service_name, span_id
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
    EOT
  }
}

# --- IAM: least privilege ---
#
# This module provisions the dataset and every table and view, so by default
# the collector's service account gets only the data path, and only on this
# dataset: read the dataset and table metadata it validates at startup, and
# append rows. Nothing is granted at project scope.

resource "google_project_iam_custom_role" "otel_collector_writer" {
  role_id     = "otelCollectorWriter"
  title       = "OTel BigQuery Collector Writer"
  description = "Startup metadata reads and Storage Write appends for the BigQuery OTel exporter"
  permissions = [
    # Read dataset/table metadata for startup validation.
    "bigquery.datasets.get",
    "bigquery.tables.get",
    # Append rows via the Storage Write API.
    "bigquery.tables.updateData",
  ]
}

resource "google_service_account" "otel_collector" {
  account_id   = var.service_account_id
  display_name = "OTel BigQuery Collector"
}

resource "google_bigquery_dataset_iam_member" "otel_collector_writer" {
  dataset_id = google_bigquery_dataset.otel.dataset_id
  role       = google_project_iam_custom_role.otel_collector_writer.id
  member     = "serviceAccount:${google_service_account.otel_collector.email}"
}

# Optional (var.grant_auto_create): what the exporter's auto_create options
# need on top of the data path, for a collector that creates or reconciles
# tables and views or creates search indexes itself. The table permissions
# stay on this dataset; only running the CREATE SEARCH INDEX query job needs a
# project-level grant. auto_create.dataset is never needed, because this module
# creates the dataset.

resource "google_project_iam_custom_role" "otel_collector_provisioner" {
  count       = var.grant_auto_create ? 1 : 0
  role_id     = "otelCollectorProvisioner"
  title       = "OTel BigQuery Collector Provisioner"
  description = "Table, view, and search index creation for the BigQuery OTel exporter's auto_create"
  permissions = [
    # Create missing tables and views; reconcile additive columns.
    "bigquery.tables.create",
    "bigquery.tables.update",
    # CREATE SEARCH INDEX on the signal tables.
    "bigquery.tables.createIndex",
  ]
}

resource "google_bigquery_dataset_iam_member" "otel_collector_provisioner" {
  count      = var.grant_auto_create ? 1 : 0
  dataset_id = google_bigquery_dataset.otel.dataset_id
  role       = google_project_iam_custom_role.otel_collector_provisioner[0].id
  member     = "serviceAccount:${google_service_account.otel_collector.email}"
}

resource "google_project_iam_custom_role" "otel_collector_jobs" {
  count       = var.grant_auto_create ? 1 : 0
  role_id     = "otelCollectorJobs"
  title       = "OTel BigQuery Collector Jobs"
  description = "Runs the BigQuery OTel exporter's CREATE SEARCH INDEX query jobs"
  permissions = [
    "bigquery.jobs.create",
  ]
}

resource "google_project_iam_member" "otel_collector_jobs" {
  count   = var.grant_auto_create ? 1 : 0
  project = var.project_id
  role    = google_project_iam_custom_role.otel_collector_jobs[0].id
  member  = "serviceAccount:${google_service_account.otel_collector.email}"
}

# --- Optional GCS offload bucket IAM ---
#
# When offload is enabled, the collector uploads large payloads to a GCS bucket
# and writes BigQuery ObjectRef references. The collector service account needs
# objectCreator (to upload) and objectViewer (to verify uploads) on the bucket.

resource "google_storage_bucket_iam_member" "otel_collector_offload_creator" {
  count  = var.offload_bucket == "" ? 0 : 1
  bucket = var.offload_bucket
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.otel_collector.email}"
}

resource "google_storage_bucket_iam_member" "otel_collector_offload_viewer" {
  count  = var.offload_bucket == "" ? 0 : 1
  bucket = var.offload_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.otel_collector.email}"
}

# --- Optional CMEK: grant the BigQuery service agent encrypt/decrypt on the KMS key ---
#
# When CMEK is enabled, the BigQuery service agent
# (serviceAccount:bq-<project-number>@bigquery-encryption.iam.gserviceaccount.com)
# needs roles/cloudkms.cryptoKeyEncrypterDecrypter on the KMS key. This module
# grants it automatically when kms_key_name is set.

data "google_bigquery_default_service_account" "bq_cmek" {
  project = var.project_id
}

resource "google_kms_crypto_key_iam_member" "bq_encryptor" {
  count         = var.kms_key_name == "" ? 0 : 1
  crypto_key_id = var.kms_key_name
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${data.google_bigquery_default_service_account.bq_cmek.email}"
}

# --- Optional Continuous Queries alerting infrastructure ---
#
# When enable_continuous_queries is set, this module provisions the alert sink
# table (otel_alerts), the Pub/Sub alert topic, a dedicated service account for
# Continuous Query jobs, least-privilege IAM bindings, and an Enterprise or
# Enterprise Plus reservation assignment with job_type = "CONTINUOUS".
# otel_alerts inherits the dataset's default encryption when CMEK is enabled.

locals {
  cq_dataset_norm = trim(replace(lower(var.dataset_id), "/[^a-z0-9]+/", "-"), "-")
  cq_dataset_slug = local.cq_dataset_norm != "" ? local.cq_dataset_norm : "ds"
  cq_dataset_hash = substr(sha256(var.dataset_id), 0, 8)
  # Always starts with "otel-cq-" ([a-z]), contains only [a-z0-9-], ends with [a-z0-9],
  # is at most 8 + 47 + 1 + 8 = 64 chars, and never collides across case/underscore/truncation variants.
  continuous_query_reservation_name = (
    can(regex("^[a-z](?:[a-z0-9_]*[a-z0-9])?$", var.dataset_id)) && !can(regex("_[0-9a-f]{8}$", var.dataset_id)) && length("otel-cq-${replace(var.dataset_id, "_", "-")}") <= 55
    ? "otel-cq-${replace(var.dataset_id, "_", "-")}"
    : "otel-cq-${trim(substr(local.cq_dataset_slug, 0, min(length(local.cq_dataset_slug), 47)), "-")}-${local.cq_dataset_hash}"
  )
  # Always starts with "otel-alerts-" (letter, not "goog"), contains only [a-z0-9_-],
  # is between 13 and 255 chars, and never collides across case/underscore/truncation variants.
  otel_alerts_topic_name = (
    can(regex("^[a-z](?:[a-z0-9_]*[a-z0-9])?$", var.dataset_id)) && !can(regex("_[0-9a-f]{8}$", var.dataset_id)) && length("otel-alerts-${var.dataset_id}") <= 240
    ? "otel-alerts-${var.dataset_id}"
    : "otel-alerts-${trim(substr(local.cq_dataset_slug, 0, min(length(local.cq_dataset_slug), 234)), "-")}-${local.cq_dataset_hash}"
  )
}

resource "google_bigquery_table" "otel_alerts" {
  count = var.enable_continuous_queries ? 1 : 0

  dataset_id               = google_bigquery_dataset.otel.dataset_id
  table_id                 = "otel_alerts"
  deletion_protection      = var.alerts_deletion_protection
  require_partition_filter = var.require_partition_filter

  time_partitioning {
    type          = "DAY"
    field         = "event_time"
    expiration_ms = var.alerts_partition_expiration_days > 0 ? var.alerts_partition_expiration_days * 86400000 : null
  }

  clustering = ["alert_kind", "service_name"]
  labels     = var.labels

  schema = jsonencode([
    { name = "alert_id", type = "STRING", mode = "REQUIRED" },
    { name = "alert_kind", type = "STRING", mode = "REQUIRED" },
    { name = "event_time", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "service_name", type = "STRING", mode = "NULLABLE" },
    { name = "trace_id", type = "STRING", mode = "REQUIRED" },
    { name = "span_id", type = "STRING", mode = "REQUIRED" },
    { name = "payload", type = "JSON", mode = "NULLABLE" },
  ])
}

resource "google_pubsub_topic" "otel_alerts" {
  count = var.enable_continuous_queries ? 1 : 0

  name   = local.otel_alerts_topic_name
  labels = var.labels

  lifecycle {
    precondition {
      condition     = can(regex("^[A-Za-z][A-Za-z0-9_~+.%-]{2,254}$", local.otel_alerts_topic_name)) && !can(regex("^(?i)goog", local.otel_alerts_topic_name)) && length(local.otel_alerts_topic_name) <= 255
      error_message = "Derived Pub/Sub topic name ${local.otel_alerts_topic_name} must start with a letter, not begin with goog, and be 3 to 255 characters."
    }
  }
}

resource "google_service_account" "continuous_queries" {
  count = var.enable_continuous_queries ? 1 : 0

  account_id   = "${var.service_account_id}-cq"
  display_name = "OTel BigQuery Continuous Queries"

  lifecycle {
    precondition {
      condition     = length(var.service_account_id) <= 27
      error_message = "service_account_id must be at most 27 characters when enable_continuous_queries is true so the derived ${var.service_account_id}-cq account ID stays within Google Cloud's 30-character limit."
    }
  }
}

resource "google_bigquery_dataset_iam_member" "continuous_queries_viewer" {
  count = var.enable_continuous_queries ? 1 : 0

  dataset_id = google_bigquery_dataset.otel.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.continuous_queries[0].email}"
}

resource "google_bigquery_table_iam_member" "continuous_queries_alerts_writer" {
  count = var.enable_continuous_queries ? 1 : 0

  project    = var.project_id
  dataset_id = google_bigquery_dataset.otel.dataset_id
  table_id   = google_bigquery_table.otel_alerts[0].table_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.continuous_queries[0].email}"
}

resource "google_pubsub_topic_iam_member" "continuous_queries_publisher" {
  count = var.enable_continuous_queries ? 1 : 0

  topic  = google_pubsub_topic.otel_alerts[0].name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.continuous_queries[0].email}"
}

resource "google_pubsub_topic_iam_member" "continuous_queries_pubsub_viewer" {
  count = var.enable_continuous_queries ? 1 : 0

  topic  = google_pubsub_topic.otel_alerts[0].name
  role   = "roles/pubsub.viewer"
  member = "serviceAccount:${google_service_account.continuous_queries[0].email}"
}

resource "google_project_iam_member" "continuous_queries_jobs" {
  count   = var.enable_continuous_queries ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.continuous_queries[0].email}"
}

resource "google_bigquery_reservation" "continuous_queries" {
  count = var.enable_continuous_queries ? 1 : 0

  name              = local.continuous_query_reservation_name
  location          = var.location
  slot_capacity     = var.continuous_query_slot_capacity
  edition           = var.continuous_query_edition
  ignore_idle_slots = false

  lifecycle {
    precondition {
      condition     = !var.enable_column_level_security
      error_message = "BigQuery Continuous Queries do not support tables with column-level security policy tags (enable_column_level_security must be false when enable_continuous_queries is true)."
    }
    precondition {
      condition     = can(regex("^[a-z][a-z0-9-]{0,62}[a-z0-9]$", local.continuous_query_reservation_name)) && length(local.continuous_query_reservation_name) <= 64
      error_message = "Derived BigQuery reservation name ${local.continuous_query_reservation_name} must match ^[a-z][a-z0-9-]{0,62}[a-z0-9]$ and be at most 64 characters."
    }
  }
}

resource "google_bigquery_reservation_assignment" "continuous_queries" {
  count = var.enable_continuous_queries ? 1 : 0

  assignee    = "projects/${var.project_id}"
  job_type    = "CONTINUOUS"
  reservation = google_bigquery_reservation.continuous_queries[0].id
  location    = var.location
}

