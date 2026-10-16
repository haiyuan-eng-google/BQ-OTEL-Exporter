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
        { name = "message", type = "STRING", mode = "NULLABLE" },
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
    { name = "severity_text", type = "STRING", mode = "NULLABLE" },
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
    { name = "error_message", type = "STRING", mode = "REQUIRED" },
    { name = "service_name", type = "STRING", mode = "NULLABLE" },
    { name = "trace_id", type = "STRING", mode = "NULLABLE" },
    { name = "span_id", type = "STRING", mode = "NULLABLE" },
    { name = "raw_payload", type = "STRING", mode = "NULLABLE" },
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

resource "google_bigquery_table" "v_gen_ai_logs" {
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
        END AS gen_ai_inference_usage_dimension
      FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_metrics_dedup.table_id}`
      WHERE metric_name LIKE 'gen_ai.%'
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

