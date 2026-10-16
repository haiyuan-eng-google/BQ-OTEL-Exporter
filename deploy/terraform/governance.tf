# Column-level security and data masking for the STRING columns that can hold
# free-form telemetry content. Every resource here is created only when
# enable_column_level_security is set, which it is not by default.
#
# This file adds the second and third of three layers that keep that content
# from readers who should not see it. The first is the collector's own
# redaction (redaction.enabled in the collector config), which runs before
# rows are written.
#
# 2. A policy tag, sensitive_payload, in a taxonomy with fine-grained access
#    control activated, and a data masking policy on the tag. main.tf attaches
#    the tag to the sensitive STRING columns through local.sensitive_column.
#    Principals with the Fine-Grained Reader role on the tag read those
#    columns as stored, and principals with the Masked Reader role on the
#    policy read them masked by var.masking_rule.
# 3. Redacted views, which read the dedup views but select no tagged column,
#    no JSON column, and no events, links, or payload_refs.
#
# No JSON column or repeated record is tagged: the companion views and most
# queries read attributes and log bodies, so tagging them would put every
# view behind the Fine-Grained Reader role. A principal who may read a table
# therefore reads its JSON columns.

locals {
  # Merged into the schema entry of each sensitive STRING column in main.tf.
  # merge skips a null argument, so with the flag off those entries, and the
  # table schemas, are exactly what they are without this file.
  sensitive_column = var.enable_column_level_security ? {
    policyTags = { names = [google_data_catalog_policy_tag.sensitive_payload[0].id] }
  } : null
}

# --- Policy tag taxonomy and data masking policy ---

resource "google_data_catalog_taxonomy" "otel_sensitivity" {
  count = var.enable_column_level_security ? 1 : 0

  region                 = lower(var.location)
  display_name           = var.policy_taxonomy_display_name
  description            = "Sensitivity of the OTel telemetry columns in BigQuery dataset ${var.dataset_id}"
  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]
}

resource "google_data_catalog_policy_tag" "sensitive_payload" {
  count = var.enable_column_level_security ? 1 : 0

  taxonomy     = google_data_catalog_taxonomy.otel_sensitivity[0].id
  display_name = "sensitive_payload"
  description  = "Free-form telemetry text that can hold prompts, responses, error details, or other user content"
}

resource "google_bigquery_datapolicy_data_policy" "sensitive_payload_mask" {
  count = var.enable_column_level_security ? 1 : 0

  project          = var.project_id
  location         = lower(var.location)
  data_policy_id   = "${var.dataset_id}_sensitive_payload_mask"
  policy_tag       = google_data_catalog_policy_tag.sensitive_payload[0].name
  data_policy_type = "DATA_MASKING_POLICY"

  data_masking_policy {
    predefined_expression = var.masking_rule
  }
}

# --- Reader roles ---
#
# Each binding is authoritative for its role on its resource, and is created
# only when it has members.

resource "google_data_catalog_policy_tag_iam_binding" "fine_grained_readers" {
  count = var.enable_column_level_security && length(var.fine_grained_reader_members) > 0 ? 1 : 0

  policy_tag = google_data_catalog_policy_tag.sensitive_payload[0].name
  role       = "roles/datacatalog.categoryFineGrainedReader"
  members    = var.fine_grained_reader_members
}

resource "google_bigquery_datapolicy_data_policy_iam_binding" "masked_readers" {
  count = var.enable_column_level_security && length(var.masked_reader_members) > 0 ? 1 : 0

  project        = google_bigquery_datapolicy_data_policy.sensitive_payload_mask[0].project
  location       = google_bigquery_datapolicy_data_policy.sensitive_payload_mask[0].location
  data_policy_id = google_bigquery_datapolicy_data_policy.sensitive_payload_mask[0].data_policy_id
  role           = "roles/bigquerydatapolicy.maskedReader"
  members        = var.masked_reader_members
}

# --- Redacted views ---
#
# Each view keeps the shape of its table where it can: a tagged column and
# every JSON column read as NULL of their type, inside records too, and the
# repeated records that carry attributes or payload references are left out.
# The time columns pass through unchanged, so a query filters these views by
# time as it filters the dedup views. They are ordinary views in the same
# dataset: BigQuery checks a query over one against the reader's own access
# to the tables it reads. A view holds no data, so it is not
# deletion-protected, and turning the flag off removes it.

resource "google_bigquery_table" "v_otel_spans_redacted" {
  count = var.enable_column_level_security ? 1 : 0

  dataset_id          = google_bigquery_dataset.otel.dataset_id
  table_id            = "v_${var.spans_table}_redacted"
  deletion_protection = false

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<-EOT
      SELECT
        start_time,
        start_time_unix_nano,
        end_time,
        end_time_unix_nano,
        receive_time,
        receive_time_unix_nano,
        duration_nano,
        trace_id,
        span_id,
        parent_span_id,
        trace_state,
        flags,
        name,
        kind,
        kind_name,
        STRUCT(
          status.code AS code,
          status.code_name AS code_name,
          CAST(NULL AS STRING) AS message
        ) AS status,
        CAST(NULL AS JSON) AS attributes,
        CAST(NULL AS JSON) AS attributes_schema,
        dropped_attributes_count,
        service_name,
        STRUCT(
          CAST(NULL AS JSON) AS attributes,
          CAST(NULL AS JSON) AS attributes_schema,
          resource.dropped_attributes_count AS dropped_attributes_count
        ) AS resource,
        resource_schema_url,
        STRUCT(
          instrumentation_scope.name AS name,
          instrumentation_scope.version AS version,
          CAST(NULL AS JSON) AS attributes,
          CAST(NULL AS JSON) AS attributes_schema,
          instrumentation_scope.dropped_attributes_count AS dropped_attributes_count
        ) AS instrumentation_scope,
        scope_schema_url,
        dropped_events_count,
        dropped_links_count,
        schema_version
      FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_spans_dedup.table_id}`
    EOT
  }
}

resource "google_bigquery_table" "v_otel_logs_redacted" {
  count = var.enable_column_level_security ? 1 : 0

  dataset_id          = google_bigquery_dataset.otel.dataset_id
  table_id            = "v_${var.logs_table}_redacted"
  deletion_protection = false

  labels = var.labels

  view {
    use_legacy_sql = false
    query          = <<-EOT
      SELECT
        timestamp,
        timestamp_source,
        time_unix_nano,
        observed_timestamp,
        observed_time_unix_nano,
        receive_time,
        receive_time_unix_nano,
        trace_id,
        span_id,
        flags,
        event_name,
        severity_number,
        CAST(NULL AS STRING) AS severity_text,
        CAST(NULL AS JSON) AS body,
        CAST(NULL AS JSON) AS attributes,
        CAST(NULL AS JSON) AS attributes_schema,
        dropped_attributes_count,
        service_name,
        STRUCT(
          CAST(NULL AS JSON) AS attributes,
          CAST(NULL AS JSON) AS attributes_schema,
          resource.dropped_attributes_count AS dropped_attributes_count
        ) AS resource,
        resource_schema_url,
        STRUCT(
          instrumentation_scope.name AS name,
          instrumentation_scope.version AS version,
          CAST(NULL AS JSON) AS attributes,
          CAST(NULL AS JSON) AS attributes_schema,
          instrumentation_scope.dropped_attributes_count AS dropped_attributes_count
        ) AS instrumentation_scope,
        scope_schema_url,
        log_name,
        insert_id,
        record_fingerprint,
        schema_version
      FROM `${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.${google_bigquery_table.otel_logs_dedup.table_id}`
    EOT
  }
}
