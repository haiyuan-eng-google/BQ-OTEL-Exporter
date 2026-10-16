output "dataset_id" {
  description = "The BigQuery dataset ID"
  value       = google_bigquery_dataset.otel.dataset_id
}

output "spans_table_id" {
  description = "Fully-qualified spans table ID"
  value       = google_bigquery_table.otel_spans.id
}

output "logs_table_id" {
  description = "Fully-qualified logs table ID"
  value       = google_bigquery_table.otel_logs.id
}

output "metrics_table_id" {
  description = "Fully-qualified metrics table ID"
  value       = google_bigquery_table.otel_metrics.id
}

output "quarantine_table_id" {
  description = "Fully-qualified quarantine table ID"
  value       = google_bigquery_table.otel_quarantine.id
}

output "v_gen_ai_metrics_table_id" {
  description = "Fully-qualified v_gen_ai_metrics view ID"
  value       = google_bigquery_table.v_gen_ai_metrics.id
}

output "service_account_email" {
  description = "Service account email for the collector"
  value       = google_service_account.otel_collector.email
}

output "iam_role_id" {
  description = "Custom IAM role granted to the collector on the dataset: metadata reads and row appends"
  value       = google_project_iam_custom_role.otel_collector_writer.id
}

output "provisioner_role_id" {
  description = "Custom IAM role granting auto_create table, view, and search index permissions on the dataset, or null unless grant_auto_create is set"
  value       = var.grant_auto_create ? google_project_iam_custom_role.otel_collector_provisioner[0].id : null
}

output "offload_bucket_iam" {
  description = "Whether GCS offload bucket IAM was granted (objectCreator + objectViewer)"
  value       = var.offload_bucket != ""
}

output "kms_key_name" {
  description = "Cloud KMS key used for CMEK encryption, or empty string when CMEK is not configured"
  value       = var.kms_key_name
}

output "policy_tag_id" {
  description = "Resource name of the policy tag attached to the sensitive STRING columns, or null unless enable_column_level_security is set"
  value       = var.enable_column_level_security ? google_data_catalog_policy_tag.sensitive_payload[0].id : null
}

output "data_policy_id" {
  description = "Resource name of the data masking policy on the policy tag, or null unless enable_column_level_security is set"
  value       = var.enable_column_level_security ? google_bigquery_datapolicy_data_policy.sensitive_payload_mask[0].name : null
}
