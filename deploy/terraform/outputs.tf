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
