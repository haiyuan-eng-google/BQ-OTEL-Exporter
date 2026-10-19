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

output "v_gen_ai_metric_exemplars_table_id" {
  description = "Fully-qualified v_gen_ai_metric_exemplars view ID, or null when require_partition_filter is set without allow_unprunable_views"
  value       = one(google_bigquery_table.v_gen_ai_metric_exemplars[*].id)
}

output "v_gen_ai_agent_trajectory_table_id" {
  description = "Fully-qualified v_gen_ai_agent_trajectory view ID, or null when require_partition_filter is set without allow_unprunable_views"
  value       = one(google_bigquery_table.v_gen_ai_agent_trajectory[*].id)
}

output "v_mcp_spans_table_id" {
  description = "Fully-qualified v_mcp_spans view ID"
  value       = google_bigquery_table.v_mcp_spans.id
}

output "v_mcp_tool_usage_table_id" {
  description = "Fully-qualified v_mcp_tool_usage view ID"
  value       = google_bigquery_table.v_mcp_tool_usage.id
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

output "otel_alerts_table_id" {
  description = "Fully-qualified otel_alerts table ID, or null unless enable_continuous_queries is set"
  value       = var.enable_continuous_queries ? google_bigquery_table.otel_alerts[0].id : null
}

output "otel_alerts_pubsub_topic_id" {
  description = "Fully-qualified Pub/Sub alert topic ID, or null unless enable_continuous_queries is set"
  value       = var.enable_continuous_queries ? google_pubsub_topic.otel_alerts[0].id : null
}

output "continuous_queries_service_account_email" {
  description = "Service account email for BigQuery Continuous Queries, or null unless enable_continuous_queries is set"
  value       = var.enable_continuous_queries ? google_service_account.continuous_queries[0].email : null
}

output "continuous_queries_rendered_sql" {
  description = "Rendered BigQuery Continuous Query SQL templates keyed by template name, or null unless enable_continuous_queries is set"
  value = var.enable_continuous_queries ? {
    for name in ["tool_error_rate_alert", "token_budget_burn", "agent_latency_p95"] :
    name => templatefile("${path.module}/../continuous-queries/${name}.sql", {
      project_id             = var.project_id
      dataset_id             = google_bigquery_dataset.otel.dataset_id
      spans_table            = google_bigquery_table.otel_spans.table_id
      pubsub_topic           = google_pubsub_topic.otel_alerts[0].name
      max_turn_input_tokens  = var.continuous_query_max_turn_input_tokens
      max_turn_output_tokens = var.continuous_query_max_turn_output_tokens
      max_turn_duration_ms   = var.continuous_query_max_turn_duration_ms
    })
  } : null
}

output "continuous_queries_bq_cli_commands" {
  description = "Ready-to-run bq query CLI commands (piping SQL on stdin) for each Continuous Query template, or null unless enable_continuous_queries is set"
  value = var.enable_continuous_queries ? {
    for name in ["tool_error_rate_alert", "token_budget_burn", "agent_latency_p95"] :
    name => format(
      "terraform -chdir=deploy/terraform output -json continuous_queries_rendered_sql | jq -r '.%s' | bq query --project_id=%s --location=%s --use_legacy_sql=false --continuous=true --connection_property=service_account=%s",
      name,
      var.project_id,
      var.location,
      google_service_account.continuous_queries[0].email,
    )
  } : null
}
