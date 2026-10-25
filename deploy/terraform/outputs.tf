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

# --- Looker Studio 1-click report creation ---
#
# The template report (dashboards/looker_studio/README.md) has six BigQuery
# custom-query data sources with the Linking API aliases ds0 to ds5. Each one
# runs a query from dashboards/looker_studio/queries/. Looker Studio validates a
# custom query when it is saved, so the template cannot hold the YOUR_PROJECT_ID
# and YOUR_DATASET_ID placeholders that the files in the repository use: its
# queries name a sandbox project and dataset instead, which
# looker_studio_template_project_id and looker_studio_template_dataset_id record.
#
# The URL rewrites those sandbox names with sqlReplace for every data source
# whose tables and views this deployment can serve. Two are left out: ds3 and
# ds5 read the trajectory and exemplar views, which the module creates under
# require_partition_filter only with allow_unprunable_views and which BigQuery
# then rejects every query over, so they are bound only while the filter is off;
# and ds4 reads otel_alerts, which exists only with enable_continuous_queries.
# A data source that is left out keeps the template's query, which names the
# sandbox and fails with an access or "not found" error instead of showing
# another project's data. Each replacement pattern is anchored to
# "<project>.<dataset>." or "project=<project>" so that a short sandbox name
# cannot match part of another identifier.

locals {
  looker_studio_data_source_tables = {
    ds0 = [google_bigquery_table.v_gen_ai_model_costs.table_id]
    ds1 = [google_bigquery_table.v_gen_ai_sessions.table_id]
    ds2 = [google_bigquery_table.v_mcp_tool_usage.table_id, google_bigquery_table.v_gen_ai_tool_usage.table_id]
    ds3 = [var.require_partition_filter ? null : one(google_bigquery_table.v_gen_ai_agent_trajectory[*].table_id)]
    ds4 = [one(google_bigquery_table.otel_alerts[*].table_id)]
    ds5 = [var.require_partition_filter ? null : one(google_bigquery_table.v_gen_ai_metric_exemplars[*].table_id)]
  }

  looker_studio_bound_data_sources = [
    for alias, table_ids in local.looker_studio_data_source_tables : alias
    if alltrue([for table_id in table_ids : table_id != null])
  ]

  # sqlReplace takes comma-separated pattern,replacement pairs, so each piece is
  # URL-encoded on its own and the commas between them stay literal.
  looker_studio_sql_replace = join(",", [
    for piece in [
      "${var.looker_studio_template_project_id}.${var.looker_studio_template_dataset_id}.",
      "${var.project_id}.${google_bigquery_dataset.otel.dataset_id}.",
      "project=${var.looker_studio_template_project_id}",
      "project=${var.project_id}",
    ] : urlencode(piece)
  ])

  looker_studio_data_source_params = [
    for alias in local.looker_studio_bound_data_sources : join("&", [
      "ds.${alias}.sqlReplace=${local.looker_studio_sql_replace}",
      "ds.${alias}.billingProjectId=${urlencode(var.project_id)}",
      "ds.${alias}.keepDatasourceName=true",
      "ds.${alias}.refreshFields=false",
    ])
  ]
}

output "looker_studio_create_url" {
  description = "Looker Studio Linking API URL that copies the template report named by looker_studio_template_report_id and points its data sources at this project and dataset, or null when that variable is empty. Open it while signed in as the user who will own the copy. Data sources this deployment cannot serve (the trajectory and exemplar pages under require_partition_filter, the alerts page without enable_continuous_queries) are left bound to the template; see dashboards/looker_studio/README.md."
  value = var.looker_studio_template_report_id != "" ? format(
    "https://lookerstudio.google.com/reporting/create?c.reportId=%s&r.reportName=%s&%s",
    urlencode(var.looker_studio_template_report_id),
    urlencode("GenAI observability ${google_bigquery_dataset.otel.dataset_id}"),
    join("&", local.looker_studio_data_source_params),
  ) : null

  precondition {
    condition = (
      var.looker_studio_template_report_id == "" ||
      (var.looker_studio_template_project_id != "" && var.looker_studio_template_dataset_id != "")
    )
    error_message = "Set looker_studio_template_project_id and looker_studio_template_dataset_id to the sandbox project and dataset that the template report's custom queries name whenever looker_studio_template_report_id is set; without them the URL cannot find the names to replace. See dashboards/looker_studio/README.md."
  }
}
