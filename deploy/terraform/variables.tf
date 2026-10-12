variable "project_id" {
  description = "Google Cloud project ID that owns the BigQuery dataset"
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset ID for OTel telemetry"
  type        = string
  default     = "otel"
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "spans_table" {
  description = "Table name for OTel spans"
  type        = string
  default     = "otel_spans"
}

variable "logs_table" {
  description = "Table name for OTel logs"
  type        = string
  default     = "otel_logs"
}

variable "metrics_table" {
  description = "Table name for OTel metrics"
  type        = string
  default     = "otel_metrics"
}

variable "quarantine_table" {
  description = "Table name for the dead-letter quarantine table"
  type        = string
  default     = "otel_quarantine"
}

variable "service_account_id" {
  description = "Service account ID for the collector"
  type        = string
  default     = "otel-collector"
}

variable "grant_auto_create" {
  description = "Also grant what the exporter's auto_create options need: creating and reconciling tables and views on this dataset, and creating search indexes. Off by default because this module provisions every table and view itself."
  type        = bool
  default     = false
}
