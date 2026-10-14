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
  description = "BigQuery dataset location (e.g. US, us-central1, eu)"
  type        = string
  default     = "US"

  validation {
    condition     = can(regex("^[A-Za-z]{2,20}(-[A-Za-z0-9]+)*$", var.location))
    error_message = "Location must be a valid BigQuery location (letters and hyphens, e.g. US, us-central1, eu)."
  }
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

# --- Enterprise governance ---

variable "kms_key_name" {
  description = "Cloud KMS key resource name for CMEK encryption of the BigQuery dataset and tables. Leave empty to use Google-managed encryption. When set, also grants the BigQuery service agent decrypt permission on the key."
  type        = string
  default     = ""

  validation {
    condition = var.kms_key_name == "" ? true : can(regex(
      "^projects/[^/\\s]+/locations/[^/\\s]+/keyRings/[^/\\s]+/cryptoKeys/[^/\\s]+$",
      var.kms_key_name
    ))
    error_message = "kms_key_name must be a full Cloud KMS key resource name (projects/<project>/locations/<location>/keyRings/<keyRing>/cryptoKeys/<key>)."
  }
}

variable "labels" {
  description = "Labels applied to the BigQuery dataset and all provisioned tables and views for cost allocation and governance."
  type        = map(string)
  default     = {}
}

variable "offload_bucket" {
  description = "GCS bucket name for optional large-payload offloading. When set, grants the collector service account objectCreator and objectViewer on the bucket. The bucket must already exist."
  type        = string
  default     = ""
}

variable "max_staleness" {
  description = "Maximum staleness of data in partitioned tables. When set, partitions newer than this duration are not queried. Applied to all signal and quarantine tables, never to views. Either a Go duration, as the exporter's max_staleness options take it, in whole seconds from 1s to 168h (e.g. 15m, 1h30m), or a BigQuery SQL interval string (e.g. 0-0 0 0:15:0). Leave empty to disable."
  type        = string
  default     = ""

  validation {
    condition     = var.max_staleness == "" ? true : can(regex("^(0-0 [0-9]+ [0-9]+:[0-9]+:[0-9]+|([0-9]+(\\.[0-9]+)?(ns|us|µs|ms|s|m|h))+)$", var.max_staleness))
    error_message = "max_staleness must be a Go duration string (e.g. 15m, 1h30m, 30s), a BigQuery SQL interval string (e.g. 0-0 0 0:15:0), or empty."
  }

  # main.tf converts a duration through timeadd, which parses it as the
  # exporter does but drops fractional seconds from its result. A duration is
  # therefore a whole number of seconds exactly when adding just under one more
  # second leaves that result unchanged. The bounds are the exporter's.
  validation {
    condition = !can(regex("^([0-9]+(\\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$", var.max_staleness)) ? true : try(
      timecmp(timeadd("2000-01-01T00:00:00Z", var.max_staleness), "2000-01-01T00:00:01Z") >= 0 &&
      timecmp(timeadd("2000-01-01T00:00:00Z", var.max_staleness), "2000-01-08T00:00:00Z") <= 0 &&
      timeadd("2000-01-01T00:00:00Z", var.max_staleness) == timeadd("2000-01-01T00:00:00Z", "${var.max_staleness}999999999ns"),
      false
    )
    error_message = "A max_staleness duration must be a whole number of seconds from 1s to 168h, the range the exporter accepts."
  }
}
