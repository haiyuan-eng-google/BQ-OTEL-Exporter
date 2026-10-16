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
  description = "BigQuery max_staleness option of the signal and quarantine tables, never views: the maximum staleness of the data a query over a table may return. Within it, BigQuery can answer a query from the changes it has already applied instead of applying pending changes, such as change data capture upserts and deletes, at query time. It does not exclude partitions from queries. Either a Go duration, as the exporter's max_staleness options take it, in whole seconds from 1s to 168h (e.g. 15m, 1h30m, 30s), or a BigQuery SQL interval string (e.g. 0-0 0 0:15:0), which is passed through as given. Leave empty to disable."
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

# --- Partition retention and partition filters ---
#
# These govern the tables this module creates. The collector's per-table
# options of the same names apply only to tables the collector creates itself.

variable "spans_partition_expiration_days" {
  description = "Partition expiration in days for otel_spans; 0 means no expiration."
  type        = number
  default     = 0

  validation {
    condition     = var.spans_partition_expiration_days >= 0 && floor(var.spans_partition_expiration_days) == var.spans_partition_expiration_days
    error_message = "spans_partition_expiration_days must be a whole number of days, 0 or more."
  }
}

variable "logs_partition_expiration_days" {
  description = "Partition expiration in days for otel_logs; 0 means no expiration."
  type        = number
  default     = 0

  validation {
    condition     = var.logs_partition_expiration_days >= 0 && floor(var.logs_partition_expiration_days) == var.logs_partition_expiration_days
    error_message = "logs_partition_expiration_days must be a whole number of days, 0 or more."
  }
}

variable "metrics_partition_expiration_days" {
  description = "Partition expiration in days for otel_metrics; 0 means no expiration."
  type        = number
  default     = 0

  validation {
    condition     = var.metrics_partition_expiration_days >= 0 && floor(var.metrics_partition_expiration_days) == var.metrics_partition_expiration_days
    error_message = "metrics_partition_expiration_days must be a whole number of days, 0 or more."
  }
}

variable "quarantine_partition_expiration_days" {
  description = "Partition expiration in days for otel_quarantine; default 30 days, 0 means no expiration."
  type        = number
  default     = 30

  validation {
    condition     = var.quarantine_partition_expiration_days >= 0 && floor(var.quarantine_partition_expiration_days) == var.quarantine_partition_expiration_days
    error_message = "quarantine_partition_expiration_days must be a whole number of days, 0 or more."
  }
}

variable "require_partition_filter" {
  description = "Require a partition filter on queries against the partitioned signal and quarantine tables. A query over a view must then filter on a column BigQuery can use to skip partitions, and a query over v_gen_ai_logs or v_gen_ai_metric_exemplars is rejected (see deploy/README.md)."
  type        = bool
  default     = false
}

# --- Column-level security and data masking ---
#
# Off by default. governance.tf creates these resources only when
# enable_column_level_security is set.

variable "enable_column_level_security" {
  description = "Create a policy tag taxonomy, a policy tag, and a data masking policy on the tag; attach the tag to the sensitive STRING columns (otel_spans.status.message, otel_logs.severity_text, otel_quarantine.raw_payload and otel_quarantine.error_message); and create the redacted views. Needs the Data Catalog and BigQuery Data Policy APIs enabled in the project."
  type        = bool
  default     = false
}

variable "policy_taxonomy_display_name" {
  description = "Display name of the policy tag taxonomy. A display name must be unique among the taxonomies of a project and location, so give each deployment that shares them its own."
  type        = string
  default     = "otel_telemetry_sensitivity"
}

variable "masking_rule" {
  description = "Predefined masking rule of the data masking policy, which decides what a Masked Reader sees in place of a tagged value: SHA256 a SHA-256 hash of the value, ALWAYS_NULL a NULL, and DEFAULT_MASKING_VALUE the default masking value of the column type, an empty string for STRING."
  type        = string
  default     = "SHA256"

  validation {
    condition     = contains(["SHA256", "ALWAYS_NULL", "DEFAULT_MASKING_VALUE"], var.masking_rule)
    error_message = "masking_rule must be SHA256, ALWAYS_NULL, or DEFAULT_MASKING_VALUE."
  }
}

variable "fine_grained_reader_members" {
  description = "Principals (for example group:otel-admins@example.com) granted the Fine-Grained Reader role on the policy tag, who read the tagged columns unmasked. The binding is authoritative for that role on the tag. Used only when enable_column_level_security is set."
  type        = list(string)
  default     = []
}

variable "masked_reader_members" {
  description = "Principals granted the Masked Reader role on the data masking policy, who read the tagged columns masked by masking_rule. The binding is authoritative for that role on the policy. Used only when enable_column_level_security is set."
  type        = list(string)
  default     = []
}
