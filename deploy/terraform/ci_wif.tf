variable "enable_github_wif" {
  description = "Provision Workload Identity Federation pool, OIDC provider, and dedicated CI service account for GitHub Actions live integration tests."
  type        = bool
  default     = false
}

variable "github_repository" {
  description = "GitHub repository in owner/repo format permitted to assume the CI service account via OIDC on refs/heads/main."
  type        = string
  default     = "haiyuan-eng-google/BQ-OTEL-Exporter"

  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "github_repository must be in owner/repo format."
  }
}

variable "github_repository_id" {
  description = "Immutable numeric GitHub repository_id claim required when enable_github_wif is true."
  type        = string
  default     = ""

  validation {
    condition     = var.github_repository_id == "" || can(regex("^[1-9][0-9]*$", var.github_repository_id))
    error_message = "github_repository_id must be a positive numeric GitHub repository ID."
  }
}

variable "github_repository_owner_id" {
  description = "Immutable numeric GitHub repository_owner_id claim required when enable_github_wif is true."
  type        = string
  default     = ""

  validation {
    condition     = var.github_repository_owner_id == "" || can(regex("^[1-9][0-9]*$", var.github_repository_owner_id))
    error_message = "github_repository_owner_id must be a positive numeric GitHub owner ID."
  }
}

variable "github_workflow_ref" {
  description = "Relative workflow path within github_repository permitted to exchange OIDC tokens on refs/heads/main."
  type        = string
  default     = ".github/workflows/live-integration.yml"

  validation {
    condition     = can(regex("^\\.github/workflows/[A-Za-z0-9_.-]+\\.ya?ml$", var.github_workflow_ref))
    error_message = "github_workflow_ref must be a .github/workflows/<name>.yml path."
  }
}

variable "github_allowed_events" {
  description = "GitHub Actions event_name values permitted to exchange OIDC tokens for live integration CI."
  type        = list(string)
  default     = ["schedule", "workflow_dispatch"]

  validation {
    condition     = length(var.github_allowed_events) > 0 && alltrue([for e in var.github_allowed_events : can(regex("^[a-z_]+$", e))])
    error_message = "github_allowed_events must be a non-empty list of GitHub event names."
  }
}

variable "ci_offload_bucket" {
  description = "Dedicated GCS bucket for live CI payload-offload integration tests (separate from production offload_bucket)."
  type        = string
  default     = ""
}

variable "ci_workload_identity_pool_id" {
  description = "Workload Identity Pool ID for GitHub Actions live integration CI."
  type        = string
  default     = "bq-otel-exporter-ci"
}

variable "ci_workload_identity_provider_id" {
  description = "Workload Identity Pool Provider ID for GitHub Actions OIDC."
  type        = string
  default     = "github-actions"
}

variable "ci_service_account_id" {
  description = "Service account ID for the GitHub Actions live integration CI runner."
  type        = string
  default     = "bq-otel-exporter-ci"
}

resource "google_iam_workload_identity_pool" "github_ci" {
  count                     = var.enable_github_wif ? 1 : 0
  project                   = var.project_id
  workload_identity_pool_id = var.ci_workload_identity_pool_id
  display_name              = "BQ OTel Exporter GitHub CI"
  description               = "Workload Identity Pool for GitHub Actions live BigQuery and GCS integration tests"
}

resource "google_iam_workload_identity_pool_provider" "github_ci" {
  count                              = var.enable_github_wif ? 1 : 0
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_ci[0].workload_identity_pool_id
  workload_identity_pool_provider_id = var.ci_workload_identity_provider_id
  display_name                       = "GitHub Actions OIDC"
  description                        = "OIDC provider restricted to ${var.github_repository} (${var.github_workflow_ref}) on refs/heads/main"

  attribute_mapping = {
    "google.subject"                = "assertion.sub"
    "attribute.actor"               = "assertion.actor"
    "attribute.repository"          = "assertion.repository"
    "attribute.repository_id"       = "assertion.repository_id"
    "attribute.repository_owner_id" = "assertion.repository_owner_id"
    "attribute.ref"                 = "assertion.ref"
    # GitHub Actions OIDC tokens for top-level schedule and workflow_dispatch
    # runs emit the workflow reference claim as assertion.workflow_ref
    # ("<owner>/<repo>/<workflow_path>@<ref>"); assertion.job_workflow_ref is
    # only populated for reusable workflow_call invocations.
    "attribute.workflow_ref" = "assertion.workflow_ref"
    "attribute.event_name"   = "assertion.event_name"
  }

  # Restrict token exchange to immutable numeric owner/repo IDs, the configured
  # workflow file on refs/heads/main, and the allowed trigger events.
  attribute_condition = join(" && ", [
    "attribute.repository_owner_id == \"${var.github_repository_owner_id}\"",
    "attribute.repository_id == \"${var.github_repository_id}\"",
    "attribute.repository == \"${var.github_repository}\"",
    "attribute.ref == \"refs/heads/main\"",
    "attribute.workflow_ref == \"${var.github_repository}/${var.github_workflow_ref}@refs/heads/main\"",
    "attribute.event_name in [${join(", ", formatlist("%q", var.github_allowed_events))}]",
  ])

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  lifecycle {
    precondition {
      condition     = can(regex("^[1-9][0-9]*$", var.github_repository_id)) && can(regex("^[1-9][0-9]*$", var.github_repository_owner_id))
      error_message = "github_repository_id and github_repository_owner_id must be set to positive numeric GitHub IDs when enable_github_wif is true."
    }
  }
}

resource "google_service_account" "github_ci" {
  count        = var.enable_github_wif ? 1 : 0
  project      = var.project_id
  account_id   = var.ci_service_account_id
  display_name = "BQ OTel Exporter GitHub CI Runner"
  description  = "Service account assumed by GitHub Actions live-integration workflow via Workload Identity Federation"
}

resource "google_service_account_iam_member" "github_ci_wif_user" {
  count              = var.enable_github_wif ? 1 : 0
  service_account_id = google_service_account.github_ci[0].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_ci[0].name}/attribute.repository_id/${var.github_repository_id}"
}

# roles/bigquery.user permits creating new ephemeral datasets (for which the
# creator automatically receives bigquery.datasets.delete and table/routine/row
# admin rights) and running query/dry-run jobs without granting write access to
# pre-existing datasets in the project.
resource "google_project_iam_member" "github_ci_bigquery_user" {
  count   = var.enable_github_wif ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.github_ci[0].email}"
}

# Prerequisite: var.ci_offload_bucket must have Uniform Bucket-Level Access
# (UBLA) enabled so Cloud Storage evaluates the conditional IAM binding below.
# The resource.name.startsWith(".../objects/it-offload-") condition scopes
# per-object create/get/delete calls (TestLiveGCSOffloadAndQuarantineReplay
# records uploaded object URIs at the write boundary and deletes each object
# directly without requiring bucket-wide storage.objects.list; operators should
# also configure a 1-day Delete lifecycle rule on prefix "it-offload-").
resource "google_storage_bucket_iam_member" "github_ci_offload_object_user" {
  count  = var.enable_github_wif && var.ci_offload_bucket != "" ? 1 : 0
  bucket = var.ci_offload_bucket
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.github_ci[0].email}"

  condition {
    title       = "ci_offload_it_prefix_only"
    description = "Restrict CI test object access to objects under the it-offload- prefix."
    expression  = "resource.name.startsWith(\"projects/_/buckets/${var.ci_offload_bucket}/objects/it-offload-\")"
  }
}

output "ci_workload_identity_provider" {
  description = "Full Workload Identity Provider resource name for GitHub Actions GCP_WORKLOAD_IDENTITY_PROVIDER repository variable."
  value       = var.enable_github_wif ? google_iam_workload_identity_pool_provider.github_ci[0].name : null
}

output "ci_service_account_email" {
  description = "Email of the GitHub Actions CI service account for GCP_SERVICE_ACCOUNT repository variable."
  value       = var.enable_github_wif ? google_service_account.github_ci[0].email : null
}
