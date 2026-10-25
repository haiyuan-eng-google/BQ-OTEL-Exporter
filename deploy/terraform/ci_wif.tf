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
  description                        = "OIDC provider restricted to ${var.github_repository} on refs/heads/main"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }

  # Restrict token exchange to the configured repository and main branch so
  # fork pull requests and non-main branches cannot mint credentials.
  attribute_condition = "assertion.repository == \"${var.github_repository}\" && assertion.ref == \"refs/heads/main\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
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
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_ci[0].name}/attribute.repository/${var.github_repository}"
}

# The live integration suite creates and deletes ephemeral datasets
# (otel_it_*, otel_ac_*, otel_rec_*, otel_cq_*, otel_offload_*), runs query and
# DDL jobs, and streams rows via the BigQuery Storage Write API.
resource "google_project_iam_member" "github_ci_bigquery_user" {
  count   = var.enable_github_wif ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.github_ci[0].email}"
}

resource "google_project_iam_member" "github_ci_bigquery_job_user" {
  count   = var.enable_github_wif ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.github_ci[0].email}"
}

resource "google_project_iam_member" "github_ci_bigquery_data_editor" {
  count   = var.enable_github_wif ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.github_ci[0].email}"
}

resource "google_bigquery_dataset_iam_member" "github_ci_telemetry_data_editor" {
  count      = var.enable_github_wif ? 1 : 0
  project    = var.project_id
  dataset_id = google_bigquery_dataset.otel.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.github_ci[0].email}"
}

resource "google_storage_bucket_iam_member" "github_ci_offload_object_admin" {
  count  = var.enable_github_wif && var.offload_bucket != "" ? 1 : 0
  bucket = var.offload_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.github_ci[0].email}"
}

output "ci_workload_identity_provider" {
  description = "Full Workload Identity Provider resource name for GitHub Actions GCP_WORKLOAD_IDENTITY_PROVIDER repository variable."
  value       = var.enable_github_wif ? google_iam_workload_identity_pool_provider.github_ci[0].name : null
}

output "ci_service_account_email" {
  description = "Email of the GitHub Actions CI service account for GCP_SERVICE_ACCOUNT repository variable."
  value       = var.enable_github_wif ? google_service_account.github_ci[0].email : null
}
