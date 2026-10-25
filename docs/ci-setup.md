# Live BigQuery & GCS Integration CI Setup

The `.github/workflows/live-integration.yml` workflow runs the full `-tags=integration` suite against a dedicated non-production Google Cloud test project on a nightly schedule (`42 4 * * *`) and via manual `workflow_dispatch`.

Authentication uses keyless OpenID Connect (OIDC) through Google Cloud Workload Identity Federation (`google-github-actions/auth@v3`) — no long-lived service account JSON keys are stored in GitHub.

---

## 1. Provision Workload Identity Federation via Terraform

`deploy/terraform/ci_wif.tf` defines an opt-in Workload Identity Pool, OIDC Provider, and dedicated least-privilege CI service account gated by `enable_github_wif = true`:

- **`google_iam_workload_identity_pool.github_ci`** — Dedicated pool (`bq-otel-exporter-ci` by default).
- **`google_iam_workload_identity_pool_provider.github_ci`** — OIDC provider for `https://token.actions.githubusercontent.com` with an attribute condition pinning the immutable numeric `repository_owner_id` and `repository_id`, repository name, `refs/heads/main`, the workflow path (`.github/workflows/live-integration.yml`), and allowed trigger events (`schedule`, `workflow_dispatch`):
  ```hcl
  attribute_condition = join(" && ", [
    "attribute.repository_owner_id == \"${var.github_repository_owner_id}\"",
    "attribute.repository_id == \"${var.github_repository_id}\"",
    "attribute.repository == \"${var.github_repository}\"",
    "attribute.ref == \"refs/heads/main\"",
    "attribute.workflow_ref == \"${var.github_repository}/${var.github_workflow_ref}@refs/heads/main\"",
    "attribute.event_name in [${join(", ", formatlist("%q", var.github_allowed_events))}]",
  ])
  ```
- **`google_service_account.github_ci`** — Dedicated CI runner identity (`bq-otel-exporter-ci@<project>.iam.gserviceaccount.com`) bound by `attribute.repository_id/${var.github_repository_id}` and granted:
  - `roles/bigquery.user` on the dedicated CI test project so integration tests can create and tear down their own ephemeral `otel_exporter_it_*`, `otel_exporter_recovery_it_*`, `otel_cq_it_*`, and `otel_offload_it_*` datasets, run query/DDL jobs, and stream rows into those ephemeral datasets via the BigQuery Storage Write API without write access to pre-existing datasets.
  - `roles/storage.objectUser` on the dedicated CI test bucket (`ci_offload_bucket`) with an IAM condition restricting object operations to the `it-offload-` prefix (`resource.name.startsWith("projects/_/buckets/${var.ci_offload_bucket}/objects/it-offload-")`).
    - **Prerequisite (`ci_offload_bucket`)**: Enable **Uniform Bucket-Level Access (UBLA)** (`uniform_bucket_level_access = true`) on `ci_offload_bucket` (required by Cloud Storage for conditional IAM bindings) and configure a 1-day Object Lifecycle Management `Delete` rule on `matchesPrefix = ["it-offload-"]` as a safety net for abruptly terminated runs.

Look up your repository's immutable numeric `id` and `owner.id` with `gh`:

```bash
REPO_ID="$(gh api repos/haiyuan-eng-google/BQ-OTEL-Exporter --jq '.id')"
OWNER_ID="$(gh api repos/haiyuan-eng-google/BQ-OTEL-Exporter --jq '.owner.id')"
```

Apply the WIF resources against your designated non-production CI test project:

```bash
cd deploy/terraform

terraform init
terraform apply \
  -var="project_id=YOUR_CI_GCP_PROJECT" \
  -var="enable_github_wif=true" \
  -var="github_repository=haiyuan-eng-google/BQ-OTEL-Exporter" \
  -var="github_repository_id=${REPO_ID}" \
  -var="github_repository_owner_id=${OWNER_ID}" \
  -var="ci_offload_bucket=YOUR_CI_OFFLOAD_TEST_BUCKET"
```

Read back the provisioned provider resource name and service account email:

```bash
WIF_PROVIDER="$(terraform output -raw ci_workload_identity_provider)"
CI_SA_EMAIL="$(terraform output -raw ci_service_account_email)"
```

---

## 2. Configure GitHub Repository Variables

Set the repository variables consumed by `.github/workflows/live-integration.yml` using the GitHub CLI (`gh`):

```bash
gh variable set BQ_PROJECT \
  --repo haiyuan-eng-google/BQ-OTEL-Exporter \
  --body "YOUR_CI_GCP_PROJECT"

gh variable set BQ_LOCATION \
  --repo haiyuan-eng-google/BQ-OTEL-Exporter \
  --body "US"

gh variable set GCP_WORKLOAD_IDENTITY_PROVIDER \
  --repo haiyuan-eng-google/BQ-OTEL-Exporter \
  --body "${WIF_PROVIDER}"

gh variable set GCP_SERVICE_ACCOUNT \
  --repo haiyuan-eng-google/BQ-OTEL-Exporter \
  --body "${CI_SA_EMAIL}"

# Optional: enables live GCS bucket round-trip in TestLiveGCSOffloadAndQuarantineReplay
gh variable set BQ_OFFLOAD_TEST_BUCKET \
  --repo haiyuan-eng-google/BQ-OTEL-Exporter \
  --body "YOUR_CI_OFFLOAD_TEST_BUCKET"
```

| Repository Variable | Required | Description |
|---|---|---|
| `BQ_PROJECT` | Yes | Dedicated non-production Google Cloud project ID where ephemeral integration datasets (`otel_exporter_it_*`, `otel_exporter_recovery_it_*`, `otel_cq_it_*`, `otel_offload_it_*`) are created and deleted. |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | Yes | Full Workload Identity Provider resource path (`projects/<num>/locations/global/workloadIdentityPools/<pool>/providers/<provider>`). |
| `GCP_SERVICE_ACCOUNT` | Yes | Email of the CI service account impersonated by `google-github-actions/auth@v3`. |
| `BQ_LOCATION` | No (defaults to `US`) | BigQuery dataset location used by the integration suite. |
| `BQ_OFFLOAD_TEST_BUCKET` | No | Dedicated GCS test bucket name (or `BQ_OFFLOAD_BUCKET` when running locally) with Uniform Bucket-Level Access enabled, used by `TestLiveGCSOffloadAndQuarantineReplay` to verify live GCS upload, Step-0 redaction before upload, BigQuery `ObjectRef` columns, and `quarantinereplay.Replay`. When `BQ_OFFLOAD_TEST_BUCKET` is set in CI, the workflow automatically sets `REQUIRE_LIVE_GCS=1`. |

> **Bootstrap Behavior Before Variables Are Configured:** If `BQ_PROJECT`, `GCP_WORKLOAD_IDENTITY_PROVIDER`, or `GCP_SERVICE_ACCOUNT` is not yet configured in repository variables, the `check-config` step emits a GitHub Actions `::warning::` annotation and skips the checkout, authentication, and test steps cleanly so scheduled runs do not fail on unconfigured forks. Once all three variables are set, `REQUIRE_LIVE_BQ=1` (and `REQUIRE_LIVE_GCS=1` when `BQ_OFFLOAD_TEST_BUCKET` is set) enforces fail-closed execution.

---

## 3. Security & Fork-PR Isolation

1. **No `pull_request` trigger on untrusted forks**: `.github/workflows/live-integration.yml` triggers only on `schedule` (`42 4 * * *`) and `workflow_dispatch`.
2. **Immutable ID, branch, workflow, and event OIDC condition**: Even if another workflow were triggered from a non-`main` branch or renamed repository, the GCP Workload Identity Pool Provider enforces `attribute.repository_owner_id`, `attribute.repository_id`, `attribute.repository`, `attribute.ref == "refs/heads/main"`, `attribute.workflow_ref`, and `attribute.event_name` at the IAM token-exchange boundary.
3. **Fail-closed enforcement once configured**: When `BQ_PROJECT`, `GCP_WORKLOAD_IDENTITY_PROVIDER`, and `GCP_SERVICE_ACCOUNT` are set, the workflow sets `REQUIRE_LIVE_BQ=1` (and `REQUIRE_LIVE_GCS=1` when `BQ_OFFLOAD_TEST_BUCKET` is set), causing the live test suite to call `t.Fatal` rather than `t.Skip` if any prerequisite is missing.
4. **Ephemeral resource cleanup**: All four live tests create uniquely suffixed datasets (`_<unix_milli>_<rand_hex>`) via `Dataset.Create` before exporter startup and register `DeleteWithContents` only when `Dataset.Create` returns `nil` (never deleting a pre-existing dataset on `409 AlreadyExists`). `TestLiveGCSOffloadAndQuarantineReplay` writes under a unique `it-offload-<unix_nano>` object prefix and records every uploaded GCS object URI at the `WriteObject` boundary so `t.Cleanup` deletes each object directly via `storage.objects.delete` (satisfying the object-prefix IAM condition without requiring bucket-wide `storage.objects.list`).
