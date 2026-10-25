# Live BigQuery & GCS Integration CI Setup

The `.github/workflows/live-integration.yml` workflow runs the full `-tags=integration` suite against a real Google Cloud project on every push to `main`, every nightly schedule (`0 6 * * *`), and via manual `workflow_dispatch`.

Authentication uses keyless OpenID Connect (OIDC) through Google Cloud Workload Identity Federation (`google-github-actions/auth@v2`) — no long-lived service account JSON keys are stored in GitHub.

---

## 1. Provision Workload Identity Federation via Terraform

`deploy/terraform/ci_wif.tf` defines an opt-in Workload Identity Pool, OIDC Provider, and dedicated least-privilege CI service account gated by `enable_github_wif = true`:

- **`google_iam_workload_identity_pool.github_ci`** — Dedicated pool (`bq-otel-exporter-ci` by default).
- **`google_iam_workload_identity_pool_provider.github_ci`** — OIDC provider for `https://token.actions.githubusercontent.com` with an attribute condition restricting token exchange to `refs/heads/main` on the configured repository:
  ```hcl
  attribute_condition = "assertion.repository == \"${var.github_repository}\" && assertion.ref == \"refs/heads/main\""
  ```
- **`google_service_account.github_ci`** — Dedicated CI runner identity (`bq-otel-exporter-ci@<project>.iam.gserviceaccount.com`) granted:
  - `roles/bigquery.user`, `roles/bigquery.jobUser`, and `roles/bigquery.dataEditor` so integration tests can create and tear down ephemeral `otel_it_*`, `otel_ac_*`, `otel_rec_*`, `otel_cq_*`, and `otel_offload_*` datasets, run query/DDL jobs, and stream rows via the BigQuery Storage Write API.
  - `roles/storage.objectAdmin` on the GCS payload offload bucket when `offload_bucket` is set.

Apply the WIF resources against your designated CI test project:

```bash
cd deploy/terraform

terraform init
terraform apply \
  -var="project_id=YOUR_CI_GCP_PROJECT" \
  -var="enable_github_wif=true" \
  -var="github_repository=haiyuan-eng-google/BQ-OTEL-Exporter" \
  -var="offload_bucket=YOUR_CI_OFFLOAD_BUCKET"
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
gh variable set BQ_OFFLOAD_BUCKET \
  --repo haiyuan-eng-google/BQ-OTEL-Exporter \
  --body "YOUR_CI_OFFLOAD_BUCKET"
```

| Repository Variable | Required | Description |
|---|---|---|
| `BQ_PROJECT` | Yes | Google Cloud project ID where ephemeral integration datasets (`otel_it_*`, `otel_ac_*`, `otel_rec_*`, `otel_cq_*`, `otel_offload_*`) are created and deleted. |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | Yes | Full Workload Identity Provider resource path (`projects/<num>/locations/global/workloadIdentityPools/<pool>/providers/<provider>`). |
| `GCP_SERVICE_ACCOUNT` | Yes | Email of the CI service account impersonated by `google-github-actions/auth@v2`. |
| `BQ_LOCATION` | No (defaults to `US`) | BigQuery dataset location used by the integration suite. |
| `BQ_OFFLOAD_BUCKET` | No | GCS bucket name used by `TestLiveGCSOffloadAndQuarantineReplay` to verify live GCS upload, Step-0 redaction before upload, BigQuery `ObjectRef` columns, and `quarantinereplay.Replay`. |

---

## 3. Security & Fork-PR Isolation

1. **No `pull_request` trigger on untrusted forks**: `.github/workflows/live-integration.yml` triggers only on `push` to `main`, `schedule`, and `workflow_dispatch`.
2. **Branch & repository OIDC condition**: Even if a workflow were triggered from a non-`main` branch or fork, the GCP Workload Identity Pool Provider enforces `assertion.repository == "<owner>/<repo>" && assertion.ref == "refs/heads/main"` at the IAM token-exchange boundary.
3. **Fail-loud enforcement**: Once `BQ_PROJECT` and `GCP_WORKLOAD_IDENTITY_PROVIDER` are set, the workflow sets `REQUIRE_LIVE_BQ=1`, which causes `integrationTestProject(t)` to call `t.Fatal` rather than `t.Skip` if project configuration is missing.
4. **Ephemeral resource cleanup**: Every live test provisions uniquely suffixed datasets (`_<unix_nano>`) and registers `t.Cleanup` hooks that delete all created BigQuery tables, views, routines, datasets, and GCS test objects when the test completes.
