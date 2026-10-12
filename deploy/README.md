# Deployment Templates

This directory contains reference deployment templates for the BigQuery OTel
Exporter custom Collector. Each subdirectory targets a different deployment
model.

## Structure

```
deploy/
  helm/bigquery-otel-collector/   Helm chart for Kubernetes / GKE
  cloudrun/service.yaml            Cloud Run v2 service manifest
  terraform/                       Terraform module for BigQuery provisioning
```

## Deployment Models

See [docs/deployment-models.md](../docs/deployment-models.md) for a detailed
comparison of the four deployment models:

| Model | Template | Best for |
|:---|:---|:---|
| **A. Sidecar** | Helm chart (1 replica, DaemonSet) | Single-node K8s, pod-local collector |
| **B. Collector Gateway** | Helm chart (N replicas) | Multi-node K8s, central ingestion |
| **C. Cloud Run** | `deploy/cloudrun/` | Serverless, auto-scaling, no K8s |
| **D. Docker Compose** | `example/docker-compose.yml` | Development, single-host |

## Quick Start

### Kubernetes / GKE with Helm

```bash
# Provision BigQuery tables first
cd deploy/terraform
terraform init
terraform apply -var="project_id=my-project"

# Install the collector
helm install otel-collector deploy/helm/bigquery-otel-collector \
  --set bigquery.project=my-project \
  --set bigquery.dataset=otel
```

### Cloud Run

```bash
# Provision BigQuery and IAM
cd deploy/terraform
terraform apply -var="project_id=my-project"

# Deploy the collector
gcloud run services replace deploy/cloudrun/service.yaml
```

### Authentication

All deployment models use Google Cloud authentication:

- **Application Default Credentials (ADC)**: Default. The collector discovers
  credentials from the environment (metadata server, env var, or well-known
  file).
- **Workload Identity Federation (GKE)**: Annotate the Kubernetes ServiceAccount
  with the GCP service account email (see `values.yaml`).
- **Service Account Key (anywhere)**: Set `GOOGLE_APPLICATION_CREDENTIALS` to
  the key file path.
- **Impersonation**: Set `credentials.impersonate_service_account` in the
  collector config.

## IAM Permissions

Terraform creates the dataset and every table and view, so by default it
grants the collector's service account one custom role, `otelCollectorWriter`,
on that dataset only:

| Permission | Purpose |
|:---|:---|
| `bigquery.datasets.get` | Startup dataset metadata validation |
| `bigquery.tables.get` | Startup table schema validation |
| `bigquery.tables.updateData` | Append rows via Storage Write API |

Run the collector with `auto_create` off on this path. The Cloud Run manifest
turns off the image's default `auto_create` with `--set` flags, and the Helm
chart leaves it off unless you set `bigquery.autoCreate*`.

A collector that creates or reconciles its own tables, views, or search
indexes needs more. Set `-var="grant_auto_create=true"` to grant it:

| Permission | Scope | Purpose |
|:---|:---|:---|
| `bigquery.tables.create` | Dataset | `auto_create.tables` / `companion_views`: create missing tables and views |
| `bigquery.tables.update` | Dataset | `auto_create.tables` / `companion_views`: reconcile additive columns and stale view definitions |
| `bigquery.tables.createIndex` | Dataset | `auto_create.search_indexes` |
| `bigquery.jobs.create` | Project | `auto_create.search_indexes`: run the `CREATE SEARCH INDEX` job |

`auto_create.dataset` is never needed here, because Terraform creates the dataset.

## Persistent Queue

For crash-replay durability, enable the `file_storage` extension:

- **Helm**: Set `persistentQueue.enabled: true` (provisions a PVC)
- **Cloud Run**: Not supported (stateless)
- **Docker Compose**: Uncomment the `file_storage` extension in the example
  config and mount a volume

See [docs/deployment-models.md](../docs/deployment-models.md) for more details.

## Enterprise Governance

### CMEK Encryption

Set `kms_key_name` in the Terraform module to encrypt the BigQuery dataset and
all tables with a Cloud KMS key:

```bash
terraform apply \
  -var="project_id=my-project" \
  -var="kms_key_name=projects/my-project/locations/us-central1/keyRings/otel/cryptoKeys/bq"
```

Terraform also grants the BigQuery encryption service agent decrypt permission
on the key automatically. When `kms_key_name` is empty (the default), the
dataset uses Google-managed encryption.

### Labels

Set `labels` in the Terraform module to apply labels to the dataset, all
tables, and all views for cost allocation and governance:

```bash
terraform apply \
  -var="project_id=my-project" \
  -var='labels={environment="production",team="observability"}'
```

### GCS Offload IAM

Set `offload_bucket` in the Terraform module to grant the collector service
account `roles/storage.objectCreator` and `roles/storage.objectViewer` on the
bucket so the collector can upload large payloads and verify them:

```bash
terraform apply \
  -var="project_id=my-project" \
  -var="offload_bucket=my-otel-offload-bucket"
```

The bucket must already exist. Configure a GCS lifecycle policy to manage
retention of offloaded objects.

### Metrics Pipeline

All deployment templates wire the `metrics` pipeline by default, writing to
`otel_metrics`. Disable it by removing the `metrics:` entry from
`service.pipelines` in the collector config.

### Quarantine and Replay

Enable the quarantine dead-letter table by setting `quarantine.enabled: true`
in the collector config. Use `cmd/bq-quarantine-replay` to replay quarantined
records after fixing the underlying issue.

### Redaction

Enable client-side PII and secret redaction by setting `redaction.enabled: true`
in the collector config. Redaction runs at Step 0, before offload,
fingerprinting, and quarantine.

