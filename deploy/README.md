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

When Terraform provisions the tables, Terraform governs their retention,
partition filter requirement, CMEK key, labels, and max staleness, through the
variables below. The collector's table options (`partition_expiration_days`,
`require_partition_filter`, `kms_key_name`, `labels`, and `max_staleness` for
each signal and for quarantine, and the top-level `kms_key_name` and `labels`)
apply only to a dataset, table, or view that the collector creates itself with
`auto_create.dataset`, `auto_create.tables`, or `auto_create.companion_views`.
The collector never changes them on one that already exists, so on tables
Terraform created, the same options in the collector config, the Helm values,
or the Cloud Run overrides have no effect. Set them here instead.

### CMEK Encryption

Set `kms_key_name` in the Terraform module to encrypt the BigQuery dataset and
all tables with a Cloud KMS key. The key must be in the dataset's location: a
dataset in the `US` multi-region needs a key in `locations/us`, one in the `EU`
multi-region a key in `locations/europe`, and one in a region such as
`us-central1` a key in that same region, `locations/us-central1`. The module
creates the dataset in `US` unless you set `location`, so a regional key needs
the matching `location`:

```bash
terraform apply \
  -var="project_id=my-project" \
  -var="location=us-central1" \
  -var="kms_key_name=projects/my-project/locations/us-central1/keyRings/otel/cryptoKeys/bq"
```

Terraform also grants the BigQuery encryption service agent
`roles/cloudkms.cryptoKeyEncrypterDecrypter` on the key automatically. When
`kms_key_name` is empty (the default), the dataset uses Google-managed
encryption.

### Max Staleness

Set `max_staleness` in the Terraform module to set BigQuery's `max_staleness`
option on the tables: the maximum staleness of the data a query over a table
may return. Within that window, BigQuery can answer a query from the changes it
has already applied instead of applying pending changes, such as change data
capture upserts and deletes, at query time. The option bounds how stale results
may be; it does not exclude recent partitions from queries.

```bash
terraform apply \
  -var="project_id=my-project" \
  -var="max_staleness=15m"
```

The value is either a Go duration, as the collector's `max_staleness` options
take it, in whole seconds from `1s` to `168h` (for example `30s`, `15m`, or
`1h30m`), or a BigQuery SQL interval string (for example `0-0 0 0:15:0`).
Terraform converts a duration to the interval the collector writes for it, so
`15m` is sent as `0-0 0 0:15:0`, and rejects a duration outside that range or
one that is not a whole number of seconds. An interval string is passed to
BigQuery as given; only its format is checked. The option is applied to all
four signal and quarantine tables, never to views.

### Partition Retention and Partition Filters

Set these variables in the Terraform module to expire old partitions and to
require a partition filter on queries:

| Variable | Default | Applies to |
|:---|:---|:---|
| `spans_partition_expiration_days` | `0` | `otel_spans` (hourly partitions on `start_time`) |
| `logs_partition_expiration_days` | `0` | `otel_logs` (daily partitions on `timestamp`) |
| `metrics_partition_expiration_days` | `0` | `otel_metrics` (daily partitions on `timestamp`) |
| `quarantine_partition_expiration_days` | `30` | `otel_quarantine` (daily partitions on `ingest_time`) |
| `require_partition_filter` | `false` | All four tables |

```bash
terraform apply \
  -var="project_id=my-project" \
  -var="spans_partition_expiration_days=365" \
  -var="logs_partition_expiration_days=90" \
  -var="metrics_partition_expiration_days=90"
```

An expiration is a whole number of days, and `0` leaves partitions without
expiration. For the signal tables this matches the collector's
`partition_expiration_days` options, but not for quarantine: the collector
treats `quarantine.partition_expiration_days: 0` as its 30-day default, while
`quarantine_partition_expiration_days=0` here keeps quarantined records
indefinitely. BigQuery allows at most 10,000 partitions per table, which is
about 416 days of hourly `otel_spans` partitions, so with the default `0` the
spans table eventually reaches that limit; set
`spans_partition_expiration_days` to 416 or less to stay under it.

With `require_partition_filter=true`, BigQuery rejects a query over these
tables, or over the views built on them, that has no filter BigQuery can use
to skip partitions. See "Filtering the GenAI views by time" under
[Operating v0alpha2 tables](../README.md#operating-v0alpha2-tables) for the
columns the views take. Queries over `v_gen_ai_logs` and `v_gen_ai_metric_exemplars` are rejected even with a
filter, because no such filter reaches their scan of `otel_spans`, so leave the
option off for a deployment that queries those views. A collector started
with `auto_create.companion_views: true` against an `otel_spans` table that
requires a partition filter skips those two views with a warning, creates the
other companion views, and starts; set `auto_create.allow_unprunable_views: true`
(Helm: `bigquery.autoCreateAllowUnprunableViews: true`) to create the two views
anyway. The collector reads the table's metadata to decide, and its own
`traces.require_partition_filter` counts only while `otel_spans` does not exist
yet, because that option applies only when the collector creates the table.

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

### Column-Level Security and Data Masking

Some STRING columns carry text from the telemetry as it was sent, which can
include prompts, responses, or error details: `otel_spans.status.message`,
`otel_logs.severity_text`, and `otel_quarantine.raw_payload` and
`error_message`. Set `enable_column_level_security=true` in the Terraform
module to put them behind a policy tag and a data masking policy:

| Variable | Default | Purpose |
|:---|:---|:---|
| `enable_column_level_security` | `false` | Create the taxonomy, policy tag, data masking policy, reader bindings, and redacted views, and attach the tag to the columns above |
| `policy_taxonomy_display_name` | `otel_telemetry_sensitivity` | Display name of the taxonomy, which must be unique among the taxonomies of the project and location |
| `masking_rule` | `SHA256` | What a Masked Reader reads in place of a value: `SHA256` (its SHA-256 hash), `ALWAYS_NULL`, or `DEFAULT_MASKING_VALUE` (an empty string) |
| `fine_grained_reader_members` | `[]` | Principals granted the Fine-Grained Reader role on the policy tag |
| `masked_reader_members` | `[]` | Principals granted the Masked Reader role on the data masking policy |

```bash
terraform apply \
  -var="project_id=my-project" \
  -var="enable_column_level_security=true" \
  -var='fine_grained_reader_members=["group:otel-admins@example.com"]' \
  -var='masked_reader_members=["group:otel-analysts@example.com"]'
```

Enable the Data Catalog API (`datacatalog.googleapis.com`) and the BigQuery
Data Policy API (`bigquerydatapolicy.googleapis.com`) in the project first.
The identity that runs Terraform also needs to create taxonomies and data
policies and set their IAM policies, and `bigquery.tables.setCategory` on the
tables to attach the tag. The `policy_tag_id` and `data_policy_id` outputs
name the policy tag and the data masking policy, and are `null` while the
option is off. Try the option on a scratch project before you turn it on for
an existing deployment.

Three layers keep this text from readers who should not see it:

1. **Collector redaction.** With `redaction.enabled: true` in the collector
   config, the collector scrubs PII and secrets before it writes a row (see
   [Redaction](#redaction)).
2. **Policy tag and data masking.** BigQuery returns a tagged column to a
   reader as the table below shows.
3. **Redacted views.** `v_otel_spans_redacted` and `v_otel_logs_redacted` read
   `otel_spans_dedup` and `otel_logs_dedup` but return `NULL` for every tagged
   column and every JSON column, including those inside records, and leave out
   `events`, `links`, and `payload_refs`.

| Reader | Tagged columns |
|:---|:---|
| Fine-Grained Reader, with or without Masked Reader | Read as stored |
| Masked Reader only | Read masked by `masking_rule` |
| Neither role | A query that reads one fails with an access denied error |

Each binding is authoritative for its role: Terraform removes any other member
of that role on the policy tag or the data masking policy. The roles decide
only what a reader gets from the tagged columns; a reader still needs read
access to the tables, as without this option. Views do not lift these rules:
the `_dedup` views and the companion views that select `status.message` or
`severity_text` (`otel_spans_as_allspans`, `v_gen_ai_spans`, and
`v_gen_ai_logs`) read the tagged columns too. Exporting quarantined rows for
`cmd/bq-quarantine-replay` reads `raw_payload`, so only a Fine-Grained Reader
exports the original payloads.

No JSON column is tagged, including `attributes`, `body`, and
`otel_quarantine.raw_attributes`, so a reader who can read a table reads its
JSON columns. The redacted views are ordinary views in the same dataset:
BigQuery checks a query over one against the reader's own access to the
tables it reads. They keep the JSON columns out of a query's results, but do
not stop a reader who can read the tables from reading those columns there.
To keep a reader off the tables, give the reader views like these in a
separate dataset, authorized on this one.

Terraform grants the collector neither role, because it only appends rows.
The collector's role does not include `bigquery.tables.setCategory`, which
changing a column's policy tags takes, and the collector never changes them: a
collector that runs with `auto_create.tables` (see
[IAM Permissions](#iam-permissions)) sends each column's policy tags back as
they are in the schema update that adds missing columns, and a restart against
a table that already has every column sends no update.
