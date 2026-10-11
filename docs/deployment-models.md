# Deployment Models

The BigQuery OTel Exporter is an OpenTelemetry Collector component. Every
deployment model involves running a Collector process that receives OTLP
telemetry and writes it to BigQuery. This document covers the four models,
their tradeoffs, and authentication options.

## Overview

```mermaid
flowchart TD
    subgraph Producers["Agent Frameworks"]
        ADK["Google ADK"]
        LG["LangGraph"]
        CC["Claude Agent SDK"]
        OA["OpenAI SDK"]
        CA["CrewAI"]
    end

    subgraph Collector["OTel Collector"]
        GRPC["OTLP/gRPC :4317"]
        HTTP["OTLP/HTTP :4318"]
        BQ["bigquery exporter"]
    end

    ADK -->|HTTP only| HTTP
    LG -->|HTTP only| HTTP
    CC -->|gRPC or HTTP| GRPC
    CC -->|gRPC or HTTP| HTTP
    OA -->|gRPC| GRPC
    CA -->|HTTP| HTTP

    GRPC --> BQ
    HTTP --> BQ
    BQ -->|Storage Write API| BQDest["BigQuery
    otel_spans, otel_logs"]
```

## Model A: Sidecar Collector

Each pod runs a Collector alongside the application. The Collector binds a
loopback address so only the pod's application can reach it.

**Pros:**
- No network hop — telemetry stays in the pod.
- Simple isolation: one Collector per application.
- No shared failure domain.

**Cons:**
- One Collector per pod increases resource overhead.
- No central processing or filtering before BigQuery.

**Best for:** Single-node Kubernetes, development, and applications with
strict data-locality requirements.

**Template:** `deploy/helm/bigquery-otel-collector/` with `replicaCount: 1`
or as a DaemonSet.

## Model B: Collector Gateway / DaemonSet

A pool of Collector pods behind a Kubernetes Service. Applications send
telemetry to the Service, which load-balances across Collector instances.

**Pros:**
- Central PII redaction, sampling, and attribute filtering before BigQuery.
- Fewer Collector instances (shared pool).
- Horizontal scaling under load.

**Cons:**
- Network hop from application to gateway.
- Shared failure domain (a gateway outage affects all producers).

**Best for:** Production multi-node clusters, centralized telemetry pipelines.

**Template:** `deploy/helm/bigquery-otel-collector/` with `replicaCount: N`.

## Model C: Cloud Run

The Collector runs as a Cloud Run service. Applications send OTLP over HTTP
(the only option for some frameworks) to the Cloud Run endpoint.

**Pros:**
- No Kubernetes to manage.
- Auto-scaling to zero when idle.
- Pay-per-use (no idle cost).

**Cons:**
- Stateless — no persistent queue for crash-replay.
- Cold start latency for the first request.
- gRPC support depends on the Cloud Run revision (HTTP is always available).

**Best for:** Serverless deployments, bursty workloads, teams without
Kubernetes expertise.

**Template:** `deploy/cloudrun/service.yaml`

## Model D: Docker Compose

Single-host deployment using Docker Compose. The collector runs in a
hardened container with read-only filesystem, dropped capabilities, and
no-new-privileges.

**Best for:** Development, testing, and single-host production.

**Template:** `example/docker-compose.yml`

## Authentication

All models authenticate to BigQuery using one of:

| Method | Config | Best for |
|:---|:---|:---|
| **Application Default Credentials** | Default (no config) | GCE, GKE, Cloud Run, Cloud Build |
| **Service Account Key File** | `credentials.file` or `GOOGLE_APPLICATION_CREDENTIALS` | On-prem, any cloud, development |
| **Workload Identity Federation** | `credentials.impersonate_service_account` | GKE with IAM Conditions |
| **Service Account Impersonation** | `credentials.impersonate_service_account` | Cross-project, least-privilege |
| **No Authentication (loopback)** | `endpoint.without_authentication: true` | Emulators, local testing |

### Workload Identity Federation (GKE)

1. Create a GCP service account with the least-privilege IAM role from
   `deploy/terraform/`.
2. Create a Kubernetes ServiceAccount and annotate it:
   ```yaml
   serviceAccount:
     create: true
     annotations:
       iam.gke.io/gcp-service-account: "otel-collector@my-project.iam.gserviceaccount.com"
   ```
3. Bind the Kubernetes ServiceAccount to the GCP service account with an
   IAM policy binding.

## Transport Requirements

Both OTLP/gRPC (port 4317) and OTLP/HTTP (port 4318) must be available.
Some agent frameworks only support HTTP:

| Framework | gRPC | HTTP |
|:---|:---|:---|
| Google ADK | — | Only option |
| LangGraph | — | Only option |
| Claude Agent SDK | Supported | Supported |
| OpenAI SDK | Supported | Supported |
| CrewAI | — | Only option |

The example config and Helm chart listen on both ports by default.

## Security Hardening

All deployment templates apply:

- `runAsNonRoot: true` / `readOnlyRootFilesystem: true`
- `cap_drop: [ALL]` / `no-new-privileges: true`
- `tmpfs: [/tmp]` for writable scratch space
- Bind to loopback where applicable (Docker Compose, sidecar)
- Health check on port 13133 for liveness/readiness probes
