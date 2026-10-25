# Implementation Provenance and Architectural Lineage

This document records the cleanroom implementation provenance, licensing, and architectural evolution of the OpenTelemetry Collector BigQuery Exporter (`github.com/haiyuan-eng-google/BQ-OTEL-Exporter`) for open-source distribution and upstream contribution to `open-telemetry/opentelemetry-collector-contrib`.

## 1. Cleanroom Implementation Statement

`BQ-OTEL-Exporter` is an independent, cleanroom implementation authored from scratch against the public specifications of:

- **OpenTelemetry Collector Component API** (`go.opentelemetry.io/collector/component`, `go.opentelemetry.io/collector/exporter`, `go.opentelemetry.io/collector/exporter/exporterhelper`, `go.opentelemetry.io/collector/pdata`);
- **OpenTelemetry Protocol (OTLP) and Semantic Conventions** (`pcommon`, `ptrace`, `plog`, `pmetric`, OpenTelemetry GenAI semantic conventions `v1.37`–`v1.40+`, and Model Context Protocol semantic conventions);
- **Google Cloud BigQuery and Cloud Storage APIs** (`cloud.google.com/go/bigquery`, `cloud.google.com/go/bigquery/storage/managedwriter`, and the Cloud Storage JSON upload API).

As required by [`CONTRIBUTING.md`](../CONTRIBUTING.md):

1. **No Forked or Copied Third-Party Exporter Code**: This repository is not a fork, port, or adaptation of any prior third-party OTLP-to-BigQuery exporter (including prior closed upstream pull requests such as `open-telemetry/opentelemetry-collector-contrib#34809` or `#46280`). No source files, proto definitions, schema layouts, or test fixtures were copied from external exporter implementations.
2. **Copyright and License Hygiene**: Every Go source file in this repository carries the two-line Apache-2.0 header:
   ```go
   // Copyright 2026 Google LLC
   // SPDX-License-Identifier: Apache-2.0
   ```
   The repository is distributed under the [Apache License, Version 2.0](../LICENSE).
3. **Behavioral Reference Policy**: Where upstream OpenTelemetry Collector components (such as `exporter/googlecloudexporter` or `exporter/otlpexporter`) were inspected for Collector-idiomatic `metadata.yaml` conventions, `mdatagen` integration, or thin-wrapper module boundaries, only the public Collector component contracts were followed.

## 2. Architectural Evolution (`v0alpha1` -> `v0alpha2` -> `v0.1.0`)

The exporter and its relational BigQuery contract evolved through three documented milestones inside this repository:

### `v0alpha1` — Initial Storage Write API Transport & Dual-Timestamp Relational Contract

- Established the core design principle documented in [`docs/schema-rationale.md`](schema-rationale.md): BigQuery tables store a **versioned relational projection of validated OTLP fields** (`otel_spans`, `otel_logs`), keeping evolving application and GenAI semantic conventions in replaceable SQL views rather than brittle table columns.
- Introduced dual-timestamp storage (`TIMESTAMP` for BigQuery partitioning/querying alongside `INT64` `*_unix_nano` for exact nanosecond ordering) and plain `JSON` attribute payloads paired with deterministic `attributes_schema` type sidecars so `int64` precision beyond $2^{53}$ and byte slices are preserved without forcing `UNNEST` into analytical queries.
- Implemented single-owner retry semantics over `exporterhelper` with `managedwriter` stream-level retries disabled, bounded per-request protobuf splitting (`write.max_request_bytes`), and deterministic log identity fingerprinting (`fp1:<sha256>`).

### `v0alpha2` — Full Three-Signal Coverage, Safety Controls & Enterprise Analytics

- **Unified Metrics Signal (`otel_metrics`)**: Added a 40-column relational table and writer pipeline (`exporter_metrics.go`, `internal/transform/metrics.go`) supporting all five OTLP metric data-point types (`GAUGE`, `SUM`, `HISTOGRAM`, `EXPONENTIAL_HISTOGRAM`, `SUMMARY`) with exemplar preservation and `otel_metrics_dedup` window views.
- **Dead-Letter Quarantine & Replay (`otel_quarantine`)**: Added opt-in dead-letter persistence for structurally rejected or permanently unappendable records together with the `cmd/bq-quarantine-replay` recovery CLI (`internal/quarantinereplay/`).
- **Step-0 Client-Side Redaction (`internal/redact/`)**: Integrated key-pattern and value-regex credential/PII scrubbing ahead of payload offloading, size checks, log/metric fingerprinting, and quarantine persistence so sensitive values never reach BigQuery, Cloud Storage, or deterministic hashes.
- **Content-Addressed GCS Payload Offloading (`internal/offload/`)**: Added conditional `ifGenerationMatch=0` Cloud Storage offloading for oversized or multimodal attributes, recording BigQuery `ObjectRef` entries in `payload_refs`.
- **Adaptive AIMD Backpressure & Stream Pool Sharding (`internal/bigquery/`)**: Added additive-increase/multiplicative-decrease request-byte and concurrency throttling (`write.backpressure`) and per-table default-stream pool sharding (`write.stream_pool_size`).
- **Companion Views, Governance & Continuous Queries (`internal/schema/`, `deploy/`)**: Expanded the companion SQL suite to 11 views (`otel_spans_as_allspans` plus 10 `v_gen_ai_*` and `v_mcp_*` views, including recursive multi-agent trajectory reconstruction `v_gen_ai_agent_trajectory` and MCP JSON-RPC analytics `v_mcp_spans` / `v_mcp_tool_usage`), Terraform Data Catalog policy-tag/masking support (`deploy/terraform/governance.tf`), and BigQuery Continuous Queries alerting templates (`deploy/continuous-queries/`).

### `v0.1.0` — Supply-Chain Attestation, Exported Constructor Seams & Upstream Contrib Readiness

- **Exported Constructor Seams (`factory.go`)**: Exported `CreateDefaultConfig`, `CreateTracesExporter`, `CreateLogsExporter`, and `CreateMetricsExporter` alongside `NewFactory` so an upstream `opentelemetry-collector-contrib/exporter/bigqueryexporter` wrapper can wire Collector-generated `exporter.Settings` directly without duplicating transport logic.
- **Upstream `mdatagen` Compatibility (`metadata.yaml`)**: Declared `tests.goleak.ignore.top` in `metadata.yaml` for stock `mdatagen` compatibility while preserving build-tag-isolated unit and live-integration goroutine leak checks (`generated_package_test.go`, `goleak_options_unit_test.go`, `goleak_options_integration_test.go`).
- **Release Supply-Chain Verification (`.github/workflows/release.yml`, `.goreleaser.yaml`)**: Added keyless Cosign signing, SLSA v1.0 build provenance attestations (`actions/attest-build-provenance@v4`) for both the multi-arch GHCR container manifest and GoReleaser binary archive checksums (`dist/checksums.txt`), and Syft SPDX SBOM generation and attachment for both binary archives and container images.
