# Implementation Declarations and Architectural Lineage

This document records maintainer declarations, repository licensing facts, and the architectural contracts of the OpenTelemetry Collector BigQuery Exporter (`github.com/haiyuan-eng-google/BQ-OTEL-Exporter`) in preparation for open-source distribution and upstream contribution to `open-telemetry/opentelemetry-collector-contrib`. It is not an independently verified authorship or provenance attestation.

## 1. Maintainer Declarations and Verification Status

The maintainers declare that `BQ-OTEL-Exporter` was independently authored from scratch against the public specifications below. This historical authorship declaration is pending verification and formal maintainer sign-off; no completed, dated provenance/CLA sign-off is linked in this repository.

- **OpenTelemetry Collector Component API** (`go.opentelemetry.io/collector/component`, `go.opentelemetry.io/collector/exporter`, `go.opentelemetry.io/collector/exporter/exporterhelper`, `go.opentelemetry.io/collector/pdata`);
- **OpenTelemetry Protocol (OTLP) and Semantic Conventions** (`pcommon`, `ptrace`, `plog`, `pmetric`, OpenTelemetry GenAI semantic conventions, and Model Context Protocol semantic conventions);
- **Google Cloud BigQuery and Cloud Storage APIs** (`cloud.google.com/go/bigquery`, `cloud.google.com/go/bigquery/storage/managedwriter`, and the Cloud Storage JSON upload API).

As required by [`CONTRIBUTING.md`](../CONTRIBUTING.md):

1. **Authorship Declaration (Pending Verification)**: The maintainers declare that the component is not a fork or adaptation of an existing OTLP-to-BigQuery exporter (including `open-telemetry/opentelemetry-collector-contrib#46280`), and that no source files, proto definitions, schema layouts, or test fixtures were copied from third-party exporter implementations. This document does not establish those historical assertions. A dated provenance/CLA sign-off must be linked here when completed before upstream donation.
2. **Repository Licensing Facts**: Repository Go files carry the two-line Apache-2.0 header below. Headers establish labeling, not authorship history:
   ```go
   // Copyright 2026 Google LLC
   // SPDX-License-Identifier: Apache-2.0
   ```
   The repository is distributed under the [Apache License, Version 2.0](../LICENSE).
3. **Disclosed Behavioral References**: Upstream `exporter/googlecloudexporter` and `exporter/otlpexporter` were consulted for Collector-idiomatic `metadata.yaml` conventions (including `tests.goleak.ignore.top`), `mdatagen` integration, and thin-wrapper module boundaries. The maintainers declare that these were behavioral references only; independent verification remains pending under `CONTRIBUTING.md`. The destination registry's JSON schema is intentionally vendored unchanged for validation, with its source revision and license recorded in [`testdata/registry/README.md`](../testdata/registry/README.md).
4. **Framework Execution Evidence**: [`docs/agent-frameworks.md`](agent-frameworks.md) reports executions dated 2026-08-12 and links their scenarios and evidence in [issue #1](https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter/issues/1#issuecomment-5277226467). Those are historical reports, not executions established by this release work. The checked-in [`testdata/otlp/manifest.yaml`](../testdata/otlp/manifest.yaml) explicitly labels its fixtures synthetic; fixture tests do not verify those historical runs or authorship.

## 2. Architectural Evolution (`v0alpha1` -> `v0alpha2` -> `v0.1.0`)

The exporter records two pre-release schema-contract milestones (`v0alpha1` and `v0alpha2`, tracked in `internal/schema/schema.go`) ahead of the planned, unreleased inaugural `v0.1.0` release. These sections describe contract snapshots and the current implementation, not first-introduction dates for each feature. The release-candidate section in `CHANGELOG.md` is a cumulative inventory; repeated features across sections are not separate additions.

### Pre-Release Contract Milestone `v0alpha1` — Initial Storage Write API Transport & Dual-Timestamp Relational Contract

- Established the core design principle documented in [`docs/schema-rationale.md`](schema-rationale.md): BigQuery tables store a **versioned relational projection of validated OTLP fields** (`otel_spans`, `otel_logs`), keeping evolving application and GenAI semantic conventions in replaceable SQL views rather than brittle table columns.
- Introduced dual-timestamp storage (`TIMESTAMP` for BigQuery partitioning/querying alongside `INT64` `*_unix_nano` for exact nanosecond ordering) and plain `JSON` attribute payloads paired with deterministic `attributes_schema` type sidecars so `int64` precision beyond $2^{53}$ and byte slices are preserved without forcing `UNNEST` into analytical queries.
- Implemented single-owner retry semantics over `exporterhelper` with `managedwriter` stream-level retries disabled, bounded per-request protobuf splitting (`write.max_request_bytes`), and deterministic log identity fingerprinting (`fp1:<sha256>`).

### Pre-Release Contract Milestone `v0alpha2` — Full Three-Signal Coverage, Safety Controls & Enterprise Analytics

- **Unified Metrics Signal (`otel_metrics`)**: Added a 40-column relational table and writer pipeline (`exporter_metrics.go`, `internal/transform/metrics.go`) supporting all five OTLP metric data-point types (`GAUGE`, `SUM`, `HISTOGRAM`, `EXPONENTIAL_HISTOGRAM`, `SUMMARY`) with exemplar preservation and `otel_metrics_dedup` window views.
- **Dead-Letter Quarantine & Replay (`otel_quarantine`)**: Added opt-in dead-letter persistence for structurally rejected or permanently unappendable records together with the `cmd/bq-quarantine-replay` recovery CLI (`internal/quarantinereplay/`).
- **Step-0 Client-Side Redaction (`internal/redact/`)**: Redaction is disabled by default (`redaction.enabled: false`). When enabled, values matching the built-in/configured key rules or configured value patterns are scrubbed before payload offloading, size checks, log/metric fingerprinting, and quarantine persistence. Unmatched values remain unchanged. Log bodies require the separate `redaction.redact_log_body: true` option (also false by default). See [Redaction](../README.md#redaction) for rule scope and defaults.
- **Content-Addressed GCS Payload Offloading (`internal/offload/`)**: Added conditional `ifGenerationMatch=0` Cloud Storage offloading for oversized or multimodal attributes, recording BigQuery `ObjectRef` entries in `payload_refs`.
- **Adaptive AIMD Backpressure & Stream Pool Sharding (`internal/bigquery/`)**: Added additive-increase/multiplicative-decrease request-byte and concurrency throttling (`write.backpressure`) and per-table default-stream pool sharding (`write.stream_pool_size` in `[1, 16]`).
- **Companion Views, Governance & Continuous Queries (`internal/schema/`, `deploy/`)**: Expanded the companion SQL suite to 11 views (`otel_spans_as_allspans`, `v_gen_ai_spans`, `v_gen_ai_logs`, `v_gen_ai_metrics`, `v_gen_ai_metric_exemplars`, `v_gen_ai_model_costs`, `v_gen_ai_sessions`, `v_gen_ai_tool_usage`, `v_gen_ai_agent_trajectory`, `v_mcp_spans`, and `v_mcp_tool_usage`), Terraform Data Catalog `sensitive_payload` policy-tag and `sensitive_payload_mask` data-masking support (`deploy/terraform/governance.tf`), and BigQuery Continuous Queries alerting templates (`deploy/continuous-queries/`).

### Planned / Unreleased `v0.1.0` — Supply-Chain Attestation, Exported Constructor Seams & Upstream Contrib Preparation

- **Exported Constructor Seams (`factory.go`)**: Exported `CreateDefaultConfig`, `CreateTracesExporter`, `CreateLogsExporter`, and `CreateMetricsExporter` alongside `NewFactory` so an upstream `opentelemetry-collector-contrib/exporter/bigqueryexporter` wrapper can wire Collector-generated `exporter.Settings` directly without duplicating transport logic.
- **Upstream `mdatagen` Compatibility (`metadata.yaml`)**: Declared `tests.goleak.ignore.top` in `metadata.yaml` for stock `mdatagen` compatibility while preserving build-tag-isolated unit and live-integration goroutine leak checks (`generated_package_test.go`, `goleak_options_unit_test.go`, `goleak_options_integration_test.go`).
- **Release Supply-Chain Configuration (`.github/workflows/release.yml`, `.goreleaser.yaml`)**: Configures keyless Cosign signing and SLSA v1.0 build provenance attestations (`actions/attest-build-provenance@v4`) for the multi-arch GHCR manifest and GoReleaser binary archive checksums (`dist/checksums.txt`). Syft generates archive SPDX SBOMs and separate inventories for `linux/amd64` and `linux/arm64`; each container inventory is attached and attested to its matching child digest. Offline tests verify shell arguments and subject/inventory correspondence; they do not establish a successful hosted release, signature verification, or publication.
- **Registry Candidate**: [`otel-registry-entry.yaml`](otel-registry-entry.yaml) is validated against the pinned destination schema. It omits a package version while the release is unpublished; after publication, use the schema's `package` object (`registry: go-collector`, module path as `name`, and the published `version`).
