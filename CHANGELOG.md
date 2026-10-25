# Changelog

All notable changes to the OpenTelemetry Collector BigQuery Exporter (`github.com/haiyuan-eng-google/BQ-OTEL-Exporter`) are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Note on Milestone Lineage**: `v0alpha1` and `v0alpha2` below record the pre-release schema-contract milestones (`internal/schema/schema.go` `Version = "v0alpha2"`) developed on `main` prior to the inaugural tagged `v0.1.0` semantic release.

## [Unreleased] — `v0.1.0` Release Candidate

### Added
- **Exported Collector Constructor Seams (`factory.go`)**: Exported `CreateDefaultConfig`, `CreateTracesExporter`, `CreateLogsExporter`, and `CreateMetricsExporter` matching `exporter.CreateTracesFunc`, `exporter.CreateLogsFunc`, and `exporter.CreateMetricsFunc` alongside `NewFactory()` so upstream OpenTelemetry Collector distribution wrappers (`opentelemetry-collector-contrib/exporter/bigqueryexporter`) can wire Collector-generated `exporter.Settings` directly.
- **Upstream `mdatagen` Goleak Configuration (`metadata.yaml`)**: Added `tests.goleak.ignore.top` (`go.opencensus.io/stats/view.(*worker).start`) to `metadata.yaml` for compatibility with stock `mdatagen` code generation while preserving build-tag-isolated unit and live-integration goroutine leak checks (`generated_package_test.go`, `goleak_options_unit_test.go`, `goleak_options_integration_test.go`).
- **Binary Archive SLSA Provenance & Container SPDX SBOM (`.github/workflows/release.yml`)**: Added `actions/attest-build-provenance@v4` attestation for GoReleaser archive checksums (`dist/checksums.txt`) and container SPDX SBOM generation and attachment (`syft`, `cosign attach sbom`, and `cosign attest --type spdxjson`) for `ghcr.io/haiyuan-eng-google/bq-otel-collector`.
- **Cleanroom Provenance & OpenTelemetry Registry Metadata (`docs/`)**: Added [`docs/provenance.md`](docs/provenance.md) recording cleanroom implementation provenance and [`docs/otel-registry-entry.yaml`](docs/otel-registry-entry.yaml) for the OpenTelemetry Registry listing.
- **Default-Stream Pool Sharding (`write.stream_pool_size`)**: Added configurable multi-stream sharding across default-stream writers per signal table (`write.stream_pool_size` in `[1, 16]`) to scale throughput beyond a single gRPC write stream.
- **Adaptive AIMD Batch Backpressure (`write.backpressure`)**: Added opt-in additive-increase/multiplicative-decrease request-byte and in-flight concurrency control triggered by ResourceExhausted/Unavailable responses or append latency thresholds, with `otelcol_exporter_bigquery_backpressure_events`, `otelcol_exporter_bigquery_backpressure_level`, and `otelcol_exporter_bigquery_effective_request_bytes` self-telemetry metrics.
- **Recursive Multi-Agent Trajectory & MCP Companion Views (`internal/schema/`, `deploy/terraform/`)**:
  - `v_gen_ai_agent_trajectory`: 16-level cycle-safe recursive CTE reconstructing parent-child agent/LLM/tool execution DAGs with `is_orphan` cross-boundary detection and `is_token_leaf` deduplicated token accounting.
  - `v_mcp_spans` and `v_mcp_tool_usage`: Model Context Protocol JSON-RPC method, tool/resource/prompt target, session, transport, and latency analytics.
  - `v_gen_ai_metric_exemplars`: Joins `otel_metrics_dedup` exemplars back to `otel_spans_dedup` by `(trace_id, span_id)` with partition-filter gating (`allow_unprunable_views`).
- **Real-Time Alerting via BigQuery Continuous Queries (`deploy/continuous-queries/`)**: Added SQL templates (`tool_error_rate_alert.sql`, `token_budget_burn.sql`, `agent_latency_p95.sql`) reading `APPENDS(TABLE ...)` and exporting to Cloud Pub/Sub, plus opt-in Terraform provisioning (`enable_continuous_queries`).
- **Column-Level Security & Data Masking (`deploy/terraform/governance.tf`)**: Added opt-in Data Catalog taxonomy (`otel_sensitivity`), the `sensitive_payload` policy tag (`google_data_catalog_policy_tag.sensitive_payload`), the `sensitive_payload_mask` BigQuery data masking policy (`google_bigquery_datapolicy_data_policy.sensitive_payload_mask`), fine-grained/masked reader IAM bindings (`roles/datacatalog.categoryFineGrainedReader`, `roles/bigquerydatapolicy.maskedReader`), and redacted views (`enable_column_level_security`).

### Changed
- **Toolchain & Collector Baseline**: Pinned Go toolchain to `1.27.3` (`GO-2026-6809`) and OpenTelemetry Collector modules to `v0.163.0` / `v1.69.0` across `go.mod`, `Makefile`, Dockerfiles, OCB manifests, and GitHub Actions workflows.

### Fixed
- **HTTP Transport Leak Checks**: Restored goroutine leak checks (`goleak`) for exporter-owned HTTP transports in unit and integration test suites while isolating ambient mTLS helper processes in live integration runs.

---

## Pre-Release Contract Milestone `v0alpha2` — Three-Signal Relational Contract & Safety Controls

### Added
- **Metrics Signal Pipeline (`exporter_metrics.go`, `internal/transform/metrics.go`)**: Added `otel_metrics` table and `otel_metrics_dedup` view supporting `GAUGE`, `SUM`, `HISTOGRAM`, `EXPONENTIAL_HISTOGRAM`, and `SUMMARY` data points with exemplar arrays and deterministic `point_fingerprint` deduplication.
- **Dead-Letter Quarantine & Replay (`otel_quarantine`, `cmd/bq-quarantine-replay`)**: Added opt-in dead-letter quarantine table for rejected records and a standalone CLI (`cmd/bq-quarantine-replay`) to reconstruct OTLP JSONL payloads (including GCS-offloaded blobs via `-fetch-gcs`).
- **Step-0 Client-Side Redaction (`internal/redact/`)**: Added configurable key-glob and regex value scrubbing executed prior to GCS payload offloading, size validation, log/metric fingerprinting, and quarantine persistence, metered by `otelcol_exporter_bigquery_redacted_values`.
- **Content-Addressed GCS Payload Offloading (`internal/offload/`)**: Added conditional (`ifGenerationMatch=0`) Cloud Storage offloading for oversized or multimodal attributes with intra-batch coalescing, cross-batch LRU caching, and BigQuery `ObjectRef` `payload_refs` records.
- **Companion SQL View Suite (`internal/schema/schema.go`)**: Added the 11 companion views (`otel_spans_as_allspans`, `v_gen_ai_spans`, `v_gen_ai_logs`, `v_gen_ai_metrics`, `v_gen_ai_metric_exemplars`, `v_gen_ai_model_costs`, `v_gen_ai_sessions`, `v_gen_ai_tool_usage`, `v_gen_ai_agent_trajectory`, `v_mcp_spans`, and `v_mcp_tool_usage`) alongside execution-verified OTLP setup guidance in [`docs/agent-frameworks.md`](docs/agent-frameworks.md) for 7 agent frameworks (Claude Code/Agent SDK, Google ADK, LangGraph, OpenAI, CrewAI, AWS Bedrock, and Microsoft Agent Framework).
- **Production Deployment Packaging (`deploy/`, `example/`, `.goreleaser.yaml`)**: Added Terraform module (`deploy/terraform/`), Helm chart (`deploy/helm/bigquery-otel-collector`), Cloud Run Knative service (`deploy/cloudrun/service.yaml`), distroless multi-arch container builds with static `/healthcheck`, and GoReleaser configuration.

---

## Pre-Release Contract Milestone `v0alpha1` — Initial Trace & Log Storage Write API Exporter

### Added
- **Traces & Logs Storage Write API Pipeline (`exporter_traces.go`, `exporter_logs.go`, `internal/bigquery/`)**: Implemented high-throughput protobuf append transport over `cloud.google.com/go/bigquery/storage/managedwriter` `_default` streams with single-owner `exporterhelper` retry and uncertain-acknowledgement subset replay.
- **Dual-Timestamp & JSON Sidecar Relational Schema (`internal/schema/`, `internal/transform/`)**: Defined `otel_spans` (hourly partitioned on `start_time`) and `otel_logs` (daily partitioned on `timestamp`) with nanosecond `INT64` companion columns, deterministic `fp1:` log fingerprinting, and `attributes_schema` leaf-type sidecars.
