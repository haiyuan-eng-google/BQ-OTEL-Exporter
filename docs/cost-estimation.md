# BigQuery OpenTelemetry GenAI & MCP Exporter — Enterprise Cost Estimation & FinOps Guide

This guide provides a quantitative Total Cost of Ownership (TCO) model for operating the BigQuery OpenTelemetry Exporter (`bigqueryexporter`) in production across **Small (`100` spans/s)**, **Medium (`2,500` spans/s)**, and **Large (`25,000` spans/s)** GenAI and Model Context Protocol (MCP) agent deployments.

All formulas use published US multi-region (`US`) list pricing as a transparent baseline; enterprise flat-rate / Editions slot commitments and negotiated custom discounts reduce these figures proportionally.

---

## 1. Cost Component Breakdown

A production telemetry warehouse on BigQuery has five distinct cost components:

| # | Cost Component | Billing Unit | US List Price Baseline | Primary Tuning Knobs in `bigqueryexporter` |
|---|---|---|---|---|
| **1** | **Storage Write API Ingestion** (`_default` stream) | Wire GB ingested (uncompressed `ProtoRows` wire bytes) | First **2 TB / month free** per billing account; **$0.025 / GB** ($25.60 / TiB) thereafter | `offload.enabled`, `offload.min_bytes`, `limits.max_value_bytes`, collector `filter` / `tail_sampling` processors |
| **2** | **BigQuery Physical Storage** (Active & Long-Term) | Compressed physical GiB-month on disk (Zstandard) | **$0.020 / GiB-month** active (first 90 days); **$0.010 / GiB-month** long-term (> 90 days unmodified) | Dataset `storage_billing_model = PHYSICAL`, `traces.partition_expiration_days`, `logs.partition_expiration_days`, `metrics.partition_expiration_days` |
| **3** | **GCS Payload Offload** (Opt-In `offload.enabled`) | Class A `storage.objects.create` operations + Standard storage GB-month | **$0.005 / 1,000** Class A ops ($5.00 / 1M); **$0.020 / GB-month** Standard storage (or **$0.010 / GB-month** Nearline) | `offload.min_bytes` (default `65536`), `offload.cache_size` (default `10000` SHA-256 LRU dedup cache) |
| **4** | **Analytical Query & Dashboard Scans** | On-Demand TiB scanned ($6.25 / TiB) or Editions Slot-Hours | **$6.25 / TiB** (On-Demand, first 1 TiB/month free) or **$0.06 / slot-hour** (Enterprise Edition Pay-As-You-Go; **$0.048 / slot-hour** 1-yr commit) | Incremental Dataform marts (`deploy/dataform/`), `require_partition_filter`, clustering on `(service_name, span_name, trace_id)`, `max_staleness` |
| **5** | **Continuous Queries** (Opt-In `deploy/continuous-queries/`) | Enterprise Edition `CONTINUOUS` assignment slots | **$0.06 / slot-hour** ($43.80 / slot-month PAYG) or **$0.048 / slot-hour** ($35.04 / slot-month 1-yr commit); minimum `50` slots | Opt-in (`enable_continuous_queries = false` by default in `deploy/terraform/`); shared reservation across all continuous routing/alerting jobs |

---

## 2. Wire Byte Sizing by Signal & Payload Profile

The BigQuery Storage Write API bills on serialized `ProtoRows` wire bytes (observable in real time via the exporter's `otelcol_exporter_bigquery_append_bytes` histogram).

### 2.1 Measured `ProtoRows` Wire Footprint per Record

From `cmd/bq-otel-testbench` measurements across the 7 GenAI frameworks (`google_adk`, `langgraph`, `openai_agents`, `anthropic_agent_sdk`, `crewai`, `aws_bedrock`, `microsoft_agent_framework`) and MCP child spans:

| Signal Table | Columns | Envelope Overhead (Identity, Resource, Scope, Typed Columns) | Average Wire Size (`1 KB` prompt/tool payload) | Average Wire Size (`4 KB` prompt/tool payload) | Average Wire Size (`16 KB` inline payload, no offload) | Average Wire Size (`16 KB` payload with `offload.min_bytes: 4096`) |
|---|---:|---:|---:|---:|---:|---:|
| `otel_spans` | 21 | ~`1,916 B` | **`2,940 B`** (`2.87 KiB`) | `6,012 B` (`5.87 KiB`) | `18,300 B` (`17.9 KiB`) | **`2,029 B`** (inline `<offloaded:gs://...>` + `ObjectRef`) |
| `otel_logs` | 17 | ~`1,233 B` | **`2,257 B`** (`2.20 KiB`) | `5,329 B` (`5.20 KiB`) | `17,617 B` (`17.2 KiB`) | **`1,345 B`** (inline `<offloaded:gs://...>` + `ObjectRef`) |
| `otel_metrics` | 25 | ~`1,195 B` | **`1,195 B`** (`1.17 KiB`) | `4,267 B` (`4.17 KiB`) | `17,580 B` (`17.2 KiB`) | **`1,195 B`** (`1.17 KiB`) |

### 2.2 Monthly Ingestion Volume Formula

For a sustained span rate $R_{\text{spans}}$ (spans/sec), Companion Log ratio $\alpha_{\text{logs}}$ (log records per span, typically `0.5` for `gen_ai.user.message` / `gen_ai.choice` events), and Metric Point ratio $\alpha_{\text{metrics}}$ (metric points per span, typically `0.25` for token and latency histograms):

$$\text{Seconds per Month} = 86{,}400 \times 30.4375 = 2{,}629{,}800\text{ s}$$

$$\text{Monthly Wire Bytes} = 2{,}629{,}800 \times R_{\text{spans}} \times \left( B_{\text{span}} + \alpha_{\text{logs}} B_{\text{log}} + \alpha_{\text{metrics}} B_{\text{metric}} \right)$$

$$\text{Monthly Write API Cost} = \max\!\left(0,\; \frac{\text{Monthly Wire Bytes}}{10^9} - 2{,}000\right) \times \$0.025$$

---

## 3. Physical Storage & Compression Ratio Sensitivity (`PHYSICAL` vs. `LOGICAL`)

BigQuery datasets default to `LOGICAL` storage billing (`$0.020 / GiB-month` active logical bytes), which charges for uncompressed `JSON` and `STRING` bytes AND charges for time-travel / fail-safe overhead only under physical billing. However, OpenTelemetry GenAI schemas contain highly repetitive keys (`gen_ai.framework`, `gen_ai.request.model`, `service.name`, JSON attribute paths, system prompts, and MCP tool schemas) that achieve **4:1 to 8:1 Zstandard columnar compression** in BigQuery Capacitor storage.

> **Recommendation:** Configure the destination dataset with `storage_billing_model = 'PHYSICAL'` (the default in `deploy/terraform/main.tf`). Even after accounting for the 7-day time-travel + 7-day fail-safe window (`+14` days of physical retention), `PHYSICAL` billing reduces steady-state storage costs by **55%–75%** compared to `LOGICAL` billing.

### 3.1 Compression Ratio & Retention Sensitivity Table (`100 TB/month` Uncompressed Logical Ingestion)

The table below shows steady-state monthly storage cost for `100,000 GiB/month` (`~3,333 GiB/day`) of uncompressed logical ingestion across retention windows (`30`, `90`, and `365` days) and Zstandard compression ratios (`4:1`, `6:1`, `8:1`), including the 14-day time-travel + fail-safe physical buffer:

| Storage Billing Model | Compression Ratio | **30-Day Retention** (All Active @ `$0.020/GiB`) | **90-Day Retention** (All Active @ `$0.020/GiB`) | **365-Day Retention** (90d Active @ `$0.020` + 275d Long-Term @ `$0.010`) |
|---|---:|---:|---:|---:|
| `LOGICAL` (uncompressed, no fail-safe charge) | `1:1` | `$2,000 / mo` | `$6,000 / mo` | `$15,167 / mo` |
| `PHYSICAL` (includes +14d time-travel & fail-safe) | **`4:1`** | `$733 / mo` | `$1,733 / mo` | `$4,025 / mo` |
| `PHYSICAL` (typical GenAI + MCP JSON mix) | **`6:1`** | **`$489 / mo`** | **`$1,156 / mo`** | **`$2,683 / mo`** |
| `PHYSICAL` (repetitive system prompts / logs) | **`8:1`** | `$367 / mo` | `$867 / mo` | `$2,013 / mo` |

---

## 4. GCS Large-Payload Offload Economics (`offload.enabled: true`)

When agents exchange large RAG contexts, multi-turn conversation histories, or verbose MCP `resources/read` payloads (`16 KB`–`512 KB`), storing them inline in BigQuery incurs **$0.025 / GB** on the Storage Write API and bloats every analytical scan of `attributes` or `body`.

Enabling `offload.enabled: true` routes strings $\ge$ `offload.min_bytes` to Google Cloud Storage (`gs://<bucket>/<prefix>/<signal>/YYYY/MM/DD/sha256/<hh>/<sha256>.txt`) and writes a compact `ObjectRef` struct into BigQuery:

1. **Write API Savings vs. GCS Class A Upload Cost**:
   - A `64 KB` payload written inline to BigQuery costs $64\text{ KB} \times \$0.025 / 10^6 = \$0.0000016$ per row ($1.60 per 1M payloads).
   - A GCS Class A `WriteObject` call costs `$0.005 / 1,000` = `$0.000005` per unique upload ($5.00 per 1M unique uploads).
2. **Impact of SHA-256 Content-Addressed Deduplication (`offload.cache_size: 10000`)**:
   - In multi-turn GenAI agent workflows, system instructions, tool definitions, and RAG chunks repeat across turns within the same session.
   - Because `internal/offload` addresses objects by `SHA-256(payload)` and maintains an in-memory LRU generation cache (`offload.cache_size`, default `10,000`) plus `ifGenerationMatch=0` idempotency, a cache hit emits the `ObjectRef` with **zero GCS HTTP requests and zero Class A operation charges**.
   - At a typical **80% deduplication hit rate** (`otelcol_exporter_bigquery_offload_dedup_hits / (offloaded_payloads + offload_dedup_hits)`), effective Class A upload cost drops from `$5.00 / 1M` to **`$1.00 / 1M`**, making GCS offload **net-cheaper on ingestion alone** for payloads $\ge 40\text{ KB}$, while eliminating `64 KB+` per row from BigQuery storage and query scans.

| Average Offloaded Payload Size | Deduplication Hit Rate (`offload.cache_size: 10000`) | Cost of 10M Inline BigQuery Appends (`$0.025/GB`) | Cost of 10M Offloaded Payloads (GCS Class A Ops + 30d Standard Storage) | Net Monthly Ingestion + Storage Delta |
|---:|---:|---:|---:|---:|
| `16 KB` | `0%` (all unique) | `$4.00` | `$50.00` (ops) + `$3.20` (storage) = `$53.20` | `+$49.20` (use inline for unique 16 KB payloads) |
| `64 KB` | `80%` (typical multi-turn RAG) | `$16.00` | `$10.00` (ops) + `$2.56` (storage) = `$12.56` | **`-$3.44`** (net savings + faster BQ queries) |
| `256 KB` | `85%` (system prompts + MCP schemas) | `$64.00` | `$7.50` (ops) + `$7.68` (storage) = `$15.18` | **`-$48.82` (76% cheaper)** |

---

## 5. Query Cost: Companion Logical Views vs. Incremental Dataform Marts

The exporter ships two complementary analytical layers:

1. **Zero-Latency Companion Views (`auto_create.companion_views: true`)**:
   - Views such as `v_gen_ai_spans`, `v_gen_ai_sessions`, `v_gen_ai_model_costs`, `v_gen_ai_tool_usage`, `v_mcp_spans`, and `v_mcp_tool_usage` query the deduplication views (`otel_spans_dedup`, `otel_logs_dedup`, `otel_metrics_dedup`), which evaluate `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY _ingest_time DESC) = 1` over the base tables.
   - Ideal for ad-hoc debugging and live tailing (especially when combined with `max_staleness = "15m"` materialized metadata cache).
2. **Incremental Dataform Reporting Marts (`deploy/dataform/`)**:
   - Pre-aggregated hourly and daily partitioned tables (`mart_gen_ai_hourly_model_costs`, `mart_gen_ai_daily_sessions`, `mart_mcp_hourly_tool_health`) refreshed every 15–60 minutes using incremental `APPENDS(TABLE otel_spans, ...)` watermarks.
   - Each incremental merge scans **only newly appended rows since the last watermark**, and downstream Looker / Grafana dashboards query the compact mart tables (`~1,000x` fewer rows than raw spans).

| Dashboard / BI Workload | Direct Companion View Scan (`v_gen_ai_model_costs` over 30-day window) | Incremental Dataform Mart (`deploy/dataform/`, 15-min incremental refresh + BI queries) | Monthly Query Cost Reduction |
|---|---:|---:|---:|
| **Medium Fleet** (`2,500` spans/s, `11.5 TiB` raw spans/30d, 200 dashboard loads/day) | `200 * 30 * 1.8 TiB` (projected cols) = `10,800 TiB` $\rightarrow$ **`$67,500 / mo`** (On-Demand) | `11.5 TiB` (1x incremental scan/mo) + `18 GiB` (mart queries) $\rightarrow$ **`$72 / mo`** | **`99.8%` reduction** |

---

## 6. Continuous Queries Enterprise Slot Sizing (`deploy/continuous-queries/`)

BigQuery Continuous Queries (`APPENDS(TABLE ...)` streaming SQL for real-time Pub/Sub alerting, Bigtable session state indexing, or Vertex AI online feature stores) require a BigQuery **Enterprise** or **Enterprise Plus** Edition reservation with a `CONTINUOUS` job assignment.

| Fleet Tier | Sustained Event Rate | Recommended `CONTINUOUS` Slot Reservation | Enterprise PAYG (`$0.06 / slot-hr`) | Enterprise 1-Year Commit (`$0.048 / slot-hr`) | Guidance |
|---|---:|---:|---:|---:|---|
| **Small** (`100` spans/s) | `175` total records/s | `50` slots (minimum assignment) | `$2,190 / mo` | `$1,752 / mo` | Keep `enable_continuous_queries = false` unless sub-5s Pub/Sub or Bigtable streaming triggers are required; use 5-minute scheduled queries instead (`<$5/mo`). |
| **Medium** (`2,500` spans/s) | `4,375` total records/s | `100` slots (`50` baseline + `50` autoscale) | `$3,285 / mo` (avg 75 slots) | `$2,628 / mo` | Consolidate PII/safety alerting and session routing into 1–2 Continuous Queries sharing the reservation. |
| **Large** (`25,000` spans/s) | `43,750` total records/s | `300` slots (`200` baseline + `100` autoscale) | `$10,950 / mo` (avg 250 slots) | `$8,760 / mo` | Dedicated `CONTINUOUS` assignment becomes <18% of total observability TCO and replaces standalone Apache Flink / Dataflow streaming clusters. |

---

## 7. End-to-End Monthly TCO Scenarios (Small, Medium, Large)

Assumptions common to all three reference architectures:
- **Signal Mix per Span**: `1.0` span (`1.78 KB` wire) + `0.5` GenAI event log (`1.58 KB` wire) + `0.25` metric data point (`0.64 KB` wire) = **`2.73 KB` combined wire bytes per span**.
- **Storage**: `PHYSICAL` storage billing model (`6:1` Zstandard compression), **90-day active retention** (`partition_expiration_days: 90`), +14-day time-travel/fail-safe window.
- **Offload**: `5%` of spans carry `64 KB` large tool/RAG payloads offloaded to GCS (`offload.min_bytes: 16384`) with an `80%` SHA-256 LRU deduplication hit rate and 30-day GCS lifecycle policy.
- **Analytics**: Incremental Dataform marts (`deploy/dataform/`) refreshed every 15 minutes + 50 ad-hoc investigator queries/month scanning 7-day partition-pruned windows.

| Cost Line Item | **Small Deployment**<br>`100` spans/s (`175` rec/s)<br>`263M` spans / mo | **Medium Deployment**<br>`2,500` spans/s (`4,375` rec/s)<br>`6.57B` spans / mo | **Large Deployment**<br>`25,000` spans/s (`43,750` rec/s)<br>`65.7B` spans / mo |
|---|---:|---:|---:|
| **Monthly Uncompressed Wire Volume** | `718 GB / mo` | `17,948 GB / mo` (`17.95 TB`) | `179,484 GB / mo` (`179.5 TB`) |
| **1. Storage Write API (`_default` stream)**<br>*(First `2,000 GB/mo` free, then `$0.025/GB`)* | **`$0.00`**<br>*(within 2 TB free tier)* | **`$398.70`**<br>*(`15,948 GB` billable)* | **`$4,437.10`**<br>*(`177,484 GB` billable)* |
| **2. BigQuery Physical Storage (90-day retention)**<br>*(`6:1` Zstandard compression, `$0.020/GiB-mo`)* | **`$8.15`**<br>*(`408 GiB` physical across 90d)* | **`$203.80`**<br>*(`10.19 TiB` physical across 90d)* | **`$2,038.00`**<br>*(`101.9 TiB` physical across 90d)* |
| **3. GCS Payload Offload** (`5%` of spans @ `64 KB`, `80%` dedup)<br>*(Class A ops `$0.005/1k` + 30d Standard storage)* | **`$16.50`**<br>*(`2.63M` unique puts + `168 GB`)* | **`$412.60`**<br>*(`65.7M` unique puts + `4.2 TB`)* | **`$4,126.00`**<br>*(`657M` unique puts + `42 TB`)* |
| **4. Analytical Queries & Dataform Marts**<br>*(15-min incremental marts + 50 ad-hoc 7d queries)* | **`$6.25`**<br>*(~`2 TiB` scanned, `1 TiB` free)* | **`$156.25`**<br>*(~`26 TiB` scanned)* | **`$1,562.50`**<br>*(~`251 TiB` scanned)* |
| **5. Collector Compute Footprint**<br>*(`~1.30` vCPU & `~123 MB` RSS per `10k` spans/s on GKE e2-standard-4)* | **`$12.00`**<br>*(shared `0.25` vCPU pod)* | **`$48.00`**<br>*(`2` replicas $\times$ `1` vCPU)* | **`$240.00`**<br>*(`5` replicas $\times$ `2` vCPU)* |
| **Total Monthly TCO (Base Warehouse + Marts)** | **`$42.90 / month`** | **`$1,219.35 / month`** | **`$12,403.60 / month`** |
| **Unit Cost per Million Spans (`Base TCO`)** | **`$0.16 / 1M spans`** | **`$0.19 / 1M spans`** | **`$0.19 / 1M spans`** |
| *Optional Add-On: Continuous Queries (1-yr Enterprise Commit)* | *`+$1,752 / mo` (`50` slots)* | *`+$2,628 / mo` (`75` avg slots)* | *`+$8,760 / mo` (`250` avg slots)* |

---

## 8. FinOps Checklist for Production Operators

1. **Always Enable `PHYSICAL` Storage Billing on the Dataset**:
   ```sql
   ALTER SCHEMA `your-project.otel`
   SET OPTIONS (storage_billing_model = 'PHYSICAL');
   ```
2. **Enforce Partition Filters on High-Volume Signal Tables**:
   Set `traces.require_partition_filter: true`, `logs.require_partition_filter: true`, and `metrics.require_partition_filter: true` in `example/otel-collector-config.yml` so ad-hoc queries without a `WHERE start_time >= ...` or `WHERE timestamp >= ...` predicate are rejected before scanning historical partitions.
3. **Set Tiered Partition Expiration**:
   Retain high-cardinality raw spans/logs for `30`–`90` days (`partition_expiration_days: 90`) while keeping lightweight aggregated Dataform marts (`deploy/dataform/`) for `365+` days.
4. **Right-Size `offload.min_bytes`**:
   Set `offload.min_bytes: 32768` (`32 KiB`) or `65536` (`64 KiB`) so routine short prompts stay inline in BigQuery (zero GCS Class A operation charge) while oversized RAG documents and MCP resource dumps are offloaded and deduplicated by SHA-256.
5. **Use `max_staleness` for High-QPS Operational Dashboards**:
   Setting `traces.max_staleness: "15m"` allows BigQuery to serve queries from pre-materialized Capacitor storage metadata without scanning recently buffered streaming blocks, lowering query slot consumption and tail latency.
