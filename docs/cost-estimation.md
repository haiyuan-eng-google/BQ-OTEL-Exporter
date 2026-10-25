# BigQuery OpenTelemetry GenAI & MCP Exporter — Enterprise Cost Estimation & FinOps Guide

This guide provides a quantitative Total Cost of Ownership (TCO) model for operating the BigQuery OpenTelemetry Exporter (`bigqueryexporter`) in production across **Small (`100` spans/s)**, **Medium (`2,500` spans/s)**, and **Large (`25,000` spans/s)** GenAI and Model Context Protocol (MCP) agent deployments.

All formulas use published US multi-region (`US`) list pricing and binary byte units (`1 GiB` = $1024^3$ B, `1 TiB` = $1024^4$ B) throughout; enterprise Editions slot commitments and negotiated custom discounts reduce these figures proportionally.

---

## 1. Cost Component Breakdown

A production telemetry warehouse on BigQuery has five distinct cost components:

| # | Cost Component | Billing Unit | US Multi-Region (`US`) List Price Baseline | Primary Tuning Knobs in `bigqueryexporter` |
|---|---|---|---|---|
| **1** | **Storage Write API Ingestion** (`_default` stream) | Wire GiB ingested (uncompressed `ProtoRows` wire bytes) | First **`2 TiB` (`2,048 GiB`) / month free** per billing account; **`$0.025 / GiB`** (`$25.60 / TiB`) thereafter | `offload.enabled`, `offload.min_bytes` (default `262144` / `256 KiB`), `limits.max_value_bytes`, collector `filter` / `tail_sampling` processors |
| **2** | **BigQuery Storage** (`PHYSICAL` vs. `LOGICAL`) | Compressed physical GiB-month (`PHYSICAL`) or uncompressed GiB-month (`LOGICAL`) | **`LOGICAL`**: `$0.020 / GiB-month` active (first 90d), `$0.010 / GiB-month` long-term (>90d)<br>**`PHYSICAL`**: **`$0.040 / GiB-month` active** (first 90d), **`$0.020 / GiB-month` long-term** (>90d), plus 7d time-travel + 7d fail-safe (`+14d`) | Dataset `storage_billing_model = PHYSICAL`, `traces.partition_expiration_days`, `logs.partition_expiration_days`, `metrics.partition_expiration_days` |
| **3** | **GCS Payload Offload** (Opt-In `offload.enabled`) | Class A `storage.objects.create` operations + Standard storage GiB-month | **`$0.005 / 1,000`** Class A ops (`$5.00 / 1M`); **`$0.020 / GiB-month`** Standard storage (or **`$0.010 / GiB-month`** Nearline) | `offload.min_bytes` (default `262144` / `256 KiB`), `offload.cache_size` (default `4096` SHA-256 LRU dedup cache) |
| **4** | **Analytical Query & Dashboard Scans** | On-Demand TiB scanned (`$6.25 / TiB`) or Editions Slot-Hours | **`$6.25 / TiB`** (On-Demand, first `1 TiB/month` free) or **`$0.060 / slot-hour`** (Enterprise Edition Pay-As-You-Go; **`$0.048 / slot-hour`** 1-yr baseline commit) | Incremental Dataform marts (`deploy/dataform/`), `require_partition_filter`, clustering on `(service_name, span_name, trace_id)`, `max_staleness` |
| **5** | **Continuous Queries** (Opt-In `deploy/continuous-queries/`) | Enterprise Edition `CONTINUOUS` assignment slots | **`$0.060 / slot-hour`** (`$43.80 / slot-month` PAYG & autoscale slots) or **`$0.048 / slot-hour`** (`$35.04 / slot-month` 1-yr commit on **baseline** slots); minimum `50` slots | Opt-in (`enable_continuous_queries = false` by default in `deploy/terraform/`); shared reservation across all continuous routing/alerting jobs |

---

## 2. Wire Byte Sizing by Signal & Payload Profile

The BigQuery Storage Write API bills on serialized `ProtoRows` wire bytes (observable in real time via the exporter's `otelcol_exporter_bigquery_append_bytes` counter).

### 2.1 Measured `ProtoRows` Wire Footprint per Record

From `cmd/bq-otel-testbench` measurements across the 7 GenAI frameworks (`google_adk`, `langgraph`, `openai_agents`, `anthropic_agent_sdk`, `crewai`, `aws_bedrock`, `microsoft_agent_framework`) and MCP child spans:

| Signal Table | Schema Columns (`internal/schema`) | Envelope Overhead (Identity, Resource, Scope, Typed Columns) | Average Wire Size (`1 KiB` prompt/tool payload) | Average Wire Size (`4 KiB` prompt/tool payload) | Average Wire Size (`16 KiB` inline payload, no offload) | Average Wire Size (`16 KiB` payload with `offload.min_bytes: 4096`) |
|---|---:|---:|---:|---:|---:|---:|
| `otel_spans` | `30` | ~`1,916 B` | **`2,940 B`** (`2.87 KiB`) | `6,012 B` (`5.87 KiB`) | `18,300 B` (`17.87 KiB`) | **`2,521 B`** (native GCS `ObjectRef`; `2,029 B` local placeholder) |
| `otel_logs` | `27` | ~`1,233 B` | **`2,257 B`** (`2.20 KiB`) | `5,329 B` (`5.20 KiB`) | `17,617 B` (`17.20 KiB`) | **`1,814 B`** (native GCS `ObjectRef`; `1,345 B` local placeholder) |
| `otel_metrics` | `40` | ~`1,195 B` | **`1,195 B`** (`1.17 KiB`) | `4,267 B` (`4.17 KiB`) | `17,580 B` (`17.17 KiB`) | **`1,195 B`** (`1.17 KiB`, metrics do not offload) |

### 2.2 Monthly Ingestion Volume Formula

For a sustained span rate $R_{\text{spans}}$ (spans/sec), Companion Log ratio $\alpha_{\text{logs}}$ (log records per span, typically `0.5` for `gen_ai.user.message` / `gen_ai.choice` events), and Metric Point ratio $\alpha_{\text{metrics}}$ (metric points per span, typically `0.25` for token and latency histograms), using the measured `1 KiB` payload profile ($B_{\text{span}} = 2{,}940\text{ B}$, $B_{\text{log}} = 2{,}257\text{ B}$, $B_{\text{metric}} = 1{,}195\text{ B}$):

$$\text{Combined Wire Bytes per Span} = 2{,}940 + 0.5 \times 2{,}257 + 0.25 \times 1{,}195 = \mathbf{4{,}367.25\text{ B}}\;(\mathbf{4.26\text{ KiB}})$$

$$\text{Seconds per Month} = 86{,}400 \times 30.4375 = 2{,}629{,}800\text{ s}$$

$$\text{Monthly Wire GiB} = \frac{2{,}629{,}800 \times R_{\text{spans}} \times 4{,}367.25}{1024^3}$$

$$\text{Monthly Write API Cost} = \max\!\left(0,\; \text{Monthly Wire GiB} - 2{,}048\right) \times \$0.025$$

---

## 3. Physical Storage & Compression Ratio Sensitivity (`PHYSICAL` vs. `LOGICAL`)

In US multi-region (`US`), BigQuery `LOGICAL` storage costs **`$0.020 / GiB-month` active** (`$0.010 / GiB-month` long-term) with no separate charge for time-travel or fail-safe storage, whereas `PHYSICAL` storage costs **`$0.040 / GiB-month` active** (`$0.020 / GiB-month` long-term) — 2x the per-GiB rate — and bills for the 7-day time-travel + 7-day fail-safe window (`+14` days of physical retention).

Even with the 2x unit rate and `+14` days of time-travel/fail-safe overhead, OpenTelemetry GenAI schemas contain highly repetitive keys (`gen_ai.framework`, `gen_ai.request.model`, `service.name`, JSON attribute paths, system prompts, and MCP tool schemas) that achieve **4:1 to 8:1 Zstandard columnar compression** in BigQuery Capacitor storage. Any compression ratio above `2.93:1` (at 30-day retention) or `2.31:1` (at 90-day retention) makes `PHYSICAL` billing strictly cheaper than `LOGICAL` billing.

> **Recommendation:** Configure the destination dataset with `storage_billing_model = 'PHYSICAL'` (the default in `deploy/terraform/main.tf`). After accounting for the `$0.040 / GiB-month` physical active rate (`$0.020 / GiB-month` long-term) and the `+14`-day time-travel + fail-safe window, `PHYSICAL` billing reduces steady-state storage costs by **`26.7%–73.5%`** across `4:1`–`8:1` compression ratios (**`51.1%–64.6%`** at a typical `6:1` GenAI + MCP ratio).

### 3.1 Compression Ratio & Retention Sensitivity Table (`100,000 GiB / 30d` Uncompressed Logical Ingestion)

The table below shows steady-state monthly storage cost for `100,000 GiB` per 30-day period (`3,333.33 GiB/day`) of uncompressed logical ingestion across retention windows (`30`, `90`, and `365` days) and Zstandard compression ratios (`4:1`, `6:1`, `8:1`), using exact US multi-region rates (`LOGICAL`: `$0.020` active / `$0.010` long-term, `0` fail-safe days; `PHYSICAL`: `$0.040` active / `$0.020` long-term, `+14` active time-travel & fail-safe days):

| Storage Billing Model | Compression Ratio | **30-Day Retention** (`LOGICAL`: 30d @ `$0.020`; `PHYSICAL`: 44d @ `$0.040`) | **90-Day Retention** (`LOGICAL`: 90d @ `$0.020`; `PHYSICAL`: 104d @ `$0.040`) | **365-Day Retention** (`LOGICAL`: 90d @ `$0.020` + 275d @ `$0.010`; `PHYSICAL`: 104d @ `$0.040` + 275d @ `$0.020`) |
|---|---:|---:|---:|---:|
| `LOGICAL` (uncompressed, no fail-safe charge) | `1:1` | `$2,000.00 / mo` | `$6,000.00 / mo` | `$15,166.67 / mo` |
| `PHYSICAL` (`$0.040/GiB` active + 14d fail-safe) | **`4:1`** | `$1,466.67 / mo` (`-26.7%`) | `$3,466.67 / mo` (`-42.2%`) | `$8,050.00 / mo` (`-46.9%`) |
| `PHYSICAL` (typical GenAI + MCP JSON mix) | **`6:1`** | **`$977.78 / mo` (`-51.1%`)** | **`$2,311.11 / mo` (`-61.5%`)** | **`$5,366.67 / mo` (`-64.6%`)** |
| `PHYSICAL` (repetitive system prompts / logs) | **`8:1`** | `$733.33 / mo` (`-63.3%`) | `$1,733.33 / mo` (`-71.1%`) | `$4,025.00 / mo` (`-73.5%`) |

---

## 4. GCS Large-Payload Offload Economics (`offload.enabled: true`)

When agents exchange large RAG contexts, multi-turn conversation histories, or verbose MCP `resources/read` payloads (`16 KiB`–`512 KiB`), storing them inline in BigQuery incurs **`$0.025 / GiB`** on the Storage Write API and bloats every analytical scan of `attributes` or `body`.

Enabling `offload.enabled: true` routes strings $\ge$ `offload.min_bytes` (default `262144` / `256 KiB` in `config.go`; configurable down to `16384` / `65536` for aggressive warehouse slimming) to Google Cloud Storage (`gs://<bucket>/<prefix>/<signal>/YYYY/MM/DD/sha256/<hh>/<sha256>.txt`) and writes a compact `ObjectRef` struct into BigQuery (`payload_refs`):

1. **Write API Savings vs. GCS Class A Upload Cost**:
   - A `64 KiB` (`65,536 B`) payload written inline to BigQuery costs $(65{,}536 / 1024^3) \times \$0.025 = \$0.000001526$ per row (`$1.53` per `1M` payloads).
   - A GCS Class A `WriteObject` call costs `$0.005 / 1,000` = `$0.000005` per unique upload (`$5.00` per `1M` unique uploads).
2. **Impact of SHA-256 Content-Addressed Deduplication (`offload.cache_size: 4096`)**:
   - In multi-turn GenAI agent workflows, system instructions, tool definitions, and RAG chunks repeat across turns within the same session.
   - Because `internal/offload` addresses objects by `SHA-256(payload)` and maintains an in-memory LRU generation cache (`offload.cache_size`, default `4,096` entries) plus per-batch upload deduplication (`WithBatchUploadCache`) and `ifGenerationMatch=0` idempotency, a cache hit emits the `ObjectRef` with **zero GCS HTTP requests and zero Class A operation charges**.
   - At a typical **80% deduplication hit rate** (`otelcol_exporter_bigquery_offload_dedup_hits / (offloaded_payloads + offload_dedup_hits)`), effective Class A upload cost drops from `$5.00 / 1M` to **`$1.00 / 1M`**, making GCS offload **net-cheaper on ingestion + storage** for payloads $\ge 45\text{ KiB}$, while eliminating `64 KiB+` per row from BigQuery storage and query scans.

| Average Offloaded Payload Size | Deduplication Hit Rate (`offload.cache_size: 4096`) | Cost of 10M Inline BigQuery Appends (`$0.025/GiB`) | Cost of 10M Offloaded Payloads (GCS Class A Ops + 30d Standard Storage @ `$0.020/GiB`) | Net Monthly Ingestion + Storage Delta |
|---:|---:|---:|---:|---:|
| `16 KiB` | `0%` (all unique) | `$3.81` | `$50.00` (ops) + `$3.05` (storage) = `$53.05` | `+$49.24` (keep unique `16 KiB` payloads inline) |
| `64 KiB` | `80%` (typical multi-turn RAG) | `$15.26` | `$10.00` (ops) + `$2.44` (storage) = `$12.44` | **`-$2.82`** (net savings + faster BQ queries) |
| `256 KiB` (default `offload.min_bytes`) | `85%` (system prompts + MCP schemas) | `$61.04` | `$7.50` (ops) + `$7.32` (storage) = `$14.82` | **`-$46.21` (`75.7%` cheaper)** |

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
| **Medium Fleet** (`2,500` spans/s, `18.0 TiB` raw spans/30d, 200 dashboard loads/day) | `200 * 30 * 1.8 TiB` (projected cols) = `10,800 TiB` $\rightarrow$ **`$67,500 / mo`** (On-Demand) | `18.0 TiB` (1x incremental scan/mo) + `18 GiB` (mart queries) $\rightarrow$ **`$113 / mo`** | **`99.8%` reduction** |

---

## 6. Continuous Queries Enterprise Slot Sizing (`deploy/continuous-queries/`)

BigQuery Continuous Queries (`APPENDS(TABLE ...)` streaming SQL for real-time Pub/Sub alerting, Bigtable session state indexing, or Vertex AI online feature stores) require a BigQuery **Enterprise** or **Enterprise Plus** Edition reservation with a `CONTINUOUS` job assignment.

Under BigQuery Editions pricing (`730 hours/month`), a **1-year slot commitment (`$0.048 / slot-hour` = `$35.04 / slot-month`) applies only to baseline slots**, while **autoscaled slots above baseline always bill at the Pay-As-You-Go rate (`$0.060 / slot-hour` = `$43.80 / slot-month`)**:

| Fleet Tier | Sustained Event Rate | Recommended `CONTINUOUS` Slot Reservation | Enterprise PAYG (`$0.060 / slot-hr` on all slots) | Enterprise 1-Year Commit (`$0.048 / slot-hr` baseline + `$0.060 / slot-hr` autoscale) | Guidance |
|---|---:|---:|---:|---:|---|
| **Small** (`100` spans/s) | `175` total records/s | `50` baseline slots (`0` autoscale) | `$2,190.00 / mo` | **`$1,752.00 / mo`** | Keep `enable_continuous_queries = false` unless sub-5s Pub/Sub or Bigtable streaming triggers are required; use 5-minute scheduled queries instead (`<$5/mo`). |
| **Medium** (`2,500` spans/s) | `4,375` total records/s | `50` baseline + `50` autoscale (avg `25` autoscale = `75` total slots) | `$3,285.00 / mo` | **`$2,847.00 / mo`** (`$1,752` baseline + `$1,095` autoscale) | Consolidate PII/safety alerting and session routing into 1–2 Continuous Queries sharing the reservation. |
| **Large** (`25,000` spans/s) | `43,750` total records/s | `200` baseline + `100` autoscale (avg `50` autoscale = `250` total slots) | `$10,950.00 / mo` | **`$9,198.00 / mo`** (`$7,008` baseline + `$2,190` autoscale) | Dedicated `CONTINUOUS` assignment is `33.1%` of combined TCO (`$27,815/mo`) and replaces standalone Apache Flink / Dataflow streaming clusters. |

---

## 7. End-to-End Monthly TCO Scenarios (Small, Medium, Large)

Assumptions common to all three reference architectures (using the exact measured wire sizes from **Section 2.1** and US multi-region rates from **Section 1**):
- **Signal Mix per Span (`1 KiB` payload profile from §2.1)**: `1.0` span (`2,940 B` wire) + `0.5` GenAI event log (`2,257 B` wire) + `0.25` metric data point (`1,195 B` wire) = **`4,367.25 B` (`4.26 KiB`) combined wire bytes per span**.
- **Storage (`PHYSICAL` Billing Model)**: `6:1` Zstandard compression, **90-day active retention** (`partition_expiration_days: 90`) + 14-day time-travel/fail-safe window = `104` physical active days (`104 / 30.4375 = 3.4168` months of daily ingest stored in steady state) billed at **`$0.040 / GiB-month`**.
- **Offload**: `5%` of spans carry `64 KiB` (`65,536 B`) large tool/RAG payloads offloaded to GCS (`offload.min_bytes: 65536`) with an `80%` SHA-256 LRU deduplication hit rate (`20%` unique Class A `WriteObject` puts at `$0.005 / 1,000` + 30-day GCS Standard storage at `$0.020 / GiB-month`).
- **Analytics**: Incremental Dataform marts (`deploy/dataform/`) refreshed every 15 minutes + 50 ad-hoc investigator queries/month scanning 7-day partition-pruned windows (`$6.25 / TiB`, first `1 TiB/month` free).

| Cost Line Item | **Small Deployment**<br>`100` spans/s (`175` rec/s)<br>`262.98M` spans / mo | **Medium Deployment**<br>`2,500` spans/s (`4,375` rec/s)<br>`6.57B` spans / mo | **Large Deployment**<br>`25,000` spans/s (`43,750` rec/s)<br>`65.75B` spans / mo |
|---|---:|---:|---:|
| **Monthly Uncompressed Wire Volume (`GiB` / `TiB`)** | `1,069.6 GiB / mo` (`1.04 TiB`) | `26,740.6 GiB / mo` (`26.11 TiB`) | `267,405.9 GiB / mo` (`261.14 TiB`) |
| **1. Storage Write API (`_default` stream)**<br>*(First `2,048 GiB/mo` free, then `$0.025/GiB`)* | **`$0.00`**<br>*(within `2 TiB` free tier)* | **`$617.31`**<br>*(`24,692.6 GiB` billable)* | **`$6,633.95`**<br>*(`265,357.9 GiB` billable)* |
| **2. BigQuery Physical Storage (90d active + 14d fail-safe)**<br>*(`6:1` Zstandard compression, `$0.040/GiB-mo`)* | **`$24.36`**<br>*(`609.1 GiB` physical across 104d)* | **`$609.12`**<br>*(`15,228.0 GiB` / `14.87 TiB` physical)* | **`$6,091.22`**<br>*(`152,280.4 GiB` / `148.71 TiB` physical)* |
| **3. GCS Payload Offload** (`5%` of spans @ `64 KiB`, `80%` dedup)<br>*(Class A ops `$0.005/1k` + 30d Standard storage `$0.020/GiB`)* | **`$16.36`**<br>*(`2.63M` unique puts + `160.5 GiB`)* | **`$408.98`**<br>*(`65.75M` unique puts + `4,012.8 GiB`)* | **`$4,089.80`**<br>*(`657.45M` unique puts + `40,127.6 GiB`)* |
| **4. Analytical Queries & Dataform Marts**<br>*(15-min incremental marts + 50 ad-hoc 7d queries @ `$6.25/TiB`)* | **`$6.25`**<br>*(~`2 TiB` scanned, `1 TiB` free)* | **`$156.25`**<br>*(~`26 TiB` scanned, `1 TiB` free)* | **`$1,562.50`**<br>*(~`251 TiB` scanned, `1 TiB` free)* |
| **5. Collector Compute Footprint**<br>*(`~1.23–1.31` vCPU & `~138 MiB` RSS per `10k` spans/s on GKE)* | **`$12.00`**<br>*(shared `0.25` vCPU pod)* | **`$48.00`**<br>*(`2` replicas $\times$ `1` vCPU)* | **`$240.00`**<br>*(`5` replicas $\times$ `2` vCPU)* |
| **Total Monthly TCO (Base Warehouse + Marts)** | **`$58.97 / month`** | **`$1,839.67 / month`** | **`$18,617.46 / month`** |
| **Unit Cost per Million Spans (`Base TCO`)** | **`$0.22 / 1M spans`** | **`$0.28 / 1M spans`** | **`$0.28 / 1M spans`** |
| *Optional Add-On: Continuous Queries (1-yr Baseline + PAYG Autoscale)* | *`+$1,752.00 / mo` (`50` slots)* | *`+$2,847.00 / mo` (`75` avg slots)* | *`+$9,198.00 / mo` (`250` avg slots)* |

---

## 8. FinOps Checklist for Production Operators

1. **Always Enable `PHYSICAL` Storage Billing on the Dataset**:
   ```sql
   ALTER SCHEMA `<gcp-project>.otel`
   SET OPTIONS (storage_billing_model = 'PHYSICAL');
   ```
2. **Enforce Partition Filters on High-Volume Signal Tables**:
   Set `traces.require_partition_filter: true`, `logs.require_partition_filter: true`, and `metrics.require_partition_filter: true` in `example/otel-collector-config.yml` so ad-hoc queries without a `WHERE start_time >= ...` or `WHERE timestamp >= ...` predicate are rejected before scanning historical partitions.
3. **Set Tiered Partition Expiration**:
   Retain high-cardinality raw spans/logs for `30`–`90` days (`partition_expiration_days: 90`) while keeping lightweight aggregated Dataform marts (`deploy/dataform/`) for `365+` days.
4. **Right-Size `offload.min_bytes`**:
   Keep the default `offload.min_bytes: 262144` (`256 KiB`) or lower to `65536` (`64 KiB`) so routine short prompts stay inline in BigQuery (zero GCS Class A operation charge) while oversized RAG documents and MCP resource dumps are offloaded and deduplicated by SHA-256 (`offload.cache_size: 4096`).
5. **Use `max_staleness` for High-QPS Operational Dashboards**:
   Setting `traces.max_staleness: "15m"` allows BigQuery to serve queries from pre-materialized Capacitor storage metadata without scanning recently buffered streaming blocks, lowering query slot consumption and tail latency.
