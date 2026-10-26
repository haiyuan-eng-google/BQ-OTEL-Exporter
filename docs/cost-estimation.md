# BigQuery OpenTelemetry Exporter — Cost Estimation & TCO Guide (`v0.1.0`)

This guide provides a component-by-component Total Cost of Ownership (TCO) model for running `bigqueryexporter` in production across five billing dimensions:

1. **BigQuery Storage Write API Ingestion** (`AppendRows` committed streams)
2. **BigQuery Table Storage** (Logical vs. Physical compressed storage with partitioning, clustering, and TTL)
3. **Google Cloud Storage (GCS) Payload Offloading** (`payload_refs` `ObjectRef` + SHA-256 content-addressed deduplication)
4. **BigQuery Query & Semantic View Compute** (On-demand TB scanned vs. Edition slots, Partition/Cluster pruning, Scheduled Rollup Tables, and Continuous Queries)
5. **OpenTelemetry Collector Compute** (CPU and RAM sizing from the `v0.1.0` benchmark envelope plus Cloud Run / GKE Autopilot minimum instance increments)

---

## 1. Pricing Reference & Modeling Methodology

### 1.1 Official Google Cloud Regional SKU Sources (October 2026)

All unit prices below reference public list prices for the **`US` multi-region** (BigQuery and Cloud Storage) and **`us-central1` Tier 1** (Cloud Run and GKE Autopilot) as of **October 2026**:

| Service | Resource / SKU | Unit List Price (`US` / `us-central1`) | Free Tier / Notes | Reference |
|---|---|---:|---|---|
| **BigQuery Storage Write API** | Committed stream ingestion (`AppendRows`) | **`$0.025` / GiB** (`$25.60` / TiB) | First **2 TiB (`2,048 GiB`) / month** per billing account free | [cloud.google.com/bigquery/pricing](https://cloud.google.com/bigquery/pricing) |
| **BigQuery Storage (Logical)** | Active logical bytes (< 90 days unmodified) | **`$0.020` / GiB-month** | First **10 GiB / month** free | [cloud.google.com/bigquery/pricing](https://cloud.google.com/bigquery/pricing) |
| **BigQuery Storage (Logical)** | Long-term logical bytes ($\ge$ 90 days unmodified) | **`$0.010` / GiB-month** | Automatic 50% discount after 90 days | [cloud.google.com/bigquery/pricing](https://cloud.google.com/bigquery/pricing) |
| **BigQuery Storage (Physical)** | Active physical compressed bytes (< 90 days) | **`$0.040` / GiB-month** | First **10 GiB / month** free; includes time-travel + fail-safe bytes | [cloud.google.com/bigquery/pricing](https://cloud.google.com/bigquery/pricing) |
| **BigQuery Storage (Physical)** | Long-term physical compressed bytes ($\ge$ 90 days) | **`$0.020` / GiB-month** | Automatic 50% discount after 90 days | [cloud.google.com/bigquery/pricing](https://cloud.google.com/bigquery/pricing) |
| **BigQuery Analysis (On-Demand)** | Bytes scanned by queries | **`$6.25` / TiB** | First **1 TiB / month** free | [cloud.google.com/bigquery/pricing](https://cloud.google.com/bigquery/pricing) |
| **BigQuery Enterprise Edition** | Autoscaling / baseline slot capacity | **`$0.06` / slot-hour** (`$0.048` 1-yr commit) | Required for BigQuery Continuous Queries | [cloud.google.com/bigquery/pricing](https://cloud.google.com/bigquery/pricing) |
| **Cloud Storage (Standard)** | Object storage (`US` multi-region) | **`$0.026` / GiB-month** (`$0.020` `us-central1`) | Regional `us-central1` saves 23% when co-located | [cloud.google.com/storage/pricing](https://cloud.google.com/storage/pricing) |
| **Cloud Storage (Operations)** | Class A operations (`storage.objects.insert`) | **`$0.005` / 1,000 ops** (`$5.00` / 1M ops) | Dedup cache hits (`ifGenerationMatch=0`) eliminate redundant uploads | [cloud.google.com/storage/pricing](https://cloud.google.com/storage/pricing) |
| **GKE Autopilot / Cloud Run** | Dedicated / always-allocated vCPU & RAM (`us-central1`) | **`~$24` / vCPU-mo**, **`~$2.65` / GiB-mo** | Subject to minimum pod/instance increments (Section 6) | [cloud.google.com/kubernetes-engine/pricing](https://cloud.google.com/kubernetes-engine/pricing), [cloud.google.com/run/pricing](https://cloud.google.com/run/pricing) |

> **Unit Basis (`GiB` / `TiB`)**: All byte-to-volume conversions across Sections 2–7 use binary units ($1\text{ KiB} = 2^{10} = 1{,}024\text{ B}$, $1\text{ MiB} = 2^{20}\text{ B}$, $1\text{ GiB} = 2^{30} = 1{,}073{,}741{,}824\text{ B}$, and $1\text{ TiB} = 2^{40}\text{ B} = 1{,}024\text{ GiB}$) so Storage Write API (`2 TiB = 2,048 GiB` free tier), BigQuery Logical/Physical Storage (`GiB-month`), Cloud Storage (`GiB-month`), and On-Demand SQL scanning (`TiB`) share a single consistent denominator.

### 1.2 Measured Benchmark Inputs vs. Illustrative Engineering Assumptions

To keep capacity planning transparent, this guide distinguishes between **measured benchmark values** from `cmd/bq-otel-testbench` ([`docs/benchmarks/v0.1.0-performance-envelope.md`](benchmarks/v0.1.0-performance-envelope.md)) and **illustrative workload assumptions** that vary by deployment:

* **Measured Benchmark Inputs (`cmd/bq-otel-testbench`)**:
  * Exact serialized `ProtoRows` wire bytes per row across `otel_spans` (`2,895 B` at `1 KiB`, `5,966 B` at `4 KiB`, `18,255 B` at `16 KiB` inline, `1,983 B` local offload), `otel_logs` (`2,384 B` at `1 KiB`, `5,456 B` at `4 KiB`, `17,745 B` at `16 KiB` inline, `1,472 B` local offload), and `otel_metrics` (`1,252 B`).
  * Exporter throughput (`15,876`–`29,459 rows/s` in `bqfake` across `stream_pool_size=1..16`, `23,544`–`28,983 rows/s` at `stream_pool_size=4`; `9,185`–`10,404 rows/s` in live BigQuery), whole-process CPU (`1.89`–`3.27` cores per `10k rows/s` unredacted across `stream_pool_size=1..16`, `2.25`–`2.96` at `stream_pool_size=4`, `3.05`–`3.14` with Step-0 redaction, and `1.24`–`1.66` in live BigQuery), and SHA-256 content-addressed offload deduplication (`99.92%` cache hit rate on a 16-entry repeated prompt corpus in Suite C).
* **Illustrative Engineering Assumptions (Configure to Match Your Workload)**:
  * **Composite Agent Turn Shape & Fleet Tiers**: `1` agent turn = `4` spans (`invoke_agent`, `chat`, `execute_tool`, `mcp.client.operation`) + `2` GenAI event logs + `2` histogram metric data points (`gen_ai.client.token.usage` and `gen_ai.client.operation.duration`) = **`8` BigQuery rows/turn**. The three workload tiers scale by $10\times$:
    * **Tier 1 (`100K turns/day`)**: `400K spans/day` (`4.6 spans/s` avg, `~46–100 spans/s` burst) / `800K combined rows/day` (`9.3 rows/s` avg).
    * **Tier 2 (`1M turns/day`)**: `4M spans/day` (`46.3 spans/s` avg, `~463–2,500 spans/s` burst) / `8M combined rows/day` (`92.6 rows/s` avg).
    * **Tier 3 (`10M turns/day`)**: `40M spans/day` (`463.0 spans/s` avg, `~4,630–25,000 spans/s` burst) / `80M combined rows/day` (`925.9 rows/s` avg).
  * **BigQuery Physical Compression Ratio**: **`6:1` baseline** (typical Capacitor + Zstandard compression range `4:1`–`8:1` on repetitive JSON telemetry; high-entropy UUIDs or base64 blobs compress closer to `2:1`–`3:1`).
  * **Production Offload Deduplication Hit Rate**: **`80%` illustrative assumption** for production fleets that repeat system instructions, tool schemas, and RAG context headers across turns (`0%` worst-case if every offloaded payload is unique; `99.9%` best-case on static templates).
  * **Scheduled Rollup Scan Reduction & Continuous Query Slot Sizing**: The `99.96%` query scan reduction in Section 5.3 assumes `20` dashboard refreshes/hour over a 7-day window reading pre-aggregated `INT64`/`FLOAT64` columns from a scheduled incremental hourly rollup table materializing `v_gen_ai_model_costs` instead of scanning raw `JSON` attributes on every dashboard load; the `50` / `100` / `250` Continuous Query slot figures in Section 5.4 are illustrative capacity-planning starting points that should be validated against your query complexity.

---

## 2. BigQuery Storage Write API Ingestion Cost

BigQuery bills Storage Write API ingestion (`AppendRows` on committed default/explicit streams) at **`$0.025 per GiB`** (`$25.60 per TiB`, where $1\text{ GiB} = 2^{30} = 1{,}073{,}741{,}824\text{ bytes}$), with the **first 2 TiB (`2,048 GiB`) per month free** per billing account.

### 2.1 Measured Per-Row Wire Size by Signal & Payload Tier

Measured directly from `otelcol_exporter_bigquery_append_bytes / otelcol_exporter_bigquery_acknowledged_rows` using `cmd/bq-otel-testbench`:

| Signal Table | Typical (`1 KiB` Payload) | Medium (`4 KiB` Payload) | Large Inline (`16 KiB`, No Offload) | Large Offloaded (`16 KiB`, `offload.enabled=true`) |
|---|---:|---:|---:|---:|
| `otel_spans` | `2,895 B` (`2.83 KiB`) | `5,966 B` (`5.83 KiB`) | `18,255 B` (`17.83 KiB`) | `1,983 B` (URI only) / `~2,228 B` (with `payload_refs` `ObjectRef`) |
| `otel_logs` | `2,384 B` (`2.33 KiB`) | `5,456 B` (`5.33 KiB`) | `17,745 B` (`17.33 KiB`) | `1,472 B` (URI only) / `~1,717 B` (with `payload_refs` `ObjectRef`) |
| `otel_metrics` | `1,252 B` (`1.22 KiB`) | `1,252 B` (`1.22 KiB`) | `1,252 B` (`1.22 KiB`) | `1,252 B` (`1.22 KiB`) |
| **Composite 8-Row Agent Turn** (`4` spans + `2` logs + `2` metrics) | **`18,852 B / turn`** (`18.41 KiB`) | **`37,280 B / turn`** (`36.41 KiB`) | **`111,014 B / turn`** (`108.41 KiB`) | **`14,850 B / turn`** (`14.50 KiB` BQ wire, **`-86.6%`**) |

> **Why `otel_metrics` stays constant across `-payload-bytes`**: OpenTelemetry GenAI metric data points (`gen_ai.client.token.usage` and `gen_ai.client.operation.duration`) carry bounded dimension attributes and trace-correlated exemplars (`trace_id`, `span_id`, `gen_ai.agent.name`), never full prompt/completion bodies.

### 2.2 Ingestion Cost Formula

$$\text{Monthly Write Volume (GiB)} = \frac{\text{Turns/Day} \times 30 \times \text{Bytes/Turn}}{2^{30}}$$

$$\text{Monthly Write API Cost (\$)} = \max\!\left(0,\; \text{Monthly Write Volume (GiB)} - 2{,}048\text{ GiB}\right) \times \$0.025$$

### 2.3 Monthly Storage Write API Cost Across Workload Tiers

| Fleet Tier | Turns / Day | Turns / Month (30d) | Rows / Month (`8x`) | Avg Rows / Sec (`Spans/s`) | Payload Profile | Monthly Write Volume (`GiB`) | Billable Volume (After `2,048 GiB` Free) | Monthly Write API Cost |
|---|---:|---:|---:|---:|---|---:|---:|---:|
| **Tier 1 — Team / Pilot** | `100,000` | `3,000,000` | `24,000,000` | `9.3 rows/s` (`4.6 spans/s`) | Typical (`1 KiB`) | `52.67 GiB` | `0 GiB` | **`$0.00`** |
| **Tier 1 — Team / Pilot** | `100,000` | `3,000,000` | `24,000,000` | `9.3 rows/s` (`4.6 spans/s`) | Large Inline (`16 KiB`) | `310.17 GiB` | `0 GiB` | **`$0.00`** |
| **Tier 2 — Production** | `1,000,000` | `30,000,000` | `240,000,000` | `92.6 rows/s` (`46.3 spans/s`) | Typical (`1 KiB`) | `526.72 GiB` | `0 GiB` | **`$0.00`** |
| **Tier 2 — Production** | `1,000,000` | `30,000,000` | `240,000,000` | `92.6 rows/s` (`46.3 spans/s`) | Medium (`4 KiB`) | `1,041.59 GiB` | `0 GiB` | **`$0.00`** |
| **Tier 2 — Production** | `1,000,000` | `30,000,000` | `240,000,000` | `92.6 rows/s` (`46.3 spans/s`) | Large Inline (`16 KiB`) | `3,101.70 GiB` | `1,053.70 GiB` | **`$26.34`** |
| **Tier 2 — Production** | `1,000,000` | `30,000,000` | `240,000,000` | `92.6 rows/s` (`46.3 spans/s`) | Large Offloaded (`16 KiB`) | `414.90 GiB` | `0 GiB` | **`$0.00`** |
| **Tier 3 — Enterprise Scale** | `10,000,000` | `300,000,000` | `2,400,000,000` | `925.9 rows/s` (`463.0 spans/s`) | Typical (`1 KiB`) | `5,267.19 GiB` | `3,219.19 GiB` | **`$80.48`** |
| **Tier 3 — Enterprise Scale** | `10,000,000` | `300,000,000` | `2,400,000,000` | `925.9 rows/s` (`463.0 spans/s`) | Medium (`4 KiB`) | `10,415.91 GiB` | `8,367.91 GiB` | **`$209.20`** |
| **Tier 3 — Enterprise Scale** | `10,000,000` | `300,000,000` | `2,400,000,000` | `925.9 rows/s` (`463.0 spans/s`) | Large Inline (`16 KiB`) | `31,016.95 GiB` | `28,968.95 GiB` | **`$724.22`** |
| **Tier 3 — Enterprise Scale** | `10,000,000` | `300,000,000` | `2,400,000,000` | `925.9 rows/s` (`463.0 spans/s`) | Large Offloaded (`16 KiB`) | `4,149.04 GiB` | `2,101.04 GiB` | **`$52.53`** |

---

## 3. BigQuery Table Storage Cost (Logical vs. Physical Billing)

### 3.1 Logical vs. Physical Storage Billing (`6:1` Illustrative Compression Model)

BigQuery datasets can be configured for **Logical Storage Billing** or **Physical (Compressed) Storage Billing** (`ALTER SCHEMA ... SET OPTIONS(storage_billing_model = 'PHYSICAL')`):

* **Illustrative Compression Assumption**: Columnar Capacitor encoding + Zstandard compression on `otel_spans` (`CLUSTER BY service_name, trace_id, name`), `otel_logs` (`CLUSTER BY service_name, trace_id, severity_number`), `otel_metrics` (`CLUSTER BY service_name, metric_name, metric_type`), and `otel_quarantine` (`CLUSTER BY signal, error_code, service_name`) typically achieves a **`4:1` to `8:1` compression ratio** on repetitive JSON keys, resource attributes, and histogram buckets.
* Even at unit price `$0.040 / GiB-month` (`2x` the logical `$0.020 / GiB-month` rate) and accounting for the **7-day time-travel + 7-day fail-safe window** (`+14 days` of active physical retention overhead), switching the telemetry dataset to `PHYSICAL` billing reduces net BigQuery storage spend by **55%–68%** when compression exceeds `4:1`.
* **Partition Expiration (TTL) & Long-Term Storage Discount**: Because append-only partitions in `otel_spans` (`TIMESTAMP_TRUNC(start_time, HOUR)`), `otel_logs` (`DATE(timestamp)`), `otel_metrics` (`DATE(timestamp)`), and `otel_quarantine` (`DATE(ingest_time)`) are never modified after the partition closes, partitions retained beyond **90 days** automatically drop 50% to the **Long-Term Storage** rate (`$0.010 / GiB-month` logical or `$0.020 / GiB-month` physical). Under a 365-day physical retention window, `104` physical days (`90` active days + `14` time-travel/fail-safe days) bill at the `$0.040 / GiB-month` active rate (after the first `10 GiB` free), while `275` days (`days 91..365`) bill at the `$0.020 / GiB-month` long-term rate.

### 3.2 Steady-State Monthly BigQuery Storage Cost (`6:1` Physical Compression Assumption)

$$\text{Physical GiB (Steady State)} = \frac{\text{Daily Logical GiB}}{\text{Compression Ratio }(6)} \times \left(\text{Retention Days} + 14\text{ time-travel/fail-safe days}\right)$$

$$\text{365d Physical Cost (\$)} = \max\!\left(0,\; 104 \times D_{\text{phys}} - 10\text{ GiB}\right) \times \$0.040 + \left(275 \times D_{\text{phys}}\right) \times \$0.020$$

| Fleet Tier | Payload Profile | Daily Logical GiB | 30-Day Logical Billing (`$0.02/GiB`, 10 GiB Free) | 30-Day Physical Billing (`6:1`, `$0.04/GiB` + 14d Fail-Safe) | 90-Day Physical Billing (`6:1`, `104d` Active) | 365-Day Physical Billing (`6:1`, `104d` Active @ `$0.04` + `275d` Long-Term @ `$0.02`) |
|---|---|---:|---:|---:|---:|---:|
| **Tier 1 (`100K/day`)** | Typical (`1 KiB`) | `1.76 GiB/day` | `$0.85 / mo` (`52.7 GiB`) | **`$0.12 / mo`** (`12.9 GiB`, first 10 GiB free) | **`$0.82 / mo`** (`30.4 GiB`) | **`$2.43 / mo`** (`110.9 GiB`) |
| **Tier 2 (`1M/day`)** | Typical (`1 KiB`) | `17.56 GiB/day` | `$10.33 / mo` (`526.7 GiB`) | **`$4.75 / mo`** (`128.8 GiB`) | **`$11.77 / mo`** (`304.3 GiB`) | **`$27.87 / mo`** (`1,109.0 GiB`) |
| **Tier 2 (`1M/day`)** | Large Inline (`16 KiB`) | `103.39 GiB/day` | `$61.83 / mo` (`3,101.7 GiB`) | **`$29.93 / mo`** (`758.2 GiB`) | **`$71.28 / mo`** (`1,792.1 GiB`) | **`$166.06 / mo`** (`6,530.8 GiB`) |
| **Tier 2 (`1M/day`)** | Large Offloaded (`16 KiB`) | `13.83 GiB/day` | `$8.10 / mo` (`414.9 GiB`) | **`$3.66 / mo`** (`101.4 GiB`) | **`$9.19 / mo`** (`239.7 GiB`) | **`$21.87 / mo`** (`873.6 GiB`) |
| **Tier 3 (`10M/day`)** | Typical (`1 KiB`) | `175.57 GiB/day` | `$105.14 / mo` (`5,267.2 GiB`) | **`$51.10 / mo`** (`1,287.5 GiB`) | **`$121.33 / mo`** (`3,043.3 GiB`) | **`$282.27 / mo`** (`11,090.4 GiB`) |
| **Tier 3 (`10M/day`)** | Large Inline (`16 KiB`) | `1,033.90 GiB/day` | `$620.14 / mo` (`31,017.0 GiB`) | **`$302.88 / mo`** (`7,581.9 GiB`) | **`$716.44 / mo`** (`17,920.9 GiB`) | **`$1,664.18 / mo`** (`65,307.9 GiB`) |
| **Tier 3 (`10M/day`)** | Large Offloaded (`16 KiB`) | `138.30 GiB/day` | `$82.78 / mo` (`4,149.0 GiB`) | **`$40.17 / mo`** (`1,014.2 GiB`) | **`$95.49 / mo`** (`2,397.2 GiB`) | **`$222.27 / mo`** (`8,736.0 GiB`) |

---

## 4. GCS Payload Offloading Cost (`offload.enabled: true`)

When `offload.enabled: true` (`min_bytes: 4096`), `internal/offload` uploads large prompt/response/tool strings to `gs://<bucket>/<prefix>/YYYY/MM/DD/<signal>/<sha256>.json` with `ifGenerationMatch=0` and records a BigQuery `ObjectRef` in `payload_refs`.

### 4.1 Deduplication Impact on Class A Operations & GCS Bytes

Each `16 KiB` agent turn contains `6` eligible large payloads (`4` spans + `2` logs = `98,304 B / turn`).

* **Benchmark Measurement (`Suite C`)**: On a repeated 16-entry prompt corpus, the in-memory SHA-256 LRU cache (`offload.cache_size: 10000`) achieved **`35,473` deduplication hits out of `35,500` spans (`99.92%`)**.
* **Illustrative Production Model (`80%` Dedup Hit Rate)**: Assuming `80%` of large payloads in a production agent fleet hit the SHA-256 cache (shared system prompts, tool definitions, and multi-turn conversation prefixes) and `20%` (`1.2` payloads/turn) trigger a new GCS object write:

| Fleet Tier (`16 KiB` Payloads) | Raw Offload Candidates / Mo | Unique GCS Uploads / Mo (Assumed `80%` Dedup) | GCS Class A Ops Cost (`$5.00 / 1M`) | Unique GCS Volume / Mo (`16 KiB`) | GCS Standard Storage (`30d`, `$0.026/GiB`) | Total GCS Offload Cost (`80%` Dedup) | Worst-Case (`0%` Dedup, 100% Unique) |
|---|---:|---:|---:|---:|---:|---:|---:|
| **Tier 1 (`100K/day`)** | `18,000,000` | `3,600,000` | `$18.00 / mo` | `54.93 GiB` | `$1.43 / mo` | **`$19.43 / mo`** | `$97.15 / mo` |
| **Tier 2 (`1M/day`)** | `180,000,000` | `36,000,000` | `$180.00 / mo` | `549.32 GiB` | `$14.28 / mo` | **`$194.28 / mo`** | `$971.40 / mo` |
| **Tier 3 (`10M/day`)** | `1,800,000,000` | `360,000,000` | `$1,800.00 / mo` | `5,493.16 GiB` | `$142.82 / mo` | **`$1,942.82 / mo`** | `$9,714.10 / mo` |

### 4.2 When to Enable GCS Offload (`min_bytes` Threshold Guidance)

* **At `min_bytes: 4096` (`16 KiB` payloads)**: GCS Class A operation fees (`$5.00 / 1M PUTs`) can exceed the BigQuery Write API + Physical Storage savings unless SHA-256 deduplication exceeds `~85%` or payloads are retained for $\ge 180$ days in Coldline/Archive storage.
* **Recommended Production Threshold (`min_bytes: 32768` to `65536`)**: Set `offload.min_bytes: 65536` (`64 KiB`) in cost-sensitive environments so routine `1 KiB`–`16 KiB` turns stay inline in BigQuery (where Capacitor `6:1` compression is cheaper than per-object GCS Class A operations), while `64 KiB`–`512 KiB` RAG dumps and full-file tool outputs are offloaded before they approach the `256 KiB` `limits.max_value_bytes` cap or `10 MiB` `AppendRows` ceiling.

---

## 5. BigQuery Query, Semantic View & Continuous Query Cost

### 5.1 Partition & Cluster Pruning Rules

The four tables in `internal/schema/schema.go` use signal-specific partitioning and clustering:

* **`otel_spans`**: `PARTITION BY TIMESTAMP_TRUNC(start_time, HOUR) CLUSTER BY service_name, trace_id, name` (hourly partitioning).
* **`otel_logs`**: `PARTITION BY DATE(timestamp) CLUSTER BY service_name, trace_id, severity_number` (daily partitioning).
* **`otel_metrics`**: `PARTITION BY DATE(timestamp) CLUSTER BY service_name, metric_name, metric_type` (daily partitioning).
* **`otel_quarantine`**: `PARTITION BY DATE(ingest_time) CLUSTER BY signal, error_code, service_name` (daily partitioning).

1. **Always filter on the table's partition column (`start_time`, `timestamp`, or `ingest_time`)**: Omitting a `WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ...)` predicate on `otel_spans` / `v_gen_ai_spans` (or `WHERE timestamp >= ...` on `otel_logs` / `otel_metrics`) causes a full-table scan across all retained partitions.
2. **Always include `service_name` in dashboard filters**: Leading-column clustering on `service_name` across `otel_spans`, `otel_logs`, and `otel_metrics` allows BigQuery to prune storage blocks that do not contain the target agent service.

### 5.2 Column Projection Discipline (Avoid `SELECT *` on Semantic Views)

In `v_gen_ai_spans` and `v_gen_ai_logs`, `JSON_VALUE(attributes, ...)` expressions read the underlying `attributes JSON` column (`~1.8 KiB`–`16 KiB` per row):

* **Lightweight Metadata Query** (`SELECT trace_id, span_id, service_name, name, duration_nano, status.code FROM otel_spans WHERE start_time >= ...`): scans only **`~140 B / row`** (`~4.8%` of a `2,895 B` span row).
* **Full Semantic View Query** (`SELECT * FROM v_gen_ai_spans WHERE start_time >= ...`): scans `attributes`, `resource.attributes`, and `instrumentation_scope` (**`~2,450 B / row`** on `1 KiB` inline spans, or **`~720 B / row`** when large payloads are offloaded to GCS).

### 5.3 Scheduled Incremental Hourly Rollup Pattern (Materializing `v_gen_ai_model_costs`, Illustrative `99.96%` Scan Reduction)

Logical companion views such as `v_gen_ai_agent_trajectory`, `v_gen_ai_model_costs`, and `v_gen_ai_sessions` are unmaterialized SQL views over `v_gen_ai_spans` (`otel_spans_dedup`). Just as `README.md` (*Materializing the dedup view*) recommends a scheduled `MERGE` bounded to a recent partition window, high-frequency operational dashboards (e.g., `20` panel refreshes/hour across a 7-day lookback window = `14,400` queries/month) should materialize `v_gen_ai_model_costs` into a scheduled incremental hourly rollup table:

* **Querying raw `v_gen_ai_spans` directly** (Tier 2: `4M spans/day` $\times 7\text{ days} \times 2{,}450\text{ B/span} \approx 63.9\text{ GiB/query}$):
  $$14{,}400\text{ queries/mo} \times 63.9\text{ GiB} = 898.6\text{ TiB/mo} \implies \mathbf{\$5{,}610/\text{mo}}\text{ (On-Demand)}$$
* **Querying a scheduled incremental hourly rollup table materializing `v_gen_ai_model_costs`** partitioned by `DATE(usage_hour)` and grouped/clustered on the `v_gen_ai_model_costs` dimensions `(usage_hour, service_name, gen_ai_agent_name, provider, model)` (`~5,000 rows/day` $\times 120\text{ B/row} \approx 4.2\text{ MiB}$ for 7 days):
  * 720 hourly incremental rollup runs/month scanning only the latest 1-hour partition of `v_gen_ai_spans` (`WHERE usage_hour >= TIMESTAMP_SUB(...)`): `720` $\times$ `0.38 GiB` = `0.27 TiB/mo`
  * 14,400 dashboard queries/month scanning `4.2 MiB` from the pre-aggregated hourly rollup table: `0.06 TiB/mo`
  * **Total monthly bytes scanned**: **`0.33 TiB/mo`** (within the **1 TiB/month free tier** $\rightarrow$ **`$0.00/mo`**, an illustrative **`99.96%` reduction** in scanned bytes).

### 5.4 Continuous Queries & Edition Slot Sizing (Illustrative Capacity Model)

BigQuery Continuous Queries (`SELECT ... FROM APPENDS(TABLE otel_spans, NULL)`) require a **BigQuery Enterprise (or Enterprise Plus) Edition** slot reservation with `job_type = CONTINUOUS`:

| Fleet Tier | Avg Row Rate | Illustrative Autoscaling Slot Cap (`0` Baseline) | Avg Slot Utilization (Illustrative) | Monthly Continuous Query Cost (`$0.06 / slot-hour` PAYG) | Monthly Cost with 1-Year Commit (`$0.048 / slot-hour`) |
|---|---:|---:|---:|---:|---:|
| **Tier 1 (`100K turns/day`)** | `9.3 rows/s` | `50 slots` | `~4 slots` (`8%`) | **`~$172.80 / mo`** | **`~$138.24 / mo`** |
| **Tier 2 (`1M turns/day`)** | `92.6 rows/s` | `100 slots` | `~12 slots` (`12%`) | **`~$518.40 / mo`** | **`~$414.72 / mo`** |
| **Tier 3 (`10M turns/day`)** | `925.9 rows/s` | `250 slots` | `~65 slots` (`26%`) | **`~$2,808.00 / mo`** | **`~$2,246.40 / mo`** |

> **Cost Tip**: If sub-second streaming alerts are not required, replace Continuous Queries with a 5-minute scheduled incremental query over `APPENDS(TABLE otel_spans, ...)` under On-Demand billing (`$0.00/mo` within the 1 TiB free tier for Tiers 1 and 2).

---

## 6. OpenTelemetry Collector Compute Cost

### 6.1 Benchmark Efficiency vs. Cloud Run / GKE Autopilot Minimum Billing Increments

From [`docs/benchmarks/v0.1.0-performance-envelope.md`](benchmarks/v0.1.0-performance-envelope.md) (`Suite A` & `Suite B`, `stream_pool_size=4`):

* **Exporter Throughput**: `23,544`–`28,983 rows/s` unredacted (`17,048`–`22,256 rows/s` with Step-0 redaction enabled) in `bqfake`; `9,185`–`10,404 rows/s` against live BigQuery Storage Write API.
* **Whole-Process CPU Consumption**: `2.25`–`2.96 CPU cores per 10,000 rows/s` unredacted (`3.05`–`3.14 CPU cores per 10,000 rows/s` with Step-0 redaction).
* **Why Production Pod Sizing Exceeds Raw Exporter CPU**:
  1. At Tier 1 (`9.3 rows/s`) and Tier 2 (`92.6 rows/s`), the exporter alone consumes **`~0.003 vCPU`** and **`~0.03 vCPU`** on average.
  2. However, GKE Autopilot and Cloud Run (always-allocated CPU) bill in discrete pod/instance increments (`0.25 vCPU / 0.5 GiB` minimum Autopilot regular pod; `1 vCPU / 2 GiB` standard production Collector replica), and production deployments run **2+ HA replicas** across zones to survive node drains, accommodate **`10x` traffic bursts**, and run OTLP receivers, `memory_limiter`, and `batch` processors alongside `bigqueryexporter`.

| Fleet Tier | Avg / Peak (`10x`) Row Rate | Raw Exporter CPU (Avg / `10x` Peak) | Recommended Production Topology (GKE Autopilot / Cloud Run `us-central1`) | Monthly Collector Compute Cost |
|---|---:|---|---:|---:|
| **Tier 1 (`100K turns/day`)** | `9.3` / `93 rows/s` | `0.003` / `0.03 vCPU` | `2` shared HA pods $\times$ (`0.25 vCPU`, `0.5 GiB` RAM) | **`~$14.65 / mo`** |
| **Tier 2 (`1M turns/day`)** | `92.6` / `926 rows/s` | `0.03` / `0.29 vCPU` | `2` HA replicas $\times$ (`1 vCPU`, `2 GiB` RAM) | **`~$58.60 / mo`** |
| **Tier 3 (`10M turns/day`)** | `925.9` / `9,259 rows/s` | `0.29` / `2.90 vCPU` | `4` HA replicas $\times$ (`2 vCPU`, `4 GiB` RAM) | **`~$234.40 / mo`** |

---

## 7. End-to-End Monthly TCO Summary Table

The table below combines all five cost components for a **30-day BigQuery Physical Storage window (`6:1` compression assumption)** plus the **scheduled incremental hourly rollup pattern** (materializing `v_gen_ai_model_costs` under On-Demand query billing):

| Cost Component | Tier 1 (`100K turns/day`, `1 KiB` Inline) | Tier 2 (`1M turns/day`, `1 KiB` Inline) | Tier 2 (`1M turns/day`, `16 KiB` Inline) | Tier 2 (`1M turns/day`, `16 KiB` Offloaded, `80%` Dedup) | Tier 3 (`10M turns/day`, `1 KiB` Inline) | Tier 3 (`10M turns/day`, `16 KiB` Inline) | Tier 3 (`10M turns/day`, `16 KiB` Offloaded, `80%` Dedup) |
|---|---:|---:|---:|---:|---:|---:|---:|
| **1. BigQuery Storage Write API** (`$0.025/GiB` after `2,048 GiB` free) | `$0.00` (`52.7 GiB`) | `$0.00` (`526.7 GiB`) | `$26.34` (`3,101.7 GiB`) | `$0.00` (`414.9 GiB`) | `$80.48` (`5,267.2 GiB`) | `$724.22` (`31,017.0 GiB`) | `$52.53` (`4,149.0 GiB`) |
| **2. BigQuery Physical Storage** (`30d` + `14d` fail-safe, `6:1` compressed) | `$0.12` (`12.9 GiB`) | `$4.75` (`128.8 GiB`) | `$29.93` (`758.2 GiB`) | `$3.66` (`101.4 GiB`) | `$51.10` (`1,287.5 GiB`) | `$302.88` (`7,581.9 GiB`) | `$40.17` (`1,014.2 GiB`) |
| **3. GCS Payload Offload** (Standard `30d` + Class A ops at `80%` dedup) | `$0.00` | `$0.00` | `$0.00` | `$194.28` (`$14.28` storage + `$180.00` ops) | `$0.00` | `$0.00` | `$1,942.82` (`$142.82` storage + `$1,800.00` ops) |
| **4. BigQuery Queries** (Hourly Rollup Table + 14.4k Dashboard Queries, On-Demand) | `$0.00` (`< 1 TiB` free tier) | `$0.00` (`0.33 TiB` < `1 TiB` free tier) | `$5.13` (`1.82 TiB` scanned) | `$0.00` (`0.24 TiB` < `1 TiB` free tier) | `$10.50` (`2.68 TiB` scanned) | `$107.50` (`18.20 TiB` scanned) | `$8.75` (`2.40 TiB` scanned) |
| **5. Collector Compute** (GKE Autopilot / Cloud Run HA Replicas) | `$14.65` (`2x 0.25 vCPU`) | `$58.60` (`2x 1 vCPU`) | `$58.60` (`2x 1 vCPU`) | `$58.60` (`2x 1 vCPU`) | `$234.40` (`4x 2 vCPU`) | `$234.40` (`4x 2 vCPU`) | `$234.40` (`4x 2 vCPU`) |
| **Total Monthly TCO** | **`$14.77 / mo`** | **`$63.35 / mo`** | **`$120.00 / mo`** | **`$256.54 / mo`** *(or **`$120.00 / mo`** inline when `min_bytes: 65536`)* | **`$376.48 / mo`** | **`$1,369.00 / mo`** | **`$2,278.67 / mo`** *(or **`$1,369.00 / mo`** inline when `min_bytes: 65536`)* |
| **Unit Cost per `1,000` Agent Turns** | **`$0.0049`** | **`$0.0021`** | **`$0.0040`** | **`$0.0086`** | **`$0.0013`** | **`$0.0046`** | **`$0.0076`** |
