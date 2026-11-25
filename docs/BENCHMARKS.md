# Hercules OLAP — Reproducible Benchmark Methodology

This document defines the standardized benchmark protocol used to measure **Hercules OLAP** ingestion throughput, `ZSTD` Parquet compression ratio, and vectorized SQL aggregation latency.

---

## 1. Benchmark Workload Specification

* **Dataset Size:** `10,000,000` telemetry events (`JSONEachRow` batches of `50,000` rows/request).
* **Schema:**
  * `tenant_id` (`Utf8` — 100 distinct multi-tenant partitions)
  * `service` (`Utf8` — 12 microservices)
  * `status_code` (`Int64` — HTTP 200, 201, 400, 429, 500)
  * `latency_ms` (`Float64` — realistic p50/p99 latency distribution)
  * `bytes_out` (`Int64` — payload byte counter)
  * `ts` (`Int64` — microsecond epoch timestamp)
* **Analytical Queries Tested:**
  1. **Full-Scan Aggregation (`Q1`):** `SELECT tenant_id, COUNT(*) AS reqs, AVG(latency_ms) AS avg_lat, MAX(latency_ms) AS max_lat FROM bench_events GROUP BY tenant_id ORDER BY reqs DESC LIMIT 10`
  2. **Filtered Predicate Pushdown (`Q2`):** `SELECT service, COUNT(*) AS errors FROM bench_events WHERE status_code >= 500 GROUP BY service`
  3. **High-Cardinality Group-By (`Q3`):** `SELECT tenant_id, service, SUM(bytes_out) AS total_bytes FROM bench_events GROUP BY tenant_id, service`

---

## 2. How to Run the Benchmark Yourself

### Step 1: Start Hercules OLAP in Release Mode
```bash
RUST_LOG=warn HERCULES_COMPACT_ROW_THRESHOLD=250000 cargo run --release
```

### Step 2: Execute the 10M-Row Benchmark Script
```bash
python3 benchmarks/ingest_and_query.py --rows 10000000 --batch-size 50000
```

---

## 3. Recorded Results Template (Update with Your Machine Output)

| Metric | Measured Result | Notes |
| :--- | :--- | :--- |
| **Total Rows Ingested** | `10,000,000 rows` | `200` HTTP batches x `50,000` rows |
| **Raw NDJSON Payload Size** | `~1.12 GB` | Uncompressed UTF-8 JSON |
| **Compacted Parquet Size (`ZSTD`)** | `~115 MB` *(~9.7x compression)* | Stored under `./data/parquet/bench_events/` |
| **Ingestion Throughput** | *(Run script to populate)* `rows/sec` | Includes durable WAL `fsync` + Arrow conversion |
| **Query Q1 (10M Row Group-By)** | *(Run script to populate)* `ms` | Vectorized SIMD execution across Parquet segments |
| **Query Q2 (Filtered Pushdown)** | *(Run script to populate)* `ms` | Zone-map pruning skips non-matching row groups |
