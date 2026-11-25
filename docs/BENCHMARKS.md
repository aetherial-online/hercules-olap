# Hercules OLAP — Reproducible Benchmark Results

This document reports reproducible ingestion throughput, `ZSTD` Parquet compression ratios, and vectorized SQL query latencies measured on **Hercules OLAP `v0.1.0`** running in single-node mode with **durable WAL `fsync` (`sync_data`) enabled**.

---

## 1. Benchmark Workload Specification

* **Dataset Size:** `10,000,000` telemetry events (`200` HTTP `JSONEachRow` batches × `50,000` rows/batch; `1,153.75 MB` raw uncompressed NDJSON).
* **Schema (6 Columns):**
  * `tenant_id` (`Utf8` — 100 distinct multi-tenant partitions)
  * `service` (`Utf8` — 12 microservices)
  * `status_code` (`Int64` — HTTP `200`, `201`, `400`, `429`, `500`)
  * `latency_ms` (`Float64` — continuous latency distribution)
  * `bytes_out` (`Int64` — payload byte counter)
  * `ts` (`Int64` — microsecond epoch timestamp)
* **Durability Guarantee:** Every ingested batch is appended to an on-disk Write-Ahead Log and flushed via `sync_data()` (`fsync`) prior to HTTP `200` acknowledgment.

---

## 2. Measured Ingestion & Storage Results (10M Rows)

| Benchmark Profile | Script | Ingestion Time | Sustained Throughput | Compacted Parquet (`ZSTD`) | Compression Ratio |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Single-Thread Sync** (`1` client) | `ingest_and_query.py` | `64.22 s` | **`155,726 rows/sec`** | `81.14 MB` | **`14.22x`** |
| **Concurrent (`8` workers)** | `ingest_and_query_workers.py` | `15.99 s` | **`625,383 rows/sec`** | `71.34 MB` | **`16.17x`** |
| **Concurrent (`16` workers)** | `ingest_and_query_workers.py` | **`14.25 s`** | **`701,931 rows/sec`** | **`70.33 MB`** | **`16.40x`** |

---

## 3. Analytical SQL Query Latency (10,000,000 Rows)

| Query ID | Analytical Workload | SQL Statement | Measured Latency |
| :--- | :--- | :--- | :---: |
| **`Q1`** | Full-Scan Group-By (Top 10) | `SELECT tenant_id, COUNT(*) AS reqs, AVG(latency_ms) AS avg_lat FROM bench_events GROUP BY tenant_id ORDER BY reqs DESC LIMIT 10` | **`135.55 ms`** |
| **`Q2`** | Filtered Predicate Pushdown | `SELECT service, COUNT(*) AS errors FROM bench_events WHERE status_code >= 500 GROUP BY service ORDER BY errors DESC` | **`99.13 ms`** |
| **`Q3`** | Multi-Column Aggregation | `SELECT tenant_id, service, SUM(bytes_out) AS total_bytes FROM bench_events GROUP BY tenant_id, service LIMIT 10` | **`237.57 ms`** |

---

## 4. Reproduce These Benchmarks Yourself

```bash
# 1. Start Hercules OLAP in release mode
RUST_LOG=warn cargo run --release

# 2. Run the 16-worker concurrent 10M-row benchmark
python3 benchmarks/ingest_and_query_workers.py --rows 10000000 --workers 16
```
