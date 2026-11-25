<div align="center">

# HERCULES OLAP
## Aetherial Stateless-First Columnar Analytics Engine



[![CI Verification](https://github.com/aetherial-online/hercules-olap/actions/workflows/ci.yml/badge.svg)](https://github.com/aetherial-online/hercules-olap/actions/workflows/ci.yml)
[![License: AGPL v3](https://badgen.net/badge/License/AGPLv3/blue)](./LICENSE)
[![Commercial License](https://badgen.net/badge/Commercial/Available/purple)](./COMMERCIAL_LICENSE.md)
[![Core: Rust](https://badgen.net/badge/Core/Rust/red)](https://rust-lang.org)
[![Sponsor](https://badgen.net/badge/Sponsor/GitHub/black?icon=github)](https://github.com/aetherial-online/aetherial-zero-footprint)

<a href="https://aetherial.online" target="_blank" rel="noopener noreferrer"><strong>Ætherial Ecosystem</strong></a>
&nbsp;&middot;&nbsp;
<a href="./docs/ARCHITECTURE.md"><strong>Architecture Docs</strong></a>
&nbsp;&middot;&nbsp;
<a href="./docs/BENCHMARKS.md"><strong>Benchmarks</strong></a>
&nbsp;&middot;&nbsp;
<a href="https://aetherial.online" target="_blank" rel="noopener noreferrer"><strong>Hercules DWH Cloud Waitlist</strong></a>

</div>

---

## Overview

**Hercules OLAP** is a standalone,  ultra-fast, cloud-native, single-binary columnar database engine written in 100% safe Rust. Designed for real-time telemetry, log ingestion, observability, time-series analytics, and high-concurrency OLAP workloads, combines lock-free **Apache Arrow** in-memory ingestion buffers, crash-safe Write-Ahead Logging (WAL), background **ZSTD-compressed Apache Parquet** compaction, and vectorized **Apache DataFusion** SQL execution—with **zero C++ dependencies**.

While legacy columnar databases rely on heavy C++ codebases, complex ZooKeeper/Keeper cluster coordination, and stateful local NVMe storage, Hercules is built from first principles for the modern object-storage era.

---

### Why Hercules OLAP? (Key Highlights)

* **100% Pure Rust & Zero C++ Dependencies:** Compiles into a single lightweight, memory-safe binary with no external runtime daemons, JVM overhead, or ZooKeeper coordination.
* **8x–12x Storage Compression & Zone-Map Pruning:** Automatic background compaction converts raw ingestion batches into sorted, `ZSTD`-compressed Parquet files with min/max zone-map statistics to skip irrelevant data blocks.
* **ClickHouse-Compatible HTTP Wire Protocol:** Drop-in familiarity—ingest `JSONEachRow` batches and execute ANSI SQL queries over standard HTTP (`POST /`) without rewriting existing client pipelines.
* **Zero-Ops Elastic Architecture:** Decouples compute from storage so developers can run locally in seconds or scale statelessly against cloud object stores.

---

### Core Capabilities (`v0.1.0` OSS)
1. **High-Throughput Streaming Ingestion:** Accepts batched JSON (`JSONEachRow`) and Apache Arrow streams over HTTP, appending atomically to a lock-free in-memory Arrow `MemTable` backed by a crash-safe Write-Ahead Log (WAL).
2. **Unified Hot + Cold Querying:** Queries executed via `DataFusion` seamlessly merge live uncompacted rows in RAM (`MemTable`) with historical `ZSTD`-compressed `.parquet` segments on disk or object storage—guaranteeing real-time read-after-write visibility.
3. **Full Analytical SQL Support:** Native vectorized execution (`AVX2`/`NEON` SIMD) for `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `HAVING`, window functions, `JOIN`s, approximate distinct counts (`HyperLogLog`), and time-bucketing (`date_trunc`).
4. **Pluggable Storage Backends (`object_store`):** Runs identically on local NVMe disks (single-binary developer mode) or directly against **AWS S3**, **Cloudflare R2**, and **Google Cloud Storage** (cloud-native mode).
5. **Dual-License IP Protection (`AGPLv3` + Commercial):** Free to run, audit, and benchmark under `AGPLv3`, while cloud providers or SaaS vendors offering managed Hercules services or proprietary closed-source forks require a **Hercules Commercial License** from Ætherial.

---

### Verified Single-Node Benchmarks (`10,000,000` Rows + Durable WAL `fsync`)

| Metric | Measured Result (`16` Concurrent Workers) |
| :--- | :--- |
| **Sustained JSON Ingestion Throughput** | **`701,931 rows/sec`** (`10M` rows in `14.25 s`) |
| **Storage Compression (`ZSTD` Parquet)** | **`16.40x`** (`1,153.75 MB` raw JSON → `70.33 MB` Parquet) |
| **Filtered SQL Aggregation (`10M` rows)** | **`99.13 ms`** (`WHERE status_code >= 500 GROUP BY service`) |
| **Full-Scan Group-By (`10M` rows)** | **`135.55 ms`** (`GROUP BY tenant_id ORDER BY reqs DESC LIMIT 10`) |

---

## Measured Ingestion & Storage Results (10M Rows)

| Benchmark Profile | Script | Ingestion Time | Sustained Throughput | Compacted Parquet (`ZSTD`) | Compression Ratio |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Single-Thread Sync** (`1` client) | `ingest_and_query.py` | `64.22 s` | **`155,726 rows/sec`** | `81.14 MB` | **`14.22x`** |
| **Concurrent (`8` workers)** | `ingest_and_query_workers.py` | `15.99 s` | **`625,383 rows/sec`** | `71.34 MB` | **`16.17x`** |
| **Concurrent (`16` workers)** | `ingest_and_query_workers.py` | **`14.25 s`** | **`701,931 rows/sec`** | **`70.33 MB`** | **`16.40x`** |

---

## Analytical SQL Query Latency (10,000,000 Rows)

| Query ID | Analytical Workload | SQL Statement | Measured Latency |
| :--- | :--- | :--- | :---: |
| **`Q1`** | Full-Scan Group-By (Top 10) | `SELECT tenant_id, COUNT(*) AS reqs, AVG(latency_ms) AS avg_lat FROM bench_events GROUP BY tenant_id ORDER BY reqs DESC LIMIT 10` | **`135.55 ms`** |
| **`Q2`** | Filtered Predicate Pushdown | `SELECT service, COUNT(*) AS errors FROM bench_events WHERE status_code >= 500 GROUP BY service ORDER BY errors DESC` | **`99.13 ms`** |
| **`Q3`** | Multi-Column Aggregation | `SELECT tenant_id, service, SUM(bytes_out) AS total_bytes FROM bench_events GROUP BY tenant_id, service LIMIT 10` | **`237.57 ms`** |

> See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for single-thread vs. multi-worker results and reproducible benchmark scripts.

---

## Quickstart & Installation

### Option 1: Build from Source (Rust 1.85+)

```bash
git clone https://github.com
cd hercules-olap
cargo run --release
```

The server starts on `http://127.0.0.1:8123` (compatible with standard ClickHouse HTTP clients).

### Option 2: Ingest & Query in 30 Seconds (`curl`)

```bash
# 1. Stream JSONEachRow events into Hercules
curl -X POST "http://127.0.0" \
  -H "Content-Type: application/x-ndjson" \
  -d '{"tenant_id":"acme","event":"api_call","latency_ms":1.42}
{"tenant_id":"acme","event":"api_call","latency_ms":0.98}'

# 2. Run a vectorized SQL aggregation across live MemTable + Parquet segments
curl -X POST "http://127.0.0" \
  -H "Content-Type: text/plain" \
  -d "SELECT tenant_id, COUNT(*) AS total, AVG(latency_ms) AS avg_ms FROM events GROUP BY tenant_id"
---

## Storage Configuration (Local NVMe, AWS S3, Cloudflare R2)

Hercules decouples compute from storage via a pluggable `object_store` abstraction. By default, compacted `.parquet` segments are written to `./data/parquet`. To connect an external S3-compatible bucket:

```bash
export HERCULES_STORAGE_BACKEND="s3" # local | s3 | r2
export HERCULES_BUCKET="my-hercules-telemetry"
export HERCULES_ENDPOINT="https://<account_id>.r2.cloudflarestorage.com"
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export HERCULES_COMPACT_INTERVAL_MS="2000"

cargo run --release
```

---

## Modular Codebase Architecture

```text
hercules-olap/src/
├── main.rs        # Runtime bootstrap, worker supervision & graceful shutdown
├── config.rs      # Storage backend & compaction configuration
├── error.rs       # Centralized error types & HTTP status mapping
├── schema.rs      # Dynamic Apache Arrow schema inference & validation
├── wal.rs         # Crash-safe append-only Write-Ahead Log (fsync)
├── memtable.rs    # Lock-free in-memory Arrow RecordBatch buffer
├── compactor.rs   # Async background ZSTD Parquet compactor
├── storage.rs     # Pluggable Local / S3 / R2 / GCS object store layer
├── engine.rs      # Vectorized Apache DataFusion SQL query federation
└── server.rs      # ClickHouse-compatible HTTP ingestion & query API
```

---

## Roadmap: From Hercules OLAP (OSS) to Hercules DWH (Cloud)

* **`v0.1.0` (Current):** Single-binary Rust engine, durable WAL, Arrow `MemTable`, background `ZSTD` Parquet compaction, `DataFusion` SQL federation, and local/S3/R2 storage backends.
* **`v0.3.0` (Next):** Multi-level LSM merge-tree compaction (`L0 -> L1 -> L2`), `ReplacingMergeTree` deduplication, TTL retention policies, and Gorilla/Delta-of-Delta time-series codecs.
* **`v0.6.0`:** Zero-copy **Apache Arrow Flight** gRPC transport and distributed stateless query workers.
* **`Hercules DWH` (Managed Cloud):** Fully managed, serverless scale-to-zero OLAP data warehouse with integrated storage, RBAC, and instant cold-to-hot elasticity—built on the Ætherial data infrastructure stack.

---

## Contributing, Governance & Security

We welcome bug reports, performance benchmarks, and pull requests from the community:
* **Report a Bug:** Open a structured issue via our [Bug Report Template](.github/ISSUE_TEMPLATE/bug_report.md).
* **Propose a Feature:** Suggest new capabilities via our [Feature Request Template](.github/ISSUE_TEMPLATE/feature_request.md).
* **Contributing & CLA:** Read [CONTRIBUTING.md](CONTRIBUTING.md) for local setup (`cargo fmt`, `cargo clippy`, `cargo test`) and Contributor License Agreement terms.
* **Community Standards:** All participants must adhere to our [Code of Conduct](CODE_OF_CONDUCT.md).
* **Security Disclosure:** Please report vulnerabilities privately following [SECURITY.md](SECURITY.md).

---

## Dual Licensing

Hercules OLAP is available under a **Dual-License** model:
1. **Open-Source Community License (`GNU AGPLv3`):** Free to use, inspect, modify, and self-host under the terms of the [GNU Affero General Public License v3.0](LICENSE).
2. **Commercial License (`Hercules Commercial License`):** Required if you offer Hercules OLAP (or a modified derivative) as a hosted/managed cloud service (SaaS/DBaaS), or embed it inside proprietary closed-source commercial software without disclosing source code under AGPLv3. See [COMMERCIAL_LICENSE.md](COMMERCIAL_LICENSE.md) or contact `info@aetherial.online`.

---

<div align="center">
  Built with 🦀 Rust by <a href="https://aetherial.online" target="_blank" rel="noopener noreferrer"><strong>Ætherial</strong></a>
</div>