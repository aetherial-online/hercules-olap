<div align="center">

# HERCULES OLAP
### **High-Throughput, Pure-Rust Columnar Analytics & Real-Time Telemetry Engine**

[![CI Verification](https://github.com)](https://github.com)
[![License: AGPL v3](https://shields.io)](LICENSE)
[![Commercial License](https://shields.io)](COMMERCIAL_LICENSE.md)
[![Rust](https://shields.io)](https://rust-lang.org)

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

**Hercules OLAP** is a standalone, single-binary columnar database engine written in 100% safe Rust. Designed for real-time telemetry, log ingestion, observability, and high-concurrency analytical queries, Hercules combines lock-free **Apache Arrow** in-memory ingestion buffers, crash-safe Write-Ahead Logging (WAL), background **ZSTD-compressed Apache Parquet** compaction, and vectorized **Apache DataFusion** SQL execution—with **zero C++ dependencies**.

### Key Features
* **100% Pure Rust & Memory-Safe:** Compiles into a single lightweight binary with zero external runtime daemons or ZooKeeper coordination.
* **Real-Time Read-After-Write:** Streaming inserts land in a lock-free columnar `MemTable` and are immediately queryable alongside compacted `.parquet` segments.
* **Pluggable Storage Backends:** Run locally on NVMe disks or attach your own **AWS S3**, **Cloudflare R2**, or **Google Cloud Storage** buckets.
* **8x–12x Storage Compression:** Automatic background compaction converts raw ingestion batches into sorted, `ZSTD`-compressed Parquet files with min/max zone-map pruning.
* **ClickHouse-Compatible HTTP API:** Ingest `JSONEachRow` batches and execute ANSI SQL queries over standard HTTP (`POST /`).

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
```

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
├── main.rs        # Runtime bootstrap & background worker supervision
├── config.rs      # Storage & compaction configuration
├── schema.rs      # Dynamic Apache Arrow schema inference
├── wal.rs         # Crash-safe append-only Write-Ahead Log
├── memtable.rs    # Lock-free in-memory Arrow RecordBatch buffer
├── compactor.rs   # Async ZSTD Parquet background compactor
├── storage.rs     # Pluggable Local / S3 / R2 object store layer
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

## Contributing & Community

We welcome bug reports, performance benchmarks, and pull requests!
* **Report a Bug or Request a Feature:** Use our [GitHub Issue Templates](../../issues/new/choose).
* **Contributing Guidelines:** Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a PR.
* **Security Vulnerabilities:** Please review [SECURITY.md](SECURITY.md) for responsible disclosure.

---

## Dual Licensing

Hercules OLAP is available under a **Dual-License** model:
1. **Open-Source Community License (`GNU AGPLv3`):** Free to use, inspect, modify, and self-host under the terms of the [GNU Affero General Public License v3.0](LICENSE).
2. **Commercial License (`Hercules Commercial License`):** Required if you offer Hercules OLAP (or a modified derivative) as a hosted/managed cloud service (SaaS/DBaaS), or embed it inside proprietary closed-source commercial software without disclosing source code under AGPLv3. See [COMMERCIAL_LICENSE.md](COMMERCIAL_LICENSE.md) or contact `info@aetherial.online`.

---

<div align="center">
  Built with 🦀 Rust by <a href="https://aetherial.online" target="_blank" rel="noopener noreferrer"><strong>Ætherial</strong></a>
</div>