# Hercules OLAP — System Architecture & Internals

## 1. Design Philosophy: Decoupled, Stateless-First Columnar Analytics

**Hercules OLAP** is engineered from the ground up in 100% pure Rust to eliminate the operational complexity of legacy C++ analytical databases. Instead of relying on external cluster coordinators (such as ZooKeeper) or tightly coupled local disks, Hercules separates **low-latency memory ingestion** from **immutable columnar object storage**.

---

## 2. End-to-End Data & Query Pipeline

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                     HERCULES OLAP ENGINE (SINGLE BINARY)                   │
│                                                                            │
│  [HTTP Clients: JSONEachRow / SQL]                                         │
│         │                                                                  │
│         ├──(POST /insert?table=...)──► 1. [wal.rs] Append-Only WAL (fsync) │
│         │                              2. [schema.rs] Arrow Schema Infer   │
│         │                              3. [memtable.rs] Lock-Free Arrow RAM│
│         │                                       │                          │
│         │                              (Async Background Flush)            │
│         │                                       ▼                          │
│         │                              4. [compactor.rs] ZSTD Parquet      │
│         │                                       │                          │
│         │                                       ▼                          │
│         │                              5. [storage.rs] Local / S3 / R2     │
│         │                                                                  │
│         └──(POST /query)─────────────► 6. [engine.rs] DataFusion SQL       │
│                                           • Unifies Hot RAM + Cold Parquet │
│                                           • Zone-Map & Column Pruning      │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Core Module Responsibilities

### 1. Write-Ahead Log & Crash Recovery (`src/wal.rs`)
* Every incoming batch is appended sequentially to `{data_dir}/wal/{table}.wal` and flushed via `sync_data()` before acknowledging the client.
* Upon node restart, `recover_pending()` replays uncompacted WAL segments directly back into the active Arrow `MemTable`, guaranteeing **zero data loss**.

### 2. Lock-Free Columnar `MemTable` (`src/memtable.rs`)
* Incoming NDJSON payloads are parsed into 64-byte aligned **Apache Arrow `RecordBatch`** structures (`src/schema.rs`) and stored in a concurrent `DashMap` keyed by table name.
* Read queries clone lightweight `Arc`-backed array pointers (`snapshot_batches`) without blocking concurrent writers.

### 3. Background `ZSTD` Parquet Compactor (`src/compactor.rs`)
* A background Tokio worker periodically drains eligible `MemTable` partitions (triggered by `HERCULES_COMPACT_INTERVAL_MS` or `HERCULES_COMPACT_ROW_THRESHOLD`).
* Batches are merged and serialized into sorted **Apache Parquet** segments compressed with **ZSTD**, achieving **8x–12x storage compression** while embedding column `min/max` zone-map statistics per row group.
* Once committed to object storage, the corresponding WAL file is atomically truncated.

### 4. Pluggable Object Store Layer (`src/storage.rs`)
* Powered by `object_store`, Hercules reads and writes compacted `.parquet` segments identically across **local NVMe directories**, **AWS S3**, **Cloudflare R2**, and **Google Cloud Storage**.

### 5. Unified Hot + Cold SQL Federation (`src/engine.rs`)
* Powered by **Apache DataFusion**, every SQL query dynamically registers both historical `.parquet` files on storage (`__cold_{table}`) and active uncompacted Arrow batches in RAM (`__hot_{table}`) under a unified `UNION ALL` view—delivering **real-time read-after-write consistency** with vectorized `AVX2`/`NEON` execution.
