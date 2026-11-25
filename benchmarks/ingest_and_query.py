#!/usr/bin/env python3
"""
Hercules OLAP — Reproducible 10M-Row Ingestion & Analytical Query Benchmark
Uses Python standard library only (zero external dependencies).
"""

import argparse
import json
import os
import random
import time
import urllib.request

BASE_URL = "http://127.0.0.1:8123"
TABLE = "bench_events"

TENANTS = [f"tenant_{i:03d}" for i in range(100)]
SERVICES = [
    "auth-gateway", "edge-router", "billing-worker", "rag-indexer",
    "webhook-dispatch", "audit-logger", "session-vault", "stream-ingest",
    "policy-engine", "kv-cache", "graph-resolver", "export-service"
]
STATUS_CODES = [200, 200, 200, 200, 201, 204, 400, 401, 429, 500, 502]


def generate_batch(batch_size: int, base_ts: int) -> bytes:
    lines = []
    for i in range(batch_size):
        row = (
            f'{{"tenant_id":"{random.choice(TENANTS)}",'
            f'"service":"{random.choice(SERVICES)}",'
            f'"status_code":{random.choice(STATUS_CODES)},'
            f'"latency_ms":{round(random.uniform(0.2, 45.0), 3)},'
            f'"bytes_out":{random.randint(128, 65536)},'
            f'"ts":{base_ts + i}}}'
        )
        lines.append(row)
    return ("\n".join(lines) + "\n").encode("utf-8")


def post_insert(payload: bytes) -> dict:
    req = urllib.request.Request(
        f"{BASE_URL}/insert?table={TABLE}",
        data=payload,
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def post_query(sql: str) -> tuple[list, float]:
    t0 = time.perf_counter()
    req = urllib.request.Request(
        f"{BASE_URL}/query",
        data=sql.encode("utf-8"),
        headers={"Content-Type": "text/plain"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return data, elapsed_ms


def dir_size_mb(path: str) -> float:
    total = 0
    if not os.path.exists(path):
        return 0.0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total / (1024 * 1024)


def main():
    parser = argparse.ArgumentParser(description="Hercules OLAP Benchmark")
    parser.add_argument("--rows", type=int, default=10_000_000, help="Total rows to ingest")
    parser.add_argument("--batch-size", type=int, default=50_000, help="Rows per HTTP batch")
    args = parser.parse_args()

    total_batches = args.rows // args.batch_size
    print(f"=== HERCULES OLAP BENCHMARK ({args.rows:,} rows | {total_batches} batches) ===")

    raw_bytes_total = 0
    t_ingest_start = time.perf_counter()

    for b in range(total_batches):
        payload = generate_batch(args.batch_size, b * args.batch_size)
        raw_bytes_total += len(payload)
        post_insert(payload)
        if (b + 1) % 20 == 0 or (b + 1) == total_batches:
            done = (b + 1) * args.batch_size
            elapsed = time.perf_counter() - t_ingest_start
            print(f"  Ingested {done:>10,} / {args.rows:,} rows ({done / elapsed:,.0f} rows/sec)")

    ingest_elapsed = time.perf_counter() - t_ingest_start
    print("\nWaiting 3.0s for background ZSTD Parquet compaction...")
    time.sleep(3.0)

    raw_mb = raw_bytes_total / (1024 * 1024)
    parquet_mb = dir_size_mb(f"./data/parquet/{TABLE}")
    ratio = (raw_mb / parquet_mb) if parquet_mb > 0 else 0.0

    print(f"\n--- STORAGE & INGESTION SUMMARY ---")
    print(f"  Total Rows:        {args.rows:,}")
    print(f"  Ingestion Time:    {ingest_elapsed:.2f} s ({args.rows / ingest_elapsed:,.0f} rows/sec)")
    print(f"  Raw JSON Size:     {raw_mb:.2f} MB")
    print(f"  Parquet ZSTD Size: {parquet_mb:.2f} MB (Compression Ratio: {ratio:.2f}x)")

    queries = {
        "Q1 (10M Group-By Top 10 Tenants)": (
            f"SELECT tenant_id, COUNT(*) AS reqs, AVG(latency_ms) AS avg_lat "
            f"FROM {TABLE} GROUP BY tenant_id ORDER BY reqs DESC LIMIT 10"
        ),
        "Q2 (Filtered HTTP >= 500 Pushdown)": (
            f"SELECT service, COUNT(*) AS errors FROM {TABLE} "
            f"WHERE status_code >= 500 GROUP BY service ORDER BY errors DESC"
        ),
        "Q3 (Multi-Column Aggregation)": (
            f"SELECT tenant_id, service, SUM(bytes_out) AS total_bytes "
            f"FROM {TABLE} GROUP BY tenant_id, service LIMIT 10"
        ),
    }

    print(f"\n--- ANALYTICAL SQL QUERY LATENCY ---")
    for name, sql in queries.items():
        # Warm-up + measured run
        _, _ = post_query(sql)
        rows, ms = post_query(sql)
        print(f"  {name}: {ms:.2f} ms (returned {len(rows)} rows)")


if __name__ == "__main__":
    main()
