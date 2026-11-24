---
name: Bug Report
about: Report a reproducible bug, crash, or query discrepancy in Hercules OLAP
title: "[BUG] "
labels: bug
assignees: ''
---

## Describe the Bug
A clear and concise description of what the bug is and what you expected to happen.

## Steps to Reproduce
1. Start Hercules OLAP with configuration: `...`
2. Ingest payload via `POST /insert`:
   ```json
   {"tenant_id": "test", "value": 42}
   ```
3. Execute SQL query via `POST /query`:
   ```sql
   SELECT * FROM events;
   ```
4. Observe error / unexpected output.

## Environment Details
* **Hercules OLAP Version / Commit Hash:** (e.g., `v0.1.0` or `git rev-parse --short HEAD`)
* **Rust Toolchain (`rustc --version`):**
* **Operating System & Architecture:** (e.g., `Ubuntu 24.04 x86_64`, `macOS ARM64`, `Windows 11 x64`)
* **Storage Backend:** (`local` NVMe / `s3` / `r2` / `gcs`)

## Logs & Stack Trace
If applicable, paste relevant terminal logs or panic backtraces here (please redact any S3/R2 secret keys).
