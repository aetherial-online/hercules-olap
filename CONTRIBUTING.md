# Contributing to Hercules OLAP

Thank you for your interest in contributing to **Hercules OLAP**! Every bug report, benchmark comparison, documentation improvement, and pull request helps make pure-Rust analytical infrastructure faster and more reliable.

Please take a moment to review our [Code of Conduct](CODE_OF_CONDUCT.md) before participating.

---

## 1. Ways to Contribute
* **Report Bugs & Edge Cases:** Open a structured report using our [Bug Report Template](.github/ISSUE_TEMPLATE/bug_report.md).
* **Propose Features & Codecs:** Suggest new SQL operators, storage connectors, or compression codecs via our [Feature Request Template](.github/ISSUE_TEMPLATE/feature_request.md).
* **Submit Benchmarks:** Share reproducible ingestion and query latency benchmarks across different hardware and object-storage setups.
* **Submit Pull Requests:** Improve core Rust modules (`wal.rs`, `memtable.rs`, `compactor.rs`, `engine.rs`, `storage.rs`), add unit/integration tests, or refine documentation.

---

## 2. Local Development Setup

### Prerequisites
* **Rust Toolchain:** Stable Rust `1.85+` (Edition `2024`) — install via [rustup.rs](https://rustup.rs).
* **Git:** Standard Git CLI.

### Build & Run Quality Checks
Before opening a Pull Request, ensure your changes pass formatting, linting, and the full test suite:

```bash
# 1. Format code according to standard rustfmt rules
cargo fmt --all -- --check

# 2. Run Clippy static analysis with zero warnings
cargo clippy --all-targets --all-features -- -D warnings

# 3. Execute all unit and integration tests
cargo test --all --verbose
```

---

## 3. Pull Request Guidelines
1. **Keep Modules Atomic:** Follow Hercules OLAP's modular architecture (`src/wal.rs`, `src/memtable.rs`, `src/compactor.rs`, `src/storage.rs`, `src/engine.rs`, `src/server.rs`). Avoid introducing monolithic files or blocking calls inside async Tokio tasks.
2. **Zero Unsafe Without Justification:** Hercules prioritizes 100% memory-safe Rust. Any `unsafe` block requires explicit safety invariants documented in comments and will undergo strict maintainer review.
3. **Include Tests:** Bug fixes must include a regression test; new features must include unit or integration tests.
4. **Clear Commit Messages:** Use conventional prefixes (`feat:`, `fix:`, `perf:`, `docs:`, `refactor:`, `test:`).

---

## 4. Contributor License Agreement (CLA)
Because Hercules OLAP is dual-licensed under `GNU AGPLv3` and the [Hercules Commercial License](COMMERCIAL_LICENSE.md), submitting a Pull Request constitutes your agreement that:
* Your contribution is your original work and you have the legal right to submit it.
* You grant **Ætherial** a perpetual, worldwide, non-exclusive, royalty-free, irrevocable license to use, modify, sublicense, and distribute your contribution under both `GNU AGPLv3` and the `Hercules Commercial License`.
