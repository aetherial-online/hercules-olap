mod compactor;
mod config;
mod engine;
mod error;
mod memtable;
mod schema;
mod server;
mod storage;
mod wal;

use crate::compactor::BackgroundCompactor;
use crate::config::HerculesConfig;
use crate::engine::QueryEngine;
use crate::memtable::ColumnarMemTable;
use crate::schema::ndjson_to_record_batch;
use crate::server::{AppState, build_router};
use crate::storage::StorageLayer;
use crate::wal::WriteAheadLog;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cfg = HerculesConfig::from_env();
    let wal = Arc::new(WriteAheadLog::new(&cfg.data_dir).await?);
    let memtable = ColumnarMemTable::new();
    let storage = StorageLayer::new(&cfg).await?;

    // Crash Recovery: ricarica in MemTable eventuali record nel WAL non ancora compattati
    for (table, pending_bytes) in wal.recover_pending().await? {
        if let Ok(batch) = ndjson_to_record_batch(&pending_bytes, None) {
            let rows = memtable.insert(&table, batch);
            info!(table = %table, recovered_rows = rows, "Recovered pending WAL records");
        }
    }

    // Avvia il worker asincrono di compaction Parquet ZSTD
    let compactor =
        BackgroundCompactor::new(cfg.clone(), memtable.clone(), wal.clone(), storage.clone());
    tokio::spawn(async move {
        compactor.run_loop().await;
    });

    let engine = QueryEngine::new(memtable.clone(), storage);
    let state = AppState {
        memtable,
        wal,
        engine,
    };

    let app = build_router(state);
    let listener = TcpListener::bind(&cfg.bind_addr).await?;
    info!(
        addr = %cfg.bind_addr,
        backend = ?cfg.storage_backend,
        "Hercules OLAP engine listening"
    );

    axum::serve(listener, app).await?;
    Ok(())
}
