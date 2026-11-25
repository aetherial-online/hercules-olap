use crate::config::HerculesConfig;
use crate::error::{HerculesError, Result};
use crate::memtable::ColumnarMemTable;
use crate::storage::StorageLayer;
use crate::wal::WriteAheadLog;
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::{error, info};
use uuid::Uuid;

pub struct BackgroundCompactor {
    cfg: HerculesConfig,
    memtable: ColumnarMemTable,
    wal: Arc<WriteAheadLog>,
    storage: StorageLayer,
}

impl BackgroundCompactor {
    pub fn new(
        cfg: HerculesConfig,
        memtable: ColumnarMemTable,
        wal: Arc<WriteAheadLog>,
        storage: StorageLayer,
    ) -> Self {
        Self {
            cfg,
            memtable,
            wal,
            storage,
        }
    }

    /// Loop asincrono in background che compatta la MemTable in file Parquet ZSTD
    pub async fn run_loop(self) {
        let interval = Duration::from_millis(self.cfg.compact_interval_ms);
        loop {
            sleep(interval).await;
            if let Err(e) = self.flush_eligible_tables(false).await {
                error!("Background compaction error: {e}");
            }
        }
    }

    pub async fn flush_eligible_tables(&self, force: bool) -> Result<usize> {
        let mut flushed_tables = 0;
        for (table, pending_rows) in self.memtable.active_tables() {
            if force || pending_rows >= self.cfg.compact_row_threshold || pending_rows > 0 {
                let batches = self.memtable.drain_table(&table);
                if batches.is_empty() {
                    continue;
                }

                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                let parquet_bytes = encode_batches_to_zstd_parquet(&batches)?;
                let filename = format!(
                    "part_{}_{}.parquet",
                    Utc::now().format("%Y%m%dT%H%M%S%3f"),
                    Uuid::new_v4().simple()
                );

                let key = self
                    .storage
                    .put_parquet(&table, &filename, parquet_bytes)
                    .await?;
                self.wal.truncate(&table).await?;
                flushed_tables += 1;

                info!(
                    table = %table,
                    rows = total_rows,
                    segment = %key,
                    "Compacted MemTable into ZSTD Parquet segment"
                );
            }
        }
        Ok(flushed_tables)
    }
}

/// Serializza un vettore di RecordBatch in un buffer Parquet compresso ZSTD
pub fn encode_batches_to_zstd_parquet(batches: &[RecordBatch]) -> Result<Bytes> {
    let schema = batches[0].schema();
    let merged = if batches.len() == 1 {
        batches[0].clone()
    } else {
        concat_batches(&schema, batches)
            .map_err(|e| HerculesError::Storage(format!("Concat batches failed: {e}")))?
    };

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_write_batch_size(65_536)
        .set_max_row_group_size(1_048_576)
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))
        .map_err(|e| HerculesError::Storage(format!("Parquet writer init error: {e}")))?;

    writer
        .write(&merged)
        .map_err(|e| HerculesError::Storage(format!("Parquet write error: {e}")))?;
    writer
        .close()
        .map_err(|e| HerculesError::Storage(format!("Parquet close error: {e}")))?;

    Ok(Bytes::from(buffer))
}
