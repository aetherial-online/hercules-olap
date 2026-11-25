use crate::error::{HerculesError, Result};
use std::path::{Path, PathBuf};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

pub struct WriteAheadLog {
    wal_dir: PathBuf,
}

impl WriteAheadLog {
    pub async fn new(base_dir: &Path) -> Result<Self> {
        let wal_dir = base_dir.join("wal");
        fs::create_dir_all(&wal_dir)
            .await
            .map_err(|e| HerculesError::Storage(format!("Failed to create WAL dir: {e}")))?;
        Ok(Self { wal_dir })
    }

    fn table_wal_path(&self, table: &str) -> PathBuf {
        self.wal_dir.join(format!("{table}.wal"))
    }

    /// Appende il batch NDJSON al WAL locale e forza la sincronizzazione su disco
    pub async fn append(&self, table: &str, payload: &[u8]) -> Result<()> {
        let path = self.table_wal_path(table);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| HerculesError::Storage(format!("WAL open error: {e}")))?;

        file.write_all(payload)
            .await
            .map_err(|e| HerculesError::Storage(format!("WAL write error: {e}")))?;
        if !payload.ends_with(b"\n") {
            file.write_all(b"\n")
                .await
                .map_err(|e| HerculesError::Storage(format!("WAL newline error: {e}")))?;
        }
        file.sync_data()
            .await
            .map_err(|e| HerculesError::Storage(format!("WAL sync_data error: {e}")))?;

        Ok(())
    }

    /// Tronca il WAL di una tabella dopo che i batch sono stati compattati in Parquet
    pub async fn truncate(&self, table: &str) -> Result<()> {
        let path = self.table_wal_path(table);
        if path.exists() {
            fs::write(&path, b"")
                .await
                .map_err(|e| HerculesError::Storage(format!("WAL truncate error: {e}")))?;
        }
        Ok(())
    }

    /// Legge eventuali record non compattati al riavvio (Crash Recovery)
    pub async fn recover_pending(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let mut recovered = Vec::new();
        let mut entries = fs::read_dir(&self.wal_dir)
            .await
            .map_err(|e| HerculesError::Storage(format!("WAL read_dir error: {e}")))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| HerculesError::Storage(format!("WAL entry error: {e}")))?
        {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("wal") {
                let bytes = fs::read(&path)
                    .await
                    .map_err(|e| HerculesError::Storage(format!("WAL read error: {e}")))?;
                if !bytes.is_empty()
                    && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                {
                    recovered.push((stem.to_string(), bytes));
                }
            }
        }
        Ok(recovered)
    }
}
