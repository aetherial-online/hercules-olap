use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Default)]
struct TableBuffer {
    batches: Vec<RecordBatch>,
    total_rows: usize,
}

/// MemTable colonnare concorrente in RAM per ingestione ad altissimo throughput
#[derive(Clone, Default)]
pub struct ColumnarMemTable {
    tables: Arc<DashMap<String, TableBuffer>>,
}

impl ColumnarMemTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Restituisce lo schema attivo della tabella se già presente in RAM
    pub fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        self.tables
            .get(table)
            .and_then(|buf| buf.batches.first().map(|b| b.schema()))
    }

    /// Inserisce un RecordBatch nella MemTable della tabella
    pub fn insert(&self, table: &str, batch: RecordBatch) -> usize {
        let rows = batch.num_rows();
        let mut entry = self.tables.entry(table.to_string()).or_default();
        entry.total_rows += rows;
        entry.batches.push(batch);
        entry.total_rows
    }

    /// Clona i batch correnti per le query SQL live (senza bloccare le scritture)
    pub fn snapshot_batches(&self, table: &str) -> Vec<RecordBatch> {
        self.tables
            .get(table)
            .map(|buf| buf.batches.clone())
            .unwrap_or_default()
    }

    /// Estrae e svuota i batch di una tabella per la compaction su Parquet
    pub fn drain_table(&self, table: &str) -> Vec<RecordBatch> {
        if let Some(mut entry) = self.tables.get_mut(table) {
            entry.total_rows = 0;
            std::mem::take(&mut entry.batches)
        } else {
            Vec::new()
        }
    }

    /// Elenco delle tabelle che hanno righe pronte per la compaction
    pub fn active_tables(&self) -> Vec<(String, usize)> {
        self.tables
            .iter()
            .filter(|e| e.value().total_rows > 0)
            .map(|e| (e.key().clone(), e.value().total_rows))
            .collect()
    }
}
