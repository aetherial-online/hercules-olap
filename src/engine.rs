use crate::error::{HerculesError, Result};
use crate::memtable::ColumnarMemTable;
use crate::storage::StorageLayer;
use arrow::json::ArrayWriter;
use datafusion::datasource::MemTable as DataFusionMemTable;
use datafusion::prelude::*;
use futures::TryStreamExt;
use object_store::path::Path as ObjPath;
use std::sync::Arc;

#[derive(Clone)]
pub struct QueryEngine {
    memtable: ColumnarMemTable,
    storage: StorageLayer,
}

impl QueryEngine {
    pub fn new(memtable: ColumnarMemTable, storage: StorageLayer) -> Self {
        Self { memtable, storage }
    }

    /// Esegue una query ANSI SQL fondendo in tempo reale MemTable (Hot RAM) + Parquet (Cold Storage)
    pub async fn execute_sql(&self, sql: &str) -> Result<String> {
        let ctx = SessionContext::new();
        ctx.register_object_store(&self.storage.base_url, self.storage.store.clone());

        // Scopre tutte le tabelle presenti in RAM o nei segmenti Parquet sullo storage
        let mut table_names = std::collections::BTreeSet::new();
        for (t, _) in self.memtable.active_tables() {
            table_names.insert(t);
        }

        let mut list_stream = self.storage.store.list(None);
        while let Some(meta) = list_stream
            .try_next()
            .await
            .map_err(|e| HerculesError::Storage(format!("ObjectStore list error: {e}")))?
        {
            if let Some((table, _)) = meta.location.as_ref().split_once('/')
                && !table.is_empty()
            {
                table_names.insert(table.to_string());
            }
        }

        // Registra ogni tabella unificando segmenti Parquet storici + batch live in RAM
        for table in table_names {
            let live_batches = self.memtable.snapshot_batches(&table);
            let parquet_prefix = format!("{}{}/", self.storage.base_url, table);
            let has_parquet = self.table_has_parquet(&table).await?;

            match (has_parquet, live_batches.is_empty()) {
                (true, true) => {
                    ctx.register_parquet(&table, &parquet_prefix, ParquetReadOptions::default())
                        .await
                        .map_err(|e| {
                            HerculesError::Query(format!("Register parquet error: {e}"))
                        })?;
                }
                (false, false) => {
                    let schema = live_batches[0].schema();
                    let mem_provider = DataFusionMemTable::try_new(schema, vec![live_batches])
                        .map_err(|e| HerculesError::Query(format!("MemTable error: {e}")))?;
                    ctx.register_table(&table, Arc::new(mem_provider))
                        .map_err(|e| HerculesError::Query(format!("Register mem error: {e}")))?;
                }
                (true, false) => {
                    let cold_name = format!("__cold_{table}");
                    let hot_name = format!("__hot_{table}");
                    ctx.register_parquet(
                        &cold_name,
                        &parquet_prefix,
                        ParquetReadOptions::default(),
                    )
                    .await
                    .map_err(|e| HerculesError::Query(format!("Register cold error: {e}")))?;

                    let schema = live_batches[0].schema();
                    let mem_provider = DataFusionMemTable::try_new(schema, vec![live_batches])
                        .map_err(|e| HerculesError::Query(format!("MemTable error: {e}")))?;
                    ctx.register_table(&hot_name, Arc::new(mem_provider))
                        .map_err(|e| HerculesError::Query(format!("Register hot error: {e}")))?;

                    let union_sql = format!(
                        "CREATE VIEW {table} AS SELECT * FROM {cold_name} UNION ALL SELECT * FROM {hot_name}"
                    );
                    ctx.sql(&union_sql)
                        .await
                        .map_err(|e| HerculesError::Query(format!("Union view error: {e}")))?
                        .collect()
                        .await
                        .map_err(|e| HerculesError::Query(format!("Union exec error: {e}")))?;
                }
                (false, true) => {}
            }
        }

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| HerculesError::Query(format!("SQL parse/plan error: {e}")))?;
        let results = df
            .collect()
            .await
            .map_err(|e| HerculesError::Query(format!("SQL execution error: {e}")))?;

        if results.is_empty() {
            return Ok("[]\n".to_string());
        }

        let mut buf = Vec::new();
        let mut writer = ArrayWriter::new(&mut buf);
        let refs: Vec<&_> = results.iter().collect();
        writer
            .write_batches(&refs)
            .map_err(|e| HerculesError::Query(format!("JSON serialization error: {e}")))?;
        writer
            .finish()
            .map_err(|e| HerculesError::Query(format!("JSON finish error: {e}")))?;

        String::from_utf8(buf)
            .map_err(|e| HerculesError::Query(format!("UTF-8 conversion error: {e}")))
    }

    async fn table_has_parquet(&self, table: &str) -> Result<bool> {
        let prefix = ObjPath::from(table);
        let mut stream = self.storage.store.list(Some(&prefix));
        while let Some(meta) = stream
            .try_next()
            .await
            .map_err(|e| HerculesError::Storage(format!("List prefix error: {e}")))?
        {
            if meta.location.as_ref().ends_with(".parquet") {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
