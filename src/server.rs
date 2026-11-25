use crate::engine::QueryEngine;
use crate::error::Result;
use crate::memtable::ColumnarMemTable;
use crate::schema::{ndjson_to_record_batch, validate_table_name};
use crate::wal::WriteAheadLog;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct AppState {
    pub memtable: ColumnarMemTable,
    pub wal: Arc<WriteAheadLog>,
    pub engine: QueryEngine,
}

#[derive(Deserialize)]
pub struct InsertParams {
    pub table: String,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/insert", post(handle_insert))
        .route("/query", post(handle_query))
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .with_state(state)
}

async fn health_check() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "engine": "hercules-olap",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

async fn handle_insert(
    State(state): State<AppState>,
    Query(params): Query<InsertParams>,
    body: Bytes,
) -> Result<Json<Value>> {
    let start = Instant::now();
    let table = validate_table_name(&params.table)?;

    // 1. Scrittura durevole sul WAL (fsync)
    state.wal.append(&table, &body).await?;

    // 2. Conversione NDJSON -> Apache Arrow RecordBatch colonnare
    let existing_schema = state.memtable.table_schema(&table);
    let batch = ndjson_to_record_batch(&body, existing_schema)?;
    let ingested_rows = batch.num_rows();

    // 3. Inserimento istantaneo nella MemTable in RAM
    let buffered_rows = state.memtable.insert(&table, batch);

    Ok(Json(json!({
        "status": "inserted",
        "table": table,
        "rows_ingested": ingested_rows,
        "memtable_buffered_rows": buffered_rows,
        "elapsed_us": start.elapsed().as_micros()
    })))
}

async fn handle_query(State(state): State<AppState>, body: String) -> Result<String> {
    let sql = body.trim();
    state.engine.execute_sql(sql).await
}
