use crate::error::{HerculesError, Result};
use arrow::datatypes::SchemaRef;
use arrow::json::ReaderBuilder;
use arrow::json::reader::infer_json_schema;
use arrow::record_batch::RecordBatch;
use std::io::{BufReader, Cursor};
use std::sync::Arc;

/// Valida il nome della tabella per evitare path traversal
pub fn validate_table_name(table: &str) -> Result<String> {
    let trimmed = table.trim();
    if trimmed.is_empty()
        || !trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(HerculesError::InvalidTable(trimmed.to_string()));
    }
    Ok(trimmed.to_lowercase())
}

/// Converte un batch NDJSON (`JSONEachRow`) in un `RecordBatch` colonnare Apache Arrow
pub fn ndjson_to_record_batch(
    ndjson_bytes: &[u8],
    existing_schema: Option<SchemaRef>,
) -> Result<RecordBatch> {
    let mut cursor = BufReader::new(Cursor::new(ndjson_bytes));
    let schema = match existing_schema {
        Some(s) => s,
        None => {
            let (inferred, _) = infer_json_schema(&mut cursor, Some(256))
                .map_err(|e| HerculesError::Ingestion(format!("Schema inference failed: {e}")))?;
            Arc::new(inferred)
        }
    };

    let read_cursor = BufReader::new(Cursor::new(ndjson_bytes));
    let mut reader = ReaderBuilder::new(schema)
        .with_batch_size(65_536)
        .build(read_cursor)
        .map_err(|e| HerculesError::Ingestion(format!("Arrow JSON reader build failed: {e}")))?;

    let mut batches = Vec::new();
    for batch_res in &mut reader {
        let batch = batch_res
            .map_err(|e| HerculesError::Ingestion(format!("JSON batch parse error: {e}")))?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    if batches.is_empty() {
        return Err(HerculesError::Ingestion(
            "Empty or invalid NDJSON payload".to_string(),
        ));
    }

    if batches.len() == 1 {
        Ok(batches.remove(0))
    } else {
        let target_schema = batches[0].schema();
        arrow::compute::concat_batches(&target_schema, &batches)
            .map_err(|e| HerculesError::Ingestion(format!("Batch concat error: {e}")))
    }
}
