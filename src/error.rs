use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HerculesError {
    #[error("Invalid table name: {0}")]
    InvalidTable(String),

    #[error("Ingestion payload error: {0}")]
    Ingestion(String),

    #[error("SQL execution error: {0}")]
    Query(String),

    #[error("Storage I/O error: {0}")]
    Storage(String),

    #[error("Internal engine error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for HerculesError {
    fn into_response(self) -> Response {
        let status = match &self {
            HerculesError::InvalidTable(_) | HerculesError::Ingestion(_) => StatusCode::BAD_REQUEST,
            HerculesError::Query(_) => StatusCode::UNPROCESSABLE_ENTITY,
            HerculesError::Storage(_) | HerculesError::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, format!("HERCULES_ERROR: {self}\n")).into_response()
    }
}

pub type Result<T> = std::result::Result<T, HerculesError>;
