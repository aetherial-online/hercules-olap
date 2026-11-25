use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackendType {
    Local,
    S3,
}

#[derive(Debug, Clone)]
pub struct HerculesConfig {
    pub bind_addr: String,
    pub data_dir: PathBuf,
    pub storage_backend: StorageBackendType,
    pub s3_bucket: Option<String>,
    pub s3_endpoint: Option<String>,
    pub s3_region: String,
    pub compact_interval_ms: u64,
    pub compact_row_threshold: usize,
}

impl HerculesConfig {
    pub fn from_env() -> Self {
        let backend = match env::var("HERCULES_STORAGE_BACKEND")
            .unwrap_or_else(|_| "local".to_string())
            .to_lowercase()
            .as_str()
        {
            "s3" | "r2" => StorageBackendType::S3,
            _ => StorageBackendType::Local,
        };

        Self {
            bind_addr: env::var("HERCULES_BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8123".to_string()),
            data_dir: PathBuf::from(
                env::var("HERCULES_DATA_DIR").unwrap_or_else(|_| "./data".to_string()),
            ),
            storage_backend: backend,
            s3_bucket: env::var("HERCULES_BUCKET").ok(),
            s3_endpoint: env::var("HERCULES_ENDPOINT").ok(),
            s3_region: env::var("HERCULES_REGION").unwrap_or_else(|_| "auto".to_string()),
            compact_interval_ms: env::var("HERCULES_COMPACT_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2000),
            compact_row_threshold: env::var("HERCULES_COMPACT_ROW_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),
        }
    }
}
