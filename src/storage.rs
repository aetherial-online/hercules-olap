use crate::config::{HerculesConfig, StorageBackendType};
use crate::error::{HerculesError, Result};
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use std::sync::Arc;
use tokio::fs;
use url::Url;

#[derive(Clone)]
pub struct StorageLayer {
    pub store: Arc<dyn ObjectStore>,
    pub base_url: Url,
}

impl StorageLayer {
    pub async fn new(cfg: &HerculesConfig) -> Result<Self> {
        match cfg.storage_backend {
            StorageBackendType::Local => {
                let parquet_dir = cfg.data_dir.join("parquet");
                fs::create_dir_all(&parquet_dir).await.map_err(|e| {
                    HerculesError::Storage(format!("Failed to create local parquet dir: {e}"))
                })?;

                let abs_dir = std::fs::canonicalize(&parquet_dir).map_err(|e| {
                    HerculesError::Storage(format!("Failed to canonicalize parquet dir: {e}"))
                })?;

                let local = LocalFileSystem::new_with_prefix(&abs_dir).map_err(|e| {
                    HerculesError::Storage(format!("LocalFileSystem init error: {e}"))
                })?;

                let base_url = Url::from_directory_path(&abs_dir).map_err(|_| {
                    HerculesError::Storage("Invalid local directory URL".to_string())
                })?;

                Ok(Self {
                    store: Arc::new(local),
                    base_url,
                })
            }
            StorageBackendType::S3 => {
                let bucket = cfg.s3_bucket.clone().ok_or_else(|| {
                    HerculesError::Storage(
                        "HERCULES_BUCKET must be set when HERCULES_STORAGE_BACKEND=s3".to_string(),
                    )
                })?;

                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(&bucket)
                    .with_region(&cfg.s3_region);

                if let Some(ep) = &cfg.s3_endpoint {
                    builder = builder.with_endpoint(ep).with_allow_http(true);
                }

                let s3 = builder.build().map_err(|e| {
                    HerculesError::Storage(format!("S3/R2 client build error: {e}"))
                })?;

                let base_url = Url::parse(&format!("s3://{bucket}/"))
                    .map_err(|e| HerculesError::Storage(format!("Invalid S3 URL: {e}")))?;

                Ok(Self {
                    store: Arc::new(s3),
                    base_url,
                })
            }
        }
    }

    /// Scrive un segmento Parquet compattato sullo storage attivo (Local / S3 / R2)
    pub async fn put_parquet(&self, table: &str, filename: &str, data: Bytes) -> Result<String> {
        let key = format!("{table}/{filename}");
        let obj_path = ObjPath::from(key.as_str());
        self.store
            .put(&obj_path, PutPayload::from_bytes(data))
            .await
            .map_err(|e| HerculesError::Storage(format!("ObjectStore put error: {e}")))?;
        Ok(key)
    }
}
