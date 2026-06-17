use thiserror::Error;

#[derive(Debug, Error)]
pub enum MapperError {
    #[error("not a FHIR Bundle: {0}")]
    NotBundle(String),
    #[error("invalid manifest: {0}")]
    InvalidManifest(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type MapperResult<T> = Result<T, MapperError>;
