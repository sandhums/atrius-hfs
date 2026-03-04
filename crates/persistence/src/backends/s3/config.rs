//! Configuration types for the S3 backend.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::{BackendError, StorageError, StorageResult};

/// Tenant-to-bucket resolution for S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum S3TenancyMode {
    /// All tenants share one bucket with tenant-specific key prefixes.
    PrefixPerTenant {
        /// Shared bucket name.
        bucket: String,
    },

    /// Each tenant maps to a specific bucket.
    ///
    /// The system tenant can use `default_system_bucket`.
    BucketPerTenant {
        /// Explicit tenant -> bucket map.
        tenant_bucket_map: HashMap<String, String>,
        /// Optional fallback bucket for `__system__` tenant.
        default_system_bucket: Option<String>,
    },
}

/// Configuration for the AWS S3 backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3BackendConfig {
    /// How tenant data is mapped to buckets/prefixes.
    pub tenancy_mode: S3TenancyMode,

    /// Optional global key prefix applied before backend keys.
    pub prefix: Option<String>,

    /// AWS region override (falls back to provider chain if unset).
    pub region: Option<String>,

    /// Optional S3-compatible endpoint URL (for example, MinIO).
    ///
    /// When unset, the backend uses normal AWS S3 endpoint resolution.
    pub endpoint_url: Option<String>,

    /// Force path-style bucket addressing.
    ///
    /// In S3-compatible endpoint mode this may be defaulted at runtime.
    /// In AWS mode (`endpoint_url == None`), defaults preserve current behavior.
    #[serde(default)]
    pub force_path_style: bool,

    /// Allow insecure HTTP endpoint URLs.
    ///
    /// This only matters when `endpoint_url` is set. AWS mode is unaffected.
    #[serde(default)]
    pub allow_http: bool,

    /// Validate all configured buckets on startup with `HeadBucket`.
    pub validate_buckets_on_startup: bool,

    /// Max NDJSON lines per export output part.
    pub bulk_export_part_size: u32,

    /// Default ingestion batch size for bulk submit processing.
    pub bulk_submit_batch_size: u32,
}

impl Default for S3BackendConfig {
    fn default() -> Self {
        Self {
            tenancy_mode: S3TenancyMode::PrefixPerTenant {
                bucket: "hfs".to_string(),
            },
            prefix: None,
            region: None,
            endpoint_url: None,
            force_path_style: false,
            allow_http: false,
            validate_buckets_on_startup: true,
            bulk_export_part_size: 10_000,
            bulk_submit_batch_size: 100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> S3BackendConfig {
        S3BackendConfig {
            tenancy_mode: S3TenancyMode::PrefixPerTenant {
                bucket: "test-bucket".to_string(),
            },
            ..Default::default()
        }
    }

    #[test]
    fn validate_accepts_https_endpoint_without_allow_http() {
        let mut config = base_config();
        config.endpoint_url = Some("https://minio.example.local:9000".to_string());

        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_rejects_http_endpoint_when_allow_http_false() {
        let mut config = base_config();
        config.endpoint_url = Some("http://127.0.0.1:9000".to_string());
        config.allow_http = false;

        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_accepts_http_endpoint_when_allow_http_true() {
        let mut config = base_config();
        config.endpoint_url = Some("http://127.0.0.1:9000".to_string());
        config.allow_http = true;

        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_rejects_malformed_endpoint_scheme() {
        let mut config = base_config();
        config.endpoint_url = Some("ftp://minio.local:9000".to_string());
        config.allow_http = true;

        assert!(config.validate().is_err());
    }
}

impl S3BackendConfig {
    /// Validates configuration invariants.
    pub fn validate(&self) -> StorageResult<()> {
        if self.bulk_export_part_size == 0 {
            return Err(StorageError::Backend(BackendError::Internal {
                backend_name: "s3".to_string(),
                message: "bulk_export_part_size must be > 0".to_string(),
                source: None,
            }));
        }

        if self.bulk_submit_batch_size == 0 {
            return Err(StorageError::Backend(BackendError::Internal {
                backend_name: "s3".to_string(),
                message: "bulk_submit_batch_size must be > 0".to_string(),
                source: None,
            }));
        }

        if let Some(endpoint_url) = self.endpoint_url.as_deref() {
            let endpoint_url = endpoint_url.trim();
            if endpoint_url.is_empty() {
                return Err(StorageError::Backend(BackendError::Internal {
                    backend_name: "s3".to_string(),
                    message: "endpoint_url must not be empty when provided".to_string(),
                    source: None,
                }));
            }

            let lower = endpoint_url.to_ascii_lowercase();
            let is_http = lower.starts_with("http://");
            let is_https = lower.starts_with("https://");
            if !is_http && !is_https {
                return Err(StorageError::Backend(BackendError::Internal {
                    backend_name: "s3".to_string(),
                    message: "endpoint_url must start with http:// or https://".to_string(),
                    source: None,
                }));
            }

            if is_http && !self.allow_http {
                return Err(StorageError::Backend(BackendError::Internal {
                    backend_name: "s3".to_string(),
                    message: "http endpoint_url requires allow_http=true".to_string(),
                    source: None,
                }));
            }
        }

        match &self.tenancy_mode {
            S3TenancyMode::PrefixPerTenant { bucket } => {
                if bucket.trim().is_empty() {
                    return Err(StorageError::Backend(BackendError::Internal {
                        backend_name: "s3".to_string(),
                        message: "prefix-per-tenant bucket must not be empty".to_string(),
                        source: None,
                    }));
                }
            }
            S3TenancyMode::BucketPerTenant {
                tenant_bucket_map,
                default_system_bucket,
            } => {
                if tenant_bucket_map.is_empty() && default_system_bucket.is_none() {
                    return Err(StorageError::Backend(BackendError::Internal {
                        backend_name: "s3".to_string(),
                        message: "bucket-per-tenant requires at least one mapped bucket or default_system_bucket"
                            .to_string(),
                        source: None,
                    }));
                }

                if tenant_bucket_map.values().any(|b| b.trim().is_empty()) {
                    return Err(StorageError::Backend(BackendError::Internal {
                        backend_name: "s3".to_string(),
                        message: "bucket-per-tenant mapping contains empty bucket name".to_string(),
                        source: None,
                    }));
                }

                if default_system_bucket
                    .as_ref()
                    .map(|b| b.trim().is_empty())
                    .unwrap_or(false)
                {
                    return Err(StorageError::Backend(BackendError::Internal {
                        backend_name: "s3".to_string(),
                        message: "default_system_bucket must not be empty when provided"
                            .to_string(),
                        source: None,
                    }));
                }
            }
        }

        Ok(())
    }

    /// Returns a de-duplicated set of all buckets referenced by this config.
    pub fn configured_buckets(&self) -> HashSet<String> {
        let mut out = HashSet::new();
        match &self.tenancy_mode {
            S3TenancyMode::PrefixPerTenant { bucket } => {
                out.insert(bucket.clone());
            }
            S3TenancyMode::BucketPerTenant {
                tenant_bucket_map,
                default_system_bucket,
            } => {
                out.extend(tenant_bucket_map.values().cloned());
                if let Some(bucket) = default_system_bucket {
                    out.insert(bucket.clone());
                }
            }
        }
        out
    }
}
