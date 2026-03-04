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
            validate_buckets_on_startup: true,
            bulk_export_part_size: 10_000,
            bulk_submit_batch_size: 100,
        }
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
