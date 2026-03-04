//! AWS S3 backend — struct definition, capability matrix, and Backend trait
//! implementation.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;

use crate::core::{Backend, BackendCapability, BackendKind};
use crate::error::{BackendError, StorageError, StorageResult};
use crate::tenant::{TenantContext, TenantId};

use super::client::{AwsS3Client, S3Api, S3ClientError};
use super::config::{S3BackendConfig, S3TenancyMode};
use super::keyspace::S3Keyspace;

/// AWS S3 backend for object-storage persistence.
#[derive(Clone)]
pub struct S3Backend {
    pub(crate) config: S3BackendConfig,
    pub(crate) client: Arc<dyn S3Api>,
}

impl std::fmt::Debug for S3Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Backend")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Opaque connection handle for the S3 backend.
///
/// S3 is stateless from the client's perspective — there is no persistent TCP
/// connection to acquire per-request. This marker type satisfies the `Backend`
/// trait's associated `Connection` type without holding any resources.
#[derive(Debug)]
pub struct S3Connection;

/// Resolved bucket name and key hierarchy for a single tenant.
///
/// Computed once per storage operation from the `TenantContext` and the
/// backend configuration, then passed through the call stack within that
/// operation.
#[derive(Debug, Clone)]
pub(crate) struct TenantLocation {
    /// S3 bucket that holds this tenant's data.
    pub bucket: String,
    /// Keyspace builder scoped to this tenant's prefix hierarchy.
    pub keyspace: S3Keyspace,
}

impl S3Backend {
    /// Creates a new S3 backend using AWS standard credential provider chain.
    pub fn new(config: S3BackendConfig) -> StorageResult<Self> {
        Self::from_env(config)
    }

    /// Creates a new S3 backend using environment/provider chain credentials.
    ///
    /// The region is resolved in priority order: `config.region`, then the
    /// `AWS_REGION` environment variable, then the standard AWS SDK provider
    /// chain (shared config file, EC2 instance metadata, etc.).
    ///
    /// If `validate_buckets_on_startup` is set, every configured bucket is
    /// verified with a `HeadBucket` call before this function returns.
    pub fn from_env(mut config: S3BackendConfig) -> StorageResult<Self> {
        config.validate()?;

        if config.region.is_none() {
            config.region = std::env::var("AWS_REGION").ok();
        }

        let sdk_config = block_on(AwsS3Client::load_sdk_config(config.region.as_deref()))?;
        let client = Arc::new(AwsS3Client::from_sdk_config(&sdk_config));

        let backend = Self { config, client };

        if backend.config.validate_buckets_on_startup {
            block_on(backend.validate_buckets())??;
        }

        Ok(backend)
    }

    /// Creates a backend with an injected `S3Api` implementation.
    ///
    /// Intended exclusively for unit tests that supply a mock client.
    /// Not compiled into non-test builds.
    #[cfg(test)]
    pub(crate) fn with_client(
        config: S3BackendConfig,
        client: Arc<dyn S3Api>,
    ) -> StorageResult<Self> {
        config.validate()?;
        Ok(Self { config, client })
    }

    /// Verifies that every bucket referenced in the configuration exists and
    /// is accessible to the current credentials.
    ///
    /// Issues a `HeadBucket` request for each distinct bucket. Returns the
    /// first error encountered; does not attempt to create missing buckets.
    pub(crate) async fn validate_buckets(&self) -> StorageResult<()> {
        for bucket in self.config.configured_buckets() {
            self.client
                .head_bucket(&bucket)
                .await
                .map_err(|e| self.map_client_error(e))?;
        }
        Ok(())
    }

    /// Resolves the bucket and keyspace for the given tenant.
    ///
    /// In `PrefixPerTenant` mode all tenants share one bucket and are separated
    /// by a key prefix derived from the tenant ID. In `BucketPerTenant` mode
    /// each tenant maps to a dedicated bucket looked up from the config map.
    ///
    /// Returns a `TenantError` if the tenant has no bucket assignment in the
    /// `BucketPerTenant` mapping.
    pub(crate) fn tenant_location(&self, tenant: &TenantContext) -> StorageResult<TenantLocation> {
        let global_prefix = self
            .config
            .prefix
            .as_ref()
            .map(|p| p.trim_matches('/').to_string())
            .filter(|p| !p.is_empty());

        match &self.config.tenancy_mode {
            S3TenancyMode::PrefixPerTenant { bucket } => Ok(TenantLocation {
                bucket: bucket.clone(),
                keyspace: S3Keyspace::new(global_prefix)
                    .with_tenant_prefix(tenant.tenant_id().as_str()),
            }),
            S3TenancyMode::BucketPerTenant {
                tenant_bucket_map,
                default_system_bucket,
            } => {
                let tenant_id = tenant.tenant_id().as_str();
                let bucket = tenant_bucket_map
                    .get(tenant_id)
                    .cloned()
                    .or_else(|| {
                        if tenant.tenant_id().is_system() {
                            default_system_bucket.clone()
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| {
                        StorageError::Tenant(crate::error::TenantError::InvalidTenant {
                            tenant_id: TenantId::new(tenant_id),
                        })
                    })?;

                Ok(TenantLocation {
                    bucket,
                    keyspace: S3Keyspace::new(global_prefix),
                })
            }
        }
    }

    /// Maps a low-level `S3ClientError` to the shared `StorageError` taxonomy.
    ///
    /// This is the error boundary between the S3 SDK layer and the storage
    /// trait layer. Keeping the translation here ensures all storage operations
    /// return consistent error variants regardless of the underlying transport.
    pub(crate) fn map_client_error(&self, error: S3ClientError) -> StorageError {
        match error {
            S3ClientError::NotFound => StorageError::Backend(BackendError::Unavailable {
                backend_name: "s3".to_string(),
                message: "resource not found in S3".to_string(),
            }),
            S3ClientError::PreconditionFailed => StorageError::Backend(BackendError::QueryError {
                message: "S3 precondition failed".to_string(),
            }),
            S3ClientError::Throttled(message) => StorageError::Backend(BackendError::Unavailable {
                backend_name: "s3".to_string(),
                message,
            }),
            S3ClientError::Unavailable(message) => {
                StorageError::Backend(BackendError::Unavailable {
                    backend_name: "s3".to_string(),
                    message,
                })
            }
            S3ClientError::InvalidInput(message) => {
                StorageError::Validation(crate::error::ValidationError::InvalidResource {
                    message,
                    details: Vec::new(),
                })
            }
            S3ClientError::Internal(message) => StorageError::Backend(BackendError::Internal {
                backend_name: "s3".to_string(),
                message,
                source: None,
            }),
        }
    }
}

#[async_trait]
impl Backend for S3Backend {
    type Connection = S3Connection;

    fn kind(&self) -> BackendKind {
        BackendKind::S3
    }

    fn name(&self) -> &'static str {
        "s3"
    }

    fn supports(&self, capability: BackendCapability) -> bool {
        matches!(
            capability,
            BackendCapability::Crud
                | BackendCapability::Versioning
                | BackendCapability::InstanceHistory
                | BackendCapability::TypeHistory
                | BackendCapability::SystemHistory
                | BackendCapability::OptimisticLocking
                | BackendCapability::CursorPagination
                | BackendCapability::BulkExport
                | BackendCapability::BulkImport
                | BackendCapability::SharedSchema
                | BackendCapability::DatabasePerTenant
        )
    }

    fn capabilities(&self) -> Vec<BackendCapability> {
        vec![
            BackendCapability::Crud,
            BackendCapability::Versioning,
            BackendCapability::InstanceHistory,
            BackendCapability::TypeHistory,
            BackendCapability::SystemHistory,
            BackendCapability::OptimisticLocking,
            BackendCapability::CursorPagination,
            BackendCapability::BulkExport,
            BackendCapability::BulkImport,
            BackendCapability::SharedSchema,
            BackendCapability::DatabasePerTenant,
        ]
    }

    async fn acquire(&self) -> Result<Self::Connection, BackendError> {
        Ok(S3Connection)
    }

    async fn release(&self, _conn: Self::Connection) {}

    async fn health_check(&self) -> Result<(), BackendError> {
        self.validate_buckets().await.map_err(|err| match err {
            StorageError::Backend(backend_err) => backend_err,
            other => BackendError::Internal {
                backend_name: "s3".to_string(),
                message: other.to_string(),
                source: None,
            },
        })
    }

    async fn initialize(&self) -> Result<(), BackendError> {
        self.health_check().await
    }

    async fn migrate(&self) -> Result<(), BackendError> {
        // No schema migrations for object storage.
        self.health_check().await
    }
}

/// Drives an async future to completion on the current thread.
///
/// If a Tokio runtime is already active the future is driven via
/// `block_in_place` to avoid nesting runtimes. Otherwise a temporary
/// single-threaded runtime is created for the duration of the call.
///
/// Used during synchronous backend construction (`from_env`) where async SDK
/// config loading must complete before the constructor can return.
fn block_on<F>(future: F) -> StorageResult<F::Output>
where
    F: Future,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        Ok(tokio::task::block_in_place(|| handle.block_on(future)))
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                StorageError::Backend(BackendError::Internal {
                    backend_name: "s3".to_string(),
                    message: format!("failed to create runtime: {e}"),
                    source: None,
                })
            })?;
        Ok(rt.block_on(future))
    }
}
