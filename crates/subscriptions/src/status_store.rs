//! Persist subscription status transitions to the stored FHIR `Subscription`.
//!
//! Runtime status lives in [`crate::manager::SubscriptionManager`]. Without
//! write-back, `GET /Subscription/{id}` stays at `"requested"` after a
//! successful handshake, and restart rehydration must re-handshake every live
//! subscription. This module patches `"status"` on the stored resource after
//! in-memory transitions succeed.

use std::sync::Arc;

use async_trait::async_trait;
use helios_fhir::FhirVersion;
use helios_persistence::core::ResourceStorage;
use helios_persistence::error::{ConcurrencyError, StorageError};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use tracing::{debug, warn};

use crate::error::SubscriptionError;
use crate::manager::SubscriptionStatusCode;

/// How many times to retry a status patch after an optimistic-lock conflict.
const MAX_VERSION_RETRIES: u32 = 3;

/// Writes subscription status codes back to durable FHIR storage.
#[async_trait]
pub trait SubscriptionStatusStore: Send + Sync {
    /// Patch `Subscription.status` for `subscription_id` in `tenant_id`.
    ///
    /// Idempotent when the stored value already matches `status`.
    async fn persist_status(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        status: SubscriptionStatusCode,
        fhir_version: FhirVersion,
    ) -> Result<(), SubscriptionError>;
}

/// Shared status-store handle attached to the engine.
pub type DynSubscriptionStatusStore = Arc<dyn SubscriptionStatusStore>;

/// [`SubscriptionStatusStore`] backed by [`ResourceStorage`].
pub struct ResourceStorageStatusStore<S> {
    storage: Arc<S>,
}

impl<S> ResourceStorageStatusStore<S> {
    /// Create a status store over the given persistence backend.
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S> SubscriptionStatusStore for ResourceStorageStatusStore<S>
where
    S: ResourceStorage + Send + Sync + 'static,
{
    async fn persist_status(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        status: SubscriptionStatusCode,
        _fhir_version: FhirVersion,
    ) -> Result<(), SubscriptionError> {
        let tenant =
            TenantContext::new(TenantId::new(tenant_id), TenantPermissions::full_access());
        let status_str = status.as_fhir_str();

        for attempt in 1..=MAX_VERSION_RETRIES {
            let Some(current) = self
                .storage
                .read(&tenant, "Subscription", subscription_id)
                .await?
            else {
                return Err(SubscriptionError::Storage(format!(
                    "Subscription/{subscription_id} not found while persisting status"
                )));
            };

            let mut resource = current.content().clone();
            let existing = resource.get("status").and_then(|v| v.as_str());
            if existing == Some(status_str) {
                debug!(
                    tenant_id,
                    subscription_id,
                    status = status_str,
                    "Stored subscription status already matches; skipping write-back"
                );
                return Ok(());
            }

            if let Some(obj) = resource.as_object_mut() {
                obj.insert("status".to_string(), serde_json::Value::String(status_str.to_string()));
            } else {
                return Err(SubscriptionError::InvalidSubscription {
                    message: format!(
                        "Subscription/{subscription_id} content is not a JSON object"
                    ),
                });
            }

            match self.storage.update(&tenant, &current, resource).await {
                Ok(_) => {
                    debug!(
                        tenant_id,
                        subscription_id,
                        status = status_str,
                        attempt,
                        "Persisted subscription status"
                    );
                    return Ok(());
                }
                Err(StorageError::Concurrency(ConcurrencyError::VersionConflict { .. }))
                    if attempt < MAX_VERSION_RETRIES =>
                {
                    warn!(
                        tenant_id,
                        subscription_id,
                        status = status_str,
                        attempt,
                        "Version conflict persisting subscription status; retrying"
                    );
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(SubscriptionError::Storage(format!(
            "exhausted retries persisting status for Subscription/{subscription_id}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helios_persistence::backends::sqlite::SqliteBackend;
    use serde_json::json;

    fn storage() -> SqliteBackend {
        let backend = SqliteBackend::in_memory().expect("sqlite");
        backend.init_schema().expect("schema");
        backend
    }

    async fn seed_subscription(backend: &SqliteBackend, id: &str, status: &str) {
        let tenant = TenantContext::new(TenantId::new("t1"), TenantPermissions::full_access());
        let resource = json!({
            "resourceType": "Subscription",
            "id": id,
            "status": status,
            "criteria": "http://example.org/topic",
            "channel": { "type": "rest-hook", "endpoint": "http://127.0.0.1/hook" }
        });
        backend
            .create(&tenant, "Subscription", resource, FhirVersion::default())
            .await
            .expect("create subscription");
    }

    #[tokio::test]
    async fn persist_status_updates_stored_resource() {
        let backend = storage();
        seed_subscription(&backend, "sub-1", "requested").await;
        let store = ResourceStorageStatusStore::new(Arc::new(backend));

        store
            .persist_status("t1", "sub-1", SubscriptionStatusCode::Active, FhirVersion::default())
            .await
            .expect("persist");

        let tenant = TenantContext::new(TenantId::new("t1"), TenantPermissions::full_access());
        let stored = store
            .storage
            .read(&tenant, "Subscription", "sub-1")
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            stored.content().get("status").and_then(|v| v.as_str()),
            Some("active")
        );
    }

    #[tokio::test]
    async fn persist_status_is_idempotent() {
        let backend = storage();
        seed_subscription(&backend, "sub-1", "active").await;
        let store = ResourceStorageStatusStore::new(Arc::new(backend));

        store
            .persist_status("t1", "sub-1", SubscriptionStatusCode::Active, FhirVersion::default())
            .await
            .expect("persist");
    }
}
