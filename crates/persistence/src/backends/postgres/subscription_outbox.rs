//! PostgreSQL implementation of [`SubscriptionOutboxStore`].

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use helios_fhir::FhirVersion;
use uuid::Uuid;

use crate::core::subscription_outbox::{
    OutboxEventType, SubscriptionOutboxEntry, SubscriptionOutboxStore, subscription_outbox_source,
    subscription_outbox_writes_enabled,
};
use crate::error::{BackendError, StorageError, StorageResult};
use crate::tenant::TenantId;
use serde_json::Value;

fn internal_error(message: String) -> StorageError {
    StorageError::Backend(BackendError::Internal {
        backend_name: "postgres".to_string(),
        message,
        source: None,
    })
}

/// Postgres-backed durable subscription outbox.
#[derive(Clone)]
pub struct PostgresSubscriptionOutbox {
    pool: Pool,
    /// CloudEvents `source` stamped into every envelope.
    source: String,
}

impl PostgresSubscriptionOutbox {
    /// Wraps an existing pool. `source` is the CloudEvents `source` on every envelope.
    pub fn new(pool: Pool, source: impl Into<String>) -> Self {
        Self {
            pool,
            source: source.into(),
        }
    }

    /// Insert an outbox row on an open connection / transaction client.
    pub(crate) async fn insert_on_client(
        client: &deadpool_postgres::Client,
        source: &str,
        entry: &SubscriptionOutboxEntry,
    ) -> StorageResult<i64> {
        let envelope = entry.envelope(source);
        let tenant_id = entry.tenant_id.as_str();
        let fhir_version = entry.fhir_version.as_mime_param();
        let event_type = entry.event_type.as_str();
        let attempts = entry.attempts as i32;

        let row = client
            .query_one(
                "INSERT INTO subscription_outbox (
                    event_id, tenant_id, fhir_version, resource_type, resource_id, version_id,
                    event_type, resource, previous_resource, envelope, created_at, available_at,
                    attempts, last_error
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                 RETURNING id",
                &[
                    &entry.event_id,
                    &tenant_id,
                    &fhir_version,
                    &entry.resource_type,
                    &entry.resource_id,
                    &entry.version_id,
                    &event_type,
                    &entry.resource,
                    &entry.previous_resource,
                    &envelope,
                    &entry.created_at,
                    &entry.available_at,
                    &attempts,
                    &entry.last_error,
                ],
            )
            .await
            .map_err(|e| internal_error(format!("outbox insert: {e}")))?;

        Ok(row.get(0))
    }

    /// Write an outbox row when subscriptions are enabled (same-TX helper).
    pub(crate) async fn maybe_enqueue_on_client(
        client: &deadpool_postgres::Client,
        tenant_id: &TenantId,
        fhir_version: FhirVersion,
        resource_type: &str,
        resource_id: &str,
        version_id: &str,
        event_type: OutboxEventType,
        resource: Option<Value>,
        previous_resource: Option<Value>,
    ) -> StorageResult<()> {
        if !subscription_outbox_writes_enabled() {
            return Ok(());
        }
        let entry = SubscriptionOutboxEntry::new(
            tenant_id.clone(),
            fhir_version,
            resource_type,
            resource_id,
            version_id,
            event_type,
            resource,
            previous_resource,
        );
        let source = subscription_outbox_source();
        Self::insert_on_client(client, &source, &entry).await?;
        Ok(())
    }

    fn row_to_entry(row: &tokio_postgres::Row) -> StorageResult<SubscriptionOutboxEntry> {
        let id: i64 = row.get("id");
        let event_id: Uuid = row.get("event_id");
        let tenant_id: String = row.get("tenant_id");
        let fhir_version_str: String = row.get("fhir_version");
        let fhir_version = FhirVersion::from_mime_param(&fhir_version_str).ok_or_else(|| {
            internal_error(format!(
                "Unknown fhir_version in outbox: {fhir_version_str}"
            ))
        })?;
        let event_type_str: String = row.get("event_type");
        let event_type = OutboxEventType::parse(&event_type_str).ok_or_else(|| {
            internal_error(format!("Unknown event_type in outbox: {event_type_str}"))
        })?;
        let attempts: i32 = row.get("attempts");

        Ok(SubscriptionOutboxEntry {
            id: Some(id),
            event_id,
            tenant_id: TenantId::new(tenant_id),
            fhir_version,
            resource_type: row.get("resource_type"),
            resource_id: row.get("resource_id"),
            version_id: row.get("version_id"),
            event_type,
            resource: row.get("resource"),
            previous_resource: row.get("previous_resource"),
            created_at: row.get("created_at"),
            available_at: row.get("available_at"),
            attempts: attempts.max(0) as u32,
            last_error: row.get("last_error"),
        })
    }
}

#[async_trait]
impl SubscriptionOutboxStore for PostgresSubscriptionOutbox {
    async fn enqueue(&self, entry: &SubscriptionOutboxEntry) -> StorageResult<i64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| internal_error(format!("outbox pool get: {e}")))?;
        Self::insert_on_client(&client, &self.source, entry).await
    }

    async fn claim(
        &self,
        worker_id: &str,
        limit: u32,
        lease: Duration,
    ) -> StorageResult<Vec<SubscriptionOutboxEntry>> {
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|e| internal_error(format!("outbox pool get: {e}")))?;

        let tx = client
            .transaction()
            .await
            .map_err(|e| internal_error(format!("outbox claim begin: {e}")))?;

        // Bind owned values: tokio-postgres rejects some borrowed/`INTERVAL`
        // expressions at serialize time ("error serializing parameter N").
        let worker_id = worker_id.to_string();
        let limit = limit.max(1) as i64;
        let locked_until: DateTime<Utc> = Utc::now()
            + chrono::Duration::from_std(lease.max(Duration::from_secs(1)))
                .unwrap_or_else(|_| chrono::Duration::seconds(60));

        let rows = tx
            .query(
                "WITH candidates AS (
                    SELECT id FROM subscription_outbox
                    WHERE processed_at IS NULL
                      AND available_at <= NOW()
                      AND (locked_until IS NULL OR locked_until < NOW())
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT $1
                 )
                 UPDATE subscription_outbox AS o
                 SET locked_by = $2,
                     locked_until = $3,
                     attempts = o.attempts + 1
                 FROM candidates
                 WHERE o.id = candidates.id
                 RETURNING o.id, o.event_id, o.tenant_id, o.fhir_version, o.resource_type,
                           o.resource_id, o.version_id, o.event_type, o.resource,
                           o.previous_resource, o.created_at, o.available_at, o.attempts,
                           o.last_error",
                &[&limit, &worker_id, &locked_until],
            )
            .await
            .map_err(|e| internal_error(format!("outbox claim: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| internal_error(format!("outbox claim commit: {e}")))?;

        rows.iter().map(Self::row_to_entry).collect()
    }

    async fn mark_processed(&self, id: i64) -> StorageResult<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| internal_error(format!("outbox pool get: {e}")))?;

        client
            .execute(
                "UPDATE subscription_outbox
                 SET processed_at = NOW(),
                     locked_by = NULL,
                     locked_until = NULL,
                     last_error = NULL
                 WHERE id = $1",
                &[&id],
            )
            .await
            .map_err(|e| internal_error(format!("outbox mark_processed: {e}")))?;
        Ok(())
    }

    async fn mark_retry(&self, id: i64, delay: Duration, error: &str) -> StorageResult<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| internal_error(format!("outbox pool get: {e}")))?;

        let available_at: DateTime<Utc> =
            Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default();

        client
            .execute(
                "UPDATE subscription_outbox
                 SET available_at = $2,
                     locked_by = NULL,
                     locked_until = NULL,
                     last_error = $3
                 WHERE id = $1",
                &[&id, &available_at, &error],
            )
            .await
            .map_err(|e| internal_error(format!("outbox mark_retry: {e}")))?;
        Ok(())
    }

    async fn list_processed(
        &self,
        tenant_id: &TenantId,
        after_id: Option<i64>,
        limit: u32,
    ) -> StorageResult<Vec<SubscriptionOutboxEntry>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| internal_error(format!("outbox pool get: {e}")))?;

        let tenant = tenant_id.as_str();
        let limit = limit.max(1) as i64;
        let after = after_id.unwrap_or(0);

        let rows = client
            .query(
                "SELECT id, event_id, tenant_id, fhir_version, resource_type, resource_id,
                        version_id, event_type, resource, previous_resource, created_at,
                        available_at, attempts, last_error
                 FROM subscription_outbox
                 WHERE tenant_id = $1
                   AND processed_at IS NOT NULL
                   AND id > $2
                 ORDER BY id ASC
                 LIMIT $3",
                &[&tenant, &after, &limit],
            )
            .await
            .map_err(|e| internal_error(format!("outbox list_processed: {e}")))?;

        rows.iter().map(Self::row_to_entry).collect()
    }
}
