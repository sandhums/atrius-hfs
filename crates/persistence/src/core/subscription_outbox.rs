//! Durable subscription event outbox.
//!
//! Resource write handlers enqueue change events here; a background consumer in
//! `helios-subscriptions` claims rows and drives matching/dispatch. Rows use a
//! CloudEvents-style envelope so a later broker relay (Kafka/MSK) can publish
//! without changing the write path.
//!
//! Delivery is at-least-once: a crash after dispatch but before `mark_processed`
//! can redeliver. Subscribers must tolerate duplicates (event numbering + `$events`
//! recovery handle gaps; clients handle duplicates).

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use helios_fhir::FhirVersion;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::error::StorageResult;
use crate::tenant::TenantId;

/// CloudEvents-compatible type URI for FHIR resource change events.
pub const OUTBOX_EVENT_TYPE: &str = "com.helios.hfs.fhir.resource.change";

/// CloudEvents spec version stamped on every envelope.
pub const CLOUDEVENTS_SPEC_VERSION: &str = "1.0";

/// Whether SQL backends should write outbox rows in the same TX as resource writes.
///
/// Mirrors the `HFS_SUBSCRIPTIONS_ENABLED` gate used by `helios-rest` when
/// constructing the subscription engine. When false, backends skip the insert
/// so unused deployments do not accumulate rows.
pub fn subscription_outbox_writes_enabled() -> bool {
    std::env::var("HFS_SUBSCRIPTIONS_ENABLED")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | ""))
        .unwrap_or(false)
}

/// CloudEvents `source` stamped into outbox envelopes.
pub fn subscription_outbox_source() -> String {
    std::env::var("HFS_BASE_URL").unwrap_or_else(|_| "hfs".to_string())
}

/// Interaction that produced an outbox event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutboxEventType {
    /// Resource was created.
    Create,
    /// Resource was updated (including restore).
    Update,
    /// Resource was deleted.
    Delete,
}

impl OutboxEventType {
    /// Wire / SQL form (`create`, `update`, `delete`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }

    /// Parses the wire / SQL form. Unknown values return `None`.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "create" => Some(Self::Create),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }
}

impl std::fmt::Display for OutboxEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A durable resource-change event awaiting (or finishing) subscription processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionOutboxEntry {
    /// Database-assigned row id (`None` before insert).
    pub id: Option<i64>,
    /// Stable event id (UUID), also used as CloudEvents `id`.
    pub event_id: Uuid,
    /// Tenant that owns the changed resource.
    pub tenant_id: TenantId,
    /// FHIR version of the stored resource.
    pub fhir_version: FhirVersion,
    /// FHIR resource type (e.g. `Patient`).
    pub resource_type: String,
    /// Logical id of the changed resource.
    pub resource_id: String,
    /// Version id after the write (`""` when unknown).
    pub version_id: String,
    /// Kind of write that produced this event.
    pub event_type: OutboxEventType,
    /// Resource content after the write (`None` for delete).
    pub resource: Option<Value>,
    /// Resource content before the write (`None` for create).
    pub previous_resource: Option<Value>,
    /// When the outbox row was first inserted.
    pub created_at: DateTime<Utc>,
    /// Earliest time the row may be claimed (supports retry backoff).
    pub available_at: DateTime<Utc>,
    /// How many times a worker has claimed this row.
    pub attempts: u32,
    /// Last processing error, if a claim failed and was released for retry.
    pub last_error: Option<String>,
}

impl SubscriptionOutboxEntry {
    /// Builds a new pending entry from a resource write.
    pub fn new(
        tenant_id: TenantId,
        fhir_version: FhirVersion,
        resource_type: impl Into<String>,
        resource_id: impl Into<String>,
        version_id: impl Into<String>,
        event_type: OutboxEventType,
        resource: Option<Value>,
        previous_resource: Option<Value>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: None,
            event_id: Uuid::new_v4(),
            tenant_id,
            fhir_version,
            resource_type: resource_type.into(),
            resource_id: resource_id.into(),
            version_id: version_id.into(),
            event_type,
            resource,
            previous_resource,
            created_at: now,
            available_at: now,
            attempts: 0,
            last_error: None,
        }
    }

    /// CloudEvents-style envelope for broker relay / debugging.
    pub fn envelope(&self, source: &str) -> Value {
        json!({
            "specversion": CLOUDEVENTS_SPEC_VERSION,
            "id": self.event_id.to_string(),
            "source": source,
            "type": OUTBOX_EVENT_TYPE,
            "time": self.created_at.to_rfc3339(),
            "datacontenttype": "application/json",
            "data": {
                "tenantId": self.tenant_id.as_str(),
                "fhirVersion": self.fhir_version.as_mime_param(),
                "resourceType": self.resource_type,
                "resourceId": self.resource_id,
                "versionId": self.version_id,
                "eventType": self.event_type.as_str(),
                "resource": self.resource,
                "previousResource": self.previous_resource,
            }
        })
    }
}

/// Durable store for subscription outbox rows.
#[async_trait]
pub trait SubscriptionOutboxStore: Send + Sync {
    /// Persist a pending event. Returns the assigned row id.
    async fn enqueue(&self, entry: &SubscriptionOutboxEntry) -> StorageResult<i64>;

    /// Claim up to `limit` available rows for this worker.
    ///
    /// Claimed rows are locked until `mark_processed` / `mark_retry` /
    /// `mark_dead` or the lock lease expires.
    ///
    /// Postgres uses `FOR UPDATE SKIP LOCKED`. SQLite takes a process-local
    /// mutex, `BEGIN IMMEDIATE`, and a compare-and-swap `UPDATE` so workers
    /// that share **one database file** cannot double-claim. That is not a
    /// cluster outbox: do not point several HFS nodes at separate SQLite
    /// copies of the same logical stream — use Postgres.
    async fn claim(
        &self,
        worker_id: &str,
        limit: u32,
        lease: std::time::Duration,
    ) -> StorageResult<Vec<SubscriptionOutboxEntry>>;

    /// Mark a claimed row as successfully processed.
    async fn mark_processed(&self, id: i64) -> StorageResult<()>;

    /// Release a claimed row for retry after `delay`, recording `error`.
    async fn mark_retry(
        &self,
        id: i64,
        delay: std::time::Duration,
        error: &str,
    ) -> StorageResult<()>;

    /// Tombstone a claimed row after retries are exhausted.
    ///
    /// Dead rows keep `processed_at` unset so `$events` does not treat them as
    /// successful deliveries, and they are excluded from later `claim` calls.
    async fn mark_dead(&self, id: i64, error: &str) -> StorageResult<()>;

    /// Fetch processed events for a tenant, oldest first.
    ///
    /// Used by `$events` recovery. `after_id` is exclusive (resume cursor).
    async fn list_processed(
        &self,
        tenant_id: &TenantId,
        after_id: Option<i64>,
        limit: u32,
    ) -> StorageResult<Vec<SubscriptionOutboxEntry>>;
}

/// Shared handle used by the subscription engine.
pub type DynSubscriptionOutboxStore = Arc<dyn SubscriptionOutboxStore>;

/// In-memory outbox for unit tests and backends without durable outbox support.
#[derive(Debug, Default)]
pub struct InMemorySubscriptionOutbox {
    inner: parking_lot::Mutex<InMemoryState>,
}

#[derive(Debug, Default)]
struct InMemoryState {
    next_id: i64,
    rows: Vec<SubscriptionOutboxEntry>,
    processed: HashSet<i64>,
    locked: HashSet<i64>,
    dead: HashSet<i64>,
}

impl InMemorySubscriptionOutbox {
    /// Empty in-memory outbox.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SubscriptionOutboxStore for InMemorySubscriptionOutbox {
    async fn enqueue(&self, entry: &SubscriptionOutboxEntry) -> StorageResult<i64> {
        let mut state = self.inner.lock();
        state.next_id += 1;
        let id = state.next_id;
        let mut stored = entry.clone();
        stored.id = Some(id);
        state.rows.push(stored);
        Ok(id)
    }

    async fn claim(
        &self,
        _worker_id: &str,
        limit: u32,
        _lease: std::time::Duration,
    ) -> StorageResult<Vec<SubscriptionOutboxEntry>> {
        let now = Utc::now();
        let mut state = self.inner.lock();
        let mut claim_ids = Vec::new();
        for row in &state.rows {
            if claim_ids.len() as u32 >= limit {
                break;
            }
            let Some(id) = row.id else {
                continue;
            };
            if state.processed.contains(&id)
                || state.locked.contains(&id)
                || state.dead.contains(&id)
            {
                continue;
            }
            if row.available_at > now {
                continue;
            }
            claim_ids.push(id);
        }
        let mut claimed = Vec::with_capacity(claim_ids.len());
        for id in claim_ids {
            state.locked.insert(id);
            if let Some(row) = state.rows.iter_mut().find(|r| r.id == Some(id)) {
                row.attempts = row.attempts.saturating_add(1);
                claimed.push(row.clone());
            }
        }
        Ok(claimed)
    }

    async fn mark_processed(&self, id: i64) -> StorageResult<()> {
        let mut state = self.inner.lock();
        state.locked.remove(&id);
        state.processed.insert(id);
        Ok(())
    }

    async fn mark_retry(
        &self,
        id: i64,
        delay: std::time::Duration,
        error: &str,
    ) -> StorageResult<()> {
        let mut state = self.inner.lock();
        state.locked.remove(&id);
        if let Some(row) = state.rows.iter_mut().find(|r| r.id == Some(id)) {
            row.available_at = Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default();
            row.last_error = Some(error.to_string());
        }
        Ok(())
    }

    async fn mark_dead(&self, id: i64, error: &str) -> StorageResult<()> {
        let mut state = self.inner.lock();
        state.locked.remove(&id);
        state.processed.remove(&id);
        state.dead.insert(id);
        if let Some(row) = state.rows.iter_mut().find(|r| r.id == Some(id)) {
            row.last_error = Some(error.to_string());
        }
        Ok(())
    }

    async fn list_processed(
        &self,
        tenant_id: &TenantId,
        after_id: Option<i64>,
        limit: u32,
    ) -> StorageResult<Vec<SubscriptionOutboxEntry>> {
        let state = self.inner.lock();
        let rows: Vec<_> = state
            .rows
            .iter()
            .filter(|r| {
                let id = r.id.unwrap_or(0);
                r.tenant_id == *tenant_id
                    && state.processed.contains(&id)
                    && after_id.map(|a| id > a).unwrap_or(true)
            })
            .take(limit as usize)
            .cloned()
            .collect();
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_enqueue_claim_process() {
        let store = InMemorySubscriptionOutbox::new();
        let entry = SubscriptionOutboxEntry::new(
            TenantId::new("t1"),
            FhirVersion::R4,
            "Patient",
            "p1",
            "1",
            OutboxEventType::Create,
            Some(json!({"resourceType":"Patient","id":"p1"})),
            None,
        );
        let id = store.enqueue(&entry).await.unwrap();
        assert!(id > 0);

        let claimed = store
            .claim("worker-1", 10, std::time::Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].resource_id, "p1");

        store.mark_processed(id).await.unwrap();
        let claimed_again = store
            .claim("worker-1", 10, std::time::Duration::from_secs(30))
            .await
            .unwrap();
        assert!(claimed_again.is_empty());
    }

    #[tokio::test]
    async fn in_memory_dead_letter_is_not_reclaimed_or_listed_as_processed() {
        let store = InMemorySubscriptionOutbox::new();
        let entry = SubscriptionOutboxEntry::new(
            TenantId::new("t1"),
            FhirVersion::R4,
            "Patient",
            "p1",
            "1",
            OutboxEventType::Create,
            Some(json!({"resourceType":"Patient","id":"p1"})),
            None,
        );
        let id = store.enqueue(&entry).await.unwrap();
        let claimed = store
            .claim("worker-1", 10, std::time::Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);

        store
            .mark_dead(id, "processing timed out; max retries exceeded")
            .await
            .unwrap();

        let claimed_again = store
            .claim("worker-1", 10, std::time::Duration::from_secs(30))
            .await
            .unwrap();
        assert!(claimed_again.is_empty());

        let processed = store
            .list_processed(&TenantId::new("t1"), None, 10)
            .await
            .unwrap();
        assert!(processed.is_empty());
    }

    #[test]
    fn envelope_is_cloudevents_shaped() {
        let entry = SubscriptionOutboxEntry::new(
            TenantId::new("t1"),
            FhirVersion::R4,
            "Observation",
            "o1",
            "2",
            OutboxEventType::Update,
            Some(json!({"resourceType":"Observation"})),
            None,
        );
        let env = entry.envelope("https://fhir.example/hfs");
        assert_eq!(env["specversion"], "1.0");
        assert_eq!(env["type"], OUTBOX_EVENT_TYPE);
        assert_eq!(env["data"]["resourceType"], "Observation");
    }
}
