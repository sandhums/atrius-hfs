//! SQLite implementation of [`SubscriptionOutboxStore`].
//!
//! Claim uses a process-local mutex, `BEGIN IMMEDIATE`, and a compare-and-swap
//! `UPDATE` so workers that share one database file cannot double-claim.
//! SQLite has no `SELECT … FOR UPDATE SKIP LOCKED`. Clustered subscription
//! dispatch needs Postgres. The mutex is required because shared-cache
//! in-memory pools fail `BEGIN IMMEDIATE` with `SQLITE_LOCKED`, which
//! `busy_timeout` does not cover.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::core::subscription_outbox::{
    OutboxEventType, SubscriptionOutboxEntry, SubscriptionOutboxStore, subscription_outbox_source,
    subscription_outbox_writes_enabled,
};
use crate::error::{BackendError, StorageError, StorageResult};
use crate::tenant::TenantId;
use helios_fhir::FhirVersion;
use serde_json::Value;

fn internal_error(message: String) -> StorageError {
    StorageError::Backend(BackendError::Internal {
        backend_name: "sqlite".to_string(),
        message,
        source: None,
    })
}

fn parse_dt(s: &str) -> StorageResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| internal_error(format!("invalid outbox timestamp '{s}': {e}")))
}

/// Serializes in-process claims. Shared-cache SQLite returns `SQLITE_LOCKED`
/// on concurrent `BEGIN IMMEDIATE`; `busy_timeout` does not apply. File WAL
/// still serializes writers via IMMEDIATE for other processes.
static OUTBOX_CLAIM_LOCK: Mutex<()> = Mutex::const_new(());

/// SQLite-backed durable subscription outbox.
#[derive(Clone)]
pub struct SqliteSubscriptionOutbox {
    pool: Pool<SqliteConnectionManager>,
    source: String,
}

impl SqliteSubscriptionOutbox {
    /// Wraps an existing pool. `source` is the CloudEvents `source` on every envelope.
    pub fn new(pool: Pool<SqliteConnectionManager>, source: impl Into<String>) -> Self {
        Self {
            pool,
            source: source.into(),
        }
    }

    /// Insert an outbox row on an open SQLite connection / transaction.
    pub(crate) fn insert_on_conn(
        conn: &rusqlite::Connection,
        source: &str,
        entry: &SubscriptionOutboxEntry,
    ) -> StorageResult<i64> {
        let envelope = entry.envelope(source);
        let resource = entry
            .resource
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| internal_error(format!("serialize resource: {e}")))?;
        let previous = entry
            .previous_resource
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| internal_error(format!("serialize previous_resource: {e}")))?;
        let envelope_str = serde_json::to_string(&envelope)
            .map_err(|e| internal_error(format!("serialize envelope: {e}")))?;

        conn.execute(
            "INSERT INTO subscription_outbox (
                event_id, tenant_id, fhir_version, resource_type, resource_id, version_id,
                event_type, resource, previous_resource, envelope, created_at, available_at,
                attempts, last_error
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            rusqlite::params![
                entry.event_id.to_string(),
                entry.tenant_id.as_str(),
                entry.fhir_version.as_mime_param(),
                entry.resource_type,
                entry.resource_id,
                entry.version_id,
                entry.event_type.as_str(),
                resource,
                previous,
                envelope_str,
                entry.created_at.to_rfc3339(),
                entry.available_at.to_rfc3339(),
                entry.attempts as i64,
                entry.last_error,
            ],
        )
        .map_err(|e| internal_error(format!("outbox insert: {e}")))?;

        Ok(conn.last_insert_rowid())
    }

    /// Write an outbox row when subscriptions are enabled (same-TX helper).
    pub(crate) fn maybe_enqueue_on_conn(
        conn: &rusqlite::Connection,
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
        Self::insert_on_conn(conn, &source, &entry)?;
        Ok(())
    }

    fn map_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SubscriptionOutboxEntry> {
        let id: i64 = row.get("id")?;
        let event_id: String = row.get("event_id")?;
        let tenant_id: String = row.get("tenant_id")?;
        let fhir_version_str: String = row.get("fhir_version")?;
        let event_type_str: String = row.get("event_type")?;
        let resource_json: Option<String> = row.get("resource")?;
        let previous_json: Option<String> = row.get("previous_resource")?;
        let created_at: String = row.get("created_at")?;
        let available_at: String = row.get("available_at")?;
        let attempts: i64 = row.get("attempts")?;
        let last_error: Option<String> = row.get("last_error")?;

        let fhir_version = helios_fhir::FhirVersion::from_mime_param(&fhir_version_str)
            .ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unknown fhir_version: {fhir_version_str}"),
                    )),
                )
            })?;
        let event_type = OutboxEventType::parse(&event_type_str).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unknown event_type: {event_type_str}"),
                )),
            )
        })?;
        let event_id = Uuid::parse_str(&event_id).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let resource = match resource_json {
            Some(s) => Some(serde_json::from_str(&s).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?),
            None => None,
        };
        let previous_resource = match previous_json {
            Some(s) => Some(serde_json::from_str(&s).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?),
            None => None,
        };

        Ok(SubscriptionOutboxEntry {
            id: Some(id),
            event_id,
            tenant_id: TenantId::new(tenant_id),
            fhir_version,
            resource_type: row.get("resource_type")?,
            resource_id: row.get("resource_id")?,
            version_id: row.get("version_id")?,
            event_type,
            resource,
            previous_resource,
            created_at: parse_dt(&created_at).map_err(|e| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                )))
            })?,
            available_at: parse_dt(&available_at).map_err(|e| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                )))
            })?,
            attempts: attempts.max(0) as u32,
            last_error,
        })
    }
}

#[async_trait]
impl SubscriptionOutboxStore for SqliteSubscriptionOutbox {
    async fn enqueue(&self, entry: &SubscriptionOutboxEntry) -> StorageResult<i64> {
        let pool = self.pool.clone();
        let source = self.source.clone();
        let entry = entry.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool
                .get()
                .map_err(|e| internal_error(format!("outbox pool get: {e}")))?;
            Self::insert_on_conn(&conn, &source, &entry)
        })
        .await
        .map_err(|e| internal_error(format!("outbox enqueue join: {e}")))?
    }

    async fn claim(
        &self,
        worker_id: &str,
        limit: u32,
        lease: Duration,
    ) -> StorageResult<Vec<SubscriptionOutboxEntry>> {
        let _guard = OUTBOX_CLAIM_LOCK.lock().await;
        let pool = self.pool.clone();
        let worker_id = worker_id.to_string();
        let limit = limit.max(1) as usize;
        let lease_secs = lease.as_secs().max(1) as i64;

        tokio::task::spawn_blocking(move || {
            let mut conn = pool
                .get()
                .map_err(|e| internal_error(format!("outbox pool get: {e}")))?;
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|e| internal_error(format!("outbox claim begin: {e}")))?;

            let now = Utc::now();
            let now_str = now.to_rfc3339();
            let locked_until = (now + chrono::Duration::seconds(lease_secs)).to_rfc3339();

            let ids: Vec<i64> = {
                let mut stmt = tx
                    .prepare(
                        "SELECT id FROM subscription_outbox
                         WHERE processed_at IS NULL
                           AND dead_at IS NULL
                           AND available_at <= ?1
                           AND (locked_until IS NULL OR locked_until < ?1)
                         ORDER BY id
                         LIMIT ?2",
                    )
                    .map_err(|e| internal_error(format!("outbox claim select: {e}")))?;
                let rows = stmt
                    .query_map(rusqlite::params![now_str, limit as i64], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(|e| internal_error(format!("outbox claim query: {e}")))?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|e| internal_error(format!("outbox claim collect: {e}")))?
            };

            let mut claimed = Vec::with_capacity(ids.len());
            for id in ids {
                let n = tx
                    .execute(
                        "UPDATE subscription_outbox
                         SET locked_by = ?2,
                             locked_until = ?3,
                             attempts = attempts + 1
                         WHERE id = ?1
                           AND processed_at IS NULL
                           AND dead_at IS NULL
                           AND (locked_until IS NULL OR locked_until < ?4)",
                        rusqlite::params![id, worker_id, locked_until, now_str],
                    )
                    .map_err(|e| internal_error(format!("outbox claim update: {e}")))?;
                if n == 0 {
                    continue;
                }

                let entry = tx
                    .query_row(
                        "SELECT id, event_id, tenant_id, fhir_version, resource_type, resource_id,
                                version_id, event_type, resource, previous_resource, created_at,
                                available_at, attempts, last_error
                         FROM subscription_outbox WHERE id = ?1",
                        rusqlite::params![id],
                        SqliteSubscriptionOutbox::map_row,
                    )
                    .map_err(|e| internal_error(format!("outbox claim fetch: {e}")))?;
                claimed.push(entry);
            }

            tx.commit()
                .map_err(|e| internal_error(format!("outbox claim commit: {e}")))?;
            Ok(claimed)
        })
        .await
        .map_err(|e| internal_error(format!("outbox claim join: {e}")))?
    }

    async fn mark_processed(&self, id: i64) -> StorageResult<()> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool
                .get()
                .map_err(|e| internal_error(format!("outbox pool get: {e}")))?;
            let now = Utc::now().to_rfc3339();
            conn.execute(
                "UPDATE subscription_outbox
                 SET processed_at = ?2,
                     locked_by = NULL,
                     locked_until = NULL,
                     last_error = NULL
                 WHERE id = ?1",
                rusqlite::params![id, now],
            )
            .map_err(|e| internal_error(format!("outbox mark_processed: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| internal_error(format!("outbox mark_processed join: {e}")))?
    }

    async fn mark_retry(&self, id: i64, delay: Duration, error: &str) -> StorageResult<()> {
        let pool = self.pool.clone();
        let error = error.to_string();
        let available_at =
            (Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default()).to_rfc3339();

        tokio::task::spawn_blocking(move || {
            let conn = pool
                .get()
                .map_err(|e| internal_error(format!("outbox pool get: {e}")))?;
            conn.execute(
                "UPDATE subscription_outbox
                 SET available_at = ?2,
                     locked_by = NULL,
                     locked_until = NULL,
                     last_error = ?3
                 WHERE id = ?1",
                rusqlite::params![id, available_at, error],
            )
            .map_err(|e| internal_error(format!("outbox mark_retry: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| internal_error(format!("outbox mark_retry join: {e}")))?
    }

    async fn mark_dead(&self, id: i64, error: &str) -> StorageResult<()> {
        let pool = self.pool.clone();
        let error = error.to_string();
        let now = Utc::now().to_rfc3339();

        tokio::task::spawn_blocking(move || {
            let conn = pool
                .get()
                .map_err(|e| internal_error(format!("outbox pool get: {e}")))?;
            conn.execute(
                "UPDATE subscription_outbox
                 SET dead_at = ?2,
                     locked_by = NULL,
                     locked_until = NULL,
                     last_error = ?3
                 WHERE id = ?1",
                rusqlite::params![id, now, error],
            )
            .map_err(|e| internal_error(format!("outbox mark_dead: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| internal_error(format!("outbox mark_dead join: {e}")))?
    }

    async fn list_processed(
        &self,
        tenant_id: &TenantId,
        after_id: Option<i64>,
        limit: u32,
    ) -> StorageResult<Vec<SubscriptionOutboxEntry>> {
        let pool = self.pool.clone();
        let tenant = tenant_id.as_str().to_string();
        let after = after_id.unwrap_or(0);
        let limit = limit.max(1) as i64;

        tokio::task::spawn_blocking(move || {
            let conn = pool
                .get()
                .map_err(|e| internal_error(format!("outbox pool get: {e}")))?;
            let mut stmt = conn
                .prepare(
                    "SELECT id, event_id, tenant_id, fhir_version, resource_type, resource_id,
                            version_id, event_type, resource, previous_resource, created_at,
                            available_at, attempts, last_error
                     FROM subscription_outbox
                     WHERE tenant_id = ?1
                       AND processed_at IS NOT NULL
                       AND id > ?2
                     ORDER BY id ASC
                     LIMIT ?3",
                )
                .map_err(|e| internal_error(format!("outbox list_processed prepare: {e}")))?;
            let rows = stmt
                .query_map(rusqlite::params![tenant, after, limit], |row| {
                    SqliteSubscriptionOutbox::map_row(row)
                })
                .map_err(|e| internal_error(format!("outbox list_processed query: {e}")))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| internal_error(format!("outbox list_processed collect: {e}")))
        })
        .await
        .map_err(|e| internal_error(format!("outbox list_processed join: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::sqlite::SqliteBackend;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;

    fn sample_entry() -> SubscriptionOutboxEntry {
        SubscriptionOutboxEntry::new(
            TenantId::new("t1"),
            FhirVersion::R4,
            "Patient",
            "p1",
            "1",
            OutboxEventType::Create,
            Some(json!({"resourceType":"Patient","id":"p1"})),
            None,
        )
    }

    #[tokio::test]
    async fn sqlite_claim_does_not_double_claim_under_contention() {
        let backend = SqliteBackend::in_memory().expect("in-memory sqlite");
        backend.init_schema().expect("schema");
        let store = Arc::new(SqliteSubscriptionOutbox::new(backend.pool(), "test"));
        let id = store.enqueue(&sample_entry()).await.expect("enqueue");

        let mut handles = Vec::new();
        for i in 0..8 {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                store
                    .claim(&format!("w{i}"), 10, Duration::from_secs(30))
                    .await
                    .expect("claim")
            }));
        }

        let mut winners = Vec::new();
        for h in handles {
            winners.extend(h.await.expect("join"));
        }
        assert_eq!(
            winners.len(),
            1,
            "exactly one worker must win the row, got {}",
            winners.len()
        );
        assert_eq!(winners[0].id, Some(id));
    }
}
