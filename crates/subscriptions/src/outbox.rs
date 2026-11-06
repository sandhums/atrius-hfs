//! Outbox enqueue + background consumer for durable subscription dispatch.
//!
//! When a [`SubscriptionOutboxStore`] is attached to the engine, resource write
//! handlers enqueue events instead of `tokio::spawn`ing evaluation on the
//! request path. A single worker claims rows, runs the existing matching and
//! dispatch pipeline, then marks them processed.

use std::sync::Arc;
use std::time::Duration;

use helios_persistence::core::{
    DynSubscriptionOutboxStore, OutboxEventType, SubscriptionOutboxEntry,
};
use tracing::{error, info, warn};

use crate::config::SubscriptionConfig;
use crate::engine::SubscriptionEngine;
use crate::engine::retry::{calculate_delay, should_retry};
use crate::event::{ResourceEvent, ResourceEventType};

/// Converts a durable outbox row into the engine's in-memory event type.
pub fn entry_to_resource_event(entry: &SubscriptionOutboxEntry) -> ResourceEvent {
    let event_type = match entry.event_type {
        OutboxEventType::Create => ResourceEventType::Create,
        OutboxEventType::Update => ResourceEventType::Update,
        OutboxEventType::Delete => ResourceEventType::Delete,
    };
    ResourceEvent {
        tenant_id: entry.tenant_id.clone(),
        fhir_version: entry.fhir_version,
        resource_type: entry.resource_type.clone(),
        resource_id: entry.resource_id.clone(),
        version_id: entry.version_id.clone(),
        event_type,
        resource: entry.resource.clone(),
        previous_resource: entry.previous_resource.clone(),
        timestamp: entry.created_at,
    }
}

/// Builds an outbox entry from a resource event.
pub fn resource_event_to_entry(event: &ResourceEvent) -> SubscriptionOutboxEntry {
    let event_type = match event.event_type {
        ResourceEventType::Create => OutboxEventType::Create,
        ResourceEventType::Update => OutboxEventType::Update,
        ResourceEventType::Delete => OutboxEventType::Delete,
    };
    SubscriptionOutboxEntry::new(
        event.tenant_id.clone(),
        event.fhir_version,
        event.resource_type.clone(),
        event.resource_id.clone(),
        event.version_id.clone(),
        event_type,
        event.resource.clone(),
        event.previous_resource.clone(),
    )
}

/// Runs the outbox claim/process loop until the process exits.
pub async fn run_outbox_worker(
    engine: Arc<SubscriptionEngine>,
    store: DynSubscriptionOutboxStore,
    config: SubscriptionConfig,
    notify: Arc<tokio::sync::Notify>,
) {
    let worker_id = format!("hfs-{}", uuid::Uuid::new_v4());
    let poll_interval = config.outbox_poll_interval;
    let batch_size = config.outbox_batch_size;
    let lease = config.outbox_claim_lease;

    info!(
        worker_id = %worker_id,
        poll_interval_ms = poll_interval.as_millis() as u64,
        batch_size,
        "Subscription outbox worker started"
    );

    loop {
        match process_batch(&engine, &store, &config, &worker_id, batch_size, lease).await {
            Ok(0) => {
                tokio::select! {
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(poll_interval) => {}
                }
            }
            Ok(n) => {
                tracing::debug!(processed = n, "Outbox batch processed");
            }
            Err(e) => {
                error!(error = %e, "Outbox worker batch failed");
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
}

async fn process_batch(
    engine: &SubscriptionEngine,
    store: &DynSubscriptionOutboxStore,
    config: &SubscriptionConfig,
    worker_id: &str,
    batch_size: u32,
    lease: Duration,
) -> Result<usize, String> {
    let claimed = store
        .claim(worker_id, batch_size, lease)
        .await
        .map_err(|e| e.to_string())?;

    let mut processed = 0usize;
    for entry in claimed {
        let Some(id) = entry.id else {
            warn!("Claimed outbox row missing id; skipping");
            continue;
        };
        let event = entry_to_resource_event(&entry);
        // Evaluation/dispatch errors are logged inside the engine. Completing
        // without panic still marks the row processed so we do not re-increment
        // `event_number` and duplicate notifications. Permanent channel failures
        // are visible via DeliveryStats and a "zero successful deliveries" warn.
        // Outbox-level retry is only for process timeout (crash/hang). After
        // max retries those rows are tombstoned (`dead_at`), not retried hourly.
        match tokio::time::timeout(
            config.outbox_process_timeout,
            engine.on_resource_event(event),
        )
        .await
        {
            Ok(()) => {
                if let Err(e) = store.mark_processed(id).await {
                    error!(outbox_id = id, error = %e, "Failed to mark outbox row processed");
                    let delay = calculate_delay(config, entry.attempts.saturating_sub(1));
                    let _ = store.mark_retry(id, delay, &e.to_string()).await;
                } else {
                    processed += 1;
                }
            }
            Err(_) => {
                on_process_timeout(store, config, id, entry.attempts.saturating_sub(1)).await;
            }
        }
    }
    Ok(processed)
}

async fn on_process_timeout(
    store: &DynSubscriptionOutboxStore,
    config: &SubscriptionConfig,
    id: i64,
    attempt: u32,
) {
    if should_retry(config, attempt) {
        let delay = calculate_delay(config, attempt);
        warn!(
            outbox_id = id,
            attempt,
            delay_ms = delay.as_millis() as u64,
            "Outbox processing timed out; scheduling retry"
        );
        let _ = store.mark_retry(id, delay, "processing timed out").await;
    } else {
        error!(
            outbox_id = id,
            "Outbox processing timed out; dead-lettering"
        );
        let _ = store
            .mark_dead(id, "processing timed out; max retries exceeded")
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helios_fhir::FhirVersion;
    use helios_persistence::core::{InMemorySubscriptionOutbox, SubscriptionOutboxEntry};
    use helios_persistence::tenant::TenantId;
    use serde_json::json;
    use std::sync::Arc;

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
    async fn exhausted_timeout_dead_letters_instead_of_hourly_retry() {
        let store: DynSubscriptionOutboxStore = Arc::new(InMemorySubscriptionOutbox::new());
        let id = store.enqueue(&sample_entry()).await.unwrap();
        let claimed = store.claim("w", 1, Duration::from_secs(30)).await.unwrap();
        assert_eq!(claimed.len(), 1);

        let config = SubscriptionConfig {
            max_retries: 0,
            ..Default::default()
        };
        on_process_timeout(&store, &config, id, claimed[0].attempts.saturating_sub(1)).await;

        let again = store.claim("w", 1, Duration::from_secs(30)).await.unwrap();
        assert!(again.is_empty(), "dead-lettered row must not be reclaimed");
        let processed = store
            .list_processed(&TenantId::new("t1"), None, 10)
            .await
            .unwrap();
        assert!(
            processed.is_empty(),
            "dead rows must not appear in $events processed"
        );
    }

    #[tokio::test]
    async fn timeout_within_retry_budget_reschedules() {
        let store: DynSubscriptionOutboxStore = Arc::new(InMemorySubscriptionOutbox::new());
        let id = store.enqueue(&sample_entry()).await.unwrap();
        let claimed = store.claim("w", 1, Duration::from_secs(30)).await.unwrap();

        let config = SubscriptionConfig {
            max_retries: 3,
            retry_initial_delay: Duration::from_millis(1),
            retry_max_delay: Duration::from_millis(1),
            ..Default::default()
        };
        on_process_timeout(&store, &config, id, claimed[0].attempts.saturating_sub(1)).await;

        tokio::time::sleep(Duration::from_millis(5)).await;
        let again = store.claim("w", 1, Duration::from_secs(30)).await.unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].id, Some(id));
    }
}
