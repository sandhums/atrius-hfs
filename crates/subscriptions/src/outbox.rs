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
        // Evaluation/dispatch errors are logged inside the engine; treat panic-free
        // completion as success. Transient channel failures use in-engine retry.
        // If we want outbox-level retry for hard failures later, wrap this.
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
                let attempt = entry.attempts.saturating_sub(1);
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
                    error!(outbox_id = id, "Outbox processing timed out; giving up");
                    let _ = store
                        .mark_retry(
                            id,
                            Duration::from_secs(3600),
                            "processing timed out; max retries exceeded",
                        )
                        .await;
                }
            }
        }
    }
    Ok(processed)
}
