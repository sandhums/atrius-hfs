//! Background heartbeat sender for active subscriptions.
//!
//! Periodically scans active subscriptions with a configured `heartbeatPeriod`
//! and dispatches heartbeat notifications when due.

use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::config::SubscriptionConfig;
use crate::engine::SubscriptionEngine;
use crate::manager::SubscriptionStatusCode;
use crate::notification;

/// Runs the heartbeat loop until the process exits.
pub async fn run_heartbeat_worker(engine: Arc<SubscriptionEngine>, config: SubscriptionConfig) {
    let interval = config.heartbeat_check_interval;
    info!(
        check_interval_ms = interval.as_millis() as u64,
        "Subscription heartbeat worker started"
    );

    loop {
        tokio::time::sleep(interval).await;
        let due = engine
            .manager()
            .subscriptions_due_for_heartbeat(chrono::Utc::now());
        if due.is_empty() {
            continue;
        }
        debug!(count = due.len(), "Dispatching due subscription heartbeats");
        for sub in due {
            if sub.status != SubscriptionStatusCode::Active {
                continue;
            }
            let public_base_url = engine.public_base_url(&sub.tenant_id);
            match notification::build_heartbeat(&sub, &public_base_url) {
                Ok(bundle) => {
                    engine.dispatch_heartbeat(&sub, &bundle).await;
                }
                Err(e) => {
                    warn!(
                        tenant_id = %sub.tenant_id,
                        subscription_id = %sub.id,
                        error = %e,
                        "Failed to build heartbeat notification"
                    );
                }
            }
        }
    }
}
