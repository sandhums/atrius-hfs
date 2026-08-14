//! Channel dispatchers.
//!
//! Delivers notification bundles to subscriber endpoints via the configured
//! channel type (rest-hook, WebSocket, email, FHIR messaging).
//!
//! Dispatchers are registered on a [`ChannelDispatcherRegistry`] keyed by FHIR
//! channel-type code (`"rest-hook"`, `"websocket"`, …). The engine looks up
//! dispatchers by subscription channel type instead of hardcoding a match —
//! future broker/custom channels (e.g. Kafka) register the same way.

pub mod email;
pub mod messaging;
pub mod rest_hook;
pub mod websocket;
pub mod ws_manager;
pub mod ws_token;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::SubscriptionError;
use crate::manager::{ActiveSubscription, ChannelType};

/// The result of attempting to deliver a notification.
#[derive(Debug)]
pub enum DispatchResult {
    /// The endpoint accepted the notification.
    Success,
    /// The delivery failed with a retryable error (e.g., 5xx, timeout).
    RetryableError(String),
    /// The delivery failed with a permanent error (e.g., 4xx).
    PermanentError(String),
}

/// Trait for channel-specific notification delivery.
#[async_trait]
pub trait ChannelDispatcher: Send + Sync {
    /// Deliver a notification bundle to the subscriber's endpoint.
    async fn dispatch(
        &self,
        subscription: &ActiveSubscription,
        notification_bundle: &serde_json::Value,
    ) -> Result<DispatchResult, SubscriptionError>;

    /// Perform the handshake sequence for a newly activated subscription.
    async fn handshake(
        &self,
        subscription: &ActiveSubscription,
        handshake_bundle: &serde_json::Value,
    ) -> Result<DispatchResult, SubscriptionError>;
}

/// Registry of [`ChannelDispatcher`] implementations keyed by FHIR channel-type code.
#[derive(Default)]
pub struct ChannelDispatcherRegistry {
    dispatchers: HashMap<String, Arc<dyn ChannelDispatcher>>,
}

impl ChannelDispatcherRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            dispatchers: HashMap::new(),
        }
    }

    /// Register (or replace) a dispatcher for a channel type code (e.g. `"rest-hook"`).
    pub fn register(
        &mut self,
        channel_type: impl Into<String>,
        dispatcher: Arc<dyn ChannelDispatcher>,
    ) {
        self.dispatchers.insert(channel_type.into(), dispatcher);
    }

    /// Look up a dispatcher by [`ChannelType`].
    pub fn get(&self, channel_type: &ChannelType) -> Option<&dyn ChannelDispatcher> {
        self.dispatchers
            .get(channel_type.as_fhir_str())
            .map(|d| d.as_ref())
    }

    /// Whether a dispatcher is registered for the given channel type.
    pub fn contains(&self, channel_type: &ChannelType) -> bool {
        self.dispatchers.contains_key(channel_type.as_fhir_str())
    }

    /// Returns the registered FHIR channel-type codes.
    pub fn registered_types(&self) -> Vec<String> {
        self.dispatchers.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channels::rest_hook::RestHookChannel;

    #[test]
    fn registry_lookup_by_channel_type() {
        let mut registry = ChannelDispatcherRegistry::new();
        assert!(!registry.contains(&ChannelType::RestHook));

        registry.register("rest-hook", Arc::new(RestHookChannel::new()));
        assert!(registry.contains(&ChannelType::RestHook));
        assert!(registry.get(&ChannelType::RestHook).is_some());
        assert!(registry.get(&ChannelType::Websocket).is_none());
        assert!(
            registry
                .registered_types()
                .contains(&"rest-hook".to_string())
        );
    }

    #[test]
    fn registry_supports_custom_channel_codes() {
        let mut registry = ChannelDispatcherRegistry::new();
        registry.register("kafka", Arc::new(RestHookChannel::new()));
        assert!(registry.contains(&ChannelType::Custom("kafka".to_string())));
        assert!(
            registry
                .get(&ChannelType::Custom("kafka".to_string()))
                .is_some()
        );
    }
}
