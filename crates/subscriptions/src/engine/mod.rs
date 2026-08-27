//! Subscription engine — the orchestrator.
//!
//! Ties together topic registry, subscription manager, event evaluator,
//! notification builder, and channel dispatchers. Invoked asynchronously
//! after each resource write.

pub mod retry;

use std::sync::Arc;

use dashmap::DashMap;
use tracing::{debug, error, info, warn};

use crate::channels::email::EmailChannel;
use crate::channels::messaging::MessagingChannel;
use crate::channels::rest_hook::RestHookChannel;
use crate::channels::websocket::WebSocketChannel;
use crate::channels::ws_manager::WebSocketManager;
use crate::channels::ws_token::WsBindingTokenManager;
use crate::channels::{ChannelDispatcher, ChannelDispatcherRegistry, DispatchResult};
use crate::config::SubscriptionConfig;
use crate::evaluator::EventEvaluator;
use crate::event::{ResourceEvent, ResourceEventType};
use crate::heartbeat::run_heartbeat_worker;
use crate::manager::{ActiveSubscription, SubscriptionManager, SubscriptionStatusCode};
use crate::notification::{self, NotificationEventData};
use crate::outbox::run_outbox_worker;
use crate::topics::InMemoryTopicRegistry;
use helios_auth::{NoOpOutboundAuthProvider, OutboundAuthProvider};
use helios_persistence::core::{DynSubscriptionOutboxStore, ResourceStorage};
use helios_persistence::error::StorageError;
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use tokio::sync::Semaphore;

/// The subscription engine orchestrates the entire subscription pipeline.
///
/// When a durable [`SubscriptionOutboxStore`] is attached, resource writes are
/// enqueued and processed by a background worker. Without an outbox the engine
/// falls back to fire-and-forget `tokio::spawn` (legacy / non-SQL backends).
pub struct SubscriptionEngine {
    topic_registry: Arc<InMemoryTopicRegistry>,
    topic_resource_index: DashMap<(String, String, String), String>,
    manager: Arc<SubscriptionManager>,
    evaluator: EventEvaluator,
    /// Channel dispatchers keyed by FHIR channel-type code.
    dispatchers: ChannelDispatcherRegistry,
    ws_manager: Arc<WebSocketManager>,
    ws_token_manager: Arc<WsBindingTokenManager>,
    config: SubscriptionConfig,
    public_base_url_for_tenant: Arc<dyn Fn(&str) -> String + Send + Sync>,
    outbox: Option<DynSubscriptionOutboxStore>,
    outbox_notify: Arc<tokio::sync::Notify>,
    /// Storage handle used to write a server-driven status transition back into
    /// the stored `Subscription` resource. `None` leaves every transition
    /// in-memory only, which is the pre-#357 behaviour.
    status_store: Option<Arc<dyn ResourceStorage>>,
    /// Bounds how many status write-backs may be in flight at once.
    status_write_slots: Arc<Semaphore>,
    /// Rolling per-subscription delivery counters (#586) — the operator page's
    /// last-24-hours figures. In-memory, like the rest of the engine's state.
    delivery_stats: Arc<crate::delivery_stats::DeliveryStats>,
}

fn calculate_handshake_retry_delay(
    config: &SubscriptionConfig,
    attempt: u32,
) -> std::time::Duration {
    let exponent = attempt.saturating_sub(1) as i32;
    let delay_secs = config.handshake_retry_initial_delay.as_secs_f64()
        * config.retry_backoff_factor.powi(exponent);
    let capped = delay_secs.min(config.handshake_retry_max_delay.as_secs_f64());

    std::time::Duration::from_secs_f64(capped)
}

impl SubscriptionEngine {
    /// Creates a new subscription engine with a no-op outbound auth provider.
    pub fn new(config: SubscriptionConfig, base_url: String) -> Self {
        Self::with_outbound_auth(config, base_url, Arc::new(NoOpOutboundAuthProvider))
    }

    /// Creates a new subscription engine with a custom outbound auth provider.
    ///
    /// The provider is used by channels that initiate outbound HTTP requests
    /// (currently the FHIR Messaging channel) to attach server-side
    /// credentials when the subscription does not supply its own
    /// `Authorization` header.
    pub fn with_outbound_auth(
        config: SubscriptionConfig,
        base_url: String,
        outbound_auth: Arc<dyn OutboundAuthProvider>,
    ) -> Self {
        let resolver: Arc<dyn Fn(&str) -> String + Send + Sync> =
            Arc::new(move |_| base_url.clone());
        Self::with_outbound_auth_and_url_resolver(config, resolver, false, outbound_auth)
    }

    /// Creates an engine whose background notifications resolve a canonical
    /// public base for each subscription tenant.
    pub fn with_outbound_auth_and_url_resolver(
        config: SubscriptionConfig,
        public_base_url_for_tenant: Arc<dyn Fn(&str) -> String + Send + Sync>,
        resolve_default_message_source: bool,
        outbound_auth: Arc<dyn OutboundAuthProvider>,
    ) -> Self {
        let topic_registry = Arc::new(InMemoryTopicRegistry::new());
        let manager = Arc::new(SubscriptionManager::new(
            Arc::clone(&topic_registry),
            config.supported_channel_types.clone(),
        ));
        let evaluator = EventEvaluator::new(Arc::clone(&topic_registry), Arc::clone(&manager));
        let ws_manager = Arc::new(WebSocketManager::new());
        let ws_token_manager = Arc::new(WsBindingTokenManager::new(config.ws_token_lifetime_secs));

        let mut dispatchers = ChannelDispatcherRegistry::new();
        dispatchers.register("rest-hook", Arc::new(RestHookChannel::new()));
        dispatchers.register(
            "websocket",
            Arc::new(WebSocketChannel::new(Arc::clone(&ws_manager))),
        );
        if let Some(settings) = &config.smtp {
            match EmailChannel::new(settings.clone()) {
                Ok(ch) => {
                    dispatchers.register("email", Arc::new(ch));
                }
                Err(e) => {
                    warn!(error = %e, "Failed to initialize email channel; email dispatch disabled");
                }
            }
        }
        if let Some(settings) = &config.messaging {
            let mut channel =
                MessagingChannel::new(settings.source_endpoint.clone(), Arc::clone(&outbound_auth));
            if resolve_default_message_source {
                channel =
                    channel.with_source_endpoint_resolver(Arc::clone(&public_base_url_for_tenant));
            }
            dispatchers.register(
                "message",
                Arc::new(channel.with_private_endpoints_allowed(settings.allow_private_endpoints)),
            );
        }

        let status_write_slots = Arc::new(Semaphore::new(config.status_write_concurrency.max(1)));

        Self {
            topic_registry,
            topic_resource_index: DashMap::new(),
            manager,
            evaluator,
            dispatchers,
            ws_manager,
            ws_token_manager,
            config,
            public_base_url_for_tenant,
            outbox: None,
            outbox_notify: Arc::new(tokio::sync::Notify::new()),
            status_store: None,
            status_write_slots,
            delivery_stats: Arc::new(crate::delivery_stats::DeliveryStats::new()),
        }
    }

    /// Attach a durable outbox store and return a new engine handle.
    ///
    /// Call [`Self::start_outbox_worker`] after wrapping in `Arc`.
    pub fn with_outbox(mut self, store: DynSubscriptionOutboxStore) -> Self {
        self.outbox = Some(store);
        self
    }

    /// Attaches the storage handle that server-driven status transitions are
    /// written back to (issue #357).
    pub fn with_status_store(mut self, storage: Arc<dyn ResourceStorage>) -> Self {
        self.status_store = Some(storage);
        self
    }

    /// Whether server-driven status transitions are written back to storage.
    pub fn persists_status(&self) -> bool {
        self.status_store.is_some() && self.config.persist_status
    }

    /// Register (or replace) a channel dispatcher.
    ///
    /// Use this to plug in broker/custom channels (e.g. Kafka) without changing
    /// the engine match sites. The channel type must also appear in
    /// [`SubscriptionConfig::supported_channel_types`] for subscription
    /// registration to accept it.
    pub fn with_dispatcher(
        mut self,
        channel_type: impl Into<String>,
        dispatcher: Arc<dyn ChannelDispatcher>,
    ) -> Self {
        self.dispatchers.register(channel_type, dispatcher);
        self
    }

    /// Returns the channel dispatcher registry.
    pub fn dispatchers(&self) -> &ChannelDispatcherRegistry {
        &self.dispatchers
    }

    /// Returns the attached outbox store, if any.
    pub fn outbox_store(&self) -> Option<&DynSubscriptionOutboxStore> {
        self.outbox.as_ref()
    }

    /// Spawn the background outbox consumer. No-op when no outbox is attached.
    pub fn start_outbox_worker(self: &Arc<Self>) {
        let Some(store) = self.outbox.clone() else {
            return;
        };
        let engine = Arc::clone(self);
        let config = self.config.clone();
        let notify = Arc::clone(&self.outbox_notify);
        tokio::spawn(async move {
            run_outbox_worker(engine, store, config, notify).await;
        });
        info!("Subscription outbox worker spawned");
    }

    /// Spawn the background heartbeat sender.
    pub fn start_heartbeat_worker(self: &Arc<Self>) {
        let engine = Arc::clone(self);
        let config = self.config.clone();
        tokio::spawn(async move {
            run_heartbeat_worker(engine, config).await;
        });
        info!("Subscription heartbeat worker spawned");
    }

    /// Canonical public base URL for background notifications for `tenant_id`.
    pub fn public_base_url(&self, tenant_id: &str) -> String {
        (self.public_base_url_for_tenant)(tenant_id)
    }

    /// Dispatch a heartbeat notification (used by the heartbeat worker).
    pub(crate) async fn dispatch_heartbeat(
        &self,
        subscription: &ActiveSubscription,
        bundle: &serde_json::Value,
    ) {
        self.dispatch_with_retry(subscription, bundle, "heartbeat", None)
            .await;
    }

    /// Wake subscription processing after a resource write.
    ///
    /// When a durable outbox is attached, SQL backends write the outbox row in
    /// the **same transaction** as the resource. This method only notifies the
    /// outbox worker (no second enqueue). Without an outbox, falls back to
    /// in-process `tokio::spawn` of [`Self::on_resource_event`].
    pub fn enqueue_resource_event(self: &Arc<Self>, event: ResourceEvent) {
        if self.outbox.is_some() {
            debug!(
                resource_type = %event.resource_type,
                resource_id = %event.resource_id,
                event_type = %event.event_type,
                "Notifying outbox worker after same-TX resource write"
            );
            self.outbox_notify.notify_one();
            return;
        }

        let engine = Arc::clone(self);
        tokio::spawn(async move {
            engine.on_resource_event(event).await;
        });
    }

    /// Returns a reference to the topic registry.
    pub fn topic_registry(&self) -> &Arc<InMemoryTopicRegistry> {
        &self.topic_registry
    }

    /// Returns a reference to the subscription manager.
    pub fn manager(&self) -> &Arc<SubscriptionManager> {
        &self.manager
    }

    /// Returns a reference to the WebSocket manager.
    pub fn ws_manager(&self) -> &Arc<WebSocketManager> {
        &self.ws_manager
    }

    /// The rolling delivery counters (#586) — the operator page reads its
    /// last-24-hours figures from here.
    pub fn delivery_stats(&self) -> &Arc<crate::delivery_stats::DeliveryStats> {
        &self.delivery_stats
    }

    /// Returns a reference to the WebSocket binding token manager.
    pub fn ws_token_manager(&self) -> &Arc<WsBindingTokenManager> {
        &self.ws_token_manager
    }

    /// Called after a resource write has been committed.
    ///
    /// This method:
    /// 1. Handles subscription/topic lifecycle events (if the written resource
    ///    is a Subscription, SubscriptionTopic, or an R4 backport Basic topic).
    /// 2. Evaluates the event against all active subscriptions.
    /// 3. Builds and dispatches notifications.
    pub async fn on_resource_event(&self, event: ResourceEvent) {
        // Handle subscription lifecycle events.
        match event.resource_type.as_str() {
            "Subscription" => {
                self.handle_subscription_event(&event).await;
                return;
            }
            "SubscriptionTopic" => {
                self.handle_topic_event(&event).await;
                return;
            }
            "Basic" => {
                if self.handle_r4_basic_topic_event(&event).await {
                    return;
                }
            }
            _ => {}
        }

        // Evaluate which subscriptions match this event.
        let matches = self.evaluator.evaluate(&event);
        info!(
            tenant_id = %event.tenant_id,
            resource_type = %event.resource_type,
            resource_id = %event.resource_id,
            event_type = %event.event_type,
            matched_subscriptions = matches.len(),
            "Subscription event evaluated"
        );
        if matches.is_empty() {
            return;
        }

        debug!(
            resource_type = %event.resource_type,
            resource_id = %event.resource_id,
            matched_subscriptions = matches.len(),
            "Event matched subscriptions"
        );

        // Build and dispatch notifications for each match.
        for eval_match in matches {
            let mut subscription = eval_match.subscription;

            // Increment event counter.
            let event_number = self
                .manager
                .increment_event_count(&subscription.tenant_id, &subscription.id)
                .unwrap_or(0);
            // Ensure notification metadata reflects the event being emitted.
            subscription.events_since_start = event_number;

            let focus_reference = format!("{}/{}", event.resource_type, event.resource_id);
            let event_data = NotificationEventData {
                event_number,
                timestamp: event.timestamp,
                focus_reference: focus_reference.clone(),
            };

            // Build notification bundle.
            let public_base_url = (self.public_base_url_for_tenant)(&subscription.tenant_id);
            let bundle = match notification::build_event_notification(
                &subscription,
                event_data,
                event.resource.as_ref(),
                &public_base_url,
            ) {
                Ok(b) => b,
                Err(e) => {
                    error!(
                        subscription_id = %subscription.id,
                        error = %e,
                        "Failed to build notification"
                    );
                    continue;
                }
            };

            info!(
                tenant_id = %subscription.tenant_id,
                subscription_id = %subscription.id,
                topic_url = %subscription.topic_url,
                channel_type = %subscription.channel.channel_type.as_fhir_str(),
                endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                event_number,
                focus_reference = %focus_reference,
                "Dispatching subscription event notification"
            );

            // Dispatch with retry.
            self.dispatch_with_retry(
                &subscription,
                &bundle,
                "event-notification",
                Some(event_number),
            )
            .await;
        }
    }

    /// Handle a Subscription resource event (create/update/delete).
    async fn handle_subscription_event(&self, event: &ResourceEvent) {
        let tenant_id = event.tenant_id.to_string();
        let subscription_id = &event.resource_id;

        info!(
            tenant_id,
            subscription_id,
            event_type = %event.event_type,
            fhir_version = %event.fhir_version,
            "Handling Subscription resource event"
        );

        match event.event_type {
            ResourceEventType::Delete => {
                let removed = self.manager.deregister(&tenant_id, subscription_id);
                self.ws_manager
                    .remove_all_clients(&tenant_id, subscription_id);
                info!(
                    tenant_id,
                    subscription_id, removed, "Subscription deregistered"
                );
            }
            ResourceEventType::Create | ResourceEventType::Update => {
                if let Some(resource) = &event.resource {
                    // Register (or re-register) the subscription.
                    match self.manager.register(
                        &tenant_id,
                        subscription_id,
                        resource,
                        event.fhir_version,
                    ) {
                        Ok(sub) => {
                            info!(
                                tenant_id = %sub.tenant_id,
                                subscription_id = %sub.id,
                                topic_url = %sub.topic_url,
                                channel_type = %sub.channel.channel_type.as_fhir_str(),
                                endpoint = sub.channel.endpoint.as_deref().unwrap_or(""),
                                status = %sub.status,
                                fhir_version = %sub.fhir_version,
                                "Subscription registered"
                            );
                            // If status is requested, perform handshake and activate.
                            if sub.status == SubscriptionStatusCode::Requested {
                                info!(
                                    tenant_id = %sub.tenant_id,
                                    subscription_id = %sub.id,
                                    channel_type = %sub.channel.channel_type.as_fhir_str(),
                                    endpoint = sub.channel.endpoint.as_deref().unwrap_or(""),
                                    "Subscription activation requested"
                                );
                                self.activate_subscription(&sub).await;
                            }
                        }
                        Err(e) => {
                            warn!(
                                tenant_id,
                                subscription_id,
                                error = %e,
                                "Failed to register subscription"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Handle a SubscriptionTopic resource event.
    async fn handle_topic_event(&self, event: &ResourceEvent) {
        let topic_key = (
            event.tenant_id.to_string(),
            event.resource_type.clone(),
            event.resource_id.clone(),
        );

        match event.event_type {
            ResourceEventType::Delete => {
                let mut candidate_urls = Vec::new();

                if let Some((_, indexed_url)) = self.topic_resource_index.remove(&topic_key) {
                    candidate_urls.push(indexed_url);
                }

                if let Some(resource) = &event.resource {
                    match InMemoryTopicRegistry::parse_topic_resource(resource) {
                        Ok(topic) => candidate_urls.push(topic.canonical_url),
                        Err(e) => {
                            warn!(
                                resource_id = %event.resource_id,
                                error = %e,
                                "Failed to parse SubscriptionTopic delete payload"
                            );
                        }
                    }
                }

                if let Some(previous_resource) = &event.previous_resource {
                    match InMemoryTopicRegistry::parse_topic_resource(previous_resource) {
                        Ok(topic) => candidate_urls.push(topic.canonical_url),
                        Err(e) => {
                            warn!(
                                resource_id = %event.resource_id,
                                error = %e,
                                "Failed to parse previous SubscriptionTopic state"
                            );
                        }
                    }
                }

                candidate_urls.sort();
                candidate_urls.dedup();

                if candidate_urls.is_empty() {
                    warn!(
                        resource_id = %event.resource_id,
                        "SubscriptionTopic deleted but canonical URL could not be resolved"
                    );
                    return;
                }

                let tenant_id = event.tenant_id.as_str();
                for canonical_url in candidate_urls {
                    let removed = self.topic_registry.remove_topic(tenant_id, &canonical_url);
                    info!(
                        tenant = %tenant_id,
                        resource_id = %event.resource_id,
                        topic_url = %canonical_url,
                        removed,
                        "SubscriptionTopic deleted"
                    );
                }
            }
            ResourceEventType::Create | ResourceEventType::Update => {
                if let Some(resource) = &event.resource {
                    match InMemoryTopicRegistry::parse_topic_resource(resource) {
                        Ok(topic) => {
                            let tenant_id = event.tenant_id.as_str();
                            let canonical_url = topic.canonical_url.clone();
                            if let Some(previous_url) = self
                                .topic_resource_index
                                .insert(topic_key, canonical_url.clone())
                                .filter(|previous_url| previous_url != &canonical_url)
                            {
                                let _ = self.topic_registry.remove_topic(tenant_id, &previous_url);
                            }
                            info!(
                                tenant = %tenant_id,
                                topic_url = %canonical_url,
                                "Registered SubscriptionTopic"
                            );
                            self.topic_registry.add_topic(tenant_id, topic);
                        }
                        Err(e) => {
                            warn!(
                                resource_id = %event.resource_id,
                                error = %e,
                                "Failed to parse SubscriptionTopic"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Handle an R4 backport `Basic` topic event.
    ///
    /// Returns `true` when the `Basic` resource was recognized as a topic lifecycle
    /// event (including malformed topic candidates), `false` otherwise.
    async fn handle_r4_basic_topic_event(&self, event: &ResourceEvent) -> bool {
        if event.fhir_version.as_str() != "R4" {
            return false;
        }

        let topic_key = (
            event.tenant_id.to_string(),
            event.resource_type.clone(),
            event.resource_id.clone(),
        );

        match event.event_type {
            ResourceEventType::Delete => {
                let mut candidate_urls = Vec::new();
                let mut recognized_topic = false;

                if let Some((_, indexed_url)) = self.topic_resource_index.remove(&topic_key) {
                    candidate_urls.push(indexed_url);
                    recognized_topic = true;
                }

                if let Some(resource) = &event.resource {
                    match InMemoryTopicRegistry::parse_r4_backport_basic_topic_resource(resource) {
                        Ok(Some(topic)) => {
                            candidate_urls.push(topic.canonical_url);
                            recognized_topic = true;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!(
                                resource_id = %event.resource_id,
                                error = %e,
                                "Failed to parse R4 Basic SubscriptionTopic delete payload"
                            );
                            recognized_topic = true;
                        }
                    }
                }

                if let Some(previous_resource) = &event.previous_resource {
                    match InMemoryTopicRegistry::parse_r4_backport_basic_topic_resource(
                        previous_resource,
                    ) {
                        Ok(Some(topic)) => {
                            candidate_urls.push(topic.canonical_url);
                            recognized_topic = true;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!(
                                resource_id = %event.resource_id,
                                error = %e,
                                "Failed to parse previous R4 Basic SubscriptionTopic state"
                            );
                            recognized_topic = true;
                        }
                    }
                }

                candidate_urls.sort();
                candidate_urls.dedup();

                let tenant_id = event.tenant_id.as_str();
                for canonical_url in candidate_urls {
                    let removed = self.topic_registry.remove_topic(tenant_id, &canonical_url);
                    info!(
                        tenant = %tenant_id,
                        resource_id = %event.resource_id,
                        topic_url = %canonical_url,
                        removed,
                        "R4 Basic SubscriptionTopic deleted"
                    );
                }

                recognized_topic
            }
            ResourceEventType::Create | ResourceEventType::Update => {
                if let Some(resource) = &event.resource {
                    match InMemoryTopicRegistry::parse_r4_backport_basic_topic_resource(resource) {
                        Ok(Some(topic)) => {
                            let tenant_id = event.tenant_id.as_str();
                            let canonical_url = topic.canonical_url.clone();
                            if let Some(previous_url) = self
                                .topic_resource_index
                                .insert(topic_key, canonical_url.clone())
                                .filter(|previous_url| previous_url != &canonical_url)
                            {
                                let _ = self.topic_registry.remove_topic(tenant_id, &previous_url);
                            }
                            info!(
                                tenant = %tenant_id,
                                topic_url = %canonical_url,
                                "Registered R4 Basic SubscriptionTopic"
                            );
                            self.topic_registry.add_topic(tenant_id, topic);
                            true
                        }
                        Ok(None) => false,
                        Err(e) => {
                            warn!(
                                resource_id = %event.resource_id,
                                error = %e,
                                "Failed to parse R4 Basic SubscriptionTopic"
                            );
                            true
                        }
                    }
                } else {
                    false
                }
            }
        }
    }

    /// Apply an in-memory status transition and, when configured, write it back.
    async fn transition_status(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        new_status: SubscriptionStatusCode,
    ) -> bool {
        match self
            .manager
            .update_status(tenant_id, subscription_id, new_status)
        {
            Ok(old) => {
                info!(
                    tenant_id,
                    subscription_id,
                    from = %old,
                    to = %new_status,
                    "Subscription status transitioned"
                );
            }
            Err(e) => {
                warn!(
                    tenant_id,
                    subscription_id,
                    to = %new_status,
                    error = %e,
                    "Failed to update in-memory subscription status"
                );
                return false;
            }
        }
        self.write_status_back(tenant_id, subscription_id, new_status)
            .await;
        true
    }

    /// Writes a server-driven status transition into the stored `Subscription`.
    ///
    /// Fails **open**, always. The in-memory transition has already been applied.
    async fn write_status_back(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        new_status: SubscriptionStatusCode,
    ) {
        if !self.config.persist_status {
            return;
        }
        let Some(storage) = self.status_store.as_ref() else {
            return;
        };

        let Ok(_permit) = self.status_write_slots.acquire().await else {
            return;
        };

        let tenant = TenantContext::new(TenantId::new(tenant_id), TenantPermissions::full_access());
        let timeout = self.config.status_write_timeout;

        let result = tokio::time::timeout(
            timeout,
            persist_status(storage.as_ref(), &tenant, subscription_id, new_status),
        )
        .await;

        match result {
            Ok(Ok(PersistOutcome::Written)) => debug!(
                tenant_id,
                subscription_id,
                status = %new_status,
                "Persisted subscription status transition"
            ),
            Ok(Ok(PersistOutcome::AlreadyCurrent)) => debug!(
                tenant_id,
                subscription_id,
                status = %new_status,
                "Subscription status already current in storage"
            ),
            Ok(Ok(PersistOutcome::Vanished)) => debug!(
                tenant_id,
                subscription_id, "Subscription resource is gone; nothing to persist status onto"
            ),
            Ok(Ok(PersistOutcome::RacedClientWrite)) => debug!(
                tenant_id,
                subscription_id, "Dropped status write-back after client raced the version"
            ),
            Ok(Err(e)) => error!(
                tenant_id,
                subscription_id,
                status = %new_status,
                error = %e,
                "Failed to persist subscription status transition;                  in-memory status kept (fail-open)"
            ),
            Err(_) => error!(
                tenant_id,
                subscription_id,
                status = %new_status,
                timeout_ms = timeout.as_millis(),
                "Timed out persisting subscription status transition;                  in-memory status kept (fail-open)"
            ),
        }
    }

    /// Activate a subscription by performing the handshake.
    ///
    /// `pub(crate)` so startup rehydration ([`crate::rehydrate`]) drives the
    /// *same* activation path as the write handler rather than reimplementing
    /// the handshake, retry, and status-transition logic.
    pub(crate) async fn activate_subscription(&self, subscription: &ActiveSubscription) {
        let tenant_id = &subscription.tenant_id;
        let sub_id = &subscription.id;
        let handshake_max_attempts = self.config.handshake_max_attempts.max(1);

        // Build handshake notification.
        let public_base_url = (self.public_base_url_for_tenant)(&subscription.tenant_id);
        let handshake_bundle = match notification::build_handshake(subscription, &public_base_url) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    tenant_id,
                    subscription_id = sub_id,
                    channel_type = %subscription.channel.channel_type.as_fhir_str(),
                    endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                    error = %e,
                    "Failed to build handshake"
                );
                self.transition_status(tenant_id, sub_id, SubscriptionStatusCode::Error)
                    .await;
                return;
            }
        };

        if !self.config.handshake_initial_delay.is_zero() {
            info!(
                tenant_id,
                subscription_id = sub_id,
                channel_type = %subscription.channel.channel_type.as_fhir_str(),
                endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                delay_ms = self.config.handshake_initial_delay.as_millis() as u64,
                "Delaying subscription handshake"
            );
            tokio::time::sleep(self.config.handshake_initial_delay).await;
        }

        let mut attempt = 1;
        loop {
            info!(
                tenant_id,
                subscription_id = sub_id,
                channel_type = %subscription.channel.channel_type.as_fhir_str(),
                endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                attempt,
                max_attempts = handshake_max_attempts,
                "Sending subscription handshake"
            );

            // Perform handshake via the registered dispatcher for this channel type.
            let Some(dispatcher) = self.dispatchers.get(&subscription.channel.channel_type) else {
                warn!(
                    tenant_id,
                    subscription_id = sub_id,
                    channel_type = subscription.channel.channel_type.as_fhir_str(),
                    endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                    "No dispatcher registered for channel type"
                );
                self.transition_status(tenant_id, sub_id, SubscriptionStatusCode::Error)
                    .await;
                return;
            };
            let result = dispatcher.handshake(subscription, &handshake_bundle).await;

            match result {
                Ok(DispatchResult::Success) => {
                    info!(
                        tenant_id,
                        subscription_id = sub_id,
                        channel_type = %subscription.channel.channel_type.as_fhir_str(),
                        endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                        attempt,
                        "Handshake successful, activating subscription"
                    );
                    self.transition_status(tenant_id, sub_id, SubscriptionStatusCode::Active)
                        .await;
                    return;
                }
                Ok(DispatchResult::PermanentError(msg)) => {
                    warn!(
                        tenant_id,
                        subscription_id = sub_id,
                        channel_type = %subscription.channel.channel_type.as_fhir_str(),
                        endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                        attempt,
                        error = %msg,
                        "Handshake failed with permanent error"
                    );
                    self.transition_status(tenant_id, sub_id, SubscriptionStatusCode::Error)
                        .await;
                    return;
                }
                Ok(DispatchResult::RetryableError(msg)) => {
                    if attempt >= handshake_max_attempts {
                        warn!(
                            tenant_id,
                            subscription_id = sub_id,
                            channel_type = %subscription.channel.channel_type.as_fhir_str(),
                            endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                            attempts = attempt,
                            error = %msg,
                            "Handshake retries exhausted"
                        );
                        self.transition_status(tenant_id, sub_id, SubscriptionStatusCode::Error)
                            .await;
                        return;
                    }

                    let delay = calculate_handshake_retry_delay(&self.config, attempt);
                    warn!(
                        tenant_id,
                        subscription_id = sub_id,
                        channel_type = %subscription.channel.channel_type.as_fhir_str(),
                        endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                        attempt,
                        next_attempt = attempt + 1,
                        delay_ms = delay.as_millis() as u64,
                        error = %msg,
                        "Retrying subscription handshake"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(e) => {
                    warn!(
                        tenant_id,
                        subscription_id = sub_id,
                        channel_type = %subscription.channel.channel_type.as_fhir_str(),
                        endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                        attempt,
                        error = %e,
                        "Handshake error"
                    );
                    self.transition_status(tenant_id, sub_id, SubscriptionStatusCode::Error)
                        .await;
                    return;
                }
            }
        }
    }

    /// Dispatch a notification with retry logic.
    async fn dispatch_with_retry(
        &self,
        subscription: &ActiveSubscription,
        bundle: &serde_json::Value,
        notification_type: &'static str,
        event_number: Option<u64>,
    ) {
        let tenant_id = &subscription.tenant_id;
        let sub_id = &subscription.id;

        let Some(dispatcher) = self.dispatchers.get(&subscription.channel.channel_type) else {
            warn!(
                tenant_id,
                subscription_id = sub_id,
                notification_type,
                channel = subscription.channel.channel_type.as_fhir_str(),
                endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                "No dispatcher registered for channel type"
            );
            return;
        };

        let mut attempt: u32 = 0;
        loop {
            let now = chrono::Utc::now().timestamp();
            match dispatcher.dispatch(subscription, bundle).await {
                Ok(DispatchResult::Success) => {
                    self.delivery_stats
                        .record_success(tenant_id, sub_id, attempt == 0, now);
                    self.manager.reset_failures(tenant_id, sub_id);
                    info!(
                        tenant_id,
                        subscription_id = sub_id,
                        notification_type,
                        event_number,
                        channel_type = %subscription.channel.channel_type.as_fhir_str(),
                        endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                        "Subscription notification dispatched"
                    );
                    return;
                }
                Ok(DispatchResult::PermanentError(msg)) => {
                    warn!(
                        tenant_id,
                        subscription_id = sub_id,
                        notification_type,
                        event_number,
                        channel_type = %subscription.channel.channel_type.as_fhir_str(),
                        endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                        error = %msg,
                        "Permanent delivery error"
                    );
                    self.delivery_stats.record_failure(tenant_id, sub_id, now);
                    self.handle_delivery_failure(tenant_id, sub_id).await;
                    return;
                }
                Ok(DispatchResult::RetryableError(msg)) => {
                    attempt += 1;
                    if !retry::should_retry(&self.config, attempt) {
                        warn!(
                            tenant_id,
                            subscription_id = sub_id,
                            notification_type,
                            event_number,
                            channel_type = %subscription.channel.channel_type.as_fhir_str(),
                            endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                            attempts = attempt,
                            error = %msg,
                            "Max retries exhausted"
                        );
                        self.delivery_stats.record_failure(tenant_id, sub_id, now);
                        self.handle_delivery_failure(tenant_id, sub_id).await;
                        return;
                    }

                    let delay = retry::calculate_delay(&self.config, attempt);
                    debug!(
                        tenant_id,
                        subscription_id = sub_id,
                        notification_type,
                        event_number,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        "Retrying delivery"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    error!(
                        tenant_id,
                        subscription_id = sub_id,
                        notification_type,
                        event_number,
                        channel_type = %subscription.channel.channel_type.as_fhir_str(),
                        endpoint = subscription.channel.endpoint.as_deref().unwrap_or(""),
                        error = %e,
                        "Dispatch error"
                    );
                    self.delivery_stats.record_failure(tenant_id, sub_id, now);
                    self.handle_delivery_failure(tenant_id, sub_id).await;
                    return;
                }
            }
        }
    }

    /// Handle a delivery failure: increment failure count and potentially
    /// transition status to error or off.
    async fn handle_delivery_failure(&self, tenant_id: &str, subscription_id: &str) {
        let Some(failure_count) = self.manager.record_failure(tenant_id, subscription_id) else {
            return;
        };
        if failure_count >= self.config.off_threshold {
            warn!(
                tenant_id,
                subscription_id,
                failures = failure_count,
                threshold = self.config.off_threshold,
                "Turning off subscription after repeated failures"
            );
            self.transition_status(tenant_id, subscription_id, SubscriptionStatusCode::Off)
                .await;
        } else if failure_count >= self.config.error_threshold {
            self.transition_status(tenant_id, subscription_id, SubscriptionStatusCode::Error)
                .await;
        }
    }
}

/// What [`persist_status`] did, so the caller can log it at the right level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistOutcome {
    /// The stored resource was rewritten with the new status.
    Written,
    /// The stored resource already carried this status — no write issued.
    AlreadyCurrent,
    /// The resource no longer exists (deleted between the transition and the write).
    Vanished,
    /// A concurrent client write bumped the version first. Dropped, not retried.
    RacedClientWrite,
}

/// Reads the stored `Subscription`, replaces its `status`, and writes it back.
async fn persist_status(
    storage: &dyn ResourceStorage,
    tenant: &TenantContext,
    subscription_id: &str,
    new_status: SubscriptionStatusCode,
) -> Result<PersistOutcome, StorageError> {
    let current = match storage.read(tenant, "Subscription", subscription_id).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return Ok(PersistOutcome::Vanished),
        Err(StorageError::Resource(_)) => return Ok(PersistOutcome::Vanished),
        Err(e) => return Err(e),
    };

    if current.content().get("status").and_then(|v| v.as_str()) == Some(new_status.as_fhir_str()) {
        return Ok(PersistOutcome::AlreadyCurrent);
    }

    let mut updated = current.content().clone();
    let Some(object) = updated.as_object_mut() else {
        return Ok(PersistOutcome::Vanished);
    };
    object.insert(
        "status".to_string(),
        serde_json::Value::String(new_status.as_fhir_str().to_string()),
    );

    match storage.update(tenant, &current, updated).await {
        Ok(_) => Ok(PersistOutcome::Written),
        Err(StorageError::Concurrency(_)) => Ok(PersistOutcome::RacedClientWrite),
        Err(StorageError::Resource(_)) => Ok(PersistOutcome::Vanished),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ResourceEventType;
    use crate::topics::{ResourceTrigger, TopicDefinition};
    use chrono::Utc;
    use helios_fhir::FhirVersion;
    use helios_persistence::tenant::TenantId;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    fn make_engine(base_url: &str) -> SubscriptionEngine {
        let config = SubscriptionConfig {
            max_retries: 2,
            error_threshold: 2,
            off_threshold: 4,
            ..Default::default()
        };
        SubscriptionEngine::new(config, base_url.to_string())
    }

    #[test]
    fn canonical_public_base_resolves_for_background_tenant() {
        let resolver: Arc<dyn Fn(&str) -> String + Send + Sync> =
            Arc::new(|tenant| format!("https://public.example/fhir/{tenant}"));
        let engine = SubscriptionEngine::with_outbound_auth_and_url_resolver(
            SubscriptionConfig::default(),
            resolver,
            false,
            Arc::new(NoOpOutboundAuthProvider),
        );
        assert_eq!(
            (engine.public_base_url_for_tenant)("acme"),
            "https://public.example/fhir/acme"
        );

        let config = SubscriptionConfig {
            messaging: Some(crate::config::MessagingSettings {
                source_endpoint: "https://fallback.example/fhir".to_string(),
                allow_private_endpoints: false,
            }),
            ..Default::default()
        };
        let resolver: Arc<dyn Fn(&str) -> String + Send + Sync> =
            Arc::new(|tenant| format!("https://public.example/fhir/{tenant}"));
        let engine = SubscriptionEngine::with_outbound_auth_and_url_resolver(
            config,
            resolver,
            true,
            Arc::new(NoOpOutboundAuthProvider),
        );
        assert!(
            engine
                .dispatchers()
                .contains(&crate::manager::ChannelType::Message)
        );
    }

    fn encounter_topic() -> TopicDefinition {
        TopicDefinition {
            canonical_url: "http://example.org/topic/encounter-start".to_string(),
            title: Some("Encounter Start".to_string()),
            resource_triggers: vec![ResourceTrigger {
                resource_type: "Encounter".to_string(),
                interactions: vec![ResourceEventType::Create],
                fhirpath_criteria: None,
            }],
            can_filter_by: vec![],
            notification_shape: vec![],
        }
    }

    fn encounter_event() -> ResourceEvent {
        ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: "Encounter".to_string(),
            resource_id: "enc-1".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(json!({
                "resourceType": "Encounter",
                "id": "enc-1",
                "status": "in-progress"
            })),
            previous_resource: None,
            timestamp: Utc::now(),
        }
    }

    #[derive(Clone)]
    struct FailFirstHandshake {
        attempts: Arc<AtomicUsize>,
    }

    impl Respond for FailFirstHandshake {
        fn respond(&self, _request: &Request) -> ResponseTemplate {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(500)
            } else {
                ResponseTemplate::new(200)
            }
        }
    }

    fn topic_event() -> ResourceEvent {
        ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: "SubscriptionTopic".to_string(),
            resource_id: "topic-1".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(json!({
                "resourceType": "SubscriptionTopic",
                "id": "topic-1",
                "url": "http://example.org/topic/encounter-start",
                "resourceTrigger": [{
                    "resource": "Encounter",
                    "supportedInteraction": ["create"]
                }]
            })),
            previous_resource: None,
            timestamp: Utc::now(),
        }
    }

    #[cfg(feature = "R4")]
    fn r4_basic_topic_event() -> ResourceEvent {
        ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::R4,
            resource_type: "Basic".to_string(),
            resource_id: "topic-basic-1".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(json!({
                "resourceType": "Basic",
                "id": "topic-basic-1",
                "code": {
                    "coding": [{
                        "system": "http://hl7.org/fhir/fhir-types",
                        "code": "SubscriptionTopic"
                    }]
                },
                "extension": [{
                    "url": "http://hl7.org/fhir/5.0/StructureDefinition/extension-SubscriptionTopic.url",
                    "valueUri": "http://example.org/topic/encounter-start-basic"
                }, {
                    "url": "http://hl7.org/fhir/4.3/StructureDefinition/extension-SubscriptionTopic.resourceTrigger",
                    "extension": [
                        { "url": "resource", "valueUri": "http://hl7.org/fhir/StructureDefinition/Encounter" },
                        { "url": "supportedInteraction", "valueCode": "create" }
                    ]
                }]
            })),
            previous_resource: None,
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_topic_event_registers_topic() {
        let engine = make_engine("http://localhost:8080");

        assert!(engine.topic_registry().list_topics("t1").is_empty());

        engine.on_resource_event(topic_event()).await;

        let topics = engine.topic_registry().list_topics("t1");
        assert_eq!(topics.len(), 1);
        assert!(topics.contains(&"http://example.org/topic/encounter-start".to_string()));
    }

    #[tokio::test]
    async fn test_topic_delete_event_removes_topic_without_payload() {
        let engine = make_engine("http://localhost:8080");
        engine.on_resource_event(topic_event()).await;

        let delete_event = ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: "SubscriptionTopic".to_string(),
            resource_id: "topic-1".to_string(),
            version_id: "2".to_string(),
            event_type: ResourceEventType::Delete,
            resource: None,
            previous_resource: None,
            timestamp: Utc::now(),
        };

        engine.on_resource_event(delete_event).await;

        let topics = engine.topic_registry().list_topics("t1");
        assert!(topics.is_empty());
    }

    #[cfg(feature = "R4")]
    #[tokio::test]
    async fn test_r4_basic_topic_event_registers_topic() {
        let engine = make_engine("http://localhost:8080");

        assert!(engine.topic_registry().list_topics("t1").is_empty());

        engine.on_resource_event(r4_basic_topic_event()).await;

        let topics = engine.topic_registry().list_topics("t1");
        assert_eq!(topics.len(), 1);
        assert!(topics.contains(&"http://example.org/topic/encounter-start-basic".to_string()));
    }

    #[tokio::test]
    async fn test_subscription_event_registers_subscription() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let engine = make_engine("http://localhost:8080");
        engine.topic_registry().add_topic("t1", encounter_topic());

        // Create a subscription event.
        let sub_resource = crate::manager::tests::build_subscription_json(
            "http://example.org/topic/encounter-start",
            "rest-hook",
            Some(&format!("{}/webhook", server.uri())),
        );

        let event = ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: "Subscription".to_string(),
            resource_id: "sub-1".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(sub_resource),
            previous_resource: None,
            timestamp: Utc::now(),
        };

        engine.on_resource_event(event).await;

        // Subscription should be registered.
        let sub = engine.manager().get_subscription("t1", "sub-1");
        assert!(sub.is_some());
    }

    #[tokio::test]
    async fn test_subscription_activation_retries_retryable_handshake_failure() {
        let server = MockServer::start().await;
        let attempts = Arc::new(AtomicUsize::new(0));

        Mock::given(method("POST"))
            .respond_with(FailFirstHandshake {
                attempts: Arc::clone(&attempts),
            })
            .mount(&server)
            .await;

        let config = SubscriptionConfig {
            handshake_max_attempts: 2,
            handshake_retry_initial_delay: std::time::Duration::from_millis(1),
            handshake_retry_max_delay: std::time::Duration::from_millis(1),
            ..Default::default()
        };
        let engine = SubscriptionEngine::new(config, "http://localhost:8080".to_string());
        engine.topic_registry().add_topic("t1", encounter_topic());

        let sub_resource = crate::manager::tests::build_subscription_json(
            "http://example.org/topic/encounter-start",
            "rest-hook",
            Some(&format!("{}/webhook", server.uri())),
        );

        let event = ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: "Subscription".to_string(),
            resource_id: "sub-1".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(sub_resource),
            previous_resource: None,
            timestamp: Utc::now(),
        };

        engine.on_resource_event(event).await;

        let sub = engine.manager().get_subscription("t1", "sub-1").unwrap();
        assert_eq!(sub.status, SubscriptionStatusCode::Active);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_full_pipeline_event_to_dispatch() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(2) // handshake + event notification
            .mount(&server)
            .await;

        let engine = make_engine("http://localhost:8080");
        engine.topic_registry().add_topic("t1", encounter_topic());

        // Register and activate a subscription.
        let sub_resource = crate::manager::tests::build_subscription_json(
            "http://example.org/topic/encounter-start",
            "rest-hook",
            Some(&format!("{}/webhook", server.uri())),
        );

        let sub_event = ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: "Subscription".to_string(),
            resource_id: "sub-1".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(sub_resource),
            previous_resource: None,
            timestamp: Utc::now(),
        };

        engine.on_resource_event(sub_event).await;

        // Fire a matching event.
        engine.on_resource_event(encounter_event()).await;

        // Verify the mock received the requests.
        // (wiremock's expect(2) will panic on drop if not satisfied)
    }

    #[tokio::test]
    async fn test_no_dispatch_when_no_matching_subscriptions() {
        let engine = make_engine("http://localhost:8080");
        engine.topic_registry().add_topic("t1", encounter_topic());

        // Fire an event with no subscriptions registered.
        engine.on_resource_event(encounter_event()).await;
        // Should complete without error.
    }

    #[tokio::test]
    async fn test_no_dispatch_for_non_matching_resource_type() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1) // Only the handshake
            .mount(&server)
            .await;

        let engine = make_engine("http://localhost:8080");
        engine.topic_registry().add_topic("t1", encounter_topic());

        // Register subscription for Encounter topic.
        let sub_resource = crate::manager::tests::build_subscription_json(
            "http://example.org/topic/encounter-start",
            "rest-hook",
            Some(&format!("{}/webhook", server.uri())),
        );

        let sub_event = ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: "Subscription".to_string(),
            resource_id: "sub-1".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(sub_resource),
            previous_resource: None,
            timestamp: Utc::now(),
        };

        engine.on_resource_event(sub_event).await;

        // Fire a Patient event (doesn't match Encounter topic).
        let patient_event = ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: "Patient".to_string(),
            resource_id: "pat-1".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(json!({"resourceType": "Patient", "id": "pat-1"})),
            previous_resource: None,
            timestamp: Utc::now(),
        };

        engine.on_resource_event(patient_event).await;
        // Only 1 mock call (handshake), not 2.
    }

    #[tokio::test]
    async fn test_delivery_failure_updates_status() {
        let server = MockServer::start().await;

        // First request (handshake) succeeds, subsequent fail.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let config = SubscriptionConfig {
            max_retries: 1,
            error_threshold: 1,
            off_threshold: 3,
            ..Default::default()
        };
        let engine = SubscriptionEngine::new(config, "http://localhost:8080".to_string());
        engine.topic_registry().add_topic("t1", encounter_topic());

        // Register subscription.
        let sub_resource = crate::manager::tests::build_subscription_json(
            "http://example.org/topic/encounter-start",
            "rest-hook",
            Some(&format!("{}/webhook", server.uri())),
        );

        let sub_event = ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: "Subscription".to_string(),
            resource_id: "sub-1".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(sub_resource),
            previous_resource: None,
            timestamp: Utc::now(),
        };

        engine.on_resource_event(sub_event).await;

        // Fire a matching event (will fail delivery).
        engine.on_resource_event(encounter_event()).await;

        // Check subscription status changed to error.
        let sub = engine.manager().get_subscription("t1", "sub-1").unwrap();
        assert_eq!(sub.status, SubscriptionStatusCode::Error);
    }

    #[tokio::test]
    async fn test_subscription_delete_event() {
        let engine = make_engine("http://localhost:8080");
        engine.topic_registry().add_topic("t1", encounter_topic());

        // Register directly.
        let resource = crate::manager::tests::default_subscription_json();
        engine
            .manager()
            .register("t1", "sub-1", &resource, FhirVersion::default())
            .unwrap();

        assert!(engine.manager().get_subscription("t1", "sub-1").is_some());

        // Delete event.
        let delete_event = ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: "Subscription".to_string(),
            resource_id: "sub-1".to_string(),
            version_id: "2".to_string(),
            event_type: ResourceEventType::Delete,
            resource: None,
            previous_resource: None,
            timestamp: Utc::now(),
        };

        engine.on_resource_event(delete_event).await;

        assert!(engine.manager().get_subscription("t1", "sub-1").is_none());
    }

    #[tokio::test]
    async fn test_tenant_isolation() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(2) // 1 handshake + 1 event notification (only tenant-a)
            .mount(&server)
            .await;

        let engine = make_engine("http://localhost:8080");
        engine
            .topic_registry()
            .add_topic("tenant-a", encounter_topic());

        // Register subscription for tenant-a.
        let sub_resource = crate::manager::tests::build_subscription_json(
            "http://example.org/topic/encounter-start",
            "rest-hook",
            Some(&format!("{}/webhook", server.uri())),
        );

        let sub_event_a = ResourceEvent {
            tenant_id: TenantId::new("tenant-a"),
            fhir_version: FhirVersion::default(),
            resource_type: "Subscription".to_string(),
            resource_id: "sub-a".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(sub_resource),
            previous_resource: None,
            timestamp: Utc::now(),
        };

        engine.on_resource_event(sub_event_a).await;

        // Fire event from tenant-a (should match).
        let mut event_a = encounter_event();
        event_a.tenant_id = TenantId::new("tenant-a");
        engine.on_resource_event(event_a).await;

        // Fire event from tenant-b (should NOT match).
        let mut event_b = encounter_event();
        event_b.tenant_id = TenantId::new("tenant-b");
        engine.on_resource_event(event_b).await;

        // wiremock expects exactly 2 calls (handshake + 1 notification).
    }
}
