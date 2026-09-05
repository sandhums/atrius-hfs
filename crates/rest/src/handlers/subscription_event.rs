//! Subscription event emission helper.
//!
//! Constructs a `ResourceEvent` from handler context and enqueues it on the
//! subscription engine (durable outbox when available).

use std::sync::Arc;

use helios_fhir::FhirVersion;
use helios_persistence::tenant::TenantContext;
use helios_persistence::types::StoredResource;
use helios_subscriptions::{ResourceEvent, ResourceEventType, SubscriptionEngine};
use tracing::debug;

/// Emits a subscription event for a successful resource write.
///
/// Constructs a `ResourceEvent` and enqueues it on the subscription engine.
/// No-op if the subscription engine is not configured.
pub fn emit_subscription_event(
    engine: &Arc<SubscriptionEngine>,
    tenant: &TenantContext,
    stored: &StoredResource,
    fhir_version: FhirVersion,
    event_type: ResourceEventType,
) {
    let event = ResourceEvent {
        tenant_id: tenant.tenant_id().clone(),
        fhir_version,
        resource_type: stored.resource_type().to_string(),
        resource_id: stored.id().to_string(),
        version_id: stored.version_id().to_string(),
        event_type,
        resource: Some(stored.content_with_meta()),
        previous_resource: None,
        timestamp: chrono::Utc::now(),
    };

    debug!(
        resource_type = %event.resource_type,
        resource_id = %event.resource_id,
        event_type = %event.event_type,
        "Emitting subscription event"
    );

    engine.enqueue_resource_event(event);
}

/// Emits a subscription event for a successful bundle write entry (batch/transaction).
///
/// Reconstructs enough of a [`StoredResource`] from the response payload to feed
/// the normal emit path. Deletes use [`emit_delete_event`].
pub fn emit_bundle_write_event(
    engine: &Arc<SubscriptionEngine>,
    tenant: &TenantContext,
    fhir_version: FhirVersion,
    method: &str,
    resource_type: &str,
    resource_id: &str,
    resource: Option<&serde_json::Value>,
    created: bool,
) {
    match method {
        "POST" | "PUT" | "PATCH" => {
            let Some(content) = resource else {
                return;
            };
            let version_id = content
                .pointer("/meta/versionId")
                .and_then(|v| v.as_str())
                .unwrap_or("1");
            let stored = helios_persistence::types::StoredResource::from_storage(
                resource_type,
                resource_id,
                version_id,
                tenant.tenant_id().clone(),
                content.clone(),
                chrono::Utc::now(),
                chrono::Utc::now(),
                None,
                fhir_version,
            );
            let event_type = if method == "POST" || created {
                ResourceEventType::Create
            } else {
                ResourceEventType::Update
            };
            emit_subscription_event(engine, tenant, &stored, fhir_version, event_type);
        }
        "DELETE" => {
            emit_delete_event(
                engine,
                tenant,
                resource_type,
                resource_id,
                fhir_version,
                None,
            );
        }
        _ => {}
    }
}

/// Emits a subscription event for a resource delete.
///
/// Delete events carry the resource type and ID but no resource content.
pub fn emit_delete_event(
    engine: &Arc<SubscriptionEngine>,
    tenant: &TenantContext,
    resource_type: &str,
    resource_id: &str,
    fhir_version: FhirVersion,
    previous_resource: Option<serde_json::Value>,
) {
    let event = ResourceEvent {
        tenant_id: tenant.tenant_id().clone(),
        fhir_version,
        resource_type: resource_type.to_string(),
        resource_id: resource_id.to_string(),
        version_id: String::new(),
        event_type: ResourceEventType::Delete,
        resource: None,
        previous_resource,
        timestamp: chrono::Utc::now(),
    };

    debug!(
        resource_type = %event.resource_type,
        resource_id = %event.resource_id,
        "Emitting subscription delete event"
    );

    engine.enqueue_resource_event(event);
}
