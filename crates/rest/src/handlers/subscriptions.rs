//! Subscription operation handlers ($status, $events).
//!
//! Implements custom FHIR operations for subscription management:
//! - `GET /Subscription/{id}/$status` — returns the current `SubscriptionStatus`
//! - `GET /Subscription/{id}/$events` — returns recent events from the durable outbox

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use helios_persistence::core::ResourceStorage;
use helios_persistence::tenant::TenantId;
use serde::Deserialize;
use serde_json::json;

use crate::error::{RestError, RestResult};
use crate::extractors::TenantExtractor;
use crate::state::AppState;

/// Handler for the `$status` operation on Subscription resources.
///
/// Returns a `SubscriptionStatus` (R4B/R5/R6) or `Parameters` (R4 backport)
/// resource reflecting the subscription's current runtime state.
///
/// # HTTP Request
///
/// `GET [base]/Subscription/{id}/$status`
pub async fn subscription_status_handler<S>(
    State(state): State<AppState<S>>,
    Path((_resource_type, id)): Path<(String, String)>,
    tenant: TenantExtractor,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    let engine = state
        .subscription_engine()
        .ok_or(RestError::NotImplemented {
            feature: "Subscriptions".to_string(),
        })?;

    let sub = engine
        .manager()
        .get_subscription(tenant.tenant_id(), &id)
        .ok_or(RestError::NotFound {
            resource_type: "Subscription".to_string(),
            id: id.clone(),
        })?;

    let status_resource = build_subscription_status(&sub, &id, state.base_url());

    Ok((StatusCode::OK, Json(status_resource)).into_response())
}

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    /// Exclusive outbox cursor (resume after this row id).
    pub after: Option<i64>,
    /// Max events to return (default 50, max 200).
    pub count: Option<u32>,
}

/// Handler for the `$events` operation on Subscription resources.
///
/// When a durable outbox is available, returns recent processed resource-change
/// events for the tenant as notification-style Bundle entries. Without an outbox,
/// returns a query-status Bundle with the in-memory event counter only.
///
/// # HTTP Request
///
/// `GET [base]/Subscription/{id}/$events`
pub async fn subscription_events_handler<S>(
    State(state): State<AppState<S>>,
    Path((_resource_type, id)): Path<(String, String)>,
    Query(query): Query<EventsQuery>,
    tenant: TenantExtractor,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    let engine = state
        .subscription_engine()
        .ok_or(RestError::NotImplemented {
            feature: "Subscriptions".to_string(),
        })?;

    let sub = engine
        .manager()
        .get_subscription(tenant.tenant_id(), &id)
        .ok_or(RestError::NotFound {
            resource_type: "Subscription".to_string(),
            id: id.clone(),
        })?;

    let bundle_type = sub.fhir_version.notification_bundle_type();
    let mut entries = vec![json!({
        "resource": {
            "resourceType": "SubscriptionStatus",
            "status": sub.status.as_fhir_str(),
            "type": "query-event",
            "eventsSinceSubscriptionStart": sub.events_since_start,
            "subscription": {
                "reference": format!("Subscription/{}", id)
            },
            "topic": sub.topic_url
        }
    })];

    if let Some(outbox) = engine.outbox_store() {
        let limit = query.count.unwrap_or(50).clamp(1, 200);
        let tenant_id = TenantId::new(tenant.tenant_id());
        match outbox
            .list_processed(&tenant_id, query.after, limit)
            .await
        {
            Ok(rows) => {
                for row in rows {
                    let focus = format!("{}/{}", row.resource_type, row.resource_id);
                    let mut entry = json!({
                        "fullUrl": format!("{}/{}", state.base_url(), focus),
                        "request": {
                            "method": match row.event_type {
                                helios_persistence::core::OutboxEventType::Create => "POST",
                                helios_persistence::core::OutboxEventType::Update => "PUT",
                                helios_persistence::core::OutboxEventType::Delete => "DELETE",
                            },
                            "url": focus
                        }
                    });
                    if let Some(resource) = row.resource {
                        entry
                            .as_object_mut()
                            .expect("entry object")
                            .insert("resource".to_string(), resource);
                    }
                    entries.push(entry);
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to list outbox events for $events");
            }
        }
    }

    let bundle = json!({
        "resourceType": "Bundle",
        "type": bundle_type,
        "entry": entries
    });

    Ok((StatusCode::OK, Json(bundle)).into_response())
}

/// Handler for the `$get-ws-binding-token` operation on Subscription resources.
///
/// Returns a `Parameters` resource containing a short-lived token, expiration,
/// and the WebSocket URL for binding.
///
/// # HTTP Request
///
/// `GET [base]/Subscription/{id}/$get-ws-binding-token`
pub async fn get_ws_binding_token_handler<S>(
    State(state): State<AppState<S>>,
    Path((_resource_type, id)): Path<(String, String)>,
    tenant: TenantExtractor,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    let engine = state
        .subscription_engine()
        .ok_or(RestError::NotImplemented {
            feature: "Subscriptions".to_string(),
        })?;

    // Verify subscription exists.
    let sub = engine
        .manager()
        .get_subscription(tenant.tenant_id(), &id)
        .ok_or(RestError::NotFound {
            resource_type: "Subscription".to_string(),
            id: id.clone(),
        })?;

    // Verify it's a websocket subscription.
    if sub.channel.channel_type != helios_subscriptions::manager::ChannelType::Websocket {
        return Err(RestError::BadRequest {
            message: format!(
                "$get-ws-binding-token is only valid for websocket subscriptions, \
                 but this subscription uses channel type '{}'",
                sub.channel.channel_type.as_fhir_str()
            ),
        });
    }

    // Generate binding token.
    let (token, expiration) = engine
        .ws_token_manager()
        .generate_token(tenant.tenant_id(), &id);

    // Build WebSocket URL from the base URL.
    let ws_base = state
        .base_url()
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    let ws_url = format!("{}/ws/subscriptions/bind", ws_base);

    // Return Parameters resource per FHIR spec.
    let parameters = json!({
        "resourceType": "Parameters",
        "parameter": [
            {
                "name": "token",
                "valueString": token
            },
            {
                "name": "expiration",
                "valueDateTime": expiration.to_rfc3339()
            },
            {
                "name": "websocket-url",
                "valueUrl": ws_url
            }
        ]
    });

    Ok((StatusCode::OK, Json(parameters)).into_response())
}

/// Builds a SubscriptionStatus resource for the $status response.
fn build_subscription_status(
    sub: &helios_subscriptions::manager::ActiveSubscription,
    id: &str,
    base_url: &str,
) -> serde_json::Value {
    if uses_backport_ig(sub.fhir_version) {
        // R4 backport: return Parameters resource
        json!({
            "resourceType": "Parameters",
            "parameter": [
                {
                    "name": "subscription",
                    "valueReference": {
                        "reference": format!("{}/Subscription/{}", base_url, id)
                    }
                },
                {
                    "name": "topic",
                    "valueCanonical": sub.topic_url
                },
                {
                    "name": "status",
                    "valueCode": sub.status.as_fhir_str()
                },
                {
                    "name": "type",
                    "valueCode": "query-status"
                },
                {
                    "name": "events-since-subscription-start",
                    "valueString": sub.events_since_start.to_string()
                }
            ]
        })
    } else {
        // R5/R6 native: return SubscriptionStatus resource
        json!({
            "resourceType": "SubscriptionStatus",
            "status": sub.status.as_fhir_str(),
            "type": "query-status",
            "eventsSinceSubscriptionStart": sub.events_since_start,
            "subscription": {
                "reference": format!("Subscription/{}", id)
            },
            "topic": sub.topic_url
        })
    }
}

/// Returns true for FHIR versions that use the Subscriptions R5 Backport IG
/// (R4), false for versions with native subscription support (R4B, R5, R6).
fn uses_backport_ig(version: helios_fhir::FhirVersion) -> bool {
    version.as_str() == "R4"
}
