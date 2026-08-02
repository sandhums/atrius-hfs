//! HFS Subscription rest-hook → `atrius-critical-labs` pipeline.
//!
//! `POST /internal/cds/fhir-notifications` accepts FHIR notification Bundles
//! (handshake / heartbeat / event). Event notifications resolve Observation
//! focus resources (id-only payloads fetch from clinical HFS), invoke the
//! critical-labs CDS service as a synthetic `patient-view`, and persist any
//! cards as FHIR `Flag` resources when a feedback FHIR base is configured.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use helios_cds_hooks::{CdsRequest, Indicator};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::AppState;
use crate::fhir_write_auth::{FhirWriteAuth, authorize_request};

const CRITICAL_LABS_SERVICE_ID: &str = "atrius-critical-labs";

/// Optional wiring for the subscription rest-hook receiver.
#[derive(Clone)]
pub struct SubscriptionNotifyConfig {
    pub webhook_secret: Option<String>,
    pub fhir_http: reqwest::Client,
    pub fhir_base_url: String,
    pub fhir_auth: Arc<dyn FhirWriteAuth>,
    pub fhir_tenant_id: Option<String>,
    /// When false, cards are logged but Flags are not written.
    pub persist_flags: bool,
}

impl SubscriptionNotifyConfig {
    pub fn auth_mode(&self) -> &'static str {
        self.fhir_auth.mode()
    }
}

pub async fn receive_fhir_notification(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::Json(bundle): axum::Json<Value>,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some(cfg) = state.subscription_notify.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "subscription notifications disabled (set CDS_SUBSCRIPTION_WEBHOOK_SECRET or \
             CDS_FEEDBACK_FHIR_BASE_URL / CDS_HFS_BASE_URL)"
                .into(),
        ));
    };

    if let Some(secret) = cfg.webhook_secret.as_deref()
        && !webhook_authorized(&headers, secret)
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            "invalid or missing subscription webhook secret".into(),
        ));
    }

    if bundle.get("resourceType").and_then(Value::as_str) != Some("Bundle") {
        return Err((StatusCode::BAD_REQUEST, "expected a FHIR Bundle".into()));
    }

    let notification_type = notification_type(&bundle);
    if matches!(
        notification_type.as_str(),
        "handshake" | "heartbeat" | "query-status"
    ) {
        debug!(%notification_type, "cds subscription notification ack");
        return Ok(StatusCode::OK);
    }

    let focuses = observation_focuses(&bundle);
    if focuses.is_empty() {
        debug!("cds subscription event with no Observation focus; acking");
        return Ok(StatusCode::OK);
    }

    let cfg = Arc::clone(cfg);
    let registry = state.registry.clone();
    tokio::spawn(async move {
        for focus in focuses {
            if let Err(e) = process_observation_focus(&registry, &cfg, focus).await {
                warn!(error = %e, "cds critical-labs subscription processing failed");
            }
        }
    });

    Ok(StatusCode::OK)
}

async fn process_observation_focus(
    registry: &crate::services::ServiceRegistry,
    cfg: &SubscriptionNotifyConfig,
    focus: ObservationFocus,
) -> Result<(), String> {
    let observation = match focus.resource {
        Some(obs) => obs,
        None => {
            let id = focus
                .id
                .as_deref()
                .ok_or_else(|| "observation focus missing id".to_string())?;
            fetch_observation(cfg, id).await?
        }
    };

    let patient_id = patient_id_from_observation(&observation)
        .ok_or_else(|| "observation missing Patient subject".to_string())?;
    let obs_id = observation
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("unknown");

    let Some(svc) = registry.by_id(CRITICAL_LABS_SERVICE_ID) else {
        return Err(format!("CDS service `{CRITICAL_LABS_SERVICE_ID}` not registered"));
    };

    let obs_bundle = json!({
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [{ "resource": observation.clone() }]
    });
    let hook_instance = format!(
        "cds-sub-{patient_id}-{obs_id}-{}",
        chrono::Utc::now().timestamp_millis()
    );
    let request = CdsRequest {
        hook: "patient-view".into(),
        hook_instance,
        fhir_server: Some(cfg.fhir_base_url.clone()),
        fhir_authorization: None,
        context: json!({
            "patientId": patient_id,
            "userId": "Device/atrius-cds-subscription"
        }),
        prefetch: Some(
            [
                (
                    "patient".into(),
                    Some(json!({
                        "resourceType": "Patient",
                        "id": patient_id
                    })),
                ),
                ("observations".into(), Some(obs_bundle.clone())),
                ("observation-1".into(), Some(obs_bundle)),
            ]
            .into_iter()
            .collect(),
        ),
        extension: None,
    };

    let response = svc
        .invoke(&request)
        .await
        .map_err(|e| format!("invoke {CRITICAL_LABS_SERVICE_ID}: {e}"))?;

    if response.cards.is_empty() {
        debug!(
            patient_id,
            observation_id = obs_id,
            "critical-labs silent for subscription event"
        );
        return Ok(());
    }

    info!(
        patient_id,
        observation_id = obs_id,
        cards = response.cards.len(),
        "critical-labs cards from subscription event"
    );

    if !cfg.persist_flags {
        return Ok(());
    }

    for card in &response.cards {
        post_flag(cfg, &patient_id, obs_id, card).await;
    }
    Ok(())
}

async fn fetch_observation(cfg: &SubscriptionNotifyConfig, id: &str) -> Result<Value, String> {
    let url = format!("{}/Observation/{}", cfg.fhir_base_url.trim_end_matches('/'), id);
    let req = cfg
        .fhir_http
        .get(&url)
        .header("Accept", "application/fhir+json");
    let mut req = authorize_request(cfg.fhir_auth.as_ref(), req).await?;
    if let Some(ref tenant) = cfg.fhir_tenant_id {
        req = req.header("X-Tenant-ID", tenant);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("fetch Observation/{id}: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "fetch Observation/{id} → {status}: {}",
            body.chars().take(200).collect::<String>()
        ));
    }
    resp.json()
        .await
        .map_err(|e| format!("decode Observation/{id}: {e}"))
}

async fn post_flag(
    cfg: &SubscriptionNotifyConfig,
    patient_id: &str,
    observation_id: &str,
    card: &helios_cds_hooks::Card,
) {
    let code = match card.indicator {
        Indicator::Critical => "critical",
        Indicator::Warning => "warning",
        Indicator::Info => "info",
    };
    let flag = json!({
        "resourceType": "Flag",
        "status": "active",
        "category": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/flag-category",
                "code": "clinical",
                "display": "Clinical"
            }]
        }],
        "code": {
            "coding": [{
                "system": "https://atrius.in/fhir/CodeSystem/cds-flag",
                "code": code,
                "display": card.summary
            }],
            "text": card.summary
        },
        "subject": { "reference": format!("Patient/{patient_id}") },
        "author": { "display": CRITICAL_LABS_SERVICE_ID },
        "extension": [{
            "url": "https://atrius.in/fhir/StructureDefinition/cds-subscription-source",
            "valueReference": { "reference": format!("Observation/{observation_id}") }
        }]
    });

    let url = format!("{}/Flag", cfg.fhir_base_url.trim_end_matches('/'));
    let req = cfg
        .fhir_http
        .post(&url)
        .header("Content-Type", "application/fhir+json")
        .json(&flag);
    let mut req = match authorize_request(cfg.fhir_auth.as_ref(), req).await {
        Ok(r) => r,
        Err(e) => {
            warn!(
                patient_id,
                observation_id,
                error = %e,
                "critical-labs Flag auth failed before write"
            );
            return;
        }
    };
    if let Some(ref tenant) = cfg.fhir_tenant_id {
        req = req.header("X-Tenant-ID", tenant);
    }

    match req.send().await {
        Ok(resp) if resp.status().is_success() => {
            debug!(patient_id, observation_id, "critical-labs Flag persisted");
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(
                patient_id,
                observation_id,
                %status,
                body = %body.chars().take(300).collect::<String>(),
                "critical-labs Flag write rejected"
            );
        }
        Err(e) => {
            warn!(patient_id, observation_id, error = %e, "critical-labs Flag write failed");
        }
    }
}

#[derive(Debug)]
struct ObservationFocus {
    id: Option<String>,
    resource: Option<Value>,
}

fn notification_type(bundle: &Value) -> String {
    let Some(status) = bundle
        .get("entry")
        .and_then(Value::as_array)
        .and_then(|e| e.first())
        .and_then(|e| e.get("resource"))
    else {
        return "event-notification".into();
    };
    if let Some(code) = status.get("type").and_then(Value::as_str) {
        return code.to_string();
    }
    if let Some(params) = status.get("parameter").and_then(Value::as_array) {
        for p in params {
            if p.get("name").and_then(Value::as_str) == Some("type")
                && let Some(code) = p.get("valueCode").and_then(Value::as_str)
            {
                return code.to_string();
            }
        }
    }
    "event-notification".into()
}

fn observation_focuses(bundle: &Value) -> Vec<ObservationFocus> {
    let mut out = Vec::new();
    let entries = bundle
        .get("entry")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for entry in &entries {
        let Some(resource) = entry.get("resource") else {
            continue;
        };
        if resource.get("resourceType").and_then(Value::as_str) == Some("Observation") {
            out.push(ObservationFocus {
                id: resource
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                resource: Some(resource.clone()),
            });
        }
    }

    // Parameters focus references (id-only payloads).
    if let Some(status) = entries.first().and_then(|e| e.get("resource"))
        && let Some(params) = status.get("parameter").and_then(Value::as_array)
    {
        for p in params {
            if p.get("name").and_then(Value::as_str) != Some("notification-event") {
                continue;
            }
            let Some(parts) = p.get("part").and_then(Value::as_array) else {
                continue;
            };
            for part in parts {
                if part.get("name").and_then(Value::as_str) != Some("focus") {
                    continue;
                }
                if let Some(reference) = part
                    .get("valueReference")
                    .and_then(|r| r.get("reference"))
                    .and_then(Value::as_str)
                {
                    if let Some(id) = reference.strip_prefix("Observation/") {
                        if !out.iter().any(|f| f.id.as_deref() == Some(id)) {
                            out.push(ObservationFocus {
                                id: Some(id.to_string()),
                                resource: None,
                            });
                        }
                    }
                }
            }
        }
    }

    // Native R5-style focus on SubscriptionStatus notificationEvent.
    if let Some(status) = entries.first().and_then(|e| e.get("resource"))
        && let Some(events) = status
            .get("notificationEvent")
            .and_then(Value::as_array)
    {
        for event in events {
            if let Some(reference) = event
                .get("focus")
                .and_then(|f| f.get("reference"))
                .and_then(Value::as_str)
                && let Some(id) = reference.strip_prefix("Observation/")
                && !out.iter().any(|f| f.id.as_deref() == Some(id))
            {
                out.push(ObservationFocus {
                    id: Some(id.to_string()),
                    resource: None,
                });
            }
        }
    }

    out
}

fn patient_id_from_observation(obs: &Value) -> Option<String> {
    let reference = obs
        .get("subject")
        .and_then(|s| s.get("reference"))
        .and_then(Value::as_str)?;
    reference
        .strip_prefix("Patient/")
        .map(str::to_string)
        .or_else(|| {
            if !reference.contains('/') {
                Some(reference.to_string())
            } else {
                None
            }
        })
}

fn webhook_authorized(headers: &HeaderMap, secret: &str) -> bool {
    if let Some(header) = headers
        .get("x-cds-webhook-secret")
        .and_then(|v| v.to_str().ok())
        && header == secret
    {
        return true;
    }
    if let Some(auth) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        let bearer = auth
            .strip_prefix("Bearer ")
            .or_else(|| auth.strip_prefix("bearer "));
        if bearer == Some(secret) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_id_only_observation_focus() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "history",
            "entry": [{
                "resource": {
                    "resourceType": "Parameters",
                    "parameter": [
                        { "name": "type", "valueCode": "event-notification" },
                        {
                            "name": "notification-event",
                            "part": [
                                {
                                    "name": "focus",
                                    "valueReference": { "reference": "Observation/obs-1" }
                                }
                            ]
                        }
                    ]
                }
            }]
        });
        let focuses = observation_focuses(&bundle);
        assert_eq!(focuses.len(), 1);
        assert_eq!(focuses[0].id.as_deref(), Some("obs-1"));
        assert!(focuses[0].resource.is_none());
    }

    #[test]
    fn parses_full_observation_entry() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "history",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Parameters",
                        "parameter": [
                            { "name": "type", "valueCode": "event-notification" }
                        ]
                    }
                },
                {
                    "resource": {
                        "resourceType": "Observation",
                        "id": "obs-2",
                        "subject": { "reference": "Patient/p1" }
                    }
                }
            ]
        });
        let focuses = observation_focuses(&bundle);
        assert_eq!(focuses.len(), 1);
        assert_eq!(focuses[0].id.as_deref(), Some("obs-2"));
        assert_eq!(
            patient_id_from_observation(focuses[0].resource.as_ref().unwrap()).as_deref(),
            Some("p1")
        );
    }
}
