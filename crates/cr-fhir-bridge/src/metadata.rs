//! Bridge CapabilityStatement — advertises `$apply` and notes proxy behaviour.

use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};
use serde_json::json;

use crate::proxy::BridgeState;

pub async fn capabilities(State(state): State<Arc<BridgeState>>) -> impl IntoResponse {
    let base = state
        .cr
        .as_ref()
        .map(|cr| cr.bridge_base.as_str())
        .unwrap_or(&state.upstream_base);

    let apply_ops = state.cr.as_ref().map(|_| {
        json!([{
            "name": "apply",
            "definition": "http://hl7.org/fhir/OperationDefinition/PlanDefinition-apply"
        }])
    });

    let plan_definition = json!({
        "type": "PlanDefinition",
        "interaction": [{ "code": "read" }],
        "operation": apply_ops.clone().unwrap_or_else(|| json!([]))
    });

    let activity_definition = json!({
        "type": "ActivityDefinition",
        "interaction": [{ "code": "read" }],
        "operation": apply_ops
            .map(|_| json!([{
                "name": "apply",
                "definition": "http://hl7.org/fhir/OperationDefinition/ActivityDefinition-apply"
            }]))
            .unwrap_or_else(|| json!([]))
    });

    let statement = json!({
        "resourceType": "CapabilityStatement",
        "status": "active",
        "date": chrono::Utc::now().to_rfc3339(),
        "kind": "instance",
        "fhirVersion": "4.0.1",
        "format": ["json", "application/fhir+json"],
        "implementation": {
            "description": "Atrius cr-fhir-bridge — clinical proxy with QI-Core projection and PlanDefinition/ActivityDefinition $apply",
            "url": base
        },
        "rest": [{
            "mode": "server",
            "documentation": "Clinical FHIR paths proxy to upstream HFS with Atrius→QI-Core projection. /Library/* proxies to KR when configured. $apply executes via JVM clinical reasoning sidecar.",
            "resource": [plan_definition, activity_definition],
            "interaction": [
                { "code": "transaction" },
                { "code": "batch" }
            ]
        }]
    });

    (
        [(axum::http::header::CONTENT_TYPE, "application/fhir+json")],
        Json(statement),
    )
}
