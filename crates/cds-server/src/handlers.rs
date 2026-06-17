//! HTTP handlers for CDS Hooks discovery, invocation, and feedback.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use helios_cds_hooks::{
    CdsHooksError, CdsRequest, CdsResponse, DiscoveryResponse, FeedbackRequest,
};

use crate::AppState;

pub async fn health() -> &'static str {
    "ok"
}

/// Readiness: KR library + PlanDefinition pins probed at startup when `CDS_VALIDATE_KR_LIBRARIES` is enabled.
pub async fn ready(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    match &state.kr_readiness {
        None => Ok(Json(serde_json::json!({
            "status": "ready",
            "krLibraries": "not_checked",
            "krPlanDefinitions": "not_checked"
        }))),
        Some(report) if report.ok => Ok(Json(serde_json::json!({
            "status": "ready",
            "krLibraries": "ok",
            "krPlanDefinitions": if report.plan_definition_pins.is_empty() { "none" } else { "ok" },
            "libraryPins": report.library_pins.len(),
            "planDefinitionPins": report.plan_definition_pins.len(),
            "pins": report.library_pins.len(),
            "message": report.message
        }))),
        Some(report) => Err((StatusCode::SERVICE_UNAVAILABLE, report.message.clone())),
    }
}

pub async fn discovery(State(state): State<AppState>) -> Json<DiscoveryResponse> {
    Json(DiscoveryResponse {
        services: state.registry.discovery_services(),
    })
}

pub async fn invoke_service(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<CdsRequest>,
) -> Result<Json<CdsResponse>, (StatusCode, String)> {
    let Some(svc) = state.registry.by_id(&id) else {
        return Err((
            StatusCode::NOT_FOUND,
            format!("unknown CDS service id: {id}"),
        ));
    };

    let def = svc.definition();
    if req.hook != def.hook {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "request hook `{}` does not match service `{}` hook `{}`",
                req.hook, id, def.hook
            ),
        ));
    }

    svc.invoke(&req).await.map(Json).map_err(cds_err_http)
}

pub async fn feedback(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<FeedbackRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some(svc) = state.registry.by_id(&id) else {
        return Err((
            StatusCode::NOT_FOUND,
            format!("unknown CDS service id: {id}"),
        ));
    };

    svc.feedback(&body).await.map_err(cds_err_http)?;
    Ok(StatusCode::NO_CONTENT)
}

fn cds_err_http(e: CdsHooksError) -> (StatusCode, String) {
    let code = StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (code, e.to_string())
}
