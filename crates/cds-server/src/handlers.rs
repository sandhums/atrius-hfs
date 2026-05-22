//! HTTP handlers for CDS Hooks discovery, invocation, and feedback.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use helios_cds_hooks::{
    CdsHooksError, CdsRequest, CdsResponse, DiscoveryResponse, FeedbackRequest,
};

use crate::services::ServiceRegistry;

pub async fn health() -> &'static str {
    "ok"
}

pub async fn discovery(State(registry): State<ServiceRegistry>) -> Json<DiscoveryResponse> {
    Json(DiscoveryResponse {
        services: registry.discovery_services(),
    })
}

pub async fn invoke_service(
    State(registry): State<ServiceRegistry>,
    Path(id): Path<String>,
    Json(req): Json<CdsRequest>,
) -> Result<Json<CdsResponse>, (StatusCode, String)> {
    let Some(svc) = registry.by_id(&id) else {
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
    State(registry): State<ServiceRegistry>,
    Path(id): Path<String>,
    Json(body): Json<FeedbackRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some(svc) = registry.by_id(&id) else {
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
