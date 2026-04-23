use std::sync::Arc;

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use helios_cds_hooks::{CdsHooksError, CdsRequest, DiscoveryResponse, FeedbackRequest};

use crate::dispatch::CdsServiceRegistry;

/// State shared by CDS Hooks routes.
pub type CdsRegistryState = Arc<CdsServiceRegistry>;

/// `GET /cds-services`, `POST /cds-services/:id`, `POST /cds-services/:id/feedback`
pub fn cds_hooks_router(registry: CdsServiceRegistry) -> Router {
    let state: CdsRegistryState = Arc::new(registry);
    Router::new()
        .route("/cds-services", get(get_discovery))
        .route("/cds-services/{id}", post(post_service))
        .route("/cds-services/{id}/feedback", post(post_feedback))
        .with_state(state)
}

async fn get_discovery(state: axum::extract::State<CdsRegistryState>) -> impl IntoResponse {
    let d: DiscoveryResponse = state.0.discovery();
    (StatusCode::OK, Json(d))
}

async fn post_service(
    state: axum::extract::State<CdsRegistryState>,
    Path(id): Path<String>,
    Json(request): Json<CdsRequest>,
) -> Response {
    let Some(svc) = state.0.get(&id) else {
        return not_found("unknown service id");
    };
    match svc.handle(request).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => cds_error_response(e),
    }
}

async fn post_feedback(
    state: axum::extract::State<CdsRegistryState>,
    Path(id): Path<String>,
    Json(request): Json<FeedbackRequest>,
) -> Response {
    let Some(svc) = state.0.get(&id) else {
        return not_found("unknown service id");
    };
    match svc.on_feedback(&request).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => cds_error_response(e),
    }
}

fn not_found(msg: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "message": msg })),
    )
        .into_response()
}

fn cds_error_response(e: CdsHooksError) -> Response {
    let status = StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let body = serde_json::json!({ "message": e.to_string() });
    (status, Json(body)).into_response()
}
