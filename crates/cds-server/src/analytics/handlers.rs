//! HTTP handlers for measure evaluate, CQL cohorts, and catalog-grounded view suggestion.

use axum::{Json, extract::State, http::StatusCode};
use serde_json::Value;

use crate::AppState;
use crate::analytics::{CohortBody, MeasureEvaluateBody, NlViewsBody};

pub async fn evaluate_measure(
    State(state): State<AppState>,
    Json(body): Json<MeasureEvaluateBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    state
        .analytics
        .evaluate_measure(body)
        .await
        .map(Json)
        .map_err(|(code, v)| (status(code), Json(v)))
}

pub async fn materialize_cohort(
    State(state): State<AppState>,
    Json(body): Json<CohortBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    state
        .analytics
        .materialize_cohort(body)
        .await
        .map(Json)
        .map_err(|(code, v)| (status(code), Json(v)))
}

pub async fn nl_views(
    State(state): State<AppState>,
    Json(body): Json<NlViewsBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match state.analytics.suggest_views(&body.text) {
        Ok(resp) => Ok(Json(
            serde_json::to_value(resp).unwrap_or_else(|_| serde_json::json!({})),
        )),
        Err((code, msg)) => Err((status(code), Json(serde_json::json!({"error": msg})))),
    }
}

fn status(code: u16) -> StatusCode {
    StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}
