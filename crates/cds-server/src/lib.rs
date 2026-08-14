//! CDS Hooks HTTP server library (router builder for the `cds-server` binary).
//!
//! # Architecture
//!
//! `cds-server` sits between **CDS Clients** (EHR apps) and the **JVM clinical reasoning sidecar**:
//!
//! 1. **Discovery** — `GET /cds-services` returns services from a KR [`Binary`](crate::kr_manifest)
//!    manifest or local JSON file.
//! 2. **Invocation** — `POST /cds-services/{id}` maps hook context →
//!    [`clinical_reasoning::EvaluateExpressionRequest`] → sidecar evaluate / `$apply` → CDS [`Card`]s.
//!
//! See `docs/clinical-reasoning/README.md` in the repo root.

pub mod apply_context;
pub mod clinical_reasoning;
pub mod config;
pub mod cr_error;
pub mod feedback_store;
pub mod fhir_authorization;
pub mod fhir_write_auth;
pub mod handlers;
pub mod hook_context;
pub mod invoke_metrics;
pub mod kr_manifest;
pub mod kr_readiness;
pub mod library_version;
pub mod measurement_period;
pub mod services;
pub mod subscription_notifications;

use std::sync::Arc;

use axum::{
    Router, middleware,
    routing::{get, post},
};
use tower_http::cors::CorsLayer;

use crate::kr_readiness::KrReadinessReport;
use crate::services::ServiceRegistry;
use crate::subscription_notifications::SubscriptionNotifyConfig;

/// Shared Axum state (service registry + optional KR readiness from startup probe).
#[derive(Clone)]
pub struct AppState {
    pub registry: ServiceRegistry,
    /// `None` in demo mode or when KR validation is disabled.
    pub kr_readiness: Option<KrReadinessReport>,
    /// HFS rest-hook → critical-labs pipeline (`POST /internal/cds/fhir-notifications`).
    pub subscription_notify: Option<Arc<SubscriptionNotifyConfig>>,
}

/// Axum router with CDS Hooks routes (state applied; ready for [`axum::serve`]).
pub fn build_router(state: AppState, enable_cors: bool) -> Router {
    let mut app = Router::new()
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/cds-services", get(handlers::discovery))
        .route("/cds-services/{id}", post(handlers::invoke_service))
        .route("/cds-services/{id}/feedback", post(handlers::feedback))
        .route(
            "/internal/cds/fhir-notifications",
            post(subscription_notifications::receive_fhir_notification),
        )
        .with_state(state)
        .merge(helios_observability::metrics::router())
        .layer(middleware::from_fn(helios_observability::middleware::track));

    if enable_cors {
        use tower_http::cors::Any;
        app = app.layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );
    }

    app
}
