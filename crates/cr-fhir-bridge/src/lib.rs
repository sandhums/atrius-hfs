//! FHIR bridge library — proxy clinical HFS with runtime Atrius→QI-Core projection.
//!
//! # Role in the clinical reasoning stack
//!
//! The JVM sidecar evaluates QI-Core CQL and reads FHIR via **`hfsBaseUrl`**. In the Atrius
//! deployment that URL must be this bridge (typically port **8081**), not raw clinical HFS:
//!
//! - **Clinical paths** (`/Patient`, `/Condition`, search, …) → upstream clinical HFS, then
//!   JSON responses are projected through [`atrius_runtime_mapper`] before returning to the sidecar.
//! - **`/Library` paths** → Knowledge Repository HFS when [`BridgeState::kr_base`] is set, because
//!   CQFramework resolves CQL **`include`** dependencies via `hfsBaseUrl`, not `libraryBaseUrl`.
//!
//! See `docs/clinical-reasoning/README.md` in the repo root for the full stack diagram and
//! environment variables (`CR_FHIR_BRIDGE_*`).
//!
//! [`apply`] exposes FHIR **`PlanDefinition/$apply`** and **`ActivityDefinition/$apply`** (Parameters
//! in/out) by delegating to the JVM sidecar.

pub mod apply;
pub mod config;
pub mod fhir_parameters;
pub mod metadata;
pub mod proxy;
pub mod transform;

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    routing::{any, get, post},
};
use reqwest::Client;
use tower_http::{cors::CorsLayer, trace::TraceLayer};

pub use apply::ClinicalReasoningEndpoints;
pub use config::Args;
pub use proxy::{BridgeProxyError, BridgeState};
pub use transform::{TransformStats, transform_fhir_value};

/// Build the Axum router for the bridge HTTP server.
pub fn build_router(state: Arc<BridgeState>, enable_cors: bool) -> Router {
    let mut app = Router::new()
        .route("/health", get(proxy::health))
        .route("/metadata", get(metadata::capabilities))
        .route(
            "/PlanDefinition/$apply",
            post(apply::plan_definition_type_apply),
        )
        .route(
            "/PlanDefinition/{id}/$apply",
            post(apply::plan_definition_instance_apply),
        )
        .route(
            "/ActivityDefinition/$apply",
            post(apply::activity_definition_type_apply),
        )
        .route(
            "/ActivityDefinition/{id}/$apply",
            post(apply::activity_definition_instance_apply),
        )
        .fallback(any(proxy::proxy_fhir))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

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

/// HTTP client for upstream clinical HFS.
pub fn upstream_http_client(timeout: Duration) -> anyhow::Result<Client> {
    Ok(Client::builder().timeout(timeout).build()?)
}
