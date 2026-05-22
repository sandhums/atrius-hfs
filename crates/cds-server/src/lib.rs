//! CDS Hooks HTTP server library (router builder for the `cds-server` binary).

pub mod config;
pub mod cr_error;
pub mod handlers;
pub mod kr_manifest;
pub mod services;

use axum::{
    Router,
    routing::{get, post},
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::services::ServiceRegistry;

/// Axum router with CDS Hooks routes (state applied; ready for [`axum::serve`]).
pub fn build_router(registry: ServiceRegistry, enable_cors: bool) -> Router {
    let mut app = Router::new()
        .route("/health", get(handlers::health))
        .route("/cds-services", get(handlers::discovery))
        .route("/cds-services/{id}", post(handlers::invoke_service))
        .route("/cds-services/{id}/feedback", post(handlers::feedback))
        .with_state(registry)
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
