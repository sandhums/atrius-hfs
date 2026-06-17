//! `cr-fhir-bridge` — proxy clinical HFS with Atrius→QI-Core runtime projection.
//!
//! See `docs/clinical-reasoning/README.md` for how this binary fits between the JVM sidecar and
//! dual HFS (clinical + Knowledge Repository).

use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser as _;
use cr_fhir_bridge::{Args, BridgeState, build_router, upstream_http_client};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(format!(
                    "cr_fhir_bridge={},tower_http=info",
                    args.log_level
                ))
            }),
        )
        .init();

    let mapper = args.load_mapper().context("load mapper manifest")?;
    let http =
        upstream_http_client(args.request_timeout()).context("build upstream HTTP client")?;

    let state = Arc::new(
        BridgeState::new(
            args.upstream_base(),
            args.kr_base(),
            http,
            mapper,
            args.max_body_size,
        )
        .with_clinical_reasoning(
            args.bridge_public_base(),
            args.hts_base(),
            args.sidecar_base()
                .context("CR_FHIR_BRIDGE_SIDECAR_URL must be set for $apply")?,
            args.request_timeout(),
        )
        .context("configure clinical reasoning sidecar client")?,
    );

    tracing::info!(
        addr = %args.socket_addr(),
        upstream = %args.upstream_base(),
        kr = ?args.kr_base(),
        sidecar = ?args.sidecar_base(),
        public_base = %args.bridge_public_base(),
        hts = %args.hts_base(),
        max_body_size = args.max_body_size,
        "cr-fhir-bridge listening"
    );

    let app = build_router(state, args.enable_cors);
    let listener = tokio::net::TcpListener::bind(args.socket_addr()).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
