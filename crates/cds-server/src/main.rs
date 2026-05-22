//! CDS Hooks HTTP server (`cds-server`).
//!
//! Bridges CDS Clients (EHR) to the JVM clinical reasoning sidecar via
//! [`atrius_clinical_reasoning`]. CDS service definitions load from a **KR `Binary`**
//! or local JSON manifest.

use anyhow::Context as _;
use cds_server::{
    build_router,
    config::Args,
    kr_manifest,
    services::{CdsEvalBackend, registry_from_manifest},
};
use clap::Parser as _;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(format!(
                    "cds_server={},tower_http=info",
                    args.log_level
                ))
            }),
        )
        .init();

    let manifest = resolve_manifest(&args).await?;

    if args.sidecar_configured() {
        let used_file = args.services_manifest_path.is_some();
        let used_binary = args
            .kr_services_binary_id
            .as_deref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        if !used_file && !used_binary {
            anyhow::bail!(
                "CDS_CLINICAL_REASONING_URL is set; configure CDS_SERVICES_MANIFEST_PATH \
                 or CDS_KR_SERVICES_BINARY_ID (with CDS_LIBRARY_BASE_URL)"
            );
        }
    }

    let backend = match args.shared_sidecar()? {
        None => CdsEvalBackend::Demo,
        Some((client, endpoints)) => CdsEvalBackend::Sidecar { client, endpoints },
    };

    tracing::info!(
        demo_only = matches!(backend, CdsEvalBackend::Demo),
        service_count = manifest.services.len(),
        "cds-server configuration"
    );

    let registry = registry_from_manifest(&manifest, backend);

    let app = build_router(registry, args.enable_cors);

    let listener = tokio::net::TcpListener::bind(args.socket_addr()).await?;
    tracing::info!(addr = %args.socket_addr(), "cds-server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn resolve_manifest(args: &Args) -> anyhow::Result<kr_manifest::CdsServicesManifestFile> {
    if let Some(ref path) = args.services_manifest_path {
        return kr_manifest::load_manifest_from_path(path).await;
    }

    if let Some(ref bid) = args.kr_services_binary_id {
        let bid = bid.trim();
        if !bid.is_empty() {
            let lb = args
                .library_base_url
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .context("CDS_KR_SERVICES_BINARY_ID requires a non-empty CDS_LIBRARY_BASE_URL")?;
            let http = args.kr_http_client()?;
            return kr_manifest::fetch_manifest_from_kr_binary(&http, lb, bid).await;
        }
    }

    Ok(kr_manifest::demo_manifest())
}
