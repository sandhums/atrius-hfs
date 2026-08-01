//! CDS Hooks HTTP server (`cds-server`).
//!
//! Bridges CDS Clients (EHR) to the JVM clinical reasoning sidecar via
//! [`cds_server::clinical_reasoning`]. CDS service definitions load from a **KR `Binary`**
//! or local JSON manifest.
//!
//! Startup, env vars, and stack wiring: `docs/clinical-reasoning/startup-guide.md`.

use anyhow::Context as _;
use cds_server::{
    AppState, build_router,
    config::Args,
    kr_manifest,
    kr_readiness::probe_kr_readiness,
    library_version::validate_manifest_versions,
    services::{CdsEvalBackend, registry_from_manifest},
};
use clap::Parser as _;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    helios_observability::uptime::init();
    let log_level = format!("cds_server={},tower_http=info", args.log_level);
    helios_observability::telemetry::init("cds-server", &log_level);
    helios_observability::metrics::init("cds-server");

    let manifest = resolve_manifest(&args).await?;
    let library_version_policy = args.library_version_policy();

    validate_manifest_versions(&manifest, &library_version_policy)
        .context("CDS manifest library version validation")?;

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

    let measurement_period = args.measurement_period();
    if measurement_period.is_none() && args.sidecar_configured() {
        tracing::warn!(
            "CDS_MEASUREMENT_PERIOD_LOW/HIGH not set; eCQM CQL needs Measurement Period on each \
             hook invoke via context.measurementPeriod (or extension fallback)"
        );
    }

    let kr_readiness = if args.sidecar_configured()
        && library_version_policy.validate_kr_on_startup
        && let Some(kr_base) = args.kr_base_url()
    {
        let http = args.kr_http_client()?;
        let report = probe_kr_readiness(&http, kr_base, &manifest).await;
        if !report.ok {
            tracing::error!(
                library_pins = report.library_pins.len(),
                plan_definition_pins = report.plan_definition_pins.len(),
                message = %report.message,
                "KR readiness probe failed"
            );
            anyhow::bail!("KR readiness probe failed: {}", report.message);
        }
        tracing::info!(
            library_pins = report.library_pins.len(),
            plan_definition_pins = report.plan_definition_pins.len(),
            message = %report.message,
            "KR readiness probe ok"
        );
        Some(report)
    } else {
        None
    };

    let backend = match args.shared_sidecar()? {
        None => CdsEvalBackend::Demo,
        Some((client, endpoints)) => CdsEvalBackend::Sidecar {
            client,
            endpoints,
            measurement_period,
            fhir_access_policy: args.fhir_access_policy(),
            library_version_policy: library_version_policy.clone(),
        },
    };

    tracing::info!(
        demo_only = matches!(backend, CdsEvalBackend::Demo),
        service_count = manifest.services.len(),
        require_library_version = library_version_policy.require_version_on_manifest,
        validate_kr_libraries = library_version_policy.validate_kr_on_startup,
        "cds-server configuration"
    );

    let feedback_store = args.feedback_store()?;
    if feedback_store.is_none() {
        tracing::info!(
            "CDS_FEEDBACK_FHIR_BASE_URL not set; card feedback will be acknowledged but not persisted"
        );
    }

    let registry = registry_from_manifest(&manifest, backend, feedback_store);

    let app = build_router(
        AppState {
            registry,
            kr_readiness,
        },
        args.enable_cors,
    );

    let listener = tokio::net::TcpListener::bind(args.socket_addr()).await?;
    tracing::info!(addr = %args.socket_addr(), "cds-server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    helios_observability::telemetry::shutdown();
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "failed to install CTRL+C handler");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => tracing::error!(error = %e, "failed to install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received ctrl-c, shutting down"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
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
