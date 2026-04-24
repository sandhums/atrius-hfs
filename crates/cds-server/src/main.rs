//! Optional demo: standalone CDS Hooks HTTP server using [`cds_core::PatientGreeterService`] and
//! [`cds_core::PatientViewQualityGapsService`] (same `patient-view` hook, separate discovery ids).
//!
//! Run: `cargo run -p cds-server --features binary -- --help`

use std::sync::Arc;

use cds_core::{PatientGreeterService, PatientViewQualityGapsService};
use cds_server::{CdsServiceDispatch, CdsServiceRegistry, ServiceWrapper, cds_hooks_router};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "cds-hooks-server")]
#[command(about = "CDS Hooks demo (discovery, invoke, feedback); evaluation in cds-core")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value = "8088")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);
    let greeter: Arc<dyn CdsServiceDispatch> =
        Arc::new(ServiceWrapper::new(Arc::new(PatientGreeterService)));
    let quality_gaps: Arc<dyn CdsServiceDispatch> =
        Arc::new(ServiceWrapper::new(Arc::new(PatientViewQualityGapsService)));
    let registry = CdsServiceRegistry::try_from_services([greeter, quality_gaps])?;
    let app = cds_hooks_router(registry);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "cds-hooks-server listening (CDS Hooks discovery + service + feedback)");
    axum::serve(listener, app).await?;
    Ok(())
}
