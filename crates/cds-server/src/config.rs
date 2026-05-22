//! Configuration for `cds-server` (CLI + env).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use atrius_clinical_reasoning::{
    ClinicalReasoningClient, ClinicalReasoningConfig, FhirServiceEndpoints,
};
use clap::Parser;

/// CDS Hooks HTTP server.
#[derive(Debug, Parser)]
#[command(name = "cds-server")]
pub struct Args {
    /// Listen host.
    #[arg(long, env = "CDS_SERVER_HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Listen port.
    #[arg(long, env = "CDS_SERVER_PORT", default_value = "8095")]
    pub port: u16,

    /// Log level (RUST_LOG style supported via tracing-subscriber env filter).
    #[arg(long, env = "CDS_LOG_LEVEL", default_value = "debug")]
    pub log_level: String,

    /// Enable CORS (wildcard origins when true).
    #[arg(long, env = "CDS_ENABLE_CORS", default_value = "true")]
    pub enable_cors: bool,

    /// JVM clinical reasoning sidecar base URL. Unset or empty → demo mode (no ELM evaluation).
    #[arg(
        long,
        env = "CDS_CLINICAL_REASONING_URL",
        default_value = "http://127.0.0.1:8088"
    )]
    pub clinical_reasoning_url: Option<String>,

    /// Clinical FHIR REST base (`hfsBaseUrl` → sidecar). Use the listen URL of your HFS process (scheme/host/port/path prefix), e.g. `http://localhost:8082` when `HFS_SERVER_PORT=8082`.
    #[arg(
        long,
        env = "CDS_HFS_BASE_URL",
        default_value = "http://127.0.0.1:8082"
    )]
    pub hfs_base_url: String,

    /// Terminology FHIR REST base (`htsBaseUrl` → sidecar). Example: `http://localhost:8090` when HTS listens on 8090.
    #[arg(
        long,
        env = "CDS_HTS_BASE_URL",
        default_value = "http://127.0.0.1:8090"
    )]
    pub hts_base_url: String,

    /// Knowledge repo base URL — used for `libraryBaseUrl` → sidecar and to fetch `Binary` manifests.
    #[arg(
        long,
        env = "CDS_LIBRARY_BASE_URL",
        default_value = "http://127.0.0.1:8079"
    )]
    pub library_base_url: Option<String>,

    /// Local JSON manifest path (same schema as KR Binary payload). Overrides KR Binary when set.
    #[arg(long, env = "CDS_SERVICES_MANIFEST_PATH")]
    pub services_manifest_path: Option<PathBuf>,

    /// FHIR `Binary.id` on the KR (`GET {CDS_LIBRARY_BASE_URL}/Binary/{id}`) holding base64 JSON manifest.
    #[arg(long, env = "CDS_KR_SERVICES_BINARY_ID")]
    pub kr_services_binary_id: Option<String>,

    /// HTTP timeout when fetching KR Binary (seconds).
    #[arg(long, env = "CDS_KR_MANIFEST_HTTP_TIMEOUT_SECS", default_value = "30")]
    pub kr_manifest_http_timeout_secs: u64,

    /// Sidecar HTTP timeout (seconds).
    #[arg(long, env = "CDS_SIDECAR_TIMEOUT_SECS", default_value = "120")]
    pub sidecar_timeout_secs: u64,
}

impl Args {
    pub fn socket_addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port)
            .parse()
            .expect("valid listen address")
    }

    pub fn kr_http_client(&self) -> anyhow::Result<reqwest::Client> {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(self.kr_manifest_http_timeout_secs))
            .build()
            .context("reqwest client for KR manifest fetch")
    }

    /// Shared sidecar client + FHIR endpoints when `CDS_CLINICAL_REASONING_URL` is non-empty.
    pub fn shared_sidecar(
        &self,
    ) -> anyhow::Result<Option<(Arc<ClinicalReasoningClient>, Arc<FhirServiceEndpoints>)>> {
        let Some(ref url) = self.clinical_reasoning_url else {
            return Ok(None);
        };
        let url = url.trim();
        if url.is_empty() {
            return Ok(None);
        }

        let cfg = ClinicalReasoningConfig::new(url)
            .request_timeout(Duration::from_secs(self.sidecar_timeout_secs));
        let client = ClinicalReasoningClient::new(cfg)?;

        let mut endpoints =
            FhirServiceEndpoints::new(self.hfs_base_url.trim(), self.hts_base_url.trim());
        if let Some(ref lb) = self.library_base_url
            && !lb.trim().is_empty()
        {
            endpoints = endpoints.with_library_base_url(lb.trim());
        }

        Ok(Some((Arc::new(client), Arc::new(endpoints))))
    }

    pub fn sidecar_configured(&self) -> bool {
        self.clinical_reasoning_url
            .as_deref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
pub struct EvalTargets {
    pub library_id: String,
    pub expression: String,
    pub library_version: Option<String>,
    pub resolve_from_fhir: bool,
}
