//! Configuration for `cds-server` (CLI + env).
//!
//! # Environment variables (`CDS_*`)
//!
//! | Variable | Role |
//! |----------|------|
//! | `CDS_CLINICAL_REASONING_URL` | JVM sidecar base; empty → demo mode (no ELM) |
//! | `CDS_HFS_BASE_URL` | **`hfsBaseUrl` sent to sidecar** — use `cr-fhir-bridge` in Atrius stack |
//! | `CDS_HTS_BASE_URL` | **`htsBaseUrl`** — HTS for ValueSet expansion during CQL |
//! | `CDS_LIBRARY_BASE_URL` | **`libraryBaseUrl`** — KR for primary CQL `Library` + manifest Binary fetch |
//! | `CDS_SERVICES_MANIFEST_PATH` | Local JSON catalog (overrides KR Binary when set) |
//! | `CDS_KR_SERVICES_BINARY_ID` | KR `Binary.id` holding base64 JSON service catalog |
//! | `CDS_SIDECAR_TIMEOUT_SECS` | HTTP timeout for evaluate calls (default 120s) |
//! | `CDS_MEASUREMENT_PERIOD_LOW` | eCQM reporting interval start (`YYYY-MM-DD`) → CQL `Measurement Period` |
//! | `CDS_MEASUREMENT_PERIOD_HIGH` | eCQM reporting interval end (`YYYY-MM-DD`) |
//! | `CDS_REQUIRE_FHIR_AUTHORIZATION` | Reject invoke without `fhirAuthorization` (production SMART) |
//! | `CDS_FHIR_SERVER_ALLOWLIST` | Comma-separated allowed `fhirServer` hosts when SMART token is present |
//! | `CDS_REQUIRE_LIBRARY_VERSION` | Require `libraryVersion` on every manifest evaluate service |
//! | `CDS_VALIDATE_KR_LIBRARIES` | Probe KR for pinned libraries at startup (`GET /ready`) |

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use atrius_clinical_reasoning::{
    ClinicalReasoningClient, ClinicalReasoningConfig, FhirServiceEndpoints,
};
use clap::Parser;

use crate::fhir_authorization::FhirAccessPolicy;
use crate::library_version::LibraryVersionPolicy;
use crate::measurement_period::MeasurementPeriod;

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

    /// eCQM measure reporting period start (inclusive). With [`Self::measurement_period_high`],
    /// forwarded as the CQL `Measurement Period` parameter on every sidecar evaluate/$apply call.
    #[arg(long, env = "CDS_MEASUREMENT_PERIOD_LOW")]
    pub measurement_period_low: Option<String>,

    /// eCQM measure reporting period end (inclusive). See [`Self::measurement_period_low`].
    #[arg(long, env = "CDS_MEASUREMENT_PERIOD_HIGH")]
    pub measurement_period_high: Option<String>,

    /// When true, each CDS invoke must include `fhirAuthorization` (SMART bearer access to clinical FHIR).
    #[arg(long, env = "CDS_REQUIRE_FHIR_AUTHORIZATION", default_value = "false")]
    pub require_fhir_authorization: bool,

    /// Comma-separated allowed hosts for `fhirServer` when `fhirAuthorization` is present (empty = any).
    #[arg(long, env = "CDS_FHIR_SERVER_ALLOWLIST")]
    pub fhir_server_allowlist: Option<String>,

    /// Require explicit `libraryVersion` on manifest services that use legacy CQL evaluate.
    #[arg(long, env = "CDS_REQUIRE_LIBRARY_VERSION", default_value = "false")]
    pub require_library_version: bool,

    /// Probe KR for manifest library pins and `planDefinitionId` at startup; expose on `GET /ready`.
    #[arg(long, env = "CDS_VALIDATE_KR_LIBRARIES", default_value = "false")]
    pub validate_kr_libraries: bool,

    /// Clinical FHIR base where CDS feedback is persisted as `GuidanceResponse`.
    /// Empty → feedback is acknowledged but not persisted.
    #[arg(long, env = "CDS_FEEDBACK_FHIR_BASE_URL")]
    pub feedback_fhir_base_url: Option<String>,

    /// Static bearer token for feedback GuidanceResponse writes (dev/smoke; feedback
    /// requests carry no `fhirAuthorization` per the CDS Hooks spec).
    #[arg(long, env = "CDS_FEEDBACK_FHIR_BEARER_TOKEN")]
    pub feedback_fhir_bearer_token: Option<String>,

    /// `X-Tenant-ID` header for feedback GuidanceResponse writes (multi-tenant HFS).
    #[arg(long, env = "CDS_FEEDBACK_FHIR_TENANT_ID")]
    pub feedback_fhir_tenant_id: Option<String>,
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

    /// Configured eCQM reporting interval for this cds-server process (both bounds required).
    pub fn measurement_period(&self) -> Option<MeasurementPeriod> {
        let low = self.measurement_period_low.as_deref()?;
        let high = self.measurement_period_high.as_deref()?;
        MeasurementPeriod::parse_bounds(low.trim(), high.trim())
    }

    pub fn fhir_access_policy(&self) -> FhirAccessPolicy {
        FhirAccessPolicy::from_config(
            self.require_fhir_authorization,
            self.fhir_server_allowlist.as_deref(),
        )
    }

    pub fn library_version_policy(&self) -> LibraryVersionPolicy {
        LibraryVersionPolicy::from_config(self.require_library_version, self.validate_kr_libraries)
    }

    pub fn kr_base_url(&self) -> Option<&str> {
        self.library_base_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    /// Feedback persistence store when `CDS_FEEDBACK_FHIR_BASE_URL` is configured.
    pub fn feedback_store(&self) -> anyhow::Result<Option<Arc<crate::feedback_store::FeedbackStore>>> {
        let Some(base) = self
            .feedback_fhir_base_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("reqwest client for feedback GuidanceResponse writes")?;
        Ok(Some(Arc::new(crate::feedback_store::FeedbackStore::new(
            http,
            base,
            self.feedback_fhir_bearer_token.clone(),
            self.feedback_fhir_tenant_id.clone(),
        ))))
    }
}

#[derive(Debug, Clone)]
pub struct EvalTargets {
    pub library_id: String,
    pub expression: String,
    pub library_version: Option<String>,
    pub resolve_from_fhir: bool,
}
