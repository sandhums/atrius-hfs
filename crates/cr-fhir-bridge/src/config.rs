//! CLI and environment configuration for `cr-fhir-bridge`.
//!
//! # Required wiring for eCQM / CDS
//!
//! - **`CR_FHIR_BRIDGE_UPSTREAM_URL`** — clinical HFS (Atrius chart data), e.g. `http://127.0.0.1:8082`
//! - **`CR_FHIR_BRIDGE_KR_URL`** — Knowledge Repository for `/Library/*` proxy, e.g. `http://127.0.0.1:8079`
//!
//! Sidecar and cds-server must set **`hfsBaseUrl` / `CDS_HFS_BASE_URL`** to this bridge's listen URL
//! (default port **8081**), not the clinical HFS URL directly.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use atrius_runtime_mapper::{MapperManifest, RuntimeMapper};
use clap::Parser;

fn trim_trailing_slash(s: String) -> String {
    let mut s = s.trim().to_string();
    while s.ends_with('/') {
        s.pop();
    }
    s
}

/// FHIR bridge: proxy clinical HFS with runtime Atrius→QI-Core projection.
#[derive(Debug, Parser)]
#[command(name = "cr-fhir-bridge")]
pub struct Args {
    /// Listen host.
    #[arg(long, env = "CR_FHIR_BRIDGE_HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Listen port.
    #[arg(long, env = "CR_FHIR_BRIDGE_PORT", default_value = "8081")]
    pub port: u16,

    /// Log level.
    #[arg(long, env = "CR_FHIR_BRIDGE_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Enable CORS (wildcard origins when true).
    #[arg(long, env = "CR_FHIR_BRIDGE_ENABLE_CORS", default_value = "true")]
    pub enable_cors: bool,

    /// Clinical HFS base URL (upstream — Atrius storage, not KR).
    #[arg(
        long,
        env = "CR_FHIR_BRIDGE_UPSTREAM_URL",
        default_value = "http://127.0.0.1:8082"
    )]
    pub upstream_url: String,

    /// Optional KR HFS base URL. When set, `/Library` reads are proxied here so the
    /// JVM sidecar can resolve CQL `include` libraries via `hfsBaseUrl` (the bridge).
    #[arg(long, env = "CR_FHIR_BRIDGE_KR_URL")]
    pub kr_url: Option<String>,

    /// Mapper manifest JSON from Atrius IG build. Built-in v0.1 when unset.
    #[arg(long, env = "ATRIUS_MAPPER_MANIFEST")]
    pub mapper_manifest_path: Option<PathBuf>,

    /// Upstream HTTP timeout (seconds).
    #[arg(long, env = "CR_FHIR_BRIDGE_REQUEST_TIMEOUT", default_value = "30")]
    pub request_timeout_secs: u64,

    /// Max request/response body size (bytes).
    #[arg(long, env = "CR_FHIR_BRIDGE_MAX_BODY_SIZE", default_value = "10485760")]
    pub max_body_size: usize,

    /// JVM clinical reasoning sidecar base URL for `$apply` (unset disables apply routes).
    #[arg(
        long,
        env = "CR_FHIR_BRIDGE_SIDECAR_URL",
        default_value = "http://127.0.0.1:8088"
    )]
    pub sidecar_url: Option<String>,

    /// HTS base URL forwarded to the sidecar on `$apply`.
    #[arg(
        long,
        env = "CR_FHIR_BRIDGE_HTS_URL",
        default_value = "http://127.0.0.1:8090"
    )]
    pub hts_url: String,

    /// Public bridge base URL used as sidecar `hfsBaseUrl` during `$apply` (defaults to listen URL).
    #[arg(long, env = "CR_FHIR_BRIDGE_PUBLIC_URL")]
    pub public_url: Option<String>,
}

impl Args {
    pub fn socket_addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port)
            .parse()
            .expect("valid listen address")
    }

    pub fn upstream_base(&self) -> String {
        trim_trailing_slash(self.upstream_url.clone())
    }

    pub fn kr_base(&self) -> Option<String> {
        self.kr_url
            .as_ref()
            .map(|url| trim_trailing_slash(url.clone()))
            .filter(|url| !url.is_empty())
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }

    pub fn bridge_public_base(&self) -> String {
        self.public_url
            .as_ref()
            .map(|url| trim_trailing_slash(url.clone()))
            .filter(|url| !url.is_empty())
            .unwrap_or_else(|| format!("http://{}:{}", self.host, self.port))
    }

    pub fn sidecar_base(&self) -> Option<String> {
        self.sidecar_url
            .as_ref()
            .map(|url| trim_trailing_slash(url.clone()))
            .filter(|url| !url.is_empty())
    }

    pub fn hts_base(&self) -> String {
        trim_trailing_slash(self.hts_url.clone())
    }

    pub fn load_mapper(&self) -> anyhow::Result<RuntimeMapper> {
        let manifest = match &self.mapper_manifest_path {
            Some(path) => MapperManifest::from_json_file(path)?,
            None => MapperManifest::full_inventory(),
        };
        Ok(RuntimeMapper::new(manifest))
    }
}
