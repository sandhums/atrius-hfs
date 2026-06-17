//! Live HTTP roundtrip: **Rust `ClinicalReasoningClient` → JVM sidecar → HFS / HTS** (URLs you configure).
//!
//! Build and run (starts nothing — you must run sidecar + HFS + HTS yourself when your ELM needs them):
//!
//! ```text
//! LIBRARY_ID=MyLib EXPRESSION=MyDefine ELM_PATH=./MyLib.elm.json \
//!   cargo run -p atrius-clinical-reasoning \
//!   --example live_sidecar_roundtrip --features integration-demo
//! ```
//!
//! Resolve ELM from FHIR `Library` on the sidecar (omit inline ELM):
//!
//! ```text
//! LIBRARY_ID=MyLib EXPRESSION=MyDefine RESOLVE_FROM_FHIR=true \
//!   LIBRARY_BASE_URL=http://127.0.0.1:8080 \
//!   cargo run -p atrius-clinical-reasoning \
//!   --example live_sidecar_roundtrip --features integration-demo
//! ```
//!
//! # Environment
//!
//! | Variable | Default | Purpose |
//! |----------|---------|---------|
//! | `CR_SIDECAR_URL` | `http://127.0.0.1:8088` | JVM sidecar base (matches typical `SIDECAR_PORT`) |
//! | `HFS_BASE_URL` | `http://127.0.0.1:8080` | `hfsBaseUrl` — clinical retrieves |
//! | `HTS_BASE_URL` | `http://127.0.0.1:8090` | `htsBaseUrl` — terminology |
//! | `LIBRARY_BASE_URL` | — | Optional `libraryBaseUrl` for `GET Library/{id}` |
//! | `RESOLVE_FROM_FHIR` | `true` | If `true`, inline ELM optional; if `false`, require `ELM` or `ELM_PATH` |
//! | `ELM_PATH` | — | Path to ELM document file (JSON or XML) |
//! | `ELM` | — | Inline ELM string if `ELM_PATH` unset |
//! | `LIBRARY_ID` | — | **Required** — must match ELM library identifier |
//! | `LIBRARY_VERSION` | — | Optional — must match ELM if present |
//! | `EXPRESSION` | — | **Required** — definition name to evaluate |
//! | `ELM_FORMAT` | `auto` | `auto`, `json`, or `xml` |
//! | `PATIENT_ID` | — | Optional logical id for evaluation context |
//! | `EVALUATION_DATE_TIME` | — | Optional ISO-8601 for engine |
//! | `PARAMETERS_JSON` | — | Optional JSON object merged into `parameters` |
//! | `REQUEST_TIMEOUT_SECS` | `120` | HTTP timeout for sidecar |
//!
//! On success, prints pretty-printed JSON with `expression`, `resultType`, `result`, and `normalizedResultKind`.

use std::env;
use std::fs;
use std::time::Duration;

use atrius_clinical_reasoning::{
    ClinicalReasoningClient, ClinicalReasoningConfig, ElmFormat, EvaluateExpressionRequest,
    NormalizedSidecarResult,
};

fn trim_trailing_slash(mut s: String) -> String {
    while s.ends_with('/') {
        s.pop();
    }
    s
}

fn env_trim(key: &str, default: &str) -> String {
    trim_trailing_slash(
        env::var(key)
            .unwrap_or_else(|_| default.to_string())
            .trim()
            .to_string(),
    )
}

fn parse_elm_format(s: &str) -> Result<ElmFormat, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "auto" => Ok(ElmFormat::Auto),
        "json" => Ok(ElmFormat::Json),
        "xml" => Ok(ElmFormat::Xml),
        other => Err(format!(
            "ELM_FORMAT must be auto, json, or xml; got {other:?}"
        )),
    }
}

fn parse_bool_env(key: &str, default: bool) -> Result<bool, String> {
    match env::var(key) {
        Ok(s) => match s.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            other => Err(format!("{key}: expected true/false, got {other:?}")),
        },
        Err(_) => Ok(default),
    }
}

fn normalized_kind_label(n: &NormalizedSidecarResult) -> &'static str {
    match n {
        NormalizedSidecarResult::Null => "null",
        NormalizedSidecarResult::Bool(_) => "bool",
        NormalizedSidecarResult::Number(_) => "number",
        NormalizedSidecarResult::String(_) => "string",
        NormalizedSidecarResult::Array(_) => "array",
        NormalizedSidecarResult::Object(_) => "object",
        NormalizedSidecarResult::FhirResource(_) => "fhirResource",
    }
}

fn usage() -> &'static str {
    "live_sidecar_roundtrip: missing LIBRARY_ID and/or EXPRESSION.\n\
     When RESOLVE_FROM_FHIR=false, set ELM or ELM_PATH.\n\
     See module docs in examples/live_sidecar_roundtrip.rs"
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let library_id = env::var("LIBRARY_ID").map_err(|_| usage())?;
    let expression = env::var("EXPRESSION").map_err(|_| usage())?;

    let resolve_from_fhir = parse_bool_env("RESOLVE_FROM_FHIR", true)?;

    let elm = if let Ok(path) = env::var("ELM_PATH") {
        Some(fs::read_to_string(path.trim()).map_err(|e| format!("read ELM_PATH: {e}"))?)
    } else {
        env::var("ELM").ok()
    };

    if !resolve_from_fhir && elm.as_ref().map(|s| s.trim().is_empty()).unwrap_or(true) {
        return Err(usage().into());
    }

    let library_version = env::var("LIBRARY_VERSION")
        .ok()
        .filter(|s| !s.trim().is_empty());

    let elm_format = parse_elm_format(&env_trim("ELM_FORMAT", "auto"))?;

    let patient_id = env::var("PATIENT_ID").ok().filter(|s| !s.trim().is_empty());

    let evaluation_date_time = env::var("EVALUATION_DATE_TIME")
        .ok()
        .filter(|s| !s.trim().is_empty());

    let parameters = match env::var("PARAMETERS_JSON") {
        Ok(s) if !s.trim().is_empty() => {
            let v: serde_json::Value =
                serde_json::from_str(s.trim()).map_err(|e| format!("PARAMETERS_JSON: {e}"))?;
            if !v.is_object() {
                return Err("PARAMETERS_JSON must be a JSON object".into());
            }
            Some(v)
        }
        _ => None,
    };

    let timeout_secs: u64 = env::var("REQUEST_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);

    let sidecar_base = env_trim("CR_SIDECAR_URL", "http://127.0.0.1:8088");
    let mut cfg = ClinicalReasoningConfig::new(sidecar_base);
    cfg.request_timeout = Duration::from_secs(timeout_secs);

    let client = ClinicalReasoningClient::new(cfg).map_err(|e| format!("client: {e}"))?;

    let library_base_url = env::var("LIBRARY_BASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|s| trim_trailing_slash(s.trim().to_string()));

    let req = EvaluateExpressionRequest {
        elm: elm.filter(|s| !s.trim().is_empty()),
        elm_format,
        library_id,
        library_version,
        expression,
        hfs_base_url: env_trim("HFS_BASE_URL", "http://127.0.0.1:8080"),
        hts_base_url: env_trim("HTS_BASE_URL", "http://127.0.0.1:8090"),
        library_base_url,
        resolve_library_artifacts_from_fhir: resolve_from_fhir,
        included_libraries: Vec::new(),
        patient_id,
        parameters,
        evaluation_date_time,
        prefetch: None,
        fhir_authorization: None,
    };

    let resp = client
        .evaluate_expression(req)
        .await
        .map_err(|e| format!("evaluate_expression: {e}"))?;

    let norm = resp.normalized_result();
    let out = serde_json::json!({
        "expression": resp.expression,
        "resultType": resp.result_type,
        "result": resp.result,
        "normalizedResultKind": normalized_kind_label(&norm),
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&out).map_err(|e| format!("serialize: {e}"))?
    );

    Ok(())
}
