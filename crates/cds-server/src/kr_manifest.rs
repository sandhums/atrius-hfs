//! Load CDS Hooks **service catalog** from the Knowledge Repository or local JSON.
//!
//! # Manifest sources (priority)
//!
//! 1. **`CDS_SERVICES_MANIFEST_PATH`** — local file (development / CI).
//! 2. **`CDS_KR_SERVICES_BINARY_ID`** — `GET {CDS_LIBRARY_BASE_URL}/Binary/{id}`, decode base64 JSON.
//! 3. Built-in [`demo_manifest`] — single placeholder service when neither is configured.
//!
//! Each [`ManifestService`] row defines both CDS Hooks discovery metadata (`id`, `hook`, `prefetch`, …)
//! and sidecar evaluation targets (`libraryId`, `expression`, `libraryVersion`, `resolveFromFhir`).
//!
//! Store production catalogs on KR as FHIR R4 `Binary` with `contentType: application/json` so
//! service definitions version with knowledge artifacts. See `cds-services.manifest.example.json`.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Context as _;
use base64::Engine as _;
use helios_cds_hooks::is_library_hook;
use serde::Deserialize;

use crate::config::EvalTargets;

/// JSON payload stored in `Binary.data` (base64) or on disk — KR-driven CDS definitions.
#[derive(Debug, Clone, Deserialize)]
pub struct CdsServicesManifestFile {
    pub services: Vec<ManifestService>,
}

/// One CDS Hooks service plus JVM evaluation targets for the sidecar.
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestService {
    pub id: String,
    pub hook: String,
    #[serde(default)]
    pub title: Option<String>,
    pub description: String,
    #[serde(default)]
    pub prefetch: Option<HashMap<String, String>>,
    /// FHIR `Library.id` (legacy evaluate/expression path).
    #[serde(rename = "libraryId", default)]
    pub library_id: String,
    #[serde(default)]
    pub expression: String,
    #[serde(rename = "libraryVersion")]
    pub library_version: Option<String>,
    #[serde(rename = "resolveFromFhir", default = "default_resolve")]
    pub resolve_from_fhir: bool,
    /// KR PlanDefinition logical id for `$apply` (preferred when present).
    #[serde(rename = "planDefinitionId")]
    pub plan_definition_id: Option<String>,
    #[serde(rename = "planDefinitionUrl")]
    pub plan_definition_url: Option<String>,
    #[serde(rename = "usageRequirements")]
    pub usage_requirements: Option<String>,
    /// CDS Hooks discovery `version` field (e.g. STU2).
    #[serde(rename = "cdsHooksVersion")]
    pub cds_hooks_version: Option<String>,
}

fn default_resolve() -> bool {
    true
}

impl ManifestService {
    pub fn eval_targets(&self) -> EvalTargets {
        EvalTargets {
            library_id: self.library_id.clone(),
            expression: self.expression.clone(),
            library_version: self.library_version.clone(),
            resolve_from_fhir: self.resolve_from_fhir,
        }
    }
}

/// CDS Hooks prefetch templates aligned with `scripts/cds_manifest_common.py` (`STANDARD_PATIENT_CHART_PREFETCH`).
fn standard_chart_prefetch() -> HashMap<String, String> {
    HashMap::from([
        ("patient".into(), "Patient/{{context.patientId}}".into()),
        (
            "conditions".into(),
            "Condition?patient={{context.patientId}}".into(),
        ),
        (
            "encounters".into(),
            "Encounter?patient={{context.patientId}}".into(),
        ),
        (
            "observations".into(),
            "Observation?patient={{context.patientId}}".into(),
        ),
        (
            "procedures".into(),
            "Procedure?patient={{context.patientId}}".into(),
        ),
        (
            "medicationRequests".into(),
            "MedicationRequest?patient={{context.patientId}}".into(),
        ),
        (
            "immunizations".into(),
            "Immunization?patient={{context.patientId}}".into(),
        ),
        (
            "diagnosticReports".into(),
            "DiagnosticReport?patient={{context.patientId}}".into(),
        ),
        (
            "serviceRequests".into(),
            "ServiceRequest?patient={{context.patientId}}".into(),
        ),
        (
            "allergies".into(),
            "AllergyIntolerance?patient={{context.patientId}}".into(),
        ),
        (
            "coverage".into(),
            "Coverage?beneficiary=Patient/{{context.patientId}}".into(),
        ),
    ])
}

/// Built-in single service for demo mode (no sidecar / no KR manifest configured).
pub fn demo_manifest() -> CdsServicesManifestFile {
    CdsServicesManifestFile {
        services: vec![ManifestService {
            id: "atrius-patient-view".into(),
            hook: "patient-view".into(),
            title: Some("Demo patient-view CDS".into()),
            description: "Demo mode — configure KR Binary or CDS_SERVICES_MANIFEST_PATH and CDS_CLINICAL_REASONING_URL."
                .into(),
            prefetch: Some(standard_chart_prefetch()),
            library_id: "_demo_".into(),
            expression: "_demo_".into(),
            library_version: None,
            resolve_from_fhir: true,
            plan_definition_id: None,
            plan_definition_url: None,
            usage_requirements: None,
            cds_hooks_version: Some("1.0".into()),
        }],
    }
}

/// Parse and validate manifest bytes (JSON).
pub fn parse_manifest_json(raw: &[u8]) -> anyhow::Result<CdsServicesManifestFile> {
    let m: CdsServicesManifestFile =
        serde_json::from_slice(raw).context("parse CDS services manifest JSON")?;

    if m.services.is_empty() {
        anyhow::bail!("CDS services manifest must declare at least one service");
    }

    let mut seen = HashSet::new();
    for s in &m.services {
        if s.id.trim().is_empty() {
            anyhow::bail!("manifest service id must not be empty");
        }
        if s.hook.trim().is_empty() {
            anyhow::bail!("manifest service `{}`: hook must not be empty", s.id);
        }
        if s.library_id.trim().is_empty() || s.expression.trim().is_empty() {
            let has_apply = s
                .plan_definition_id
                .as_ref()
                .is_some_and(|x| !x.trim().is_empty())
                || s.plan_definition_url
                    .as_ref()
                    .is_some_and(|x| !x.trim().is_empty());
            if !has_apply {
                anyhow::bail!(
                    "manifest service `{}`: planDefinitionId/url or libraryId+expression required",
                    s.id
                );
            }
        }
        if !seen.insert(s.id.clone()) {
            anyhow::bail!("duplicate CDS service id in manifest: `{}`", s.id);
        }
        if !is_library_hook(&s.hook) {
            anyhow::bail!(
                "manifest service `{}`: unsupported hook `{}` (supported: {})",
                s.id,
                s.hook,
                helios_cds_hooks::LIBRARY_HOOK_NAMES.join(", ")
            );
        }
    }

    Ok(m)
}

/// Read manifest from a local path (JSON).
pub async fn load_manifest_from_path(
    path: impl AsRef<Path>,
) -> anyhow::Result<CdsServicesManifestFile> {
    let raw = tokio::fs::read(path.as_ref())
        .await
        .with_context(|| format!("read manifest {:?}", path.as_ref()))?;
    parse_manifest_json(&raw)
}

/// Fetch `GET {kr_base}/Binary/{binary_id}` (`Accept: application/fhir+json`) and decode JSON payload from `Binary.data`.
pub async fn fetch_manifest_from_kr_binary(
    http: &reqwest::Client,
    kr_base: &str,
    binary_id: &str,
) -> anyhow::Result<CdsServicesManifestFile> {
    let kr_base = kr_base.trim_end_matches('/');
    let url = format!("{kr_base}/Binary/{}", binary_id.trim_matches('/'));
    let resp = http
        .get(url)
        .header(reqwest::header::ACCEPT, "application/fhir+json")
        .send()
        .await
        .context("KR Binary GET request")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("KR Binary read failed: HTTP {status} {body}");
    }

    let val: serde_json::Value = resp.json().await.context("KR Binary response JSON")?;
    let data_b64 = val
        .get("data")
        .and_then(|v| v.as_str())
        .context("Binary.resource missing base64 `data` field")?;

    let raw = base64::engine::general_purpose::STANDARD
        .decode(data_b64.trim())
        .context("decode Binary.data base64")?;

    parse_manifest_json(&raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_manifest() {
        let j = r#"{"services":[{"id":"a","hook":"patient-view","description":"d","libraryId":"L","expression":"E"}]}"#;
        let m = parse_manifest_json(j.as_bytes()).unwrap();
        assert_eq!(m.services.len(), 1);
        assert_eq!(m.services[0].id, "a");
    }

    #[test]
    fn rejects_duplicate_ids() {
        let j = r#"{"services":[
            {"id":"x","hook":"patient-view","description":"d","libraryId":"L","expression":"E"},
            {"id":"x","hook":"patient-view","description":"d","libraryId":"L","expression":"E"}
        ]}"#;
        assert!(parse_manifest_json(j.as_bytes()).is_err());
    }

    #[test]
    fn parses_manifest_with_plan_definition_only() {
        let j = r#"{"services":[{"id":"a","hook":"patient-view","description":"d","planDefinitionId":"pd1"}]}"#;
        let m = parse_manifest_json(j.as_bytes()).unwrap();
        assert_eq!(m.services[0].plan_definition_id.as_deref(), Some("pd1"));
    }

    #[test]
    fn rejects_service_without_targets() {
        let j = r#"{"services":[{"id":"a","hook":"patient-view","description":"d"}]}"#;
        assert!(parse_manifest_json(j.as_bytes()).is_err());
    }

    #[test]
    fn accepts_all_library_hooks() {
        for hook in helios_cds_hooks::LIBRARY_HOOK_NAMES {
            let j = format!(
                r#"{{"services":[{{"id":"svc-{hook}","hook":"{hook}","description":"d","libraryId":"L","expression":"E"}}]}}"#
            );
            parse_manifest_json(j.as_bytes()).unwrap_or_else(|e| panic!("hook {hook}: {e}"));
        }
    }

    #[test]
    fn rejects_unknown_hook() {
        let j = r#"{"services":[{"id":"a","hook":"not-a-real-hook","description":"d","libraryId":"L","expression":"E"}]}"#;
        assert!(parse_manifest_json(j.as_bytes()).is_err());
    }
}
