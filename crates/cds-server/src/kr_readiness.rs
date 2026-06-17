//! KR readiness probes for cds-server startup and `GET /ready`.
//!
//! Validates manifest pins against the Knowledge Repository:
//! - **Library** `(libraryId, libraryVersion)` for legacy evaluate services
//! - **PlanDefinition** `planDefinitionId` for `$apply` services (slice 3)

use crate::kr_manifest::CdsServicesManifestFile;
use crate::library_version::{LibraryPin, collect_library_pins, probe_one_library};

/// Pinned `PlanDefinition` referenced by the CDS catalog (`$apply` path).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanDefinitionPin {
    pub id: String,
}

/// Combined result of KR library + PlanDefinition probes at startup.
#[derive(Debug, Clone)]
pub struct KrReadinessReport {
    pub library_pins: Vec<LibraryPin>,
    pub plan_definition_pins: Vec<PlanDefinitionPin>,
    pub ok: bool,
    pub message: String,
}

/// Collect unique `planDefinitionId` values from manifest services.
pub fn collect_plan_definition_pins(manifest: &CdsServicesManifestFile) -> Vec<PlanDefinitionPin> {
    use std::collections::HashSet;

    let mut seen = HashSet::new();
    let mut pins = Vec::new();
    for svc in &manifest.services {
        let Some(id) = svc
            .plan_definition_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        if seen.insert(id.to_string()) {
            pins.push(PlanDefinitionPin { id: id.to_string() });
        }
    }
    pins.sort_by(|a, b| a.id.cmp(&b.id));
    pins
}

/// Probe KR for all manifest library and PlanDefinition pins.
pub async fn probe_kr_readiness(
    http: &reqwest::Client,
    kr_base: &str,
    manifest: &CdsServicesManifestFile,
) -> KrReadinessReport {
    let library_pins = collect_library_pins(manifest);
    let plan_definition_pins = collect_plan_definition_pins(manifest);

    if library_pins.is_empty() && plan_definition_pins.is_empty() {
        return KrReadinessReport {
            library_pins,
            plan_definition_pins,
            ok: true,
            message: "no KR pins to probe".into(),
        };
    }

    let mut failures = Vec::new();

    for pin in &library_pins {
        if let Err(e) = probe_one_library(http, kr_base, pin).await {
            failures.push(format!(
                "Library {}@{}: {e}",
                pin.library_id, pin.library_version
            ));
        }
    }

    for pin in &plan_definition_pins {
        if let Err(e) = probe_one_plan_definition(http, kr_base, pin).await {
            failures.push(format!("PlanDefinition {}: {e}", pin.id));
        }
    }

    let ok = failures.is_empty();
    let message = if ok {
        format!(
            "KR readiness ok ({} library pin(s), {} PlanDefinition(s))",
            library_pins.len(),
            plan_definition_pins.len()
        )
    } else {
        failures.join("; ")
    };

    KrReadinessReport {
        library_pins,
        plan_definition_pins,
        ok,
        message,
    }
}

async fn probe_one_plan_definition(
    http: &reqwest::Client,
    kr_base: &str,
    pin: &PlanDefinitionPin,
) -> Result<(), String> {
    let kr_base = kr_base.trim_end_matches('/');
    let read_url = format!("{kr_base}/PlanDefinition/{}", pin.id);

    let resp = http
        .get(&read_url)
        .header(reqwest::header::ACCEPT, "application/fhir+json")
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err("PlanDefinition not found on KR".into());
    }

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "GET PlanDefinition/{} failed: HTTP {status} {body}",
            pin.id
        ));
    }

    let val: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("invalid JSON: {e}"))?;

    if val.get("resourceType").and_then(|t| t.as_str()) != Some("PlanDefinition") {
        return Err("response is not a PlanDefinition resource".into());
    }

    let id = val.get("id").and_then(|v| v.as_str()).unwrap_or_default();
    if id != pin.id {
        return Err(format!(
            "PlanDefinition id mismatch: expected {}, found {id}",
            pin.id
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kr_manifest::parse_manifest_json;

    #[test]
    fn collect_plan_definition_pins_dedupes() {
        let j = r#"{"services":[
            {"id":"a","hook":"patient-view","description":"d","libraryId":"L","libraryVersion":"1","expression":"E",
             "planDefinitionId":"pd1"},
            {"id":"b","hook":"patient-view","description":"d","libraryId":"L2","libraryVersion":"2","expression":"E2",
             "planDefinitionId":"pd1"},
            {"id":"c","hook":"patient-view","description":"d","libraryId":"L3","libraryVersion":"3","expression":"E3",
             "planDefinitionId":"pd2"}
        ]}"#;
        let m = parse_manifest_json(j.as_bytes()).unwrap();
        let pins = collect_plan_definition_pins(&m);
        assert_eq!(pins.len(), 2);
        assert!(pins.contains(&PlanDefinitionPin { id: "pd1".into() }));
        assert!(pins.contains(&PlanDefinitionPin { id: "pd2".into() }));
    }

    #[tokio::test]
    async fn probe_kr_readiness_checks_plan_definition() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let srv = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/PlanDefinition/CMS165"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "resourceType": "PlanDefinition",
                "id": "CMS165",
                "status": "active"
            })))
            .mount(&srv)
            .await;

        let j = r#"{"services":[{
            "id":"svc","hook":"patient-view","description":"d",
            "planDefinitionId":"CMS165","libraryId":"L","libraryVersion":"1","expression":"E"
        }]}"#;
        let m = parse_manifest_json(j.as_bytes()).unwrap();

        Mock::given(method("GET"))
            .and(path("/Library/L"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "resourceType": "Library",
                "id": "L",
                "name": "L",
                "version": "1"
            })))
            .mount(&srv)
            .await;

        let http = reqwest::Client::new();
        let report = probe_kr_readiness(&http, &srv.uri(), &m).await;
        assert!(report.ok, "{}", report.message);
        assert_eq!(report.plan_definition_pins.len(), 1);
        assert_eq!(report.library_pins.len(), 1);
    }

    #[tokio::test]
    async fn probe_kr_readiness_fails_missing_plan_definition() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let srv = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/PlanDefinition/Missing"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&srv)
            .await;

        let j = r#"{"services":[{
            "id":"svc","hook":"patient-view","description":"d",
            "planDefinitionId":"Missing","libraryId":"","expression":""
        }]}"#;
        let m = parse_manifest_json(j.as_bytes()).unwrap();

        let http = reqwest::Client::new();
        let report = probe_kr_readiness(&http, &srv.uri(), &m).await;
        assert!(!report.ok);
        assert!(report.message.contains("PlanDefinition Missing"));
    }
}
