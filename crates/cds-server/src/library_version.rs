//! KR **library version pinning** — manifest validation and readiness probes.
//!
//! Ensures each CDS service references an explicit `(libraryId, libraryVersion)` and that those
//! artifacts exist on the Knowledge Repository before serving invokes (when validation is enabled).

use std::collections::HashSet;

use helios_cds_hooks::CdsHooksError;

use crate::kr_manifest::{CdsServicesManifestFile, ManifestService};

/// Pinned knowledge artifact referenced by the CDS catalog.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LibraryPin {
    pub library_id: String,
    pub library_version: String,
}

/// Result of probing KR for all manifest pins.
#[derive(Debug, Clone)]
pub struct KrLibraryProbeReport {
    pub pins: Vec<LibraryPin>,
    pub ok: bool,
    pub message: String,
}

/// Policy for library version enforcement.
#[derive(Debug, Clone, Default)]
pub struct LibraryVersionPolicy {
    pub require_version_on_manifest: bool,
    pub validate_kr_on_startup: bool,
}

impl LibraryVersionPolicy {
    pub fn from_config(require_library_version: bool, validate_kr_libraries: bool) -> Self {
        Self {
            require_version_on_manifest: require_library_version,
            validate_kr_on_startup: validate_kr_libraries,
        }
    }
}

/// Collect unique `(libraryId, libraryVersion)` pins from manifest services that evaluate CQL.
pub fn collect_library_pins(manifest: &CdsServicesManifestFile) -> Vec<LibraryPin> {
    let mut seen = HashSet::new();
    let mut pins = Vec::new();
    for svc in &manifest.services {
        if let Some(pin) = pin_from_service(svc) {
            if seen.insert((pin.library_id.clone(), pin.library_version.clone())) {
                pins.push(pin);
            }
        }
    }
    pins.sort_by(|a, b| {
        (&a.library_id, &a.library_version).cmp(&(&b.library_id, &b.library_version))
    });
    pins
}

fn pin_from_service(svc: &ManifestService) -> Option<LibraryPin> {
    let library_id = svc.library_id.trim();
    if library_id.is_empty() || library_id == "_demo_" {
        return None;
    }
    let uses_expression = !svc.expression.trim().is_empty();
    if !uses_expression {
        return None;
    }
    let version = svc.library_version.as_deref()?.trim();
    if version.is_empty() {
        return None;
    }
    Some(LibraryPin {
        library_id: library_id.to_string(),
        library_version: version.to_string(),
    })
}

/// Validate manifest rows declare `libraryVersion` when required.
pub fn validate_manifest_versions(
    manifest: &CdsServicesManifestFile,
    policy: &LibraryVersionPolicy,
) -> Result<(), anyhow::Error> {
    if !policy.require_version_on_manifest {
        return Ok(());
    }
    for svc in &manifest.services {
        if pin_from_service(svc).is_none() && service_needs_library_version(svc) {
            anyhow::bail!(
                "manifest service `{}`: libraryVersion is required (libraryId `{}`, expression `{}`)",
                svc.id,
                svc.library_id,
                svc.expression
            );
        }
    }
    Ok(())
}

fn service_needs_library_version(svc: &ManifestService) -> bool {
    let library_id = svc.library_id.trim();
    if library_id.is_empty() || library_id == "_demo_" {
        return false;
    }
    !svc.expression.trim().is_empty()
}

/// Ensure invoke targets include a pinned version when policy requires it.
pub fn ensure_invoke_library_version(
    library_id: &str,
    library_version: &Option<String>,
    policy: &LibraryVersionPolicy,
) -> Result<(), CdsHooksError> {
    if !policy.require_version_on_manifest {
        return Ok(());
    }
    let id = library_id.trim();
    if id.is_empty() || id == "_demo_" {
        return Ok(());
    }
    let ver = library_version
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if ver.is_none() {
        return Err(CdsHooksError::PreconditionFailed(format!(
            "libraryVersion is required for library `{id}` on this CDS server"
        )));
    }
    Ok(())
}

/// Probe KR FHIR server for each pinned Library (search by name + version).
pub async fn probe_kr_libraries(
    http: &reqwest::Client,
    kr_base: &str,
    pins: &[LibraryPin],
) -> KrLibraryProbeReport {
    if pins.is_empty() {
        return KrLibraryProbeReport {
            pins: vec![],
            ok: true,
            message: "no library pins to probe".into(),
        };
    }

    let mut failures = Vec::new();
    for pin in pins {
        if let Err(e) = probe_one_library(http, kr_base, pin).await {
            failures.push(format!("{}@{}: {e}", pin.library_id, pin.library_version));
        }
    }

    if failures.is_empty() {
        KrLibraryProbeReport {
            pins: pins.to_vec(),
            ok: true,
            message: format!("KR library probe ok ({} pin(s))", pins.len()),
        }
    } else {
        KrLibraryProbeReport {
            pins: pins.to_vec(),
            ok: false,
            message: failures.join("; "),
        }
    }
}

pub(crate) async fn probe_one_library(
    http: &reqwest::Client,
    kr_base: &str,
    pin: &LibraryPin,
) -> Result<(), String> {
    let kr_base = kr_base.trim_end_matches('/');

    // Prefer direct read (same as JVM sidecar) — KR search on name/version is not
    // reliable on all HFS deployments (e.g. token/string index gaps).
    let read_url = format!("{kr_base}/Library/{}", pin.library_id);
    if let Ok(resp) = http
        .get(&read_url)
        .header(reqwest::header::ACCEPT, "application/fhir+json")
        .send()
        .await
    {
        if resp.status().is_success() {
            let val: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| format!("invalid JSON: {e}"))?;
            if library_matches_pin(&val, pin) {
                return Ok(());
            }
            return Err(format!(
                "Library {} exists but name/version do not match pin",
                pin.library_id
            ));
        }
        if resp.status() != reqwest::StatusCode::NOT_FOUND {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!(
                "GET Library/{} failed: HTTP {status} {body}",
                pin.library_id
            ));
        }
    }

    // Fallback: FHIR search by name + version (legacy path).
    let mut url = reqwest::Url::parse(&format!("{kr_base}/Library"))
        .map_err(|e| format!("bad KR URL: {e}"))?;
    url.query_pairs_mut()
        .append_pair("name", &pin.library_id)
        .append_pair("version", &pin.library_version);

    let resp = http
        .get(url)
        .header(reqwest::header::ACCEPT, "application/fhir+json")
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("HTTP {status} {body}"));
    }

    let val: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("invalid JSON: {e}"))?;

    let entries = val
        .get("entry")
        .and_then(|e| e.as_array())
        .filter(|a| !a.is_empty())
        .ok_or_else(|| "Library search returned no entries".to_string())?;

    for entry in entries {
        let Some(resource) = entry.get("resource") else {
            continue;
        };
        if library_matches_pin(resource, pin) {
            return Ok(());
        }
    }

    Err("no matching Library resource in search Bundle".into())
}

fn library_matches_pin(resource: &serde_json::Value, pin: &LibraryPin) -> bool {
    if resource.get("resourceType").and_then(|t| t.as_str()) != Some("Library") {
        return false;
    }
    let id = resource
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let name = resource
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or_default();
    let version = resource
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let id_or_name_matches = id == pin.library_id || name == pin.library_id;
    id_or_name_matches && versions_compatible(version, &pin.library_version)
}

fn versions_compatible(found: &str, requested: &str) -> bool {
    if requested.is_empty() {
        return true;
    }
    found.is_empty() || found == requested
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kr_manifest::parse_manifest_json;

    #[test]
    fn collect_pins_dedupes_and_sorts() {
        let j = r#"{"services":[
            {"id":"a","hook":"patient-view","description":"d","libraryId":"L1","libraryVersion":"1.0","expression":"E1"},
            {"id":"b","hook":"patient-view","description":"d","libraryId":"L1","libraryVersion":"1.0","expression":"E2"},
            {"id":"c","hook":"patient-view","description":"d","libraryId":"L2","libraryVersion":"2.0","expression":"E3"}
        ]}"#;
        let m = parse_manifest_json(j.as_bytes()).unwrap();
        let pins = collect_library_pins(&m);
        assert_eq!(pins.len(), 2);
        assert!(pins.contains(&LibraryPin {
            library_id: "L1".into(),
            library_version: "1.0".into(),
        }));
    }

    #[test]
    fn require_version_fails_when_missing() {
        let j = r#"{"services":[{"id":"a","hook":"patient-view","description":"d","libraryId":"L","expression":"E"}]}"#;
        let m = parse_manifest_json(j.as_bytes()).unwrap();
        let policy = LibraryVersionPolicy {
            require_version_on_manifest: true,
            validate_kr_on_startup: false,
        };
        assert!(validate_manifest_versions(&m, &policy).is_err());
    }

    #[test]
    fn demo_library_id_skipped() {
        let j = r#"{"services":[{"id":"a","hook":"patient-view","description":"d","libraryId":"_demo_","expression":"E"}]}"#;
        let m = parse_manifest_json(j.as_bytes()).unwrap();
        assert!(collect_library_pins(&m).is_empty());
    }

    #[tokio::test]
    async fn probe_kr_libraries_finds_library_by_direct_read() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let srv = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/Library/L1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "resourceType": "Library",
                "id": "L1",
                "name": "L1",
                "version": "1.0.0"
            })))
            .mount(&srv)
            .await;

        let http = reqwest::Client::new();
        let pins = vec![LibraryPin {
            library_id: "L1".into(),
            library_version: "1.0.0".into(),
        }];
        let report = probe_kr_libraries(&http, &srv.uri(), &pins).await;
        assert!(report.ok, "{}", report.message);
    }

    #[tokio::test]
    async fn probe_kr_libraries_falls_back_to_search() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let srv = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/Library/MissingId"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&srv)
            .await;
        Mock::given(method("GET"))
            .and(path("/Library"))
            .and(query_param("name", "MissingId"))
            .and(query_param("version", "2.0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [{
                    "resource": {
                        "resourceType": "Library",
                        "name": "MissingId",
                        "version": "2.0"
                    }
                }]
            })))
            .mount(&srv)
            .await;

        let http = reqwest::Client::new();
        let pins = vec![LibraryPin {
            library_id: "MissingId".into(),
            library_version: "2.0".into(),
        }];
        let report = probe_kr_libraries(&http, &srv.uri(), &pins).await;
        assert!(report.ok, "{}", report.message);
    }
}
