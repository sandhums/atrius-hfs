//! HTTP patterns exercised by CQFramework [`R4FhirTerminologyProvider`](https://github.com/cqframework/clinical_quality_language/blob/main/Src/java/engine-fhir/src/main/kotlin/org/opencds/cqf/cql/engine/fhir/terminology/R4FhirTerminologyProvider.kt).
//!
//! Verification target for plan item **hts-terminology-ops**: HTS aligns with terminology
//! calls the JVM CQL engine issues against a FHIR `IGenericClient`. Narrative gaps:
//! [`docs/cqframework_terminology.md`](../docs/cqframework_terminology.md).

mod common;

#[cfg(feature = "sqlite")]
use axum::http::StatusCode;
#[cfg(feature = "sqlite")]
use common::{TestApp, bundles};

/// Percent-encode `s` as a single `application/x-www-form-urlencoded` component.
#[cfg(feature = "sqlite")]
fn qp_component(s: &str) -> String {
    form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/// `POST /CodeSystem/$lookup` — CQFramework uses `CodeType` + `Uri`; JSON uses `valueCode` / `valueUri`.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn cq_post_codesystem_lookup_value_uri_and_code() {
    let app = TestApp::new();
    app.import_bundle_ok(bundles::r4_bundle()).await;

    let body = common::build_params(&[
        ("system", "valueUri", bundles::ANATOMY_CS_URL),
        ("code", "valueCode", "arm"),
    ]);
    let (status, json) = app.post_fhir("/CodeSystem/$lookup", body).await;
    assert_eq!(status, StatusCode::OK, "{json}");

    let display = json["parameter"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "display")
        .and_then(|p| p["valueString"].as_str())
        .expect("display parameter");
    assert_eq!(display, "Arm");
}

/// `GET /ValueSet/{id}/$validate-code?code=&system=` — CQFramework uses GET with plain string query params (`StringType` in Parameters form).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn cq_get_valueset_instance_validate_code_code_and_system() {
    let app = TestApp::new();
    app.import_bundle_ok(bundles::r4_bundle()).await;

    let sys = qp_component(bundles::ANATOMY_CS_URL);
    let path = format!("/ValueSet/limbs/$validate-code?code=arm&system={sys}");

    let (status, json) = app.get_fhir(&path).await;
    assert_eq!(status, StatusCode::OK, "{json}");

    let ok = json["parameter"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "result")
        .and_then(|p| p["valueBoolean"].as_bool())
        .expect("result boolean");
    assert!(ok);
}

/// `GET /ValueSet/{id}/$expand` — no Parameters body (CQFramework `withNoParameters` + GET).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn cq_get_valueset_instance_expand_returns_valueset() {
    let app = TestApp::new();
    app.import_bundle_ok(bundles::r4_bundle()).await;

    let (status, json) = app.get_fhir("/ValueSet/limbs/$expand").await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["resourceType"], "ValueSet");
    let contains = json["expansion"]["contains"]
        .as_array()
        .expect("expansion.contains");
    let codes: Vec<&str> = contains.iter().filter_map(|c| c["code"].as_str()).collect();
    assert!(
        codes.contains(&"arm") && codes.contains(&"leg"),
        "unexpected expansion codes: {codes:?}"
    );
}

/// `GET /ValueSet?url=` — CQFramework resolves canonical URL before GET instance operations.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn cq_search_valueset_by_url_single_match_bundle() {
    let app = TestApp::new();
    app.import_bundle_ok(bundles::r4_bundle()).await;

    let url_enc = qp_component(bundles::LIMBS_VS_URL);
    let path = format!("/ValueSet?url={url_enc}");

    let (status, json) = app.get_fhir(&path).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["resourceType"], "Bundle");
    let entries = json["entry"].as_array().expect("Bundle.entry");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["resource"]["resourceType"], "ValueSet");
    assert_eq!(entries[0]["resource"]["id"], "limbs");
}
