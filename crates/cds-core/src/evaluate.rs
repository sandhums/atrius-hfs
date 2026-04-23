//! **Domain** evaluation. Replace/extend with rules, PlanDefinition `$apply`, and FHIR client calls.
//!
//! [`patient_view_greeting`] receives the full [`CdsRequest`](helios_cds_hooks::CdsRequest) so you can
//! use [`CdsRequest::prefetch`](helios_cds_hooks::CdsRequest::prefetch), `fhir_server`, and
//! `fhir_authorization` for additional reads in addition to the typed hook context.

use std::time::Duration;

use helios_cds_hooks::CdsRequest;
use helios_cds_hooks::hooks::PatientViewContext;

use crate::fhir_fetch::try_patient_display_name;

/// Placeholder delay so call sites stay `async` like future longer reasoning.
const STUB_YIELD: Duration = Duration::from_millis(0);

/// `patient-view` summary line for the card, using context plus EHR `CdsRequest` data.
///
/// * [`PatientViewContext`] — `user_id`, `patient_id`, `encounter_id`.
/// * If both `fhir_server` and `fhir_authorization` are set, runs [`try_patient_display_name`](crate::fhir_fetch::try_patient_display_name) (`GET Patient/{id}`) and, on success, appends a human name from the EHR.
/// * Lists non-empty `prefetch` keys for debugging when no EHR name was merged.
pub async fn patient_view_greeting(request: &CdsRequest, context: &PatientViewContext) -> String {
    tokio::time::sleep(STUB_YIELD).await;
    let mut out = format!(
        "Hello, patient {} (user {}) — cds-core",
        context.patient_id, context.user_id
    );
    if let Some(enc) = &context.encounter_id {
        out.push_str(&format!(" [encounter {enc}]"));
    }

    if let (Some(base), Some(auth)) = (&request.fhir_server, &request.fhir_authorization) {
        if let Some(display) = try_patient_display_name(base, auth, &context.patient_id).await {
            out.push_str(&format!(" · EHR: {display}"));
        }
    } else {
        if request.fhir_server.is_some() {
            out.push_str(" · fhirServer set (pair with fhirAuthorization to read EHR)");
        }
        if request.fhir_authorization.is_some() {
            out.push_str(" · fhirAuthorization set (pair with fhirServer to read EHR)");
        }
    }

    if let Some(pref) = &request.prefetch {
        if !pref.is_empty() {
            let mut keys: Vec<_> = pref.keys().map(String::as_str).collect();
            keys.sort();
            out.push_str(&format!(" · prefetch: [{}]", keys.join(", ")));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use helios_cds_hooks::CdsRequest;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn sample_context() -> PatientViewContext {
        PatientViewContext {
            user_id: "Practitioner/1".to_string(),
            patient_id: "1288992".to_string(),
            encounter_id: None,
        }
    }

    fn minimal_request() -> CdsRequest {
        CdsRequest {
            hook: "patient-view".to_string(),
            hook_instance: "00000000-0000-0000-0000-000000000001".to_string(),
            fhir_server: None,
            fhir_authorization: None,
            context: serde_json::json!({}),
            prefetch: None,
            extension: None,
        }
    }

    #[tokio::test]
    async fn greeting_contains_patient_and_user() {
        let s = patient_view_greeting(&minimal_request(), &sample_context()).await;
        assert!(s.contains("1288992"), "{s}");
        assert!(s.contains("Practitioner/1"), "{s}");
    }

    #[tokio::test]
    async fn greeting_includes_prefetch_keys_without_fhir() {
        let request = CdsRequest {
            hook: "patient-view".to_string(),
            hook_instance: "u".to_string(),
            fhir_server: None,
            fhir_authorization: None,
            context: serde_json::json!({}),
            prefetch: Some(HashMap::from([(
                "patientToGreet".to_string(),
                Some(serde_json::json!({
                    "resourceType": "Patient",
                    "id": "1288992"
                })),
            )])),
            extension: None,
        };
        let s = patient_view_greeting(&request, &sample_context()).await;
        assert!(s.contains("patientToGreet"), "{s}");
    }

    #[tokio::test]
    async fn greeting_fetches_patient_name_from_mock_fhir() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/Patient/1288992"))
            .and(header("Accept", "application/fhir+json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "resourceType": "Patient",
                "id": "1288992",
                "name": [{ "family": "Doe", "given": ["Jane"] }]
            })))
            .mount(&server)
            .await;

        let request = CdsRequest {
            hook: "patient-view".to_string(),
            hook_instance: "u".to_string(),
            fhir_server: Some(server.uri()),
            fhir_authorization: Some(helios_cds_hooks::FhirAuthorization {
                access_token: "test-token".to_string(),
                token_type: "Bearer".to_string(),
                expires_in: 3600,
                scope: "patient/*.read".to_string(),
                subject: "client".to_string(),
                patient: None,
            }),
            context: serde_json::json!({}),
            prefetch: None,
            extension: None,
        };
        let s = patient_view_greeting(&request, &sample_context()).await;
        assert!(s.contains("EHR: Jane Doe"), "got: {s}");
    }
}
