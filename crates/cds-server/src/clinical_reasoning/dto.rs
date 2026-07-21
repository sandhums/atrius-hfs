//! Request / response payloads aligned with JVM sidecar
//! `EvaluateExpressionRequest` / `EvaluateExpressionResponse` (kotlinx.serialization JSON).
//!
//! # Sidecar endpoints
//!
//! - `POST {sidecarBase}/v1/evaluate/expression` — named CQL expression evaluation
//! - `POST {sidecarBase}/v1/plandefinition/apply` — FHIR **`PlanDefinition/$apply`** (returns CarePlan + RequestGroup)
//! - `POST {sidecarBase}/v1/activitydefinition/apply` — FHIR **`ActivityDefinition/$apply`** (returns draft request resource)
//!
//! # URL fields (critical for stack wiring)
//!
//! | Field | Sidecar use |
//! |-------|-------------|
//! | `hfsBaseUrl` | Clinical FHIR retrieve only |
//! | `libraryBaseUrl` | Primary + CQL `include` `Library` reads (KR; required when resolving from FHIR) |
//! | `htsBaseUrl` | Terminology (`$expand`, `$validate-code`, `$lookup`) |
//!
//! Legacy JSON keys `fhirDataUrl` / `fhirTerminologyUrl` deserialize to `hfsBaseUrl` / `htsBaseUrl`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::fhir_authorization::SidecarFhirAuthorization;
use super::normalized_result::NormalizedSidecarResult;

/// How to interpret ELM payload strings (`elmFormat` on JVM; `"json"` \| `"xml"` \| `"auto"`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ElmFormat {
    #[default]
    Auto,
    Json,
    Xml,
}

fn default_resolve_library_artifacts_from_fhir() -> bool {
    true
}

/// Included library for resolving ELM `include` definitions (`includedLibraries` on JVM).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncludedLibrary {
    /// Inline ELM for this include; omit when the sidecar resolves from classpath or FHIR `Library`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elm: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elm_format: Option<ElmFormat>,
    pub library_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub library_version: Option<String>,
}

/// Evaluate a named CQL (`Library`/ELM-backed) expression in the sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluateExpressionRequest {
    /// ELM document string (JSON or XML). Omitted when loading primary ELM from FHIR `Library`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "elmJson",
        deserialize_with = "deserialize_optional_elm"
    )]
    pub elm: Option<String>,
    #[serde(default)]
    pub elm_format: ElmFormat,
    pub library_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub library_version: Option<String>,
    pub expression: String,
    /// FHIR REST base for clinical data (`RestFhirRetrieveProvider`).
    #[serde(alias = "fhirDataUrl")]
    pub hfs_base_url: String,
    /// FHIR REST base for terminology (`R4FhirTerminologyProvider`).
    #[serde(alias = "fhirTerminologyUrl")]
    pub hts_base_url: String,
    /// FHIR base used for `GET Library/{libraryId}` when resolving artifacts; JVM defaults to `hfsBaseUrl` if omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub library_base_url: Option<String>,
    /// When true (default), blank inline `elm` loads ELM from FHIR `Library` via `libraryBaseUrl` / `hfsBaseUrl`.
    #[serde(default = "default_resolve_library_artifacts_from_fhir")]
    pub resolve_library_artifacts_from_fhir: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub included_libraries: Vec<IncludedLibrary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patient_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation_date_time: Option<String>,
    /// CDS Hooks prefetch map (key → FHIR resource or searchset Bundle JSON).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefetch: Option<serde_json::Map<String, Value>>,
    /// SMART bearer token for clinical FHIR (`hfsBaseUrl`). Omitted when using server-trust clinical access (no SMART token).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fhir_authorization: Option<SidecarFhirAuthorization>,
}

fn deserialize_optional_elm<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.filter(|s| !s.trim().is_empty()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluateExpressionResponse {
    pub expression: String,
    /// JVM class name when present; sidecar may omit or null in edge cases.
    #[serde(default)]
    pub result_type: Option<String>,
    pub result: Value,
}

impl EvaluateExpressionResponse {
    /// Decode [`Self::result`] for downstream use (scalars, collections, double-encoded FHIR JSON strings).
    #[must_use]
    pub fn normalized_result(&self) -> NormalizedSidecarResult {
        super::normalized_result::normalize_sidecar_result(&self.result)
    }
}

/// Invoke **`PlanDefinition/$apply`** on the JVM sidecar (CQF Clinical Reasoning).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyPlanDefinitionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_definition_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_definition_url: Option<String>,
    /// FHIR `$apply` **subject** (Patient logical id or reference).
    pub patient_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encounter_id: Option<String>,
    /// FHIR `$apply` **practitioner** (Practitioner / PractitionerRole reference or id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub practitioner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_type: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_language: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_task_context: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setting: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setting_context: Option<Value>,
    #[serde(alias = "fhirDataUrl")]
    pub hfs_base_url: String,
    #[serde(alias = "fhirTerminologyUrl")]
    pub hts_base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub library_base_url: Option<String>,
    #[serde(default)]
    pub use_server_data: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefetch: Option<serde_json::Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    /// SMART bearer token for clinical FHIR (`hfsBaseUrl`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fhir_authorization: Option<SidecarFhirAuthorization>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyPlanDefinitionResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_definition_id: Option<String>,
    /// Primary FHIR `$apply` result: CarePlan with activity → RequestGroup.
    pub care_plan: Value,
    /// Extracted RequestGroup for CDS card mapping.
    pub request_group: Value,
}

impl ApplyPlanDefinitionResponse {
    #[must_use]
    pub fn request_group_value(&self) -> &Value {
        &self.request_group
    }

    #[must_use]
    pub fn care_plan_value(&self) -> &Value {
        &self.care_plan
    }
}

/// Invoke **`ActivityDefinition/$apply`** on the JVM sidecar (CQF Clinical Reasoning).
///
/// CQF maps [ActivityDefinition] structural elements to the target resource ([kind]), resolves
/// participant/location from context, evaluates [dynamicValue] expressions (with `%parameter`
/// context variables), and applies optional StructureMap [transform].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyActivityDefinitionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_definition_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_definition_url: Option<String>,
    /// FHIR `$apply` **subject** (Patient logical id or reference).
    pub patient_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encounter_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub practitioner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_type: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_language: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_task_context: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setting: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setting_context: Option<Value>,
    #[serde(alias = "fhirDataUrl")]
    pub hfs_base_url: String,
    #[serde(alias = "fhirTerminologyUrl")]
    pub hts_base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub library_base_url: Option<String>,
    #[serde(default)]
    pub use_server_data: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefetch: Option<serde_json::Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fhir_authorization: Option<SidecarFhirAuthorization>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyActivityDefinitionResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_definition_id: Option<String>,
    /// FHIR `$apply` **return** parameter — draft request/event resource (transient, not persisted).
    pub resource: Value,
}

impl ApplyActivityDefinitionResponse {
    #[must_use]
    pub fn resource_value(&self) -> &Value {
        &self.resource
    }
}

/// Response from sidecar `POST /v1/admin/cache/libraries/clear`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClearLibraryCacheResponse {
    pub cleared: Vec<String>,
    pub evaluation_stacks_removed: u32,
    pub fhir_library_resources_removed: u32,
    pub terminology_expansion_buckets_removed: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserializes_legacy_fhir_url_keys() {
        let j = json!({
            "elm": "{}",
            "libraryId": "L",
            "expression": "E",
            "fhirDataUrl": "http://hfs",
            "fhirTerminologyUrl": "http://hts"
        });
        let r: EvaluateExpressionRequest = serde_json::from_value(j).unwrap();
        assert_eq!(r.hfs_base_url, "http://hfs");
        assert_eq!(r.hts_base_url, "http://hts");
        assert!(r.resolve_library_artifacts_from_fhir);
        assert_eq!(r.elm.as_deref(), Some("{}"));
    }

    #[test]
    fn deserializes_elm_json_alias() {
        let j = json!({
            "elmJson": "<library/>",
            "libraryId": "L",
            "expression": "E",
            "hfsBaseUrl": "http://hfs",
            "htsBaseUrl": "http://hts"
        });
        let r: EvaluateExpressionRequest = serde_json::from_value(j).unwrap();
        assert_eq!(r.elm.as_deref(), Some("<library/>"));
    }

    #[test]
    fn blank_elm_deserializes_to_none() {
        let j = json!({
            "elm": "  ",
            "libraryId": "L",
            "expression": "E",
            "hfsBaseUrl": "http://hfs",
            "htsBaseUrl": "http://hts"
        });
        let r: EvaluateExpressionRequest = serde_json::from_value(j).unwrap();
        assert!(r.elm.is_none());
    }

    #[test]
    fn serializes_fhir_authorization_on_evaluate_request() {
        let j = json!({
            "libraryId": "L",
            "expression": "E",
            "hfsBaseUrl": "https://ehr.example.com/fhir",
            "htsBaseUrl": "http://hts",
            "fhirAuthorization": {
                "accessToken": "tok",
                "tokenType": "Bearer",
                "expiresIn": 300,
                "scope": "user/Patient.read",
                "subject": "cds-svc"
            }
        });
        let r: EvaluateExpressionRequest = serde_json::from_value(j).unwrap();
        let auth = r.fhir_authorization.expect("auth");
        assert_eq!(auth.access_token, "tok");
        assert_eq!(auth.token_type, "Bearer");
    }

    #[test]
    fn serializes_prefetch_on_evaluate_request() {
        let mut prefetch = serde_json::Map::new();
        prefetch.insert(
            "conditions".into(),
            json!({"resourceType": "Bundle", "type": "searchset"}),
        );
        let req = EvaluateExpressionRequest {
            elm: None,
            elm_format: ElmFormat::default(),
            library_id: "L".into(),
            library_version: None,
            expression: "E".into(),
            hfs_base_url: "http://hfs".into(),
            hts_base_url: "http://hts".into(),
            library_base_url: None,
            resolve_library_artifacts_from_fhir: true,
            included_libraries: vec![],
            patient_id: Some("p1".into()),
            parameters: None,
            evaluation_date_time: None,
            prefetch: Some(prefetch),
            fhir_authorization: None,
        };
        let val = serde_json::to_value(&req).unwrap();
        assert!(val.get("prefetch").is_some());
        assert!(val["prefetch"]["conditions"].is_object());
    }

    #[test]
    fn deserializes_without_elm_when_resolve_from_fhir_default() {
        let j = json!({
            "libraryId": "MyLib",
            "expression": "Def",
            "hfsBaseUrl": "http://hfs",
            "htsBaseUrl": "http://hts",
            "libraryBaseUrl": "http://kr"
        });
        let r: EvaluateExpressionRequest = serde_json::from_value(j).unwrap();
        assert!(r.elm.is_none());
        assert!(r.resolve_library_artifacts_from_fhir);
        assert_eq!(r.library_base_url.as_deref(), Some("http://kr"));
    }

    #[test]
    fn deserializes_response_without_result_type() {
        let j = json!({
            "expression": "X",
            "result": null
        });
        let r: EvaluateExpressionResponse = serde_json::from_value(j).unwrap();
        assert_eq!(r.expression, "X");
        assert!(r.result_type.is_none());
        assert!(r.result.is_null());
    }

    #[test]
    fn normalized_result_on_response() {
        let inner = json!({"resourceType": "Patient", "id": "1"});
        let r = EvaluateExpressionResponse {
            expression: "E".into(),
            result_type: None,
            result: Value::String(inner.to_string()),
        };
        match r.normalized_result() {
            NormalizedSidecarResult::FhirResource(v) => assert_eq!(v, inner),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn deserializes_apply_response_with_care_plan_and_request_group() {
        let j = json!({
            "planDefinitionId": "CMS165",
            "carePlan": {
                "resourceType": "CarePlan",
                "id": "cp-1",
                "status": "active",
                "intent": "proposal",
                "subject": { "reference": "Patient/p1" }
            },
            "requestGroup": {
                "resourceType": "RequestGroup",
                "id": "rg-1",
                "status": "active",
                "intent": "proposal"
            }
        });
        let r: ApplyPlanDefinitionResponse = serde_json::from_value(j).unwrap();
        assert_eq!(r.plan_definition_id.as_deref(), Some("CMS165"));
        assert_eq!(r.care_plan_value()["resourceType"], "CarePlan");
        assert_eq!(r.request_group_value()["resourceType"], "RequestGroup");
    }

    #[test]
    fn deserializes_apply_activity_definition_response() {
        let j = json!({
            "activityDefinitionId": "order-ecg",
            "resource": {
                "resourceType": "ServiceRequest",
                "status": "draft",
                "intent": "proposal",
                "subject": { "reference": "Patient/p1" }
            }
        });
        let r: ApplyActivityDefinitionResponse = serde_json::from_value(j).unwrap();
        assert_eq!(r.activity_definition_id.as_deref(), Some("order-ecg"));
        assert_eq!(r.resource_value()["resourceType"], "ServiceRequest");
    }
}
