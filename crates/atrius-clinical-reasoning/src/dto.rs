//! Request / response payloads aligned with JVM sidecar
//! `EvaluateExpressionRequest` / `EvaluateExpressionResponse` (kotlinx.serialization JSON).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::normalized_result::NormalizedSidecarResult;

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
        crate::normalized_result::normalize_sidecar_result(&self.result)
    }
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
}
