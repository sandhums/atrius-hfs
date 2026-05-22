//! Build [`crate::dto::EvaluateExpressionRequest`] from tenant / FHIR endpoint configuration.

use serde_json::Value;
use thiserror::Error;

use crate::dto::{ElmFormat, EvaluateExpressionRequest, IncludedLibrary};

fn trim_trailing_slash(s: String) -> String {
    let mut s = s.trim().to_string();
    while s.ends_with('/') {
        s.pop();
    }
    s
}

/// Clinical + terminology (+ optional library KR) bases for a tenant or environment.
#[derive(Debug, Clone)]
pub struct FhirServiceEndpoints {
    pub hfs_base_url: String,
    pub hts_base_url: String,
    pub library_base_url: Option<String>,
}

impl FhirServiceEndpoints {
    pub fn new(hfs_base_url: impl Into<String>, hts_base_url: impl Into<String>) -> Self {
        Self {
            hfs_base_url: trim_trailing_slash(hfs_base_url.into()),
            hts_base_url: trim_trailing_slash(hts_base_url.into()),
            library_base_url: None,
        }
    }

    /// Separate FHIR server for `Library` artifacts (KR). If unset, request omits `libraryBaseUrl` and the sidecar uses `hfsBaseUrl`.
    pub fn with_library_base_url(mut self, url: impl Into<String>) -> Self {
        self.library_base_url = Some(trim_trailing_slash(url.into()));
        self
    }
}

#[derive(Debug, Error)]
pub enum EvaluateExpressionRequestBuildError {
    #[error(
        "inline ELM is required when resolve_library_artifacts_from_fhir is false (sidecar will not fetch Library)"
    )]
    InlineElmRequiredWhenArtifactResolutionDisabled,
}

/// Fluent builder with validation matching JVM sidecar rules for inline ELM vs FHIR resolution.
#[derive(Debug, Clone)]
pub struct EvaluateExpressionRequestBuilder {
    endpoints: FhirServiceEndpoints,
    library_id: String,
    library_version: Option<String>,
    expression: String,
    elm: Option<String>,
    elm_format: ElmFormat,
    resolve_library_artifacts_from_fhir: bool,
    included_libraries: Vec<IncludedLibrary>,
    patient_id: Option<String>,
    parameters: Option<Value>,
    evaluation_date_time: Option<String>,
}

impl EvaluateExpressionRequestBuilder {
    pub fn new(
        endpoints: FhirServiceEndpoints,
        library_id: impl Into<String>,
        expression: impl Into<String>,
    ) -> Self {
        Self {
            endpoints,
            library_id: library_id.into(),
            library_version: None,
            expression: expression.into(),
            elm: None,
            elm_format: ElmFormat::default(),
            resolve_library_artifacts_from_fhir: true,
            included_libraries: Vec::new(),
            patient_id: None,
            parameters: None,
            evaluation_date_time: None,
        }
    }

    pub fn library_version(mut self, v: impl Into<String>) -> Self {
        self.library_version = Some(v.into());
        self
    }

    pub fn inline_elm(mut self, elm: impl Into<String>) -> Self {
        self.elm = Some(elm.into());
        self
    }

    pub fn elm_format(mut self, f: ElmFormat) -> Self {
        self.elm_format = f;
        self
    }

    pub fn resolve_library_artifacts_from_fhir(mut self, yes: bool) -> Self {
        self.resolve_library_artifacts_from_fhir = yes;
        self
    }

    pub fn included_libraries(mut self, libs: Vec<IncludedLibrary>) -> Self {
        self.included_libraries = libs;
        self
    }

    pub fn patient_id(mut self, id: impl Into<String>) -> Self {
        self.patient_id = Some(id.into());
        self
    }

    pub fn parameters(mut self, p: Value) -> Self {
        self.parameters = Some(p);
        self
    }

    pub fn evaluation_date_time(mut self, iso: impl Into<String>) -> Self {
        self.evaluation_date_time = Some(iso.into());
        self
    }

    pub fn build(self) -> Result<EvaluateExpressionRequest, EvaluateExpressionRequestBuildError> {
        if !self.resolve_library_artifacts_from_fhir {
            let missing = self
                .elm
                .as_ref()
                .map(|s| s.trim().is_empty())
                .unwrap_or(true);
            if missing {
                return Err(
                    EvaluateExpressionRequestBuildError::InlineElmRequiredWhenArtifactResolutionDisabled,
                );
            }
        }

        Ok(EvaluateExpressionRequest {
            elm: self.elm.filter(|s| !s.trim().is_empty()),
            elm_format: self.elm_format,
            library_id: self.library_id,
            library_version: self.library_version,
            expression: self.expression,
            hfs_base_url: self.endpoints.hfs_base_url,
            hts_base_url: self.endpoints.hts_base_url,
            library_base_url: self.endpoints.library_base_url,
            resolve_library_artifacts_from_fhir: self.resolve_library_artifacts_from_fhir,
            included_libraries: self.included_libraries,
            patient_id: self.patient_id,
            parameters: self.parameters,
            evaluation_date_time: self.evaluation_date_time,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_requires_elm_when_resolve_disabled() {
        let ep = FhirServiceEndpoints::new("http://hfs", "http://hts");
        let err = EvaluateExpressionRequestBuilder::new(ep, "L", "E")
            .resolve_library_artifacts_from_fhir(false)
            .build()
            .unwrap_err();
        assert!(matches!(
            err,
            EvaluateExpressionRequestBuildError::InlineElmRequiredWhenArtifactResolutionDisabled
        ));
    }

    #[test]
    fn build_ok_resolve_disabled_with_elm() {
        let ep = FhirServiceEndpoints::new("http://hfs", "http://hts");
        let req = EvaluateExpressionRequestBuilder::new(ep, "L", "E")
            .resolve_library_artifacts_from_fhir(false)
            .inline_elm("{}")
            .build()
            .unwrap();
        assert!(!req.resolve_library_artifacts_from_fhir);
        assert_eq!(req.elm.as_deref(), Some("{}"));
    }

    #[test]
    fn build_ok_resolve_true_without_elm() {
        let ep = FhirServiceEndpoints::new("http://hfs", "http://hts")
            .with_library_base_url("http://kr");
        let req = EvaluateExpressionRequestBuilder::new(ep, "L", "E")
            .build()
            .unwrap();
        assert!(req.elm.is_none());
        assert!(req.resolve_library_artifacts_from_fhir);
        assert_eq!(req.library_base_url.as_deref(), Some("http://kr"));
    }
}
