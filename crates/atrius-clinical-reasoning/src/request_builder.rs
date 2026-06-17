//! Build [`crate::dto::EvaluateExpressionRequest`] from tenant / FHIR endpoint configuration.
//!
//! [`FhirServiceEndpoints`] holds the three bases forwarded to the sidecar on every evaluate call.
//! [`EvaluateExpressionRequestBuilder`] adds library/expression/patient context and validates that
//! inline ELM is present when FHIR artifact resolution is disabled (matching JVM rules).

use serde_json::Value;
use thiserror::Error;

use crate::dto::{
    ApplyActivityDefinitionRequest, ApplyPlanDefinitionRequest, ElmFormat,
    EvaluateExpressionRequest, IncludedLibrary,
};
use crate::fhir_authorization::SidecarFhirAuthorization;

fn trim_trailing_slash(s: String) -> String {
    let mut s = s.trim().to_string();
    while s.ends_with('/') {
        s.pop();
    }
    s
}

/// Clinical + terminology (+ optional library KR) bases for a tenant or environment.
///
/// Passed to [`EvaluateExpressionRequestBuilder`] and serialized into every sidecar evaluate
/// request. Trailing slashes are stripped for consistent URL joining on the JVM side.
///
/// # Atrius defaults (local dev)
///
/// - `hfs_base_url` → `http://127.0.0.1:8081` (**cr-fhir-bridge**)
/// - `hts_base_url` → `http://127.0.0.1:9091` (or 8090 — align with HTS listen port)
/// - `library_base_url` → `http://127.0.0.1:8079` (KR HFS)
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
    prefetch: Option<serde_json::Map<String, serde_json::Value>>,
    fhir_authorization: Option<SidecarFhirAuthorization>,
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
            prefetch: None,
            fhir_authorization: None,
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

    pub fn prefetch(mut self, prefetch: serde_json::Map<String, serde_json::Value>) -> Self {
        self.prefetch = Some(prefetch);
        self
    }

    pub fn fhir_authorization(mut self, auth: SidecarFhirAuthorization) -> Self {
        self.fhir_authorization = Some(auth);
        self
    }

    /// Override clinical FHIR base (e.g. CDS Hooks `fhirServer` when SMART token is present).
    pub fn clinical_base_url(mut self, url: impl Into<String>) -> Self {
        self.endpoints.hfs_base_url = trim_trailing_slash(url.into());
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
            prefetch: self.prefetch,
            fhir_authorization: self.fhir_authorization,
        })
    }
}

#[derive(Debug, Error)]
pub enum ApplyPlanDefinitionRequestBuildError {
    #[error("planDefinitionId or planDefinitionUrl is required")]
    MissingPlanDefinitionTarget,
    #[error("patientId must not be blank")]
    MissingPatientId,
}

/// Fluent builder for [`ApplyPlanDefinitionRequest`].
#[derive(Debug, Clone)]
pub struct ApplyPlanDefinitionRequestBuilder {
    endpoints: FhirServiceEndpoints,
    plan_definition_id: Option<String>,
    plan_definition_url: Option<String>,
    patient_id: String,
    encounter_id: Option<String>,
    practitioner_id: Option<String>,
    organization_id: Option<String>,
    user_type: Option<serde_json::Value>,
    user_language: Option<serde_json::Value>,
    user_task_context: Option<serde_json::Value>,
    setting: Option<serde_json::Value>,
    setting_context: Option<serde_json::Value>,
    use_server_data: bool,
    prefetch: Option<serde_json::Map<String, serde_json::Value>>,
    parameters: Option<serde_json::Value>,
    fhir_authorization: Option<SidecarFhirAuthorization>,
}

impl ApplyPlanDefinitionRequestBuilder {
    pub fn new(endpoints: FhirServiceEndpoints, patient_id: impl Into<String>) -> Self {
        Self {
            endpoints,
            plan_definition_id: None,
            plan_definition_url: None,
            patient_id: patient_id.into(),
            encounter_id: None,
            practitioner_id: None,
            organization_id: None,
            user_type: None,
            user_language: None,
            user_task_context: None,
            setting: None,
            setting_context: None,
            use_server_data: false,
            prefetch: None,
            parameters: None,
            fhir_authorization: None,
        }
    }

    pub fn plan_definition_id(mut self, id: impl Into<String>) -> Self {
        self.plan_definition_id = Some(id.into());
        self
    }

    pub fn plan_definition_url(mut self, url: impl Into<String>) -> Self {
        self.plan_definition_url = Some(url.into());
        self
    }

    pub fn practitioner_id(mut self, id: impl Into<String>) -> Self {
        self.practitioner_id = Some(id.into());
        self
    }

    pub fn encounter_id(mut self, id: impl Into<String>) -> Self {
        self.encounter_id = Some(id.into());
        self
    }

    pub fn organization_id(mut self, id: impl Into<String>) -> Self {
        self.organization_id = Some(id.into());
        self
    }

    pub fn user_type(mut self, value: serde_json::Value) -> Self {
        self.user_type = Some(value);
        self
    }

    pub fn user_language(mut self, value: serde_json::Value) -> Self {
        self.user_language = Some(value);
        self
    }

    pub fn user_task_context(mut self, value: serde_json::Value) -> Self {
        self.user_task_context = Some(value);
        self
    }

    pub fn setting(mut self, value: serde_json::Value) -> Self {
        self.setting = Some(value);
        self
    }

    pub fn setting_context(mut self, value: serde_json::Value) -> Self {
        self.setting_context = Some(value);
        self
    }

    pub fn use_server_data(mut self, yes: bool) -> Self {
        self.use_server_data = yes;
        self
    }

    pub fn prefetch(mut self, prefetch: serde_json::Map<String, serde_json::Value>) -> Self {
        self.prefetch = Some(prefetch);
        self
    }

    pub fn parameters(mut self, p: serde_json::Value) -> Self {
        self.parameters = Some(p);
        self
    }

    pub fn fhir_authorization(mut self, auth: SidecarFhirAuthorization) -> Self {
        self.fhir_authorization = Some(auth);
        self
    }

    pub fn clinical_base_url(mut self, url: impl Into<String>) -> Self {
        self.endpoints.hfs_base_url = trim_trailing_slash(url.into());
        self
    }

    pub fn build(self) -> Result<ApplyPlanDefinitionRequest, ApplyPlanDefinitionRequestBuildError> {
        if self.patient_id.trim().is_empty() {
            return Err(ApplyPlanDefinitionRequestBuildError::MissingPatientId);
        }
        let has_id = self
            .plan_definition_id
            .as_ref()
            .is_some_and(|s| !s.trim().is_empty());
        let has_url = self
            .plan_definition_url
            .as_ref()
            .is_some_and(|s| !s.trim().is_empty());
        if !has_id && !has_url {
            return Err(ApplyPlanDefinitionRequestBuildError::MissingPlanDefinitionTarget);
        }

        Ok(ApplyPlanDefinitionRequest {
            plan_definition_id: self.plan_definition_id,
            plan_definition_url: self.plan_definition_url,
            patient_id: self.patient_id,
            encounter_id: self.encounter_id,
            practitioner_id: self.practitioner_id,
            organization_id: self.organization_id,
            user_type: self.user_type,
            user_language: self.user_language,
            user_task_context: self.user_task_context,
            setting: self.setting,
            setting_context: self.setting_context,
            hfs_base_url: self.endpoints.hfs_base_url,
            hts_base_url: self.endpoints.hts_base_url,
            library_base_url: self.endpoints.library_base_url,
            use_server_data: self.use_server_data,
            prefetch: self.prefetch,
            parameters: self.parameters,
            fhir_authorization: self.fhir_authorization,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyActivityDefinitionRequestBuildError {
    MissingActivityDefinitionTarget,
    MissingPatientId,
}

/// Fluent builder for [`ApplyActivityDefinitionRequest`].
#[derive(Debug, Clone)]
pub struct ApplyActivityDefinitionRequestBuilder {
    endpoints: FhirServiceEndpoints,
    activity_definition_id: Option<String>,
    activity_definition_url: Option<String>,
    patient_id: String,
    encounter_id: Option<String>,
    practitioner_id: Option<String>,
    organization_id: Option<String>,
    user_type: Option<serde_json::Value>,
    user_language: Option<serde_json::Value>,
    user_task_context: Option<serde_json::Value>,
    setting: Option<serde_json::Value>,
    setting_context: Option<serde_json::Value>,
    use_server_data: bool,
    prefetch: Option<serde_json::Map<String, serde_json::Value>>,
    parameters: Option<serde_json::Value>,
    fhir_authorization: Option<SidecarFhirAuthorization>,
}

impl ApplyActivityDefinitionRequestBuilder {
    pub fn new(endpoints: FhirServiceEndpoints, patient_id: impl Into<String>) -> Self {
        Self {
            endpoints,
            activity_definition_id: None,
            activity_definition_url: None,
            patient_id: patient_id.into(),
            encounter_id: None,
            practitioner_id: None,
            organization_id: None,
            user_type: None,
            user_language: None,
            user_task_context: None,
            setting: None,
            setting_context: None,
            use_server_data: false,
            prefetch: None,
            parameters: None,
            fhir_authorization: None,
        }
    }

    pub fn activity_definition_id(mut self, id: impl Into<String>) -> Self {
        self.activity_definition_id = Some(id.into());
        self
    }

    pub fn activity_definition_url(mut self, url: impl Into<String>) -> Self {
        self.activity_definition_url = Some(url.into());
        self
    }

    pub fn practitioner_id(mut self, id: impl Into<String>) -> Self {
        self.practitioner_id = Some(id.into());
        self
    }

    pub fn encounter_id(mut self, id: impl Into<String>) -> Self {
        self.encounter_id = Some(id.into());
        self
    }

    pub fn organization_id(mut self, id: impl Into<String>) -> Self {
        self.organization_id = Some(id.into());
        self
    }

    pub fn user_type(mut self, value: serde_json::Value) -> Self {
        self.user_type = Some(value);
        self
    }

    pub fn user_language(mut self, value: serde_json::Value) -> Self {
        self.user_language = Some(value);
        self
    }

    pub fn user_task_context(mut self, value: serde_json::Value) -> Self {
        self.user_task_context = Some(value);
        self
    }

    pub fn setting(mut self, value: serde_json::Value) -> Self {
        self.setting = Some(value);
        self
    }

    pub fn setting_context(mut self, value: serde_json::Value) -> Self {
        self.setting_context = Some(value);
        self
    }

    pub fn use_server_data(mut self, yes: bool) -> Self {
        self.use_server_data = yes;
        self
    }

    pub fn prefetch(mut self, prefetch: serde_json::Map<String, serde_json::Value>) -> Self {
        self.prefetch = Some(prefetch);
        self
    }

    pub fn parameters(mut self, p: serde_json::Value) -> Self {
        self.parameters = Some(p);
        self
    }

    pub fn fhir_authorization(mut self, auth: SidecarFhirAuthorization) -> Self {
        self.fhir_authorization = Some(auth);
        self
    }

    pub fn clinical_base_url(mut self, url: impl Into<String>) -> Self {
        self.endpoints.hfs_base_url = trim_trailing_slash(url.into());
        self
    }

    pub fn build(
        self,
    ) -> Result<ApplyActivityDefinitionRequest, ApplyActivityDefinitionRequestBuildError> {
        if self.patient_id.trim().is_empty() {
            return Err(ApplyActivityDefinitionRequestBuildError::MissingPatientId);
        }
        let has_id = self
            .activity_definition_id
            .as_ref()
            .is_some_and(|s| !s.trim().is_empty());
        let has_url = self
            .activity_definition_url
            .as_ref()
            .is_some_and(|s| !s.trim().is_empty());
        if !has_id && !has_url {
            return Err(ApplyActivityDefinitionRequestBuildError::MissingActivityDefinitionTarget);
        }

        Ok(ApplyActivityDefinitionRequest {
            activity_definition_id: self.activity_definition_id,
            activity_definition_url: self.activity_definition_url,
            patient_id: self.patient_id,
            encounter_id: self.encounter_id,
            practitioner_id: self.practitioner_id,
            organization_id: self.organization_id,
            user_type: self.user_type,
            user_language: self.user_language,
            user_task_context: self.user_task_context,
            setting: self.setting,
            setting_context: self.setting_context,
            hfs_base_url: self.endpoints.hfs_base_url,
            hts_base_url: self.endpoints.hts_base_url,
            library_base_url: self.endpoints.library_base_url,
            use_server_data: self.use_server_data,
            prefetch: self.prefetch,
            parameters: self.parameters,
            fhir_authorization: self.fhir_authorization,
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
