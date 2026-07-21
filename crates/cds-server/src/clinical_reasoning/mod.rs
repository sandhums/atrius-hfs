//! HTTP façade types and client for the **Atrius** JVM clinical reasoning sidecar.
//!
//! This module lives inside `cds-server` (folded from the former `atrius-clinical-reasoning`
//! crate). It does **not** embed a CQL engine — evaluation runs in the external JVM service:
//!
//! - **`POST /v1/evaluate/expression`** — named CQL expression (legacy / fallback)
//! - **`POST /v1/plandefinition/apply`** — FHIR **`PlanDefinition/$apply`** via CQF Clinical Reasoning
//! - **`POST /v1/activitydefinition/apply`** — FHIR **`ActivityDefinition/$apply`** via CQF Clinical Reasoning
//!
//! Full architecture: `docs/clinical-reasoning/README.md`.

pub mod client;
pub mod config;
pub mod dto;
pub mod error;
pub mod fhir_authorization;
pub mod normalized_result;
pub mod request_builder;

pub use client::{ClinicalReasoningClient, EvaluateExpressionFacade};
pub use config::ClinicalReasoningConfig;
pub use dto::{
    ApplyActivityDefinitionRequest, ApplyActivityDefinitionResponse, ApplyPlanDefinitionRequest,
    ApplyPlanDefinitionResponse, ClearLibraryCacheResponse, ElmFormat, EvaluateExpressionRequest,
    EvaluateExpressionResponse, IncludedLibrary,
};
pub use error::{ClinicalReasoningError, SidecarRejectionDetail};
pub use fhir_authorization::SidecarFhirAuthorization;
pub use normalized_result::{
    NormalizedSidecarResult, normalize_sidecar_result, unwrap_nested_fhir_json_strings,
};
pub use request_builder::{
    ApplyActivityDefinitionRequestBuildError, ApplyActivityDefinitionRequestBuilder,
    ApplyPlanDefinitionRequestBuildError, ApplyPlanDefinitionRequestBuilder,
    EvaluateExpressionRequestBuildError, EvaluateExpressionRequestBuilder, FhirServiceEndpoints,
};
