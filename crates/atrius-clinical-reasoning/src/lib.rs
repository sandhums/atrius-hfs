//! HTTP façade types and client for the **Atrius** JVM clinical reasoning sidecar.
//!
//! CDS Hooks ([`cds-server`]) and other orchestration layers use this crate; it does **not** embed
//! a CQL engine. Evaluation runs in the external JVM service:
//!
//! - **`POST /v1/evaluate/expression`** — named CQL expression (legacy / fallback)
//! - **`POST /v1/plandefinition/apply`** — FHIR **`PlanDefinition/$apply`** via CQF Clinical Reasoning
//! - **`POST /v1/activitydefinition/apply`** — FHIR **`ActivityDefinition/$apply`** via CQF Clinical Reasoning
//!
//! # Stack placement
//!
//! ```text
//! cds-server ──► ClinicalReasoningClient ──► JVM sidecar (:8088)
//!                      │                         │
//!                      │ ApplyPlanDefinitionRequest │
//!                      │  hfsBaseUrl  ────────────┼──► cr-fhir-bridge (:8081)
//!                      │  libraryBaseUrl ─────────┼──► KR HFS (:8079)
//!                      │  htsBaseUrl  ────────────┼──► HTS (:8090/9091)
//! ```
//!
//! # Modules
//!
//! - [`dto`] — request/response JSON contract aligned with Kotlin sidecar
//! - [`request_builder`] — [`EvaluateExpressionRequestBuilder`] + [`FhirServiceEndpoints`]
//! - [`client`] — async HTTP client
//! - [`normalized_result`] — decode JVM `result` (scalars, collections, double-encoded FHIR JSON)
//!
//! Full architecture: `docs/clinical-reasoning/README.md`.

#![deny(unsafe_code)]

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
