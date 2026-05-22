//! HTTP façade types and client for the **Atrius** JVM clinical reasoning sidecar.
//!
//! CDS Hooks and HFS orchestration layers call this façade; the façade speaks HTTP to
//! CQL/CQFramework-backed services—not an in-process ELM runtime.

#![deny(unsafe_code)]

pub mod client;
pub mod config;
pub mod dto;
pub mod error;
pub mod normalized_result;
pub mod request_builder;

pub use client::{ClinicalReasoningClient, EvaluateExpressionFacade};
pub use config::ClinicalReasoningConfig;
pub use dto::{ElmFormat, EvaluateExpressionRequest, EvaluateExpressionResponse, IncludedLibrary};
pub use error::{ClinicalReasoningError, SidecarRejectionDetail};
pub use normalized_result::{
    NormalizedSidecarResult, normalize_sidecar_result, unwrap_nested_fhir_json_strings,
};
pub use request_builder::{
    EvaluateExpressionRequestBuildError, EvaluateExpressionRequestBuilder, FhirServiceEndpoints,
};
