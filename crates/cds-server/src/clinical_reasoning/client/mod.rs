//! HTTP client façade to the JVM clinical reasoning sidecar.
//!
//! [`ClinicalReasoningClient`] implements [`EvaluateExpressionFacade`].
//! Configure via [`ClinicalReasoningConfig`](super::ClinicalReasoningConfig) — default base URL
//! matches JVM `SIDECAR_PORT` **8088**.

use async_trait::async_trait;

mod http;

pub use http::ClinicalReasoningClient;

#[async_trait]
pub trait EvaluateExpressionFacade: Send + Sync {
    async fn evaluate_expression(
        &self,
        request: super::dto::EvaluateExpressionRequest,
    ) -> Result<super::dto::EvaluateExpressionResponse, super::ClinicalReasoningError>;
}

#[async_trait]
impl EvaluateExpressionFacade for ClinicalReasoningClient {
    async fn evaluate_expression(
        &self,
        request: super::dto::EvaluateExpressionRequest,
    ) -> Result<super::dto::EvaluateExpressionResponse, super::ClinicalReasoningError> {
        ClinicalReasoningClient::evaluate_expression(self, request).await
    }
}
