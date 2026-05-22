//! HTTP client façade to the JVM clinical reasoning sidecar.

use async_trait::async_trait;

#[cfg(feature = "http-client")]
mod http;

#[cfg(feature = "http-client")]
pub use http::ClinicalReasoningClient;

/// Stub used when `--no-default-features` drops `http-client`; keeps trait available for mocks.
#[cfg(not(feature = "http-client"))]
mod stub {
    use crate::dto::{EvaluateExpressionRequest, EvaluateExpressionResponse};
    use crate::error::ClinicalReasoningError;

    /// Compiles-only placeholder when HTTP client dependency is disabled.
    #[derive(Debug, Clone)]
    pub struct ClinicalReasoningClient;

    impl ClinicalReasoningClient {
        pub async fn evaluate_expression(
            &self,
            _request: EvaluateExpressionRequest,
        ) -> Result<EvaluateExpressionResponse, ClinicalReasoningError> {
            Err(ClinicalReasoningError::Http(
                "atrius-clinical-reasoning built without `http-client` feature".into(),
            ))
        }
    }
}

#[cfg(not(feature = "http-client"))]
pub use stub::ClinicalReasoningClient;

#[async_trait]
pub trait EvaluateExpressionFacade: Send + Sync {
    async fn evaluate_expression(
        &self,
        request: crate::dto::EvaluateExpressionRequest,
    ) -> Result<crate::dto::EvaluateExpressionResponse, crate::ClinicalReasoningError>;
}

#[async_trait]
impl EvaluateExpressionFacade for ClinicalReasoningClient {
    async fn evaluate_expression(
        &self,
        request: crate::dto::EvaluateExpressionRequest,
    ) -> Result<crate::dto::EvaluateExpressionResponse, crate::ClinicalReasoningError> {
        ClinicalReasoningClient::evaluate_expression(self, request).await
    }
}
