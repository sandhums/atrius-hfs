use reqwest::{Client, Url};

use crate::ClinicalReasoningError;
use crate::SidecarRejectionDetail;
use crate::config::ClinicalReasoningConfig;
use crate::dto::{EvaluateExpressionRequest, EvaluateExpressionResponse};

const EVAL_EXPR_PATH: &str = "/v1/evaluate/expression";

#[derive(Debug, Clone)]
pub struct ClinicalReasoningClient {
    http: Client,
    base: Url,
    timeout: std::time::Duration,
}

impl ClinicalReasoningClient {
    pub fn new(config: ClinicalReasoningConfig) -> Result<Self, ClinicalReasoningError> {
        let base: Url = config
            .base_url
            .parse()
            .map_err(|e| ClinicalReasoningError::InvalidUrl(format!("{}", e)))?;

        let http = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|e| ClinicalReasoningError::Http(format!("reqwest builder: {e}")))?;

        Ok(Self {
            http,
            base,
            timeout: config.request_timeout,
        })
    }

    /// Returns base URL timeout (explicit field for callers / tests).
    pub fn timeout(&self) -> std::time::Duration {
        self.timeout
    }

    pub async fn evaluate_expression(
        &self,
        request: EvaluateExpressionRequest,
    ) -> Result<EvaluateExpressionResponse, ClinicalReasoningError> {
        let url = self
            .base
            .join(EVAL_EXPR_PATH.trim_start_matches('/'))
            .map_err(|e| {
                ClinicalReasoningError::InvalidUrl(format!(
                    "{} + {}: {}",
                    self.base, EVAL_EXPR_PATH, e
                ))
            })?;

        let resp = self
            .http
            .post(url)
            .json(&request)
            .send()
            .await
            .map_err(|e| ClinicalReasoningError::Http(e.to_string()))?;

        let status = resp.status().as_u16();

        let body_preview = resp
            .text()
            .await
            .map_err(|e| ClinicalReasoningError::Http(e.to_string()));

        match body_preview {
            Ok(text) if (200..300).contains(&status) => {
                serde_json::from_str::<EvaluateExpressionResponse>(&text).map_err(|e| {
                    ClinicalReasoningError::Http(format!("invalid JSON response: {e}: {text}"))
                })
            }
            Ok(text) => Err(ClinicalReasoningError::SidecarRejected(
                SidecarRejectionDetail::new(status, text),
            )),
            Err(e) => Err(e),
        }
    }
}
