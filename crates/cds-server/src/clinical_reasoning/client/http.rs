//! Async HTTP client for the JVM clinical reasoning sidecar.
//!
//! [`ClinicalReasoningClient`] posts [`EvaluateExpressionRequest`](super::super::dto::EvaluateExpressionRequest)
//! to **`POST /v1/evaluate/expression`** and deserializes
//! [`EvaluateExpressionResponse`](super::super::dto::EvaluateExpressionResponse).
//!
//! Non-2xx responses become [`ClinicalReasoningError::SidecarRejected`] with the raw body for
//! diagnostics (CQL errors, missing Library, FHIR 404 from retrieve provider, etc.).

use reqwest::{Client, Url};

use super::super::ClinicalReasoningError;
use super::super::SidecarRejectionDetail;
use super::super::config::ClinicalReasoningConfig;
use super::super::dto::{
    ApplyActivityDefinitionRequest, ApplyActivityDefinitionResponse, ApplyPlanDefinitionRequest,
    ApplyPlanDefinitionResponse, ClearLibraryCacheResponse, EvaluateExpressionRequest,
    EvaluateExpressionResponse,
};

/// Sidecar evaluate path (appended to configured base URL).
const EVAL_EXPR_PATH: &str = "/v1/evaluate/expression";
const APPLY_PLAN_DEFINITION_PATH: &str = "/v1/plandefinition/apply";
const APPLY_ACTIVITY_DEFINITION_PATH: &str = "/v1/activitydefinition/apply";
const CLEAR_LIBRARY_CACHE_PATH: &str = "/v1/admin/cache/libraries/clear";

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

    /// Evaluate a named CQL expression for a patient context.
    ///
    /// The request must carry correct [`EvaluateExpressionRequest::hfs_base_url`] /
    /// [`EvaluateExpressionRequest::hts_base_url`] / optional `library_base_url` — the sidecar
    /// performs all FHIR retrieve and terminology calls itself; this client only transports JSON.
    pub async fn evaluate_expression(
        &self,
        request: EvaluateExpressionRequest,
    ) -> Result<EvaluateExpressionResponse, ClinicalReasoningError> {
        self.post_json(EVAL_EXPR_PATH, &request).await
    }

    /// Run **`PlanDefinition/$apply`** and return the resulting RequestGroup JSON.
    pub async fn apply_plan_definition(
        &self,
        request: ApplyPlanDefinitionRequest,
    ) -> Result<ApplyPlanDefinitionResponse, ClinicalReasoningError> {
        self.post_json(APPLY_PLAN_DEFINITION_PATH, &request).await
    }

    /// Run **`ActivityDefinition/$apply`** and return the resulting draft request resource.
    pub async fn apply_activity_definition(
        &self,
        request: ApplyActivityDefinitionRequest,
    ) -> Result<ApplyActivityDefinitionResponse, ClinicalReasoningError> {
        self.post_json(APPLY_ACTIVITY_DEFINITION_PATH, &request)
            .await
    }

    /// Flush JVM sidecar ELM / KR Library / ValueSet expansion caches after KR re-import.
    pub async fn clear_library_cache(
        &self,
    ) -> Result<ClearLibraryCacheResponse, ClinicalReasoningError> {
        self.post_empty(CLEAR_LIBRARY_CACHE_PATH).await
    }

    async fn post_empty<R>(&self, path: &str) -> Result<R, ClinicalReasoningError>
    where
        R: serde::de::DeserializeOwned,
    {
        let url = self.base.join(path.trim_start_matches('/')).map_err(|e| {
            ClinicalReasoningError::InvalidUrl(format!("{} + {}: {}", self.base, path, e))
        })?;

        let resp = self
            .http
            .post(url)
            .send()
            .await
            .map_err(|e| ClinicalReasoningError::Http(e.to_string()))?;

        let status = resp.status().as_u16();
        let text = resp
            .text()
            .await
            .map_err(|e| ClinicalReasoningError::Http(e.to_string()))?;

        if (200..300).contains(&status) {
            serde_json::from_str::<R>(&text).map_err(|e| {
                ClinicalReasoningError::Http(format!("invalid JSON response: {e}: {text}"))
            })
        } else {
            Err(ClinicalReasoningError::SidecarRejected(
                SidecarRejectionDetail::new(status, text),
            ))
        }
    }

    async fn post_json<T, R>(&self, path: &str, request: &T) -> Result<R, ClinicalReasoningError>
    where
        T: serde::Serialize + ?Sized,
        R: serde::de::DeserializeOwned,
    {
        let url = self.base.join(path.trim_start_matches('/')).map_err(|e| {
            ClinicalReasoningError::InvalidUrl(format!("{} + {}: {}", self.base, path, e))
        })?;

        let resp = self
            .http
            .post(url)
            .json(request)
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
                serde_json::from_str::<R>(&text).map_err(|e| {
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
