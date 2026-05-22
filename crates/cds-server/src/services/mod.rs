//! CDS service implementations and registry.

mod cards;

use std::sync::Arc;

use async_trait::async_trait;
use atrius_clinical_reasoning::{
    ClinicalReasoningClient, EvaluateExpressionRequestBuilder, FhirServiceEndpoints,
};
use helios_cds_hooks::{
    CdsHooksError, CdsRequest, CdsResponse, CdsService, FeedbackRequest, PatientViewContext,
    hooks::HookContext,
};

use crate::config::EvalTargets;
use crate::cr_error::map_clinical_reasoning_err;
use crate::kr_manifest::ManifestService;
use cards::{cards_from_normalized, demo_card_for_patient};
use tracing::debug;

/// Shared evaluation backend for all manifest-defined services.
#[derive(Debug, Clone)]
pub enum CdsEvalBackend {
    Demo,
    Sidecar {
        client: Arc<ClinicalReasoningClient>,
        endpoints: Arc<FhirServiceEndpoints>,
    },
}

/// One CDS Hooks service: discovery metadata + optional sidecar evaluate targets.
#[derive(Debug)]
pub struct SidecarEvalService {
    definition: CdsService,
    backend: CdsEvalBackend,
    targets: EvalTargets,
}

impl SidecarEvalService {
    pub fn from_manifest_entry(entry: &ManifestService, backend: CdsEvalBackend) -> Self {
        let definition = CdsService {
            hook: entry.hook.clone(),
            title: entry.title.clone(),
            description: entry.description.clone(),
            id: entry.id.clone(),
            prefetch: entry.prefetch.clone(),
            usage_requirements: entry.usage_requirements.clone(),
            version: entry.cds_hooks_version.clone(),
            extension: None,
        };
        Self {
            definition,
            backend,
            targets: entry.eval_targets(),
        }
    }
}

#[async_trait]
pub trait CdsInvocation: Send + Sync {
    fn definition(&self) -> CdsService;
    async fn invoke(&self, request: &CdsRequest) -> Result<CdsResponse, CdsHooksError>;
    async fn feedback(&self, _feedback: &FeedbackRequest) -> Result<(), CdsHooksError> {
        Ok(())
    }
}

#[async_trait]
impl CdsInvocation for SidecarEvalService {
    fn definition(&self) -> CdsService {
        self.definition.clone()
    }

    async fn invoke(&self, request: &CdsRequest) -> Result<CdsResponse, CdsHooksError> {
        match self.definition.hook.as_str() {
            PatientViewContext::HOOK_NAME => self.invoke_patient_view(request).await,
            other => Err(CdsHooksError::InternalError(format!(
                "unsupported hook `{other}` for service `{}`",
                self.definition.id
            ))),
        }
    }
}

impl SidecarEvalService {
    async fn invoke_patient_view(
        &self,
        request: &CdsRequest,
    ) -> Result<CdsResponse, CdsHooksError> {
        let ctx: PatientViewContext = serde_json::from_value(request.context.clone())?;

        match &self.backend {
            CdsEvalBackend::Demo => Ok(CdsResponse::with_cards(demo_card_for_patient(
                &ctx.patient_id,
                &request.hook_instance,
            ))),
            CdsEvalBackend::Sidecar { client, endpoints } => {
                let mut b = EvaluateExpressionRequestBuilder::new(
                    endpoints.as_ref().clone(),
                    self.targets.library_id.clone(),
                    self.targets.expression.clone(),
                )
                .resolve_library_artifacts_from_fhir(self.targets.resolve_from_fhir)
                .patient_id(ctx.patient_id.clone());

                if let Some(ref v) = self.targets.library_version {
                    b = b.library_version(v.clone());
                }

                let req = b.build().map_err(|e| {
                    CdsHooksError::InternalError(format!("evaluate request build: {e}"))
                })?;

                debug!(
                    service_id = %self.definition.id,
                    hfs_base_url = %endpoints.hfs_base_url,
                    hts_base_url = %endpoints.hts_base_url,
                    library_base_url = ?endpoints.library_base_url.as_deref(),
                    patient_id = ?req.patient_id,
                    "cds-server invoking clinical reasoning sidecar (FHIR bases as sent in evaluate request)"
                );

                let resp = client
                    .evaluate_expression(req)
                    .await
                    .map_err(map_clinical_reasoning_err)?;

                let cards =
                    cards_from_normalized(&self.targets.expression, resp.normalized_result());
                Ok(CdsResponse::with_cards(cards))
            }
        }
    }
}

/// Build a registry from a KR manifest and shared backend.
pub fn registry_from_manifest(
    manifest: &crate::kr_manifest::CdsServicesManifestFile,
    backend: CdsEvalBackend,
) -> ServiceRegistry {
    let services: Vec<Arc<dyn CdsInvocation>> = manifest
        .services
        .iter()
        .map(|entry| {
            Arc::new(SidecarEvalService::from_manifest_entry(
                entry,
                backend.clone(),
            )) as Arc<dyn CdsInvocation>
        })
        .collect();
    ServiceRegistry::new(services)
}

/// Registry of invocable CDS services (by `{id}` path segment).
#[derive(Clone)]
pub struct ServiceRegistry {
    inner: Arc<Vec<Arc<dyn CdsInvocation>>>,
}

impl ServiceRegistry {
    pub fn new(services: Vec<Arc<dyn CdsInvocation>>) -> Self {
        Self {
            inner: Arc::new(services),
        }
    }

    pub fn discovery_services(&self) -> Vec<CdsService> {
        self.inner.iter().map(|s| s.definition()).collect()
    }

    pub fn by_id(&self, id: &str) -> Option<Arc<dyn CdsInvocation>> {
        self.inner.iter().find(|s| s.definition().id == id).cloned()
    }
}
