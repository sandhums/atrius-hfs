//! `patient-view` service: wires [`crate::evaluate`] into CDS Hooks types.

use helios_cds_hooks::hooks::PatientViewContext;
use helios_cds_hooks::{
    Card, CdsHooksError, CdsHooksService, CdsRequest, CdsResponse, CdsService, FeedbackRequest,
    hooks::HookContext,
};

use crate::patient_view_greeting;

/// Discovery id `patient-greeter` — used by the `cds-hooks-server` demo binary.
pub struct PatientGreeterService;

#[async_trait::async_trait]
impl CdsHooksService for PatientGreeterService {
    type Context = PatientViewContext;

    fn definition(&self) -> CdsService {
        CdsService {
            hook: PatientViewContext::HOOK_NAME.to_string(),
            title: Some("Patient Greeter (demo)".to_string()),
            description: "Greeting for patient-view (demo binary via cds-core)".to_string(),
            id: "patient-greeter".to_string(),
            prefetch: None,
            usage_requirements: None,
            version: None,
            extension: None,
        }
    }

    async fn call(
        &self,
        request: &CdsRequest,
        context: &PatientViewContext,
    ) -> Result<CdsResponse, CdsHooksError> {
        let summary = patient_view_greeting(request, context).await;
        Ok(CdsResponse::with_cards(vec![Card::info(
            summary,
            "Patient Greeter",
        )]))
    }

    async fn on_feedback(&self, _feedback: &FeedbackRequest) -> Result<(), CdsHooksError> {
        Ok(())
    }
}
