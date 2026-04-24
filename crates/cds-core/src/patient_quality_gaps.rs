//! `patient-view` CDS service for **quality / care gaps** only (separate discovery id from the greeter).

use std::collections::HashMap;

use helios_cds_hooks::hooks::PatientViewContext;
use helios_cds_hooks::{
    Card, CdsHooksError, CdsHooksService, CdsRequest, CdsResponse, CdsService, FeedbackRequest,
    hooks::HookContext,
};

use crate::gaps::evaluate_patient_view_gaps;

/// Discovery id `patient-quality-gaps` — same hook as [`PatientGreeterService`](crate::PatientGreeterService), different service.
pub struct PatientViewQualityGapsService;

#[async_trait::async_trait]
impl CdsHooksService for PatientViewQualityGapsService {
    type Context = PatientViewContext;

    fn definition(&self) -> CdsService {
        CdsService {
            hook: PatientViewContext::HOOK_NAME.to_string(),
            title: Some("Quality & care gaps".to_string()),
            description: "Heuristic gap checks from Patient and lab prefetch (not a HEDIS engine).".to_string(),
            id: "patient-quality-gaps".to_string(),
            prefetch: Some(HashMap::from([
                (
                    "patient".to_string(),
                    "Patient/{{context.patientId}}".to_string(),
                ),
                (
                    "laboratory".to_string(),
                    "Observation?patient={{context.patientId}}&code=http://loinc.org|4548-6&_count=20"
                        .to_string(),
                ),
            ])),
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
        let cards: Vec<Card> = evaluate_patient_view_gaps(request, context)
            .into_iter()
            .map(|f| f.to_card())
            .collect();
        Ok(CdsResponse::with_cards(cards))
    }

    async fn on_feedback(&self, _feedback: &FeedbackRequest) -> Result<(), CdsHooksError> {
        Ok(())
    }
}
