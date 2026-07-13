//! Persist CDS Hooks feedback (accepted / overridden cards) as FHIR **`GuidanceResponse`**
//! resources on the clinical FHIR server.
//!
//! Feedback is the audit trail for interruptive alerts: which suggestions clinicians
//! accepted, which cards they overrode, and why. Each [`Feedback`] entry becomes one
//! `GuidanceResponse` POSTed to `{CDS_FEEDBACK_FHIR_BASE_URL}/GuidanceResponse`.
//!
//! Persistence failures are logged but do not fail the feedback endpoint — the CDS Hooks
//! spec requires a 200 response to feedback regardless.

use helios_cds_hooks::{Feedback, FeedbackOutcome, FeedbackRequest};
use serde_json::{Value, json};
use tracing::{debug, error};

/// Extension carrying the structured CDS Hooks feedback payload on the GuidanceResponse.
const FEEDBACK_EXTENSION_URL: &str = "https://atrius.in/fhir/StructureDefinition/cds-feedback";
/// Identifier system for CDS card uuids.
const CARD_IDENTIFIER_SYSTEM: &str = "https://atrius.in/fhir/cds-hooks/card";

/// Writes feedback GuidanceResponses to a clinical FHIR base.
#[derive(Debug)]
pub struct FeedbackStore {
    http: reqwest::Client,
    fhir_base_url: String,
    bearer_token: Option<String>,
    tenant_id: Option<String>,
}

impl FeedbackStore {
    pub fn new(
        http: reqwest::Client,
        fhir_base_url: impl Into<String>,
        bearer_token: Option<String>,
        tenant_id: Option<String>,
    ) -> Self {
        Self {
            http,
            fhir_base_url: fhir_base_url.into(),
            bearer_token: bearer_token.filter(|t| !t.trim().is_empty()),
            tenant_id: tenant_id.filter(|t| !t.trim().is_empty()),
        }
    }

    /// Persist every feedback entry for a service invocation. Errors are logged, not returned.
    pub async fn record(
        &self,
        service_id: &str,
        plan_definition_url: Option<&str>,
        request: &FeedbackRequest,
    ) {
        for feedback in &request.feedback {
            let resource = guidance_response_for_feedback(service_id, plan_definition_url, feedback);
            self.post_guidance_response(service_id, &feedback.card, resource)
                .await;
        }
    }

    async fn post_guidance_response(&self, service_id: &str, card: &str, resource: Value) {
        let url = format!(
            "{}/GuidanceResponse",
            self.fhir_base_url.trim_end_matches('/')
        );
        let mut req = self
            .http
            .post(&url)
            .header("Content-Type", "application/fhir+json")
            .json(&resource);
        if let Some(ref token) = self.bearer_token {
            req = req.bearer_auth(token);
        }
        if let Some(ref tenant) = self.tenant_id {
            req = req.header("X-Tenant-ID", tenant);
        }

        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                debug!(service_id, card, "cds feedback persisted as GuidanceResponse");
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                error!(
                    service_id,
                    card,
                    %status,
                    body = %body.chars().take(500).collect::<String>(),
                    "cds feedback GuidanceResponse write rejected"
                );
            }
            Err(e) => {
                error!(service_id, card, error = %e, "cds feedback GuidanceResponse write failed");
            }
        }
    }
}

/// Build the FHIR R4 GuidanceResponse for one feedback entry.
fn guidance_response_for_feedback(
    service_id: &str,
    plan_definition_url: Option<&str>,
    feedback: &Feedback,
) -> Value {
    let outcome_code = match feedback.outcome {
        FeedbackOutcome::Accepted => "accepted",
        FeedbackOutcome::Overridden => "overridden",
    };

    let mut note_lines = vec![format!("CDS card {outcome_code} (service `{service_id}`)")];
    if let Some(ref accepted) = feedback.accepted_suggestions {
        let ids: Vec<&str> = accepted.iter().map(|s| s.id.as_str()).collect();
        note_lines.push(format!("Accepted suggestions: {}", ids.join(", ")));
    }
    if let Some(ref reason) = feedback.override_reason {
        if let Some(ref coding) = reason.reason {
            note_lines.push(format!(
                "Override reason: {}",
                coding.display.as_deref().unwrap_or(&coding.code)
            ));
        }
        if let Some(ref comment) = reason.user_comment {
            note_lines.push(format!("Comment: {comment}"));
        }
    }

    let mut resource = json!({
        "resourceType": "GuidanceResponse",
        "status": "success",
        "requestIdentifier": {
            "system": CARD_IDENTIFIER_SYSTEM,
            "value": feedback.card
        },
        "occurrenceDateTime": feedback.outcome_timestamp.to_rfc3339(),
        "note": [{ "text": note_lines.join("\n") }],
        "extension": [{
            "url": FEEDBACK_EXTENSION_URL,
            "valueString": serde_json::to_string(&json!({
                "serviceId": service_id,
                "card": feedback.card,
                "outcome": outcome_code,
                "acceptedSuggestions": feedback.accepted_suggestions,
                "overrideReason": feedback.override_reason,
            })).unwrap_or_default()
        }]
    });

    match plan_definition_url {
        Some(url) if !url.trim().is_empty() => {
            resource["moduleCanonical"] = json!(url);
        }
        _ => {
            resource["moduleUri"] = json!(format!("urn:atrius:cds-service:{service_id}"));
        }
    }

    resource
}

#[cfg(test)]
mod tests {
    use super::*;
    use helios_cds_hooks::{AcceptedSuggestion, Coding, OverrideReason};

    fn feedback(outcome: FeedbackOutcome) -> Feedback {
        Feedback {
            card: "card-1".into(),
            outcome,
            accepted_suggestions: Some(vec![AcceptedSuggestion { id: "acs-ecg".into() }]),
            override_reason: Some(OverrideReason {
                reason: Some(Coding {
                    code: "clinical-judgment".into(),
                    system: "https://atrius.in/fhir/CodeSystem/cds-override-reason".into(),
                    display: Some("Clinical judgment".into()),
                }),
                user_comment: Some("Not applicable for this patient".into()),
            }),
            outcome_timestamp: "2026-07-11T10:00:00Z".parse().unwrap(),
        }
    }

    #[test]
    fn builds_guidance_response_with_module_canonical() {
        let gr = guidance_response_for_feedback(
            "er-chest-pain-pathway",
            Some("https://atrius.in/fhir/r4/atrius-in/PlanDefinition/er-chest-pain-pathway"),
            &feedback(FeedbackOutcome::Accepted),
        );
        assert_eq!(gr["resourceType"], "GuidanceResponse");
        assert_eq!(gr["status"], "success");
        assert_eq!(gr["requestIdentifier"]["value"], "card-1");
        assert!(gr["moduleCanonical"].as_str().unwrap().contains("er-chest-pain-pathway"));
        assert!(gr["note"][0]["text"].as_str().unwrap().contains("accepted"));
        assert!(gr["note"][0]["text"].as_str().unwrap().contains("acs-ecg"));
    }

    #[test]
    fn falls_back_to_module_uri_without_plan_definition() {
        let gr = guidance_response_for_feedback("svc-x", None, &feedback(FeedbackOutcome::Overridden));
        assert_eq!(gr["moduleUri"], "urn:atrius:cds-service:svc-x");
        let note = gr["note"][0]["text"].as_str().unwrap();
        assert!(note.contains("overridden"));
        assert!(note.contains("Clinical judgment"));
        assert!(note.contains("Not applicable"));
    }
}
