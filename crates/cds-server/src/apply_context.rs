//! Map CDS Hooks hook context → sidecar **`PlanDefinition/$apply`** operation parameters.

use crate::clinical_reasoning::ApplyPlanDefinitionRequestBuilder;
use helios_cds_hooks::CdsRequest;
use serde_json::{Value, json};

/// Optional `$apply` context fields extracted from CDS Hooks `context` JSON.
#[derive(Debug, Clone, Default)]
pub struct ApplyHookContext {
    pub encounter_id: Option<String>,
    pub practitioner_id: Option<String>,
    pub organization_id: Option<String>,
    pub user_type: Option<Value>,
    pub user_language: Option<Value>,
    pub user_task_context: Option<Value>,
    pub setting: Option<Value>,
    pub setting_context: Option<Value>,
}

/// Extract FHIR `$apply` inputs from any CDS Hooks library hook request.
pub fn apply_hook_context(request: &CdsRequest) -> ApplyHookContext {
    let raw = &request.context;
    let user_id = optional_string(raw, "userId");

    ApplyHookContext {
        encounter_id: optional_string(raw, "encounterId"),
        practitioner_id: user_id.as_deref().and_then(practitioner_from_user_id),
        organization_id: optional_string(raw, "organizationId"),
        user_type: raw
            .get("userType")
            .cloned()
            .or_else(|| user_id.as_deref().and_then(user_type_from_user_id)),
        user_language: raw.get("userLanguage").cloned(),
        user_task_context: raw
            .get("userTaskContext")
            .cloned()
            .or_else(|| user_task_context_for_hook(&request.hook)),
        setting: raw
            .get("setting")
            .cloned()
            .or_else(|| default_setting_for_hook(&request.hook)),
        setting_context: raw.get("settingContext").cloned(),
    }
}

/// Apply extracted hook context onto an [`ApplyPlanDefinitionRequestBuilder`].
pub fn apply_hook_context_to_builder(
    mut builder: ApplyPlanDefinitionRequestBuilder,
    ctx: &ApplyHookContext,
) -> ApplyPlanDefinitionRequestBuilder {
    if let Some(ref id) = ctx.encounter_id {
        builder = builder.encounter_id(id.clone());
    }
    if let Some(ref id) = ctx.practitioner_id {
        builder = builder.practitioner_id(id.clone());
    }
    if let Some(ref id) = ctx.organization_id {
        builder = builder.organization_id(id.clone());
    }
    if let Some(ref v) = ctx.user_type {
        builder = builder.user_type(v.clone());
    }
    if let Some(ref v) = ctx.user_language {
        builder = builder.user_language(v.clone());
    }
    if let Some(ref v) = ctx.user_task_context {
        builder = builder.user_task_context(v.clone());
    }
    if let Some(ref v) = ctx.setting {
        builder = builder.setting(v.clone());
    }
    if let Some(ref v) = ctx.setting_context {
        builder = builder.setting_context(v.clone());
    }
    builder
}

fn optional_string(raw: &Value, key: &str) -> Option<String> {
    raw.get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// CDS `userId` → `$apply` **practitioner** when it references a clinical user.
fn practitioner_from_user_id(user_id: &str) -> Option<String> {
    let trimmed = user_id.trim();
    if trimmed.is_empty() {
        return None;
    }
    let resource_type = trimmed.split('/').next().unwrap_or("");
    match resource_type {
        "Practitioner" | "PractitionerRole" => Some(trimmed.to_string()),
        _ => None,
    }
}

/// Derive **userType** from `userId` resource type when not supplied explicitly on context.
fn user_type_from_user_id(user_id: &str) -> Option<Value> {
    let resource_type = user_id.split('/').next().unwrap_or("");
    let code = match resource_type {
        "Practitioner" | "PractitionerRole" => "provider",
        "Patient" | "RelatedPerson" => "patient",
        _ => return None,
    };
    Some(json!({
        "coding": [{
            "system": "http://terminology.hl7.org/CodeSystem/participant-type",
            "code": code
        }]
    }))
}

fn user_task_context_for_hook(hook: &str) -> Option<Value> {
    let text = match hook {
        "patient-view" => "patient chart review",
        "order-select" => "order entry",
        "order-sign" => "order signing",
        "encounter-start" => "encounter start",
        "encounter-discharge" => "encounter discharge",
        "appointment-book" => "appointment booking",
        "order-dispatch" => "order dispatch",
        "allergyintolerance-create" => "allergy documentation",
        "medication-refill" => "medication refill",
        "problem-list-item-create" => "problem list documentation",
        _ => return None,
    };
    Some(json!({ "text": text }))
}

fn default_setting_for_hook(hook: &str) -> Option<Value> {
    let code = match hook {
        "patient-view"
        | "order-select"
        | "order-sign"
        | "appointment-book"
        | "allergyintolerance-create"
        | "medication-refill"
        | "problem-list-item-create" => "AMB",
        "encounter-start" | "encounter-discharge" | "order-dispatch" => "IMP",
        _ => return None,
    };
    Some(json!({
        "coding": [{
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": code
        }]
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn maps_patient_view_context_to_apply_params() {
        let request = CdsRequest {
            hook: "patient-view".into(),
            hook_instance: "test".into(),
            context: json!({
                "patientId": "p1",
                "userId": "PractitionerRole/role-1",
                "encounterId": "enc-1",
                "organizationId": "org-1"
            }),
            prefetch: Some(HashMap::new()),
            fhir_server: None,
            fhir_authorization: None,
            extension: None,
        };
        let ctx = apply_hook_context(&request);
        assert_eq!(ctx.encounter_id.as_deref(), Some("enc-1"));
        assert_eq!(
            ctx.practitioner_id.as_deref(),
            Some("PractitionerRole/role-1")
        );
        assert_eq!(ctx.organization_id.as_deref(), Some("org-1"));
        assert!(ctx.user_type.is_some());
        assert!(ctx.user_task_context.is_some());
        assert!(ctx.setting.is_some());
    }

    #[test]
    fn maps_encounter_start_inpatient_setting() {
        let request = CdsRequest {
            hook: "encounter-start".into(),
            hook_instance: "test".into(),
            context: json!({
                "userId": "Practitioner/1",
                "patientId": "p1",
                "encounterId": "e1"
            }),
            prefetch: Some(HashMap::new()),
            fhir_server: None,
            fhir_authorization: None,
            extension: None,
        };
        let ctx = apply_hook_context(&request);
        assert_eq!(
            ctx.setting
                .as_ref()
                .and_then(|v| v["coding"][0]["code"].as_str()),
            Some("IMP")
        );
    }

    #[test]
    fn practitioner_from_user_id_skips_patient() {
        assert!(practitioner_from_user_id("Patient/p1").is_none());
        assert_eq!(
            practitioner_from_user_id("Practitioner/doc").as_deref(),
            Some("Practitioner/doc")
        );
    }
}
