//! Parse CDS Hooks **library** hook contexts for sidecar invocation.
//!
//! All hooks defined in [`helios_cds_hooks::LIBRARY_HOOK_NAMES`] share `patientId` and are
//! validated with the typed context structs from `helios-cds-hooks`.

use helios_cds_hooks::{
    AllergyIntoleranceCreateContext, AppointmentBookContext, CdsHooksError, CdsRequest,
    EncounterDischargeContext, EncounterStartContext, MeasurementPeriodContext,
    MedicationRefillContext, OrderDispatchContext, OrderSelectContext, OrderSignContext,
    PatientViewContext, ProblemListItemCreateContext, hooks::HookContext, is_library_hook,
};

/// Parsed fields required to invoke the clinical reasoning sidecar for any library hook.
#[derive(Debug, Clone)]
pub struct InvokeHookContext {
    pub patient_id: String,
    pub user_id: Option<String>,
    pub measurement_period: Option<MeasurementPeriodContext>,
}

/// Validate hook name and deserialize `context` into typed CDS Hooks fields.
pub fn parse_invoke_hook_context(request: &CdsRequest) -> Result<InvokeHookContext, CdsHooksError> {
    if !is_library_hook(&request.hook) {
        return Err(CdsHooksError::InternalError(format!(
            "unsupported hook `{}`",
            request.hook
        )));
    }

    let ctx = request.context.clone();
    match request.hook.as_str() {
        PatientViewContext::HOOK_NAME => {
            let c: PatientViewContext = serde_json::from_value(ctx)?;
            Ok(InvokeHookContext {
                patient_id: c.patient_id,
                user_id: Some(c.user_id),
                measurement_period: c.measurement_period,
            })
        }
        OrderSelectContext::HOOK_NAME => {
            let c: OrderSelectContext = serde_json::from_value(ctx)?;
            Ok(InvokeHookContext {
                patient_id: c.patient_id,
                user_id: Some(c.user_id),
                measurement_period: measurement_period_from_raw(&request.context),
            })
        }
        OrderSignContext::HOOK_NAME => {
            let c: OrderSignContext = serde_json::from_value(ctx)?;
            Ok(InvokeHookContext {
                patient_id: c.patient_id,
                user_id: Some(c.user_id),
                measurement_period: measurement_period_from_raw(&request.context),
            })
        }
        EncounterStartContext::HOOK_NAME => {
            let c: EncounterStartContext = serde_json::from_value(ctx)?;
            Ok(InvokeHookContext {
                patient_id: c.patient_id,
                user_id: Some(c.user_id),
                measurement_period: measurement_period_from_raw(&request.context),
            })
        }
        EncounterDischargeContext::HOOK_NAME => {
            let c: EncounterDischargeContext = serde_json::from_value(ctx)?;
            Ok(InvokeHookContext {
                patient_id: c.patient_id,
                user_id: Some(c.user_id),
                measurement_period: measurement_period_from_raw(&request.context),
            })
        }
        AppointmentBookContext::HOOK_NAME => {
            let c: AppointmentBookContext = serde_json::from_value(ctx)?;
            Ok(InvokeHookContext {
                patient_id: c.patient_id,
                user_id: Some(c.user_id),
                measurement_period: measurement_period_from_raw(&request.context),
            })
        }
        OrderDispatchContext::HOOK_NAME => {
            let c: OrderDispatchContext = serde_json::from_value(ctx)?;
            Ok(InvokeHookContext {
                patient_id: c.patient_id,
                user_id: None,
                measurement_period: measurement_period_from_raw(&request.context),
            })
        }
        AllergyIntoleranceCreateContext::HOOK_NAME => {
            let c: AllergyIntoleranceCreateContext = serde_json::from_value(ctx)?;
            Ok(InvokeHookContext {
                patient_id: c.patient_id,
                user_id: Some(c.user_id),
                measurement_period: measurement_period_from_raw(&request.context),
            })
        }
        MedicationRefillContext::HOOK_NAME => {
            let c: MedicationRefillContext = serde_json::from_value(ctx)?;
            Ok(InvokeHookContext {
                patient_id: c.patient_id,
                user_id: c.user_id,
                measurement_period: measurement_period_from_raw(&request.context),
            })
        }
        ProblemListItemCreateContext::HOOK_NAME => {
            let c: ProblemListItemCreateContext = serde_json::from_value(ctx)?;
            Ok(InvokeHookContext {
                patient_id: c.patient_id,
                user_id: Some(c.user_id),
                measurement_period: measurement_period_from_raw(&request.context),
            })
        }
        other => Err(CdsHooksError::InternalError(format!(
            "hook `{other}` not wired for context parsing"
        ))),
    }
}

fn measurement_period_from_raw(context: &serde_json::Value) -> Option<MeasurementPeriodContext> {
    context
        .get("measurementPeriod")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn request(hook: &str, context: serde_json::Value) -> CdsRequest {
        CdsRequest {
            hook: hook.into(),
            hook_instance: "test".into(),
            context,
            prefetch: Some(HashMap::new()),
            fhir_server: None,
            fhir_authorization: None,
            extension: None,
        }
    }

    #[test]
    fn parses_encounter_start_context() {
        let ctx = parse_invoke_hook_context(&request(
            "encounter-start",
            json!({
                "userId": "Practitioner/1",
                "patientId": "p1",
                "encounterId": "e1"
            }),
        ))
        .unwrap();
        assert_eq!(ctx.patient_id, "p1");
        assert_eq!(ctx.user_id.as_deref(), Some("Practitioner/1"));
    }

    #[test]
    fn parses_order_sign_context() {
        let ctx = parse_invoke_hook_context(&request(
            "order-sign",
            json!({
                "userId": "PractitionerRole/1",
                "patientId": "p1",
                "draftOrders": { "resourceType": "Bundle", "type": "collection", "entry": [] }
            }),
        ))
        .unwrap();
        assert_eq!(ctx.patient_id, "p1");
    }

    #[test]
    fn parses_order_dispatch_without_user_id() {
        let ctx = parse_invoke_hook_context(&request(
            "order-dispatch",
            json!({
                "patientId": "p1",
                "dispatchedOrders": ["ServiceRequest/s1"],
                "performer": "Organization/o1"
            }),
        ))
        .unwrap();
        assert_eq!(ctx.patient_id, "p1");
        assert!(ctx.user_id.is_none());
    }

    #[test]
    fn rejects_invalid_context() {
        let err =
            parse_invoke_hook_context(&request("encounter-start", json!({ "patientId": "p1" })))
                .unwrap_err();
        assert!(matches!(err, CdsHooksError::InvalidContext(_)));
    }
}
