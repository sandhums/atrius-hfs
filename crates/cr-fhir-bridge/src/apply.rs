//! FHIR `$apply` proxy: Parameters → JVM sidecar → Parameters with `return`.

use std::sync::Arc;

use atrius_clinical_reasoning::{
    ApplyActivityDefinitionRequest, ApplyPlanDefinitionRequest, ClinicalReasoningClient,
    SidecarFhirAuthorization,
};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use tracing::debug;

use crate::fhir_parameters::{
    ApplyInput, ParametersParseError, parameters_with_return, parse_apply_parameters,
};
use crate::proxy::BridgeState;

pub async fn plan_definition_type_apply(
    State(state): State<Arc<BridgeState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    apply_plan_definition(state, headers, body, None).await
}

pub async fn plan_definition_instance_apply(
    State(state): State<Arc<BridgeState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    apply_plan_definition(state, headers, body, Some(id)).await
}

pub async fn activity_definition_type_apply(
    State(state): State<Arc<BridgeState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    apply_activity_definition(state, headers, body, None).await
}

pub async fn activity_definition_instance_apply(
    State(state): State<Arc<BridgeState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    apply_activity_definition(state, headers, body, Some(id)).await
}

async fn apply_plan_definition(
    state: Arc<BridgeState>,
    headers: HeaderMap,
    body: Bytes,
    instance_id: Option<String>,
) -> Response {
    let Some(cr) = state.cr.as_ref() else {
        return apply_not_configured();
    };

    let parsed: Value = match parse_json_body(&body, state.max_body_size) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    let input = match parse_apply_parameters(&parsed, instance_id.as_deref(), "planDefinition") {
        Ok(v) => v,
        Err(e) => return parameters_error_response(StatusCode::BAD_REQUEST, &e),
    };

    let req = match build_plan_definition_request(cr, &input, bearer_auth(&headers)) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };

    debug!(
        plan_definition_id = ?req.plan_definition_id,
        plan_definition_url = ?req.plan_definition_url,
        patient_id = %req.patient_id,
        "bridge PlanDefinition/$apply via sidecar"
    );

    match cr.client.apply_plan_definition(req).await {
        Ok(resp) => {
            let return_resource = resp.care_plan_value().clone();
            fhir_json_response(StatusCode::OK, parameters_with_return(return_resource))
        }
        Err(e) => sidecar_error_response(e),
    }
}

async fn apply_activity_definition(
    state: Arc<BridgeState>,
    headers: HeaderMap,
    body: Bytes,
    instance_id: Option<String>,
) -> Response {
    let Some(cr) = state.cr.as_ref() else {
        return apply_not_configured();
    };

    let parsed: Value = match parse_json_body(&body, state.max_body_size) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    let input = match parse_apply_parameters(&parsed, instance_id.as_deref(), "activityDefinition")
    {
        Ok(v) => v,
        Err(e) => return parameters_error_response(StatusCode::BAD_REQUEST, &e),
    };

    let req = match build_activity_definition_request(cr, &input, bearer_auth(&headers)) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };

    debug!(
        activity_definition_id = ?req.activity_definition_id,
        activity_definition_url = ?req.activity_definition_url,
        patient_id = %req.patient_id,
        "bridge ActivityDefinition/$apply via sidecar"
    );

    match cr.client.apply_activity_definition(req).await {
        Ok(resp) => {
            let return_resource = resp.resource_value().clone();
            fhir_json_response(StatusCode::OK, parameters_with_return(return_resource))
        }
        Err(e) => sidecar_error_response(e),
    }
}

fn build_plan_definition_request(
    cr: &ClinicalReasoningEndpoints,
    input: &ApplyInput,
    auth: Option<SidecarFhirAuthorization>,
) -> Result<ApplyPlanDefinitionRequest, Box<Response>> {
    let mut patient_id = input.subject.clone();
    if !patient_id.contains('/') {
        patient_id = format!("Patient/{patient_id}");
    }

    let req = ApplyPlanDefinitionRequest {
        plan_definition_id: input.definition_id.clone(),
        plan_definition_url: input.definition_url.clone(),
        patient_id,
        encounter_id: input.encounter.clone(),
        practitioner_id: input.practitioner.clone(),
        organization_id: input.organization.clone(),
        user_type: input.user_type.clone(),
        user_language: input.user_language.clone(),
        user_task_context: input.user_task_context.clone(),
        setting: input.setting.clone(),
        setting_context: input.setting_context.clone(),
        hfs_base_url: cr.bridge_base.clone(),
        hts_base_url: cr.hts_base.clone(),
        library_base_url: Some(cr.library_base.clone()),
        use_server_data: false,
        prefetch: None,
        parameters: input.cql_parameters.clone(),
        fhir_authorization: auth,
    };

    if req.plan_definition_id.is_none() && req.plan_definition_url.is_none() {
        return Err(Box::new(parameters_error_response(
            StatusCode::BAD_REQUEST,
            &ParametersParseError::MissingRequired("planDefinition"),
        )));
    }

    Ok(req)
}

fn build_activity_definition_request(
    cr: &ClinicalReasoningEndpoints,
    input: &ApplyInput,
    auth: Option<SidecarFhirAuthorization>,
) -> Result<ApplyActivityDefinitionRequest, Box<Response>> {
    let mut patient_id = input.subject.clone();
    if !patient_id.contains('/') {
        patient_id = format!("Patient/{patient_id}");
    }

    let req = ApplyActivityDefinitionRequest {
        activity_definition_id: input.definition_id.clone(),
        activity_definition_url: input.definition_url.clone(),
        patient_id,
        encounter_id: input.encounter.clone(),
        practitioner_id: input.practitioner.clone(),
        organization_id: input.organization.clone(),
        user_type: input.user_type.clone(),
        user_language: input.user_language.clone(),
        user_task_context: input.user_task_context.clone(),
        setting: input.setting.clone(),
        setting_context: input.setting_context.clone(),
        hfs_base_url: cr.bridge_base.clone(),
        hts_base_url: cr.hts_base.clone(),
        library_base_url: Some(cr.library_base.clone()),
        use_server_data: false,
        prefetch: None,
        parameters: input.cql_parameters.clone(),
        fhir_authorization: auth,
    };

    if req.activity_definition_id.is_none() && req.activity_definition_url.is_none() {
        return Err(Box::new(parameters_error_response(
            StatusCode::BAD_REQUEST,
            &ParametersParseError::MissingRequired("activityDefinition"),
        )));
    }

    Ok(req)
}

fn parse_json_body(body: &Bytes, max: usize) -> Result<Value, Box<Response>> {
    if body.len() > max {
        return Err(Box::new(operation_outcome_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "error",
            "too-costly",
            format!("request body exceeds max size ({max} bytes)"),
        )));
    }
    serde_json::from_slice(body).map_err(|e| {
        Box::new(operation_outcome_response(
            StatusCode::BAD_REQUEST,
            "error",
            "invalid",
            format!("invalid JSON: {e}"),
        ))
    })
}

fn bearer_auth(headers: &HeaderMap) -> Option<SidecarFhirAuthorization> {
    let header = headers.get("authorization")?.to_str().ok()?;
    let token = header.strip_prefix("Bearer ")?.trim();
    if token.is_empty() {
        return None;
    }
    Some(SidecarFhirAuthorization::access_token_only(token))
}

fn apply_not_configured() -> Response {
    operation_outcome_response(
        StatusCode::NOT_IMPLEMENTED,
        "error",
        "not-supported",
        "PlanDefinition/ActivityDefinition $apply requires CR_FHIR_BRIDGE_SIDECAR_URL",
    )
}

fn parameters_error_response(status: StatusCode, err: &ParametersParseError) -> Response {
    let code = match err {
        ParametersParseError::NotParameters | ParametersParseError::InvalidParameter { .. } => {
            "invalid"
        }
        ParametersParseError::MissingRequired(_)
        | ParametersParseError::DisallowedOnInstance(_) => "required",
    };
    operation_outcome_response(status, "error", code, err.to_string())
}

fn sidecar_error_response(err: atrius_clinical_reasoning::ClinicalReasoningError) -> Response {
    use atrius_clinical_reasoning::ClinicalReasoningError;

    match err {
        ClinicalReasoningError::InvalidUrl(msg) => operation_outcome_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "error",
            "exception",
            format!("clinical reasoning URL misconfigured: {msg}"),
        ),
        ClinicalReasoningError::Http(msg) => operation_outcome_response(
            StatusCode::BAD_GATEWAY,
            "error",
            "transient",
            format!("cannot reach clinical reasoning sidecar: {msg}"),
        ),
        ClinicalReasoningError::SidecarRejected(r) => {
            let status = match r.status {
                404 | 410 => StatusCode::NOT_FOUND,
                400 | 422 => StatusCode::UNPROCESSABLE_ENTITY,
                s if (500..600).contains(&s) => StatusCode::BAD_GATEWAY,
                _ => StatusCode::BAD_GATEWAY,
            };
            operation_outcome_response(status, "error", "processing", r.summarize())
        }
    }
}

fn operation_outcome_response(
    status: StatusCode,
    severity: &str,
    code: &str,
    diagnostics: impl Into<String>,
) -> Response {
    let body = json!({
        "resourceType": "OperationOutcome",
        "issue": [{
            "severity": severity,
            "code": code,
            "diagnostics": diagnostics.into()
        }]
    });
    fhir_json_response(status, body)
}

fn fhir_json_response(status: StatusCode, body: Value) -> Response {
    (
        status,
        [(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/fhir+json"),
        )],
        Json(body),
    )
        .into_response()
}

/// Sidecar + terminology endpoints wired for `$apply`.
#[derive(Clone)]
pub struct ClinicalReasoningEndpoints {
    pub bridge_base: String,
    pub library_base: String,
    pub hts_base: String,
    pub client: ClinicalReasoningClient,
}

impl ClinicalReasoningEndpoints {
    pub fn new(
        bridge_base: String,
        library_base: String,
        hts_base: String,
        client: ClinicalReasoningClient,
    ) -> Self {
        Self {
            bridge_base,
            library_base,
            hts_base,
            client,
        }
    }
}
