//! `$validate` operation handlers.
//!
//! Implements the FHIR [Resource-validate operation](https://hl7.org/fhir/OperationDefinition/Resource-validate):
//!
//! - `POST [base]/[type]/$validate` — validate the body (a raw resource or
//!   a `Parameters` wrapper carrying `resource`/`mode`/`profile` parts)
//! - `GET  [base]/[type]/[id]/$validate` — validate the **stored** resource
//! - `POST [base]/[type]/[id]/$validate` — validate the body in the context
//!   of the addressed instance (update semantics when `mode` is omitted)
//!
//! Per the operation definition, validation **always returns `200 OK`**
//! with an `OperationOutcome` — an invalid resource is a successful
//! validation with error issues. Non-200 responses are reserved for
//! malformed requests (unknown type, missing body, bad `Parameters`,
//! `mode=profile` without a profile).
//!
//! `mode` changes enforcement: `create` reports a duplicate id, `update`
//! requires an existing id, `delete` checks existence and AuditEvent
//! immutability (no referential integrity yet), `profile` ignores
//! `meta.profile` and uses only the supplied profile canonicals.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use helios_persistence::core::ResourceStorage;
use serde::Deserialize;
use serde_json::Value;
use tracing::debug;

use crate::error::{RestError, RestResult};
use crate::extractors::{FhirResource, FhirVersionExtractor, TenantExtractor};
use crate::middleware::content_type::negotiate_format;
use crate::responses::format_resource_response;
use crate::responses::operation_outcome::{Issue, IssueType};
use crate::state::AppState;
use crate::validation::validation_outcome_from_parts;

/// Query-string fallbacks for the operation inputs (GET, or POST without a
/// `Parameters` wrapper).
#[derive(Debug, Default, Deserialize)]
pub struct ValidateQuery {
    mode: Option<String>,
    profile: Option<String>,
}

/// The operation inputs after unwrapping body/query.
struct ValidateInputs {
    resource: Option<Value>,
    mode: ValidateMode,
    profiles: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ValidateMode {
    General,
    Create,
    Update,
    Delete,
    Profile,
}

impl ValidateMode {
    fn parse(raw: Option<&str>) -> Result<Self, RestError> {
        match raw {
            None => Ok(ValidateMode::General),
            Some("create") => Ok(ValidateMode::Create),
            Some("update") => Ok(ValidateMode::Update),
            Some("delete") => Ok(ValidateMode::Delete),
            Some("profile") => Ok(ValidateMode::Profile),
            Some(other) => Err(RestError::BadRequest {
                message: format!(
                    "Invalid $validate mode '{other}' (expected create | update | delete | profile)"
                ),
            }),
        }
    }
}

/// Unwrap the request body: either a raw resource, or a `Parameters`
/// carrying `resource` / `mode` / `profile` parts. Query parameters fill in
/// whatever the body did not provide.
fn unwrap_inputs(body: Option<Value>, query: &ValidateQuery) -> Result<ValidateInputs, RestError> {
    let mut resource = None;
    let mut mode_raw: Option<String> = None;
    let mut profiles: Vec<String> = Vec::new();

    if let Some(body) = body {
        if body.get("resourceType").and_then(Value::as_str) == Some("Parameters") {
            for param in body
                .get("parameter")
                .and_then(Value::as_array)
                .map(|a| a.as_slice())
                .unwrap_or_default()
            {
                match param.get("name").and_then(Value::as_str) {
                    Some("resource") => {
                        resource = param.get("resource").cloned();
                    }
                    Some("mode") => {
                        mode_raw = param
                            .get("valueCode")
                            .or_else(|| param.get("valueString"))
                            .and_then(Value::as_str)
                            .map(str::to_string);
                    }
                    Some("profile") => {
                        if let Some(p) = param
                            .get("valueUri")
                            .or_else(|| param.get("valueCanonical"))
                            .or_else(|| param.get("valueString"))
                            .and_then(Value::as_str)
                        {
                            profiles.push(p.to_string());
                        }
                    }
                    _ => {}
                }
            }
        } else {
            resource = Some(body);
        }
    }

    if mode_raw.is_none() {
        mode_raw = query.mode.clone();
    }
    if let Some(p) = &query.profile
        && !profiles.iter().any(|existing| existing == p)
    {
        profiles.push(p.clone());
    }

    let mode = ValidateMode::parse(mode_raw.as_deref())?;
    if mode == ValidateMode::Profile && profiles.is_empty() {
        return Err(RestError::BadRequest {
            message: "$validate mode=profile requires a profile parameter".to_string(),
        });
    }

    Ok(ValidateInputs {
        resource,
        mode,
        profiles,
    })
}

/// Shared tail: run validation and shape the 200 OperationOutcome response.
async fn respond<S>(
    state: &AppState<S>,
    fhir_version: helios_fhir::FhirVersion,
    mut inputs: ValidateInputs,
    resource_type: &str,
    tenant: &TenantExtractor,
    req_headers: &HeaderMap,
    instance_id: Option<String>,
    instance_write: bool,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    let negotiated = negotiate_format(req_headers, None);
    let instance_id = instance_id.or_else(|| {
        inputs
            .resource
            .as_ref()
            .and_then(|r| r.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string)
    });
    if instance_write && inputs.mode == ValidateMode::General {
        inputs.mode = ValidateMode::Update;
    }

    if inputs.mode == ValidateMode::Delete {
        let extra =
            delete_mode_issues(state, tenant, resource_type, instance_id.as_deref()).await?;
        let outcome = validation_outcome_from_parts(&[], extra);
        return format_resource_response(
            StatusCode::OK,
            HeaderMap::new(),
            &outcome,
            negotiated.format,
        )
        .map_err(|_| RestError::InternalError {
            message: "Failed to serialize response".to_string(),
        });
    }

    let Some(resource) = inputs.resource else {
        return Err(RestError::BadRequest {
            message: "$validate requires a resource (in the body or as the 'resource' parameter)"
                .to_string(),
        });
    };

    // The resource must be of the addressed type.
    if let Some(body_type) = resource.get("resourceType").and_then(Value::as_str)
        && body_type != resource_type
    {
        return Err(RestError::BadRequest {
            message: format!(
                "Resource type in body ({body_type}) does not match URL ({resource_type})"
            ),
        });
    }

    let issues = if inputs.mode == ValidateMode::Profile {
        state
            .validation()
            .validate_resource_with(
                fhir_version,
                &resource,
                inputs.profiles,
                Some(tenant.tenant_id()),
                false,
            )
            .await
    } else {
        state
            .validation()
            .validate_resource(
                fhir_version,
                &resource,
                inputs.profiles,
                Some(tenant.tenant_id()),
            )
            .await
    };

    let extra = match inputs.mode {
        ValidateMode::Create => {
            create_mode_issues(state, tenant, resource_type, instance_id.as_deref()).await?
        }
        ValidateMode::Update => {
            update_mode_issues(state, tenant, resource_type, instance_id.as_deref()).await?
        }
        _ => Vec::new(),
    };

    debug!(
        resource_type = %resource_type,
        issue_count = issues.len() + extra.len(),
        "$validate completed"
    );

    let outcome = validation_outcome_from_parts(&issues, extra);
    format_resource_response(
        StatusCode::OK,
        HeaderMap::new(),
        &outcome,
        negotiated.format,
    )
    .map_err(|_| RestError::InternalError {
        message: "Failed to serialize response".to_string(),
    })
}

async fn create_mode_issues<S>(
    state: &AppState<S>,
    tenant: &TenantExtractor,
    resource_type: &str,
    instance_id: Option<&str>,
) -> RestResult<Vec<Issue>>
where
    S: ResourceStorage + Send + Sync,
{
    let Some(id) = instance_id.filter(|id| !id.is_empty()) else {
        return Ok(Vec::new());
    };
    if state
        .storage()
        .read(tenant.context(), resource_type, id)
        .await?
        .is_some()
    {
        return Ok(vec![Issue::error(
            IssueType::Duplicate,
            format!("{resource_type}/{id} already exists; mode=create is not applicable"),
        )]);
    }
    Ok(Vec::new())
}

async fn update_mode_issues<S>(
    state: &AppState<S>,
    tenant: &TenantExtractor,
    resource_type: &str,
    instance_id: Option<&str>,
) -> RestResult<Vec<Issue>>
where
    S: ResourceStorage + Send + Sync,
{
    let Some(id) = instance_id.filter(|id| !id.is_empty()) else {
        return Ok(vec![Issue::error(
            IssueType::Required,
            "mode=update requires a resource id (URL or body)",
        )]);
    };
    if state
        .storage()
        .read(tenant.context(), resource_type, id)
        .await?
        .is_none()
    {
        return Ok(vec![Issue::error(
            IssueType::NotFound,
            format!("{resource_type}/{id} was not found"),
        )]);
    }
    Ok(Vec::new())
}

async fn delete_mode_issues<S>(
    state: &AppState<S>,
    tenant: &TenantExtractor,
    resource_type: &str,
    instance_id: Option<&str>,
) -> RestResult<Vec<Issue>>
where
    S: ResourceStorage + Send + Sync,
{
    let Some(id) = instance_id.filter(|id| !id.is_empty()) else {
        return Ok(vec![Issue::error(
            IssueType::Required,
            "mode=delete requires a resource id (URL or body)",
        )]);
    };
    if resource_type == "AuditEvent" {
        return Ok(vec![Issue::error(
            IssueType::NotSupported,
            "AuditEvent resources cannot be deleted",
        )]);
    }
    if state
        .storage()
        .read(tenant.context(), resource_type, id)
        .await?
        .is_none()
    {
        return Ok(vec![Issue::error(
            IssueType::NotFound,
            format!("{resource_type}/{id} was not found"),
        )]);
    }
    Ok(Vec::new())
}

/// `POST [base]/[type]/$validate`
pub async fn validate_type_handler<S>(
    State(state): State<AppState<S>>,
    Path(resource_type): Path<String>,
    tenant: TenantExtractor,
    version: FhirVersionExtractor,
    Query(query): Query<ValidateQuery>,
    req_headers: HeaderMap,
    FhirResource(body): FhirResource,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    let inputs = unwrap_inputs(Some(body), &query)?;
    respond(
        &state,
        version.storage_version_or(state.config().default_fhir_version),
        inputs,
        &resource_type,
        &tenant,
        &req_headers,
        None,
        false,
    )
    .await
}

/// `GET [base]/[type]/[id]/$validate` — validate the stored resource.
pub async fn validate_instance_get_handler<S>(
    State(state): State<AppState<S>>,
    Path((resource_type, id)): Path<(String, String)>,
    tenant: TenantExtractor,
    version: FhirVersionExtractor,
    Query(query): Query<ValidateQuery>,
    req_headers: HeaderMap,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    let stored = state
        .storage()
        .read(tenant.context(), &resource_type, &id)
        .await?
        .ok_or_else(|| RestError::NotFound {
            resource_type: resource_type.clone(),
            id: id.clone(),
        })?;

    let mut inputs = unwrap_inputs(None, &query)?;
    inputs.resource = Some(stored.content().clone());
    respond(
        &state,
        version.storage_version_or(state.config().default_fhir_version),
        inputs,
        &resource_type,
        &tenant,
        &req_headers,
        Some(id),
        false,
    )
    .await
}

/// `POST [base]/[type]/[id]/$validate` — validate the body in the context
/// of the addressed instance (update semantics).
pub async fn validate_instance_post_handler<S>(
    State(state): State<AppState<S>>,
    Path((resource_type, id)): Path<(String, String)>,
    tenant: TenantExtractor,
    version: FhirVersionExtractor,
    Query(query): Query<ValidateQuery>,
    req_headers: HeaderMap,
    FhirResource(body): FhirResource,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    let inputs = unwrap_inputs(Some(body), &query)?;
    respond(
        &state,
        version.storage_version_or(state.config().default_fhir_version),
        inputs,
        &resource_type,
        &tenant,
        &req_headers,
        Some(id),
        true,
    )
    .await
}
