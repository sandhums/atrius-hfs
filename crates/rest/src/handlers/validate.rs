//! Resource `$validate` operation handler.

use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use helios_persistence::core::ResourceStorage;
use tracing::debug;

use crate::error::{RestError, RestResult};
use crate::extractors::{FhirResource, FhirVersionExtractor, TenantExtractor};
use crate::profile_validation::extract_resource_from_validate_body;
use crate::state::AppState;

/// `POST [base]/[type]/$validate` — validate a resource against loaded IG profiles.
pub async fn type_validate_handler<S>(
    State(state): State<AppState<S>>,
    Path(resource_type): Path<String>,
    tenant: TenantExtractor,
    version: FhirVersionExtractor,
    FhirResource(body): FhirResource,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    run_validate(&state, &resource_type, None, &tenant, version, &body).await
}

/// `POST [base]/[type]/[id]/$validate` — validate an instance (body may omit id).
pub async fn instance_validate_handler<S>(
    State(state): State<AppState<S>>,
    Path((resource_type, id)): Path<(String, String)>,
    tenant: TenantExtractor,
    version: FhirVersionExtractor,
    FhirResource(body): FhirResource,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    run_validate(&state, &resource_type, Some(&id), &tenant, version, &body).await
}

async fn run_validate<S>(
    state: &AppState<S>,
    resource_type: &str,
    path_id: Option<&str>,
    tenant: &TenantExtractor,
    version: FhirVersionExtractor,
    body: &serde_json::Value,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    let Some(svc) = state.profile_validation() else {
        return Err(RestError::NotImplemented {
            feature: "Resource/$validate (set HFS_PROFILE_MANIFEST)".to_string(),
        });
    };

    let fhir_version = version.storage_version();
    let resource_json = extract_resource_from_validate_body(body)?;

    if let Some(rt) = resource_json.get("resourceType").and_then(|v| v.as_str())
        && rt != resource_type
    {
        return Err(RestError::BadRequest {
            message: format!("Resource type in body ({rt}) does not match URL ({resource_type})"),
        });
    }

    if let Some(id) = path_id {
        if let Some(body_id) = resource_json.get("id").and_then(|v| v.as_str())
            && body_id != id
        {
            return Err(RestError::BadRequest {
                message: format!("Resource id in body ({body_id}) does not match URL ({id})"),
            });
        }
    }

    debug!(
        resource_type = %resource_type,
        tenant = %tenant.tenant_id(),
        fhir_version = %fhir_version,
        "Processing $validate"
    );

    let outcome = svc.validate_to_outcome(resource_json, fhir_version)?;
    Ok(Json(outcome).into_response())
}
