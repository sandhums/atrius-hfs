//! Resolve CDS Hooks `fhirServer` + `fhirAuthorization` for SMART clinical FHIR access.
//!
//! When the CDS client passes a bearer token, cds-server overrides the configured
//! `CDS_HFS_BASE_URL` with `fhirServer` and forwards credentials to the JVM sidecar
//! for outbound clinical REST (prefetch gaps, CQL retrieve). HTS and KR bases stay on
//! cds-server configuration.

use crate::clinical_reasoning::SidecarFhirAuthorization;
use helios_cds_hooks::{CdsHooksError, CdsRequest, FhirAuthorization};

/// Production policy for SMART FHIR access on CDS invoke.
#[derive(Debug, Clone, Default)]
pub struct FhirAccessPolicy {
    /// When true, invoke without `fhirAuthorization` is rejected (412).
    pub require_authorization: bool,
    /// Allowed `fhirServer` hosts (exact or subdomain). Empty = allow any http(s) host.
    pub allowed_fhir_hosts: Vec<String>,
}

impl FhirAccessPolicy {
    pub fn from_config(require_authorization: bool, allowlist: Option<&str>) -> Self {
        let allowed_fhir_hosts = allowlist
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|h| !h.is_empty())
                    .map(str::to_ascii_lowercase)
                    .collect()
            })
            .unwrap_or_default();
        Self {
            require_authorization,
            allowed_fhir_hosts,
        }
    }
}

/// Clinical FHIR base + optional bearer credentials for one invoke.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedClinicalFhirAccess {
    pub hfs_base_url: String,
    pub fhir_authorization: Option<SidecarFhirAuthorization>,
}

/// Map CDS Hooks request → sidecar clinical base + SMART bearer token (if any).
pub fn resolve_clinical_fhir_access(
    request: &CdsRequest,
    default_hfs_base_url: &str,
    policy: &FhirAccessPolicy,
    context_patient_id: Option<&str>,
) -> Result<ResolvedClinicalFhirAccess, CdsHooksError> {
    match &request.fhir_authorization {
        None => {
            if policy.require_authorization {
                return Err(CdsHooksError::PreconditionFailed(
                    "fhirAuthorization is required for this CDS server deployment".into(),
                ));
            }
            Ok(ResolvedClinicalFhirAccess {
                hfs_base_url: trim_trailing_slash(default_hfs_base_url),
                fhir_authorization: None,
            })
        }
        Some(auth) => {
            let fhir_server = request
                .fhir_server
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    CdsHooksError::PreconditionFailed(
                        "fhirServer is required when fhirAuthorization is provided".into(),
                    )
                })?;

            validate_fhir_authorization(auth)?;
            validate_fhir_server_host(fhir_server, policy)?;
            if let (Some(token_patient), Some(ctx_patient)) =
                (auth.patient.as_deref(), context_patient_id)
            {
                if token_patient != ctx_patient {
                    tracing::warn!(
                        token_patient = %token_patient,
                        context_patient = %ctx_patient,
                        "fhirAuthorization.patient differs from hook context.patientId"
                    );
                }
            }

            let forward = to_sidecar_authorization(auth);
            tracing::debug!(
                fhir_server = %fhir_server,
                scope = %auth.scope,
                subject = %auth.subject,
                expires_in = auth.expires_in,
                "SMART fhirAuthorization accepted for clinical FHIR access"
            );

            Ok(ResolvedClinicalFhirAccess {
                hfs_base_url: trim_trailing_slash(fhir_server),
                fhir_authorization: Some(forward),
            })
        }
    }
}

fn validate_fhir_authorization(auth: &FhirAuthorization) -> Result<(), CdsHooksError> {
    if auth.access_token.trim().is_empty() {
        return Err(CdsHooksError::PreconditionFailed(
            "fhirAuthorization.access_token must not be empty".into(),
        ));
    }
    if !auth.token_type.eq_ignore_ascii_case("bearer") {
        return Err(CdsHooksError::PreconditionFailed(
            "fhirAuthorization.token_type must be Bearer".into(),
        ));
    }
    if auth.scope.trim().is_empty() {
        return Err(CdsHooksError::PreconditionFailed(
            "fhirAuthorization.scope must not be empty".into(),
        ));
    }
    if auth.subject.trim().is_empty() {
        return Err(CdsHooksError::PreconditionFailed(
            "fhirAuthorization.subject must not be empty".into(),
        ));
    }
    Ok(())
}

fn validate_fhir_server_host(
    fhir_server: &str,
    policy: &FhirAccessPolicy,
) -> Result<(), CdsHooksError> {
    let (scheme, host) = parse_http_url_host(fhir_server).ok_or_else(|| {
        CdsHooksError::PreconditionFailed(format!(
            "fhirServer must be an absolute http or https URL: {fhir_server}"
        ))
    })?;
    if scheme != "http" && scheme != "https" {
        return Err(CdsHooksError::PreconditionFailed(
            "fhirServer scheme must be http or https".into(),
        ));
    }
    if policy.allowed_fhir_hosts.is_empty() {
        return Ok(());
    }
    let host = host.to_ascii_lowercase();
    let allowed = policy
        .allowed_fhir_hosts
        .iter()
        .any(|entry| host == *entry || host.ends_with(&format!(".{entry}")));
    if !allowed {
        return Err(CdsHooksError::PreconditionFailed(format!(
            "fhirServer host `{host}` is not in CDS_FHIR_SERVER_ALLOWLIST"
        )));
    }
    Ok(())
}

fn parse_http_url_host(url: &str) -> Option<(&str, &str)> {
    let url = url.trim();
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let host = rest.split('/').next()?.split(':').next()?;
    if host.is_empty() {
        return None;
    }
    let scheme = if url.starts_with("https://") {
        "https"
    } else {
        "http"
    };
    Some((scheme, host))
}

fn to_sidecar_authorization(auth: &FhirAuthorization) -> SidecarFhirAuthorization {
    SidecarFhirAuthorization {
        access_token: auth.access_token.clone(),
        token_type: auth.token_type.clone(),
        expires_in: Some(auth.expires_in),
        scope: Some(auth.scope.clone()),
        subject: Some(auth.subject.clone()),
        patient: auth.patient.clone(),
    }
}

fn trim_trailing_slash(s: &str) -> String {
    let mut s = s.trim().to_string();
    while s.ends_with('/') {
        s.pop();
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use helios_cds_hooks::CdsRequest;
    use serde_json::json;

    fn sample_auth() -> FhirAuthorization {
        FhirAuthorization {
            access_token: "tok".into(),
            token_type: "Bearer".into(),
            expires_in: 300,
            scope: "user/Patient.read".into(),
            subject: "cds-service4".into(),
            patient: Some("p1".into()),
        }
    }

    fn base_request(auth: Option<FhirAuthorization>, fhir_server: Option<&str>) -> CdsRequest {
        CdsRequest {
            hook: "patient-view".into(),
            hook_instance: "i".into(),
            fhir_server: fhir_server.map(str::to_string),
            fhir_authorization: auth,
            context: json!({"patientId": "p1", "userId": "u"}),
            prefetch: None,
            extension: None,
        }
    }

    #[test]
    fn default_base_when_no_auth() {
        let policy = FhirAccessPolicy::default();
        let r = resolve_clinical_fhir_access(
            &base_request(None, None),
            "http://127.0.0.1:8082",
            &policy,
            Some("p1"),
        )
        .unwrap();
        assert_eq!(r.hfs_base_url, "http://127.0.0.1:8082");
        assert!(r.fhir_authorization.is_none());
    }

    #[test]
    fn require_auth_rejects_missing() {
        let policy = FhirAccessPolicy {
            require_authorization: true,
            allowed_fhir_hosts: vec![],
        };
        let err = resolve_clinical_fhir_access(
            &base_request(None, None),
            "http://127.0.0.1:8082",
            &policy,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, CdsHooksError::PreconditionFailed(_)));
    }

    #[test]
    fn auth_requires_fhir_server() {
        let policy = FhirAccessPolicy::default();
        let err = resolve_clinical_fhir_access(
            &base_request(Some(sample_auth()), None),
            "http://127.0.0.1:8082",
            &policy,
            Some("p1"),
        )
        .unwrap_err();
        assert!(matches!(err, CdsHooksError::PreconditionFailed(_)));
    }

    #[test]
    fn auth_overrides_clinical_base() {
        let policy = FhirAccessPolicy::default();
        let r = resolve_clinical_fhir_access(
            &base_request(Some(sample_auth()), Some("https://ehr.example.com/fhir")),
            "http://127.0.0.1:8082",
            &policy,
            Some("p1"),
        )
        .unwrap();
        assert_eq!(r.hfs_base_url, "https://ehr.example.com/fhir");
        assert_eq!(r.fhir_authorization.as_ref().unwrap().access_token, "tok");
    }

    #[test]
    fn allowlist_blocks_unknown_host() {
        let policy = FhirAccessPolicy {
            require_authorization: false,
            allowed_fhir_hosts: vec!["ehr.example.com".into()],
        };
        let err = resolve_clinical_fhir_access(
            &base_request(Some(sample_auth()), Some("https://evil.example.org/fhir")),
            "http://127.0.0.1:8082",
            &policy,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, CdsHooksError::PreconditionFailed(_)));
    }

    #[test]
    fn rejects_non_bearer_token_type() {
        let mut auth = sample_auth();
        auth.token_type = "Basic".into();
        let policy = FhirAccessPolicy::default();
        let err = resolve_clinical_fhir_access(
            &base_request(Some(auth), Some("https://ehr.example.com/fhir")),
            "http://127.0.0.1:8082",
            &policy,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, CdsHooksError::PreconditionFailed(_)));
    }
}
