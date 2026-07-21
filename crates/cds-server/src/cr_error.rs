//! Map [`ClinicalReasoningError`](crate::clinical_reasoning::ClinicalReasoningError) to CDS Hooks HTTP errors.

use crate::clinical_reasoning::ClinicalReasoningError;
use helios_cds_hooks::CdsHooksError;

/// Convert façade failures into [`CdsHooksError`] with actionable HTTP status codes.
///
/// - Sidecar **404/410** → [`CdsHooksError::PreconditionFailed`] (412) — missing FHIR artifact / failed read against a configured FHIR base (not “zero Conditions”; those are typically 200 + empty bundle).
/// - Sidecar **422/400** and **5xx** → [`CdsHooksError::BadGateway`] (502) — engine rejected evaluation or failed internally.
/// - Transport errors → **502** — cannot reach sidecar.
pub fn map_clinical_reasoning_err(e: ClinicalReasoningError) -> CdsHooksError {
    match e {
        ClinicalReasoningError::InvalidUrl(msg) => {
            CdsHooksError::InternalError(format!("clinical reasoning URL misconfigured: {msg}"))
        }
        ClinicalReasoningError::Http(msg) => {
            CdsHooksError::BadGateway(format!("cannot reach clinical reasoning service: {msg}"))
        }
        ClinicalReasoningError::SidecarRejected(r) => {
            let detail = r.summarize();
            let status = r.status;
            match status {
                404 | 410 => CdsHooksError::PreconditionFailed(format!(
                    "clinical reasoning reported missing data or resource (HTTP {status}). \
                     Empty clinical results normally return HTTP 200 from FHIR search; 404 usually means the engine could not fetch an artifact or reached a non-success HTTP response from a configured FHIR base. Detail: {detail}"
                )),
                422 | 400 => CdsHooksError::BadGateway(format!(
                    "clinical reasoning rejected evaluation (HTTP {status}): {detail}"
                )),
                s if (500..600).contains(&s) => CdsHooksError::BadGateway(format!(
                    "clinical reasoning engine error (HTTP {status}): {detail}"
                )),
                _ => CdsHooksError::BadGateway(format!(
                    "clinical reasoning rejected request (HTTP {status}): {detail}"
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clinical_reasoning::SidecarRejectionDetail;

    #[test]
    fn maps_404_to_precondition_failed() {
        let e = ClinicalReasoningError::SidecarRejected(SidecarRejectionDetail::new(
            404,
            r#"{"resourceType":"OperationOutcome","issue":[{"diagnostics":"Library not found"}]}"#
                .into(),
        ));
        let err = map_clinical_reasoning_err(e);
        assert_eq!(err.status_code(), 412);
        assert!(err.to_string().contains("404"));
    }

    #[test]
    fn maps_422_to_bad_gateway() {
        let e = ClinicalReasoningError::SidecarRejected(SidecarRejectionDetail::new(
            422,
            r#"{"message":"bad"}"#.into(),
        ));
        let err = map_clinical_reasoning_err(e);
        assert_eq!(err.status_code(), 502);
    }
}
