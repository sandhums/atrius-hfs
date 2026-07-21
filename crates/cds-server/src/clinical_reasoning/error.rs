//! Errors returned by the clinical reasoning façade.

use std::fmt;
use std::io::Error as IoError;

use serde_json::Value;

/// Non-success response from the JVM clinical reasoning HTTP API (`/v1/evaluate/expression`, …).
#[derive(Debug, Clone)]
pub struct SidecarRejectionDetail {
    pub status: u16,
    /// Parsed JSON body when valid JSON.
    pub body: Option<Value>,
    /// Raw response body (truncated in [`Self::summarize`] output only).
    pub raw_body: Option<String>,
}

impl SidecarRejectionDetail {
    /// Build from HTTP status and raw response text (always preserve raw for diagnostics).
    pub fn new(status: u16, raw_text: String) -> Self {
        let body = serde_json::from_str(&raw_text).ok();
        Self {
            status,
            body,
            raw_body: Some(raw_text),
        }
    }

    /// Short human-readable explanation for logs and CDS error strings.
    #[must_use]
    pub fn summarize(&self) -> String {
        summarize_sidecar_payload(self.body.as_ref(), self.raw_body.as_deref())
    }
}

fn summarize_sidecar_payload(body: Option<&Value>, raw: Option<&str>) -> String {
    if let Some(v) = body
        && let Some(s) = summarize_json_body(v)
    {
        return s;
    }
    raw.map(truncate_raw)
        .unwrap_or_else(|| "(empty response body)".into())
}

fn truncate_raw(s: &str) -> String {
    const MAX: usize = 2048;
    let t = s.trim();
    if t.len() <= MAX {
        return t.to_string();
    }
    format!("{}… (truncated, {} bytes)", &t[..MAX], t.len())
}

fn summarize_json_body(v: &Value) -> Option<String> {
    if v.get("resourceType").and_then(|x| x.as_str()) == Some("OperationOutcome") {
        return Some(format_operation_outcome(v));
    }
    if let Some(s) = summarize_spring_style_http_error(v) {
        return Some(s);
    }
    for key in ["message", "error", "detail", "reason"] {
        if let Some(s) = v
            .get(key)
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
        {
            return Some(s.to_string());
        }
    }
    None
}

/// Spring Boot (and similar) default JSON for 404/500 — not FHIR.
/// The CQFramework client inside the sidecar reached an HTTP stack that is not your clinical FHIR API for that path (proxy, different process on “localhost”, Docker network, or mismatched base URL vs what you use in Postman).
fn summarize_spring_style_http_error(v: &Value) -> Option<String> {
    let path = v.get("path").and_then(|p| p.as_str())?;
    if !path.starts_with('/') {
        return None;
    }
    // Typical Spring error body: timestamp, status, error, path (trace optional).
    v.get("timestamp")?;
    let status = json_status_code(v.get("status")?)?;
    let err = v
        .get("error")
        .and_then(|e| e.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("HTTP error");
    Some(format!(
        "{err} (HTTP {status}) at {path} — response looks like a generic web framework error, not FHIR. \
         The sidecar resolved clinical data against some base URL for this path; it must match your Helios HFS base **exactly** as reachable from the sidecar process \
         (scheme, host, port, any path prefix). Use the same host form you verify with curl/Postman (e.g. localhost vs 127.0.0.1); if the sidecar runs in Docker, localhost inside the container is not the host's HFS."
    ))
}

fn json_status_code(v: &Value) -> Option<u64> {
    if let Some(n) = v.as_u64() {
        return Some(n);
    }
    if let Some(i) = v.as_i64() {
        return Some(i as u64);
    }
    v.as_str()?.parse().ok()
}

fn format_operation_outcome(v: &Value) -> String {
    let Some(issues) = v.get("issue").and_then(|i| i.as_array()) else {
        return "OperationOutcome (no issue details)".into();
    };
    let mut parts = Vec::new();
    for issue in issues.iter().take(8) {
        let severity = issue
            .get("severity")
            .and_then(|x| x.as_str())
            .unwrap_or("?");
        let code = issue.get("code").and_then(|x| x.as_str()).unwrap_or("?");
        let mut chunk = format!("[{severity}/{code}]");
        if let Some(d) = issue
            .get("diagnostics")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
        {
            chunk.push(' ');
            chunk.push_str(d);
        } else if let Some(t) = issue
            .get("details")
            .and_then(|d| d.get("text"))
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
        {
            chunk.push(' ');
            chunk.push_str(t);
        }
        parts.push(chunk);
    }
    if parts.is_empty() {
        "OperationOutcome (empty issue list)".into()
    } else {
        parts.join("; ")
    }
}

/// Façade or sidecar invocation failure.
#[derive(Debug)]
pub enum ClinicalReasoningError {
    /// Sidecar base URL invalid or malformed.
    InvalidUrl(String),
    /// Transport / network failure (includes status when available).
    Http(String),
    /// Sidecar returned a non-success HTTP status (see [`SidecarRejectionDetail`]).
    SidecarRejected(SidecarRejectionDetail),
}

impl fmt::Display for ClinicalReasoningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl(s) => write!(f, "invalid clinical reasoning URL: {s}"),
            Self::Http(s) => write!(f, "clinical reasoning HTTP error: {s}"),
            Self::SidecarRejected(r) => {
                write!(
                    f,
                    "clinical reasoning rejected request (HTTP {}): {}",
                    r.status,
                    r.summarize()
                )
            }
        }
    }
}

impl std::error::Error for ClinicalReasoningError {}

impl ClinicalReasoningError {
    /// HTTP status returned by the sidecar when this is [`Self::SidecarRejected`], else `None`.
    #[must_use]
    pub fn sidecar_http_status(&self) -> Option<u16> {
        match self {
            Self::SidecarRejected(r) => Some(r.status),
            _ => None,
        }
    }

    /// Structured rejection payload when the sidecar returned an error status.
    #[must_use]
    pub fn sidecar_rejection(&self) -> Option<&SidecarRejectionDetail> {
        match self {
            Self::SidecarRejected(r) => Some(r),
            _ => None,
        }
    }
}

impl From<IoError> for ClinicalReasoningError {
    fn from(e: IoError) -> Self {
        ClinicalReasoningError::Http(format!("IO: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn operation_outcome_formatted() {
        let v = json!({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": "processing",
                "diagnostics": "HTTP 404 fetching Library"
            }]
        });
        let s = summarize_json_body(&v).unwrap();
        assert!(s.contains("HTTP 404"));
        assert!(s.contains("processing"));
    }

    #[test]
    fn sidecar_rejection_display_includes_summary() {
        let r = SidecarRejectionDetail::new(422, r#"{"message":"bad expr"}"#.into());
        let e = ClinicalReasoningError::SidecarRejected(r);
        let t = e.to_string();
        assert!(t.contains("422"));
        assert!(t.contains("bad expr"));
    }

    #[test]
    fn spring_style_json_gets_actionable_hint() {
        let v = json!({
            "timestamp": 1779026571171_u64,
            "status": 404,
            "error": "Not Found",
            "path": "/Condition"
        });
        let s = summarize_json_body(&v).unwrap();
        assert!(s.contains("Not Found"));
        assert!(s.contains("/Condition"));
        assert!(s.contains("localhost"));
    }
}
