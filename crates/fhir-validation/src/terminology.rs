use crate::ValidationError;
use async_trait::async_trait;
use helios_fhir::FhirVersion;
use helios_fhirpath::terminology_client::TerminologyClient;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct TerminologyMembershipOutcome {
    pub is_member: bool,
    pub message: Option<String>,
    pub diagnostics: Vec<String>,
    pub system: Option<String>,
    pub code: Option<String>,
    pub version: Option<String>,
    pub display: Option<String>,
}
#[derive(Debug, Clone)]
pub struct TerminologyRemoteError {
    pub status: Option<u16>,
    pub diagnostics: Vec<String>,
    pub raw_body: Option<String>,
}
/// Trait representing a terminology validation service.
///
/// This allows the validator to remain independent of the specific
/// terminology backend (Snowstorm, HAPI, local cache, etc.).
#[async_trait]
pub trait TerminologyService: Send + Sync {
    async fn member_of(
        &self,
        valueset_url: &str,
        system: Option<&str>,
        code: &str,
        display: Option<&str>,
    ) -> Result<TerminologyMembershipOutcome, ValidationError>;
}
/// Synchronous terminology validation interface used by backward-compatible
/// sync validation paths.
///
/// This trait is intended for local-only or explicitly synchronous terminology
/// integrations. Production remote terminology lookups should generally prefer
/// the asynchronous [`TerminologyService`] path.
pub trait TerminologyServiceSync {
    fn member_of(
        &self,
        valueset_url: &str,
        system: Option<&str>,
        code: &str,
        display: Option<&str>,
    ) -> Result<TerminologyMembershipOutcome, ValidationError>;
}

/// Terminology service backed by a remote FHIR terminology server.
///
/// This adapter uses the FHIR `$validate-code` operation via the
/// `helios_fhirpath::terminology_client::TerminologyClient`.
pub struct RemoteTerminologyService {
    client: TerminologyClient,
}
impl RemoteTerminologyService {
    /// Create a remote terminology service with the default HTTP client.
    ///
    /// This is suitable for local development and simple integration setups.
    /// For production use, prefer `with_client(...)` so timeouts, connection
    /// pooling, authentication, and other transport behavior can be configured
    /// explicitly.
    pub fn new(base_url: String, fhir_version: FhirVersion) -> Self {
        Self {
            client: TerminologyClient::new(base_url, fhir_version),
        }
    }

    /// Create a remote terminology service with a caller-provided HTTP client.
    ///
    /// This allows production callers to configure request timeout, connect
    /// timeout, connection pooling, keepalive, authentication, proxies, and
    /// related transport settings once and reuse them across terminology calls.
    pub fn with_client(
        client: reqwest::Client,
        base_url: String,
        fhir_version: FhirVersion,
    ) -> Self {
        Self {
            client: TerminologyClient::with_client(client, base_url, fhir_version),
        }
    }
}
fn extract_operation_outcome_diagnostics(body: &str) -> Vec<String> {
    let Ok(json) = serde_json::from_str::<serde_json::Value>(body) else {
        return Vec::new();
    };

    if json.get("resourceType").and_then(|v| v.as_str()) != Some("OperationOutcome") {
        return Vec::new();
    }

    let mut out = Vec::new();
    if let Some(issues) = json.get("issue").and_then(|v| v.as_array()) {
        for issue in issues {
            if let Some(diag) = issue.get("diagnostics").and_then(|v| v.as_str()) {
                out.push(diag.to_string());
                continue;
            }

            if let Some(details_text) = issue
                .get("details")
                .and_then(|d| d.get("text"))
                .and_then(|v| v.as_str())
            {
                out.push(details_text.to_string());
            }
        }
    }

    out
}

pub fn build_remote_terminology_error(msg: &str) -> TerminologyRemoteError {
    let status = msg
        .split("status ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|s| s.parse::<u16>().ok());

    if let Some((_, body)) = msg.split_once(": {") {
        let json_body = format!("{{{}", body);
        let diagnostics = extract_operation_outcome_diagnostics(&json_body);
        return TerminologyRemoteError {
            status,
            diagnostics,
            raw_body: Some(json_body),
        };
    }

    TerminologyRemoteError {
        status,
        diagnostics: Vec::new(),
        raw_body: Some(msg.to_string()),
    }
}

#[async_trait]
impl TerminologyService for RemoteTerminologyService {
    async fn member_of(
        &self,
        valueset_url: &str,
        system: Option<&str>,
        code: &str,
        display: Option<&str>,
    ) -> Result<TerminologyMembershipOutcome, ValidationError> {
        let response = self
            .client
            .validate_vs(valueset_url, system, code, display, None)
            .await
            .map_err(|e| {
                ValidationError::TerminologyRemote(build_remote_terminology_error(&e.to_string()))
            })?;

        // parse_validate_vs_result(&response)
        let outcome = parse_validate_vs_result(&response)?;
        debug!(
            valueset_url,
            system,
            code,
            ?outcome,
            "remote terminology outcome"
        );
        Ok(outcome)
    }
}

/// Parse the FHIR `Parameters` response returned by `ValueSet/$validate-code`.
///
/// Successful responses are converted into a [`TerminologyMembershipOutcome`],
/// preserving the membership result together with any server-provided message
/// and basic terminology metadata such as code, system, version, and display.
///
/// Malformed `Parameters` payloads produce `ValidationError::Terminology(...)`.
fn parse_validate_vs_result(
    body: &serde_json::Value,
) -> Result<TerminologyMembershipOutcome, ValidationError> {
    let params = body
        .get("parameter")
        .and_then(|p| p.as_array())
        .ok_or_else(|| ValidationError::Terminology("Missing Parameters.parameter".to_string()))?;

    let mut result = None;
    let mut message = None;
    let mut diagnostics = Vec::new();
    let mut code = None;
    let mut display = None;
    let mut version = None;
    let mut system = None;

    for p in params {
        let name = p.get("name").and_then(|n| n.as_str()).unwrap_or_default();

        match name {
            "result" => {
                result = p.get("valueBoolean").and_then(|v| v.as_bool());
            }
            "code" => {
                code = p
                    .get("valueString")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
            }
            "display" => {
                display = p
                    .get("valueString")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
            }
            "system" => {
                system = p
                    .get("valueString")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
            }
            "version" => {
                version = p
                    .get("valueString")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
            }
            "message" => {
                if let Some(msg) = p.get("valueString").and_then(|v| v.as_str()) {
                    let msg = msg.to_string();
                    message = Some(msg.clone());
                    diagnostics.push(msg);
                }
            }
            _ => {}
        }
    }

    match result {
        Some(is_member) => Ok(TerminologyMembershipOutcome {
            is_member,
            message,
            diagnostics,
            code,
            system,
            version,
            display,
        }),
        None => Err(ValidationError::Terminology(
            "Terminology server response missing result".to_string(),
        )),
    }
}
