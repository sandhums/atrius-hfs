//! Validation-facing terminology service interfaces and adapters.
//!
//! This module defines the async and sync terminology service contracts used by
//! the validation engine, along with the remote service implementation that
//! adapts simple membership queries into typed backend requests and converts
//! backend `$validate-code` responses into validation-friendly outcomes.
use crate::ValidationError;
use crate::backend::TerminologyBackend;
use crate::helpers::parse_validate_vs_result;
use crate::requests::ValidateVsRequest;
use crate::terminology::helios::HeliosTerminologyBackend;
use crate::terminology::types::TerminologyMembershipOutcome;
use async_trait::async_trait;
use helios_fhir::FhirVersion;
use std::sync::Arc;
use tracing::debug;

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

/// Terminology service backed by a remote FHIR terminology backend.
///
/// This validation-facing adapter delegates terminology operations to a broader
/// backend abstraction and converts `$validate-code` responses into
/// `TerminologyMembershipOutcome`.
pub struct RemoteTerminologyService {
    backend: Arc<dyn TerminologyBackend>,
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
            backend: Arc::new(HeliosTerminologyBackend::new(base_url, fhir_version)),
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
            backend: Arc::new(HeliosTerminologyBackend::with_client(
                client,
                base_url,
                fhir_version,
            )),
        }
    }

    pub fn with_backend(backend: Arc<dyn TerminologyBackend>) -> Self {
        Self { backend }
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
        let req = ValidateVsRequest {
            valueset_url: valueset_url.to_string(),
            code: Some(code.to_string()),
            system: system.map(str::to_string),
            display: display.map(str::to_string),
            ..Default::default()
        };

        let response = self.backend.validate_vs(&req).await?;

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
