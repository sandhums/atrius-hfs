//! Helios-backed implementation of the terminology backend.
//!
//! This module adapts the internal `ValidateVsRequest` model into calls to the
//! Helios `TerminologyClient` and returns raw FHIR responses for further
//! processing by the service layer.
//!
//! Requests are sent as full FHIR `Parameters` bodies via
//! [`TerminologyClient::validate_code_with_parameters`], matching the fields
//! populated on [`ValidateVsRequest`](crate::requests::ValidateVsRequest) (not
//! only primitive `code` / `system` / `display`).
use crate::ValidationError;
use crate::backend::TerminologyBackend;
use crate::helpers::build_remote_terminology_error;
use crate::requests::ValidateVsRequest;
use async_trait::async_trait;
use helios_fhir::FhirVersion;
use helios_fhirpath::terminology_client::TerminologyClient;

pub struct HeliosTerminologyBackend {
    client: TerminologyClient,
}

impl HeliosTerminologyBackend {
    pub fn new(base_url: String, fhir_version: FhirVersion) -> Self {
        Self {
            client: TerminologyClient::new(base_url, fhir_version),
        }
    }

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

#[async_trait]
impl TerminologyBackend for HeliosTerminologyBackend {
    async fn validate_vs(
        &self,
        req: &ValidateVsRequest,
    ) -> Result<serde_json::Value, ValidationError> {
        if let Err(e) = req.validate() {
            return Err(ValidationError::TerminologyRemote(
                build_remote_terminology_error(&e),
            ));
        }

        let body = req.to_parameters_json();
        self.client
            .validate_code_with_parameters(&req.valueset_url, body)
            .await
            .map_err(|e| {
                ValidationError::TerminologyRemote(build_remote_terminology_error(&e.to_string()))
            })
    }
}
