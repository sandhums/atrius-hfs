//! Terminology service client for HFS/HTS integration.
//!
//! Thin REST-layer wrapper around [`helios_terminology_client`] used by the
//! FHIR search handler to resolve `:in` / `:below` / `:above` via `$expand`.
//! Configure the server with `HFS_TERMINOLOGY_SERVER`.

pub use helios_terminology_client::{ExpandedCode, TerminologyError};

/// Async HTTP client for FHIR terminology server operations used by search.
#[derive(Clone)]
pub struct TerminologyServiceClient {
    inner: helios_terminology_client::TerminologyClient,
}

impl TerminologyServiceClient {
    /// Creates a new client targeting the given base URL.
    ///
    /// Trailing slashes are trimmed. Uses the shared client's REST-search
    /// profile: 10s timeout, 2s connect, `no_proxy`.
    pub fn new(base_url: String) -> Self {
        Self {
            inner: helios_terminology_client::TerminologyClient::new(
                base_url,
                helios_terminology_client::ClientOptions::rest_search(),
            ),
        }
    }

    /// Returns the configured base URL.
    pub fn base_url(&self) -> &str {
        self.inner.base_url()
    }

    /// Expands a ValueSet by URL and returns the codes in its expansion.
    pub async fn expand_value_set(
        &self,
        value_set_url: &str,
    ) -> Result<Vec<ExpandedCode>, TerminologyError> {
        self.inner.expand_value_set(value_set_url).await
    }

    /// Expands the concepts subsumed by (or subsuming) a code.
    ///
    /// - `op = "is-a"` returns the code and all its descendants (`:below`).
    /// - `op = "generalizes"` returns the code and all its ancestors (`:above`).
    pub async fn expand_subsumption(
        &self,
        system: &str,
        code: &str,
        op: &str,
    ) -> Result<Vec<ExpandedCode>, TerminologyError> {
        self.inner.expand_subsumption(system, code, op).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_trims_trailing_slash() {
        let client = TerminologyServiceClient::new("http://localhost:9091/".to_string());
        assert_eq!(client.base_url(), "http://localhost:9091");
    }

    #[test]
    fn test_client_no_trailing_slash() {
        let client = TerminologyServiceClient::new("http://localhost:9091".to_string());
        assert_eq!(client.base_url(), "http://localhost:9091");
    }
}
