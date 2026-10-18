//! Terminology service client for HFS/HTS integration.
//!
//! Thin REST-layer wrapper around [`helios_terminology_client`] used by the
//! FHIR search handler to resolve `:in` / `:below` / `:above` via `$expand`,
//! and by SQL-on-FHIR terminology relations (#1824) for paged `$expand` and
//! ConceptMap lookup. Configure the server with `HFS_TERMINOLOGY_SERVER`.

use serde_json::Value;

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

    /// Fetches one page of a ValueSet expansion as the raw expanded ValueSet:
    /// `POST {base_url}/ValueSet/$expand` with `url`, `offset` and `count`.
    ///
    /// Returns `Ok(None)` when the server answers `404` (it knows no value set
    /// at `value_set_url`). Used by SQL-on-FHIR terminology relations (#1824),
    /// which need nesting, `abstract`, `version` and `inactive` intact.
    pub async fn expand_value_set_page(
        &self,
        value_set_url: &str,
        offset: usize,
        count: usize,
    ) -> Result<Option<Value>, TerminologyError> {
        self.inner
            .expand_value_set_page(value_set_url, offset, count)
            .await
    }

    /// Looks up a ConceptMap by canonical (`url`, or `url|version`) with
    /// `GET {base_url}/ConceptMap?url=…[&version=…]`.
    ///
    /// Returns `Ok(None)` when no map matches, and the first match otherwise
    /// (a server holding several versions of an unversioned canonical decides
    /// the order).
    pub async fn find_concept_map(
        &self,
        canonical: &str,
    ) -> Result<Option<Value>, TerminologyError> {
        self.inner.find_concept_map(canonical).await
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
