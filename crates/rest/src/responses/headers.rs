//! Response header generation.
//!
//! Provides utilities for building FHIR-standard response headers.

use axum::http::{HeaderMap, HeaderValue, header};
use helios_persistence::core::ResourceStorage;
use helios_persistence::types::StoredResource;

use crate::state::AppState;

/// Builder for resource response headers.
///
/// Generates standard FHIR response headers including:
/// - ETag (version identifier)
/// - Last-Modified
/// - Location (for create operations)
/// - Content-Type
#[derive(Debug, Default)]
pub struct ResourceHeaders {
    /// ETag value (weak validator).
    etag: Option<String>,
    /// Last-Modified timestamp.
    last_modified: Option<String>,
    /// Location URL (for created resources).
    location: Option<String>,
    /// Content-Type.
    content_type: String,
}

impl ResourceHeaders {
    /// Creates a new ResourceHeaders builder.
    pub fn new() -> Self {
        Self {
            content_type: "application/fhir+json".to_string(),
            ..Default::default()
        }
    }

    /// Creates headers from a StoredResource.
    pub fn from_stored<S>(stored: &StoredResource, state: &AppState<S>) -> Self
    where
        S: ResourceStorage,
    {
        let etag = if state.versioning_enabled() {
            Some(format!("W/\"{}\"", stored.version_id()))
        } else {
            None
        };

        let last_modified = Some(
            stored
                .last_modified()
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        );

        Self {
            etag,
            last_modified,
            location: None,
            content_type: "application/fhir+json".to_string(),
        }
    }

    /// Sets the ETag value.
    pub fn with_etag(mut self, etag: impl Into<String>) -> Self {
        self.etag = Some(etag.into());
        self
    }

    /// Sets the ETag from a version ID.
    pub fn with_version(mut self, version_id: &str) -> Self {
        self.etag = Some(format!("W/\"{}\"", version_id));
        self
    }

    /// Sets the Last-Modified timestamp.
    pub fn with_last_modified(mut self, timestamp: impl Into<String>) -> Self {
        self.last_modified = Some(timestamp.into());
        self
    }

    /// Sets the Location URL.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Sets the Content-Type.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    /// Converts to an Axum HeaderMap.
    pub fn to_header_map(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();

        // Content-Type
        if let Ok(value) = HeaderValue::from_str(&self.content_type) {
            headers.insert(header::CONTENT_TYPE, value);
        }

        // ETag
        if let Some(etag) = &self.etag
            && let Ok(value) = HeaderValue::from_str(etag)
        {
            headers.insert(header::ETAG, value);
        }

        // Last-Modified
        if let Some(last_modified) = &self.last_modified
            && let Ok(value) = HeaderValue::from_str(last_modified)
        {
            headers.insert(header::LAST_MODIFIED, value);
        }

        // Location
        if let Some(location) = &self.location
            && let Ok(value) = HeaderValue::from_str(location)
        {
            headers.insert(header::LOCATION, value);
        }

        headers
    }

    /// Returns the ETag value.
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Returns the Last-Modified value.
    pub fn last_modified(&self) -> Option<&str> {
        self.last_modified.as_deref()
    }

    /// Returns the Location value.
    pub fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let headers = ResourceHeaders::new();
        assert_eq!(headers.content_type, "application/fhir+json");
        assert!(headers.etag.is_none());
    }

    #[test]
    fn test_with_etag() {
        let headers = ResourceHeaders::new().with_etag("W/\"1\"");
        assert_eq!(headers.etag(), Some("W/\"1\""));
    }

    #[test]
    fn test_with_version() {
        let headers = ResourceHeaders::new().with_version("42");
        assert_eq!(headers.etag(), Some("W/\"42\""));
    }

    #[test]
    fn test_to_header_map() {
        let headers = ResourceHeaders::new()
            .with_etag("W/\"1\"")
            .with_location("http://example.com/Patient/123");

        let map = headers.to_header_map();

        assert!(map.contains_key(header::CONTENT_TYPE));
        assert!(map.contains_key(header::ETAG));
        assert!(map.contains_key(header::LOCATION));
    }
}
