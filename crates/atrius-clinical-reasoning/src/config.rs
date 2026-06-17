//! Client configuration — sidecar endpoint and timeouts.

use std::time::Duration;

/// Connection settings for the CR sidecar.
#[derive(Debug, Clone)]
pub struct ClinicalReasoningConfig {
    /// Base URL without trailing slash, e.g. `http://localhost:8091`.
    pub base_url: String,
    pub request_timeout: Duration,
}

impl ClinicalReasoningConfig {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: trim_trailing_slash(base_url.into()),
            request_timeout: Duration::from_secs(30),
        }
    }

    pub fn request_timeout(mut self, d: Duration) -> Self {
        self.request_timeout = d;
        self
    }
}

impl Default for ClinicalReasoningConfig {
    fn default() -> Self {
        // Matches JVM sidecar default when `SIDECAR_PORT` unset (8088).
        Self::new("http://127.0.0.1:8088")
    }
}

fn trim_trailing_slash(mut s: String) -> String {
    while s.ends_with('/') {
        s.pop();
    }
    s
}
