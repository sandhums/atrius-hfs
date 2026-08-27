//! W3C Trace Context injection for outbound HTTP.
//!
//! [`inject_trace_context`] copies the current request span onto a header map as
//! `traceparent` / `tracestate` so downstream HIS / HFS / CDS calls join the
//! same trace. Without the `otel` feature, or when no OpenTelemetry span is
//! current, the map is left unchanged.

use http::HeaderMap;

/// Inject the current span's W3C trace headers into `headers`.
///
/// Callers that start from an empty map can skip attaching headers when the map
/// stays empty.
pub fn inject_trace_context(headers: &mut HeaderMap) {
    #[cfg(feature = "otel")]
    inject_otel(headers);
    #[cfg(not(feature = "otel"))]
    let _ = headers;
}

#[cfg(feature = "otel")]
fn inject_otel(headers: &mut HeaderMap) {
    use opentelemetry::propagation::{Injector, TextMapPropagator};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    struct HeaderInjector<'a>(&'a mut HeaderMap);

    impl Injector for HeaderInjector<'_> {
        fn set(&mut self, key: &str, value: String) {
            let Ok(name) = http::HeaderName::from_bytes(key.as_bytes()) else {
                return;
            };
            let Ok(val) = http::HeaderValue::from_str(&value) else {
                return;
            };
            self.0.insert(name, val);
        }
    }

    let cx = tracing::Span::current().context();
    TraceContextPropagator::new().inject_context(&cx, &mut HeaderInjector(headers));
}

#[cfg(test)]
mod tests {
    use super::inject_trace_context;
    use http::HeaderMap;

    #[test]
    fn inject_without_span_leaves_headers_empty() {
        let mut headers = HeaderMap::new();
        inject_trace_context(&mut headers);
        assert!(headers.is_empty());
    }
}
