//! W3C Trace Context inject/extract helpers (feature `otel`).
//!
//! Used by outbound HTTP clients (inject) so BFF → HIS → HFS → CDS spans join
//! into one distributed trace. Inbound requests adopt the caller's
//! `traceparent` via [`crate::telemetry::adopt_remote_parent`].

use http::HeaderMap;

/// Inject the current tracing span's OpenTelemetry context into `headers`
/// as W3C `traceparent` / `tracestate`.
///
/// No-op when built without the `otel` feature, when no global propagator is
/// installed, or when the current span has no valid trace context.
pub fn inject_trace_context(headers: &mut HeaderMap) {
    #[cfg(feature = "otel")]
    {
        use opentelemetry::propagation::Injector;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        struct HeaderInjector<'a>(&'a mut HeaderMap);

        impl Injector for HeaderInjector<'_> {
            fn set(&mut self, key: &str, value: String) {
                if let (Ok(name), Ok(val)) = (
                    http::HeaderName::try_from(key),
                    http::HeaderValue::try_from(value),
                ) {
                    self.0.insert(name, val);
                }
            }
        }

        let cx = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut HeaderInjector(headers));
        });
    }
    #[cfg(not(feature = "otel"))]
    {
        let _ = headers;
    }
}
