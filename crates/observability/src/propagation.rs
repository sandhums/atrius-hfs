//! W3C Trace Context inject/extract helpers (feature `otel`).
//!
//! Used by outbound HTTP clients (inject) and by [`crate::middleware::track`]
//! (extract) so BFF → HIS → HFS → CDS spans join into one distributed trace.

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

/// Extract a parent OpenTelemetry [`Context`](opentelemetry::Context) from
/// inbound HTTP headers. Returns the current context when extraction yields
/// nothing or when built without `otel`.
#[cfg(feature = "otel")]
pub(crate) fn extract_trace_context(headers: &HeaderMap) -> opentelemetry::Context {
    use opentelemetry::propagation::Extractor;

    struct HeaderExtractor<'a>(&'a HeaderMap);

    impl Extractor for HeaderExtractor<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|v| v.to_str().ok())
        }

        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|k| k.as_str()).collect()
        }
    }

    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(headers))
    })
}
