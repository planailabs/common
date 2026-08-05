//! Server-side HTTP instrumentation for axum routers.
//!
//! ```ignore
//! let app = Router::new()
//!     .route("/orders/{id}", get(handler))
//!     // Applies to the routes above it, so probes registered after stay untraced.
//!     .layer(otel::http::trace_layer())
//!     .layer(axum::middleware::from_fn(otel::http::record_request_metrics))
//!     .route("/healthz", get(|| async { "ok" }));
//! ```
//!
//! Both layers key off `MatchedPath` (`/orders/{id}`), never the raw URI, so a
//! path parameter can't explode span names or metric cardinality.

use std::time::{Duration, Instant};

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;
use opentelemetry::global;
use opentelemetry_http::HeaderExtractor;
use tower_http::classify::{ServerErrorsAsFailures, SharedClassifier};
use tower_http::trace::{DefaultOnFailure, MakeSpan, OnResponse, TraceLayer};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Route template for a request, or `"unmatched"` when routing found nothing.
fn route_of<B>(req: &http::Request<B>) -> String {
    req.extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned())
}

/// Opens the server span and continues an upstream trace when the caller sent
/// a `traceparent`.
#[derive(Clone, Copy, Debug, Default)]
pub struct OtelMakeSpan;

impl<B> MakeSpan<B> for OtelMakeSpan {
    fn make_span(&mut self, req: &http::Request<B>) -> tracing::Span {
        let route = route_of(req);
        let span = tracing::info_span!(
            "http-request",
            otel.name = %format!("{} {route}", req.method()),
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
            http.request.method = %req.method(),
            http.route = %route,
            url.path = %req.uri().path(),
            url.scheme = %req.uri().scheme_str().unwrap_or("http"),
            user_agent.original = %req
                .headers()
                .get("user-agent")
                .and_then(|v| v.to_str().ok())
                .unwrap_or(""),
            http.response.status_code = tracing::field::Empty,
        );
        let parent =
            global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(req.headers())));
        // Errors only when no otel layer is installed — nothing to do about it.
        let _ = span.set_parent(parent);
        span
    }
}

/// Records the response status onto the server span.
#[derive(Clone, Copy, Debug, Default)]
pub struct OtelOnResponse;

impl<B> OnResponse<B> for OtelOnResponse {
    fn on_response(self, res: &http::Response<B>, _latency: Duration, span: &tracing::Span) {
        span.record("http.response.status_code", res.status().as_u16() as i64);
        if res.status().is_server_error() {
            span.record("otel.status_code", "ERROR");
        }
    }
}

/// The `TraceLayer` flavour returned by [`trace_layer`].
pub type OtelTraceLayer = TraceLayer<
    SharedClassifier<ServerErrorsAsFailures>,
    OtelMakeSpan,
    (),
    OtelOnResponse,
    (),
    (),
    DefaultOnFailure,
>;

/// Server spans, with W3C context extraction and HTTP semantic-convention
/// attributes.
pub fn trace_layer() -> OtelTraceLayer {
    TraceLayer::new_for_http()
        .make_span_with(OtelMakeSpan)
        .on_request(())
        .on_response(OtelOnResponse)
        .on_body_chunk(())
        .on_eos(())
}

#[cfg(all(test, feature = "otel-prometheus"))]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::get;
    use tower::ServiceExt;

    #[tokio::test]
    async fn records_the_route_template_not_the_uri() {
        crate::otel::init("test-service", "info");

        let app = Router::new()
            .route("/orders/{id}", get(|| async { "ok" }))
            .layer(trace_layer())
            .layer(axum::middleware::from_fn(record_request_metrics));

        let res = app
            .oneshot(
                http::Request::builder()
                    .uri("/orders/8a3f-2b91")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);

        let out = crate::metrics::prom::encode().expect("encode");
        assert!(out.contains(r#"http_route="/orders/{id}""#), "{out}");
        assert!(!out.contains("8a3f-2b91"), "{out}");
        assert!(out.contains(r#"http_response_status_code="200""#), "{out}");
    }
}

/// Records [`crate::metrics::HTTP_SERVER_DURATION`]. Separate from
/// [`trace_layer`] because `TraceLayer`'s response hook can't see the request.
pub async fn record_request_metrics(req: Request, next: Next) -> Response {
    let method = req.method().as_str().to_owned();
    let route = route_of(&req);
    let start = Instant::now();

    let res = next.run(req).await;

    crate::metrics::HTTP_SERVER_DURATION.record(
        start.elapsed().as_secs_f64(),
        &[
            crate::otel::kv("http.request.method", method),
            crate::otel::kv("http.route", route),
            crate::otel::kv("http.response.status_code", res.status().as_u16() as i64),
        ],
    );
    res
}
