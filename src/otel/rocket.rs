//! Server-side HTTP instrumentation for Rocket.
//!
//! ```ignore
//! rocket::custom(config)
//!     .attach(otel::rocket::OtelFairing)
//!     .mount("/", routes![...])
//! ```
//!
//! Rocket fairings are two disconnected hooks with the handler running in
//! between, so unlike a tower layer the span can't wrap handler execution. The
//! fairing opens the span in `on_request` and closes it in `on_response`;
//! handlers that want their own spans nested underneath take the
//! [`TraceSpan`] request guard and `.instrument()` their body with it.
//!
//! Routing hasn't happened yet in `on_request`, so `http.route` is declared
//! `Empty` and recorded on the way out — the raw URI is never used as a span
//! name or a metric attribute, or one path parameter would explode both.

use std::sync::Mutex;
use std::time::Instant;

use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use rocket::fairing::{Fairing, Info, Kind};
use rocket::request::{FromRequest, Outcome};
use rocket::{Data, Request, Response};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Path suffixes that never get a span: scrape and liveness traffic is
/// constant, and tracing it buys nothing but export volume. Matched as a
/// suffix so a mounted prefix (`/api/metrics`) is covered too.
const UNTRACED_SUFFIXES: &[&str] = &["/metrics", "/health", "/healthz", "/readyz"];

fn untraced(path: &str) -> bool {
    UNTRACED_SUFFIXES.iter().any(|s| path.ends_with(s))
}

/// `rocket::http::HeaderMap` is not `http::HeaderMap`, so
/// `opentelemetry_http::HeaderExtractor` doesn't apply.
struct RocketHeaders<'a>(&'a rocket::http::HeaderMap<'a>);

impl Extractor for RocketHeaders<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        // Case-insensitive, so a `Traceparent` from a non-conforming caller
        // still resolves.
        self.0.get_one(key)
    }

    fn keys(&self) -> Vec<&str> {
        // Rocket's iterator yields owned `Header`s, so borrowed names can't
        // outlive the iteration. W3C TraceContext only ever calls `get`.
        Vec::new()
    }
}

/// Carries the request span (and its start instant) from `on_request` to
/// `on_response` and to handlers, through Rocket's per-request local cache.
/// `local_cache` hands back a shared reference, hence the interior mutability.
#[derive(Default)]
struct SpanSlot(Mutex<Option<(Span, Instant)>>);

/// The request span, for handlers that want their own spans nested under it.
///
/// ```ignore
/// #[get("/orders/<id>")]
/// async fn get_order(id: &str, span: TraceSpan) -> Result<String, Status> {
///     use tracing::Instrument;
///     async move { lookup(id).await }.instrument(span.0).await
/// }
/// ```
pub struct TraceSpan(pub Span);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for TraceSpan {
    type Error = std::convert::Infallible;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let slot: &SpanSlot = req.local_cache(SpanSlot::default);
        let span = slot
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|(s, _)| s.clone())
            // Untraced path, or the fairing wasn't attached: degrade to a
            // no-op span rather than panicking.
            .unwrap_or_else(Span::none);
        Outcome::Success(TraceSpan(span))
    }
}

/// Server spans with W3C context extraction, HTTP semantic-convention
/// attributes, and the request-duration histogram.
pub struct OtelFairing;

#[rocket::async_trait]
impl Fairing for OtelFairing {
    fn info(&self) -> Info {
        Info {
            name: "opentelemetry",
            kind: Kind::Request | Kind::Response,
        }
    }

    async fn on_request(&self, req: &mut Request<'_>, _data: &mut Data<'_>) {
        if untraced(req.uri().path().as_str()) {
            return;
        }

        let span = tracing::info_span!(
            "http-request",
            otel.name = tracing::field::Empty,
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
            http.request.method = %req.method(),
            http.route = tracing::field::Empty,
            url.path = %req.uri().path(),
            user_agent.original = %req.headers().get_one("user-agent").unwrap_or(""),
            http.response.status_code = tracing::field::Empty,
        );
        // Continue an upstream trace when the caller sent `traceparent`.
        let parent = global::get_text_map_propagator(|p| p.extract(&RocketHeaders(req.headers())));
        let _ = span.set_parent(parent);

        *req.local_cache(SpanSlot::default)
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some((span, Instant::now()));
    }

    async fn on_response<'r>(&self, req: &'r Request<'_>, res: &mut Response<'r>) {
        let slot: &SpanSlot = req.local_cache(SpanSlot::default);
        // `take` so the slot doesn't keep the span alive past the response.
        let taken = slot
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let Some((span, start)) = taken else {
            return;
        };

        // The route template is only known now that routing has run.
        let route = req
            .route()
            .map(|r| r.uri.to_string())
            .unwrap_or_else(|| "unmatched".to_owned());
        let status = res.status().code;

        span.record("http.route", route.as_str());
        span.record(
            "otel.name",
            format!("{} {route}", req.method()).as_str(),
        );
        span.record("http.response.status_code", status as i64);
        if status >= 500 {
            span.record("otel.status_code", "ERROR");
        }

        crate::metrics::HTTP_SERVER_DURATION.record(
            start.elapsed().as_secs_f64(),
            &[
                crate::otel::kv("http.request.method", req.method().as_str().to_owned()),
                crate::otel::kv("http.route", route),
                crate::otel::kv("http.response.status_code", status as i64),
            ],
        );

        // The last clone dropping is what closes the span, and closing is what
        // exports it.
        drop(span);
    }
}

#[cfg(all(test, feature = "otel-prometheus"))]
mod tests {
    use super::*;
    use rocket::local::asynchronous::Client;
    use rocket::{get, routes};

    #[get("/orders/<id>")]
    fn order(id: &str) -> String {
        id.to_owned()
    }

    #[get("/metrics")]
    fn metrics() -> &'static str {
        "ok"
    }

    #[rocket::async_test]
    async fn records_the_route_template_not_the_uri() {
        crate::otel::init("test-service", "info");

        let client = Client::tracked(
            rocket::build()
                .attach(OtelFairing)
                .mount("/", routes![order, metrics]),
        )
        .await
        .expect("rocket client");

        assert_eq!(client.get("/orders/8a3f-2b91").dispatch().await.status().code, 200);
        // Untraced: must not contribute a series.
        assert_eq!(client.get("/metrics").dispatch().await.status().code, 200);

        let out = crate::metrics::prom::encode().expect("encode");
        assert!(out.contains(r#"http_route="/orders/<id>""#), "{out}");
        assert!(!out.contains("8a3f-2b91"), "{out}");
        assert!(!out.contains(r#"http_route="/metrics""#), "{out}");
    }
}
