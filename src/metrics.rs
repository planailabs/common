//! Process-wide OpenTelemetry metrics.
//!
//! Instruments bind to the global meter provider the first time they are
//! touched, so [`init`] forces them right after [`crate::otel::init`] installs
//! the provider. Without a provider they are no-ops — recording is always
//! safe, it just goes nowhere.

use std::sync::LazyLock;

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter, ObservableGauge};

use crate::otel::{kv, started_at};

const SCOPE: &str = "mac-mgmt-common";

pub fn meter() -> Meter {
    global::meter(SCOPE)
}

/// Inbound HTTP request duration, seconds. Attributes:
/// `http.request.method`, `http.route`, `http.response.status_code`.
pub static HTTP_SERVER_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    meter()
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .with_description("Duration of inbound HTTP requests")
        .build()
});

/// Emitted `tracing` events, by `level`. Gives an error/warn rate for free —
/// no counter has to be wired into individual call sites.
pub static LOG_EVENTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter()
        .u64_counter("log.events")
        .with_description("tracing events emitted, by level")
        .build()
});

/// Seconds since process start. Doubles as a restart detector.
static UPTIME: LazyLock<ObservableGauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_observable_gauge("process.uptime")
        .with_unit("s")
        .with_description("Seconds since process start")
        .with_callback(|o| o.observe(started_at().elapsed().as_secs_f64(), &[]))
        .build()
});

/// An ad-hoc counter for application events (`counter("deploy.failed").add(1, &[])`).
/// Cheap enough to call per event, but hoist it into a `LazyLock` on hot paths.
pub fn counter(name: &'static str) -> Counter<u64> {
    meter().u64_counter(name).build()
}

/// Bind the instruments above to the currently-installed meter provider.
pub fn init() {
    LazyLock::force(&HTTP_SERVER_DURATION);
    LazyLock::force(&LOG_EVENTS);
    LazyLock::force(&UPTIME);
    let _ = started_at();
}

/// Counts `tracing` events into [`LOG_EVENTS`].
pub struct EventMetricsLayer;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EventMetricsLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        LOG_EVENTS.add(1, &[kv("level", level_str(event.metadata().level()))]);
    }
}

fn level_str(level: &tracing::Level) -> &'static str {
    match *level {
        tracing::Level::ERROR => "error",
        tracing::Level::WARN => "warn",
        tracing::Level::INFO => "info",
        tracing::Level::DEBUG => "debug",
        tracing::Level::TRACE => "trace",
    }
}
