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

/// Prometheus bridge: the OTel pipeline is the source of truth, this renders
/// it in the Prometheus text format for scrape endpoints.
#[cfg(feature = "otel-prometheus")]
pub mod prom {
    use std::sync::OnceLock;

    /// Re-exported so callers encode with the same version this was built with.
    pub use prometheus;

    static REGISTRY: OnceLock<prometheus::Registry> = OnceLock::new();

    /// The registry fed by the OTel meter provider. Empty until
    /// [`crate::otel::init`] has run.
    pub fn registry() -> &'static prometheus::Registry {
        REGISTRY.get_or_init(prometheus::Registry::new)
    }

    /// Collect and encode everything in the registry. Collecting runs the
    /// observable-instrument callbacks, so gauges are read at scrape time.
    pub fn encode() -> Result<String, prometheus::Error> {
        encode_families(registry().gather())
    }

    /// [`encode`] over a caller-filtered family list (e.g. one tenant's series).
    pub fn encode_families(families: Vec<prometheus::proto::MetricFamily>) -> Result<String, prometheus::Error> {
        use prometheus::Encoder;
        let mut buf = Vec::new();
        prometheus::TextEncoder::new().encode(&families, &mut buf)?;
        String::from_utf8(buf).map_err(|e| prometheus::Error::Msg(e.to_string()))
    }
}

/// The metric reader that feeds [`prom::registry`]. Installed by
/// [`crate::otel::init`].
#[cfg(feature = "otel-prometheus")]
pub(crate) fn prometheus_reader() -> opentelemetry_prometheus::PrometheusExporter {
    opentelemetry_prometheus::exporter()
        .with_registry(prom::registry().clone())
        // Scope info adds an `otel_scope_name` label to every series and a
        // metadata metric; neither earns its bytes here.
        .without_scope_info()
        .build()
        .expect("failed to build prometheus exporter")
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

#[cfg(all(test, feature = "otel-prometheus"))]
mod tests {
    use super::*;

    #[test]
    fn otel_metrics_render_as_prometheus() {
        crate::otel::init("test-service", "info");
        counter("test.events").add(3, &[kv("kind", "demo")]);
        tracing::warn!("counted by the event layer");

        let out = prom::encode().expect("encode");
        assert!(out.contains("test_events_total{kind=\"demo\"} 3"), "{out}");
        // Observable callback ran at collection time.
        assert!(out.contains("process_uptime_seconds"), "{out}");
        assert!(out.contains("log_events_total{level=\"warn\"}"), "{out}");
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
