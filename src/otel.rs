//! OpenTelemetry traces + metrics over OTLP/HTTP.
//!
//! [`init`] replaces [`crate::tracing_init::init_tracing`] for services that
//! should export to a collector. It is a no-op upgrade: with no
//! `OTEL_EXPORTER_OTLP_ENDPOINT` in the environment (or with
//! `OTEL_SDK_DISABLED=true`) it installs the plain fmt subscriber and exports
//! nothing, so a dev machine without a collector stays quiet.
//!
//! Everything else is configured through the standard OTel environment
//! variables (`OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`,
//! `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_TRACES_SAMPLER_ARG`, ...).
//!
//! Instrument with `tracing` (`#[tracing::instrument]`, `info_span!`), never
//! with the OTel API directly — the bridge layer translates spans on close.

use std::sync::OnceLock;
use std::time::Instant;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, global};
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracerProvider},
};

#[cfg(feature = "otel-axum")]
pub mod http;

use crate::metrics;

static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();
static METER_PROVIDER: OnceLock<SdkMeterProvider> = OnceLock::new();

/// The exporter's own HTTP stack must not be traced: its spans would produce
/// exports, which produce more spans. Appended to whatever filter is in use.
const EXPORTER_NOISE: &str = "hyper=off,hyper_util=off,h2=off,reqwest=off,tower=off,\
                              opentelemetry=off,opentelemetry_sdk=off,opentelemetry_otlp=off";

/// Whether an OTLP endpoint is configured (and the SDK isn't explicitly off).
pub fn enabled() -> bool {
    if std::env::var("OTEL_SDK_DISABLED").as_deref() == Ok("true") {
        return false;
    }
    [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    ]
    .iter()
    .any(|k| std::env::var(k).is_ok_and(|v| !v.trim().is_empty()))
}

/// Install the global subscriber, and — when an OTLP endpoint is configured —
/// the tracer/meter providers exporting to it.
///
/// `service` names the service unless `OTEL_SERVICE_NAME` overrides it;
/// `default_filter` is the `EnvFilter` used when `RUST_LOG` is unset.
/// Safe to call twice (the second call is ignored).
pub fn init(service: &str, default_filter: &str) {
    use tracing_subscriber::prelude::*;

    if !enabled() {
        crate::tracing_init::init_tracing(default_filter);
        return;
    }

    global::set_text_map_propagator(TraceContextPropagator::new());

    let base = std::env::var("RUST_LOG").unwrap_or_else(|_| default_filter.to_string());
    let filter = tracing_subscriber::EnvFilter::new(format!("{base},{EXPORTER_NOISE}"));

    let resource = {
        // `Resource::builder` already reads OTEL_SERVICE_NAME and
        // OTEL_RESOURCE_ATTRIBUTES; only fill in the name when it didn't.
        let b = Resource::builder();
        if std::env::var("OTEL_SERVICE_NAME").is_ok_and(|v| !v.trim().is_empty()) {
            b.build()
        } else {
            b.with_service_name(service.to_string()).build()
        }
    };

    // Honour an upstream sampling decision; the ratio applies only to traces
    // that originate here.
    let ratio = std::env::var("OTEL_TRACES_SAMPLER_ARG")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(1.0);

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP span exporter");
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            ratio,
        ))))
        .with_resource(resource.clone())
        .build();
    let tracer = tracer_provider.tracer(service.to_string());
    global::set_tracer_provider(tracer_provider.clone());
    let _ = TRACER_PROVIDER.set(tracer_provider);

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP metric exporter");
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(metric_exporter).build())
        .with_resource(resource)
        .build();
    global::set_meter_provider(meter_provider.clone());
    let _ = METER_PROVIDER.set(meter_provider);

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(metrics::EventMetricsLayer)
        .try_init();

    // Bind the instruments to the provider that was just installed.
    metrics::init();

    tracing::info!(service, sampler_ratio = ratio, "opentelemetry export enabled");
}

/// Flush and stop the exporters. Call before exiting — the batch processors
/// buffer, so without this the last batch (typically the interesting one) is
/// lost. Errors are logged, never propagated.
pub fn shutdown() {
    if let Some(p) = TRACER_PROVIDER.get() {
        if let Err(e) = p.shutdown() {
            tracing::warn!("otel tracer shutdown: {e}");
        }
    }
    if let Some(p) = METER_PROVIDER.get() {
        if let Err(e) = p.shutdown() {
            tracing::warn!("otel meter shutdown: {e}");
        }
    }
}

/// Process start, for the uptime gauge.
pub(crate) fn started_at() -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now)
}

/// Attribute helper: `KeyValue::new` with a `'static` key.
pub(crate) fn kv(key: &'static str, value: impl Into<opentelemetry::Value>) -> KeyValue {
    KeyValue::new(key, value.into())
}
