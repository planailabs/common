//! OpenTelemetry traces, metrics and logs over OTLP/HTTP.
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
//! with the OTel API directly — the bridge layers translate spans on close and
//! events into log records, the latter carrying the enclosing span's trace and
//! span id. Events also keep going to stdout: journald stays readable when the
//! collector is unreachable.

use std::sync::OnceLock;
use std::time::Instant;

/// Re-exported so dependents build their own instruments against the exact
/// version pinned here. A different minor would register with a different
/// global meter provider and silently report nothing.
pub use opentelemetry;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, global};
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracerProvider},
};

#[cfg(feature = "otel-axum")]
pub mod http;
#[cfg(feature = "otel-rocket")]
pub mod rocket;

use crate::metrics;

static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();
static METER_PROVIDER: OnceLock<SdkMeterProvider> = OnceLock::new();
static LOGGER_PROVIDER: OnceLock<SdkLoggerProvider> = OnceLock::new();

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

/// The `EnvFilter` directives to run with: `RUST_LOG` when set, otherwise
/// `default_filter` — plus the exporter-silencing directives when an OTLP
/// endpoint is configured.
///
/// [`init`] applies this itself; call it directly only when building the
/// subscriber by hand, as the daemon does for its reloadable filter.
pub fn effective_filter(default_filter: &str) -> String {
    let base = std::env::var("RUST_LOG").unwrap_or_else(|_| default_filter.to_string());
    if enabled() {
        silence_exporter(&base)
    } else {
        base
    }
}

/// Install the tracer, meter and logger providers and return the subscriber
/// layers that feed them, for composing into a hand-built registry:
///
/// ```ignore
/// tracing_subscriber::registry()
///     .with(my_reloadable_filter)
///     .with(tracing_subscriber::fmt::layer())
///     .with(otel::layers("my-service"))
///     .init();
/// ```
///
/// Most services want [`init`] instead. Returns an empty vec on a second call
/// — a second set of providers would register a second Prometheus collector on
/// the same registry, which the registry rejects.
pub fn layers<S>(service: &str) -> Vec<Box<dyn tracing_subscriber::Layer<S> + Send + Sync>>
where
    S: tracing::Subscriber + Send + Sync + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    static ONCE: OnceLock<()> = OnceLock::new();
    if ONCE.set(()).is_err() {
        return Vec::new();
    }

    let otlp = enabled();
    // Metrics are also collected for a local Prometheus scrape endpoint, so
    // they stay on without a collector; traces only exist to be exported.
    let metrics_enabled = otlp || cfg!(feature = "otel-prometheus");

    if otlp {
        global::set_text_map_propagator(TraceContextPropagator::new());
    }

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

    let tracer = otlp.then(|| {
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .build()
            .expect("failed to build OTLP span exporter");
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter)
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                ratio,
            ))))
            .with_resource(resource.clone())
            .build();
        let tracer = provider.tracer(service.to_string());
        global::set_tracer_provider(provider.clone());
        let _ = TRACER_PROVIDER.set(provider);
        tracer
    });

    if metrics_enabled {
        let mut builder = SdkMeterProvider::builder().with_resource(resource.clone());
        #[cfg(feature = "otel-prometheus")]
        {
            builder = builder.with_reader(metrics::prometheus_reader());
        }
        if otlp {
            let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .build()
                .expect("failed to build OTLP metric exporter");
            builder = builder.with_reader(PeriodicReader::builder(metric_exporter).build());
        }
        let meter_provider = builder.build();
        global::set_meter_provider(meter_provider.clone());
        let _ = METER_PROVIDER.set(meter_provider);
    }

    // Log records. `tracing-opentelemetry` activates the OTel context on span
    // entry, so records emitted inside a span carry its trace and span id.
    let logs = otlp.then(|| {
        let log_exporter = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .build()
            .expect("failed to build OTLP log exporter");
        let provider = SdkLoggerProvider::builder()
            .with_batch_exporter(log_exporter)
            .with_resource(resource)
            .build();
        let bridge =
            opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&provider);
        let _ = LOGGER_PROVIDER.set(provider);
        bridge
    });

    let mut out: Vec<Box<dyn tracing_subscriber::Layer<S> + Send + Sync>> = Vec::new();
    if let Some(tracer) = tracer {
        out.push(Box::new(
            tracing_opentelemetry::layer().with_tracer(tracer),
        ));
    }
    if let Some(logs) = logs {
        out.push(Box::new(logs));
    }
    if metrics_enabled {
        out.push(Box::new(metrics::EventMetricsLayer));
        // Bind the instruments to the provider that was just installed.
        metrics::init();
    }
    // Sentry stays a peer of the OTLP bridge rather than an alternative to it:
    // errors keep their existing triage path while traces go to the collector.
    // A no-op until `sentry_ext::init_sentry` installs a client.
    #[cfg(feature = "sentry")]
    out.push(Box::new(sentry::integrations::tracing::layer()));

    if otlp {
        tracing::info!(service, sampler_ratio = ratio, "exporting to OTLP collector");
    }
    out
}

/// Install the global subscriber, and — when an OTLP endpoint is configured —
/// the tracer/meter/logger providers exporting to it.
///
/// `service` names the service unless `OTEL_SERVICE_NAME` overrides it;
/// `default_filter` is the `EnvFilter` used when `RUST_LOG` is unset.
/// Safe to call twice (the second call is ignored).
pub fn init(service: &str, default_filter: &str) {
    use tracing_subscriber::prelude::*;

    // `get_or_init` rather than `set`: a second caller has to *wait* for the
    // first to finish, not skip ahead. Anything that builds an instrument
    // right after calling init would otherwise bind to the no-op provider
    // that is still installed while the first call is mid-flight.
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let filter = tracing_subscriber::EnvFilter::new(effective_filter(default_filter));
        let _ = tracing_subscriber::registry()
            .with(filter)
            // Kept alongside the OTLP bridge on purpose: journald stays useful
            // when the collector is unreachable, which is when logs matter most.
            .with(tracing_subscriber::fmt::layer())
            .with(layers(service))
            .try_init();
    });
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
    if let Some(p) = LOGGER_PROVIDER.get() {
        if let Err(e) = p.shutdown() {
            tracing::warn!("otel logger shutdown: {e}");
        }
    }
}

/// Append the exporter-silencing directives that `filter` doesn't already
/// speak to — an explicit `RUST_LOG=hyper=debug` still wins, everything else
/// gets muted so exports can't feed themselves.
fn silence_exporter(filter: &str) -> String {
    let configured: Vec<&str> = filter
        .split(',')
        .filter_map(|d| d.split('=').next())
        .map(str::trim)
        .collect();
    let mut out = filter.to_string();
    for directive in EXPORTER_NOISE.split(',').map(str::trim) {
        let target = directive.split('=').next().unwrap_or_default();
        if !configured.contains(&target) {
            out.push(',');
            out.push_str(directive);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::silence_exporter;

    #[test]
    fn keeps_explicit_directives_and_mutes_the_rest() {
        let out = silence_exporter("info,hyper=debug");
        assert!(out.starts_with("info,hyper=debug"));
        assert!(!out.contains("hyper=off"), "{out}");
        assert!(out.contains("reqwest=off"), "{out}");
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
