//! OTLP span export: exporter + tracer-provider bring-up, the export filter,
//! the export-failure counter, and the exit-time flush.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;

use crate::metrics::Metrics;

/// Upper bound on the exit-time OTLP flush, so an unreachable collector
/// cannot hold the process open after the runtime has drained.
const OTLP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Slack past [`OTLP_SHUTDOWN_TIMEOUT`] for the shutdown thread to report
/// back before the exit path stops waiting for it.
const OTLP_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

/// The deCDN crates whose spans and events the OpenTelemetry layer exports at
/// `INFO` and above. Every other crate, except the [`OTLP_TRANSPORT_TARGETS`],
/// exports `WARN` and `ERROR` events only, and no spans.
///
/// Fixed, not the log filter: the log level is operator-tunable and
/// hot-reloadable, and a `log_level = "warn"` must not silently stop every
/// trace. A dependency's `WARN` or `ERROR` event — an alloy RPC failure, an
/// iroh connection error — lands on the deCDN span it happened in. A
/// dependency's own spans never export, whatever their level, so a trace holds
/// deCDN operations only and a library's periodic background work (iroh's
/// net-report runs, for one) cannot outnumber them. `debug_span!`s stay local
/// to the logs.
const EXPORTED_TARGETS: &[&str] = &[
    "decdn_node",
    "decdn_cache",
    "decdn_client",
    "decdn_incentive",
    "decdn_reputation",
    "decdn_common",
];

/// Crates that make up the OTLP export connection itself, always off: at any
/// level their spans and events would become the next export's payload.
const OTLP_TRANSPORT_TARGETS: &[&str] = &["h2", "hyper", "hyper_util", "tonic", "tower"];

/// The level filter: [`EXPORTED_TARGETS`] at `INFO`, the OTLP transport
/// ([`OTLP_TRANSPORT_TARGETS`]) off, everything else at `WARN`.
fn export_filter() -> tracing_subscriber::filter::Targets {
    use tracing::level_filters::LevelFilter;

    tracing_subscriber::filter::Targets::new()
        .with_default(LevelFilter::WARN)
        .with_targets(
            EXPORTED_TARGETS
                .iter()
                .map(|target| (*target, LevelFilter::INFO)),
        )
        .with_targets(
            OTLP_TRANSPORT_TARGETS
                .iter()
                .map(|target| (*target, LevelFilter::OFF)),
        )
}

/// Whether `target` names one of the [`EXPORTED_TARGETS`] crates or a module
/// inside one. The match stops at a `::` boundary, so `decdn_nodes` is not
/// `decdn_node`; that is stricter than the plain prefix match
/// [`tracing_subscriber::filter::Targets`] applies to events.
fn is_exported_target(target: &str) -> bool {
    EXPORTED_TARGETS.iter().any(|crate_name| {
        target
            .strip_prefix(crate_name)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
    })
}

/// The tracing layer that exports spans through `provider`: events and spans
/// pass [`export_filter`], and a span must also come from one of the
/// [`EXPORTED_TARGETS`].
pub(super) fn otel_layer<S>(provider: &SdkTracerProvider) -> impl Layer<S> + use<S>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
{
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::filter::FilterExt;

    let tracer = opentelemetry::trace::TracerProvider::tracer(provider, "decdn");
    // The TRACE hint is what this filter admits by level. Without a hint, `and`
    // yields no hint and the subscriber's max level falls to TRACE, so every
    // dependency `trace!`/`log` record reaches the per-layer filters.
    let spans_from_decdn_only = tracing_subscriber::filter::filter_fn(|meta| {
        meta.is_event() || is_exported_target(meta.target())
    })
    .with_max_level_hint(LevelFilter::TRACE);
    tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(export_filter().and(spans_from_decdn_only))
}

/// Span exporter that counts failed export batches into
/// `decdn_otlp_export_failures_total` and otherwise defers to `inner`.
#[derive(Debug)]
struct CountingSpanExporter<E> {
    inner: E,
    metrics: Arc<Metrics>,
}

impl<E: SpanExporter> SpanExporter for CountingSpanExporter<E> {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let result = self.inner.export(batch).await;
        if result.is_err() {
            self.metrics.otlp_export_failure();
        }
        result
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn shutdown(&self) -> OTelSdkResult {
        self.inner.shutdown()
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

/// The OTLP resource every exported span carries.
///
/// `service.name` is `decdn` (not `decdn-node`), the same prefix as the
/// metrics — see ADR appendix-binaries. `service.version` is the binary's
/// crate version. `service.instance.id` is left to the collector, which knows
/// the host; the node's own iroh id rides on the spans that need it as
/// `local_node_id`. A deployment's collector may rewrite `service.name` (the
/// reference Grafana stack's Alloy sets `decdn-node`, which the dashboards
/// query).
fn resource() -> Resource {
    use opentelemetry::KeyValue;

    Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", "decdn"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build()
}

/// Build an OTLP span exporter and register its tracer provider as the
/// `opentelemetry` global. The returned handle shares state with the global.
///
/// The sampler is explicit: `ParentBased(AlwaysOn)` keeps every trace. No
/// trace context crosses the wire, so every root is local and the parent
/// check never defers to a remote peer's sampling decision. Spans are one per
/// stream, pull, lookup or transaction — never per frame — which keeps the
/// kept volume bounded by request rate.
pub(super) fn init_otlp_provider(
    endpoint: &str,
    metrics: Arc<Metrics>,
) -> anyhow::Result<SdkTracerProvider> {
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::Sampler;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build OTLP exporter: {e}"))?;

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(CountingSpanExporter {
            inner: exporter,
            metrics,
        })
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
        .with_resource(resource())
        .build();

    opentelemetry::global::set_tracer_provider(provider.clone());

    Ok(provider)
}

/// Record a failed run's error, then flush and shut the OTLP tracer provider
/// down.
///
/// The error is recorded before the flush, so it is exported with the
/// run's last spans instead of waiting behind the flush for `main`'s stderr
/// line.
pub(super) async fn finish_run(provider: SdkTracerProvider, result: &anyhow::Result<()>) {
    if let Err(err) = result {
        record_run_failure(err);
    }
    shutdown_tracer_provider(provider).await;
}

/// Log a failed run's error inside its own short span. The OpenTelemetry
/// layer exports an event only as part of a span, and nothing guarantees a
/// span is open when `runtime::run` returns. The error takes `main`'s
/// redaction, because the chain can carry `rpc_url`.
fn record_run_failure(err: &anyhow::Error) {
    tracing::error_span!("node_run_failed").in_scope(|| {
        tracing::error!(
            error = %decdn_common::redact::sanitize_err_chain(err),
            "node run failed"
        );
    });
}

/// Flush queued spans and shut the OTLP tracer provider down.
///
/// The `opentelemetry` global is a static that is never dropped, so without
/// this call the batch processor's queue is discarded at exit. Shutdown waits
/// synchronously on the batch worker, so it runs off the async workers, and
/// before `main` returns because this runtime drives the tonic channel.
///
/// It runs on a dedicated thread, not the blocking pool: long-running blocking
/// work can saturate the pool and queue the shutdown past its bound. The wait
/// is bounded too, so the exit path never waits longer than
/// [`OTLP_SHUTDOWN_TIMEOUT`] plus [`OTLP_SHUTDOWN_GRACE`].
async fn shutdown_tracer_provider(provider: SdkTracerProvider) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let spawned = std::thread::Builder::new()
        .name("otlp-shutdown".into())
        .spawn(move || {
            // The receiver is gone only if the exit path stopped waiting.
            let _ = tx.send(provider.shutdown_with_timeout(OTLP_SHUTDOWN_TIMEOUT));
        });
    if let Err(e) = spawned {
        tracing::warn!(error = %e, "could not start the OTLP shutdown thread; queued spans are lost");
        return;
    }
    let outcome = tokio::time::timeout(OTLP_SHUTDOWN_TIMEOUT + OTLP_SHUTDOWN_GRACE, rx).await;
    if let Some(problem) = shutdown_problem(outcome) {
        tracing::warn!("{problem}");
    }
}

/// Operator-facing description of a shutdown that did not flush cleanly, or
/// `None` when it did.
fn shutdown_problem(
    outcome: Result<
        Result<OTelSdkResult, tokio::sync::oneshot::error::RecvError>,
        tokio::time::error::Elapsed,
    >,
) -> Option<String> {
    match outcome {
        Ok(Ok(Ok(()))) => None,
        Ok(Ok(Err(OTelSdkError::Timeout(after)))) => Some(format!(
            "OTLP collector did not answer the exit-time flush within {after:?}; \
             the in-flight batch is abandoned"
        )),
        Ok(Ok(Err(e))) => Some(format!(
            "OTLP exit-time flush failed; its spans are lost: {e}"
        )),
        Ok(Err(_)) => Some(
            "OTLP shutdown thread ended without reporting; queued spans may be lost".to_string(),
        ),
        Err(_) => {
            Some("OTLP shutdown did not finish within its bound; exiting without it".to_string())
        }
    }
}

#[cfg(test)]
mod tests;
