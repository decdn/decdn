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
/// `INFO` and above. Every other crate exports `WARN` and `ERROR` events only,
/// and no spans.
///
/// Fixed, not the log filter: the log level is operator-tunable and
/// hot-reloadable, and a `log_level = "warn"` must not silently stop every
/// trace. A dependency's `WARN` or `ERROR` event — an alloy RPC failure, an
/// iroh connection error — lands on the deCDN span it happened in. A
/// dependency's own spans never export, whatever their level: iroh opens its
/// periodic net-report spans at `WARN`, and those alone would outnumber every
/// deCDN span. `debug_span!`s stay local to the logs.
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
/// inside one, matched the way [`tracing_subscriber::filter::Targets`] matches.
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
    use tracing_subscriber::filter::FilterExt;

    let tracer = opentelemetry::trace::TracerProvider::tracer(provider, "decdn");
    let spans_from_decdn_only = tracing_subscriber::filter::filter_fn(|meta| {
        meta.is_event() || is_exported_target(meta.target())
    });
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
mod tests {
    use std::sync::{Arc, Mutex};

    use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
    use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};

    use super::{
        CountingSpanExporter, init_otlp_provider, otel_layer, record_run_failure, resource,
        shutdown_tracer_provider,
    };
    use crate::metrics::Metrics;

    /// In-memory exporter that records exported span names and returns a
    /// fixed result.
    #[derive(Debug, Clone)]
    struct StubExporter {
        names: Arc<Mutex<Vec<String>>>,
        /// Event count of each exported span, in export order.
        events: Arc<Mutex<Vec<usize>>>,
        fail: bool,
    }

    impl StubExporter {
        fn new(fail: bool) -> Self {
            Self {
                names: Arc::new(Mutex::new(Vec::new())),
                events: Arc::new(Mutex::new(Vec::new())),
                fail,
            }
        }
    }

    impl SpanExporter for StubExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            if let Ok(mut events) = self.events.lock() {
                events.extend(batch.iter().map(|s| s.events.len()));
            }
            if let Ok(mut names) = self.names.lock() {
                names.extend(batch.into_iter().map(|s| s.name.into_owned()));
            }
            if self.fail {
                Err(OTelSdkError::InternalFailure("stub failure".into()))
            } else {
                Ok(())
            }
        }
    }

    fn failures_line(metrics: &Metrics, count: u64) -> anyhow::Result<bool> {
        let expected = format!("decdn_otlp_export_failures_total {count}");
        Ok(metrics.encode()?.lines().any(|l| l == expected))
    }

    /// Only a failed export counts: a regression that drops or inverts the
    /// `is_err` check fires the alert on healthy nodes or never at all.
    #[tokio::test]
    async fn counting_exporter_counts_failures_only() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());

        let ok = CountingSpanExporter {
            inner: StubExporter::new(false),
            metrics: Arc::clone(&metrics),
        };
        ok.export(Vec::new()).await?;
        anyhow::ensure!(failures_line(&metrics, 0)?, "successful export was counted");

        let failing = CountingSpanExporter {
            inner: StubExporter::new(true),
            metrics: Arc::clone(&metrics),
        };
        anyhow::ensure!(
            failing.export(Vec::new()).await.is_err(),
            "failure must pass through"
        );
        anyhow::ensure!(failures_line(&metrics, 1)?, "failed export was not counted");
        Ok(())
    }

    /// A failed run's error reaches the exporter even with no span open:
    /// `record_run_failure` wraps its event in a span of its own.
    #[test]
    fn run_failure_is_exported_without_an_open_span() -> anyhow::Result<()> {
        use tracing_subscriber::prelude::*;

        let stub = StubExporter::new(false);
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(stub.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(otel_layer(&provider));
        {
            let _guard = tracing::subscriber::set_default(subscriber);
            record_run_failure(&anyhow::anyhow!("rpc unreachable"));
        }
        provider.force_flush()?;

        let names = stub
            .names
            .lock()
            .map_err(|_| anyhow::anyhow!("stub mutex poisoned"))?
            .clone();
        let events = stub
            .events
            .lock()
            .map_err(|_| anyhow::anyhow!("stub mutex poisoned"))?
            .clone();
        anyhow::ensure!(names == ["node_run_failed"], "exported spans: {names:?}");
        anyhow::ensure!(events == [1], "event counts: {events:?}");
        Ok(())
    }

    /// The layer `init_tracing` installs exports deCDN `info` spans and drops
    /// the OTLP transport's own, other dependencies', and `debug` spans.
    #[test]
    fn otel_layer_exports_only_decdn_info_spans() -> anyhow::Result<()> {
        use tracing_subscriber::prelude::*;

        let stub = StubExporter::new(false);
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(stub.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(otel_layer(&provider));
        {
            let _guard = tracing::subscriber::set_default(subscriber);
            tracing::info_span!(target: "h2::codec", "h2_span").in_scope(|| {});
            tracing::info_span!(target: "tonic::transport", "tonic_span").in_scope(|| {});
            tracing::info_span!(target: "iroh::endpoint", "iroh_span").in_scope(|| {});
            tracing::debug_span!(target: "decdn_node::runtime", "debug_span").in_scope(|| {});
            tracing::info_span!(target: "decdn_node::runtime", "app_span").in_scope(|| {});
            tracing::info_span!(target: "decdn_client", "client_span").in_scope(|| {
                // A dependency's WARN lands on the deCDN span; its INFO and the
                // OTLP transport's ERROR do not.
                tracing::warn!(target: "alloy::rpc", "dependency warning");
                tracing::info!(target: "iroh::endpoint", "dependency info");
                tracing::error!(target: "h2::codec", "transport error");
            });
        }
        provider.force_flush()?;

        let names = stub
            .names
            .lock()
            .map_err(|_| anyhow::anyhow!("stub mutex poisoned"))?
            .clone();
        anyhow::ensure!(
            names == ["app_span", "client_span"],
            "exported spans: {names:?}"
        );
        let events = stub
            .events
            .lock()
            .map_err(|_| anyhow::anyhow!("stub mutex poisoned"))?
            .clone();
        anyhow::ensure!(events == [0, 1], "event counts: {events:?}");
        Ok(())
    }

    /// A dependency span never exports, whatever its level: iroh opens its
    /// net-report spans at `WARN`. A dependency's `WARN` event still lands on the
    /// deCDN span around it, and a crate named like a deCDN crate is not one.
    #[test]
    fn otel_layer_drops_dependency_spans_at_any_level() -> anyhow::Result<()> {
        use tracing_subscriber::prelude::*;

        let stub = StubExporter::new(false);
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(stub.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(otel_layer(&provider));
        {
            let _guard = tracing::subscriber::set_default(subscriber);
            tracing::warn_span!(target: "iroh::net_report", "QADv4").in_scope(|| {});
            tracing::error_span!(target: "alloy::rpc", "alloy_span").in_scope(|| {});
            tracing::info_span!(target: "decdn_nodes", "lookalike_span").in_scope(|| {});
            tracing::info_span!(target: "decdn_node", "app_span").in_scope(|| {
                tracing::warn_span!(target: "iroh::net_report", "nested_dep_span").in_scope(|| {
                    tracing::warn!(target: "iroh::net_report", "dependency warning");
                });
            });
        }
        provider.force_flush()?;

        let names = stub
            .names
            .lock()
            .map_err(|_| anyhow::anyhow!("stub mutex poisoned"))?
            .clone();
        anyhow::ensure!(names == ["app_span"], "exported spans: {names:?}");
        let events = stub
            .events
            .lock()
            .map_err(|_| anyhow::anyhow!("stub mutex poisoned"))?
            .clone();
        anyhow::ensure!(events == [1], "event counts: {events:?}");
        Ok(())
    }

    /// Trace export does not follow the log level: with the log filter at
    /// `warn`, the stack `init_tracing` installs still exports an `info` span.
    #[test]
    fn log_level_does_not_filter_exported_spans() -> anyhow::Result<()> {
        use tracing_subscriber::prelude::*;

        let stub = StubExporter::new(false);
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(stub.clone())
            .build();
        let (log_filter, _handle) =
            tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::new("warn"));
        let subscriber = crate::commands::subscriber(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::sink)
                .boxed(),
            log_filter,
            Some(&provider),
        );
        {
            let _guard = tracing::subscriber::set_default(subscriber);
            tracing::info_span!(target: "decdn_node::handlers", "serve_stream").in_scope(|| {});
        }
        provider.force_flush()?;

        let names = stub
            .names
            .lock()
            .map_err(|_| anyhow::anyhow!("stub mutex poisoned"))?
            .clone();
        anyhow::ensure!(names == ["serve_stream"], "exported spans: {names:?}");
        Ok(())
    }

    #[test]
    fn resource_names_the_service_and_version() {
        use opentelemetry::{Key, Value};

        let resource = resource();
        assert_eq!(
            resource.get(&Key::new("service.name")),
            Some(Value::from("decdn"))
        );
        assert_eq!(
            resource.get(&Key::new("service.version")),
            Some(Value::from(env!("CARGO_PKG_VERSION")))
        );
    }

    /// A span still in the batch queue reaches the collector at shutdown, and
    /// the failed export is counted. The SDK's default batch delay (5 s,
    /// `OTEL_BSP_SCHEDULE_DELAY`) outlasts this test, so only the shutdown
    /// flush can open the connection. The listener closes each connection on
    /// accept, so the export fails fast (a silent listener would hang it past
    /// the shutdown bound); the accepted connection is the proof of the
    /// attempt.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_flushes_queued_spans_and_counts_the_failed_export() -> anyhow::Result<()> {
        use opentelemetry::trace::{Tracer, TracerProvider};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let accept = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await?;
            drop(stream);
            std::io::Result::Ok(peer)
        });

        let metrics = Arc::new(Metrics::new());
        let provider = init_otlp_provider(&endpoint, Arc::clone(&metrics))?;
        provider.tracer("test").in_span("queued", |_| {});
        shutdown_tracer_provider(provider).await;

        let accepted = tokio::time::timeout(std::time::Duration::from_secs(1), accept).await;
        anyhow::ensure!(
            matches!(accepted, Ok(Ok(Ok(_)))),
            "shutdown did not attempt an export: {accepted:?}"
        );
        anyhow::ensure!(failures_line(&metrics, 1)?, "failed export not counted");
        Ok(())
    }
}
