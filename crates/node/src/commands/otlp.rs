//! OTLP span export: exporter + tracer-provider bring-up, the transport-span
//! filter, the export-failure counter, and the exit-time flush.

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

/// Crates whose spans make up the OTLP export connection itself. The
/// OpenTelemetry layer drops them: at `trace` level each export would
/// otherwise emit h2/tonic spans that become the next export's payload.
const OTLP_TRANSPORT_TARGETS: &[&str] = &["h2", "hyper", "hyper_util", "tonic", "tower"];

/// Whether a span/event target belongs to the OTLP export transport
/// ([`OTLP_TRANSPORT_TARGETS`]), matched on the crate-path boundary.
fn is_otlp_transport_target(target: &str) -> bool {
    OTLP_TRANSPORT_TARGETS.iter().any(|krate| {
        target
            .strip_prefix(krate)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
    })
}

/// The tracing layer that exports spans through `provider`, with the OTLP
/// transport's own spans filtered out.
pub(super) fn otel_layer<S>(provider: &SdkTracerProvider) -> impl Layer<S> + use<S>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
{
    let tracer = opentelemetry::trace::TracerProvider::tracer(provider, "decdn");
    tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(tracing_subscriber::filter::filter_fn(|meta| {
            !is_otlp_transport_target(meta.target())
        }))
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

/// Build an OTLP span exporter and register its tracer provider as the
/// `opentelemetry` global. The returned handle shares state with the global.
///
/// Note: the metric prefix and OTLP `service.name` stay `decdn` (not
/// `decdn-node`) for dashboard/alert continuity across the binary
/// split — see ADR appendix-binaries.
pub(super) fn init_otlp_provider(
    endpoint: &str,
    metrics: Arc<Metrics>,
) -> anyhow::Result<SdkTracerProvider> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;

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
        .with_resource(
            Resource::builder()
                .with_attributes([KeyValue::new("service.name", "decdn")])
                .build(),
        )
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
        CountingSpanExporter, init_otlp_provider, is_otlp_transport_target, otel_layer,
        record_run_failure, shutdown_tracer_provider,
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

    /// The layer `init_tracing` installs exports application spans and drops
    /// the OTLP transport's own.
    #[test]
    fn otel_layer_drops_transport_spans() -> anyhow::Result<()> {
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
            tracing::info_span!(target: "decdn_node::runtime", "app_span").in_scope(|| {});
        }
        provider.force_flush()?;

        let names = stub
            .names
            .lock()
            .map_err(|_| anyhow::anyhow!("stub mutex poisoned"))?
            .clone();
        anyhow::ensure!(names == ["app_span"], "exported spans: {names:?}");
        Ok(())
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

    #[test]
    fn otlp_transport_targets_match_on_crate_boundary() {
        for target in [
            "h2",
            "h2::codec::framed_write",
            "tonic::transport",
            "hyper_util::client",
        ] {
            assert!(
                is_otlp_transport_target(target),
                "{target} should be filtered"
            );
        }
        for target in ["decdn_node::runtime", "h2o", "towering", "hyperion::x"] {
            assert!(!is_otlp_transport_target(target), "{target} should pass");
        }
    }
}
