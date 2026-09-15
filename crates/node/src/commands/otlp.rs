//! OTLP span export: exporter + tracer-provider bring-up, the transport-span
//! filter, the export-failure counter, and the exit-time flush.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};

use crate::metrics::Metrics;

/// Upper bound on the exit-time OTLP flush, so an unreachable collector
/// cannot hold the process open after the runtime has drained.
const OTLP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Crates whose spans make up the OTLP export connection itself. The
/// OpenTelemetry layer drops them: at `trace` level each export would
/// otherwise emit h2/tonic spans that become the next export's payload.
const OTLP_TRANSPORT_TARGETS: &[&str] = &["h2", "hyper", "hyper_util", "tonic", "tower"];

/// Whether a span/event target belongs to the OTLP export transport
/// ([`OTLP_TRANSPORT_TARGETS`]), matched on the crate-path boundary.
pub(super) fn is_otlp_transport_target(target: &str) -> bool {
    OTLP_TRANSPORT_TARGETS.iter().any(|krate| {
        target
            .strip_prefix(krate)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
    })
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

/// Flush queued spans and shut the OTLP tracer provider down.
///
/// The `opentelemetry` global is a static that is never dropped, so without
/// this call the batch processor's queue is discarded at exit. Shutdown waits
/// synchronously on the batch worker, so it runs on a blocking thread; it runs
/// before `main` returns because this runtime drives the tonic channel.
pub(super) async fn shutdown_tracer_provider(provider: SdkTracerProvider) {
    let joined =
        tokio::task::spawn_blocking(move || provider.shutdown_with_timeout(OTLP_SHUTDOWN_TIMEOUT))
            .await;
    match joined {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "OTLP tracer provider shutdown failed; queued spans may be lost");
        }
        Err(e) => {
            tracing::warn!(error = %e, "OTLP tracer provider shutdown task did not complete");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{init_otlp_provider, is_otlp_transport_target, shutdown_tracer_provider};
    use crate::metrics::Metrics;

    /// A span still in the batch queue reaches the collector at shutdown, and
    /// the failed export is counted. The batch delay (5 s) outlasts this test,
    /// so only the shutdown flush can open the connection. The listener closes
    /// each connection on accept, so the export fails fast (a silent listener
    /// would hang it past the shutdown bound); the accepted connection is the
    /// proof of the attempt.
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
        let body = metrics.encode()?;
        anyhow::ensure!(
            body.lines()
                .any(|l| l == "decdn_otlp_export_failures_total 1"),
            "failed export not counted"
        );
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
