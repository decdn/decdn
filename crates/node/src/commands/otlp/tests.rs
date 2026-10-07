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
/// deCDN span around it, and a span from a crate named like a deCDN crate
/// is not a deCDN span.
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
        tracing::info_span!(target: "decdn_node::handlers", "serve_stream").in_scope(|| {
            // The fmt layer admits this WARN dependency span, so it exists in
            // the registry; the export layer must still skip it and hang the
            // event on `serve_stream`.
            tracing::warn_span!(target: "iroh::net_report", "QADv4").in_scope(|| {
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
    anyhow::ensure!(names == ["serve_stream"], "exported spans: {names:?}");
    let events = stub
        .events
        .lock()
        .map_err(|_| anyhow::anyhow!("stub mutex poisoned"))?
        .clone();
    anyhow::ensure!(events == [1], "event counts: {events:?}");
    Ok(())
}

/// The export layer keeps the subscriber's max level at `INFO`, so a
/// dependency's `debug`/`trace` callsites and `log` records stop at the
/// static level check instead of reaching the per-layer filters.
#[test]
fn otel_layer_keeps_the_info_max_level_hint() {
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::prelude::*;

    let provider = SdkTracerProvider::builder().build();
    let subscriber = tracing_subscriber::registry().with(otel_layer(&provider));
    assert_eq!(
        tracing::Subscriber::max_level_hint(&subscriber),
        Some(LevelFilter::INFO)
    );
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
