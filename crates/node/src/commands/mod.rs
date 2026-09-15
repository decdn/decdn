//! Daemon entry point: `commands::run` parses the resolved config and
//! hands off to [`crate::runtime::run`].
//!
//! User-facing subcommands (probe, node admin, key-gen, config) live in
//! `crates/cli/`. This module retains only the `run` function and the
//! tracing/OTLP bring-up the daemon needs at start-up — pulling them
//! out into a separate file mostly avoids churn in
//! `crates/node/src/main.rs`, which stays a thin clap shell.

use std::time::Duration;

use decdn_common::{cli, config};
use opentelemetry_sdk::trace::SdkTracerProvider;

use crate::runtime;

/// Upper bound on the exit-time OTLP flush, so an unreachable collector
/// cannot hold the process open after the runtime has drained.
const OTLP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Crates whose spans make up the OTLP export connection itself. The
/// OpenTelemetry layer drops them: at `trace` level each export would otherwise emit
/// h2/tonic spans that become the next export's payload.
const OTLP_TRANSPORT_TARGETS: &[&str] = &["h2", "hyper", "hyper_util", "tonic", "tower"];

/// Run the deCDN node with resolved configuration.
#[expect(
    clippy::print_stderr,
    reason = "runs before the tracing subscriber is initialized"
)]
pub async fn run(
    config_path: Option<&std::path::Path>,
    run_args: &cli::RunArgs,
) -> anyhow::Result<()> {
    let (resolved, notices) = config::resolve_config(config_path, run_args)?;

    // Initialize tracing — RUST_LOG env var takes precedence over resolved log level.
    let filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(f) => f,
        Err(e) => {
            // Only warn if RUST_LOG was actually set (not just absent).
            if std::env::var_os("RUST_LOG").is_some() {
                eprintln!("warning: ignoring malformed RUST_LOG: {e}");
            }
            tracing_subscriber::EnvFilter::new(resolved.observability.log_level.to_string())
        }
    };

    let (log_level_setter, tracer_provider) = init_tracing(filter, &resolved)?;

    // Replay what `resolve_config` recorded. It runs before `init_tracing`
    // (the fallback filter above is built from the resolved log level), so a
    // resolver cannot emit these itself — it hands them back and this is the
    // first point at which a subscriber exists to receive them.
    runtime::emit_config_notices(&notices);

    tracing::info!("deCDN node starting");
    tracing::debug!(
        data_dir = %resolved.identity.data_dir.display(),
        bind_port = resolved.network.bind_port,
        rpc_url = "<redacted>",
        cache_dir = %resolved.cache.cache_dir.display(),
        cache_size_mb = resolved.cache.cache_size_mb,
        rate_per_mb = resolved.payment.rate_per_mb,
        metrics_port = resolved.observability.metrics_port,
        "resolved configuration"
    );

    let reload_state = std::sync::Arc::new(runtime::RuntimeReloadState::new(
        run_args.observability.clone(),
        &resolved,
        log_level_setter,
    ));

    let result = runtime::run(
        resolved,
        config_path.map(std::path::Path::to_path_buf),
        reload_state,
    )
    .await;

    // On both exit paths: the spans around a failed run are the ones an
    // operator most wants exported.
    if let Some(provider) = tracer_provider {
        shutdown_tracer_provider(provider).await;
    }

    result
}

/// Initialize the tracing subscriber with a fmt layer, plus an OTLP span
/// export layer when `observability.otlp_endpoint` is set.
///
/// Returns a [`runtime::LogLevelSetter`] closure that swaps the live
/// `EnvFilter` to one matching a new `LogLevel` — used by the SIGHUP
/// hot-reload path (#236). The closure captures a `reload::Handle` to the
/// `EnvFilter` layer; calls to `modify` must respect any errors from the
/// handle (e.g. the registry was dropped) by surfacing them.
///
/// Also returns the OTLP tracer provider when export is on; the caller
/// passes it to [`shutdown_tracer_provider`] before the process exits.
fn init_tracing(
    filter: tracing_subscriber::EnvFilter,
    resolved: &config::ResolvedConfig,
) -> anyhow::Result<(runtime::LogLevelSetter, Option<SdkTracerProvider>)> {
    use tracing_subscriber::prelude::*;

    let fmt_layer = match resolved.observability.log_format {
        cli::LogFormat::Json => tracing_subscriber::fmt::layer().json().boxed(),
        cli::LogFormat::Pretty => tracing_subscriber::fmt::layer().boxed(),
    };

    // Wrap the EnvFilter in a `reload::Layer` so the SIGHUP reload path
    // can swap it without rebuilding the rest of the subscriber stack.
    let (reload_filter, reload_handle) = tracing_subscriber::reload::Layer::new(filter);

    let registry = tracing_subscriber::registry()
        .with(reload_filter)
        .with(fmt_layer);

    let tracer_provider = if let Some(ref endpoint) = resolved.observability.otlp_endpoint {
        let provider = init_otlp_provider(endpoint)?;
        let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "decdn");
        let otel_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(tracing_subscriber::filter::filter_fn(|meta| {
                !is_otlp_transport_target(meta.target())
            }));
        registry.with(otel_layer).init();
        Some(provider)
    } else {
        registry.init();
        None
    };

    let setter: runtime::LogLevelSetter = Box::new(move |lvl| {
        // Build a fresh EnvFilter from the level's lowercase name. This
        // matches the startup default-filter construction above; we don't
        // attempt to honour `RUST_LOG` here because reload is driven by
        // the file, not the launching shell.
        let new_filter = tracing_subscriber::EnvFilter::try_new(lvl.to_string())
            .map_err(|e| anyhow::anyhow!("invalid log_level {lvl}: {e}"))?;
        reload_handle
            .modify(|f| *f = new_filter)
            .map_err(|e| anyhow::anyhow!("failed to swap tracing filter: {e}"))?;
        Ok(())
    });

    Ok((setter, tracer_provider))
}

/// Whether a span/event target belongs to the OTLP export transport
/// ([`OTLP_TRANSPORT_TARGETS`]), matched on the crate-path boundary.
fn is_otlp_transport_target(target: &str) -> bool {
    OTLP_TRANSPORT_TARGETS.iter().any(|krate| {
        target
            .strip_prefix(krate)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
    })
}

/// Build an OTLP span exporter and register its tracer provider as the
/// `opentelemetry` global. The returned handle shares state with the global.
///
/// Note: the metric prefix and OTLP `service.name` stay `decdn` (not
/// `decdn-node`) for dashboard/alert continuity across the binary
/// split — see ADR appendix-binaries.
fn init_otlp_provider(endpoint: &str) -> anyhow::Result<SdkTracerProvider> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::{SpanExporter, WithExportConfig};
    use opentelemetry_sdk::Resource;

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build OTLP exporter: {e}"))?;

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
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
async fn shutdown_tracer_provider(provider: SdkTracerProvider) {
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
    use super::{init_otlp_provider, is_otlp_transport_target, shutdown_tracer_provider};

    /// A span still in the batch queue reaches the collector at shutdown.
    /// The batch delay (5 s) outlasts this test, so only the shutdown flush
    /// can open the connection. The listener never speaks gRPC, so the export
    /// itself fails; the accepted connection is the proof of the attempt.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_flushes_queued_spans() -> anyhow::Result<()> {
        use opentelemetry::trace::{Tracer, TracerProvider};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let accept = tokio::spawn(async move { listener.accept().await });

        let provider = init_otlp_provider(&endpoint)?;
        provider.tracer("test").in_span("queued", |_| {});
        shutdown_tracer_provider(provider).await;

        let accepted = tokio::time::timeout(std::time::Duration::from_secs(1), accept).await;
        anyhow::ensure!(
            matches!(accepted, Ok(Ok(Ok(_)))),
            "shutdown did not attempt an export: {accepted:?}"
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
