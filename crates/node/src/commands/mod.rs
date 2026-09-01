//! Daemon entry point: `commands::run` parses the resolved config and
//! hands off to [`crate::runtime::run`].
//!
//! User-facing subcommands (probe, node admin, key-gen, config) live in
//! `crates/cli/`. This module retains only the `run` function and the
//! tracing/OTLP bring-up the daemon needs at start-up — pulling them
//! out into a separate file mostly avoids churn in
//! `crates/node/src/main.rs`, which stays a thin clap shell.

use decdn_common::{cli, config};

use crate::runtime;

/// Run the deCDN node with resolved configuration.
#[expect(
    clippy::print_stderr,
    reason = "runs before the tracing subscriber is initialized"
)]
pub async fn run(
    config_path: Option<&std::path::Path>,
    run_args: &cli::RunArgs,
) -> anyhow::Result<()> {
    let resolved = config::resolve_config(config_path, run_args)?;

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

    let log_level_setter = init_tracing(filter, &resolved)?;

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

    runtime::run(
        resolved,
        config_path.map(std::path::Path::to_path_buf),
        reload_state,
    )
    .await
}

/// Initialize the tracing subscriber with fmt layer and optional OTLP layer.
///
/// Returns a [`runtime::LogLevelSetter`] closure that swaps the live
/// `EnvFilter` to one matching a new `LogLevel` — used by the SIGHUP
/// hot-reload path (#236). The closure captures a `reload::Handle` to the
/// `EnvFilter` layer; calls to `modify` must respect any errors from the
/// handle (e.g. the registry was dropped) by surfacing them.
#[allow(clippy::unnecessary_wraps)] // Returns Result only when otlp feature is enabled.
fn init_tracing(
    filter: tracing_subscriber::EnvFilter,
    resolved: &config::ResolvedConfig,
) -> anyhow::Result<runtime::LogLevelSetter> {
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

    #[cfg(feature = "otlp")]
    {
        if let Some(ref endpoint) = resolved.observability.otlp_endpoint {
            let tracer = init_otlp_tracer(endpoint)?;
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            registry.with(otel_layer).init();
        } else {
            registry.init();
        }
    }

    #[cfg(not(feature = "otlp"))]
    {
        // `allow`, not `expect`: this `eprintln!` is itself cfg-gated, so an
        // `--all-features` build would find the expectation unfulfilled. Scoped
        // to this block rather than the function so the `LogLevelSetter` closure
        // below — which runs on SIGHUP, long after `registry.init()` — stays
        // covered by the workspace deny.
        #[allow(clippy::print_stderr)]
        if resolved.observability.otlp_endpoint.is_some() {
            eprintln!("warning: --otlp-endpoint ignored (binary not built with 'otlp' feature)");
        }
        registry.init();
    }

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

    Ok(setter)
}

/// Build an OTLP span exporter and tracer provider.
///
/// Note: the metric prefix and OTLP `service.name` stay `decdn` (not
/// `decdn-node`) for dashboard/alert continuity across the binary
/// split — see ADR appendix-binaries.
#[cfg(feature = "otlp")]
fn init_otlp_tracer(endpoint: &str) -> anyhow::Result<opentelemetry_sdk::trace::SdkTracer> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::{SpanExporter, WithExportConfig};
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::trace::SdkTracerProvider;

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

    let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "decdn");
    opentelemetry::global::set_tracer_provider(provider);

    Ok(tracer)
}
