//! Daemon entry point: `commands::run` parses the resolved config and
//! hands off to [`crate::runtime::run`].
//!
//! User-facing subcommands (probe, node admin, key-gen, config) live in
//! `crates/cli/`. This module retains only the `run` function and the
//! tracing/OTLP bring-up the daemon needs at start-up — pulling them
//! out into a separate file mostly avoids churn in
//! `crates/node/src/main.rs`, which stays a thin clap shell.

mod otlp;

use std::sync::Arc;

use decdn_common::{cli, config};
use opentelemetry_sdk::trace::SdkTracerProvider;

use crate::{metrics, runtime};

/// Run the deCDN node with resolved configuration.
pub async fn run(
    config_path: Option<&std::path::Path>,
    run_args: &cli::RunArgs,
) -> anyhow::Result<()> {
    let (resolved, notices) = config::resolve_config(config_path, run_args)?;

    let (filter, rust_log_pinned) = startup_log_filter(&resolved);

    // Built before tracing so the OTLP exporter counts into the registry
    // `/metrics` serves.
    let node_metrics = Arc::new(metrics::Metrics::new());

    let (log_level_setter, tracer_provider) = init_tracing(
        filter,
        rust_log_pinned,
        &resolved,
        Arc::clone(&node_metrics),
    )?;

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

    let reload_state = Arc::new(runtime::RuntimeReloadState::new(
        run_args.observability.clone(),
        &resolved,
        log_level_setter,
    ));

    let result = runtime::run(
        resolved,
        config_path.map(std::path::Path::to_path_buf),
        reload_state,
        node_metrics,
    )
    .await;

    // On both exit paths: the spans around a failed run are the ones an
    // operator most wants exported.
    if let Some(provider) = tracer_provider {
        otlp::finish_run(provider, &result).await;
    }

    result
}

/// The startup log filter, and whether it came from `RUST_LOG`.
///
/// `RUST_LOG` takes precedence over the resolved log level, for the whole life
/// of the process (see [`reload_directive`]). A malformed `RUST_LOG` falls back
/// to the resolved level with a warning on stderr.
#[expect(
    clippy::print_stderr,
    reason = "runs before the tracing subscriber is initialized"
)]
fn startup_log_filter(resolved: &config::ResolvedConfig) -> (tracing_subscriber::EnvFilter, bool) {
    match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(f) => (f, true),
        Err(e) => {
            // Only warn if RUST_LOG was actually set (not just absent).
            if std::env::var_os("RUST_LOG").is_some() {
                eprintln!("warning: ignoring malformed RUST_LOG: {e}");
            }
            (
                tracing_subscriber::EnvFilter::new(resolved.observability.log_level.to_string()),
                false,
            )
        }
    }
}

/// Initialize the tracing subscriber with a fmt layer, plus an OTLP span
/// export layer when `observability.otlp_endpoint` is set.
///
/// Returns a [`runtime::LogLevelSetter`] closure that swaps the live
/// `EnvFilter` to one matching a new `LogLevel` — used by the SIGHUP
/// hot-reload path (#236). When `rust_log_pinned` is set, the filter came from
/// `RUST_LOG` and the closure leaves it in place (see [`reload_directive`]).
/// The closure captures a `reload::Handle` to the `EnvFilter` layer; calls to
/// `modify` must respect any errors from the handle (e.g. the registry was
/// dropped) by surfacing them.
///
/// Also returns the OTLP tracer provider when export is on; the caller
/// passes it to [`otlp::finish_run`] before the process exits.
fn init_tracing(
    filter: tracing_subscriber::EnvFilter,
    rust_log_pinned: bool,
    resolved: &config::ResolvedConfig,
    node_metrics: Arc<metrics::Metrics>,
) -> anyhow::Result<(runtime::LogLevelSetter, Option<SdkTracerProvider>)> {
    use tracing_subscriber::prelude::*;

    let fmt_layer = match resolved.observability.log_format {
        cli::LogFormat::Json => tracing_subscriber::fmt::layer().json().boxed(),
        cli::LogFormat::Pretty => tracing_subscriber::fmt::layer().boxed(),
    };

    // Wrap the EnvFilter in a `reload::Layer` so the SIGHUP reload path
    // can swap it without rebuilding the rest of the subscriber stack. It
    // filters the fmt layer only: span export has its own fixed filter
    // (`otlp::otel_layer`), so lowering the log level never drops traces.
    let (reload_filter, reload_handle) = tracing_subscriber::reload::Layer::new(filter);

    let registry = tracing_subscriber::registry().with(fmt_layer.with_filter(reload_filter));

    let tracer_provider = if let Some(ref endpoint) = resolved.observability.otlp_endpoint {
        let provider = otlp::init_otlp_provider(endpoint, node_metrics)?;
        registry.with(otlp::otel_layer(&provider)).init();
        Some(provider)
    } else {
        registry.init();
        None
    };

    let setter: runtime::LogLevelSetter = Box::new(move |lvl| {
        let Some(directive) = reload_directive(rust_log_pinned, lvl) else {
            tracing::info!(
                log_level = %lvl,
                "RUST_LOG is set; the config file log_level is not applied"
            );
            return Ok(runtime::LogLevelApply::KeptRustLog);
        };
        let new_filter = tracing_subscriber::EnvFilter::try_new(directive)
            .map_err(|e| anyhow::anyhow!("invalid log_level {lvl}: {e}"))?;
        reload_handle
            .modify(|f| *f = new_filter)
            .map_err(|e| anyhow::anyhow!("failed to swap tracing filter: {e}"))?;
        Ok(runtime::LogLevelApply::Installed)
    });

    Ok((setter, tracer_provider))
}

/// The filter directive a config-file `log_level` reload installs, or `None`
/// when the live filter must stay as it is.
///
/// `RUST_LOG` wins over the file `log_level` at startup, so it wins on every
/// reload too: a reload that replaced a `RUST_LOG` filter with a bare level
/// would silently drop its per-target directives. Without a valid `RUST_LOG`, the
/// directive is the level's lowercase name, the same construction as the
/// startup fallback.
fn reload_directive(rust_log_pinned: bool, lvl: cli::common::LogLevel) -> Option<String> {
    (!rust_log_pinned).then(|| lvl.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reload_directive_keeps_a_rust_log_filter_and_applies_the_file_level_otherwise() {
        for lvl in [
            cli::common::LogLevel::Error,
            cli::common::LogLevel::Warn,
            cli::common::LogLevel::Info,
            cli::common::LogLevel::Debug,
            cli::common::LogLevel::Trace,
        ] {
            assert_eq!(
                reload_directive(true, lvl),
                None,
                "RUST_LOG pins the filter"
            );
            assert_eq!(reload_directive(false, lvl), Some(lvl.to_string()));
        }
    }

    /// The reload handle still swaps the level when the `EnvFilter` is a
    /// per-layer filter on the fmt layer (the `init_tracing` shape), not a
    /// global layer: a debug event is dropped at `info` and written after the
    /// swap to `debug`.
    #[test]
    fn per_layer_reload_filter_applies_a_new_level() -> anyhow::Result<()> {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::prelude::*;

        #[derive(Clone, Default)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .map_err(|_| std::io::Error::other("poisoned"))?
                    .extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buf = Buf::default();
        let writer = buf.clone();
        let (filter, handle) =
            tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::new("info"));
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_writer(move || writer.clone())
                .with_filter(filter),
        );
        let _guard = tracing::subscriber::set_default(subscriber);

        tracing::debug!("before the swap");
        handle.modify(|f| *f = tracing_subscriber::EnvFilter::new("debug"))?;
        tracing::debug!("after the swap");

        let logs = String::from_utf8(
            buf.0
                .lock()
                .map_err(|_| anyhow::anyhow!("poisoned"))?
                .clone(),
        )?;
        anyhow::ensure!(!logs.contains("before the swap"), "logs: {logs}");
        anyhow::ensure!(logs.contains("after the swap"), "logs: {logs}");
        Ok(())
    }
}
