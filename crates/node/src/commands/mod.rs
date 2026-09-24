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

use decdn_common::{cli, config, identity};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, MakeWriter, format};
use tracing_subscriber::registry::LookupSpan;

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

    // Before the key load: a bad endpoint fails the start before a first
    // start writes a key whose "generated" line would then never be logged.
    let tracer_provider = resolved
        .observability
        .otlp_endpoint
        .as_deref()
        .map(|endpoint| otlp::init_otlp_provider(endpoint, Arc::clone(&node_metrics)))
        .transpose()?;

    // Before `init_tracing`: the JSON formatter writes this key's `node_id`
    // on every event, the first one included. A load failure returns to
    // `main`, which prints it to stderr like a config error.
    let node_key = identity::load_or_create(&resolved.identity.data_dir)?;
    let node_id = node_key.key.public();

    let log_level_setter = init_tracing(
        filter,
        rust_log_pinned,
        resolved.observability.log_format,
        &node_id,
        tracer_provider.as_ref(),
    );

    // Replay what `resolve_config` recorded. It runs before `init_tracing`
    // (the fallback filter above is built from the resolved log level), so a
    // resolver cannot emit these itself — it hands them back and this is the
    // first point at which a subscriber exists to receive them.
    runtime::emit_config_notices(&notices);

    tracing::info!("deCDN node starting");
    log_node_key(&node_key, &resolved.identity.data_dir);
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

    // `Box::pin` the runtime future: its state machine crosses clippy's
    // `large_futures` threshold. It runs once per process, so the heap
    // allocation costs nothing that matters.
    let result = Box::pin(runtime::run(
        resolved,
        node_key.key,
        config_path.map(std::path::Path::to_path_buf),
        reload_state,
        node_metrics,
    ))
    .await;

    // On both exit paths: the spans around a failed run are the ones an
    // operator most wants exported.
    if let Some(provider) = tracer_provider {
        otlp::finish_run(provider, &result).await;
    }

    result
}

/// Log the node id from [`identity::load_or_create`], and the key path when
/// that call generated the key. The load runs before the subscriber exists,
/// so it cannot log for itself.
fn log_node_key(node_key: &identity::LoadedKey, data_dir: &std::path::Path) {
    if node_key.generated {
        tracing::info!(
            path = %identity::key_path(data_dir).display(),
            "generated new node secret key"
        );
    }
    tracing::info!(node_id = %node_key.key.public(), "loaded node identity");
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

/// Initialize the tracing subscriber with a fmt layer for `log_format`, plus
/// an OTLP span export layer through `provider` when export is on. In the
/// JSON log format every event carries `node_id` (see [`NodeIdJson`]).
///
/// Returns a [`runtime::LogLevelSetter`] closure that swaps the live
/// `EnvFilter` to one matching a new `LogLevel` — used by the SIGHUP
/// hot-reload path (#236). When `rust_log_pinned` is set, the filter came from
/// `RUST_LOG` and the closure leaves it in place (see [`reload_directive`]).
/// The closure captures a `reload::Handle` to the fmt layer's per-layer
/// `EnvFilter` (see [`subscriber`]); calls to `modify` must respect any errors
/// from the handle (e.g. the registry was dropped) by surfacing them.
fn init_tracing(
    filter: tracing_subscriber::EnvFilter,
    rust_log_pinned: bool,
    log_format: cli::LogFormat,
    node_id: &iroh::PublicKey,
    provider: Option<&SdkTracerProvider>,
) -> runtime::LogLevelSetter {
    use tracing_subscriber::prelude::*;

    let fmt_layer = fmt_layer(log_format, node_id, std::io::stdout);

    // Wrap the EnvFilter in a `reload::Layer` so the SIGHUP reload path
    // can swap it without rebuilding the rest of the subscriber stack.
    let (reload_filter, reload_handle) = tracing_subscriber::reload::Layer::new(filter);

    subscriber(fmt_layer, reload_filter, provider).init();

    Box::new(move |lvl| {
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
    })
}

/// The fmt layer for `format`, writing to `writer`. The JSON format carries
/// `node_id` on every event; the pretty format is for a terminal and does not.
fn fmt_layer<W>(
    format: cli::LogFormat,
    node_id: &iroh::PublicKey,
    writer: W,
) -> Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync>
where
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    use tracing_subscriber::prelude::*;

    let layer = tracing_subscriber::fmt::layer().with_writer(writer);
    match format {
        cli::LogFormat::Json => layer
            .json()
            .map_event_format(|inner| NodeIdJson::new(inner, node_id))
            .boxed(),
        cli::LogFormat::Pretty => layer.boxed(),
    }
}

/// A JSON event formatter that writes `node_id` as the first top-level key of
/// every event, beside `timestamp`, `level`, `target` and `fields`.
///
/// Logs reach the aggregator with host labels only. This key joins a log line
/// to the iroh `NodeId` that peers and the chain see.
///
/// The inner formatter must keep `fields` nested. `flatten_event(true)` lifts
/// event fields to the top level, so an event with its own `node_id` field
/// would then write the key twice.
struct NodeIdJson<F> {
    inner: F,
    /// The opening of every event: `{"node_id":<JSON string>,`.
    prefix: String,
}

impl<F> NodeIdJson<F> {
    fn new(inner: F, node_id: &iroh::PublicKey) -> Self {
        let id = serde_json::Value::from(node_id.to_string());
        Self {
            inner,
            prefix: format!("{{\"node_id\":{id},"),
        }
    }
}

impl<S, N, F> FormatEvent<S, N> for NodeIdJson<F>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let mut line = String::new();
        self.inner
            .format_event(ctx, format::Writer::new(&mut line), event)?;
        // The inner formatter writes an unflattened `fields` object on every
        // event, so the object after `{` is never empty and the prefix's
        // trailing comma is valid.
        let rest = line.strip_prefix('{').ok_or(std::fmt::Error)?;
        writer.write_str(&self.prefix)?;
        writer.write_str(rest)
    }
}

/// The fmt layer's log filter behind a reload handle.
type LogFilter =
    tracing_subscriber::reload::Layer<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>;

/// The node's subscriber stack: `fmt_layer` filtered by `log_filter` alone,
/// plus span export through `provider` when OTLP is on.
///
/// The log filter is a per-layer filter on the fmt layer, never a global one:
/// span export has its own fixed filter (`otlp::otel_layer`), so lowering the
/// log level never drops traces.
fn subscriber(
    fmt_layer: Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync>,
    log_filter: LogFilter,
    provider: Option<&SdkTracerProvider>,
) -> impl tracing::Subscriber + Send + Sync + for<'span> tracing_subscriber::registry::LookupSpan<'span>
{
    use tracing_subscriber::prelude::*;

    tracing_subscriber::registry()
        .with(fmt_layer.with_filter(log_filter))
        .with(provider.map(otlp::otel_layer))
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

    /// A shared in-memory log sink.
    #[derive(Clone, Default)]
    struct Buf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

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

    impl Buf {
        fn text(&self) -> anyhow::Result<String> {
            let bytes = self
                .0
                .lock()
                .map_err(|_| anyhow::anyhow!("poisoned"))?
                .clone();
            Ok(String::from_utf8(bytes)?)
        }
    }

    /// The reload handle still swaps the level in the stack `init_tracing`
    /// installs, where the `EnvFilter` is a per-layer filter on the fmt layer:
    /// a debug event is dropped at `info` and written after the swap to
    /// `debug`.
    #[test]
    fn per_layer_reload_filter_applies_a_new_level() -> anyhow::Result<()> {
        let buf = Buf::default();
        let writer = buf.clone();
        let (filter, handle) =
            tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::new("info"));
        let node_id = iroh::SecretKey::generate().public();
        let subscriber = subscriber(
            fmt_layer(cli::LogFormat::Pretty, &node_id, move || writer.clone()),
            filter,
            None,
        );
        let _guard = tracing::subscriber::set_default(subscriber);

        tracing::debug!("before the swap");
        handle.modify(|f| *f = tracing_subscriber::EnvFilter::new("debug"))?;
        tracing::debug!("after the swap");

        let logs = buf.text()?;
        anyhow::ensure!(!logs.contains("before the swap"), "logs: {logs}");
        anyhow::ensure!(logs.contains("after the swap"), "logs: {logs}");
        // The pretty format is plain text for a terminal: no JSON, no node id.
        anyhow::ensure!(!logs.starts_with('{'), "logs: {logs}");
        anyhow::ensure!(!logs.contains("node_id"), "logs: {logs}");
        Ok(())
    }

    /// Every JSON event, inside a span or not, is one valid JSON object that
    /// carries the ADR appendix-observability mandatory top-level keys,
    /// `node_id` among them. An event field named `node_id` stays under
    /// `fields` and does not replace the node's own.
    #[test]
    fn json_events_carry_the_mandatory_top_level_fields() -> anyhow::Result<()> {
        let buf = Buf::default();
        let writer = buf.clone();
        let (filter, _handle) =
            tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::new("info"));
        let node_id = iroh::SecretKey::generate().public();
        let subscriber = subscriber(
            fmt_layer(cli::LogFormat::Json, &node_id, move || writer.clone()),
            filter,
            None,
        );
        let _guard = tracing::subscriber::set_default(subscriber);

        tracing::info!(hash = "ab", "outside a span");
        tracing::info_span!("serve_stream", peer = "cd").in_scope(|| {
            tracing::warn!("inside a span");
        });
        tracing::info!(node_id = "other", "with a node_id field");

        let logs = buf.text()?;
        let events = logs
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        anyhow::ensure!(events.len() == 3, "logs: {logs}");
        let want_id = node_id.to_string();
        anyhow::ensure!(
            want_id.len() == 64
                && want_id
                    .chars()
                    .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
            "node_id is not 64 lowercase hex digits: {want_id}"
        );
        let str_at = |event: &serde_json::Value, pointer: &str| {
            event
                .pointer(pointer)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        };
        let messages = ["outside a span", "inside a span", "with a node_id field"];
        for (event, message) in events.iter().zip(messages) {
            anyhow::ensure!(
                str_at(event, "/node_id") == Some(want_id.clone()),
                "event: {event}"
            );
            for key in ["/timestamp", "/level", "/target"] {
                anyhow::ensure!(str_at(event, key).is_some(), "no {key}: {event}");
            }
            anyhow::ensure!(
                str_at(event, "/fields/message").as_deref() == Some(message),
                "event: {event}"
            );
        }
        anyhow::ensure!(
            events
                .get(1)
                .and_then(|e| str_at(e, "/span/name"))
                .as_deref()
                == Some("serve_stream"),
            "logs: {logs}"
        );
        anyhow::ensure!(
            events
                .get(2)
                .and_then(|e| str_at(e, "/fields/node_id"))
                .as_deref()
                == Some("other"),
            "logs: {logs}"
        );
        Ok(())
    }
}
