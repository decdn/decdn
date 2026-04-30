//! Hot-reload of mutable configuration fields on SIGHUP (#236).
//!
//! Only `payment.rate_per_mb` and `observability.log_level` are reloadable
//! today. Every other field that changed in the file is logged and ignored
//! with a "requires restart" message — the runtime would otherwise need to
//! tear down the iroh endpoint, the metrics listener, the gossip
//! subscriptions, etc., which is far beyond the scope of a quick reload.
//!
//! The reload entry point ([`reload_runtime_config`]) is also called
//! directly by tests, so its only side effects are mutating values
//! inside [`RuntimeReloadState`] and emitting `tracing` events. It
//! does not touch any global state.
//!
//! On parse error, previous values are retained: the reload is
//! best-effort and a malformed file must never crash a running node.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cli::common::LogLevel;
use crate::cli::run::{ObservabilityArgs, PaymentArgs};
use crate::config::{
    ResolvedObservability, ResolvedPayment, load_file_config, resolve_observability,
    resolve_payment,
};

/// Closure that swaps the live `EnvFilter` to one matching `level`.
///
/// Boxed so the runtime can hold it without naming the (large, layered)
/// concrete subscriber type that `tracing_subscriber::reload::Handle` is
/// generic over. Returning `anyhow::Result` lets the closure surface
/// filter-parse failures from the new directive string.
pub type LogLevelSetter = Box<dyn Fn(LogLevel) -> anyhow::Result<()> + Send + Sync + 'static>;

/// Shared, mutable handles for the fields the runtime can hot-reload.
///
/// Held inside an `Arc` so the SIGHUP handler and the `ProbeHandler` can
/// both observe updates. Values are updated in place by
/// [`reload_runtime_config`]; readers (e.g. the probe handler) load
/// `rate_per_mb` with `Ordering::Relaxed` because it's a single-word
/// monotonic-ish counter without ordering requirements relative to other
/// state.
pub struct RuntimeReloadState {
    /// CLI overrides as parsed at startup. CLI > file > default precedence
    /// is preserved across reloads — a CLI flag set once at launch keeps
    /// winning until the process restarts.
    payment_cli: PaymentArgs,
    /// CLI overrides for observability fields.
    observability_cli: ObservabilityArgs,
    /// Current `payment.rate_per_mb`. Probe handler reads this on every
    /// request via [`Self::rate_per_mb`].
    rate_per_mb: Arc<AtomicU64>,
    /// Closure to apply a new log-level directive to the running tracing
    /// subscriber.
    log_level_setter: LogLevelSetter,
    /// Cached log level that was last applied — used to suppress redundant
    /// `EnvFilter` rebuilds when the file hasn't actually changed it.
    current_log_level: std::sync::Mutex<LogLevel>,
}

impl std::fmt::Debug for RuntimeReloadState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeReloadState")
            .field("rate_per_mb", &self.rate_per_mb.load(Ordering::Relaxed))
            .field("current_log_level", &self.current_log_level)
            .finish_non_exhaustive()
    }
}

impl RuntimeReloadState {
    /// Build a reload state from the resolved-at-startup values plus the
    /// closure that mutates the live tracing filter.
    pub fn new(
        payment_cli: PaymentArgs,
        observability_cli: ObservabilityArgs,
        initial: &crate::config::ResolvedConfig,
        log_level_setter: LogLevelSetter,
    ) -> Self {
        Self {
            payment_cli,
            observability_cli,
            rate_per_mb: Arc::new(AtomicU64::new(initial.payment.rate_per_mb)),
            log_level_setter,
            current_log_level: std::sync::Mutex::new(initial.observability.log_level),
        }
    }

    /// Shared atomic backing `payment.rate_per_mb`. Cloned into the probe
    /// handler at startup; subsequent reloads `store()` into this without
    /// rebuilding the handler.
    pub fn rate_per_mb(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.rate_per_mb)
    }
}

/// Re-read the config file and apply changes to reloadable fields.
///
/// Triggered by SIGHUP (see `runtime::run`'s select loop). On parse error
/// the previous values are retained and the error is logged; the caller
/// (the SIGHUP arm of the select) discards the returned error so a
/// malformed reload never propagates and stops the node.
///
/// Fields outside the reloadable set are diffed against the freshly
/// resolved values and a single info line is emitted naming each ignored
/// change — operators see "you changed X but it needs a restart" without
/// silent drops.
#[allow(clippy::cognitive_complexity)] // Diff/log/apply for two fields stays linear.
pub async fn reload_runtime_config(path: &Path, state: &RuntimeReloadState) -> anyhow::Result<()> {
    let file = match load_file_config(Some(path)) {
        Ok(f) => f,
        Err(err) => {
            tracing::warn!(%err, path = %path.display(), "config reload failed; previous values retained");
            return Err(err);
        }
    };

    // Re-resolve only the reloadable sections, preserving the same
    // CLI > file > default precedence used at startup.
    let new_payment: ResolvedPayment =
        match resolve_payment(&state.payment_cli, file.payment.as_ref()) {
            Ok(p) => p,
            Err(err) => {
                tracing::warn!(%err, "config reload rejected (payment); previous values retained");
                return Err(err);
            }
        };
    let new_observability: ResolvedObservability = match resolve_observability(
        &state.observability_cli,
        file.observability.as_ref(),
    ) {
        Ok(o) => o,
        Err(err) => {
            tracing::warn!(%err, "config reload rejected (observability); previous values retained");
            return Err(err);
        }
    };

    // Apply rate_per_mb. `Relaxed` is fine: probe handlers don't need to
    // see this update synchronised with any other state, only eventually.
    let prev_rate = state
        .rate_per_mb
        .swap(new_payment.rate_per_mb, Ordering::Relaxed);

    // Apply log level if it changed. Skipping the no-op rebuild keeps
    // reload cheap when an operator is just pruning unrelated keys.
    let new_level = new_observability.log_level;
    let mut current = state
        .current_log_level
        .lock()
        .map_err(|e| anyhow::anyhow!("log-level mutex poisoned: {e}"))?;
    let log_level_changed = *current != new_level;
    if log_level_changed && let Err(err) = (state.log_level_setter)(new_level) {
        tracing::warn!(%err, ?new_level, "failed to apply new log level; previous level retained");
        return Err(err);
    }
    *current = new_level;
    drop(current);

    log_ignored_fields(&file, &new_observability);

    tracing::info!(
        rate_per_mb = new_payment.rate_per_mb,
        prev_rate_per_mb = prev_rate,
        log_level = %new_level,
        log_level_changed,
        "config reload applied"
    );
    Ok(())
}

/// Emit a single info line per ignored-but-changed field.
fn warn_ignored(field: &'static str) {
    tracing::info!(
        field,
        "config reload: ignoring change to {field} (requires restart)"
    );
}

/// Log a notice for every reloaded section's non-reloadable field that the
/// operator changed. We don't track previous values here — knowing *that*
/// a change is ignored is enough for the operator to plan a restart, and
/// avoids carrying a per-field snapshot through the runtime.
fn log_ignored_fields(file: &crate::config::FileConfig, new_obs: &ResolvedObservability) {
    log_ignored_observability(file.observability.as_ref(), new_obs);
    log_ignored_other_sections(file);
}

/// Observability sub-fields outside the reloadable set. Diffed against the
/// freshly resolved values (not against the previous resolved values),
/// because what an operator cares about on reload is "did the file change
/// from a value we don't honour to one we'd need a restart for".
fn log_ignored_observability(
    obs: Option<&crate::config::types::ObservabilityConfig>,
    new_obs: &ResolvedObservability,
) {
    let Some(obs) = obs else {
        return;
    };
    if obs.log_format.is_some() && obs.log_format != Some(new_obs.log_format) {
        warn_ignored("observability.log_format");
    }
    if obs.metrics_port.is_some() && obs.metrics_port != Some(new_obs.metrics_port) {
        warn_ignored("observability.metrics_port");
    }
    if obs.metrics_bind.is_some() && obs.metrics_bind != Some(new_obs.metrics_bind) {
        warn_ignored("observability.metrics_bind");
    }
    if obs.otlp_endpoint.is_some() && obs.otlp_endpoint != new_obs.otlp_endpoint {
        warn_ignored("observability.otlp_endpoint");
    }
    if obs.admin_port.is_some() {
        // admin_port=0 means disabled; resolved value is None then.
        // Any divergence between file value and resolved value would
        // require a restart to re-bind, so flag the change.
        let resolved_admin = new_obs.admin_port.unwrap_or(0);
        if obs.admin_port != Some(resolved_admin) {
            warn_ignored("observability.admin_port");
        }
    }
}

/// identity / network / blockchain / cache / gossip: any presence of
/// the section in the new file is reported as ignored. We don't diff
/// against the original file because we don't keep it; the worst-case
/// false positive is one extra info line.
fn log_ignored_other_sections(file: &crate::config::FileConfig) {
    if file.identity.is_some() {
        warn_ignored("identity.* (data_dir, region)");
    }
    if file.network.is_some() {
        warn_ignored("network.* (bind_port, relay_url)");
    }
    if file.blockchain.is_some() {
        warn_ignored("blockchain.* (rpc_url, eth_keystore, contract addresses)");
    }
    if file.cache.is_some() {
        warn_ignored("cache.* (cache_dir, sizes, origin)");
    }
    if file.gossip.is_some() {
        warn_ignored("gossip.* (announce_interval, peer_ttl, allowlist, subscribe_global)");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex;

    use super::*;
    use crate::cli::common::LogLevel;
    use crate::config::{
        ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedGossip, ResolvedIdentity,
        ResolvedNetwork, ResolvedObservability, ResolvedPayment,
    };

    /// Build a no-op log-level setter that records the most recent level.
    fn recording_setter() -> (LogLevelSetter, Arc<Mutex<Option<LogLevel>>>) {
        let last = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&last);
        let setter: LogLevelSetter = Box::new(move |lvl| {
            *captured.lock().unwrap() = Some(lvl);
            Ok(())
        });
        (setter, last)
    }

    /// Minimal `ResolvedConfig` for seeding the reload state. Only the
    /// fields the reload path reads are populated meaningfully.
    fn seed_resolved(rate: u64, level: LogLevel) -> ResolvedConfig {
        ResolvedConfig {
            identity: ResolvedIdentity {
                data_dir: PathBuf::from("/tmp/decdn-test"),
                region: None,
            },
            network: ResolvedNetwork {
                bind_port: 4433,
                relay_url: None,
            },
            blockchain: ResolvedBlockchain {
                rpc_url: "http://localhost:8545".into(),
                eth_keystore: PathBuf::from("/tmp/keystore.json"),
                payment_channel_address: "0x0000000000000000000000000000000000000001".into(),
                staking_registry_address: "0x0000000000000000000000000000000000000002".into(),
            },
            cache: ResolvedCache {
                cache_dir: PathBuf::from("/tmp/cache"),
                cache_size_mb: 1024,
                max_blob_size_mb: 128,
                origin_url: None,
                origin_path: None,
            },
            payment: ResolvedPayment { rate_per_mb: rate },
            observability: ResolvedObservability {
                log_level: level,
                log_format: crate::cli::common::LogFormat::Pretty,
                metrics_port: 9090,
                metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                admin_port: Some(9191),
                otlp_endpoint: None,
            },
            gossip: ResolvedGossip {
                announce_interval_sec: 60,
                peer_ttl_sec: 600,
                subscribe_global: false,
                allowlist: Vec::new(),
            },
        }
    }

    fn write_config(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("node.toml");
        std::fs::write(&path, body).unwrap();
        path
    }

    #[tokio::test]
    async fn reload_applies_rate_and_log_level() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[payment]\nrate_per_mb = 99\n\n[observability]\nlog_level = \"debug\"\n",
        );

        let initial = seed_resolved(10, LogLevel::Info);
        let (setter, captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs { rate_per_mb: None },
            ObservabilityArgs {
                log_level: None,
                log_format: None,
                metrics_port: None,
                metrics_bind: None,
                admin_port: None,
                otlp_endpoint: None,
            },
            &initial,
            setter,
        );
        let shared_rate = state.rate_per_mb();

        reload_runtime_config(&path, &state).await.unwrap();

        assert_eq!(shared_rate.load(Ordering::Relaxed), 99);
        assert_eq!(*captured.lock().unwrap(), Some(LogLevel::Debug));
    }

    #[tokio::test]
    async fn reload_skips_log_level_when_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[payment]\nrate_per_mb = 5\n\n[observability]\nlog_level = \"info\"\n",
        );

        let initial = seed_resolved(10, LogLevel::Info);
        let (setter, captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs { rate_per_mb: None },
            ObservabilityArgs {
                log_level: None,
                log_format: None,
                metrics_port: None,
                metrics_bind: None,
                admin_port: None,
                otlp_endpoint: None,
            },
            &initial,
            setter,
        );

        reload_runtime_config(&path, &state).await.unwrap();
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 5);
        // Setter never called when the level matched.
        assert!(captured.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn reload_rejects_invalid_rate_and_keeps_previous() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(dir.path(), "[payment]\nrate_per_mb = 0\n");

        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs { rate_per_mb: None },
            ObservabilityArgs {
                log_level: None,
                log_format: None,
                metrics_port: None,
                metrics_bind: None,
                admin_port: None,
                otlp_endpoint: None,
            },
            &initial,
            setter,
        );

        let err = reload_runtime_config(&path, &state).await.unwrap_err();
        assert!(format!("{err:#}").contains("rate_per_mb"));
        // Previous value retained on rejection.
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 42);
    }

    #[tokio::test]
    async fn reload_returns_error_on_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist.toml");

        let initial = seed_resolved(7, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs { rate_per_mb: None },
            ObservabilityArgs {
                log_level: None,
                log_format: None,
                metrics_port: None,
                metrics_bind: None,
                admin_port: None,
                otlp_endpoint: None,
            },
            &initial,
            setter,
        );
        let err = reload_runtime_config(&missing, &state).await.unwrap_err();
        assert!(format!("{err:#}").contains("failed to read config file"));
        // No changes applied.
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 7);
    }

    #[tokio::test]
    async fn reload_preserves_cli_override() {
        // CLI sets rate_per_mb=50; file says 99; resolution must keep 50.
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(dir.path(), "[payment]\nrate_per_mb = 99\n");

        let initial = seed_resolved(50, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs {
                rate_per_mb: Some(50),
            },
            ObservabilityArgs {
                log_level: None,
                log_format: None,
                metrics_port: None,
                metrics_bind: None,
                admin_port: None,
                otlp_endpoint: None,
            },
            &initial,
            setter,
        );

        reload_runtime_config(&path, &state).await.unwrap();
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 50);
    }
}
