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
    FileConfig, ResolvedObservability, ResolvedPayment, load_file_config, resolve_observability,
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
/// `rate_per_mb` with `Ordering::Relaxed` because the atomic carries no
/// happens-before obligation to other state — it's a standalone config
/// knob (the rate may move up *or* down across a SIGHUP), and readers
/// tolerate seeing either generation across the swap.
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
    /// Cached log level that was last applied. `None` until the first
    /// successful reload — this forces the first SIGHUP to apply the file
    /// value unconditionally, since the live `EnvFilter` at startup may
    /// have been built from `RUST_LOG` rather than the resolved config
    /// (see `commands::run`). Tracking "what we last applied" rather than
    /// "what the resolved config said at startup" is what the setter
    /// actually controls.
    current_log_level: std::sync::Mutex<Option<LogLevel>>,
    /// Per-section snapshot of the *previously seen* file contents,
    /// captured as `serde_json::Value` for cheap structural diffing.
    /// Only sections covered by [`log_ignored_other_sections`] are
    /// tracked; we use them to suppress the noisy "ignored (requires
    /// restart)" line when the operator hasn't actually changed anything
    /// in those sections between reloads. Updated only after a fully
    /// successful reload so a rejected file doesn't poison future diffs.
    last_file_sections: std::sync::Mutex<FileSectionSnapshot>,
}

/// JSON-serialised snapshots of every section we don't hot-reload. Stored
/// as `Option<serde_json::Value>` so "section absent" and "section present
/// but empty" diff distinctly. `None` everywhere on construction; populated
/// after the first successful reload.
#[derive(Debug, Default, Clone)]
struct FileSectionSnapshot {
    identity: Option<serde_json::Value>,
    network: Option<serde_json::Value>,
    blockchain: Option<serde_json::Value>,
    cache: Option<serde_json::Value>,
    gossip: Option<serde_json::Value>,
    observability: Option<serde_json::Value>,
}

impl FileSectionSnapshot {
    /// Capture a snapshot from a freshly parsed `FileConfig`. Serialisation
    /// failures collapse to `None` (treated as "section absent"), with a
    /// `debug!` line per failure so an operator chasing phantom
    /// "ignored (requires restart)" warnings has a thread to pull. The
    /// tracking is best-effort noise reduction, not a correctness gate.
    fn capture(file: &FileConfig) -> Self {
        Self {
            identity: snap_section("identity", file.identity.as_ref()),
            network: snap_section("network", file.network.as_ref()),
            blockchain: snap_section("blockchain", file.blockchain.as_ref()),
            cache: snap_section("cache", file.cache.as_ref()),
            gossip: snap_section("gossip", file.gossip.as_ref()),
            observability: snap_section("observability", file.observability.as_ref()),
        }
    }
}

/// Serialise a config section to `serde_json::Value`, logging serialisation
/// failures at `debug!` instead of swallowing them silently. A poisoned
/// baseline would cause subsequent unchanged reloads to spuriously emit
/// "ignored (requires restart)" warnings forever — exactly the noise the
/// diff was meant to suppress — so we want the failure visible to anyone
/// who turns up the log level.
fn snap_section<T: serde::Serialize>(
    section: &'static str,
    value: Option<&T>,
) -> Option<serde_json::Value> {
    value.and_then(|v| {
        serde_json::to_value(v)
            .map_err(|err| {
                tracing::debug!(
                    section,
                    %err,
                    "config diff snapshot serialisation failed; treating as unchanged"
                );
                err
            })
            .ok()
    })
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
    ///
    /// `current_log_level` is intentionally seeded to `None` rather than
    /// to `initial.observability.log_level`: at startup the live
    /// `EnvFilter` may have been built from `RUST_LOG` (see
    /// `commands::run`'s `try_from_default_env`), in which case the
    /// resolved config value is *not* what's running. Forcing the first
    /// reload to apply unconditionally is simpler and more correct than
    /// trying to reflect the env-filter directive back into a `LogLevel`.
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
            current_log_level: std::sync::Mutex::new(None),
            last_file_sections: std::sync::Mutex::new(FileSectionSnapshot::default()),
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
/// Ordering matters: every fallible step (file parse, sub-section
/// resolution, both mutex locks) runs *before* any committing
/// side-effect (log-level setter, atomic swap, snapshot write-back). A
/// partial reload that swapped the live tracing filter and *then* hit a
/// poisoned snapshot mutex would leave the running node in a state the
/// operator never saw in the file. Lock both mutexes first, then commit
/// in a single fall-through block where nothing else can fail.
///
/// Fields outside the reloadable set are diffed against the previously
/// seen file contents (snapshot stored on `state`) and a single info
/// line is emitted only when those sections actually changed — operators
/// see "you changed X but it needs a restart" without false positives on
/// every routine SIGHUP.
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

    // Lock both caches *before* any committing side-effect. Holding both
    // guards across the rest of the function serialises concurrent
    // reloads (a second SIGHUP racing the first one waits here) and lets
    // us treat the whole "apply log level + swap rate + write back" path
    // as one critical section. Acquiring the locks here is the *last*
    // fallible step: PoisonError must surface before the setter or the
    // atomic swap commit anything to live state.
    let mut current = state
        .current_log_level
        .lock()
        .map_err(|_| anyhow::anyhow!("log-level mutex poisoned"))?;
    let mut sections = state
        .last_file_sections
        .lock()
        .map_err(|_| anyhow::anyhow!("file-section snapshot mutex poisoned"))?;

    // Diff non-reloadable sections against the previous snapshot before
    // committing. Emitting "ignored" lines is read-only and we want them
    // out of the way before the commit step.
    log_ignored_fields(&file, &new_observability, &sections);

    let new_level = new_observability.log_level;
    // First reload (current is `None`) always applies, regardless of
    // whether the file value matches the resolved-at-startup level — the
    // live `EnvFilter` may be a `RUST_LOG` directive we can't reflect
    // back into a `LogLevel`, so we defer the "is this a change?"
    // judgement to the very first apply.
    let log_level_changed = match *current {
        None => true,
        Some(prev) => prev != new_level,
    };

    // Commit step. Order within the commit:
    //   1. log-level setter (only fallible commit; tracing filter swap)
    //   2. atomic swap of rate_per_mb (infallible)
    //   3. write-back of cached values via the held guards (infallible)
    // If the setter fails we bail before touching the rate atomic or the
    // snapshot caches, preserving the "previous values retained on
    // error" contract. AtomicU64::swap and the guard writes themselves
    // cannot fail.
    if log_level_changed && let Err(err) = (state.log_level_setter)(new_level) {
        tracing::warn!(%err, ?new_level, "failed to apply new log level; previous level retained");
        return Err(err);
    }
    let prev_rate = state
        .rate_per_mb
        .swap(new_payment.rate_per_mb, Ordering::Relaxed);
    *current = Some(new_level);
    *sections = FileSectionSnapshot::capture(&file);
    drop(current);
    drop(sections);

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

/// Log a notice for every non-reloadable field the operator changed
/// *since the last successful reload*. The previous-file snapshot lives
/// on `state.last_file_sections` so we can diff structurally rather than
/// emitting a warning every time a section is merely present.
fn log_ignored_fields(
    file: &crate::config::FileConfig,
    new_obs: &ResolvedObservability,
    prev: &FileSectionSnapshot,
) {
    log_ignored_observability(
        file.observability.as_ref(),
        new_obs,
        prev.observability.as_ref(),
    );
    log_ignored_other_sections(file, prev);
}

/// Observability sub-fields outside the reloadable set. Compared
/// field-by-field against the freshly resolved values: the operator
/// cares whether the *current file* contains a non-honoured value that
/// disagrees with what's running, not whether any value is set at all.
fn log_ignored_observability(
    obs: Option<&crate::config::types::ObservabilityConfig>,
    new_obs: &ResolvedObservability,
    prev_obs_json: Option<&serde_json::Value>,
) {
    let Some(obs) = obs else {
        return;
    };
    // If the previous snapshot's observability section is byte-identical
    // to the current one, nothing in the section changed — short-circuit
    // before the per-field diffs to keep the common no-change reload
    // silent.
    if let Some(prev) = prev_obs_json
        && let Some(curr) = snap_section("observability", Some(obs))
        && *prev == curr
    {
        return;
    }
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
    // `admin_port`: file value None vs Some(0) vs Some(N) are three
    // distinct cases. The resolved value is `None` only when the file
    // explicitly set 0 (disabled). Compare the `Option`s directly rather
    // than collapsing both into a sentinel `0`.
    if let Some(file_admin) = obs.admin_port {
        let resolved_matches = match new_obs.admin_port {
            Some(p) => p == file_admin,
            // resolved=None means disabled; only matches file=Some(0).
            None => file_admin == 0,
        };
        if !resolved_matches {
            warn_ignored("observability.admin_port");
        }
    }
}

/// identity / network / blockchain / cache / gossip: warn only when the
/// section's TOML serialisation differs from the previous successful
/// reload. The first reload (snapshot empty) treats any present section
/// as a change so the operator still gets the "ignored" notice once;
/// thereafter we stay silent unless the section actually moved.
fn log_ignored_other_sections(file: &crate::config::FileConfig, prev: &FileSectionSnapshot) {
    /// Compare a freshly parsed section to its baseline snapshot. A
    /// serialisation failure is treated as a change ("can't prove it
    /// didn't move, so warn"); the underlying error is logged at
    /// `debug!` via [`snap_section`] so the noise is traceable.
    fn changed<T: serde::Serialize>(
        section: &'static str,
        curr: Option<&T>,
        prev: Option<&serde_json::Value>,
    ) -> bool {
        match (curr, prev) {
            (None, None) => false,
            (Some(c), Some(p)) => snap_section(section, Some(c)).is_none_or(|c| &c != p),
            // Section appearing or disappearing counts as a change.
            (Some(_), None) | (None, Some(_)) => true,
        }
    }
    if changed("identity", file.identity.as_ref(), prev.identity.as_ref())
        && file.identity.is_some()
    {
        warn_ignored("identity.* (data_dir, region)");
    }
    if changed("network", file.network.as_ref(), prev.network.as_ref()) && file.network.is_some() {
        warn_ignored("network.* (bind_port, relay_url)");
    }
    if changed(
        "blockchain",
        file.blockchain.as_ref(),
        prev.blockchain.as_ref(),
    ) && file.blockchain.is_some()
    {
        warn_ignored("blockchain.* (rpc_url, eth_keystore, contract addresses)");
    }
    if changed("cache", file.cache.as_ref(), prev.cache.as_ref()) && file.cache.is_some() {
        warn_ignored("cache.* (cache_dir, sizes, origin)");
    }
    if changed("gossip", file.gossip.as_ref(), prev.gossip.as_ref()) && file.gossip.is_some() {
        warn_ignored("gossip.* (announce_interval, peer_ttl, allowlist, subscribe_global)");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
        // First reload always applies (cache starts as `None` to handle
        // a startup `RUST_LOG` override). The skip-when-unchanged
        // behaviour is observable on the *second* reload, when the
        // cached level matches the file.
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

        // First reload applies (forces apply on `None` cache).
        reload_runtime_config(&path, &state).await.unwrap();
        assert_eq!(*captured.lock().unwrap(), Some(LogLevel::Info));
        // Drop the captured value to detect a no-op on the second pass.
        *captured.lock().unwrap() = None;
        // Second reload sees the cached level and skips the setter.
        reload_runtime_config(&path, &state).await.unwrap();
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 5);
        assert!(captured.lock().unwrap().is_none());
    }

    /// Bug 3: `current_log_level` initialises to `None`, so the first
    /// reload after startup always invokes the setter even when the
    /// file's `log_level` matches `initial.observability.log_level`.
    /// This is the `RUST_LOG=debug` + `config.log_level="info"` case:
    /// the live filter is `debug`, the resolved value is `info`, and a
    /// first SIGHUP must push `info` through to the subscriber.
    #[tokio::test]
    async fn first_reload_applies_log_level_even_when_matching_initial() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[payment]\nrate_per_mb = 1\n\n[observability]\nlog_level = \"info\"\n",
        );

        // Resolved-at-startup level is also `info` — old code would
        // think "nothing changed" and skip. New code must still apply.
        let initial = seed_resolved(1, LogLevel::Info);
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
        assert_eq!(*captured.lock().unwrap(), Some(LogLevel::Info));
    }

    /// Bug 1: when the log-level setter fails, `rate_per_mb` must stay
    /// at its previous value — the function's "previous values retained
    /// on error" contract requires the atomic store to be the last
    /// commit step. Inject a setter that always returns an error and
    /// assert the rate doesn't move.
    #[tokio::test]
    async fn reload_keeps_rate_when_log_level_setter_fails() {
        let dir = tempfile::tempdir().unwrap();
        // File asks for rate=88 and a log level that differs from the
        // cached value (None, i.e. force-apply path) so the setter is
        // actually called and gets the chance to fail.
        let path = write_config(
            dir.path(),
            "[payment]\nrate_per_mb = 88\n\n[observability]\nlog_level = \"debug\"\n",
        );

        let failing_setter: LogLevelSetter =
            Box::new(|_| Err(anyhow::anyhow!("simulated tracing-reload failure")));

        let initial = seed_resolved(42, LogLevel::Info);
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
            failing_setter,
        );

        let err = reload_runtime_config(&path, &state).await.unwrap_err();
        assert!(format!("{err:#}").contains("simulated tracing-reload failure"));
        // The atomic must NOT have been swapped — that's the whole point
        // of putting the store after the fallible work.
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 42);
    }

    /// Bug 1 (variant): a poisoned `current_log_level` mutex must
    /// surface as an error from the reload function *before* the
    /// rate atomic is swapped, so the previous rate is retained.
    #[tokio::test]
    async fn reload_keeps_rate_when_log_level_mutex_poisoned() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(dir.path(), "[payment]\nrate_per_mb = 77\n");

        let initial = seed_resolved(33, LogLevel::Info);
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

        // Poison the log-level mutex by holding the lock in a thread
        // that panics. A normal `JoinHandle` lets us discard the panic
        // payload — `std::thread::scope` would rethrow on join and
        // abort the test before the assertion runs.
        let st = Arc::new(state);
        let st_for_thread = Arc::clone(&st);
        let join = std::thread::spawn(move || {
            let _guard = st_for_thread.current_log_level.lock().unwrap();
            panic!("intentional panic to poison mutex");
        });
        let _ = join.join(); // discard the panic payload
        assert!(st.current_log_level.is_poisoned());

        let err = reload_runtime_config(&path, &st).await.unwrap_err();
        assert!(format!("{err:#}").contains("log-level mutex poisoned"));
        // Rate atomic must not have moved.
        assert_eq!(st.rate_per_mb().load(Ordering::Relaxed), 33);
    }

    /// Transactional contract for the *snapshot* mutex: if
    /// `last_file_sections` is poisoned the reload must surface an
    /// error before the live tracing filter is mutated and before the
    /// rate atomic is swapped. The setter committing while a later
    /// fallible step (snapshot lock) blew up was the original bug —
    /// keep that path in the regression suite.
    #[tokio::test]
    async fn reload_keeps_log_level_when_sections_mutex_poisoned() {
        let dir = tempfile::tempdir().unwrap();
        // File asks for a log-level change *and* a rate change so the
        // setter would be exercised if we reached the commit step.
        let path = write_config(
            dir.path(),
            "[payment]\nrate_per_mb = 99\n\n[observability]\nlog_level = \"debug\"\n",
        );

        let initial = seed_resolved(42, LogLevel::Info);
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

        // Same poisoning pattern as the log-level variant: hold the
        // guard on a thread that panics, swallow the join payload to
        // keep the test running.
        let st = Arc::new(state);
        let st_for_thread = Arc::clone(&st);
        let join = std::thread::spawn(move || {
            let _guard = st_for_thread.last_file_sections.lock().unwrap();
            panic!("intentional panic to poison snapshot mutex");
        });
        let _ = join.join();
        assert!(st.last_file_sections.is_poisoned());

        let err = reload_runtime_config(&path, &st).await.unwrap_err();
        assert!(format!("{err:#}").contains("file-section snapshot mutex poisoned"));
        // The setter must NOT have been called — that's the whole point
        // of locking both mutexes before any committing side-effect.
        assert!(captured.lock().unwrap().is_none());
        // Rate atomic must not have moved either.
        assert_eq!(st.rate_per_mb().load(Ordering::Relaxed), 42);
    }

    /// Diff-based ignored-section logging: a second reload of the same
    /// file must succeed and not touch the rate (already at target),
    /// proving the snapshot is being captured and is queryable. We
    /// can't directly capture `tracing` lines without a subscriber
    /// fixture, but the public-state behaviour the operator cares
    /// about is "reload remains idempotent across repeated SIGHUPs".
    #[tokio::test]
    async fn reload_is_idempotent_across_repeated_sighups() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            concat!(
                "[identity]\nregion = \"US\"\n\n",
                "[network]\nbind_port = 4433\n\n",
                "[payment]\nrate_per_mb = 11\n\n",
                "[observability]\nlog_level = \"info\"\n",
            ),
        );

        let initial = seed_resolved(11, LogLevel::Info);
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

        // Three reloads of the same file: first applies, the next two
        // are no-ops on log level.
        reload_runtime_config(&path, &state).await.unwrap();
        *captured.lock().unwrap() = None;
        reload_runtime_config(&path, &state).await.unwrap();
        reload_runtime_config(&path, &state).await.unwrap();
        assert!(captured.lock().unwrap().is_none());
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 11);
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
