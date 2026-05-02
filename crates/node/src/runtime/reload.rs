//! Hot-reload of mutable configuration fields on SIGHUP.
//!
//! Reloadable fields (applied in place; no restart required):
//!   - `payment.rate_per_mb`
//!   - `observability.log_level`
//!   - `cache.pinned_hashes`
//!   - all of `security.*` — the live `ConnectionLimiter` resizes its
//!     `Arc<Semaphore>` via `add_permits` / `acquire_many_owned(...)
//!     .forget()` (identity stable for in-flight permits) and updates
//!     its token-bucket maps in place under their per-map `Mutex`,
//!     preserving `last_refill` and accumulated `tokens`. `0` in any
//!     `security.*` field disables that layer.
//!
//! Every other field that changed in the file is logged and ignored
//! with a "requires restart" message — the runtime would otherwise need
//! to tear down the iroh endpoint, the metrics listener, the gossip
//! subscriptions, etc., which is far beyond the scope of a quick
//! reload.
//!
//! The reload entry point ([`RuntimeReloadState::reload`]) is also
//! called directly by tests, so its only side effects are mutating
//! values inside [`RuntimeReloadState`] and emitting `tracing` events.
//! It does not touch any global state.
//!
//! On parse error, previous values are retained: the reload is
//! best-effort and a malformed file must never crash a running node.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cli::common::LogLevel;
use crate::cli::run::{ObservabilityArgs, PaymentArgs};
use crate::config::{
    FileConfig, ResolvedObservability, ResolvedPayment, ResolvedSecurity, load_file_config,
    parse_pinned_hashes, resolve_observability, resolve_payment, resolve_security,
};
use crate::dispatch::ConnectionLimiter;

/// Read-only snapshot of the reloadable fields, returned by
/// [`RuntimeReloadState::current`]. Used by `admin_v1_reload` to report
/// what's running after a successful reload — operators get back the
/// values their RPC just applied without having to scrape logs.
///
/// `log_level` is `Option` for the same reason `current_log_level` is on
/// the parent state: the startup `EnvFilter` may have been built from
/// `RUST_LOG`, in which case there is no `LogLevel` to report until the
/// first reload commits one.
#[derive(Debug, Clone, Copy)]
pub struct ReloadSnapshot {
    pub rate_per_mb: u64,
    pub log_level: Option<crate::cli::common::LogLevel>,
}

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
/// [`RuntimeReloadState::reload`]; readers (e.g. the probe handler)
/// load `rate_per_mb` with `Ordering::Relaxed` because the atomic carries no
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
    /// Optional handle to the live cache engine. When present, SIGHUP
    /// reloads re-parse `cache.pinned_hashes` and atomically swap the
    /// engine's pinned set (#276). Held as `Option` so unit tests that
    /// exercise reload semantics without a real cache engine can pass
    /// `None` — the cache is initialised after `RuntimeReloadState::new`
    /// in the runtime startup sequence, then attached via
    /// [`Self::attach_cache`].
    cache: std::sync::Mutex<Option<decdn_cache::CacheEngine>>,
    /// Optional handle to the live connection limiter. When present,
    /// SIGHUP reloads forward the resolved `ResolvedSecurity` to
    /// [`ConnectionLimiter::reload`] so the live token-bucket maps and
    /// semaphore cap reflect the new file. Held as `Option` for the
    /// same reason as `cache` — the limiter is built after
    /// `RuntimeReloadState::new` in `runtime::run`, then attached via
    /// [`Self::attach_limiter`]. Tests exercising reload semantics
    /// without a real limiter pass `None` and the security commit
    /// becomes a parse-and-validate-only no-op.
    limiter: std::sync::Mutex<Option<Arc<ConnectionLimiter>>>,
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

/// JSON-serialised snapshots of every section we don't (fully) hot-reload.
/// Stored as `Option<serde_json::Value>` so "section absent" and "section
/// present but empty" diff distinctly. `None` everywhere on construction;
/// populated after the first successful reload.
///
/// `cache.*` is captured even though `cache.pinned_hashes` is reloadable —
/// `cache_changed_only_reloadable_fields` needs the structural baseline to
/// suppress the "requires restart" warning on a pin/unpin reload. `payment`
/// and `security` are fully reloadable and have no snapshot field: nothing
/// to diff against.
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
            cache: std::sync::Mutex::new(None),
            limiter: std::sync::Mutex::new(None),
            current_log_level: std::sync::Mutex::new(None),
            last_file_sections: std::sync::Mutex::new(FileSectionSnapshot::default()),
        }
    }

    /// Attach the live cache engine after it's been built. Must be called
    /// before the SIGHUP select loop runs — see `runtime::run`.
    /// Detaching is permitted (pass `None`) but production code never
    /// needs to: the engine outlives the reload state by construction.
    ///
    /// **Poison handling.** A poisoned mutex is recovered by replacing
    /// the inner value via `PoisonError::into_inner()`, but the poison
    /// flag is *not* cleared — `Mutex::lock()` will return `Err` again
    /// the next time anyone tries to take the guard. The first
    /// subsequent `reload()` will therefore fail-stop with `"cache
    /// attach mutex poisoned"`, retaining the previous values for
    /// every section. Recovery here ensures the new engine is at least
    /// stored for the (non-reload-driven) live path; it does not
    /// resurrect future reloads. Silently no-op'ing on the poison would
    /// have been worse — it would leave subsequent reloads applying
    /// `pinned_hashes` against a stale engine forever.
    pub fn attach_cache(&self, engine: Option<decdn_cache::CacheEngine>) {
        match self.cache.lock() {
            Ok(mut guard) => *guard = engine,
            Err(poisoned) => {
                tracing::error!(
                    "runtime reload cache mutex poisoned during attach; recovering inner state"
                );
                *poisoned.into_inner() = engine;
            }
        }
    }

    /// Attach the live `ConnectionLimiter` after it's been built. Same
    /// shape as [`Self::attach_cache`]: must be called before the SIGHUP
    /// select loop, supports `None` for tests, recovers from a poisoned
    /// mutex by replacing the inner state. The poison-handling story is
    /// the same as `attach_cache` — see that doc for the fail-stop
    /// guarantee on subsequent reloads.
    pub fn attach_limiter(&self, limiter: Option<Arc<ConnectionLimiter>>) {
        match self.limiter.lock() {
            Ok(mut guard) => *guard = limiter,
            Err(poisoned) => {
                tracing::error!(
                    "runtime reload limiter mutex poisoned during attach; recovering inner state"
                );
                *poisoned.into_inner() = limiter;
            }
        }
    }

    /// Seed the file-section snapshot from the config file loaded at
    /// startup. Without this, the very first SIGHUP after startup falls
    /// into the "no baseline → warn once" branch of the cache-section
    /// diff (`cache_changed_only_reloadable_fields`), which means an
    /// operator who only changed `cache.pinned_hashes` between startup
    /// and the first SIGHUP gets a misleading `cache.* (cache_dir,
    /// sizes, origin, decompress)` "requires restart" warning alongside
    /// the "config reload applied" success line.
    ///
    /// Idempotent. A poisoned mutex is recovered the same way
    /// [`Self::attach_cache`] handles its slot — silently no-op'ing on
    /// a poison would re-introduce the very UX bug this method exists
    /// to fix.
    pub fn seed_initial_file_snapshot(&self, file: &crate::config::FileConfig) {
        let snapshot = FileSectionSnapshot::capture(file);
        match self.last_file_sections.lock() {
            Ok(mut guard) => *guard = snapshot,
            Err(poisoned) => {
                tracing::error!(
                    "runtime reload snapshot mutex poisoned during seed; recovering inner state"
                );
                *poisoned.into_inner() = snapshot;
            }
        }
    }

    /// Shared atomic backing `payment.rate_per_mb`. Cloned into the probe
    /// handler at startup; subsequent reloads `store()` into this without
    /// rebuilding the handler.
    pub fn rate_per_mb(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.rate_per_mb)
    }

    /// Build a state for tests outside `runtime::reload::tests` that need
    /// to drive `reload()` end-to-end (e.g. `admin::tests` exercising
    /// `admin_v1_reload`). Centralised here rather than duplicated per
    /// test module so the boilerplate `ResolvedConfig` for which fields
    /// reload reads stays in one place — drift between two copies would
    /// give different test surfaces for the same code path.
    #[cfg(test)]
    pub(crate) fn for_test_with_setter(
        rate_per_mb: u64,
        level: crate::cli::common::LogLevel,
        log_level_setter: LogLevelSetter,
    ) -> Self {
        use std::path::PathBuf;

        use crate::config::{
            ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedGossip, ResolvedIdentity,
            ResolvedNetwork, ResolvedObservability, ResolvedPayment, ResolvedSecurity,
        };

        let cfg = ResolvedConfig {
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
                rpc_watchdog_interval_sec: 30,
            },
            cache: ResolvedCache {
                cache_dir: PathBuf::from("/tmp/cache"),
                cache_size_mb: 1024,
                max_blob_size_mb: 128,
                origin_url: None,
                origin_path: None,
                decompress: decdn_cache::DecompressMode::Auto,
                pinned_hashes: decdn_cache::PinnedHashes::empty(),
            },
            payment: ResolvedPayment { rate_per_mb },
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
            security: ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_node_rate_per_sec: 20.0,
                per_node_burst: 40,
                per_ip_rate_per_sec: 100.0,
                per_ip_burst: 200,
                max_tracked_sources: 4096,
            },
        };
        Self::new(
            crate::cli::run::PaymentArgs { rate_per_mb: None },
            crate::cli::run::ObservabilityArgs {
                log_level: None,
                log_format: None,
                metrics_port: None,
                metrics_bind: None,
                admin_port: None,
                otlp_endpoint: None,
            },
            &cfg,
            log_level_setter,
        )
    }

    /// Snapshot the post-reload values an operator wants to confirm: the
    /// current `rate_per_mb` and the most recently applied log level.
    ///
    /// `log_level` is `None` until the first successful reload — the
    /// startup `EnvFilter` may have been built from `RUST_LOG` rather
    /// than the resolved config (see [`Self::new`]'s docs), so reporting
    /// the resolved-at-startup level there would be misleading. After
    /// any successful reload the field tracks what the setter actually
    /// applied.
    ///
    /// A poisoned `current_log_level` mutex collapses to `None` rather
    /// than propagating: the caller is `admin_v1_reload`, which has
    /// already received an `Ok(())` from `reload()` (so the rate value
    /// is authoritative); the snapshot is best-effort metadata.
    pub fn current(&self) -> ReloadSnapshot {
        let log_level = self.current_log_level.lock().ok().and_then(|guard| *guard);
        ReloadSnapshot {
            rate_per_mb: self.rate_per_mb.load(Ordering::Relaxed),
            log_level,
        }
    }

    /// Re-read the config file at `path` and apply changes to reloadable
    /// fields. Triggered by SIGHUP (see `runtime::run`'s select loop). On
    /// parse error the previous values are retained and the error is
    /// logged; the caller (the SIGHUP arm of the select) discards the
    /// returned error so a malformed reload never propagates and stops
    /// the node.
    ///
    /// Ordering matters: every fallible step (file parse, sub-section
    /// resolution, both mutex locks) runs *before* any committing
    /// side-effect (log-level setter, atomic swap, snapshot write-back).
    /// A partial reload that swapped the live tracing filter and *then*
    /// hit a poisoned snapshot mutex would leave the running node in a
    /// state the operator never saw in the file. Lock both mutexes
    /// first, then commit in a single fall-through block where nothing
    /// else can fail.
    ///
    /// This is a method (not a free function) so the "atomic swap is
    /// the last committing step" rule lives next to the state it
    /// guards: callers can't accidentally reorder against external
    /// helpers, and the borrow checker tracks the `&self` lifetime
    /// through the whole transactional block.
    ///
    /// Fields outside the reloadable set are diffed against the
    /// previously seen file contents (snapshot stored on `self`) and a
    /// single info line is emitted only when those sections actually
    /// changed — operators see "you changed X but it needs a restart"
    /// without false positives on every routine SIGHUP.
    #[allow(
        clippy::cognitive_complexity, // Diff/log/apply for two fields stays linear.
        clippy::unused_async, // Future-shaped on purpose: see below.
        clippy::too_many_lines, // Linear "fallible work, then atomic commit" reads better as one unit than split apart.
    )]
    // `async` is preserved even though no body is currently `.await`ed:
    // the runtime select loop awaits this future inside `tokio::select!`
    // (see `runtime::run`), so the signature is part of the contract
    // with the caller. A future revision adding `tokio::fs::read_to_string`
    // for the config file would also need it.
    pub async fn reload(&self, path: &Path) -> anyhow::Result<()> {
        let file = match load_file_config(Some(path)) {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(%err, path = %path.display(), "config reload aborted (file load failed); previous values retained for every section");
                return Err(err);
            }
        };

        // Re-resolve only the reloadable sections, preserving the same
        // CLI > file > default precedence used at startup.
        let new_payment: ResolvedPayment = match resolve_payment(
            &self.payment_cli,
            file.payment.as_ref(),
        ) {
            Ok(p) => p,
            Err(err) => {
                tracing::warn!(%err, "config reload aborted at [payment]; entire reload rolled back (all-or-nothing): payment, observability, cache.pinned_hashes, and security all retained at their previous values");
                return Err(err);
            }
        };
        let new_observability: ResolvedObservability = match resolve_observability(
            &self.observability_cli,
            file.observability.as_ref(),
        ) {
            Ok(o) => o,
            Err(err) => {
                tracing::warn!(%err, "config reload aborted at [observability]; entire reload rolled back (all-or-nothing): payment, observability, cache.pinned_hashes, and security all retained at their previous values");
                return Err(err);
            }
        };

        // Re-parse `cache.pinned_hashes`. The vast majority of fields in
        // `cache.*` aren't reloadable (cache_dir, sizes, origin), but
        // pinned_hashes is — see #276. Parse here (fallible) so a
        // malformed entry rejects the whole reload before any side
        // effect runs, consistent with the "previous values retained on
        // error" contract.
        let new_pinned = match parse_pinned_hashes(
            file.cache.as_ref().and_then(|c| c.pinned_hashes.as_deref()),
        ) {
            Ok(p) => p,
            Err(err) => {
                tracing::warn!(
                    %err,
                    "config reload aborted at [cache.pinned_hashes]; entire reload rolled back (all-or-nothing): payment, observability, cache.pinned_hashes, and security all retained at their previous values"
                );
                return Err(err);
            }
        };

        // Re-resolve `[security]`. All fields are hot-reloadable; an
        // invalid value rejects the entire reload (all-or-nothing
        // semantics — operators in incident-response don't want a typo
        // here to silently leave half their reload applied).
        let new_security: ResolvedSecurity = match resolve_security(file.security.as_ref()) {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(
                    %err,
                    "config reload aborted at [security]; entire reload rolled back (all-or-nothing): payment, observability, cache.pinned_hashes, and security all retained at their previous values"
                );
                return Err(err);
            }
        };

        // Lock the snapshot mutex *and* the cache and limiter attach
        // slots before any committing side-effect. Holding all guards
        // across the rest of the function serialises concurrent reloads
        // (a second SIGHUP racing the first one waits here) and lets us
        // treat the whole "apply log level + swap rate + swap pinned
        // set + reload limiter + write back" path as one critical
        // section. Acquiring the locks here is the *last* fallible
        // step: a `PoisonError` on any of them must surface before the
        // setter, the atomic swap, the pinned-set swap, or the limiter
        // reload commit anything to live state.
        let mut current = self
            .current_log_level
            .lock()
            .map_err(|_| anyhow::anyhow!("log-level mutex poisoned"))?;
        let mut sections = self
            .last_file_sections
            .lock()
            .map_err(|_| anyhow::anyhow!("file-section snapshot mutex poisoned"))?;
        let cache_guard = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("cache attach mutex poisoned"))?;
        let limiter_guard = self
            .limiter
            .lock()
            .map_err(|_| anyhow::anyhow!("limiter attach mutex poisoned"))?;

        // Diff non-reloadable sections against the previous snapshot
        // before committing. Emitting "ignored" lines is read-only and
        // we want them out of the way before the commit step.
        log_ignored_fields(&file, &new_observability, &sections);

        let new_level = new_observability.log_level;
        // First reload (current is `None`) always applies, regardless
        // of whether the file value matches the resolved-at-startup
        // level — the live `EnvFilter` may be a `RUST_LOG` directive
        // we can't reflect back into a `LogLevel`, so we defer the
        // "is this a change?" judgement to the very first apply.
        let log_level_changed = match *current {
            None => true,
            Some(prev) => prev != new_level,
        };

        // Commit step. Order within the commit:
        //   1. log-level setter (only fallible commit; tracing filter swap)
        //   2. atomic swap of rate_per_mb (infallible)
        //   3. ArcSwap of cache.pinned set (infallible)
        //   4. ConnectionLimiter::reload (infallible: lock_recover
        //      handles poison; add_permits is infallible; the shrink
        //      path is `tokio::spawn` and surfaces only via panic)
        //   5. write-back of cached values via the held guards (infallible)
        // If the setter fails we bail before touching the rate atomic,
        // pinned set, the limiter, or the snapshot caches — preserving
        // the "previous values retained on error" contract. AtomicU64::
        // swap, ArcSwap, ConnectionLimiter::reload, and the guard writes
        // themselves cannot fail.
        if log_level_changed && let Err(err) = (self.log_level_setter)(new_level) {
            tracing::warn!(%err, ?new_level, "failed to apply new log level; previous level retained");
            return Err(err);
        }
        let prev_rate = self
            .rate_per_mb
            .swap(new_payment.rate_per_mb, Ordering::Relaxed);
        let pinned_count = new_pinned.len();
        // Swap the cache engine's pinned set if a cache is attached.
        // `cache_guard` was acquired up top with the rest of the
        // mutexes, so a poisoned mutex was already returned as an error
        // before any commit step. `None` here is the no-cache-attached
        // case (unit tests, very early startup) and a routine no-op.
        // The `PinDiff` lets the success log line distinguish "applied
        // (engine swapped)" from "parsed (no engine attached)" — log
        // scrapers and operators reasoning about pin/unpin events
        // shouldn't have to reverse-engineer the difference from a
        // trailing pinned-set count alone.
        let pin_diff = cache_guard
            .as_ref()
            .map(|engine| engine.set_pinned(&new_pinned));
        // Apply the new security snapshot to the live limiter if one
        // is attached. `None` matches `cache_guard.as_ref()` semantics
        // — early-startup and unit-test contexts run the parse-and-
        // validate path without touching a live limiter.
        let security_attached = limiter_guard.as_ref().is_some();
        if let Some(lim) = limiter_guard.as_ref() {
            lim.reload(&new_security);
        }
        *current = Some(new_level);
        *sections = FileSectionSnapshot::capture(&file);
        drop(current);
        drop(sections);
        drop(cache_guard);
        drop(limiter_guard);

        // `pinned_added` / `pinned_removed` are emitted only when a cache
        // is attached. When none is (early startup / unit tests) we
        // signal that with `pinned_skipped_no_cache_attached = true` so
        // log scrapers don't see a zero count and conclude "no pins
        // changed" — they did, the engine just wasn't there to apply
        // them.
        let pinned_skipped_no_cache_attached = pin_diff.is_none();
        tracing::info!(
            rate_per_mb = new_payment.rate_per_mb,
            prev_rate_per_mb = prev_rate,
            log_level = %new_level,
            log_level_changed,
            pinned_hashes = pinned_count,
            pinned_added = pin_diff.map(|d| d.added),
            pinned_removed = pin_diff.map(|d| d.removed),
            pinned_skipped_no_cache_attached,
            cache_attached = pin_diff.is_some(),
            security_attached,
            max_concurrent_handlers = new_security.max_concurrent_handlers,
            per_node_rate_per_sec = new_security.per_node_rate_per_sec,
            per_ip_rate_per_sec = new_security.per_ip_rate_per_sec,
            max_tracked_sources = new_security.max_tracked_sources,
            "config reload applied"
        );
        Ok(())
    }
}

/// Return true iff every cache field outside the reloadable set is
/// byte-identical between the new file and the previous snapshot — i.e.
/// the operator only changed reloadable fields (`pinned_hashes` today)
/// and no "requires restart" warning is needed.
///
/// Returning `false` means "I can't prove the operator only changed
/// reloadable fields", which the caller turns into the warning. The
/// failure modes (no baseline yet; serialisation failed) are
/// deliberately conservative — they emit a one-shot warning rather than
/// silently swallowing a real change.
fn cache_changed_only_reloadable_fields(
    file_cache: Option<&crate::config::types::CacheConfig>,
    prev_cache_json: Option<&serde_json::Value>,
) -> bool {
    let Some(file_cache) = file_cache else {
        return false;
    };
    let Some(prev) = prev_cache_json else {
        // First reload after startup with a populated cache section:
        // we have no baseline to compare. Conservative: warn once so
        // the operator sees the "requires restart" notice for any
        // non-reloadable change. Better than swallowing a real change
        // because we happened to lack a baseline.
        return false;
    };
    // Strip pinned_hashes from both sides before diffing. Easiest way is
    // to serialise both sides without that field. Since prev is a
    // serde_json::Value, we can clone-and-remove. For the new file we
    // serialise into Value first.
    let Some(mut curr_val) = snap_section("cache", Some(file_cache)) else {
        // Serialisation failed: we cannot prove the non-reloadable
        // fields are unchanged. Conservatively assume they changed and
        // emit the "requires restart" warning — silent suppression here
        // is exactly the failure mode the diff was meant to surface.
        return false;
    };
    let mut prev_val = prev.clone();
    if let Some(obj) = curr_val.as_object_mut() {
        obj.remove("pinned_hashes");
    }
    if let Some(obj) = prev_val.as_object_mut() {
        obj.remove("pinned_hashes");
    }
    curr_val == prev_val
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
        // Suppress the "ignored" notice when the only fields that
        // changed inside `cache.*` are reloadable ones (pinned_hashes
        // today). Otherwise an operator who pinned/unpinned a hash
        // would see a misleading "requires restart" warning right
        // alongside the "config reload applied" success line.
        if !cache_changed_only_reloadable_fields(file.cache.as_ref(), prev.cache.as_ref()) {
            warn_ignored("cache.* (cache_dir, sizes, origin, decompress)");
        }
    }
    if changed("gossip", file.gossip.as_ref(), prev.gossip.as_ref()) && file.gossip.is_some() {
        warn_ignored("gossip.* (announce_interval, peer_ttl, allowlist, subscribe_global)");
    }
    // `security.*` is fully reloadable — see `RuntimeReloadState::reload`'s
    // commit step. Invalid values reject the entire reload via
    // `resolve_security` upstream rather than landing here.
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
        ResolvedNetwork, ResolvedObservability, ResolvedPayment, ResolvedSecurity,
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
                rpc_watchdog_interval_sec: 30,
            },
            cache: ResolvedCache {
                cache_dir: PathBuf::from("/tmp/cache"),
                cache_size_mb: 1024,
                max_blob_size_mb: 128,
                origin_url: None,
                origin_path: None,
                decompress: decdn_cache::DecompressMode::Auto,
                pinned_hashes: decdn_cache::PinnedHashes::empty(),
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
            security: ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_node_rate_per_sec: 20.0,
                per_node_burst: 40,
                per_ip_rate_per_sec: 100.0,
                per_ip_burst: 200,
                max_tracked_sources: 4096,
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

        state.reload(&path).await.unwrap();

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
        state.reload(&path).await.unwrap();
        assert_eq!(*captured.lock().unwrap(), Some(LogLevel::Info));
        // Drop the captured value to detect a no-op on the second pass.
        *captured.lock().unwrap() = None;
        // Second reload sees the cached level and skips the setter.
        state.reload(&path).await.unwrap();
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 5);
        assert!(captured.lock().unwrap().is_none());
    }

    /// First-reload-applies guarantee: `current_log_level` initialises
    /// to `None`, so the first reload after startup always invokes the
    /// setter even when the file's `log_level` matches
    /// `initial.observability.log_level`. This is the
    /// `RUST_LOG=debug` + `config.log_level="info"` case: the live
    /// filter is `debug`, the resolved value is `info`, and a first
    /// SIGHUP must push `info` through to the subscriber.
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

        state.reload(&path).await.unwrap();
        assert_eq!(*captured.lock().unwrap(), Some(LogLevel::Info));
    }

    /// Fail-stop guarantee: when the setter errors, `rate_per_mb` must
    /// not move. The "previous values retained on error" contract
    /// requires the atomic store to be the last commit step. Inject a
    /// setter that always returns an error and assert the rate doesn't
    /// move.
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

        let err = state.reload(&path).await.unwrap_err();
        assert!(format!("{err:#}").contains("simulated tracing-reload failure"));
        // The atomic must NOT have been swapped — that's the whole point
        // of putting the store after the fallible work.
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 42);
    }

    /// Transactional contract for the *log-level* mutex: a poisoned
    /// `current_log_level` must surface as an error from the reload
    /// function *before* the rate atomic is swapped, so the previous
    /// rate is retained.
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

        let err = st.reload(&path).await.unwrap_err();
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

        let err = st.reload(&path).await.unwrap_err();
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
        state.reload(&path).await.unwrap();
        *captured.lock().unwrap() = None;
        state.reload(&path).await.unwrap();
        state.reload(&path).await.unwrap();
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

        let err = state.reload(&path).await.unwrap_err();
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
        let err = state.reload(&missing).await.unwrap_err();
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

        state.reload(&path).await.unwrap();
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 50);
    }

    /// A malformed TOML body must reject the reload before any commit
    /// side-effect runs: the setter is never called, the rate atomic
    /// stays at its previous value, and the snapshot baseline (which
    /// other reloads diff against) is unchanged. Without this test the
    /// transactional guarantees only get exercised on the *resolution*
    /// failure paths, not on the parse failure path.
    #[tokio::test]
    async fn reload_returns_error_on_malformed_toml() {
        let dir = tempfile::tempdir().unwrap();
        // Unterminated section header + dangling assignment — guaranteed
        // to fail the TOML parser without depending on any specific
        // diagnostic message.
        let path = write_config(dir.path(), "[payment\nrate_per_mb = ");

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

        // Sample the snapshot before the reload to compare against the
        // post-reload value. `FileSectionSnapshot` derives `Clone` so
        // we can take a structural copy through the guard.
        let snapshot_before = state.last_file_sections.lock().unwrap().clone();

        let err = state.reload(&path).await.unwrap_err();
        // Don't bind the test to a specific TOML diagnostic; just check
        // the call failed.
        assert!(!format!("{err:#}").is_empty());

        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 42);
        assert!(captured.lock().unwrap().is_none());

        let snapshot_after = state.last_file_sections.lock().unwrap().clone();
        // Snapshots cmp by serde_json::Value equality — any drift across
        // a rejected parse would mean we mutated the diff baseline,
        // which is exactly what the test guards against.
        assert_eq!(
            format!("{snapshot_before:?}"),
            format!("{snapshot_after:?}"),
            "snapshot baseline must not move on parse-failed reload"
        );
    }

    // ----- pinned_hashes hot-reload (#276) -----

    /// Build a 64-char lowercase hex hash for tests.
    fn make_hex_hash(seed: u8) -> String {
        use std::fmt::Write as _;

        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            let i_u8 = u8::try_from(i).unwrap_or(0);
            *b = i_u8.wrapping_add(seed);
        }
        let mut s = String::with_capacity(64);
        for b in bytes {
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    /// Construct a real (filesystem-backed) cache engine in a temp dir
    /// for pinning-reload tests. Tests that don't need a full cache
    /// just leave the engine unattached.
    async fn build_test_cache() -> (decdn_cache::CacheEngine, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let engine = decdn_cache::CacheEngine::open(tmp.path(), None, 16)
            .await
            .unwrap();
        (engine, tmp)
    }

    #[tokio::test]
    async fn reload_swaps_pinned_hashes_on_attached_cache() {
        let dir = tempfile::tempdir().unwrap();
        let h1 = make_hex_hash(1);
        let h2 = make_hex_hash(2);
        let body = format!("[cache]\npinned_hashes = [\"{h1}\", \"{h2}\"]\n");
        let path = write_config(dir.path(), &body);

        let initial = seed_resolved(10, LogLevel::Info);
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
        let (cache, _tmp_cache) = build_test_cache().await;
        state.attach_cache(Some(cache.clone()));

        // Before reload: empty pinned set.
        assert_eq!(cache.pinned_snapshot().len(), 0);

        state.reload(&path).await.unwrap();

        // After reload: both hashes pinned.
        let pinned = cache.pinned_snapshot();
        assert_eq!(pinned.len(), 2, "expected both hashes pinned");
    }

    #[tokio::test]
    async fn reload_rejects_invalid_pinned_hash_and_keeps_previous_set() {
        let dir = tempfile::tempdir().unwrap();
        let h_good = make_hex_hash(3);
        // First reload: pin a valid hash.
        let path = write_config(
            dir.path(),
            &format!("[cache]\npinned_hashes = [\"{h_good}\"]\n"),
        );

        let initial = seed_resolved(10, LogLevel::Info);
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
        let (cache, _tmp_cache) = build_test_cache().await;
        state.attach_cache(Some(cache.clone()));
        state.reload(&path).await.unwrap();
        assert_eq!(cache.pinned_snapshot().len(), 1);

        // Second reload: invalid hash. Must reject and keep the previous set.
        let bad_path = write_config(dir.path(), "[cache]\npinned_hashes = [\"zzz-not-hex\"]\n");
        let err = state.reload(&bad_path).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("pinned_hashes") || format!("{err:#}").contains("64 hex"),
            "error should reference the invalid pinned hash: {err:#}"
        );
        // Previous pinned set retained.
        assert_eq!(
            cache.pinned_snapshot().len(),
            1,
            "previous pinned set must survive a rejected reload"
        );
    }

    #[tokio::test]
    async fn reload_without_attached_cache_is_noop_for_pinning() {
        // Confirms the reload path doesn't blow up when no cache has
        // been attached yet (early startup window) — `attach_cache(None)`
        // is the default, and parse_pinned_hashes still runs but the
        // ArcSwap never happens.
        let dir = tempfile::tempdir().unwrap();
        let h = make_hex_hash(9);
        let path = write_config(dir.path(), &format!("[cache]\npinned_hashes = [\"{h}\"]\n"));

        let initial = seed_resolved(10, LogLevel::Info);
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
        // Intentionally NOT calling attach_cache.

        // Reload should still succeed; pinned hashes are parsed (so a
        // malformed entry would still reject), they just don't land
        // anywhere.
        state.reload(&path).await.unwrap();
    }

    /// N → 0 transition: operator removes pinned hashes between
    /// reloads. Without this test, a regression where `set_pinned`
    /// short-circuits on empty input (e.g. `if new.is_empty() {
    /// return; }`) would slip through silently — pinned hashes would
    /// stay pinned forever, resisting eviction even after the operator
    /// took them off the list.
    #[tokio::test]
    async fn reload_unpins_when_pinned_hashes_emptied() {
        let dir = tempfile::tempdir().unwrap();
        let h = make_hex_hash(11);

        let initial = seed_resolved(10, LogLevel::Info);
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
        let (cache, _tmp_cache) = build_test_cache().await;
        state.attach_cache(Some(cache.clone()));

        // First reload pins one hash.
        let pin_path = write_config(dir.path(), &format!("[cache]\npinned_hashes = [\"{h}\"]\n"));
        state.reload(&pin_path).await.unwrap();
        assert_eq!(cache.pinned_snapshot().len(), 1);

        // Second reload presents an empty list — the engine's pinned
        // set must shrink to zero.
        let empty_path = write_config(dir.path(), "[cache]\npinned_hashes = []\n");
        state.reload(&empty_path).await.unwrap();
        assert!(
            cache.pinned_snapshot().is_empty(),
            "pinned set must be empty after operator removes all entries"
        );
    }

    /// Direct test for the poison-recovery branch in `attach_cache`.
    /// The existing `reload_keeps_*_when_*_mutex_poisoned` tests
    /// poison `current_log_level` and `last_file_sections`, but never
    /// the cache slot itself. This locks in the recovery path that
    /// commit `a148c02` introduced — silently no-op'ing on a poisoned
    /// cache mutex would turn every subsequent reload into a silent
    /// no-op for pinning.
    #[tokio::test]
    async fn attach_cache_recovers_from_poisoned_mutex() {
        let initial = seed_resolved(10, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = Arc::new(RuntimeReloadState::new(
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
        ));

        // Poison the cache mutex by panicking inside a held guard.
        let st_for_thread = Arc::clone(&state);
        let join = std::thread::spawn(move || {
            let _guard = st_for_thread.cache.lock().unwrap();
            panic!("intentional panic to poison cache mutex");
        });
        let _ = join.join();
        assert!(
            state.cache.is_poisoned(),
            "test setup: cache mutex should be poisoned"
        );

        // Recovery path: `attach_cache` must accept the new engine
        // despite the poison and a subsequent reload must actually
        // swap pinned hashes on it (proving recovery wasn't a silent
        // no-op).
        let (cache, _tmp_cache) = build_test_cache().await;
        state.attach_cache(Some(cache.clone()));

        // `attach_cache`'s `into_inner` recovery updates the slot but
        // leaves the mutex's poison flag set (we don't call
        // `clear_poison`). Subsequent `reload` calls will surface
        // "cache attach mutex poisoned" — that's the deliberate
        // fail-stop. The contract we lock in here is the narrower one:
        // the new engine *was* stored, not silently dropped.
        let stored = state
            .cache
            .lock()
            .map_or_else(|p| p.into_inner().is_some(), |g| g.is_some());
        assert!(stored, "attach_cache must store engine despite poison");
    }

    // ----- security hot-reload (#235) -----

    /// Build a `ConnectionLimiter` with the same defaults `seed_resolved`
    /// uses for `ResolvedSecurity`. Returned by `Arc` so tests can clone
    /// it into both `attach_limiter` and assertions about the live state.
    fn build_test_limiter() -> Arc<crate::dispatch::ConnectionLimiter> {
        use crate::config::ResolvedSecurity;
        use crate::dispatch::ConnectionLimiter;
        use crate::metrics::Metrics;
        let metrics = Arc::new(Metrics::new());
        Arc::new(ConnectionLimiter::new(
            &ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_node_rate_per_sec: 20.0,
                per_node_burst: 40,
                per_ip_rate_per_sec: 100.0,
                per_ip_burst: 200,
                max_tracked_sources: 4096,
            },
            metrics,
        ))
    }

    #[tokio::test]
    async fn reload_applies_security_when_limiter_attached() {
        // Tighten per-node burst from default 40 down to 1; after
        // reload the live limiter must reject the second per-node
        // acquire on a fresh node-id pair.
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[security]\n\
             per_node_rate_per_sec = 1.0\n\
             per_node_burst = 1\n\
             per_ip_rate_per_sec = 1000.0\n\
             per_ip_burst = 1000\n",
        );

        let initial = seed_resolved(10, LogLevel::Info);
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
        let limiter = build_test_limiter();
        state.attach_limiter(Some(Arc::clone(&limiter)));

        state.reload(&path).await.unwrap();

        // Per-node burst is now 1.
        let node = [7u8; 32];
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
        let _p1 = limiter
            .acquire_for_test(node, Some(ip))
            .expect("first per-node acquire");
        let err = limiter
            .acquire_for_test(
                node,
                Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2))),
            )
            .expect_err("second per-node acquire must reject after reload");
        assert_eq!(err, crate::dispatch::RejectReason::PerNodeId);
    }

    #[tokio::test]
    async fn reload_without_attached_limiter_is_noop_for_security() {
        // No limiter attached → reload still parses + validates security
        // but doesn't blow up. Equivalent of the cache "noop for pinning"
        // test that already exists.
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[security]\nper_node_burst = 5\nper_node_rate_per_sec = 1.0\n",
        );
        let initial = seed_resolved(10, LogLevel::Info);
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
        // Intentionally NOT calling attach_limiter.
        state.reload(&path).await.unwrap();
    }

    #[tokio::test]
    async fn reload_rejects_invalid_security_and_keeps_previous() {
        // Negative rate is invalid; reload must reject *and* the rate
        // atomic and log-level setter must NOT have moved (all-or-
        // nothing reload — invalid security blocks every other field
        // too).
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[payment]\n\
             rate_per_mb = 99\n\
             [observability]\n\
             log_level = \"debug\"\n\
             [security]\n\
             per_node_rate_per_sec = -1.0\n",
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
        state.attach_limiter(Some(build_test_limiter()));

        let err = state.reload(&path).await.unwrap_err();
        assert!(format!("{err:#}").contains("per_node_rate_per_sec"));
        // Payment rate must NOT have moved despite being valid in the
        // file — "previous values retained on error" applies to the
        // whole reload.
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 42);
        assert!(
            captured.lock().unwrap().is_none(),
            "log-level setter must not have run when security rejected"
        );
    }

    /// Setter-failure case extended with security: the log-level setter
    /// returning Err must rollback before the limiter reload runs.
    #[tokio::test]
    async fn reload_keeps_security_when_log_level_setter_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[observability]\nlog_level = \"debug\"\n\
             [security]\n\
             per_node_rate_per_sec = 1.0\n\
             per_node_burst = 1\n\
             per_ip_rate_per_sec = 1000.0\n\
             per_ip_burst = 1000\n",
        );
        let failing_setter: LogLevelSetter =
            Box::new(|_| Err(anyhow::anyhow!("simulated tracing-reload failure")));
        let initial = seed_resolved(10, LogLevel::Info);
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
        let limiter = build_test_limiter();
        state.attach_limiter(Some(Arc::clone(&limiter)));

        let err = state.reload(&path).await.unwrap_err();
        assert!(format!("{err:#}").contains("simulated tracing-reload failure"));

        // Limiter must NOT have been mutated — the per-node burst stays
        // at the seed_resolved default (40), so two acquires from the
        // same node still succeed.
        let node = [3u8; 32];
        let _p1 = limiter
            .acquire_for_test(
                node,
                Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1))),
            )
            .expect("setter failure must not have shrunk per-node burst");
        let _p2 = limiter
            .acquire_for_test(
                node,
                Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2))),
            )
            .expect("burst still 40 → second succeeds");
    }

    /// Mirror of `attach_cache_recovers_from_poisoned_mutex` for the
    /// new `limiter` slot — silent no-op on a poisoned mutex would turn
    /// every subsequent reload into a silent no-op for security.
    #[tokio::test]
    async fn attach_limiter_recovers_from_poisoned_mutex() {
        let initial = seed_resolved(10, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = Arc::new(RuntimeReloadState::new(
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
        ));

        let st_for_thread = Arc::clone(&state);
        let join = std::thread::spawn(move || {
            let _guard = st_for_thread.limiter.lock().unwrap();
            panic!("intentional panic to poison limiter mutex");
        });
        let _ = join.join();
        assert!(
            state.limiter.is_poisoned(),
            "test setup: limiter mutex should be poisoned"
        );

        let limiter = build_test_limiter();
        state.attach_limiter(Some(Arc::clone(&limiter)));

        let stored = state
            .limiter
            .lock()
            .map_or_else(|p| p.into_inner().is_some(), |g| g.is_some());
        assert!(stored, "attach_limiter must store engine despite poison");
    }

    /// Mirror of `reload_keeps_log_level_when_sections_mutex_poisoned`
    /// for the new `limiter` slot. Confirms the limiter-slot lock is
    /// acquired *before* any commit step.
    #[tokio::test]
    async fn reload_keeps_security_when_limiter_mutex_poisoned() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[payment]\nrate_per_mb = 99\n\
             [observability]\nlog_level = \"debug\"\n\
             [security]\nper_node_burst = 1\n",
        );

        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, captured) = recording_setter();
        let state = Arc::new(RuntimeReloadState::new(
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
        ));

        let st_for_thread = Arc::clone(&state);
        let join = std::thread::spawn(move || {
            let _guard = st_for_thread.limiter.lock().unwrap();
            panic!("intentional panic to poison limiter mutex");
        });
        let _ = join.join();
        assert!(state.limiter.is_poisoned());

        let err = state.reload(&path).await.unwrap_err();
        assert!(format!("{err:#}").contains("limiter attach mutex poisoned"));
        // No commit ran: log-level setter not called, rate atomic intact.
        assert!(captured.lock().unwrap().is_none());
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 42);
    }

    /// `seed_initial_file_snapshot` primes the diff baseline from the
    /// startup config, so the first SIGHUP after startup doesn't fall
    /// into the "no baseline → warn once" branch in
    /// `cache_changed_only_reloadable_fields`. The behaviour we can
    /// assert directly: after seeding, `last_file_sections` reflects
    /// the populated cache section instead of `Default::default()`.
    #[test]
    fn seed_initial_file_snapshot_primes_diff_baseline() {
        use crate::config::FileConfig;
        use crate::config::types::CacheConfig;

        let initial = seed_resolved(10, LogLevel::Info);
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

        // Default state: cache snapshot is `None`.
        let before = state.last_file_sections.lock().unwrap().clone();
        assert!(before.cache.is_none(), "baseline should start empty");

        let file = FileConfig {
            cache: Some(CacheConfig {
                cache_size_mb: Some(2048),
                ..CacheConfig::default()
            }),
            ..FileConfig::default()
        };
        state.seed_initial_file_snapshot(&file);

        let after = state.last_file_sections.lock().unwrap().clone();
        assert!(
            after.cache.is_some(),
            "seeded snapshot should populate cache section"
        );
    }
}
