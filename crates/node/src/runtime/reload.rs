//! Hot-reload of mutable configuration fields on SIGHUP.
//!
//! Reloadable fields (applied in place; no restart required):
//!   - `observability.log_level`
//!   - `cache.pinned_hashes`
//!   - all of `security.*` — the live `ConnectionLimiter` swaps its
//!     `Arc<Semaphore>` wholesale on reload (already-held permits drain
//!     into the previous semaphore on drop; new acquires hit the new
//!     one) and rebuilds its keyed [`governor`] rate limiter from the
//!     new quota, swapping it in under an `RwLock`. Token-bucket state
//!     is *not* preserved across the rebuild. `0` in any `security.*`
//!     field disables that layer.
//!   - the `[content]` denylist (`content.denied_hashes`,
//!     `content.denied_origins`) — hot reload here is load-bearing, not a
//!     convenience: ADR 011 sizes this mechanism to the one-hour removal
//!     clock, so a takedown must never wait on a daemon restart.
//!   - all of `[load_shed]` — the live `LoadShedController` swaps its
//!     policy in place.
//!
//! This list is the authority; `NodeCommand::Reload`'s help text in
//! `decdn-common` repeats it for operators, so extend both together.
//!
//! Any non-reloadable field the file carries gets a "requires restart"
//! message (logged on presence, not on change) — the runtime would
//! otherwise need to tear down the iroh endpoint, the metrics listener,
//! etc., which is far beyond the scope of a quick reload.
//!
//! The reload entry point ([`RuntimeReloadState::reload`]) is also
//! called directly by tests, so its only side effects are mutating
//! values inside [`RuntimeReloadState`] and emitting `tracing` events.
//! It does not touch any global state.
//!
//! On parse error, previous values are retained: the reload is
//! best-effort and a malformed file must never crash a running node.
//!
//! ## Section trait
//!
//! Each reloadable knob is a `ReloadableSection` impl. The trait
//! drives a fixed three-phase iteration in [`RuntimeReloadState::reload`]:
//!
//! 1. **Resolve every section.** A single [`ConfigErrorBag`] is threaded
//!    through every section's `resolve`, so an operator who broke
//!    multiple fields sees them all in one error rather than fixing them
//!    one SIGHUP at a time, and collapsed once at the end; any problem
//!    → return early with previous values retained for *every* section
//!    (the all-or-nothing contract). Resolved values
//!    are stashed in a per-section buffer cell
//!    (`Mutex<Option<Self::Resolved>>`) so the trait stays `dyn`-safe
//!    despite each section having its own `Resolved` type; every section
//!    populates its buffer on every resolve (a sentinel placeholder if
//!    it pushed to the bag). The early return skips the swap phase, and
//!    the next reload's `clear_buffer` evicts any leftover sentinel.
//! 2. **Run every `fallible_commit`.** The log-level filter swap is the
//!    only currently-fallible commit. This phase is the rollback
//!    boundary: commits already applied stay applied; a later failure
//!    aborts the rest. (The old monolithic body had the same property,
//!    documented inline; the trait makes it explicit.)
//! 3. **Run every `infallible_swap`.** Atomic stores, `ArcSwap` swaps,
//!    `ConnectionLimiter::reload`, and the per-section "applied"
//!    tracing event live here. None can fail.
//!
//! The buffer cell is emptied at the start of every `reload()`, filled
//! by `resolve`, read by `fallible_commit`, and drained by
//! `infallible_swap`. A panic between `resolve` and `infallible_swap`
//! would leave a buffer populated; the next `reload()`'s clear step
//! evicts it before anything else runs.

use std::path::Path;
use std::sync::Arc;

use crate::dispatch::ConnectionLimiter;
use anyhow::Context;
use decdn_common::cli::common::LogLevel;
use decdn_common::cli::run::ObservabilityArgs;
use decdn_common::config::{
    ConfigErrorBag, FileConfig, ResolvedLoadShed, ResolvedObservability, ResolvedSecurity,
    load_file_config, parse_pinned_hashes, resolve_load_shed_into, resolve_observability_into,
    resolve_security_into,
};

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
    pub log_level: Option<decdn_common::cli::common::LogLevel>,
}

/// Closure that swaps the live `EnvFilter` to one matching `level`.
///
/// Boxed so the runtime can hold it without naming the (large, layered)
/// concrete subscriber type that `tracing_subscriber::reload::Handle` is
/// generic over. Returning `anyhow::Result` lets the closure surface
/// filter-parse failures from the new directive string.
pub type LogLevelSetter = Box<dyn Fn(LogLevel) -> anyhow::Result<()> + Send + Sync + 'static>;

// ---------------------------------------------------------------------------
// Reloadable-section trait + section impls
// ---------------------------------------------------------------------------

/// One reloadable config section.
///
/// The three phases (`resolve` → `fallible_commit` → `infallible_swap`)
/// are driven in lockstep by [`RuntimeReloadState::reload`]: phase N runs
/// for *every* section before phase N+1 starts. That ordering is what
/// gives the all-or-nothing contract — a section that fails to resolve
/// aborts the reload before any other section's commit runs.
///
/// Each impl owns a `Mutex<Option<Resolved>>` buffer cell. `resolve`
/// fills it; `fallible_commit` reads it; `infallible_swap` drains it.
/// `clear_buffer` empties the cell at the start of every reload to
/// recover from a panic-mid-reload that left a stale value behind. The
/// associated `Resolved` type stays internal to each impl, so the
/// trait stays `dyn`-safe.
pub(crate) trait ReloadableSection: Send + Sync {
    /// Stable identifier for tracing event keys. Returning `&'static str`
    /// keeps it cheap and non-allocating in the hot path.
    fn name(&self) -> &'static str;

    /// Drop any value lingering in the buffer cell.
    ///
    /// Called once per `reload()` before phase 1 to keep the cell
    /// invariant ("populated only between `resolve` and `infallible_swap`")
    /// robust across previous panic-mid-reload paths. A poisoned mutex
    /// is recovered in place — losing the previous (now stale) value
    /// is the desired outcome.
    fn clear_buffer(&self);

    /// Phase 1: re-resolve from file, store the result in the section's
    /// buffer cell. Problems are pushed to the shared `bag` rather than
    /// returned as a `Result`, so every section's resolve runs before the
    /// aggregated bag is collapsed in `reload()`. Implementations must
    /// always populate the buffer (with a sentinel/default value if
    /// validation failed) — the buffer is only read downstream when the
    /// bag is empty, so a placeholder there is never observed by the
    /// commit/swap phases on the failure path.
    fn resolve(&self, file: &FileConfig, bag: &mut ConfigErrorBag);

    /// Phase 2: any commit step that can fail (e.g. swapping the live
    /// tracing filter). Default: no-op. The order of `fallible_commit`
    /// across sections is the same registration order as `resolve` and
    /// `infallible_swap`. **Rollback boundary:** once one section's
    /// `fallible_commit` returns `Ok`, that side-effect stays applied
    /// even if a later section's `fallible_commit` fails. This matches
    /// the old monolithic behaviour; the trait just makes it explicit.
    fn fallible_commit(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Phase 3: drain the buffer and apply the change to live state.
    /// Must not fail — atomic stores, `Arc` swaps, and `ConnectionLimiter::reload`
    /// are the only operations allowed here.
    fn infallible_swap(&self);
}

/// Set a cache-engine slot owned by a `ReloadableSection`, recovering
/// the inner value on a poisoned mutex. Used by [`RuntimeReloadState::attach_cache`]
/// to populate both the pinned-hashes and origin-retry sections from a
/// single attach call.
///
/// The poison flag is intentionally *not* cleared. Reachable poison
/// here implies a panic in some other code path that held the lock —
/// extremely rare since the lock's only callers are `attach_cache`
/// (this function) and the section's `infallible_swap`, both of which
/// are panic-free under workspace lints. The recovery write is
/// belt-and-braces: it ensures the engine reference is at least stored
/// (so e.g. the `pinned.engine` slot still receives the new engine if
/// it had been transiently poisoned), but the next `infallible_swap`
/// will see `lock()` return `Err` again and emit the section's
/// `tracing::error!` and skip its swap. This matches the codebase's
/// established "recover-the-data, log-loudly, do-not-clear-poison"
/// pattern (see also `attach_limiter` and the trait doc on
/// `ReloadableSection::infallible_swap`).
fn attach_engine_to_section(
    slot: &std::sync::Mutex<Option<decdn_cache::CacheEngine>>,
    engine: Option<decdn_cache::CacheEngine>,
    section: &'static str,
) {
    match slot.lock() {
        Ok(mut guard) => *guard = engine,
        Err(poisoned) => {
            tracing::error!(
                section,
                "runtime reload cache mutex poisoned during attach; recovering inner state"
            );
            *poisoned.into_inner() = engine;
        }
    }
}

/// Helper for the buffer-cell pattern. Each section keeps a
/// `Mutex<Option<Resolved>>`; this expression captures the "drain or log
/// and skip" idiom without each `infallible_swap` re-implementing it.
fn drain_or_log<T>(slot: &std::sync::Mutex<Option<T>>, section: &'static str) -> Option<T> {
    match slot.lock() {
        Ok(mut g) => g.take(),
        Err(poisoned) => {
            tracing::error!(
                section,
                "section buffer mutex poisoned during infallible_swap; \
                 swap skipped (previous value retained on the live path)"
            );
            // Drop the (possibly poisoned) value defensively.
            let _ = poisoned.into_inner().take();
            None
        }
    }
}

// ----- log_level ----------------------------------------------------------

/// Reloadable observability log level. The full `[observability]`
/// section isn't reloadable (`metrics_port`, `log_format`, etc. all
/// need a restart), but the level is. The other sub-fields get a
/// "requires restart" notice from `warn_restart_required_sections`.
struct LogLevelSection {
    cli: ObservabilityArgs,
    setter: LogLevelSetter,
    /// Cached log level last applied. `None` until the first successful
    /// reload — the live `EnvFilter` at startup may be a `RUST_LOG`
    /// directive we cannot reflect back into a `LogLevel`, so we force
    /// the first reload to apply unconditionally and only thereafter
    /// suppress no-op writes.
    current: std::sync::Mutex<Option<LogLevel>>,
    buf: std::sync::Mutex<Option<ResolvedObservability>>,
    /// Set by `fallible_commit` so `infallible_swap` knows whether the
    /// setter actually ran (we only update the cached `current` on a
    /// real apply, not on a skip — keeps "first reload always applies"
    /// honest).
    swap_applied: std::sync::Mutex<bool>,
}

impl ReloadableSection for LogLevelSection {
    fn name(&self) -> &'static str {
        "log_level"
    }
    fn clear_buffer(&self) {
        if let Ok(mut g) = self.buf.lock() {
            *g = None;
        }
        if let Ok(mut g) = self.swap_applied.lock() {
            *g = false;
        }
    }
    fn resolve(&self, file: &FileConfig, bag: &mut ConfigErrorBag) {
        let resolved = resolve_observability_into(&self.cli, file.observability.as_ref(), bag);
        if let Ok(mut g) = self.buf.lock() {
            *g = Some(resolved);
        }
    }
    fn fallible_commit(&self) -> anyhow::Result<()> {
        // Read (don't drain) the buffer — `infallible_swap` still needs
        // the value to update the cached `current` and emit the
        // tracing line.
        let new_level = {
            let g = self
                .buf
                .lock()
                .map_err(|_| anyhow::anyhow!("log-level buffer mutex poisoned"))?;
            match g.as_ref() {
                Some(r) => r.log_level,
                // `resolve` populates the buffer on success; if it's
                // empty here something upstream skipped the section,
                // which is a bug. Fail loud rather than silently no-op.
                None => return Err(anyhow::anyhow!("log-level buffer empty in fallible_commit")),
            }
        };
        // Lock the cached level for the duration of the change so a
        // racing reload can't observe a half-updated state.
        let mut current = self
            .current
            .lock()
            .map_err(|_| anyhow::anyhow!("log-level mutex poisoned"))?;
        // First reload (current is `None`) always applies — see the
        // struct's `current` doc.
        let log_level_changed = match *current {
            None => true,
            Some(prev) => prev != new_level,
        };
        if log_level_changed && let Err(err) = (self.setter)(new_level) {
            tracing::warn!(%err, ?new_level, "failed to apply new log level; previous level retained");
            return Err(err);
        }
        if log_level_changed {
            *current = Some(new_level);
            if let Ok(mut g) = self.swap_applied.lock() {
                *g = true;
            }
        }
        Ok(())
    }
    fn infallible_swap(&self) {
        let Some(resolved) = drain_or_log(&self.buf, self.name()) else {
            return;
        };
        let log_level_changed = self.swap_applied.lock().is_ok_and(|g| *g);
        tracing::info!(
            section = self.name(),
            log_level = %resolved.log_level,
            log_level_changed,
            "config reload section applied"
        );
    }
}

// ----- pinned_hashes ------------------------------------------------------

/// Reloadable `cache.pinned_hashes`. The rest of `cache.*`
/// (`cache_dir`, sizes, origin, `decompress`) is not hot-reloadable.
struct PinnedHashesSection {
    /// Optional handle to the live cache engine. `None` in unit tests
    /// that exercise reload semantics without a real engine; populated
    /// at runtime via [`RuntimeReloadState::attach_cache`] before the
    /// SIGHUP select loop runs.
    engine: std::sync::Mutex<Option<decdn_cache::CacheEngine>>,
    buf: std::sync::Mutex<Option<decdn_cache::PinnedHashes>>,
    /// `cache.cache_size_mb` at boot, for the reload-time pin-budget re-check.
    /// Restart-required, so the boot value stays authoritative.
    cache_size_mb: u64,
}

impl ReloadableSection for PinnedHashesSection {
    fn name(&self) -> &'static str {
        "pinned_hashes"
    }
    fn clear_buffer(&self) {
        if let Ok(mut g) = self.buf.lock() {
            *g = None;
        }
    }
    fn resolve(&self, file: &FileConfig, bag: &mut ConfigErrorBag) {
        // Same shape as `resolve_cache_into`: bag-push on parse failure,
        // empty placeholder so later sections still run. `reload()`'s
        // early return guarantees the placeholder never reaches swap.
        let resolved = bag
            .try_with(
                "cache.pinned_hashes",
                parse_pinned_hashes(file.cache.as_ref().and_then(|c| c.pinned_hashes.as_deref()))
                    .context("invalid cache.pinned_hashes"),
            )
            .unwrap_or_else(decdn_cache::PinnedHashes::empty);
        if let Ok(mut g) = self.buf.lock() {
            *g = Some(resolved);
        }
    }
    fn infallible_swap(&self) {
        let Some(resolved) = drain_or_log(&self.buf, self.name()) else {
            return;
        };
        let pinned_count = resolved.len();
        // The engine slot lock can be poisoned (see
        // `attach_cache`'s recovery path). On poison we forfeit the
        // swap rather than panic — operationally indistinguishable
        // from "cache not yet attached" and the same dedicated tracing
        // line covers it.
        let pin_diff = if let Ok(g) = self.engine.lock() {
            g.as_ref().map(|engine| {
                let diff = engine.set_pinned(&resolved);
                // Refresh the origin-held index so a newly-added pin (or a file
                // dropped into the fs origin) is announced on this reload rather
                // than only at the next periodic rescan (#1130). Detached so we
                // honor reload()'s no-await invariant; rescan is idempotent.
                let engine = engine.clone();
                let cache_size_mb = self.cache_size_mb;
                tokio::spawn(async move {
                    engine.rescan_origins().await;
                    // Re-check the pin budget here too, not just at boot: a reload
                    // is the one moment the pin set can grow past the cache ceiling
                    // on a running node.
                    super::warn_if_pins_exceed_cache(&engine, cache_size_mb);
                });
                diff
            })
        } else {
            tracing::error!(
                section = self.name(),
                "cache engine mutex poisoned in infallible_swap; pinning skipped"
            );
            None
        };
        let pinned_skipped_no_cache_attached = pin_diff.is_none();
        tracing::info!(
            section = self.name(),
            pinned_hashes = pinned_count,
            pinned_added = pin_diff.map(|d| d.added),
            pinned_removed = pin_diff.map(|d| d.removed),
            pinned_skipped_no_cache_attached,
            cache_attached = pin_diff.is_some(),
            "config reload section applied"
        );
    }
}

// ----- content ------------------------------------------------------------

/// Reloadable `[content]` section — the ADR 011 local denylist.
///
/// Hot-reloadable is the whole point, not a convenience: ADR 011 §One-hour
/// removal orders sizes this mechanism to the EU TCO one-hour clock, and a
/// restart-only denylist would put a daemon bounce (dropping every in-flight
/// paid stream) on the critical path of discharging a legal order.
///
/// Unlike the other sections there is no `attach_*` handle to be missing: the
/// deny-set is constructed before the handler is, and both hold the same `Arc`.
/// A swap is therefore always effective.
struct ContentSection {
    deny: Arc<crate::content_deny::ContentDenylist>,
    /// Live cache engine, for the hash half of the denylist. Same lifecycle as
    /// [`PinnedHashesSection::engine`] — attached via
    /// [`RuntimeReloadState::attach_cache`] before the reload loop runs.
    ///
    /// Hashes go to the engine rather than to `deny` because "will this node
    /// serve/announce/acquire this hash" has four consumers and ADR 011 needs
    /// one answer for all of them; see `CacheEngine::refuses`. `deny` keeps the
    /// origin half, which the cache knows nothing about.
    engine: std::sync::Mutex<Option<decdn_cache::CacheEngine>>,
    buf: std::sync::Mutex<Option<decdn_common::config::ResolvedContent>>,
}

impl ReloadableSection for ContentSection {
    fn name(&self) -> &'static str {
        "content"
    }
    fn clear_buffer(&self) {
        if let Ok(mut g) = self.buf.lock() {
            *g = None;
        }
    }
    fn resolve(&self, file: &FileConfig, bag: &mut ConfigErrorBag) {
        // Same shape as the other sections: bag-push on parse failure, empty
        // placeholder so later sections still run. `reload()`'s early return
        // guarantees the placeholder never reaches swap — which matters more
        // here than elsewhere, since swapping an empty placeholder would
        // silently UN-deny everything the operator had denied.
        let denied_hashes = bag
            .try_with(
                "content.denied_hashes",
                decdn_common::config::parse_denied_hashes(
                    file.content
                        .as_ref()
                        .and_then(|c| c.denied_hashes.as_deref()),
                )
                .context("invalid content.denied_hashes"),
            )
            .unwrap_or_default();
        let denied_origins = bag
            .try_with(
                "content.denied_origins",
                decdn_common::config::parse_denied_origins(
                    file.content
                        .as_ref()
                        .and_then(|c| c.denied_origins.as_deref()),
                )
                .context("invalid content.denied_origins"),
            )
            .unwrap_or_default();
        if let Ok(mut g) = self.buf.lock() {
            *g = Some(decdn_common::config::ResolvedContent {
                denied_hashes,
                denied_origins,
            });
        }
    }
    fn infallible_swap(&self) {
        let Some(resolved) = drain_or_log(&self.buf, self.name()) else {
            return;
        };
        let denied_origins = self.deny.set_local_origins(&resolved);
        let Ok(engine_guard) = self.engine.lock() else {
            // Same poison handling as `PinnedHashesSection`: forfeit the swap
            // rather than panic. Unlike pinning, a forfeited swap here leaves a
            // takedown undischarged, so it is ERROR and says so.
            tracing::error!(
                section = self.name(),
                "cache engine mutex poisoned; the denied-hash set was NOT updated and any \
                 takedown in this reload is undischarged"
            );
            return;
        };
        let hash_diff = engine_guard
            .as_ref()
            .map(|engine| engine.set_denied(&resolved.denied_hashes));
        tracing::info!(
            section = self.name(),
            denied_hashes = resolved.denied_hashes.len(),
            denied_hashes_added = hash_diff.map(|d| d.added),
            denied_hashes_removed = hash_diff.map(|d| d.removed),
            denied_hashes_skipped_no_cache_attached = hash_diff.is_none(),
            denied_origins,
            "config reload section applied"
        );
    }
}

// ----- security -----------------------------------------------------------

/// Reloadable `[security]` section.
struct SecuritySection {
    /// Optional handle to the live `ConnectionLimiter`. Same lifecycle
    /// rules as `PinnedHashesSection::engine` — populated via
    /// [`RuntimeReloadState::attach_limiter`] before the select loop.
    limiter: std::sync::Mutex<Option<Arc<ConnectionLimiter>>>,
    buf: std::sync::Mutex<Option<ResolvedSecurity>>,
}

impl ReloadableSection for SecuritySection {
    fn name(&self) -> &'static str {
        "security"
    }
    fn clear_buffer(&self) {
        if let Ok(mut g) = self.buf.lock() {
            *g = None;
        }
    }
    fn resolve(&self, file: &FileConfig, bag: &mut ConfigErrorBag) {
        let resolved = resolve_security_into(file.security.as_ref(), bag);
        if let Ok(mut g) = self.buf.lock() {
            *g = Some(resolved);
        }
    }
    fn infallible_swap(&self) {
        let Some(resolved) = drain_or_log(&self.buf, self.name()) else {
            return;
        };
        let security_attached = if let Ok(g) = self.limiter.lock() {
            if let Some(lim) = g.as_ref() {
                lim.reload(&resolved);
                true
            } else {
                false
            }
        } else {
            tracing::error!(
                section = self.name(),
                "limiter mutex poisoned in infallible_swap; security swap skipped"
            );
            false
        };
        tracing::info!(
            section = self.name(),
            security_attached,
            max_concurrent_handlers = resolved.max_concurrent_handlers,
            per_source_rate_per_sec = resolved.per_source_rate_per_sec,
            max_tracked_sources = resolved.max_tracked_sources,
            "config reload section applied"
        );
    }
}

// ----- load shed -----------------------------------------------------------

/// Reloadable `[load_shed]` section: resolves the config block into a
/// buffer, then swaps the live `LoadShedController`'s policy on
/// `infallible_swap`.
struct LoadShedSection {
    /// Optional handle to the live `LoadShedController`. Same lifecycle
    /// rules as `SecuritySection::limiter` — populated via
    /// [`RuntimeReloadState::attach_load_shed`] before the select loop.
    controller: std::sync::Mutex<Option<Arc<crate::load_shed::LoadShedController>>>,
    buf: std::sync::Mutex<Option<ResolvedLoadShed>>,
}

impl ReloadableSection for LoadShedSection {
    fn name(&self) -> &'static str {
        "load_shed"
    }
    fn clear_buffer(&self) {
        if let Ok(mut g) = self.buf.lock() {
            *g = None;
        }
    }
    fn resolve(&self, file: &FileConfig, bag: &mut ConfigErrorBag) {
        let resolved = resolve_load_shed_into(file.load_shed.as_ref(), bag);
        if let Ok(mut g) = self.buf.lock() {
            *g = Some(resolved);
        }
    }
    fn infallible_swap(&self) {
        let Some(resolved) = drain_or_log(&self.buf, self.name()) else {
            return;
        };
        let load_shed_attached = if let Ok(g) = self.controller.lock() {
            if let Some(ctrl) = g.as_ref() {
                ctrl.reload(&resolved);
                true
            } else {
                false
            }
        } else {
            tracing::error!(
                section = self.name(),
                "controller mutex poisoned in infallible_swap; load-shed swap skipped"
            );
            false
        };
        tracing::info!(
            section = self.name(),
            load_shed_attached,
            policy = ?resolved.policy,
            egress_budget_mbps = resolved.egress_budget_mbps,
            max_concurrent_serves_high = resolved.max_concurrent_serves_high,
            max_concurrent_serves_low = resolved.max_concurrent_serves_low,
            per_client_serve_cap = resolved.per_client_serve_cap,
            "config reload section applied"
        );
    }
}

// ---------------------------------------------------------------------------
// RuntimeReloadState
// ---------------------------------------------------------------------------

/// Shared, mutable handles for the fields the runtime can hot-reload.
///
/// Held inside an `Arc` so the SIGHUP handler and the `ProbeHandler` can
/// both observe updates. Each section is registered as both a concrete
/// `Arc<SectionStruct>` (so `attach_*` and accessors can reach into its
/// state) and as `Arc<dyn ReloadableSection>` (so `reload()` drives the
/// three-phase iteration without a central match arm per section).
///
/// Adding a new reloadable knob: implement `ReloadableSection` for a
/// new struct, `Arc` it once at construction, and append to the
/// `sections` vec — no central touch of `RuntimeReloadState::reload`.
pub struct RuntimeReloadState {
    log_level: Arc<LogLevelSection>,
    pinned: Arc<PinnedHashesSection>,
    security: Arc<SecuritySection>,
    content: Arc<ContentSection>,
    load_shed: Arc<LoadShedSection>,
    /// Iteration order for the three-phase reload: `log_level`,
    /// `pinned_hashes`, `security`, `content`, then `load_shed`. The order
    /// matters for reproducibility (operator-visible tracing event order)
    /// and for the rollback-boundary contract documented on the trait —
    /// moving `log_level` earlier or later would change which pre-`log_level`
    /// commits survive a setter failure.
    sections: Vec<Arc<dyn ReloadableSection>>,
    /// Serialises concurrent reloads. A SIGHUP racing an `admin_v1_reload`
    /// (both call [`Self::reload`]) waits here so the two-phase commit of
    /// one reload never interleaves with another's. Guards no data — the
    /// per-section buffer cells own the in-flight state — so a poisoned
    /// lock is recovered in place rather than surfaced: a prior
    /// panic-mid-reload must not wedge every future reload.
    reload_lock: std::sync::Mutex<()>,
}

impl std::fmt::Debug for RuntimeReloadState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeReloadState")
            .field("current_log_level", &self.log_level.current)
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
        observability_cli: ObservabilityArgs,
        initial: &decdn_common::config::ResolvedConfig,
        log_level_setter: LogLevelSetter,
    ) -> Self {
        let log_level = Arc::new(LogLevelSection {
            cli: observability_cli,
            setter: log_level_setter,
            current: std::sync::Mutex::new(None),
            buf: std::sync::Mutex::new(None),
            swap_applied: std::sync::Mutex::new(false),
        });
        let pinned = Arc::new(PinnedHashesSection {
            engine: std::sync::Mutex::new(None),
            buf: std::sync::Mutex::new(None),
            cache_size_mb: initial.cache.cache_size_mb,
        });
        let security = Arc::new(SecuritySection {
            limiter: std::sync::Mutex::new(None),
            buf: std::sync::Mutex::new(None),
        });
        let content = Arc::new(ContentSection {
            deny: Arc::new(crate::content_deny::ContentDenylist::new(&initial.content)),
            engine: std::sync::Mutex::new(None),
            buf: std::sync::Mutex::new(None),
        });
        let load_shed = Arc::new(LoadShedSection {
            controller: std::sync::Mutex::new(None),
            buf: std::sync::Mutex::new(None),
        });
        // Registration order sets the commit order: log_level,
        // pinned_hashes, security, content, load_shed. The order matters
        // for reproducibility (operator-visible tracing event order) and
        // for the rollback-boundary contract documented on the trait —
        // moving log_level later or earlier would change which
        // pre-log_level commits survive a setter failure.
        let sections: Vec<Arc<dyn ReloadableSection>> = vec![
            Arc::clone(&log_level) as _,
            Arc::clone(&pinned) as _,
            Arc::clone(&security) as _,
            Arc::clone(&content) as _,
            Arc::clone(&load_shed) as _,
        ];
        Self {
            log_level,
            pinned,
            security,
            content,
            load_shed,
            sections,
            reload_lock: std::sync::Mutex::new(()),
        }
    }

    /// Attach the live cache engine after it's been built. Must be called
    /// before the SIGHUP select loop runs — see `runtime::run`.
    /// Detaching is permitted (pass `None`) but the runtime never needs
    /// to: the engine outlives the reload state by construction.
    ///
    /// **Poison handling.** A poisoned mutex is recovered by replacing
    /// the inner value via `PoisonError::into_inner()`, but the poison
    /// flag is *not* cleared — `Mutex::lock()` will return `Err` again
    /// the next time anyone tries to take the guard. The first
    /// subsequent `reload()` will therefore skip the pinned-set swap
    /// for that section (see `PinnedHashesSection::infallible_swap`).
    /// Recovery here ensures the new engine is at least stored for the
    /// (non-reload-driven) live path.
    pub fn attach_cache(&self, engine: Option<decdn_cache::CacheEngine>) {
        attach_engine_to_section(&self.content.engine, engine.clone(), "content");
        attach_engine_to_section(&self.pinned.engine, engine, "pinned_hashes");
    }

    /// The live ADR 011 deny-set, for wiring into the client handler.
    ///
    /// Deliberately a getter rather than an `attach_*`: the deny-set is owned
    /// here and shared out, so there is no window in which the handler holds a
    /// deny-set the reload path cannot reach. The `attach_*` pattern exists for
    /// handles built *after* this state; this one is built *with* it.
    pub fn content_denylist(&self) -> Arc<crate::content_deny::ContentDenylist> {
        Arc::clone(&self.content.deny)
    }

    /// Attach the live `ConnectionLimiter` after it's been built. Same
    /// shape as [`Self::attach_cache`]: must be called before the SIGHUP
    /// select loop, supports `None` for tests, recovers from a poisoned
    /// mutex by replacing the inner state.
    pub fn attach_limiter(&self, limiter: Option<Arc<ConnectionLimiter>>) {
        match self.security.limiter.lock() {
            Ok(mut guard) => *guard = limiter,
            Err(poisoned) => {
                tracing::error!(
                    "runtime reload limiter mutex poisoned during attach; recovering inner state"
                );
                *poisoned.into_inner() = limiter;
            }
        }
    }

    /// Attach the live `LoadShedController` after it's been built. Same
    /// shape as [`Self::attach_limiter`]: must be called before the SIGHUP
    /// select loop, supports `None` for tests, recovers from a poisoned
    /// mutex by replacing the inner state.
    pub fn attach_load_shed(&self, controller: Option<Arc<crate::load_shed::LoadShedController>>) {
        match self.load_shed.controller.lock() {
            Ok(mut guard) => *guard = controller,
            Err(poisoned) => {
                tracing::error!(
                    "runtime reload load-shed controller mutex poisoned during attach; \
                     recovering inner state"
                );
                *poisoned.into_inner() = controller;
            }
        }
    }

    /// Build a state for tests outside `runtime::reload::tests` that need
    /// to drive `reload()` end-to-end (e.g. `admin::tests` exercising
    /// `admin_v1_reload`). Centralised here rather than duplicated per
    /// test module so the boilerplate `ResolvedConfig` for which fields
    /// reload reads stays in one place — drift between two copies would
    /// give different test surfaces for the same code path.
    #[cfg(test)]
    // One flat `ResolvedConfig` literal enumerating every reload-relevant field;
    // it crossed 100 lines once both #651 (`discovery`) and #831 (node-pull cache
    // knobs) added fields. Splitting the single struct literal across helpers
    // would obscure which fields the reload path reads, not clarify it.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn for_test_with_setter(
        level: decdn_common::cli::common::LogLevel,
        log_level_setter: LogLevelSetter,
    ) -> Self {
        use std::path::PathBuf;

        use decdn_common::config::{
            ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedIdentity, ResolvedNetwork,
            ResolvedObservability, ResolvedPayment, ResolvedSecurity,
        };

        let cfg = ResolvedConfig {
            identity: ResolvedIdentity {
                data_dir: PathBuf::from("/tmp/decdn-test"),
                region: None,
            },
            network: ResolvedNetwork {
                bind_port: 4433,
                relay_urls: Vec::new(),
                discovery: decdn_common::config::ResolvedDiscovery::default(),
            },
            blockchain: ResolvedBlockchain {
                origin_assignment_address: None,
                origin_directory_positive_ttl_sec:
                    decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_POSITIVE_TTL_SEC,
                origin_directory_negative_ttl_sec:
                    decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_NEGATIVE_TTL_SEC,
                origin_directory_cache_capacity:
                    decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_CACHE_CAPACITY,
                publisher_registry_address: None,
                rpc_url: "http://localhost:8545".into(),
                eth_keystore: PathBuf::from("/tmp/keystore.json"),
                keystore_password_file: None,
                payment_pool_address: "0x0000000000000000000000000000000000000001".into(),
                capacity_bond_address: "0x0000000000000000000000000000000000000002".into(),
                rpc_watchdog_interval_sec: 30,
                event_poll_interval_ms: 7000,
                rate_bounds_poll_interval_sec: 3600,
                fee_shares_poll_interval_sec: 3600,
                redeem_threshold_micro_usdc: 1_000_000,
                redeem_max_vouchers_per_tx: 300,
                redeem_interval_secs: 300,
                buyer_working_deposit_micro_usdc: 10_000_000,
                buyer_max_approve: true,
                pool_min_remaining_deposit_micro_usdc: 1_000_000,
                slash_judge_address: "0x0000000000000000000000000000000000000003".to_string(),
                content_blacklist_address: None,
                content_blacklist_poll_interval_sec: 600,
                chain_id: decdn_common::config::DEFAULT_CHAIN_ID,
            },
            cache: ResolvedCache {
                cache_dir: PathBuf::from("/tmp/cache"),
                cache_size_mb: 1024,
                max_blob_size_mb: 128,
                max_rate_per_mb: 0,
                origins: Vec::new(),
                pinned_hashes: decdn_cache::PinnedHashes::empty(),
                origin_retry: decdn_cache::RetryPolicy::default(),
                circuit_breaker: decdn_cache::CircuitBreakerPolicy::default(),
                user_agent: decdn_cache::DEFAULT_USER_AGENT.to_string(),
                gc_interval_sec: 0,
                fs_rescan_interval_sec: 0,
                origin_probe_ttl_sec: decdn_common::config::DEFAULT_ORIGIN_PROBE_TTL_SEC,
                origin_probe_negative_ttl_sec:
                    decdn_common::config::DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL_SEC,
                origin_probe_fault_ttl_sec:
                    decdn_common::config::DEFAULT_ORIGIN_PROBE_FAULT_TTL_SEC,
                origin_probe_timeout_ms: decdn_common::config::DEFAULT_ORIGIN_PROBE_TIMEOUT_MS,
                origin_probe_memo_capacity:
                    decdn_common::config::DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY,
                eviction_high_water_pct: 90,
                eviction_target_pct: 80,
                eviction_per_sweep_budget: 16,
                eviction_tick_secs: 1,
                max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
                stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
                node_to_node_pull_through_enabled: false,
                relay_foreign_namespaces: decdn_common::config::DEFAULT_RELAY_FOREIGN_NAMESPACES,
                node_pull_probe_fanout: decdn_common::config::DEFAULT_NODE_PULL_PROBE_FANOUT,
                node_pull_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_TIMEOUT_SEC,
                node_pull_stall_timeout_sec:
                    decdn_common::config::DEFAULT_NODE_PULL_STALL_TIMEOUT_SEC,
                eviction_policy: decdn_common::config::DEFAULT_EVICTION_POLICY.to_string(),
                admission_policy: decdn_common::config::DEFAULT_ADMISSION_POLICY.to_string(),
                tinylfu: decdn_common::config::ResolvedTinyLfu {
                    sketch_bytes: decdn_common::config::DEFAULT_TINYLFU_SKETCH_BYTES,
                    promotion_threshold: decdn_common::config::DEFAULT_TINYLFU_PROMOTION_THRESHOLD,
                    probation_target_pct:
                        decdn_common::config::DEFAULT_TINYLFU_PROBATION_TARGET_PCT,
                    aging_halflife_sec: decdn_common::config::DEFAULT_TINYLFU_AGING_HALFLIFE_SEC,
                },
                serve_economics: decdn_common::config::ResolvedServeEconomics {
                    policy: decdn_common::config::DEFAULT_SERVE_ECONOMICS_POLICY.to_string(),
                    discount_bps: decdn_common::config::DEFAULT_SERVE_ECONOMICS_DISCOUNT_BPS,
                    n_max: decdn_common::config::DEFAULT_SERVE_ECONOMICS_N_MAX,
                    warming_budget: decdn_common::config::DEFAULT_SERVE_ECONOMICS_WARMING_BUDGET,
                    warming_refill: decdn_common::config::DEFAULT_SERVE_ECONOMICS_WARMING_REFILL,
                },
            },
            // Placeholder rate — `payment.*` is restart-required.
            payment: ResolvedPayment {
                rate_per_mb: 1,
                delivery_floor: 0,
                credit_max: decdn_common::config::DEFAULT_CREDIT_MAX,
                frame_target_bytes: decdn_common::config::DEFAULT_FRAME_TARGET_BYTES,
                credit_ramp_divisor: decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
                voucher_commit_interval_ms:
                    decdn_common::config::DEFAULT_VOUCHER_COMMIT_INTERVAL_MS,
            },
            observability: ResolvedObservability {
                log_level: level,
                log_format: decdn_common::cli::common::LogFormat::Pretty,
                metrics_port: 9090,
                metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                admin_port: Some(9191),
                otlp_endpoint: None,
            },
            security: ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_source_rate_per_sec: 100.0,
                per_source_burst: 200,
                max_tracked_sources: 4096,
            },
            load_shed: decdn_common::config::ResolvedLoadShed::default(),
            dht: decdn_common::config::ResolvedDht::default(),
            probe: decdn_common::config::ResolvedProbe::default(),
            receipts: decdn_common::config::ResolvedReceipts::default(),
            content: decdn_common::config::ResolvedContent::default(),
        };
        Self::new(
            decdn_common::cli::run::ObservabilityArgs {
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
    /// most recently applied log level.
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
    /// already received an `Ok(())` from `reload()`; the snapshot is
    /// best-effort metadata.
    pub fn current(&self) -> ReloadSnapshot {
        let log_level = self.log_level.current.lock().ok().and_then(|guard| *guard);
        ReloadSnapshot { log_level }
    }

    /// Re-read the config file at `path` and apply changes to reloadable
    /// fields. Triggered by SIGHUP (see `runtime::run`'s select loop). On
    /// parse error the previous values are retained and the error is
    /// logged; the caller (the SIGHUP arm of the select) discards the
    /// returned error so a malformed reload never propagates and stops
    /// the node.
    ///
    /// Three phases, each driven over the registered `sections` in registration
    /// order:
    ///
    /// 1. **Resolve.** Each section re-derives its `Resolved` value from
    ///    `file` (and any CLI overrides held on the section struct) and
    ///    stashes it in its buffer cell. Any failure aborts the reload
    ///    before any side-effect runs — the all-or-nothing contract.
    /// 2. **Fallible commit.** The log-level filter swap is the only
    ///    currently-fallible commit. **Rollback boundary:** commits
    ///    that already succeeded stay applied even if a later section's
    ///    `fallible_commit` fails. This matches the old monolithic
    ///    behaviour; the trait makes it explicit.
    /// 3. **Infallible swap.** Atomic stores, `Arc` swaps, and
    ///    `ConnectionLimiter::reload`. Each section also emits a
    ///    `config reload section applied` tracing event keyed by
    ///    `section = name()`.
    ///
    /// A single "requires restart" info line is emitted for each
    /// non-reloadable section that carries a non-reloadable field in the
    /// new file (see `warn_restart_required_sections`) so an operator who
    /// edited one of those fields sees "you changed X but it needs a
    /// restart". It fires on presence of the field, not on a change to it
    /// (no diff against the previous file) — best-effort operator
    /// guidance, not a correctness gate.
    #[allow(
        clippy::cognitive_complexity, // A few short phase loops; reads better as one unit than split apart.
        clippy::unused_async, // Future-shaped on purpose: see below.
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

        // Serialise concurrent reloads before *any* shared-state mutation.
        // `reload()` has no `.await` points, so on a multi-threaded runtime
        // two callers (a SIGHUP racing an `admin_v1_reload`) can execute in
        // true parallel; the per-section buffer cells are cleared in phase 1
        // and drained in phase 3, so the guard must cover clear+resolve
        // through commit+swap — otherwise a second reload could clear/
        // overwrite the first's freshly-resolved buffers mid-flight and make
        // it swap a mixed set. Only the file load above (no shared state)
        // stays outside. The lock guards no data itself, so a poisoned lock
        // is recovered in place: a prior panic-mid-reload must not wedge
        // every future reload. Recovery is logged loudly (this file's
        // "recover-the-data, log-loudly, do-not-clear-poison" pattern — see
        // `attach_engine_to_section` / `drain_or_log`): a poisoned
        // `reload_lock` is the sole surviving evidence that a previous reload
        // panicked mid-commit, and swallowing it silently would erase it.
        let _reload_guard = match self.reload_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!(
                    "runtime reload_lock poisoned by a prior panic-mid-reload; \
                     recovering and proceeding (an earlier reload may have \
                     partially applied before panicking)"
                );
                poisoned.into_inner()
            }
        };

        // Clear every section's buffer up-front so a panic-mid-reload
        // from a previous attempt can't leave stale `Resolved` data
        // behind. Each `clear_buffer` is infallible (poison-recovering).
        for section in &self.sections {
            section.clear_buffer();
        }

        // Phase 1: resolve every section into one shared bag, then
        // collapse it once. Non-empty bag → return early, no side-effects
        // committed (the all-or-nothing contract).
        let mut bag = ConfigErrorBag::new();
        for section in &self.sections {
            section.resolve(&file, &mut bag);
        }
        let problem_count = bag.problem_count();
        if let Err(err) = bag.into_result() {
            tracing::warn!(
                %err,
                problem_count,
                "config reload aborted; entire reload rolled back \
                 (all-or-nothing) — all reloadable sections retained at \
                 their previous values",
            );
            return Err(err);
        }

        // Emit a "requires restart" notice for each non-reloadable field the
        // file carries. Read-only, so do it before the commit step.
        warn_restart_required_sections(&file);

        // Phase 2: fallible commits. The log-level section is the only
        // one that can fail here today; future sections may add more.
        // Rollback boundary: a section that already returned `Ok`
        // stays applied even if a later section fails. Surface the
        // first error and let the caller log/return.
        for section in &self.sections {
            if let Err(err) = section.fallible_commit() {
                // Sections that already committed in this loop stay
                // applied — that's the rollback boundary. We do *not*
                // run any infallible_swap to avoid driving sections
                // partway through a phase. This matches the old
                // monolithic body's behaviour where a setter failure
                // returned before the rate atomic swap.
                tracing::warn!(
                    %err,
                    section = section.name(),
                    "config reload fallible_commit failed; later sections skipped"
                );
                return Err(err);
            }
        }

        // Phase 3: infallible swaps. None can fail.
        for section in &self.sections {
            section.infallible_swap();
        }

        // Final summary line, retained for backwards compatibility
        // with any operators / log scrapers that grep for it. The
        // per-section `config reload section applied` events carry
        // the structured fields; this one is the "all done" marker.
        tracing::info!("config reload applied");
        Ok(())
    }
}

/// Emit a single info line naming a non-reloadable field group.
fn warn_ignored(field: &'static str) {
    tracing::info!(
        field,
        "config reload: ignoring change to {field} (requires restart)"
    );
}

/// Emit a "requires restart" notice for each non-reloadable field the
/// operator can't hot-apply. Fully non-reloadable sections (including
/// `payment`) warn whenever they are *present*; the partially-reloadable
/// sections (`cache`, `observability`) warn only when they set a field
/// *outside* their reloadable subset, so the common `cache.pinned_hashes`-only
/// or `observability.log_level`-only reload stays quiet. `security` is fully
/// reloadable and never warns. Best-effort operator guidance, not a correctness
/// gate — this does not diff against the previous file, so a present-but-unchanged
/// non-reloadable field still warns on every reload.
fn warn_restart_required_sections(file: &decdn_common::config::FileConfig) {
    if file.identity.is_some() {
        warn_ignored("identity.* (data_dir, region)");
    }
    if file.network.is_some() {
        warn_ignored("network.* (bind_port, relay_urls, relay_url, discovery)");
    }
    if file.blockchain.is_some() {
        warn_ignored("blockchain.* (rpc_url, eth_keystore, contract addresses)");
    }
    if file
        .cache
        .as_ref()
        .is_some_and(cache_has_restart_required_field)
    {
        warn_ignored("cache.* (cache_dir, sizes, origin, decompress, max_probe_holds)");
    }
    if file.payment.is_some() {
        // The whole `[payment]` section is restart-required. `rate_per_mb` is
        // the served price — an economic commitment on the wire — so it takes
        // effect only at startup; reprice by draining and restarting.
        // `delivery_floor` is governed on-chain via `getRateBounds()`; the
        // other serve knobs are read once at handler construction.
        warn_ignored(
            "payment.* (rate_per_mb, delivery_floor, credit_max, \
             credit_ramp_divisor, frame_target_bytes, voucher_commit_interval_ms)",
        );
    }
    if file
        .observability
        .as_ref()
        .is_some_and(observability_has_restart_required_field)
    {
        warn_ignored(
            "observability.* (log_format, metrics_port, metrics_bind, otlp_endpoint, \
             admin_port)",
        );
    }
    if file.dht.is_some() {
        // `dht.*` (rate-limit caps) is not currently hot-reloadable — the
        // limiter is constructed once at startup.
        // Reloadability would follow the dispatch-limiter pattern
        // (`security.*`) once the limiter grows an ArcSwap on its inner
        // state.
        warn_ignored("dht.* (rate-limit)");
    }
    if file.probe.is_some() {
        // `probe.rate_limit` is read once at startup; changing it requires
        // a restart (same footing as `dht.*`).
        warn_ignored("probe.* (rate_limit)");
    }
    if file.receipts.is_some() {
        // `receipts.*` (rotation cap, retained backups) is read once when
        // `JsonlReceiptLog` is opened at bring-up; changing it requires a
        // restart.
        warn_ignored("receipts.* (max_file_bytes, retained_files)");
    }
    // `security.*` is fully reloadable — see `RuntimeReloadState::reload`'s
    // commit step. Invalid values reject the entire reload via
    // `resolve_security` upstream rather than landing here.
}

/// Whether the file's `[cache]` section sets any field that a restart is
/// required to apply — i.e. anything other than the hot-reloadable
/// `pinned_hashes` (#276). Destructured exhaustively so that adding a new
/// `CacheConfig` field is a compile error here until it's classified
/// reloadable-or-not, rather than silently escaping the restart notice.
const fn cache_has_restart_required_field(c: &decdn_common::config::types::CacheConfig) -> bool {
    let decdn_common::config::types::CacheConfig {
        pinned_hashes: _, // the only hot-reloadable cache field
        cache_dir,
        cache_size_mb,
        max_blob_size_mb,
        max_rate_per_mb,
        origin,
        origins,
        origin_retry,
        circuit_breaker,
        user_agent,
        gc_interval_sec,
        fs_rescan_interval_sec,
        origin_probe_ttl_sec,
        origin_probe_negative_ttl_sec,
        origin_probe_fault_ttl_sec,
        origin_probe_timeout_ms,
        origin_probe_memo_capacity,
        eviction_high_water_pct,
        eviction_target_pct,
        eviction_per_sweep_budget,
        eviction_tick_secs,
        max_probe_holds,
        stake_lane_reserved_holds,
        node_to_node_pull_through_enabled,
        relay_foreign_namespaces,
        node_pull_probe_fanout,
        node_pull_timeout_sec,
        node_pull_stall_timeout_sec,
        eviction_policy,
        admission_policy,
        tinylfu,
        serve_economics,
    } = c;
    cache_dir.is_some()
        || cache_size_mb.is_some()
        || max_blob_size_mb.is_some()
        || max_rate_per_mb.is_some()
        || origin.is_some()
        || origins.is_some()
        || origin_retry.is_some()
        || circuit_breaker.is_some()
        || user_agent.is_some()
        || gc_interval_sec.is_some()
        // The rescan *cadence* needs a restart to rebuild the interval timer;
        // a reload still re-runs one rescan to pick up newly-added files.
        || fs_rescan_interval_sec.is_some()
        // Live-origin probe memo (#1130 pt3) is built once at bring-up, so any
        // of its knobs changing needs a restart.
        || origin_probe_ttl_sec.is_some()
        || origin_probe_negative_ttl_sec.is_some()
        || origin_probe_fault_ttl_sec.is_some()
        || origin_probe_timeout_ms.is_some()
        || origin_probe_memo_capacity.is_some()
        || eviction_high_water_pct.is_some()
        || eviction_target_pct.is_some()
        || eviction_per_sweep_budget.is_some()
        || eviction_tick_secs.is_some()
        || max_probe_holds.is_some()
        || stake_lane_reserved_holds.is_some()
        || node_to_node_pull_through_enabled.is_some()
        || relay_foreign_namespaces.is_some()
        || node_pull_probe_fanout.is_some()
        || node_pull_timeout_sec.is_some()
        || node_pull_stall_timeout_sec.is_some()
        // Admission/eviction policy selection and its tuning knobs (ADR 040)
        // are wired once at bring-up — the estimator and policy objects are
        // constructed in `build_infra` and injected into the engine/driver;
        // changing them requires a restart.
        || eviction_policy.is_some()
        || admission_policy.is_some()
        || tinylfu.is_some()
        // Refuse-to-serve economics (ADR 041) is wired once at bring-up
        // alongside the admission/eviction policy objects; changing it
        // requires a restart.
        || serve_economics.is_some()
}

/// Whether the file's `[observability]` section sets any field that a
/// restart is required to apply — i.e. anything other than the
/// hot-reloadable `log_level`. Exhaustively destructured for the same
/// compile-time-classification reason as [`cache_has_restart_required_field`].
const fn observability_has_restart_required_field(
    o: &decdn_common::config::types::ObservabilityConfig,
) -> bool {
    let decdn_common::config::types::ObservabilityConfig {
        log_level: _, // the only hot-reloadable observability field
        log_format,
        metrics_port,
        metrics_bind,
        admin_port,
        otlp_endpoint,
    } = o;
    log_format.is_some()
        || metrics_port.is_some()
        || metrics_bind.is_some()
        || admin_port.is_some()
        || otlp_endpoint.is_some()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr as _;
    use std::sync::Mutex;

    use super::*;
    use decdn_common::cli::common::LogLevel;
    use decdn_common::config::{
        ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedIdentity, ResolvedNetwork,
        ResolvedObservability, ResolvedPayment, ResolvedSecurity,
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
    #[allow(clippy::too_many_lines)] // exhaustive struct literal, not real complexity
    fn seed_resolved(rate: u64, level: LogLevel) -> ResolvedConfig {
        ResolvedConfig {
            identity: ResolvedIdentity {
                data_dir: PathBuf::from("/tmp/decdn-test"),
                region: None,
            },
            network: ResolvedNetwork {
                bind_port: 4433,
                relay_urls: Vec::new(),
                discovery: decdn_common::config::ResolvedDiscovery::default(),
            },
            blockchain: ResolvedBlockchain {
                origin_assignment_address: None,
                origin_directory_positive_ttl_sec:
                    decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_POSITIVE_TTL_SEC,
                origin_directory_negative_ttl_sec:
                    decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_NEGATIVE_TTL_SEC,
                origin_directory_cache_capacity:
                    decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_CACHE_CAPACITY,
                publisher_registry_address: None,
                rpc_url: "http://localhost:8545".into(),
                eth_keystore: PathBuf::from("/tmp/keystore.json"),
                keystore_password_file: None,
                payment_pool_address: "0x0000000000000000000000000000000000000001".into(),
                capacity_bond_address: "0x0000000000000000000000000000000000000002".into(),
                rpc_watchdog_interval_sec: 30,
                event_poll_interval_ms: 7000,
                rate_bounds_poll_interval_sec: 3600,
                fee_shares_poll_interval_sec: 3600,
                redeem_threshold_micro_usdc: 1_000_000,
                redeem_max_vouchers_per_tx: 300,
                redeem_interval_secs: 300,
                buyer_working_deposit_micro_usdc: 10_000_000,
                buyer_max_approve: true,
                pool_min_remaining_deposit_micro_usdc: 1_000_000,
                slash_judge_address: "0x0000000000000000000000000000000000000003".to_string(),
                content_blacklist_address: None,
                content_blacklist_poll_interval_sec: 600,
                chain_id: decdn_common::config::DEFAULT_CHAIN_ID,
            },
            cache: ResolvedCache {
                cache_dir: PathBuf::from("/tmp/cache"),
                cache_size_mb: 1024,
                max_blob_size_mb: 128,
                max_rate_per_mb: 0,
                origins: Vec::new(),
                pinned_hashes: decdn_cache::PinnedHashes::empty(),
                origin_retry: decdn_cache::RetryPolicy::default(),
                circuit_breaker: decdn_cache::CircuitBreakerPolicy::default(),
                user_agent: decdn_cache::DEFAULT_USER_AGENT.to_string(),
                gc_interval_sec: 0,
                fs_rescan_interval_sec: 0,
                origin_probe_ttl_sec: decdn_common::config::DEFAULT_ORIGIN_PROBE_TTL_SEC,
                origin_probe_negative_ttl_sec:
                    decdn_common::config::DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL_SEC,
                origin_probe_fault_ttl_sec:
                    decdn_common::config::DEFAULT_ORIGIN_PROBE_FAULT_TTL_SEC,
                origin_probe_timeout_ms: decdn_common::config::DEFAULT_ORIGIN_PROBE_TIMEOUT_MS,
                origin_probe_memo_capacity:
                    decdn_common::config::DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY,
                eviction_high_water_pct: 90,
                eviction_target_pct: 80,
                eviction_per_sweep_budget: 16,
                eviction_tick_secs: 1,
                max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
                stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
                node_to_node_pull_through_enabled: false,
                relay_foreign_namespaces: decdn_common::config::DEFAULT_RELAY_FOREIGN_NAMESPACES,
                node_pull_probe_fanout: decdn_common::config::DEFAULT_NODE_PULL_PROBE_FANOUT,
                node_pull_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_TIMEOUT_SEC,
                node_pull_stall_timeout_sec:
                    decdn_common::config::DEFAULT_NODE_PULL_STALL_TIMEOUT_SEC,
                eviction_policy: decdn_common::config::DEFAULT_EVICTION_POLICY.to_string(),
                admission_policy: decdn_common::config::DEFAULT_ADMISSION_POLICY.to_string(),
                tinylfu: decdn_common::config::ResolvedTinyLfu {
                    sketch_bytes: decdn_common::config::DEFAULT_TINYLFU_SKETCH_BYTES,
                    promotion_threshold: decdn_common::config::DEFAULT_TINYLFU_PROMOTION_THRESHOLD,
                    probation_target_pct:
                        decdn_common::config::DEFAULT_TINYLFU_PROBATION_TARGET_PCT,
                    aging_halflife_sec: decdn_common::config::DEFAULT_TINYLFU_AGING_HALFLIFE_SEC,
                },
                serve_economics: decdn_common::config::ResolvedServeEconomics {
                    policy: decdn_common::config::DEFAULT_SERVE_ECONOMICS_POLICY.to_string(),
                    discount_bps: decdn_common::config::DEFAULT_SERVE_ECONOMICS_DISCOUNT_BPS,
                    n_max: decdn_common::config::DEFAULT_SERVE_ECONOMICS_N_MAX,
                    warming_budget: decdn_common::config::DEFAULT_SERVE_ECONOMICS_WARMING_BUDGET,
                    warming_refill: decdn_common::config::DEFAULT_SERVE_ECONOMICS_WARMING_REFILL,
                },
            },
            payment: ResolvedPayment {
                rate_per_mb: rate,
                delivery_floor: 0,
                credit_max: decdn_common::config::DEFAULT_CREDIT_MAX,
                frame_target_bytes: decdn_common::config::DEFAULT_FRAME_TARGET_BYTES,
                credit_ramp_divisor: decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
                voucher_commit_interval_ms:
                    decdn_common::config::DEFAULT_VOUCHER_COMMIT_INTERVAL_MS,
            },
            observability: ResolvedObservability {
                log_level: level,
                log_format: decdn_common::cli::common::LogFormat::Pretty,
                metrics_port: 9090,
                metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                admin_port: Some(9191),
                otlp_endpoint: None,
            },
            security: ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_source_rate_per_sec: 100.0,
                per_source_burst: 200,
                max_tracked_sources: 4096,
            },
            load_shed: decdn_common::config::ResolvedLoadShed::default(),
            receipts: decdn_common::config::ResolvedReceipts::default(),
            dht: decdn_common::config::ResolvedDht::default(),
            probe: decdn_common::config::ResolvedProbe::default(),
            content: decdn_common::config::ResolvedContent::default(),
        }
    }

    fn write_config(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("node.toml");
        std::fs::write(&path, body).unwrap();
        path
    }

    /// A reload whose file also changes the restart-required
    /// `payment.rate_per_mb` still applies the reloadable sections (here
    /// `log_level`) and succeeds. The restart-required *notice* is covered by
    /// the SIGHUP integration test in `crates/node/tests/sighup_signal.rs`.
    #[tokio::test]
    async fn reload_applies_log_level_and_ignores_rate_change() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[payment]\nrate_per_mb = 99\n\n[observability]\nlog_level = \"debug\"\n",
        );

        let initial = seed_resolved(10, LogLevel::Info);
        let (setter, captured) = recording_setter();
        let state = RuntimeReloadState::new(
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

    /// Fail-stop guarantee: when the setter errors, the reload surfaces the
    /// error rather than partially applying. Inject a setter that always
    /// returns an error and assert the reload fails.
    #[tokio::test]
    async fn reload_errors_when_log_level_setter_fails() {
        let dir = tempfile::tempdir().unwrap();
        // Log level differs from the cached value (None, i.e. force-apply
        // path) so the setter is actually called and gets the chance to fail.
        let path = write_config(dir.path(), "[observability]\nlog_level = \"debug\"\n");

        let failing_setter: LogLevelSetter =
            Box::new(|_| Err(anyhow::anyhow!("simulated tracing-reload failure")));

        let initial = seed_resolved(42, LogLevel::Info);
        let state = RuntimeReloadState::new(
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
    }

    /// Transactional contract for the *log-level* mutex: a poisoned
    /// `current_log_level` must surface as an error from the reload function
    /// rather than committing a partial reload.
    #[tokio::test]
    async fn reload_errors_when_log_level_mutex_poisoned() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(dir.path(), "[observability]\nlog_level = \"debug\"\n");

        let initial = seed_resolved(33, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
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
            let _guard = st_for_thread.log_level.current.lock().unwrap();
            panic!("intentional panic to poison mutex");
        });
        let _ = join.join(); // discard the panic payload
        assert!(st.log_level.current.is_poisoned());

        let err = st.reload(&path).await.unwrap_err();
        assert!(format!("{err:#}").contains("log-level mutex poisoned"));
    }

    /// The `reload_lock` only serialises concurrent reloads; it guards no
    /// data, so a poisoned lock (a prior panic-mid-reload) must not wedge
    /// future reloads. A reload after poisoning still recovers the guard
    /// and applies the file — the setter fires.
    #[tokio::test]
    async fn reload_recovers_from_poisoned_reload_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(dir.path(), "[observability]\nlog_level = \"debug\"\n");

        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, captured) = recording_setter();
        let state = RuntimeReloadState::new(
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

        // Poison the reload lock on a thread that panics while holding it.
        let st = Arc::new(state);
        let st_for_thread = Arc::clone(&st);
        let join = std::thread::spawn(move || {
            let _guard = st_for_thread.reload_lock.lock().unwrap();
            panic!("intentional panic to poison reload lock");
        });
        let _ = join.join();
        assert!(st.reload_lock.is_poisoned());

        // Reload still succeeds — the poisoned lock is recovered in place.
        st.reload(&path).await.expect("reload recovers from poison");
        assert_eq!(captured.lock().unwrap().as_ref(), Some(&LogLevel::Debug));
    }

    /// A second reload of the same file must succeed and not touch the
    /// rate (already at target). We can't directly capture `tracing`
    /// lines without a subscriber fixture, but the public-state behaviour
    /// the operator cares about is "reload remains idempotent across
    /// repeated SIGHUPs".
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
    }

    /// `payment.*` is restart-required, so the reload path leaves it unparsed:
    /// even a `rate_per_mb = 0` (which the startup resolver rejects) reloads
    /// cleanly. Startup validation catches the invalid value.
    #[tokio::test]
    async fn reload_ignores_invalid_rate_since_payment_is_restart_required() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(dir.path(), "[payment]\nrate_per_mb = 0\n");

        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
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

        state
            .reload(&path)
            .await
            .expect("a restart-required payment field must not fail the reload");
    }

    #[tokio::test]
    async fn reload_returns_error_on_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist.toml");

        let initial = seed_resolved(7, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
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
    }

    /// A malformed TOML body must reject the reload before any commit
    /// side-effect runs: the setter is never called. Without this test the
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
        // Don't bind the test to a specific TOML diagnostic; just check
        // the call failed.
        assert!(!format!("{err:#}").is_empty());

        assert!(captured.lock().unwrap().is_none());
    }

    // ----- content denylist hot-reload (ADR 011 §Local Denylist, #1168) -----

    fn denylist_state(initial: &decdn_common::config::ResolvedConfig) -> RuntimeReloadState {
        RuntimeReloadState::new(
            ObservabilityArgs {
                log_level: None,
                log_format: None,
                metrics_port: None,
                metrics_bind: None,
                admin_port: None,
                otlp_endpoint: None,
            },
            initial,
            recording_setter().0,
        )
    }

    /// The point of making `[content]` reloadable: an operator discharging a
    /// one-hour removal order must not have to bounce the daemon (dropping
    /// every in-flight paid stream) to do it.
    ///
    /// Asserts through `CacheEngine::is_denied`, not the deny-set handle,
    /// because reaching the cache is the whole fix — that is what suppresses
    /// probe `has_blob`, DHT republish, and `populate` alongside the serve gate.
    #[tokio::test]
    async fn reload_applies_content_denylist_to_the_cache_lever() {
        let dir = tempfile::tempdir().unwrap();
        let h = make_hex_hash(7);
        let origin = "0x000000000000000000000000000000000000dEaD";
        let path = write_config(
            dir.path(),
            &format!("[content]\ndenied_hashes = [\"{h}\"]\ndenied_origins = [\"{origin}\"]\n"),
        );

        let initial = seed_resolved(10, LogLevel::Info);
        let state = denylist_state(&initial);
        let (engine, _tmp) = build_test_cache().await;
        state.attach_cache(Some(engine.clone()));
        let deny = state.content_denylist();
        let hash = decdn_cache::Hash::from_str(&h).unwrap();

        assert!(!engine.is_denied(hash), "nothing denied before reload");
        assert!(!engine.refuses(hash), "and nothing refused");

        state.reload(&path).await.expect("reload succeeds");

        assert!(engine.is_denied(hash), "hash denied after reload");
        assert!(
            engine.refuses(hash),
            "and therefore refused by probe/DHT/populate too"
        );
        assert!(
            deny.is_origin_denied(&origin.parse().unwrap()),
            "origin denied after reload"
        );
    }

    /// The handler holds the same `Arc` the reload path swaps, so a reload is
    /// effective without rebuilding the handler. If these ever diverged the
    /// denylist would silently stop applying to live connections.
    #[tokio::test]
    async fn content_denylist_handle_is_shared_not_copied() {
        let initial = seed_resolved(10, LogLevel::Info);
        let state = denylist_state(&initial);
        assert!(
            Arc::ptr_eq(&state.content_denylist(), &state.content_denylist()),
            "every caller must get the same deny-set"
        );
    }

    /// A malformed entry must fail the whole reload rather than committing a
    /// partial (or empty) denylist — un-denying content mid-takedown is the
    /// failure mode the two-phase commit exists to prevent.
    #[tokio::test]
    async fn reload_rejects_malformed_denied_hash_and_keeps_prior_set() {
        let dir = tempfile::tempdir().unwrap();
        let h = make_hex_hash(7);
        let good = write_config(
            dir.path(),
            &format!("[content]\ndenied_hashes = [\"{h}\"]\n"),
        );
        let initial = seed_resolved(10, LogLevel::Info);
        let state = denylist_state(&initial);
        let (engine, _tmp) = build_test_cache().await;
        state.attach_cache(Some(engine.clone()));
        state.reload(&good).await.expect("first reload succeeds");

        let bad = dir.path().join("bad.toml");
        std::fs::write(&bad, "[content]\ndenied_hashes = [\"zzz-not-hex\"]\n").unwrap();
        let err = state.reload(&bad).await.expect_err("malformed entry fails");
        assert!(
            format!("{err:#}").contains("denied_hashes"),
            "error names the field: {err:#}"
        );

        assert!(
            engine.is_denied(decdn_cache::Hash::from_str(&h).unwrap()),
            "the prior denylist must survive a failed reload"
        );
    }

    /// Emptying the section un-denies — the operator's own lever works in both
    /// directions (a wrongful takedown must be reversible without a restart).
    /// This is the property that rules out reusing the sticky `evict` latch.
    #[tokio::test]
    async fn reload_clears_content_denylist_when_emptied() {
        let dir = tempfile::tempdir().unwrap();
        let h = make_hex_hash(7);
        let with = write_config(
            dir.path(),
            &format!("[content]\ndenied_hashes = [\"{h}\"]\n"),
        );
        let initial = seed_resolved(10, LogLevel::Info);
        let state = denylist_state(&initial);
        let (engine, _tmp) = build_test_cache().await;
        state.attach_cache(Some(engine.clone()));
        state.reload(&with).await.unwrap();
        let hash = decdn_cache::Hash::from_str(&h).unwrap();
        assert!(engine.is_denied(hash));

        let without = dir.path().join("empty.toml");
        std::fs::write(&without, "[content]\n").unwrap();
        state.reload(&without).await.unwrap();

        assert!(!engine.is_denied(hash), "denylist cleared");
        assert!(!engine.refuses(hash), "and no longer refused anywhere");
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
        let engine = decdn_cache::CacheEngine::open(tmp.path(), Vec::new(), 16)
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
    /// `reload_errors_when_log_level_mutex_poisoned` poisons the
    /// log-level slot, but never the cache slot itself. This locks in the
    /// recovery path that commit `a148c02` introduced — silently
    /// no-op'ing on a poisoned cache mutex would turn every subsequent
    /// reload into a silent no-op for pinning.
    #[tokio::test]
    async fn attach_cache_recovers_from_poisoned_mutex() {
        let initial = seed_resolved(10, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = Arc::new(RuntimeReloadState::new(
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
            let _guard = st_for_thread.pinned.engine.lock().unwrap();
            panic!("intentional panic to poison cache mutex");
        });
        let _ = join.join();
        assert!(
            state.pinned.engine.is_poisoned(),
            "test setup: cache mutex should be poisoned"
        );

        // Recovery path: `attach_cache` must accept the new engine
        // despite the poison.
        let (cache, _tmp_cache) = build_test_cache().await;
        state.attach_cache(Some(cache.clone()));

        let stored = state
            .pinned
            .engine
            .lock()
            .map_or_else(|p| p.into_inner().is_some(), |g| g.is_some());
        assert!(stored, "attach_cache must store engine despite poison");
    }

    // ----- security hot-reload (#235) -----

    /// Build a `ConnectionLimiter` with the same defaults `seed_resolved`
    /// uses for `ResolvedSecurity`. Returned by `Arc` so tests can clone
    /// it into both `attach_limiter` and assertions about the live state.
    fn build_test_limiter() -> Arc<crate::dispatch::ConnectionLimiter> {
        use crate::dispatch::ConnectionLimiter;
        use crate::metrics::Metrics;
        use decdn_common::config::ResolvedSecurity;
        let metrics = Arc::new(Metrics::new());
        Arc::new(ConnectionLimiter::new(
            &ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_source_rate_per_sec: 100.0,
                per_source_burst: 200,
                max_tracked_sources: 4096,
            },
            metrics,
        ))
    }

    #[tokio::test]
    async fn reload_applies_security_when_limiter_attached() {
        // Tighten per-source burst from default 200 down to 1; after
        // reload the live limiter must reject the second acquire from
        // the same source IP.
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[security]\n\
             per_source_rate_per_sec = 0.001\n\
             per_source_burst = 1\n",
        );

        let initial = seed_resolved(10, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
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

        // Per-source burst is now 1.
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
        let _p1 = limiter
            .acquire_for_test(Some(ip))
            .expect("first per-source acquire");
        let err = limiter
            .acquire_for_test(Some(ip))
            .expect_err("second per-source acquire must reject after reload");
        assert_eq!(err, crate::dispatch::RejectReason::PerSource);
    }

    #[tokio::test]
    async fn reload_without_attached_limiter_is_noop_for_security() {
        // No limiter attached → reload still parses + validates security
        // but doesn't blow up. Equivalent of the cache "noop for pinning"
        // test that already exists.
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[security]\nper_source_burst = 5\nper_source_rate_per_sec = 1.0\n",
        );
        let initial = seed_resolved(10, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
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
             per_source_rate_per_sec = -1.0\n",
        );
        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, captured) = recording_setter();
        let state = RuntimeReloadState::new(
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
        assert!(format!("{err:#}").contains("per_source_rate_per_sec"));
        // "Previous values retained on error" applies to the whole reload:
        // the log-level setter must not have run when security rejected.
        assert!(
            captured.lock().unwrap().is_none(),
            "log-level setter must not have run when security rejected"
        );
    }

    /// SIGHUP aggregates problems across sections into one error;
    /// all-or-nothing — no section's previous value moves and the
    /// log-level setter is never called.
    #[tokio::test]
    async fn reload_aggregates_problems_across_sections() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[cache]\n\
             pinned_hashes = [\"notahash\"]\n\
             [security]\n\
             per_source_rate_per_sec = -1.0\n",
        );

        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, captured) = recording_setter();
        let state = RuntimeReloadState::new(
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
        let msg = format!("{err:#}");
        // Aggregated envelope from `ConfigErrorBag::into_result`. `[payment]`
        // is restart-required and never resolved on reload, so it cannot
        // contribute a problem here — the two reloadable sections do.
        assert!(
            msg.contains("configuration has 2 problem(s)"),
            "expected 2-problem envelope, got: {msg}"
        );
        // Every offending field is named in the same error.
        assert!(
            msg.contains("cache.pinned_hashes"),
            "missing pinned-hashes field: {msg}"
        );
        assert!(
            msg.contains("security.per_source_rate_per_sec"),
            "missing security field: {msg}"
        );

        // All-or-nothing: the log-level setter must not have run.
        assert!(
            captured.lock().unwrap().is_none(),
            "log-level setter must not have run when any section rejected"
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
             per_source_rate_per_sec = 0.001\n\
             per_source_burst = 1\n",
        );
        let failing_setter: LogLevelSetter =
            Box::new(|_| Err(anyhow::anyhow!("simulated tracing-reload failure")));
        let initial = seed_resolved(10, LogLevel::Info);
        let state = RuntimeReloadState::new(
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
        // at the seed_resolved default (200), so two acquires from
        // different IPs still succeed.
        let _p1 = limiter
            .acquire_for_test(Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                10, 0, 0, 1,
            ))))
            .expect("setter failure must not have shrunk per-source burst");
        let _p2 = limiter
            .acquire_for_test(Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                10, 0, 0, 2,
            ))))
            .expect("seed burst (200) still in effect → second succeeds");
    }

    /// Mirror of `attach_cache_recovers_from_poisoned_mutex` for the
    /// limiter slot — silent no-op on a poisoned mutex would turn every
    /// subsequent reload into a silent no-op for security.
    #[tokio::test]
    async fn attach_limiter_recovers_from_poisoned_mutex() {
        let initial = seed_resolved(10, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = Arc::new(RuntimeReloadState::new(
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
            let _guard = st_for_thread.security.limiter.lock().unwrap();
            panic!("intentional panic to poison limiter mutex");
        });
        let _ = join.join();
        assert!(
            state.security.limiter.is_poisoned(),
            "test setup: limiter mutex should be poisoned"
        );

        let limiter = build_test_limiter();
        state.attach_limiter(Some(Arc::clone(&limiter)));

        let stored = state
            .security
            .limiter
            .lock()
            .map_or_else(|p| p.into_inner().is_some(), |g| g.is_some());
        assert!(stored, "attach_limiter must store engine despite poison");
    }

    /// A non-reloadable section present in the file only earns a
    /// "requires restart" notice — it must not gate the reload. A file
    /// carrying `[network]` (restart-only) alongside a reloadable
    /// `log_level` change still applies the reloadable field.
    #[tokio::test]
    async fn reload_applies_despite_restart_only_section_present() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[observability]\nlog_level = \"debug\"\n\n[network]\nbind_port = 4433\n",
        );

        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, captured) = recording_setter();
        let state = RuntimeReloadState::new(
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

        state.reload(&path).await.expect("reload applies");
        assert_eq!(*captured.lock().unwrap(), Some(LogLevel::Debug));
    }

    /// The `[cache]` restart notice is gated on a *non-reloadable* field
    /// being set: a `pinned_hashes`-only edit (the section's sole
    /// hot-reloadable field) must not trip it, while any other field must.
    #[test]
    fn cache_notice_gate_ignores_pinned_hashes_only() {
        use decdn_common::config::types::CacheConfig;

        // Only the hot-reloadable field set -> no restart notice.
        let pinned_only = CacheConfig {
            pinned_hashes: Some(vec!["deadbeef".to_string()]),
            ..CacheConfig::default()
        };
        assert!(!cache_has_restart_required_field(&pinned_only));
        // Empty section -> no restart notice.
        assert!(!cache_has_restart_required_field(&CacheConfig::default()));
        // A non-reloadable field set -> notice.
        let with_dir = CacheConfig {
            cache_dir: Some(PathBuf::from("/tmp/decdn-cache")),
            ..CacheConfig::default()
        };
        assert!(cache_has_restart_required_field(&with_dir));
    }

    /// The `[observability]` restart notice is gated on a *non-reloadable*
    /// field being set: a `log_level`-only edit (hot-reloadable) must not
    /// trip it, while e.g. `metrics_port` must.
    #[test]
    fn observability_notice_gate_ignores_log_level_only() {
        use decdn_common::config::types::ObservabilityConfig;

        let level_only = ObservabilityConfig {
            log_level: Some(LogLevel::Debug),
            ..ObservabilityConfig::default()
        };
        assert!(!observability_has_restart_required_field(&level_only));
        assert!(!observability_has_restart_required_field(
            &ObservabilityConfig::default()
        ));
        let with_metrics = ObservabilityConfig {
            metrics_port: Some(9090),
            ..ObservabilityConfig::default()
        };
        assert!(observability_has_restart_required_field(&with_metrics));
    }

    /// SIGHUP with a `[load_shed]` block swaps the live controller's policy:
    /// a controller pinned to `resource-pressure` with a single-slot high
    /// water mark sheds a second concurrent miss, and after reloading to
    /// `always-admit` the same shape of request admits.
    #[tokio::test]
    async fn load_shed_section_swaps_policy_on_reload() {
        let start = decdn_common::config::ResolvedLoadShed {
            policy: decdn_common::config::LoadShedPolicyKind::ResourcePressure,
            egress_budget_mbps: 0,
            max_concurrent_serves_high: 1,
            max_concurrent_serves_low: 0,
            per_client_serve_cap: 0,
        };
        let controller = crate::load_shed::LoadShedController::from_config(&start);
        // Occupy the single slot so ResourcePressure would shed a new miss.
        let _held = controller
            .try_admit(
                crate::load_shed::RequestClass::CacheHit,
                alloy::primitives::B256::ZERO,
            )
            .unwrap();
        assert!(
            controller
                .try_admit(
                    crate::load_shed::RequestClass::CacheMiss,
                    alloy::primitives::B256::from([1u8; 32])
                )
                .is_err(),
            "a full slot must shed a new miss under resource-pressure"
        );

        let dir = tempfile::tempdir().unwrap();
        let path = write_config(dir.path(), "[load_shed]\npolicy = \"always-admit\"\n");
        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
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
        state.attach_load_shed(Some(Arc::clone(&controller)));

        state.reload(&path).await.unwrap();

        // Now always-admit: the previously-shed miss admits.
        assert!(
            controller
                .try_admit(
                    crate::load_shed::RequestClass::CacheMiss,
                    alloy::primitives::B256::from([2u8; 32])
                )
                .is_ok(),
            "reload to always-admit must let a previously-shed miss through"
        );
    }
}
