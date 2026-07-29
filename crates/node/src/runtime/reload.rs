//! Hot-reload of mutable configuration fields on SIGHUP.
//!
//! Reloadable fields (applied in place; no restart required):
//!   - `payment.rate_per_mb`
//!   - `observability.log_level`
//!   - `cache.pinned_hashes`
//!   - all of `security.*` — the live `ConnectionLimiter` swaps its
//!     `Arc<Semaphore>` wholesale on reload (already-held permits drain
//!     into the previous semaphore on drop; new acquires hit the new
//!     one) and rebuilds its keyed [`governor`] rate limiter from the
//!     new quota, swapping it in under an `RwLock`. Token-bucket state
//!     is *not* preserved across the rebuild. `0` in any `security.*`
//!     field disables that layer.
//!
//! Any non-reloadable field the file carries gets a "requires restart"
//! message (logged on presence, not on change) — the runtime would
//! otherwise need to tear down the iroh endpoint, the metrics listener,
//! the gossip subscriptions, etc., which is far beyond the scope of a
//! quick reload.
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::dispatch::ConnectionLimiter;
use anyhow::Context;
use decdn_common::cli::common::LogLevel;
use decdn_common::cli::run::{ObservabilityArgs, PaymentArgs};
use decdn_common::config::{
    ConfigErrorBag, FileConfig, ResolvedObservability, ResolvedPayment, ResolvedSecurity,
    load_file_config, parse_pinned_hashes, resolve_observability_into, resolve_payment_into,
    resolve_security_into,
};
use tokio_util::sync::CancellationToken;

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

// ----- payment ------------------------------------------------------------

/// Reloadable `[payment]` section. Owns the shared `Arc<AtomicU64>`
/// behind `payment.rate_per_mb` so the probe handler reads the current
/// value without a lock.
struct PaymentSection {
    cli: PaymentArgs,
    rate_per_mb: Arc<AtomicU64>,
    /// Last applied rate, retained across reloads for the per-section
    /// `prev_rate_per_mb` field on the success line.
    buf: std::sync::Mutex<Option<ResolvedPayment>>,
    /// `delivery_floor` as seeded into the live handlers at startup. Only
    /// `rate_per_mb` is hot-reloadable. Since #1172 the live delivery floor is
    /// sourced from on-chain `getRateBounds()` and tracked by the
    /// `RateBoundsUpdated` watcher, so the config `delivery_floor` is only a
    /// pre-chain seed — a reload that changes it is accepted by
    /// `resolve_payment` but has no effect on the live clamp (the chain value
    /// is authoritative). Retained here so the swap can warn instead of
    /// silently ignoring the change.
    applied_floor: u64,
}

impl ReloadableSection for PaymentSection {
    fn name(&self) -> &'static str {
        "payment"
    }
    fn clear_buffer(&self) {
        if let Ok(mut g) = self.buf.lock() {
            *g = None;
        }
    }
    fn resolve(&self, file: &FileConfig, bag: &mut ConfigErrorBag) {
        let resolved = resolve_payment_into(&self.cli, file.payment.as_ref(), bag);
        if let Ok(mut g) = self.buf.lock() {
            *g = Some(resolved);
        }
    }
    fn infallible_swap(&self) {
        let Some(resolved) = drain_or_log(&self.buf, self.name()) else {
            return;
        };
        let prev = self
            .rate_per_mb
            .swap(resolved.rate_per_mb, Ordering::Relaxed);
        // Since #1172 the live delivery floor comes from on-chain
        // `getRateBounds()` (seeded at startup, kept current by the
        // `RateBoundsUpdated` watcher); the config value is only the pre-chain
        // seed. A reload that changes it has no effect on the live clamp —
        // governance owns it on-chain. Surface that rather than silently
        // ignoring the change.
        if resolved.delivery_floor != self.applied_floor {
            tracing::warn!(
                section = self.name(),
                applied_delivery_floor = self.applied_floor,
                new_delivery_floor = resolved.delivery_floor,
                "payment.delivery_floor change ignored; the live delivery floor \
                 is governed on-chain via getRateBounds() (#1172), not this \
                 config seed"
            );
        }
        tracing::info!(
            section = self.name(),
            rate_per_mb = resolved.rate_per_mb,
            prev_rate_per_mb = prev,
            "config reload section applied"
        );
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
    /// Whether remote-origin prewarm was enabled at boot (#1130) — the resolved
    /// [`decdn_common::config::prewarm_enabled_for`] predicate, not the raw
    /// flag. Fixed for the process lifetime: `cache.prewarm` is restart-required,
    /// so the boot value is authoritative and a reload cannot turn prewarm on or
    /// off — only warm the pins a reload adds, when it was already on.
    prewarm: bool,
    /// `cache.cache_size_mb` at boot, for the reload-time pin-budget re-check.
    /// Restart-required like `prewarm`, so the boot value stays authoritative.
    cache_size_mb: u64,
    /// Held for the duration of a detached rescan/warm so overlapping reloads
    /// cannot stack them. A `tokio::sync::Mutex` (not `std`) because it is held
    /// across awaits.
    ///
    /// Single-flight, **not** drop-the-loser: a reload that cannot take the lock
    /// has already set [`Self::warm_dirty`], and the holder re-checks that flag
    /// before releasing. Dropping the loser outright would strand its pins — the
    /// in-flight pass snapshots the pinned set when it starts, so a pin added
    /// after that point is invisible to it, and nothing would retry until the
    /// next reload that happens to add one.
    warm_in_flight: Arc<tokio::sync::Mutex<()>>,
    /// A reload landed and its pin set has not been covered by a completed pass
    /// yet. Set before the spawn, cleared by the lock holder at the top of each
    /// iteration, so there is no lost wakeup in either interleaving.
    warm_dirty: Arc<AtomicBool>,
    /// At least one coalesced reload actually added a pin, so the next pass must
    /// warm and not merely rescan. Accumulates across coalesced reloads: a
    /// reload that only *removes* a pin must not swallow the warm owed to one
    /// that added one.
    warm_wanted: Arc<AtomicBool>,
    /// Shared with the startup warm so shutdown cancels both legs. A reload warm
    /// left running while `cache.shutdown()` closes the store underneath it
    /// fails every remaining pin and inflates `prewarm_failures_total` into the
    /// exact shape that metric documents as "your `pinned_hashes` are wrong".
    prewarm_stop: CancellationToken,
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
                // Flag the work BEFORE contending for the lock, so no update can
                // be lost in either interleaving: if the holder is about to
                // finish, it re-checks and loops; if it is mid-pass, it re-checks
                // when it gets there.
                self.warm_dirty.store(true, Ordering::SeqCst);
                if self.prewarm && diff.added > 0 {
                    // Accumulated, not per-task: a reload that only *removes* a
                    // pin must not swallow the warm owed to a concurrent one that
                    // added one.
                    self.warm_wanted.store(true, Ordering::SeqCst);
                }
                let single_flight = Arc::clone(&self.warm_in_flight);
                let dirty = Arc::clone(&self.warm_dirty);
                let wanted = Arc::clone(&self.warm_wanted);
                let cache_size_mb = self.cache_size_mb;
                let stop = self.prewarm_stop.clone();
                tokio::spawn(async move {
                    // Single-flight so a config-management loop that SIGHUPs on a
                    // timer cannot stack passes: `rescan_origins` issues one HEAD
                    // per pinned hash and is NOT coalesced by the engine (only
                    // fills are), so N overlapping passes would cost N x pin-set
                    // HEADs. Losing the lock is safe precisely BECAUSE the flags
                    // above are already set — the holder's loop below picks the
                    // work up. Dropping the loser outright would strand its pins:
                    // both `rescan_origins` and `prewarm_pinned` snapshot the
                    // pinned set when they start, so a pin added after that point
                    // is invisible to the pass in flight.
                    let Ok(_guard) = single_flight.try_lock() else {
                        tracing::debug!(
                            "pinned_hashes reload: coalescing into the rescan/warm \
                             already in flight; it will re-run for this pin set"
                        );
                        return;
                    };
                    // Re-run while anything arrived during the previous pass. The
                    // flag is cleared BEFORE the snapshots are taken, so a reload
                    // landing mid-pass always earns another iteration.
                    while dirty.swap(false, Ordering::SeqCst) {
                        engine.rescan_origins().await;
                        // Re-check the pin budget here too, not just at boot: a
                        // reload is the one moment the pin set can grow past the
                        // cache ceiling on a running node.
                        super::warn_if_pins_exceed_cache(&engine, cache_size_mb);
                        // Then warm anything pinned that isn't resident (#1130) —
                        // this is what makes a pin added at runtime effective
                        // without a restart. The whole pin set, not just the
                        // delta: `prewarm` no-ops on a hash already in the store,
                        // so the extra cost is one local presence check per pin,
                        // and in exchange a pin warmed earlier but since lost is
                        // repaired on any reload that warms.
                        if wanted.swap(false, Ordering::SeqCst) {
                            // Cancellable, like the startup warm: a reload warm
                            // still running when `cache.shutdown()` closes the
                            // store fails every remaining pin and inflates
                            // `prewarm_failures_total` into the shape that metric
                            // documents as "your pinned_hashes are wrong".
                            let report = engine.prewarm_cancellable(&stop).await;
                            tracing::info!(
                                fetched = report.fetched,
                                already_present = report.already_present,
                                // Same field set as the startup log. `refused`
                                // earns its place here more than there: a reload
                                // is exactly when an operator pins a hash that is
                                // already denylisted or operator-evicted, and
                                // without this the pin silently never warms.
                                refused = report.refused,
                                failed = report.failed,
                                bytes = report.bytes,
                                cancelled = report.cancelled,
                                "cache.prewarm: warmed the reloaded pin set (#1130)"
                            );
                        }
                        if stop.is_cancelled() {
                            break;
                        }
                    }
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
    payment: Arc<PaymentSection>,
    log_level: Arc<LogLevelSection>,
    pinned: Arc<PinnedHashesSection>,
    security: Arc<SecuritySection>,
    content: Arc<ContentSection>,
    /// Iteration order for the three-phase reload. Matches the order
    /// the previous monolithic body used (`payment`, `log_level`,
    /// `pinned_hashes`, `security`) so the user-visible commit ordering
    /// across sections doesn't shift behind the refactor. `content` is
    /// appended after them — it is new, so no prior ordering to preserve,
    /// and it commits last because it has no `attach_*` dependency that
    /// could make an earlier position matter.
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
            .field(
                "rate_per_mb",
                &self.payment.rate_per_mb.load(Ordering::Relaxed),
            )
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
        payment_cli: PaymentArgs,
        observability_cli: ObservabilityArgs,
        initial: &decdn_common::config::ResolvedConfig,
        log_level_setter: LogLevelSetter,
    ) -> Self {
        let payment = Arc::new(PaymentSection {
            cli: payment_cli,
            rate_per_mb: Arc::new(AtomicU64::new(initial.payment.rate_per_mb)),
            buf: std::sync::Mutex::new(None),
            applied_floor: initial.payment.delivery_floor,
        });
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
            prewarm: decdn_common::config::prewarm_enabled_for(&initial.cache),
            cache_size_mb: initial.cache.cache_size_mb,
            warm_in_flight: Arc::new(tokio::sync::Mutex::new(())),
            warm_dirty: Arc::new(AtomicBool::new(false)),
            warm_wanted: Arc::new(AtomicBool::new(false)),
            prewarm_stop: CancellationToken::new(),
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
        // Registration order is the same as the old monolithic body's
        // commit order: payment, log_level, pinned_hashes, security.
        // The order matters for reproducibility (operator-visible
        // tracing event order) and for the rollback-boundary contract
        // documented on the trait — moving log_level later or earlier
        // would change which pre-log_level commits survive a setter
        // failure.
        let sections: Vec<Arc<dyn ReloadableSection>> = vec![
            Arc::clone(&payment) as _,
            Arc::clone(&log_level) as _,
            Arc::clone(&pinned) as _,
            Arc::clone(&security) as _,
            Arc::clone(&content) as _,
        ];
        Self {
            payment,
            log_level,
            pinned,
            security,
            content,
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
    /// The cancellation token that stops any in-flight prewarm — the reload
    /// warm's, and (because the runtime clones this one for it) the startup
    /// warm's too. One token for both legs, cancelled at the top of `shutdown()`
    /// before the blob store closes.
    #[must_use]
    pub fn prewarm_stop(&self) -> CancellationToken {
        self.pinned.prewarm_stop.clone()
    }

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

    /// Shared atomic backing `payment.rate_per_mb`. Cloned into the probe
    /// handler at startup; subsequent reloads `store()` into this without
    /// rebuilding the handler.
    pub fn rate_per_mb(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.payment.rate_per_mb)
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
        rate_per_mb: u64,
        level: decdn_common::cli::common::LogLevel,
        log_level_setter: LogLevelSetter,
    ) -> Self {
        use std::path::PathBuf;

        use decdn_common::config::{
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
                relay_urls: Vec::new(),
                discovery: decdn_common::config::ResolvedDiscovery::default(),
            },
            blockchain: ResolvedBlockchain {
                origin_assignment_address: None,
                publisher_registry_address: None,
                rpc_url: "http://localhost:8545".into(),
                eth_keystore: PathBuf::from("/tmp/keystore.json"),
                keystore_password_file: None,
                payment_channel_address: "0x0000000000000000000000000000000000000001".into(),
                capacity_bond_address: "0x0000000000000000000000000000000000000002".into(),
                rpc_watchdog_interval_sec: 30,
                event_poll_interval_ms: 7000,
                rate_bounds_poll_interval_sec: 3600,
                redeem_threshold_micro_usdc: 1_000_000,
                buyer_deposit_micro_usdc: 10_000_000,
                buyer_max_approve: true,
                settlement_auto_threshold_micro_usdc: None,
                settlement_auto_by_voucher_nonce_span: None,
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
                prewarm: false,
                eviction_high_water_pct: 90,
                eviction_target_pct: 80,
                eviction_per_sweep_budget: 16,
                eviction_tick_secs: 1,
                max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
                stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
                node_to_node_pull_through_enabled: false,
                node_pull_probe_fanout: decdn_common::config::DEFAULT_NODE_PULL_PROBE_FANOUT,
                node_pull_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_TIMEOUT_SEC,
                node_pull_stall_timeout_sec:
                    decdn_common::config::DEFAULT_NODE_PULL_STALL_TIMEOUT_SEC,
                pull_ahead_bytes: decdn_cache::Bytes::new(
                    decdn_common::config::DEFAULT_PULL_AHEAD_BYTES,
                ),
                max_unrecouped_leech_bytes: decdn_cache::Bytes::new(
                    decdn_common::config::DEFAULT_MAX_UNRECOUPED_LEECH_BYTES,
                ),
                pull_share_ratio_percent: decdn_cache::Percent::new(
                    decdn_common::config::DEFAULT_PULL_SHARE_RATIO_PERCENT,
                ),
                pull_through_require_authorized_origin: false,
            },
            payment: ResolvedPayment {
                rate_per_mb,
                delivery_floor: 0,
                voucher_interval_mb: decdn_protocol::DEFAULT_VOUCHER_INTERVAL_MB,
                credit_window_bytes: decdn_common::config::DEFAULT_CREDIT_WINDOW_BYTES,
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
                region_accounting_interval_sec:
                    decdn_common::config::DEFAULT_REGION_ACCOUNTING_INTERVAL_SEC,
            },
            gossip: ResolvedGossip {
                announce_interval_sec: 60,
                peer_ttl_sec: 600,
                subscribe_global: false,
                max_peer_entries: Some(100_000),
            },
            security: ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_source_rate_per_sec: 100.0,
                per_source_burst: 200,
                max_tracked_sources: 4096,
            },
            dht: decdn_common::config::ResolvedDht::default(),
            probe: decdn_common::config::ResolvedProbe::default(),
            receipts: decdn_common::config::ResolvedReceipts::default(),
            content: decdn_common::config::ResolvedContent::default(),
        };
        Self::new(
            decdn_common::cli::run::PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
            },
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
        let log_level = self.log_level.current.lock().ok().and_then(|guard| *guard);
        ReloadSnapshot {
            rate_per_mb: self.payment.rate_per_mb.load(Ordering::Relaxed),
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
/// operator can't hot-apply. Fully non-reloadable sections warn whenever
/// they are *present*; the partially-reloadable sections (`cache`,
/// `observability`, `payment`) warn only when they set a field *outside*
/// their reloadable subset, so the common `cache.pinned_hashes`-only,
/// `observability.log_level`-only, or `payment.rate_per_mb`-only reload
/// stays quiet. `security` is fully reloadable and never warns.
/// Best-effort operator guidance, not a correctness gate — this does not
/// diff against the previous file, so a present-but-unchanged
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
    if file
        .payment
        .as_ref()
        .is_some_and(payment_has_restart_required_field)
    {
        // Only `voucher_interval_mb` lands here: `rate_per_mb` reloads, and
        // `delivery_floor` gets a *change-based* notice from
        // `PaymentSection::infallible_swap` (which holds the applied floor to
        // diff against). `voucher_interval_mb` has no such applied value to
        // diff, so the presence-based notice is its only home.
        warn_ignored("payment.* (voucher_interval_mb)");
    }
    if file.gossip.is_some() {
        warn_ignored("gossip.* (announce_interval, peer_ttl, subscribe_global)");
    }
    if file
        .observability
        .as_ref()
        .is_some_and(observability_has_restart_required_field)
    {
        warn_ignored(
            "observability.* (log_format, metrics_port, metrics_bind, otlp_endpoint, \
             admin_port, region_accounting_interval_sec)",
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
        prewarm,
        eviction_high_water_pct,
        eviction_target_pct,
        eviction_per_sweep_budget,
        eviction_tick_secs,
        max_probe_holds,
        stake_lane_reserved_holds,
        node_to_node_pull_through_enabled,
        node_pull_probe_fanout,
        node_pull_timeout_sec,
        node_pull_stall_timeout_sec,
        pull_ahead_bytes,
        max_unrecouped_leech_bytes,
        pull_share_ratio_percent,
        pull_through_require_authorized_origin,
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
        // Whether prewarm is ON needs a restart (the startup warm has already
        // run); a reload still warms whatever pins it *adds*, when prewarm was
        // already enabled at boot.
        || prewarm.is_some()
        || eviction_high_water_pct.is_some()
        || eviction_target_pct.is_some()
        || eviction_per_sweep_budget.is_some()
        || eviction_tick_secs.is_some()
        || max_probe_holds.is_some()
        || stake_lane_reserved_holds.is_some()
        || node_to_node_pull_through_enabled.is_some()
        || node_pull_probe_fanout.is_some()
        || node_pull_timeout_sec.is_some()
        || node_pull_stall_timeout_sec.is_some()
        || pull_ahead_bytes.is_some()
        || max_unrecouped_leech_bytes.is_some()
        || pull_share_ratio_percent.is_some()
        || pull_through_require_authorized_origin.is_some()
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
        region_accounting_interval_sec,
    } = o;
    log_format.is_some()
        || metrics_port.is_some()
        || metrics_bind.is_some()
        || admin_port.is_some()
        || otlp_endpoint.is_some()
        || region_accounting_interval_sec.is_some()
}

/// Whether the file's `[payment]` section sets a field whose restart notice
/// belongs here. `rate_per_mb` reloads and `delivery_floor` is warned
/// change-based in [`PaymentSection::infallible_swap`], so the read-once serve
/// knobs — `voucher_interval_mb`, `credit_window_bytes`, and
/// `voucher_commit_interval_ms`, each read into the client handler at bring-up
/// with no applied value to diff — are the ones that trip this gate.
/// Exhaustively destructured for the same compile-time-classification reason as
/// [`cache_has_restart_required_field`].
const fn payment_has_restart_required_field(
    p: &decdn_common::config::types::PaymentConfig,
) -> bool {
    let decdn_common::config::types::PaymentConfig {
        rate_per_mb: _,    // hot-reloadable
        delivery_floor: _, // warned change-based in PaymentSection::infallible_swap
        voucher_interval_mb,
        // Read once at handler construction (like `voucher_interval_mb`); a change
        // needs a restart to take effect (ADR 003 §Credit window).
        credit_window_bytes,
        // Read once at handler construction (#1483 group commit); a change needs a
        // restart to take effect (ADR 003 §Off-chain voucher state persistence).
        voucher_commit_interval_ms,
    } = p;
    voucher_interval_mb.is_some()
        || credit_window_bytes.is_some()
        || voucher_commit_interval_ms.is_some()
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
                publisher_registry_address: None,
                rpc_url: "http://localhost:8545".into(),
                eth_keystore: PathBuf::from("/tmp/keystore.json"),
                keystore_password_file: None,
                payment_channel_address: "0x0000000000000000000000000000000000000001".into(),
                capacity_bond_address: "0x0000000000000000000000000000000000000002".into(),
                rpc_watchdog_interval_sec: 30,
                event_poll_interval_ms: 7000,
                rate_bounds_poll_interval_sec: 3600,
                redeem_threshold_micro_usdc: 1_000_000,
                buyer_deposit_micro_usdc: 10_000_000,
                buyer_max_approve: true,
                settlement_auto_threshold_micro_usdc: None,
                settlement_auto_by_voucher_nonce_span: None,
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
                prewarm: false,
                eviction_high_water_pct: 90,
                eviction_target_pct: 80,
                eviction_per_sweep_budget: 16,
                eviction_tick_secs: 1,
                max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
                stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
                node_to_node_pull_through_enabled: false,
                node_pull_probe_fanout: decdn_common::config::DEFAULT_NODE_PULL_PROBE_FANOUT,
                node_pull_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_TIMEOUT_SEC,
                node_pull_stall_timeout_sec:
                    decdn_common::config::DEFAULT_NODE_PULL_STALL_TIMEOUT_SEC,
                pull_ahead_bytes: decdn_cache::Bytes::new(
                    decdn_common::config::DEFAULT_PULL_AHEAD_BYTES,
                ),
                max_unrecouped_leech_bytes: decdn_cache::Bytes::new(
                    decdn_common::config::DEFAULT_MAX_UNRECOUPED_LEECH_BYTES,
                ),
                pull_share_ratio_percent: decdn_cache::Percent::new(
                    decdn_common::config::DEFAULT_PULL_SHARE_RATIO_PERCENT,
                ),
                pull_through_require_authorized_origin: false,
            },
            payment: ResolvedPayment {
                rate_per_mb: rate,
                delivery_floor: 0,
                voucher_interval_mb: decdn_protocol::DEFAULT_VOUCHER_INTERVAL_MB,
                credit_window_bytes: decdn_common::config::DEFAULT_CREDIT_WINDOW_BYTES,
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
                region_accounting_interval_sec:
                    decdn_common::config::DEFAULT_REGION_ACCOUNTING_INTERVAL_SEC,
            },
            gossip: ResolvedGossip {
                announce_interval_sec: 60,
                peer_ttl_sec: 600,
                subscribe_global: false,
                max_peer_entries: Some(100_000),
            },
            security: ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_source_rate_per_sec: 100.0,
                per_source_burst: 200,
                max_tracked_sources: 4096,
            },
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
        // Rate atomic must not have moved.
        assert_eq!(st.rate_per_mb().load(Ordering::Relaxed), 33);
    }

    /// The `reload_lock` only serialises concurrent reloads; it guards no
    /// data, so a poisoned lock (a prior panic-mid-reload) must not wedge
    /// future reloads. A reload after poisoning still recovers the guard
    /// and applies the file — the setter fires and the rate atomic moves.
    #[tokio::test]
    async fn reload_recovers_from_poisoned_reload_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[payment]\nrate_per_mb = 99\n\n[observability]\nlog_level = \"debug\"\n",
        );

        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
        assert_eq!(st.rate_per_mb().load(Ordering::Relaxed), 99);
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
                delivery_floor: None,
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
    /// side-effect runs: the setter is never called and the rate atomic
    /// stays at its previous value. Without this test the transactional
    /// guarantees only get exercised on the *resolution* failure paths,
    /// not on the parse failure path.
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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

        let err = state.reload(&path).await.unwrap_err();
        // Don't bind the test to a specific TOML diagnostic; just check
        // the call failed.
        assert!(!format!("{err:#}").is_empty());

        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 42);
        assert!(captured.lock().unwrap().is_none());
    }

    // ----- content denylist hot-reload (ADR 011 §Local Denylist, #1168) -----

    fn denylist_state(initial: &decdn_common::config::ResolvedConfig) -> RuntimeReloadState {
        RuntimeReloadState::new(
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
            },
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
    /// `reload_keeps_rate_when_log_level_mutex_poisoned` poisons the
    /// log-level slot, but never the cache slot itself. This locks in the
    /// recovery path that commit `a148c02` introduced — silently
    /// no-op'ing on a poisoned cache mutex would turn every subsequent
    /// reload into a silent no-op for pinning.
    #[tokio::test]
    async fn attach_cache_recovers_from_poisoned_mutex() {
        let initial = seed_resolved(10, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = Arc::new(RuntimeReloadState::new(
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
        state.attach_limiter(Some(build_test_limiter()));

        let err = state.reload(&path).await.unwrap_err();
        assert!(format!("{err:#}").contains("per_source_rate_per_sec"));
        // Payment rate must NOT have moved despite being valid in the
        // file — "previous values retained on error" applies to the
        // whole reload.
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 42);
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
            "[payment]\n\
             rate_per_mb = 0\n\
             [cache]\n\
             pinned_hashes = [\"notahash\"]\n\
             [security]\n\
             per_source_rate_per_sec = -1.0\n",
        );

        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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

        let err = state.reload(&path).await.unwrap_err();
        let msg = format!("{err:#}");
        // Aggregated envelope from `ConfigErrorBag::into_result`.
        assert!(
            msg.contains("configuration has 3 problem(s)"),
            "expected 3-problem envelope, got: {msg}"
        );
        // Every offending field is named in the same error.
        assert!(
            msg.contains("payment.rate_per_mb"),
            "missing payment field: {msg}"
        );
        assert!(
            msg.contains("cache.pinned_hashes"),
            "missing pinned-hashes field: {msg}"
        );
        assert!(
            msg.contains("security.per_source_rate_per_sec"),
            "missing security field: {msg}"
        );

        // All-or-nothing: every section's previous value is retained.
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 42);
        assert!(
            captured.lock().unwrap().is_none(),
            "log-level setter must not have run when any section rejected"
        );
    }

    /// Two problems inside one section must both surface — guards
    /// against `*_into` workers short-circuiting on the first push.
    #[tokio::test]
    async fn reload_aggregates_two_problems_in_one_section() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[payment]\n\
             rate_per_mb = 0\n\
             voucher_interval_mb = 0\n",
        );

        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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

        let err = state.reload(&path).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("configuration has 2 problem(s)"),
            "expected 2-problem envelope, got: {msg}"
        );
        assert!(
            msg.contains("payment.rate_per_mb"),
            "missing rate_per_mb: {msg}"
        );
        assert!(
            msg.contains("payment.voucher_interval_mb"),
            "missing voucher_interval_mb: {msg}"
        );
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 42);
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
    /// carrying `[network]` (restart-only) alongside a `[payment]` rate
    /// change still applies the reloadable field.
    #[tokio::test]
    async fn reload_applies_despite_restart_only_section_present() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(
            dir.path(),
            "[payment]\nrate_per_mb = 77\n\n[network]\nbind_port = 4433\n",
        );

        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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

        state.reload(&path).await.expect("reload applies");
        assert_eq!(state.rate_per_mb().load(Ordering::Relaxed), 77);
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
        // `prewarm` specifically (#1130). The exhaustive destructure makes
        // *adding* a field a compile error, but not deleting a clause from the
        // `||` chain — and dropping this one is a silent no-op for the operator:
        // they set `prewarm = true`, SIGHUP, get no "requires restart" notice,
        // and the section's boot-fixed flag stays `false` for the process
        // lifetime.
        let with_prewarm = CacheConfig {
            prewarm: Some(true),
            ..CacheConfig::default()
        };
        assert!(cache_has_restart_required_field(&with_prewarm));
    }

    /// `PinnedHashesSection.prewarm` is fixed at boot from the resolved config
    /// and gates every reload-time warm. Nothing else observes it, so a wiring
    /// mistake here is silent: runtime-added pins would simply never warm, with
    /// an absent log line as the only symptom.
    #[test]
    fn pinned_section_takes_its_prewarm_flag_from_the_boot_config() {
        use decdn_common::config::resolved::ResolvedOrigin;

        let section_prewarm = |prewarm: bool, origins: Vec<ResolvedOrigin>| {
            let mut initial = seed_resolved(10, LogLevel::Info);
            initial.cache.prewarm = prewarm;
            initial.cache.origins = origins;
            let (setter, _captured) = recording_setter();
            let state = RuntimeReloadState::new(
                PaymentArgs {
                    rate_per_mb: None,
                    delivery_floor: None,
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
            state.pinned.prewarm
        };

        let http = || ResolvedOrigin::Http {
            url: decdn_cache::parse_origin_url("https://origin.example/").expect("url"),
            decompress: decdn_cache::DecompressMode::Auto,
        };
        let fs = || ResolvedOrigin::Fs {
            path: PathBuf::from("/srv/origin"),
        };

        assert!(
            section_prewarm(true, vec![http()]),
            "prewarm on with a remote origin must arm the reload warm"
        );
        assert!(
            !section_prewarm(false, vec![http()]),
            "prewarm off must disarm it even with a remote origin"
        );
        assert!(
            !section_prewarm(true, vec![fs()]),
            "an fs-only chain is inert, so the reload warm must stay disarmed"
        );
        assert!(
            section_prewarm(true, vec![fs(), http()]),
            "a mixed chain warms — this is the case every doc correction in #1510 \
             is about, and it was previously covered nowhere"
        );
        assert!(
            !section_prewarm(true, Vec::new()),
            "an empty chain has nothing to warm from"
        );
    }

    /// The single-flight guard must COALESCE a losing reload, not drop it. The
    /// in-flight pass snapshots the pinned set when it starts, so a pin added
    /// after that point is invisible to it — dropping the loser outright would
    /// strand that pin until a restart, which is exactly the silent failure the
    /// dirty flag exists to prevent.
    #[tokio::test]
    async fn a_reload_that_loses_the_single_flight_race_leaves_its_work_flagged() {
        let dir = tempfile::tempdir().unwrap();
        let h = make_hex_hash(9);
        let path = write_config(dir.path(), &format!("[cache]\npinned_hashes = [\"{h}\"]\n"));

        let mut initial = seed_resolved(10, LogLevel::Info);
        initial.cache.prewarm = true;
        initial.cache.origins = vec![decdn_common::config::resolved::ResolvedOrigin::Http {
            url: decdn_cache::parse_origin_url("https://origin.example/").expect("url"),
            decompress: decdn_cache::DecompressMode::Auto,
        }];
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
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
        let (cache, _tmp_cache) = build_test_cache().await;
        state.attach_cache(Some(cache.clone()));

        // Stand in for a pass already in flight: hold the guard for the whole
        // reload, so the spawned task's `try_lock` is guaranteed to fail.
        let held = state
            .pinned
            .warm_in_flight
            .clone()
            .try_lock_owned()
            .unwrap();
        state.reload(&path).await.unwrap();
        // Let the losing task run to its `try_lock` and return.
        tokio::task::yield_now().await;

        assert!(
            state.pinned.warm_dirty.load(Ordering::SeqCst),
            "the losing reload must leave the dirty flag set so the holder re-runs"
        );
        assert!(
            state.pinned.warm_wanted.load(Ordering::SeqCst),
            "and must leave the warm owed, since it added a pin"
        );
        assert_eq!(
            cache.pinned_snapshot().len(),
            1,
            "the pin itself still landed"
        );
        drop(held);
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

    /// The `[payment]` restart notice is gated on `voucher_interval_mb`
    /// only: `rate_per_mb` reloads and the delivery bounds are warned
    /// change-based in `PaymentSection::infallible_swap`, so neither trips
    /// this gate; `voucher_interval_mb` must.
    #[test]
    fn payment_notice_gate_covers_only_voucher_interval() {
        use decdn_common::config::types::PaymentConfig;

        // Reloadable field only -> no notice here.
        let rate_only = PaymentConfig {
            rate_per_mb: Some(100),
            ..PaymentConfig::default()
        };
        assert!(!payment_has_restart_required_field(&rate_only));
        // Delivery bounds are warned change-based elsewhere -> not here.
        let bounds_only = PaymentConfig {
            delivery_floor: Some(1),
            ..PaymentConfig::default()
        };
        assert!(!payment_has_restart_required_field(&bounds_only));
        assert!(!payment_has_restart_required_field(
            &PaymentConfig::default()
        ));
        // The one field with no other home -> notice.
        let voucher = PaymentConfig {
            voucher_interval_mb: Some(8),
            ..PaymentConfig::default()
        };
        assert!(payment_has_restart_required_field(&voucher));
    }
}
