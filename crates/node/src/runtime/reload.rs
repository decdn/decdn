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
use std::sync::atomic::{AtomicU64, Ordering};

use crate::dispatch::ConnectionLimiter;
use anyhow::Context;
use decdn_common::cli::common::LogLevel;
use decdn_common::cli::run::{ObservabilityArgs, PaymentArgs};
use decdn_common::config::{
    ConfigErrorBag, FileConfig, ResolvedObservability, ResolvedPayment, ResolvedSecurity,
    load_file_config, parse_pinned_hashes, resolve_observability_into, resolve_payment_into,
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
    /// Stable identifier for tracing event keys and snapshot-diff
    /// machinery. Returning `&'static str` keeps it cheap and
    /// non-allocating in the hot path.
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
    /// `(delivery_floor, delivery_ceiling)` as wired into the live probe
    /// handler at startup. Only `rate_per_mb` is hot-reloadable; the
    /// delivery bounds are copied into the handler by value, so a reload
    /// that changes them is accepted by `resolve_payment` but cannot take
    /// effect until restart. Retained here so the swap can emit a
    /// restart-required warning instead of silently diverging.
    applied_bounds: (u64, u64),
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
        // The probe handler holds `delivery_floor`/`delivery_ceiling` by
        // value (ADR 005 §Rate bounds validation is a locally enforced
        // seam); a changed bound is accepted by `resolve_payment` but
        // cannot take effect until restart. Surface that rather than
        // silently diverging.
        if (resolved.delivery_floor, resolved.delivery_ceiling) != self.applied_bounds {
            tracing::warn!(
                section = self.name(),
                applied_delivery_floor = self.applied_bounds.0,
                applied_delivery_ceiling = self.applied_bounds.1,
                new_delivery_floor = resolved.delivery_floor,
                new_delivery_ceiling = resolved.delivery_ceiling,
                "payment.delivery_floor/delivery_ceiling change ignored \
                 (requires restart); the live probe handler keeps the \
                 startup bounds"
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
/// need a restart), but the level is. Other sub-fields fall through
/// to the "ignored field X (requires restart)" diff machinery in
/// `log_ignored_observability`.
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
            g.as_ref().map(|engine| engine.set_pinned(&resolved))
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
    /// Iteration order for the three-phase reload. Matches the order
    /// the previous monolithic body used (`payment`, `log_level`,
    /// `pinned_hashes`, `security`) so the user-visible commit ordering
    /// across sections doesn't shift behind the refactor.
    sections: Vec<Arc<dyn ReloadableSection>>,
    /// Per-section snapshot of the *previously seen* file contents,
    /// captured as `serde_json::Value` for cheap structural diffing.
    /// Cross-cutting (not a `ReloadableSection`) — used to suppress
    /// the noisy "ignored (requires restart)" line when the operator
    /// hasn't actually changed anything in those sections between
    /// reloads. Updated only after a fully successful reload so a
    /// rejected file doesn't poison future diffs.
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
    dht: Option<serde_json::Value>,
    receipts: Option<serde_json::Value>,
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
            dht: snap_section("dht", file.dht.as_ref()),
            receipts: snap_section("receipts", file.receipts.as_ref()),
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
            applied_bounds: (
                initial.payment.delivery_floor,
                initial.payment.delivery_ceiling,
            ),
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
        });
        let security = Arc::new(SecuritySection {
            limiter: std::sync::Mutex::new(None),
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
        ];
        Self {
            payment,
            log_level,
            pinned,
            security,
            sections,
            last_file_sections: std::sync::Mutex::new(FileSectionSnapshot::default()),
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
        attach_engine_to_section(&self.pinned.engine, engine, "pinned_hashes");
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
    /// [`Self::attach_cache`] handles its slot.
    pub fn seed_initial_file_snapshot(&self, file: &decdn_common::config::FileConfig) {
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
                enable_0rtt: true,
            },
            blockchain: ResolvedBlockchain {
                origin_assignment_address: None,
                publisher_registry_address: None,
                origin_directory_from_block: 0,
                rpc_url: "http://localhost:8545".into(),
                eth_keystore: PathBuf::from("/tmp/keystore.json"),
                keystore_password_file: None,
                payment_channel_address: "0x0000000000000000000000000000000000000001".into(),
                capacity_bond_address: "0x0000000000000000000000000000000000000002".into(),
                rpc_watchdog_interval_sec: 30,
                redeem_threshold_micro_usdc: 1_000_000,
                buyer_deposit_micro_usdc: 10_000_000,
                buyer_max_approve: true,
                settlement_auto_threshold_micro_usdc: None,
                settlement_auto_by_voucher_nonce_span: None,
                slash_judge_address: "0x0000000000000000000000000000000000000003".to_string(),
                chain_id: decdn_common::config::DEFAULT_CHAIN_ID,
            },
            cache: ResolvedCache {
                cache_dir: PathBuf::from("/tmp/cache"),
                cache_size_mb: 1024,
                max_blob_size_mb: 128,
                origins: Vec::new(),
                pinned_hashes: decdn_cache::PinnedHashes::empty(),
                origin_retry: decdn_cache::RetryPolicy::default(),
                user_agent: decdn_cache::DEFAULT_USER_AGENT.to_string(),
                gc_interval_sec: 0,
                max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
                stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
                node_to_node_pull_through_enabled: false,
                node_pull_probe_fanout: decdn_common::config::DEFAULT_NODE_PULL_PROBE_FANOUT,
                node_pull_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_TIMEOUT_SEC,
            },
            payment: ResolvedPayment {
                rate_per_mb,
                delivery_floor: 0,
                delivery_ceiling: decdn_protocol::MAX_RATE_PER_MB,
                voucher_interval_mb: decdn_protocol::DEFAULT_VOUCHER_INTERVAL_MB,
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
                subscribe_reputation: true,
                reputation_publish_interval_sec: 3600,
                allowlist: Vec::new(),
                max_peer_table_entries: 100_000,
            },
            security: ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_source_rate_per_sec: 100.0,
                per_source_burst: 200,
                max_tracked_sources: 4096,
            },
            dht: decdn_common::config::ResolvedDht::default(),
            receipts: decdn_common::config::ResolvedReceipts::default(),
            prefetch: decdn_common::config::ResolvedPrefetch::default(),
        };
        Self::new(
            decdn_common::cli::run::PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
                delivery_ceiling: None,
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
    /// Fields outside the reloadable set are diffed against the
    /// previously seen file contents (snapshot stored on `self`) and a
    /// single info line is emitted only when those sections actually
    /// changed — operators see "you changed X but it needs a restart"
    /// without false positives on every routine SIGHUP.
    #[allow(
        clippy::cognitive_complexity, // Three short loops + one read scope; reads better as one unit than split apart.
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

        // Lock the snapshot mutex before the fallible-commit phase.
        // Holding it across the rest of the function serialises
        // concurrent reloads (a second SIGHUP racing the first one
        // waits here) and makes the snapshot write-back at the end
        // part of the same critical section. The lock acquisition is
        // the *last* fallible step before phase 2; a `PoisonError`
        // here surfaces before any commit runs, preserving the
        // "previous values retained on error" contract.
        let mut sections_snapshot_guard = self
            .last_file_sections
            .lock()
            .map_err(|_| anyhow::anyhow!("file-section snapshot mutex poisoned"))?;

        // Diff non-reloadable sections against the previous snapshot
        // before committing. Emitting "ignored" lines is read-only and
        // we want them out of the way before the commit step.
        //
        // The log-level section needs the freshly resolved value to
        // compare against the file's raw observability fields (the
        // diff suppresses "ignored field X" lines that match what the
        // resolver already honoured). Borrow it through the buffer's
        // mutex guard rather than cloning — `infallible_swap` still
        // needs the value and `ResolvedObservability` isn't `Clone`.
        {
            let buf_guard = self
                .log_level
                .buf
                .lock()
                .map_err(|_| anyhow::anyhow!("log-level buffer mutex poisoned"))?;
            let new_observability = buf_guard
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("log-level buffer empty after resolve"))?;
            log_ignored_fields(&file, new_observability, &sections_snapshot_guard);
        }

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

        // Write back the snapshot baseline so the next reload's diff
        // compares against this file (not the previous one).
        *sections_snapshot_guard = FileSectionSnapshot::capture(&file);
        drop(sections_snapshot_guard);

        // Final summary line, retained for backwards compatibility
        // with any operators / log scrapers that grep for it. The
        // per-section `config reload section applied` events carry
        // the structured fields; this one is the "all done" marker.
        tracing::info!("config reload applied");
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
    file_cache: Option<&decdn_common::config::types::CacheConfig>,
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
    file: &decdn_common::config::FileConfig,
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
    obs: Option<&decdn_common::config::types::ObservabilityConfig>,
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
fn log_ignored_other_sections(file: &decdn_common::config::FileConfig, prev: &FileSectionSnapshot) {
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
        warn_ignored("network.* (bind_port, relay_urls, relay_url, discovery, enable_0rtt)");
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
        warn_ignored(
            "gossip.* (announce_interval, peer_ttl, allowlist, subscribe_global, \
             subscribe_reputation, reputation_publish_interval_sec)",
        );
    }
    if changed("dht", file.dht.as_ref(), prev.dht.as_ref()) && file.dht.is_some() {
        // `dht.*` (rate-limit caps, trusted IPs) is not currently
        // hot-reloadable — the limiter is constructed once at startup.
        // Surfacing "requires restart" here keeps DHT on the same footing
        // as the other restart-required sections; reloadability follows
        // the dispatch-limiter pattern (`security.*`) and is a candidate
        // for a future change once the limiter grows an ArcSwap on its
        // inner state.
        warn_ignored("dht.* (rate-limit, trusted_ips)");
    }
    if changed("receipts", file.receipts.as_ref(), prev.receipts.as_ref())
        && file.receipts.is_some()
    {
        // `receipts.*` (rotation cap, retained backups) is read once when
        // `JsonlReceiptLog` is opened at bring-up; changing it requires a
        // restart, so surface the same "ignored (requires restart)" notice
        // as the other startup-only sections.
        warn_ignored("receipts.* (max_file_bytes, retained_files)");
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
                enable_0rtt: true,
            },
            blockchain: ResolvedBlockchain {
                origin_assignment_address: None,
                publisher_registry_address: None,
                origin_directory_from_block: 0,
                rpc_url: "http://localhost:8545".into(),
                eth_keystore: PathBuf::from("/tmp/keystore.json"),
                keystore_password_file: None,
                payment_channel_address: "0x0000000000000000000000000000000000000001".into(),
                capacity_bond_address: "0x0000000000000000000000000000000000000002".into(),
                rpc_watchdog_interval_sec: 30,
                redeem_threshold_micro_usdc: 1_000_000,
                buyer_deposit_micro_usdc: 10_000_000,
                buyer_max_approve: true,
                settlement_auto_threshold_micro_usdc: None,
                settlement_auto_by_voucher_nonce_span: None,
                slash_judge_address: "0x0000000000000000000000000000000000000003".to_string(),
                chain_id: decdn_common::config::DEFAULT_CHAIN_ID,
            },
            cache: ResolvedCache {
                cache_dir: PathBuf::from("/tmp/cache"),
                cache_size_mb: 1024,
                max_blob_size_mb: 128,
                origins: Vec::new(),
                pinned_hashes: decdn_cache::PinnedHashes::empty(),
                origin_retry: decdn_cache::RetryPolicy::default(),
                user_agent: decdn_cache::DEFAULT_USER_AGENT.to_string(),
                gc_interval_sec: 0,
                max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
                stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
                node_to_node_pull_through_enabled: false,
                node_pull_probe_fanout: decdn_common::config::DEFAULT_NODE_PULL_PROBE_FANOUT,
                node_pull_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_TIMEOUT_SEC,
            },
            payment: ResolvedPayment {
                rate_per_mb: rate,
                delivery_floor: 0,
                delivery_ceiling: decdn_protocol::MAX_RATE_PER_MB,
                voucher_interval_mb: decdn_protocol::DEFAULT_VOUCHER_INTERVAL_MB,
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
                subscribe_reputation: true,
                reputation_publish_interval_sec: 3600,
                allowlist: Vec::new(),
                max_peer_table_entries: 100_000,
            },
            security: ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_source_rate_per_sec: 100.0,
                per_source_burst: 200,
                max_tracked_sources: 4096,
            },
            receipts: decdn_common::config::ResolvedReceipts::default(),
            dht: decdn_common::config::ResolvedDht::default(),
            prefetch: decdn_common::config::ResolvedPrefetch::default(),
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
                delivery_ceiling: None,
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
        // of locking the snapshot mutex before any committing side-effect.
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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
             delivery_ceiling = 0\n",
        );

        let initial = seed_resolved(42, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
                delivery_ceiling: None,
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
            msg.contains("payment.delivery_ceiling"),
            "missing delivery_ceiling: {msg}"
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
                delivery_ceiling: None,
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
                delivery_ceiling: None,
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

    /// `seed_initial_file_snapshot` primes the diff baseline from the
    /// startup config, so the first SIGHUP after startup doesn't fall
    /// into the "no baseline → warn once" branch in
    /// `cache_changed_only_reloadable_fields`. The behaviour we can
    /// assert directly: after seeding, `last_file_sections` reflects
    /// the populated cache section instead of `Default::default()`.
    #[test]
    fn seed_initial_file_snapshot_primes_diff_baseline() {
        use decdn_common::config::FileConfig;
        use decdn_common::config::types::CacheConfig;

        let initial = seed_resolved(10, LogLevel::Info);
        let (setter, _captured) = recording_setter();
        let state = RuntimeReloadState::new(
            PaymentArgs {
                rate_per_mb: None,
                delivery_floor: None,
                delivery_ceiling: None,
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
