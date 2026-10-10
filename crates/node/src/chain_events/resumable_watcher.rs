//! Resumable `eth_getLogs` scan vocabulary (#1092/#1106/#1108).
//!
//! This module owns the cursor + sink vocabulary every on-chain watcher shares;
//! the loop that drives it lives in
//! [`multiplexed_poller`](super::multiplexed_poller), which reads head once per
//! tick, scans each route's `[cursor, head]` gap in bounded windows
//! (`blockchain.get_logs_max_block_span`) via one merged `eth_getLogs`, hands
//! each log to the owning [`LogSink`], then advances — and, for a route whose [`CursorStart`] owns a
//! [`Checkpoint`], durably records — the cursor per window. The **first tick's**
//! large range *is* the historical backfill; later ticks are the live tail. The
//! poller uses `eth_getLogs` rather than alloy's `watch_logs` (`eth_newFilter` +
//! `eth_getFilterChanges`), which the default public Arbitrum Sepolia RPC and
//! most keyless endpoints reject with `-32601` (#1106).
//!
//! Two properties matter for correctness:
//!
//! - **Resumability (#1108).** Persisting the cursor *per window* during the
//!   first big backfill means a rate-limited cold start that dies part-way
//!   through resumes near where it stopped instead of re-scanning from the
//!   configured floor. Combined with the per-tick backoff (which never exits the
//!   process — only the startup preflight does), a throttled boot makes forward
//!   progress across restarts rather than crash-looping.
//! - **Idempotency.** A tick that errors mid-window leaves the cursor at the
//!   last *completed* window, so the failed window re-scans on retry; a shallow
//!   reorg on resume re-scans `reorg_margin` blocks. Every sink must therefore
//!   apply logs idempotently (dedup by id / set insertion / authoritative
//!   re-read).

use std::future::Future;
use std::time::{Duration, Instant};

use alloy::rpc::types::Log;
use anyhow::{Context, Result};
use decdn_common::redact::sanitize_error_sources as n;
use decdn_incentive::{CheckpointKey, KeyedCheckpointStore};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::warn;

/// A durable per-key scan checkpoint: the store plus the key it reads and writes
/// under. Carried by [`CursorStart::FromCheckpoint`], which persists its cursor
/// forward and reads its resume floor back from the same key.
pub(crate) struct Checkpoint {
    pub(crate) store: Arc<dyn KeyedCheckpointStore>,
    pub(crate) key: CheckpointKey,
}

/// Where a [`CursorStart::FromCheckpoint`] watcher starts on a first-ever boot,
/// when no cursor has ever been persisted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ColdStart {
    /// Start at **head**: nothing predates this node, so there is no history
    /// worth replaying. The settlement watcher — no `PoolRedeemed` toward this
    /// node can predate the node itself. A pool opened before the head anchor
    /// emits no `PoolOpened` the watcher sees; it reaches the projection through
    /// the admit-path `getPool` read
    /// ([`ResolvingPoolView`](crate::payment_settlement::ResolvingPoolView)).
    Head,
}

/// How the **first** tick derives its scan floor, and whether the cursor is
/// persisted forward.
///
/// Persistence rides inside the variants that own a [`Checkpoint`] rather than
/// on a separate field, so two invalid shapes cannot be constructed: a watcher
/// that resumes from a checkpoint cannot be configured without one, and a
/// watcher that re-derives its floor from head cannot declare a margin or window
/// it never reads.
pub(crate) enum CursorStart {
    /// Start at an explicit block — a bootstrap snapshot block, already covered
    /// by an out-of-band enumeration. Bypasses floor derivation entirely and
    /// never persists: a seeded projection is an ephemeral live-follow rebuilt
    /// from its enumeration each boot (the capacity-bond registry, the slash
    /// store, the blacklist deny-set).
    Seeded { at: u64 },
    /// Resume from a durable checkpoint, rewound by `reorg_margin`, persisting
    /// forward each window. This is the only start that reads a checkpoint to
    /// derive its floor (the settlement watcher's `PoolOpened` checkpoint).
    ///
    /// What a first-ever (cold-store) boot does is the caller's choice — see
    /// [`ColdStart`]. Getting that wrong is a correctness bug, not a tuning
    /// knob, so it is an explicit field rather than a default.
    FromCheckpoint {
        checkpoint: Checkpoint,
        /// Blocks to rewind the checkpoint on resume (reorg safety), normally
        /// [`super::REORG_MARGIN_BLOCKS`]. Only this variant resolves a floor
        /// from a durable cursor, so it is the only reader of a margin — every
        /// other start re-derives its floor from head each boot.
        reorg_margin: u64,
        /// Floor to use when no cursor has ever been written.
        cold_start: ColdStart,
    },
    /// Re-derive the floor from head each boot as `head - window_blocks` (clamped
    /// `>= from_block`); do not persist. Used where a resume cursor is unsafe or
    /// unnecessary — a bounded recent window re-scanned every boot (the rate-bounds
    /// watcher's `window_blocks: 0` live-from-head follow). `window_blocks` is
    /// always a *bounded* recent window.
    HeadMinusWindow { window_blocks: u64 },
}

impl CursorStart {
    /// The explicit starting cursor, if this is a [`Self::Seeded`] start. `Some`
    /// pre-sets the route's cursor so the first tick scans from here and never
    /// resolves a floor.
    pub(crate) const fn seed(&self) -> Option<u64> {
        match self {
            Self::Seeded { at, .. } => Some(*at),
            Self::FromCheckpoint { .. } | Self::HeadMinusWindow { .. } => None,
        }
    }

    /// The durable checkpoint this start writes forward to (and, for
    /// [`Self::FromCheckpoint`], reads its resume floor from), if any.
    const fn checkpoint(&self) -> Option<&Checkpoint> {
        match self {
            Self::FromCheckpoint { checkpoint, .. } => Some(checkpoint),
            Self::Seeded { .. } | Self::HeadMinusWindow { .. } => None,
        }
    }

    /// Resolve the first tick's scan floor against the current scan upper bound.
    /// Reached only on the `None` cursor branch, so [`Self::Seeded`] — whose
    /// cursor is pre-set from [`Self::seed`] — never lands here and resolves to
    /// `from_block` defensively.
    ///
    /// A checkpoint read error is **retryable**: falling back to head would
    /// anchor the first persisted window at head and durably *overwrite* the
    /// stored floor, permanently discarding the downtime gap the checkpoint
    /// exists to cover (#751/#762) — so the tick fails into backoff and
    /// re-resolves next tick rather than degrading to a silent rescan.
    pub(crate) fn initial_from(&self, from_block: u64, head: u64) -> Result<u64> {
        match self {
            Self::FromCheckpoint {
                checkpoint,
                reorg_margin,
                cold_start,
            } => {
                let last = checkpoint
                    .store
                    .load_checkpoint(checkpoint.key)
                    .with_context(|| {
                        format!("read watcher scan checkpoint {}", checkpoint.key.as_str())
                    })?;
                // `ColdStart::Head`: a first-ever (cold-store) boot has no history
                // to replay, so `resolve_persisted_start`'s `None` arm anchors it
                // at head; a warm resume rewinds the stored cursor by `reorg_margin`.
                match cold_start {
                    ColdStart::Head => Ok(resolve_persisted_start(
                        last,
                        head,
                        from_block,
                        *reorg_margin,
                    )),
                }
            }
            Self::HeadMinusWindow { window_blocks } => {
                Ok(resolve_head_window_start(head, *window_blocks, from_block))
            }
            // A seeded start pre-sets the cursor (see `seed`), so the poller's
            // floor-resolution `None` branch never calls this arm. The deploy floor is the safe
            // fallback (anti-panic policy); the `warn!` makes a future break of
            // the "Seeded ⇒ cursor pre-seeded" invariant observable rather than a
            // silent full re-scan from the deploy block.
            Self::Seeded { at, .. } => {
                warn!(
                    invariant = "seeded_start_reached_initial_from",
                    seed = *at,
                    from_block,
                    "cursor seed invariant violated; falling back to deploy floor"
                );
                Ok(from_block)
            }
        }
    }

    /// Durably record `block` as scanned (no-op for the non-persisting
    /// [`Self::HeadMinusWindow`] and [`Self::Seeded`] starts).
    /// Best-effort: a lost write only widens the next rescan (see the store's
    /// monotonic-floor contract).
    pub(crate) fn persist(&self, block: u64) {
        if let Some(cp) = self.checkpoint()
            && let Err(err) = cp.store.record_checkpoint(cp.key, block)
        {
            warn!(error = %n(&err), key = cp.key.as_str(), block, "failed to persist watcher checkpoint");
        }
    }

    /// Force any buffered checkpoint out on graceful shutdown.
    pub(crate) fn flush(&self) {
        if let Some(cp) = self.checkpoint()
            && let Err(err) = cp.store.flush_checkpoint(cp.key)
        {
            warn!(error = %n(&err), key = cp.key.as_str(), "failed to flush watcher checkpoint");
        }
    }
}

/// Per-log consumer. Decodes the log and applies it to the watcher's in-memory
/// projection. Implementations MUST be idempotent under re-scan (see the module
/// doc) and MUST NOT return `Err` for an *undecodable* log — a deterministic
/// re-scan of a permanently-undecodable log would hot-loop the cursor forever,
/// so a decode failure is logged and skipped (`Ok`). Return `Err` only for a
/// *retryable* failure (a durable-write error, a follow-up RPC that should be
/// retried) so the tick backs off and re-scans the window.
pub(crate) trait LogSink: Send {
    /// Apply one log to the projection.
    fn apply(&mut self, log: Log) -> impl Future<Output = Result<()>> + Send;

    /// Run once at the end of a tick after every window drained cleanly — the
    /// seam for an authoritative reconcile (e.g. origin's `getOrigins` re-read).
    /// Default: no-op.
    fn on_tick_complete(&mut self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Run on the tick a route recovers from an errored one, before that tick's
    /// [`Self::on_tick_complete`]. The seam for forcing an authoritative re-read
    /// after an outage rather than waiting out a cadence: a sink whose reconcile
    /// is cadence-gated clears its own clock here, and its next
    /// `on_tick_complete` — the one on this same tick — re-reads.
    ///
    /// A cadence is not equivalent. The reconcile does not run at all while the
    /// route is errored, and a reconcile whose own read failed defers itself a
    /// further interval, so after a long RPC outage the first repair is whatever
    /// cadence tick happens to land after recovery, with no relationship to when
    /// the watcher came back.
    ///
    /// Sync and infallible on purpose: it exists to clear local state, and a
    /// failure here has nowhere useful to go — the reconcile that follows on
    /// this same tick is what does the work, and reports its own outcome.
    ///
    /// A sink whose `on_tick_complete` is cadence-gated wants
    /// [`clear_cadence_on_recovery`] here. Default: no-op, for a sink whose
    /// reconcile runs every tick and so has no clock to clear.
    fn on_recovered(&mut self) {}
}

/// Divisor turning a sink's own reconcile cadence into the minimum age its last
/// reconcile must have before a watcher recovery forces another.
///
/// A recovery edge fires whenever a route comes back from an errored tick, and
/// an endpoint that flaps rather than staying down produces one on roughly the
/// backoff interval. Without a floor each flap costs a full authoritative
/// re-read aimed at an endpoint that is already failing. Fifteen keeps the
/// forced repair an order of magnitude faster than the cadence — which is the
/// entire point of the edge — while bounding what a flap can cost.
const RECOVERY_FLOOR_DIVISOR: u32 = 15;

/// Clear a cadence clock so the reconcile on this same tick re-reads, unless it
/// already read within `interval / RECOVERY_FLOOR_DIVISOR`.
///
/// The idiom for [`LogSink::on_recovered`] in a sink whose `on_tick_complete`
/// re-reads authoritative state on a cadence. Such a sink is at its most stale
/// exactly when a route recovers: the reconcile is skipped entirely while the
/// route is errored, so the cadence repair has not been running, and a reconcile
/// whose own read then failed stamped the clock anyway and deferred a further
/// interval.
pub(crate) fn clear_cadence_on_recovery<C: CadenceClock>(
    clock: &mut Option<C>,
    interval: Duration,
) {
    let floor = interval / RECOVERY_FLOOR_DIVISOR;
    if clock.is_none_or(|last| last.elapsed() >= floor) {
        *clock = None;
    }
}

/// The instant types a sink stamps its cadence clock with. Sinks differ —
/// `blacklist_watcher` runs on `tokio::time` so its tests can pause the clock —
/// and [`clear_cadence_on_recovery`] only ever asks one question of either.
pub(crate) trait CadenceClock: Copy {
    /// Time since this instant was taken.
    fn elapsed(self) -> Duration;
}

impl CadenceClock for Instant {
    fn elapsed(self) -> Duration {
        Instant::elapsed(&self)
    }
}

impl CadenceClock for tokio::time::Instant {
    fn elapsed(self) -> Duration {
        tokio::time::Instant::elapsed(&self)
    }
}

/// A loop-level observability hook carried on a
/// [`Route`](super::multiplexed_poller::Route). Boxed so a watcher can close over
/// its `Arc<Metrics>` without the generic poller knowing the concrete metric.
///
/// Hooks SHOULD be panic-free: in practice every hook is a `metric_hook` closure
/// that only bumps an infallible counter/gauge. `on_task_panic` additionally runs
/// from a `Drop` guard during an unwind — where a second panic would abort the
/// process — so the poller's guard wraps that one call in `catch_unwind` as a
/// backstop; the other hooks fire on normal paths and are not guarded.
pub(crate) type WatcherHook = Box<dyn Fn() + Send + Sync>;

pub(crate) fn fire(hook: Option<&WatcherHook>) {
    if let Some(hook) = hook {
        hook();
    }
}

/// Resolve a [`CursorStart::FromCheckpoint`] watcher's initial scan floor from
/// its stored checkpoint and the current scan upper bound. `Some(b)` →
/// `b - margin` (reorg rewind), floored at `from_block` (the contract does not
/// exist below its deploy block) and clamped `<= head` (a checkpoint momentarily
/// ahead of a lagging RPC head never inverts the range). `None` (first-ever,
/// cold-store boot) → `head`: nothing predates this node, so there is no history
/// to replay. Pure so the start is unit-testable without a live provider.
/// Generalizes the settlement watcher's original `resolve_backfill_start` (which
/// was `None => head`).
pub(crate) const fn resolve_persisted_start(
    last: Option<u64>,
    head: u64,
    from_block: u64,
    margin: u64,
) -> u64 {
    match last {
        Some(b) => {
            let rewound = b.saturating_sub(margin);
            let floored = if rewound > from_block {
                rewound
            } else {
                from_block
            };
            if floored < head { floored } else { head }
        }
        None => head,
    }
}

/// Resolve a [`CursorStart::HeadMinusWindow`] watcher's floor: `head - window`,
/// clamped `>= floor` and `<= head`. `window == 0` → `head` (live-from-head, no
/// backfill). Pure and unit-testable.
pub(crate) const fn resolve_head_window_start(head: u64, window: u64, floor: u64) -> u64 {
    let start = head.saturating_sub(window);
    let floored = if start > floor { start } else { floor };
    if floored < head { floored } else { head }
}

/// Owns a running watcher task and the shutdown token that stops it. The token
/// is minted inside [`super::multiplexed_poller::spawn`] and never escapes except
/// through [`shutdown`], so no call site can conjure or drop an inert token — the
/// mistake #1230 fixed by convention, made unrepresentable (#1236). [`shutdown`]
/// is the graceful path (the loop flushes every route's cursor and returns); the
/// wrapped [`AbortOnDropHandle`] guard is the hard safety net when the handle drops
/// without a prior `shutdown`.
///
/// [`shutdown`]: WatcherHandle::shutdown
///
/// Public because [`multiplexed_poller::spawn`](super::multiplexed_poller::spawn)
/// returns it and external integration tests drive that spawn.
#[derive(Debug)]
pub struct WatcherHandle {
    shutdown: CancellationToken,
    _task: AbortOnDropHandle<()>,
}

impl WatcherHandle {
    /// Wrap an already-minted shutdown token and its owning task. The
    /// [`multiplexed_poller`](super::multiplexed_poller) spawn mints the token
    /// and constructs this handle, so the token is always paired with a live
    /// task and can never be inert.
    pub(crate) const fn new(shutdown: CancellationToken, task: AbortOnDropHandle<()>) -> Self {
        Self {
            shutdown,
            _task: task,
        }
    }

    /// Signal the loop to flush every route's cursor and return. Idempotent; the
    /// runtime calls it at the ordered stop point in graceful shutdown.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests;
