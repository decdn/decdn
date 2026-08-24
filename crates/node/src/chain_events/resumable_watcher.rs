//! Resumable `eth_getLogs` scan vocabulary (#1092/#1106/#1108).
//!
//! This module owns the cursor + sink vocabulary every on-chain watcher shares;
//! the loop that drives it lives in
//! [`multiplexed_poller`](super::multiplexed_poller), which reads head once per
//! tick, scans each route's `[cursor, head]` gap in `MAX_BACKFILL_BLOCK_SPAN`
//! windows via one merged `eth_getLogs`, hands each log to the owning
//! [`LogSink`], then advances — and, for a route whose [`CursorStart`] owns a
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

use alloy::rpc::types::Log;
use anyhow::{Context, Result};
use decdn_common::redact::sanitize_rpc_display as n;
use decdn_incentive::{CheckpointKey, KeyedCheckpointStore};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::AbortOnDrop;

/// A durable per-key scan checkpoint: the store plus the key it reads and writes
/// under. Carried by the [`CursorStart`] variants that persist their cursor
/// forward — and, for [`CursorStart::FromCheckpoint`], the source its resume
/// floor is read from.
pub(crate) struct Checkpoint {
    pub(crate) store: Arc<dyn KeyedCheckpointStore>,
    pub(crate) key: CheckpointKey,
}

/// How the **first** tick derives its scan floor, and whether the cursor is
/// persisted forward.
///
/// Persistence rides inside the variants that own a [`Checkpoint`] rather than
/// on a separate field, so two invalid shapes cannot be constructed: a watcher
/// that resumes from a checkpoint cannot be configured without one, and a
/// watcher that re-derives its floor from head cannot declare a margin or window
/// it never reads.
/// Where a [`CursorStart::FromCheckpoint`] watcher starts on a first-ever boot,
/// when no cursor has ever been persisted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ColdStart {
    /// Start at **head**: nothing predates this node, so there is no history
    /// worth replaying. Settlement `ChannelOpened` — a channel opened before the
    /// node's keypair existed cannot be one of ours.
    Head,
}

pub(crate) enum CursorStart {
    /// Start at an explicit block — a bootstrap snapshot head, already covered
    /// by an out-of-band enumeration. Bypasses floor derivation entirely.
    /// `persist` carries the cursor forward when the projection has a durable
    /// checkpoint (the origin watcher's former cursor) and is `None` for an
    /// ephemeral live-follow rebuilt from its enumeration each boot (the
    /// capacity-bond staker set).
    Seeded {
        at: u64,
        persist: Option<Checkpoint>,
    },
    /// Resume from a durable checkpoint, rewound by `reorg_margin`, persisting
    /// forward each window. This is the only start that reads a checkpoint to
    /// derive its floor (settlement `ChannelOpened`, blacklist deny-set).
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
            Self::Seeded { persist, .. } => persist.as_ref(),
            Self::FromCheckpoint { checkpoint, .. } => Some(checkpoint),
            Self::HeadMinusWindow { .. } => None,
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
    /// [`Self::HeadMinusWindow`] and unpersisted [`Self::Seeded`] starts).
    /// Best-effort: a lost write only widens the next rescan (see the store's
    /// monotonic-floor contract).
    pub(crate) fn persist(&self, block: u64) {
        if let Some(cp) = self.checkpoint()
            && let Err(err) = cp.store.record_checkpoint(cp.key, block)
        {
            warn!(err = %n(&err), key = cp.key.as_str(), block, "failed to persist watcher checkpoint");
        }
    }

    /// Force any buffered checkpoint out on graceful shutdown.
    pub(crate) fn flush(&self) {
        if let Some(cp) = self.checkpoint()
            && let Err(err) = cp.store.flush_checkpoint(cp.key)
        {
            warn!(err = %n(&err), key = cp.key.as_str(), "failed to flush watcher checkpoint");
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
    /// Default: no-op.
    fn on_recovered(&mut self) {}
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
/// wrapped `AbortOnDrop` guard is the hard safety net when the handle drops
/// without a prior `shutdown`.
///
/// [`shutdown`]: WatcherHandle::shutdown
///
/// Public because [`multiplexed_poller::spawn`](super::multiplexed_poller::spawn)
/// returns it and external integration tests drive that spawn.
#[derive(Debug)]
pub struct WatcherHandle {
    shutdown: CancellationToken,
    _task: AbortOnDrop,
}

impl WatcherHandle {
    /// Wrap an already-minted shutdown token and its owning task. The
    /// [`multiplexed_poller`](super::multiplexed_poller) spawn mints the token
    /// and constructs this handle, so the token is always paired with a live
    /// task and can never be inert.
    pub(crate) const fn new(shutdown: CancellationToken, task: AbortOnDrop) -> Self {
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
mod tests {
    use super::*;

    // The production reorg rewind, exercised by the persisted-cursor cases.
    const MARGIN: u64 = crate::chain_events::REORG_MARGIN_BLOCKS;

    #[test]
    fn persisted_none_starts_at_head() {
        // Cold store: nothing predates the node → scan essentially nothing.
        assert_eq!(resolve_persisted_start(None, 1_000, 0, MARGIN), 1_000);
    }

    #[test]
    fn persisted_none_starts_at_head_ignoring_from_block() {
        // The cold-store floor is head, not the deploy block — a persisted-cursor
        // watcher has no history to replay before its own first channel.
        assert_eq!(resolve_persisted_start(None, 1_000, 200, MARGIN), 1_000);
    }

    #[test]
    fn persisted_some_rewinds_by_margin() {
        assert_eq!(
            resolve_persisted_start(Some(10_000), 20_000, 0, MARGIN),
            10_000 - MARGIN
        );
    }

    #[test]
    fn persisted_some_floored_at_from_block() {
        // The rewound cursor never drops below the deploy floor.
        assert_eq!(resolve_persisted_start(Some(300), 20_000, 250, MARGIN), 250);
    }

    #[test]
    fn persisted_some_clamped_to_head() {
        // A checkpoint ahead of a lagging head clamps to head, not an inverted range.
        assert_eq!(resolve_persisted_start(Some(20_000), 500, 0, MARGIN), 500);
    }

    #[test]
    fn persisted_saturates_at_zero() {
        assert_eq!(resolve_persisted_start(Some(10), 1_000, 0, MARGIN), 0);
    }

    #[test]
    fn head_window_zero_is_live_from_head() {
        // Enumeration-bootstrapped watchers: no backfill, start at head.
        assert_eq!(resolve_head_window_start(5_000, 0, 0), 5_000);
    }

    #[test]
    fn head_window_bounds_recent_lookback() {
        assert_eq!(resolve_head_window_start(50_000, 10_000, 0), 40_000);
    }

    #[test]
    fn head_window_floored_at_from_block() {
        // Slash: head - appeal_window never drops below the deploy floor.
        assert_eq!(resolve_head_window_start(1_000, 10_000, 200), 200);
    }

    #[test]
    fn head_window_floor_above_head_clamps_to_head() {
        assert_eq!(resolve_head_window_start(100, 10, 5_000), 100);
    }

    #[test]
    fn head_window_saturates_at_zero() {
        assert_eq!(resolve_head_window_start(500, 10_000, 0), 0);
    }

    // The variant → resolver wiring: each `CursorStart` must feed its own fields
    // (and the config `from_block`) into the right pure resolver. The
    // `resolve_*` tests above cover the arithmetic; these pin the hookup so a
    // mis-wired variant (e.g. `HeadMinusWindow` resolving from the wrong floor)
    // is caught.

    /// `seed` pre-sets the cursor for a `Seeded` start and only that start;
    /// every other start resolves its floor on the first tick (`seed` → `None`).
    #[test]
    fn seed_is_some_only_for_seeded() {
        assert_eq!(
            CursorStart::Seeded {
                at: 42,
                persist: None
            }
            .seed(),
            Some(42)
        );
        assert_eq!(
            CursorStart::HeadMinusWindow { window_blocks: 10 }.seed(),
            None
        );
    }

    /// A `Seeded { persist: Some(_) }` start writes its cursor forward (origin's
    /// checkpoint, which no production watcher does today. A `persist: None` seed records nothing
    /// (capacity-bond, rebuilt each boot).
    #[test]
    fn seeded_persist_drives_the_forward_checkpoint() {
        let store = Arc::new(MemoryCheckpointStore::default());
        let persisting = CursorStart::Seeded {
            at: 0,
            persist: Some(Checkpoint {
                store: Arc::clone(&store) as Arc<dyn KeyedCheckpointStore>,
                key: CheckpointKey::Origin,
            }),
        };
        persisting.persist(99);
        assert_eq!(
            store.load_checkpoint(CheckpointKey::Origin).ok().flatten(),
            Some(99),
            "a persisting seed records its cursor forward"
        );

        let ephemeral = CursorStart::Seeded {
            at: 0,
            persist: None,
        };
        assert!(
            ephemeral.checkpoint().is_none(),
            "a None-persist seed has no durable checkpoint"
        );
    }

    /// `HeadMinusWindow` resolves `head - window_blocks`, clamped to `from_block`
    /// — pins that the variant feeds its own `window_blocks` and the config floor.
    #[test]
    fn head_minus_window_initial_from_subtracts_the_window() {
        let start = CursorStart::HeadMinusWindow {
            window_blocks: 1_000,
        };
        assert_eq!(start.initial_from(500, 20_000).ok(), Some(19_000));
    }

    /// The defensive `Seeded` arm of `initial_from` is unreachable in production
    /// (`seed` pre-sets the cursor), but if reached it falls back to the deploy
    /// floor rather than panicking.
    #[test]
    fn seeded_initial_from_defensive_fallback_is_the_deploy_floor() {
        let start = CursorStart::Seeded {
            at: 5_000,
            persist: None,
        };
        assert_eq!(start.initial_from(500, 20_000).ok(), Some(500));
    }

    /// A store whose reads always fail, for the load-error policy cases.
    struct FailingLoadStore;

    impl KeyedCheckpointStore for FailingLoadStore {
        fn load_checkpoint(
            &self,
            _key: CheckpointKey,
        ) -> std::result::Result<Option<u64>, decdn_incentive::StoreError> {
            Err(decdn_incentive::StoreError::Backend("boom".into()))
        }

        fn record_checkpoint(
            &self,
            _key: CheckpointKey,
            _block: u64,
        ) -> std::result::Result<(), decdn_incentive::StoreError> {
            Ok(())
        }
    }

    /// Minimal in-memory durable store for the tick-loop tests.
    #[derive(Default)]
    struct MemoryCheckpointStore {
        stored: std::sync::Mutex<std::collections::HashMap<CheckpointKey, u64>>,
    }

    impl KeyedCheckpointStore for MemoryCheckpointStore {
        fn load_checkpoint(
            &self,
            key: CheckpointKey,
        ) -> std::result::Result<Option<u64>, decdn_incentive::StoreError> {
            Ok(self
                .stored
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&key)
                .copied())
        }

        fn record_checkpoint(
            &self,
            key: CheckpointKey,
            block: u64,
        ) -> std::result::Result<(), decdn_incentive::StoreError> {
            self.stored
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key, block);
            Ok(())
        }
    }

    /// A store that counts `flush_checkpoint` calls.
    ///
    /// `MemoryCheckpointStore` takes the trait's default no-op flush, so it
    /// cannot observe one — and `KeyedCheckpointStore::flush_checkpoint`
    /// defaulting to a no-op is exactly why a watcher whose flush never runs
    /// looks fine in every other test.
    fn persisted() -> CursorStart {
        CursorStart::FromCheckpoint {
            cold_start: ColdStart::Head,
            checkpoint: Checkpoint {
                store: Arc::new(FailingLoadStore),
                key: CheckpointKey::PoolOpened,
            },
            reorg_margin: MARGIN,
        }
    }

    /// The configured `reorg_margin` actually reaches `resolve_persisted_start`
    /// — the *wiring*, not the arithmetic.
    ///
    /// Nothing else covers this seam, which is why it exists. The
    /// `resolve_persisted_start` tests call that pure fn directly, so they prove
    /// the rewind *given* a margin. The `cursor_start` pins prove settlement's
    /// production config *carries* `REORG_MARGIN_BLOCKS`. Neither proves
    /// `initial_from` hands one to the other: hard-coding `0` at that call site
    /// passed the entire suite. Two legs of three — the same shape that let
    /// #1227 ship documented-but-disabled, one field over.
    ///
    /// The other `FromCheckpoint` tests reach the `None` arm (a `FailingLoadStore`
    /// or an empty store), which ignores the margin entirely; only a stored
    /// `Some(checkpoint)` exercises the rewind. The three constants are mutually
    /// distinct so an argument-order slip among `resolve_persisted_start`'s
    /// consecutive `u64`s fails here too.
    #[test]
    fn persisted_initial_from_engages_the_configured_margin() {
        const CHECKPOINT: u64 = 10_000;
        const HEAD: u64 = 20_000;
        const FROM_BLOCK: u64 = 500;

        let store = Arc::new(MemoryCheckpointStore::default());
        let recorded = store.record_checkpoint(CheckpointKey::PoolOpened, CHECKPOINT);
        assert!(recorded.is_ok(), "seeding the checkpoint must succeed");
        let start = CursorStart::FromCheckpoint {
            cold_start: ColdStart::Head,
            checkpoint: Checkpoint {
                store,
                key: CheckpointKey::PoolOpened,
            },
            reorg_margin: MARGIN,
        };

        assert_eq!(
            start.initial_from(FROM_BLOCK, HEAD).unwrap_or(u64::MAX),
            CHECKPOINT - MARGIN,
            "a resumed floor must be rewound by the start's own reorg_margin"
        );
    }

    #[test]
    fn load_error_is_retryable() {
        // A checkpoint read error must fail the tick into backoff so the read is
        // retried: falling back to head would durably overwrite the stored floor
        // and permanently discard the downtime gap (#751/#762).
        assert!(persisted().initial_from(0, 1_000).is_err());
    }
}
