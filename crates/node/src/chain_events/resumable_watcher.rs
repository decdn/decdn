//! Resumable `eth_getLogs` polling watcher (#1092/#1106/#1108).
//!
//! One cursor loop drives every on-chain watcher: each poll tick reads head,
//! scans `[cursor, head]` in `MAX_BACKFILL_BLOCK_SPAN` windows via
//! `eth_getLogs`, hands each log to a [`LogSink`], then advances — and, for a
//! [`CursorPolicy::Persisted`] watcher, durably records — the cursor per
//! window. The **first tick's** large range *is* the historical backfill; later
//! ticks are the live tail. This replaces alloy's `watch_logs`
//! (`eth_newFilter` + `eth_getFilterChanges`), which the default public
//! Arbitrum Sepolia RPC and most keyless endpoints reject with `-32601` (#1106).
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
use std::time::Duration;

use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use anyhow::{Context, Result};
use decdn_common::redact::{sanitize_err_chain, sanitize_rpc_display as n};
use decdn_incentive::{CheckpointKey, KeyedCheckpointStore};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::shared_head::HeadSource;
use super::{backfill_windows, timed};

/// What a first-ever boot (no persisted checkpoint) falls back to for a
/// [`CursorPolicy::Persisted`] watcher.
#[derive(Debug, Clone, Copy)]
pub(crate) enum NoneFallback {
    /// Nothing existed before this node did (e.g. settlement `ChannelOpened`):
    /// start at head, scanning essentially nothing.
    Head,
    /// The stream must be replayed from the deploy block for correctness
    /// because the contract exposes no current-state enumeration (origin's
    /// `ContentClaimed` cursor — blacklist has the same constraint but uses
    /// [`CursorPolicy::FullReplay`] instead of a persisted cursor): start at
    /// the configured `from_block` (deploy block).
    FromBlock,
}

/// How a watcher derives its initial scan floor and whether it persists progress.
pub(crate) enum CursorPolicy {
    /// Resume from a durable per-key checkpoint, rewound by `reorg_margin`;
    /// persist forward each window. First boot uses `none_fallback`.
    Persisted {
        store: Arc<dyn KeyedCheckpointStore>,
        key: CheckpointKey,
        none_fallback: NoneFallback,
        /// Blocks to rewind the checkpoint on resume (reorg safety), normally
        /// [`super::REORG_MARGIN_BLOCKS`].
        ///
        /// Lives in this variant rather than on [`WatcherConfig`] because only
        /// this arm reads it: it is the rewind applied to a *durable* cursor,
        /// and the other two policies re-derive their floor from head on every
        /// boot. As a `WatcherConfig` field it was inert for five of the six
        /// watchers — four of which passed a real 128 into something that never
        /// read it (#1227).
        ///
        /// Moving it here shrinks that to two sites but does not fully close it:
        /// a [`WatcherConfig::seed_cursor`] watcher never resolves a floor, so
        /// the origin directory still carries a margin it cannot read. Only the
        /// settlement watcher actually rewinds. Splitting this variant's
        /// persistence and floor-derivation axes is what would make that
        /// unrepresentable (#1238).
        reorg_margin: u64,
    },
    /// Re-derive the floor from head each boot as `head - window_blocks`
    /// (clamped `>= floor`); do not persist. Used where a resume cursor is
    /// unsafe or unnecessary: slash (re-scan the appeal window every boot),
    /// reputation (bounded recent window), and the enumeration-bootstrapped
    /// live-follow watchers (`window_blocks = 0` → start at head). `window_blocks`
    /// is always a *bounded* recent window here — full-history replay is
    /// [`Self::FullReplay`], not a giant window.
    HeadMinusWindow { window_blocks: u64, floor: u64 },
    /// Replay the entire stream from `floor` (the deploy block) on **every** boot;
    /// do not persist. For an in-memory projection with no on-chain enumeration
    /// source, where a resume cursor would drop entries that must be rebuilt (the
    /// blacklist deny-set — see `blacklist_watcher`).
    FullReplay { floor: u64 },
}

impl CursorPolicy {
    /// Resolve the first tick's scan floor against the current scan upper bound.
    ///
    /// A checkpoint read error is **retryable** for a [`NoneFallback::Head`]
    /// watcher: falling back to head would anchor the first persisted window at
    /// head and durably *overwrite* the stored floor, permanently discarding the
    /// downtime gap the checkpoint exists to cover (#751/#762) — so the tick
    /// fails into backoff and re-resolves next tick. For [`NoneFallback::FromBlock`]
    /// the fallback is the deploy floor, where the worst case of a lost
    /// checkpoint is only a wider (idempotent) rescan, so a read error degrades
    /// to the fallback with a warning.
    fn initial_from(&self, from_block: u64, head: u64) -> Result<u64> {
        match self {
            Self::Persisted {
                store,
                key,
                none_fallback,
                reorg_margin,
            } => {
                let last = match (store.load_checkpoint(*key), none_fallback) {
                    (Ok(v), _) => v,
                    (Err(err), NoneFallback::Head) => {
                        return Err(anyhow::Error::new(err)).with_context(|| {
                            format!("read watcher scan checkpoint {}", key.as_str())
                        });
                    }
                    (Err(err), NoneFallback::FromBlock) => {
                        warn!(
                            err = %n(&err),
                            key = key.as_str(),
                            "failed to read watcher scan checkpoint; rescanning from deploy floor"
                        );
                        None
                    }
                };
                Ok(resolve_persisted_start(
                    last,
                    head,
                    from_block,
                    *reorg_margin,
                    *none_fallback,
                ))
            }
            Self::HeadMinusWindow {
                window_blocks,
                floor,
            } => Ok(resolve_head_window_start(head, *window_blocks, *floor)),
            // Full replay from the deploy floor: `head - u64::MAX` saturates to 0,
            // clamped up to `floor` and down to `head`.
            Self::FullReplay { floor } => Ok(resolve_head_window_start(head, u64::MAX, *floor)),
        }
    }

    /// Durably record `block` as scanned (no-op for the non-persisting
    /// [`Self::HeadMinusWindow`] and [`Self::FullReplay`] policies).
    /// Best-effort: a lost write only widens the next rescan (see the store's
    /// monotonic-floor contract).
    fn persist(&self, block: u64) {
        if let Self::Persisted { store, key, .. } = self
            && let Err(err) = store.record_checkpoint(*key, block)
        {
            warn!(err = %n(&err), key = key.as_str(), block, "failed to persist watcher checkpoint");
        }
    }

    /// Force any buffered checkpoint out on graceful shutdown.
    fn flush(&self) {
        if let Self::Persisted { store, key, .. } = self
            && let Err(err) = store.flush_checkpoint(*key)
        {
            warn!(err = %n(&err), key = key.as_str(), "failed to flush watcher checkpoint");
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
}

/// Immutable per-watcher configuration for [`run`].
pub(crate) struct WatcherConfig {
    /// Shared head-block source. Every watcher reads head through the same
    /// [`super::shared_head::SharedHead`], so N watchers ticking on one endpoint
    /// cost one `eth_blockNumber` per TTL window rather than one each per tick.
    /// The value may lag by up to the TTL but never runs ahead of the chain,
    /// which is what makes it safe here — see that module's doc.
    pub(crate) head: Arc<dyn HeadSource>,
    /// Base filter (address(es) + `topic0` OR-set, plus any indexed-topic
    /// constraint such as slash's `topic2` operator). The block range is set per
    /// window.
    pub(crate) filter: Filter,
    /// Contract deploy block — the floor a `FromBlock` first boot scans from.
    pub(crate) from_block: u64,
    /// Delay between poll ticks (the chain `event_poll_interval`).
    pub(crate) poll_interval: Duration,
    /// Max block span per `eth_getLogs` window.
    pub(crate) max_backfill_span: u64,
    /// Floor-derivation and persistence policy.
    pub(crate) cursor: CursorPolicy,
    /// Initial / max failed-tick retry backoff.
    pub(crate) initial_backoff: Duration,
    pub(crate) max_backoff: Duration,
    /// Per-call timeout for each `get_logs`; `None` = the shared
    /// [`super::DEFAULT_RPC_CALL_TIMEOUT`] (10s). The head read is NOT bounded by
    /// this — it is issued by the shared [`super::shared_head::HeadSource`],
    /// which carries its own timeout.
    pub(crate) rpc_call_timeout: Option<Duration>,
    /// Cancelled on graceful shutdown; flushes the checkpoint and returns.
    ///
    /// Must be a token some owner actually cancels — the runtime for five of the
    /// six watchers, and the owning service for settlement (which cancels its
    /// own before the channel-close deadline, an ordering only it knows). That
    /// this field is a `CancellationToken` and not an `Option` enforces only
    /// *presence*, which was never the problem: three watchers used to construct
    /// one inline and drop it, so the token was present and inert, and every
    /// branch below was unreachable (#1230).
    ///
    /// Liveness is a property of the surrounding program and no type expresses
    /// it, so the enforcement is the `bootstrap` signatures: each takes a token
    /// rather than minting one, which makes `CancellationToken::new()` greppable
    /// to the runtime and to `payment_settlement`'s deliberate self-mint. It is
    /// not a proof — reviewing a change to the shutdown *sequence* is.
    pub(crate) shutdown: CancellationToken,
    /// Explicit starting cursor (origin's bootstrap snapshot head). When set,
    /// the first tick scans from here instead of resolving the policy floor.
    pub(crate) seed_cursor: Option<u64>,
    /// Log label for backoff/established diagnostics.
    pub(crate) label: &'static str,
    /// Called once each time the watcher transitions into a healthy cycle (first
    /// success, and after recovering from a backoff) — the seam each watcher
    /// wires to its `*_cycle_established` gauge (down-seconds → 0).
    pub(crate) on_established: Option<WatcherHook>,
    /// Called each time a tick fails and the watcher enters backoff — the seam
    /// each watcher wires to its `*_backoff_started` gauge.
    pub(crate) on_backoff: Option<WatcherHook>,
}

/// A loop-level observability hook (see [`WatcherConfig::on_established`] /
/// [`WatcherConfig::on_backoff`]). Boxed so a watcher can close over its
/// `Arc<Metrics>` without the generic loop knowing the concrete metric.
pub(crate) type WatcherHook = Box<dyn Fn() + Send + Sync>;

fn fire(hook: Option<&WatcherHook>) {
    if let Some(hook) = hook {
        hook();
    }
}

/// Resolve a [`CursorPolicy::Persisted`] watcher's initial scan floor from its
/// stored checkpoint and the current scan upper bound. `Some(b)` → `b - margin`
/// (reorg rewind), floored at `from_block` (the contract does not exist below
/// its deploy block) and clamped `<= head` (a checkpoint momentarily ahead of a
/// lagging RPC head never inverts the range). `None` (first-ever boot) → the
/// configured `none_fallback`: `Head` (nothing predates this node) or
/// `FromBlock` (the event stream must be replayed for correctness). Pure so the
/// policy is unit-testable without a live provider. Generalizes the settlement
/// watcher's original `resolve_backfill_start` (which was `None => head`).
pub(crate) const fn resolve_persisted_start(
    last: Option<u64>,
    head: u64,
    from_block: u64,
    margin: u64,
    none_fallback: NoneFallback,
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
        None => match none_fallback {
            NoneFallback::Head => head,
            NoneFallback::FromBlock => {
                if from_block < head {
                    from_block
                } else {
                    head
                }
            }
        },
    }
}

/// Resolve a [`CursorPolicy::HeadMinusWindow`] watcher's floor: `head - window`,
/// clamped `>= floor` and `<= head`. `window == 0` → `head` (live-from-head, no
/// backfill). Pure and unit-testable.
pub(crate) const fn resolve_head_window_start(head: u64, window: u64, floor: u64) -> u64 {
    let start = head.saturating_sub(window);
    let floored = if start > floor { start } else { floor };
    if floored < head { floored } else { head }
}

/// Run one poll tick: read head, scan `[cursor, head]` in windows, apply each
/// log, advancing + persisting the cursor per completed window. Returns `Err` on
/// any retryable failure (RPC error/timeout or a sink's retryable error) with
/// the cursor left at the last completed window.
async fn run_tick<P, S>(
    provider: &P,
    cfg: &WatcherConfig,
    sink: &mut S,
    cursor: &mut Option<u64>,
) -> Result<()>
where
    P: Provider + Clone,
    S: LogSink,
{
    // The context stays here rather than in the head source so a failed head read
    // renders identically to before the read was shared, and so a test fake
    // produces the same error shape as production.
    let to = cfg.head.head().await.context("read head block")?;
    let from = match *cursor {
        Some(c) => c,
        None => cfg.cursor.initial_from(cfg.from_block, to)?,
    };
    if from > to {
        // Head has not advanced past the cursor yet (idle tick). Retain the
        // resolved floor so the next tick does not re-resolve.
        *cursor = Some(from);
    } else {
        for (start, end) in backfill_windows(from, to, cfg.max_backfill_span) {
            // Check between windows so a large first-boot backfill (blacklist's
            // full replay, slash's appeal-window span) yields promptly to a
            // graceful shutdown rather than blocking it until the whole tick
            // completes. Progress persisted per window resumes on the next boot.
            if cfg.shutdown.is_cancelled() {
                return Ok(());
            }
            let filter = cfg.filter.clone().from_block(start).to_block(end);
            let logs = timed(cfg.rpc_call_timeout, "get_logs", provider.get_logs(&filter))
                .await
                .with_context(|| format!("get_logs [{start}, {end}] for {}", cfg.label))?;
            for log in logs {
                sink.apply(log).await?;
            }
            // Window drained: advance the cursor and (Persisted only) persist it,
            // so a mid-backfill crash resumes here rather than at the floor (#1108).
            *cursor = Some(end.saturating_add(1));
            cfg.cursor.persist(end);
        }
    }
    // Always run the end-of-tick reconcile, including on an idle tick — a sink
    // whose `on_tick_complete` is a periodic, time-gated pass (e.g. the blacklist
    // re-scope that catches no-event scope transitions) would otherwise never run
    // on a quiet chain where every tick has `from > to`.
    sink.on_tick_complete().await?;
    Ok(())
}

/// Drive `sink` from `cfg.filter` on an `eth_getLogs` polling loop until
/// `cfg.shutdown` is cancelled. The outer loop ticks on `cfg.poll_interval`,
/// resetting the backoff on a clean tick and backing off (bounded) on a failing
/// one; the cursor is carried across ticks so backfill flows straight into the
/// live tail.
pub(crate) async fn run<P, S>(provider: P, cfg: WatcherConfig, mut sink: S)
where
    P: Provider + Clone,
    S: LogSink,
{
    let mut cursor: Option<u64> = cfg.seed_cursor;
    let mut backoff = cfg.initial_backoff;
    let mut established = false;
    let mut ticker = tokio::time::interval(cfg.poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cfg.shutdown.cancelled() => {
                cfg.cursor.flush();
                return;
            }
            _ = ticker.tick() => {}
        }
        match run_tick(&provider, &cfg, &mut sink, &mut cursor).await {
            Ok(()) => {
                backoff = cfg.initial_backoff;
                if !established {
                    established = true;
                    fire(cfg.on_established.as_ref());
                    debug!(label = cfg.label, "watcher established getLogs stream");
                }
            }
            Err(err) => {
                established = false;
                fire(cfg.on_backoff.as_ref());
                // The chain, not the checkpoint store: render the full cause
                // chain. This error is a `timed` bound ("get_logs timed out
                // after 10s") under whatever context the tick added above it
                // ("getChannel for closing reconciliation of 0x…"), and plain
                // Display would print only the latter — naming what was
                // attempted while dropping why it failed.
                warn!(
                    label = cfg.label,
                    err = %sanitize_err_chain(&err),
                    backoff_secs = backoff.as_secs(),
                    "watcher RPC error; restarting after backoff"
                );
                tokio::select! {
                    biased;
                    () = cfg.shutdown.cancelled() => { cfg.cursor.flush(); return; }
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(cfg.max_backoff);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::shared_head::SharedHead;
    use super::*;

    // The production reorg rewind, exercised by the persisted-cursor cases.
    const MARGIN: u64 = crate::chain_events::REORG_MARGIN_BLOCKS;

    #[test]
    fn persisted_none_head_starts_at_head() {
        // Settlement: nothing predates the node → scan essentially nothing.
        assert_eq!(
            resolve_persisted_start(None, 1_000, 0, MARGIN, NoneFallback::Head),
            1_000
        );
    }

    #[test]
    fn persisted_none_from_block_starts_at_from_block() {
        // Origin: no enumeration view for claims → replay from the deploy block.
        assert_eq!(
            resolve_persisted_start(None, 1_000, 200, MARGIN, NoneFallback::FromBlock),
            200
        );
    }

    #[test]
    fn persisted_none_from_block_clamped_to_head() {
        // A deploy block momentarily above a lagging RPC head never inverts.
        assert_eq!(
            resolve_persisted_start(None, 50, 200, MARGIN, NoneFallback::FromBlock),
            50
        );
    }

    #[test]
    fn persisted_some_rewinds_by_margin() {
        assert_eq!(
            resolve_persisted_start(Some(10_000), 20_000, 0, MARGIN, NoneFallback::Head),
            10_000 - MARGIN
        );
    }

    #[test]
    fn persisted_some_floored_at_from_block() {
        // The rewound cursor never drops below the deploy floor.
        assert_eq!(
            resolve_persisted_start(Some(300), 20_000, 250, MARGIN, NoneFallback::FromBlock),
            250
        );
    }

    #[test]
    fn persisted_some_clamped_to_head() {
        // A checkpoint ahead of a lagging head clamps to head, not an inverted range.
        assert_eq!(
            resolve_persisted_start(Some(20_000), 500, 0, MARGIN, NoneFallback::Head),
            500
        );
    }

    #[test]
    fn persisted_saturates_at_zero() {
        assert_eq!(
            resolve_persisted_start(Some(10), 1_000, 0, MARGIN, NoneFallback::Head),
            0
        );
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
    #[derive(Default)]
    struct FlushCountingStore {
        flushes: std::sync::atomic::AtomicUsize,
    }

    impl FlushCountingStore {
        fn flushes(&self) -> usize {
            self.flushes.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl KeyedCheckpointStore for FlushCountingStore {
        fn load_checkpoint(
            &self,
            _key: CheckpointKey,
        ) -> std::result::Result<Option<u64>, decdn_incentive::StoreError> {
            Ok(None)
        }

        fn record_checkpoint(
            &self,
            _key: CheckpointKey,
            _block: u64,
        ) -> std::result::Result<(), decdn_incentive::StoreError> {
            Ok(())
        }

        fn flush_checkpoint(
            &self,
            _key: CheckpointKey,
        ) -> std::result::Result<(), decdn_incentive::StoreError> {
            self.flushes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// Sink scripted to fail on the N-th apply (0-indexed), counting deliveries.
    struct ScriptedSink {
        applied: usize,
        fail_on: Option<usize>,
    }

    impl LogSink for ScriptedSink {
        async fn apply(&mut self, _log: Log) -> Result<()> {
            if self.fail_on == Some(self.applied) {
                anyhow::bail!("scripted sink failure");
            }
            self.applied += 1;
            Ok(())
        }
    }

    /// One default log, typed so the mock transport can serialize it as a
    /// `get_logs` response.
    fn one_log() -> Vec<Log> {
        vec![Log::default()]
    }

    /// Build a `run_tick` config over the mocked provider: span-10 windows,
    /// no reorg rewind, persisting through `store`.
    ///
    /// The `Duration::ZERO` head TTL is load-bearing. These tests push head and
    /// `get_logs` responses onto one ordered `Asserter` queue, so a cached head
    /// would skip a queued `U64` and hand it to the *next* `get_logs` instead —
    /// surfacing as a deserialization error that looks nothing like the cause.
    fn tick_cfg<P: Provider + 'static>(
        provider: P,
        store: Arc<MemoryCheckpointStore>,
    ) -> WatcherConfig {
        WatcherConfig {
            head: Arc::new(SharedHead::with_ttl(provider, Duration::ZERO, None)),
            filter: Filter::new(),
            from_block: 0,
            poll_interval: Duration::from_secs(1),
            max_backfill_span: 10,
            cursor: CursorPolicy::Persisted {
                store,
                key: CheckpointKey::ChannelOpened,
                none_fallback: NoneFallback::FromBlock,
                reorg_margin: 0,
            },
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            rpc_call_timeout: None,
            shutdown: CancellationToken::new(),
            seed_cursor: None,
            label: "test",
            on_established: None,
            on_backoff: None,
        }
    }

    /// The anti-strand invariant, end to end through `run_tick` on a mocked
    /// transport: a sink `Err` mid-backfill aborts the tick with the in-memory
    /// cursor AND the durable checkpoint at the last *completed* window, and
    /// the retry tick re-delivers the failed window's log before flowing on.
    /// Pins the apply-before-persist ordering a refactor could silently break
    /// (persisting per tick, or hoisting the persist above the log loop, would
    /// strand a `ChannelOpened` past the durable floor forever).
    #[tokio::test]
    async fn sink_error_leaves_cursor_and_checkpoint_at_last_completed_window() {
        use alloy::primitives::U64;
        use alloy::providers::ProviderBuilder;

        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let store = Arc::new(MemoryCheckpointStore::default());
        let cfg = tick_cfg(provider.clone(), Arc::clone(&store));
        let load = |store: &MemoryCheckpointStore| {
            store
                .load_checkpoint(CheckpointKey::ChannelOpened)
                .ok()
                .flatten()
        };

        // Tick 1: head 25 → windows [0,9], [10,19], [20,25]. One log per queried
        // window; the sink fails on the second delivery (window [10,19]).
        asserter.push_success(&U64::from(25));
        asserter.push_success(&one_log());
        asserter.push_success(&one_log());
        let mut sink = ScriptedSink {
            applied: 0,
            fail_on: Some(1),
        };
        let mut cursor = None;
        let result = run_tick(&provider, &cfg, &mut sink, &mut cursor).await;
        assert!(result.is_err(), "the failed window must fail the tick");
        assert_eq!(cursor, Some(10), "cursor at the last completed window");
        assert_eq!(
            load(&store),
            Some(9),
            "durable checkpoint must not advance past the failed window"
        );
        assert_eq!(sink.applied, 1, "only window [0,9]'s log applied");

        // Retry tick: same head; the failed window re-scans and re-delivers its
        // log, then the final window drains empty and the cursor reaches head+1.
        asserter.push_success(&U64::from(25));
        asserter.push_success(&one_log());
        asserter.push_success(&Vec::<Log>::new());
        sink.fail_on = None;
        let result = run_tick(&provider, &cfg, &mut sink, &mut cursor).await;
        assert!(result.is_ok(), "retry tick should complete: {result:?}");
        assert_eq!(cursor, Some(26));
        assert_eq!(load(&store), Some(25));
        assert_eq!(sink.applied, 2, "the failed window's log was re-delivered");
    }

    /// An idle tick (`cursor` already at/above the confirmed head) scans no
    /// windows, keeps the durable checkpoint untouched, and still succeeds —
    /// the seam `on_tick_complete`-driven sinks (blacklist re-scope, origin
    /// deferred re-reads) rely on firing every tick on a quiet chain.
    #[tokio::test]
    async fn idle_tick_scans_nothing_and_retains_cursor() {
        use alloy::primitives::U64;
        use alloy::providers::ProviderBuilder;

        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let store = Arc::new(MemoryCheckpointStore::default());
        let cfg = tick_cfg(provider.clone(), Arc::clone(&store));

        // Cursor already past the head → no get_logs response queued at all.
        asserter.push_success(&U64::from(25));
        let mut sink = ScriptedSink {
            applied: 0,
            fail_on: None,
        };
        let mut cursor = Some(26);
        let result = run_tick(&provider, &cfg, &mut sink, &mut cursor).await;
        assert!(result.is_ok(), "idle tick should succeed: {result:?}");
        assert_eq!(cursor, Some(26), "cursor retained");
        assert_eq!(sink.applied, 0);
        let stored = store
            .load_checkpoint(CheckpointKey::ChannelOpened)
            .ok()
            .flatten();
        assert_eq!(stored, None, "idle tick persists nothing");
    }

    /// A failing head read still fails the tick with the `read head block`
    /// context, so `run` fires `on_backoff` and retries. Pins that routing the
    /// head through the shared source did not swallow the error or move the
    /// context off the loop (a head source that added its own would double it,
    /// and one that dropped it would leave a bare transport error in the log).
    #[tokio::test]
    async fn head_read_failure_fails_the_tick() {
        use alloy::providers::ProviderBuilder;

        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let store = Arc::new(MemoryCheckpointStore::default());
        let cfg = tick_cfg(provider.clone(), Arc::clone(&store));

        asserter.push_failure_msg("head is down");
        let mut sink = ScriptedSink {
            applied: 0,
            fail_on: None,
        };
        let mut cursor = None;
        let err = run_tick(&provider, &cfg, &mut sink, &mut cursor)
            .await
            .err()
            .map(|e| format!("{e:#}"));
        assert!(
            err.as_ref().is_some_and(|e| e.contains("read head block")),
            "head failure must fail the tick with its context: {err:?}"
        );
        assert_eq!(cursor, None, "a tick that never read head advances nothing");
        assert_eq!(sink.applied, 0);
    }

    /// Cancelling the shutdown token makes `run` flush the checkpoint and
    /// return.
    ///
    /// This is the mechanism the whole graceful-shutdown path rests on, and
    /// nothing exercised it: every other test here drives `run_tick` directly,
    /// so `run`'s two flush arms and `CursorPolicy::flush` had no coverage at
    /// all. That gap is what let three watchers ship with a token nothing could
    /// cancel (#1230) — an unreachable branch looks no different from a
    /// reachable one when neither is tested.
    ///
    /// `run` is spawned rather than awaited: it loops until cancelled, so a
    /// bare `.await` would hang forever on success.
    #[tokio::test(start_paused = true)]
    async fn cancelling_shutdown_flushes_the_checkpoint_and_returns() {
        use alloy::providers::ProviderBuilder;

        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let store = Arc::new(FlushCountingStore::default());
        let shutdown = CancellationToken::new();

        let mut cfg = tick_cfg(provider.clone(), Arc::new(MemoryCheckpointStore::default()));
        cfg.cursor = CursorPolicy::Persisted {
            store: Arc::clone(&store) as Arc<dyn KeyedCheckpointStore>,
            key: CheckpointKey::ChannelOpened,
            none_fallback: NoneFallback::FromBlock,
            reorg_margin: 0,
        };
        cfg.shutdown = shutdown.clone();

        // Cancel before spawning: the loop's first act is the `biased` select on
        // the token, so the flush arm is taken deterministically and no queued
        // `Asserter` response is needed.
        shutdown.cancel();
        let task = tokio::spawn(run(
            provider,
            cfg,
            ScriptedSink {
                applied: 0,
                fail_on: None,
            },
        ));
        assert!(task.await.is_ok(), "run must return, not hang or panic");
        assert_eq!(
            store.flushes(),
            1,
            "a cancelled watcher must flush its checkpoint exactly once"
        );
    }

    /// A multi-window backfill yields to a cancel *between* windows: the window
    /// in flight finishes and persists, and the next one never starts.
    ///
    /// `run_tick`'s cancel check cites "slash's appeal-window span" as a case it
    /// exists for — but slash's token was inert, so the check never fired for
    /// one of the two watchers it names, on the longest tick in the system
    /// (~1037 windows). #1230 gives slash a live token; this pins the behaviour
    /// that now reaches it.
    ///
    /// The cancel fires from inside the sink, mid-window-1. Cancelling before
    /// the call instead would only prove that a tick cancelled up front scans
    /// nothing — the check would fire at window 1 and the *between*-windows
    /// property, which is the whole point, would go untested.
    #[tokio::test]
    async fn cancel_between_windows_stops_the_backfill_at_a_window_boundary() {
        use alloy::primitives::U64;
        use alloy::providers::ProviderBuilder;

        /// Cancels the token as it applies its first log — i.e. while window 1
        /// is still draining.
        struct CancelOnApply {
            applied: usize,
            shutdown: CancellationToken,
        }

        impl LogSink for CancelOnApply {
            async fn apply(&mut self, _log: Log) -> Result<()> {
                self.applied += 1;
                self.shutdown.cancel();
                Ok(())
            }
        }

        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let store = Arc::new(MemoryCheckpointStore::default());
        let shutdown = CancellationToken::new();
        let mut cfg = tick_cfg(provider.clone(), Arc::clone(&store));
        cfg.shutdown = shutdown.clone();

        // head=25 over span-10 windows → [0,9], [10,19], [20,25]. Only window
        // 1's response is queued: an `Asserter` errors on an empty queue, so if
        // the tick did NOT stop between windows the second `get_logs` would fail
        // the tick. The `is_ok` below therefore asserts the stop rather than
        // merely coexisting with it.
        asserter.push_success(&U64::from(25));
        asserter.push_success(&one_log());

        let mut sink = CancelOnApply {
            applied: 0,
            shutdown: shutdown.clone(),
        };
        let mut cursor = None;
        let result = run_tick(&provider, &cfg, &mut sink, &mut cursor).await;

        assert!(result.is_ok(), "a cancelled tick exits cleanly: {result:?}");
        assert_eq!(sink.applied, 1, "window 1 drained before the cancel landed");
        assert_eq!(
            cursor,
            Some(10),
            "the in-flight window completes and advances the cursor to the next \
             window's start; progress is not discarded by the cancel"
        );
        let stored = store
            .load_checkpoint(CheckpointKey::ChannelOpened)
            .ok()
            .flatten();
        assert_eq!(
            stored,
            Some(9),
            "the completed window is persisted, so the next boot resumes here"
        );
    }

    fn persisted(none_fallback: NoneFallback) -> CursorPolicy {
        CursorPolicy::Persisted {
            store: Arc::new(FailingLoadStore),
            key: CheckpointKey::ChannelOpened,
            none_fallback,
            reorg_margin: MARGIN,
        }
    }

    /// The configured `reorg_margin` actually reaches `resolve_persisted_start`
    /// — the *wiring*, not the arithmetic.
    ///
    /// Nothing else covers this seam, which is why it exists. The seven
    /// `resolve_persisted_start` tests call that pure fn directly, so they prove
    /// the rewind *given* a margin. The `cursor_policy` pins prove settlement's
    /// production config *carries* `REORG_MARGIN_BLOCKS`. Neither proves
    /// `initial_from` hands one to the other: hard-coding `0` at that call site
    /// passed the entire suite. Two legs of three — the same shape that let
    /// #1227 ship documented-but-disabled, one field over.
    ///
    /// Every other `Persisted` test reaches the `None` arm (a `FailingLoadStore`
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
        let recorded = store.record_checkpoint(CheckpointKey::ChannelOpened, CHECKPOINT);
        assert!(recorded.is_ok(), "seeding the checkpoint must succeed");
        let policy = CursorPolicy::Persisted {
            store,
            key: CheckpointKey::ChannelOpened,
            none_fallback: NoneFallback::Head,
            reorg_margin: MARGIN,
        };

        assert_eq!(
            policy.initial_from(FROM_BLOCK, HEAD).unwrap_or(u64::MAX),
            CHECKPOINT - MARGIN,
            "a resumed floor must be rewound by the policy's own reorg_margin"
        );
    }

    #[test]
    fn load_error_is_retryable_for_head_fallback() {
        // Falling back to head would durably overwrite the stored floor and
        // permanently discard the downtime gap (#751/#762) — the tick must fail
        // into backoff instead so the read is retried.
        assert!(
            persisted(NoneFallback::Head)
                .initial_from(0, 1_000)
                .is_err()
        );
    }

    #[test]
    fn load_error_degrades_to_deploy_floor_for_from_block_fallback() {
        // For a deploy-floor watcher the worst case of a lost checkpoint is a
        // wider idempotent rescan, so a read error degrades instead of failing.
        let start = persisted(NoneFallback::FromBlock)
            .initial_from(200, 1_000)
            .unwrap_or(u64::MAX);
        assert_eq!(start, 200);
    }
}
