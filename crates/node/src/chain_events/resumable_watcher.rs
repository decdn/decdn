//! Resumable `eth_getLogs` polling watcher (#1092/#1106/#1108).
//!
//! One cursor loop drives every on-chain watcher: each poll tick reads head,
//! scans `[cursor, head - confirmations]` in `MAX_BACKFILL_BLOCK_SPAN` windows
//! via `eth_getLogs`, hands each log to a [`LogSink`], then advances — and, for
//! a [`CursorPolicy::Persisted`] watcher, durably records — the cursor per
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
use decdn_common::redact::sanitize_rpc_display as n;
use decdn_incentive::{CheckpointKey, KeyedCheckpointStore};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

// The windowing/reorg primitives are shared with the settlement bootstrap and
// the buyer-side reconciliation scan; they live in `payment_settlement` for now
// and are re-used here rather than duplicated.
use crate::payment_settlement::backfill_windows;

/// What a first-ever boot (no persisted checkpoint) falls back to for a
/// [`CursorPolicy::Persisted`] watcher.
#[derive(Debug, Clone, Copy)]
pub(crate) enum NoneFallback {
    /// Nothing existed before this node did (e.g. settlement `ChannelOpened`):
    /// start at head, scanning essentially nothing.
    Head,
    /// The stream must be replayed from genesis for correctness because the
    /// contract exposes no current-state enumeration (blacklist, origin): start
    /// at the configured `from_block` (deploy block).
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
    },
    /// Re-derive the floor from head each boot as `head - window_blocks`
    /// (clamped `>= floor`); do not persist. Used where a resume cursor is
    /// unsafe or unnecessary: slash (re-scan the appeal window every boot),
    /// reputation (bounded recent window), and the enumeration-bootstrapped
    /// live-follow watchers (`window_blocks = 0` → start at head).
    HeadMinusWindow { window_blocks: u64, floor: u64 },
}

impl CursorPolicy {
    /// Resolve the first tick's scan floor against the current scan upper bound.
    fn initial_from(&self, from_block: u64, head: u64, reorg_margin: u64) -> u64 {
        match self {
            Self::Persisted {
                store,
                key,
                none_fallback,
            } => {
                let last = match store.load_checkpoint(*key) {
                    Ok(v) => v,
                    Err(err) => {
                        warn!(
                            err = %n(&err),
                            key = key.as_str(),
                            "failed to read watcher scan checkpoint; using none-fallback floor"
                        );
                        None
                    }
                };
                resolve_persisted_start(last, head, from_block, reorg_margin, *none_fallback)
            }
            Self::HeadMinusWindow {
                window_blocks,
                floor,
            } => resolve_head_window_start(head, *window_blocks, *floor),
        }
    }

    /// Durably record `block` as scanned (no-op for [`Self::HeadMinusWindow`]).
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
    /// Base filter (address(es) + `topic0` OR-set, plus any indexed-topic
    /// constraint such as slash's `topic2` operator). The block range is set per
    /// window.
    pub(crate) filter: Filter,
    /// Contract deploy block — the floor a `FromBlock` first boot scans from.
    pub(crate) from_block: u64,
    /// Delay between poll ticks (the chain `event_poll_interval`).
    pub(crate) poll_interval: Duration,
    /// Confirmation lag: the scan upper bound is `head - confirmations`, so the
    /// live tail never emits logs from the unstable chain tip.
    pub(crate) confirmations: u64,
    /// Blocks to rewind a persisted checkpoint on resume (reorg safety).
    pub(crate) reorg_margin: u64,
    /// Max block span per `eth_getLogs` window.
    pub(crate) max_backfill_span: u64,
    /// Floor-derivation and persistence policy.
    pub(crate) cursor: CursorPolicy,
    /// Initial / max resubscribe backoff.
    pub(crate) initial_backoff: Duration,
    pub(crate) max_backoff: Duration,
    /// Per-RPC-call timeout (head read + each `get_logs`); `None` = no timeout.
    pub(crate) rpc_call_timeout: Option<Duration>,
    /// Cancelled on graceful shutdown; flushes the checkpoint and returns.
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

/// The scan upper bound for a given head: lag by `confirmations` so the live
/// tail never emits logs from the unstable chain tip.
const fn scan_upper_bound(head: u64, confirmations: u64) -> u64 {
    head.saturating_sub(confirmations)
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

/// Fallback per-RPC-call timeout when a watcher does not set its own. The alloy
/// HTTP provider has no request timeout of its own, so a provider that keeps the
/// connection open but never responds would otherwise wedge the tick forever —
/// silently stopping event processing and blocking graceful shutdown. A bounded
/// default makes such a call fail fast into the retry/backoff path instead.
const DEFAULT_RPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Apply a per-call timeout to an RPC future, mapping its error into `anyhow`. A
/// timeout is a retryable error (the tick backs off). A `None` config uses
/// [`DEFAULT_RPC_CALL_TIMEOUT`] — no call runs unbounded, so a stalled provider
/// can never permanently wedge the watcher.
async fn timed<T, E, F>(timeout: Option<Duration>, what: &str, fut: F) -> Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    let d = timeout.unwrap_or(DEFAULT_RPC_CALL_TIMEOUT);
    tokio::time::timeout(d, fut)
        .await
        .map_err(|_| anyhow::anyhow!("{what} timed out after {d:?}"))?
        .map_err(anyhow::Error::new)
}

/// Run one poll tick: read head, scan `[cursor, head - confirmations]` in
/// windows, apply each log, advancing + persisting the cursor per completed
/// window. Returns `Err` on any retryable failure (RPC error/timeout or a
/// sink's retryable error) with the cursor left at the last completed window.
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
    let head = timed(
        cfg.rpc_call_timeout,
        "get_block_number",
        provider.get_block_number(),
    )
    .await
    .context("read head block")?;
    let to = scan_upper_bound(head, cfg.confirmations);
    let from = match *cursor {
        Some(c) => c,
        None => cfg
            .cursor
            .initial_from(cfg.from_block, to, cfg.reorg_margin),
    };
    if from > to {
        // Head has not advanced past the cursor yet (idle tick or confirmations
        // lag). Retain the resolved floor so the next tick does not re-resolve.
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
                warn!(
                    label = cfg.label,
                    err = %n(&err),
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
    use super::*;

    // The production reorg rewind, exercised by the persisted-cursor cases.
    const MARGIN: u64 = crate::payment_settlement::REORG_MARGIN_BLOCKS;

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
        // Blacklist/origin: no enumeration view → replay from the deploy block.
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

    #[test]
    fn scan_upper_bound_lags_by_confirmations() {
        assert_eq!(scan_upper_bound(1_000, 12), 988);
        assert_eq!(scan_upper_bound(5, 12), 0);
    }
}
