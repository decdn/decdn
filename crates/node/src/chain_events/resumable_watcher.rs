//! Resumable `eth_getLogs` polling watcher (#1092/#1106/#1108).
//!
//! One cursor loop drives every on-chain watcher: each poll tick reads head,
//! scans `[cursor, head]` in `MAX_BACKFILL_BLOCK_SPAN` windows via
//! `eth_getLogs`, hands each log to a [`LogSink`], then advances — and, for a
//! watcher whose [`CursorStart`] owns a [`Checkpoint`], durably records — the
//! cursor per window. The **first tick's** large range *is* the historical backfill; later
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
use tracing::{debug, error, warn};

use super::shared_head::HeadSource;
use super::{AbortOnDrop, backfill_windows, timed};

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
/// on a separate field, so the two dead-field shapes #1238 removed cannot recur:
/// a watcher that resumes from a checkpoint cannot be configured without one,
/// and a watcher that re-derives its floor from head cannot declare a margin or
/// window it never reads. This replaces the fused `CursorPolicy` + the
/// `WatcherConfig::seed_cursor` override, where a seeded watcher silently
/// bypassed floor derivation and left its policy's derivation fields inert
/// (#1227).
/// Where a [`CursorStart::FromCheckpoint`] watcher starts on a first-ever boot,
/// when no cursor has ever been persisted.
///
/// The two answers are not interchangeable: the choice is about whether on-chain
/// state predating this node is *relevant* to the projection being built.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ColdStart {
    /// Start at **head**: nothing predates this node, so there is no history
    /// worth replaying. Settlement `ChannelOpened` — a channel opened before the
    /// node's keypair existed cannot be one of ours.
    Head,
    /// Start at **`from_block`** (the deploy block) and replay the full history.
    /// For a projection that must reflect *all* pre-existing on-chain state, not
    /// merely what changed since this node first booted.
    ///
    /// Currently unconstructed: the blacklist deny-set was the last projection
    /// that needed a full-history replay, and it now enumerates its state from
    /// chain instead (#1497). Retained as the framework's full-replay floor pending
    /// a dedicated cleanup of the replay surface, rather than deleted piecemeal in a
    /// compliance change.
    #[allow(dead_code)]
    FromBlock,
}

pub(crate) enum CursorStart {
    /// Start at an explicit block — a bootstrap snapshot head, already covered
    /// by an out-of-band enumeration. Bypasses floor derivation entirely.
    /// `persist` carries the cursor forward when the projection has a durable
    /// checkpoint (origin's `AssignmentActivated` cursor) and is `None` for an
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
    /// unnecessary: slash (re-scan the appeal window every boot) and reputation
    /// (bounded recent window). `window_blocks` is always a *bounded* recent
    /// window here — full-history replay is [`Self::FullReplay`], not a giant
    /// window.
    HeadMinusWindow { window_blocks: u64 },
    /// Replay the entire stream from `from_block` (the deploy block) on **every**
    /// boot; do not persist. For an in-memory projection with no on-chain
    /// enumeration source, where a resume cursor would drop entries that must be
    /// rebuilt.
    ///
    /// Currently unconstructed outside tests: the blacklist deny-set was the last
    /// such projection and now enumerates its state from chain (#1497). Retained
    /// (like [`ColdStart::FromBlock`]) as the framework's full-replay surface
    /// pending a dedicated cleanup, rather than deleted piecemeal here.
    #[allow(dead_code)]
    FullReplay,
}

impl CursorStart {
    /// The explicit starting cursor, if this is a [`Self::Seeded`] start. `Some`
    /// pre-sets `run`'s cursor so the first tick scans from here and never
    /// resolves a floor.
    const fn seed(&self) -> Option<u64> {
        match self {
            Self::Seeded { at, .. } => Some(*at),
            Self::FromCheckpoint { .. } | Self::HeadMinusWindow { .. } | Self::FullReplay => None,
        }
    }

    /// The durable checkpoint this start writes forward to (and, for
    /// [`Self::FromCheckpoint`], reads its resume floor from), if any.
    const fn checkpoint(&self) -> Option<&Checkpoint> {
        match self {
            Self::Seeded { persist, .. } => persist.as_ref(),
            Self::FromCheckpoint { checkpoint, .. } => Some(checkpoint),
            Self::HeadMinusWindow { .. } | Self::FullReplay => None,
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
    fn initial_from(&self, from_block: u64, head: u64) -> Result<u64> {
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
                // A cold store under `ColdStart::FromBlock` replays the whole
                // history; every other case (including a warm resume) rewinds the
                // stored cursor normally.
                match (last, cold_start) {
                    (None, ColdStart::FromBlock) => Ok(from_block),
                    _ => Ok(resolve_persisted_start(
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
            // Full replay from the deploy floor: `head - u64::MAX` saturates to 0,
            // clamped up to `from_block` and down to `head`.
            Self::FullReplay => Ok(resolve_head_window_start(head, u64::MAX, from_block)),
            // A seeded start pre-sets the cursor (see `seed`), so `run_tick`'s
            // `None` branch never calls this arm. The deploy floor is the safe
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
    /// [`Self::HeadMinusWindow`], [`Self::FullReplay`], and unpersisted
    /// [`Self::Seeded`] starts). Best-effort: a lost write only widens the next
    /// rescan (see the store's monotonic-floor contract).
    fn persist(&self, block: u64) {
        if let Some(cp) = self.checkpoint()
            && let Err(err) = cp.store.record_checkpoint(cp.key, block)
        {
            warn!(err = %n(&err), key = cp.key.as_str(), block, "failed to persist watcher checkpoint");
        }
    }

    /// Force any buffered checkpoint out on graceful shutdown.
    fn flush(&self) {
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
    /// Contract deploy block — the scan floor for `FullReplay` / `HeadMinusWindow`
    /// starts and the lower clamp on a rewound `FromCheckpoint` resume (the
    /// contract does not exist below its deploy block).
    pub(crate) from_block: u64,
    /// Delay between poll ticks (the chain `event_poll_interval`).
    pub(crate) poll_interval: Duration,
    /// Max block span per `eth_getLogs` window.
    pub(crate) max_backfill_span: u64,
    /// Floor-derivation and persistence for the first tick.
    pub(crate) start: CursorStart,
    /// Initial / max failed-tick retry backoff.
    pub(crate) initial_backoff: Duration,
    pub(crate) max_backoff: Duration,
    /// Per-call timeout for each `get_logs`; `None` = the shared
    /// [`super::DEFAULT_RPC_CALL_TIMEOUT`] (10s). The head read is NOT bounded by
    /// this — it is issued by the shared [`super::shared_head::HeadSource`],
    /// which carries its own timeout.
    pub(crate) rpc_call_timeout: Option<Duration>,
    /// Log label for backoff/established diagnostics.
    pub(crate) label: &'static str,
    /// Called once each time the watcher transitions into a healthy cycle (first
    /// success, and after recovering from a backoff) — the seam each watcher
    /// wires to its `*_cycle_established` gauge (down-seconds → 0).
    pub(crate) on_established: Option<WatcherHook>,
    /// Called each time a tick fails and the watcher enters backoff — the seam
    /// each watcher wires to its `*_backoff_started` gauge.
    pub(crate) on_backoff: Option<WatcherHook>,
    /// Called on EVERY successful tick (not edge-triggered like
    /// [`on_established`](field@Self::on_established)) — the seam each watcher
    /// wires to its `*_last_tick_timestamp_seconds` liveness gauge (#1316). A
    /// task that panicked, wedged, or exited cleanly stops firing this, so the
    /// gauge goes stale where the error-triggered down-seconds gauge stays a
    /// healthy `0`.
    pub(crate) on_tick_success: Option<WatcherHook>,
    /// Called from a `Drop` guard in [`run`] only when the task is unwinding on a
    /// panic — the seam each watcher wires to its `*_task_panicked` counter
    /// (#1316). Nothing awaits the detached task, so this is the only trace a
    /// panic leaves.
    pub(crate) on_task_panic: Option<WatcherHook>,
}

impl WatcherConfig {
    /// Construct with the defaults the six `LogSink` sites share, so each site
    /// spells out only its own inputs. `max_backfill_span`, `initial_backoff`,
    /// and `rpc_call_timeout` (`None`) are invariant across all six and have no
    /// setter; `from_block` (`0`), `max_backoff`, and both hooks (unset) are
    /// defaults a site overrides with the chained setters below when it needs to.
    pub(crate) fn new(
        head: Arc<dyn HeadSource>,
        filter: Filter,
        start: CursorStart,
        poll_interval: Duration,
        label: &'static str,
    ) -> Self {
        Self {
            head,
            filter,
            from_block: 0,
            poll_interval,
            max_backfill_span: super::MAX_BACKFILL_BLOCK_SPAN,
            start,
            initial_backoff: super::WATCHER_INITIAL_BACKOFF,
            max_backoff: super::WATCHER_MAX_BACKOFF,
            rpc_call_timeout: None,
            label,
            on_established: None,
            on_backoff: None,
            on_tick_success: None,
            on_task_panic: None,
        }
    }

    /// Override the scan floor for a site whose contract deploy block is not 0.
    ///
    /// Currently unused: the only site that set a non-zero floor was the blacklist
    /// watcher's full-history replay, now retired for chain enumeration (#1497).
    /// Retained alongside [`CursorStart::FullReplay`] / [`ColdStart::FromBlock`].
    #[allow(dead_code)]
    pub(crate) const fn with_from_block(mut self, from_block: u64) -> Self {
        self.from_block = from_block;
        self
    }

    /// Override the failed-tick backoff ceiling (slash uses a tighter 30s).
    pub(crate) const fn max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    /// Wire the healthy-cycle hook; see the [`on_established`](field@Self::on_established) field.
    pub(crate) fn on_established(mut self, hook: WatcherHook) -> Self {
        self.on_established = Some(hook);
        self
    }

    /// Wire the backoff hook; see the [`on_backoff`](field@Self::on_backoff) field.
    pub(crate) fn on_backoff(mut self, hook: WatcherHook) -> Self {
        self.on_backoff = Some(hook);
        self
    }

    /// Wire the per-tick liveness hook; see the
    /// [`on_tick_success`](field@Self::on_tick_success) field.
    pub(crate) fn on_tick_success(mut self, hook: WatcherHook) -> Self {
        self.on_tick_success = Some(hook);
        self
    }

    /// Wire the task-panic hook; see the
    /// [`on_task_panic`](field@Self::on_task_panic) field.
    pub(crate) fn on_task_panic(mut self, hook: WatcherHook) -> Self {
        self.on_task_panic = Some(hook);
        self
    }
}

/// A loop-level observability hook (see [`WatcherConfig::on_established`] /
/// [`WatcherConfig::on_backoff`]). Boxed so a watcher can close over its
/// `Arc<Metrics>` without the generic loop knowing the concrete metric.
///
/// Hooks SHOULD be panic-free: in practice every hook is a `metric_hook` closure
/// that only bumps an infallible counter/gauge. `on_task_panic` additionally runs
/// from a `Drop` guard during an unwind — where a second panic would abort the
/// process — so [`run`]'s guard wraps that one call in `catch_unwind` as a
/// backstop; the other hooks fire on normal paths and are not guarded.
pub(crate) type WatcherHook = Box<dyn Fn() + Send + Sync>;

fn fire(hook: Option<&WatcherHook>) {
    if let Some(hook) = hook {
        hook();
    }
}

/// Fire the observability hooks for a successful tick: the per-tick liveness
/// stamp always, and the first-cycle `on_established` edge unless shutdown has
/// been cancelled (see the call site in [`run`] for why the edge is suppressed
/// under shutdown). Factored out of [`run`] to keep the loop within clippy's
/// cognitive-complexity budget.
fn fire_tick_success(cfg: &WatcherConfig, established: &mut bool, shutdown: &CancellationToken) {
    // Every successful tick — including an idle one (a successful head read is
    // still proof of life) — stamps the liveness gauge, so a wedged/panicked/
    // exited task is detectable by staleness where the edge-triggered
    // down-seconds gauge cannot (#1316).
    fire(cfg.on_tick_success.as_ref());
    // The established EDGE is suppressed once shutdown is cancelled: a tick that
    // "succeeded" only because a sink observed the token mid-pass (blacklist's
    // re-scope short-circuits on it) must NOT signal a readiness gate that its
    // first sync completed — that would open a slash-relevant gate fail-OPEN on
    // an incomplete deny-set replay. Liveness above still stamps; the node is
    // stopping, so the missed edge is moot.
    if !*established && !shutdown.is_cancelled() {
        *established = true;
        fire(cfg.on_established.as_ref());
        debug!(label = cfg.label, "watcher established getLogs stream");
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

/// Run one poll tick: read head, scan `[cursor, head]` in windows, apply each
/// log, advancing + persisting the cursor per completed window. Returns `Err` on
/// any retryable failure (RPC error/timeout or a sink's retryable error) with
/// the cursor left at the last completed window.
async fn run_tick<P, S>(
    provider: &P,
    cfg: &WatcherConfig,
    sink: &mut S,
    cursor: &mut Option<u64>,
    shutdown: &CancellationToken,
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
        None => cfg.start.initial_from(cfg.from_block, to)?,
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
            if shutdown.is_cancelled() {
                return Ok(());
            }
            let filter = cfg.filter.clone().from_block(start).to_block(end);
            let logs = timed(cfg.rpc_call_timeout, "get_logs", provider.get_logs(&filter))
                .await
                .with_context(|| format!("get_logs [{start}, {end}] for {}", cfg.label))?;
            for log in logs {
                sink.apply(log).await?;
            }
            // Window drained: advance the cursor and (persisting starts only)
            // persist it, so a mid-backfill crash resumes here rather than at the
            // floor (#1108).
            *cursor = Some(end.saturating_add(1));
            cfg.start.persist(end);
        }
    }
    // Always run the end-of-tick reconcile, including on an idle tick — a sink
    // whose `on_tick_complete` is a periodic, time-gated pass (e.g. the blacklist
    // re-scope that catches no-event scope transitions) would otherwise never run
    // on a quiet chain where every tick has `from > to`.
    sink.on_tick_complete().await?;
    Ok(())
}

/// Fires `on_panic` and logs at `error!` iff the enclosing [`run`] is unwinding
/// on a panic (#1316). The watcher task is spawned detached ([`AbortOnDrop`]) and
/// never awaited, so a panic is otherwise discarded with no log, counter, or
/// restart; this guard — the only thing that still runs on the unwind — makes it
/// visible. A graceful `return` (shutdown) or an `AbortOnDrop` teardown drops the
/// guard with `thread::panicking() == false`, so neither trips it. Mirrors the
/// `WarmOutcome` precedent in `crate::handlers::client`.
struct PanicGuard<'a> {
    label: &'static str,
    on_panic: Option<&'a WatcherHook>,
}

impl Drop for PanicGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // This runs during an unwind: a second panic here would abort the
            // process. Hooks are contractually panic-free (see [`WatcherHook`]),
            // but unlike the `WarmOutcome` precedent — which calls one hardcoded,
            // known-infallible method — this guard fires an arbitrary
            // caller-supplied closure, so catch defensively. A future hook bug
            // must degrade to a swallowed panic, never take the node down.
            let hook = self.on_panic;
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fire(hook)));
            error!(
                label = self.label,
                "watcher task PANICKED; nothing awaits this task, so this counter is its only trace"
            );
        }
    }
}

/// Drive `sink` from `cfg.filter` on an `eth_getLogs` polling loop until
/// `shutdown` is cancelled. The outer loop ticks on `cfg.poll_interval`,
/// resetting the backoff on a clean tick and backing off (bounded) on a failing
/// one; the cursor is carried across ticks so backfill flows straight into the
/// live tail.
///
/// `shutdown` is passed explicitly rather than living on [`WatcherConfig`] so no
/// caller can construct an inert token: the only way to obtain one paired with a
/// running task is [`spawn`], which mints it internally and returns the owning
/// [`WatcherHandle`] (#1236). Kept `pub(crate)` so the flush tests can drive the
/// loop directly with a token they cancel.
pub(crate) async fn run<P, S>(
    provider: P,
    cfg: WatcherConfig,
    mut sink: S,
    shutdown: CancellationToken,
) where
    P: Provider + Clone,
    S: LogSink,
{
    // Only fires if the loop below unwinds on a panic; a graceful return drops
    // it inert. Borrows `cfg` immutably for the whole function, alongside the
    // loop's other shared `&cfg` reads.
    let _panic_guard = PanicGuard {
        label: cfg.label,
        on_panic: cfg.on_task_panic.as_ref(),
    };
    let mut cursor: Option<u64> = cfg.start.seed();
    let mut backoff = cfg.initial_backoff;
    let mut established = false;
    let mut ticker = tokio::time::interval(cfg.poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                cfg.start.flush();
                return;
            }
            _ = ticker.tick() => {}
        }
        match run_tick(&provider, &cfg, &mut sink, &mut cursor, &shutdown).await {
            Ok(()) => {
                backoff = cfg.initial_backoff;
                fire_tick_success(&cfg, &mut established, &shutdown);
            }
            Err(err) => {
                established = false;
                // Likewise suppress the backoff EDGE under shutdown: a tick that
                // failed only because the sink bailed on the cancel token is not a
                // drift window, so it must not stamp `*_restarts_total` /
                // `*_down_seconds` in the node's final scrapes (#1321).
                if !shutdown.is_cancelled() {
                    fire(cfg.on_backoff.as_ref());
                }
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
                    () = shutdown.cancelled() => { cfg.start.flush(); return; }
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(cfg.max_backoff);
            }
        }
    }
}

/// Owns a running watcher task and the shutdown token that stops it. The token
/// is minted inside [`spawn`] and never escapes except through [`shutdown`], so
/// no call site can conjure or drop an inert token — the mistake #1230 fixed by
/// convention, made unrepresentable (#1236). [`shutdown`] is the graceful path
/// (the loop flushes its cursor and returns); the wrapped [`AbortOnDrop`] is the
/// hard safety net when the handle drops without a prior `shutdown`.
///
/// [`shutdown`]: WatcherHandle::shutdown
#[derive(Debug)]
pub(crate) struct WatcherHandle {
    shutdown: CancellationToken,
    _task: AbortOnDrop,
}

impl WatcherHandle {
    /// Signal the loop to flush its cursor and return. Idempotent; the owner
    /// calls it at whatever point in graceful shutdown its ordering demands
    /// (settlement, for one, cancels before its channel-close deadline).
    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// Mint the shutdown token, build the sink with access to it, spawn [`run`], and
/// return the owning [`WatcherHandle`]. This is the only way to pair a live task
/// with its token, which is what keeps the token from ever being inert (#1236).
///
/// `make_sink` receives the freshly-minted token by reference so a sink that
/// itself needs to observe shutdown (blacklist's re-scope short-circuits on it)
/// closes over the *same* token the loop cancels. Sinks that do not care ignore
/// the argument (`|_| sink`); the clone they could take is harmless because the
/// returned handle still owns the canonical token and is the only thing that
/// cancels it.
pub(crate) fn spawn<P, S>(
    provider: P,
    cfg: WatcherConfig,
    make_sink: impl FnOnce(&CancellationToken) -> S,
) -> WatcherHandle
where
    P: Provider + Clone + 'static,
    S: LogSink + 'static,
{
    let shutdown = CancellationToken::new();
    let sink = make_sink(&shutdown);
    let task = AbortOnDrop(tokio::spawn(run(provider, cfg, sink, shutdown.clone())));
    WatcherHandle {
        shutdown,
        _task: task,
    }
}

#[cfg(test)]
mod tests {
    use super::super::shared_head::SharedHead;
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
    // mis-wired variant (e.g. `FullReplay` feeding a bounded window) is caught.

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
        assert_eq!(CursorStart::FullReplay.seed(), None);
        assert_eq!(
            CursorStart::HeadMinusWindow { window_blocks: 10 }.seed(),
            None
        );
    }

    /// A `Seeded { persist: Some(_) }` start writes its cursor forward (origin's
    /// `CheckpointKey::Origin` resume); a `persist: None` seed records nothing
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

    /// `FullReplay` resolves to the deploy floor (`from_block`): it feeds
    /// `u64::MAX` as the window, so a swap to a bounded window would stop being a
    /// full replay and fail here.
    #[test]
    fn full_replay_initial_from_is_the_deploy_floor() {
        assert_eq!(
            CursorStart::FullReplay.initial_from(500, 20_000).ok(),
            Some(500)
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
            start: CursorStart::FromCheckpoint {
                cold_start: ColdStart::Head,
                checkpoint: Checkpoint {
                    store,
                    key: CheckpointKey::ChannelOpened,
                },
                reorg_margin: 0,
            },
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            rpc_call_timeout: None,
            label: "test",
            on_established: None,
            on_backoff: None,
            on_tick_success: None,
            on_task_panic: None,
        }
    }

    /// A never-cancelled token for the tick tests that don't exercise shutdown
    /// (the flush/cancel tests below mint and cancel their own).
    fn no_shutdown() -> CancellationToken {
        CancellationToken::new()
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

        // Tick 1: cursor resumed at 0, head 25 → windows [0,9], [10,19], [20,25].
        // One log per queried window; the sink fails on the second delivery
        // (window [10,19]). Starting the cursor explicitly keeps this test on the
        // window/persist loop rather than floor derivation (covered separately).
        asserter.push_success(&U64::from(25));
        asserter.push_success(&one_log());
        asserter.push_success(&one_log());
        let mut sink = ScriptedSink {
            applied: 0,
            fail_on: Some(1),
        };
        let mut cursor = Some(0);
        let result = run_tick(&provider, &cfg, &mut sink, &mut cursor, &no_shutdown()).await;
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
        let result = run_tick(&provider, &cfg, &mut sink, &mut cursor, &no_shutdown()).await;
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
        let result = run_tick(&provider, &cfg, &mut sink, &mut cursor, &no_shutdown()).await;
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
        let err = run_tick(&provider, &cfg, &mut sink, &mut cursor, &no_shutdown())
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
    /// so `run`'s two flush arms and `CursorStart::flush` had no coverage at
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
        cfg.start = CursorStart::FromCheckpoint {
            cold_start: ColdStart::Head,
            checkpoint: Checkpoint {
                store: Arc::clone(&store) as Arc<dyn KeyedCheckpointStore>,
                key: CheckpointKey::ChannelOpened,
            },
            reorg_margin: 0,
        };

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
            shutdown,
        ));
        assert!(task.await.is_ok(), "run must return, not hang or panic");
        assert_eq!(
            store.flushes(),
            1,
            "a cancelled watcher must flush its checkpoint exactly once"
        );
    }

    /// `spawn` hands the sink factory the *same* token the returned
    /// `WatcherHandle::shutdown` cancels. This is the seam blacklist relies on
    /// (its sink observes the token to short-circuit its re-scope) and the whole
    /// point of #1236: the token cannot be inert, because the only way to hold
    /// one paired with a live task is this factory. `run` responding to that
    /// token is proven by the flush test above; this pins that `spawn` wires the
    /// sink and handle to one token, not two.
    #[tokio::test]
    async fn spawn_hands_the_sink_the_token_shutdown_cancels() {
        use alloy::providers::ProviderBuilder;

        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let cfg = tick_cfg(provider.clone(), Arc::new(MemoryCheckpointStore::default()));

        // The factory runs synchronously inside `spawn`, so the token it receives
        // is captured before `spawn` returns.
        let captured: Arc<std::sync::Mutex<Option<CancellationToken>>> =
            Arc::new(std::sync::Mutex::new(None));
        let captured_in_sink = Arc::clone(&captured);
        let handle = spawn(provider, cfg, move |token| {
            if let Ok(mut slot) = captured_in_sink.lock() {
                *slot = Some(token.clone());
            }
            ScriptedSink {
                applied: 0,
                fail_on: None,
            }
        });

        let sink_token: Option<CancellationToken> = captured.lock().ok().and_then(|g| g.clone());
        assert!(
            sink_token.as_ref().is_some_and(|t| !t.is_cancelled()),
            "make_sink must run and receive a live (uncancelled) token"
        );
        handle.shutdown();
        assert!(
            sink_token
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled),
            "handle.shutdown() must cancel the very token make_sink received"
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
        let cfg = tick_cfg(provider.clone(), Arc::clone(&store));

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
        let mut cursor = Some(0);
        let result = run_tick(&provider, &cfg, &mut sink, &mut cursor, &shutdown).await;

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

    fn persisted() -> CursorStart {
        CursorStart::FromCheckpoint {
            cold_start: ColdStart::Head,
            checkpoint: Checkpoint {
                store: Arc::new(FailingLoadStore),
                key: CheckpointKey::ChannelOpened,
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
        let recorded = store.record_checkpoint(CheckpointKey::ChannelOpened, CHECKPOINT);
        assert!(recorded.is_ok(), "seeding the checkpoint must succeed");
        let start = CursorStart::FromCheckpoint {
            cold_start: ColdStart::Head,
            checkpoint: Checkpoint {
                store,
                key: CheckpointKey::ChannelOpened,
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

    /// A `LogSink` that cancels its shutdown token from the N-th `on_tick_complete`
    /// (1-indexed), so the loop runs exactly N ticks then returns. `bail` makes
    /// that cancelling tick also fail, mimicking blacklist's shutdown-interrupted
    /// re-scope bail.
    struct CancelOnNthTick {
        shutdown: CancellationToken,
        ticks: usize,
        cancel_on: usize,
        bail: bool,
    }
    impl LogSink for CancelOnNthTick {
        async fn apply(&mut self, _log: Log) -> Result<()> {
            Ok(())
        }
        async fn on_tick_complete(&mut self) -> Result<()> {
            self.ticks += 1;
            if self.ticks >= self.cancel_on {
                self.shutdown.cancel();
                if self.bail {
                    anyhow::bail!("shutdown-interrupted tick");
                }
            }
            Ok(())
        }
    }

    /// Three hooks wired to atomic counters, for driving `run` and asserting which
    /// loop edges fired.
    fn counting_hooks() -> (
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<std::sync::atomic::AtomicUsize>,
        WatcherHook,
        WatcherHook,
        WatcherHook,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (tick, est, backoff) = (
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        let (t, e, b) = (Arc::clone(&tick), Arc::clone(&est), Arc::clone(&backoff));
        let tick_hook: WatcherHook = Box::new(move || {
            t.fetch_add(1, Ordering::SeqCst);
        });
        let est_hook: WatcherHook = Box::new(move || {
            e.fetch_add(1, Ordering::SeqCst);
        });
        let backoff_hook: WatcherHook = Box::new(move || {
            b.fetch_add(1, Ordering::SeqCst);
        });
        (tick, est, backoff, tick_hook, est_hook, backoff_hook)
    }

    /// The liveness hook (#1316) fires on EVERY successful tick — a log-bearing
    /// one and an idle one — whereas `on_established` fires only on the first-cycle
    /// edge. Driving two ticks and asserting `on_tick_success == 2` while
    /// `on_established == 1` pins that distinction (a regression that gated the
    /// liveness hook behind the `!established` edge would read `1`/`1`).
    #[tokio::test]
    async fn on_tick_success_fires_every_tick_established_only_once() {
        use alloy::primitives::U64;
        use alloy::providers::ProviderBuilder;
        use std::sync::atomic::Ordering;

        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let shutdown = CancellationToken::new();

        let (tick, est, _b, tick_hook, est_hook, _bh) = counting_hooks();
        let mut cfg = tick_cfg(provider.clone(), Arc::new(MemoryCheckpointStore::default()));
        cfg.start = CursorStart::HeadMinusWindow { window_blocks: 0 };
        cfg.on_tick_success = Some(tick_hook);
        cfg.on_established = Some(est_hook);

        // tick 1: head(25) + one empty `get_logs` over [25,25] (cursor → 26).
        // tick 2: head(25) only — from=26 > to=25 is an idle tick (no get_logs).
        asserter.push_success(&U64::from(25));
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_success(&U64::from(25));

        run(
            provider,
            cfg,
            CancelOnNthTick {
                shutdown: shutdown.clone(),
                ticks: 0,
                cancel_on: 2,
                bail: false,
            },
            shutdown,
        )
        .await;

        assert_eq!(
            tick.load(Ordering::SeqCst),
            2,
            "on_tick_success must fire on every tick, including the idle one"
        );
        assert_eq!(
            est.load(Ordering::SeqCst),
            1,
            "on_established is the first-cycle edge — exactly once across both ticks"
        );
    }

    /// The readiness fail-open fix: a successful tick that completes only because
    /// the sink observed the cancel token must NOT fire the `on_established` edge
    /// — that edge signals blacklist's readiness gate `Ok`, so firing it on an
    /// incomplete initial sync would open a slash-relevant gate fail-OPEN.
    /// Liveness still stamps (the tick did succeed).
    #[tokio::test]
    async fn shutdown_suppresses_the_established_edge() {
        use alloy::primitives::U64;
        use alloy::providers::ProviderBuilder;
        use std::sync::atomic::Ordering;

        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let shutdown = CancellationToken::new();

        let (tick, est, _b, tick_hook, est_hook, _bh) = counting_hooks();
        let mut cfg = tick_cfg(provider.clone(), Arc::new(MemoryCheckpointStore::default()));
        cfg.start = CursorStart::HeadMinusWindow { window_blocks: 0 };
        cfg.on_tick_success = Some(tick_hook);
        cfg.on_established = Some(est_hook);

        asserter.push_success(&U64::from(25));
        asserter.push_success(&Vec::<Log>::new());

        run(
            provider,
            cfg,
            CancelOnNthTick {
                shutdown: shutdown.clone(),
                ticks: 0,
                cancel_on: 1,
                bail: false,
            },
            shutdown,
        )
        .await;

        assert_eq!(
            est.load(Ordering::SeqCst),
            0,
            "on_established must be suppressed once the token is cancelled (fail-closed)"
        );
        assert_eq!(
            tick.load(Ordering::SeqCst),
            1,
            "liveness still stamps on the successful (if cancelled) tick"
        );
    }

    /// The #1321 fix: a tick that fails only because the sink bailed on the cancel
    /// token must NOT fire `on_backoff` — otherwise an orderly shutdown stamps a
    /// false drift window (`*_restarts_total = 1` + climbing down-seconds) in the
    /// node's final scrapes.
    #[tokio::test]
    async fn shutdown_suppresses_the_backoff_edge() {
        use alloy::primitives::U64;
        use alloy::providers::ProviderBuilder;
        use std::sync::atomic::Ordering;

        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let shutdown = CancellationToken::new();

        let (_t, _e, backoff, tick_hook, est_hook, backoff_hook) = counting_hooks();
        let mut cfg = tick_cfg(provider.clone(), Arc::new(MemoryCheckpointStore::default()));
        cfg.start = CursorStart::HeadMinusWindow { window_blocks: 0 };
        cfg.on_tick_success = Some(tick_hook);
        cfg.on_established = Some(est_hook);
        cfg.on_backoff = Some(backoff_hook);

        asserter.push_success(&U64::from(25));
        asserter.push_success(&Vec::<Log>::new());

        run(
            provider,
            cfg,
            CancelOnNthTick {
                shutdown: shutdown.clone(),
                ticks: 0,
                cancel_on: 1,
                bail: true,
            },
            shutdown,
        )
        .await;

        assert_eq!(
            backoff.load(Ordering::SeqCst),
            0,
            "on_backoff must be suppressed when the tick failed due to shutdown"
        );
    }

    /// End-to-end: a panic inside a watcher tick, driven through the real `run`
    /// task, fires the `on_task_panic` hook via the in-`run` `PanicGuard` — proving
    /// the guard is actually installed and wired, not just correct in isolation.
    #[tokio::test]
    #[allow(clippy::panic)] // deliberately panic inside a tick to exercise the guard.
    async fn run_task_panic_fires_the_panic_hook() {
        use alloy::primitives::U64;
        use alloy::providers::ProviderBuilder;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct PanicOnApply;
        impl LogSink for PanicOnApply {
            async fn apply(&mut self, _log: Log) -> Result<()> {
                panic!("intentional tick panic");
            }
        }

        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let shutdown = CancellationToken::new();

        let panicked = Arc::new(AtomicUsize::new(0));
        let hook_counter = Arc::clone(&panicked);
        let mut cfg = tick_cfg(provider.clone(), Arc::new(MemoryCheckpointStore::default()));
        cfg.start = CursorStart::HeadMinusWindow { window_blocks: 0 };
        cfg.on_task_panic = Some(Box::new(move || {
            hook_counter.fetch_add(1, Ordering::SeqCst);
        }));

        // head(25) then one log to apply — `apply` panics on it.
        asserter.push_success(&U64::from(25));
        asserter.push_success(&one_log());

        let joined = tokio::spawn(run(provider, cfg, PanicOnApply, shutdown)).await;
        assert!(joined.is_err(), "the watcher task must have panicked");
        assert_eq!(
            panicked.load(Ordering::SeqCst),
            1,
            "the PanicGuard in run must fire on_task_panic exactly once on the unwind"
        );
    }

    /// The panic guard (#1316) is the only trace a panic in the detached watcher
    /// task leaves: it fires `on_task_panic` iff [`run`] is unwinding, and NOT on
    /// a graceful drop. Mirrors the `WarmOutcome` false-positive guard.
    #[test]
    #[allow(clippy::panic)] // deliberately unwind a thread to exercise the guard.
    fn panic_guard_fires_only_on_unwind() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let fired = Arc::new(AtomicUsize::new(0));

        // A graceful drop (no panic) must NOT fire the hook.
        {
            let f = Arc::clone(&fired);
            let hook: WatcherHook = Box::new(move || {
                f.fetch_add(1, Ordering::SeqCst);
            });
            let _guard = PanicGuard {
                label: "test",
                on_panic: Some(&hook),
            };
        }
        assert_eq!(
            fired.load(Ordering::SeqCst),
            0,
            "a graceful drop must not fire the panic hook"
        );

        // A drop during an unwind must fire the hook exactly once.
        let f = Arc::clone(&fired);
        let joined = std::thread::spawn(move || {
            let hook: WatcherHook = Box::new(move || {
                f.fetch_add(1, Ordering::SeqCst);
            });
            let _guard = PanicGuard {
                label: "test",
                on_panic: Some(&hook),
            };
            panic!("intentional");
        })
        .join();
        assert!(joined.is_err(), "the worker thread must have panicked");
        assert_eq!(
            fired.load(Ordering::SeqCst),
            1,
            "an unwinding drop must fire the panic hook exactly once"
        );
    }
}
