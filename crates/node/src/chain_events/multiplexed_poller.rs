//! Multiplexed `eth_getLogs` polling: one poll tick, one `get_logs` call per
//! backfill window, demuxed by `(address, topic0)` into per-route sinks.
//!
//! [`resumable_watcher`](super::resumable_watcher) gives every watcher its own
//! cursor loop and its own `eth_getLogs` call each tick. That is right when a
//! watcher's filter genuinely differs from its siblings', but five watchers on
//! this node scan disjoint `(address, topic0)` slices of the *same* three
//! contracts — so five independent loops cost five `eth_getLogs` calls per tick
//! where one merged call, demuxed after the fact, would do. [`MultiplexedPoller`]
//! is that merge: one filter, one `eth_getLogs` per window, fanned out by
//! `(address, topic0)` into each watcher's own [`ErasedSink`].
//!
//! Three properties carry over from `resumable_watcher` unchanged:
//!
//! - **Resumability and idempotency** (see that module's doc) — the cursor
//!   machinery ([`CursorStart::seed`]/[`CursorStart::initial_from`]/
//!   [`CursorStart::persist`]/[`CursorStart::flush`]) is reused verbatim, one
//!   instance per route.
//! - **Per-route cursors, not one shared cursor.** Merging the RPC call does
//!   not merge the cursors: each route floor derives, advances, and persists
//!   independently, scanned as `[min(route floors), head]` in one call. This is
//!   what preserves settlement's deep `FromCheckpoint` backfill without
//!   dragging every sibling route through the same history, and what makes
//!   per-route error isolation possible below.
//! - **Per-route error isolation.** A route whose sink errors is marked
//!   `errored` for the rest of the tick: it holds its cursor (re-scanning the
//!   same range next tick) while sibling routes keep advancing and persisting.
//!   The tick as a whole still returns `Err` when any route errored, so the
//!   *loop* backs off — a route that is failing should not poll as fast as one
//!   that is not — but that backoff never discards a healthy sibling's
//!   progress.
//!
//! # Hook firing differs from `resumable_watcher`
//!
//! In `resumable_watcher::run` the *loop* fires `on_backoff`/`on_established`
//! once per tick, because there is exactly one sink. Here the *tick* fires each
//! route's hooks independently (routes fail independently), and the loop's
//! `Err` arm only sleeps the backoff — it never fires a hook itself. The one
//! exception is a failure in the shared, pre-route-loop work (the head read, or
//! a route's checkpoint-load floor derivation, or the merged `get_logs` call):
//! those fail the *whole* tick before any route-specific step runs, so
//! [`fail_whole_tick`] fires `on_backoff` for every route directly at the
//! failure site — mirroring what happened before the merge, when every watcher
//! read its own head and they all convoyed into backoff together through
//! [`super::shared_head::SharedHead`].
//!
//! As in `resumable_watcher`, the `on_established`/`on_backoff` *edges* are
//! suppressed once `shutdown` is cancelled (a tick that only "succeeded"
//! because a sink observed the cancel token must not flip a readiness gate
//! open), while `on_tick_success` keeps stamping unconditionally.
//!
//! No watcher constructs a [`Route`] yet — this driver is unit-tested here
//! against a mocked provider on its own, ahead of the migration that points
//! the five `eth_getLogs` watchers at it in place of their own
//! `resumable_watcher::run` loops. `dead_code` is allowed at the module level
//! until that wiring lands.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use anyhow::{Context, Result};
use async_trait::async_trait;
use decdn_common::redact::sanitize_err_chain;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use super::resumable_watcher::{CursorStart, LogSink, WatcherHandle, WatcherHook, fire};
use super::shared_head::HeadSource;
use super::{AbortOnDrop, MAX_BACKFILL_BLOCK_SPAN, WATCHER_INITIAL_BACKOFF, WATCHER_MAX_BACKOFF};
use super::{backfill_windows, timed};

/// Object-safe adapter over [`LogSink`], so routes with different concrete sink
/// types live in one `Vec<Box<dyn ErasedSink>>`. `LogSink::apply` /
/// `on_tick_complete` return `impl Future`, which is not object-safe; this
/// trait is, and is blanket-implemented for every `LogSink` so no sink writes
/// it by hand.
#[async_trait]
pub(crate) trait ErasedSink: Send {
    async fn apply(&mut self, log: Log) -> Result<()>;
    async fn on_tick_complete(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl<S: LogSink> ErasedSink for S {
    async fn apply(&mut self, log: Log) -> Result<()> {
        LogSink::apply(self, log).await
    }
    async fn on_tick_complete(&mut self) -> Result<()> {
        LogSink::on_tick_complete(self).await
    }
}

/// One registered watcher on the multiplexed poller. `addresses`/`topic0s` are
/// its demux keys; every `(address, topic0)` pair in the cartesian product is a
/// route key, and every route key must be globally unique across the poller's
/// routes ([`MultiplexedPollerBuilder::build`] checks this). Each existing
/// watcher is single-address, so `addresses` is a 1-element vec today, but the
/// field is a set so a future multi-address watcher needs no reshape.
pub(crate) struct Route {
    pub(crate) addresses: Vec<Address>,
    pub(crate) topic0s: Vec<B256>,
    pub(crate) start: CursorStart,
    pub(crate) sink: Box<dyn ErasedSink>,
    pub(crate) label: &'static str,
    pub(crate) on_established: Option<WatcherHook>,
    pub(crate) on_backoff: Option<WatcherHook>,
    pub(crate) on_tick_success: Option<WatcherHook>,
    pub(crate) on_task_panic: Option<WatcherHook>,
}

/// A [`Route`]'s per-tick working state: its cursor machinery and sink, plus
/// the fields `run_tick` mutates each tick. Does not carry `on_task_panic` —
/// that hook is extracted into [`MultiplexedPoller::panic_hooks`] at build
/// time, a separate field the [`PanicGuard`] in [`run`] can borrow immutably
/// for the whole task without conflicting with `run_tick`'s per-tick `&mut`
/// borrows of `routes` (see that guard's doc for why the split exists).
struct RouteState {
    start: CursorStart,
    sink: Box<dyn ErasedSink>,
    label: &'static str,
    on_established: Option<WatcherHook>,
    on_backoff: Option<WatcherHook>,
    on_tick_success: Option<WatcherHook>,
    /// This route's cursor: `None` until the first tick resolves it (seed or
    /// checkpoint/head-window floor), `Some` thereafter.
    cursor: Option<u64>,
    /// This tick's floor, captured at the start of the tick so the demux gate
    /// (`log.block_number < tick_floor`) has a stable value to compare against
    /// even as `cursor` advances window-by-window within the same tick.
    tick_floor: u64,
    /// Set when this route's sink (or reconcile) errored this tick; gates the
    /// demux and the advance/persist step so the route holds its cursor and
    /// re-scans next tick instead of racing ahead of a failure.
    errored: bool,
    /// First-cycle edge tracking for `on_established`, mirroring
    /// `resumable_watcher::run`'s `established` local.
    established: bool,
}

/// Poller configuration, its resolved routes, and the demux index built once
/// at [`MultiplexedPollerBuilder::build`].
pub(crate) struct MultiplexedPoller {
    head: Arc<dyn HeadSource>,
    routes: Vec<RouteState>,
    /// `(address, topic0) -> route index`. Built once at `build()`; a log
    /// whose key is absent is not subscribed by any route and is dropped.
    key_index: HashMap<(Address, B256), usize>,
    /// Merged filter over every route's addresses and topic0s, built once at
    /// `build()`. The block range is set per window in `run_tick`.
    base_filter: Filter,
    /// Contract deploy floor — mirrors `WatcherConfig::from_block` (always `0`
    /// today; see that field's doc for why it is retained as a floor anyway).
    from_block: u64,
    poll_interval: Duration,
    max_backfill_span: u64,
    initial_backoff: Duration,
    /// Ceiling for the shared loop's backoff. One loop now serves every route,
    /// so a route that used to carry its own tighter cap (slash's 30s) no
    /// longer can — [`WATCHER_MAX_BACKOFF`] (60s) applies to the whole poller.
    max_backoff: Duration,
    rpc_call_timeout: Option<Duration>,
    /// Every route's `(label, on_task_panic)`, extracted out of `routes` at
    /// build time — see [`RouteState`]'s doc for why they live here instead.
    panic_hooks: Vec<(&'static str, Option<WatcherHook>)>,
}

/// Accumulates [`Route`]s and builds a [`MultiplexedPoller`].
pub(crate) struct MultiplexedPollerBuilder {
    head: Arc<dyn HeadSource>,
    routes: Vec<Route>,
    from_block: u64,
    poll_interval: Duration,
    max_backfill_span: u64,
    initial_backoff: Duration,
    max_backoff: Duration,
    rpc_call_timeout: Option<Duration>,
}

impl MultiplexedPollerBuilder {
    /// Construct with the defaults every route shares: the deploy floor (`0`),
    /// [`MAX_BACKFILL_BLOCK_SPAN`], and the default watcher backoff schedule.
    pub(crate) fn new(head: Arc<dyn HeadSource>, poll_interval: Duration) -> Self {
        Self {
            head,
            routes: Vec::new(),
            from_block: 0,
            poll_interval,
            max_backfill_span: MAX_BACKFILL_BLOCK_SPAN,
            initial_backoff: WATCHER_INITIAL_BACKOFF,
            max_backoff: WATCHER_MAX_BACKOFF,
            rpc_call_timeout: None,
        }
    }

    /// Register one watcher's route.
    pub(crate) fn route(mut self, route: Route) -> Self {
        self.routes.push(route);
        self
    }

    /// Override the max block span per `eth_getLogs` window (tests use a small
    /// span to exercise multi-window backfills without huge fixtures).
    #[cfg(test)]
    pub(crate) const fn max_backfill_span(mut self, span: u64) -> Self {
        self.max_backfill_span = span;
        self
    }

    /// Override the per-call RPC timeout.
    #[cfg(test)]
    pub(crate) const fn rpc_call_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_call_timeout = Some(timeout);
        self
    }

    /// Build the poller: construct the merged filter and the `(address,
    /// topic0) -> route` demux index. Returns `Err` if two routes claim the
    /// same `(address, topic0)` key — two watchers subscribing to the same
    /// event is a wiring bug, not a runtime condition, so it fails fast at
    /// startup rather than silently routing every such log to whichever route
    /// happened to register first.
    pub(crate) fn build(self) -> Result<MultiplexedPoller> {
        let mut key_index = HashMap::new();
        let mut all_addresses = Vec::new();
        let mut all_topic0s = Vec::new();
        for (idx, route) in self.routes.iter().enumerate() {
            for &address in &route.addresses {
                for &topic0 in &route.topic0s {
                    if let Some(&existing) = key_index.get(&(address, topic0)) {
                        anyhow::bail!(
                            "duplicate multiplexed poller route key (address={address}, \
                             topic0={topic0}): routes {existing} and {idx} ({} and {}) both \
                             claim it",
                            self.routes.get(existing).map_or("?", |r: &Route| r.label),
                            route.label,
                        );
                    }
                    key_index.insert((address, topic0), idx);
                }
            }
            all_addresses.extend(route.addresses.iter().copied());
            all_topic0s.extend(route.topic0s.iter().copied());
        }
        let base_filter = Filter::new()
            .address(all_addresses)
            .event_signature(all_topic0s);

        let mut routes = Vec::with_capacity(self.routes.len());
        let mut panic_hooks = Vec::with_capacity(self.routes.len());
        for route in self.routes {
            panic_hooks.push((route.label, route.on_task_panic));
            routes.push(RouteState {
                start: route.start,
                sink: route.sink,
                label: route.label,
                on_established: route.on_established,
                on_backoff: route.on_backoff,
                on_tick_success: route.on_tick_success,
                cursor: None,
                tick_floor: 0,
                errored: false,
                established: false,
            });
        }

        Ok(MultiplexedPoller {
            head: self.head,
            routes,
            key_index,
            base_filter,
            from_block: self.from_block,
            poll_interval: self.poll_interval,
            max_backfill_span: self.max_backfill_span,
            initial_backoff: self.initial_backoff,
            max_backoff: self.max_backoff,
            rpc_call_timeout: self.rpc_call_timeout,
            panic_hooks,
        })
    }
}

/// Fire `on_backoff` for every route (unless `shutdown` is cancelled — the
/// same edge suppression `resumable_watcher::run` applies) and clear every
/// route's `established` flag, then hand back `err` unchanged. Used only at
/// the shared, pre-route-loop failure points (head read, a route's floor
/// derivation, the merged `get_logs` call): a failure there aborts the whole
/// tick before any route-specific step has run, so no single route's
/// `errored` flag would otherwise capture it, and every route is equally
/// "down" — exactly as before the merge, when every watcher read its own head
/// and they all convoyed into backoff together.
fn fail_whole_tick(
    poller: &mut MultiplexedPoller,
    shutdown: &CancellationToken,
    err: anyhow::Error,
) -> anyhow::Error {
    if !shutdown.is_cancelled() {
        for r in &poller.routes {
            fire(r.on_backoff.as_ref());
        }
    }
    for r in &mut poller.routes {
        r.established = false;
    }
    err
}

/// Step 1: resolve every route's floor for this tick (its cursor, or a
/// first-tick `initial_from`) and return the union scan range's lower bound
/// (`min` over resolved cursors, or `to` if every route is already there — an
/// idle tick). A `FromCheckpoint` load error is retryable and propagates
/// (settlement's #751/#762 guard); the caller fails the whole tick on it.
fn resolve_route_floors(poller: &mut MultiplexedPoller, to: u64) -> Result<u64> {
    for r in &mut poller.routes {
        if r.cursor.is_none() {
            let resolved = match r.start.seed() {
                Some(seed) => seed,
                None => r.start.initial_from(poller.from_block, to)?,
            };
            r.cursor = Some(resolved);
        }
        // Capture the floor for this tick's demux gate; `unwrap_or` never
        // actually falls through (the branch above always leaves `cursor`
        // `Some`), kept as the anti-panic-safe idiom rather than `expect`.
        r.tick_floor = r.cursor.unwrap_or(poller.from_block);
        r.errored = false;
    }
    Ok(poller
        .routes
        .iter()
        .filter_map(|r| r.cursor)
        .min()
        .unwrap_or(to))
}

/// Demux one window's logs by `(address, topic0)`, gated by each route's own
/// floor for this tick. A sink `Err` isolates that route (see the module doc);
/// siblings are unaffected.
async fn demux_window_logs(poller: &mut MultiplexedPoller, logs: Vec<Log>) {
    for log in logs {
        let Some(t0) = log.topic0().copied() else {
            continue;
        };
        let Some(&idx) = poller.key_index.get(&(log.address(), t0)) else {
            continue; // not a subscribed (address, topic0)
        };
        let Some(route) = poller.routes.get_mut(idx) else {
            continue;
        };
        if route.errored {
            continue; // this route re-scans the whole window next tick
        }
        // Floor gate: a route whose floor sits above this window's start (it
        // enumerated at head, or is a sibling still ahead) ignores logs below
        // its own floor. A `None` block_number applies rather than being
        // silently dropped.
        if log.block_number.is_some_and(|b| b < route.tick_floor) {
            continue;
        }
        if let Err(err) = route.sink.apply(log).await {
            // Retryable sink error: isolate this route. It holds its cursor
            // and re-scans; sibling routes keep advancing.
            route.errored = true;
            warn!(
                label = route.label,
                err = %sanitize_err_chain(&err),
                "route sink error; isolating and re-scanning this route"
            );
        }
    }
}

/// Advance + persist per route after a window drains: a route whose floor
/// this window covered and which did not error moves to `end + 1`.
fn advance_routes(poller: &mut MultiplexedPoller, end: u64) {
    for r in &mut poller.routes {
        if !r.errored && r.tick_floor <= end {
            r.cursor = Some(end.saturating_add(1));
            r.start.persist(end);
        }
    }
}

/// End-of-tick reconcile per route (blacklist re-scope, origin deferred
/// re-reads, capacity-bond/slash resync, rate-bounds hourly re-read). A
/// reconcile `Err` marks that route errored (holds its cursor, retries).
async fn reconcile_routes(poller: &mut MultiplexedPoller) {
    for r in &mut poller.routes {
        if r.errored {
            continue;
        }
        if let Err(err) = r.sink.on_tick_complete().await {
            r.errored = true;
            warn!(label = r.label, err = %sanitize_err_chain(&err), "route reconcile error");
        }
    }
}

/// Fire per-route hooks for this tick's outcome and report whether any route
/// errored (the caller fails the tick on `true`, driving the loop's backoff).
fn fire_route_hooks(poller: &mut MultiplexedPoller, shutdown: &CancellationToken) -> bool {
    let any_errored = poller.routes.iter().any(|r| r.errored);
    for r in &mut poller.routes {
        if r.errored {
            if !shutdown.is_cancelled() {
                fire(r.on_backoff.as_ref());
            }
            r.established = false;
        } else {
            // Liveness stamps on every successful tick, including idle ones —
            // a wedged/panicked/exited task is detectable by staleness.
            fire(r.on_tick_success.as_ref());
            // The established EDGE is suppressed once shutdown is cancelled;
            // see `fire_tick_success` in `resumable_watcher` for the fail-open
            // rationale this mirrors.
            if !r.established && !shutdown.is_cancelled() {
                r.established = true;
                fire(r.on_established.as_ref());
            }
        }
    }
    any_errored
}

/// Run one poll tick: read head, resolve each route's floor, scan the merged
/// `[min(floor), head]` range in windows (one `get_logs` per window), demux
/// each log to its owning route, then reconcile and fire hooks. Returns `Err`
/// if the shared head/floor/`get_logs` reads fail, or if any route errored
/// applying a log or reconciling — either way the *loop* backs off, but a
/// route that itself succeeded this tick has already advanced and persisted.
async fn run_tick<P: Provider + Clone>(
    provider: &P,
    poller: &mut MultiplexedPoller,
    shutdown: &CancellationToken,
) -> Result<()> {
    let to = match poller.head.head().await.context("read head block") {
        Ok(to) => to,
        Err(err) => return Err(fail_whole_tick(poller, shutdown, err)),
    };

    let from = match resolve_route_floors(poller, to) {
        Ok(from) => from,
        Err(err) => return Err(fail_whole_tick(poller, shutdown, err)),
    };

    // If every route is already at/above head this is an idle tick: no
    // `get_logs`, but the reconcile below still runs every route's
    // `on_tick_complete`.
    if from <= to {
        for (start, end) in backfill_windows(from, to, poller.max_backfill_span) {
            // Yield between windows so a large merged backfill does not block
            // graceful shutdown (mirrors `resumable_watcher::run_tick`).
            if shutdown.is_cancelled() {
                return Ok(());
            }
            let filter = poller.base_filter.clone().from_block(start).to_block(end);
            let logs = match timed(
                poller.rpc_call_timeout,
                "get_logs",
                provider.get_logs(&filter),
            )
            .await
            .with_context(|| format!("multiplexed get_logs [{start}, {end}]"))
            {
                Ok(logs) => logs,
                Err(err) => return Err(fail_whole_tick(poller, shutdown, err)),
            };
            demux_window_logs(poller, logs).await;
            advance_routes(poller, end);
        }
    }

    reconcile_routes(poller).await;
    if fire_route_hooks(poller, shutdown) {
        anyhow::bail!("one or more routes errored this tick"); // drives the loop's backoff sleep
    }
    Ok(())
}

/// Fires every route's `on_task_panic` and logs at `error!` iff the enclosing
/// [`run`] is unwinding on a panic (mirrors
/// `resumable_watcher::PanicGuard`, #1316). The poller task is spawned
/// detached ([`AbortOnDrop`]) and never awaited, so a panic is otherwise
/// discarded with no log, counter, or restart; this guard is the only thing
/// that still runs on the unwind.
///
/// Borrows `hooks` — a `Vec` [`run`] moves out of the poller's
/// `panic_hooks` field via `mem::take` *before* the tick loop starts — rather
/// than reading `poller.panic_hooks` directly, so this immutable borrow can
/// live for the whole function alongside the loop's `&mut poller` uses without
/// the borrow checker treating them as aliasing the same value.
struct PanicGuard<'a> {
    hooks: &'a [(&'static str, Option<WatcherHook>)],
}

impl Drop for PanicGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            for (label, hook) in self.hooks {
                let hook_ref = hook.as_ref();
                // A second panic here (inside an unwind) would abort the
                // process; hooks are contractually panic-free, but this guard
                // fires an arbitrary caller-supplied closure, so catch
                // defensively (mirrors `resumable_watcher::PanicGuard`).
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fire(hook_ref)));
                error!(
                    label = *label,
                    "multiplexed poller task PANICKED; nothing awaits this task, so this is its \
                     only trace for this route"
                );
            }
        }
    }
}

/// Drive every registered route from one merged `eth_getLogs` polling loop
/// until `shutdown` is cancelled. Structurally mirrors
/// `resumable_watcher::run`: `tokio::time::interval` with
/// `MissedTickBehavior::Delay`, a `biased` select on `shutdown.cancelled()`
/// (flush every route's checkpoint and return) vs `ticker.tick()`, backoff
/// reset on `Ok`, and bounded exponential backoff on `Err`. The `Err` arm
/// itself fires no hook — `run_tick` already fired every route's hooks (or, on
/// a shared-read failure, [`fail_whole_tick`] fired all of them) — it only
/// sleeps.
pub(crate) async fn run<P>(provider: P, mut poller: MultiplexedPoller, shutdown: CancellationToken)
where
    P: Provider + Clone,
{
    // Moved out of `poller` so the guard's borrow does not alias the `&mut
    // poller` used throughout the loop below; see `PanicGuard`'s doc.
    let panic_hooks = std::mem::take(&mut poller.panic_hooks);
    let _panic_guard = PanicGuard {
        hooks: &panic_hooks,
    };

    let mut backoff = poller.initial_backoff;
    let mut ticker = tokio::time::interval(poller.poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                for r in &poller.routes {
                    r.start.flush();
                }
                return;
            }
            _ = ticker.tick() => {}
        }
        match run_tick(&provider, &mut poller, &shutdown).await {
            Ok(()) => {
                backoff = poller.initial_backoff;
            }
            Err(err) => {
                warn!(
                    err = %sanitize_err_chain(&err),
                    backoff_secs = backoff.as_secs(),
                    "multiplexed poller tick error; restarting after backoff"
                );
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        for r in &poller.routes {
                            r.start.flush();
                        }
                        return;
                    }
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(poller.max_backoff);
            }
        }
    }
}

/// Mint the shutdown token, spawn [`run`], and return the owning
/// [`WatcherHandle`] — mirrors `resumable_watcher::spawn` (#1236).
pub(crate) fn spawn<P>(provider: P, poller: MultiplexedPoller) -> WatcherHandle
where
    P: Provider + Clone + 'static,
{
    let shutdown = CancellationToken::new();
    let task = AbortOnDrop(tokio::spawn(run(provider, poller, shutdown.clone())));
    WatcherHandle::new(shutdown, task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain_events::resumable_watcher::{Checkpoint, ColdStart};
    use crate::chain_events::shared_head::SharedHead;
    use alloy::primitives::{Bytes, U64, address, b256};
    use alloy::providers::ProviderBuilder;
    use decdn_incentive::{CheckpointKey, KeyedCheckpointStore, StoreError};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Build a log matching `(addr, topic0)` at `block`, per the brief's
    /// harness note: `alloy_primitives::Log::new_unchecked` plus the `Log`
    /// wrapper's `Default` for every other field.
    fn log_at(addr: Address, topic0: B256, block: u64) -> Log {
        Log {
            inner: alloy::primitives::Log::new_unchecked(addr, vec![topic0], Bytes::new()),
            block_number: Some(block),
            ..Default::default()
        }
    }

    /// A `LogSink` that records every `(address, topic0, block)` it applies,
    /// optionally erroring on the N-th apply or on `on_tick_complete`.
    /// `ErasedSink` reaches every scripted sink through the blanket impl.
    #[derive(Default)]
    struct ScriptedSink {
        applied: Vec<(Address, B256, Option<u64>)>,
        fail_apply_on: Option<usize>,
        fail_tick_complete: bool,
        tick_completes: usize,
    }

    impl LogSink for ScriptedSink {
        async fn apply(&mut self, log: Log) -> Result<()> {
            if self.fail_apply_on == Some(self.applied.len()) {
                anyhow::bail!("scripted sink failure");
            }
            let topic0 = log.topic0().copied().unwrap_or_default();
            self.applied.push((log.address(), topic0, log.block_number));
            Ok(())
        }

        async fn on_tick_complete(&mut self) -> Result<()> {
            self.tick_completes += 1;
            if self.fail_tick_complete {
                anyhow::bail!("scripted reconcile failure");
            }
            Ok(())
        }
    }

    fn no_shutdown() -> CancellationToken {
        CancellationToken::new()
    }

    /// Assert a `build()` result succeeded and hand it back, without
    /// `.expect()` (denied workspace-wide, tests included). A call site
    /// destructures the `Ok` via `let Ok(x) = assert_built(built) else {
    /// return };` — the preceding `assert!` already fails the test loudly, so
    /// the `else` arm is unreachable in practice.
    fn assert_built(built: Result<MultiplexedPoller>) -> Result<MultiplexedPoller> {
        assert!(
            built.is_ok(),
            "build must succeed: {}",
            built
                .as_ref()
                .err()
                .map(|e| format!("{e:#}"))
                .unwrap_or_default()
        );
        built
    }

    const ADDR_A: Address = address!("0x1111111111111111111111111111111111111111");
    const ADDR_B: Address = address!("0x2222222222222222222222222222222222222222");
    const TOPIC_A: B256 =
        b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const TOPIC_B: B256 =
        b256!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    /// A route on `(addr, topic0)` starting live-from-head (no backfill),
    /// wrapping a fresh `ScriptedSink`. The common shape most tests build on.
    fn head_route(
        label: &'static str,
        addr: Address,
        topic0: B256,
    ) -> (Route, Arc<std::sync::Mutex<ScriptedSink>>) {
        seeded_route(
            label,
            addr,
            topic0,
            CursorStart::HeadMinusWindow { window_blocks: 0 },
        )
    }

    /// Records every delivery through an `Arc<Mutex<_>>` handle so tests can
    /// assert on the sink after it has been boxed into the route.
    struct MutexSink(Arc<std::sync::Mutex<ScriptedSink>>);

    impl LogSink for MutexSink {
        async fn apply(&mut self, log: Log) -> Result<()> {
            let mut guard = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let fail = guard.fail_apply_on == Some(guard.applied.len());
            if fail {
                anyhow::bail!("scripted sink failure");
            }
            let topic0 = log.topic0().copied().unwrap_or_default();
            guard
                .applied
                .push((log.address(), topic0, log.block_number));
            Ok(())
        }

        async fn on_tick_complete(&mut self) -> Result<()> {
            let mut guard = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.tick_completes += 1;
            if guard.fail_tick_complete {
                anyhow::bail!("scripted reconcile failure");
            }
            Ok(())
        }
    }

    fn seeded_route(
        label: &'static str,
        addr: Address,
        topic0: B256,
        start: CursorStart,
    ) -> (Route, Arc<std::sync::Mutex<ScriptedSink>>) {
        let recorded = Arc::new(std::sync::Mutex::new(ScriptedSink::default()));
        let route = Route {
            addresses: vec![addr],
            topic0s: vec![topic0],
            start,
            sink: Box::new(MutexSink(Arc::clone(&recorded))),
            label,
            on_established: None,
            on_backoff: None,
            on_tick_success: None,
            on_task_panic: None,
        };
        (route, recorded)
    }

    // --- Step 2/3: ErasedSink blanket adapter --------------------------------

    #[tokio::test]
    async fn erased_sink_forwards_apply_and_tick_complete() {
        let mut sink: Box<dyn ErasedSink> = Box::new(ScriptedSink::default());
        let log = log_at(ADDR_A, TOPIC_A, 1);
        assert!(sink.apply(log).await.is_ok());
        assert!(sink.on_tick_complete().await.is_ok());

        // Downcasting isn't available (no `Any` bound), so drive counts
        // through a second, directly-observable sink instance instead.
        let recorded = Arc::new(std::sync::Mutex::new(ScriptedSink::default()));
        let mut erased: Box<dyn ErasedSink> = Box::new(MutexSink(Arc::clone(&recorded)));
        let applied = erased.apply(log_at(ADDR_A, TOPIC_A, 1)).await;
        assert!(
            applied.is_ok(),
            "apply must forward through the blanket impl: {applied:?}"
        );
        let completed = erased.on_tick_complete().await;
        assert!(
            completed.is_ok(),
            "on_tick_complete must forward through the blanket impl: {completed:?}"
        );
        let guard = recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            guard.applied.len(),
            1,
            "apply must forward through the blanket impl"
        );
        assert_eq!(
            guard.tick_completes, 1,
            "on_tick_complete must forward through the blanket impl"
        );
    }

    // --- Step 4/5: build() dup-key rejection + key_index -----------------------

    #[test]
    fn duplicate_route_key_is_a_build_error() {
        let head: Arc<dyn HeadSource> = Arc::new(StaticHead(100));
        let (route_a, _) = head_route("a", ADDR_A, TOPIC_A);
        let (route_b, _) = head_route("b", ADDR_A, TOPIC_A); // same (address, topic0)

        let built = MultiplexedPollerBuilder::new(head, Duration::from_secs(1))
            .route(route_a)
            .route(route_b)
            .build();
        assert!(
            built.is_err(),
            "two routes claiming the same (address, topic0) must fail to build"
        );
    }

    #[test]
    fn distinct_route_keys_build_successfully() {
        let head: Arc<dyn HeadSource> = Arc::new(StaticHead(100));
        let (route_a, _) = head_route("a", ADDR_A, TOPIC_A);
        let (route_b, _) = head_route("b", ADDR_A, TOPIC_B); // same address, different topic0

        let built = MultiplexedPollerBuilder::new(head, Duration::from_secs(1))
            .route(route_a)
            .route(route_b)
            .build();
        assert!(
            built.is_ok(),
            "distinct (address, topic0) keys must build: {}",
            built.err().map(|e| format!("{e:#}")).unwrap_or_default()
        );
    }

    /// A `HeadSource` that always reports a fixed head — used where a test only
    /// needs `build()` to succeed and never drives a tick.
    struct StaticHead(u64);

    #[async_trait]
    impl HeadSource for StaticHead {
        async fn head(&self) -> Result<u64> {
            Ok(self.0)
        }
    }

    // --- Mocked-provider tick harness (mirrors resumable_watcher's) -----------

    /// `SharedHead::with_ttl(.., Duration::ZERO, None)`: queued head/`get_logs`
    /// responses are consumed in strict order (see `resumable_watcher.rs`'s
    /// `tick_cfg` doc for why a non-zero TTL would desync the queue).
    fn shared_head<P: Provider + 'static>(provider: P) -> Arc<dyn HeadSource> {
        Arc::new(SharedHead::with_ttl(provider, Duration::ZERO, None))
    }

    // --- Step 6/7: demux by (address, topic0) ----------------------------------

    #[tokio::test]
    async fn demux_routes_logs_by_address_and_topic0() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        // Two routes on the SAME address, different topic0s (models
        // PaymentPool settlement + rate-bounds sharing one contract).
        let (route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
        let (route_b, sink_b) = head_route("b", ADDR_A, TOPIC_B);
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(10)
                .route(route_a)
                .route(route_b)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        // head=1: both HeadMinusWindow{0} routes float to floor=1, one window
        // [1,1] carries one log per topic.
        asserter.push_success(&U64::from(1));
        asserter.push_success(&vec![
            log_at(ADDR_A, TOPIC_A, 1),
            log_at(ADDR_A, TOPIC_B, 1),
        ]);

        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_ok(), "tick must succeed: {result:?}");

        let a = sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let b = sink_b
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            a.applied,
            vec![(ADDR_A, TOPIC_A, Some(1))],
            "route A gets only topic A"
        );
        assert_eq!(
            b.applied,
            vec![(ADDR_A, TOPIC_B, Some(1))],
            "route B gets only topic B"
        );
    }

    // --- Step 8: per-route floor gate -------------------------------------------

    #[tokio::test]
    async fn route_below_its_floor_ignores_old_logs() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        // Route A floors at head (HeadMinusWindow{0}); route B seeds low, so
        // the merged scan range is dragged down to B's floor and includes
        // blocks below A's.
        let (route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
        let (route_b, sink_b) = seeded_route(
            "b",
            ADDR_B,
            TOPIC_B,
            CursorStart::Seeded {
                at: 0,
                persist: None,
            },
        );
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(10)
                .route(route_a)
                .route(route_b)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        // head=5: A floors at 5, B floors at 0 -> merged range [0,5], one
        // window. A log for A's key sits at block 2, below A's floor of 5.
        asserter.push_success(&U64::from(5));
        asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 2)]);

        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_ok(), "tick must succeed: {result:?}");

        let a = sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let b = sink_b
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            a.applied.is_empty(),
            "route A must gate out a log below its own floor"
        );
        assert!(
            b.applied.is_empty(),
            "route B saw no matching log this tick"
        );
        drop(a);
        drop(b);
        assert_eq!(
            poller.routes.first().and_then(|r| r.cursor),
            Some(6),
            "route A's cursor still advances past the window despite gating the log"
        );
    }

    // --- Step 9: error isolation + sibling advance + idempotent re-scan --------

    #[tokio::test]
    async fn route_error_isolates_and_siblings_advance() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let (route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
        let (route_b, sink_b) = head_route("b", ADDR_B, TOPIC_B);
        sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fail_apply_on = Some(0);
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(10)
                .route(route_a)
                .route(route_b)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        // Tick 1: head=1, one window carrying a log for each route. A's sink
        // errors; B's succeeds.
        asserter.push_success(&U64::from(1));
        asserter.push_success(&vec![
            log_at(ADDR_A, TOPIC_A, 1),
            log_at(ADDR_B, TOPIC_B, 1),
        ]);
        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(
            result.is_err(),
            "a route error must fail the tick (drives loop backoff)"
        );
        assert_eq!(
            poller.routes.first().and_then(|r| r.cursor),
            Some(1),
            "route A holds its cursor at its floor after erroring"
        );
        assert_eq!(
            poller.routes.get(1).and_then(|r| r.cursor),
            Some(2),
            "route B advances past the window despite A's error"
        );
        assert!(
            sink_a
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .applied
                .is_empty(),
            "route A's failed apply is not recorded"
        );
        assert_eq!(
            sink_b
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .applied
                .len(),
            1,
            "route B's log applied despite A's sibling error"
        );

        // Tick 2 (retry): A no longer errors. Same head; the merged range is
        // still [1,1] (A's floor). A re-applies the window's log (idempotent
        // re-scan); B's floor has moved to 2, so it gates the same log out.
        sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fail_apply_on = None;
        asserter.push_success(&U64::from(1));
        asserter.push_success(&vec![
            log_at(ADDR_A, TOPIC_A, 1),
            log_at(ADDR_B, TOPIC_B, 1),
        ]);
        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_ok(), "retry tick should complete: {result:?}");
        assert_eq!(
            sink_a
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .applied
                .len(),
            1,
            "route A re-applies the window's log on retry"
        );
        assert_eq!(
            sink_b
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .applied
                .len(),
            1,
            "route B does not re-apply — its floor already covers this range"
        );
    }

    // --- Step 10: one get_logs per tick for N routes ----------------------------

    /// A JSON-RPC mock counting `eth_blockNumber` and `eth_getLogs` hits.
    struct CountingRpc {
        head_hits: Arc<AtomicUsize>,
        getlogs_hits: Arc<AtomicUsize>,
    }

    impl wiremock::Respond for CountingRpc {
        fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null);
            let id = body.get("id").cloned().unwrap_or(serde_json::json!(0));
            let result = match body.get("method").and_then(serde_json::Value::as_str) {
                Some("eth_blockNumber") => {
                    self.head_hits.fetch_add(1, Ordering::SeqCst);
                    serde_json::json!("0x64")
                }
                Some("eth_getLogs") => {
                    self.getlogs_hits.fetch_add(1, Ordering::SeqCst);
                    serde_json::json!([])
                }
                _ => serde_json::json!("0x1"),
            };
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": id, "result": result,
            }))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn n_routes_share_one_getlogs_per_tick() {
        const ROUTES: usize = 4;
        const INTERVAL: Duration = Duration::from_millis(100);
        const RUN_FOR: Duration = Duration::from_millis(350);

        let head_hits = Arc::new(AtomicUsize::new(0));
        let getlogs_hits = Arc::new(AtomicUsize::new(0));
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(CountingRpc {
                head_hits: Arc::clone(&head_hits),
                getlogs_hits: Arc::clone(&getlogs_hits),
            })
            .mount(&server)
            .await;
        let parsed_url = server.uri().parse();
        assert!(
            parsed_url.is_ok(),
            "mock server uri must parse: {}",
            parsed_url
                .as_ref()
                .err()
                .map(ToString::to_string)
                .unwrap_or_default()
        );
        let Ok(url) = parsed_url else { return };
        let provider = ProviderBuilder::new().connect_http(url);

        let head: Arc<dyn HeadSource> = Arc::new(SharedHead::new(provider.clone(), INTERVAL));
        let mut builder = MultiplexedPollerBuilder::new(head, INTERVAL).max_backfill_span(10_000);
        // 4 routes across 2 addresses, all HeadMinusWindow{0} (live tail).
        for (i, (addr, topic0)) in [
            (ADDR_A, TOPIC_A),
            (ADDR_A, TOPIC_B),
            (ADDR_B, TOPIC_A),
            (ADDR_B, TOPIC_B),
        ]
        .into_iter()
        .enumerate()
        {
            let (route, _) = head_route(
                Box::leak(format!("route-{i}").into_boxed_str()),
                addr,
                topic0,
            );
            builder = builder.route(route);
        }
        let Ok(poller) = assert_built(builder.build()) else {
            return;
        };

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(run(provider, poller, shutdown.clone()));
        tokio::time::sleep(RUN_FOR).await;
        shutdown.cancel();
        let _ = handle.await;

        let getlogs = getlogs_hits.load(Ordering::SeqCst);
        let per_ms = |d: Duration| d.as_millis().max(1);
        let ticks = (per_ms(RUN_FOR) / per_ms(INTERVAL)) as usize + 2;
        assert!(
            getlogs < ROUTES * ticks,
            "merged polling must beat one get_logs per route per tick \
             (getlogs={getlogs}, unmerged would approach {})",
            ROUTES * ticks
        );
        assert!(
            getlogs >= 1,
            "the poller must have issued at least one get_logs"
        );
    }

    // --- Step 11: idle tick reconciles all routes -------------------------------

    #[tokio::test]
    async fn idle_tick_reconciles_all_routes() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        // Both routes seeded past head -> no get_logs issued this tick.
        let (route_a, sink_a) = seeded_route(
            "a",
            ADDR_A,
            TOPIC_A,
            CursorStart::Seeded {
                at: 100,
                persist: None,
            },
        );
        let (route_b, sink_b) = seeded_route(
            "b",
            ADDR_B,
            TOPIC_B,
            CursorStart::Seeded {
                at: 100,
                persist: None,
            },
        );
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .route(route_a)
                .route(route_b)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        asserter.push_success(&U64::from(5)); // head < both cursors: idle
        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_ok(), "idle tick must succeed: {result:?}");
        assert_eq!(
            sink_a
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .tick_completes,
            1,
            "route A's on_tick_complete must fire on an idle tick"
        );
        assert_eq!(
            sink_b
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .tick_completes,
            1,
            "route B's on_tick_complete must fire on an idle tick"
        );
    }

    // --- Step 12: shutdown flushes every persisting route -----------------------

    #[derive(Default)]
    struct FlushCountingStore {
        flushes: AtomicUsize,
    }

    impl KeyedCheckpointStore for FlushCountingStore {
        fn load_checkpoint(
            &self,
            _key: CheckpointKey,
        ) -> std::result::Result<Option<u64>, StoreError> {
            Ok(None)
        }
        fn record_checkpoint(
            &self,
            _key: CheckpointKey,
            _block: u64,
        ) -> std::result::Result<(), StoreError> {
            Ok(())
        }
        fn flush_checkpoint(&self, _key: CheckpointKey) -> std::result::Result<(), StoreError> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_shutdown_flushes_every_persisting_route() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let store = Arc::new(FlushCountingStore::default());

        let (persisting, _) = seeded_route(
            "persisting",
            ADDR_A,
            TOPIC_A,
            CursorStart::FromCheckpoint {
                checkpoint: Checkpoint {
                    store: Arc::clone(&store) as Arc<dyn KeyedCheckpointStore>,
                    key: CheckpointKey::PoolOpened,
                },
                reorg_margin: 0,
                cold_start: ColdStart::Head,
            },
        );
        let (ephemeral, _) = head_route("ephemeral", ADDR_B, TOPIC_B); // no checkpoint: flush is a no-op
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .route(persisting)
                .route(ephemeral)
                .build();
        let Ok(poller) = assert_built(built) else {
            return;
        };

        let shutdown = CancellationToken::new();
        // Cancel before spawning: the loop's first act is the biased select on
        // the token, so the flush arm is taken deterministically.
        shutdown.cancel();
        let task = tokio::spawn(run(provider, poller, shutdown));
        assert!(task.await.is_ok(), "run must return, not hang or panic");
        assert_eq!(
            store.flushes.load(Ordering::SeqCst),
            1,
            "the persisting route must flush its checkpoint exactly once"
        );
    }

    // --- Step 13: head-read failure fails the tick + fires on_backoff for all --

    #[tokio::test]
    async fn head_read_failure_fails_the_tick_and_backs_off_every_route() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let backoff_a = Arc::new(AtomicUsize::new(0));
        let backoff_b = Arc::new(AtomicUsize::new(0));
        let (mut route_a, _) = head_route("a", ADDR_A, TOPIC_A);
        let (mut route_b, _) = head_route("b", ADDR_B, TOPIC_B);
        {
            let counter = Arc::clone(&backoff_a);
            route_a.on_backoff = Some(Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        }
        {
            let counter = Arc::clone(&backoff_b);
            route_b.on_backoff = Some(Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        }
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .route(route_a)
                .route(route_b)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        asserter.push_failure_msg("head is down");
        let err = run_tick(&provider, &mut poller, &no_shutdown())
            .await
            .err()
            .map(|e| format!("{e:#}"));
        assert!(
            err.as_ref().is_some_and(|e| e.contains("read head block")),
            "head failure must fail the tick with its context: {err:?}"
        );
        assert_eq!(
            backoff_a.load(Ordering::SeqCst),
            1,
            "route A must fire on_backoff"
        );
        assert_eq!(
            backoff_b.load(Ordering::SeqCst),
            1,
            "route B must fire on_backoff"
        );
    }

    // --- Step 14: panic in a route sink fires every route's on_task_panic ------

    #[tokio::test]
    #[allow(clippy::panic)] // deliberately panic inside a tick to exercise the guard.
    async fn run_task_panic_fires_every_routes_panic_hook() {
        struct PanicOnApply;
        impl LogSink for PanicOnApply {
            async fn apply(&mut self, _log: Log) -> Result<()> {
                panic!("intentional tick panic");
            }
        }

        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let panicked_a = Arc::new(AtomicUsize::new(0));
        let panicked_b = Arc::new(AtomicUsize::new(0));
        let route_a = {
            let counter = Arc::clone(&panicked_a);
            Route {
                addresses: vec![ADDR_A],
                topic0s: vec![TOPIC_A],
                start: CursorStart::HeadMinusWindow { window_blocks: 0 },
                sink: Box::new(PanicOnApply),
                label: "a",
                on_established: None,
                on_backoff: None,
                on_tick_success: None,
                on_task_panic: Some(Box::new(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                })),
            }
        };
        let (route_b, _) = head_route("b", ADDR_B, TOPIC_B);
        let route_b = Route {
            on_task_panic: Some({
                let counter = Arc::clone(&panicked_b);
                Box::new(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                })
            }),
            ..route_b
        };

        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .route(route_a)
                .route(route_b)
                .build();
        let Ok(poller) = assert_built(built) else {
            return;
        };

        asserter.push_success(&U64::from(1));
        asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 1)]);

        let shutdown = CancellationToken::new();
        let joined = tokio::spawn(run(provider, poller, shutdown)).await;
        assert!(joined.is_err(), "the poller task must have panicked");
        assert_eq!(
            panicked_a.load(Ordering::SeqCst),
            1,
            "the panicking route's own on_task_panic must fire"
        );
        assert_eq!(
            panicked_b.load(Ordering::SeqCst),
            1,
            "a sibling route's on_task_panic must ALSO fire — the whole task died"
        );
    }
}
