//! Multiplexed `eth_getLogs` polling: one poll tick, one `get_logs` call per
//! backfill window, demuxed by `(address, topic0)` into per-route sinks.
//!
//! [`resumable_watcher`](super::resumable_watcher) gives every watcher its own
//! cursor loop and its own `eth_getLogs` call each tick. That is right when a
//! watcher's filter genuinely differs from its siblings', but five watchers on
//! this node scan disjoint `(address, topic0)` slices of the same few
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
//! # Hooks fire per route, not per loop
//!
//! A single-sink loop would fire `on_backoff`/`on_established` once per tick,
//! because there is exactly one sink. Here the *tick* fires each route's hooks
//! independently (routes fail independently), and the loop's `Err` arm only
//! sleeps the backoff — it never fires a hook itself. The exception is a
//! failure in the shared window scan: the head read, a route's checkpoint-load
//! floor derivation, or a merged `get_logs` call that is not retried, all
//! before any route-specific step runs; or a stall past
//! [`GET_LOGS_STALL_BUDGET`], after the tick's completed windows are applied
//! and persisted but before the reconcile. Those fail the *whole* tick, so
//! [`fail_whole_tick`] fires `on_backoff` for every route directly at the
//! failure site — every route is equally down, as five independent watchers
//! that each read the shared head through
//! [`super::shared_head::SharedHead`] would all convoy into backoff together.
//!
//! The `on_established`/`on_backoff` *edges* are suppressed once `shutdown` is
//! cancelled (a tick that only "succeeded" because a sink observed the cancel
//! token must not flip a readiness gate open), while `on_tick_success` keeps
//! stamping unconditionally.
//!
//! # Provider range caps
//!
//! A `get_logs` failure that [`is_range_rejection`] recognises does not fail the
//! tick. [`run_tick`] sets the window span to half the rejected window's length
//! and retries the same start block at once; no route hook fires. The span
//! doubles back after a run of accepted windows ([`WindowSpan`]), so a
//! transient rejection does not cost throughput until restart. A rejected one-block
//! window, and any other `get_logs` failure that is not retried below, goes
//! through [`fail_whole_tick`].
//!
//! # Transient window failures
//!
//! A tick that is behind head scans several windows, and it reaches head only
//! if every window succeeds. A provider that fails a fraction of its calls
//! would therefore stop most long ticks short of head. So [`run_tick`] retries a
//! failed window in place:
//! a `get_logs` error that [`is_transient_window_error`] accepts is retried on
//! the same block range up to [`GET_LOGS_WINDOW_RETRIES`] times, after
//! [`GET_LOGS_WINDOW_RETRY_DELAY`] each. A retry that succeeds is a plain
//! success: no route's `on_backoff` fires and no route is marked down. Each
//! retry logs its cause at `info` and fires the `on_window_retry` hook (the
//! `decdn_chain_get_logs_retries_total` counter), so an unreliable provider
//! stays visible although the retries keep it out of watcher downtime.
//!
//! Three kinds of failure are not retried in place. A range rejection takes the
//! shrink path above. An error that [`is_permanent_rpc_error`] calls
//! deterministic fails the same way on every attempt. A rate limit
//! ([`is_rate_limit`]) goes to the loop's exponential backoff, because a
//! fixed-delay retry spends more of a quota that is already used up.
//!
//! The retries lengthen a failing window. A provider that hangs costs each
//! window `GET_LOGS_WINDOW_RETRIES + 1` per-call timeouts plus the retry
//! sleeps (34 s at the 10 s default timeout) before the window is deferred.
//!
//! # Deferred windows
//!
//! A provider's transient failures cluster in time, so a window that fails
//! every retry often fails again a few seconds later. Failing the tick then
//! would mark every route down and count a watcher restart for an outage that
//! loses no event. So [`run_tick`] defers such a window instead: it ends the
//! tick there, keeps the progress of the windows before it, and the next tick
//! resumes at the deferred window's start. Each deferral logs its cause at
//! `info` and fires the `on_window_deferred` hook (the
//! `decdn_chain_get_logs_deferred_total` counter).
//!
//! A deferred tick that completed a window is a success that ended early: it
//! reconciles and fires every route's hooks. A deferred tick that completed no
//! window fires no hook and skips the reconcile, so no tick gauge, freshness
//! stamp or `on_tick_complete` backstop moves. A stall
//! clock starts at the first deferred tick and clears only when a tick reaches
//! head, so progress that keeps falling behind head cannot hide behind the
//! early successes. A deferred tick fails through [`fail_whole_tick`] once no
//! tick has reached head for [`GET_LOGS_STALL_BUDGET`].
//!
//! The runtime registers each `eth_getLogs` watcher's [`Route`] on one
//! poller in `build_chain_and_handlers` and spawns it once — one merged loop in
//! place of five independent per-watcher loops (four when the fee-shares
//! route is absent: it registers only when the startup `feeRouter()` read
//! succeeds).
//!
//! This module is `pub` only so `Route` can appear in the `pub` watcher
//! `bootstrap` signatures and the external settlement e2e can drive `spawn`; its
//! docs still reference the crate-internal collaborators (`ErasedSink`,
//! `CursorStart`, the `resumable_watcher` cursor vocabulary), so intra-doc links
//! to those private items are allowed here rather than downgraded to prose.
#![allow(rustdoc::private_intra_doc_links)]

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::transports::TransportError;
use anyhow::{Context, Result};
use async_trait::async_trait;
use decdn_client::provider::is_permanent_rpc_error;
use decdn_common::config::DEFAULT_GET_LOGS_MAX_BLOCK_SPAN;
use decdn_common::redact::sanitize_err_chain;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error, info, warn};

use crate::rpc_metrics::is_rate_limit;

use super::resumable_watcher::{CursorStart, LogSink, WatcherHandle, WatcherHook, fire};
use super::shared_head::HeadSource;
use super::{Shrunk, WindowSpan, timed, window_end};
use super::{WATCHER_INITIAL_BACKOFF, WATCHER_MAX_BACKOFF};

/// Object-safe adapter over [`LogSink`], so routes with different concrete sink
/// types live in one `Vec<Box<dyn ErasedSink>>`. `LogSink::apply` /
/// `on_tick_complete` return `impl Future`, which is not object-safe; this
/// trait is, and is blanket-implemented for every `LogSink` so no sink writes
/// it by hand.
#[async_trait]
pub(crate) trait ErasedSink: Send {
    async fn apply(&mut self, log: Log) -> Result<()>;
    async fn on_tick_complete(&mut self) -> Result<()>;
    fn on_recovered(&mut self);
}

#[async_trait]
impl<S: LogSink> ErasedSink for S {
    async fn apply(&mut self, log: Log) -> Result<()> {
        LogSink::apply(self, log).await
    }
    async fn on_tick_complete(&mut self) -> Result<()> {
        LogSink::on_tick_complete(self).await
    }
    fn on_recovered(&mut self) {
        LogSink::on_recovered(self);
    }
}

/// A route's sink, resolved to a concrete [`ErasedSink`] inside [`spawn`] once
/// the poller mints its single shutdown token.
///
/// Most routes carry a `Ready` sink built at registration. The blacklist route
/// is the exception: its sink must observe the poller's own shutdown token (its
/// re-scope pass polls it between per-hash `eth_call`s so a large deny-set does
/// not overrun the shutdown deadline), and that token does not exist until
/// [`spawn`] mints it. Such a route registers a `Factory` that [`spawn`] invokes
/// with the freshly-minted token — a per-route `make_sink` seam that keeps the
/// token paired with a live task so it can never be inert (#1236).
pub(crate) enum SinkSource {
    Ready(Box<dyn ErasedSink>),
    Factory(SinkFactory),
}

/// A per-route sink builder invoked inside [`spawn`] with the poller's
/// freshly-minted shutdown token — see [`SinkSource::Factory`].
pub(crate) type SinkFactory = Box<dyn FnOnce(&CancellationToken) -> Box<dyn ErasedSink> + Send>;

impl SinkSource {
    /// Resolve a `Factory` against the poller's shutdown token; a `Ready` sink
    /// passes through unchanged. Consumed by value so no placeholder sink is
    /// needed to swap it out.
    fn resolved(self, shutdown: &CancellationToken) -> Self {
        match self {
            Self::Factory(make) => Self::Ready(make(shutdown)),
            ready @ Self::Ready(_) => ready,
        }
    }

    /// The concrete sink once resolved. `None` for a still-`Factory` source —
    /// only reachable if [`run`] is driven without going through [`spawn`], as
    /// the unit tests do, and those always register `Ready` sinks.
    fn as_erased_mut(&mut self) -> Option<&mut (dyn ErasedSink + 'static)> {
        match self {
            Self::Ready(sink) => Some(sink.as_mut()),
            Self::Factory(_) => None,
        }
    }
}

/// One registered watcher on the multiplexed poller. `addresses`/`topic0s` are
/// its demux keys; every `(address, topic0)` pair in the cartesian product is a
/// route key, and every route key must be globally unique across the poller's
/// routes ([`MultiplexedPollerBuilder::build`] checks this). Each existing
/// watcher is single-address, so `addresses` is a 1-element vec today, but the
/// field is a set so a future multi-address watcher needs no reshape.
pub struct Route {
    pub(crate) addresses: Vec<Address>,
    pub(crate) topic0s: Vec<B256>,
    pub(crate) start: CursorStart,
    pub(crate) sink: SinkSource,
    pub(crate) label: &'static str,
    pub(crate) on_established: Option<WatcherHook>,
    pub(crate) on_backoff: Option<WatcherHook>,
    pub(crate) on_tick_success: Option<WatcherHook>,
    pub(crate) on_task_panic: Option<WatcherHook>,
}

impl std::fmt::Debug for Route {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The sink and hooks are opaque closures/trait objects; the demux keys
        // and label are what identify a route in a log line.
        f.debug_struct("Route")
            .field("label", &self.label)
            .field("addresses", &self.addresses)
            .field("topic0s", &self.topic0s)
            .finish_non_exhaustive()
    }
}

/// A [`Route`]'s per-tick working state: its cursor machinery and sink, plus
/// the fields `run_tick` mutates each tick. Does not carry `on_task_panic` —
/// that hook is extracted into [`MultiplexedPoller::panic_hooks`] at build
/// time, a separate field the [`PanicGuard`] in [`run`] can borrow immutably
/// for the whole task without conflicting with `run_tick`'s per-tick `&mut`
/// borrows of `routes` (see that guard's doc for why the split exists).
struct RouteState {
    start: CursorStart,
    sink: SinkSource,
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
    /// First-cycle edge tracking for `on_established`.
    established: bool,
    /// Set when this route — or the tick as a whole, via `fail_whole_tick` —
    /// errored on an earlier tick and has not yet been told it recovered;
    /// consumed by `notify_recovered_routes` on the first tick it comes back
    /// clean.
    ///
    /// Not derived from `!established`: that is also true on the first-ever
    /// tick, and a sink whose reconcile just ran at bootstrap would then be
    /// asked to re-read immediately. The condition this records is "errored at
    /// least once", which a shared-read failure on the very first tick does
    /// satisfy — that route then gets one redundant re-read, which is safe.
    recovering: bool,
}

/// Poller configuration, its resolved routes, and the demux index built once
/// at [`MultiplexedPollerBuilder::build`].
pub struct MultiplexedPoller {
    head: Arc<dyn HeadSource>,
    routes: Vec<RouteState>,
    /// `(address, topic0) -> route index`. Built once at `build()`; a log
    /// whose key is absent is not subscribed by any route and is dropped.
    key_index: HashMap<(Address, B256), usize>,
    /// Merged filter over every route's addresses and topic0s, built once at
    /// `build()`. The block range is set per window in `run_tick`.
    base_filter: Filter,
    /// Contract deploy floor — the lower clamp on a rewound `FromCheckpoint`
    /// resume and the floor a `HeadMinusWindow` start derives from. Always `0`
    /// today (every route either seeds its cursor from an enumeration or clamps a
    /// persisted resume that never predates deploy), retained as that floor.
    from_block: u64,
    poll_interval: Duration,
    /// Block span of the next `eth_getLogs` window, learned from the provider:
    /// it starts at the configured ceiling, shrinks on a range rejection and
    /// doubles back after a run of accepted windows (see [`WindowSpan`]).
    span: WindowSpan,
    /// Called with the new span when it changes, and once with the starting
    /// span when [`run`] starts (the `decdn_chain_get_logs_span` gauge).
    on_span_change: Option<SpanHook>,
    /// Called on every range rejection the poller recognises, including a
    /// rejected one-block window.
    on_range_rejection: Option<WatcherHook>,
    /// Called on every in-tick retry of a transiently failed `get_logs`
    /// window (see the module doc's "Transient window failures").
    on_window_retry: Option<WatcherHook>,
    /// Called each time a window fails every retry and the tick defers it to
    /// the next tick (see the module doc's "Deferred windows").
    on_window_deferred: Option<WatcherHook>,
    /// When the poller stopped reaching head: set by the first deferred tick,
    /// cleared only by a tick that reaches head. A stall older than
    /// [`GET_LOGS_STALL_BUDGET`] fails the tick.
    stalled_since: Option<tokio::time::Instant>,
    initial_backoff: Duration,
    /// Ceiling for the shared loop's backoff. One loop serves every route, so
    /// no route carries its own tighter cap — [`WATCHER_MAX_BACKOFF`] (60s)
    /// applies to the whole poller.
    max_backoff: Duration,
    rpc_call_timeout: Option<Duration>,
    /// Every route's `(label, on_task_panic)`, extracted out of `routes` at
    /// build time — see [`RouteState`]'s doc for why they live here instead.
    panic_hooks: Vec<(&'static str, Option<WatcherHook>)>,
}

impl std::fmt::Debug for MultiplexedPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiplexedPoller")
            .field("routes", &self.routes.len())
            .field("poll_interval", &self.poll_interval)
            .finish_non_exhaustive()
    }
}

/// Receives the poller's `eth_getLogs` window span once when the poller starts
/// and each time the span changes.
pub(crate) type SpanHook = Box<dyn Fn(u64) + Send + Sync>;

/// Accumulates [`Route`]s and builds a [`MultiplexedPoller`].
pub struct MultiplexedPollerBuilder {
    head: Arc<dyn HeadSource>,
    routes: Vec<Route>,
    from_block: u64,
    poll_interval: Duration,
    max_backfill_span: u64,
    on_span_change: Option<SpanHook>,
    on_range_rejection: Option<WatcherHook>,
    on_window_retry: Option<WatcherHook>,
    on_window_deferred: Option<WatcherHook>,
    initial_backoff: Duration,
    max_backoff: Duration,
    rpc_call_timeout: Option<Duration>,
}

impl std::fmt::Debug for MultiplexedPollerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiplexedPollerBuilder")
            .field("routes", &self.routes.len())
            .field("poll_interval", &self.poll_interval)
            .finish_non_exhaustive()
    }
}

impl MultiplexedPollerBuilder {
    /// Construct with the defaults every route shares: the deploy floor (`0`),
    /// [`DEFAULT_GET_LOGS_MAX_BLOCK_SPAN`], and the default watcher backoff
    /// schedule.
    #[must_use]
    pub fn new(head: Arc<dyn HeadSource>, poll_interval: Duration) -> Self {
        Self {
            head,
            routes: Vec::new(),
            from_block: 0,
            poll_interval,
            max_backfill_span: DEFAULT_GET_LOGS_MAX_BLOCK_SPAN,
            on_span_change: None,
            on_range_rejection: None,
            on_window_retry: None,
            on_window_deferred: None,
            initial_backoff: WATCHER_INITIAL_BACKOFF,
            max_backoff: WATCHER_MAX_BACKOFF,
            rpc_call_timeout: None,
        }
    }

    /// Register one watcher's route.
    #[must_use]
    pub fn route(mut self, route: Route) -> Self {
        self.routes.push(route);
        self
    }

    /// Set the block-span ceiling per `eth_getLogs` window
    /// (`blockchain.get_logs_max_block_span`). The poller starts there,
    /// shrinks below it on a provider range rejection and regrows back to it.
    /// [`Self::build`] rejects `0`.
    #[must_use]
    pub const fn max_backfill_span(mut self, span: u64) -> Self {
        self.max_backfill_span = span;
        self
    }

    /// The configured block-span ceiling.
    #[cfg(test)]
    pub(crate) const fn span_ceiling(&self) -> u64 {
        self.max_backfill_span
    }

    /// Observe the window span: called once when the poller starts and on
    /// every shrink or regrow.
    #[must_use]
    pub(crate) fn on_span_change(mut self, hook: SpanHook) -> Self {
        self.on_span_change = Some(hook);
        self
    }

    /// Observe every provider range rejection the poller recognises.
    #[must_use]
    pub(crate) fn on_range_rejection(mut self, hook: WatcherHook) -> Self {
        self.on_range_rejection = Some(hook);
        self
    }

    /// Observe every in-tick retry of a transiently failed `get_logs` window.
    #[must_use]
    pub(crate) fn on_window_retry(mut self, hook: WatcherHook) -> Self {
        self.on_window_retry = Some(hook);
        self
    }

    /// Observe every `get_logs` window the poller defers to the next tick
    /// after it fails every in-tick retry.
    #[must_use]
    pub(crate) fn on_window_deferred(mut self, hook: WatcherHook) -> Self {
        self.on_window_deferred = Some(hook);
        self
    }

    /// Build the poller: construct the merged filter and the `(address,
    /// topic0) -> route` demux index. Returns `Err` if two routes claim the
    /// same `(address, topic0)` key — two watchers subscribing to the same
    /// event is a wiring bug, not a runtime condition, so it fails fast at
    /// startup rather than silently routing every such log to whichever route
    /// happened to register first.
    pub fn build(self) -> Result<MultiplexedPoller> {
        let span_ceiling = NonZeroU64::new(self.max_backfill_span)
            .context("multiplexed poller max_backfill_span must be at least 1 block")?;
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
        // Routes commonly share a contract address (settlement + rate-bounds on
        // PaymentPool; capacity-bond + slash on CapacityBond), so the merged
        // filter would otherwise carry duplicate addresses/topic0s — bloating the
        // `eth_getLogs` request for no benefit. Dedup before building it; the
        // per-route `key_index` above is what preserves demux correctness, not
        // the filter's multiplicity. Order is irrelevant to `eth_getLogs`.
        all_addresses.sort_unstable();
        all_addresses.dedup();
        all_topic0s.sort_unstable();
        all_topic0s.dedup();
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
                recovering: false,
            });
        }

        Ok(MultiplexedPoller {
            head: self.head,
            routes,
            key_index,
            base_filter,
            from_block: self.from_block,
            poll_interval: self.poll_interval,
            span: WindowSpan::new(span_ceiling),
            on_span_change: self.on_span_change,
            on_range_rejection: self.on_range_rejection,
            on_window_retry: self.on_window_retry,
            on_window_deferred: self.on_window_deferred,
            stalled_since: None,
            initial_backoff: self.initial_backoff,
            max_backoff: self.max_backoff,
            rpc_call_timeout: self.rpc_call_timeout,
            panic_hooks,
        })
    }
}

/// Fire `on_backoff` for every route (unless `shutdown` is cancelled — the
/// same edge suppression a successful tick applies) and clear every
/// route's `established` flag, then hand back `err` unchanged. Used only at
/// the shared window-scan failure points: the head read, a route's floor
/// derivation, or a merged `get_logs` call that is not retried, all before any
/// route-specific step has run; or a stall past [`GET_LOGS_STALL_BUDGET`],
/// after the completed windows are applied but before the reconcile. No single
/// route's `errored` flag would otherwise capture such a failure, and every
/// route is equally "down".
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
        r.recovering = true;
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
        // enumerated at a later snapshot block, or is a sibling still ahead) ignores logs below
        // its own floor. A `None` block_number applies rather than being
        // silently dropped.
        if log.block_number.is_some_and(|b| b < route.tick_floor) {
            continue;
        }
        // The sink borrow is confined to this match so `route.errored` can be
        // set afterward without aliasing it.
        let applied = if let Some(sink) = route.sink.as_erased_mut() {
            sink.apply(log).await
        } else {
            // Unreachable in production: `spawn` always resolves every
            // `SinkSource::Factory` before handing the poller to `run` (see
            // `SinkSource::as_erased_mut`'s doc). Silently `continue`ing here
            // would advance this route's cursor past a log it never applied —
            // a future wiring bug that skips factory resolution would drop
            // data with no signal, so fail loudly in debug/test builds and
            // hold the release-build fallback of skipping the log.
            debug_assert!(
                false,
                "route {} has an unresolved SinkSource::Factory at demux time; \
                 spawn() must resolve every factory before run()",
                route.label
            );
            continue;
        };
        if let Err(err) = applied {
            // Retryable sink error: isolate this route. It holds its cursor
            // and re-scans; sibling routes keep advancing.
            route.errored = true;
            warn!(
                label = route.label,
                error = %sanitize_err_chain(&err),
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

/// End-of-tick reconcile per route (blacklist re-scope — origin blacklisting
/// rides the blacklist route — capacity-bond/slash resync, rate-bounds and
/// fee-shares safety-net re-reads). A
/// reconcile `Err` marks that route errored (holds its cursor, retries).
async fn reconcile_routes(poller: &mut MultiplexedPoller) {
    for r in &mut poller.routes {
        if r.errored {
            continue;
        }
        let reconciled = if let Some(sink) = r.sink.as_erased_mut() {
            sink.on_tick_complete().await
        } else {
            // Unreachable in production; see the matching branch in
            // `demux_window_logs` for why this asserts instead of silently
            // skipping the reconcile.
            debug_assert!(
                false,
                "route {} has an unresolved SinkSource::Factory at reconcile time; \
                 spawn() must resolve every factory before run()",
                r.label
            );
            continue;
        };
        if let Err(err) = reconciled {
            r.errored = true;
            warn!(label = r.label, error = %sanitize_err_chain(&err), "route reconcile error");
        }
    }
}

/// Tell every route that just came back from an errored tick, before the
/// reconcile below acts on it.
///
/// Ordering is the point: a sink whose reconcile is cadence-gated clears its
/// clock here and re-reads on this same tick's `on_tick_complete`. Firing from
/// [`fire_route_hooks`], which runs after the reconcile, would delay that repair
/// a full tick. Suppressed once shutdown is cancelled, mirroring the established
/// edge — there is no point forcing a re-read the process is about to abandon.
fn notify_recovered_routes(poller: &mut MultiplexedPoller, shutdown: &CancellationToken) {
    if shutdown.is_cancelled() {
        return;
    }
    for r in &mut poller.routes {
        if r.errored || !r.recovering {
            continue;
        }
        let Some(sink) = r.sink.as_erased_mut() else {
            // Unreachable in production; see the matching branch in
            // `demux_window_logs`. The edge is deliberately NOT consumed here —
            // clearing it would lose the recovery notification for the whole
            // outage rather than for one tick.
            debug_assert!(
                false,
                "route {} has an unresolved SinkSource::Factory at recovery time; \
                 spawn() must resolve every factory before run()",
                r.label
            );
            continue;
        };
        r.recovering = false;
        sink.on_recovered();
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
            r.recovering = true;
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

/// Whether a failed `eth_getLogs` is the provider refusing the window's block
/// range (or its result count), so a smaller window can succeed. Providers use
/// different codes for this: dRPC `35 "ranges over 10000 blocks are not
/// supported"`, Alchemy `-32600 "… up to a 10 block range"`, Infura `-32005
/// "query returned more than 10000 results"` (also its rate-limit code),
/// Quicknode `-32614 "eth_getLogs is limited to a 10,000 range"`, Ankr
/// `"block range is too wide"`. The codes do not agree, so the test is the
/// message: a JSON-RPC error response that names a range or a result count
/// *and* a limit ([`LIMIT_WORDS`]). Both parts are required because a false
/// match still costs throughput until the span doubles back: a load-balanced provider whose backend lags head reports a
/// `toBlock` past that backend's head with range wording but no limit ("block
/// range extends beyond current head block", "block number is out of range"),
/// and a smaller window is not the fix for that. Any other error — a timeout, a
/// transport failure, a rate limit, an unknown block — leaves the window span
/// alone, so a transient fault cannot shrink it: it is retried in place
/// ([`is_transient_window_error`]) or takes the backoff path.
fn is_range_rejection(err: &anyhow::Error) -> bool {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<TransportError>())
        .and_then(TransportError::as_error_resp)
        .is_some_and(|resp| {
            let message = resp.message.to_ascii_lowercase();
            (message.contains("range") || message.contains("results"))
                && !message.contains("out of range")
                && LIMIT_WORDS.iter().any(|word| message.contains(word))
        })
}

/// Words that mark a range or result-count message as a limit, not a
/// head-lag or lookup error. See [`is_range_rejection`].
const LIMIT_WORDS: [&str; 10] = [
    "limit",
    "max",
    "exceed",
    "up to",
    " over ",
    "more than",
    "too many",
    "too wide",
    "too large",
    "not supported",
];

/// How many times [`run_tick`] retries a transiently failed `get_logs` window
/// before it defers the window to the next tick. At an independent per-call
/// failure rate of 20 %, all three attempts fail together 0.8 % of the time.
const GET_LOGS_WINDOW_RETRIES: u32 = 2;

/// The sleep before each in-tick retry of a failed `get_logs` window. A
/// load-balanced provider whose backend lags head fails a window at the tip
/// until the backend catches up, so an immediate retry often fails again.
const GET_LOGS_WINDOW_RETRY_DELAY: Duration = Duration::from_secs(2);

/// How long the poller may defer windows without a tick that reaches head
/// before a tick fails (see the module doc's "Deferred windows"). It is a
/// duration, not a tick count, because at the default 7 s poll interval a
/// deferred tick lasts from about 11 s (the interval plus the retry sleeps) to
/// about 41 s (three call timeouts plus the sleeps). The clock starts when the
/// first deferred tick ends and is read when a later one ends, so a poller
/// that makes no progress enters backoff about 130–165 s after its last tick
/// stamp: under the 180 s at which the `*WatcherStalled` expressions turn true
/// (they then hold for 5 m before they fire).
const GET_LOGS_STALL_BUDGET: Duration = Duration::from_mins(2);
/// Whether a failed `get_logs` window is worth a retry on the same block
/// range inside the tick. A range rejection is not: it takes the shrink path
/// ([`shrink_on_range_rejection`]). An error that [`is_permanent_rpc_error`]
/// calls deterministic is not either: a revert, an invalid request, method or
/// params response, an HTTP 4xx other than 408/429 (an expired API key, for
/// example), or a local serialization or usage error. A rate limit
/// ([`is_rate_limit`]) is not either: the quota refills on the provider's
/// schedule, not after [`GET_LOGS_WINDOW_RETRY_DELAY`], so it goes to the
/// loop's backoff.
/// Everything else is: a provider's "temporary internal error" or "request
/// timeout" response, a head-lag error, a transport failure, and a `timed`
/// timeout, which carries no typed cause.
fn is_transient_window_error(err: &anyhow::Error) -> bool {
    !is_range_rejection(err)
        && err
            .chain()
            .find_map(|cause| cause.downcast_ref::<TransportError>())
            .is_none_or(|cause| !is_permanent_rpc_error(cause) && !is_rate_limit(cause))
}

/// Handle a failed `get_logs` for the window `[start, end]`. A range rejection
/// the poller recognises shrinks the span to half the window and returns
/// `None`: the caller retries the same start block at once. A provider that
/// caps the `eth_getLogs` range rejects the window outright, and retrying the
/// same (or, as head moves, a wider) range can never succeed. Any other error
/// — and a rejected one-block window, which cannot shrink — comes back for the
/// backoff path.
fn shrink_on_range_rejection(
    poller: &mut MultiplexedPoller,
    start: u64,
    end: u64,
    err: anyhow::Error,
) -> Option<anyhow::Error> {
    if !is_range_rejection(&err) {
        return Some(err);
    }
    fire(poller.on_range_rejection.as_ref());
    let Some(Shrunk {
        span,
        rejected_probe,
    }) = poller.span.shrink(start, end)
    else {
        return Some(err.context(
            "the RPC provider rejects even a one-block eth_getLogs window; \
             it cannot serve the chain watchers — use another provider",
        ));
    };
    let rejected_span = (end - start).saturating_add(1);
    // A rejected regrow probe is the expected cycle against a known cap; any
    // other shrink is news about the provider.
    if rejected_probe {
        debug!(
            error = %sanitize_err_chain(&err),
            rejected_span,
            span,
            "RPC provider rejected a regrown eth_getLogs window; shrinking again"
        );
    } else {
        warn!(
            error = %sanitize_err_chain(&err),
            rejected_span,
            span,
            "RPC provider rejected the eth_getLogs block range; \
             shrinking the poll window (set blockchain.get_logs_max_block_span \
             to the lowest span logged while rejections keep coming)"
        );
    }
    report_span(poller, span);
    None
}

impl MultiplexedPoller {
    /// Fire the range-rejection hook once, as [`run_tick`] does on a
    /// recognised rejection. Lets the runtime test the hook's metric wiring
    /// without scripting a provider.
    #[cfg(test)]
    pub(crate) fn fire_range_rejection_for_test(&self) {
        fire(self.on_range_rejection.as_ref());
    }

    /// Fire the window-retry hook once, as [`run_tick`] does on each in-tick
    /// retry. Lets the runtime test the hook's metric wiring without
    /// scripting a provider.
    #[cfg(test)]
    pub(crate) fn fire_window_retry_for_test(&self) {
        fire(self.on_window_retry.as_ref());
    }

    /// Fire the window-deferred hook once, as [`run_tick`] does on each
    /// deferred window. Lets the runtime test the hook's metric wiring without
    /// scripting a provider.
    #[cfg(test)]
    pub(crate) fn fire_window_deferred_for_test(&self) {
        fire(self.on_window_deferred.as_ref());
    }
}

/// Hand the new window span to the `on_span_change` hook, if any.
fn report_span(poller: &MultiplexedPoller, span: u64) {
    if let Some(hook) = poller.on_span_change.as_ref() {
        hook(span);
    }
}

/// What [`fetch_window_logs`] got for one window.
enum WindowFetch {
    /// The window's logs.
    Logs(Vec<Log>),
    /// A transient error that survived every in-tick retry. The tick defers
    /// the window to the next tick.
    Exhausted(anyhow::Error),
    /// An error that is not retried in place: a range rejection, a permanent
    /// error or a rate limit.
    Failed(anyhow::Error),
}

/// Fetch the logs of the window `[start, end]`, retrying a transient failure
/// on the same range (see the module doc's "Transient window failures").
/// Returns `None` when `shutdown` is cancelled during a retry sleep.
///
/// Takes the poller's parts rather than `&MultiplexedPoller`: the poller is
/// not `Sync`, so a shared borrow of it cannot live across the awaits here.
async fn fetch_window_logs<P: Provider + Clone>(
    provider: &P,
    base_filter: &Filter,
    rpc_call_timeout: Option<Duration>,
    on_window_retry: Option<&WatcherHook>,
    start: u64,
    end: u64,
    shutdown: &CancellationToken,
) -> Option<WindowFetch> {
    let filter = base_filter.clone().from_block(start).to_block(end);
    let mut retries = 0;
    loop {
        let err = match timed(rpc_call_timeout, "get_logs", provider.get_logs(&filter))
            .await
            .with_context(|| format!("multiplexed get_logs [{start}, {end}]"))
        {
            Ok(logs) => return Some(WindowFetch::Logs(logs)),
            Err(err) => err,
        };
        if !is_transient_window_error(&err) {
            return Some(WindowFetch::Failed(err));
        }
        if retries == GET_LOGS_WINDOW_RETRIES {
            return Some(WindowFetch::Exhausted(err.context(format!(
                "gave up after {GET_LOGS_WINDOW_RETRIES} in-tick retries"
            ))));
        }
        retries += 1;
        info!(
            error = %sanitize_err_chain(&err),
            start,
            end,
            retry = retries,
            "eth_getLogs window failed; retrying the same window"
        );
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return None,
            () = tokio::time::sleep(GET_LOGS_WINDOW_RETRY_DELAY) => {}
        }
        // Counted after the sleep, so a retry that shutdown cancels is not.
        fire(on_window_retry);
    }
}

/// Run one poll tick: read head, resolve each route's floor, scan the merged
/// `[min(floor), head]` range in windows (one `get_logs` per window), demux
/// each log to its owning route, then reconcile and fire hooks. A recognised
/// provider range rejection retries the window at half its length instead of
/// failing (see the module doc's "Provider range caps"), and a transient
/// `get_logs` failure retries the same window (see "Transient window
/// failures"). A window that fails every retry ends the tick early (see
/// "Deferred windows"). Returns `Err` if the shared head/floor reads fail, if a
/// `get_logs` window fails in a way that is not retried, if a window is
/// deferred after no tick has reached head for [`GET_LOGS_STALL_BUDGET`], or
/// if any route errored
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
    let mut start = from;
    let mut progressed = false;
    let mut deferred: Option<anyhow::Error> = None;
    while start <= to {
        // Yield between windows so a large merged backfill does not block
        // graceful shutdown (mirrors the per-window shutdown check).
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let end = window_end(start, to, poller.span.current());
        let fetched = fetch_window_logs(
            provider,
            &poller.base_filter,
            poller.rpc_call_timeout,
            poller.on_window_retry.as_ref(),
            start,
            end,
            shutdown,
        )
        .await;
        let Some(fetched) = fetched else {
            // Shutdown ends the tick as the per-window check above does.
            return Ok(());
        };
        let logs = match fetched {
            WindowFetch::Logs(logs) => logs,
            WindowFetch::Exhausted(err) => {
                fire(poller.on_window_deferred.as_ref());
                info!(
                    error = %sanitize_err_chain(&err),
                    start,
                    end,
                    "eth_getLogs window failed on every retry; deferring it to the next tick"
                );
                deferred = Some(err);
                break;
            }
            WindowFetch::Failed(err) => match shrink_on_range_rejection(poller, start, end, err) {
                // Retry the same start block with the smaller window.
                None => continue,
                Some(err) => return Err(fail_whole_tick(poller, shutdown, err)),
            },
        };
        demux_window_logs(poller, logs).await;
        advance_routes(poller, end);
        progressed = true;
        if let Some(span) = poller.span.record_success() {
            debug!(
                span,
                "eth_getLogs windows accepted; growing the poll window"
            );
            report_span(poller, span);
        }
        if end >= to {
            break;
        }
        start = end + 1;
    }

    // A deferred tick did not reach head. The stall clock runs from the first
    // such tick and clears only when a tick reaches head, so sparse progress
    // cannot hide a lag that keeps growing. Past the budget the tick fails,
    // with the completed windows already persisted.
    if let Some(err) = deferred {
        let since = *poller
            .stalled_since
            .get_or_insert_with(tokio::time::Instant::now);
        if since.elapsed() >= GET_LOGS_STALL_BUDGET {
            let err = err.context(format!(
                "no poll tick reached head for {} s",
                GET_LOGS_STALL_BUDGET.as_secs()
            ));
            return Err(fail_whole_tick(poller, shutdown, err));
        }
        // A deferred tick that completed no window fires no hook: nothing moved,
        // so no tick gauge or freshness stamp is earned.
        if !progressed {
            return Ok(());
        }
    } else {
        poller.stalled_since = None;
    }

    notify_recovered_routes(poller, shutdown);
    reconcile_routes(poller).await;
    if fire_route_hooks(poller, shutdown) {
        anyhow::bail!("one or more routes errored this tick"); // drives the loop's backoff sleep
    }
    Ok(())
}

/// Fires every route's `on_task_panic` and logs at `error!` iff the enclosing
/// [`run`] is unwinding on a panic (mirrors
/// `resumable_watcher::PanicGuard`, #1316). The poller task is held
/// only by an [`AbortOnDropHandle`] that nothing awaits, so a panic is otherwise
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
/// until `shutdown` is cancelled. Uses `tokio::time::interval` with
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

    report_span(&poller, poller.span.current());
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
                    error = %sanitize_err_chain(&err),
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
/// [`WatcherHandle`], the only pairing of a live task with its shutdown token
/// (#1236).
pub fn spawn<P>(provider: P, mut poller: MultiplexedPoller) -> WatcherHandle
where
    P: Provider + Clone + 'static,
{
    let shutdown = CancellationToken::new();
    // Resolve each route's sink against the freshly-minted token now that it
    // exists: a `Factory` (blacklist) is built here so its sink observes the
    // very token this handle cancels; a `Ready` sink passes through.
    let routes = std::mem::take(&mut poller.routes);
    poller.routes = routes
        .into_iter()
        .map(|rs| RouteState {
            sink: rs.sink.resolved(&shutdown),
            ..rs
        })
        .collect();
    let task = AbortOnDropHandle::new(tokio::spawn(run(provider, poller, shutdown.clone())));
    WatcherHandle::new(shutdown, task)
}

#[cfg(test)]
mod tests;
