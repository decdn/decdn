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
//! sleeps the backoff — it never fires a hook itself. The one exception is a
//! failure in the shared, pre-route-loop work (the head read, or a route's
//! checkpoint-load floor derivation, or the merged `get_logs` call): those fail
//! the *whole* tick before any route-specific step runs, so [`fail_whole_tick`]
//! fires `on_backoff` for every route directly at the failure site — every route
//! is equally down, just as five independent watchers all convoyed into backoff
//! together when every one read the shared head through
//! [`super::shared_head::SharedHead`].
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
//! regrows after a streak of accepted windows ([`WindowSpan`]), so a transient
//! rejection does not cost throughput until restart. Only a rejected one-block
//! window, or any other `get_logs` failure, goes through [`fail_whole_tick`].
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
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::transports::TransportError;
use anyhow::{Context, Result};
use async_trait::async_trait;
use decdn_common::config::DEFAULT_GET_LOGS_MAX_BLOCK_SPAN;
use decdn_common::redact::sanitize_err_chain;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use super::resumable_watcher::{CursorStart, LogSink, WatcherHandle, WatcherHook, fire};
use super::shared_head::HeadSource;
use super::{AbortOnDrop, WATCHER_INITIAL_BACKOFF, WATCHER_MAX_BACKOFF};
use super::{WindowSpan, timed, window_end};

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
    /// regrows after a streak of accepted windows (see [`WindowSpan`]).
    span: WindowSpan,
    /// Called with the new span when it changes, and once with the starting
    /// span when [`run`] starts (the `decdn_chain_get_logs_span` gauge).
    on_span_change: Option<SpanHook>,
    /// Called on every range rejection the poller recognises, including a
    /// rejected one-block window.
    on_range_rejection: Option<WatcherHook>,
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

impl std::fmt::Debug for MultiplexedPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiplexedPoller")
            .field("routes", &self.routes.len())
            .field("poll_interval", &self.poll_interval)
            .finish_non_exhaustive()
    }
}

/// Receives the poller's `eth_getLogs` window span each time it changes.
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

    /// Build the poller: construct the merged filter and the `(address,
    /// topic0) -> route` demux index. Returns `Err` if two routes claim the
    /// same `(address, topic0)` key — two watchers subscribing to the same
    /// event is a wiring bug, not a runtime condition, so it fails fast at
    /// startup rather than silently routing every such log to whichever route
    /// happened to register first.
    pub fn build(self) -> Result<MultiplexedPoller> {
        anyhow::ensure!(
            self.max_backfill_span > 0,
            "multiplexed poller max_backfill_span must be at least 1 block"
        );
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
            span: WindowSpan::new(self.max_backfill_span),
            on_span_change: self.on_span_change,
            on_range_rejection: self.on_range_rejection,
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
        // enumerated at head, or is a sibling still ahead) ignores logs below
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
/// *and* a limit ([`LIMIT_WORDS`]). Both parts are required because the shrink
/// is sticky: a load-balanced provider whose backend lags head reports a
/// `toBlock` past that backend's head with range wording but no limit ("block
/// range extends beyond current head block", "block number is out of range"),
/// and a smaller window is not the fix for that. Any other error — a timeout, a
/// transport failure, a rate limit, an unknown block — takes the backoff path
/// and leaves the window span alone, so a transient fault cannot shrink it.
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
    let Some((span, new_low)) = poller.span.shrink(start, end) else {
        return Some(err.context(
            "the RPC provider rejects even a one-block eth_getLogs window; \
             it cannot serve the chain watchers — use another provider",
        ));
    };
    let rejected_span = (end - start).saturating_add(1);
    // A new low is news; returning to a span already seen is the regrow
    // probing a known cap again.
    if new_low {
        warn!(
            error = %sanitize_err_chain(&err),
            rejected_span,
            span,
            "RPC provider rejected the eth_getLogs block range; \
             shrinking the poll window (set blockchain.get_logs_max_block_span \
             to the logged span to skip this after a restart)"
        );
    } else {
        debug!(
            error = %sanitize_err_chain(&err),
            rejected_span,
            span,
            "RPC provider rejected a regrown eth_getLogs window; shrinking again"
        );
    }
    report_span(poller, span);
    None
}

/// Hand the new window span to the `on_span_change` hook, if any.
fn report_span(poller: &MultiplexedPoller, span: u64) {
    if let Some(hook) = poller.on_span_change.as_ref() {
        hook(span);
    }
}

/// Run one poll tick: read head, resolve each route's floor, scan the merged
/// `[min(floor), head]` range in windows (one `get_logs` per window), demux
/// each log to its owning route, then reconcile and fire hooks. A recognised
/// provider range rejection retries the window at half its length instead of
/// failing (see the module doc's "Provider range caps"). Returns `Err`
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
    let mut start = from;
    while start <= to {
        // Yield between windows so a large merged backfill does not block
        // graceful shutdown (mirrors the per-window shutdown check).
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let end = window_end(start, to, poller.span.current());
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
            Err(err) => match shrink_on_range_rejection(poller, start, end, err) {
                // Retry the same start block with the smaller window.
                None => continue,
                Some(err) => return Err(fail_whole_tick(poller, shutdown, err)),
            },
        };
        demux_window_logs(poller, logs).await;
        advance_routes(poller, end);
        if let Some(span) = poller.span.record_success(start, end) {
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

    notify_recovered_routes(poller, shutdown);
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
        /// One entry per `on_recovered`, holding `tick_completes` as it stood at
        /// that moment — so a test can pin the ordering against the reconcile,
        /// not just the count.
        recovered_at: Vec<usize>,
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

        fn on_recovered(&mut self) {
            self.recovered_at.push(self.tick_completes);
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

        fn on_recovered(&mut self) {
            let mut guard = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let seen = guard.tick_completes;
            guard.recovered_at.push(seen);
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
            sink: SinkSource::Ready(Box::new(MutexSink(Arc::clone(&recorded)))),
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
        drop(guard);

        erased.on_recovered();
        let guard = recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            guard.recovered_at.len(),
            1,
            "on_recovered must forward through the blanket impl"
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
        let (route_b, sink_b) = seeded_route("b", ADDR_B, TOPIC_B, CursorStart::Seeded { at: 0 });
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

    /// A shared-read failure arms the recovery edge for every route.
    ///
    /// `fail_whole_tick` returns before `notify_recovered_routes` and
    /// `fire_route_hooks` ever run, so it is the *only* place the edge is armed
    /// for a whole-RPC outage — the most common real one, and the one a
    /// cadence-gated sink most needs the forced re-read after. Nothing else
    /// covers this leg: a per-route apply failure takes an entirely different
    /// path through `fire_route_hooks`.
    #[tokio::test]
    async fn a_whole_tick_failure_arms_the_recovery_edge_for_every_route() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let (route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
        let (route_b, sink_b) = head_route("b", ADDR_B, TOPIC_B);
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .route(route_a)
                .route(route_b)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        // Tick 1: the shared head read fails, so no route errored individually.
        asserter.push_failure_msg("head is down");
        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_err(), "a head-read failure must fail the tick");

        // Tick 2: the RPC is back.
        asserter.push_success(&U64::from(1));
        asserter.push_success(&Vec::<Log>::new());
        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_ok(), "recovery tick should complete: {result:?}");

        for (label, sink) in [("a", &sink_a), ("b", &sink_b)] {
            let guard = sink
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(
                guard.recovered_at.len(),
                1,
                "route {label} must be told it recovered from a whole-tick failure, \
                 or a cadence-gated sink waits out its full interval after the \
                 outage that made it stale"
            );
            assert_eq!(
                guard.recovered_at.first().copied(),
                Some(0),
                "route {label}'s recovery must land before that tick's reconcile"
            );
        }
    }

    /// A route that recovers must be told, on the recovery tick and before that
    /// tick's reconcile.
    ///
    /// A sink whose authoritative re-read is cadence-gated repairs itself here.
    /// The cadence alone is at its weakest in exactly this scenario: the
    /// reconcile is skipped while the route is errored, so the repair does not
    /// run during the outage at all, and a repair whose own read then fails
    /// defers itself a further interval. A route that never errored must not be
    /// told it recovered — its bootstrap read just ran.
    #[tokio::test]
    async fn a_recovered_route_is_notified_before_its_reconcile() {
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

        // Tick 1: A's apply errors, B is clean.
        asserter.push_success(&U64::from(1));
        asserter.push_success(&vec![
            log_at(ADDR_A, TOPIC_A, 1),
            log_at(ADDR_B, TOPIC_B, 1),
        ]);
        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_err(), "a route error must fail the tick");
        assert!(
            sink_a
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .recovered_at
                .is_empty(),
            "an errored route has not recovered yet"
        );

        // Tick 2: A comes back.
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

        {
            let guard_a = sink_a
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(
                guard_a.recovered_at.len(),
                1,
                "the recovery edge fires exactly once"
            );
            // A's reconcile is skipped on the errored tick, so it has run once —
            // this tick's — by the end. The recovery must have been seen before it.
            assert_eq!(guard_a.tick_completes, 1);
            assert_eq!(
                guard_a.recovered_at.first().copied(),
                Some(0),
                "on_recovered must run before this tick's reconcile, so the sink can \
                 force it to re-read now rather than a cadence later"
            );
        }

        assert!(
            sink_b
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .recovered_at
                .is_empty(),
            "a route that never errored must not be told it recovered"
        );

        // Tick 3: A stays healthy, so the edge does not re-fire.
        asserter.push_success(&U64::from(1));
        asserter.push_success(&Vec::<Log>::new());
        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_ok(), "third tick should complete: {result:?}");
        assert_eq!(
            sink_a
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .recovered_at
                .len(),
            1,
            "the edge is consumed, not re-fired on every healthy tick"
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
                    // Strictly increasing head: every real RPC (i.e. every
                    // TTL window, not every tick) advances the chain by one
                    // block, so — unlike a static head, which idles after
                    // tick 1 and would pass even an unmerged N-loop
                    // regression — every poller tick has a non-empty merged
                    // range and must issue a `get_logs`.
                    let n = self.head_hits.fetch_add(1, Ordering::SeqCst);
                    serde_json::json!(format!("0x{:x}", 100 + n))
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
        let heads = head_hits.load(Ordering::SeqCst);
        let per_ms = |d: Duration| d.as_millis().max(1);
        let ticks = (per_ms(RUN_FOR) / per_ms(INTERVAL)) as usize + 2;
        // The head advances by one block on every real `eth_blockNumber` RPC
        // (see `CountingRpc`), so — unlike a static head, under which every
        // tick after the first is idle and issues no `get_logs` at all, and
        // the old `< ROUTES * ticks` bound would pass even an unmerged
        // 4-loop regression — every tick here has a non-empty merged range
        // and must issue exactly one `get_logs`. A regression back to one
        // loop per route would issue up to `ROUTES` times as many; bounding
        // close to the tick count (rather than the much looser `ROUTES *
        // ticks`) is what actually discriminates merged from unmerged.
        assert!(
            getlogs <= ticks + 2,
            "merged polling must track ~1 get_logs per tick, not ROUTES per tick \
             (getlogs={getlogs}, ticks~={ticks}, heads={heads}, unmerged would approach {})",
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
        let (route_a, sink_a) = seeded_route("a", ADDR_A, TOPIC_A, CursorStart::Seeded { at: 100 });
        let (route_b, sink_b) = seeded_route("b", ADDR_B, TOPIC_B, CursorStart::Seeded { at: 100 });
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
                sink: SinkSource::Ready(Box::new(PanicOnApply)),
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

    // --- Coverage: shutdown suppresses the established/backoff edges, not liveness ---
    //
    // Each pair below mirrors `resumable_watcher`'s
    // `shutdown_suppresses_the_established_edge` /
    // `shutdown_suppresses_the_backoff_edge`: a positive case that proves the
    // hook *can* fire, and a negative case that isolates the
    // `!shutdown.is_cancelled()` guard as the ONLY thing standing between an
    // otherwise-identical tick and that hook firing. A test that cancels
    // shutdown only on a SECOND tick (after the route is already established)
    // is vacuous for the established edge — `!r.established` is already false
    // by then, so the guard is never reached — and a test whose tick
    // succeeds is vacuous for the backoff edge, since that hook only fires
    // from the `errored` branch. Both negative cases below avoid that: the
    // established case cancels before the route's very first tick (so
    // `!r.established` is still true and only the shutdown guard withholds
    // the fire), and the backoff case drives a genuinely failing tick (so
    // `r.errored` is true and only the shutdown guard withholds the fire).

    /// Positive case: an uncancelled first tick fires `on_established` once,
    /// alongside the per-tick `on_tick_success` liveness stamp.
    #[tokio::test]
    async fn established_edge_fires_on_first_healthy_tick_when_not_cancelled() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let established = Arc::new(AtomicUsize::new(0));
        let tick_success = Arc::new(AtomicUsize::new(0));
        let (mut route_a, _) = head_route("a", ADDR_A, TOPIC_A);
        {
            let counter = Arc::clone(&established);
            route_a.on_established = Some(Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        }
        {
            let counter = Arc::clone(&tick_success);
            route_a.on_tick_success = Some(Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        }

        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(10)
                .route(route_a)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        asserter.push_success(&U64::from(1));
        asserter.push_success(&Vec::<Log>::new());
        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_ok(), "tick must succeed: {result:?}");
        assert_eq!(
            established.load(Ordering::SeqCst),
            1,
            "on_established must fire once on an uncancelled first healthy tick"
        );
        assert_eq!(
            tick_success.load(Ordering::SeqCst),
            1,
            "on_tick_success must fire on the same tick"
        );
    }

    /// Cancels a shared shutdown token from inside `on_tick_complete` —
    /// i.e. strictly *after* the per-window boundary check (`run_tick`'s
    /// `if shutdown.is_cancelled() { return Ok(()); }`, which only runs
    /// between windows) but *before* `fire_route_hooks` reads the token.
    /// Optionally fails that same call, so the cancellation and the route's
    /// `errored` transition land in the same tick. Mirrors
    /// `resumable_watcher.rs`'s `CancelOnNthTick` — cancelling *before*
    /// calling `run_tick` at all would instead trip the window-boundary
    /// check and return early, short-circuiting the tick before it ever
    /// reaches reconcile or hook-firing (proven the hard way: an earlier
    /// draft of these tests cancelled up front and both went vacuous the
    /// other way, asserting on a tick that never ran far enough to prove
    /// anything).
    struct CancelInReconcile {
        shutdown: CancellationToken,
        bail: bool,
    }

    impl LogSink for CancelInReconcile {
        async fn apply(&mut self, _log: Log) -> Result<()> {
            Ok(())
        }
        async fn on_tick_complete(&mut self) -> Result<()> {
            self.shutdown.cancel();
            if self.bail {
                anyhow::bail!("cancelled mid-tick reconcile");
            }
            Ok(())
        }
    }

    /// Negative case: shutdown becomes cancelled *during* the route's very
    /// first tick (from its `on_tick_complete`, run strictly before
    /// `fire_route_hooks`), so `!r.established` is still `true` and the tick
    /// itself succeeds — the ONLY thing that can withhold `on_established`
    /// is the `!shutdown.is_cancelled()` guard. If that guard were deleted
    /// this assertion would fail (the edge would fire), which is what makes
    /// this non-vacuous, unlike a cancel-on-the-second-tick construction
    /// (where the route is already established and the guard is never
    /// reached) or a cancel-before-calling-`run_tick` construction (where
    /// the window-boundary check returns early and no hook fires at all —
    /// see `CancelInReconcile`'s doc). `on_tick_success` still fires — the
    /// tick itself succeeds; only the edge-triggered readiness signal is
    /// suppressed (fail-open guard).
    #[tokio::test]
    async fn shutdown_during_first_tick_suppresses_established_edge() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let established = Arc::new(AtomicUsize::new(0));
        let tick_success = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new(); // NOT cancelled yet
        let route_a = {
            let counter = Arc::clone(&established);
            let tick_counter = Arc::clone(&tick_success);
            Route {
                addresses: vec![ADDR_A],
                topic0s: vec![TOPIC_A],
                start: CursorStart::HeadMinusWindow { window_blocks: 0 },
                sink: SinkSource::Ready(Box::new(CancelInReconcile {
                    shutdown: shutdown.clone(),
                    bail: false,
                })),
                label: "a",
                on_established: Some(Box::new(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                })),
                on_backoff: None,
                on_tick_success: Some(Box::new(move || {
                    tick_counter.fetch_add(1, Ordering::SeqCst);
                })),
                on_task_panic: None,
            }
        };

        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(10)
                .route(route_a)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        asserter.push_success(&U64::from(1));
        asserter.push_success(&Vec::<Log>::new());
        let result = run_tick(&provider, &mut poller, &shutdown).await;
        assert!(result.is_ok(), "the tick itself still succeeds: {result:?}");
        assert_eq!(
            established.load(Ordering::SeqCst),
            0,
            "on_established must be suppressed once shutdown is cancelled \
             mid-tick, even though the route was never established before"
        );
        assert_eq!(
            tick_success.load(Ordering::SeqCst),
            1,
            "on_tick_success still fires — liveness is unconditional"
        );
    }

    /// Positive case: an uncancelled tick whose sink genuinely errors fires
    /// `on_backoff`.
    #[tokio::test]
    async fn backoff_edge_fires_on_a_failing_tick_when_not_cancelled() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let backoff = Arc::new(AtomicUsize::new(0));
        let (mut route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
        sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fail_apply_on = Some(0);
        {
            let counter = Arc::clone(&backoff);
            route_a.on_backoff = Some(Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        }

        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(10)
                .route(route_a)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        asserter.push_success(&U64::from(1));
        asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 1)]);
        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_err(), "the route's sink error must fail the tick");
        assert_eq!(
            backoff.load(Ordering::SeqCst),
            1,
            "on_backoff must fire once on an uncancelled failing tick"
        );
    }

    /// Negative case: shutdown becomes cancelled *during* the same tick that
    /// makes the route error (`CancelInReconcile { bail: true }` cancels the
    /// token and fails `on_tick_complete` in one call), so `r.errored` is
    /// genuinely `true` and the backoff branch IS entered — the ONLY thing
    /// that can withhold `on_backoff` is the `!shutdown.is_cancelled()`
    /// guard. If that guard were deleted this assertion would fail (the edge
    /// would fire), unlike a construction whose tick never actually errors
    /// (where the backoff branch is never reached regardless of the guard)
    /// or one that cancels before calling `run_tick` (which trips the
    /// window-boundary check and returns early before the sink — and so the
    /// error — is ever reached; see `CancelInReconcile`'s doc).
    #[tokio::test]
    async fn shutdown_during_failing_tick_suppresses_backoff_edge() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let backoff = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new(); // NOT cancelled yet
        let route_a = {
            let counter = Arc::clone(&backoff);
            Route {
                addresses: vec![ADDR_A],
                topic0s: vec![TOPIC_A],
                start: CursorStart::HeadMinusWindow { window_blocks: 0 },
                sink: SinkSource::Ready(Box::new(CancelInReconcile {
                    shutdown: shutdown.clone(),
                    bail: true,
                })),
                label: "a",
                on_established: None,
                on_backoff: Some(Box::new(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                })),
                on_tick_success: None,
                on_task_panic: None,
            }
        };

        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(10)
                .route(route_a)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        asserter.push_success(&U64::from(1));
        asserter.push_success(&Vec::<Log>::new());
        let result = run_tick(&provider, &mut poller, &shutdown).await;
        assert!(
            result.is_err(),
            "the route still errors this tick — shutdown suppresses only the \
             hook, not the tick's Err outcome: {result:?}"
        );
        assert_eq!(
            backoff.load(Ordering::SeqCst),
            0,
            "on_backoff must be suppressed once shutdown is cancelled mid-tick, \
             even though the route genuinely errored"
        );
    }

    // --- Coverage: a None block_number applies rather than being gated ---------

    /// The floor gate is `log.block_number.is_some_and(|b| b < tick_floor)`:
    /// a `None` block makes `is_some_and` false, so the log proceeds to
    /// `apply` rather than being silently dropped. `log_at` always sets
    /// `Some`, so this builds the log directly to exercise the `None` arm.
    #[tokio::test]
    async fn none_block_number_log_is_applied_not_gated() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let (route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(10)
                .route(route_a)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        let mut log = log_at(ADDR_A, TOPIC_A, 1);
        log.block_number = None; // e.g. a pending-tag response shape

        asserter.push_success(&U64::from(1));
        asserter.push_success(&vec![log]);

        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_ok(), "tick must succeed: {result:?}");

        let a = sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            a.applied,
            vec![(ADDR_A, TOPIC_A, None)],
            "a log with no block_number must be applied, not silently dropped by the floor gate"
        );
    }

    // --- Coverage: a later-window error holds the cursor at that window's start ---

    /// Minimal in-memory durable store for this test — mirrors
    /// `resumable_watcher.rs`'s test-only `MemoryCheckpointStore` (private to
    /// that module, so duplicated here rather than reused).
    #[derive(Default)]
    struct MemoryCheckpointStore {
        stored: std::sync::Mutex<std::collections::HashMap<CheckpointKey, u64>>,
    }

    impl KeyedCheckpointStore for MemoryCheckpointStore {
        fn load_checkpoint(
            &self,
            key: CheckpointKey,
        ) -> std::result::Result<Option<u64>, StoreError> {
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
        ) -> std::result::Result<(), StoreError> {
            self.stored
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key, block);
            Ok(())
        }
    }

    /// A route whose floor sits several windows behind head must, on a
    /// mid-backfill sink error, hold its cursor at the *failed* window's
    /// start — not the original floor, and not an un-scanned later window —
    /// while the windows that already completed stay advanced and persisted.
    /// Mirrors `resumable_watcher.rs`'s
    /// `sink_error_leaves_cursor_and_checkpoint_at_last_completed_window`,
    /// carried over to the per-route `tick_floor`/`errored` machinery.
    #[tokio::test]
    async fn multi_window_error_holds_cursor_at_failed_windows_start() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let store = Arc::new(MemoryCheckpointStore::default());
        // Pre-recorded cursor 1 with a zero reorg margin: the first tick's floor
        // resolves to block 1, giving the three-window script below. The memory
        // store's record is infallible; assert rather than expect (anti-panic lint).
        assert!(
            store
                .record_checkpoint(CheckpointKey::PoolOpened, 1)
                .is_ok()
        );

        let (route_a, sink_a) = seeded_route(
            "a",
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
        // Fail on the 2nd apply (0-indexed): window [1,1] succeeds, window
        // [2,2] fails, window [3,3] is never applied (route already errored).
        sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fail_apply_on = Some(1);

        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(1)
                .route(route_a)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        // floor=1, head=3, span=1 -> three windows: [1,1], [2,2], [3,3]. All
        // three windows are scanned in this one tick (the window set is fixed
        // from the tick's start floor before any route errors), so all three
        // `get_logs` responses are queued regardless of the mid-tick error.
        asserter.push_success(&U64::from(3));
        asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 1)]);
        asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 2)]);
        asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 3)]);

        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_err(), "the failed window must fail the tick");

        assert_eq!(
            poller.routes.first().and_then(|r| r.cursor),
            Some(2),
            "cursor holds at the failed window's start (2): past the completed \
             window [1,1] but not into the un-scanned window [3,3]"
        );
        let persisted = store
            .load_checkpoint(CheckpointKey::PoolOpened)
            .ok()
            .flatten();
        assert_eq!(
            persisted,
            Some(1),
            "only the completed window [1,1] is durably persisted"
        );
        let applied = sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .applied
            .len();
        assert_eq!(
            applied, 1,
            "only window [1,1]'s log applied; the failed and un-scanned windows did not"
        );
    }

    // --- Provider range caps: the window shrinks on a range rejection ------------

    fn error_payload(code: i64, message: &str) -> Option<alloy_json_rpc::ErrorPayload> {
        serde_json::from_value(serde_json::json!({ "code": code, "message": message })).ok()
    }

    fn rpc_error(code: i64, message: &str) -> anyhow::Error {
        error_payload(code, message).map_or_else(
            || anyhow::anyhow!("unbuildable error payload"),
            |payload| {
                anyhow::Error::new(TransportError::ErrorResp(payload))
                    .context("multiplexed get_logs [1, 20]")
            },
        )
    }

    #[test]
    fn range_rejection_matches_provider_range_and_result_caps() {
        // dRPC's free tier rejects ranges above ~100-178 blocks, whatever the text says.
        assert!(is_range_rejection(&rpc_error(
            35,
            "ranges over 10000 blocks are not supported on free plan"
        )));
        // Alchemy free tier.
        assert!(is_range_rejection(&rpc_error(
            -32600,
            "Under the Free tier plan, you can make eth_getLogs requests with up to a 10 \
             block range."
        )));
        // Infura result cap: -32005 is also its rate-limit code.
        assert!(is_range_rejection(&rpc_error(
            -32005,
            "query returned more than 10000 results"
        )));
        // Ankr and Chainstack.
        assert!(is_range_rejection(&rpc_error(
            -32600,
            "block range is too wide"
        )));
        assert!(is_range_rejection(&rpc_error(
            -32000,
            "Block range limit exceeded"
        )));
        // geth / Erigon result cap.
        assert!(is_range_rejection(&rpc_error(
            -32000,
            "query exceeds max results 20000"
        )));
        // QuickNode.
        assert!(is_range_rejection(&rpc_error(
            -32614,
            "eth_getLogs is limited to a 10,000 range"
        )));
    }

    #[test]
    fn range_rejection_ignores_every_other_failure() {
        // Rate limits back off; they never shrink the window.
        assert!(!is_range_rejection(&rpc_error(429, "Too Many Requests")));
        assert!(!is_range_rejection(&rpc_error(
            -32005,
            "project ID request rate exceeded"
        )));
        // A lagging load-balanced backend: a smaller window is not the fix, and
        // the shrink is sticky.
        assert!(!is_range_rejection(&rpc_error(
            -32000,
            "block number is out of range"
        )));
        assert!(!is_range_rejection(&rpc_error(-32000, "unknown block")));
        assert!(!is_range_rejection(&rpc_error(-32603, "internal error")));
        // Head-lag range wording without a limit word.
        assert!(!is_range_rejection(&rpc_error(
            -32000,
            "block range extends beyond current head block"
        )));
        assert!(!is_range_rejection(&rpc_error(
            -32602,
            "invalid block range params"
        )));
        // "results" alone is not a result limit.
        assert!(!is_range_rejection(&rpc_error(
            -32000,
            "failed to marshal results"
        )));
        // Not a JSON-RPC error response at all.
        assert!(!is_range_rejection(&anyhow::Error::new(
            alloy::transports::TransportErrorKind::custom_str("connection reset")
        )));
        assert!(!is_range_rejection(&anyhow::anyhow!(
            "get_logs timed out after 10s"
        )));
    }

    /// A route resuming from a checkpoint at `from`, so the first tick backfills
    /// `[from, head]`.
    fn checkpoint_route(
        from: u64,
    ) -> (
        Route,
        Arc<std::sync::Mutex<ScriptedSink>>,
        Arc<MemoryCheckpointStore>,
    ) {
        let store = Arc::new(MemoryCheckpointStore::default());
        assert!(
            store
                .record_checkpoint(CheckpointKey::PoolOpened, from)
                .is_ok()
        );
        let (route, sink) = seeded_route(
            "a",
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
        (route, sink, store)
    }

    #[tokio::test]
    async fn a_range_rejection_halves_the_window_and_the_tick_completes() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let (route, _sink, store) = checkpoint_route(1);
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .route(route)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        // head=20, default span: [1, 20] is rejected, then [1, 10] and [11, 20].
        asserter.push_success(&U64::from(20));
        if let Some(payload) = error_payload(
            -32600,
            "you can make eth_getLogs requests with up to a 10 block range",
        ) {
            asserter.push_failure(payload);
        }
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_success(&Vec::<Log>::new());

        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(
            result.is_ok(),
            "the halved windows must complete the tick: {result:?}"
        );
        assert_eq!(
            poller.span.current(),
            10,
            "the span is halved from the 20-block window"
        );
        assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(21));
        assert_eq!(
            store
                .load_checkpoint(CheckpointKey::PoolOpened)
                .ok()
                .flatten(),
            Some(20)
        );
    }

    /// A result cap that trips on a later, dense window after earlier windows
    /// already applied and persisted: the retry resumes at the rejected
    /// window's start, so no log is skipped or applied twice.
    #[tokio::test]
    async fn a_mid_backfill_rejection_retries_from_the_rejected_window() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let (route, sink, store) = checkpoint_route(1);
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(10)
                .route(route)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        // head=30, span 10: [1,10] ok, [11,20] rejected, then span 5:
        // [11,15], [16,20], [21,25], [26,30].
        asserter.push_success(&U64::from(30));
        asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 5)]);
        if let Some(payload) = error_payload(-32005, "query returned more than 10000 results") {
            asserter.push_failure(payload);
        }
        asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 12)]);
        asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 18)]);
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_success(&Vec::<Log>::new());

        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(
            result.is_ok(),
            "the retried windows must complete the tick: {result:?}"
        );
        assert_eq!(poller.span.current(), 5);
        let applied: Vec<Option<u64>> = sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .applied
            .iter()
            .map(|(_, _, block)| *block)
            .collect();
        assert_eq!(
            applied,
            vec![Some(5), Some(12), Some(18)],
            "each log applied exactly once, in order"
        );
        assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(31));
        assert_eq!(
            store
                .load_checkpoint(CheckpointKey::PoolOpened)
                .ok()
                .flatten(),
            Some(30)
        );
    }

    /// The span hook sees every shrink and regrow and the rejection hook counts
    /// every range rejection: they drive `decdn_chain_get_logs_span` and
    /// `decdn_chain_get_logs_range_rejections_total`.
    #[tokio::test]
    async fn span_hooks_report_the_shrink_and_the_regrow() {
        use crate::chain_events::backfill::SPAN_REGROW_AFTER_WINDOWS;
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let (route, _sink, _store) = checkpoint_route(1);
        let spans = Arc::new(std::sync::Mutex::new(Vec::new()));
        let rejections = Arc::new(AtomicUsize::new(0));
        let spans_hook = Arc::clone(&spans);
        let rejections_hook = Arc::clone(&rejections);
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(20)
                .on_span_change(Box::new(move |span| {
                    spans_hook
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(span);
                }))
                .on_range_rejection(Box::new(move || {
                    rejections_hook.fetch_add(1, Ordering::SeqCst);
                }))
                .route(route)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        // [1, 20] is rejected (span 10), then 32 full 10-block windows regrow it
        // to the 20-block ceiling on the last one.
        let windows = u64::from(SPAN_REGROW_AFTER_WINDOWS);
        asserter.push_success(&U64::from(windows * 10));
        if let Some(payload) = error_payload(35, "ranges over 10000 blocks are not supported") {
            asserter.push_failure(payload);
        }
        for _ in 0..windows {
            asserter.push_success(&Vec::<Log>::new());
        }

        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(result.is_ok(), "the tick must complete: {result:?}");
        assert_eq!(rejections.load(Ordering::SeqCst), 1);
        assert_eq!(
            spans
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            vec![10, 20],
            "one shrink to 10, then one regrow back to the ceiling"
        );
        assert_eq!(poller.span.current(), 20);
    }

    #[tokio::test]
    async fn a_rate_limit_backs_off_and_keeps_the_window() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let (route, _sink, _store) = checkpoint_route(1);
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .route(route)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        asserter.push_success(&U64::from(20));
        if let Some(payload) = error_payload(429, "Too Many Requests") {
            asserter.push_failure(payload);
        }

        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(
            result.is_err(),
            "a rate limit fails the tick into the backoff path"
        );
        assert_eq!(poller.span.current(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN);
        assert_eq!(
            poller.routes.first().and_then(|r| r.cursor),
            Some(1),
            "cursor held"
        );
    }

    #[tokio::test]
    async fn a_rejected_one_block_window_fails_the_tick_instead_of_looping() {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let (route, _sink, _store) = checkpoint_route(1);
        let built =
            MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
                .max_backfill_span(1)
                .route(route)
                .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        asserter.push_success(&U64::from(1));
        if let Some(payload) = error_payload(35, "ranges over 10000 blocks are not supported") {
            asserter.push_failure(payload);
        }

        let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(
            result.is_err(),
            "a one-block window cannot shrink: back off"
        );
        assert_eq!(poller.span.current(), 1);
    }

    #[test]
    fn a_zero_span_is_a_build_error() {
        let head: Arc<dyn HeadSource> = Arc::new(StaticHead(0));
        let built = MultiplexedPollerBuilder::new(head, Duration::from_secs(1))
            .max_backfill_span(0)
            .build();
        assert!(built.is_err(), "a zero span would scan no blocks");
    }

    /// A head the test moves between ticks.
    struct MovingHead(Arc<std::sync::atomic::AtomicU64>);

    #[async_trait]
    impl HeadSource for MovingHead {
        async fn head(&self) -> Result<u64> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    /// A JSON-RPC mock with a provider-side `eth_getLogs` range cap. It answers
    /// dRPC's free-tier error for any window wider than `cap` blocks and records
    /// every requested window as `(from, to, accepted)`.
    struct CappedGetLogsRpc {
        cap: u64,
        windows: Arc<std::sync::Mutex<Vec<(u64, u64, bool)>>>,
    }

    fn hex_block(value: Option<&serde_json::Value>) -> Option<u64> {
        let text = value?.as_str()?.strip_prefix("0x")?;
        u64::from_str_radix(text, 16).ok()
    }

    impl wiremock::Respond for CappedGetLogsRpc {
        fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null);
            let id = body.get("id").cloned().unwrap_or(serde_json::json!(0));
            let filter = body.get("params").and_then(|p| p.get(0));
            let from = hex_block(filter.and_then(|f| f.get("fromBlock")));
            let to = hex_block(filter.and_then(|f| f.get("toBlock")));
            let (Some(from), Some(to)) = (from, to) else {
                return wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": { "code": -32602, "message": "missing block bounds" },
                }));
            };
            let accepted = to - from < self.cap;
            self.windows
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((from, to, accepted));
            let payload = if accepted {
                serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": [] })
            } else {
                serde_json::json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {
                        "code": 35,
                        "message": "ranges over 10000 blocks are not supported on free plan",
                    },
                })
            };
            wiremock::ResponseTemplate::new(200).set_body_json(payload)
        }
    }

    /// A provider capping `eth_getLogs` at 150 blocks and a 1 651-block gap
    /// behind head. The poller must shrink its window over
    /// the real HTTP error path, finish the backfill in one tick, and keep the
    /// learned span so the next tick sends no rejected request.
    #[tokio::test]
    async fn a_capped_provider_backfill_recovers_and_the_span_sticks() {
        const CAP: u64 = 150;
        let windows = Arc::new(std::sync::Mutex::new(Vec::new()));
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(CappedGetLogsRpc {
                cap: CAP,
                windows: Arc::clone(&windows),
            })
            .mount(&server)
            .await;
        let parsed_url = server.uri().parse();
        assert!(parsed_url.is_ok(), "mock server uri must parse");
        let Ok(url) = parsed_url else { return };
        let provider = ProviderBuilder::new().connect_http(url);

        let head_block = Arc::new(std::sync::atomic::AtomicU64::new(2_000));
        let head: Arc<dyn HeadSource> = Arc::new(MovingHead(Arc::clone(&head_block)));
        let (route, _sink, store) = checkpoint_route(350);
        let built = MultiplexedPollerBuilder::new(head, Duration::from_secs(1))
            .route(route)
            .build();
        let Ok(mut poller) = assert_built(built) else {
            return;
        };

        let first = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(
            first.is_ok(),
            "the capped backfill must complete: {first:?}"
        );
        assert!(
            poller.span.current() <= CAP,
            "span {} over the cap",
            poller.span.current()
        );
        assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(2_001));
        assert_eq!(
            store
                .load_checkpoint(CheckpointKey::PoolOpened)
                .ok()
                .flatten(),
            Some(2_000)
        );
        let recorded = windows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let accepted: Vec<(u64, u64)> = recorded
            .iter()
            .filter(|w| w.2)
            .map(|w| (w.0, w.1))
            .collect();
        let mut next = 350;
        for (from, to) in &accepted {
            assert_eq!(
                *from, next,
                "accepted windows must be contiguous: {accepted:?}"
            );
            next = to + 1;
        }
        assert_eq!(next, 2_001, "accepted windows must cover the whole gap");

        // Tick 2: 100 new blocks fit the learned span, so nothing is rejected.
        head_block.store(2_100, Ordering::SeqCst);
        let rejected_before = recorded.iter().filter(|w| !w.2).count();
        let second = run_tick(&provider, &mut poller, &no_shutdown()).await;
        assert!(second.is_ok(), "the live tail must complete: {second:?}");
        let rejected_after = windows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|w| !w.2)
            .count();
        assert_eq!(
            rejected_after, rejected_before,
            "the learned span must stick"
        );
        assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(2_101));
    }
}
