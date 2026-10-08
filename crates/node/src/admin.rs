//! Loopback-only admin JSON-RPC server (`appendix-local-admin-http.md`).
//!
//! Wire types (the `AdminRpc` trait, DTOs, error codes, [`parse_hash_arg`]) live
//! in [`decdn_common::admin`] so the user-facing `decdn` CLI can speak the
//! generated client without dragging in the daemon's runtime
//! dependencies. This module keeps the server-side implementation:
//! [`AdminState`], the [`AdminRpcImpl`] that backs the trait against the
//! live cache and runtime handles, and the bind/serve helpers the runtime
//! calls during start-up and graceful shutdown.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use decdn_cache::{CacheEngine, CacheError};
use decdn_common::admin::{
    AdminRpcServer, BUYER_POOL_STORE_ERROR_CODE, BUYER_POOL_UNAVAILABLE_CODE, BucketStat,
    BuyerLaneSnapshot, BuyerPoolSnapshot, BuyerPoolsResponse, CACHE_ERROR_CODE,
    CONFIG_PATH_UNSET_CODE, DHT_POISONED_CODE, DHT_UNAVAILABLE_CODE, DrainRequest, DrainResponse,
    EvictPreview, EvictRequest, EvictResponse, HealthResponse, LaneSnapshot, LanesResponse,
    POOL_STORE_ERROR_CODE, RELOAD_ERROR_CODE, RecordStoreHealth, ReloadResponse, RepublishHealth,
    RoutingHealth, SLASH_DETECTION_UNAVAILABLE_CODE, SlashRecordDto, SlashesResponse,
    StatusResponse, parse_hash_arg,
};
use decdn_incentive::buyer_pool::{BuyerPoolState, BuyerPoolStore};
use decdn_incentive::{LaneKey, LaneState, PoolStateStore};

pub use crate::handlers::client::LaneActivityClock;
use jsonrpsee::core::{RpcResult, async_trait};
use jsonrpsee::server::{Server, ServerConfig};
use jsonrpsee::types::ErrorObjectOwned;
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};

use crate::binding_check::BindingReport;
use crate::dht::{RecordStore, RepublishScheduler, RoutingTable, StakerSet};
use crate::metrics::Metrics;

// Wire types live in `decdn_common::admin`. We import the server-side
// trait, the request DTO, and the few error codes the server impl
// raises — clients reach the response DTOs through `decdn_common`
// directly.

use crate::runtime::RuntimeReloadState;

/// Cap concurrent admin connections. The surface is local-operator-only,
/// but an errant operator script looping requests must not be able to
/// exhaust the runtime's task budget. `u32` rather than `usize` because
/// `ServerConfig::max_connections` is typed that way.
const MAX_ADMIN_CONNECTIONS: u32 = 16;

/// Shared state for admin RPC handlers.
#[derive(Debug, Clone)]
pub struct AdminState {
    /// Raw bytes of this node's iroh `PublicKey`. Hex-encoded on the
    /// wire by `admin_v1_health`.
    node_id: [u8; 32],
    /// Process-start `Instant`, captured by the runtime at the top of
    /// `run()` before any `await` or I/O. Used as the origin of the
    /// `uptime_s` field returned by `admin_v1_health`. `Instant` (not
    /// `SystemTime`) so wall-clock skew during the process's lifetime
    /// can't make uptime go backwards. May predate `AdminState`
    /// construction by the time the RPC preflight, identity load, and
    /// cache open take.
    started_at: Instant,
    /// Cache engine handle for `admin_v1_evict` (issue #279). The engine
    /// is `Clone` (its `Arc<Inner>` is shared), so cloning into
    /// `AdminState` is cheap.
    cache: CacheEngine,
    /// Hot-reload hook for `admin_v1_reload` (issue #373). `None` when
    /// the node was started without a config file path (CLI-only flag
    /// invocation), in which case there's nothing on disk for reload to
    /// re-read; the RPC method translates that into a "config path
    /// unset" error so the operator gets a specific message instead of
    /// silently no-op'ing.
    reload_hook: Option<ReloadHook>,
    /// Drain trigger for `admin_v1_drain` (issue #244). Always present —
    /// drain has no preconditions analogous to "no config path", so this
    /// field is `Arc<DrainTrigger>` (not `Option<…>` like `reload_hook`)
    /// and the runtime wires it unconditionally.
    drain_trigger: Arc<DrainTrigger>,
    /// Live process metrics handle, used by `admin_v1_health` to read
    /// the current `dispatch_in_flight` gauge for the
    /// `in_flight_streams` field (issue #604). The `Arc<Metrics>` is
    /// the same handle the dispatch limiter increments/decrements via
    /// its permit RAII pair, so the value is always consistent with
    /// the live in-flight handler count.
    metrics: Arc<Metrics>,
    /// DHT introspection handles backing `admin_v1_status` (issue #741).
    /// `None` when the DHT subsystem isn't wired (the unit tests that
    /// exercise the cache methods build `AdminState` without it; the
    /// production runtime always attaches it via
    /// [`AdminState::with_dht`]). The `status` RPC returns
    /// [`DHT_UNAVAILABLE_CODE`] when this is `None`.
    dht: Option<DhtStatusHandles>,
    /// Lane introspection handles backing `admin_v1_lanes`
    /// (issue #749). `None` when the lane subsystem isn't wired (the
    /// unit tests that exercise the cache methods build `AdminState`
    /// without it; the production runtime always attaches it
    /// via [`AdminState::with_lanes`]). The `lanes` RPC returns an
    /// empty list (not an error) when this is `None` — a node with no
    /// payment surface legitimately has zero lanes to report, and the
    /// CLI's empty-table sentinel covers it.
    lanes: Option<LaneStatusHandles>,
    /// The node's buyer-side pool store, backing `admin_v1_pools` (#2078),
    /// attached via [`AdminState::with_buyer_pools`]. The SAME handle the buy
    /// loop records adoptions and top-ups into, so the admin surface reports
    /// what the daemon actually believes. The production runtime always
    /// attaches it, so `None` means a hand-built `AdminState` — a test, or a
    /// future bring-up with no buy leg — and answers
    /// [`BUYER_POOL_UNAVAILABLE_CODE`] rather than an empty list: a node that
    /// tracks nothing and a node that owns no pools call for opposite operator
    /// responses.
    buyer_pools: Option<BuyerPoolHandle>,
    /// Slash-detection handles backing `admin_v1_slashes` (#1032), attached via
    /// [`AdminState::with_slash_detection`]. `None` (no `slash_judge_address`
    /// wired / unit tests) → `slashes` returns [`SLASH_DETECTION_UNAVAILABLE_CODE`].
    slash_detection: Option<SlashStatusHandles>,
    /// Outcome of the bring-up node-id binding check (#1034), attached via
    /// [`AdminState::with_binding`]. Defaults to
    /// [`BindingReport::unknown`] — the honest answer for a state that was
    /// never sampled, and the one the unit tests here construct.
    binding: BindingReport,
    /// The SAME ADR 041 per-source warming allowance the buy loop debits, the
    /// serve path credits, and the eviction driver forgets from, attached via
    /// [`AdminState::with_warming`]. `admin_v1_evict` durably removes a blob
    /// exactly like a governance takedown does, so it must also drop the
    /// hash's `source_of` provenance tag — otherwise a re-admitted hash could
    /// spuriously credit its old source. `None` (unit tests that don't wire
    /// it) means the evict handler simply skips the forget.
    warming: Option<Arc<crate::warming_allowance::WarmingAllowance>>,
    /// Live on-chain active-staker set, attached via
    /// [`AdminState::with_staker_set`] (#1030). `None` (unit tests, and any
    /// build with no chain wiring) reports `registry_active: false`: an unwired
    /// `AdminState` cannot confirm registration, and the optimistic answer would
    /// tell an operator their node is registered when nothing checked.
    staker_set: Option<Arc<dyn crate::dht::staker_set::StakerSet>>,
    /// The live content denylist, attached via [`AdminState::with_denylist`].
    /// `None` (unit tests that don't wire it) reports `chain_denied_origins:
    /// 0` — an unwired `AdminState` has no chain-origin deny-set to count.
    denylist: Option<Arc<crate::content_deny::ContentDenylist>>,
    /// The operator Ethereum address this node runs under, attached via
    /// [`AdminState::with_operator_address`]. Sampled once at bring-up from the
    /// loaded eth keystore — the same value the node-id binding check derives —
    /// so `admin_v1_status` can report it without an extra chain round-trip.
    /// `None` (unit tests, and any build with no chain wiring) reports
    /// `operator_address: None`, the honest answer when nothing supplied one.
    operator_address: Option<Address>,
}

/// The buyer pool store handle `admin_v1_pools` reads, wrapped so
/// [`AdminState`] keeps its `Debug` derive. An `Arc` clone of the store the
/// buy loop already writes through, so attaching it adds read access, not new
/// ownership.
#[derive(Clone)]
pub struct BuyerPoolHandle(Arc<dyn BuyerPoolStore>);

// `AdminState` derives `Debug`, and `dyn BuyerPoolStore` carries no `Debug`
// bound, so name the wrapper without formatting the handle.
impl std::fmt::Debug for BuyerPoolHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuyerPoolHandle").finish_non_exhaustive()
    }
}

/// Read-only lane handles the `admin_v1_lanes` handler snapshots (issue
/// #749). Each is an `Arc` clone of state the runtime already owns and
/// shares with the `cdn/client/v1` handler and the settlement service, so
/// attaching this to [`AdminState`] adds read access, not new ownership.
/// Bundled into one struct so the `with_lanes` builder stays a single
/// argument.
#[derive(Clone)]
pub struct LaneStatusHandles {
    /// Persistent per-lane voucher state (same handle the client handler
    /// commits accepted vouchers to). Read via `load_all` to build the
    /// snapshot — the freshest committed `last_*` per lane.
    pub pool_store: Arc<dyn PoolStateStore>,
    /// Read handle over the client handler's live lane registry (issue #1733).
    /// Read for `seconds_since_last_voucher`; a lane reads back `None` until
    /// this process accepts a voucher for it.
    pub lane_activity: LaneActivityClock,
    /// Configured redemption threshold in micro-USDC
    /// (`blockchain.redeem_threshold_micro_usdc`). A lane whose accrued
    /// claim has reached this is reported `settlement_eligible`.
    pub redeem_threshold_micro_usdc: u64,
}

// `AdminState` derives `Debug`, so the bundled handles must too. The
// trait-object `Arc<dyn PoolStateStore>` carries no `Debug` bound, so
// hand-roll a terse impl that names the struct without formatting the handles.
impl std::fmt::Debug for LaneStatusHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneStatusHandles")
            .field(
                "redeem_threshold_micro_usdc",
                &self.redeem_threshold_micro_usdc,
            )
            .finish_non_exhaustive()
    }
}

/// Read-only slash-detection handle the `admin_v1_slashes` handler snapshots
/// (#1032). The shared in-memory store is populated by the slash watcher; the
/// admin surface only reads it. Bundled into a struct so the
/// `with_slash_detection` builder stays a single argument (and to leave room
/// for future fields without churning the signature).
#[derive(Debug, Clone)]
pub struct SlashStatusHandles {
    /// Detected slashes against this node's operator, appended in detection
    /// order (deduped by `slashId`). Same handle the watcher writes to.
    pub store: crate::slash_watcher::SlashStore,
}

/// Read-only DHT subsystem handles the `admin_v1_status` handler snapshots
/// (issue #741). Each shared-state field is an `Arc` clone of state the
/// runtime already owns and shares with the DHT handler / refresh /
/// republish tasks (plus the `Copy` `refresh_interval`), so attaching this
/// to [`AdminState`] adds no new ownership — only read access. Bundled into
/// one struct so the `AdminState` constructor stays a single optional
/// argument rather than five positional ones.
#[derive(Clone)]
pub struct DhtStatusHandles {
    /// Kademlia routing table (same handle the DHT handler / bucket-refresh
    /// task mutate). Locked briefly to snapshot bucket fill counts.
    pub routing: Arc<StdMutex<RoutingTable>>,
    /// Active-staker set (chain-backed in production). Read for its count.
    pub staker_set: Arc<dyn StakerSet>,
    /// Provider-record store. Locked briefly to read size + capacity.
    pub record_store: Arc<StdMutex<RecordStore>>,
    /// Republish scheduler. Read for its scheduled-record depth.
    pub republish: Arc<RepublishScheduler>,
    /// Wall-clock-µs of the last completed bucket-refresh pass (0 = never),
    /// stamped by [`crate::dht::bucket_refresh::run_bucket_refresh`].
    pub refresh_clock: Arc<AtomicU64>,
    /// Bucket-refresh interval, echoed so the operator sees the cadence the
    /// last-refresh timestamp is relative to.
    pub refresh_interval: Duration,
}

// `AdminState` derives `Debug`, so the bundled handles must too. The
// trait-object `Arc<dyn StakerSet>` carries no `Debug` bound, so hand-roll
// a terse impl that names the struct without formatting the handles.
impl std::fmt::Debug for DhtStatusHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtStatusHandles")
            .field("refresh_interval", &self.refresh_interval)
            .finish_non_exhaustive()
    }
}

/// One-shot trigger that lets `admin_v1_drain` wake the runtime's main
/// select loop and request graceful shutdown (issue #244). Modelled on
/// [`tokio::sync::Notify`] so the RPC handler can fire-and-return
/// without blocking on the runtime's shutdown latency. The runtime
/// awaits [`wait`](Self::wait) in its select loop; firing twice is a
/// no-op (the second `notify_one` coalesces, same as `Notify`).
///
/// `wait_admin` (issue #604) is a per-drain configuration bit:
/// [`fire`](Self::fire) takes it as a parameter. **First writer wins**
/// — once a fire has established the value, subsequent fires return
/// that same value rather than overwriting. This gives two concurrent
/// drain RPCs one well-defined outcome: the effective `wait_admin`
/// value the runtime will see is the first one written, and every
/// response reports that same value.
///
/// Cross-thread visibility comes from the `Notify::notify_one →
/// Notify::notified()` happens-before edge: the runtime's
/// [`wait_admin`](Self::wait_admin) load is sequenced after
/// `notified()` resolves, and tokio's `Notify` publishes prior writes
/// (including the `OnceLock` store) on that edge. The `OnceLock`'s own
/// synchronization is belt-and-braces — important only for readers
/// that don't go through `wait()`.
#[derive(Debug, Default)]
pub struct DrainTrigger {
    notify: Notify,
    /// `None` until the first `fire(...)` call lands; `Some(v)` once
    /// some writer has established the effective `wait_admin` value
    /// for this drain. Subsequent fires return this value without
    /// overwriting it.
    wait_admin: OnceLock<bool>,
}

impl DrainTrigger {
    /// Create a new `DrainTrigger`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Signal the runtime to begin graceful shutdown with the requested
    /// `wait_admin` ordering. Returns the *effective* value the runtime
    /// will see — equal to `wait_admin` if this is the first fire, or
    /// the prior writer's value if a fire already happened. The drain
    /// RPC uses the return as the `wait_admin_honored` ack so callers
    /// see the truth about what the runtime will actually do, even
    /// under a concurrent-drain race (issue #604 review).
    ///
    /// Fire-and-forget: calling this more than once is a no-op as far
    /// as wake-up goes (the second `notify_one` coalesces into the
    /// pending permit the first call stored).
    pub fn fire(&self, wait_admin: bool) -> bool {
        // First writer wins. `OnceLock::get_or_init` is the atomic
        // CAS-equivalent: only the first caller's closure runs and
        // its value is stored; every subsequent call returns the
        // previously-stored value.
        let effective = *self.wait_admin.get_or_init(|| wait_admin);
        // Notify *after* the OnceLock store so the runtime's
        // `notified()` resolution synchronizes-with our store. Any
        // reader that calls `wait_admin()` after observing `notified()`
        // is guaranteed to see `Some(effective)`.
        self.notify.notify_one();
        effective
    }

    /// Read the current `wait_admin` value. `false` when no fire has
    /// happened yet (runtime should pick the SIGTERM-equivalent
    /// ordering). The runtime calls this only after `wait()` has
    /// resolved, at which point `OnceLock::get()` returns `Some`.
    #[must_use]
    pub fn wait_admin(&self) -> bool {
        self.wait_admin.get().copied().unwrap_or(false)
    }

    /// Wait until [`fire`](Self::fire) is called. Returns immediately
    /// if `fire` was already called before `wait` was polled.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

/// Pair of values needed by `admin_v1_reload`: the runtime's reload state
/// (the same `Arc` the SIGHUP arm holds in `runtime::run`'s select loop)
/// and the path to the config file the operator started with. Bundled so
/// `AdminState` can carry "both or neither" as a single `Option`, matching
/// the runtime's "no path → no reload" invariant.
#[derive(Debug, Clone)]
pub struct ReloadHook {
    /// The runtime's reload state — the same `Arc` the SIGHUP arm holds.
    pub reload_state: Arc<RuntimeReloadState>,
    /// Path to the config file the operator started with, re-read on reload.
    pub config_path: PathBuf,
}

impl AdminState {
    // A builder would be tidier but is deferred until the next
    // signature change forces a refactor — the existing call sites
    // are few and each already lists every argument explicitly.
    //
    // The DHT introspection handles (issue #741) are attached via the
    // separate [`with_dht`](Self::with_dht) builder rather than as another
    // positional argument: `new` has several call sites (mostly unit
    // tests of the cache methods that don't need a DHT), and `with_dht`
    // lets the production runtime opt in without touching any of them.
    /// Assemble the admin surface's view of the running node. `reload_hook`
    /// is `None` when the daemon started with no config path, which is what
    /// makes reload unavailable rather than merely inert.
    pub const fn new(
        node_id: [u8; 32],
        started_at: Instant,
        cache: CacheEngine,
        reload_hook: Option<ReloadHook>,
        drain_trigger: Arc<DrainTrigger>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            node_id,
            started_at,
            cache,
            reload_hook,
            drain_trigger,
            metrics,
            dht: None,
            lanes: None,
            buyer_pools: None,
            slash_detection: None,
            binding: BindingReport::unknown(),
            warming: None,
            staker_set: None,
            denylist: None,
            operator_address: None,
        }
    }

    /// Whether this node is in the on-chain active-staker set right now
    /// (#1030), read from the same shared projection that feeds DHT admission
    /// so the two can never disagree.
    ///
    /// Diagnostic only — nothing in the serve path consults it. See
    /// [`decdn_common::admin::HealthResponse::registry_active`] for why the
    /// daemon does not gate delivery on its own registration.
    ///
    /// `false` when no set is attached: an unwired `AdminState` cannot confirm
    /// registration, and the optimistic answer would claim something nothing
    /// checked.
    fn registry_active(&self) -> bool {
        self.staker_set.as_ref().is_some_and(|set| {
            set.is_active(&crate::dht::routing::NodeId::from_bytes(self.node_id))
        })
    }

    /// Attach the bring-up node-id binding check so `admin_v1_health` can
    /// report whether this node's key is the one bound on-chain (#1034). The
    /// production runtime calls this once after `new`; without it, `health`
    /// reports `BindingStatus::Unknown`, which means "not checked" rather
    /// than "fine".
    #[must_use]
    pub const fn with_binding(mut self, binding: BindingReport) -> Self {
        self.binding = binding;
        self
    }

    /// Attach the live active-staker set so `admin_v1_health` can report
    /// whether this node is registered on-chain (#1030). The production runtime
    /// calls this once after `new` with the SAME `Arc` DHT admission holds. Not
    /// `const`: unlike [`Self::with_binding`]'s `Copy` report, assigning over an
    /// `Option<Arc<_>>` runs a destructor.
    #[must_use]
    pub fn with_staker_set(
        mut self,
        staker_set: Arc<dyn crate::dht::staker_set::StakerSet>,
    ) -> Self {
        self.staker_set = Some(staker_set);
        self
    }

    /// Attach DHT introspection handles so `admin_v1_status` can report
    /// routing-table health (issue #741). The production runtime calls
    /// this once after `new`; without it, `status` returns
    /// [`DHT_UNAVAILABLE_CODE`].
    #[must_use]
    pub fn with_dht(mut self, dht: DhtStatusHandles) -> Self {
        self.dht = Some(dht);
        self
    }

    /// Attach lane introspection handles so `admin_v1_lanes` can report
    /// lane state (issue #749). The production runtime calls this once
    /// after `new`; without it, `lanes` returns an empty list with the
    /// default-zero threshold.
    #[must_use]
    pub fn with_lanes(mut self, lanes: LaneStatusHandles) -> Self {
        self.lanes = Some(lanes);
        self
    }

    /// Attach the buyer pool store so `admin_v1_pools` can report this node's
    /// buyer-side `PaymentPool` state (#2078). The production runtime calls
    /// this once after `new` with the SAME handle `BuyerPoolService` writes
    /// through, unconditionally; without it, `pools` returns
    /// [`BUYER_POOL_UNAVAILABLE_CODE`].
    #[must_use]
    pub fn with_buyer_pools(mut self, buyer_pools: Arc<dyn BuyerPoolStore>) -> Self {
        self.buyer_pools = Some(BuyerPoolHandle(buyer_pools));
        self
    }

    /// Attach slash-detection handles so `admin_v1_slashes` can report slashes
    /// against this node's operator (#1032). The production runtime calls this
    /// once after `new` when a `slash_judge_address` is configured; without it,
    /// `slashes` returns [`SLASH_DETECTION_UNAVAILABLE_CODE`].
    #[must_use]
    pub fn with_slash_detection(mut self, slash_detection: SlashStatusHandles) -> Self {
        self.slash_detection = Some(slash_detection);
        self
    }

    /// Attach the shared ADR 041 warming allowance so `admin_v1_evict` can
    /// forget an evicted hash's `source_of` provenance tag (issue #1751
    /// review). The production runtime calls this once after `new` with the
    /// SAME `Arc` the buy loop, the eviction driver, and the warming-credit
    /// aggregator share; without it, an admin evict leaves the tag in place.
    #[must_use]
    pub fn with_warming(
        mut self,
        warming: Arc<crate::warming_allowance::WarmingAllowance>,
    ) -> Self {
        self.warming = Some(warming);
        self
    }

    /// Attach the live content denylist so `admin_v1_status` can report the
    /// current on-chain origin deny-set size (`chain_denied_origins`). The
    /// production runtime calls this once after `new` with the SAME `Arc` the
    /// client handler and blacklist watcher hold; without it, `status`
    /// reports `0`.
    #[must_use]
    pub fn with_denylist(mut self, denylist: Arc<crate::content_deny::ContentDenylist>) -> Self {
        self.denylist = Some(denylist);
        self
    }

    /// Attach the operator Ethereum address so `admin_v1_status` can report the
    /// wallet this node runs under. The production runtime calls this once after
    /// `new` with the loaded eth signer's address (the same value the bring-up
    /// binding check derives); without it, `status` reports
    /// `operator_address: None`.
    #[must_use]
    pub const fn with_operator_address(mut self, operator_address: Address) -> Self {
        self.operator_address = Some(operator_address);
        self
    }
}

/// Convert a [`CacheError`] into a JSON-RPC error suitable for
/// `admin_v1_evict`. The operator-facing message names the failure and its
/// whole cause chain (`store error: <context>: <root cause>`) rather than a
/// generic "cache failed".
fn cache_error_to_rpc(err: &CacheError) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(
        CACHE_ERROR_CODE,
        err.display_chain().to_string(),
        None::<()>,
    )
}

/// Lock a DHT-subsystem `std::sync::Mutex` for `admin_v1_status`,
/// converting a poisoned lock into a [`DHT_POISONED_CODE`] RPC error
/// rather than propagating the panic the standard `unwrap` would (the
/// workspace anti-panic policy forbids `unwrap`/`expect`). `what` names
/// the guarded structure for the operator-facing message.
///
/// A poisoned lock means a writer panicked while holding it — a severe
/// in-process fault, distinct from the benign "no DHT wired" state
/// ([`DHT_UNAVAILABLE_CODE`]). It is logged at `error` level so the fault
/// is diagnosable from the node's logs even when a scripted client
/// discards the RPC error body.
fn lock_or_rpc_err<'a, T>(
    mutex: &'a StdMutex<T>,
    what: &str,
) -> Result<std::sync::MutexGuard<'a, T>, ErrorObjectOwned> {
    mutex.lock().map_err(|_| {
        tracing::error!(
            guard = %what,
            "DHT mutex poisoned — a writer panicked while holding it; \
             admin_v1_status cannot read a consistent snapshot"
        );
        ErrorObjectOwned::owned(
            DHT_POISONED_CODE,
            format!("DHT {what} mutex poisoned"),
            None::<()>,
        )
    })
}

/// Concrete server implementation backed by the live [`AdminState`].
#[derive(Debug, Clone)]
pub struct AdminRpcImpl {
    state: AdminState,
}

impl AdminRpcImpl {
    /// Serve the admin RPC methods against `state`.
    pub const fn new(state: AdminState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AdminRpcServer for AdminRpcImpl {
    async fn health(&self) -> RpcResult<HealthResponse> {
        Ok(HealthResponse {
            node_id: alloy::primitives::hex::encode(self.state.node_id),
            uptime_s: self.state.started_at.elapsed().as_secs(),
            in_flight_streams: self.state.metrics.dispatch_in_flight_value(),
            binding: self.state.binding.status,
            // Same lowercase-hex encoding as `node_id`, so the two are
            // directly comparable by eye and by script on a mismatch.
            bound_node_id: self
                .state
                .binding
                .bound_node_id
                .map(alloy::primitives::hex::encode),
            registry_active: self.state.registry_active(),
        })
    }

    async fn evict(&self, req: EvictRequest) -> RpcResult<EvictResponse> {
        // `parse_hash_arg` yields the config-vocabulary leaf hash (keeps
        // iroh-blobs out of the publisher CLI, #578); the blob store
        // keys on `decdn_cache::Hash`, so convert at this boundary.
        let hash = decdn_cache::to_store_hash(parse_hash_arg(&req.hash)?);

        // One `inspect` call snapshots size, pin, evicted, served — the
        // pre-evict state the response will carry. Any cache I/O
        // failure here is a genuine problem (the iroh-blobs store is
        // misbehaving), not a routine "blob is absent" path. Note that
        // `inspect` reads `BlobStatus` directly, so a complete blob reports
        // its size even when `already_evicted` is already true — operators
        // want to see disk-reclaim potential. A partial reports its
        // validated total, or `None` before its last chunk arrives.
        let preview = self
            .state
            .cache
            .inspect(hash)
            .await
            .map_err(|err| cache_error_to_rpc(&err))?;

        // For a real evict we mutate after the inspect. For a dry-run
        // we skip the `evict()` call entirely — the response is the
        // snapshot only.
        if !req.dry_run {
            // `evict` returns `Err` if it can't durably persist the
            // eviction (e.g. evicted-log fsync failed). For DMCA
            // takedowns the operator must learn about that failure
            // rather than getting an "ok" response that silently
            // degraded to in-memory-only.
            self.state
                .cache
                .evict(hash)
                .await
                .map_err(|err| cache_error_to_rpc(&err))?;
            // ADR 041: drop the warming tag for the evicted hash, so a later
            // reuse of this slot can never credit a stale source's allowance.
            if let Some(warming) = self.state.warming.as_ref() {
                warming.forget(hash);
            }
        }

        Ok(EvictResponse {
            was_present: preview.served,
            dry_run: req.dry_run,
            preview: EvictPreview {
                size_bytes: preview.size_bytes,
                last_accessed_us_ago: preview.last_accessed_us_ago,
                pinned: preview.pinned,
                already_evicted: preview.already_evicted,
                origin_kinds: preview.origin_kinds,
            },
        })
    }

    async fn reload(&self) -> RpcResult<ReloadResponse> {
        let hook = self.state.reload_hook.as_ref().ok_or_else(|| {
            ErrorObjectOwned::owned(
                CONFIG_PATH_UNSET_CODE,
                "no config file path is in use; restart the node with \
                 --config <path> to enable admin_v1_reload",
                None::<()>,
            )
        })?;
        // Delegating to `RuntimeReloadState::reload` keeps the SIGHUP
        // and RPC paths byte-identical: same parse, same resolution,
        // same "previous values retained on error" contract, and the
        // same internal mutexes serialise concurrent reloads (a SIGHUP
        // arriving mid-RPC waits, and vice versa). Surface the error
        // text via `{err:#}` so the operator sees the underlying
        // resolution failure rather than a generic wrapper.
        hook.reload_state
            .reload(&hook.config_path)
            .await
            .map_err(|err| {
                ErrorObjectOwned::owned(
                    RELOAD_ERROR_CODE,
                    format!("config reload failed: {err:#}"),
                    None::<()>,
                )
            })?;
        let snap = hook.reload_state.current();
        Ok(ReloadResponse {
            // `current()` is `None` when `RUST_LOG` drives the live filter
            // (a reload never replaces it) or on a poisoned mutex; in both
            // cases no config-file level is running. Returning a stable
            // string ("unknown") rather than the empty string makes operator
            // scripts that parse the response trivially unambiguous.
            log_level: snap
                .log_level
                .map_or_else(|| "unknown".to_string(), |l| l.to_string()),
        })
    }

    async fn drain(&self, req: DrainRequest) -> RpcResult<DrainResponse> {
        // Atomic fire-with-wait_admin: returns the *effective* value
        // the runtime will see. First writer wins, so a concurrent
        // drain race cannot produce a response that lies about
        // what the runtime will do (issue #604 review). The CLI reads
        // this ack and refuses to poll when another caller's
        // `wait_admin: false` won the race.
        let honored = self.state.drain_trigger.fire(req.wait_admin);
        Ok(DrainResponse {
            initiated: true,
            wait_admin_honored: honored,
        })
    }

    async fn status(&self) -> RpcResult<StatusResponse> {
        let Some(dht) = self.state.dht.as_ref() else {
            return Err(ErrorObjectOwned::owned(
                DHT_UNAVAILABLE_CODE,
                "DHT subsystem not wired on this node",
                None::<()>,
            ));
        };

        // Snapshot the routing table under a brief lock: copy out only the
        // per-bucket fill counts, then release before building DTOs. A
        // poisoned mutex (a panicking writer elsewhere) surfaces as a
        // specific RPC error rather than propagating the panic — the
        // workspace anti-panic policy forbids `unwrap`/`expect` here.
        let (total_peers, bucket_fills) = {
            let table = lock_or_rpc_err(&dht.routing, "routing table")?;
            (table.len(), table.non_empty_bucket_fills())
        };
        let buckets: Vec<BucketStat> = bucket_fills
            .into_iter()
            .map(|(index, fill)| BucketStat {
                // Bucket indices are 0..=255 and fills 0..=K_BUCKET_SIZE
                // (20), so both fit u16 with room to spare. `try_from`
                // (not `as`) avoids the `cast_possible_truncation` lint on
                // the narrowing `usize → u16`, saturating rather than
                // panicking on the unreachable overflow.
                index: u16::try_from(index).unwrap_or(u16::MAX),
                fill: u16::try_from(fill).unwrap_or(u16::MAX),
            })
            .collect();
        // Capacity is the same Kademlia K for every bucket, so report it
        // once on RoutingHealth rather than per BucketStat.
        let bucket_capacity = u16::try_from(crate::dht::routing::K_BUCKET_SIZE).unwrap_or(u16::MAX);

        let (records, records_capacity) = {
            let store = lock_or_rpc_err(&dht.record_store, "record store")?;
            (store.len(), store.capacity())
        };

        // 0 means "no refresh pass has completed yet" → None on the wire.
        let last_refresh = match dht.refresh_clock.load(Ordering::Relaxed) {
            0 => None,
            us => Some(us),
        };

        // These `usize → u64` conversions are widening, so no
        // `cast_possible_truncation` lint fires and no policy requires
        // `try_from` here — it's used purely for stylistic uniformity with
        // the narrowing bucket conversions above. The `unwrap_or(u64::MAX)`
        // arms are unreachable on every supported (≤64-bit) target.
        // Poison reporting to the operator is *partial* by design. The
        // routing-table and record-store reads above go through
        // `lock_or_rpc_err`, so a poisoned lock there surfaces as
        // `DHT_POISONED_CODE`. The `staker_set` / `republish` counts below
        // can't: their `len()` signatures own their own poison policy and
        // return a plain `usize`. `RepublishScheduler::len` degrades a
        // poisoned lock to `0` (so `scheduled_records` under-reports rather
        // than failing the call); `ChainStakerSet::len` recovers the inner
        // set and warns (true count, no error). We can't change those
        // signatures from here, so these two fields trade poison-visibility
        // for not aborting the whole snapshot — accepted, documented.
        Ok(StatusResponse {
            node_id: alloy::primitives::hex::encode(self.state.node_id),
            routing: RoutingHealth {
                total_peers: u64::try_from(total_peers).unwrap_or(u64::MAX),
                non_empty_buckets: u64::try_from(buckets.len()).unwrap_or(u64::MAX),
                buckets,
                bucket_capacity,
                refresh_interval_s: dht.refresh_interval.as_secs(),
                last_refresh_us: last_refresh,
            },
            known_stakers: u64::try_from(dht.staker_set.len()).unwrap_or(u64::MAX),
            record_store: RecordStoreHealth {
                records: u64::try_from(records).unwrap_or(u64::MAX),
                capacity: u64::try_from(records_capacity).unwrap_or(u64::MAX),
            },
            republish: RepublishHealth {
                scheduled_records: u64::try_from(dht.republish.len()).unwrap_or(u64::MAX),
            },
            chain_denied_origins: self.state.denylist.as_ref().map_or(0, |d| {
                u64::try_from(d.chain_origin_count()).unwrap_or(u64::MAX)
            }),
            // `Address`'s `Display` is the EIP-55 mixed-case checksum, the same
            // rendering the lane snapshots use. Sampled once at bring-up (no
            // chain read here); `None` when no operator was wired in.
            operator_address: self.state.operator_address.map(|a| a.to_string()),
        })
    }

    async fn lanes(&self) -> RpcResult<LanesResponse> {
        // No lane subsystem wired (unit tests / a node with no payment
        // surface): report an empty list rather than an error. Zero
        // lanes is a legitimate state, and the CLI renders the
        // empty-table sentinel for it.
        let Some(ch) = self.state.lanes.as_ref() else {
            return Ok(LanesResponse {
                lanes: Vec::new(),
                redeem_threshold_micro_usdc: 0,
            });
        };

        // `load_all` on the redb-backed store is blocking I/O (the store
        // trait is sync per ADR appendix-poc-production-seams §1). Run it
        // on the blocking pool so we don't stall a runtime worker, then
        // build the wire DTOs off the loaded `Vec` — the cheap part. A
        // store failure surfaces as a specific RPC error so the operator
        // sees "lane snapshot unavailable" rather than a transport fault.
        let store = Arc::clone(&ch.pool_store);
        let states = tokio::task::spawn_blocking(move || store.load_all())
            .await
            .map_err(|join_err| {
                // `JoinError` is returned on panic OR cancellation (e.g.
                // runtime shutdown); name the actual cause so a cancelled
                // task at teardown isn't misread as a panic.
                let cause = if join_err.is_cancelled() {
                    "cancelled"
                } else {
                    "panicked"
                };
                tracing::error!(
                    error = %join_err,
                    cause,
                    "pool-store load task did not complete"
                );
                ErrorObjectOwned::owned(
                    POOL_STORE_ERROR_CODE,
                    "pool state store load task failed",
                    None::<()>,
                )
            })?
            .map_err(|store_err| {
                tracing::error!(error = %store_err, "admin_v1_lanes could not load pool store");
                ErrorObjectOwned::owned(
                    POOL_STORE_ERROR_CODE,
                    format!("pool state store load failed: {store_err}"),
                    None::<()>,
                )
            })?;

        let threshold = U256::from(ch.redeem_threshold_micro_usdc);
        // Read each live lane's last-voucher age off the handler's registry
        // (issue #1733); lanes with no voucher since restart are absent from the
        // map and read back as `None`.
        let ages = ch.lane_activity.ages().await;
        let lanes = build_lane_snapshots(&states, threshold, &ages);
        Ok(LanesResponse {
            lanes,
            redeem_threshold_micro_usdc: ch.redeem_threshold_micro_usdc,
        })
    }

    async fn slashes(&self) -> RpcResult<SlashesResponse> {
        let Some(handles) = self.state.slash_detection.as_ref() else {
            return Err(ErrorObjectOwned::owned(
                SLASH_DETECTION_UNAVAILABLE_CODE,
                "slash detection is not wired on this node",
                None::<()>,
            ));
        };
        // Short read of an in-memory Vec — no await held. Poison-tolerant: a
        // panicked writer must not wedge the read-only admin surface.
        let guard = handles
            .store
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Newest-first: the watcher appends in detection order.
        let slashes = guard
            .iter()
            .rev()
            .map(|s| SlashRecordDto {
                slash_id: s.slash_id.to_string(),
                offense_type: s.offense_type,
                amount: s.amount.to_string(),
                evidence_hash: format!("{:#x}", s.evidence_hash),
                block_number: s.block_number,
                appeal_window_close: s.appeal_window_close,
            })
            .collect();
        Ok(SlashesResponse { slashes })
    }

    async fn pools(&self) -> RpcResult<BuyerPoolsResponse> {
        // Not wired is an ERROR here, unlike `lanes`. A node with no buyer leg
        // never pays for an upstream pull, and rendering that as "zero pools"
        // is what sends an operator hunting a stranded deposit in the wrong
        // place (#2078).
        let Some(store) = self.state.buyer_pools.as_ref() else {
            return Err(ErrorObjectOwned::owned(
                BUYER_POOL_UNAVAILABLE_CODE,
                "no buyer payment-pool store is attached to this admin surface, so this node \
                 tracks no pools and pays no provider",
                None::<()>,
            ));
        };

        // `load_all` on the redb-backed store is blocking I/O (the store trait
        // is sync per ADR appendix-poc-production-seams §1). Run it on the
        // blocking pool so we don't stall a runtime worker.
        let store = Arc::clone(&store.0);
        let load = tokio::task::spawn_blocking(move || store.load_all())
            .await
            .map_err(|join_err| {
                // `JoinError` is returned on panic OR cancellation (e.g.
                // runtime shutdown); name the actual cause so a cancelled task
                // at teardown isn't misread as a panic.
                let cause = if join_err.is_cancelled() {
                    "cancelled"
                } else {
                    "panicked"
                };
                tracing::error!(
                    error = %join_err,
                    cause,
                    "buyer-pool-store load task did not complete"
                );
                ErrorObjectOwned::owned(
                    BUYER_POOL_STORE_ERROR_CODE,
                    "buyer pool store load task failed",
                    None::<()>,
                )
            })?
            .map_err(|store_err| {
                tracing::error!(
                    error = %store_err,
                    "admin_v1_pools could not load buyer pool store"
                );
                ErrorObjectOwned::owned(
                    BUYER_POOL_STORE_ERROR_CODE,
                    format!("buyer pool store load failed: {store_err}"),
                    None::<()>,
                )
            })?;

        Ok(build_buyer_pools_response(load.pools, &load.skipped))
    }
}

/// Build the wire [`BuyerPoolsResponse`] from loaded buyer pool states and the
/// `pool_id`s whose rows would not decode. Pure (no I/O) so the shape and the
/// ordering are unit-testable without a store or an async runtime.
///
/// Both lists are sorted — pools by `pool_id`, lanes by `(signer, provider)` —
/// because the store's iteration order is a redb implementation detail and an
/// operator diffing two calls should see a change only when the state changed.
/// `U256` amounts narrow to `u64` micro-USDC via `try_from(..).unwrap_or(u64::MAX)`;
/// a real pool is bounded by its on-chain deposit, so the saturation arm is
/// unreachable.
fn build_buyer_pools_response(
    mut pools: Vec<BuyerPoolState>,
    skipped: &[decdn_incentive::lane::PoolId],
) -> BuyerPoolsResponse {
    pools.sort_by_key(|p| p.pool_id);
    let pools = pools
        .iter()
        .map(|p| {
            let mut lanes: Vec<_> = p.lanes().collect();
            lanes.sort_by_key(|(lane, _)| (lane.signer, lane.provider));
            BuyerPoolSnapshot {
                pool_id: format!("{:#x}", p.pool_id),
                chain_id: p.deployment.chain_id,
                payment_pool: p.deployment.payment_pool.to_string(),
                owner: p.owner.to_string(),
                token: p.token.to_string(),
                deposit_micro_usdc: u64::try_from(p.deposit).unwrap_or(u64::MAX),
                lanes: lanes
                    .into_iter()
                    .map(|(lane, progress)| BuyerLaneSnapshot {
                        voucher_signer: lane.signer.to_string(),
                        provider: lane.provider.to_string(),
                        last_amount_micro_usdc: u64::try_from(progress.last_amount)
                            .unwrap_or(u64::MAX),
                        last_bytes_delivered: u64::try_from(progress.last_bytes)
                            .unwrap_or(u64::MAX),
                    })
                    .collect(),
            }
        })
        .collect();
    let mut skipped: Vec<_> = skipped.iter().map(|p| format!("{p:#x}")).collect();
    skipped.sort();
    BuyerPoolsResponse { pools, skipped }
}

/// Build the wire `LaneSnapshot` list from loaded lane states, the
/// redemption `threshold` (in micro-USDC as a `U256`), and each live lane's
/// whole-seconds last-voucher age (`ages`, keyed by [`LaneKey`]; absent ⇒
/// `None`). Pure (no I/O, no locks) so it's unit-testable without a store, an
/// async runtime, or a live lane registry.
///
/// Ordering: lanes with a known last-voucher age first (most recently
/// active ahead of those with `None`), then by descending outstanding
/// amount — the on-call use case is "which lanes are closest to a
/// settlement / liquidity event?". `U256` amounts narrow to `u64`
/// micro-USDC via `try_from(...).unwrap_or(u64::MAX)`; a real pool is
/// bounded by its on-chain deposit so the saturation arm is unreachable.
fn build_lane_snapshots(
    states: &[LaneState],
    threshold: U256,
    ages: &HashMap<LaneKey, u64>,
) -> Vec<LaneSnapshot> {
    let mut snapshots: Vec<LaneSnapshot> = states
        .iter()
        .map(|state| {
            let outstanding = state.last_amount();
            LaneSnapshot {
                // A lane is keyed by `(pool_id, signer, provider)`. The admin
                // surface reports the pool id as the lane id; the signer as
                // both counterparty and voucher signer (the shared-pool model
                // has no separate delegate). There is no per-voucher nonce —
                // cumulative amount is the sole ordering key — so `last_nonce`
                // reports 0, and the pool-level deposit is not carried per lane
                // so `deposit_micro_usdc` reports 0.
                pool_id: state.pool_id.to_string(),
                counterparty: state.signer.to_string(),
                voucher_signer: state.signer.to_string(),
                last_nonce: 0,
                outstanding_micro_usdc: u64::try_from(outstanding).unwrap_or(u64::MAX),
                deposit_micro_usdc: 0,
                seconds_since_last_voucher: ages.get(&state.key()).copied(),
                // Upper-bound eligibility: the admin surface doesn't read
                // the on-chain `withdrawnAmount`, so it compares the full
                // accrued claim against the threshold (documented on the
                // DTO field). `>=` matches the redeemer's `< floor`
                // short-circuit (`redeem_planned_lanes` in
                // payment_settlement.rs), whose floor is this threshold.
                settlement_eligible: outstanding >= threshold,
            }
        })
        .collect();
    // Most recently active first: `Some(age)` sorts ahead of `None`, and
    // within each group smaller age (more recent) first; ties broken by
    // descending outstanding. `Reverse` on the outstanding term puts the
    // largest claim first. The key tuple is a total order so `sort_unstable`
    // (no allocation) is safe — equal keys mean identical sort-relevant
    // fields, with no insertion-order tiebreak promised.
    snapshots.sort_unstable_by_key(|s| {
        (
            s.seconds_since_last_voucher.is_none(),
            s.seconds_since_last_voucher.unwrap_or(0),
            std::cmp::Reverse(s.outstanding_micro_usdc),
        )
    });
    snapshots
}

/// Bind the admin listener. Kept synchronous-at-startup so port
/// conflicts fail fast rather than deep inside the runtime task graph.
///
/// # Errors
/// Returns an error if the underlying `crate::net::bind_reuseaddr` fails
/// (socket creation, `bind`, or `listen`).
pub fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    // `SO_REUSEADDR` so a restart rebinds this fixed port immediately instead of
    // racing a `TIME_WAIT` remnant from the prior process (see `crate::net`).
    let listener = crate::net::bind_reuseaddr(addr)
        .map_err(|e| anyhow::anyhow!("admin bind {addr} failed: {e}"))?;
    // Defense-in-depth for the unauthenticated admin RPC surface (#845),
    // mirroring `metrics::bind` (#579). `to_canonical()` unwraps IPv4-mapped
    // IPv6 (e.g. `::ffff:127.0.0.1`) so the dual-stack loopback form doesn't
    // trip a false warning; `Ipv6Addr::is_loopback()` only matches `::1`.
    let ip = addr.ip().to_canonical();
    if !ip.is_loopback() {
        if ip.is_unspecified() {
            tracing::warn!(
                %addr,
                "admin server is binding all interfaces (non-loopback); the admin RPC is \
                 unauthenticated and can drain lanes, trigger announces, and read peer \
                 state — gate it behind loopback or a private network"
            );
        } else {
            tracing::warn!(
                %addr,
                "admin server is binding a non-loopback address; the admin RPC is \
                 unauthenticated and can drain lanes, trigger announces, and read peer \
                 state — restrict reachability to trusted operators"
            );
        }
    }
    tracing::info!(%addr, "admin server listening");
    Ok(listener)
}

/// Serve admin RPC methods on `listener` until `shutdown` fires.
///
/// Concurrency is capped via `ServerConfig::max_connections`; shutdown
/// is signalled by calling `ServerHandle::stop()`, then we await
/// `stopped()` so a caller that drops the future cannot leave the
/// background accept loop running.
#[allow(clippy::cognitive_complexity)] // Config build + select on two shutdown paths reads linearly.
pub async fn serve(
    listener: TcpListener,
    state: AdminState,
    mut shutdown: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    // jsonrpsee's `build_from_tcp` expects a `std::net::TcpListener` in
    // blocking mode. `into_std` is the official handoff; it must be
    // called before any incoming connections have been accepted on the
    // tokio side, which is the case here (we've only just bound).
    let std_listener = listener
        .into_std()
        .map_err(|e| anyhow::anyhow!("convert admin listener to std: {e}"))?;

    let config = ServerConfig::builder()
        .max_connections(MAX_ADMIN_CONNECTIONS)
        .http_only()
        .build();

    let server = Server::builder()
        .set_config(config)
        .build_from_tcp(std_listener)
        .map_err(|e| anyhow::anyhow!("build admin RPC server: {e}"))?;

    let rpc = AdminRpcImpl::new(state);
    let handle = server.start(rpc.into_rpc());

    // Two shutdown paths:
    //   1. Runtime fires the oneshot -> we call `handle.stop()` and
    //      wait for the accept loop to drain.
    //   2. Server exits on its own (shouldn't happen for HTTP-only but
    //      guard against it) -> return Ok so the runtime can notice
    //      via the `admin_stop_tx` / `warn!` path.
    let stopped = handle.clone().stopped();
    tokio::pin!(stopped);
    tokio::select! {
        biased;
        _ = &mut shutdown => {
            tracing::debug!("admin server shutdown signal received");
            if handle.stop().is_err() {
                tracing::debug!("admin server already stopped before shutdown signal");
            }
            handle.stopped().await;
        }
        () = &mut stopped => {
            tracing::warn!("admin server self-stopped before shutdown signal");
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
