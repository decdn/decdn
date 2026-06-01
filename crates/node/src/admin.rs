//! Loopback-only admin JSON-RPC server (ADR 025).
//!
//! Wire types (the `AdminRpc` trait, DTOs, error codes, [`parse_hash_arg`]) live
//! in [`decdn_common::admin`] so the user-facing `decdn` CLI can speak the
//! generated client without dragging in the daemon's runtime
//! dependencies. This module keeps the server-side implementation:
//! [`AdminState`], the [`AdminRpcImpl`] that backs the trait against a
//! live cache + peer table, and the bind/serve helpers the runtime calls
//! during start-up and graceful shutdown.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{CacheEngine, CacheError};
use decdn_common::admin::{
    AdminRpcServer, AnnounceResponse, BucketStat, CACHE_ERROR_CODE, CONFIG_PATH_UNSET_CODE,
    DHT_POISONED_CODE, DHT_UNAVAILABLE_CODE, DrainRequest, DrainResponse, EvictPreview,
    EvictRequest, EvictResponse, HealthResponse, PUBLISHER_DISABLED_CODE, PeerView, PeersResponse,
    RELOAD_ERROR_CODE, RecordStoreHealth, ReloadResponse, RepublishHealth, RoutingHealth,
    StatusResponse, parse_hash_arg,
};
use decdn_gossip::{AnnounceTrigger, PeerEntry, PeerTable};
use jsonrpsee::core::{RpcResult, async_trait};
use jsonrpsee::server::{Server, ServerConfig};
use jsonrpsee::types::ErrorObjectOwned;
use tokio::net::TcpListener;
use tokio::sync::{Notify, RwLock, oneshot};

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
    peer_table: Arc<RwLock<PeerTable>>,
    /// Raw bytes of this node's iroh `PublicKey`. Hex-encoded on the
    /// wire by `admin_v1_health`, matching the encoding `PeerView`
    /// already uses for peer node ids on `admin_v1_peersList`.
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
    /// One-shot announce trigger for `admin_v1_announce` (issue #280).
    /// `None` when the publisher is disabled (no region configured); the
    /// RPC method translates that into a "publisher disabled" error so the
    /// operator gets a specific message rather than a generic failure.
    announce_trigger: Option<Arc<AnnounceTrigger>>,
    /// Hot-reload hook for `admin_v1_reload` (issue #373). `None` when
    /// the node was started without a config file path (CLI-only flag
    /// invocation), in which case there's nothing on disk for reload to
    /// re-read; the RPC method translates that into a "config path
    /// unset" error so the operator gets a specific message instead of
    /// silently no-op'ing.
    reload_hook: Option<ReloadHook>,
    /// Drain trigger for `admin_v1_drain` (issue #244). Always present —
    /// drain has no preconditions analogous to "publisher disabled" or
    /// "no config path", so this field is `Arc<DrainTrigger>` (not
    /// `Option<…>` like `announce_trigger` / `reload_hook`) and the
    /// runtime wires it unconditionally.
    drain_trigger: Arc<DrainTrigger>,
    /// Loaded Ethereum keystore signer (issue #406). Required because the
    /// resolved blockchain config validates the keystore at startup, so any
    /// successful runtime is guaranteed to have a signer. Stored here
    /// because `AdminState` is the cross-runtime shared-state struct that
    /// already plumbs to multiple consumers; future non-RPC consumers
    /// (vouchers per #319, on-chain txs per #327) clone this `Arc`. The
    /// `Debug` impl on `PrivateKeySigner` prints the address only — never
    /// the secret scalar.
    #[allow(dead_code)] // Wired in #406; consumed by #319 / #327.
    signer: Arc<PrivateKeySigner>,
    /// Live process metrics handle, used by `admin_v1_health` to read
    /// the current `dispatch_in_flight` gauge for the
    /// `in_flight_streams` field (issue #604). The `Arc<Metrics>` is
    /// the same handle the dispatch limiter increments/decrements via
    /// its permit RAII pair, so the value is always consistent with
    /// the live in-flight handler count.
    metrics: Arc<Metrics>,
    /// DHT introspection handles backing `admin_v1_status` (issue #741).
    /// `None` when the DHT subsystem isn't wired (the unit tests that
    /// exercise the cache/peer-table methods build `AdminState` without
    /// it; the production runtime always attaches it via
    /// [`AdminState::with_dht`]). The `status` RPC returns
    /// [`DHT_UNAVAILABLE_CODE`] when this is `None`.
    dht: Option<DhtStatusHandles>,
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
/// that same value rather than overwriting. This collapses two prior
/// races into one well-defined outcome:
///
/// 1. A handler that called `set_wait_admin` after `fire` (no longer
///    possible — the API has no such method).
/// 2. Two concurrent drain RPCs whose `wait_admin` values disagreed
///    (the earlier one's response now correctly reports the effective
///    value the runtime will see).
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
    pub reload_state: Arc<RuntimeReloadState>,
    pub config_path: PathBuf,
}

impl AdminState {
    // A builder would be tidier but is deferred until the next
    // signature change forces a refactor — the existing call sites
    // are few and each already lists every argument explicitly.
    //
    // The DHT introspection handles (issue #741) are attached via the
    // separate [`with_dht`](Self::with_dht) builder rather than as another
    // positional argument: `new` has ~15 call sites (mostly unit tests of
    // the cache/peer-table methods that don't need a DHT), and `with_dht`
    // lets the production runtime opt in without touching any of them.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        peer_table: Arc<RwLock<PeerTable>>,
        node_id: [u8; 32],
        started_at: Instant,
        cache: CacheEngine,
        announce_trigger: Option<Arc<AnnounceTrigger>>,
        reload_hook: Option<ReloadHook>,
        drain_trigger: Arc<DrainTrigger>,
        signer: Arc<PrivateKeySigner>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            peer_table,
            node_id,
            started_at,
            cache,
            announce_trigger,
            reload_hook,
            drain_trigger,
            signer,
            metrics,
            dht: None,
        }
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
}

/// Owned snapshot of one [`PeerEntry`]'s fields-of-interest, captured
/// under the read lock so the lock can be dropped before hex encoding
/// and final DTO assembly run. Hex-encoding the node id and allocating
/// the wire-format `node_id` string don't need to see live state, so
/// keeping them inside the locked region would block concurrent
/// announce-writers for no benefit.
struct RawPeer {
    node_id: [u8; 32],
    region: String,
    first_seen_us: u64,
    last_seen_us: u64,
    announced_at_us: u64,
}

impl RawPeer {
    fn from_entry(node_id: &[u8; 32], entry: &PeerEntry) -> Self {
        Self {
            node_id: *node_id,
            region: entry.announce.body.region.clone(),
            first_seen_us: entry.first_seen_us,
            last_seen_us: entry.last_seen_us,
            announced_at_us: entry.announce.body.timestamp_us,
        }
    }

    fn into_view(self) -> PeerView {
        PeerView {
            node_id: alloy::primitives::hex::encode(self.node_id),
            region: self.region,
            first_seen_us: self.first_seen_us,
            last_seen_us: self.last_seen_us,
            announced_at_us: self.announced_at_us,
        }
    }
}

/// Convert a [`CacheError`] into a JSON-RPC error suitable for
/// `admin_v1_evict`. Distinguished mainly so the operator-facing message
/// can name the underlying failure mode (`NotFound`, `OriginError`, etc.)
/// rather than a generic "cache failed".
fn cache_error_to_rpc(err: &CacheError) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(CACHE_ERROR_CODE, err.to_string(), None::<()>)
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

/// Concrete server implementation backed by the live gossip peer table.
#[derive(Debug, Clone)]
pub struct AdminRpcImpl {
    state: AdminState,
}

impl AdminRpcImpl {
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
        // `inspect` reads `BlobStatus` directly so the size we report
        // is the on-disk byte count, even when `already_evicted` is
        // already true — operators want to see disk-reclaim potential.
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
                .map_err(|err| cache_error_to_rpc(&err))?;
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

    async fn announce(&self) -> RpcResult<AnnounceResponse> {
        let trigger = self.state.announce_trigger.as_ref().ok_or_else(|| {
            ErrorObjectOwned::owned(
                PUBLISHER_DISABLED_CODE,
                "gossip publisher is disabled (no identity.region configured); \
                 set a region in the node config and restart to enable announces",
                None::<()>,
            )
        })?;
        trigger.announce_now();
        Ok(AnnounceResponse { triggered: true })
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
            rate_per_mb: snap.rate_per_mb,
            // After a successful reload `current_log_level` is `Some`;
            // the `unwrap_or` branch is the (unreachable in practice)
            // poisoned-mutex case where `current()` returned `None`.
            // Returning a stable string ("unknown") rather than the
            // empty string makes operator scripts that parse the
            // response trivially unambiguous.
            log_level: snap
                .log_level
                .map_or_else(|| "unknown".to_string(), |l| l.to_string()),
        })
    }

    async fn drain(&self, req: Option<DrainRequest>) -> RpcResult<DrainResponse> {
        // Normalize the optional wire param to the resolved-form
        // `DrainRequest` so the runtime logic operates on a single
        // canonical type. `None` (caller sent no `params`) is
        // indistinguishable from `Some(DrainRequest::default())` from
        // here on — both mean "SIGTERM-equivalent drain".
        let req = req.unwrap_or_default();
        // Atomic fire-with-wait_admin: returns the *effective* value
        // the runtime will see. First writer wins, so a concurrent
        // drain race can no longer produce a response that lies about
        // what the runtime will do (issue #604 review). The CLI reads
        // this ack to refuse polling against any server that didn't
        // honor `wait_admin` — older binary, future regression, or a
        // race where another caller's `wait_admin: false` won.
        let honored = self.state.drain_trigger.fire(req.wait_admin);
        Ok(DrainResponse {
            initiated: true,
            wait_admin_honored: honored,
        })
    }

    async fn peers_list(&self) -> RpcResult<PeersResponse> {
        // Two-pass snapshot: under the read lock we copy only the
        // owned data needed to build a PeerView (raw node_id bytes,
        // region clone, scalar fields). Hex encoding of node_id and
        // final DTO assembly run *after* the lock is released, along
        // with sorting. Lock hold time stays proportional to peer
        // count and to the per-entry data extraction, no further.
        let raw: Vec<RawPeer> = {
            let guard = self.state.peer_table.read().await;
            guard
                .iter()
                .map(|(id, entry)| RawPeer::from_entry(id, entry))
                .collect()
        };
        let mut snapshot: Vec<PeerView> = raw.into_iter().map(RawPeer::into_view).collect();
        // Most recently seen first — on-call use case is "is gossip alive?".
        snapshot.sort_by_key(|v| std::cmp::Reverse(v.last_seen_us));
        Ok(PeersResponse { peers: snapshot })
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
        })
    }
}

/// Bind the admin listener. Kept synchronous-at-startup so port
/// conflicts fail fast rather than deep inside the runtime task graph.
///
/// # Errors
/// Returns an error if `TcpListener::bind` fails.
pub async fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("admin bind {addr} failed: {e}"))?;
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
mod tests {
    use super::*;
    use decdn_cache::{Hash, Origin};
    use decdn_protocol::{NodeAnnounce, NodeAnnounceBody};

    fn mk_announce(node_id: [u8; 32], region: &str, ts_us: u64) -> NodeAnnounce {
        NodeAnnounce {
            body: NodeAnnounceBody {
                node_id,
                region: region.to_string(),
                timestamp_us: ts_us,
            },
            signature: vec![0u8; 64],
        }
    }

    /// Build a throwaway tempdir-backed cache for tests. The peers /
    /// health methods don't touch it, but `AdminState::new` requires one
    /// — wrapping the engine over a `tempfile::TempDir` keeps each test
    /// self-contained, and the returned `TempDir` must outlive the engine
    /// (callers bind it with `_tmp` so RAII handles cleanup at end of test).
    async fn test_cache() -> (CacheEngine, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = CacheEngine::open(tmp.path(), Vec::new(), 1)
            .await
            .expect("cache open");
        (cache, tmp)
    }

    /// Throwaway `PrivateKeySigner` for tests that need an `AdminState` but
    /// don't exercise any signing logic. The chain id is pinned to Arbitrum
    /// Sepolia to keep parity with the real runtime — tests that *do*
    /// exercise signing should construct their own signer with the chain id
    /// under test.
    fn throwaway_signer() -> Arc<PrivateKeySigner> {
        use alloy::signers::Signer;
        use decdn_incentive::eth_identity::ARBITRUM_SEPOLIA_CHAIN_ID;
        Arc::new(PrivateKeySigner::random().with_chain_id(Some(ARBITRUM_SEPOLIA_CHAIN_ID)))
    }

    async fn state_with(peers: Vec<([u8; 32], &str, u64, u64)>) -> (AdminState, tempfile::TempDir) {
        let mut table = PeerTable::new(0, 0);
        for (id, region, ts_us, now_us) in peers {
            table
                .insert_or_refresh(mk_announce(id, region, ts_us), now_us)
                .expect("seed insert succeeds");
        }
        let (cache, tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(table)),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        (state, tmp)
    }

    #[tokio::test]
    async fn empty_peer_table_returns_empty_vec() {
        let (state, _tmp) = state_with(vec![]).await;
        let rpc = AdminRpcImpl::new(state);
        let resp = rpc.peers_list().await.expect("peers_list ok");
        assert!(resp.peers.is_empty());
    }

    #[tokio::test]
    async fn single_peer_serializes_with_hex_node_id() {
        let id = [0xABu8; 32];
        // Seed once at now_us=100, then refresh at now_us=300 so
        // first_seen_us and last_seen_us differ. Distinct values catch a
        // field-swap regression (first↔last) that identical seeds would
        // not.
        let mut table = PeerTable::new(0, 0);
        table
            .insert_or_refresh(mk_announce(id, "US", 10), 100)
            .expect("seed insert");
        table
            .insert_or_refresh(mk_announce(id, "US", 20), 300)
            .expect("refresh insert");
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(table)),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc.peers_list().await.expect("peers_list ok");
        assert_eq!(resp.peers.len(), 1);
        let p = &resp.peers[0];
        assert_eq!(p.node_id, "ab".repeat(32));
        assert_eq!(p.region, "US");
        assert_eq!(p.first_seen_us, 100);
        assert_eq!(p.last_seen_us, 300);
        assert_eq!(p.announced_at_us, 20);
    }

    #[tokio::test]
    async fn health_returns_hex_node_id_and_nondecreasing_uptime() {
        let id = [0xCDu8; 32];
        let started = Instant::now();
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            id,
            started,
            cache,
            None,
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);

        let first = rpc.health().await.expect("health ok");
        assert_eq!(first.node_id, "cd".repeat(32));

        // Uptime is monotonic non-decreasing across calls — `Instant`
        // is monotonic, so a second call after at least one elapsed-tick
        // worth of work must report a value >= the first.
        let second = rpc.health().await.expect("health ok");
        assert!(
            second.uptime_s >= first.uptime_s,
            "uptime regressed: {} -> {}",
            first.uptime_s,
            second.uptime_s,
        );
    }

    #[tokio::test]
    async fn peers_sorted_by_last_seen_descending() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let c = [3u8; 32];
        // (id, region, announce ts, now_us) — `now_us` becomes last_seen_us.
        let (state, _tmp) = state_with(vec![
            (a, "US", 1, 500),
            (b, "EU", 1, 700),
            (c, "AP", 1, 600),
        ])
        .await;
        let rpc = AdminRpcImpl::new(state);
        let resp = rpc.peers_list().await.expect("peers_list ok");
        let order: Vec<u64> = resp.peers.iter().map(|p| p.last_seen_us).collect();
        assert_eq!(order, vec![700, 600, 500]);
    }

    /// Evict round-trip: `admin_v1_evict` of a hex hash that's been pulled
    /// into the cache returns `was_present: true` and subsequent gets fail
    /// with `NotFound`. Without this, a regression that no-op'd
    /// `admin_v1_evict` would leak through unit tests.
    #[tokio::test]
    async fn admin_evict_blocks_subsequent_serve() -> anyhow::Result<()> {
        use bytes::Bytes;
        use std::future::Future;
        use std::pin::Pin;

        // Inline stub origin so we don't have to depend on a test-only
        // `decdn-cache` export. Single-blob, hash matches payload.
        #[derive(Debug)]
        struct StubOrigin {
            data: Bytes,
            hash: Hash,
        }
        impl decdn_cache::Origin for StubOrigin {
            fn kind(&self) -> decdn_cache::OriginKind {
                decdn_cache::OriginKind::Http
            }

            fn fetch(
                &self,
                hash: Hash,
                _max_bytes: u64,
            ) -> Pin<
                Box<
                    dyn Future<
                            Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>,
                        > + Send
                        + '_,
                >,
            > {
                let result = if hash == self.hash {
                    Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
                } else {
                    Ok(decdn_cache::OriginFetch::NotFound)
                };
                Box::pin(async move { result })
            }
        }

        let payload = b"admin evict";
        let hash = Hash::new(payload);
        let tmp = tempfile::tempdir()?;
        let origin = Arc::new(StubOrigin {
            data: Bytes::from(payload.to_vec()),
            hash,
        }) as Arc<dyn decdn_cache::Origin>;
        let cache = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 1).await?;

        // Prime the cache with the blob so the evict has something to remove.
        let _ = cache.get(hash).await?;

        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            [0u8; 32],
            Instant::now(),
            cache.clone(),
            None,
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc
            .evict(EvictRequest {
                hash: alloy::primitives::hex::encode(hash.as_bytes()),
                dry_run: false,
            })
            .await
            .expect("evict ok");
        assert!(resp.was_present, "expected was_present=true");
        assert!(!resp.dry_run, "real evict must not set dry_run");

        match cache.get(hash).await {
            Err(CacheError::NotFound { .. }) => Ok(()),
            other => Err(anyhow::anyhow!(
                "expected NotFound after admin evict, got {other:?}"
            )),
        }
    }

    #[tokio::test]
    async fn admin_evict_rejects_bad_hex() {
        let (state, _tmp) = state_with(vec![]).await;
        let rpc = AdminRpcImpl::new(state);
        let err = rpc
            .evict(EvictRequest {
                hash: "not-hex".into(),
                dry_run: false,
            })
            .await
            .expect_err("expected invalid-params error");
        // INVALID_PARAMS_CODE; double-checked here so a typo'd code constant
        // still surfaces as a test failure.
        assert_eq!(err.code(), -32_602);
    }

    #[tokio::test]
    async fn admin_evict_accepts_uppercase_0x_prefix() {
        // `0X` and uppercase hex must both be tolerated; this guards
        // against the case-sensitive `strip_prefix("0x")` regression
        // flagged in PR review.
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);
        let hash = Hash::new(b"prefix-test");
        let upper = format!(
            "0X{}",
            alloy::primitives::hex::encode(hash.as_bytes()).to_uppercase()
        );
        let resp = rpc
            .evict(EvictRequest {
                hash: upper,
                dry_run: false,
            })
            .await
            .expect("0X-prefixed uppercase hex should parse");
        // was_present=false because the test cache has no origin and we
        // never `get`-ed the hash; the parse alone must succeed.
        assert!(!resp.was_present);
    }

    /// `admin_v1_evict { dry_run: true }` returns the pre-evict
    /// snapshot but does *not* mutate cache state — a follow-up `has`
    /// must still report the blob present, and a follow-up `get` must
    /// still serve. Without this assertion a regression that ignored
    /// the flag and ran the real `evict()` would silently slip through
    /// (the response shape is the same; the side-effect is what
    /// matters).
    #[tokio::test]
    async fn admin_evict_dry_run_does_not_mutate() -> anyhow::Result<()> {
        use bytes::Bytes;
        use std::future::Future;
        use std::pin::Pin;

        #[derive(Debug)]
        struct StubOrigin {
            data: Bytes,
            hash: Hash,
        }
        impl decdn_cache::Origin for StubOrigin {
            fn kind(&self) -> decdn_cache::OriginKind {
                decdn_cache::OriginKind::Http
            }

            fn fetch(
                &self,
                hash: Hash,
                _max_bytes: u64,
            ) -> Pin<
                Box<
                    dyn Future<
                            Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>,
                        > + Send
                        + '_,
                >,
            > {
                let result = if hash == self.hash {
                    Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
                } else {
                    Ok(decdn_cache::OriginFetch::NotFound)
                };
                Box::pin(async move { result })
            }
        }

        let payload = b"dry-run preview";
        let hash = Hash::new(payload);
        let tmp = tempfile::tempdir()?;
        let origin = Arc::new(StubOrigin {
            data: Bytes::from(payload.to_vec()),
            hash,
        }) as Arc<dyn decdn_cache::Origin>;
        let cache = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 1).await?;
        let _ = cache.get(hash).await?;

        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            [0u8; 32],
            Instant::now(),
            cache.clone(),
            None,
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc
            .evict(EvictRequest {
                hash: alloy::primitives::hex::encode(hash.as_bytes()),
                dry_run: true,
            })
            .await
            .expect("dry-run evict ok");

        // Wire shape — every dry-run-only field is meaningful.
        assert!(resp.dry_run, "expected dry_run=true on response");
        assert!(resp.was_present, "blob primed via get(); should be served");
        assert_eq!(
            resp.preview.size_bytes,
            Some(payload.len() as u64),
            "expected size_bytes={}, got {:?}",
            payload.len(),
            resp.preview.size_bytes,
        );
        assert!(
            resp.preview.last_accessed_us_ago.is_some(),
            "expected Some(last_accessed_us_ago) after get()"
        );
        assert!(!resp.preview.pinned);
        assert!(
            !resp.preview.already_evicted,
            "dry-run must not flip evicted flag"
        );
        // Origin egress-cost cue (#439, #284). Engine here is
        // configured with a single-origin chain, so the preview must
        // carry exactly one entry — the StubOrigin reports `Http`.
        assert_eq!(
            resp.preview.origin_kinds,
            vec![decdn_cache::OriginKind::Http],
            "expected [Http], got {:?}",
            resp.preview.origin_kinds,
        );

        // Cache state untouched: the blob is still served, the
        // evicted-log entry was not created, and no fsync hit disk.
        assert!(
            !cache.is_evicted(hash),
            "dry-run must not commit to evicted set"
        );
        assert!(cache.has(hash).await?, "dry-run must not stop serve");
        assert!(
            !tmp.path().join("evicted.log").exists(),
            "dry-run must not create evicted.log"
        );
        Ok(())
    }

    /// A real evict followed by a dry-run on the same hash must report
    /// `already_evicted: true` and `was_present: false` — the operator
    /// is using dry-run to confirm an idempotent re-run is in fact a
    /// no-op. The size field still reports the on-disk bytes since
    /// the iroh-blobs store hasn't been GC'd yet (#518).
    #[tokio::test]
    async fn admin_evict_dry_run_after_real_evict_reports_already_evicted() -> anyhow::Result<()> {
        use bytes::Bytes;
        use std::future::Future;
        use std::pin::Pin;

        #[derive(Debug)]
        struct StubOrigin {
            data: Bytes,
            hash: Hash,
        }
        impl decdn_cache::Origin for StubOrigin {
            fn kind(&self) -> decdn_cache::OriginKind {
                decdn_cache::OriginKind::Http
            }

            fn fetch(
                &self,
                hash: Hash,
                _max_bytes: u64,
            ) -> Pin<
                Box<
                    dyn Future<
                            Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>,
                        > + Send
                        + '_,
                >,
            > {
                let result = if hash == self.hash {
                    Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
                } else {
                    Ok(decdn_cache::OriginFetch::NotFound)
                };
                Box::pin(async move { result })
            }
        }

        let payload = b"already-evicted preview";
        let hash = Hash::new(payload);
        let tmp = tempfile::tempdir()?;
        let origin = Arc::new(StubOrigin {
            data: Bytes::from(payload.to_vec()),
            hash,
        }) as Arc<dyn decdn_cache::Origin>;
        let cache = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 1).await?;
        let _ = cache.get(hash).await?;
        cache.evict(hash)?;

        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc
            .evict(EvictRequest {
                hash: alloy::primitives::hex::encode(hash.as_bytes()),
                dry_run: true,
            })
            .await
            .expect("dry-run evict ok");

        assert!(resp.dry_run);
        assert!(
            !resp.was_present,
            "post-evict has() should report absent → was_present=false"
        );
        assert!(
            resp.preview.already_evicted,
            "expected already_evicted=true on a re-run"
        );
        // On-disk size still reported — operators want to see the
        // disk-reclaim potential even though `served=false`.
        assert_eq!(resp.preview.size_bytes, Some(payload.len() as u64));
        Ok(())
    }

    #[tokio::test]
    async fn admin_announce_without_publisher_returns_publisher_disabled() {
        // No region configured → AnnounceTrigger absent → method must
        // return PUBLISHER_DISABLED rather than silently succeeding.
        let (state, _tmp) = state_with(vec![]).await;
        let rpc = AdminRpcImpl::new(state);
        let err = rpc.announce().await.expect_err("expected error");
        assert_eq!(err.code(), -32_001);
    }

    #[tokio::test]
    async fn admin_announce_fires_trigger_when_publisher_present() {
        use tokio::sync::Notify;

        // `AnnounceTrigger::announce_now` is a thin wrapper that calls
        // `notify_one` on the inner `Arc<Notify>`. We construct the
        // trigger with a `Notify` we own (via the `for_test` seam, which
        // is `#[doc(hidden)]` and exists for exactly this assertion path)
        // so we can `.notified()` after the RPC fires and observe that
        // the permit landed. This proves the admin handler -> trigger ->
        // notify chain end-to-end without spinning up a real publisher
        // task; the publisher's own `select!` arm is exercised by
        // `service::tests::publisher_publishes_on_announce_trigger`.
        let notify = Arc::new(Notify::new());
        let trigger = Arc::new(decdn_gossip::AnnounceTrigger::for_test(Arc::clone(&notify)));
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            [0u8; 32],
            Instant::now(),
            cache,
            Some(trigger),
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc.announce().await.expect("announce ok");
        assert!(resp.triggered);

        // `Notify::notify_one` stores a permit if no waiter is pending;
        // calling `notified()` after the RPC claims that permit
        // immediately. A short timeout makes a regression that lost the
        // notify fail fast rather than hanging the test runner.
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), notify.notified()).await;
        assert!(
            waited.is_ok(),
            "announce_now did not fire the underlying Notify"
        );
    }

    /// `admin_v1_reload` on a node with no `ReloadHook` (started without
    /// `--config`) must surface `CONFIG_PATH_UNSET_CODE` rather than a
    /// generic failure, so an operator script can tell "no path on disk
    /// to re-read" from "reload tried and failed".
    #[tokio::test]
    async fn admin_reload_without_hook_returns_config_path_unset() {
        let (state, _tmp) = state_with(vec![]).await;
        let rpc = AdminRpcImpl::new(state);
        let err = rpc.reload().await.expect_err("expected error");
        assert_eq!(err.code(), -32_003);
    }

    /// Happy path: a hook pointing at a valid config file applies the
    /// reload via the same `RuntimeReloadState::reload` SIGHUP uses, and
    /// the response carries the post-reload `rate_per_mb` and
    /// `log_level`. Asserts both wire-format fields so a regression that
    /// dropped one (or stringified the level wrong) fails the unit test.
    #[tokio::test]
    async fn admin_reload_applies_and_returns_post_reload_values() {
        use crate::runtime::RuntimeReloadState;
        use decdn_common::cli::common::LogLevel;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("node.toml");
        std::fs::write(
            &path,
            "[payment]\nrate_per_mb = 99\n\n[observability]\nlog_level = \"debug\"\n",
        )
        .expect("write config");

        let setter: crate::runtime::LogLevelSetter = Box::new(|_| Ok(()));
        let reload_state = Arc::new(RuntimeReloadState::for_test_with_setter(
            10,
            LogLevel::Info,
            setter,
        ));
        let hook = ReloadHook {
            reload_state: Arc::clone(&reload_state),
            config_path: path.clone(),
        };
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            Some(hook),
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc.reload().await.expect("reload ok");
        assert_eq!(resp.rate_per_mb, 99);
        assert_eq!(resp.log_level, "debug");
    }

    /// `DrainTrigger::fire` followed by `wait()` resolves. The Notify
    /// stores a permit when no waiter is present, so the `notified()`
    /// future claims it immediately — no race window or ordering
    /// requirement between `fire` and `wait` in tests.
    #[tokio::test]
    async fn drain_trigger_fire_then_wait_resolves() {
        let trigger = DrainTrigger::new();
        let honored = trigger.fire(false);
        assert!(!honored, "first writer's value is the effective one");
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
        assert!(waited.is_ok(), "wait() did not resolve after fire()");
    }

    /// Firing repeatedly must not deadlock a subsequent `wait`. `Notify`
    /// coalesces multiple `notify_one` calls into a single permit, so the
    /// second and third `fire` while no waiter is pending are no-ops and
    /// the *first* permit is still available for the next `wait`. Catches
    /// a future reimplementation that internally tracks "fired" state and
    /// burns one permit per call (e.g. a hand-rolled `Mutex<bool>` that
    /// returns a never-resolving future on the second call). The third
    /// fire makes the test robust against a "burns one permit per fire"
    /// bug — two would still resolve under that buggy implementation if
    /// the first fire stored a permit and the second consumed it before
    /// `wait` was polled.
    #[tokio::test]
    async fn drain_trigger_repeated_fire_does_not_deadlock_wait() {
        let trigger = DrainTrigger::new();
        let h1 = trigger.fire(false);
        let h2 = trigger.fire(false); // second fire — coalesces, permit still available
        let h3 = trigger.fire(false); // third fire — same coalesce; defends against the
        // "burns one permit per fire" regression class
        assert_eq!(
            (h1, h2, h3),
            (false, false, false),
            "all three fires must report the same effective value"
        );
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
        assert!(
            waited.is_ok(),
            "wait() did not resolve after three fire() calls"
        );
    }

    /// First-writer-wins on `wait_admin` (issue #604 review): two
    /// races on the same trigger must not produce an ack that lies
    /// about what the runtime will see. The first `fire(true)` stores
    /// `true`; a second `fire(false)` must return `true` (the
    /// effective value the runtime will read), not its own argument.
    #[tokio::test]
    async fn drain_trigger_fire_is_first_writer_wins() {
        let trigger = DrainTrigger::new();
        let first = trigger.fire(true);
        let second = trigger.fire(false);
        assert!(first, "first fire reports its own value");
        assert!(
            second,
            "second fire reports the prior writer's value, not its own"
        );
        assert!(
            trigger.wait_admin(),
            "runtime reader sees the first writer's value"
        );
    }

    /// `admin_v1_drain` fires the trigger and returns `initiated: true`.
    /// Mirrors the `admin_announce_fires_trigger_when_publisher_present`
    /// test — we verify the RPC handler -> trigger -> Notify chain end-to-end
    /// without spinning up a full runtime.
    #[tokio::test]
    async fn admin_drain_fires_trigger_and_returns_initiated() {
        let trigger = Arc::new(DrainTrigger::new());
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            None,
            Arc::clone(&trigger),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc
            .drain(Some(DrainRequest::default()))
            .await
            .expect("drain ok");
        assert!(resp.initiated, "expected initiated=true");
        // Default DrainRequest leaves `wait_admin` at false, matching
        // SIGTERM-equivalent ordering. `wait_admin_honored` therefore
        // mirrors the request and is also `false` — the CLI uses this
        // to refuse polling against a server that didn't opt in.
        assert!(
            !trigger.wait_admin(),
            "default DrainRequest must not enable wait_admin"
        );
        assert!(
            !resp.wait_admin_honored,
            "default DrainRequest must report wait_admin_honored=false"
        );

        // Verify the trigger actually fired: `wait()` should resolve
        // immediately because the Notify stored a permit.
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
        assert!(
            waited.is_ok(),
            "drain RPC did not fire the underlying DrainTrigger"
        );
    }

    /// `admin_v1_drain` with `wait_admin: true` (issue #604)
    /// establishes the trigger's effective value atomically via
    /// `fire(true)` so the runtime's reader sees it after `wait()`
    /// resolves. Cross-thread visibility is via the
    /// `Notify::notify_one → notified()` happens-before edge.
    #[tokio::test]
    async fn admin_drain_with_wait_admin_sets_flag_before_fire() {
        let trigger = Arc::new(DrainTrigger::new());
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            None,
            Arc::clone(&trigger),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);

        // Pre-condition: trigger starts with wait_admin=false.
        assert!(
            !trigger.wait_admin(),
            "trigger must start with wait_admin=false"
        );

        let resp = rpc
            .drain(Some(DrainRequest { wait_admin: true }))
            .await
            .expect("drain ok");
        assert!(resp.initiated, "expected initiated=true");
        assert!(
            trigger.wait_admin(),
            "wait_admin=true request must set the trigger flag"
        );
        // Server reports the ack so the CLI can refuse to poll when a
        // server doesn't honor `--wait`.
        assert!(
            resp.wait_admin_honored,
            "wait_admin=true request must report wait_admin_honored=true"
        );

        // The fire must still happen — runtime needs to wake up either way.
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
        assert!(
            waited.is_ok(),
            "drain RPC with wait_admin=true must still fire the trigger"
        );
    }

    /// Regression for the wire-compat hole the reviewers flagged
    /// (#662): when an older client (or a curl/python script) calls
    /// `admin_v1_drain` with no `params` field, the RPC must still
    /// trigger drain and return the default response. Per
    /// `crates/common/src/admin.rs` the trait declares `req:
    /// Option<DrainRequest>`, so jsonrpsee's proc-macro uses
    /// `optional_next()` and decodes a missing parameter to `None`;
    /// the impl normalizes to `DrainRequest::default()`. This test
    /// asserts that the `None` path produces the same observable
    /// effects as `Some(DrainRequest::default())`.
    #[tokio::test]
    async fn admin_drain_with_no_params_still_triggers() {
        let trigger = Arc::new(DrainTrigger::new());
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            None,
            Arc::clone(&trigger),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc.drain(None).await.expect("drain ok");
        assert!(resp.initiated, "expected initiated=true");
        assert!(
            !resp.wait_admin_honored,
            "no-params drain must report wait_admin_honored=false"
        );
        assert!(
            !trigger.wait_admin(),
            "no-params drain must keep wait_admin=false"
        );

        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
        assert!(
            waited.is_ok(),
            "no-params drain must still fire the underlying trigger"
        );
    }

    /// `admin_v1_health.in_flight_streams` reflects the live
    /// `dispatch_in_flight` gauge value (issue #604). Without this
    /// wiring, `decdn node drain --wait` would loop forever — the
    /// polling client sees a constant 0 regardless of the actual
    /// in-flight handler count.
    #[tokio::test]
    async fn health_reports_in_flight_streams_from_dispatch_gauge() {
        use decdn_common::config::ResolvedSecurity;

        let trigger = Arc::new(DrainTrigger::new());
        let (cache, _tmp) = test_cache().await;
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            None,
            Arc::clone(&trigger),
            throwaway_signer(),
            Arc::clone(&metrics),
        );
        let rpc = AdminRpcImpl::new(state);

        // Baseline: no permits held, gauge reads 0.
        let h = rpc.health().await.expect("health ok");
        assert_eq!(h.in_flight_streams, 0, "baseline must be 0");

        // Acquire a permit via the limiter; the gauge increments.
        // Use a permissive resolved-security so neither layer rejects.
        let limiter = crate::dispatch::ConnectionLimiter::new(
            &ResolvedSecurity {
                max_concurrent_handlers: u32::MAX,
                per_source_rate_per_sec: 1e9,
                per_source_burst: u32::MAX,
                max_tracked_sources: 16,
            },
            Arc::clone(&metrics),
        );
        let permit = limiter
            .acquire_for_test(Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)))
            .expect("acquire permit");

        let h = rpc.health().await.expect("health ok");
        assert_eq!(
            h.in_flight_streams, 1,
            "expected gauge=1 while one permit is held"
        );

        // Dropping the permit decrements the gauge.
        drop(permit);
        let h = rpc.health().await.expect("health ok");
        assert_eq!(
            h.in_flight_streams, 0,
            "expected gauge=0 after dropping the permit"
        );
    }

    /// Failure path: a hook pointing at a missing file must surface
    /// `RELOAD_ERROR_CODE` and the underlying reload state must keep its
    /// previous values (the SIGHUP arm's "previous values retained"
    /// contract — we route through the same code, so this is a check
    /// that the RPC layer didn't accidentally swap an `Ok(...)` somewhere
    /// in the error mapping).
    #[tokio::test]
    async fn admin_reload_with_missing_config_returns_reload_error() {
        use crate::runtime::RuntimeReloadState;
        use decdn_common::cli::common::LogLevel;

        let dir = tempfile::tempdir().expect("tempdir");
        // Path inside a tempdir that we never write to — guaranteed
        // missing without depending on filesystem state outside the test.
        let path = dir.path().join("does-not-exist.toml");

        let setter: crate::runtime::LogLevelSetter = Box::new(|_| Ok(()));
        let reload_state = Arc::new(RuntimeReloadState::for_test_with_setter(
            42,
            LogLevel::Info,
            setter,
        ));
        let hook = ReloadHook {
            reload_state: Arc::clone(&reload_state),
            config_path: path,
        };
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            Some(hook),
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
            Arc::new(crate::metrics::Metrics::new()),
        );
        let rpc = AdminRpcImpl::new(state);

        let err = rpc.reload().await.expect_err("expected error");
        assert_eq!(err.code(), -32_004);
        // Previous rate retained — RPC error path didn't accidentally
        // commit anything to the live state.
        assert_eq!(
            reload_state
                .rate_per_mb()
                .load(std::sync::atomic::Ordering::Relaxed),
            42,
        );
    }

    /// Build a `DhtStatusHandles` seeded with two peers in distinct
    /// buckets, a fixed staker set, one provider record, one scheduled
    /// republish, and a non-zero refresh clock — enough to exercise every
    /// field of `StatusResponse`.
    fn seeded_dht_handles() -> DhtStatusHandles {
        use crate::dht::routing::NodeId;
        use crate::dht::{ConfigStakerSet, RecordStore, RecordStoreConfig, RepublishScheduler};
        use decdn_protocol::ContentHash;

        // self_id = all-zero; p_high lands in bucket 255 (top bit set),
        // p_low in bucket 0 (only the lowest bit differs).
        let self_id = NodeId::from_bytes([0u8; 32]);
        let mut table = RoutingTable::new(self_id);
        let mut high = [0u8; 32];
        high[0] = 0x80;
        let mut low = [0u8; 32];
        low[31] = 0x01;
        assert!(table.insert(NodeId::from_bytes(high)));
        assert!(table.insert(NodeId::from_bytes(low)));

        let mut stakers = std::collections::HashSet::new();
        stakers.insert(NodeId::from_bytes([1u8; 32]));
        stakers.insert(NodeId::from_bytes([2u8; 32]));
        let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(stakers));

        let mut store = RecordStore::new(RecordStoreConfig::default());
        store.insert_at(
            NodeId::from_bytes([5u8; 32]),
            ContentHash::from_bytes([7u8; 32]),
            1_000,
        );

        let republish = Arc::new(RepublishScheduler::new());
        republish.schedule_steady(ContentHash::from_bytes([9u8; 32]));

        DhtStatusHandles {
            routing: Arc::new(StdMutex::new(table)),
            staker_set,
            record_store: Arc::new(StdMutex::new(store)),
            republish,
            refresh_clock: Arc::new(AtomicU64::new(1_700_000_000_000_000)),
            refresh_interval: Duration::from_hours(1),
        }
    }

    #[tokio::test]
    async fn status_reports_routing_and_dht_health() {
        let (state, _tmp) = state_with(vec![]).await;
        let state = state.with_dht(seeded_dht_handles());
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc.status().await.expect("status ok");

        assert_eq!(resp.routing.total_peers, 2);
        assert_eq!(resp.routing.non_empty_buckets, 2);
        // Buckets are reported ascending by index: bucket 0 then bucket 255.
        let indices: Vec<u16> = resp.routing.buckets.iter().map(|b| b.index).collect();
        assert_eq!(indices, vec![0, 255]);
        for b in &resp.routing.buckets {
            assert_eq!(b.fill, 1);
        }
        assert_eq!(resp.routing.bucket_capacity, 20);
        assert_eq!(resp.routing.refresh_interval_s, 3_600);
        assert_eq!(resp.routing.last_refresh_us, Some(1_700_000_000_000_000));
        assert_eq!(resp.known_stakers, 2);
        assert_eq!(resp.record_store.records, 1);
        assert_eq!(resp.record_store.capacity, 100_000);
        assert_eq!(resp.republish.scheduled_records, 1);
    }

    /// A zero refresh clock (no bucket-refresh pass has completed yet)
    /// must surface as `last_refresh_us: None`, not `Some(0)`.
    #[tokio::test]
    async fn status_never_refreshed_reports_none() {
        let (state, _tmp) = state_with(vec![]).await;
        let mut handles = seeded_dht_handles();
        handles.refresh_clock = Arc::new(AtomicU64::new(0));
        let rpc = AdminRpcImpl::new(state.with_dht(handles));

        let resp = rpc.status().await.expect("status ok");
        assert_eq!(resp.routing.last_refresh_us, None);
    }

    /// Without DHT handles attached (the `new`-only construction the other
    /// admin tests use), `status` returns [`DHT_UNAVAILABLE_CODE`] rather
    /// than panicking or returning a misleading empty snapshot.
    #[tokio::test]
    async fn status_without_dht_returns_unavailable_error() {
        let (state, _tmp) = state_with(vec![]).await;
        let rpc = AdminRpcImpl::new(state);

        let err = rpc
            .status()
            .await
            .expect_err("expected DHT-unavailable error");
        assert_eq!(err.code(), DHT_UNAVAILABLE_CODE);
    }

    /// A poisoned routing-table mutex (a writer panicked while holding it)
    /// must surface as the distinct [`DHT_POISONED_CODE`], never panic and
    /// never be conflated with the benign "no DHT wired" case. Exercises
    /// the `lock_or_rpc_err` anti-panic net.
    #[tokio::test]
    async fn status_poisoned_routing_mutex_returns_poisoned_error() {
        let (state, _tmp) = state_with(vec![]).await;
        let handles = seeded_dht_handles();
        // Poison the routing-table mutex by panicking while holding it.
        let routing = Arc::clone(&handles.routing);
        std::thread::spawn(move || {
            let _guard = routing.lock();
            panic!("intentional poison");
        })
        .join()
        .expect_err("the spawned thread must panic to poison the lock");

        let rpc = AdminRpcImpl::new(state.with_dht(handles));
        let err = rpc.status().await.expect_err("expected DHT-poisoned error");
        assert_eq!(err.code(), DHT_POISONED_CODE);
    }
}
