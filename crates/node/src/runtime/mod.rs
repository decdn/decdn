//! Node runtime: owns the iroh endpoint, metrics server, and protocol router.

pub mod eviction;
pub mod reload;

pub use reload::{LogLevelSetter, ReloadSnapshot, RuntimeReloadState};

use crate::chain_events::shared_head::{HeadSource, SharedHead};

use std::net::{Ipv4Addr, SocketAddrV4};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use decdn_cache::{
    CacheEngine, FilesystemOrigin, HttpOrigin, Origin, S3Credentials, S3Origin, S3OriginConfig,
};
use decdn_common::config::{ResolvedDiscovery, ResolvedOrigin, ResolvedS3Credentials};
use iroh::address_lookup::{DnsAddressLookup, MemoryLookup, PkarrPublisher};
use iroh::endpoint::{IdleTimeout, QuicTransportConfig, VarInt, presets};
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayMap, RelayMode, RelayUrl, SecretKey};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::admin;
use crate::channel_store::PersistentPoolStateStore;
use crate::dht::{DhtRateLimiter, RecordStore, RecordStoreConfig, StakerSet};
use crate::dispatch::ConnectionLimiter;
use crate::handlers::client::{ClientHandler, MAX_CLIENT_STREAMS};
use crate::handlers::dht::DhtHandler;
use crate::handlers::probe::{ProbeHandler, StakeLanePolicy as ProbeStakeLanePolicy};
use crate::handlers::probe_rate_limit::ProbeRateLimiter;
use crate::metrics;
use crate::payment_settlement::PoolSettlementService;
use alloy::network::EthereumWallet;
use alloy::primitives::U256;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use decdn_common::address::parse_nonzero_address;
use decdn_common::config::ResolvedConfig;
use decdn_common::identity;
use decdn_common::redact::{redact_userinfo, sanitize_rpc_display};
use decdn_incentive::PoolStateStore;
use decdn_incentive::eth_identity::{self, PasswordSource};

/// Ceiling on how long we wait for spawned tasks to drain after the endpoint
/// and metrics server have been signalled to stop. Sized comfortably larger
/// than the sum of the probe handler's accept + close timeouts (see
/// `handlers::probe::ACCEPT_BI_TIMEOUT` + `PROBE_READ_TIMEOUT` +
/// `PROBE_CLOSE_TIMEOUT`) so in-flight handlers finish naturally; the
/// `abort_all` branch only fires as a safety net.
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(15);

/// Period between dispatch per-source rate-limiter GC sweeps (#440).
/// The acquire path already prunes opportunistically when the keyspace
/// exceeds `cap + cap/10`, but a node whose connection rate falls below
/// the over-cap threshold can carry millions of stale buckets
/// indefinitely. 60s is comfortably larger than the longest realistic
/// bucket refill window, so the sweep is essentially free when the
/// keyspace is empty.
const DISPATCH_GC_INTERVAL: Duration = Duration::from_mins(1);

/// Interval between periodic GC sweeps of the DHT rate-limiter's per-IP
/// and per-peer keyed maps (#645). 60s matches `DISPATCH_GC_INTERVAL` —
/// the two limiters share the same operator mental model for keyspace
/// cleanup cadence. Separate constant (rather than reusing
/// `DISPATCH_GC_INTERVAL`) so a future tune to one limiter doesn't drag
/// the other along.
const DHT_RATE_LIMIT_GC_INTERVAL: Duration = Duration::from_mins(1);

/// Interval between periodic GC sweeps of the probe rate-limiter's per-IP and
/// per-peer keyed maps (#645, #982). Matches `DHT_RATE_LIMIT_GC_INTERVAL` — the
/// probe and DHT keyed limiters share the same keyspace-cleanup mental model.
/// Separate constant so a future tune to one limiter doesn't drag the other.
const PROBE_RATE_LIMIT_GC_INTERVAL: Duration = Duration::from_mins(1);

/// QUIC-level idle timeout: the transport closes a connection if no
/// packets arrive for this long. Set to match ADR 005's 30s
/// connection-lifetime ceiling.
///
/// Because [`QUIC_KEEP_ALIVE_INTERVAL`] (10s) is shorter than this
/// timeout, healthy peers keep refreshing it via PING ACKs and the QUIC
/// idle reaper rarely fires on its own — that is the spec's intent (see
/// ADR 005: "below the idle timeout to prevent NAT middleboxes from
/// dropping the mapping"). The QUIC timer is a defense-in-depth floor
/// for genuinely silent paths (e.g. peer crash / network partition); the
/// "close 30s after last stream and no unacked vouchers" rule is
/// application-layer and lives in the cdn/client/v1 handler's serve loop
/// (`handlers::client::APP_IDLE_TIMEOUT`), which reaps keep-alive'd-but-
/// streamless connections the transport timer cannot.
const QUIC_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval between QUIC PING keep-alive frames. Per ADR 005 §Connection
/// lifetime, endpoints SHOULD send PINGs at 10s intervals — strictly
/// less than [`QUIC_MAX_IDLE_TIMEOUT`] so a single dropped probe doesn't
/// trip the idle reaper, and well below the typical NAT mapping timeout
/// so middleboxes don't drop the path under quiet load. Quinn applies
/// this on every connection regardless of role, so this endpoint emits
/// keep-alives on both client- and server-initiated connections.
const QUIC_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Maximum number of concurrent bidirectional streams a peer may open on
/// a single connection. Sized for `cdn/client/v1` (the largest cap in
/// ADR 005 §Concurrent stream limits: client=100, probe=1, dht=1).
/// `QuicTransportConfig` is per-endpoint, not per-ALPN, so the QUIC
/// layer can only enforce the union of these caps; tighter per-ALPN
/// limits are enforced inside each handler. The probe handler already
/// does this implicitly by calling `accept_bi()` exactly once per
/// connection and then closing.
const QUIC_MAX_CONCURRENT_BIDI_STREAMS: u32 = 100;

/// Apply an explicit client poll interval to a freshly built provider,
/// overriding alloy's localhost-detected 250 ms default (#1011).
///
/// Despite the config knob's name, this does not touch event watching: the
/// multiplexed poller ticks on its own `poll_interval` and never reads the
/// client interval. The one consumer still reachable from this node is
/// `PendingTransactionBuilder::get_receipt`'s heartbeat (alloy-provider
/// `heart.rs`), which the node awaits in `payment_settlement` and
/// `buyer_channel` — so this bounds how fast a node awaiting a mined settlement /
/// channel tx re-polls for its receipt, and alloy's 250 ms localhost default
/// would otherwise hammer a dev anvil for the life of every pending tx.
///
/// `set_poll_interval` uses interior mutability, so this applies to the
/// already-constructed provider and returns it unchanged in type —
/// wallet/nonce-filler providers route through it too, because `client()` is a
/// default `Provider` trait method available on every provider. The interval
/// comes from `blockchain.event_poll_interval_ms`.
fn with_poll_interval<P: Provider>(provider: P, interval: Duration) -> P {
    provider.client().set_poll_interval(interval);
    provider
}

/// Keep every iroh ALPN listener closed until the blacklist watcher reports one
/// clean full replay + operator-scope pass. The closure is the explicit seam
/// that makes it impossible to construct the `Router` on either pending or
/// failed readiness.
async fn gate_listener_on_blacklist_sync<T>(
    readiness: oneshot::Receiver<crate::blacklist_watcher::InitialSyncResult>,
    start_listener: impl FnOnce() -> T,
) -> anyhow::Result<T> {
    readiness
        .await
        .context("blacklist watcher exited before initial sync completed")?
        .map_err(anyhow::Error::msg)
        .context("initial ContentBlacklist sync unavailable")?;
    Ok(start_listener())
}

type HttpUrl = alloy::transports::http::reqwest::Url;

/// Construct the four HTTP-provider roles owned by the node runtime (#1252).
///
/// Keeping these constructors together makes the pending-transaction poll
/// interval and wallet nonce policy single-sourced while preserving separate
/// provider instances for every consumer. The shared-head provider is the one
/// deliberate exception to the poll override: it never creates pending
/// transactions and therefore keeps Alloy's transport default.
struct ProviderFactory;

impl ProviderFactory {
    fn read_only(url: HttpUrl, interval: Duration) -> impl Provider + Clone {
        with_poll_interval(ProviderBuilder::new().connect_http(url), interval)
    }

    fn shared_head(url: HttpUrl) -> impl Provider + Clone {
        ProviderBuilder::new().connect_http(url)
    }

    fn seller_wallet(
        url: HttpUrl,
        signer: PrivateKeySigner,
        interval: Duration,
    ) -> impl Provider + Clone {
        Self::wallet(url, signer, interval)
    }

    fn buyer_wallet(
        url: HttpUrl,
        signer: PrivateKeySigner,
        interval: Duration,
    ) -> impl Provider + Clone {
        Self::wallet(url, signer, interval)
    }

    fn wallet(url: HttpUrl, signer: PrivateKeySigner, interval: Duration) -> impl Provider + Clone {
        with_poll_interval(
            ProviderBuilder::new()
                .with_simple_nonce_management()
                .wallet(EthereumWallet::from(signer))
                .connect_http(url),
            interval,
        )
    }
}

/// Runtime [`QuicTransportConfig`]. Tests build their own config via
/// the same builder when they need to shorten the idle timeout to keep
/// the test runtime under a second.
fn quic_transport_config() -> anyhow::Result<QuicTransportConfig> {
    let idle_timeout: IdleTimeout = QUIC_MAX_IDLE_TIMEOUT.try_into().map_err(|e| {
        anyhow::anyhow!(
            "BUG: QUIC_MAX_IDLE_TIMEOUT={QUIC_MAX_IDLE_TIMEOUT:?} not representable as IdleTimeout: {e}"
        )
    })?;
    Ok(QuicTransportConfig::builder()
        .max_idle_timeout(Some(idle_timeout))
        .keep_alive_interval(QUIC_KEEP_ALIVE_INTERVAL)
        .max_concurrent_bidi_streams(VarInt::from_u32(QUIC_MAX_CONCURRENT_BIDI_STREAMS))
        .build())
}

/// Spawn a periodic task onto `tasks`: burn the first tick, then run `tick`
/// every `interval` until the returned stop signal is sent (or its sender is
/// dropped). Every tick body here is synchronous, so `tick` is a plain
/// `FnMut()` run to completion inside the task.
///
/// The `biased; stop-before-tick` select keeps shutdown prompt: the stop arm is
/// polled first, so a signalled stop returns without waiting out the current
/// `interval`. A stop arm that failed to return would instead stall shutdown by
/// up to one full `interval`, which the runtime's normal path would mask as a
/// "task slow to drain" rather than a bug. `spawn_periodic_exits_promptly_on_shutdown`
/// pins that a signalled stop returns promptly and that the first tick is
/// burned; `spawn_periodic_fires_the_tick_body` pins that ticks otherwise run.
///
/// The first tick is burned so the first pass lands one interval after startup
/// rather than on the same tick as bring-up. `name` labels the shutdown log.
fn spawn_periodic<F>(
    tasks: &mut JoinSet<()>,
    name: &'static str,
    interval: Duration,
    mut tick: F,
) -> oneshot::Sender<()>
where
    F: FnMut() + Send + 'static,
{
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    tasks.spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = &mut stop_rx => {
                    tracing::debug!(task = name, "periodic task shutdown signal received");
                    return;
                }
                _ = ticker.tick() => tick(),
            }
        }
    });
    stop_tx
}

/// Warn when the pin set cannot fit under the cache ceiling (#1130).
///
/// Pinned hashes are LRU-exempt (`eviction_candidates` filters them out), so a
/// pin set larger than `cache.cache_size_mb` makes the eviction driver's
/// high-water mark permanently unreachable: it will starve every tick while the
/// disk keeps growing. The hazard applies to every node however the pinned
/// content arrived, so this runs unconditionally at startup and on reload.
///
/// Warn rather than fail or truncate. The operator's pin set is a deliberate
/// statement about what this node must serve, and silently warming only part of
/// it would be worse than warming all of it noisily. Sizes come from the
/// origin-held index, so a hash the index cannot size contributes nothing and
/// the total is a **lower bound**. That matters: an origin that blocks `HEAD`
/// leaves the index empty and this check silently never fires. It is a cheap
/// early hint, not the authoritative signal — that is
/// `decdn_cache_evictions_starved_total` with `decdn_cache_bytes` at
/// `decdn_cache_size_limit_bytes`.
fn warn_if_pins_exceed_cache(cache: &decdn_cache::CacheEngine, cache_size_mb: u64) {
    let pinned = cache.pinned_snapshot();
    let pinned_bytes: u64 = pinned
        .iter()
        .filter_map(|h| cache.origin_held_size(decdn_cache::Hash::from_bytes(*h.as_bytes())))
        .fold(0u64, u64::saturating_add);
    let limit_bytes = cache_size_mb.saturating_mul(1024 * 1024);
    if pinned_bytes > limit_bytes {
        tracing::warn!(
            pinned_count = pinned.len(),
            pinned_bytes,
            cache_size_mb,
            limit_bytes,
            "cache.pinned_hashes is larger than cache.cache_size_mb; pinned blobs are \
             LRU-exempt, so the eviction driver can never reach its high-water target and \
             disk use will exceed the configured ceiling. Raise cache_size_mb or trim the \
             pin set (#1130)"
        );
    }
}

/// Emit one `debug` line for a keyed rate-limiter GC sweep that pruned buckets.
/// Shared by the DHT and probe rate-limiter GC ticks, whose per-layer
/// (`per_ip` / `per_peer`) log arms are otherwise byte-identical.
fn log_keyspace_gc(kind: &'static str, layer: &'static str, sweep: Option<(usize, usize)>) {
    if let Some((before, after)) = sweep {
        tracing::debug!(
            layer,
            before,
            after,
            dropped = before.saturating_sub(after),
            "{kind} rate-limit GC sweep complete"
        );
    }
}

/// Runtime infrastructure built during the front bring-up phase of [`run`].
///
/// A plain by-value bundle of the long-lived handles the rest of `run` (and the
/// shutdown sequence) consumes. `run` destructures it immediately so every
/// downstream call site keeps referring to the same-named locals.
struct Infra {
    node_metrics: Arc<metrics::Metrics>,
    secret_key: SecretKey,
    eth_signer: Arc<PrivateKeySigner>,
    concrete_channel_store: Arc<PersistentPoolStateStore>,
    channel_state_store: Arc<dyn PoolStateStore>,
    watcher_checkpoint_store: Arc<dyn decdn_incentive::KeyedCheckpointStore>,
    receipt_writer_shutdown: CancellationToken,
    receipt_sink: Arc<dyn crate::receipt_log::ReceiptSink>,
    receipt_writer: tokio::task::JoinHandle<()>,
    /// Periodic lane-store flush timer (ADR 003 §Off-chain voucher state
    /// persistence). Aborted after one final durable flush on shutdown.
    lane_flush_task: tokio::task::JoinHandle<()>,
    node_origin: Option<crate::node_origin::NodeOrigin>,
    pull_through_origin: Option<Arc<crate::node_origin::NodeOrigin>>,
    cache: CacheEngine,
    /// Cache eviction policy selected by `cache.eviction_policy` (ADR 040).
    /// Injected into the eviction driver at spawn time
    /// ([`spawn_background_tasks`]); the engine itself never chooses a policy.
    eviction_policy: Arc<dyn decdn_cache::EvictionPolicy>,
    ep: Endpoint,
    limiter: Arc<ConnectionLimiter>,
}

/// Front bring-up phase: RPC preflight, identity/keystore load, voucher-state
/// and receipt stores, cache + origin chain, iroh endpoint, and the
/// connection limiter. Extracted verbatim from [`run`]; the two `reload_state`
/// attach side effects stay inline at their original positions so a SIGHUP
/// delivered mid-bring-up still finds a target.
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
async fn build_infra(
    cfg: &ResolvedConfig,
    reload_state: &RuntimeReloadState,
) -> anyhow::Result<Infra> {
    // Preflight: verify RPC endpoint is reachable before committing to
    // port binding. A 5-second timeout keeps startup responsive on flaky
    // networks while still catching typos and dead endpoints early.
    check_rpc_reachability(&cfg.blockchain.rpc_url).await?;

    let node_metrics = Arc::new(metrics::Metrics::new());
    // Registry-mandatory `decdn_probe_hold_slots_max` (ADR
    // appendix-observability.md) — static, set once from config.
    node_metrics.probe_hold_slots_max(cfg.cache.max_probe_holds);

    let secret_key = identity::load_or_generate(&cfg.identity.data_dir)?;
    tracing::info!(node_id = %secret_key.public(), "loaded node identity");

    // Issue #406: load the Ethereum keystore into a live `PrivateKeySigner`
    // before the rest of startup so any password-source error (env unset,
    // missing file, prompt aborted) fails fast with a clear message rather
    // than after the cache + endpoint have been built. `decrypt_keystore`
    // runs scrypt/argon2 (hundreds of milliseconds to multi-second under
    // hardened KDF params), so it must run on a blocking thread to avoid
    // stalling the tokio runtime.
    let eth_signer = Arc::new(load_eth_signer(cfg).await?);
    tracing::info!(address = %eth_signer.address(), "loaded eth keystore");

    // Open the off-chain voucher-state store (issue #527, ADR 003
    // §Off-chain voucher state persistence) before any handler that could
    // accept a voucher comes online. `PersistentPoolStateStore::open`
    // performs disk I/O (file create + mode tighten + redb header read), so
    // run it on a blocking thread to avoid stalling the tokio runtime.
    // Failure here MUST abort startup: continuing with a fresh in-memory
    // map silently reopens the replay window the store exists to close.
    let channel_store_data_dir = cfg.identity.data_dir.clone();
    // Keep the concrete store `Arc` so it can back the seller
    // `ChannelStateStore` (channel_state_v1 table), the pending-settle store
    // (pending_settle_v1 table, PR #743 review), and the buyer
    // `BuyerChannelStore` (buyer_channel_state_v2 table, #744) — redb forbids a
    // second `Database` handle to the same file, so one shared store owns all.
    let concrete_channel_store: Arc<PersistentPoolStateStore> = Arc::new(
        tokio::task::spawn_blocking(move || {
            PersistentPoolStateStore::open(&channel_store_data_dir)
        })
        .await
        .context("channel state store open task panicked")?
        .context("failed to open channel state store (issue #527 voucher replay guard)")?,
    );
    // The one redb-backed store implements the voucher-state trait (for the
    // handler + #527 replay guard), the pending-settle trait (for the on-chain
    // settlement sweep, PR #743 review), and the buyer-channel trait (#744).
    // Derive trait-object handles from the single concrete store so all tables
    // share one open file and one fsync discipline; `concrete_channel_store`
    // stays bound for the buyer handle built further below.
    let channel_state_store: Arc<dyn PoolStateStore> = concrete_channel_store.clone();
    // Debounce the scan-checkpoint writes (#784, keyed in #1092): each persisted
    // watcher (settlement `ChannelOpened`, origin `Origin`) advances its cursor
    // once per completed `eth_getLogs` window — on the live tail, once per poll
    // tick with new confirmed blocks — and the directly-durable store fsyncs on
    // each. The persisted value is only a *floor* for the resume backfill
    // (`resolve_persisted_start` rewinds it by the reorg margin; the sinks are
    // idempotent), so coarsening the write cadence is safe — and the one key
    // still persisted (settlement) is force-flushed on graceful shutdown by its
    // own service, so steady-state progress is not lost. The origin directory
    // stopped persisting a cursor in #1504; it enumerates on every boot.
    // Wrapping here (the wiring layer) keeps the domain trait and the disk store
    // free of the debounce policy.
    let watcher_checkpoint_store: Arc<dyn decdn_incentive::KeyedCheckpointStore> = Arc::new(
        crate::payment_settlement::DebouncedCheckpointStore::new(concrete_channel_store.clone()),
    );
    // Boot-time smoke test: read every persisted record so startup fails
    // fast on corruption / forward-incompatible schema, well before the
    // `cdn/client/v1` handler is constructed further down the bring-up.
    // That handler calls `load_all` again to bootstrap its in-memory channel
    // map — the duplicate read is by design. Infra bring-up hands the handler
    // the *store*, not a snapshot: a pre-built map threaded through
    // `ClientHandlerDeps` would couple this stage to the handler's internal
    // channel representation. Cost: one extra `load_all` on startup.
    let persisted_count = tokio::task::spawn_blocking({
        let store = Arc::clone(&channel_state_store);
        move || store.load_all()
    })
    .await
    .context("channel state store load task panicked")?
    .context("failed to hydrate persisted channel state")?
    .len();
    tracing::info!(
        channels = persisted_count,
        "channel state store ready (issue #527 replay guard active)",
    );

    // Open the append-only download-receipt audit log (issue #248). Unlike the
    // channel store, a failure here is NON-fatal: the receipt log is an audit
    // artifact (revenue reconciliation, dispute evidence), not the replay
    // guard, so the node still serves paid delivery — falling back to a
    // discard-only log — rather than refusing to start. The open does a small
    // amount of disk I/O (create + chmod), so run it on the blocking pool.
    let receipt_log_data_dir = cfg.identity.data_dir.clone();
    let receipt_log_policy = crate::receipt_log::RotationPolicy::from(&cfg.receipts);
    let receipt_log: Arc<dyn crate::receipt_log::ReceiptLog> =
        match tokio::task::spawn_blocking(move || {
            crate::receipt_log::JsonlReceiptLog::open(&receipt_log_data_dir, receipt_log_policy)
        })
        .await
        .context("download-receipt log open task panicked")?
        {
            Ok(log) => {
                tracing::info!(path = %log.path().display(), "download-receipt log ready");
                Arc::new(log)
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    event = "download_receipt_log_open_failed",
                    "failed to open download-receipt log; continuing without the audit log \
                     (paid delivery is unaffected; revenue reconciliation will be incomplete)",
                );
                Arc::new(crate::receipt_log::NoopReceiptLog)
            }
        };

    // Decouple the audit write from the paid-delivery hot path (#803): a single
    // background task owns the receipt log and drains a bounded queue, so the
    // voucher-accept path only does a non-blocking enqueue before it continues
    // delivery and a slow/full disk can never back-pressure delivery. The token is
    // cancelled after the router drains on shutdown (below) so the writer
    // flushes its tail before exiting.
    let receipt_writer_shutdown = CancellationToken::new();
    let (receipt_sink, receipt_writer) = crate::receipt_log::spawn_receipt_writer(
        receipt_log,
        Arc::clone(&node_metrics),
        receipt_writer_shutdown.clone(),
    );

    // Background lane-store flush (ADR 003 §Off-chain voucher state persistence):
    // mirror the in-memory voucher watermark to disk every
    // `payment.voucher_commit_interval_ms`. A failed flush retains the dirty set
    // for the next tick; it never blocks delivery.
    let flush_store = Arc::clone(&channel_state_store);
    let flush_metrics = Arc::clone(&node_metrics);
    let flush_interval =
        std::time::Duration::from_millis(cfg.payment.voucher_commit_interval_ms.max(1));
    let lane_flush_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(flush_interval);
        ticker.tick().await; // skip the immediate first tick
        loop {
            ticker.tick().await;
            let store = Arc::clone(&flush_store);
            let res = tokio::task::spawn_blocking(move || store.flush()).await;
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    flush_metrics.lane_flush_failure();
                    tracing::warn!(%err, "lane store background flush failed; retrying next tick");
                }
                Err(join_err) => {
                    flush_metrics.lane_flush_failure();
                    tracing::warn!(%join_err, "lane store flush task join failed");
                }
            }
        }
    });

    // Node-to-node pull-through origin (#831, ADR 001/022). Constructed empty up
    // front so it can be appended to the cache's origin chain here; its
    // dependencies (DHT, buyer channel, reputation handles) don't exist yet and
    // are injected via `provision` once bring-up completes (below). When the
    // feature is off it is never created and never enters the chain. The handle
    // is retained to provision later.
    let node_origin = cfg
        .cache
        .node_to_node_pull_through_enabled
        .then(crate::node_origin::NodeOrigin::new);
    // A shared `Arc<NodeOrigin>` handle for the window-paced serve path (#856).
    // `NodeOrigin` is `Clone` over an `Arc<OnceLock<deps>>`, so this clone sees
    // the dependencies `provision`ed (below) on the chain's copy. Held so the
    // client handler can drive progressive pulls directly, bypassing the buffered
    // `populate` for the serve-miss pull-through path.
    let pull_through_origin = node_origin.clone().map(Arc::new);
    let cache = build_cache(
        cfg,
        Arc::clone(&node_metrics),
        node_origin.clone().map(|o| Arc::new(o) as Arc<dyn Origin>),
    )
    .await?;
    // Attach the cache to the reload state so SIGHUP handlers can swap
    // the pinned-hashes set atomically (#276). Done immediately after
    // `build_cache` succeeds so a SIGHUP delivered during the rest of
    // startup will still find a target.
    reload_state.attach_cache(Some(cache.clone()));

    // Admission/eviction policy selection (ADR 040). One shared frequency
    // estimator feeds both the engine's hit-signal sink and whichever policy
    // objects need it; the engine itself owns no policy knowledge beyond the
    // estimator handle.
    let want_tinylfu =
        cfg.cache.eviction_policy == "tinylfu" || cfg.cache.admission_policy == "tinylfu";
    let estimator: Option<Arc<dyn decdn_cache::FrequencyEstimator>> = want_tinylfu.then(|| {
        Arc::new(decdn_cache::policy::tinylfu::TinyLfuEstimator::new(
            cfg.cache.tinylfu.sketch_bytes,
        )) as Arc<dyn decdn_cache::FrequencyEstimator>
    });
    if let Some(est) = &estimator {
        cache.set_frequency_estimator(est.clone());
    }
    if cfg.cache.admission_policy == "tinylfu"
        && let Some(est) = &estimator
    {
        cache.set_admission_policy(Arc::new(decdn_cache::policy::tinylfu::ProbationAdmission {
            freq: est.clone(),
            promotion_threshold: cfg.cache.tinylfu.promotion_threshold,
        }));
    }
    // promotion_threshold + probation_target_pct live entirely on the policy
    // object, not on the engine or the eviction driver's `EvictionParams`.
    let eviction_policy: Arc<dyn decdn_cache::EvictionPolicy> =
        match cfg.cache.eviction_policy.as_str() {
            "tinylfu" => match &estimator {
                Some(est) => Arc::new(decdn_cache::policy::tinylfu::TinyLfuEviction::new(
                    est.clone(),
                    cfg.cache.tinylfu.promotion_threshold,
                    cfg.cache.tinylfu.probation_target_pct,
                )),
                // Unreachable: `want_tinylfu` is true whenever eviction_policy
                // == "tinylfu", so `estimator` is always `Some` here.
                None => Arc::new(decdn_cache::policy::LruEviction),
            },
            _ => Arc::new(decdn_cache::policy::LruEviction),
        };

    // `tinylfu` admission only does useful work paired with `tinylfu` eviction:
    // promotion out of probation and the probation cap both live in
    // `TinyLfuEviction::plan`. With `lru` eviction the probation labels are set
    // but never promoted or capped, and the estimator pays a per-serve cost for
    // no effect. Warn rather than silently no-op (the resolver already rejects
    // typos; this valid-but-inert combination deserves a heads-up).
    if cfg.cache.admission_policy == "tinylfu" && cfg.cache.eviction_policy != "tinylfu" {
        tracing::warn!(
            admission_policy = %cfg.cache.admission_policy,
            eviction_policy = %cfg.cache.eviction_policy,
            "cache.admission_policy = \"tinylfu\" is inert unless cache.eviction_policy is \
             also \"tinylfu\": probation admission relies on the tinylfu eviction policy to \
             promote and cap probation members; under lru eviction the labels do nothing and \
             the frequency estimator runs for no effect",
        );
    }

    let retry = cfg.cache.origin_retry;
    tracing::info!(
        cache_dir = %cfg.cache.cache_dir.display(),
        has_origin = !cfg.cache.origins.is_empty(),
        origin_count = cfg.cache.origins.len(),
        origin_kinds = %origin_kinds_label(&cfg.cache.origins),
        pinned_hashes = cfg.cache.pinned_hashes.len(),
        // Origin retry policy (#285). Logged once at startup so operators
        // can audit the active resilience budget without hitting an RPC.
        retry_max_retries = retry.max_retries,
        retry_initial_backoff_ms = retry.initial_backoff_ms,
        retry_max_backoff_ms = retry.max_backoff_ms,
        retry_jitter_ratio = retry.jitter_ratio,
        "cache engine ready",
    );

    let transport_config =
        quic_transport_config().context("failed to build QUIC transport config")?;
    let ep = build_endpoint(
        &secret_key,
        cfg.network.bind_port,
        &cfg.network.relay_urls,
        &cfg.network.discovery,
        transport_config,
    )
    .await
    .context("failed to build iroh endpoint")?;

    node_metrics
        .register_iroh_endpoint(&ep)
        .context("failed to register iroh metrics")?;

    let limiter = Arc::new(ConnectionLimiter::new(
        &cfg.security,
        Arc::clone(&node_metrics),
    ));
    // Attach the limiter to the reload state so SIGHUP / admin reloads
    // can forward `[security]` changes via `ConnectionLimiter::reload`
    // (#235). Done immediately after construction so a SIGHUP delivered
    // during the rest of startup still finds a target.
    reload_state.attach_limiter(Some(Arc::clone(&limiter)));

    Ok(Infra {
        node_metrics,
        secret_key,
        eth_signer,
        concrete_channel_store,
        channel_state_store,
        watcher_checkpoint_store,
        receipt_writer_shutdown,
        receipt_sink,
        receipt_writer,
        lane_flush_task,
        node_origin,
        pull_through_origin,
        cache,
        eviction_policy,
        ep,
        limiter,
    })
}

/// By-value bundle of the endpoint, handlers, limiter, and blacklist
/// readiness receiver consumed when [`serve_until_shutdown`] builds the router.
/// Every field is moved into the router builder (or its gate), so none is
/// referenced by [`run`] after the serve call returns.
struct ServeInputs {
    ep: Endpoint,
    probe_handler: Arc<ProbeHandler>,
    client_handler: Arc<ClientHandler>,
    dht_handler: Arc<DhtHandler>,
    blacklist_ready_rx: oneshot::Receiver<crate::blacklist_watcher::InitialSyncResult>,
}

/// Serve phase extracted verbatim from the tail of [`run`] (issue #1253 PR3):
/// build the paid-delivery router behind the fail-closed blacklist gate, install
/// the signal + SIGHUP streams, and run the select loop until a shutdown signal
/// arrives. Returns the live [`Router`] and the observed [`ShutdownSignal`] so
/// [`run`] can drive the teardown sequence. `drain_trigger` is borrowed, not
/// consumed, because [`run`] still passes it to [`shutdown`] afterwards.
#[allow(clippy::cognitive_complexity)]
async fn serve_until_shutdown(
    reload_state: &RuntimeReloadState,
    config_path: Option<&Path>,
    drain_trigger: &Arc<admin::DrainTrigger>,
    serve_in: ServeInputs,
) -> anyhow::Result<(Router, ShutdownSignal)> {
    let ServeInputs {
        ep,
        probe_handler,
        client_handler,
        dht_handler,
        blacklist_ready_rx,
    } = serve_in;

    // No Probe, Client, or DHT ALPN is registered before the mandatory
    // first global + operator-region blacklist replay/scope pass succeeds. The
    // gate sits here — after the metrics/admin listeners are bound and every
    // background task is spawned — so a *slow* (still-pending) initial sync keeps
    // the paid-delivery listeners closed while observability, the admin control
    // surface, and the startup banner stay up. A *failed* initial sync is
    // fail-closed the hard way: the gate returns `Err`, `run` propagates it, and
    // the process exits (tearing those listeners down with it) rather than ever
    // serving un-vetted content. The watcher was spawned earlier in bring-up
    // so its initial replay runs
    // concurrently and is often already complete by the time control reaches
    // this gate.
    let router = gate_listener_on_blacklist_sync(blacklist_ready_rx, || {
        Router::builder(ep.clone())
            .accept(ProbeHandler::ALPN, probe_handler)
            .accept(ClientHandler::ALPN, client_handler)
            .accept(DhtHandler::ALPN, dht_handler)
            .spawn()
    })
    .await?;

    // Install signal streams once, before entering the select loop.
    // tokio docs are explicit that `Signal::recv` is the supported way
    // to await repeated signals, and re-creating the stream per signal
    // is not — see `tokio::signal::unix::signal` for the reasoning.
    let mut shutdown_streams = ShutdownStreams::install();
    let mut hup_stream = HupStream::install();

    let signal = loop {
        tokio::select! {
            sig = shutdown_streams.recv() => break sig,
            () = hup_stream.recv() => {
                match config_path {
                    Some(path) => {
                        if let Err(err) = reload_state.reload(path).await {
                            tracing::warn!(%err, "config reload error");
                        }
                    }
                    None => {
                        tracing::warn!(
                            "SIGHUP received but no config file path is in use; ignoring"
                        );
                    }
                }
            }
            () = drain_trigger.wait() => {
                tracing::info!("admin_v1_drain received; initiating graceful shutdown");
                break ShutdownSignal::AdminDrain;
            }
        }
    };
    tracing::info!(signal = %signal, "shutdown signal received; closing router");

    Ok((router, signal))
}

/// Chain providers, event watchers, and paid-delivery handlers built during the
/// middle phase of [`run`] (issue #1253 PR4). A by-value bundle of the
/// long-lived handles the background-task, serve, and shutdown phases consume,
/// mirroring [`Infra`]. Generic over the seller-wallet provider `P` (an opaque
/// `impl Provider` threaded through [`PoolSettlementService`]); [`run`] infers
/// `P` at the call site and hands it to [`ShutdownHandles`].
struct ChainHandlers<P: Provider + Clone + 'static> {
    rpc_url: HttpUrl,
    event_poll_interval: Duration,
    slash_domain: alloy::dyn_abi::Eip712Domain,
    voucher_domain: alloy::dyn_abi::Eip712Domain,
    bind_domain: alloy::dyn_abi::Eip712Domain,
    payment_pool_addr: alloy::primitives::Address,
    staker_set: Arc<dyn StakerSet>,
    /// The single multiplexed chain-event poller driving all five watcher routes.
    /// Stopped once, late in shutdown (after `router.shutdown`), because the
    /// capacity-bond route's staker set gates DHT admission through drain.
    poller: crate::chain_events::resumable_watcher::WatcherHandle,
    slash_store: crate::slash_watcher::SlashStore,
    node_address_resolver: Option<Arc<dyn crate::dht::NodeAddressResolver>>,
    probe_rate_limiter: Arc<ProbeRateLimiter>,
    probe_handler: Arc<ProbeHandler>,
    dht_rate_limiter: Arc<DhtRateLimiter>,
    record_store: Arc<std::sync::Mutex<RecordStore>>,
    origin_directory: Arc<dyn crate::dht::origin::OriginDirectory>,
    dht_handler: Arc<DhtHandler>,
    dht_routing: Arc<std::sync::Mutex<crate::dht::RoutingTable>>,
    /// The `CapacityBond` registry's `NodeId → regionHint` projection (ADR 030),
    /// threaded to the node-origin pull path for the region-latency penalty.
    registry_regions: Arc<std::sync::RwLock<std::collections::HashMap<crate::dht::NodeId, String>>>,
    client_handler: Arc<ClientHandler>,
    payment_service: PoolSettlementService<P>,
    blacklist_ready_rx: oneshot::Receiver<crate::blacklist_watcher::InitialSyncResult>,
    /// Bring-up node-id binding self-check (#1034), carried through to
    /// `AdminState` so `admin_v1_health` can report it. A plain value, not a
    /// handle: the binding only moves by an explicit operator transaction, so
    /// there is nothing live to keep.
    binding_report: crate::binding_check::BindingReport,
}

/// Middle phase extracted verbatim from [`run`] (issue #1253 PR4): parse the
/// chain contract addresses and EIP-712 domains, bootstrap the `CapacityBond`
/// registry / slash / origin watchers, and construct the probe, client, and DHT
/// handlers plus the seller-side [`PoolSettlementService`]. Borrows [`Infra`];
/// the buyer-side provider/store construction and the background tasks stay in
/// [`run`]. Returns [`ChainHandlers`], whose seller-wallet provider `P` [`run`]
/// infers and threads into [`ShutdownHandles`].
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
async fn build_chain_and_handlers(
    cfg: &ResolvedConfig,
    reload_state: &RuntimeReloadState,
    infra: &Infra,
) -> anyhow::Result<ChainHandlers<impl Provider + Clone + 'static>> {
    // SlashJudge EIP-712 domain for probe `slash_sig` (ADR 014 §1–2). Parsed
    // through the shared zero-address guard (#1219) for a uniform "every daemon
    // contract address is non-zero" invariant. `resolve_blockchain` already
    // rejects a zero or malformed `slash_judge_address` at config load, so here
    // the guard is defense-in-depth rather than the sole check.
    let slash_judge_addr = parse_nonzero_address(
        &cfg.blockchain.slash_judge_address,
        "blockchain.slash_judge_address",
    )?;
    let slash_domain =
        decdn_incentive::slash_judge_domain(cfg.blockchain.chain_id, slash_judge_addr);

    // Chain-backed active-staker set. Bootstrap failure is fatal: an
    // empty set silently rejects every inbound `Store`, and once the
    // iterative `FindValue` lookup filter exists it would drop every
    // responder. Built here (ahead of the probe handler) so the probe
    // handler can consult it for stake-lane probe-acceptance (#757); the
    // DHT handler below shares the same `Arc`.
    let rpc_url: HttpUrl = cfg.blockchain.rpc_url.parse().with_context(|| {
        format!(
            "blockchain.rpc_url {:?} is not a valid URL",
            cfg.blockchain.rpc_url
        )
    })?;
    let capacity_bond_addr = parse_nonzero_address(
        &cfg.blockchain.capacity_bond_address,
        "blockchain.capacity_bond_address",
    )?;
    // Mandatory for paid delivery. Config resolution rejects absence; this
    // runtime guard preserves fail-closed behavior for directly constructed
    // `ResolvedConfig` values as well.
    let content_blacklist_addr = parse_nonzero_address(
        cfg.blockchain
            .content_blacklist_address
            .as_deref()
            .context("missing mandatory blockchain.content_blacklist_address")?,
        "blockchain.content_blacklist_address",
    )?;
    // Retained for the blacklist watcher's read-only provider after `rpc_url`
    // moves into the buyer wallet provider below.
    let blacklist_rpc_url = rpc_url.clone();
    // One value, two consumers (#1011/#1106): the multiplexed poller's merged
    // `eth_getLogs` tick cadence, and — through `with_poll_interval` below — the
    // pending-tx receipt heartbeat, overriding alloy's 250 ms localhost default
    // that would hammer a dev anvil.
    let event_poll_interval = Duration::from_millis(cfg.blockchain.event_poll_interval_ms);
    let chain_provider = ProviderFactory::read_only(rpc_url.clone(), event_poll_interval);
    // One `eth_blockNumber` per TTL window for ALL watchers, instead of one per
    // watcher per tick. Plain read-only provider: a head read needs no wallet or
    // nonce filler, and coupling it to the signer stack would give every watcher's
    // head read a dependency on it. Deliberately NOT wrapped in
    // `with_poll_interval` — that only sets the pending-tx receipt heartbeat, and
    // this provider never builds a `PendingTransactionBuilder`.
    let head: Arc<dyn HeadSource> = Arc::new(SharedHead::new(
        ProviderFactory::shared_head(rpc_url.clone()),
        event_poll_interval,
    ));
    // Every on-chain watcher registers a `Route` on ONE shared multiplexed
    // poller instead of running its own `eth_getLogs` loop: five watchers scan
    // disjoint `(address, topic0)` slices of the same three contracts, so one
    // merged `get_logs` per tick — demuxed after the fact — replaces five. The
    // routes are collected as each watcher bootstraps below and spawned once (see
    // `build`/`spawn` near the end of this function). The poller mints one
    // shutdown token; the runtime drives graceful stop through the single handle.
    let mut poller_routes: Vec<crate::chain_events::multiplexed_poller::Route> = Vec::new();
    // One read-only provider for the poller's merged `get_logs` (each sink still
    // holds its own contract instance for its follow-up reads/writes).
    let poller_provider = ProviderFactory::read_only(rpc_url.clone(), event_poll_interval);

    // One CapacityBond enumeration + one route feeding both registry projections
    // (#1110). The bindings half is built only when pull-through is on; it derives
    // from page data already read here, so — unlike when it had its own bootstrap
    // — it has no RPC that can fail on its own. `getRegisteredNodes` failure was
    // already fatal via this same (unconditional, first-to-run) call, so nothing
    // that boots today loses pull-through.
    let registry = crate::dht::capacity_bond_registry::bootstrap(
        chain_provider,
        capacity_bond_addr,
        Arc::clone(&head),
        cfg.cache.node_to_node_pull_through_enabled,
        Arc::clone(&infra.node_metrics),
    )
    .await
    .with_context(|| format!("CapacityBond registry bootstrap at {capacity_bond_addr}"))?;
    let staker_set: Arc<dyn StakerSet> = registry.staker_set;
    let registry_regions = Arc::clone(&registry.regions);
    // The shared `operator address → NodeId` reverse projection (#1110), kept
    // current by the same registry route. The lazy `ChainOriginDirectory`
    // resolves against it directly rather than maintaining its own binding cache.
    let operator_to_node = Arc::clone(&registry.operator_to_node);
    // The registry route MUST stay live through drain (its staker set gates DHT
    // admission), which forces the single poller stop to the LATE point below.
    poller_routes.push(registry.route);

    // Slash-detection watcher (#1032, G-NODE-05): enumerate this operator's
    // still-appealable slashes from `CapacityBond` and follow `SlashRecorded`, so
    // a slash surfaces over `admin_v1_slashes` (+ the `decdn_slashes_detected_total`
    // metric) and the operator can file `decdn appeal slash` in time. Read-only;
    // held to `run()`'s end so its background task lives as long as the daemon.
    // The boot enumeration is fatal, like the CapacityBond registry bootstrap
    // above on the same contract that already gates startup — so it adds no new
    // failure mode; thereafter a tail blip retries and the periodic resync heals
    // drift, so detection is never disabled for the daemon's lifetime.
    let (slash_store, slash_route) = crate::slash_watcher::bootstrap(
        ProviderFactory::read_only(rpc_url.clone(), event_poll_interval),
        capacity_bond_addr,
        infra.eth_signer.address(),
        Arc::clone(&head),
        Arc::clone(&infra.node_metrics),
    )
    .await
    .context("bootstrap the slash-detection watcher")?;
    poller_routes.push(slash_route);

    // NodeId → bonded operator address resolver for node-to-node pulls (#831),
    // produced by the same CapacityBond bootstrap as the staker set above and
    // `Some` exactly when pull-through is enabled. Held to provision the
    // `NodeOrigin` below.
    let node_address_resolver: Option<Arc<dyn crate::dht::NodeAddressResolver>> =
        if cfg.cache.node_to_node_pull_through_enabled {
            registry.node_addresses
        } else {
            None
        };

    // Stake-lane probe-acceptance reservation (#757, ADR 003 §Admission and
    // Priority). Strictly operator opt-in: a policy is built only for a
    // non-zero `cache.stake_lane_reserved_holds` (the `NonZeroUsize` gate
    // makes the "off" case unrepresentable in `StakeLanePolicy`), so the
    // default single-lane node passes `None` and the probe handler's hot
    // path is unchanged. A reservation paired with disabled holds
    // (`max_probe_holds == 0`) can never fire — surface that misconfig as a
    // warning and leave the lane off rather than wire a dead per-probe lookup.
    let stake_lane_policy =
        NonZeroUsize::new(cfg.cache.stake_lane_reserved_holds).and_then(|reserved| {
            if cfg.cache.max_probe_holds == 0 {
                tracing::warn!(
                    reserved_holds = reserved.get(),
                    "cache.stake_lane_reserved_holds is set but cache.max_probe_holds=0; \
                     the stake-lane reservation has no effect (probe holds are disabled)"
                );
                None
            } else {
                Some(ProbeStakeLanePolicy::new(
                    Arc::clone(&staker_set),
                    reserved,
                    cfg.cache.max_probe_holds,
                ))
            }
        });

    // ADR 005 §Probe rate limiting three-layer token-bucket limiter for
    // `cdn/probe/v1`. Built from `[probe.rate_limit]` and run *in addition* to
    // the shared `ConnectionLimiter` (see `probe_rate_limit` module docs); the
    // per-peer (NodeId) layer it adds is the gap #982 closed.
    // `from_resolved` rather than `new`: both limiter configs are aliases of
    // the same struct, so passing `&cfg.dht` here would compile (#1457).
    let probe_rate_limiter = Arc::new(ProbeRateLimiter::from_resolved(
        &cfg.probe,
        Arc::clone(&infra.node_metrics),
    ));

    // Live per-MB delivery-rate floor (#1172, ADR 019 §3.1). Seed from the
    // config stand-in (`payment.delivery_floor`) so the handlers hold the shared
    // clamp from construction; the authoritative on-chain `getRateBounds()` read
    // below (once `payment_pool_addr` is parsed) overwrites it before serving
    // begins, and the `RateBoundsUpdated` watcher keeps it live thereafter. The
    // same handle is cloned into the probe handler, the client handler, and the
    // watcher.
    let rate_bounds = crate::rate_bounds::RateBounds::new(cfg.payment.delivery_floor);

    let probe_handler = Arc::new(ProbeHandler::new(
        infra.secret_key.public(),
        cfg.payment.rate_per_mb,
        Arc::clone(&infra.node_metrics),
        Arc::clone(&infra.limiter),
        Arc::clone(&probe_rate_limiter),
        infra.cache.clone(),
        Arc::clone(&infra.eth_signer),
        slash_domain.clone(),
        rate_bounds.clone(),
        stake_lane_policy,
    ));

    // `cdn/dht/v1` handler (ADR 022 / #320). FindNode + FindValue +
    // Store all wired up; iterative requester-side lookup and the
    // republish scheduler land in PR 4 of #320. Three-layer rate limiter
    // operates at the full ADR 022 spec.
    let dht_rate_limiter = Arc::new(DhtRateLimiter::from_resolved(
        &cfg.dht,
        Arc::clone(&infra.node_metrics),
    ));
    // Record store sized from the ADR 022 defaults; per-publisher /
    // global / per-hash caps are pinned by the protocol and only the
    // TTL field is plausibly operator-tunable, but no knob is exposed
    // yet — operators with non-default needs should file a follow-up
    // rather than tune in TOML.
    let record_store = Arc::new(std::sync::Mutex::new(RecordStore::new(
        RecordStoreConfig::default(),
    )));
    // Origin directory with a single consumer: the node-origin FIND_VALUE
    // last-resort fallback used when the DHT returns no providers (ADR 022
    // §FIND_VALUE Flow; #912). When the operator configures the
    // OriginAssignment + PublisherRegistry addresses, use the chain-backed
    // `ChainOriginDirectory` — a lazy TTL cache resolving hash → namespace →
    // authorized origin → active NodeId on demand, reusing the
    // already-bootstrapped `staker_set` and `operator_to_node` reverse
    // projection for operator liveness and binding. No bootstrap RPC: the
    // cache populates on the first lookup miss per namespace.
    // Without those addresses this is an `EmptyOriginDirectory`: the FIND_VALUE
    // fallback resolves nothing.
    let origin_directory: Arc<dyn crate::dht::origin::OriginDirectory> =
        if let Some(origin_addr) = cfg.blockchain.origin_assignment_address.as_deref() {
            let origin_assignment_addr =
                parse_nonzero_address(origin_addr, "blockchain.origin_assignment_address")?;
            Arc::new(crate::dht::ChainOriginDirectory::new(
                ProviderFactory::read_only(rpc_url.clone(), event_poll_interval),
                origin_assignment_addr,
                Arc::clone(&operator_to_node),
                Arc::clone(&staker_set),
                cfg.blockchain.origin_directory_cache_capacity,
                Duration::from_secs(cfg.blockchain.origin_directory_positive_ttl_sec),
                Duration::from_secs(cfg.blockchain.origin_directory_negative_ttl_sec),
                Arc::clone(&infra.node_metrics),
            ))
        } else {
            Arc::new(crate::dht::origin::EmptyOriginDirectory)
        };
    let dht_handler = Arc::new(DhtHandler::new(
        infra.secret_key.public(),
        Arc::clone(&dht_rate_limiter),
        Arc::clone(&infra.limiter),
        Arc::clone(&infra.node_metrics),
        Arc::clone(&staker_set),
        Arc::clone(&record_store),
    ));

    // The DHT handler builds its own routing table internally; grab a
    // shared handle so the bootstrap path + republish + bucket-refresh
    // tasks can all operate on the same instance.
    let dht_routing = dht_handler.routing_table();

    // `cdn/client/v1` paid-delivery handler (#317). The voucher EIP-712 domain
    // binds to the `PaymentPool` deployment; the ephemeral-binding
    // domain to the `CapacityBond` deployment (== `capacity_bond_addr`,
    // which holds the NodeId↔address mappings). The handler hydrates per-channel
    // voucher state from `channel_state_store` so a restart cannot replay an
    // already-accepted voucher (#527).
    let payment_pool_addr = parse_nonzero_address(
        &cfg.blockchain.payment_pool_address,
        "blockchain.payment_pool_address",
    )?;

    // Authoritative on-chain delivery-rate floor (#1172, ADR 019 §3.1 / ADR
    // 003). Read once at startup — a fail-fast self-check in the same spirit as
    // `PaymentPool.usdc()` — and seed the shared clamp created above,
    // replacing the config stand-in. The on-chain floor is `uint256`; the node
    // clamps in `u64`, so an out-of-range value must refuse startup rather than
    // silently truncate. The `RateBoundsUpdated` watcher spawned below keeps the
    // clamp live for governance retunes without a restart.
    {
        let contract = decdn_incentive::payment_pool::PaymentPool::new(
            payment_pool_addr,
            ProviderFactory::read_only(rpc_url.clone(), event_poll_interval),
        );
        let on_chain_floor = contract.getRateBounds().call().await.with_context(|| {
            format!("PaymentPool.getRateBounds() startup read at {payment_pool_addr}")
        })?;
        // Both rejection arms live in `rate_bounds::on_chain_floor_to_u64` so a
        // unit test can reach them; inline here they sat behind an async chain
        // read no fixture could drive to a bad value.
        let floor = crate::rate_bounds::on_chain_floor_to_u64(
            on_chain_floor,
            &payment_pool_addr.to_string(),
        )?;
        rate_bounds.store(floor);
        tracing::info!(
            floor,
            %payment_pool_addr,
            "seeded live delivery-rate floor from on-chain getRateBounds()"
        );
    }

    // Bring-up self-check: is the key we are about to serve under the one bound
    // to this operator on-chain? A node that answers "no" is UNSLASHABLE
    // (`SlashJudge` resolves the accused through `nodeIdOf`), and nothing else
    // in the runtime would notice — so the check is here rather than left to an
    // operator to think of. Advisory: it never blocks startup, because a daemon
    // that refused to boot on a transient RPC failure could not be used to run
    // the rotation that repairs the binding. See `crate::binding_check`.
    let binding_report = crate::binding_check::check(
        ProviderFactory::read_only(rpc_url.clone(), event_poll_interval),
        capacity_bond_addr,
        infra.eth_signer.address(),
        *infra.secret_key.public().as_bytes(),
    )
    .await;

    let voucher_domain =
        decdn_incentive::voucher_domain(cfg.blockchain.chain_id, payment_pool_addr);
    let bind_domain =
        decdn_incentive::bind_node_id_domain(cfg.blockchain.chain_id, capacity_bond_addr);

    // Reactive LOCAL-origin pull-through (#1116). Arm a local-only populate on the
    // serve-miss path whenever the operator configured any origin (`[cache.origin]`),
    // INDEPENDENT of `node_to_node_pull_through_enabled`: a cache-only operator must
    // be able to reactively serve content it holds in its OWN fs/http/s3 origin, and
    // — with node→node on — that local origin is preferred over the paid peer window
    // path. The handler still gates it on proven channel ownership
    // (`pull_authorized`), so it fronts no free egress, and `populate_local` never
    // consults the paid `Peer` node→node origin. The node→node
    // window/buffered/governor/gate paths below stay flag-gated.
    //
    // A single local origin-chain walk (fs/http/s3), NOT a provider fan-out — so
    // budget it at the per-attempt `node_pull_timeout_sec`, not `outer_pull_deadline`
    // (which budgets the sequential node→node pull). Using the outer deadline would
    // let a wedged local origin block many times longer (>7× at defaults) before
    // falling through to the node→node paths.
    let local_populate = (!cfg.cache.origins.is_empty())
        .then(|| Duration::from_secs(cfg.cache.node_pull_timeout_sec));

    // Node→node pull-through deps (#831/#856/#821), all gated on
    // `node_to_node_pull_through_enabled`. Computed as `Option`s here so the handler's
    // wiring is one construction-time literal; the gates (and their nesting) are
    // preserved exactly.
    let mut pull_through = None;
    let mut pull_through_origin = None;
    if cfg.cache.node_to_node_pull_through_enabled {
        // The outer deadline bounds how long a miss blocks the delivery path before
        // falling back to `NotFound`. It is *derived* from the per-candidate budget
        // (`node_pull_timeout_sec`) rather than equal to it: the handler wraps the
        // whole `discover → probe → rank → pull` fetch in one `tokio::time::timeout`,
        // so an outer deadline equal to the per-candidate timeout would cancel the
        // fetch the instant candidate #1 stalls, before the `MAX_PROVIDER_ATTEMPTS`
        // fallback loop ever reaches candidates #2..N (#859). `outer_pull_deadline`
        // budgets all three of each candidate's sequential stages — channel open,
        // stream open, and one silent-streaming window — plus one-time discovery
        // slack. Whether the pull can actually succeed additionally depends on the
        // `NodeOrigin` being provisioned; an unprovisioned origin just makes the
        // `get` a fast miss.
        let per_candidate = Duration::from_secs(cfg.cache.node_pull_timeout_sec);
        let stall = Duration::from_secs(cfg.cache.node_pull_stall_timeout_sec);
        let outer_deadline = crate::selection::outer_pull_deadline(per_candidate, stall);
        pull_through = Some(outer_deadline);
        // Window-paced pull-through (#856, ADR 037): when the `NodeOrigin` is
        // available, serve cache misses by fusing the upstream pull with downstream
        // delivery, paced by the ramped credit window (ADR 003 §Credit window,
        // #1669) instead of the buffered `populate`.
        if let Some(origin) = &infra.pull_through_origin {
            pull_through_origin = Some(Arc::clone(origin));
        }
    }

    // Build the paid-delivery handler from a single deps literal (#1254): every
    // optional wiring hook above is supplied at construction, not via a setter chain.
    // ADR 011 deny-set. Taken from the reload state rather than built here, so
    // the handler and the SIGHUP / `decdn node reload` path hold the SAME `Arc`
    // — a denylist entry added to the config file takes effect on reload with no
    // handler rebuild and no restart, which is what makes it usable against a
    // one-hour statutory clock. Passed into `new()` as a required argument: it
    // is a compliance gate, so a construction site that forgets to wire it must
    // not silently degrade to "deny nothing".
    let content_denylist = reload_state.content_denylist();
    // Redeem-hint channel (#327), created outside `PoolSettlementService::bootstrap`
    // so the sender can be cloned into the handler deps below while the service
    // takes the receiver. `redeem_tx` is cloned into the handler deps and also
    // handed to the service (so `redeem_hint_sender()` keeps working); `redeem_rx`
    // drives the service's redeemer loop.
    let (redeem_tx, redeem_rx) =
        tokio::sync::mpsc::channel(crate::payment_settlement::REDEEM_HINT_CAPACITY);
    let mut client_deps = crate::handlers::client::ClientHandlerDeps::new(
        infra.secret_key.public(),
        Arc::clone(&infra.node_metrics),
        Arc::clone(&infra.limiter),
        infra.cache.clone(),
        Arc::clone(&infra.eth_signer),
        slash_domain.clone(),
        voucher_domain.clone(),
        bind_domain.clone(),
        Arc::clone(&infra.channel_state_store),
        Arc::clone(&infra.receipt_sink),
        cfg.payment.rate_per_mb,
        rate_bounds.clone(),
        cfg.cache
            .max_blob_size_mb
            .saturating_mul(decdn_protocol::MB_BYTES),
        MAX_CLIENT_STREAMS,
        Arc::clone(&content_denylist),
        U256::from(cfg.blockchain.pool_min_remaining_deposit_micro_usdc),
    );
    // Owner-signed capability intake (ADR 003 §Capability delegation): the serve
    // gate persists a presented capability so the redeemer registers the signer
    // on first redemption. Same redb file every lane record lives in.
    client_deps.capability_sink =
        Some(Arc::clone(&infra.concrete_channel_store)
            as Arc<dyn crate::channel_store::CapabilitySink>);
    // Durable per-pool floor dead-charge (ADR 003 §Pool solvency): the same redb
    // store that holds every lane record also mirrors each pool's unrecoverable
    // floor loss, so a restart reloads it rather than granting a fresh free-floor
    // budget.
    client_deps.floor_loss_store =
        Some(Arc::clone(&infra.concrete_channel_store)
            as Arc<dyn decdn_incentive::PoolFloorLossStore>);
    // Event-fed pool view (owner + remaining) for the floor-`M` solvency gate and
    // the ADR 011 funder gate. The settlement watcher below folds every
    // `PaymentPool` event into this projection, so a serve request reads
    // `{owner, remaining}` in-memory — no per-serve `getPool` `eth_call`. The same
    // instance is handed to the settlement service (its watcher is the writer).
    let pool_view = crate::pool_view::PoolProjection::new();
    client_deps.pool_view =
        Some(Arc::new(pool_view.clone()) as Arc<dyn crate::pool_view::PoolView>);
    client_deps.local_populate = local_populate;
    client_deps.pull_through = pull_through;
    client_deps.pull_through_origin = pull_through_origin;
    // Downstream credit-window ramp (ADR 003 §Credit window, #1477, #1669).
    client_deps.credit_max = cfg.payment.credit_max;
    client_deps.credit_ramp_divisor = cfg.payment.credit_ramp_divisor;
    // Hint the settlement service on each accepted voucher so a lane's accrued
    // claim is planned into a chunk promptly rather than waiting the self-tick.
    client_deps.redeem_hint = Some(redeem_tx.clone());
    let client_handler = Arc::new(ClientHandler::new(client_deps)?);

    // On-chain seller-settlement service (#327). A wallet-filled provider
    // (the staker-set provider above is read-only) signs the `withdraw` /
    // `closeChannel` transactions with the same eth keystore signer. The
    // bootstrap self-checks the contract via `usdc()`; the watcher persists
    // channels opened against this node so the handler accepts their vouchers,
    // and forgets settled ones. Redemption is purely periodic: a self-tick
    // flushes the lane store then sweeps every above-threshold lane.
    // Simple (re-fetch-each-send) nonce management, not alloy's default cached
    // manager (#904). The cached manager advances its in-memory nonce when it
    // *prepares* a tx; if that send then fails (e.g. its `eth_estimateGas`
    // reverts on an always-reverting settlement under contention), the tx never
    // lands but the cached nonce stays advanced, so every subsequent tx from
    // this wallet carries a gapped nonce, sits unmined, and wedges the lane
    // until restart. `SimpleNonceManager` stores nothing — each send re-reads
    // the pending nonce — so a failed send can't gap the lane. The buyer
    // provider below relies on this same property for the retried `reclaimExpired`.
    let wallet_provider = ProviderFactory::seller_wallet(
        rpc_url.clone(),
        (*infra.eth_signer).clone(),
        event_poll_interval,
    );
    // First-redemption capability material (owner signature over the EIP-712
    // `Capability`) for each signer, persisted by the seller voucher-intake path
    // and read here so the redeemer can register a signer on its first redemption
    // (ADR 003 §Capability delegation). Backed by the same redb file every other
    // lane record lives in.
    let capability_source: Arc<dyn crate::payment_settlement::CapabilitySource> =
        Arc::new(crate::channel_store::StoredCapabilitySource::new(
            Arc::clone(&infra.concrete_channel_store),
        ));
    let (payment_service, settlement_route) = PoolSettlementService::bootstrap(
        wallet_provider,
        payment_pool_addr,
        infra.eth_signer.address(),
        Arc::clone(&infra.channel_state_store),
        Arc::clone(&infra.watcher_checkpoint_store),
        Arc::clone(&client_handler),
        capability_source,
        U256::from(cfg.blockchain.redeem_threshold_micro_usdc),
        usize::try_from(cfg.blockchain.redeem_max_vouchers_per_tx).unwrap_or(usize::MAX),
        Duration::from_secs(cfg.blockchain.redeem_interval_secs),
        Arc::clone(&infra.node_metrics),
        pool_view,
        redeem_tx,
        redeem_rx,
    )
    .await
    .context("PaymentPool settlement service bootstrap")?;
    poller_routes.push(settlement_route);

    // Blacklist compliance watcher (ADR 011/031, issue #1031). Its boot pass
    // ENUMERATES the current on-chain deny-set at one pinned block
    // (`blacklistedAddresses` ∪ per-region `blacklistedHashes`) rather than
    // replaying logs, then follows the live tail seeded at that block. It evicts
    // held blobs whose hash is blacklisted in scope for this operator, which
    // cascades to DHT-announce suppression (the republisher's `is_evicted` gate),
    // probe `has_blob:false`, and delivery refusal — the node's only local
    // protection against the slash for serving blacklisted content.
    // `blacklist_ready_rx` gates the ALPN router below on that first enumeration.
    // The route carries a shutdown-token-observing sink factory (its re-scope
    // polls the poller's token, #1236); the tail carries no durable cursor, so
    // teardown has no scan checkpoint to flush.
    let (blacklist_ready_tx, blacklist_ready_rx) = oneshot::channel();
    let blacklist_route = crate::blacklist_watcher::bootstrap(
        ProviderFactory::read_only(blacklist_rpc_url, event_poll_interval),
        content_blacklist_addr,
        infra.eth_signer.address(),
        infra.cache.clone(),
        Arc::clone(&head),
        Duration::from_secs(cfg.blockchain.content_blacklist_poll_interval_sec),
        blacklist_ready_tx,
        &infra.node_metrics,
        Arc::clone(&content_denylist),
    )
    .await
    .context("blacklist compliance watcher boot enumeration")?;
    poller_routes.push(blacklist_route);

    // Rate-bounds route (#1172, ADR 019 §3.1): follows `RateBoundsUpdated` on the
    // shared poller and re-reads `getRateBounds()` authoritatively every
    // `rate_bounds_poll_interval_sec` as a safety net, storing into the same
    // shared `rate_bounds` clamp the handlers hold (seeded by the startup read
    // above). Read-only, no durable cursor.
    let rate_bounds_route = crate::rate_bounds_watcher::route(
        ProviderFactory::read_only(rpc_url.clone(), event_poll_interval),
        payment_pool_addr,
        rate_bounds.clone(),
        Duration::from_secs(cfg.blockchain.rate_bounds_poll_interval_sec),
        &infra.node_metrics,
    );
    poller_routes.push(rate_bounds_route);

    // Assemble and spawn the ONE poller for all five routes. `build` fails fast
    // if two routes claim the same `(address, topic0)` — a wiring bug, not a
    // runtime condition — surfacing it at boot. Three contract addresses
    // (payment_pool, capacity_bond, content_blacklist) with disjoint topic0s per
    // (address, topic0).
    let poller = crate::chain_events::multiplexed_poller::MultiplexedPollerBuilder::new(
        Arc::clone(&head),
        event_poll_interval,
    );
    let poller = poller_routes
        .into_iter()
        .fold(
            poller,
            crate::chain_events::multiplexed_poller::MultiplexedPollerBuilder::route,
        )
        .build()
        .context("assemble the multiplexed chain-event poller")?;
    let poller = crate::chain_events::multiplexed_poller::spawn(poller_provider, poller);

    // Bootstrap (ADR 022 §Bootstrap): seed the routing table from the
    // active-staker set + parallel `FindNode(self.node_id)` against a
    // fan-out of seeds. Best-effort — failures here log but don't
    // abort startup.
    let bootstrap_outcome = crate::dht::bootstrap::bootstrap(
        &infra.ep,
        infra.secret_key.public(),
        &dht_routing,
        &staker_set,
    )
    .await;
    tracing::info!(
        seeds = bootstrap_outcome.seeds_seen,
        inserted = bootstrap_outcome.seeds_inserted,
        find_node_ok = bootstrap_outcome.find_node_ok,
        find_node_err = bootstrap_outcome.find_node_err,
        closer_added = bootstrap_outcome.closer_peers_inserted,
        "dht bootstrap complete"
    );

    Ok(ChainHandlers {
        rpc_url,
        event_poll_interval,
        slash_domain,
        voucher_domain,
        bind_domain,
        payment_pool_addr,
        staker_set,
        poller,
        slash_store,
        node_address_resolver,
        probe_rate_limiter,
        probe_handler,
        dht_rate_limiter,
        record_store,
        origin_directory,
        dht_handler,
        dht_routing,
        registry_regions,
        client_handler,
        payment_service,
        blacklist_ready_rx,
        binding_report,
    })
}

/// Owned handle bundle produced by [`spawn_background_tasks`]: every local born
/// in the background region that [`run`] still needs after it returns — the
/// stop signals, task handles, and drain trigger that flow into
/// [`serve_until_shutdown`] and [`ShutdownHandles`]. Non-generic: the buyer
/// provider is consumed inside the region and the settlement indexer stores a
/// concrete watcher handle, so no opaque provider type escapes.
struct Background {
    metrics_stop_tx: oneshot::Sender<()>,
    dispatch_gc_stop_tx: oneshot::Sender<()>,
    /// Origin-held-index periodic rescan (#1130). `None` when
    /// `cache.fs_rescan_interval_sec == 0` disables it.
    origin_rescan_stop_tx: Option<oneshot::Sender<()>>,
    record_store_gc_stop_tx: oneshot::Sender<()>,
    eviction_stop_tx: oneshot::Sender<()>,
    dht_rate_limit_gc_stop_tx: oneshot::Sender<()>,
    probe_rate_limit_gc_stop_tx: oneshot::Sender<()>,
    republish_stop_tx: oneshot::Sender<()>,
    bucket_refresh_stop_tx: oneshot::Sender<()>,
    buyer_bootstrap_stop_tx: oneshot::Sender<()>,
    rpc_watchdog: Option<(oneshot::Sender<()>, tokio::task::JoinHandle<()>)>,
    admin_stop_tx: Option<oneshot::Sender<()>>,
    drain_trigger: Arc<admin::DrainTrigger>,
    tasks: JoinSet<()>,
}

/// Background-tasks phase extracted verbatim from the middle of [`run`] (issue
/// #1253 PR5): construct the buyer-side provider/stores, spawn every periodic
/// GC / DHT / metrics / admin task, and emit the startup
/// banner. Borrows [`Infra`] and [`ChainHandlers`]; the by-value `ch` moves the
/// region performed on owned locals (`rpc_url` and the three EIP-712 domains)
/// become `.clone()`s here since they are read through a shared reference — each
/// field is consumed exactly once and never read again, so the clone is
/// behavior-identical. Returns the [`Background`] handles [`run`]
/// threads into the serve call and [`ShutdownHandles`].
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
async fn spawn_background_tasks<P: Provider + Clone + 'static>(
    cfg: &ResolvedConfig,
    config_path: Option<&PathBuf>,
    reload_state: &Arc<RuntimeReloadState>,
    infra: &Infra,
    ch: &ChainHandlers<P>,
    started_at: std::time::Instant,
) -> anyhow::Result<Background> {
    // On-chain buyer-side service (#744). When this node pulls content from an
    // upstream provider on a cache miss it pays via the same channel mechanism,
    // acting as the client: a separate wallet-filled provider signs `approve` /
    // `openChannel` / `reclaimExpired`. It shares the persistent store (a
    // distinct `buyer_channel_state_v2` table) and re-derives the voucher domain
    // the handler consumed above. The cache-miss hook that *calls*
    // `open_or_reuse_channel` is the `NodeOrigin` provisioned below (#831), gated
    // on `cache.node_to_node_pull_through_enabled`; the service is also held for
    // the process lifetime so its reclaim sweep keeps running even when
    // pull-through is off. The backgrounded bootstrap task (#1109, below) owns
    // that binding via its `let _service` hold — and thus keeps the service's
    // `AbortOnDrop` reclaim task alive to shutdown.
    //
    // Unlike the seller service, a buyer-bootstrap failure is NON-fatal: buying
    // is opportunistic cost-recovery, so a failed startup `approve` tx (e.g.
    // insufficient gas) must not block the node's core seller function. Log and
    // continue with the buyer path disabled (and thus pull-through disabled).
    // Simple nonce management, for the reason given on the seller
    // `wallet_provider` above (#904). It matters most here: `reclaimExpired` is
    // *expected* to revert under host-clock-vs-chain skew and be retried, so a
    // reverting send must not leak a cached nonce and wedge the buyer lane. This
    // provider is shared across `approve`/`openChannel`/`topUp`/`reclaimExpired`
    // and the reclaim sweep runs concurrently with opens, so correctness relies
    // on `SimpleNonceManager` re-reading the pending nonce each send (a transient
    // racing collision just gets a fresh nonce on the next attempt), not on the
    // sends being strictly serialized.
    let buyer_wallet_provider = ProviderFactory::buyer_wallet(
        ch.rpc_url.clone(),
        (*infra.eth_signer).clone(),
        ch.event_poll_interval,
    );
    let buyer_channel_store: Arc<dyn decdn_incentive::BuyerPoolStore> = Arc::new(
        crate::channel_store::BuyerPoolStoreHandle::new(Arc::clone(&infra.concrete_channel_store)),
    );
    // The buyer-side PaymentPool bootstrap (whose on-chain round-trips —
    // notably the one-time USDC `approve` receipt — can block for many
    // minutes on a stuck tx, bounded by `APPROVE_RECEIPT_TIMEOUT`) and
    // the node-origin pull-through provisioning it feeds are BOTH deferred to a
    // background task spawned below (#1109), so the metrics/admin listeners and
    // the "node runtime ready" banner come up independent of any chain RPC. The
    // provider/store/pending handles built just above are moved into that task.

    let metrics_addr = std::net::SocketAddr::new(
        cfg.observability.metrics_bind,
        cfg.observability.metrics_port,
    );
    let metrics_listener =
        metrics::bind(metrics_addr).context("failed to bind metrics listener")?;

    let (metrics_stop_tx, metrics_stop_rx) = oneshot::channel::<()>();

    let metrics_handle = Arc::clone(&infra.node_metrics);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        if let Err(err) = metrics::serve(metrics_listener, metrics_handle, metrics_stop_rx).await {
            tracing::error!(%err, "metrics server exited with error");
        }
    });

    // Periodic dispatch-limiter GC (#440). The acquire path only prunes
    // under flood (when the keyspace exceeds `cap + cap/10`); a node
    // with bursty short-lived clients can otherwise accumulate stale
    // per-source buckets between bursts and never reclaim them until
    // restart. The sweep is `O(n)` over the live keyspace; on an idle
    // limiter `n = 0` so the steady-state cost is one mutex acquire
    // per minute. Stops on its own oneshot — same pattern as the
    // metrics and admin servers.
    let dispatch_gc_stop_tx = {
        let limiter = Arc::clone(&infra.limiter);
        spawn_periodic(&mut tasks, "dispatch_gc", DISPATCH_GC_INTERVAL, move || {
            if let Some((before, after)) = limiter.gc_per_source() {
                tracing::debug!(
                    before,
                    after,
                    dropped = before.saturating_sub(after),
                    "dispatch GC sweep complete"
                );
            }
        })
    };

    // Periodic DHT record-store GC (ADR 022 §Content Records and TTL).
    // `RecordStore::providers_at` lazily scrubs expired records for the hash it
    // is queried about, but a hash nobody queries again keeps its expired
    // entries — they keep counting against the publisher's per-publisher quota
    // and against the global cap until this sweep fires. Without it a node that
    // publishes a one-shot blob can exhaust its 200-record quota and reject
    // every future `Store`. The sweep is `O(expired × log N)` over the global
    // LRU (front-walk in `RecordStore::gc`); default 60s is well below the
    // 1-hour record TTL.
    let record_store_gc_stop_tx = {
        let records = Arc::clone(&ch.record_store);
        spawn_periodic(
            &mut tasks,
            "record_store_gc",
            DISPATCH_GC_INTERVAL,
            move || {
                let now_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX));
                let removed = if let Ok(mut store) = records.lock() {
                    store.gc(now_us)
                } else {
                    tracing::error!("dht record-store mutex poisoned; skipping GC sweep this tick");
                    0
                };
                if removed > 0 {
                    tracing::debug!(removed, "dht record-store GC sweep complete");
                }
            },
        )
    };

    // Cache-eviction driver (#1173, ADR 040). Async
    // (the sweep does `total_bytes()`/`release_for_eviction().await`), so it
    // cannot ride `spawn_periodic`'s sync `FnMut`; it spawns directly into the
    // JoinSet with its own oneshot stop, mirroring the metrics server. Enforces
    // the `cache.cache_size_mb` ceiling the write path leaves unbounded.
    let eviction_stop_tx = {
        // The driver's actuator is `release_for_eviction`, which only drops GC
        // protection — the iroh-blobs GC sweep is what actually reclaims disk.
        // With the sweep disabled the ceiling is unenforceable, and the symptom
        // (a cache pinned at 100% while the driver reports work) points at
        // pinning/probe-holds rather than at the real cause. Say so at boot.
        if cfg.cache.gc_interval_sec == 0 {
            tracing::warn!(
                cache_size_mb = cfg.cache.cache_size_mb,
                "eviction driver is running but cache.gc_interval_sec = 0: released blobs are \
                 never reclaimed, so cache.cache_size_mb cannot be enforced. Set a nonzero \
                 gc_interval_sec (default 300) to make eviction reclaim disk."
            );
        }
        let (eviction_stop_tx, eviction_stop_rx) = oneshot::channel::<()>();
        let params = eviction::EvictionParams {
            cache_size_mb: cfg.cache.cache_size_mb,
            high_water_pct: cfg.cache.eviction_high_water_pct,
            target_pct: cfg.cache.eviction_target_pct,
            per_sweep_budget: cfg.cache.eviction_per_sweep_budget,
            tick: Duration::from_secs(cfg.cache.eviction_tick_secs),
        };
        tasks.spawn(eviction::run(
            infra.cache.clone(),
            infra.node_metrics.cache_metrics(),
            params,
            infra.eviction_policy.clone(),
            eviction_stop_rx,
        ));
        eviction_stop_tx
    };

    // Periodic DHT rate-limiter GC (#645). The acquire path opportunistically
    // prunes when the per-IP / per-peer keyed maps exceed `cap + cap/10`, but a
    // node whose DHT traffic falls below that threshold can carry millions of
    // stale buckets indefinitely — the same DoS shape the dispatch-limiter GC
    // prevents at the connection layer. `gc_per_ip` / `gc_per_peer` use
    // independent single-flight flags, so the two sequential sweeps here never
    // block a concurrent lazy prune from `check`.
    //
    // Panic contract: a panic inside `retain_recent` (governor arithmetic bug,
    // allocator OOM during the walk) unwinds out of the sweep and kills this
    // task. `PruneGuard`'s Drop still releases the single-flight flag, so the
    // lazy-prune path in `check` keeps working — but the periodic sweep is dead
    // until restart, surfacing only via `JoinSet::join_next` at shutdown.
    // Operators wanting earlier notice should alert on the
    // `decdn_dht_rate_limit_tracked_{per_ip,per_peer}` gauges failing to drop on
    // a quiet node (there the periodic sweep is their only writer). A map already
    // empty when the sweep died sits at `0` legitimately — indistinguishable
    // from a healthy sweep over an empty map.
    let dht_rate_limit_gc_stop_tx = {
        let limiter = Arc::clone(&ch.dht_rate_limiter);
        spawn_periodic(
            &mut tasks,
            "dht_rate_limit_gc",
            DHT_RATE_LIMIT_GC_INTERVAL,
            move || {
                log_keyspace_gc("dht", "per_ip", limiter.gc_per_ip());
                log_keyspace_gc("dht", "per_peer", limiter.gc_per_peer());
            },
        )
    };

    // Probe rate-limiter keyspace GC (#982) — same DoS-shape rationale and panic
    // contract as the DHT GC above, for the `cdn/probe/v1` per-IP / per-peer
    // keyed maps.
    let probe_rate_limit_gc_stop_tx = {
        let limiter = Arc::clone(&ch.probe_rate_limiter);
        spawn_periodic(
            &mut tasks,
            "probe_rate_limit_gc",
            PROBE_RATE_LIMIT_GC_INTERVAL,
            move || {
                log_keyspace_gc("probe", "per_ip", limiter.gc_per_ip());
                log_keyspace_gc("probe", "per_peer", limiter.gc_per_peer());
            },
        )
    };

    // DHT republish scheduler (ADR 022 §STORE Flow). The subscribe
    // handle is taken before the cold-start seed so a commit racing
    // with seed-time lands in the channel backlog rather than the
    // gap between the snapshot and the spawn.
    let republish_scheduler = Arc::new(crate::dht::RepublishScheduler::new());
    let (republish_stop_tx, republish_stop_rx) = oneshot::channel::<()>();
    let cache_inserts_rx = infra.cache.subscribe_inserts();
    // Walk the on-disk store (NOT `access_times_snapshot`, which maps `Hash →
    // Instant` and is empty on every cold start) so every committed,
    // non-evicted blob gets a `uniform(0, 40 min)` republish entry per ADR
    // 022 §Bootstrap AC 16. On a transient list-error we degrade: the
    // steady-state `subscribe_inserts` path catches only blobs newly fetched
    // post-boot — blobs already on disk that get cache-HIT requests are NOT
    // re-scheduled until the next successful restart.
    // Populate the origin-held index before seeding announces (#1130) so cold
    // origin content (fs directory entries + present pins) is advertised from
    // the first republish, not only after a warm pulls it into the store.
    infra.cache.rescan_origins().await;

    // Union store-complete blobs with origin-held content. The scheduler dedups
    // internally (idempotent `scheduled` set); collecting into a set first keeps
    // the logged count honest and coalesces a hash that is both stored and
    // origin-held into one jitter draw. On a store list-error we still seed the
    // origin-held set — announce degrades only for the store half.
    let mut cold_start_set: std::collections::HashSet<decdn_cache::Hash> =
        infra.cache.origin_held_hashes().into_iter().collect();
    match infra.cache.iter_hashes().await {
        Ok(hashes) => cold_start_set.extend(hashes),
        Err(err) => tracing::warn!(
            error = %err,
            "cold-start store seed failed; blobs not re-fetched this session will go un-republished until next restart (ADR 022 §Bootstrap AC 16 degraded)"
        ),
    }
    let cold_start_count = republish_scheduler.seed_cold_start(
        cold_start_set
            .into_iter()
            .map(|h| decdn_protocol::ContentHash::from_bytes(*h.as_bytes())),
    );
    tracing::info!(
        cold_start_count,
        "republish scheduler seeded from existing cache + origin-held index (ADR 022 §Bootstrap cold-start, #1130)"
    );
    tasks.spawn(crate::dht::publish::run_republish(
        infra.ep.clone(),
        infra.secret_key.public(),
        Arc::clone(&ch.dht_routing),
        Arc::clone(&republish_scheduler),
        infra.cache.clone(),
        cache_inserts_rx,
        republish_stop_rx,
    ));

    // Periodic origin rescan (#1130): re-walk the fs origin + re-check pins so a
    // file added after boot becomes discoverable within one interval, then push
    // newly-found hashes into the republish scheduler — origin content never
    // generates a store-insert event, so this is its only steady-state announce
    // trigger. `fs_rescan_interval_sec == 0` disables it (startup + reload still
    // rescan). The tick body offloads the async rescan to a detached task
    // (`spawn_periodic` bodies are synchronous); scheduling is idempotent, so an
    // already-announced hash coalesces rather than double-drawing jitter.
    let origin_rescan_stop_tx = if cfg.cache.fs_rescan_interval_sec > 0 {
        let cache = infra.cache.clone();
        let scheduler = Arc::clone(&republish_scheduler);
        Some(spawn_periodic(
            &mut tasks,
            "origin_rescan",
            Duration::from_secs(cfg.cache.fs_rescan_interval_sec),
            move || {
                let cache = cache.clone();
                let scheduler = Arc::clone(&scheduler);
                tokio::spawn(async move {
                    cache.rescan_origins().await;
                    scheduler.seed_cold_start(
                        cache
                            .origin_held_hashes()
                            .into_iter()
                            .map(|h| decdn_protocol::ContentHash::from_bytes(*h.as_bytes())),
                    );
                });
            },
        ))
    } else {
        None
    };

    // Oversized pin set starves the eviction driver on EVERY node, because
    // pinned blobs are LRU-exempt however they arrived. Runs unconditionally at
    // startup (and again on reload).
    warn_if_pins_exceed_cache(&infra.cache, cfg.cache.cache_size_mb);

    // DHT bucket-refresh (ADR 022 §Routing Table). Once per hour
    // picks the bucket with the oldest last-refresh timestamp and
    // runs `FindNode(random_id_in_bucket)` against the bucket's
    // freshest peer to repopulate it. Best-effort hygiene — a failed
    // refresh is silently retried on the next tick.
    let (bucket_refresh_stop_tx, bucket_refresh_stop_rx) = oneshot::channel::<()>();
    // Shared "last bucket-refresh completed at" clock (wall-clock µs, 0 =
    // never). The refresh task stamps it each pass; `admin_v1_status`
    // reads it for the routing-table health view (issue #741).
    let bucket_refresh_clock = Arc::new(std::sync::atomic::AtomicU64::new(0));
    tasks.spawn(crate::dht::bucket_refresh::run_bucket_refresh(
        infra.ep.clone(),
        infra.secret_key.public(),
        Arc::clone(&ch.dht_routing),
        bucket_refresh_stop_rx,
        crate::dht::bucket_refresh::BUCKET_REFRESH_TICK,
        Arc::clone(&bucket_refresh_clock),
    ));

    // RPC connectivity watchdog (issue #283). Updates `decdn_rpc_healthy`
    // each tick; a sustained transition fires an alert. `interval == 0`
    // disables the watchdog entirely (operators can opt out for offline
    // dev). Spawned outside the `JoinSet` because we drive it via its own
    // `oneshot` and an explicit `await` during drain, since `JoinSet::abort_all`
    // cancels eagerly and we'd rather let the watchdog observe its `shutdown`
    // arm.
    //
    // Seed the gauge to `1` here unconditionally: the startup
    // `check_rpc_reachability` above already established the endpoint is
    // reachable, and registered Prometheus gauges otherwise default to 0
    // — which alerts would (correctly, by their own logic) read as an
    // outage. Seeding before the watchdog-spawn branch covers the
    // `interval == 0` case too.
    infra.node_metrics.rpc_healthy(true);
    let rpc_watchdog = if cfg.blockchain.rpc_watchdog_interval_sec > 0 {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .context("failed to build HTTP client for RPC watchdog")?;
        let interval = Duration::from_secs(cfg.blockchain.rpc_watchdog_interval_sec);
        let (tx, rx) = oneshot::channel::<()>();
        let handle = spawn_rpc_watchdog(
            client,
            cfg.blockchain.rpc_url.clone(),
            interval,
            Arc::clone(&infra.node_metrics),
            rx,
        );
        Some((tx, handle))
    } else {
        tracing::info!("RPC watchdog disabled (blockchain.rpc_watchdog_interval_sec = 0)");
        None
    };

    // Admin HTTP surface (ADR 025). Bind here so startup fails fast on a port
    // collision rather than after side-effectful subscriptions have
    // registered. The `serve` task itself is spawned later, once the rest of
    // the runtime state it reads is wired.
    let admin_listener = if let Some(admin_port) = cfg.observability.admin_port {
        let admin_addr = std::net::SocketAddr::from(([127, 0, 0, 1], admin_port));
        Some(admin::bind(admin_addr).context("failed to bind admin listener")?)
    } else {
        tracing::info!("admin server disabled (observability.admin_port = 0)");
        None
    };

    // Local per-peer EWMA reputation score store (ADR 008 §Local Score
    // Calculation). Reputation is local-only: a node ranks its peers solely from
    // its own delivery observations — there is no cross-node aggregation. The
    // store feeds node selection via the `NodeOrigin` pull path (provisioned
    // below).
    // Opt into ADR 008 §Score Clamping's ±0.05 per-report clamp (the library
    // default is a no-op cap) so one bad interaction cannot over-penalize an
    // otherwise good peer (#1176).
    let local_reputation = Arc::new(
        decdn_reputation::LocalReputation::new(
            decdn_reputation::LocalReputationConfig::default()
                .with_max_delta_per_update(decdn_reputation::LOCAL_SCORE_MAX_DELTA_PER_REPORT),
        )
        .context("local reputation config invalid")?,
    );

    // Buyer-side PaymentPool bootstrap + node-to-node pull-through
    // provisioning (#831), fully backgrounded off the startup critical path
    // (#1109). The USDC `approve` receipt that `bootstrap` awaits could hang for
    // many minutes on a stuck tx (capped by `APPROVE_RECEIPT_TIMEOUT`);
    // running it inline would gate the metrics/admin binds and the
    // "node runtime ready" banner below. The origin shares its `OnceLock` with
    // the clone already in the cache's origin chain, so a later `provision` from
    // this task arms pull-through; reads before it land as clean misses (no
    // spend, no panic). Bootstrap failure stays NON-fatal — buying is
    // opportunistic cost-recovery.
    //
    // Precompute every cfg/secret-derived value the task needs: the closure is
    // `'static` so it can't borrow `cfg`/`secret_key`, and those are used later.
    let (buyer_bootstrap_stop_tx, buyer_bootstrap_stop_rx) = oneshot::channel::<()>();
    let buyer_voucher_domain = ch.voucher_domain.clone();
    let buyer_working_deposit = U256::from(cfg.blockchain.buyer_working_deposit_micro_usdc);
    let buyer_ensure_max_approval = cfg.blockchain.buyer_max_approve;
    let buyer_signer_address = infra.eth_signer.address();
    let pull_through_enabled = cfg.cache.node_to_node_pull_through_enabled;
    let node_origin_self_id = crate::dht::NodeId::from_bytes(*infra.secret_key.public().as_bytes());
    let node_origin_slash_domain = ch.slash_domain.clone();
    // #1117: the `CapacityBond` bind domain this node signs its node→node client
    // identity binding under — reuses the serving-side `bind_domain` constructed
    // once above so an upstream verifies against an identical domain.
    let node_origin_bind_domain = ch.bind_domain.clone();
    let node_origin_config = crate::node_origin::NodeOriginConfig {
        probe_fanout: cfg.cache.node_pull_probe_fanout,
        pull_timeout: std::time::Duration::from_secs(cfg.cache.node_pull_timeout_sec),
        stall_timeout: std::time::Duration::from_secs(cfg.cache.node_pull_stall_timeout_sec),
        max_blob_size_bytes: cfg
            .cache
            .max_blob_size_mb
            .saturating_mul(decdn_protocol::MB_BYTES),
        max_rate_per_mb: cfg.cache.max_rate_per_mb,
        // Miss pulls open at the working deposit and graduate to it on a mid-pull
        // reactive top-up when a single pull outruns the deposit (#1530). The
        // proactive low-water refill targets the same deposit.
        working_deposit: buyer_working_deposit,
        event_poll_interval: std::time::Duration::from_millis(
            cfg.blockchain.event_poll_interval_ms,
        ),
        lookup: crate::dht::LookupConfig::default(),
        // Own self-attested region for the ADR 030 latency-vs-claim penalty
        // (#1177); `None` disables it (nothing to compare a peer's claim against).
        own_region: cfg.identity.region.clone(),
    };
    // Arc/handle clones for the task — the originals are used later in `run()`.
    let ep_for_buyer = infra.ep.clone();
    let eth_signer_for_buyer = Arc::clone(&infra.eth_signer);
    let resolver_opt = ch.node_address_resolver.clone();
    let node_origin_opt = infra.node_origin.clone();
    let dht_routing_c = Arc::clone(&ch.dht_routing);
    let staker_set_c = Arc::clone(&ch.staker_set);
    let origin_directory_c = Arc::clone(&ch.origin_directory);
    let local_reputation_c = Arc::clone(&local_reputation);
    let registry_regions_c = Arc::clone(&ch.registry_regions);
    let node_metrics_for_buyer = Arc::clone(&infra.node_metrics);
    let node_metrics_for_origin = Arc::clone(&infra.node_metrics);
    let node_origin_engine = infra.cache.clone();
    let mut buyer_bootstrap_stop_rx = buyer_bootstrap_stop_rx;
    let payment_pool_addr_for_buyer = ch.payment_pool_addr;
    tasks.spawn(async move {
        let service = tokio::select! {
            biased;
            // A shutdown that races a slow bootstrap unwinds cleanly here.
            _ = &mut buyer_bootstrap_stop_rx => return,
            res = crate::buyer_channel::BuyerPoolService::bootstrap(
                buyer_wallet_provider,
                payment_pool_addr_for_buyer,
                buyer_signer_address,
                buyer_channel_store,
                eth_signer_for_buyer,
                buyer_voucher_domain,
                buyer_working_deposit,
                buyer_ensure_max_approval,
                node_metrics_for_buyer,
            ) => match res {
                Ok(service) => Arc::new(service),
                Err(err) => {
                    tracing::warn!(
                        err = %sanitize_rpc_display(&err),
                        payment_pool_addr = %payment_pool_addr_for_buyer,
                        "buyer-side PaymentPool bootstrap failed; node→node paid cache-miss \
                         pulls are DISABLED for this process (seller settlement is unaffected). \
                         This condition is sticky — restart the node to retry. Check: (1) \
                         blockchain.payment_pool_address is correct, (2) the RPC endpoint is \
                         reachable, (3) the wallet holds gas for the one-time USDC approve."
                    );
                    if pull_through_enabled {
                        tracing::warn!(
                            "cache.node_to_node_pull_through_enabled is set, but the buyer service \
                             failed to bootstrap; pull-through stays DISABLED this process \
                             (restart to retry)"
                        );
                    }
                    return;
                }
            }
        };

        // Provision the node-to-node pull origin now that the buyer service is
        // up (#831). `node_origin_opt` is `Some` iff the feature is on; the
        // buyer is always present at this point, so the remaining gate is the
        // address resolver. The origin's `OnceLock` is shared with the cache
        // chain's clone, so this set is what actually arms pull-through.
        if let Some(origin) = node_origin_opt.as_ref() {
            if let Some(resolver) = resolver_opt.as_ref() {
                origin.provision(crate::node_origin::NodeOriginDeps {
                    endpoint: ep_for_buyer.clone(),
                    routing_table: dht_routing_c,
                    staker_set: staker_set_c,
                    // FIND_VALUE last-resort fallback when the DHT returns no
                    // providers (ADR 022 §FIND_VALUE Flow; #912). Shares the
                    // single origin directory built above with the reactive
                    // pull-through gate, so a configured `ChainOriginDirectory`
                    // backs this fallback for every node. Absent chain addresses
                    // it is empty (same prior behavior).
                    origin_directory: origin_directory_c,
                    addr_resolver: Arc::clone(resolver),
                    buyer: Arc::clone(&service) as Arc<dyn crate::buyer_channel::PoolOpener>,
                    self_id: node_origin_self_id,
                    slash_domain: node_origin_slash_domain,
                    bind_domain: node_origin_bind_domain,
                    local_rep: local_reputation_c,
                    negative_cache: crate::dht::NegativeProbeCache::new(),
                    probe_cache: crate::dht::PositiveProbeCache::new(),
                    metrics: node_metrics_for_origin,
                    registry_regions: registry_regions_c,
                    config: node_origin_config,
                    // One voucher ledger per provider channel, shared by every concurrent
                    // pull on it (#1145 review). Built here, at the single place the pull
                    // paths' deps are assembled, so both paths necessarily share it —
                    // which is the point: a per-pull ledger makes concurrent pulls collide
                    // on `prior_nonce + 1`. See `buyer_ledgers::BuyerLedgers`.
                    ledgers: Arc::new(crate::buyer_ledgers::BuyerLedgers::default()),
                    // Providers whose channel wedged on a terminal voucher rejection, skipped
                    // in ranking until the channel expires (#1145 review). In-memory, like the
                    // negative cache beside it.
                    wedged_providers: Arc::new(std::sync::Mutex::new(
                        std::collections::HashMap::new(),
                    )),
                    engine: node_origin_engine.clone(),
                });
                tracing::info!(
                    "node-to-node cache-miss pull-through provisioned and enabled (#831)"
                );
            } else {
                tracing::warn!(
                    "cache.node_to_node_pull_through_enabled is set, but the node-address \
                     resolver failed to bootstrap; pull-through stays DISABLED this process \
                     (restart to retry)"
                );
            }
        }

        // Hold the service (and thus its `AbortOnDrop` reclaim/reconcile
        // sweeps) alive until shutdown — reclaim must keep running even when
        // pull-through is off (`node_origin_opt` is `None`).
        let _service = service;
        let _ = buyer_bootstrap_stop_rx.await;
    });

    // Drain trigger for `admin_v1_drain` (issue #244). Constructed
    // unconditionally so the admin handler always has a live target —
    // there is no "drain disabled" state analogous to "no region" or
    // "no config path". The `Arc` is shared with the admin state (via
    // `Arc::clone`) and the select loop arm below.
    let drain_trigger = Arc::new(admin::DrainTrigger::new());

    // Spawn the admin serve task with the full state. Bind happened
    // earlier (see `admin_listener` above) so a port collision would
    // have failed startup before any side-effectful subscribes ran.
    let admin_stop_tx = if let Some(listener) = admin_listener {
        let (tx, rx) = oneshot::channel::<()>();
        // Build the reload hook only when a config file path was passed
        // (CLI-only invocation has nothing on disk to re-read). The
        // hook hands `admin_v1_reload` the same `RuntimeReloadState` and
        // file path the SIGHUP arm uses, so both paths converge on a
        // single mutex-serialised reload — see `admin::AdminRpcImpl::reload`
        // and the SIGHUP arm of the select loop below.
        let reload_hook = config_path.map(|path| admin::ReloadHook {
            reload_state: Arc::clone(reload_state),
            config_path: path.clone(),
        });
        let state = admin::AdminState::new(
            *infra.secret_key.public().as_bytes(),
            started_at,
            infra.cache.clone(),
            reload_hook,
            Arc::clone(&drain_trigger),
            Arc::clone(&infra.node_metrics),
        )
        // DHT introspection for `admin_v1_status` (issue #741). All handles
        // are clones of state the DHT tasks already share — read-only here.
        .with_dht(admin::DhtStatusHandles {
            routing: Arc::clone(&ch.dht_routing),
            staker_set: Arc::clone(&ch.staker_set),
            record_store: Arc::clone(&ch.record_store),
            republish: Arc::clone(&republish_scheduler),
            refresh_clock: Arc::clone(&bucket_refresh_clock),
            refresh_interval: crate::dht::bucket_refresh::BUCKET_REFRESH_TICK,
        })
        // Lane introspection for `admin_v1_lanes` (issue #749). Shares the
        // same persistent lane-state store the client handler and settlement
        // service use, plus a read handle over the handler's live lane
        // registry for the last-voucher clock (issue #1733) — read-only here.
        .with_lanes(admin::LaneStatusHandles {
            pool_store: Arc::clone(&infra.channel_state_store),
            lane_activity: ch.client_handler.lane_activity_clock(),
            redeem_threshold_micro_usdc: cfg.blockchain.redeem_threshold_micro_usdc,
        })
        // Slash-detection introspection for `admin_v1_slashes` (#1032). Shares
        // the in-memory store the watcher appends to — read-only here.
        .with_slash_detection(admin::SlashStatusHandles {
            store: Arc::clone(&ch.slash_store),
        })
        // Bring-up node-id binding check (#1034), already logged during
        // `build_chain_and_handlers`. Surfacing it here too is what lets an
        // operator (or the G-NODE-07 journey) detect the unslashable state
        // without scraping the daemon's log for a WARN line.
        .with_binding(ch.binding_report);
        tasks.spawn(async move {
            if let Err(err) = admin::serve(listener, state, rx).await {
                tracing::error!(%err, "admin server exited with error");
            }
        });
        Some(tx)
    } else {
        None
    };

    // Startup banner (#274). One structured INFO event per restart lets
    // operators correlate log streams across a fleet and across restarts
    // without stitching multiple lines together. Field names are stable —
    // log aggregators key on them.
    tracing::info!(
        event = "startup_banner",
        node_id = %infra.secret_key.public(),
        version = env!("CARGO_PKG_VERSION"),
        region = cfg.identity.region.as_deref().unwrap_or(""),
        bind_port = cfg.network.bind_port,
        metrics_addr = %metrics_addr,
        admin_port = ?cfg.observability.admin_port,
        rate_per_mb = cfg.payment.rate_per_mb,
        cache_dir = %cfg.cache.cache_dir.display(),
        has_origin = !cfg.cache.origins.is_empty(),
        origin_count = cfg.cache.origins.len(),
        origin_kinds = %origin_kinds_label(&cfg.cache.origins),
        "node runtime ready"
    );

    Ok(Background {
        metrics_stop_tx,
        dispatch_gc_stop_tx,
        origin_rescan_stop_tx,
        record_store_gc_stop_tx,
        eviction_stop_tx,
        dht_rate_limit_gc_stop_tx,
        probe_rate_limit_gc_stop_tx,
        republish_stop_tx,
        bucket_refresh_stop_tx,
        buyer_bootstrap_stop_tx,
        rpc_watchdog,
        admin_stop_tx,
        drain_trigger,
        tasks,
    })
}

/// Build the endpoint, register handlers on a `Router`, spawn the metrics
/// server and other background tasks, and run until a shutdown signal is
/// received.
///
/// SIGHUP triggers a hot-reload of mutable config fields via
/// [`RuntimeReloadState`] (see issue #236). Other signals
/// (SIGINT/SIGTERM) trigger graceful shutdown.
///
/// The signal streams (SIGHUP, SIGTERM) are registered **once** before
/// the select loop and reused on every iteration. Re-creating
/// `tokio::signal::unix::Signal` each iteration would race with signal
/// delivery: a SIGHUP that arrives while `reload_runtime_config(..)` is
/// running would have nowhere to land if the future holding the
/// `Signal` had already been dropped, and would be silently lost. The
/// persistent stream queues the signal until the next `recv()` call
/// (kernel-managed, with coalescing) so concurrent or rapidly-repeated
/// signals are observed deterministically.
pub async fn run(
    cfg: ResolvedConfig,
    config_path: Option<PathBuf>,
    reload_state: Arc<RuntimeReloadState>,
) -> anyhow::Result<()> {
    // Captured at the very top of `run()`, before any `await` or I/O,
    // so `admin_v1_health.uptime_s` reflects the entire process lifetime
    // — including the RPC reachability preflight below (which can spend
    // up to its 5s timeout on flaky networks). Operators reasoning about
    // "how long has this node been up?" want every second since `decdn
    // run` was invoked, not just everything after the admin server bound.
    let started_at = std::time::Instant::now();

    let infra = build_infra(&cfg, &reload_state).await?;

    let ch = build_chain_and_handlers(&cfg, &reload_state, &infra).await?;

    let bg = spawn_background_tasks(
        &cfg,
        config_path.as_ref(),
        &reload_state,
        &infra,
        &ch,
        started_at,
    )
    .await?;

    let (router, signal) = serve_until_shutdown(
        &reload_state,
        config_path.as_deref(),
        &bg.drain_trigger,
        ServeInputs {
            ep: infra.ep,
            probe_handler: ch.probe_handler,
            client_handler: ch.client_handler,
            dht_handler: ch.dht_handler,
            blacklist_ready_rx: ch.blacklist_ready_rx,
        },
    )
    .await?;

    let handles = ShutdownHandles {
        metrics_stop_tx: bg.metrics_stop_tx,
        dispatch_gc_stop_tx: bg.dispatch_gc_stop_tx,
        buyer_bootstrap_stop_tx: bg.buyer_bootstrap_stop_tx,
        origin_rescan_stop_tx: bg.origin_rescan_stop_tx,
        record_store_gc_stop_tx: bg.record_store_gc_stop_tx,
        eviction_stop_tx: bg.eviction_stop_tx,
        dht_rate_limit_gc_stop_tx: bg.dht_rate_limit_gc_stop_tx,
        probe_rate_limit_gc_stop_tx: bg.probe_rate_limit_gc_stop_tx,
        republish_stop_tx: bg.republish_stop_tx,
        bucket_refresh_stop_tx: bg.bucket_refresh_stop_tx,
        admin_stop_tx: bg.admin_stop_tx,
        rpc_watchdog: bg.rpc_watchdog,
        poller: ch.poller,
        receipt_writer_shutdown: infra.receipt_writer_shutdown,
        payment_service: ch.payment_service,
        receipt_writer: infra.receipt_writer,
        lane_flush_task: infra.lane_flush_task,
        channel_state_store: infra.channel_state_store,
        node_metrics: infra.node_metrics,
        tasks: bg.tasks,
    };
    shutdown(handles, router, signal, &bg.drain_trigger, infra.cache).await
}

/// Owned teardown state handed from [`run`] to [`shutdown`]. Every field is a
/// stop signal, watcher handle, or task handle that the graceful-shutdown
/// sequence consumes exactly once; the exhaustive destructure at the top of
/// [`shutdown`] (no `..` rest) is a deliberate safety net — a field left
/// unconsumed becomes an unused-variable error under `-D warnings`, proving no
/// stop signal was dropped on the floor.
struct ShutdownHandles<P: Provider + Clone + 'static> {
    metrics_stop_tx: oneshot::Sender<()>,
    dispatch_gc_stop_tx: oneshot::Sender<()>,
    buyer_bootstrap_stop_tx: oneshot::Sender<()>,
    /// Origin-held-index periodic rescan (#1130). `None` when
    /// `cache.fs_rescan_interval_sec == 0` disables it.
    origin_rescan_stop_tx: Option<oneshot::Sender<()>>,
    record_store_gc_stop_tx: oneshot::Sender<()>,
    eviction_stop_tx: oneshot::Sender<()>,
    dht_rate_limit_gc_stop_tx: oneshot::Sender<()>,
    probe_rate_limit_gc_stop_tx: oneshot::Sender<()>,
    republish_stop_tx: oneshot::Sender<()>,
    bucket_refresh_stop_tx: oneshot::Sender<()>,
    admin_stop_tx: Option<oneshot::Sender<()>>,
    rpc_watchdog: Option<(oneshot::Sender<()>, tokio::task::JoinHandle<()>)>,
    /// The single multiplexed chain-event poller. Stopped once, late (after
    /// `router.shutdown`), because its capacity-bond route gates DHT admission
    /// through drain — see the cancel site in [`shutdown`].
    poller: crate::chain_events::resumable_watcher::WatcherHandle,
    receipt_writer_shutdown: CancellationToken,
    payment_service: PoolSettlementService<P>,
    receipt_writer: tokio::task::JoinHandle<()>,
    /// Periodic lane-store flush timer, aborted below after one final durable
    /// flush of `channel_state_store`.
    lane_flush_task: tokio::task::JoinHandle<()>,
    channel_state_store: Arc<dyn PoolStateStore>,
    node_metrics: Arc<metrics::Metrics>,
    tasks: JoinSet<()>,
}

/// Graceful teardown extracted verbatim from the tail of [`run`] (issue #1253
/// PR1). Consumes every field of [`ShutdownHandles`] via an exhaustive
/// destructure — see that type's docs for why the `..`-free binding is
/// load-bearing. The teardown ordering here is itself load-bearing (metrics
/// accept-loop stop first; the multiplexed `poller` stops after
/// `router.shutdown`) and must not be reordered.
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
async fn shutdown<P: Provider + Clone + 'static>(
    handles: ShutdownHandles<P>,
    router: Router,
    signal: ShutdownSignal,
    drain_trigger: &Arc<admin::DrainTrigger>,
    cache: CacheEngine,
) -> anyhow::Result<()> {
    let ShutdownHandles {
        metrics_stop_tx,
        dispatch_gc_stop_tx,
        buyer_bootstrap_stop_tx,
        origin_rescan_stop_tx,
        record_store_gc_stop_tx,
        eviction_stop_tx,
        dht_rate_limit_gc_stop_tx,
        probe_rate_limit_gc_stop_tx,
        republish_stop_tx,
        bucket_refresh_stop_tx,
        mut admin_stop_tx,
        rpc_watchdog,
        poller,
        receipt_writer_shutdown,
        payment_service,
        receipt_writer,
        lane_flush_task,
        channel_state_store,
        node_metrics,
        mut tasks,
    } = handles;

    // Signal the HTTP accept loops to stop *before* awaiting
    // `router.shutdown()`. Router shutdown can block indefinitely if a
    // protocol handler is slow, and while it's blocked the metrics/admin
    // servers would otherwise keep accepting fresh loopback connections —
    // wasting the outer `SHUTDOWN_DEADLINE` budget and emitting misleading
    // "still serving" signals. The accept loops are cheap to unwind, so
    // stopping them first is strictly cleaner.
    if metrics_stop_tx.send(()).is_err() {
        // Receiver dropped → the metrics server task already exited on
        // its own. The spawn closure logs its own error on abnormal exit
        // (see `metrics server exited with error` above), so this branch
        // is purely informational: under a healthy shutdown we'd have
        // been the ones signaling it.
        tracing::warn!("metrics server exited before shutdown signal was sent");
    }
    // Best-effort: silently ignore the dispatch GC stop send failure.
    // The task only exits early on a panic, and its panic surfaces
    // through `JoinSet::join_next` during the drain phase below — no
    // operator-actionable signal to log at this seam.
    let _ = dispatch_gc_stop_tx.send(());
    // Cancel a still-running buyer bootstrap and release the task's
    // process-lifetime hold on the service (#1109). Best-effort: on the
    // bootstrap-failed path the task already returned and dropped the receiver,
    // so this send errors harmlessly.
    let _ = buyer_bootstrap_stop_tx.send(());
    // origin-held-index periodic rescan (#1130)
    if let Some(tx) = origin_rescan_stop_tx {
        let _ = tx.send(());
    }
    let _ = record_store_gc_stop_tx.send(());
    // Eviction driver (#1173): best-effort stop, same shape as the GC sweeps.
    // The task also drains via `JoinSet::join_next` below.
    let _ = eviction_stop_tx.send(());
    let _ = dht_rate_limit_gc_stop_tx.send(());
    let _ = probe_rate_limit_gc_stop_tx.send(());
    let _ = republish_stop_tx.send(());
    let _ = bucket_refresh_stop_tx.send(());
    // The five chain watchers are now one multiplexed poller with one shutdown
    // token, so the previous staggered per-watcher stops collapse to a SINGLE
    // `poller.shutdown()` at the LATE point below (after `router.shutdown`). The
    // blacklist and rate-bounds routes, which used to stop here early, move to
    // that late stop: it is harmless — neither carries a durable cursor to flush,
    // and neither is consulted for a drain-time decision (unlike the
    // capacity-bond route's staker set), so they only keep applying
    // compliance/rate updates a little longer.
    // Admin server shutdown is ordered per `admin_stop_order`:
    //
    //   - `Early` (the default, including SIGTERM/SIGINT and plain
    //     `admin_v1_drain`): stop admin *before* `router.shutdown` so
    //     admin doesn't keep accepting fresh loopback connections
    //     during drain. This is the original ordering (see
    //     `appendix-local-admin-http`).
    //   - `AfterRouter` (only when an `admin_v1_drain` call carried
    //     `wait_admin: true`, i.e. `decdn node drain --wait`): keep
    //     admin alive *through* `router.shutdown` so a polling client
    //     can observe `admin_v1_health.in_flight_streams` reach 0. The
    //     admin stop signal fires below, after `router.shutdown`
    //     returns.
    //
    // `wait_admin` is sampled once into a local; the two `if` blocks
    // below are mutually exclusive, so the single `Option<Sender>` is
    // consumed by exactly one branch. The signal-gating on
    // `AdminDrain` means a sticky `wait_admin=true` from an in-flight
    // RPC that was racing a SIGTERM does *not* flip the runtime onto
    // the AfterRouter path when SIGTERM actually won the select (the
    // operator's stated intent wins; the RPC's request becomes a
    // no-op since drain is already in progress).
    let stop_order = admin_stop_order(signal, drain_trigger);
    if matches!(stop_order, AdminStopOrder::Early)
        && let Some(tx) = admin_stop_tx.take()
        && tx.send(()).is_err()
    {
        tracing::warn!("admin server exited before shutdown signal was sent");
    }
    let rpc_watchdog_handle = if let Some((tx, handle)) = rpc_watchdog {
        if tx.send(()).is_err() {
            tracing::warn!("RPC watchdog exited before shutdown signal was sent");
        }
        Some(handle)
    } else {
        None
    };

    // Router::shutdown waits for ProtocolHandler::shutdown on each handler,
    // then closes the endpoint.
    if let Err(err) = router.shutdown().await {
        tracing::warn!(%err, "router shutdown reported an error");
    }
    // The ONE multiplexed poller driving all five watcher routes stops here,
    // once. It exits cooperatively — cancelling its loop at the next await
    // boundary and flushing every persisting route's checkpoint (settlement's
    // `PoolOpened`) before returning — with its `WatcherHandle`'s `AbortOnDrop`
    // as the backstop.
    //
    // *After* `router.shutdown` deliberately, and the capacity-bond route is why:
    // its projection is the cached active-staker set, which gates DHT `Store`
    // admission and decides which probes the stake-lane reservation sheds.
    // Cancelling it before the drain would freeze that set while the router is
    // still serving, so a membership change landing mid-drain would be missed by
    // exactly the requests still in flight. This binding constraint forces the
    // single stop late; the other four routes (slash, settlement, blacklist,
    // rate-bounds) are not drain-consulted and could stop earlier, but one shared
    // token means they stop here too. `shutdown` only cancels the token — it is
    // not awaited — so settlement's checkpoint flush races the redeem sweep just
    // below rather than completing before it; that is fine because the flush is
    // best-effort and the checkpoint is independent of, and idempotent with
    // respect to, the redeem sweep.
    poller.shutdown();
    // The router has drained, so no further vouchers — and therefore no further
    // receipts — will be produced. Signal the receipt writer to flush whatever
    // is already enqueued and exit; it is awaited in the drain phase below so
    // the audit tail survives shutdown (#803).
    receipt_writer_shutdown.cancel();

    // Redeem on shutdown (#327): now that the router has drained, no further
    // vouchers arrive and the persisted lane state is final. A pool is
    // owner-closed only, so there is nothing to close here. The poller cancel
    // above triggers settlement's tail stop and checkpoint flush asynchronously
    // (best-effort, not awaited before this point), so this quiesces the redeemer
    // and runs one final best-effort redeem sweep so an above-threshold lane is
    // not left un-redeemed. The redeem sweep does not depend on the checkpoint
    // flush having completed — the scan checkpoint is independent lane state and
    // idempotent to re-scan. Bounded by the deadline
    // so a slow RPC cannot hang shutdown.
    payment_service.shutdown(SHUTDOWN_DEADLINE).await;

    // Final durable flush before stop, so the last interval of frontier lands.
    // The router has drained and the redeem sweep above already ran, so the
    // in-memory lane state is final; this is the last write before the
    // background flush task is aborted below.
    let flush_result = match tokio::task::spawn_blocking(move || channel_state_store.flush()).await
    {
        Ok(result) => result,
        Err(join_err) => Err(decdn_incentive::StoreError::Backend(format!(
            "flush join: {join_err}"
        ))),
    };
    if let Err(err) = flush_result {
        node_metrics.lane_flush_failure();
        tracing::warn!(%err, "final lane store flush on shutdown failed");
    }
    lane_flush_task.abort();

    // Late admin stop (issue #604 `AfterRouter` path). The polling
    // client (`decdn node drain --wait`) needed admin to stay open
    // while `router.shutdown` awaited the last in-flight client
    // streams; now that it's returned, tear admin down so the polling
    // client either sees `in_flight_streams == 0` on its next tick or
    // observes ECONNREFUSED — both of which it treats as "drain
    // complete".
    //
    // If the receiver has already dropped (i.e. the admin task
    // crashed/panicked between the Early skip and this point), that's
    // a violated invariant — `decdn node drain --wait` clients may
    // have misread the ECONNREFUSED as drain completion while
    // in-flight streams were still draining. Log at `error!`
    // accordingly so post-mortems surface it.
    if matches!(stop_order, AdminStopOrder::AfterRouter)
        && let Some(tx) = admin_stop_tx.take()
        && tx.send(()).is_err()
    {
        tracing::error!(
            "admin server exited while runtime was awaiting router.shutdown; \
             `decdn node drain --wait` clients may have observed ECONNREFUSED \
             before in-flight streams completed (false drain-complete)"
        );
    }

    // Flush the cache store before the drain deadline so in-flight writes
    // hit disk. Intentionally *not* gated by `SHUTDOWN_DEADLINE`: a slow
    // flush is preferable to a lost write, and the watchdog at the outer
    // process level catches a truly stuck shutdown. On failure we still
    // finish the task drain cleanly (so metrics/dispatch don't leak) and
    // then bubble the error out of `run()` — supervisors need a non-zero
    // exit to know the store may be inconsistent.
    let cache_shutdown_err = cache.shutdown().await.err();
    if let Some(err) = cache_shutdown_err.as_ref() {
        tracing::error!(%err, "cache shutdown failed; store state may be inconsistent");
    }

    // Last-resort abort handle for the RPC watchdog, which lives *outside* the
    // `JoinSet`. On the normal path the watchdog oneshot drains it cleanly;
    // this is fired only if `drain` overruns `SHUTDOWN_DEADLINE`, because
    // dropping the timed-out `drain` future would otherwise merely *detach*
    // a wedged task (a dropped `JoinHandle` keeps running), not stop it.
    // `tasks.abort_all()` covers the `JoinSet` (reaped + logged per task
    // below); this covers the watchdog as a fire-and-forget abort — we do NOT
    // await it past the deadline, since `abort()` only lands at a poll point
    // and a truly non-yielding loop would re-hang the shutdown the timeout
    // escaped.
    let watchdog_abort = rpc_watchdog_handle
        .as_ref()
        .map(tokio::task::JoinHandle::abort_handle);
    // The receipt writer also lives outside `tasks`; same fire-and-forget abort
    // backstop if the drain overruns the deadline (#803).
    let receipt_writer_abort = receipt_writer.abort_handle();

    let drain = async {
        while let Some(result) = tasks.join_next().await {
            log_join_result(result, "shutdown");
        }
        // Await the receipt writer so the runtime doesn't return while it's
        // still flushing its tail. The cancel (above) makes it drain the queue
        // and return cleanly; a panic surfaces at `warn` and a cancellation at
        // `debug`, matching how every other drained task is logged.
        log_join_result(receipt_writer.await, "receipt-writer-shutdown");
        // Await the RPC watchdog. We signalled it via oneshot above, so
        // a healthy run resolves cleanly here. A panic surfaces as a
        // warning; cancellation is silent (same as the receipt writer).
        if let Some(handle) = rpc_watchdog_handle
            && let Err(err) = handle.await
            && !err.is_cancelled()
        {
            tracing::warn!(%err, "RPC watchdog task panicked during shutdown");
        }
    };
    if tokio::time::timeout(SHUTDOWN_DEADLINE, drain).await.is_ok() {
        tracing::info!("graceful shutdown complete");
    } else {
        tracing::warn!(
            deadline = ?SHUTDOWN_DEADLINE,
            out_of_joinset_aborts = usize::from(watchdog_abort.is_some()) + 1,
            "graceful shutdown timed out; aborting remaining tasks",
        );
        tasks.abort_all();
        // The RPC watchdog lives outside `tasks`; dropping the timed-out
        // `drain` only detached it, so abort explicitly. Unlike the
        // `JoinSet`, it is not reaped/awaited afterwards (see the rationale
        // where the abort handle is collected) — the count above is its
        // only per-shutdown record.
        if let Some(abort) = &watchdog_abort {
            abort.abort();
        }
        // Aborting the writer mid-tail-drain discards any still-enqueued audit
        // receipts. This only happens once shutdown has already blown its
        // deadline (an abnormal, already-warned event), but name it specifically
        // so a post-mortem can correlate a gap in `download_receipts.jsonl` with
        // the overrun — the drop counter does not cover this path (#803).
        tracing::warn!(
            event = "receipt_writer_aborted",
            "receipt writer aborted at shutdown deadline; enqueued audit receipts may be lost"
        );
        receipt_writer_abort.abort();
        while let Some(result) = tasks.join_next().await {
            log_join_result(result, "abort");
        }
    }

    tracing::info!("node stopped");
    match cache_shutdown_err {
        None => Ok(()),
        Some(err) => Err(anyhow::Error::from(err).context("cache shutdown failed")),
    }
}

/// Build an iroh `Endpoint` bound to `bind_port`. ALPNs are set by the
/// `Router` when it spawns. The bind address is included in the error
/// message so a port collision surfaces as a config-level diagnostic
/// (matching `metrics::bind` and `admin::bind`), rather than an opaque
/// "endpoint bind failed".
///
/// No transient pre-bind probe: it would introduce a TOCTOU window
/// between the probe and the real bind, and iroh's
/// `Endpoint::builder().bind_addr(...).bind()` owns its UDP socket
/// internally — there is nothing to hand off. Single-bind matches the
/// existing TCP listeners.
async fn build_endpoint(
    secret_key: &SecretKey,
    bind_port: u16,
    relay_urls: &[String],
    discovery: &ResolvedDiscovery,
    transport_config: QuicTransportConfig,
) -> anyhow::Result<Endpoint> {
    let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, bind_port);

    // Base preset selection is the discovery-provider seam (#818, see
    // adr/appendix-poc-production-seams.md): with no `network.discovery` keys we
    // keep `presets::N0` (n0-hosted pkarr/DNS lookup + n0 relay defaults,
    // unchanged back-compat); with operator discovery configured we drop to
    // `presets::Minimal` (crypto provider only) and compose exactly the
    // configured lookup legs. Discovery and relay are independent legs:
    // `Minimal` sets NO relay mode, so when no custom relay map is configured we
    // must restore the n0 relay default the `N0` preset would have applied —
    // dropping the n0 *discovery* leg must not silently disable relays. A custom
    // `network.relay_urls` map (if any) is applied for both bases below.
    let mut builder = if discovery.is_empty() {
        Endpoint::builder(presets::N0)
    } else {
        let mut b = add_discovery_lookups(Endpoint::builder(presets::Minimal), discovery)?;
        if relay_urls.is_empty() {
            b = b.relay_mode(RelayMode::Default);
        }
        b
    };

    builder = builder
        .secret_key(secret_key.clone())
        .transport_config(transport_config);

    // A configured `network.relay_urls` list swaps the n0 default relay map for
    // the operator's self-hosted relays (`RelayMode::Custom`). The relay leg is
    // independent of the discovery leg selected above: a custom relay map routes
    // connectivity through the operator's relays while NodeId→address discovery
    // uses whichever provider the base-preset branch wired (n0 pkarr/DNS, or the
    // operator's `network.discovery` providers). Multiple entries give
    // redundancy/failover. A custom relay sits on the
    // NAT-traversal critical path, so we probe reachability at bring-up — but
    // the probe is advisory, never fatal: if every relay we could probe was
    // unreachable we log a loud warning and proceed, trusting iroh's background
    // retry, rather than turning a *transient* relay outage into a node outage
    // (a correlated relay blip during a rolling restart must not wedge the
    // fleet). Note the probe is a bare TCP connect, so it proves liveness of the
    // host:port, not relay-protocol health — a fronting LB / reverse proxy can
    // accept the connection while the relay behind it is unhealthy.
    if !relay_urls.is_empty() {
        let relays = parse_relay_urls(relay_urls)?;
        let tally = probe_relays(&relays).await;
        // Advisory only: warn loudly when at least one relay was actually probed
        // and none were reachable, but proceed regardless. Relays we couldn't
        // probe (no derivable host/port — iroh may still route them) are
        // excluded; their per-relay "skipping" warning already fired.
        if tally.reachable == 0 && tally.unprobeable < relays.len() {
            tracing::warn!(
                "none of the {} probeable network.relay_urls were reachable at bring-up; \
                 proceeding and letting iroh retry in the background — check the URLs and \
                 that a relay is up, or clear network.relay_urls to fall back to the n0 relays",
                relays.len().saturating_sub(tally.unprobeable)
            );
        }
        builder = builder.relay_mode(RelayMode::Custom(RelayMap::from_iter(relays)));
    }

    builder
        .bind_addr(bind_addr)
        .map_err(|e| anyhow::anyhow!("invalid bind addr {bind_addr}: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("endpoint bind {bind_addr} failed: {e}"))
}

/// Compose the operator-configured `network.discovery` address-lookup legs onto
/// `builder` (#818 scope 1) — the `node`-side half of the discovery-provider
/// seam: config resolution holds plain shape-validated Strings, and this is
/// where they become iroh providers.
///
/// - `pkarr_url` + `dns_origin` wire a `PkarrPublisher` (this node publishes its
///   signed address record) and a `DnsAddressLookup` (it resolves peers via DNS
///   TXT). The endpoint builder pulls this node's secret key and TLS config into
///   the publisher automatically, so we do not pass the key here.
/// - `peers` seed a `MemoryLookup` static address book.
///
/// The `pkarr_url` (parsed as `url::Url`) and peer `node_id` (parsed as
/// `iroh::PublicKey`) are re-parsed here with the exact parsers config
/// resolution used, so a failure on those legs is an internal invariant break.
/// A peer `relay_url` is parsed as the stricter `iroh::RelayUrl` here while
/// resolution only checked it as a generic `url::Url` (mirroring the top-level
/// `network.relay_urls` path, where bring-up's `RelayUrl` parse is the
/// authoritative relay-shape gate) — so that leg can legitimately fail here on a
/// relay-specific shape issue. Every failure is surfaced as a bring-up error
/// (never a panic), and any echoed URL is `redact_userinfo`'d, upholding the
/// same no-credentials-in-errors invariant as the relay path.
fn add_discovery_lookups(
    mut builder: iroh::endpoint::Builder,
    discovery: &ResolvedDiscovery,
) -> anyhow::Result<iroh::endpoint::Builder> {
    if let Some(pkarr) = &discovery.pkarr_url {
        let url = pkarr.parse::<url::Url>().map_err(|e| {
            anyhow::anyhow!(
                "invalid network.discovery.pkarr_url {:?}: {e}",
                redact_userinfo(pkarr)
            )
        })?;
        builder = builder.address_lookup(PkarrPublisher::builder(url));
    }
    if let Some(origin) = &discovery.dns_origin {
        builder = builder.address_lookup(DnsAddressLookup::builder(origin.clone()));
    }
    if !discovery.peers.is_empty() {
        let mut infos = Vec::with_capacity(discovery.peers.len());
        for peer in &discovery.peers {
            let id = peer.node_id.parse::<PublicKey>().map_err(|e| {
                anyhow::anyhow!("invalid network.discovery peer id {}: {e}", peer.node_id)
            })?;
            let mut addr = EndpointAddr::new(id);
            if let Some(relay) = &peer.relay_url {
                let relay_url = relay.parse::<RelayUrl>().map_err(|e| {
                    anyhow::anyhow!(
                        "invalid relay_url {:?} for network.discovery peer {}: {e}",
                        redact_userinfo(relay),
                        peer.node_id
                    )
                })?;
                addr = addr.with_relay_url(relay_url);
            }
            for a in &peer.addrs {
                let sock = a.parse::<std::net::SocketAddr>().map_err(|e| {
                    anyhow::anyhow!(
                        "invalid addr {a:?} for network.discovery peer {}: {e}",
                        peer.node_id
                    )
                })?;
                addr = addr.with_ip_addr(sock);
            }
            infos.push(addr);
        }
        builder = builder.address_lookup(MemoryLookup::from_endpoint_info(infos));
    }
    Ok(builder)
}

/// Parse the configured relay URL strings into `RelayUrl`s, surfacing the
/// offending entry on a parse failure. The entry is `redact_userinfo`'d first:
/// a malformed URL can still carry `user:pass@` credentials, and this node
/// never lets relay userinfo reach the logs (same invariant the probe upholds).
fn parse_relay_urls(urls: &[String]) -> anyhow::Result<Vec<RelayUrl>> {
    urls.iter()
        .map(|u| {
            u.parse::<RelayUrl>().map_err(|e| {
                anyhow::anyhow!(
                    "invalid network.relay_urls entry {:?}: {e}",
                    redact_userinfo(u)
                )
            })
        })
        .collect()
}

/// Outcome tally from probing a relay set. `reachable` + `unprobeable` +
/// (implicit) unreachable == the number of relays probed.
#[derive(Debug, Default, PartialEq, Eq)]
struct RelayProbeTally {
    /// Relays a TCP connect reached.
    reachable: usize,
    /// Relays excluded from the gate: no derivable host/port to probe, or
    /// (defensively) a probe task that failed to join.
    unprobeable: usize,
}

/// Probe the relays concurrently and tally the outcomes. Per-relay failures are
/// logged (not fatal); the caller decides the bring-up gate.
///
/// Concurrency matters here: a serial loop would add ~8s per unreachable relay
/// (3 attempts × 2s timeout plus backoff) to bring-up, so a handful of down
/// relays could stall startup for tens of seconds. Spawning the probes bounds
/// the wait to roughly a single relay's probe time. The tally is
/// order-independent, so join order doesn't matter.
async fn probe_relays(relays: &[RelayUrl]) -> RelayProbeTally {
    let mut set = JoinSet::new();
    for relay in relays {
        let relay = relay.clone();
        set.spawn(async move { probe_relay(&relay).await });
    }

    let mut tally = RelayProbeTally::default();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Some(true)) => tally.reachable = tally.reachable.saturating_add(1),
            Ok(Some(false)) => {} // probed and unreachable — feeds the gate implicitly
            Ok(None) => tally.unprobeable = tally.unprobeable.saturating_add(1),
            // A probe task never panics; if one somehow failed to join, don't
            // let that internal error hard-fail bring-up — treat it as
            // "couldn't probe" so it's excluded from the reachability gate.
            Err(e) => {
                tracing::warn!("relay probe task failed to join: {e}");
                tally.unprobeable = tally.unprobeable.saturating_add(1);
            }
        }
    }
    tally
}

/// Reachability probe for a single self-hosted relay. Resolves the relay's
/// `host:port` and delegates the connect-with-retry to [`relay_host_reachable`].
/// Returns `Some(true)`/`Some(false)` for a reachable/unreachable probe, or
/// `None` when the URL has no host/port we can probe (iroh may still route it,
/// so the caller treats this as "unknown", not a failure).
async fn probe_relay(relay: &RelayUrl) -> Option<bool> {
    let Some((host, port)) = relay_host_port(relay) else {
        tracing::warn!("relay URL has no host/port to probe; skipping its reachability check");
        return None;
    };
    Some(relay_host_reachable(&host, port).await)
}

/// Retry loop behind [`probe_relay`]: a few short TCP connects with a backoff,
/// returning `true` on the first success.
async fn relay_host_reachable(host: &str, port: u16) -> bool {
    const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
    const BACKOFF: Duration = Duration::from_secs(1);
    const ATTEMPTS: u32 = 3;

    for attempt in 1..=ATTEMPTS {
        match relay_connect_once(host, port, ATTEMPT_TIMEOUT).await {
            Ok(()) => return true,
            Err(reason) => {
                tracing::debug!("relay {host}:{port} probe attempt {attempt}/{ATTEMPTS}: {reason}");
            }
        }
        if attempt < ATTEMPTS {
            tokio::time::sleep(BACKOFF).await;
        }
    }
    tracing::warn!(
        "relay {host}:{port} unreachable after {ATTEMPTS} attempts; iroh will keep retrying it in the background"
    );
    false
}

/// A single TCP connect attempt with a timeout, mapping every outcome to a
/// short reason string for logging.
async fn relay_connect_once(host: &str, port: u16, timeout: Duration) -> Result<(), String> {
    match tokio::time::timeout(timeout, tokio::net::TcpStream::connect((host, port))).await {
        Ok(Ok(_stream)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!("timed out after {timeout:?}")),
    }
}

/// Extract `(host, port)` from a relay URL for the TCP probe, falling back to
/// the scheme's well-known port. The host is un-bracketed for IPv6 literals.
fn relay_host_port(relay: &RelayUrl) -> Option<(String, u16)> {
    let host = relay.host_str()?;
    let port = relay.port_or_known_default()?;
    Some((unbracket_host(host).to_string(), port))
}

/// Strip the surrounding brackets `url::Url::host_str` adds to IPv6 literals
/// (`[::1]` → `::1`). `TcpStream::connect`'s `ToSocketAddrs` impl rejects the
/// bracketed form, so the host must be un-bracketed before dialing. Hosts
/// without a matching bracket pair (domains, IPv4) pass through unchanged.
fn unbracket_host(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// Resolve the keystore password from CLI/env/prompt, then decrypt the
/// keystore JSON via alloy's KDF on a blocking thread. The Arbitrum Sepolia
/// chain id is bound on the signer so EIP-712 signers and any
/// `eth_sendTransaction` paths inherit a deterministic value. To target a
/// different chain, thread the value through `ResolvedBlockchain` next to
/// `rpc_url` (chain-id-keyed config is already a seam pattern; see
/// `appendix-poc-production-seams.md` §Seam 8).
async fn load_eth_signer(cfg: &ResolvedConfig) -> anyhow::Result<PrivateKeySigner> {
    use alloy::signers::Signer;

    let mut sources = vec![PasswordSource::Env(eth_identity::KEYSTORE_PASSWORD_ENV)];
    if let Some(path) = cfg.blockchain.keystore_password_file.clone() {
        sources.push(PasswordSource::File(path));
    }
    sources.push(PasswordSource::Prompt { confirm: false });
    let password = eth_identity::read_password(&sources, "eth keystore password")?;

    let path = cfg.blockchain.eth_keystore.clone();
    // `spawn_blocking` because alloy's `decrypt_keystore` runs scrypt /
    // argon2 (synchronous, CPU-bound). Calling it on the runtime's worker
    // thread would stall every other task for the duration of the KDF.
    let signer = tokio::task::spawn_blocking(move || eth_identity::load_signer(&path, &password))
        .await
        .map_err(|e| anyhow::anyhow!("keystore decrypt task panicked: {e}"))??;
    // Bind the signer to the *configured* chain id, not a hardcoded
    // constant, so the signer's chain id and the `slash_sig` EIP-712 domain
    // chain id (also `cfg.blockchain.chain_id`, see the slash domain build)
    // can never silently diverge — e.g. when the chain target changes via
    // the seam in `appendix-poc-production-seams.md` §Seam 8. Note: raw
    // EIP-712 `sign_hash_sync` does not consult the signer's bound chain
    // id, so this binding only matters for any future `eth_sendTransaction`
    // path; keeping it single-sourced is defensive against that future code.
    Ok(signer.with_chain_id(Some(cfg.blockchain.chain_id)))
}

/// Log a `JoinError` from a shutdown-drained task with a phase label.
fn log_join_result(result: Result<(), tokio::task::JoinError>, phase: &'static str) {
    if let Err(err) = result {
        if err.is_cancelled() {
            tracing::debug!(phase, "task cancelled during shutdown");
        } else {
            tracing::warn!(phase, %err, "task failed during shutdown");
        }
    }
}

/// Construct the cache engine from resolved config. The
/// `[cache.origins]` array picks an ordered list of backends — HTTP,
/// filesystem, or S3 (#437, #284). The singular `[cache.origin]` TOML
/// form is collapsed by the resolver into a single-element vec, so
/// this function sees one canonical representation. An empty vec means
/// no pull-through is configured; the engine then serves only
/// already-cached content and cache misses surface as
/// `CacheError::NoOrigin`.
///
/// Init failure is fail-fast: if origin #i can't be constructed (bad
/// path, S3 auth failure, etc.) the node refuses to start and the
/// context chain identifies which entry. Skip-and-warn would let a
/// misconfigured fallback rot silently until the primary fails — the
/// opposite of operator intent.
///
/// `node_metrics` provides the shared `Arc<CacheMetrics>` that the
/// engine bumps on origin fetches, retry exhaustions (#285), and
/// chain-walk fallback advances (#284).
async fn build_cache(
    cfg: &ResolvedConfig,
    node_metrics: Arc<metrics::Metrics>,
    node_origin: Option<Arc<dyn Origin>>,
) -> anyhow::Result<CacheEngine> {
    let mut origins: Vec<Arc<dyn Origin>> = Vec::with_capacity(cfg.cache.origins.len());
    for (idx, resolved) in cfg.cache.origins.iter().enumerate() {
        let backend: Arc<dyn Origin> = match resolved {
            ResolvedOrigin::Http { url, decompress } => Arc::new(
                HttpOrigin::new_with_user_agent(url.clone(), &cfg.cache.user_agent)
                    .with_context(|| {
                        format!("failed to build HTTP origin client for cache.origins[{idx}]")
                    })?
                    .with_decompress_mode(*decompress),
            ),
            ResolvedOrigin::Fs { path } => {
                Arc::new(FilesystemOrigin::new(path.clone()).await.with_context(|| {
                    format!("failed to open filesystem origin for cache.origins[{idx}]")
                })?)
            }
            // S3-compatible origin (#437 PR2). Conversion from the resolved-
            // config form to the cache-crate's runtime form happens here
            // because `decdn-cache` deliberately doesn't depend on
            // `decdn-common` (the dependency direction is `common -> cache`,
            // and reversing it would be circular).
            ResolvedOrigin::S3(s3_cfg) => Arc::new(
                S3Origin::new(&s3_origin_config_from_resolved(s3_cfg))
                    .await
                    .with_context(|| {
                        format!("failed to construct S3 origin client for cache.origins[{idx}]")
                    })?
                    .with_decompress_mode(s3_cfg.decompress),
            ),
        };
        origins.push(backend);
    }
    // Append the node-to-node pull origin LAST so configured HTTP/FS/S3 origins
    // are tried first and the paid network pull is the final fallback (#831).
    if let Some(node_origin) = node_origin {
        origins.push(node_origin);
    }
    let engine = CacheEngine::open_full(
        &cfg.cache.cache_dir,
        origins,
        cfg.cache.max_blob_size_mb,
        cfg.cache.pinned_hashes.clone(),
        cfg.cache.origin_retry,
        cfg.cache.circuit_breaker,
        Some(node_metrics.cache_metrics()),
        std::time::Duration::from_secs(cfg.cache.gc_interval_sec),
    )
    .await
    .context("failed to open cache engine")?;
    // `cache.*` is restart-required (not hot-reloaded), so applying the
    // probe-hold budget once here is sufficient (ADR 005 §Hold budget, #318).
    engine.set_max_probe_holds(cfg.cache.max_probe_holds);
    // Live-origin probe memo (#1130 pt3) — likewise restart-configured once.
    engine.set_origin_probe_config(
        std::time::Duration::from_secs(cfg.cache.origin_probe_ttl_sec),
        std::time::Duration::from_millis(cfg.cache.origin_probe_timeout_ms),
        usize::try_from(cfg.cache.origin_probe_memo_capacity).unwrap_or(usize::MAX),
    );
    Ok(engine)
}

/// Translate the `decdn-common` resolved-config S3 form into the
/// `decdn-cache` runtime form. The two types carry the same data —
/// they exist as separate types only because `decdn-cache` deliberately
/// doesn't pull `decdn-common` (the dep direction is `common -> cache`).
///
/// Credential redaction (`SecretString::expose`) happens here, at the
/// crate boundary: the SDK call site gets plain `String`s, and the
/// `Debug`-redacted wrapper stays inside the config layer where it
/// guards against incidental log leaks.
fn s3_origin_config_from_resolved(cfg: &decdn_common::config::ResolvedS3Config) -> S3OriginConfig {
    let credentials = cfg.credentials.as_ref().map(|c| match c {
        ResolvedS3Credentials::Static {
            access_key_id,
            secret_access_key,
            session_token,
        } => S3Credentials::Static {
            access_key_id: access_key_id.expose().to_string(),
            secret_access_key: secret_access_key.expose().to_string(),
            session_token: session_token.as_ref().map(|t| t.expose().to_string()),
        },
        ResolvedS3Credentials::DefaultChain { profile } => S3Credentials::DefaultChain {
            profile: profile.clone(),
        },
    });
    S3OriginConfig {
        bucket: cfg.bucket.clone(),
        region: cfg.region.clone(),
        endpoint_url: cfg.endpoint_url.clone(),
        path_style: cfg.path_style,
        prefix: cfg.prefix.clone(),
        credentials,
    }
}

/// Stable comma-separated label for the resolved-origin chain, used
/// in startup logs so operators can grep for `origin_kinds=http,s3`
/// without parsing the structured fields back out. Returns `"none"`
/// when the chain is empty (rather than emitting an empty string) so
/// the field is always present and machine-parseable.
fn origin_kinds_label(origins: &[ResolvedOrigin]) -> String {
    if origins.is_empty() {
        return "none".to_string();
    }
    origins
        .iter()
        .map(|o| match o {
            ResolvedOrigin::Http { .. } => "http",
            ResolvedOrigin::Fs { .. } => "fs",
            ResolvedOrigin::S3(_) => "s3",
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Which OS signal (or admin RPC call) triggered shutdown. Returned by
/// [`ShutdownStreams::recv`] or synthesised by the `drain_trigger` arm so
/// the "shutdown signal received" log line records the cause (SIGINT,
/// SIGTERM, or admin drain) — operators grep that field for post-incident
/// analysis. `Sigterm` is unreachable on non-unix targets.
#[derive(Debug, Clone, Copy)]
enum ShutdownSignal {
    Sigint,
    #[cfg(unix)]
    Sigterm,
    /// Graceful shutdown requested via `admin_v1_drain` (issue #244).
    AdminDrain,
}

impl std::fmt::Display for ShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sigint => f.write_str("SIGINT"),
            #[cfg(unix)]
            Self::Sigterm => f.write_str("SIGTERM"),
            Self::AdminDrain => f.write_str("admin-drain"),
        }
    }
}

/// Whether the admin server's stop signal fires before or after
/// `router.shutdown()` for this drain (issue #604). `Early` is the
/// SIGTERM-equivalent default; `AfterRouter` opts in to keeping the
/// admin port alive so `decdn node drain --wait` can poll
/// `admin_v1_health.in_flight_streams` until it reaches 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdminStopOrder {
    Early,
    AfterRouter,
}

/// Decide the admin shutdown ordering for a drain.
///
/// `AfterRouter` is selected **only** when the drain originated from
/// the admin RPC (`AdminDrain`) *and* the in-flight RPC asked for
/// `wait_admin: true`. Signal-driven shutdowns (SIGINT/SIGTERM)
/// always pick `Early` — even if an `admin_v1_drain { wait_admin:
/// true }` call was in-flight when the signal landed, the operator's
/// stated intent wins. This prevents a "sticky `wait_admin`" race
/// where an RPC stored the flag, SIGTERM won the select, and the
/// runtime would otherwise take the `AfterRouter` path against the
/// operator's signal (issue #604 review).
fn admin_stop_order(signal: ShutdownSignal, drain_trigger: &admin::DrainTrigger) -> AdminStopOrder {
    if matches!(signal, ShutdownSignal::AdminDrain) && drain_trigger.wait_admin() {
        AdminStopOrder::AfterRouter
    } else {
        AdminStopOrder::Early
    }
}

/// Persistent SIGHUP stream, installed once at startup. The
/// `Signal` instance lives on `self` for the lifetime of the runtime
/// so successive SIGHUPs delivered while a reload is in progress are
/// queued by tokio (kernel-coalesced) rather than dropped. Re-creating
/// the `Signal` per iteration of the select loop would race signal
/// delivery — that's the bug this struct exists to prevent.
///
/// On non-Unix targets `recv` returns a `pending` future since SIGHUP
/// doesn't exist there.
struct HupStream {
    #[cfg(unix)]
    hup: Option<tokio::signal::unix::Signal>,
}

impl HupStream {
    fn install() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let hup = match signal(SignalKind::hangup()) {
                Ok(s) => Some(s),
                Err(err) => {
                    tracing::warn!(%err, "failed to install SIGHUP handler; hot-reload disabled");
                    None
                }
            };
            Self { hup }
        }
        #[cfg(not(unix))]
        {
            Self {}
        }
    }

    /// Wait for the next SIGHUP on the persistent stream. If
    /// installation failed (`None`) or the platform isn't Unix, this
    /// future never resolves.
    ///
    /// `Signal::recv` resolves to `Option<()>`. `None` indicates the
    /// underlying stream has been closed (the driver dropped, signal
    /// handler torn down) — once that happens the future would resolve
    /// *immediately, forever*, and the runtime select-loop would
    /// hot-spin. Park on `pending` in that branch instead so hot-reload
    /// is silently disabled rather than turning into a spin-loop.
    async fn recv(&mut self) {
        #[cfg(unix)]
        {
            // Three cases:
            //   1. install succeeded + stream yielded a real signal →
            //      return so the caller runs the reload arm.
            //   2. install succeeded but the stream is now closed →
            //      `recv()` returns `None`, which would otherwise
            //      resolve *immediately, forever* and hot-spin the
            //      runtime's select. Take ownership, log once, and park
            //      on `pending` for the rest of the process.
            //   3. install failed at startup (`self.hup` is `None`) →
            //      already logged a warning at install time; just park
            //      on `pending`.
            if let Some(stream) = self.hup.as_mut() {
                if stream.recv().await.is_some() {
                    return;
                }
                self.hup = None;
                tracing::warn!("SIGHUP stream closed; hot-reload disabled for this process");
            }
            std::future::pending::<()>().await;
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>().await
        }
    }
}

/// Persistent SIGINT/SIGTERM streams. Installed once at startup so
/// rapid-fire shutdown signals (e.g. operator pressing Ctrl-C twice)
/// are coalesced by the kernel rather than potentially lost between
/// re-registrations of `Signal`.
struct ShutdownStreams {
    /// `tokio::signal::ctrl_c` is itself a one-shot future that
    /// re-registers internally — but `tokio::signal::unix::signal`
    /// (used for SIGTERM) is not. We park `ctrl_c()` inside `recv`
    /// directly and rely on tokio's own implementation for SIGINT.
    #[cfg(unix)]
    term: Option<tokio::signal::unix::Signal>,
}

impl ShutdownStreams {
    fn install() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let term = match signal(SignalKind::terminate()) {
                Ok(s) => Some(s),
                Err(err) => {
                    tracing::warn!(
                        %err,
                        "failed to install SIGTERM handler; falling back to SIGINT only",
                    );
                    None
                }
            };
            Self { term }
        }
        #[cfg(not(unix))]
        {
            Self {}
        }
    }

    /// Wait for the next SIGINT or (on Unix) SIGTERM and report which
    /// one fired. The persistent SIGTERM stream lives on `self`, so
    /// successive calls observe successive signals — there's no
    /// race window between iterations of the runtime's select loop.
    ///
    /// `Signal::recv` resolves to `Option<()>`. A `None` from the
    /// SIGTERM arm would otherwise resolve immediately forever and
    /// short-circuit the select to a spurious shutdown; demote that
    /// branch to a `pending` future so we wait for a real SIGINT
    /// instead. (`tokio::signal::ctrl_c` re-registers internally and
    /// is safe to await directly.)
    async fn recv(&mut self) -> ShutdownSignal {
        #[cfg(unix)]
        {
            if let Some(term) = self.term.as_mut() {
                let race = tokio::select! {
                    _ = tokio::signal::ctrl_c() => Some(ShutdownSignal::Sigint),
                    res = term.recv() => res.map(|()| ShutdownSignal::Sigterm),
                };
                if let Some(sig) = race {
                    return sig;
                }
                // SIGTERM arm closed (`None`). Drop the stream so we
                // don't keep racing a future that resolves immediately
                // forever, then wait for a real SIGINT.
                tracing::warn!("SIGTERM stream closed; falling back to SIGINT only");
                self.term = None;
            }
            let _ = tokio::signal::ctrl_c().await;
            ShutdownSignal::Sigint
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            ShutdownSignal::Sigint
        }
    }
}

/// Classification of an RPC preflight/watchdog probe (#1106/#1108). A
/// rate-limited (`429`) or server-side (`5xx`) response — and any
/// timeout/connection error — is [`Transient`](RpcProbe::Transient): the startup
/// preflight retries it with backoff rather than aborting the process, so a
/// commodity endpoint that throttles the startup burst does not crash-loop the
/// node. Any other non-2xx (a `4xx` such as a bad path/auth) is
/// [`Fatal`](RpcProbe::Fatal) — retrying will not help.
enum RpcProbe {
    Healthy,
    Transient(String),
    Fatal(String),
}

/// Number of preflight attempts before a persistent transient failure is treated
/// as fatal (a real outage still aborts bring-up). Backoff caps at 8s, so the
/// worst-case wait is bounded (~5×5s request timeouts + 1+2+4+8s backoff).
const RPC_PREFLIGHT_MAX_ATTEMPTS: u32 = 5;

/// Verify that the JSON-RPC endpoint is reachable by sending a lightweight
/// `net_version` request with a short timeout, retrying a transient (429/5xx/
/// timeout) response with bounded backoff (#1108) — a rate-limited endpoint must
/// not crash-loop the node at startup. A fatal (other 4xx) response, or an
/// exhausted retry budget, aborts bring-up so operators still catch typos and
/// dead endpoints before the node binds ports and starts serving.
async fn check_rpc_reachability(rpc_url: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("failed to build HTTP client for RPC check")?;
    preflight_retry(&client, rpc_url, Duration::from_secs(1)).await
}

/// Retry loop behind [`check_rpc_reachability`], split out so the retry/classify
/// policy is testable with a small `initial_backoff` (the production caller
/// passes 1s). Retries a [`RpcProbe::Transient`] with doubling backoff (capped at
/// 8s) up to [`RPC_PREFLIGHT_MAX_ATTEMPTS`]; a [`RpcProbe::Fatal`] bails at once.
async fn preflight_retry(
    client: &reqwest::Client,
    rpc_url: &str,
    initial_backoff: Duration,
) -> anyhow::Result<()> {
    let mut backoff = initial_backoff;
    for attempt in 1..=RPC_PREFLIGHT_MAX_ATTEMPTS {
        match probe_rpc_classified(client, rpc_url).await {
            RpcProbe::Healthy => {
                tracing::info!("RPC endpoint reachable");
                return Ok(());
            }
            RpcProbe::Fatal(msg) => anyhow::bail!("blockchain.rpc_url {msg}"),
            RpcProbe::Transient(msg) if attempt == RPC_PREFLIGHT_MAX_ATTEMPTS => {
                anyhow::bail!(
                    "blockchain.rpc_url {msg}; still failing after {RPC_PREFLIGHT_MAX_ATTEMPTS} attempts"
                );
            }
            RpcProbe::Transient(msg) => {
                tracing::warn!(
                    attempt,
                    backoff_secs = backoff.as_secs(),
                    "RPC preflight transient failure ({msg}); retrying after backoff"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(8));
            }
        }
    }
    // The loop returns or bails on every path; this is unreachable but keeps the
    // signature total without an `unwrap`/`unreachable!` (anti-panic policy).
    anyhow::bail!("blockchain.rpc_url preflight exhausted retries")
}

/// Issue a single `net_version` JSON-RPC probe and classify the outcome (see
/// [`RpcProbe`]). Shared by the startup preflight and the watchdog so their
/// health verdict can never diverge.
async fn probe_rpc_classified(client: &reqwest::Client, rpc_url: &str) -> RpcProbe {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "net_version",
        "params": [],
        "id": 1
    });

    let resp = match client.post(rpc_url).json(&body).send().await {
        Ok(resp) => resp,
        // A timeout or connection error is transient — the endpoint may be
        // momentarily overloaded (the #1108 startup-burst 429 arrives here as a
        // reset on some gateways) rather than misconfigured.
        Err(err) => {
            return RpcProbe::Transient(format!(
                "is not reachable (timeout or connection error): {}",
                sanitize_rpc_display(&err)
            ));
        }
    };

    let status = resp.status();
    if status.is_success() {
        RpcProbe::Healthy
    } else if status.as_u16() == 429 || status.is_server_error() {
        RpcProbe::Transient(format!(
            "returned status {status} (rate-limited or server error)"
        ))
    } else {
        RpcProbe::Fatal(format!(
            "returned unexpected status {status}; verify the endpoint is a valid JSON-RPC server"
        ))
    }
}

/// Issue a single `net_version` JSON-RPC probe against `rpc_url`. Returns
/// `Ok(())` on a healthy (2xx) response, an error otherwise — a thin adapter over
/// [`probe_rpc_classified`] preserving the watchdog's original pass/fail
/// semantics (it treats any non-2xx as unhealthy for the gauge, transient or not).
pub(crate) async fn probe_rpc(client: &reqwest::Client, rpc_url: &str) -> anyhow::Result<()> {
    match probe_rpc_classified(client, rpc_url).await {
        RpcProbe::Healthy => Ok(()),
        RpcProbe::Transient(msg) | RpcProbe::Fatal(msg) => {
            anyhow::bail!("blockchain.rpc_url {msg}")
        }
    }
}

/// Spawn the RPC connectivity watchdog. On each tick of `interval`, calls
/// [`probe_rpc`] and updates `metrics.rpc_healthy(...)`. Logs a `warn`
/// when health transitions down and an `info` when it transitions back
/// up; steady-state ticks are silent. Stops when `shutdown` resolves.
///
/// The caller is responsible for seeding the gauge before this is
/// spawned — `run()` does so right after the successful
/// `check_rpc_reachability` startup probe. The watchdog only writes the
/// gauge on tick boundaries. The previous-state tracking starts
/// assuming the endpoint is healthy for the same reason, which prevents
/// a spurious "recovered" log on the first tick.
fn spawn_rpc_watchdog(
    client: reqwest::Client,
    rpc_url: String,
    interval: Duration,
    metrics: Arc<metrics::Metrics>,
    mut shutdown: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Caller seeded the gauge from the startup probe; see fn docs.
        let mut prev_healthy = true;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::debug!("RPC watchdog shutdown signal received");
                    return;
                }
                () = tokio::time::sleep(interval) => {}
            }

            let now_healthy = match probe_rpc(&client, &rpc_url).await {
                Ok(()) => {
                    if !prev_healthy {
                        // Deliberately omit the URL: the node configures one
                        // RPC endpoint, and it may carry basic-auth credentials
                        // in the userinfo component.
                        tracing::info!("RPC endpoint healthy");
                    }
                    true
                }
                Err(err) => {
                    if prev_healthy {
                        // Sanitize: `probe_rpc` wraps the reqwest error, whose
                        // Display embeds the full `rpc_url` (an API key may live
                        // in its path/query, not just userinfo). The transport
                        // failure class survives; the URL does not (issue #954).
                        tracing::warn!(err = %sanitize_rpc_display(&err), "RPC endpoint unhealthy");
                    }
                    false
                }
            };
            metrics.rpc_healthy(now_healthy);
            prev_healthy = now_healthy;
        }
    })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// `with_poll_interval` overrides alloy's localhost-detected 250 ms client
    /// poll default — the interval alloy's pending-tx receipt heartbeat polls on
    /// (#1011). Building against a `127.0.0.1` URL exercises the exact path that
    /// triggers it — alloy would seed 250 ms — and the helper must replace it. No
    /// network I/O: `poll_interval()` reads a local atomic on the client.
    #[test]
    fn with_poll_interval_overrides_alloy_local_default() {
        let url: alloy::transports::http::reqwest::Url =
            "http://127.0.0.1:8545".parse().expect("static URL parses");
        let bare = ProviderBuilder::new().connect_http(url.clone());
        // Precondition: alloy seeds the 250 ms localhost default we are fixing.
        assert_eq!(bare.client().poll_interval(), Duration::from_millis(250));

        let provider = with_poll_interval(
            ProviderBuilder::new().connect_http(url),
            Duration::from_secs(7),
        );
        assert_eq!(provider.client().poll_interval(), Duration::from_secs(7));
    }

    #[test]
    fn provider_factory_preserves_role_polling_policy() {
        let url: HttpUrl = "http://localhost:8545".parse().expect("valid URL");
        let interval = Duration::from_secs(7);

        let read = ProviderFactory::read_only(url.clone(), interval);
        let head = ProviderFactory::shared_head(url.clone());
        let seller =
            ProviderFactory::seller_wallet(url.clone(), PrivateKeySigner::random(), interval);
        let buyer = ProviderFactory::buyer_wallet(url, PrivateKeySigner::random(), interval);

        assert_eq!(read.client().poll_interval(), interval);
        assert_eq!(head.client().poll_interval(), Duration::from_millis(250));
        assert_eq!(seller.client().poll_interval(), interval);
        assert_eq!(buyer.client().poll_interval(), interval);
    }

    /// `admin_stop_order` defaults to `Early` (the original
    /// `appendix-local-admin-http` ordering) — the admin server stops
    /// before `router.shutdown` for SIGINT/SIGTERM and for any drain
    /// RPC that didn't ask for `wait_admin`.
    #[test]
    fn admin_stop_order_default_is_early() {
        let trigger = admin::DrainTrigger::new();
        // No fire yet → wait_admin() returns false.
        assert_eq!(
            admin_stop_order(ShutdownSignal::AdminDrain, &trigger),
            AdminStopOrder::Early,
        );
        // A drain RPC that explicitly asks for the default ordering.
        let _ = trigger.fire(false);
        assert_eq!(
            admin_stop_order(ShutdownSignal::AdminDrain, &trigger),
            AdminStopOrder::Early,
        );
    }

    /// `admin_stop_order` returns `AfterRouter` when (and only when)
    /// the drain originated from the admin RPC *and* it asked for
    /// `wait_admin: true`.
    #[test]
    fn admin_stop_order_admin_drain_with_wait_admin_is_after_router() {
        let trigger = admin::DrainTrigger::new();
        let _ = trigger.fire(true);
        assert_eq!(
            admin_stop_order(ShutdownSignal::AdminDrain, &trigger),
            AdminStopOrder::AfterRouter,
        );
    }

    /// SIGINT wins over a sticky `wait_admin=true` flag: an RPC that
    /// stored `wait_admin=true` mid-flight when an operator hit
    /// Ctrl-C must not flip the runtime onto the `AfterRouter` path.
    /// The operator's stated intent (SIGINT = stop now) wins.
    #[test]
    fn admin_stop_order_sigint_overrides_sticky_wait_admin() {
        let trigger = admin::DrainTrigger::new();
        let _ = trigger.fire(true);
        assert_eq!(
            admin_stop_order(ShutdownSignal::Sigint, &trigger),
            AdminStopOrder::Early,
        );
    }

    /// Same as the SIGINT test, but for SIGTERM (unix only).
    #[cfg(unix)]
    #[test]
    fn admin_stop_order_sigterm_overrides_sticky_wait_admin() {
        let trigger = admin::DrainTrigger::new();
        let _ = trigger.fire(true);
        assert_eq!(
            admin_stop_order(ShutdownSignal::Sigterm, &trigger),
            AdminStopOrder::Early,
        );
    }

    #[tokio::test]
    async fn listener_waits_for_successful_initial_blacklist_sync() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (ready_tx, ready_rx) = oneshot::channel();
        let started = Arc::new(AtomicBool::new(false));
        let gate = gate_listener_on_blacklist_sync(ready_rx, {
            let started = Arc::clone(&started);
            move || started.store(true, Ordering::SeqCst)
        });
        tokio::pin!(gate);

        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut gate)
                .await
                .is_err(),
            "listener gate must stay pending while initial blacklist sync is pending"
        );
        assert!(
            !started.load(Ordering::SeqCst),
            "no ALPN listener may start before blacklist readiness"
        );

        ready_tx.send(Ok(())).expect("readiness receiver is live");
        assert!(gate.await.is_ok(), "successful sync should open the gate");
        assert!(
            started.load(Ordering::SeqCst),
            "listener starts only after successful blacklist readiness"
        );
    }

    #[tokio::test]
    async fn listener_stays_closed_when_initial_blacklist_sync_fails() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (ready_tx, ready_rx) = oneshot::channel();
        let started = Arc::new(AtomicBool::new(false));
        ready_tx
            .send(Err("ContentBlacklist getLogs unavailable".to_string()))
            .expect("readiness receiver is live");

        let err = gate_listener_on_blacklist_sync(ready_rx, {
            let started = Arc::clone(&started);
            move || started.store(true, Ordering::SeqCst)
        })
        .await
        .expect_err("failed initial sync must fail startup");

        assert!(
            format!("{err:#}").contains("ContentBlacklist getLogs unavailable"),
            "startup error should preserve the sync failure: {err:#}"
        );
        assert!(
            !started.load(Ordering::SeqCst),
            "listener must remain closed after a failed initial sync"
        );
    }

    #[tokio::test]
    async fn listener_fails_closed_when_watcher_drops_readiness() {
        use std::sync::atomic::{AtomicBool, Ordering};

        // The watcher task died (panicked/aborted) before signaling either
        // outcome: the gate must treat a dropped sender as a hard failure, not
        // hang or silently open the listeners.
        let (ready_tx, ready_rx) = oneshot::channel();
        let started = Arc::new(AtomicBool::new(false));
        drop(ready_tx);

        let err = gate_listener_on_blacklist_sync(ready_rx, {
            let started = Arc::clone(&started);
            move || started.store(true, Ordering::SeqCst)
        })
        .await
        .expect_err("a dropped readiness channel must fail startup closed");

        assert!(
            format!("{err:#}").contains("blacklist watcher exited before initial sync completed"),
            "startup error should explain the watcher exited early: {err:#}"
        );
        assert!(
            !started.load(Ordering::SeqCst),
            "listener must remain closed when the watcher never reported readiness"
        );
    }

    /// Build a minimal `ResolvedConfig` with the cache section
    /// pointed at the given `origin`. Other sections carry sensible
    /// dummies — only the cache is exercised. Mirrors the fixture
    /// shape used in `runtime::reload::tests` and
    /// `crates/node/tests/sighup_signal.rs`.
    ///
    /// Returns the owning `TempDir` alongside the config so the
    /// caller binds it (`let (_tmp, cfg) = ...`) and the directory
    /// lives until end-of-test. A pid-keyed directory is not safe
    /// here: tests in a binary that uses `cargo test` (rather than
    /// `cargo nextest`) share the process and would race on shared
    /// `cache_dir` state inside `CacheEngine::open_full`.
    fn cfg_with_origin(origin: Option<ResolvedOrigin>) -> (tempfile::TempDir, ResolvedConfig) {
        cfg_with_origins(origin.map(|o| vec![o]).unwrap_or_default())
    }

    #[allow(clippy::too_many_lines)] // exhaustive ResolvedConfig test builder
    fn cfg_with_origins(origins: Vec<ResolvedOrigin>) -> (tempfile::TempDir, ResolvedConfig) {
        use decdn_common::config::{
            ResolvedBlockchain, ResolvedIdentity, ResolvedNetwork, ResolvedObservability,
            ResolvedPayment, ResolvedSecurity,
        };
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache_dir = tmp.path().to_path_buf();
        let cfg = ResolvedConfig {
            identity: ResolvedIdentity {
                data_dir: cache_dir.clone(),
                region: None,
            },
            network: ResolvedNetwork {
                bind_port: 4433,
                relay_urls: Vec::new(),
                discovery: decdn_common::config::ResolvedDiscovery::default(),
            },
            blockchain: ResolvedBlockchain {
                origin_assignment_address: None,
                origin_directory_positive_ttl_sec:
                    decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_POSITIVE_TTL_SEC,
                origin_directory_negative_ttl_sec:
                    decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_NEGATIVE_TTL_SEC,
                origin_directory_cache_capacity:
                    decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_CACHE_CAPACITY,
                publisher_registry_address: None,
                rpc_url: "http://localhost:8545".into(),
                eth_keystore: PathBuf::from("/tmp/keystore.json"),
                keystore_password_file: None,
                payment_pool_address: "0x0000000000000000000000000000000000000001".into(),
                capacity_bond_address: "0x0000000000000000000000000000000000000002".into(),
                rpc_watchdog_interval_sec: 30,
                event_poll_interval_ms: 7000,
                rate_bounds_poll_interval_sec: 3600,
                redeem_threshold_micro_usdc: 1_000_000,
                redeem_max_vouchers_per_tx: 300,
                redeem_interval_secs: 300,
                buyer_working_deposit_micro_usdc: 10_000_000,
                buyer_max_approve: true,
                pool_min_remaining_deposit_micro_usdc: 1_000_000,
                slash_judge_address: "0x0000000000000000000000000000000000000003".to_string(),
                content_blacklist_address: None,
                content_blacklist_poll_interval_sec: 600,
                chain_id: decdn_common::config::DEFAULT_CHAIN_ID,
            },
            cache: decdn_common::config::ResolvedCache {
                cache_dir,
                cache_size_mb: 1024,
                max_blob_size_mb: 128,
                max_rate_per_mb: 0,
                origins,
                pinned_hashes: decdn_cache::PinnedHashes::empty(),
                origin_retry: decdn_cache::RetryPolicy::default(),
                circuit_breaker: decdn_cache::CircuitBreakerPolicy::default(),
                user_agent: decdn_cache::DEFAULT_USER_AGENT.to_string(),
                gc_interval_sec: 0,
                fs_rescan_interval_sec: 0,
                origin_probe_ttl_sec: decdn_common::config::DEFAULT_ORIGIN_PROBE_TTL_SEC,
                origin_probe_timeout_ms: decdn_common::config::DEFAULT_ORIGIN_PROBE_TIMEOUT_MS,
                origin_probe_memo_capacity:
                    decdn_common::config::DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY,
                eviction_high_water_pct: 90,
                eviction_target_pct: 80,
                eviction_per_sweep_budget: 16,
                eviction_tick_secs: 1,
                max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
                stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
                node_to_node_pull_through_enabled: false,
                node_pull_probe_fanout: decdn_common::config::DEFAULT_NODE_PULL_PROBE_FANOUT,
                node_pull_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_TIMEOUT_SEC,
                node_pull_stall_timeout_sec:
                    decdn_common::config::DEFAULT_NODE_PULL_STALL_TIMEOUT_SEC,
                eviction_policy: decdn_common::config::DEFAULT_EVICTION_POLICY.to_string(),
                admission_policy: decdn_common::config::DEFAULT_ADMISSION_POLICY.to_string(),
                tinylfu: decdn_common::config::ResolvedTinyLfu {
                    sketch_bytes: decdn_common::config::DEFAULT_TINYLFU_SKETCH_BYTES,
                    promotion_threshold: decdn_common::config::DEFAULT_TINYLFU_PROMOTION_THRESHOLD,
                    probation_target_pct:
                        decdn_common::config::DEFAULT_TINYLFU_PROBATION_TARGET_PCT,
                    aging_halflife_sec: decdn_common::config::DEFAULT_TINYLFU_AGING_HALFLIFE_SEC,
                },
                serve_economics: decdn_common::config::ResolvedServeEconomics {
                    policy: decdn_common::config::DEFAULT_SERVE_ECONOMICS_POLICY.to_string(),
                    discount_bps: decdn_common::config::DEFAULT_SERVE_ECONOMICS_DISCOUNT_BPS,
                    n_max: decdn_common::config::DEFAULT_SERVE_ECONOMICS_N_MAX,
                    warming_budget: decdn_common::config::DEFAULT_SERVE_ECONOMICS_WARMING_BUDGET,
                    warming_refill: decdn_common::config::DEFAULT_SERVE_ECONOMICS_WARMING_REFILL,
                },
            },
            payment: ResolvedPayment {
                rate_per_mb: 10,
                delivery_floor: 0,
                credit_max: decdn_common::config::DEFAULT_CREDIT_MAX,
                credit_ramp_divisor: decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
                voucher_commit_interval_ms:
                    decdn_common::config::DEFAULT_VOUCHER_COMMIT_INTERVAL_MS,
            },
            observability: ResolvedObservability {
                log_level: decdn_common::cli::common::LogLevel::Info,
                log_format: decdn_common::cli::common::LogFormat::Pretty,
                metrics_port: 9090,
                metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                admin_port: Some(9191),
                otlp_endpoint: None,
            },
            security: ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_source_rate_per_sec: 100.0,
                per_source_burst: 200,
                max_tracked_sources: 4096,
            },
            dht: decdn_common::config::ResolvedDht::default(),
            probe: decdn_common::config::ResolvedProbe::default(),
            receipts: decdn_common::config::ResolvedReceipts::default(),
            content: decdn_common::config::ResolvedContent::default(),
        };
        (tmp, cfg)
    }

    /// `build_cache` must construct the S3 backend without performing
    /// network I/O (#437). The SDK lazily connects on the first
    /// `GetObject` call, so a successful `build_cache` proves the
    /// resolver-to-runtime conversion (`s3_origin_config_from_resolved`)
    /// runs end-to-end and that the SDK's `ClientBuilder::build` doesn't
    /// surface its `BehaviorVersion`-missing runtime error for either
    /// credential variant. The integration suite at
    /// `crates/cache/tests/s3_origin.rs` exercises the wire path via
    /// `aws-smithy-mocks`.
    #[tokio::test]
    async fn build_cache_constructs_s3_origin_for_default_chain() {
        use decdn_common::config::{ResolvedS3Config, ResolvedS3Credentials};

        let s3 = ResolvedS3Config {
            bucket: "decdn-blobs".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            path_style: false,
            prefix: String::new(),
            credentials: Some(ResolvedS3Credentials::DefaultChain { profile: None }),
            decompress: decdn_cache::DecompressMode::Auto,
        };
        let (_tmp, cfg) = cfg_with_origin(Some(ResolvedOrigin::S3(s3)));
        let metrics_handle = Arc::new(metrics::Metrics::new());

        // Construction must succeed end-to-end. A failure here means the
        // resolver-to-runtime conversion regressed or the SDK's lazy-
        // connect contract changed (and we'd be doing I/O at startup).
        let _engine = build_cache(&cfg, metrics_handle, None)
            .await
            .expect("S3 origin must construct without I/O");
    }

    /// Same as above but with the `Static` credential path so the
    /// `SecretString::expose` unwrap arm in `s3_origin_config_from_resolved`
    /// is exercised. The integration tests use `mock_client!` which doesn't
    /// route through the resolved-config layer at all, so this is the only
    /// place the conversion gets covered.
    #[tokio::test]
    async fn build_cache_constructs_s3_origin_for_static_credentials() {
        use decdn_common::config::secret::SecretString;
        use decdn_common::config::{ResolvedS3Config, ResolvedS3Credentials};

        let s3 = ResolvedS3Config {
            bucket: "decdn-blobs".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            path_style: false,
            prefix: "blobs/".to_string(),
            credentials: Some(ResolvedS3Credentials::Static {
                access_key_id: SecretString::new("AKIA-test-fake"),
                secret_access_key: SecretString::new("secret-fake"),
                session_token: None,
            }),
            decompress: decdn_cache::DecompressMode::Auto,
        };
        let (_tmp, cfg) = cfg_with_origin(Some(ResolvedOrigin::S3(s3)));
        let metrics_handle = Arc::new(metrics::Metrics::new());

        let _engine = build_cache(&cfg, metrics_handle, None)
            .await
            .expect("S3 origin with static credentials must construct without I/O");
    }

    /// The conversion helper unwraps `SecretString` via `.expose()`.
    /// Verifying the cleartext bytes survive the conversion — without
    /// this, a refactor that replaces `.expose()` with a placeholder
    /// would silently break `SigV4` signing at runtime. Direct unit
    /// test on the conversion function avoids the SDK round-trip.
    ///
    /// Also pins `region` and `endpoint_url` field-equivalence between
    /// the resolved form and the runtime form. The conversion uses
    /// field access (not destructuring), so a new field added to one
    /// side and forgotten on the other wouldn't be caught at compile
    /// time — this assertion is the safety net.
    #[test]
    fn s3_origin_config_from_resolved_preserves_static_credentials() {
        use decdn_common::config::secret::SecretString;
        use decdn_common::config::{ResolvedS3Config, ResolvedS3Credentials};

        let endpoint =
            decdn_cache::parse_origin_url("https://r2.example/").expect("test URL must parse");
        let resolved = ResolvedS3Config {
            bucket: "b".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: Some(endpoint),
            path_style: true,
            prefix: "blobs/".to_string(),
            credentials: Some(ResolvedS3Credentials::Static {
                access_key_id: SecretString::new("ak-1"),
                secret_access_key: SecretString::new("sk-1"),
                session_token: Some(SecretString::new("tok-1")),
            }),
            decompress: decdn_cache::DecompressMode::Auto,
        };
        let runtime = s3_origin_config_from_resolved(&resolved);
        assert_eq!(runtime.bucket, "b");
        assert_eq!(runtime.region, "us-east-1");
        // OriginUrl doesn't implement PartialEq; compare via Display.
        assert_eq!(
            runtime.endpoint_url.as_ref().map(ToString::to_string),
            Some("https://r2.example/".to_string()),
        );
        assert!(runtime.path_style);
        assert_eq!(runtime.prefix, "blobs/");
        match runtime.credentials.expect("static creds preserved") {
            S3Credentials::Static {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                assert_eq!(access_key_id, "ak-1");
                assert_eq!(secret_access_key, "sk-1");
                assert_eq!(session_token.as_deref(), Some("tok-1"));
            }
            S3Credentials::DefaultChain { .. } => panic!("expected Static after conversion"),
        }
    }

    /// Sibling of the Static-credentials round-trip: pins the
    /// `DefaultChain` arm of `s3_origin_config_from_resolved` along
    /// with `endpoint_url: None` and `path_style: false` (the
    /// virtual-hosted-style AWS / R2 default). Without this test the
    /// `DefaultChain { profile }` -> `DefaultChain { profile }` arm
    /// has no direct coverage; the construction tests above call
    /// `build_cache` but only assert it returns `Ok`, not that
    /// `profile` survived the conversion.
    #[test]
    fn s3_origin_config_from_resolved_preserves_default_chain() {
        use decdn_common::config::{ResolvedS3Config, ResolvedS3Credentials};

        let resolved = ResolvedS3Config {
            bucket: "b".to_string(),
            region: "eu-west-1".to_string(),
            endpoint_url: None,
            path_style: false,
            prefix: String::new(),
            credentials: Some(ResolvedS3Credentials::DefaultChain {
                profile: Some("decdn-prod".to_string()),
            }),
            decompress: decdn_cache::DecompressMode::Auto,
        };
        let runtime = s3_origin_config_from_resolved(&resolved);
        assert_eq!(runtime.region, "eu-west-1");
        assert!(runtime.endpoint_url.is_none());
        assert!(!runtime.path_style);
        assert!(runtime.prefix.is_empty());
        match runtime.credentials.expect("default-chain creds preserved") {
            S3Credentials::DefaultChain { profile } => {
                assert_eq!(profile.as_deref(), Some("decdn-prod"));
            }
            S3Credentials::Static { .. } => panic!("expected DefaultChain after conversion"),
        }
    }

    // Operators grep `signal=SIGINT` / `signal=SIGTERM` / `signal=admin-drain`
    // in the structured "shutdown signal received" log line; a rename here
    // would silently break dashboards and runbooks.
    #[test]
    fn shutdown_signal_display_is_stable() {
        assert_eq!(ShutdownSignal::Sigint.to_string(), "SIGINT");
        #[cfg(unix)]
        assert_eq!(ShutdownSignal::Sigterm.to_string(), "SIGTERM");
        assert_eq!(ShutdownSignal::AdminDrain.to_string(), "admin-drain");
    }

    /// A `spawn_periodic` task exits promptly when its stop signal fires, even
    /// when the next tick is far away. This is the shared shutdown contract for
    /// every periodic runner (dispatch / region / record-store / DHT / probe
    /// GC): without the `biased; stop-before-tick` select, a regression that
    /// dropped the stop arm or polled it after `ticker.tick()` would silently
    /// extend `SHUTDOWN_DEADLINE` by up to one full tick interval (60s at
    /// defaults). Also pins the burned first tick — the body must not run before
    /// the first interval elapses.
    #[tokio::test]
    async fn spawn_periodic_exits_promptly_on_shutdown() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let ticked = Arc::new(AtomicUsize::new(0));
        let mut tasks: JoinSet<()> = JoinSet::new();
        // 60s interval mirrors the runtime default: on a regression that polled
        // the ticker before the stop signal the task would hang for 60s, so the
        // timeout below catches the real bug rather than an unrelated
        // short-interval race.
        let stop_tx = {
            let ticked = Arc::clone(&ticked);
            spawn_periodic(&mut tasks, "test", Duration::from_mins(1), move || {
                ticked.fetch_add(1, Ordering::Relaxed);
            })
        };

        // Give the task a moment to enter the select loop (past the burned first
        // tick), then signal shutdown; it should exit well within the timeout,
        // with generous headroom for slow CI runners.
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop_tx.send(()).expect("receiver still alive");

        let joined = tokio::time::timeout(Duration::from_millis(500), tasks.join_next()).await;
        assert!(
            matches!(joined, Ok(Some(Ok(())))),
            "spawn_periodic must exit within 500ms of shutdown signal; a 60s hang \
             here means the stop arm of the select was lost"
        );
        assert_eq!(
            ticked.load(Ordering::Relaxed),
            0,
            "tick body ran even though the first tick is burned and the interval never elapsed"
        );
    }

    /// `spawn_periodic` actually runs its tick body on each interval — the
    /// complement to the burned-first-tick assertion above. A regression that
    /// broke the `ticker.tick() => tick()` arm (or burned every tick) would
    /// leave all five production GC/log tasks silently dead, and no shutdown
    /// test would catch it.
    #[tokio::test]
    async fn spawn_periodic_fires_the_tick_body() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let ticked = Arc::new(AtomicUsize::new(0));
        let mut tasks: JoinSet<()> = JoinSet::new();
        // Short interval so the first (post-burn) tick lands fast; the stop
        // sender is held for the task's lifetime so it is not cancelled early.
        let _stop_tx = {
            let ticked = Arc::clone(&ticked);
            spawn_periodic(&mut tasks, "test", Duration::from_millis(10), move || {
                ticked.fetch_add(1, Ordering::Relaxed);
            })
        };

        // Poll for at least one tick with generous headroom for slow CI runners,
        // rather than sleeping a fixed duration that could race the first tick.
        tokio::time::timeout(Duration::from_secs(2), async {
            while ticked.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("spawn_periodic never ran its tick body within 2s");
    }

    /// Mount a JSON-RPC `200 OK` POST handler. The mount lives on
    /// `server` until the next `server.reset().await`; callers flip
    /// state by resetting and mounting `mount_unhealthy` instead.
    async fn mount_healthy(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(server)
            .await;
    }

    /// Mount a JSON-RPC `500` POST handler. Used to flip the watchdog
    /// from healthy to unhealthy without tearing the listener down.
    async fn mount_unhealthy(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(500))
            .mount(server)
            .await;
    }

    /// Poll `metrics.rpc_healthy_value()` until it equals `expected` or
    /// the deadline expires. Returns the observed value either way so
    /// failures show what we actually saw rather than just timing out.
    async fn wait_for_gauge(metrics: &Arc<metrics::Metrics>, expected: i64) -> i64 {
        let deadline = std::time::Instant::now() + Duration::from_millis(1500);
        loop {
            let v = metrics.rpc_healthy_value();
            if v == expected || std::time::Instant::now() >= deadline {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = false)]
    async fn rpc_watchdog_tracks_endpoint_transitions() {
        // Healthy -> unhealthy -> healthy transitions, all observed via
        // the `rpc_healthy` gauge. Tight 100ms tick keeps the test under
        // 5s wall-clock while still exercising multiple poll cycles.
        let server = MockServer::start().await;
        mount_healthy(&server).await;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let metrics = Arc::new(metrics::Metrics::new());
        // Mirror `run()`: the caller seeds the gauge from the startup
        // probe, so the watchdog can assume `prev_healthy = true` without
        // emitting a spurious "recovered" log on the first tick.
        metrics.rpc_healthy(true);
        let (tx, rx) = oneshot::channel::<()>();
        let handle = spawn_rpc_watchdog(
            client,
            server.uri(),
            Duration::from_millis(100),
            Arc::clone(&metrics),
            rx,
        );

        // Caller-seeded above; the wait still confirms the loop is
        // running and has observed at least one healthy probe.
        assert_eq!(wait_for_gauge(&metrics, 1).await, 1, "should be healthy");

        // Flip to unhealthy. wiremock's last-mounted response wins for
        // matching POSTs, so the next probe sees a 500.
        server.reset().await;
        mount_unhealthy(&server).await;
        assert_eq!(
            wait_for_gauge(&metrics, 0).await,
            0,
            "should detect unhealthy",
        );

        // Bring it back. Watchdog should recover within a few ticks.
        server.reset().await;
        mount_healthy(&server).await;
        assert_eq!(
            wait_for_gauge(&metrics, 1).await,
            1,
            "should detect recovery",
        );

        // Clean shutdown.
        let _ = tx.send(());
        let join_res = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(join_res.is_ok(), "watchdog should exit on shutdown signal");
    }

    /// The preflight classifier maps a probe response to the right retry verdict
    /// (#1106/#1108): 2xx = healthy, 429/5xx = transient (retryable), other 4xx =
    /// fatal (retrying won't help). A misclassification would either crash-loop a
    /// node on a transient startup 429 or retry a genuine misconfig to budget.
    #[tokio::test]
    async fn preflight_classifier_maps_statuses_to_verdicts() {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("build client");
        for (status, want) in [
            (200u16, "healthy"),
            (429, "transient"),
            (500, "transient"),
            (503, "transient"),
            (400, "fatal"),
            (404, "fatal"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(status).set_body_string("{}"))
                .mount(&server)
                .await;
            let got = match probe_rpc_classified(&client, &server.uri()).await {
                RpcProbe::Healthy => "healthy",
                RpcProbe::Transient(_) => "transient",
                RpcProbe::Fatal(_) => "fatal",
            };
            assert_eq!(got, want, "status {status} misclassified");
        }
    }

    /// Returns 429 for the first `fail_first` calls, then 200 — models a
    /// rate-limited endpoint recovering after the startup burst.
    struct FlakyThenHealthy {
        calls: std::sync::atomic::AtomicUsize,
        fail_first: usize,
    }

    impl wiremock::Respond for FlakyThenHealthy {
        fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < self.fail_first {
                ResponseTemplate::new(429)
            } else {
                ResponseTemplate::new(200).set_body_string("{}")
            }
        }
    }

    /// Build the 5s-timeout preflight client the production path uses.
    fn preflight_client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build client")
    }

    /// A transient (429) preflight is retried with backoff and succeeds once the
    /// endpoint recovers — the #1108 startup-burst-429 case, where a transient 429
    /// must not exit the process. Drives `preflight_retry` with a 1ms backoff so the
    /// real HTTP path
    /// to wiremock runs unpaused but the retries stay fast.
    #[tokio::test]
    async fn preflight_retries_transient_then_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(FlakyThenHealthy {
                calls: std::sync::atomic::AtomicUsize::new(0),
                fail_first: 2,
            })
            .mount(&server)
            .await;
        preflight_retry(&preflight_client(), &server.uri(), Duration::from_millis(1))
            .await
            .expect("preflight should recover after two transient 429s");
    }

    /// A persistent transient failure aborts bring-up after the retry budget — a
    /// genuinely-throttled/dead endpoint is not silently tolerated forever.
    #[tokio::test]
    async fn preflight_exhausts_budget_on_persistent_transient() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;
        let err = preflight_retry(&preflight_client(), &server.uri(), Duration::from_millis(1))
            .await
            .expect_err("persistent 429 must fail after the retry budget");
        assert!(
            err.to_string().contains("attempts"),
            "error should report the exhausted retry budget: {err}"
        );
    }

    /// A fatal (non-429 4xx) preflight aborts immediately — retrying a bad
    /// path/auth won't help, so fail fast with a clear message.
    #[tokio::test]
    async fn preflight_fatal_aborts_immediately() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(400))
            .mount(&server)
            .await;
        let err = preflight_retry(&preflight_client(), &server.uri(), Duration::from_millis(1))
            .await
            .expect_err("a fatal 4xx must abort bring-up");
        assert!(
            err.to_string().contains("unexpected status"),
            "error should surface the fatal status: {err}"
        );
    }

    /// Smoke test for the post-fixup `ShutdownStreams::recv` contract:
    /// a real SIGTERM raised from inside the test process must resolve
    /// the future to `ShutdownSignal::Sigterm`. nextest runs each test
    /// in its own process so the signal cannot leak across tests.
    /// `nix::sys::signal::raise` keeps the workspace `unsafe_code =
    /// "forbid"` lint clean.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_streams_recv_resolves_on_sigterm() {
        use std::time::Duration;

        use nix::sys::signal::{Signal, raise};

        let mut streams = ShutdownStreams::install();
        // Spawn the raise on a separate task so `recv()` is awaiting
        // on the SIGTERM stream by the time the signal arrives. The
        // small sleep gives `install()` a chance to register tokio's
        // handler — without it the kernel could deliver SIGTERM with
        // the default disposition (terminate the process) before the
        // tokio handler is in place. 20ms is far longer than the
        // install path needs.
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            raise(Signal::SIGTERM).expect("raise SIGTERM");
        });
        let signal = tokio::time::timeout(Duration::from_millis(500), streams.recv())
            .await
            .expect("ShutdownStreams::recv did not resolve within 500ms of SIGTERM");
        assert!(matches!(signal, ShutdownSignal::Sigterm));
    }

    /// Guard test for ADR 005 transport defaults. Catches accidental edits to
    /// the constants and verifies `quic_transport_config()` builds without
    /// error — the integration test in `tests/probe_loopback.rs` rebuilds
    /// the config on its own to shorten the idle window, so without this
    /// assertion a regression that changes the defaults (or removes the
    /// helper's call site) would not be caught.
    #[test]
    fn adr_005_transport_defaults() {
        assert_eq!(QUIC_MAX_IDLE_TIMEOUT, Duration::from_secs(30));
        assert_eq!(QUIC_KEEP_ALIVE_INTERVAL, Duration::from_secs(10));
        assert_eq!(QUIC_MAX_CONCURRENT_BIDI_STREAMS, 100);
        quic_transport_config().expect("quic_transport_config builds");
    }

    #[test]
    fn unbracket_host_strips_ipv6_brackets_only() {
        assert_eq!(unbracket_host("[::1]"), "::1");
        assert_eq!(unbracket_host("[2001:db8::1]"), "2001:db8::1");
        // Domains and IPv4 pass through unchanged.
        assert_eq!(unbracket_host("relay.example"), "relay.example");
        assert_eq!(unbracket_host("127.0.0.1"), "127.0.0.1");
        // A lone unmatched bracket is left intact.
        assert_eq!(unbracket_host("[oops"), "[oops");
    }

    #[test]
    fn parse_relay_urls_reports_offending_entry() {
        let ok = parse_relay_urls(&[
            "https://relay-a.example".to_string(),
            "https://relay-b.example".to_string(),
        ])
        .expect("valid relay URLs parse");
        assert_eq!(ok.len(), 2);

        let err = parse_relay_urls(&["not a url".to_string()])
            .expect_err("invalid relay URL is rejected");
        assert!(
            err.to_string().contains("not a url"),
            "error should name the offending entry: {err}"
        );
    }

    #[test]
    fn parse_relay_urls_error_redacts_userinfo() {
        // Malformed URLs that still carry credentials must not leak them — both
        // the well-formed-authority shape and a password containing a literal
        // `@` (which a naive first-`@` split would leak).
        for entry in [
            "https://user:s3cret@host:notaport",
            "https://user:p@s3cret@host:notaport",
        ] {
            let err =
                parse_relay_urls(&[entry.to_string()]).expect_err("malformed URL is rejected");
            let msg = err.to_string();
            assert!(
                !msg.contains("s3cret"),
                "credentials leaked for {entry:?}: {msg}"
            );
            assert!(
                !msg.contains("user:"),
                "userinfo leaked for {entry:?}: {msg}"
            );
            assert!(
                msg.contains("***@host"),
                "host should still appear for {entry:?}: {msg}"
            );
        }
    }

    #[tokio::test]
    async fn probe_relay_true_when_listener_is_up() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url: RelayUrl = format!("http://127.0.0.1:{port}").parse().unwrap();
        assert_eq!(probe_relay(&url).await, Some(true));
    }

    #[tokio::test]
    async fn probe_relay_false_when_nothing_listening() {
        // Claim a free port, then drop the listener so the port is closed. The
        // probe exhausts its retries and reports the relay unreachable.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let url: RelayUrl = format!("http://127.0.0.1:{port}").parse().unwrap();
        assert_eq!(probe_relay(&url).await, Some(false));
    }

    #[tokio::test]
    async fn probe_relay_none_when_unprobeable() {
        // A scheme with no `url`-known default port and no explicit port yields
        // no probe target — iroh may still route it, so the probe reports
        // "unknown" (None) rather than a reachability failure.
        let url: RelayUrl = "relay://no-port-host".parse().unwrap();
        assert_eq!(relay_host_port(&url), None);
        assert_eq!(probe_relay(&url).await, None);
    }

    #[tokio::test]
    async fn probe_relay_unbrackets_ipv6_host() {
        // Best-effort: skip where the sandbox has no IPv6 loopback.
        let Ok(listener) = tokio::net::TcpListener::bind("[::1]:0").await else {
            return;
        };
        let port = listener.local_addr().unwrap().port();
        // `host_str()` yields "[::1]"; the probe must strip the brackets to connect.
        let url: RelayUrl = format!("http://[::1]:{port}").parse().unwrap();
        assert_eq!(probe_relay(&url).await, Some(true));
    }

    #[tokio::test]
    async fn probe_relays_tallies_reachable_and_unprobeable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_port = listener.local_addr().unwrap().port();
        let up: RelayUrl = format!("http://127.0.0.1:{up_port}").parse().unwrap();

        let dead_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_port = dead_listener.local_addr().unwrap().port();
        drop(dead_listener);
        let down: RelayUrl = format!("http://127.0.0.1:{dead_port}").parse().unwrap();

        let unprobeable: RelayUrl = "relay://no-port-host".parse().unwrap();

        assert_eq!(
            probe_relays(&[up.clone(), down.clone()]).await,
            RelayProbeTally {
                reachable: 1,
                unprobeable: 0
            }
        );
        assert_eq!(
            probe_relays(&[up.clone(), up]).await,
            RelayProbeTally {
                reachable: 2,
                unprobeable: 0
            }
        );
        assert_eq!(
            probe_relays(&[down, unprobeable]).await,
            RelayProbeTally {
                reachable: 0,
                unprobeable: 1
            }
        );
    }

    #[tokio::test]
    async fn build_endpoint_binds_with_one_reachable_relay() {
        // One live + one dead relay: bring-up succeeds because >=1 is reachable.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_port = listener.local_addr().unwrap().port();
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_port = dead.local_addr().unwrap().port();
        drop(dead);

        let sk = SecretKey::generate();
        let transport = quic_transport_config().unwrap();
        let relays = vec![
            format!("http://127.0.0.1:{up_port}"),
            format!("http://127.0.0.1:{dead_port}"),
        ];
        let ep = build_endpoint(&sk, 0, &relays, &ResolvedDiscovery::default(), transport)
            .await
            .expect("endpoint binds with at least one reachable relay");
        ep.close().await;
    }

    #[tokio::test]
    async fn build_endpoint_binds_when_all_relays_unreachable() {
        // A non-empty, all-unreachable relay set is advisory only: bring-up logs
        // a warning and proceeds (iroh retries in the background) rather than
        // failing, so a transient relay outage can't wedge node startup.
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_port = dead.local_addr().unwrap().port();
        drop(dead);

        let sk = SecretKey::generate();
        let transport = quic_transport_config().unwrap();
        let relays = vec![format!("http://127.0.0.1:{dead_port}")];
        let ep = build_endpoint(&sk, 0, &relays, &ResolvedDiscovery::default(), transport)
            .await
            .expect("all-unreachable relay set proceeds with a warning");
        ep.close().await;
    }

    #[tokio::test]
    async fn build_endpoint_binds_when_all_relays_unprobeable() {
        // Relays iroh accepts but we can't TCP-probe (no derivable port) are not
        // counted as reachability failures, so bring-up proceeds on trust rather
        // than hard-failing a config iroh would have routed.
        let sk = SecretKey::generate();
        let transport = quic_transport_config().unwrap();
        let relays = vec!["relay://unprobeable-a".to_string()];
        let ep = build_endpoint(&sk, 0, &relays, &ResolvedDiscovery::default(), transport)
            .await
            .expect("all-unprobeable relay set proceeds");
        ep.close().await;
    }

    #[tokio::test]
    async fn build_endpoint_uses_n0_when_discovery_empty() {
        // Empty discovery keeps the `presets::N0` base (back-compat). The preset
        // choice isn't introspectable, so assert bring-up still binds cleanly.
        let sk = SecretKey::generate();
        let transport = quic_transport_config().unwrap();
        let ep = build_endpoint(&sk, 0, &[], &ResolvedDiscovery::default(), transport)
            .await
            .expect("n0 endpoint binds with empty discovery");
        ep.close().await;
    }

    #[tokio::test]
    async fn build_endpoint_binds_with_custom_discovery() {
        // Custom pkarr+DNS plus a static peer drop the build onto
        // `presets::Minimal` and compose the configured address-lookup legs; with
        // no relay map the n0 relay default is restored. Exercises
        // `add_discovery_lookups` end to end (publish/resolve are background/lazy,
        // so binding does not require reaching the configured infra).
        let sk = SecretKey::generate();
        let transport = quic_transport_config().unwrap();
        let peer_id = SecretKey::generate().public().to_string();
        let discovery = ResolvedDiscovery {
            pkarr_url: Some("https://pkarr.example/".to_string()),
            dns_origin: Some("discovery.example.".to_string()),
            peers: vec![decdn_common::config::ResolvedDiscoveryPeer {
                node_id: peer_id,
                relay_url: Some("https://relay.example/".to_string()),
                addrs: vec!["203.0.113.4:4433".to_string()],
            }],
        };
        let ep = build_endpoint(&sk, 0, &[], &discovery, transport)
            .await
            .expect("custom-discovery endpoint binds");
        ep.close().await;
    }

    #[tokio::test]
    async fn build_endpoint_binds_with_discovery_and_custom_relay() {
        // Discovery and a reachable custom relay together: both legs wire onto the
        // Minimal base (custom relay map overrides the restored n0 default).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_port = listener.local_addr().unwrap().port();
        let sk = SecretKey::generate();
        let transport = quic_transport_config().unwrap();
        let relays = vec![format!("http://127.0.0.1:{up_port}")];
        let discovery = ResolvedDiscovery {
            pkarr_url: None,
            dns_origin: Some("discovery.example.".to_string()),
            peers: Vec::new(),
        };
        let ep = build_endpoint(&sk, 0, &relays, &discovery, transport)
            .await
            .expect("discovery + custom relay endpoint binds");
        ep.close().await;
    }
}
