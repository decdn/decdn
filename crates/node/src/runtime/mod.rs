//! Node runtime: owns the iroh endpoint, metrics server, and protocol router.

pub mod reload;

pub use reload::{LogLevelSetter, ReloadSnapshot, RuntimeReloadState};

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use decdn_cache::{
    CacheEngine, FilesystemOrigin, HttpOrigin, Origin, S3Credentials, S3Origin, S3OriginConfig,
};
use decdn_common::config::{ResolvedOrigin, ResolvedS3Credentials};
use iroh::endpoint::{IdleTimeout, QuicTransportConfig, VarInt, presets};
use iroh::protocol::Router;
use iroh::{Endpoint, SecretKey};
use iroh_gossip::ALPN as GOSSIP_ALPN;
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use decdn_gossip::{GossipMetrics, GossipRuntimeConfig, GossipService, PeerTable, build_gossip};

use crate::admin;
use crate::channel_store::PersistentChannelStateStore;
use crate::dht::{
    ChainStakerSet, DhtRateLimiter, RecordStore, RecordStoreConfig, StakerSet,
    rate_limit::DhtRateLimitConfig,
};
use crate::dispatch::ConnectionLimiter;
use crate::handlers::client::{ClientHandler, MAX_CLIENT_STREAMS};
use crate::handlers::dht::DhtHandler;
use crate::handlers::limited::LimitedHandler;
use crate::handlers::probe::{ProbeHandler, StakeLanePolicy as ProbeStakeLanePolicy};
use crate::metrics;
use crate::payment_settlement::PaymentChannelService;
use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use decdn_common::config::ResolvedConfig;
use decdn_common::identity;
use decdn_incentive::eth_identity::{self, PasswordSource};
use decdn_incentive::{ChannelStateStore, PendingSettleStore};

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
/// indefinitely. 60s matches the steady-state cadence of the gossip
/// peer-table TTL sweeper and is comfortably larger than the longest
/// realistic bucket refill window, so the sweep is essentially free
/// when the keyspace is empty.
const DISPATCH_GC_INTERVAL: Duration = Duration::from_mins(1);

/// Interval between periodic GC sweeps of the DHT rate-limiter's per-IP
/// and per-peer keyed maps (#645). 60s matches `DISPATCH_GC_INTERVAL` —
/// the two limiters share the same operator mental model for keyspace
/// cleanup cadence. Separate constant (rather than reusing
/// `DISPATCH_GC_INTERVAL`) so a future tune to one limiter doesn't drag
/// the other along.
const DHT_RATE_LIMIT_GC_INTERVAL: Duration = Duration::from_mins(1);

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
/// application-layer and lives in the cdn/client/v1 handler (not yet
/// implemented).
const QUIC_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval between QUIC PING keep-alive frames. Per ADR 005 §Connection
/// lifecycle, endpoints SHOULD send PINGs at 10s intervals — strictly
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

/// Body of the periodic dispatch-limiter GC task (#440). Extracted from
/// the spawn site so the shutdown-promptness contract can be tested
/// directly: a regression where the loop ignores `stop_rx` would
/// silently extend `SHUTDOWN_DEADLINE` by up to one tick interval
/// (`DISPATCH_GC_INTERVAL` = 60s by default), which the runtime's normal
/// shutdown path would mask as a "task slow to drain" rather than a bug.
///
/// The first tick is burned so the first GC pass lands one interval
/// after startup rather than on the same tick — there are no stale
/// buckets to clean up at t=0.
async fn run_dispatch_gc(
    limiter: Arc<ConnectionLimiter>,
    mut stop_rx: oneshot::Receiver<()>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = &mut stop_rx => {
                tracing::debug!("dispatch GC shutdown signal received");
                return;
            }
            _ = ticker.tick() => {
                if let Some((before, after)) = limiter.gc_per_source() {
                    let dropped = before.saturating_sub(after);
                    tracing::debug!(
                        before,
                        after,
                        dropped,
                        "dispatch GC sweep complete"
                    );
                }
            }
        }
    }
}

/// Periodic DHT record-store GC task (ADR 022 §Content Records and TTL).
///
/// `RecordStore::providers_at` lazily scrubs expired records for the
/// hash it's queried about, but a hash that nobody ever queries again
/// keeps its expired entries — they continue to count against the
/// publisher's per-publisher quota and against the global cap until
/// this task fires. Without it a node that publishes a one-shot blob
/// can eventually exhaust its 200-record quota and have every future
/// `Store` rejected.
///
/// The sweep is `O(expired × log N)` over the global LRU thanks to the
/// front-walk in [`crate::dht::RecordStore::gc`]. Default interval is
/// 60s — well below the 1-hour record TTL.
// Same linear shape as `run_dispatch_gc` above (tick → maybe-prune →
// log → loop). Splitting the body would scatter the "every tick takes
// the lock and runs the BTreeSet walk" admission seam — the cognitive
// complexity is the linear seq of two await points, not branching depth.
#[allow(clippy::cognitive_complexity)]
async fn run_record_store_gc(
    records: Arc<std::sync::Mutex<RecordStore>>,
    mut stop_rx: oneshot::Receiver<()>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = &mut stop_rx => {
                tracing::debug!("dht record-store GC shutdown signal received");
                return;
            }
            _ = ticker.tick() => {
                let now_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX));
                let removed = if let Ok(mut store) = records.lock() {
                    store.gc(now_us)
                } else {
                    tracing::error!(
                        "dht record-store mutex poisoned; skipping GC sweep this tick"
                    );
                    0
                };
                if removed > 0 {
                    tracing::debug!(removed, "dht record-store GC sweep complete");
                }
            }
        }
    }
}

/// Periodic DHT rate-limiter GC task (#645).
///
/// The acquire path opportunistically prunes when the per-IP / per-peer
/// keyed maps exceed `cap + cap/10`, but a node whose DHT traffic falls
/// below the over-cap threshold can carry millions of stale buckets
/// indefinitely. One task drives both keyed layers; `gc_per_ip` and
/// `gc_per_peer` use independent single-flight flags internally so a
/// concurrent lazy prune of one layer (from `check`) does not block the
/// GC sweep of the other layer. The two sweeps in this task itself run
/// sequentially within one tick — there is no internal parallelism here.
///
/// **Panic contract.** A panic inside `retain_recent` (e.g. from a
/// `governor` internal arithmetic bug or an allocator OOM during the
/// walk) unwinds out of `gc_per_ip` / `gc_per_peer` and exits this
/// task. The `PruneGuard` Drop impl still releases the single-flight
/// flag during unwind, so the lazy-prune path in `check` keeps working
/// — but the periodic sweep is permanently dead until the next process
/// restart. The panic surfaces via `JoinSet::join_next` at graceful
/// shutdown, not earlier; operators who want earlier notice should
/// alert on the `decdn_dht_rate_limit_tracked_{per_ip,per_peer}` gauges
/// failing to drop on a quiet node (the lazy-prune path in `check` also
/// writes these gauges, but on a quiet node `check` is not called, so
/// the periodic sweep is the only writer — and it stops writing if it
/// dies). The signal needs a non-zero starting value: a map that was
/// already empty when the sweep died will sit at `0` legitimately,
/// indistinguishable from a healthy GC task on an empty map.
// Same linear shape as `run_dispatch_gc` and `run_record_store_gc`
// above (tick → maybe-prune-per-layer → log → loop), with two
// `if let Some(...)` branches instead of one because there are two
// keyed maps to sweep. Splitting the per-layer arm into a helper
// would scatter the `biased; stop_rx | ticker.tick()` seam that the
// `*_exits_promptly_on_shutdown` tests pin — the cognitive
// complexity is the count of mostly-identical `tracing::debug!`
// log-arms, not branching depth.
#[allow(clippy::cognitive_complexity)]
async fn run_dht_rate_limit_gc(
    limiter: Arc<DhtRateLimiter>,
    mut stop_rx: oneshot::Receiver<()>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = &mut stop_rx => {
                tracing::debug!("dht rate-limit GC shutdown signal received");
                return;
            }
            _ = ticker.tick() => {
                if let Some((before, after)) = limiter.gc_per_ip() {
                    tracing::debug!(
                        layer = "per_ip",
                        before,
                        after,
                        dropped = before.saturating_sub(after),
                        "dht rate-limit GC sweep complete"
                    );
                }
                if let Some((before, after)) = limiter.gc_per_peer() {
                    tracing::debug!(
                        layer = "per_peer",
                        before,
                        after,
                        dropped = before.saturating_sub(after),
                        "dht rate-limit GC sweep complete"
                    );
                }
            }
        }
    }
}

/// Build the endpoint, register handlers on a `Router`, spawn the metrics
/// server and gossip tasks, and run until a shutdown signal is received.
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
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
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

    // Preflight: verify RPC endpoint is reachable before committing to
    // port binding. A 5-second timeout keeps startup responsive on flaky
    // networks while still catching typos and dead endpoints early.
    check_rpc_reachability(&cfg.blockchain.rpc_url).await?;

    let node_metrics = Arc::new(metrics::Metrics::new());
    node_metrics.started();
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
    let eth_signer = Arc::new(load_eth_signer(&cfg).await?);
    tracing::info!(address = %eth_signer.address(), "loaded eth keystore");

    // Open the off-chain voucher-state store (issue #527, ADR 003
    // §Off-chain voucher state persistence) before any handler that could
    // accept a voucher comes online. `PersistentChannelStateStore::open`
    // performs disk I/O (file create + mode tighten + redb header read), so
    // run it on a blocking thread to avoid stalling the tokio runtime.
    // Failure here MUST abort startup: continuing with a fresh in-memory
    // map silently reopens the replay window the store exists to close.
    let channel_store_data_dir = cfg.identity.data_dir.clone();
    // Keep the concrete store `Arc` so it can back the seller
    // `ChannelStateStore` (channel_state_v1 table), the pending-settle store
    // (pending_settle_v1 table, PR #743 review), and the buyer
    // `BuyerChannelStore` (buyer_channel_state_v1 table, #744) — redb forbids a
    // second `Database` handle to the same file, so one shared store owns all.
    let concrete_channel_store: Arc<PersistentChannelStateStore> = Arc::new(
        tokio::task::spawn_blocking(move || {
            PersistentChannelStateStore::open(&channel_store_data_dir)
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
    let channel_state_store: Arc<dyn ChannelStateStore> = concrete_channel_store.clone();
    let pending_settle_store: Arc<dyn PendingSettleStore> = concrete_channel_store.clone();
    // Debounce the scan-checkpoint write (#784): the live watcher advances the
    // checkpoint once per distinct block carrying a provider-owned
    // `ChannelOpened`, and the directly-durable store fsyncs on each. The
    // persisted value is only a *floor* for the resume backfill
    // (`resolve_backfill_start` rewinds it by the reorg margin; registration is
    // idempotent), so coarsening the write cadence is safe — and the settlement
    // service forces a final flush on graceful shutdown so steady-state progress
    // is not lost. Wrapping here (the wiring layer) keeps the domain trait and the
    // disk store free of the debounce policy.
    let watcher_checkpoint_store: Arc<dyn decdn_incentive::WatcherCheckpointStore> = Arc::new(
        crate::payment_settlement::DebouncedCheckpointStore::new(concrete_channel_store.clone()),
    );
    // Boot-time smoke test: read every persisted record so startup fails
    // fast on corruption / forward-incompatible schema even before the
    // future cdn/client/v1 handler (#317) is constructed. The handler will
    // call `load_all` again to bootstrap its in-memory channel map — that
    // duplicate read is by design; the runtime cannot keep the snapshot
    // because no consumer exists yet, and threading a pre-built map
    // through `Router::builder` would couple the runtime to the (still
    // unwritten) handler signature. Cost: one extra `load_all` on startup.
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
    let receipt_log: Arc<dyn crate::receipt_log::ReceiptLog> =
        match tokio::task::spawn_blocking(move || {
            crate::receipt_log::JsonlReceiptLog::open(&receipt_log_data_dir)
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

    let cache = build_cache(&cfg, Arc::clone(&node_metrics)).await?;
    // Attach the cache to the reload state so SIGHUP handlers can swap
    // the pinned-hashes set atomically (#276). Done immediately after
    // `build_cache` succeeds so a SIGHUP delivered during the rest of
    // startup will still find a target.
    reload_state.attach_cache(Some(cache.clone()));
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
    let ep = build_endpoint(&secret_key, cfg.network.bind_port, transport_config)
        .await
        .context("failed to build iroh endpoint")?;

    node_metrics
        .register_iroh_endpoint(&ep)
        .context("failed to register iroh metrics")?;

    // Pin iroh-gossip's per-actor frame ceiling to a deCDN-controlled
    // value (ADR 013 §Gossip Framing, #660). `read_lp` enforces this
    // cap before allocating the inbound `BytesMut`, bounding per-peer
    // DoS exposure. Constructed via `build_gossip` so a regression
    // that drops the cap fails the gossip-crate test that exercises
    // the same helper.
    let gossip = build_gossip(ep.clone());

    let limiter = Arc::new(ConnectionLimiter::new(
        &cfg.security,
        Arc::clone(&node_metrics),
    ));
    // Attach the limiter to the reload state so SIGHUP / admin reloads
    // can forward `[security]` changes via `ConnectionLimiter::reload`
    // (#235). Done immediately after construction so a SIGHUP delivered
    // during the rest of startup still finds a target.
    reload_state.attach_limiter(Some(Arc::clone(&limiter)));

    // SlashJudge EIP-712 domain for probe `slash_sig` (ADR 014 §1–2). The
    // address was validated checksummed at config resolution
    // (`parse_contract_address`), so the parse here cannot fail in practice;
    // map the error rather than unwrap to satisfy the anti-panic policy.
    let slash_judge_addr: alloy::primitives::Address = cfg
        .blockchain
        .slash_judge_address
        .parse()
        .with_context(|| {
        format!(
            "blockchain.slash_judge_address is not a valid address: {}",
            cfg.blockchain.slash_judge_address
        )
    })?;
    let slash_domain =
        decdn_incentive::slash_judge_domain(cfg.blockchain.chain_id, slash_judge_addr);

    // Chain-backed active-staker set. Bootstrap failure is fatal: an
    // empty set silently rejects every inbound `Store`, and once the
    // iterative `FindValue` lookup filter exists it would drop every
    // responder. Built here (ahead of the probe handler) so the probe
    // handler can consult it for stake-lane probe-acceptance (#757); the
    // DHT handler below shares the same `Arc`.
    let rpc_url: alloy::transports::http::reqwest::Url =
        cfg.blockchain.rpc_url.parse().with_context(|| {
            format!(
                "blockchain.rpc_url {:?} is not a valid URL",
                cfg.blockchain.rpc_url
            )
        })?;
    let capacity_bond_addr: Address =
        cfg.blockchain
            .capacity_bond_address
            .parse()
            .with_context(|| {
                format!(
                    "blockchain.capacity_bond_address {:?} is not a valid address",
                    cfg.blockchain.capacity_bond_address
                )
            })?;
    let chain_provider = ProviderBuilder::new().connect_http(rpc_url.clone());
    let staker_set: Arc<dyn StakerSet> = Arc::new(
        ChainStakerSet::bootstrap(
            chain_provider,
            capacity_bond_addr,
            Arc::clone(&node_metrics),
        )
        .await
        .with_context(|| {
            format!("ChainStakerSet bootstrap from CapacityBond at {capacity_bond_addr}")
        })?,
    );

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

    let probe_handler = Arc::new(ProbeHandler::new(
        secret_key.public(),
        reload_state.rate_per_mb(),
        Arc::clone(&node_metrics),
        Arc::clone(&limiter),
        cache.clone(),
        Arc::clone(&eth_signer),
        slash_domain,
        cfg.payment.delivery_floor,
        cfg.payment.delivery_ceiling,
        // ADR 015 master switch. Restart-required (it changes the
        // `on_accepting` wiring): the SIGHUP path reports any
        // `[network]` change as "requires restart".
        cfg.network.enable_0rtt,
        stake_lane_policy,
    ));

    // Wrap the foreign `iroh-gossip` handler with `LimitedHandler` so the
    // gossip ALPN goes through the same `ConnectionLimiter` (#235) that
    // gates the probe ALPN. Without the wrapper, a connection flood on
    // `iroh-gossip/0` bypasses the global semaphore entirely (#433): the
    // per-task resource ceiling holds for probe but not network-wide.
    //
    // `cdn/dht/v1` handler (ADR 022 / #320). FindNode + FindValue +
    // Store all wired up; iterative requester-side lookup and the
    // republish scheduler land in PR 4 of #320. Three-layer rate limiter
    // operates at the full ADR 022 spec.
    let dht_rate_limit_cfg = DhtRateLimitConfig {
        per_peer_rate_per_sec: cfg.dht.per_peer_rate_per_sec,
        per_peer_burst: cfg.dht.per_peer_burst,
        per_ip_rate_per_sec: cfg.dht.per_ip_rate_per_sec,
        per_ip_burst: cfg.dht.per_ip_burst,
        global_rate_per_sec: cfg.dht.global_rate_per_sec,
        global_burst: cfg.dht.global_burst,
        trusted_ips: cfg.dht.trusted_ips.clone(),
        max_tracked_per_ip: cfg.dht.max_tracked_per_ip,
        max_tracked_per_peer: cfg.dht.max_tracked_per_peer,
    };
    let dht_rate_limiter = Arc::new(DhtRateLimiter::new(
        &dht_rate_limit_cfg,
        Arc::clone(&node_metrics),
    ));
    // Record store sized from the ADR 022 defaults; per-publisher /
    // global / per-hash caps are pinned by the protocol and only the
    // TTL field is plausibly operator-tunable, but no knob is exposed
    // yet — operators with non-default needs should file a follow-up
    // rather than tune in TOML.
    let record_store = Arc::new(std::sync::Mutex::new(RecordStore::new(
        RecordStoreConfig::default(),
    )));
    let dht_handler = Arc::new(DhtHandler::new(
        secret_key.public(),
        Arc::clone(&dht_rate_limiter),
        Arc::clone(&limiter),
        Arc::clone(&node_metrics),
        Arc::clone(&staker_set),
        Arc::clone(&record_store),
    ));

    // The DHT handler builds its own routing table internally; grab a
    // shared handle so the bootstrap path + republish + bucket-refresh
    // tasks can all operate on the same instance.
    let dht_routing = dht_handler.routing_table();

    // `cdn/client/v1` paid-delivery handler (#317). The voucher EIP-712 domain
    // binds to the `PaymentChannel` deployment; the ephemeral-binding
    // domain to the `CapacityBond` deployment (== `capacity_bond_addr`,
    // which holds the NodeId↔address mappings). The handler hydrates per-channel
    // voucher state from `channel_state_store` so a restart cannot replay an
    // already-accepted voucher (#527).
    let payment_channel_addr: Address = cfg
        .blockchain
        .payment_channel_address
        .parse()
        .with_context(|| {
            format!(
                "blockchain.payment_channel_address {:?} is not a valid address",
                cfg.blockchain.payment_channel_address
            )
        })?;
    let voucher_domain =
        decdn_incentive::voucher_domain(cfg.blockchain.chain_id, payment_channel_addr);
    let bind_domain =
        decdn_incentive::bind_node_id_domain(cfg.blockchain.chain_id, capacity_bond_addr);
    let client_handler = Arc::new(ClientHandler::new(
        secret_key.public(),
        Arc::clone(&node_metrics),
        Arc::clone(&limiter),
        cache.clone(),
        Arc::clone(&eth_signer),
        decdn_incentive::slash_judge_domain(cfg.blockchain.chain_id, slash_judge_addr),
        voucher_domain,
        bind_domain,
        Arc::clone(&channel_state_store),
        Arc::clone(&receipt_log),
        reload_state.rate_per_mb(),
        cfg.payment.delivery_floor,
        cfg.payment.delivery_ceiling,
        cfg.payment.voucher_interval_mb,
        cfg.cache
            .max_blob_size_mb
            .saturating_mul(decdn_protocol::MB_BYTES),
        MAX_CLIENT_STREAMS,
    )?);

    // On-chain seller-settlement service (#327). A wallet-filled provider
    // (the staker-set provider above is read-only) signs the `withdraw` /
    // `closeChannel` transactions with the same eth keystore signer. The
    // bootstrap self-checks the contract via `usdc()`; the watcher persists
    // channels opened against this node so the handler accepts their vouchers,
    // and forgets settled ones. The redeem hint lets the handler nudge the
    // service when an accrued claim may have crossed the threshold.
    let wallet_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from((*eth_signer).clone()))
        .connect_http(rpc_url.clone());
    let payment_service = PaymentChannelService::bootstrap(
        wallet_provider,
        payment_channel_addr,
        eth_signer.address(),
        Arc::clone(&channel_state_store),
        Arc::clone(&pending_settle_store),
        Arc::clone(&watcher_checkpoint_store),
        Arc::clone(&client_handler),
        U256::from(cfg.blockchain.redeem_threshold_micro_usdc),
        crate::payment_settlement::AutoSettleConfig {
            value_threshold: cfg
                .blockchain
                .settlement_auto_threshold_micro_usdc
                .map(U256::from),
            voucher_nonce_span_threshold: cfg.blockchain.settlement_auto_by_voucher_nonce_span,
        },
        Arc::clone(&node_metrics),
    )
    .await
    .context("PaymentChannel settlement service bootstrap")?;
    client_handler.attach_redeem_hint(payment_service.redeem_hint_sender());

    // In-memory last-voucher clock shared between the client handler (writer:
    // stamps on each accepted voucher) and `admin_v1_channels` (reader:
    // reports "time since last voucher"), issue #749. Non-durable by design —
    // a restart resets it and channels report "no activity yet" until their
    // next voucher (see `decdn_incentive::VoucherActivity`).
    let voucher_activity = Arc::new(decdn_incentive::VoucherActivity::new());
    client_handler.attach_voucher_activity(Arc::clone(&voucher_activity));

    // On-chain buyer-side service (#744). When this node pulls content from an
    // upstream provider on a cache miss it pays via the same channel mechanism,
    // acting as the client: a separate wallet-filled provider signs `approve` /
    // `openChannel` / `reclaimExpired`. It shares the persistent store (a
    // distinct `buyer_channel_state_v1` table) and re-derives the voucher domain
    // the handler consumed above. The cache-engine hook that *calls*
    // `open_or_reuse_channel` needs provider-discovery (ADR 001/022) and is out
    // of scope here; the service is held for the process lifetime so its reclaim
    // sweep keeps running. `_buyer_channel_service` (leading underscore) keeps
    // the binding — and thus its `AbortOnDrop` reclaim task — alive to shutdown.
    //
    // Unlike the seller service, a buyer-bootstrap failure is NON-fatal: buying
    // is opportunistic cost-recovery (and the cache-engine hook isn't wired yet),
    // so a failed startup `approve` tx (e.g. insufficient gas) must not block the
    // node's core seller function. Log and continue with the buyer path disabled.
    let buyer_wallet_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from((*eth_signer).clone()))
        .connect_http(rpc_url);
    let buyer_channel_store: Arc<dyn decdn_incentive::BuyerChannelStore> = Arc::new(
        crate::channel_store::BuyerChannelStoreHandle::new(Arc::clone(&concrete_channel_store)),
    );
    let _buyer_channel_service = match crate::buyer_channel::BuyerChannelService::bootstrap(
        buyer_wallet_provider,
        payment_channel_addr,
        eth_signer.address(),
        buyer_channel_store,
        Arc::clone(&eth_signer),
        decdn_incentive::voucher_domain(cfg.blockchain.chain_id, payment_channel_addr),
        U256::from(cfg.blockchain.buyer_deposit_micro_usdc),
        cfg.blockchain.buyer_max_approve,
    )
    .await
    {
        Ok(service) => Some(service),
        Err(err) => {
            tracing::warn!(
                %err,
                %payment_channel_addr,
                "buyer-side PaymentChannel bootstrap failed; node→node paid cache-miss pulls are \
                 DISABLED for this process (seller settlement is unaffected). This condition is \
                 sticky — restart the node to retry. Check: (1) blockchain.payment_channel_address \
                 is correct, (2) the RPC endpoint is reachable, (3) the wallet holds gas for the \
                 one-time USDC approve."
            );
            None
        }
    };

    let router = Router::builder(ep.clone())
        .accept(ProbeHandler::ALPN, probe_handler)
        .accept(ClientHandler::ALPN, client_handler)
        .accept(DhtHandler::ALPN, dht_handler)
        .accept(
            GOSSIP_ALPN,
            LimitedHandler::new(gossip.clone(), Arc::clone(&limiter)),
        )
        .spawn();

    // Bootstrap (ADR 022 §Bootstrap): seed the routing table from the
    // active-staker set + parallel `FindNode(self.node_id)` against a
    // fan-out of seeds. Best-effort — failures here log but don't
    // abort startup.
    let bootstrap_outcome =
        crate::dht::bootstrap::bootstrap(&ep, secret_key.public(), &dht_routing, &staker_set).await;
    tracing::info!(
        seeds = bootstrap_outcome.seeds_seen,
        inserted = bootstrap_outcome.seeds_inserted,
        find_node_ok = bootstrap_outcome.find_node_ok,
        find_node_err = bootstrap_outcome.find_node_err,
        closer_added = bootstrap_outcome.closer_peers_inserted,
        "dht bootstrap complete"
    );

    let metrics_addr = std::net::SocketAddr::new(
        cfg.observability.metrics_bind,
        cfg.observability.metrics_port,
    );
    let metrics_listener = metrics::bind(metrics_addr)
        .await
        .context("failed to bind metrics listener")?;

    let mut tasks = JoinSet::new();
    let (metrics_stop_tx, metrics_stop_rx) = oneshot::channel::<()>();

    let metrics_handle = Arc::clone(&node_metrics);
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
    let (dispatch_gc_stop_tx, dispatch_gc_stop_rx) = oneshot::channel::<()>();
    let dispatch_gc_limiter = Arc::clone(&limiter);
    tasks.spawn(run_dispatch_gc(
        dispatch_gc_limiter,
        dispatch_gc_stop_rx,
        DISPATCH_GC_INTERVAL,
    ));

    // Periodic DHT record-store GC (ADR 022 §Content Records and TTL).
    // Without this, expired records accumulate against per-publisher
    // and global caps — a node could exhaust its 200-record per-
    // publisher quota and start rejecting every `Store` even though
    // the TTL window has long passed. Same shutdown shape as the
    // dispatch GC above.
    let (record_store_gc_stop_tx, record_store_gc_stop_rx) = oneshot::channel::<()>();
    tasks.spawn(run_record_store_gc(
        Arc::clone(&record_store),
        record_store_gc_stop_rx,
        DISPATCH_GC_INTERVAL,
    ));

    // Periodic DHT rate-limiter GC (#645). Without this, the per-IP and
    // per-peer keyed maps accumulate stale buckets indefinitely on nodes
    // whose DHT traffic falls below the over-cap lazy-prune threshold —
    // same DoS shape that the dispatch-limiter GC above prevents at the
    // connection layer.
    let (dht_rate_limit_gc_stop_tx, dht_rate_limit_gc_stop_rx) = oneshot::channel::<()>();
    tasks.spawn(run_dht_rate_limit_gc(
        Arc::clone(&dht_rate_limiter),
        dht_rate_limit_gc_stop_rx,
        DHT_RATE_LIMIT_GC_INTERVAL,
    ));

    // DHT republish scheduler (ADR 022 §STORE Flow). The subscribe
    // handle is taken before the cold-start seed so a commit racing
    // with seed-time lands in the channel backlog rather than the
    // gap between the snapshot and the spawn.
    let republish_scheduler = Arc::new(crate::dht::RepublishScheduler::new());
    let (republish_stop_tx, republish_stop_rx) = oneshot::channel::<()>();
    let cache_inserts_rx = cache.subscribe_inserts();
    // Walk the on-disk store (NOT `access_times_snapshot`, which maps `Hash →
    // Instant` and is empty on every cold start) so every committed,
    // non-evicted blob gets a `uniform(0, 40 min)` republish entry per ADR
    // 022 §Bootstrap AC 16. On a transient list-error we degrade: the
    // steady-state `subscribe_inserts` path catches only blobs newly fetched
    // post-boot — blobs already on disk that get cache-HIT requests are NOT
    // re-scheduled until the next successful restart.
    let cold_start_count = match cache.iter_hashes().await {
        Ok(hashes) => republish_scheduler.seed_cold_start(
            hashes
                .into_iter()
                .map(|h| decdn_protocol::ContentHash::from_bytes(*h.as_bytes())),
        ),
        Err(err) => {
            tracing::warn!(
                error = %err,
                "cold-start seed failed; blobs not re-fetched this session will go un-republished until next restart (ADR 022 §Bootstrap AC 16 degraded)"
            );
            0
        }
    };
    tracing::info!(
        cold_start_count,
        "republish scheduler seeded from existing cache (ADR 022 §Bootstrap cold-start)"
    );
    tasks.spawn(crate::dht::publish::run_republish(
        ep.clone(),
        secret_key.public(),
        Arc::clone(&dht_routing),
        Arc::clone(&republish_scheduler),
        cache.clone(),
        cache_inserts_rx,
        republish_stop_rx,
    ));

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
        ep.clone(),
        secret_key.public(),
        Arc::clone(&dht_routing),
        bucket_refresh_stop_rx,
        crate::dht::bucket_refresh::BUCKET_REFRESH_TICK,
        Arc::clone(&bucket_refresh_clock),
    ));

    // RPC connectivity watchdog (issue #283). Updates `decdn_rpc_healthy`
    // each tick; a sustained transition fires an alert. `interval == 0`
    // disables the watchdog entirely (operators can opt out for offline
    // dev). Spawned outside the `JoinSet` because we drive it via its own
    // `oneshot` and an explicit `await` during drain — same shape as the
    // gossip handles, since `JoinSet::abort_all` cancels eagerly and we'd
    // rather let the watchdog observe its `shutdown` arm.
    //
    // Seed the gauge to `1` here unconditionally: the startup
    // `check_rpc_reachability` above already established the endpoint is
    // reachable, and registered Prometheus gauges otherwise default to 0
    // — which alerts would (correctly, by their own logic) read as an
    // outage. Seeding before the watchdog-spawn branch covers the
    // `interval == 0` case too.
    node_metrics.rpc_healthy(true);
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
            Arc::clone(&node_metrics),
            rx,
        );
        Some((tx, handle))
    } else {
        tracing::info!("RPC watchdog disabled (blockchain.rpc_watchdog_interval_sec = 0)");
        None
    };

    let peer_table = Arc::new(RwLock::new(PeerTable::new(
        cfg.gossip.peer_ttl_sec.saturating_mul(1_000_000),
        // Saturate at `usize::MAX` on 32-bit targets where the configured
        // `u64` cap might not fit; mirrors the saturating cast pattern
        // already used on `i64::try_from(table.len())` in the sweeper
        // path. The config resolver rejects `0`, so the runtime never
        // hits the unbounded escape hatch.
        usize::try_from(cfg.gossip.max_peer_table_entries).unwrap_or(usize::MAX),
    )));

    // Admin HTTP surface (ADR 025). Bind here — *before* the gossip service
    // spawns — so startup fails fast on a port collision rather than after
    // side-effectful subscriptions have registered. The `serve` task is
    // spawned later, once gossip has produced its `AnnounceTrigger`, so the
    // `admin_v1_announce` method has somewhere to forward to.
    let admin_listener = if let Some(admin_port) = cfg.observability.admin_port {
        let admin_addr = std::net::SocketAddr::from(([127, 0, 0, 1], admin_port));
        Some(
            admin::bind(admin_addr)
                .await
                .context("failed to bind admin listener")?,
        )
    } else {
        tracing::info!("admin server disabled (observability.admin_port = 0)");
        None
    };

    let gossip_runtime_cfg = GossipRuntimeConfig {
        announce_interval_sec: cfg.gossip.announce_interval_sec,
        subscribe_global: cfg.gossip.subscribe_global,
        region: cfg.identity.region.clone(),
        allowlist: cfg.gossip.allowlist.iter().copied().collect::<HashSet<_>>(),
    };
    let gossip_metrics: Arc<dyn GossipMetrics> =
        Arc::new(NodeGossipMetrics::new(Arc::clone(&node_metrics)));
    // GossipService owns its own shutdown via this token (#805): cancelling
    // it makes the publisher / subscriber / TTL-sweeper loops return at a
    // clean await boundary, so the runtime no longer reaches in with
    // `.abort()`. The handles still live outside the `JoinSet` because we
    // cancel the token *after* `router.shutdown()` (an `abort_all()` would
    // cancel eagerly), then await them in the drain phase below.
    let gossip_shutdown = CancellationToken::new();
    let decdn_gossip::GossipHandles {
        tasks: gossip_handles,
        announce_trigger,
    } = GossipService::spawn(
        ep.clone(),
        secret_key.clone(),
        gossip.clone(),
        gossip_runtime_cfg,
        Arc::clone(&peer_table),
        gossip_metrics,
        gossip_shutdown.clone(),
    )
    .await
    .context("gossip service failed to start")?;

    // Drain trigger for `admin_v1_drain` (issue #244). Constructed
    // unconditionally so the admin handler always has a live target —
    // there is no "drain disabled" state analogous to "no region" or
    // "no config path". The `Arc` is shared with the admin state (via
    // `Arc::clone`) and the select loop arm below.
    let drain_trigger = Arc::new(admin::DrainTrigger::new());

    // Now that gossip is up and we know whether the publisher produced an
    // `AnnounceTrigger`, spawn the admin serve task with the full state.
    // Bind happened earlier (see `admin_listener` above) so a port collision
    // would have failed startup before any side-effectful subscribes ran.
    let mut admin_stop_tx = if let Some(listener) = admin_listener {
        let (tx, rx) = oneshot::channel::<()>();
        // Build the reload hook only when a config file path was passed
        // (CLI-only invocation has nothing on disk to re-read). The
        // hook hands `admin_v1_reload` the same `RuntimeReloadState` and
        // file path the SIGHUP arm uses, so both paths converge on a
        // single mutex-serialised reload — see `admin::AdminRpcImpl::reload`
        // and the SIGHUP arm of the select loop below.
        let reload_hook = config_path.as_ref().map(|path| admin::ReloadHook {
            reload_state: Arc::clone(&reload_state),
            config_path: path.clone(),
        });
        let state = admin::AdminState::new(
            Arc::clone(&peer_table),
            *secret_key.public().as_bytes(),
            started_at,
            cache.clone(),
            announce_trigger,
            reload_hook,
            Arc::clone(&drain_trigger),
            Arc::clone(&eth_signer),
            Arc::clone(&node_metrics),
        )
        // DHT introspection for `admin_v1_status` (issue #741). All handles
        // are clones of state the DHT tasks already share — read-only here.
        .with_dht(admin::DhtStatusHandles {
            routing: Arc::clone(&dht_routing),
            staker_set: Arc::clone(&staker_set),
            record_store: Arc::clone(&record_store),
            republish: Arc::clone(&republish_scheduler),
            refresh_clock: Arc::clone(&bucket_refresh_clock),
            refresh_interval: crate::dht::bucket_refresh::BUCKET_REFRESH_TICK,
        })
        // Payment-channel introspection for `admin_v1_channels` (issue #749).
        // Shares the same persistent channel-state store the client handler
        // and settlement service use, plus the in-memory voucher-activity
        // clock — read-only here.
        .with_channels(admin::ChannelStatusHandles {
            channel_store: Arc::clone(&channel_state_store),
            voucher_activity: Arc::clone(&voucher_activity),
            redeem_threshold_micro_usdc: cfg.blockchain.redeem_threshold_micro_usdc,
        });
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
        node_id = %secret_key.public(),
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
        subscribe_global = cfg.gossip.subscribe_global,
        "node runtime ready"
    );

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
                match config_path.as_deref() {
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
    let _ = record_store_gc_stop_tx.send(());
    let _ = dht_rate_limit_gc_stop_tx.send(());
    let _ = republish_stop_tx.send(());
    let _ = bucket_refresh_stop_tx.send(());
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
    let stop_order = admin_stop_order(signal, &drain_trigger);
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
    // then closes the endpoint. After this returns we cancel gossip's
    // shutdown token so its infinite loops (publisher / subscriber / TTL
    // sweeper) exit cooperatively at their next await boundary, letting the
    // drain phase finish rather than hitting the 15s timeout every time.
    if let Err(err) = router.shutdown().await {
        tracing::warn!(%err, "router shutdown reported an error");
    }
    gossip_shutdown.cancel();

    // Redeem on shutdown (#327): now that the router has drained, no further
    // vouchers will arrive and the persisted channel state is final. Close
    // any channel still carrying an un-redeemed claim so a later
    // `settleChannel` can finalize it. Best-effort and bounded so a slow RPC
    // cannot hang shutdown past the deadline.
    payment_service
        .close_open_channels_on_shutdown(SHUTDOWN_DEADLINE)
        .await;

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

    // Last-resort abort handles for the tasks that live *outside* the
    // `JoinSet` — the gossip loops and the RPC watchdog. On the normal path
    // the cooperative cancel (above) and the watchdog oneshot drain them
    // cleanly; these are fired only if `drain` overruns `SHUTDOWN_DEADLINE`,
    // because dropping the timed-out `drain` future would otherwise merely
    // *detach* a wedged task (a dropped `JoinHandle` keeps running), not stop
    // it. `tasks.abort_all()` covers the `JoinSet` (reaped + logged per task
    // below); these cover the rest as fire-and-forget aborts — we do NOT await
    // them past the deadline, since `abort()` only lands at a poll point and a
    // truly non-yielding loop would re-hang the shutdown the timeout escaped.
    // The aggregate count is logged so a post-mortem knows they were hit.
    let gossip_aborts: Vec<_> = gossip_handles
        .iter()
        .map(tokio::task::JoinHandle::abort_handle)
        .collect();
    let watchdog_abort = rpc_watchdog_handle
        .as_ref()
        .map(tokio::task::JoinHandle::abort_handle);

    let drain = async {
        while let Some(result) = tasks.join_next().await {
            log_join_result(result, "shutdown");
        }
        // Await the gossip tasks so the runtime doesn't return while they're
        // still draining. Cancelling the token (above) makes each loop return
        // `Ok(())` cleanly; `log_join_result` reports a genuine panic at `warn`
        // and a cancellation at `debug`, matching how every other drained task
        // is logged. A loop that never reached an await boundary would keep
        // `drain` from completing, in which case the `SHUTDOWN_DEADLINE` timeout
        // below fires and the `else` branch force-aborts via `gossip_aborts`.
        for handle in gossip_handles {
            log_join_result(handle.await, "gossip-shutdown");
        }
        // Await the RPC watchdog. We signalled it via oneshot above, so
        // a healthy run resolves cleanly here. A panic surfaces as a
        // warning; cancellation is silent (matches gossip handling).
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
            out_of_joinset_aborts = gossip_aborts.len() + usize::from(watchdog_abort.is_some()),
            "graceful shutdown timed out; aborting remaining tasks",
        );
        tasks.abort_all();
        // The gossip loops and RPC watchdog live outside `tasks`; dropping the
        // timed-out `drain` only detached them, so abort explicitly. Unlike the
        // `JoinSet`, these are not reaped/awaited afterwards (see the rationale
        // where the abort handles are collected) — the count above is their
        // only per-shutdown record.
        for abort in &gossip_aborts {
            abort.abort();
        }
        if let Some(abort) = &watchdog_abort {
            abort.abort();
        }
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
    transport_config: QuicTransportConfig,
) -> anyhow::Result<Endpoint> {
    let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, bind_port);
    Endpoint::builder(presets::N0)
        .secret_key(secret_key.clone())
        .transport_config(transport_config)
        // ADR 015 §Session Ticket Management. In iroh this knob sizes
        // only the *client-side* `ClientSessionMemoryCache` — i.e. the
        // tickets THIS node caches when it probes others (default 256;
        // we raise it). The inbound/serving side's ticket store is
        // rustls-internal and unaffected by this. Set unconditionally:
        // it only matters when this node resumes outbound, and
        // `network.enable_0rtt` gates whether the probe handler accepts
        // inbound resumption.
        .max_tls_tickets(decdn_protocol::SESSION_TICKET_CACHE_SIZE)
        .bind_addr(bind_addr)
        .map_err(|e| anyhow::anyhow!("invalid bind addr {bind_addr}: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("endpoint bind {bind_addr} failed: {e}"))
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

/// Adapter: implements `decdn_gossip::GossipMetrics` against the node's
/// Prometheus registry.
#[derive(Debug)]
struct NodeGossipMetrics {
    metrics: Arc<metrics::Metrics>,
}

impl NodeGossipMetrics {
    const fn new(metrics: Arc<metrics::Metrics>) -> Self {
        Self { metrics }
    }
}

impl GossipMetrics for NodeGossipMetrics {
    fn inc_published(&self, topic: &str) {
        self.metrics.gossip_published(topic);
    }
    fn inc_received(&self, topic: &str) {
        self.metrics.gossip_received(topic);
    }
    fn inc_rejected(&self, reason: &'static str) {
        self.metrics.gossip_rejected(reason);
    }
    fn set_peer_table_size(&self, n: i64) {
        self.metrics.gossip_peer_table_size(n);
    }
    fn inc_reconnected(&self, topic: &str) {
        self.metrics.gossip_reconnected(topic);
    }
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
    let engine = CacheEngine::open_full(
        &cfg.cache.cache_dir,
        origins,
        cfg.cache.max_blob_size_mb,
        cfg.cache.pinned_hashes.clone(),
        cfg.cache.origin_retry,
        Some(node_metrics.cache_metrics()),
        std::time::Duration::from_secs(cfg.cache.gc_interval_sec),
    )
    .await
    .context("failed to open cache engine")?;
    // `cache.*` is restart-required (not hot-reloaded), so applying the
    // probe-hold budget once here is sufficient (ADR 005 §Hold budget, #318).
    engine.set_max_probe_holds(cfg.cache.max_probe_holds);
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

/// Verify that the JSON-RPC endpoint is reachable by sending a lightweight
/// `net_version` request with a short timeout. Logs a warning and returns an
/// error if the endpoint does not respond, letting operators catch typos and
/// dead endpoints before the node binds ports and joins the gossip network.
async fn check_rpc_reachability(rpc_url: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("failed to build HTTP client for RPC check")?;
    probe_rpc(&client, rpc_url).await?;
    tracing::info!("RPC endpoint reachable");
    Ok(())
}

/// Issue a single `net_version` JSON-RPC probe against `rpc_url` using the
/// given client. Returns `Ok(())` on a 2xx response, an error otherwise.
/// Extracted so the startup check and the watchdog share identical
/// success/failure semantics — a deviation between the two would mean
/// "startup says healthy, gauge says unhealthy" or vice versa.
pub(crate) async fn probe_rpc(client: &reqwest::Client, rpc_url: &str) -> anyhow::Result<()> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "net_version",
        "params": [],
        "id": 1
    });

    let resp = client
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .context("blockchain.rpc_url is not reachable (timeout or connection refused)")?;

    anyhow::ensure!(
        resp.status().is_success(),
        "blockchain.rpc_url returned unexpected status {}; \
         verify the endpoint is a valid JSON-RPC server",
        resp.status()
    );

    Ok(())
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
                        // URL omitted for the same reason as the recovery log
                        // above; `%err` keeps the actionable context.
                        tracing::warn!(%err, "RPC endpoint unhealthy");
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

    fn cfg_with_origins(origins: Vec<ResolvedOrigin>) -> (tempfile::TempDir, ResolvedConfig) {
        use decdn_common::config::{
            ResolvedBlockchain, ResolvedGossip, ResolvedIdentity, ResolvedNetwork,
            ResolvedObservability, ResolvedPayment, ResolvedSecurity,
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
                relay_url: None,
                enable_0rtt: true,
            },
            blockchain: ResolvedBlockchain {
                rpc_url: "http://localhost:8545".into(),
                eth_keystore: PathBuf::from("/tmp/keystore.json"),
                keystore_password_file: None,
                payment_channel_address: "0x0000000000000000000000000000000000000001".into(),
                capacity_bond_address: "0x0000000000000000000000000000000000000002".into(),
                rpc_watchdog_interval_sec: 30,
                redeem_threshold_micro_usdc: 1_000_000,
                buyer_deposit_micro_usdc: 10_000_000,
                buyer_max_approve: true,
                settlement_auto_threshold_micro_usdc: None,
                settlement_auto_by_voucher_nonce_span: None,
                slash_judge_address: "0x0000000000000000000000000000000000000003".to_string(),
                chain_id: decdn_common::config::DEFAULT_CHAIN_ID,
            },
            cache: decdn_common::config::ResolvedCache {
                cache_dir,
                cache_size_mb: 1024,
                max_blob_size_mb: 128,
                origins,
                pinned_hashes: decdn_cache::PinnedHashes::empty(),
                origin_retry: decdn_cache::RetryPolicy::default(),
                user_agent: decdn_cache::DEFAULT_USER_AGENT.to_string(),
                gc_interval_sec: 0,
                max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
                stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
            },
            payment: ResolvedPayment {
                rate_per_mb: 10,
                delivery_floor: 0,
                delivery_ceiling: decdn_protocol::MAX_RATE_PER_MB,
                voucher_interval_mb: decdn_protocol::DEFAULT_VOUCHER_INTERVAL_MB,
            },
            observability: ResolvedObservability {
                log_level: decdn_common::cli::common::LogLevel::Info,
                log_format: decdn_common::cli::common::LogFormat::Pretty,
                metrics_port: 9090,
                metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                admin_port: Some(9191),
                otlp_endpoint: None,
            },
            gossip: ResolvedGossip {
                announce_interval_sec: 60,
                peer_ttl_sec: 600,
                subscribe_global: false,
                allowlist: Vec::new(),
                max_peer_table_entries: 100_000,
            },
            security: ResolvedSecurity {
                max_concurrent_handlers: 256,
                per_source_rate_per_sec: 100.0,
                per_source_burst: 200,
                max_tracked_sources: 4096,
            },
            dht: decdn_common::config::ResolvedDht::default(),
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
        let _engine = build_cache(&cfg, metrics_handle)
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

        let _engine = build_cache(&cfg, metrics_handle)
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

    /// `run_dispatch_gc` exits promptly when the stop oneshot fires,
    /// even if the next ticker tick is far away. Without the
    /// shutdown-wins-over-tick `biased` select, a regression that
    /// dropped the stop arm or polled it after `ticker.tick()` would
    /// silently extend `SHUTDOWN_DEADLINE` by up to one full
    /// `DISPATCH_GC_INTERVAL` (60s by default).
    #[tokio::test]
    async fn run_dispatch_gc_exits_promptly_on_shutdown() {
        use crate::dispatch::ConnectionLimiter;
        use crate::metrics::Metrics;
        use decdn_common::config::ResolvedSecurity;

        let metrics = Arc::new(Metrics::new());
        let cfg = ResolvedSecurity {
            max_concurrent_handlers: u32::MAX,
            per_source_rate_per_sec: 0.0,
            per_source_burst: 0,
            max_tracked_sources: 0,
        };
        let limiter = Arc::new(ConnectionLimiter::new(&cfg, metrics));

        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        // 60s interval to mirror the runtime default: the test would hang
        // for 60s on a regression that polled the ticker before the stop
        // signal, so the timeout below catches the real bug rather than
        // an unrelated short-interval race.
        let task = tokio::spawn(run_dispatch_gc(
            Arc::clone(&limiter),
            stop_rx,
            Duration::from_mins(1),
        ));

        // Give the task a moment to enter the select loop, then signal
        // shutdown. The task should exit well within the timeout —
        // we allow generous headroom for slow CI runners.
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop_tx.send(()).expect("receiver still alive");

        let result = tokio::time::timeout(Duration::from_millis(500), task).await;
        assert!(
            result.is_ok(),
            "run_dispatch_gc must exit within 500ms of shutdown signal; \
             a 60s hang here means the stop arm of the select was lost"
        );
        result
            .expect("timeout already asserted")
            .expect("task should not panic");
    }

    /// `run_record_store_gc` exits promptly when the stop oneshot
    /// fires, mirroring the `run_dispatch_gc` contract above. A
    /// regression that reordered the `tokio::select!` arms or dropped
    /// `biased` would silently extend `SHUTDOWN_DEADLINE` by up to
    /// one tick interval — under default `DISPATCH_GC_INTERVAL=60s`
    /// the node would hang for a minute at shutdown.
    #[tokio::test]
    async fn run_record_store_gc_exits_promptly_on_shutdown() {
        use crate::dht::{RecordStore, RecordStoreConfig};

        let records = Arc::new(std::sync::Mutex::new(RecordStore::new(
            RecordStoreConfig::default(),
        )));
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        // 60s interval matches the runtime default so the test would
        // hang for 60s on a regression rather than racing through a
        // shorter interval.
        let task = tokio::spawn(run_record_store_gc(
            Arc::clone(&records),
            stop_rx,
            Duration::from_mins(1),
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        stop_tx.send(()).expect("receiver still alive");
        let result = tokio::time::timeout(Duration::from_millis(500), task).await;
        assert!(
            result.is_ok(),
            "run_record_store_gc must exit within 500ms of shutdown signal; \
             a 60s hang here means the stop arm of the select was lost"
        );
        result
            .expect("timeout already asserted")
            .expect("task should not panic");
    }

    /// `run_dht_rate_limit_gc` exits promptly when the stop oneshot
    /// fires. Same shutdown-promptness contract as the sibling GC
    /// tasks: a regression that reordered the `tokio::select!` arms or
    /// dropped `biased` would silently extend `SHUTDOWN_DEADLINE` by up
    /// to one `DHT_RATE_LIMIT_GC_INTERVAL` (60s) — at default settings
    /// the node would hang for a minute at shutdown.
    #[tokio::test]
    async fn run_dht_rate_limit_gc_exits_promptly_on_shutdown() {
        use crate::dht::rate_limit::{DhtRateLimitConfig, DhtRateLimiter};
        use crate::metrics::Metrics;

        let metrics = Arc::new(Metrics::new());
        let limiter = Arc::new(DhtRateLimiter::new(&DhtRateLimitConfig::default(), metrics));

        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        // 60s interval matches the runtime default so the test would
        // hang for 60s on a regression rather than racing through a
        // shorter interval.
        let task = tokio::spawn(run_dht_rate_limit_gc(
            Arc::clone(&limiter),
            stop_rx,
            Duration::from_mins(1),
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        stop_tx.send(()).expect("receiver still alive");
        let result = tokio::time::timeout(Duration::from_millis(500), task).await;
        assert!(
            result.is_ok(),
            "run_dht_rate_limit_gc must exit within 500ms of shutdown signal; \
             a 60s hang here means the stop arm of the select was lost"
        );
        result
            .expect("timeout already asserted")
            .expect("task should not panic");
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
}
