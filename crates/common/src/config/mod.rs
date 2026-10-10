//! Configuration loading and resolution.
//!
//! Three-layer merge: CLI flags > TOML config file > built-in defaults.

mod errors;
pub mod resolved;
pub mod secret;
pub mod types;

use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use anyhow::Context;

use errors::{IDENTITY_DATA_DIR, IDENTITY_REGION, one_section};

use crate::cli::common::{self, expand_tilde};
use crate::cli::run::RunArgs;
use crate::redact::redact_userinfo;

pub use errors::{ConfigDiagnostics, ConfigNotice, ConfigNoticeLevel};
pub use resolved::{
    LoadShedPolicyKind, ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedContent,
    ResolvedDht, ResolvedDiscovery, ResolvedDiscoveryPeer, ResolvedIdentity, ResolvedLoadShed,
    ResolvedNetwork, ResolvedObservability, ResolvedOrigin, ResolvedPayment, ResolvedProbe,
    ResolvedReceipts, ResolvedS3Config, ResolvedS3Credentials, ResolvedSecurity,
    ResolvedServeEconomics, ResolvedTinyLfu,
};
pub use types::FileConfig;

/// Default QUIC bind port.
const DEFAULT_BIND_PORT: u16 = 4433;

/// The daemon's QUIC bind port as a command without `RunArgs` resolves it:
/// `DECDN_BIND_PORT`, then `file_bind_port` (the config file's
/// `network.bind_port`), then the default. The same precedence
/// [`resolve_config`] applies, minus the `--bind-port` flag that only
/// `decdn-node run` takes. An unparseable env value falls through to the file,
/// as the daemon would reject it at bring-up anyway.
#[must_use]
pub fn configured_bind_port(file_bind_port: Option<u16>) -> u16 {
    std::env::var("DECDN_BIND_PORT")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .or(file_bind_port)
        .unwrap_or(DEFAULT_BIND_PORT)
}
/// Default maximum cache size in megabytes (100 GB) — sized for the large-file
/// (AI model) wedge, where a node holds many multi-GB shards.
const DEFAULT_CACHE_SIZE_MB: u64 = 102_400;
/// Default free disk kept unused on the `cache_dir` volume (8 GiB), defended by
/// the eviction driver against any process (#1930). Sized so a node dropped onto
/// an arbitrary machine leaves comfortable room for the OS, logs, and other
/// services below the cache's reactive (soft-evict + GC-lagged) ceiling.
pub const DEFAULT_DISK_HEADROOM_MB: u64 = 8192;
/// Default largest single blob admitted (50 GB), when `max_blob_size_mb` is unset.
/// Comfortably holds the largest model shard the chunked-manifest wedge delivers
/// while capping the RAM the buffered miss tier spends on one pull. Clamped down to
/// `cache_size_mb` at resolve time, so the `max_blob_size_mb <= cache_size_mb`
/// invariant holds even when an operator shrinks the cache below this.
const DEFAULT_MAX_BLOB_SIZE_MB: u64 = 51_200;
/// Default rate per MB in USDC base units ($0.00001/MB).
const DEFAULT_RATE_PER_MB: u64 = 10;
/// Default Prometheus metrics port. Exposed publicly so `decdn node top`
/// (issue #275) can fall back to the same number the daemon binds on
/// without duplicating the constant.
pub const DEFAULT_METRICS_PORT: u16 = 9090;
/// Default metrics bind address (loopback). Operators in containerised
/// deployments override to `0.0.0.0` via CLI/env/config.
const DEFAULT_METRICS_BIND: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
/// Default loopback admin HTTP port (`appendix-local-admin-http.md`). Exposed to the rest of
/// the `node` crate so `decdn node <sub>` clients can fall back to the
/// same default the server binds on, without duplicating the number.
pub const DEFAULT_ADMIN_PORT: u16 = 9191;
/// Default interval between RPC connectivity watchdog probes. `0`
/// disables the watchdog; absent in config => this value.
const DEFAULT_RPC_WATCHDOG_INTERVAL_SEC: u64 = 30;
/// Minimum non-zero watchdog interval. Values below this would have
/// the watchdog probing the RPC endpoint frequently enough to risk
/// tripping provider rate limits or exhausting paid quotas.
const MIN_RPC_WATCHDOG_INTERVAL_SEC: u64 = 10;
/// Default chain-event poll interval (milliseconds). One knob, two unrelated
/// consumers (see `ResolvedBlockchain::event_poll_interval_ms`): the
/// `eth_getLogs` watcher tick cadence, and alloy's pending-transaction receipt
/// heartbeat. 7000 ms matches alloy's non-local default, so live-RPC load is
/// unchanged from before #1011; it also overrides alloy's 250 ms localhost
/// default, which the receipt heartbeat would otherwise use against a dev anvil.
const DEFAULT_EVENT_POLL_INTERVAL_MS: u64 = 7000;
/// Minimum chain-event poll interval. The node polls chain events with
/// `eth_getLogs` and issues no `eth_getFilterChanges` anywhere. At 250 ms the node's
/// ~6 watcher loops would each scan `[cursor, head]` four times a second against
/// one endpoint, tripping provider rate limits and burning paid quota just as the
/// original flood did. The same floor bounds the receipt-heartbeat consumer.
/// 250 ms is kept as the floor because it is alloy's own localhost cadence: a dev
/// anvil can still be driven at the fastest interval alloy itself considers sane.
const MIN_EVENT_POLL_INTERVAL_MS: u64 = 250;
/// Default ceiling on the block span of one chain-watcher `eth_getLogs`
/// request. A node that resumes far behind head scans the gap in windows of at
/// most this many blocks, because one unbounded request would exceed the range
/// and result caps that RPC providers enforce. 10 000 clears the common
/// paid-tier range caps. It bounds the block span, not the result count, so a
/// dense window can still trip a result cap. A provider with a lower cap (free
/// tiers go down to 10 blocks) or a result cap rejects the request, and the
/// poller then halves its window until the provider accepts it.
pub const DEFAULT_GET_LOGS_MAX_BLOCK_SPAN: u64 = 10_000;

/// Default accrued-claim redemption threshold: 1 USDC (`1_000_000` `µUSDC`).
/// At this size the ~$0.10 redeem gas is a few percent of the redeemed
/// amount while bounding unsettled exposure to ~1 USDC per pool (#327).
const DEFAULT_REDEEM_THRESHOLD_MICRO_USDC: u64 = 1_000_000;
/// Default redemption chunk size: 300 vouchers per `redeemMany` transaction.
/// The benchmarks in `contracts/test/PaymentPool.t.sol` pin two marginals for
/// one added cold-lane voucher: ~34.5k gas when the signer is already
/// registered (`test_redeemMany_gas_N…`), and ~63.4k gas when the signer is
/// first-time and its capability registers in the same call
/// (`test_redeemMany_gas_firstTime_N…`). A high-fan-out node serving one-time
/// payers hits the first-time case on every lane, so ~63.4k is the sizing
/// figure. Registration cost is bounded: the node only redeems capabilities
/// whose owner signature it verified off-chain against an EOA pool owner
/// (`ClientHandler::intake_capability` rejects contract/ERC-1271 owners), so no
/// unbounded owner-signature verification enters a `redeemMany`. Against
/// Arbitrum One's block gas limit (~32M gas, an external reference — confirm
/// live via `eth_getBlockByNumber` before a deploy decision), 300 first-time
/// lanes cost ~19M gas: they fit a full block (the first-time ceiling is ~504
/// lanes) but exceed a conservative half-block budget (~252 lanes). The
/// reactive halve-retry in `submit_chunk` splits any chunk that a live block
/// still rejects, so 300 stays safe with that backstop.
const DEFAULT_REDEEM_MAX_VOUCHERS_PER_TX: u64 = 300;
/// Default redeemer self-tick interval: 300s (5 min). Kept well below the
/// hourly expiry sweep so accrued earnings are withdrawn promptly without
/// leaning on the advisory per-voucher hints (#327, #751).
///
/// `pub` so a `decdn-node` client handler built without config, and `decdn pool
/// assign`, take the same interval for the capability-expiry margin.
pub const DEFAULT_REDEEM_INTERVAL_SECS: u64 = 300;
/// Time the node's redeemer allows a `redeemMany` to land on chain: 120 s. The
/// redeemer skips a lane whose capability expires, or whose pool's close deadline
/// falls, within this slack of now: a transaction that lands past the expiry pays
/// 0, and one that lands past the deadline reverts `PoolClosed` for its whole
/// batch.
pub const REDEEM_LANDING_SLACK_SECS: u64 = 120;
/// The node's capability-expiry margin in seconds for a redeem interval: one
/// interval plus [`REDEEM_LANDING_SLACK_SECS`] (ADR 003 §Revocation). Inside the
/// margin the node accepts no voucher and no preimage for the capability
/// ([`inside_capability_expiry_margin`]), so the lane's claim is final. The
/// redeemer sweeps the lane one second after the margin starts, and that sweep
/// has one interval to start the redemption and the landing slack to land it.
#[must_use]
pub const fn capability_expiry_margin_secs(redeem_interval_secs: u64) -> u64 {
    redeem_interval_secs.saturating_add(REDEEM_LANDING_SLACK_SECS)
}
/// Whether the Unix second `now` falls inside the expiry margin of a capability
/// that expires at `expiry`: `now + margin_secs >= expiry`. An `expiry` of `0`
/// means "not tracked" and is never inside the margin.
#[must_use]
pub const fn inside_capability_expiry_margin(expiry: u64, margin_secs: u64, now: u64) -> bool {
    expiry != 0 && now.saturating_add(margin_secs) >= expiry
}
/// Upper bound on the redeemer self-tick interval: 6h (`21_600s`). The sweep is the
/// node's only defense against an owner's grace-window close — it must run several
/// times inside the 48h grace floor so accrued vouchers redeem before the owner
/// can `reclaim`. 6h leaves 8× headroom for tx landing and retries. An operator
/// who wants a laxer cadence to shave gas builds from source or opens an issue.
const MAX_REDEEM_INTERVAL_SECS: u64 = 6 * 60 * 60;
/// Default pool-open and refill-target deposit every top-up restores the pool
/// balance toward: 10 USDC (`10_000_000` `µUSDC`). ADR 003 § Deposit Economics
/// recommends a 10 USDC practical minimum (gas overhead ~2.3%); it is a
/// client-side recommendation, not an on-chain floor, so the resolved value is
/// escrowed as configured (#744).
///
/// `pub` so `decdn-cli`'s `--working-deposit-micro-usdc` resolution shares this
/// single source of truth with the config resolver rather than duplicating it.
pub const DEFAULT_BUYER_WORKING_DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// Default refundable floor `M`: 1 USDC (`1_000_000` `µUSDC`). ADR 003 §
/// Sizing defines `M = k·ρ·B·Δ` (redeem cadence × rate × credit window ×
/// round-trip slack); this is a conservative static value at the
/// redeem-threshold scale, picked so the node keeps enough headroom to cover
/// one in-flight credit window even against a buyer that stops topping up.
/// Precise sizing per ADR 003 is governance/ops policy, not a build-time
/// constant.
pub const DEFAULT_POOL_MIN_REMAINING_DEPOSIT_MICRO_USDC: u64 = 1_000_000;
/// Default per-signer LIVE concurrency cap `k`, in ramp-start credit windows: `8`.
/// The most live un-vouchered floor reservation any one capability signer may hold
/// against a pool is `k · one credit window`, underneath the pool-wide `remaining − M`
/// ceiling (ADR 003 § Pool solvency, per-signer floor isolation). The window is the
/// unit rationed — honest need is `concurrent un-vouchered streams × one window`
/// regardless of pool size — so an absolute count fits it directly. It carries no
/// permanent memory: it recycles as each stream pays, and never penalizes a signer for
/// quitting.
pub const DEFAULT_POOL_FLOOR_SIGNER_LIVE_WINDOWS: u64 = 8;
/// Default global cap on concurrent in-flight QUIC handler tasks.
const DEFAULT_MAX_CONCURRENT_HANDLERS: u32 = 256;
/// Default per-source rate-limit refill (cells/second). A single source
/// may legitimately host a fleet of clients; the value is generous
/// enough to absorb that without rejecting well-behaved peers.
const DEFAULT_PER_SOURCE_RATE_PER_SEC: f64 = 100.0;
/// Default per-source burst — 2× the steady-state rate gives well-behaved
/// peers headroom for jitter/clumping that would otherwise produce
/// spurious rejections at burst == rate.
const DEFAULT_PER_SOURCE_BURST: u32 = 200;
/// Default hard cap on tracked source entries in the keyed limiter.
const DEFAULT_MAX_TRACKED_SOURCES: usize = 4096;
/// Egress ceiling off unless the operator sets one.
const DEFAULT_LOAD_SHED_EGRESS_BUDGET_MBPS: u64 = 0;
/// Load-shed concurrency high-water mark (start shedding misses at/above).
const DEFAULT_LOAD_SHED_SERVES_HIGH: u32 = 256;
/// Load-shed concurrency low-water mark (resume at/below).
const DEFAULT_LOAD_SHED_SERVES_LOW: u32 = 192;
/// Default per-client concurrent-serve cap under pressure.
const DEFAULT_LOAD_SHED_PER_CLIENT_CAP: u32 = 32;
/// Default sustained per-peer (`NodeId`) rate for `cdn/dht/v1` inbound
/// (ADR 022 §DHT Rate Limiting). Conservative ceiling on adversarial load,
/// not a steady-state target.
const DEFAULT_DHT_PER_PEER_RATE_PER_SEC: f64 = 20.0;
/// Default per-peer burst capacity for `cdn/dht/v1`. ADR 022 default: 40.
const DEFAULT_DHT_PER_PEER_BURST: u32 = 40;
/// Default sustained per-IP rate for `cdn/dht/v1`. ADR 022 default: 100.
const DEFAULT_DHT_PER_IP_RATE_PER_SEC: f64 = 100.0;
/// Default per-IP burst capacity for `cdn/dht/v1`. ADR 022 default: 200.
const DEFAULT_DHT_PER_IP_BURST: u32 = 200;
/// Default sustained global rate for `cdn/dht/v1`. ADR 022 default: 1000.
const DEFAULT_DHT_GLOBAL_RATE_PER_SEC: f64 = 1000.0;
/// Default global burst capacity for `cdn/dht/v1`. ADR 022 default: 2000.
const DEFAULT_DHT_GLOBAL_BURST: u32 = 2000;
/// Default hard cap on tracked per-IP entries in the DHT keyed limiter
/// (#645). Mirrors `DEFAULT_MAX_TRACKED_SOURCES` for the dispatch layer.
const DEFAULT_DHT_MAX_TRACKED_PER_IP: usize = 4096;
/// Default hard cap on tracked per-peer (`NodeId`) entries in the DHT
/// keyed limiter (#645).
const DEFAULT_DHT_MAX_TRACKED_PER_PEER: usize = 4096;
/// Default sustained per-peer (`NodeId`) rate for `cdn/probe/v1` inbound
/// (ADR 005 §Probe rate limiting). Tighter than the DHT layer because a
/// probe is unauthenticated and cheaper to flood.
const DEFAULT_PROBE_PER_PEER_RATE_PER_SEC: f64 = 5.0;
/// Default per-peer burst capacity for `cdn/probe/v1`. ADR 005 default: 5.
const DEFAULT_PROBE_PER_PEER_BURST: u32 = 5;
/// Default sustained per-IP rate for `cdn/probe/v1`. ADR 005 default: 50.
const DEFAULT_PROBE_PER_IP_RATE_PER_SEC: f64 = 50.0;
/// Default per-IP burst capacity for `cdn/probe/v1`. ADR 005 default: 200.
const DEFAULT_PROBE_PER_IP_BURST: u32 = 200;
/// Default sustained global rate for `cdn/probe/v1`. ADR 005 default: 1000.
const DEFAULT_PROBE_GLOBAL_RATE_PER_SEC: f64 = 1000.0;
/// Default global burst capacity for `cdn/probe/v1`. ADR 005 default: 2000.
const DEFAULT_PROBE_GLOBAL_BURST: u32 = 2000;
/// Default hard cap on tracked per-IP entries in the probe keyed limiter
/// (#645). Mirrors `DEFAULT_DHT_MAX_TRACKED_PER_IP`.
const DEFAULT_PROBE_MAX_TRACKED_PER_IP: usize = 4096;
/// Default hard cap on tracked per-peer (`NodeId`) entries in the probe
/// keyed limiter (#645).
const DEFAULT_PROBE_MAX_TRACKED_PER_PEER: usize = 4096;
/// Default interval between iroh-blobs GC sweeps in seconds (#518). Five
/// minutes balances the hostile-origin amplification window against the
/// per-sweep cost of walking the blob list. The window matters because
/// a single failed pull-through orphans up to `max_blob_size_mb`
/// (one upload-per-request bound, not multiplied by the interval), but
/// a stream of failed requests inside one interval compounds: total
/// leak before reclaim is bounded by `requests_in_window * max_blob_size_mb`.
/// Tuning the interval down shrinks that window. Operators on lean disks
/// can tune lower; setting to `0` disables the periodic sweep entirely.
pub const DEFAULT_GC_INTERVAL_SEC: u64 = 300;

/// Default interval between origin-held-index rescans in seconds (#1130). A
/// minute balances "drop a file, it's fetchable soon" against directory-walk
/// cost; operators indexing a large fs origin can raise it, and `0` disables
/// the periodic rescan (startup + reload still run one).
pub const DEFAULT_FS_RESCAN_INTERVAL_SEC: u64 = 60;

// Live-origin probe memo defaults (#1130 pt3). Canonical u64 values live in
// `decdn-config-types` so `decdn-cache` (which wraps them as `Duration`s) and
// this crate agree from a single source; re-exported here so the existing
// `crate::config::DEFAULT_ORIGIN_PROBE_*` paths keep resolving.
pub use decdn_config_types::{
    DEFAULT_ORIGIN_PROBE_FAULT_TTL_SEC, DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY,
    DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL_SEC, DEFAULT_ORIGIN_PROBE_TIMEOUT_MS,
    DEFAULT_ORIGIN_PROBE_TTL_SEC,
};

/// Fallback for `cache.relay_foreign_namespaces` (#1759) on a node with no
/// origin configured. The resolver's effective default is role-derived, not
/// this flat constant: an origin node (a backend is configured) defaults to
/// origin-only (`false`) — an origin is not a general proxy — while a
/// no-origin node is a pure relay edge and falls back to this `true`. An
/// operator sets the field explicitly to override either default.
pub const DEFAULT_RELAY_FOREIGN_NAMESPACES: bool = true;

/// Positive-hit TTL for the lazy origin directory cache: how long a resolved,
/// non-empty `getOrigins(namespaceId)` set is served before a re-read. Bounds
/// origin-set staleness (there is no event tail); 5 min matches the negative
/// probe cache order of magnitude.
pub const DEFAULT_ORIGIN_DIRECTORY_POSITIVE_TTL_SEC: u64 = 300;
/// Negative-hit TTL: how long "this namespace has no origins / does not
/// exist" is cached. Shorter than the positive TTL so a namespace that later
/// gains an origin becomes reachable within one short window — and long
/// enough that a flood of bogus/attacker-chosen request namespaces cannot
/// force a `getOrigins` RPC per request. Namespace creation is permissionless
/// and free (`PublisherRegistry.createNamespace`), so this is the `DoS` bound.
pub const DEFAULT_ORIGIN_DIRECTORY_NEGATIVE_TTL_SEC: u64 = 30;
/// Max distinct namespaces held in the lazy origin cache (LRU eviction).
/// Bounds memory against the permissionless global namespace count — the
/// cache only ever holds namespaces this node was actually asked to resolve.
pub const DEFAULT_ORIGIN_DIRECTORY_CACHE_CAPACITY: usize = 4096;

/// Default LRU eviction driver high-water percent of `cache.cache_size_mb`
/// (#1173, ADR 040). Above this
/// fraction the driver actively evicts.
pub const DEFAULT_EVICTION_HIGH_WATER_PCT: u64 = 90;
/// Hard bounds `[60, 95]` for [`DEFAULT_EVICTION_HIGH_WATER_PCT`].
pub const EVICTION_HIGH_WATER_PCT_BOUNDS: (u64, u64) = (60, 95);
/// Default LRU eviction driver target percent of `cache.cache_size_mb` — the
/// driver evicts down to this fraction before idling (#1173).
pub const DEFAULT_EVICTION_TARGET_PCT: u64 = 80;
/// Hard bounds `[40, 90]` for [`DEFAULT_EVICTION_TARGET_PCT`].
pub const EVICTION_TARGET_PCT_BOUNDS: (u64, u64) = (40, 90);
/// Structural hysteresis gap: `eviction_target_pct` must be at least this many
/// points below `eviction_high_water_pct` (#1173).
pub const EVICTION_HYSTERESIS_GAP_PCT: u64 = 5;
/// Default LRU eviction driver max candidates removed per tick (#1173).
pub const DEFAULT_EVICTION_PER_SWEEP_BUDGET: u64 = 16;
/// Hard bounds `[1, 256]` for [`DEFAULT_EVICTION_PER_SWEEP_BUDGET`].
pub const EVICTION_PER_SWEEP_BUDGET_BOUNDS: (u64, u64) = (1, 256);
/// Default LRU eviction driver wakeup cadence in seconds (#1173).
pub const DEFAULT_EVICTION_TICK_SECS: u64 = 1;
/// Hard bounds `[1, 60]` for [`DEFAULT_EVICTION_TICK_SECS`].
pub const EVICTION_TICK_SECS_BOUNDS: (u64, u64) = (1, 60);

/// Default `cache.eviction_policy` (ADR 040). Reproduces pre-ADR-040 behavior.
pub const DEFAULT_EVICTION_POLICY: &str = "lru";
/// Default `cache.admission_policy` (ADR 040). Reproduces pre-ADR-040 behavior.
pub const DEFAULT_ADMISSION_POLICY: &str = "always";
/// Default `cache.tinylfu.sketch_bytes` (ADR 040).
pub const DEFAULT_TINYLFU_SKETCH_BYTES: usize = 262_144;
/// Floor for `cache.tinylfu.sketch_bytes` (ADR 040 §Configuration surface).
///
/// One `u8` per counter over the sketch's four rows means `cols = bytes / 4`,
/// so this floor buys 4096 columns.
///
/// It is set from an over-report target. A count-min sketch reads a key hotter
/// than it is when every one of its four row counters also holds some other
/// key's count — the polluting keys need not be the same one across rows, so
/// the far rarer "one twin collides in all four rows" event does not bound the
/// error. For `N` live hashes over `cols` columns the rate is
/// `(1 - e^(-N / cols))^4`. Sharding cancels out of that expression: a shard
/// divides the columns and the hashes in the same proportion, so the width
/// alone sets the accuracy.
///
/// Because the rate turns only on `N / cols`, a target rate fixes a hash count
/// proportional to the width — about `0.38 * cols` live hashes hold it under
/// one percent. This floor is therefore good for roughly `1_500` hashes and
/// [`DEFAULT_TINYLFU_SKETCH_BYTES`] for roughly `25_000`; a node holding more
/// needs a proportionally wider sketch, not a fixed step up. Counts are an
/// upper bound, since the sketch counts every hash it observes between
/// halvings rather than only the resident ones.
pub const MIN_TINYLFU_SKETCH_BYTES: usize = 16_384;
// A default below its own floor would make every node that ships without a
// `[cache.tinylfu]` block fail to start.
const _: () = assert!(DEFAULT_TINYLFU_SKETCH_BYTES >= MIN_TINYLFU_SKETCH_BYTES);
/// Default `cache.tinylfu.promotion_threshold` (ADR 040).
pub const DEFAULT_TINYLFU_PROMOTION_THRESHOLD: u32 = 2;
/// Default `cache.tinylfu.probation_target_pct` (ADR 040).
pub const DEFAULT_TINYLFU_PROBATION_TARGET_PCT: u64 = 10;
/// Hard bounds `[1, 50]` for [`DEFAULT_TINYLFU_PROBATION_TARGET_PCT`].
pub const TINYLFU_PROBATION_TARGET_PCT_BOUNDS: (u64, u64) = (1, 50);
/// Default `cache.tinylfu.aging_halflife_sec` (ADR 040). Reserved: resolved
/// and stored but not currently consulted by the shipped sketch.
pub const DEFAULT_TINYLFU_AGING_HALFLIFE_SEC: u64 = 600;

/// Default `cache.serve_economics.policy` (ADR 041).
pub const DEFAULT_SERVE_ECONOMICS_POLICY: &str = "margin";
/// Default `cache.serve_economics.discount` (0.5), expressed in basis points.
pub const DEFAULT_SERVE_ECONOMICS_DISCOUNT_BPS: u32 = 5000;
/// Default `cache.serve_economics.n_max` (ADR 041).
pub const DEFAULT_SERVE_ECONOMICS_N_MAX: u32 = 64;
/// Default `cache.serve_economics.warming_budget` (ADR 041). $5 in USDC
/// 6-decimal base units. The per-source net-P&L ledger is debited the full
/// upstream buy cost on a speculative pull and credited the realized operator
/// margin on each serve, so a one-hit blob nets the fee skim (~`0.4·P_sell`) as
/// its lasting loss; on a $0.01/GB flat mesh this `$5` bounds roughly 1,250 GB of
/// unrecovered one-hit warming per source before it drops to the profit floor.
pub const DEFAULT_SERVE_ECONOMICS_WARMING_BUDGET: u64 = 5_000_000;
/// Default `cache.serve_economics.warming_refill` (ADR 041). ~$5/day: honest
/// sources recover their allowance daily; sustained grief is bounded to
/// ≤ $5/day/source. Base units/sec ≈ `5_000_000 / 86_400`.
pub const DEFAULT_SERVE_ECONOMICS_WARMING_REFILL: u64 = 58;

/// Default EIP-712 `chainId` for the `slash_sig` domain separator (ADR 014).
/// Arbitrum Sepolia — the initial network target; matches the chain id bound
/// on the runtime `PrivateKeySigner` (`decdn_incentive::eth_identity`). To
/// target a different chain, override via `blockchain.chain_id` (see
/// `appendix-poc-production-seams.md` §Seam 8).
pub const DEFAULT_CHAIN_ID: u64 = 421_614;

/// Default seconds between the blacklist watcher's periodic re-enumeration +
/// re-scope pass (ADR 011 § Node Behavior's 10-minute cadence).
pub const DEFAULT_CONTENT_BLACKLIST_POLL_INTERVAL_SEC: u64 = 600;

/// Default seconds the node may go without a successful chain read before the
/// serve and probe paths refuse (ADR 011 § Serving while chain-stale). Thirty
/// minutes: comfortably inside the one-hour compliance-window floor, so a node
/// stops serving well before an outage-hidden takedown could cross into
/// slashable territory, while short RPC blips never take the node dark.
pub const DEFAULT_CHAIN_STALENESS_GRACE_SEC: u64 = 1800;

/// Default seconds between authoritative `FeeRouter.getShares()` re-reads by
/// the fee-shares watcher (ADR 041 / ADR 016 § Tunable Economics) — the
/// safety-net cadence alongside the `SharesUpdated` event subscription. One
/// hour.
pub const DEFAULT_FEE_SHARES_POLL_INTERVAL_SEC: u64 = 3600;

/// Default maximum concurrently held (eviction-exempt) blobs for the
/// probe-triggered hold (ADR 005 §Hold budget, #318). Per-blob holds: many
/// peers probing one hash share a single slot. Re-exported from the
/// `decdn_config_types` leaf crate (its canonical home, #578) so
/// the config default and the cache engine's own default (used by
/// direct `CacheEngine::open` callers) cannot drift apart.
pub const DEFAULT_MAX_PROBE_HOLDS: usize = decdn_config_types::DEFAULT_MAX_PROBE_HOLDS;

/// Default probe-hold slots reserved for the stake lane (#757). `0` keeps
/// the stake-lane reservation off by default, so a node that does not opt in
/// shares every probe-hold slot across all peers — the reservation is strictly
/// operator opt-in.
pub const DEFAULT_STAKE_LANE_RESERVED_HOLDS: usize = 0;

/// Default providers probed before ranking on a node-to-node cache-miss pull
/// (#831). Five is enough to find a healthy upstream in a tens-of-nodes
/// network without spending the miss-latency budget on a wide probe fan-out.
pub const DEFAULT_NODE_PULL_PROBE_FANOUT: usize = 5;
/// Default wall-clock bound (seconds) on the STREAM-OPEN stage of a single upstream
/// pull during a node-to-node cache-miss fill (#831) — connect, handshake, signed
/// `StreamResponse`. Matches the integration-test budget; a slow upstream is abandoned
/// for the next ranked candidate at this deadline.
///
/// It does NOT cover the buyer pool open, which precedes it on its own 5 s budget
/// (`CHANNEL_OPEN_CALLER_BUDGET`), nor the streaming that follows it, which is bounded by
/// the throughput floor ([`DEFAULT_NODE_PULL_STALL_WINDOW_SEC`]). All three are sequential
/// stages of ONE candidate attempt, and the node derives the overall pull-through deadline
/// as `MAX_PROVIDER_ATTEMPTS × (pool open + this + window) + a fixed discovery allowance`,
/// so the fallback loop can reach every ranked candidate before the serving path gives up
/// (#859).
///
/// Raising this to give a slow L2 more room does nothing: that is the pool open, on the
/// budget named above.
pub const DEFAULT_NODE_PULL_TIMEOUT_SEC: u64 = 20;
/// Default THROUGHPUT-FLOOR window (seconds) on the streaming stage of an upstream pull
/// (#1797). Bytes are counted off the QUIC stream sub-frame, so it trips only when
/// throughput falls below the floor — never because a blob is large or a frame is big.
///
/// Set equal to [`DEFAULT_NODE_PULL_TIMEOUT_SEC`] because both answer the same
/// question ("how long do we wait on an unresponsive upstream?"), just at
/// different stages; they are separate knobs because only one of them can be
/// safely raised for large content.
///
/// Raising it is not free, even though it does not scale with blob size. A candidate that
/// goes SILENT costs one full window of this before the pull abandons it, and
/// `outer_pull_deadline` must budget that window for each of `MAX_PROVIDER_ATTEMPTS`
/// candidates — otherwise a single silent peer eats the whole deadline and the fallback
/// loop never reaches the others (#859, and the reason this knob is an argument to that
/// function). So each second added here adds ~3 to the worst-case wait a client can see on
/// a total miss: 167.5 s at defaults.
pub const DEFAULT_NODE_PULL_STALL_WINDOW_SEC: u64 = 20;
/// Default minimum sustained upstream throughput (bytes/sec) over
/// [`DEFAULT_NODE_PULL_STALL_WINDOW_SEC`] (#1797). At ~40× a 100 B/s drip and far below any
/// honest link, it catches a slow-drip wedge without tripping on a genuinely slow client.
/// `0` disables the throughput test and leaves pure idle detection (one byte per window).
pub const DEFAULT_NODE_PULL_MIN_THROUGHPUT_BPS: u64 = 4096;

/// Default downstream credit-window ceiling (ADR 003 §Credit window): 64 MiB. A
/// stream's window ramps from one chunk toward this cap in proportion to what
/// the stream has already paid; a fully-ramped high-bandwidth lane runs
/// link-bound within this bound, while a non-paying lane stays pinned at the
/// chunk floor. Node-local policy, floored to one chunk by the serve loop.
pub const DEFAULT_CREDIT_MAX: u64 = 64 * 1024 * 1024;
/// Default ramp divisor (ADR 003 §Credit window): 2. The credit window is at most
/// `paid / credit_ramp_divisor`, so the node's unbilled egress on a stream never
/// exceeds half the revenue the stream has already confirmed. Lower ramps faster;
/// `0` opens the full [`DEFAULT_CREDIT_MAX`] from the first byte.
pub const DEFAULT_CREDIT_RAMP_DIVISOR: u64 = 2;
/// Default serve-path wire-frame target (ADR 005 §`cdn/client/v1`): 1 MiB, matching
/// the payment quantum so a fully-ramped stream sends about one frame per priced
/// chunk. Node-local policy that travels on no message: the payer accepts any
/// non-empty frame, and neither the bao codec's chunk groups nor the payment meter
/// is defined over frame boundaries. Larger frames cost proportionally less
/// per-frame CPU per byte served, and one payment interval is both the default and
/// the maximum — a frame never crosses a payment-chunk boundary. The serve loop also
/// clamps each frame to the credit window's remaining room, so this is a ceiling
/// rather than an exact size.
pub const DEFAULT_FRAME_TARGET_BYTES: u64 = decdn_protocol::CHUNK_BYTES;
/// Default background flush period in milliseconds when
/// `payment.voucher_commit_interval_ms` is unset (ADR 003 §Off-chain voucher
/// state persistence): 5 s.
///
/// The node advances each lane's voucher watermark in memory on verify and
/// mirrors the working set to the redb lane store on this interval — one fsynced
/// transaction covering every dirty lane. A crash loses at most one interval of
/// *frontier*, which is safe to lose: an honest client resumes forward and an
/// un-redeemed replay is still on-chain-payable. The redeemed watermark is
/// floored separately by the pre-redeem flush, so this interval trades only
/// throughput-nines against seconds of harmless frontier replay. Must be `> 0`.
/// Node-local policy, not a wire or governance parameter.
pub const DEFAULT_VOUCHER_COMMIT_INTERVAL_MS: u64 = 5_000;

/// Default size at which the download-receipt log rotates (#802): 128 MiB.
/// With the default `retained_files` this bounds the audit log to ~640 MiB
/// of `data_dir` while still keeping a multi-hundred-MiB delivery history.
pub const DEFAULT_RECEIPT_MAX_FILE_BYTES: u64 = 128 << 20;
/// Floor for `receipts.max_file_bytes` (#802): 1 MiB. Below this, rotation
/// would churn near-constantly at the ~1-line-per-MiB-delivered write rate.
pub const MIN_RECEIPT_MAX_FILE_BYTES: u64 = 1 << 20;
/// Ceiling for `receipts.max_file_bytes` (#802): 1 GiB. A single live file
/// larger than this defeats the point of bounding `data_dir` growth.
pub const MAX_RECEIPT_MAX_FILE_BYTES: u64 = 1 << 30;
/// Default number of rotated receipt-log backups retained (#802).
pub const DEFAULT_RECEIPT_RETAINED_FILES: u32 = 4;
/// Ceiling for `receipts.retained_files` (#802). Generous; the total disk
/// bound is `(retained_files + 1) * max_file_bytes`.
pub const MAX_RECEIPT_RETAINED_FILES: u32 = 100;
/// Canonical filename of the download-receipt audit log within `data_dir`
/// (#802). The path is derived (`data_dir`/this), not a configurable field;
/// the daemon writer and the `config validate` summary both reference this so
/// the surfaced path can't drift from where the log actually lands.
pub const RECEIPT_LOG_FILE: &str = "download_receipts.jsonl";

/// Load config from file (if present) and merge with CLI args.
///
/// CLI args take precedence over file values; defaults fill gaps.
///
/// Returns the resolved config together with every [`ConfigNotice`] the
/// resolvers recorded. Notices are non-fatal by construction, and the caller
/// renders them: the daemon replays them through `tracing` once `init_tracing`
/// has installed a subscriber, `decdn config validate` prints them in its
/// summary, and `decdn node doctor` turns them into findings. Nothing here
/// writes to a sink: no subscriber exists yet in the daemon, the CLI links
/// none at all, and stderr is not the stream an operator is reading.
///
/// # Errors
///
/// Returns an error if:
/// - The config file exists but cannot be read or parsed.
/// - A required field (`rpc_url`, `payment_pool_address`,
///   `capacity_bond_address`) is not provided by any source.
/// - The home directory cannot be determined for default paths.
pub fn resolve_config(
    config_path: Option<&Path>,
    cli: &RunArgs,
) -> anyhow::Result<(ResolvedConfig, Vec<ConfigNotice>)> {
    // `load_file_config` stays fail-fast: a file we could not read, parse,
    // or env-expand never produced a `FileConfig`, so there is nothing to
    // validate. Everything *after* this accumulates into one `bag` so an
    // operator sees every problem in a single pass.
    let file = load_file_config(config_path)?;

    let mut bag = ConfigDiagnostics::new();

    let identity = resolve_identity_into(&cli.identity, file.identity.as_ref(), &mut bag);
    // A region that was supplied but failed `normalize_region` is already
    // reported under `identity.region`; the cross-section publish check
    // must not also fire (see `ensure_region_when_publishing_global_into`).
    // A data_dir we could not determine becomes a `/nonexistent` placeholder
    // (see `resolve_identity_into`); the keystore/cache-dir derived from it
    // would surface as a "cannot access" cascade on top of the real
    // `identity.data_dir` problem, so downstream resolvers skip path-existence
    // checks driven off it when `data_dir_valid` is false. The placeholder
    // never escapes `resolve_config`: `bag.into_result()?` below fails before
    // the materialized `ResolvedConfig` is returned to the caller.
    let data_dir_valid = !bag.has_field(IDENTITY_DATA_DIR);
    let network = resolve_network_into(&cli.network, file.network.as_ref(), &mut bag);
    let blockchain = resolve_blockchain_into(
        &cli.blockchain,
        file.blockchain.as_ref(),
        &identity.data_dir,
        data_dir_valid,
        &mut bag,
    );
    let cache = resolve_cache_into(
        &cli.cache,
        file.cache.as_ref(),
        &identity.data_dir,
        &mut bag,
    );
    let payment = resolve_payment_into(&cli.payment, file.payment.as_ref(), &mut bag);
    let observability =
        resolve_observability_into(&cli.observability, file.observability.as_ref(), &mut bag);
    let security = resolve_security_into(file.security.as_ref(), &mut bag);
    let load_shed = resolve_load_shed_into(file.load_shed.as_ref(), &mut bag);
    let dht = resolve_dht_into(file.dht.as_ref(), &mut bag);
    let probe = resolve_probe_into(file.probe.as_ref(), &mut bag);
    let receipts = resolve_receipts_into(file.receipts.as_ref(), &mut bag);
    let content = resolve_content_into(file.content.as_ref(), &mut bag);

    validate_port_layout_into(&network, &observability, &mut bag);
    ensure_no_hash_pinned_and_denied_into(&cache, &content, &mut bag);

    // Drain before `into_result` consumes the bag. Notices are dropped on the
    // error path deliberately: a config that does not resolve is not the one
    // the operator is running, so its notices would describe nothing live.
    let notices = bag.take_notices();
    bag.into_result()?;

    Ok((
        ResolvedConfig {
            identity,
            network,
            blockchain,
            cache,
            payment,
            observability,
            security,
            load_shed,
            dht,
            probe,
            receipts,
            content,
        },
        notices,
    ))
}

/// Reject a hash that is simultaneously **pinned** (`cache.pinned_hashes`) and
/// **denied** (`content.denied_hashes`, which the serve gate refuses) —
/// contradictory operator intent. A plain pinned hash is held forever and
/// excluded from LRU eviction; a denied one is unservable, so pinning it means
/// paying storage for content the node will never serve. (At runtime the cache's
/// "deny wins over pin" carve-out — see [`decdn_config_types::DeniedHashes`] and
/// `CacheEngine::eviction_candidates` — makes a denied + pinned hash reclaimable
/// under space pressure, so the bytes are not *strictly* held forever; that is a
/// safety net, not a reason to configure the combination.) An operator almost
/// certainly meant one or the other.
///
/// Cross-section because the two lists live in different config sections, so it
/// belongs at the resolver boundary alongside the port-layout check rather than
/// inside either single-section resolver.
///
/// Scope: this is a *static local-config* check only. The on-chain governance
/// deny-set (fed at runtime through `CacheEngine::set_chain_denied`) can still
/// collide with a pinned hash after startup; no static check can catch that.
// Single-section shim preserving the `anyhow::Result` API for the unit tests;
// `resolve_config` uses the `*_into` worker with the shared bag instead.
#[cfg(test)]
fn ensure_no_hash_pinned_and_denied(
    cache: &ResolvedCache,
    content: &ResolvedContent,
) -> anyhow::Result<()> {
    one_section(|bag| ensure_no_hash_pinned_and_denied_into(cache, content, bag))
}

fn ensure_no_hash_pinned_and_denied_into(
    cache: &ResolvedCache,
    content: &ResolvedContent,
    bag: &mut ConfigDiagnostics,
) {
    let mut both: Vec<String> = cache
        .pinned_hashes
        .iter()
        .filter(|&h| content.denied_hashes.contains(h))
        .map(ToString::to_string)
        .collect();
    both.sort();
    bag.check_with(both.is_empty(), "content.denied_hashes", || {
        format!(
            "hash(es) appear in both cache.pinned_hashes and \
             content.denied_hashes: {}. Pinning a denied hash is contradictory — \
             a denied hash is refused by the serve gate, so pinning it only pays \
             storage for content the node will never serve. Remove each from one \
             of the two lists",
            both.join(", ")
        )
    });
}

/// Cross-section check on the running node's port assignments. Lives at the
/// resolver boundary because `bind_port`, `metrics_port`, and `admin_port`
/// come from different sections — the collision rules are inherently
/// cross-section, and operators who set two of them equal almost certainly
/// made a typo. Also emits a stderr warning for well-known ports (<1024),
/// which bind on Unix only with elevated privilege and may collide with
/// standardized services.
///
/// Collision rules:
/// - `bind_port` vs. `metrics_port` — QUIC (UDP) and metrics (TCP) would
///   not collide at bind time, but sharing a number is an operator typo.
/// - `bind_port` vs. `admin_port` — same UDP/TCP story as above.
/// - `metrics_port` vs. `admin_port` — both TCP; sharing the port would
///   silently make the second bind fail at startup.
///
/// Port `0` is the sentinel for OS-assigned ephemeral ports: it collides
/// with nothing (the OS picks distinct values) and needs no elevated
/// privilege, so every check skips it. Admin disabled (`None`) means we
/// skip the pair checks that involve it.
#[cfg(test)]
fn validate_port_layout(
    network: &ResolvedNetwork,
    observability: &ResolvedObservability,
) -> anyhow::Result<()> {
    one_section(|bag| validate_port_layout_into(network, observability, bag))
}

fn validate_port_layout_into(
    network: &ResolvedNetwork,
    observability: &ResolvedObservability,
    bag: &mut ConfigDiagnostics,
) {
    let bind = network.bind_port;
    let metrics = observability.metrics_port;
    let admin = observability.admin_port;

    // bind vs metrics — UDP/TCP, same-number operator typo. The three pair
    // checks are independent: accumulate every collision so an operator who
    // set all three equal sees all of them, not just the first. Each pair
    // gets a distinct label encoding both fields, so the bag can carry all
    // three problems without two collisions colliding under one key (and so
    // a future `has_field` guard could pick out a specific pair).
    if bind != 0 && metrics != 0 {
        bag.check_with(
            bind != metrics,
            "network.bind_port vs observability.metrics_port",
            || format!(
                "network.bind_port ({bind}) must differ from observability.metrics_port ({metrics}); \
                 QUIC (UDP) and metrics (TCP) would not collide at bind time, but sharing \
                 the same port number is almost certainly an operator typo"
            ),
        );
    }

    // bind vs admin — UDP/TCP, same-number operator typo.
    if let Some(admin) = admin
        && bind != 0
        && admin != 0
    {
        bag.check_with(
            bind != admin,
            "network.bind_port vs observability.admin_port",
            || format!(
                "network.bind_port ({bind}) must differ from observability.admin_port ({admin}); \
                 QUIC (UDP) and admin (TCP) would not collide at bind time, but sharing \
                 the same port number is almost certainly an operator typo"
            ),
        );
    }

    // metrics vs admin — both TCP, second bind would fail silently.
    if let Some(admin) = admin
        && metrics != 0
        && admin != 0
    {
        bag.check_with(
            admin != metrics,
            "observability.admin_port vs observability.metrics_port",
            || format!(
                "observability.admin_port ({admin}) must differ from observability.metrics_port ({metrics}); \
                 the two servers cannot share a TCP port"
            ),
        );
    }

    for (name, port) in [
        ("network.bind_port", Some(bind)),
        ("observability.metrics_port", Some(metrics)),
        ("observability.admin_port", admin),
    ] {
        if let Some(p) = port
            && (1..1024).contains(&p)
        {
            bag.warn(
                name,
                format!(
                    "{p} is in the well-known range (<1024); requires elevated privilege to \
                     bind on Unix and may collide with a standardized service"
                ),
            );
        }
    }
}

/// Resolve identity fields.
#[cfg(test)]
fn resolve_identity(
    cli: &crate::cli::run::IdentityArgs,
    file: Option<&types::IdentityConfig>,
) -> anyhow::Result<ResolvedIdentity> {
    one_section(|bag| resolve_identity_into(cli, file, bag))
}

fn resolve_identity_into(
    cli: &crate::cli::run::IdentityArgs,
    file: Option<&types::IdentityConfig>,
    bag: &mut ConfigDiagnostics,
) -> ResolvedIdentity {
    let data_dir = cli
        .data_dir
        .clone()
        .map(|p| expand_tilde(&p))
        .or_else(|| {
            file.and_then(|i| i.data_dir.clone())
                .map(|p| expand_tilde(&p))
        })
        .or_else(common::default_data_dir)
        .unwrap_or_else(|| {
            // Placeholder so blockchain/cache resolution keeps running and
            // accumulating their own problems. The placeholder never escapes:
            // `resolve_config` derives `data_dir_valid` from
            // `bag.has_field(IDENTITY_DATA_DIR)` and stops path-existence
            // cascades; the materialized `ResolvedConfig` carrying the
            // placeholder is dropped when `bag.into_result()?` short-circuits.
            bag.push(
                IDENTITY_DATA_DIR,
                "cannot determine data directory: home dir not found",
            );
            PathBuf::from("/nonexistent")
        });

    let region = match cli
        .region
        .clone()
        .or_else(|| file.and_then(|i| i.region.clone()))
    {
        Some(raw) => bag.try_with(IDENTITY_REGION, normalize_region(&raw)),
        None => None,
    };

    ResolvedIdentity { data_dir, region }
}

/// Normalize an operator-supplied region code: uppercase it and check it
/// against the ISO 3166-1 alpha-2 allowlist in
/// [`decdn_protocol::is_valid_region`] (assigned codes + the user-reserved
/// ranges `AA`, `QM`–`QZ`, `XA`–`XZ`, `ZZ`). A bad value here would
/// otherwise reach the ADR 030 region-latency penalty and region-accounting
/// paths as an unrecognized code peers cannot compare against their own.
/// Fail loudly at startup.
fn normalize_region(raw: &str) -> anyhow::Result<String> {
    let upper = raw.to_ascii_uppercase();
    anyhow::ensure!(
        decdn_protocol::is_valid_region(&upper),
        "identity.region must be an ISO 3166-1 alpha-2 code \
         (assigned or user-reserved AA/QM-QZ/XA-XZ/ZZ), got {raw:?}"
    );
    Ok(upper)
}

/// Resolve network fields, recording any malformed `relay_urls` entry into
/// `bag` (#818).
///
/// Each resolved relay URL is parse-checked with [`url::Url`] so
/// `decdn config validate` fails fast and names a bad entry, instead of the
/// failure only surfacing later at node bring-up (`parse_relay_urls` in the
/// `node` runtime). The check is deliberately a *subset* of bring-up's
/// `iroh::RelayUrl` parse: `RelayUrl` wraps a `url::Url`, so anything
/// `url::Url::parse` rejects `RelayUrl` rejects too — the validate-time gate is
/// never stricter than bring-up (no false positives, e.g. a `relay://` scheme
/// passes both), and bring-up's `RelayUrl` parse stays the authoritative gate
/// for relay-specific shape. The validation here uses only `url`, so the config
/// module needs no `iroh` import (the `common` crate depends on `iroh`
/// elsewhere, e.g. identity loading — this check does not).
///
/// A rejected entry is run through [`redact_userinfo`](crate::redact) before it
/// is echoed, upholding the same "relay userinfo never reaches a log/error"
/// invariant bring-up enforces — a credential-bearing typo
/// (`relay://user:pass@bad host`) is exactly the malformed shape that lands on
/// the error path.
fn resolve_network_into(
    cli: &crate::cli::run::NetworkArgs,
    file: Option<&types::NetworkConfig>,
    bag: &mut ConfigDiagnostics,
) -> ResolvedNetwork {
    let bind_port = cli
        .bind_port
        .or_else(|| file.and_then(|n| n.bind_port))
        .unwrap_or(DEFAULT_BIND_PORT);

    // Relay precedence (highest first): CLI `--relay-url` (singular) >
    // file `network.relay_urls` (list). An empty result means "use the n0
    // default relays" — the node only swaps in a custom relay map when this is
    // non-empty. The CLI/env surface stays singular; the multi-relay surface
    // is the TOML `relay_urls` array (issue #795).
    let relay_urls = if let Some(url) = cli.relay_url.clone() {
        vec![url]
    } else {
        file.and_then(|n| n.relay_urls.clone()).unwrap_or_default()
    };

    // #843: `--relay-url` (and `DECDN_RELAY_URL`, which clap folds into it)
    // takes precedence over the file list, so a stale exported env var silently
    // collapses a multi-entry `network.relay_urls` failover list (#795/#817) to
    // the single env value. Warn rather than defeat relay redundancy quietly.
    let file_relay_list_len = file.and_then(|n| n.relay_urls.as_ref()).map_or(0, Vec::len);
    if cli.relay_url.is_some() && file_relay_list_len > 0 {
        bag.warn(
            "network.relay_url",
            format!(
                "--relay-url (or DECDN_RELAY_URL) overrides the {file_relay_list_len}-entry \
                 network.relay_urls list; multi-relay failover is disabled"
            ),
        );
    }

    // Validate each resolved entry. The label names the source the operator
    // actually wrote: the indexed array field (`network.relay_urls[i]`, matching
    // the `cache.origins[i]` convention) only when the list branch above was
    // taken, otherwise the singular `network.relay_url` (the `--relay-url` /
    // `DECDN_RELAY_URL` override, which always resolves to a one-element vec) —
    // reporting `relay_urls[0]` there would point at an array the operator never
    // defined.
    // The offending entry is echoed with userinfo redacted (a malformed entry
    // can still carry `user:pass@`), mirroring bring-up's `parse_relay_urls`.
    let from_array = cli.relay_url.is_none()
        && file
            .and_then(|n| n.relay_urls.as_ref())
            .is_some_and(|l| !l.is_empty());
    for (i, entry) in relay_urls.iter().enumerate() {
        if let Err(e) = url::Url::parse(entry) {
            let label = if from_array {
                format!("network.relay_urls[{i}]")
            } else {
                "network.relay_url".to_string()
            };
            bag.push(
                label,
                format!("invalid relay URL {:?}: {e}", redact_userinfo(entry)),
            );
        }
    }

    let discovery = resolve_discovery_into(file, bag);

    ResolvedNetwork {
        bind_port,
        relay_urls,
        discovery,
    }
}

/// Resolve just `[network.discovery]` from a loaded [`FileConfig`], for the
/// one-shot client commands (`decdn fetch`/`probe`) that dial nodes by their
/// iroh `NodeId` via discovery but do not run the full node `resolve_config`
/// pass. Applies the exact same shape validation as `resolve_config` does for
/// this section; an absent `[network]`/`[network.discovery]` yields the empty
/// default (the caller falls back to `presets::N0`).
///
/// # Errors
///
/// Fails if any configured discovery field is malformed (same shape checks the
/// full `resolve_config` pass applies to this section).
pub fn resolve_discovery(file: &FileConfig) -> anyhow::Result<ResolvedDiscovery> {
    one_section(|bag| resolve_discovery_into(file.network.as_ref(), bag))
}

/// Resolve and shape-validate `[network.discovery]` (#818 scope 1), recording
/// every malformed entry into `bag`.
///
/// Validation is a parse check (a parseable URL, a non-empty origin, a valid
/// iroh `NodeId` — the canonical 64-char lowercase-hex form — and a parseable
/// `SocketAddr`)
/// using the same parsers the node uses; the authoritative build into iroh
/// types happens in the `node` wiring layer (`build_endpoint`), per the
/// discovery-provider seam in `adr/appendix-poc-production-seams.md`. Echoed
/// URLs are run through [`redact_userinfo`](crate::redact) so a credential-
/// bearing typo never reaches an error string, matching the relay-URL path.
///
/// `pkarr_url` without `dns_origin` is rejected: publishing this node's record
/// to a pkarr relay that no configured resolver reads from is a misconfiguration
/// (the node would advertise into a namespace the fleet never resolves). The
/// reverse — `dns_origin` alone — is valid (a resolve-only node).
fn resolve_discovery_into(
    file: Option<&types::NetworkConfig>,
    bag: &mut ConfigDiagnostics,
) -> ResolvedDiscovery {
    let Some(disc) = file.and_then(|n| n.discovery.as_ref()) else {
        return ResolvedDiscovery::default();
    };

    let pkarr_url = disc.pkarr_url.as_ref().and_then(|raw| {
        bag.try_with(
            "network.discovery.pkarr_url",
            url::Url::parse(raw)
                .map(|_| raw.clone())
                .map_err(|e| anyhow::anyhow!("invalid URL {:?}: {e}", redact_userinfo(raw))),
        )
    });

    let dns_origin = disc.dns_origin.as_ref().and_then(|raw| {
        bag.check(
            !raw.trim().is_empty(),
            "network.discovery.dns_origin",
            "must not be empty when set; omit the key instead",
        )
        .then(|| raw.clone())
    });

    bag.check(
        !(disc.pkarr_url.is_some() && disc.dns_origin.is_none()),
        "network.discovery.dns_origin",
        "network.discovery.pkarr_url publishes this node's address record to a pkarr \
         relay, but no network.discovery.dns_origin is configured to resolve peers from \
         it; set dns_origin or remove pkarr_url",
    );

    let mut peers: Vec<ResolvedDiscoveryPeer> = disc
        .peers
        .iter()
        .flatten()
        .filter_map(|(node_id, peer)| resolve_discovery_peer(node_id, peer, bag))
        .collect();
    // `HashMap` iteration order is nondeterministic; sort so the node build and
    // any test assertions are stable. Unstable sort: peer node_ids are unique
    // (HashMap keys), so stable ordering buys nothing and `sort_unstable_by`
    // avoids the aux allocation.
    peers.sort_unstable_by(|a, b| a.node_id.cmp(&b.node_id));

    ResolvedDiscovery {
        pkarr_url,
        dns_origin,
        peers,
    }
}

/// Shape-validate one `[network.discovery.peers.<id>]` entry, recording any
/// problem into `bag`. Returns the resolved peer only when the `NodeId`, relay
/// URL, and every socket address are well-formed; a bad entry is dropped from
/// the address book but all of its problems are still recorded so an operator
/// sees every fix needed at once.
///
/// The `NodeId` is validated with the exact parser the node uses at bring-up
/// (`iroh::PublicKey`, via `add_discovery_lookups`), not just a 64-hex shape
/// check: `PublicKey::from_str` requires lowercase hex *and* a valid Ed25519
/// curve point, so an uppercase or non-curve-point id that a bare hex check
/// would accept must be rejected here too — otherwise it would pass
/// `config validate` and then fail node startup. This path carries the id as a
/// String the node re-parses, so the config-time and bring-up checks must
/// agree.
fn resolve_discovery_peer(
    node_id: &str,
    peer: &types::DiscoveryPeer,
    bag: &mut ConfigDiagnostics,
) -> Option<ResolvedDiscoveryPeer> {
    let mut ok = bag
        .try_with(
            format!("network.discovery.peers[{node_id}]"),
            node_id.parse::<iroh::PublicKey>().map(|_| ()).map_err(|e| {
                anyhow::anyhow!(
                    "invalid NodeId (expected a 64-char lowercase-hex iroh NodeId): {e}"
                )
            }),
        )
        .is_some();

    if let Some(relay) = peer.relay_url.as_ref() {
        ok &= bag
            .try_with(
                format!("network.discovery.peers[{node_id}].relay_url"),
                url::Url::parse(relay)
                    .map(|_| ())
                    .map_err(|e| anyhow::anyhow!("invalid URL {:?}: {e}", redact_userinfo(relay))),
            )
            .is_some();
    }

    for (i, addr) in peer.addrs.iter().enumerate() {
        ok &= bag
            .try_with(
                format!("network.discovery.peers[{node_id}].addrs[{i}]"),
                addr.parse::<std::net::SocketAddr>()
                    .map(|_| ())
                    .map_err(|e| anyhow::anyhow!("invalid socket address {addr:?}: {e}")),
            )
            .is_some();
    }

    ok.then(|| ResolvedDiscoveryPeer {
        node_id: node_id.to_string(),
        relay_url: peer.relay_url.clone(),
        addrs: peer.addrs.clone(),
    })
}

/// Infallible field-resolution shim for the unit tests that assert on resolved
/// values without exercising the relay-URL validation (those tests supply
/// well-formed URLs, so the discarded bag is always empty). The validation path
/// is covered by the dedicated `resolve_network_*` tests that inspect the bag.
#[cfg(test)]
fn resolve_network(
    cli: &crate::cli::run::NetworkArgs,
    file: Option<&types::NetworkConfig>,
) -> ResolvedNetwork {
    let mut bag = ConfigDiagnostics::new();
    let resolved = resolve_network_into(cli, file, &mut bag);
    // This shim discards the bag, so it must only be fed well-formed relay
    // URLs. Trip loudly if a future caller passes a malformed entry whose
    // problem would otherwise be silently dropped (the validation path proper
    // is covered by the `resolve_network_*` tests that inspect the bag).
    debug_assert_eq!(
        bag.problem_count(),
        0,
        "resolve_network shim discards validation problems; use a bag-aware path"
    );
    resolved
}

/// Validate a user-supplied EVM contract address string.
///
/// Requires `0x` prefix, 40 hex characters, and a correct EIP-55 checksum.
/// Returns the canonical checksummed form. Public so callers that generate
/// config (e.g. `decdn config init --chain`) can apply the same check to
/// baked-in addresses.
pub fn parse_contract_address(flag_name: &str, raw: &str) -> anyhow::Result<String> {
    let trimmed = raw.trim();
    let addr = Address::parse_checksummed(trimmed, None).with_context(|| {
        format!(
            "invalid {flag_name}: value {raw:?} (trimmed: {trimmed:?}); expected an EIP-55 \
             checksummed 0x-prefixed 40-hex-character address"
        )
    })?;
    Ok(addr.to_checksum(None))
}

/// `metadata`/`is_file`/`File::open` triad proving the keystore path
/// exists, is a regular file, and is readable. Kept as one fail-fast
/// `Result` (the three steps are sequentially dependent — one keystore
/// problem, not three) and routed through the bag by the caller so a
/// bad keystore doesn't abort the rest of blockchain resolution.
fn check_keystore_readable(path: &std::path::Path) -> anyhow::Result<()> {
    let meta = std::fs::metadata(path)
        .with_context(|| format!("invalid eth_keystore: cannot access {}", path.display()))?;
    anyhow::ensure!(
        meta.is_file(),
        "invalid eth_keystore: {} is not a regular file",
        path.display()
    );
    std::fs::File::open(path).with_context(|| {
        format!(
            "invalid eth_keystore: cannot open {} for reading",
            path.display()
        )
    })?;
    Ok(())
}

/// Resolve a required contract address: record the "missing required
/// option" problem when absent, or the parse/checksum problem when
/// malformed, and return the canonical checksummed form (empty-string
/// placeholder on failure so resolution keeps accumulating).
fn resolve_contract_address(
    field: &str,
    flag_name: &str,
    missing_msg: &str,
    raw: Option<String>,
    bag: &mut ConfigDiagnostics,
) -> String {
    match raw.filter(|s| !s.is_empty()) {
        None => {
            bag.push(field, missing_msg);
            String::new()
        }
        Some(v) => bag
            .try_with(field, parse_contract_address(flag_name, &v))
            .unwrap_or_default(),
    }
}

/// Resolve blockchain fields.
#[cfg(test)]
fn resolve_blockchain(
    cli: &crate::cli::run::BlockchainArgs,
    file: Option<&types::BlockchainConfig>,
    data_dir: &std::path::Path,
) -> anyhow::Result<ResolvedBlockchain> {
    one_section(|bag| resolve_blockchain_into(cli, file, data_dir, true, bag))
}

// Linear field-by-field resolution (rpc_url, keystore, three contract
// addresses, chain_id, watchdog) — splitting it would scatter the
// "missing required option" error wording that tests assert on.
#[allow(clippy::too_many_lines)]
fn resolve_blockchain_into(
    cli: &crate::cli::run::BlockchainArgs,
    file: Option<&types::BlockchainConfig>,
    data_dir: &std::path::Path,
    data_dir_valid: bool,
    bag: &mut ConfigDiagnostics,
) -> ResolvedBlockchain {
    let rpc_url = match cli
        .rpc_url
        .clone()
        .or_else(|| file.and_then(|b| b.rpc_url.clone()))
        .filter(|s| !s.is_empty())
    {
        None => {
            bag.push(
                "blockchain.rpc_url",
                "missing required option: --rpc-url (or blockchain.rpc_url in config file)",
            );
            // Placeholder; skip the URL parse + scheme checks below — a
            // synthesized URL would only emit a misleading second error.
            String::new()
        }
        Some(raw) => {
            match bag.try_with(
                "blockchain.rpc_url",
                url::Url::parse(&raw).context("blockchain.rpc_url is not a valid URL"),
            ) {
                None => String::new(),
                Some(parsed) => {
                    if bag.check_with(
                        parsed.scheme() == "http" || parsed.scheme() == "https",
                        "blockchain.rpc_url",
                        || {
                            format!(
                                "blockchain.rpc_url must use http or https scheme (got {:?})",
                                parsed.scheme()
                            )
                        },
                    ) {
                        // Store the normalized form (lowercase scheme,
                        // trailing slash, etc.). Userinfo (basic auth) is
                        // preserved by `url::Url::to_string` and we depend
                        // on that for RPC providers that require it.
                        parsed.to_string()
                    } else {
                        String::new()
                    }
                }
            }
        }
    };

    let keystore_from_cli_or_file =
        cli.eth_keystore.is_some() || file.and_then(|b| b.eth_keystore.as_ref()).is_some();
    let eth_keystore = cli
        .eth_keystore
        .clone()
        .map(|p| expand_tilde(&p))
        .or_else(|| {
            file.and_then(|b| b.eth_keystore.clone())
                .map(|p| expand_tilde(&p))
        })
        .unwrap_or_else(|| data_dir.join("keystore.json"));

    // An explicitly-set keystore path (CLI/file) is always validated. A
    // *defaulted* keystore is validated only when data_dir resolved: when
    // data_dir itself failed, the keystore path is `/nonexistent/keystore.json`
    // and a "cannot access" error here is pure cascade noise on top of the
    // real `identity.data_dir` problem (which already fails resolution), so
    // the skip is deliberate cascade-suppression, not an unchecked hole.
    if keystore_from_cli_or_file || data_dir_valid {
        bag.try_with(
            "blockchain.eth_keystore",
            check_keystore_readable(&eth_keystore),
        );
    }

    let payment_pool_address = resolve_contract_address(
        "blockchain.payment_pool_address",
        "payment_pool_address",
        "missing required option: --payment-pool-address \
         (or blockchain.payment_pool_address in config file)",
        cli.payment_pool_address
            .clone()
            .or_else(|| file.and_then(|b| b.payment_pool_address.clone())),
        bag,
    );

    let capacity_bond_address = resolve_contract_address(
        "blockchain.capacity_bond_address",
        "capacity_bond_address",
        "missing required option: --capacity-bond-address \
         (or blockchain.capacity_bond_address in config file)",
        cli.capacity_bond_address
            .clone()
            .or_else(|| file.and_then(|b| b.capacity_bond_address.clone())),
        bag,
    );

    // The all-zero address checksum-validates cleanly but is never a real
    // deployment; settling/staking against a codeless address surfaces only as
    // an opaque on-chain revert. Reject it here — mirroring `slash_judge_address`
    // and `content_blacklist_address` below — so `decdn config check` catches it
    // and the daemon's runtime guard (#1219) is uniform defense-in-depth rather
    // than the sole check. Skipped when the value is an empty placeholder: the
    // missing/parse problem is already recorded.
    if !payment_pool_address.is_empty() {
        bag.check(
            payment_pool_address
                .trim_start_matches("0x")
                .bytes()
                .any(|b| b != b'0'),
            "blockchain.payment_pool_address",
            "blockchain.payment_pool_address must not be the zero address — \
             set it to the deployed PaymentPool contract",
        );
    }
    if !capacity_bond_address.is_empty() {
        bag.check(
            capacity_bond_address
                .trim_start_matches("0x")
                .bytes()
                .any(|b| b != b'0'),
            "blockchain.capacity_bond_address",
            "blockchain.capacity_bond_address must not be the zero address — \
             set it to the deployed CapacityBond contract",
        );
    }

    // Optional chain-backed origin directory (ADR 022 §FIND_VALUE Flow): keyed
    // solely on `OriginAssignment` — a request's namespace resolves directly to
    // `getOrigins(namespaceId)`, with no hash→namespace lookup, so the directory
    // needs only this one address. Unset => the runtime uses an empty (deny-all)
    // directory: the pull-through authorized-origin gate finds no on-chain
    // origins. `publisher_registry_address` is independent — it is the publish
    // CLI's `namespace create` target and is not consumed by the node runtime.
    let origin_assignment_raw = cli
        .origin_assignment_address
        .clone()
        .or_else(|| file.and_then(|b| b.origin_assignment_address.clone()))
        .filter(|s| !s.is_empty());
    let publisher_registry_raw = cli
        .publisher_registry_address
        .clone()
        .or_else(|| file.and_then(|b| b.publisher_registry_address.clone()))
        .filter(|s| !s.is_empty());
    let origin_assignment_address = origin_assignment_raw.and_then(|v| {
        bag.try_with(
            "blockchain.origin_assignment_address",
            parse_contract_address("origin_assignment_address", &v),
        )
    });
    let publisher_registry_address = publisher_registry_raw.and_then(|v| {
        bag.try_with(
            "blockchain.publisher_registry_address",
            parse_contract_address("publisher_registry_address", &v),
        )
    });
    // Present-but-zero is a fail-open trap like the other contract addresses:
    // the directory would resolve every hash against a codeless address. Reject
    // it (mirroring `content_blacklist_address`); a missing value stays `None`.
    if let Some(addr) = origin_assignment_address.as_deref() {
        bag.check(
            addr.trim_start_matches("0x").bytes().any(|b| b != b'0'),
            "blockchain.origin_assignment_address",
            "blockchain.origin_assignment_address must not be the zero address — \
             set it to the deployed OriginAssignment contract",
        );
    }
    if let Some(addr) = publisher_registry_address.as_deref() {
        bag.check(
            addr.trim_start_matches("0x").bytes().any(|b| b != b'0'),
            "blockchain.publisher_registry_address",
            "blockchain.publisher_registry_address must not be the zero address — \
             set it to the deployed PublisherRegistry contract",
        );
    }
    // Required like the other contract addresses: a wrong/zero
    // `verifyingContract` silently produces `slash_sig`s no verifier accepts
    // (ADR 014 §1).
    let slash_judge_address = resolve_contract_address(
        "blockchain.slash_judge_address",
        "slash_judge_address",
        "missing required option: --slash-judge-address \
         (or blockchain.slash_judge_address in config file)",
        cli.slash_judge_address
            .clone()
            .or_else(|| file.and_then(|b| b.slash_judge_address.clone())),
        bag,
    );
    // The all-zero address is syntactically valid but is never a real
    // `SlashJudge` deployment; signing against it produces `slash_sig`s no
    // verifier can attribute (ADR 014 §1). Only meaningful once the address
    // itself parsed — a placeholder empty string here just means the
    // missing/parse problem is already recorded, so skip the cascade.
    if !slash_judge_address.is_empty() {
        bag.check(
            slash_judge_address
                .trim_start_matches("0x")
                .bytes()
                .any(|b| b != b'0'),
            "blockchain.slash_judge_address",
            "blockchain.slash_judge_address must not be the zero address — \
             set it to the deployed SlashJudge contract (ADR 014 §1)",
        );
    }

    // `blockchain.slash_appeal_address` is accepted in the file (see
    // `BlockchainConfig`) but consumed only by the `decdn appeal slash` CLI
    // (via `chain_ctx::resolve_appeal`, which validates it) — the daemon does
    // not resolve or use it, so there is nothing to resolve here.

    // Required for every paid-delivery node: without this contract the daemon
    // has no local protection against serving slashable blacklisted content.
    let content_blacklist_address = resolve_contract_address(
        "blockchain.content_blacklist_address",
        "content_blacklist_address",
        "missing required option: --content-blacklist-address \
         (or blockchain.content_blacklist_address in config file)",
        cli.content_blacklist_address
            .clone()
            .or_else(|| file.and_then(|b| b.content_blacklist_address.clone())),
        bag,
    );
    // The zero address is a fail-open trap for compliance (unlike the opt-in
    // origin-directory addresses): every `isHashBlacklistedForOperator` call
    // against a codeless address reverts on empty return data, the watcher maps
    // that to "leave the blob in place", and nothing is ever evicted while the
    // operator believes compliance is active — maximal slash exposure with a
    // "configured" watcher. Reject it explicitly, mirroring `slash_judge_address`.
    if !content_blacklist_address.is_empty() {
        bag.check(
            content_blacklist_address
                .trim_start_matches("0x")
                .bytes()
                .any(|b| b != b'0'),
            "blockchain.content_blacklist_address",
            "blockchain.content_blacklist_address must not be the zero address — \
             set it to the deployed ContentBlacklist contract (ADR 011/031)",
        );
    }
    let content_blacklist_address =
        (!content_blacklist_address.is_empty()).then_some(content_blacklist_address);
    let content_blacklist_poll_interval_sec = file
        .and_then(|b| b.content_blacklist_poll_interval_sec)
        .unwrap_or(DEFAULT_CONTENT_BLACKLIST_POLL_INTERVAL_SEC);
    // `0` is not a "disable" sentinel — disabling the periodic re-scope would
    // break the retain/re-scope compliance guarantee — and it panics
    // `tokio::time::interval_at` ("period must be non-zero"), which would kill
    // the watcher task and silently stop enforcement. Reject it up front.
    bag.check(
        content_blacklist_poll_interval_sec != 0,
        "blockchain.content_blacklist_poll_interval_sec",
        "blockchain.content_blacklist_poll_interval_sec must not be 0 — the \
         reconcile interval must be non-zero; omit it for the default (600s)",
    );

    let chain_staleness_grace_sec = file
        .and_then(|b| b.chain_staleness_grace_sec)
        .unwrap_or(DEFAULT_CHAIN_STALENESS_GRACE_SEC);
    // `0` is not a "disable" sentinel — a zero grace makes every chain read
    // instantly stale, so the node would refuse every serve. Opting out is done
    // by setting a large window, not `0`. Reject it up front.
    bag.check(
        chain_staleness_grace_sec != 0,
        "blockchain.chain_staleness_grace_sec",
        "blockchain.chain_staleness_grace_sec must not be 0 — a zero grace \
         refuses every serve; set a large value to opt out, or omit it for the \
         default (1800s)",
    );

    let fee_shares_poll_interval_sec = file
        .and_then(|b| b.fee_shares_poll_interval_sec)
        .unwrap_or(DEFAULT_FEE_SHARES_POLL_INTERVAL_SEC);
    // `0` would make the authoritative re-read run every tick (no throttle),
    // hammering the RPC — the `SharesUpdated` event subscription is already the
    // prompt path, so the re-read is a slow safety net. Reject rather than
    // silently over-poll.
    bag.check(
        fee_shares_poll_interval_sec != 0,
        "blockchain.fee_shares_poll_interval_sec",
        "blockchain.fee_shares_poll_interval_sec must not be 0 — the \
         authoritative getShares() re-read is a slow safety net; omit it \
         for the default (3600s)",
    );

    let origin_directory_positive_ttl_sec = file
        .and_then(|b| b.origin_directory_positive_ttl_sec)
        .unwrap_or(DEFAULT_ORIGIN_DIRECTORY_POSITIVE_TTL_SEC);
    let origin_directory_negative_ttl_sec = file
        .and_then(|b| b.origin_directory_negative_ttl_sec)
        .unwrap_or(DEFAULT_ORIGIN_DIRECTORY_NEGATIVE_TTL_SEC);
    let origin_directory_cache_capacity = file
        .and_then(|b| b.origin_directory_cache_capacity)
        .unwrap_or(DEFAULT_ORIGIN_DIRECTORY_CACHE_CAPACITY);
    // No `!= 0` rejection: a `0` TTL is a valid "disable caching" choice
    // (every entry reads as already-expired, matching `probe_cache`'s
    // documented zero-TTL behavior), and the cache clamps capacity to `>= 1`
    // itself.

    let chain_id = cli
        .chain_id
        .or_else(|| file.and_then(|b| b.chain_id))
        .unwrap_or(DEFAULT_CHAIN_ID);
    // `chain_id` is bound into every `slash_sig` EIP-712 domain separator
    // (and the runtime signer). Chain id 0 is not a real network; signing
    // against it produces `slash_sig`s no `SlashJudge` can verify — the same
    // silently-broken-but-running failure mode the zero-`slash_judge_address`
    // check above prevents.
    bag.check_with(chain_id != 0, "blockchain.chain_id", || {
        format!(
            "blockchain.chain_id must not be 0 — set it to the deployed L2 \
             chain id (default {DEFAULT_CHAIN_ID}, Arbitrum Sepolia)"
        )
    });

    let rpc_watchdog_interval_sec = file
        .and_then(|b| b.rpc_watchdog_interval_sec)
        .unwrap_or(DEFAULT_RPC_WATCHDOG_INTERVAL_SEC);
    bag.check_with(
        rpc_watchdog_interval_sec == 0
            || rpc_watchdog_interval_sec >= MIN_RPC_WATCHDOG_INTERVAL_SEC,
        "blockchain.rpc_watchdog_interval_sec",
        || {
            format!(
                "blockchain.rpc_watchdog_interval_sec={rpc_watchdog_interval_sec} \
             would flood the RPC endpoint (minimum {MIN_RPC_WATCHDOG_INTERVAL_SEC}s, \
             or 0 to disable)"
            )
        },
    );

    let event_poll_interval_ms = file
        .and_then(|b| b.event_poll_interval_ms)
        .unwrap_or(DEFAULT_EVENT_POLL_INTERVAL_MS);
    bag.check_with(
        event_poll_interval_ms >= MIN_EVENT_POLL_INTERVAL_MS,
        "blockchain.event_poll_interval_ms",
        || {
            format!(
                "blockchain.event_poll_interval_ms={event_poll_interval_ms} would poll the \
                 RPC endpoint too frequently — it drives both the eth_getLogs watcher \
                 tick and pending-tx receipt polling \
                 (minimum {MIN_EVENT_POLL_INTERVAL_MS}ms)"
            )
        },
    );

    let get_logs_max_block_span = file
        .and_then(|b| b.get_logs_max_block_span)
        .unwrap_or(DEFAULT_GET_LOGS_MAX_BLOCK_SPAN);
    bag.check_with(
        get_logs_max_block_span > 0,
        "blockchain.get_logs_max_block_span",
        || {
            "blockchain.get_logs_max_block_span=0 would scan no blocks; use at least 1 \
             (the provider's eth_getLogs range limit)"
                .to_owned()
        },
    );

    let redeem_threshold_micro_usdc = file
        .and_then(|b| b.redeem_threshold_micro_usdc)
        .unwrap_or(DEFAULT_REDEEM_THRESHOLD_MICRO_USDC);
    // A `0` threshold would withdraw on every accepted voucher — burning gas
    // per MB and reverting on-chain (`NothingToWithdraw`) for any zero-delta
    // re-hint. Reject it; operators wanting aggressive redemption set a small
    // positive value (base units, µUSDC).
    bag.check_with(
        redeem_threshold_micro_usdc > 0,
        "blockchain.redeem_threshold_micro_usdc",
        || {
            "blockchain.redeem_threshold_micro_usdc must be > 0 (a 0 threshold \
             withdraws on every voucher, burning gas and reverting on zero-delta)"
                .to_string()
        },
    );

    let redeem_interval_secs = file
        .and_then(|b| b.redeem_interval_secs)
        .unwrap_or(DEFAULT_REDEEM_INTERVAL_SECS);
    // A `0` interval would build a zero-period `tokio::time::interval`, which
    // panics. Reject it; operators wanting a tighter backstop set a small
    // positive value (the advisory hints already redeem between sweeps).
    bag.check_with(
        redeem_interval_secs > 0,
        "blockchain.redeem_interval_secs",
        || {
            "blockchain.redeem_interval_secs must be > 0 (a 0 interval is not a valid \
             sweep period)"
                .to_string()
        },
    );
    // The sweep is the only thing that redeems this node's vouchers before an
    // owner's grace-window close lets them `reclaim`. It must run several times
    // inside the 48h grace floor, so cap the interval at 6h — a laxer cadence
    // risks forfeiting real earnings on a pool that closes between sweeps.
    bag.check_with(
        redeem_interval_secs <= MAX_REDEEM_INTERVAL_SECS,
        "blockchain.redeem_interval_secs",
        || {
            format!(
                "blockchain.redeem_interval_secs must be <= {MAX_REDEEM_INTERVAL_SECS} (6h): the \
                 redeem sweep must run well inside the 48h grace window to secure vouchers \
                 before an owner can reclaim a closing pool"
            )
        },
    );

    let redeem_max_vouchers_per_tx = file
        .and_then(|b| b.redeem_max_vouchers_per_tx)
        .unwrap_or(DEFAULT_REDEEM_MAX_VOUCHERS_PER_TX);
    // A `0` chunk size could never carry a voucher, stranding every redemption.
    // Reject it; operators tuning gas set a small positive value.
    bag.check_with(
        redeem_max_vouchers_per_tx > 0,
        "blockchain.redeem_max_vouchers_per_tx",
        || {
            "blockchain.redeem_max_vouchers_per_tx must be > 0 (a 0 chunk size \
             carries no vouchers and strands every redemption)"
                .to_string()
        },
    );

    let buyer_working_deposit_micro_usdc = file
        .and_then(|b| b.buyer_working_deposit_micro_usdc)
        .unwrap_or(DEFAULT_BUYER_WORKING_DEPOSIT_MICRO_USDC);
    // The buyer opens the pool at this deposit, and `openPool` reverts
    // `ZeroAmount` on a zero deposit, so a configured 0 can never open a pool.
    // Reject it here too: the contract is the authority, but catching it at
    // load time beats surfacing it as a failed transaction on the first
    // cache-miss pull.
    bag.check_with(
        buyer_working_deposit_micro_usdc > 0,
        "blockchain.buyer_working_deposit_micro_usdc",
        || {
            "blockchain.buyer_working_deposit_micro_usdc must be > 0 (openPool reverts \
             ZeroAmount on a zero deposit)"
                .to_string()
        },
    );
    // Default-on: the one-time max approval is what lets the buyer path join
    // pools without a manual approve step (ADR 003 § Deposit Economics).
    let buyer_max_approve = file.and_then(|b| b.buyer_max_approve).unwrap_or(true);

    // The node's refundable floor `M` (ADR 003 § Sizing). No `> 0` check — `0`
    // is a legal (if aggressive) operator choice that keeps no reserve at all.
    let pool_min_remaining_deposit_micro_usdc = file
        .and_then(|b| b.pool_min_remaining_deposit_micro_usdc)
        .unwrap_or(DEFAULT_POOL_MIN_REMAINING_DEPOSIT_MICRO_USDC);

    // Per-signer LIVE concurrency cap `k`, in credit windows (ADR 003 § Pool
    // solvency, per-signer floor isolation). Any value is legal: the node
    // lower-clamps the cap to one window at use, so `0` behaves as "admit a lone
    // signer's first stream" rather than wedging it — no bound to enforce.
    let pool_floor_signer_live_windows = file
        .and_then(|b| b.pool_floor_signer_live_windows)
        .unwrap_or(DEFAULT_POOL_FLOOR_SIGNER_LIVE_WINDOWS);

    // CLI/env only — no TOML field. `expand_tilde` for parity with the
    // keystore path itself. Existence check is intentionally deferred to
    // the runtime loader: a stale path falls through there, so a headless run
    // fails with "no keystore password source available" naming the path,
    // which is clearer than a config-resolution-time stat() error. On a TTY it
    // falls through to the prompt instead, and the stale path goes unreported.
    let keystore_password_file = cli.keystore_password_file.clone().map(|p| expand_tilde(&p));

    ResolvedBlockchain {
        rpc_url,
        eth_keystore,
        keystore_password_file,
        payment_pool_address,
        capacity_bond_address,
        origin_assignment_address,
        origin_directory_positive_ttl_sec,
        origin_directory_negative_ttl_sec,
        origin_directory_cache_capacity,
        publisher_registry_address,
        slash_judge_address,
        content_blacklist_address,
        content_blacklist_poll_interval_sec,
        chain_staleness_grace_sec,
        chain_id,
        rpc_watchdog_interval_sec,
        event_poll_interval_ms,
        get_logs_max_block_span,
        fee_shares_poll_interval_sec,
        redeem_threshold_micro_usdc,
        redeem_max_vouchers_per_tx,
        redeem_interval_secs,
        buyer_working_deposit_micro_usdc,
        buyer_max_approve,
        pool_min_remaining_deposit_micro_usdc,
        pool_floor_signer_live_windows,
    }
}

/// Resolve cache fields.
///
/// Enforces `max_blob_size_mb <= cache_size_mb`: a blob larger than the whole
/// cache could never be admitted, so a value above the disk budget is a
/// permanent-reject misconfiguration. Equality is allowed and is the default —
/// the disk budget is the ceiling, and a node serves any blob it can hold.
#[cfg(test)]
fn resolve_cache(
    cli: &crate::cli::run::CacheArgs,
    file: Option<&types::CacheConfig>,
    data_dir: &std::path::Path,
) -> anyhow::Result<ResolvedCache> {
    one_section(|bag| resolve_cache_into(cli, file, data_dir, bag))
}

#[allow(clippy::too_many_lines)] // one flat field-by-field resolution; splitting obscures it.
fn resolve_cache_into(
    cli: &crate::cli::run::CacheArgs,
    file: Option<&types::CacheConfig>,
    data_dir: &std::path::Path,
    bag: &mut ConfigDiagnostics,
) -> ResolvedCache {
    let cache_dir = cli
        .cache_dir
        .clone()
        .map(|p| expand_tilde(&p))
        .or_else(|| {
            file.and_then(|c| c.cache_dir.clone())
                .map(|p| expand_tilde(&p))
        })
        .unwrap_or_else(|| data_dir.join("cache"));

    let cache_size_mb = cli
        .cache_size_mb
        .or_else(|| file.and_then(|c| c.cache_size_mb))
        .unwrap_or(DEFAULT_CACHE_SIZE_MB);

    // Free-disk headroom the eviction driver defends on the cache volume (#1930).
    // No range check: `0` is the valid opt-out (only `cache_size_mb` binds), and
    // a headroom larger than the volume simply keeps the effective ceiling pinned
    // to the footprint — degenerate but safe, and `decdn node doctor` flags a
    // headroom that leaves the cache no room to grow.
    let disk_headroom_mb = file
        .and_then(|c| c.disk_headroom_mb)
        .unwrap_or(DEFAULT_DISK_HEADROOM_MB);

    // Unset => `DEFAULT_MAX_BLOB_SIZE_MB` (50 GB), clamped to `cache_size_mb` so a
    // node with a smaller-than-default cache still resolves a valid ceiling rather
    // than tripping the `max_blob <= cache_size` invariant below. Operators who want
    // a different per-blob bound (for example to hold a larger monolithic blob, or to
    // cap the RAM the buffered miss tier spends on one pull) set it explicitly.
    let max_blob_size_mb = cli
        .max_blob_size_mb
        .or_else(|| file.and_then(|c| c.max_blob_size_mb))
        .unwrap_or_else(|| DEFAULT_MAX_BLOB_SIZE_MB.min(cache_size_mb));

    // Buyer-side absolute per-MB rate ceiling (#1375); `0` = unlimited (the
    // default). CLI/env override wins over the file, matching every other knob.
    let max_rate_per_mb = cli
        .max_rate_per_mb
        .or_else(|| file.and_then(|c| c.max_rate_per_mb))
        .unwrap_or(0);

    bag.check_with(
        max_blob_size_mb <= cache_size_mb,
        "cache.max_blob_size_mb",
        || {
            format!(
                "cache.max_blob_size_mb ({max_blob_size_mb}) must not exceed \
             cache.cache_size_mb ({cache_size_mb}); a blob larger than the whole \
             cache could never be admitted"
            )
        },
    );

    let origins = resolve_origins_into(file, bag);

    let pinned_hashes = bag
        .try_with(
            "cache.pinned_hashes",
            parse_pinned_hashes(file.and_then(|c| c.pinned_hashes.as_deref()))
                .context("invalid cache.pinned_hashes"),
        )
        .unwrap_or_else(decdn_config_types::PinnedHashes::empty);

    let origin_retry = bag
        .try_with(
            "cache.origin_retry",
            resolve_origin_retry(file.and_then(|c| c.origin_retry.as_ref()))
                .context("invalid cache.origin_retry"),
        )
        .unwrap_or_default();

    let circuit_breaker = bag
        .try_with(
            "cache.circuit_breaker",
            resolve_circuit_breaker(file.and_then(|c| c.circuit_breaker.as_ref()))
                .context("invalid cache.circuit_breaker"),
        )
        .unwrap_or_default();

    // `cache.user_agent` (#435): operator override of the default
    // `decdn-node/<version>` UA we send on every origin pull. We
    // validate the value at config load — both for a clear error
    // message and to slam the door on header-injection (CRLF, NUL,
    // other control bytes) before it reaches `reqwest::Client::builder`,
    // which would otherwise surface a generic "failed to build reqwest
    // client" at runtime. Whitespace inside the value is preserved
    // verbatim so an operator who genuinely wants `MyCdn / 1.0` gets
    // exactly that.
    let user_agent = match file.and_then(|c| c.user_agent.as_ref()) {
        Some(s)
            if bag
                .try_with("cache.user_agent", validate_user_agent(s))
                .is_some() =>
        {
            s.clone()
        }
        // Absent, or present-but-invalid (the problem is already in the
        // bag): fall back to the default UA so resolution can continue.
        _ => decdn_config_types::DEFAULT_USER_AGENT.to_string(),
    };

    let gc_interval_sec = file
        .and_then(|c| c.gc_interval_sec)
        .unwrap_or(DEFAULT_GC_INTERVAL_SEC);

    let fs_rescan_interval_sec = file
        .and_then(|c| c.fs_rescan_interval_sec)
        .unwrap_or(DEFAULT_FS_RESCAN_INTERVAL_SEC);

    // Live-origin probe memo knobs (#1130 pt3). No range checks: a `0` TTL simply
    // memoises nothing (every probe re-HEADs), a `0` timeout is clamped to a live
    // future by `tokio::time::timeout`, and a `0` capacity is floored to 1 by
    // `OriginProbeMemo::new` — all degenerate-but-safe, so there is no invalid
    // value to reject.
    let origin_probe_ttl_sec = file
        .and_then(|c| c.origin_probe_ttl_sec)
        .unwrap_or(DEFAULT_ORIGIN_PROBE_TTL_SEC);
    let origin_probe_negative_ttl_sec = file
        .and_then(|c| c.origin_probe_negative_ttl_sec)
        .unwrap_or(DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL_SEC);
    let origin_probe_fault_ttl_sec = file
        .and_then(|c| c.origin_probe_fault_ttl_sec)
        .unwrap_or(DEFAULT_ORIGIN_PROBE_FAULT_TTL_SEC);
    // The three probe TTLs are one graded policy, not three independent knobs.
    // A fault must be re-probed at least as eagerly as a positive answer, or a
    // recovered origin stays hidden behind a stale fault — and the origin-only
    // serve gate refuses paying clients for as long as it does. It must be at
    // least as patient as an absence, or memoising it buys nothing over the
    // negative TTL it would otherwise share. Enforced here so the ordering the
    // knob docs state is a property of every accepted config, not of the
    // defaults alone.
    bag.check_with(
        origin_probe_negative_ttl_sec <= origin_probe_fault_ttl_sec
            && origin_probe_fault_ttl_sec <= origin_probe_ttl_sec,
        "cache.origin_probe_fault_ttl_sec",
        || {
            format!(
                "cache.origin_probe_fault_ttl_sec ({origin_probe_fault_ttl_sec}) must sit \
                 between cache.origin_probe_negative_ttl_sec \
                 ({origin_probe_negative_ttl_sec}) and cache.origin_probe_ttl_sec \
                 ({origin_probe_ttl_sec}) inclusive: a fault is re-probed no less eagerly \
                 than a positive answer and no more eagerly than an absence"
            )
        },
    );
    let origin_probe_timeout_ms = file
        .and_then(|c| c.origin_probe_timeout_ms)
        .unwrap_or(DEFAULT_ORIGIN_PROBE_TIMEOUT_MS);
    let origin_probe_memo_capacity = file
        .and_then(|c| c.origin_probe_memo_capacity)
        .unwrap_or(DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY);

    // LRU eviction driver knobs (#1173, ADR 040). Each
    // is range-checked against its structural bounds; the target/high-water
    // hysteresis gap is a cross-field invariant enforced after both resolve.
    let eviction_high_water_pct = file
        .and_then(|c| c.eviction_high_water_pct)
        .unwrap_or(DEFAULT_EVICTION_HIGH_WATER_PCT);
    bag.check_with(
        (EVICTION_HIGH_WATER_PCT_BOUNDS.0..=EVICTION_HIGH_WATER_PCT_BOUNDS.1)
            .contains(&eviction_high_water_pct),
        "cache.eviction_high_water_pct",
        || {
            format!(
                "cache.eviction_high_water_pct ({eviction_high_water_pct}) must be within \
                 [{}, {}]",
                EVICTION_HIGH_WATER_PCT_BOUNDS.0, EVICTION_HIGH_WATER_PCT_BOUNDS.1
            )
        },
    );
    let eviction_target_pct = file
        .and_then(|c| c.eviction_target_pct)
        .unwrap_or(DEFAULT_EVICTION_TARGET_PCT);
    bag.check_with(
        (EVICTION_TARGET_PCT_BOUNDS.0..=EVICTION_TARGET_PCT_BOUNDS.1)
            .contains(&eviction_target_pct),
        "cache.eviction_target_pct",
        || {
            format!(
                "cache.eviction_target_pct ({eviction_target_pct}) must be within [{}, {}]",
                EVICTION_TARGET_PCT_BOUNDS.0, EVICTION_TARGET_PCT_BOUNDS.1
            )
        },
    );
    // Structural hysteresis gap: target must sit at least
    // EVICTION_HYSTERESIS_GAP_PCT points below high-water, else the driver
    // would thrash on writes hovering near the trigger. `saturating_sub` keeps
    // the comparison well-defined when high-water is below the gap.
    bag.check_with(
        eviction_target_pct <= eviction_high_water_pct.saturating_sub(EVICTION_HYSTERESIS_GAP_PCT),
        "cache.eviction_target_pct",
        || {
            format!(
                "cache.eviction_target_pct ({eviction_target_pct}) must be at least \
                 {EVICTION_HYSTERESIS_GAP_PCT} points below cache.eviction_high_water_pct \
                 ({eviction_high_water_pct}): the hysteresis gap is structural"
            )
        },
    );
    let eviction_per_sweep_budget = file
        .and_then(|c| c.eviction_per_sweep_budget)
        .unwrap_or(DEFAULT_EVICTION_PER_SWEEP_BUDGET);
    bag.check_with(
        (EVICTION_PER_SWEEP_BUDGET_BOUNDS.0..=EVICTION_PER_SWEEP_BUDGET_BOUNDS.1)
            .contains(&eviction_per_sweep_budget),
        "cache.eviction_per_sweep_budget",
        || {
            format!(
                "cache.eviction_per_sweep_budget ({eviction_per_sweep_budget}) must be within \
                 [{}, {}]",
                EVICTION_PER_SWEEP_BUDGET_BOUNDS.0, EVICTION_PER_SWEEP_BUDGET_BOUNDS.1
            )
        },
    );
    let eviction_tick_secs = file
        .and_then(|c| c.eviction_tick_secs)
        .unwrap_or(DEFAULT_EVICTION_TICK_SECS);
    bag.check_with(
        (EVICTION_TICK_SECS_BOUNDS.0..=EVICTION_TICK_SECS_BOUNDS.1).contains(&eviction_tick_secs),
        "cache.eviction_tick_secs",
        || {
            format!(
                "cache.eviction_tick_secs ({eviction_tick_secs}) must be within [{}, {}]",
                EVICTION_TICK_SECS_BOUNDS.0, EVICTION_TICK_SECS_BOUNDS.1
            )
        },
    );

    let max_probe_holds = cli
        .max_probe_holds
        .or_else(|| file.and_then(|c| c.max_probe_holds))
        .map_or(DEFAULT_MAX_PROBE_HOLDS, |v| {
            usize::try_from(v).unwrap_or(usize::MAX)
        });

    let stake_lane_reserved_holds = cli
        .stake_lane_reserved_holds
        .or_else(|| file.and_then(|c| c.stake_lane_reserved_holds))
        .map_or(DEFAULT_STAKE_LANE_RESERVED_HOLDS, |v| {
            usize::try_from(v).unwrap_or(usize::MAX)
        });

    let node_to_node_pull_through_enabled = file
        .and_then(|c| c.node_to_node_pull_through_enabled)
        .unwrap_or(false);
    let relay_foreign_namespaces = file
        .and_then(|c| c.relay_foreign_namespaces)
        // Role-derived default: an origin node (a backend is configured) serves only
        // its own namespace; a node with no origin is a pure relay edge and relays.
        // Explicit config overrides either way.
        .unwrap_or(origins.is_empty());
    let node_pull_probe_fanout = file
        .and_then(|c| c.node_pull_probe_fanout)
        .unwrap_or(DEFAULT_NODE_PULL_PROBE_FANOUT);
    let node_pull_timeout_sec = file
        .and_then(|c| c.node_pull_timeout_sec)
        .unwrap_or(DEFAULT_NODE_PULL_TIMEOUT_SEC);
    bag.check(
        node_pull_timeout_sec > 0,
        "cache.node_pull_timeout_sec",
        "cache.node_pull_timeout_sec must be > 0 (a 0 budget abandons every upstream \
         before its handshake can complete, so no pull can ever succeed)",
    );
    let node_pull_stall_window_sec = file
        .and_then(|c| c.node_pull_stall_window_sec)
        .unwrap_or(DEFAULT_NODE_PULL_STALL_WINDOW_SEC);
    // A 0 window makes the throughput floor demand progress over no time at all, so it
    // trips on the first poll of every streaming read and abandons every upstream before a
    // byte can arrive. The abort is non-attributable (#1797), so a fat-fingered value here
    // does not defame peers — but it still wedges this node's pull path, so it is rejected
    // at load.
    bag.check(
        node_pull_stall_window_sec > 0,
        "cache.node_pull_stall_window_sec",
        "cache.node_pull_stall_window_sec must be > 0 (a 0 window makes the throughput \
         floor unsatisfiable, abandoning every upstream on the first read)",
    );
    // The floor RATE may legitimately be 0 — that is idle-detection mode (one byte per
    // window) — so only the window duration is bounded below.
    let node_pull_min_throughput_bps = file
        .and_then(|c| c.node_pull_min_throughput_bps)
        .unwrap_or(DEFAULT_NODE_PULL_MIN_THROUGHPUT_BPS);
    // Cache admission/eviction policy selectors (ADR 040). Unknown names are
    // rejected here, at config load — never a silent fallback to the default
    // policy, which would mask an operator typo behind quietly-unchanged
    // behavior.
    let eviction_policy = file
        .and_then(|c| c.eviction_policy.clone())
        .unwrap_or_else(|| DEFAULT_EVICTION_POLICY.to_string());
    bag.check_with(
        matches!(eviction_policy.as_str(), "lru" | "tinylfu"),
        "cache.eviction_policy",
        || format!("cache.eviction_policy ({eviction_policy}) must be \"lru\" or \"tinylfu\""),
    );
    let admission_policy = file
        .and_then(|c| c.admission_policy.clone())
        .unwrap_or_else(|| DEFAULT_ADMISSION_POLICY.to_string());
    bag.check_with(
        matches!(admission_policy.as_str(), "always" | "tinylfu"),
        "cache.admission_policy",
        || format!("cache.admission_policy ({admission_policy}) must be \"always\" or \"tinylfu\""),
    );

    let tinylfu_file = file.and_then(|c| c.tinylfu.as_ref());
    let tinylfu_sketch_bytes = tinylfu_file
        .and_then(|t| t.sketch_bytes)
        .unwrap_or(DEFAULT_TINYLFU_SKETCH_BYTES);
    let tinylfu_promotion_threshold = tinylfu_file
        .and_then(|t| t.promotion_threshold)
        .unwrap_or(DEFAULT_TINYLFU_PROMOTION_THRESHOLD);
    // `sketch_bytes / 4` is the sketch's column count, and the over-report rate
    // `(1 - e^(-N / cols))^4` climbs steeply as that count shrinks. Reject an
    // undersized one at load, never clamp (ADR 040 §Configuration surface).
    // The check does not consult the policy selectors, and for `sketch_bytes`
    // that is not a precaution: the estimator is also built when
    // `serve_economics.policy` is `margin`, which is the default, so this knob
    // sizes a live sketch on a node whose selectors are `lru`/`always`. The two
    // checks below gate genuinely selector-only knobs, and are unconditional so
    // that a value which is wrong stays rejected when an operator switches.
    bag.check_with(
        tinylfu_sketch_bytes >= MIN_TINYLFU_SKETCH_BYTES,
        "cache.tinylfu.sketch_bytes",
        || {
            format!(
                "cache.tinylfu.sketch_bytes ({tinylfu_sketch_bytes}) must be >= \
                 {MIN_TINYLFU_SKETCH_BYTES}: a narrower sketch reports cold blobs \
                 as hot often enough to move admission, eviction and \
                 serve-economics decisions. It applies whether or not a policy \
                 selector names \"tinylfu\" — the default \
                 cache.serve_economics.policy = \"margin\" builds the same \
                 sketch."
            )
        },
    );
    // `promotion_threshold` counts prior sightings before a probation member
    // admits to `Main`; zero is nonsensical — it would make admission
    // always-`Main` and promote everything, defeating probationary admission.
    // Reject it at load, never clamp (ADR 040 §Probationary admission).
    bag.check_with(
        tinylfu_promotion_threshold >= 1,
        "cache.tinylfu.promotion_threshold",
        || {
            format!(
                "cache.tinylfu.promotion_threshold ({tinylfu_promotion_threshold}) must be >= 1"
            )
        },
    );
    let tinylfu_probation_target_pct = tinylfu_file
        .and_then(|t| t.probation_target_pct)
        .unwrap_or(DEFAULT_TINYLFU_PROBATION_TARGET_PCT);
    bag.check_with(
        (TINYLFU_PROBATION_TARGET_PCT_BOUNDS.0..=TINYLFU_PROBATION_TARGET_PCT_BOUNDS.1)
            .contains(&tinylfu_probation_target_pct),
        "cache.tinylfu.probation_target_pct",
        || {
            format!(
                "cache.tinylfu.probation_target_pct ({tinylfu_probation_target_pct}) must be \
                 within [{}, {}]",
                TINYLFU_PROBATION_TARGET_PCT_BOUNDS.0, TINYLFU_PROBATION_TARGET_PCT_BOUNDS.1
            )
        },
    );
    let tinylfu_aging_halflife_sec = tinylfu_file
        .and_then(|t| t.aging_halflife_sec)
        .unwrap_or(DEFAULT_TINYLFU_AGING_HALFLIFE_SEC);

    // Refuse-to-serve economics (ADR 041). Unknown policy names and
    // out-of-range knobs are rejected here, at config load — never a silent
    // fallback to the default.
    let se_file = file.and_then(|c| c.serve_economics.as_ref());
    let se_policy = se_file
        .and_then(|s| s.policy.clone())
        .unwrap_or_else(|| DEFAULT_SERVE_ECONOMICS_POLICY.to_string());
    bag.check_with(
        matches!(se_policy.as_str(), "off" | "margin"),
        "cache.serve_economics.policy",
        || format!("cache.serve_economics.policy ({se_policy}) must be \"off\" or \"margin\""),
    );
    let se_discount = se_file.and_then(|s| s.discount);
    bag.check_with(
        se_discount.is_none_or(|d| d > 0.0 && d <= 1.0),
        "cache.serve_economics.discount",
        || format!("cache.serve_economics.discount ({se_discount:?}) must be within (0.0, 1.0]"),
    );
    // Round to bps without float panics; the clamp (on the float, before the
    // cast) defends the conversion even if validation above is bypassed, so
    // the truncation/sign-loss casts below are always in `[1, 10_000]`.
    let se_discount_bps = se_discount.map_or(DEFAULT_SERVE_ECONOMICS_DISCOUNT_BPS, |d| {
        let bps = (d * 10_000.0).round().clamp(1.0, 10_000.0);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let bps = bps as u32;
        bps
    });
    let se_n_max = se_file
        .and_then(|s| s.n_max)
        .unwrap_or(DEFAULT_SERVE_ECONOMICS_N_MAX);
    bag.check_with(se_n_max >= 1, "cache.serve_economics.n_max", || {
        format!("cache.serve_economics.n_max ({se_n_max}) must be >= 1")
    });
    let se_warming_budget = se_file
        .and_then(|s| s.warming_budget)
        .unwrap_or(DEFAULT_SERVE_ECONOMICS_WARMING_BUDGET);
    bag.check_with(
        se_warming_budget > 0,
        "cache.serve_economics.warming_budget",
        || format!("cache.serve_economics.warming_budget ({se_warming_budget}) must be > 0"),
    );
    // `warming_refill` may be 0: that disables time-based refill (allowance
    // only resets on restart or via the serve-vindicated upgrade), which is
    // a valid operator choice, not an error.
    let se_warming_refill = se_file
        .and_then(|s| s.warming_refill)
        .unwrap_or(DEFAULT_SERVE_ECONOMICS_WARMING_REFILL);

    ResolvedCache {
        cache_dir,
        cache_size_mb,
        disk_headroom_mb,
        max_blob_size_mb,
        max_rate_per_mb,
        origins,
        pinned_hashes,
        origin_retry,
        circuit_breaker,
        user_agent,
        gc_interval_sec,
        fs_rescan_interval_sec,
        origin_probe_ttl_sec,
        origin_probe_negative_ttl_sec,
        origin_probe_fault_ttl_sec,
        origin_probe_timeout_ms,
        origin_probe_memo_capacity,
        eviction_high_water_pct,
        eviction_target_pct,
        eviction_per_sweep_budget,
        eviction_tick_secs,
        max_probe_holds,
        stake_lane_reserved_holds,
        node_to_node_pull_through_enabled,
        relay_foreign_namespaces,
        node_pull_probe_fanout,
        node_pull_timeout_sec,
        node_pull_stall_window_sec,
        node_pull_min_throughput_bps,
        eviction_policy,
        admission_policy,
        tinylfu: ResolvedTinyLfu {
            sketch_bytes: tinylfu_sketch_bytes,
            promotion_threshold: tinylfu_promotion_threshold,
            probation_target_pct: tinylfu_probation_target_pct,
            aging_halflife_sec: tinylfu_aging_halflife_sec,
        },
        serve_economics: ResolvedServeEconomics {
            policy: se_policy,
            discount_bps: se_discount_bps,
            n_max: se_n_max,
            warming_budget: se_warming_budget,
            warming_refill: se_warming_refill,
        },
    }
}

/// Resolve and validate the cache origin section (#437, #284). Collapses
/// the singular `[cache.origin]` table and the plural `[[cache.origins]]`
/// array-of-tables into a single canonical [`Vec<ResolvedOrigin>`]:
/// absent => `vec![]`, singular => one-element vec, plural => the
/// resolved vec in operator-supplied order. The two wire forms are
/// mutually exclusive — setting both at once is an operator mistake
/// (each form names a distinct fallback policy) and the error message
/// names both keys so the fix is unambiguous.
///
/// Empty `origins = []` is rejected rather than treated as "no
/// pull-through" — an operator who wrote `origins = []` almost
/// certainly meant to populate it later and forgot. Failing at config
/// load surfaces the mistake before the first cache miss instead of
/// silently degrading to `NoOrigin`.
///
/// Duplicate entries (same kind + identity key) are permitted, with a
/// [`ConfigNotice`] recorded per duplicate. Two HTTP origins pointing at the
/// same URL is legitimate for connection-pool sharding, but is more often a
/// copy-paste mistake worth putting in front of the operator.
fn resolve_origins_into(
    file: Option<&types::CacheConfig>,
    bag: &mut ConfigDiagnostics,
) -> Vec<crate::config::ResolvedOrigin> {
    let Some(cache) = file else {
        return Vec::new();
    };

    match (&cache.origin, &cache.origins) {
        (Some(_), Some(_)) => {
            // One unambiguous problem: the operator picked two mutually
            // exclusive fallback policies. Don't also emit "must contain
            // at least one entry" — resolve to no pull-through.
            bag.push(
                "cache.origins",
                "cache.origin and cache.origins are mutually exclusive — \
                 use [cache.origin] for a single backend or [[cache.origins]] \
                 for an ordered fallback list, not both",
            );
            Vec::new()
        }
        (Some(single), None) => bag
            .try_with(
                "cache.origin",
                resolve_origin(single).context("invalid cache.origin"),
            )
            .map(|o| vec![o])
            .unwrap_or_default(),
        (None, Some(list)) => {
            if !bag.check(
                !list.is_empty(),
                "cache.origins",
                "cache.origins must contain at least one entry; \
                 omit the key entirely for no pull-through",
            ) {
                return Vec::new();
            }
            // Independent entries: validate every one so an operator with
            // several bad backends sees all of them, not just the first.
            let mut resolved = Vec::with_capacity(list.len());
            for (idx, entry) in list.iter().enumerate() {
                if let Some(o) = bag.try_with(
                    format!("cache.origins[{idx}]"),
                    resolve_origin(entry).with_context(|| format!("invalid cache.origins[{idx}]")),
                ) {
                    resolved.push(o);
                }
            }
            warn_on_duplicate_origins(&resolved, bag);
            resolved
        }
        (None, None) => Vec::new(),
    }
}

/// Operator-visible identity key for a resolved origin — used solely to
/// detect duplicates within a `[[cache.origins]]` array. Carries the
/// fields that distinguish two backends at the operator level (URL for
/// HTTP, filesystem path for fs, bucket+region+endpoint+prefix for S3).
/// Credentials are deliberately excluded: rotating an access key on an
/// otherwise-identical S3 backend should still count as a duplicate.
fn origin_identity_key(origin: &crate::config::ResolvedOrigin) -> String {
    use crate::config::ResolvedOrigin;
    match origin {
        ResolvedOrigin::Http { url, .. } => format!("http|{url}"),
        ResolvedOrigin::Fs { path } => format!("fs|{}", path.display()),
        ResolvedOrigin::S3(s3) => format!(
            "s3|{}|{}|{}|{}",
            s3.bucket,
            s3.region,
            s3.endpoint_url
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            s3.prefix,
        ),
    }
}

/// Emit a `tracing::warn!` for any `[[cache.origins]]` entry whose
/// identity key matches an earlier entry. Two origins pointing at the
/// same backend can be intentional (connection-pool sharding) but is
/// usually a copy-paste mistake; warning at startup gives the operator
/// a chance to notice before debugging a "why is one origin being hit
/// twice as often" puzzle. Not an error: ordering still determines
/// fallback behaviour, and the engine handles duplicate backends
/// without misbehaviour.
fn warn_on_duplicate_origins(
    origins: &[crate::config::ResolvedOrigin],
    bag: &mut ConfigDiagnostics,
) {
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(origins.len());
    for (idx, origin) in origins.iter().enumerate() {
        let key = origin_identity_key(origin);
        if !seen.insert(key.clone()) {
            bag.warn(
                format!("cache.origins[{idx}]"),
                format!(
                    "{key} duplicates an earlier entry — the cache engine will dispatch the \
                     same backend twice in the fallback chain (intentional for connection-pool \
                     sharding, otherwise a likely copy-paste mistake)"
                ),
            );
        }
    }
}

/// Resolve and validate a `[cache.origin]` table into the typed
/// runtime form. The match arms run per-variant validation (URL parse
/// for HTTP, S3 bucket/region/prefix shape) and reject empty paths or
/// URLs early so a misconfigured backend surfaces at startup rather
/// than on the first cache miss.
fn resolve_origin(cfg: &types::OriginConfig) -> anyhow::Result<crate::config::ResolvedOrigin> {
    use crate::config::ResolvedOrigin;
    match cfg {
        types::OriginConfig::Http { url, decompress } => {
            anyhow::ensure!(!url.is_empty(), "cache.origin.url must not be empty");
            let parsed =
                decdn_config_types::parse_origin_url(url).context("invalid cache.origin.url")?;
            Ok(ResolvedOrigin::Http {
                url: parsed,
                decompress: decompress.unwrap_or_default(),
            })
        }
        types::OriginConfig::Fs { path } => {
            let expanded = expand_tilde(path);
            anyhow::ensure!(
                !expanded.as_os_str().is_empty(),
                "cache.origin.path must not be empty"
            );
            Ok(ResolvedOrigin::Fs { path: expanded })
        }
        types::OriginConfig::S3(s3) => {
            let resolved = resolve_s3_origin(s3.clone())?;
            Ok(ResolvedOrigin::S3(resolved))
        }
    }
}

/// Validate, normalize, and lift an `S3OriginConfig` into the runtime
/// form `ResolvedS3Config` (#437). This is the **intended** path
/// from the TOML wire form to a resolved-and-validated runtime form,
/// and the only producer the rest of the resolution layer
/// (`resolve_origin`, `resolve_cache`, `resolve_config`) feeds into
/// the runtime. `ResolvedS3Config` itself has `pub` fields (matching
/// the `Resolved*` shape used throughout this crate) — Rust
/// visibility doesn't *enforce* the validator-only contract, but the
/// runtime never bypasses it. See the type doc on
/// [`ResolvedS3Config`].
///
/// Normalization performed here:
/// - `prefix` gets a trailing `/` auto-appended if non-empty and
///   missing one (mirrors `parse_origin_url`'s normalization).
///   Absent prefix becomes `String::new()` on the resolved side.
/// - `path_style: Option<bool>` is collapsed to `bool` with `None`
///   mapped to `false` (the SDK default = virtual-hosted-style).
/// - `endpoint_url` is parsed into `OriginUrl` so the S3 backend
///   can hand it straight to the SDK without re-parsing.
///
/// Validation covers the cases the type can't express:
/// - DNS-safe bucket name (see [`validate_s3_bucket_name`]);
/// - non-empty region (the AWS SDK uses it for `SigV4` signing even
///   when a custom `endpoint_url` is set);
/// - `endpoint_url`, when present, parses as `http`/`https`;
/// - `prefix` is a key prefix, not a path: must not start with `/`,
///   must not contain `..`, must not contain backslashes, and must
///   not contain ASCII control or whitespace characters.
fn resolve_s3_origin(
    cfg: types::S3OriginConfig,
) -> anyhow::Result<crate::config::ResolvedS3Config> {
    use crate::config::resolved::{ResolvedS3Config, ResolvedS3Credentials};

    let types::S3OriginConfig {
        bucket,
        region,
        endpoint_url,
        path_style,
        prefix,
        credentials,
        decompress,
    } = cfg;

    validate_s3_bucket_name(&bucket).context("invalid cache.origin.bucket")?;
    anyhow::ensure!(
        !region.trim().is_empty(),
        "cache.origin.region must not be empty (the AWS SDK uses it for SigV4 signing \
         even when a custom endpoint_url is set)"
    );

    let endpoint_url = if let Some(endpoint) = endpoint_url {
        anyhow::ensure!(
            !endpoint.is_empty(),
            "cache.origin.endpoint_url must not be empty when set; omit the key instead"
        );
        // `parse_origin_url` rejects query/fragment and forces
        // trailing-slash normalization. That's fine for an S3
        // endpoint base — the SDK appends `/{bucket}/{key}` itself,
        // and storing the parsed/normalized form here means the
        // runtime hands a single canonical string to the SDK
        // regardless of whether the operator wrote
        // `http://minio:9000` or `http://minio:9000/`.
        Some(
            decdn_config_types::parse_origin_url(&endpoint)
                .context("invalid cache.origin.endpoint_url")?,
        )
    } else {
        None
    };

    let prefix = match prefix {
        None => String::new(),
        Some(mut prefix) => {
            anyhow::ensure!(
                !prefix.starts_with('/'),
                "cache.origin.prefix is a key prefix, not a filesystem path: \
                 it must not start with `/` (use `prefix = \"\"` or omit the key for no prefix)"
            );
            anyhow::ensure!(
                !prefix.contains(".."),
                "cache.origin.prefix must not contain `..` (key-prefix shape)"
            );
            anyhow::ensure!(
                !prefix.contains('\\'),
                "cache.origin.prefix must not contain `\\` (key-prefix shape)"
            );
            anyhow::ensure!(
                !prefix.bytes().any(|b| b.is_ascii_control()),
                "cache.origin.prefix must not contain ASCII control characters"
            );
            anyhow::ensure!(
                !prefix.bytes().any(|b| b == b' ' || b == b'\t'),
                "cache.origin.prefix must not contain whitespace"
            );
            if !prefix.is_empty() && !prefix.ends_with('/') {
                prefix.push('/');
            }
            prefix
        }
    };

    let credentials = credentials.map(|c| match c {
        types::S3Credentials::Static {
            access_key_id,
            secret_access_key,
            session_token,
        } => ResolvedS3Credentials::Static {
            access_key_id,
            secret_access_key,
            session_token,
        },
        types::S3Credentials::DefaultChain { profile } => {
            // Collapse `profile = ""` (post env-var expansion) to None
            // so the runtime's `loader.profile_name(...)` arm only fires
            // for a real name. The SDK treats `profile_name("")` as
            // distinct from "no override" and would surface a confusing
            // "profile '' not found" error at first credential need.
            let profile = profile.filter(|p| !p.is_empty());
            ResolvedS3Credentials::DefaultChain { profile }
        }
    });

    Ok(ResolvedS3Config {
        bucket,
        region,
        endpoint_url,
        path_style: path_style.unwrap_or(false),
        prefix,
        credentials,
        decompress: decompress.unwrap_or_default(),
    })
}

/// AWS S3 bucket-name validation (#437). Mirrors the documented
/// constraints: 3–63 chars; lowercase `[a-z0-9.-]`; no leading or
/// trailing `.` or `-`; no consecutive dots; must not be formatted
/// as an IPv4 address. Catches the common operator typo at config
/// load instead of on the first request.
///
/// Public because `decdn config init --origin s3://<bucket>` (the CLI)
/// validates the bucket through this same guard before writing it, so a
/// bucket `config init` accepts cannot fail config resolution.
///
/// AWS-permissive choices we deliberately accept (but stricter
/// frontends like virtual-hosted-style URLs may reject): names
/// shorter than 3 chars are rejected (per AWS rule), but `xn--`
/// prefix and `--ol-s3` suffix are not rejected here — they're
/// reserved by AWS to never be assigned and the operator-typo case
/// is rare enough not to warrant the extra code.
pub fn validate_s3_bucket_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!name.is_empty(), "bucket name must not be empty");
    let len = name.len();
    anyhow::ensure!(
        (3..=63).contains(&len),
        "bucket name must be 3..=63 chars (got {len})"
    );
    // AWS: "Bucket names must begin and end with a letter or number"
    // — i.e. no leading/trailing `.` or `-`. Both the leading/trailing
    // dot and hyphen are rejected here. Names like `-foo` or `foo-`
    // would parse but fail virtual-hosted-style URLs at request time.
    let first = name.bytes().next().unwrap_or(0);
    let last = name.bytes().next_back().unwrap_or(0);
    anyhow::ensure!(
        first.is_ascii_lowercase() || first.is_ascii_digit(),
        "bucket name must begin with a lowercase letter or digit"
    );
    anyhow::ensure!(
        last.is_ascii_lowercase() || last.is_ascii_digit(),
        "bucket name must end with a lowercase letter or digit"
    );
    anyhow::ensure!(
        !name.contains(".."),
        "bucket name must not contain consecutive dots"
    );
    anyhow::ensure!(
        name.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.'),
        "bucket name must contain only lowercase letters, digits, `-`, or `.`"
    );
    // Reject IPv4-literal-shaped names (e.g. `192.168.1.1`). AWS
    // rejects these on the wire; pre-flighting here gives a
    // friendlier error. The check uses `Ipv4Addr::from_str` which
    // only accepts the canonical 4-octet decimal form — a
    // 5-segment all-numeric name like `1.2.3.4.5` is technically
    // accepted by this validator and rejected by AWS at request
    // time. That gap is acknowledged; the typo it would catch is
    // exotic enough that the extra parsing complexity isn't earned
    // here.
    if name.parse::<std::net::Ipv4Addr>().is_ok() {
        anyhow::bail!("bucket name must not be formatted as an IPv4 address");
    }
    Ok(())
}

/// Validate `cache.user_agent` as an HTTP header value at config load
/// time. Rejects empty strings and any byte that would break header
/// framing (CR, LF, NUL, other C0 controls, DEL). Tab is permitted —
/// HTTP allows it in field values and operators occasionally use it as
/// a separator inside the UA. Non-ASCII bytes (≥ 0x80) are rejected
/// because real-world `User-Agent` strings are pure visible ASCII and
/// silently passing through obs-text would hide a typo or a botched
/// env-var expansion. Catching this here, rather than letting reqwest
/// surface a generic `"failed to build reqwest client"` at startup,
/// gives the operator a message that actually names the offending
/// field and position.
fn validate_user_agent(s: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !s.is_empty(),
        "cache.user_agent must be a non-empty string when set"
    );
    if let Some((idx, byte)) = s
        .bytes()
        .enumerate()
        .find(|(_, b)| !(*b == b'\t' || (0x20..=0x7e).contains(b)))
    {
        anyhow::bail!(
            "cache.user_agent contains an invalid byte 0x{byte:02x} at position {idx}; \
             only visible ASCII (0x20..=0x7e) and tab (0x09) are allowed in HTTP header values"
        );
    }
    Ok(())
}

/// Hard ceiling on `cache.origin_retry.buffered_max_bytes` (#519).
/// 64 MiB is well above the 4 MiB default but small enough that an
/// accidental "set to 4 GiB" gets caught at config-load time. The
/// streaming abort+restart path covers blobs of any size without
/// raising this knob; lifting the ceiling would let an operator
/// silently amplify per-fetch RSS into territory that doesn't
/// actually buy them anything.
pub const MAX_BUFFERED_MAX_BYTES: u64 = 64 << 20;

/// Resolve the origin retry policy (#285, extended in #519). Absent =>
/// defaults via `RetryPolicy::default()`. Present partial sections
/// fill missing fields from the same defaults (handled by
/// `#[serde(default)]` on `RetryPolicy` itself). This function
/// enforces the cross-field invariants the type can't express:
/// monotone schedule, finite jitter in `[0, 1]`, and a sane ceiling
/// on the body-phase buffer budget.
pub fn resolve_origin_retry(
    file: Option<&decdn_config_types::RetryPolicy>,
) -> anyhow::Result<decdn_config_types::RetryPolicy> {
    let p = file.copied().unwrap_or_default();
    anyhow::ensure!(
        p.initial_backoff_ms <= p.max_backoff_ms,
        "cache.origin_retry: initial_backoff_ms ({}) must be <= max_backoff_ms ({}); \
         otherwise the schedule never grows",
        p.initial_backoff_ms,
        p.max_backoff_ms,
    );
    anyhow::ensure!(
        p.jitter_ratio.is_finite() && (0.0..=1.0).contains(&p.jitter_ratio),
        "cache.origin_retry: jitter_ratio={} must be a finite number in 0.0..=1.0",
        p.jitter_ratio,
    );
    anyhow::ensure!(
        p.buffered_max_bytes <= MAX_BUFFERED_MAX_BYTES,
        "cache.origin_retry: buffered_max_bytes ({}) exceeds the hard ceiling of {} bytes \
         (#519). The streaming abort+restart path covers blobs of any size without raising \
         this knob; if mid-stream retries on large blobs is the goal, leave \
         buffered_max_bytes at the default and rely on the streaming path",
        p.buffered_max_bytes,
        MAX_BUFFERED_MAX_BYTES,
    );
    Ok(p)
}

/// Resolve the per-origin circuit-breaker policy (#963). Absent =>
/// defaults via `CircuitBreakerPolicy::default()`. Present partial
/// sections fill missing fields from the same defaults (handled by
/// `#[serde(default)]` on `CircuitBreakerPolicy` itself). This function
/// enforces the one cross-field invariant the type can't express: an
/// *active* breaker must admit at least one half-open trial, otherwise
/// it could never probe for recovery and would stay OPEN forever after
/// the first trip.
pub fn resolve_circuit_breaker(
    file: Option<&decdn_config_types::CircuitBreakerPolicy>,
) -> anyhow::Result<decdn_config_types::CircuitBreakerPolicy> {
    let p = file.copied().unwrap_or_default();
    // Only bind the invariant when the breaker is actually active —
    // a disabled breaker (`enabled = false` or `failure_threshold = 0`)
    // never reaches HALF-OPEN, so `half_open_max_calls = 0` is harmless
    // there and an operator opting out shouldn't have to also set a
    // half-open value.
    if p.is_active() {
        anyhow::ensure!(
            p.half_open_max_calls >= 1,
            "cache.circuit_breaker: half_open_max_calls ({}) must be >= 1 when the breaker is \
             active; otherwise it could never admit a trial pull to probe recovery and would \
             stay open forever after the first trip",
            p.half_open_max_calls,
        );
    }
    Ok(p)
}

/// Parse the operator-supplied `cache.pinned_hashes` list (#276) into a
/// [`decdn_config_types::PinnedHashes`]. Each entry must be 64 lowercase hex
/// chars (BLAKE3 digest size); anything else fails resolution. Duplicates
/// are silently de-duplicated — they're harmless.
///
/// `None` and the empty list both resolve to the empty set, so an absent
/// or empty `pinned_hashes` key just means "no pinning".
pub fn parse_pinned_hashes(
    raw: Option<&[String]>,
) -> anyhow::Result<decdn_config_types::PinnedHashes> {
    Ok(decdn_config_types::PinnedHashes::new(parse_hash_list(
        raw,
        "cache.pinned_hashes",
    )?))
}

/// Parse the operator-supplied `content.denied_hashes` list (ADR 011 §Local
/// Denylist) into a [`decdn_config_types::DeniedHashes`].
///
/// Same spelling as `cache.pinned_hashes` — bare 64-char lowercase hex, no
/// `blake3:` prefix — so an operator has one hash format across the whole
/// config file. The distinct return type is what keeps a denylist from ever
/// being handed to the pinning slot.
pub fn parse_denied_hashes(
    raw: Option<&[String]>,
) -> anyhow::Result<decdn_config_types::DeniedHashes> {
    Ok(decdn_config_types::DeniedHashes::new(parse_hash_list(
        raw,
        "content.denied_hashes",
    )?))
}

/// Shared validation behind `cache.pinned_hashes` and `content.denied_hashes`.
/// `label` is the config key, so each list's errors name their own field.
fn parse_hash_list(
    raw: Option<&[String]>,
    label: &str,
) -> anyhow::Result<std::collections::HashSet<decdn_config_types::Hash>> {
    use std::str::FromStr;

    let mut out = std::collections::HashSet::new();
    let Some(entries) = raw else {
        return Ok(out);
    };
    for (idx, entry) in entries.iter().enumerate() {
        let trimmed = entry.trim();
        // BLAKE3 lowercase hex is 64 chars. We require lowercase rather
        // than letting `Hash::from_str` accept either case because
        // mixed-case entries are almost always a copy-paste mistake from
        // somewhere they got upper-cased; surfacing it as a config error
        // now beats a silent "did the operator pin this or not?" later.
        anyhow::ensure!(
            trimmed.len() == 64,
            "{label}[{idx}] must be 64 hex chars (BLAKE3); got {} chars",
            trimmed.len()
        );
        anyhow::ensure!(
            trimmed.chars().all(|c| c.is_ascii_hexdigit())
                && !trimmed.chars().any(|c| c.is_ascii_uppercase()),
            "{label}[{idx}] must be lowercase hex (0-9, a-f)"
        );
        let parsed = decdn_config_types::Hash::from_str(trimmed)
            .with_context(|| format!("{label}[{idx}] failed to parse as a BLAKE3 hash"))?;
        out.insert(parsed);
    }
    Ok(out)
}

/// Parse the operator-supplied `content.denied_origins` list (ADR 011 §Local
/// Denylist) into a set of operator addresses.
///
/// These are operator **EOAs**, not contract addresses, so this does NOT use
/// [`crate::address::parse_nonzero_address`] — that helper's zero-address hint
/// ("set it to the deployed contract address") is wrong advice for a denylist of
/// operator accounts (see the note on that fn). The zero address is still
/// rejected — it can never own a payment pool, and accepting it would let a
/// stray empty string sit in the denylist reading as a real entry — but with an
/// operator-appropriate message.
pub fn parse_denied_origins(
    raw: Option<&[String]>,
) -> anyhow::Result<std::collections::HashSet<alloy::primitives::Address>> {
    let mut out = std::collections::HashSet::new();
    let Some(entries) = raw else {
        return Ok(out);
    };
    for (idx, entry) in entries.iter().enumerate() {
        let label = format!("content.denied_origins[{idx}]");
        let addr = crate::address::parse_address(entry.trim(), &label)?;
        anyhow::ensure!(
            addr != alloy::primitives::Address::ZERO,
            "{label} must not be the zero address — list a real operator address, \
             or remove the entry"
        );
        out.insert(addr);
    }
    Ok(out)
}

/// Resolve the local content denylist (ADR 011 §Local Denylist).
///
/// Both lists fail resolution on a malformed entry rather than skipping it. A
/// denylist is discharging a legal order; "one line was ignored" is the one
/// outcome an operator must never get silently.
fn resolve_content_into(
    file: Option<&types::ContentConfig>,
    bag: &mut ConfigDiagnostics,
) -> ResolvedContent {
    let denied_hashes = bag
        .try_with(
            "content.denied_hashes",
            parse_denied_hashes(file.and_then(|c| c.denied_hashes.as_deref()))
                .context("invalid content.denied_hashes"),
        )
        .unwrap_or_else(decdn_config_types::DeniedHashes::empty);
    let denied_origins = bag
        .try_with(
            "content.denied_origins",
            parse_denied_origins(file.and_then(|c| c.denied_origins.as_deref()))
                .context("invalid content.denied_origins"),
        )
        .unwrap_or_default();
    ResolvedContent {
        denied_hashes,
        denied_origins,
    }
}

/// Resolve payment fields.
///
/// Rejects `rate_per_mb == 0`: the value participates in the node selection
/// score (`rate_per_mb × rtt_ms × …`, ADR 001 § Node Selection Algorithm) and
/// feeds the on-chain rate-mismatch evidence path (ADR 014). A zero rate would
/// make this node trivially win every client selection while earning no
/// payable revenue — an obvious misconfiguration that should fail startup, not
/// silently degrade the network.
///
/// Rejects `rate_per_mb > MAX_RATE_PER_MB` (issue #378): the wire boundary
/// enforces the same ceiling on inbound `ProbeResponse`s, so an operator
/// configuring a rate above this would publish probe responses that every
/// honest client decoder rejects — fail at startup rather than silently
/// emit unparseable wire traffic. The bound is also a defense-in-depth
/// against the selection-score overflow path (issue #322).
#[cfg(test)]
pub fn resolve_payment(
    cli: &crate::cli::run::PaymentArgs,
    file: Option<&types::PaymentConfig>,
) -> anyhow::Result<ResolvedPayment> {
    one_section(|bag| resolve_payment_into(cli, file, bag))
}

/// Bag-threading variant of the test-only `resolve_payment` shim. Used by both
/// [`resolve_config`] (single bag across every section at startup) and the
/// SIGHUP hot-reload path in `runtime::reload` (single bag across every
/// reloadable section), so an operator sees every problem in one error
/// instead of fixing them one SIGHUP at a time. Always returns a
/// [`ResolvedPayment`] (with placeholder values for fields that failed
/// validation) so later checks can still run against it.
pub fn resolve_payment_into(
    cli: &crate::cli::run::PaymentArgs,
    file: Option<&types::PaymentConfig>,
    bag: &mut ConfigDiagnostics,
) -> ResolvedPayment {
    let rate_per_mb = cli
        .rate_per_mb
        .or_else(|| file.and_then(|p| p.rate_per_mb))
        .unwrap_or(DEFAULT_RATE_PER_MB);
    bag.check(
        rate_per_mb > 0,
        "payment.rate_per_mb",
        "payment.rate_per_mb must be > 0 (used in the node selection score, \
         ADR 001); got 0",
    );
    bag.check_with(
        rate_per_mb <= decdn_protocol::MAX_RATE_PER_MB,
        "payment.rate_per_mb",
        || {
            format!(
                "payment.rate_per_mb {rate_per_mb} exceeds protocol MAX_RATE_PER_MB ({}); \
             honest clients reject `ProbeResponse`s above this ceiling (issue #378)",
                decdn_protocol::MAX_RATE_PER_MB,
            )
        },
    );
    // Downstream credit-window ceiling (ADR 003 §Credit window). Default 64 MiB;
    // no upper bound beyond the runtime deposit guard — a larger ceiling is more
    // unbilled egress the node fronts once a stream has ramped up, which the
    // operator owns. Floored to one chunk by the serve loop, so no lower
    // bound is enforced here.
    let credit_max = file
        .and_then(|p| p.credit_max.as_ref())
        .map_or(DEFAULT_CREDIT_MAX, |b| b.get());
    // Ramp divisor (ADR 003 §Credit window). Default 2; `0` (open the full
    // ceiling immediately) is a valid setting, so it merges as a first-class
    // value rather than falling back to the default.
    let credit_ramp_divisor = file
        .and_then(|p| p.credit_ramp_divisor)
        .unwrap_or(DEFAULT_CREDIT_RAMP_DIVISOR);
    // Serve-path wire-frame target (ADR 005 §`cdn/client/v1`). Default and ceiling
    // are both one `CHUNK_BYTES` payment interval: a frame never crosses a payment
    // boundary, so the serve loop clamps every request to the interval remainder and
    // a larger value could not reach the wire. Rejected rather than silently clamped
    // — an operator who sets 8 MiB expecting bigger frames deserves to be told the
    // value is unreachable, not to watch for a change that never comes. `0` is
    // rejected for the opposite reason: it would ask the framers for zero-length
    // frames, and an empty `ChunkData` is a protocol error. The framers refuse a
    // zero target themselves, so this is the first of two doors, not the only one.
    let frame_target_bytes = file
        .and_then(|p| p.frame_target_bytes.as_ref())
        .map_or(DEFAULT_FRAME_TARGET_BYTES, |b| b.get());
    bag.check_with(
        frame_target_bytes > 0 && frame_target_bytes <= decdn_protocol::CHUNK_BYTES,
        "payment.frame_target_bytes",
        || {
            format!(
                "payment.frame_target_bytes must be in 1..={} (one payment chunk); a \
                 frame never crosses a payment-chunk boundary, so a larger value \
                 could never reach the wire",
                decdn_protocol::CHUNK_BYTES
            )
        },
    );
    // Background flush period (ADR 003 §Off-chain voucher state persistence).
    // `0` would build a zero-period `tokio::time::interval`, which panics; reject
    // it so an operator wanting tight durability sets a small positive value.
    let voucher_commit_interval_ms = file
        .and_then(|p| p.voucher_commit_interval_ms)
        .unwrap_or(DEFAULT_VOUCHER_COMMIT_INTERVAL_MS);
    bag.check_with(
        voucher_commit_interval_ms > 0,
        "payment.voucher_commit_interval_ms",
        || {
            "payment.voucher_commit_interval_ms must be > 0 (a 0 interval is not a \
             valid flush period)"
                .to_string()
        },
    );
    ResolvedPayment {
        rate_per_mb,
        credit_max,
        credit_ramp_divisor,
        frame_target_bytes,
        voucher_commit_interval_ms,
    }
}

/// Resolve observability fields.
///
/// The admin port is merged with `0` as a first-class "disable" value so
/// operators can turn the surface off without removing the line from their
/// config. Cross-port collision checks (bind/metrics/admin) live in
/// `validate_port_layout`, which sees all three sections at once — see
/// there for the full ruleset.
#[cfg(test)]
pub fn resolve_observability(
    cli: &crate::cli::run::ObservabilityArgs,
    file: Option<&types::ObservabilityConfig>,
) -> anyhow::Result<ResolvedObservability> {
    one_section(|bag| resolve_observability_into(cli, file, bag))
}

/// Bag-threading variant of the test-only `resolve_observability` shim. Shares a bag with
/// other sections during startup ([`resolve_config`]) and SIGHUP reload
/// (`runtime::reload`); see [`resolve_payment_into`] for the rationale.
pub fn resolve_observability_into(
    cli: &crate::cli::run::ObservabilityArgs,
    file: Option<&types::ObservabilityConfig>,
    bag: &mut ConfigDiagnostics,
) -> ResolvedObservability {
    let log_level = cli
        .log_level
        .or_else(|| file.and_then(|o| o.log_level))
        .unwrap_or_default();

    let log_format = cli
        .log_format
        .or_else(|| file.and_then(|o| o.log_format))
        .unwrap_or_default();

    let metrics_port = cli
        .metrics_port
        .or_else(|| file.and_then(|o| o.metrics_port))
        .unwrap_or(DEFAULT_METRICS_PORT);

    let metrics_bind = cli
        .metrics_bind
        .or_else(|| file.and_then(|o| o.metrics_bind))
        .unwrap_or(DEFAULT_METRICS_BIND);

    let admin_port_raw = cli
        .admin_port
        .or_else(|| file.and_then(|o| o.admin_port))
        .unwrap_or(DEFAULT_ADMIN_PORT);
    let admin_port = if admin_port_raw == 0 {
        None
    } else {
        Some(admin_port_raw)
    };

    let otlp_endpoint = cli
        .otlp_endpoint
        .clone()
        .or_else(|| file.and_then(|o| o.otlp_endpoint.clone()))
        .filter(|s| !s.is_empty());

    if let Some(ref ep) = otlp_endpoint {
        bag.try_with("observability.otlp_endpoint", validate_otlp_endpoint(ep));
    }

    ResolvedObservability {
        log_level,
        log_format,
        metrics_port,
        metrics_bind,
        admin_port,
        otlp_endpoint,
    }
}

/// Check that `endpoint` is a URL the node's OTLP gRPC exporter can use:
/// `http://host:port` with nothing after the authority.
///
/// - `http` only: the exporter is built without TLS, so an `https://`
///   endpoint would pass resolution and then abort daemon start-up.
/// - An explicit port: without one the exporter dials port 80, which is
///   almost never an OTLP/gRPC collector. The port is read from the raw
///   authority because `Url::port` hides an explicit default port (`:80`).
/// - No path, query, fragment, or userinfo: gRPC uses none of them, and an
///   OTLP/HTTP URL (`…:4318/v1/traces`) there means the wrong protocol.
///
/// - No surrounding whitespace, tab, or newline: the URL parser strips them
///   but the exporter does not, so such a value would pass here and fail at
///   start-up.
///
/// Errors never echo any part of the endpoint: it can carry credentials, so
/// it is redacted like `rpc_url`.
fn validate_otlp_endpoint(endpoint: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        endpoint.trim() == endpoint && !endpoint.contains(['\t', '\n', '\r']),
        "observability.otlp_endpoint must not contain whitespace, tabs, or newlines"
    );
    // Checked on the raw text, before parsing: `http:host:4317` parses as a URL
    // but is not `http://…`, and a scheme-less value would parse its first
    // segment (possibly a username) as the scheme.
    anyhow::ensure!(
        endpoint
            .get(..7)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://")),
        "observability.otlp_endpoint must start with http://; the OTLP gRPC exporter has \
         no TLS, so point it at a local collector and terminate TLS there"
    );
    let parsed = url::Url::parse(endpoint)
        .map_err(|e| anyhow::anyhow!("observability.otlp_endpoint is not a valid URL: {e}"))?;
    anyhow::ensure!(
        parsed.host().is_some(),
        "observability.otlp_endpoint has no host"
    );
    // Authority = text after `://` up to the first `/`, `?`, or `#`. Read raw:
    // the parsed URL reports an empty userinfo (`http://@host`) as absent and
    // hides an explicit default port.
    let authority = endpoint
        .split_once("://")
        .map_or("", |(_, rest)| rest)
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    anyhow::ensure!(
        !authority.contains('@') && parsed.username().is_empty() && parsed.password().is_none(),
        "observability.otlp_endpoint must not carry userinfo; pass collector \
         credentials through the collector, not the URL"
    );
    anyhow::ensure!(
        parsed.path() == "/" && parsed.query().is_none() && parsed.fragment().is_none(),
        "observability.otlp_endpoint must be http://host:port with no path, query, or \
         fragment; an OTLP/HTTP URL (port 4318, /v1/traces) is the wrong protocol"
    );
    // IPv6 hosts are bracketed, so a trailing `:<digits>` is always a port.
    let has_port = authority.rsplit_once(':').is_some_and(|(host, port)| {
        !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) && !host.is_empty()
    });
    anyhow::ensure!(
        has_port,
        "observability.otlp_endpoint must name the collector port (e.g. http://localhost:4317)"
    );
    Ok(())
}

/// Resolve download-receipt audit-log retention fields (#802).
///
/// `max_file_bytes` is clamped to `[MIN_RECEIPT_MAX_FILE_BYTES,
/// MAX_RECEIPT_MAX_FILE_BYTES]` (rejected, not silently clamped) so a typo
/// can neither rotate on every line nor defeat rotation. `retained_files`
/// accepts `0` (truncate-in-place, no backups) up to
/// `MAX_RECEIPT_RETAINED_FILES`.
#[cfg(test)]
fn resolve_receipts(file: Option<&types::ReceiptsConfig>) -> anyhow::Result<ResolvedReceipts> {
    one_section(|bag| resolve_receipts_into(file, bag))
}

/// Bag-threading worker for the `[receipts]` section. Shares a bag with the
/// other sections during startup ([`resolve_config`]); see
/// [`resolve_payment_into`] for the rationale. The `#[cfg(test)]`
/// `resolve_receipts` shim wraps this for direct unit tests.
fn resolve_receipts_into(
    file: Option<&types::ReceiptsConfig>,
    bag: &mut ConfigDiagnostics,
) -> ResolvedReceipts {
    let max_file_bytes = file
        .and_then(|r| r.max_file_bytes)
        .unwrap_or(DEFAULT_RECEIPT_MAX_FILE_BYTES);
    bag.check_with(
        (MIN_RECEIPT_MAX_FILE_BYTES..=MAX_RECEIPT_MAX_FILE_BYTES).contains(&max_file_bytes),
        "receipts.max_file_bytes",
        || {
            format!(
                "receipts.max_file_bytes ({max_file_bytes}) must be within \
                 [{MIN_RECEIPT_MAX_FILE_BYTES}, {MAX_RECEIPT_MAX_FILE_BYTES}] bytes"
            )
        },
    );

    let retained_files = file
        .and_then(|r| r.retained_files)
        .unwrap_or(DEFAULT_RECEIPT_RETAINED_FILES);
    bag.check_with(
        retained_files <= MAX_RECEIPT_RETAINED_FILES,
        "receipts.retained_files",
        || {
            format!(
                "receipts.retained_files ({retained_files}) must be <= \
                 {MAX_RECEIPT_RETAINED_FILES}"
            )
        },
    );

    ResolvedReceipts {
        max_file_bytes,
        retained_files,
    }
}

/// Resolve security / rate-limiting fields.
///
/// Each numeric field accepts `0` as the "disable this layer" sentinel:
/// `max_concurrent_handlers = 0` skips the global semaphore acquire,
/// `per_*_rate_per_sec = 0.0` skips the corresponding token bucket, and
/// `max_tracked_sources = 0` makes the bookkeeping map unbounded (operator
/// opt-in — an attacker churning identities can grow the map without
/// bound). The `per_*_burst > 0` rule is paired with the matching rate:
/// `burst` is irrelevant when `rate == 0` (the layer's `try_consume` short-
/// circuits before the bucket is touched), but a `rate > 0` with `burst == 0`
/// is a deny-all corner case operators don't actually want — reject it
/// outright so a typo turns into a config error rather than a black-hole node.
// Each field is a linear "default-or-file → validate → log-if-disabled"
// triple; splitting them out would scatter the field-pair invariants
// (rate/burst coupling) across helpers that have to take both arguments
// anyway. Keep it linear.
#[cfg(test)]
pub fn resolve_security(file: Option<&types::SecurityConfig>) -> anyhow::Result<ResolvedSecurity> {
    one_section(|bag| resolve_security_into(file, bag))
}

/// Bag-threading variant of the test-only `resolve_security` shim. Shares a bag with other
/// sections during startup ([`resolve_config`]) and SIGHUP reload
/// (`runtime::reload`); see [`resolve_payment_into`] for the rationale.
#[allow(clippy::cognitive_complexity)]
pub fn resolve_security_into(
    file: Option<&types::SecurityConfig>,
    bag: &mut ConfigDiagnostics,
) -> ResolvedSecurity {
    let max_concurrent_handlers = file
        .and_then(|s| s.max_concurrent_handlers)
        .unwrap_or(DEFAULT_MAX_CONCURRENT_HANDLERS);
    // `tokio::sync::Semaphore` panics if asked to hold more than
    // `MAX_PERMITS` (= `usize::MAX >> 3`). On 64-bit this is ~2.3×10^18
    // so any `u32` is safe; on 32-bit it's ~5.4×10^8 and any
    // `max_concurrent_handlers > MAX_PERMITS` would crash startup *or*
    // a SIGHUP-driven reload via `add_permits`. Reject at config time
    // so the failure mode is "node refuses to boot with a clear
    // error" rather than "node panics on next reload."
    let max_permits = tokio::sync::Semaphore::MAX_PERMITS;
    bag.check_with(
        usize::try_from(max_concurrent_handlers).is_ok_and(|v| v <= max_permits),
        "security.max_concurrent_handlers",
        || format!(
            "security.max_concurrent_handlers={max_concurrent_handlers} exceeds tokio Semaphore::MAX_PERMITS={max_permits} on this target"
        ),
    );

    let per_source_rate_per_sec = file
        .and_then(|s| s.per_source_rate_per_sec)
        .unwrap_or(DEFAULT_PER_SOURCE_RATE_PER_SEC);
    bag.check(
        per_source_rate_per_sec.is_finite() && per_source_rate_per_sec >= 0.0,
        "security.per_source_rate_per_sec",
        "security.per_source_rate_per_sec must be a finite non-negative number \
         (0 disables the per-source layer)",
    );

    let per_source_burst = file
        .and_then(|s| s.per_source_burst)
        .unwrap_or(DEFAULT_PER_SOURCE_BURST);
    bag.check(
        per_source_rate_per_sec == 0.0 || per_source_burst > 0,
        "security.per_source_burst",
        "security.per_source_burst must be > 0 when per_source_rate_per_sec > 0 \
         (set both to 0 to disable the per-source layer)",
    );

    let max_tracked_sources = file
        .and_then(|s| s.max_tracked_sources)
        .unwrap_or(DEFAULT_MAX_TRACKED_SOURCES);

    if max_concurrent_handlers == 0 {
        bag.note(
            "security.max_concurrent_handlers",
            "0: global concurrency cap disabled",
        );
    }
    if per_source_rate_per_sec == 0.0 {
        bag.note(
            "security.per_source_rate_per_sec",
            "0: per-source rate-limit disabled",
        );
    }
    if max_tracked_sources == 0 {
        bag.warn(
            "security.max_tracked_sources",
            "0: rate-limit bookkeeping map is unbounded; an attacker churning sources can \
             grow it without limit",
        );
    }

    ResolvedSecurity {
        max_concurrent_handlers,
        per_source_rate_per_sec,
        per_source_burst,
        max_tracked_sources,
    }
}

/// Resolve node-local load-shedding thresholds.
#[cfg(test)]
pub fn resolve_load_shed(file: Option<&types::LoadShedConfig>) -> anyhow::Result<ResolvedLoadShed> {
    one_section(|bag| resolve_load_shed_into(file, bag))
}

/// Bag-threading variant of the test-only `resolve_load_shed` shim. Shares a bag with other
/// sections during startup ([`resolve_config`]) and SIGHUP reload
/// (`runtime::reload`); see [`resolve_payment_into`] for the rationale.
pub fn resolve_load_shed_into(
    file: Option<&types::LoadShedConfig>,
    bag: &mut ConfigDiagnostics,
) -> ResolvedLoadShed {
    let policy = match file.and_then(|c| c.policy.as_deref()) {
        None | Some("resource-pressure") => LoadShedPolicyKind::ResourcePressure,
        Some("always-admit") => LoadShedPolicyKind::AlwaysAdmit,
        Some(other) => {
            bag.push(
                "load_shed.policy",
                format!(
                    "load_shed.policy must be \"resource-pressure\" or \"always-admit\", got {other:?}"
                ),
            );
            LoadShedPolicyKind::ResourcePressure
        }
    };
    let high = file
        .and_then(|c| c.max_concurrent_serves_high)
        .unwrap_or(DEFAULT_LOAD_SHED_SERVES_HIGH);
    let low = file
        .and_then(|c| c.max_concurrent_serves_low)
        .unwrap_or(DEFAULT_LOAD_SHED_SERVES_LOW);
    bag.check_with(high >= low, "load_shed.max_concurrent_serves_high", || {
        format!("load_shed.max_concurrent_serves_high ({high}) must be >= _low ({low})")
    });
    ResolvedLoadShed {
        policy,
        egress_budget_mbps: file
            .and_then(|c| c.egress_budget_mbps)
            .unwrap_or(DEFAULT_LOAD_SHED_EGRESS_BUDGET_MBPS),
        max_concurrent_serves_high: high,
        max_concurrent_serves_low: low,
        per_client_serve_cap: file
            .and_then(|c| c.per_client_serve_cap)
            .unwrap_or(DEFAULT_LOAD_SHED_PER_CLIENT_CAP),
    }
}

/// Resolve `cdn/dht/v1` rate-limit settings (ADR 022 §DHT Rate Limiting).
///
/// Each `*_rate_per_sec == 0.0` paired with `*_burst == 0` disables that
/// layer (operator opt-out). Mixing `rate > 0` with `burst == 0` is
/// rejected as a deny-all corner case — the resolver treats it the same
/// way [`resolve_security_into`] handles the `per_source` pairing.
#[allow(clippy::cognitive_complexity)] // linear "default-or-file → validate" rows.
pub fn resolve_dht_into(
    file: Option<&types::DhtConfig>,
    bag: &mut ConfigDiagnostics,
) -> ResolvedDht {
    // ADR 022 nests the rate-limit knobs under `dht.rate_limit.*`.
    // The file shape mirrors that; an absent `[dht.rate_limit]` collapses
    // to "all defaults" through the same `.and_then` chain the other
    // resolvers use.
    let rate_limit = file.and_then(|d| d.rate_limit.as_ref());
    let per_peer_rate_per_sec = rate_limit
        .and_then(|r| r.per_peer_rate_per_sec)
        .unwrap_or(DEFAULT_DHT_PER_PEER_RATE_PER_SEC);
    bag.check(
        per_peer_rate_per_sec.is_finite() && per_peer_rate_per_sec >= 0.0,
        "dht.rate_limit.per_peer_rate_per_sec",
        "dht.rate_limit.per_peer_rate_per_sec must be a finite non-negative number (0 disables the layer)",
    );
    let per_peer_burst = rate_limit
        .and_then(|r| r.per_peer_burst)
        .unwrap_or(DEFAULT_DHT_PER_PEER_BURST);
    bag.check(
        per_peer_rate_per_sec == 0.0 || per_peer_burst > 0,
        "dht.rate_limit.per_peer_burst",
        "dht.rate_limit.per_peer_burst must be > 0 when per_peer_rate_per_sec > 0 (set both to 0 to disable)",
    );

    let per_ip_rate_per_sec = rate_limit
        .and_then(|r| r.per_ip_rate_per_sec)
        .unwrap_or(DEFAULT_DHT_PER_IP_RATE_PER_SEC);
    bag.check(
        per_ip_rate_per_sec.is_finite() && per_ip_rate_per_sec >= 0.0,
        "dht.rate_limit.per_ip_rate_per_sec",
        "dht.rate_limit.per_ip_rate_per_sec must be a finite non-negative number (0 disables the layer)",
    );
    let per_ip_burst = rate_limit
        .and_then(|r| r.per_ip_burst)
        .unwrap_or(DEFAULT_DHT_PER_IP_BURST);
    bag.check(
        per_ip_rate_per_sec == 0.0 || per_ip_burst > 0,
        "dht.rate_limit.per_ip_burst",
        "dht.rate_limit.per_ip_burst must be > 0 when per_ip_rate_per_sec > 0 (set both to 0 to disable)",
    );

    let global_rate_per_sec = rate_limit
        .and_then(|r| r.global_rate_per_sec)
        .unwrap_or(DEFAULT_DHT_GLOBAL_RATE_PER_SEC);
    bag.check(
        global_rate_per_sec.is_finite() && global_rate_per_sec >= 0.0,
        "dht.rate_limit.global_rate_per_sec",
        "dht.rate_limit.global_rate_per_sec must be a finite non-negative number (0 disables the layer)",
    );
    let global_burst = rate_limit
        .and_then(|r| r.global_burst)
        .unwrap_or(DEFAULT_DHT_GLOBAL_BURST);
    bag.check(
        global_rate_per_sec == 0.0 || global_burst > 0,
        "dht.rate_limit.global_burst",
        "dht.rate_limit.global_burst must be > 0 when global_rate_per_sec > 0 (set both to 0 to disable)",
    );

    let max_tracked_per_ip = rate_limit
        .and_then(|r| r.max_tracked_per_ip)
        .unwrap_or(DEFAULT_DHT_MAX_TRACKED_PER_IP);
    let max_tracked_per_peer = rate_limit
        .and_then(|r| r.max_tracked_per_peer)
        .unwrap_or(DEFAULT_DHT_MAX_TRACKED_PER_PEER);
    if max_tracked_per_ip == 0 {
        bag.warn(
            "dht.rate_limit.max_tracked_per_ip",
            "0: per-IP bookkeeping map is unbounded; an attacker churning source IPs can \
             grow it without limit",
        );
    }
    if max_tracked_per_peer == 0 {
        bag.warn(
            "dht.rate_limit.max_tracked_per_peer",
            "0: per-peer bookkeeping map is unbounded; an attacker churning NodeIds can \
             grow it without limit",
        );
    }

    ResolvedDht {
        per_peer_rate_per_sec,
        per_peer_burst,
        per_ip_rate_per_sec,
        per_ip_burst,
        global_rate_per_sec,
        global_burst,
        max_tracked_per_ip,
        max_tracked_per_peer,
    }
}

/// Convenience wrapper for [`resolve_dht_into`] that takes a fresh
/// `ConfigDiagnostics`. Test-only.
#[cfg(test)]
pub fn resolve_dht(file: Option<&types::DhtConfig>) -> anyhow::Result<ResolvedDht> {
    one_section(|bag| resolve_dht_into(file, bag))
}

/// Resolve the `[probe.rate_limit]` section into [`ResolvedProbe`], applying
/// the ADR 005 §Probe rate limiting defaults and the same validation the DHT
/// layer uses: each `*_rate_per_sec` finite and `>= 0` (0 disables the layer),
/// and the matching `*_burst > 0` whenever its rate is `> 0` (no deny-all).
/// Mirrors [`resolve_dht_into`]; only the
/// defaults and the `probe.rate_limit.*` field keys differ.
#[allow(clippy::cognitive_complexity)] // linear "default-or-file → validate" rows.
pub fn resolve_probe_into(
    file: Option<&types::ProbeConfig>,
    bag: &mut ConfigDiagnostics,
) -> ResolvedProbe {
    // ADR 005 nests the rate-limit knobs under `probe.rate_limit.*`.
    let rate_limit = file.and_then(|p| p.rate_limit.as_ref());
    let per_peer_rate_per_sec = rate_limit
        .and_then(|r| r.per_peer_rate_per_sec)
        .unwrap_or(DEFAULT_PROBE_PER_PEER_RATE_PER_SEC);
    bag.check(
        per_peer_rate_per_sec.is_finite() && per_peer_rate_per_sec >= 0.0,
        "probe.rate_limit.per_peer_rate_per_sec",
        "probe.rate_limit.per_peer_rate_per_sec must be a finite non-negative number (0 disables the layer)",
    );
    let per_peer_burst = rate_limit
        .and_then(|r| r.per_peer_burst)
        .unwrap_or(DEFAULT_PROBE_PER_PEER_BURST);
    bag.check(
        per_peer_rate_per_sec == 0.0 || per_peer_burst > 0,
        "probe.rate_limit.per_peer_burst",
        "probe.rate_limit.per_peer_burst must be > 0 when per_peer_rate_per_sec > 0 (set both to 0 to disable)",
    );

    let per_ip_rate_per_sec = rate_limit
        .and_then(|r| r.per_ip_rate_per_sec)
        .unwrap_or(DEFAULT_PROBE_PER_IP_RATE_PER_SEC);
    bag.check(
        per_ip_rate_per_sec.is_finite() && per_ip_rate_per_sec >= 0.0,
        "probe.rate_limit.per_ip_rate_per_sec",
        "probe.rate_limit.per_ip_rate_per_sec must be a finite non-negative number (0 disables the layer)",
    );
    let per_ip_burst = rate_limit
        .and_then(|r| r.per_ip_burst)
        .unwrap_or(DEFAULT_PROBE_PER_IP_BURST);
    bag.check(
        per_ip_rate_per_sec == 0.0 || per_ip_burst > 0,
        "probe.rate_limit.per_ip_burst",
        "probe.rate_limit.per_ip_burst must be > 0 when per_ip_rate_per_sec > 0 (set both to 0 to disable)",
    );

    let global_rate_per_sec = rate_limit
        .and_then(|r| r.global_rate_per_sec)
        .unwrap_or(DEFAULT_PROBE_GLOBAL_RATE_PER_SEC);
    bag.check(
        global_rate_per_sec.is_finite() && global_rate_per_sec >= 0.0,
        "probe.rate_limit.global_rate_per_sec",
        "probe.rate_limit.global_rate_per_sec must be a finite non-negative number (0 disables the layer)",
    );
    let global_burst = rate_limit
        .and_then(|r| r.global_burst)
        .unwrap_or(DEFAULT_PROBE_GLOBAL_BURST);
    bag.check(
        global_rate_per_sec == 0.0 || global_burst > 0,
        "probe.rate_limit.global_burst",
        "probe.rate_limit.global_burst must be > 0 when global_rate_per_sec > 0 (set both to 0 to disable)",
    );

    let max_tracked_per_ip = rate_limit
        .and_then(|r| r.max_tracked_per_ip)
        .unwrap_or(DEFAULT_PROBE_MAX_TRACKED_PER_IP);
    let max_tracked_per_peer = rate_limit
        .and_then(|r| r.max_tracked_per_peer)
        .unwrap_or(DEFAULT_PROBE_MAX_TRACKED_PER_PEER);
    if max_tracked_per_ip == 0 {
        bag.warn(
            "probe.rate_limit.max_tracked_per_ip",
            "0: per-IP bookkeeping map is unbounded; an attacker churning source IPs can \
             grow it without limit",
        );
    }
    if max_tracked_per_peer == 0 {
        bag.warn(
            "probe.rate_limit.max_tracked_per_peer",
            "0: per-peer bookkeeping map is unbounded; an attacker churning NodeIds can \
             grow it without limit",
        );
    }

    ResolvedProbe {
        per_peer_rate_per_sec,
        per_peer_burst,
        per_ip_rate_per_sec,
        per_ip_burst,
        global_rate_per_sec,
        global_burst,
        max_tracked_per_ip,
        max_tracked_per_peer,
    }
}

/// Convenience wrapper for [`resolve_probe_into`] that takes a fresh
/// `ConfigDiagnostics`. Test-only.
#[cfg(test)]
pub fn resolve_probe(file: Option<&types::ProbeConfig>) -> anyhow::Result<ResolvedProbe> {
    one_section(|bag| resolve_probe_into(file, bag))
}

/// Load a [`FileConfig`] from disk.
///
/// - If `explicit_path` is `Some`, reads that file (errors if missing).
/// - If `explicit_path` is `None`, tries the default path; returns
///   `FileConfig::default()` if the file does not exist.
pub fn load_file_config(explicit_path: Option<&Path>) -> anyhow::Result<FileConfig> {
    let path = match explicit_path {
        Some(p) => p.to_path_buf(),
        None => match common::default_config_path() {
            Some(p) if p.exists() => p,
            _ => return Ok(FileConfig::default()),
        },
    };

    let contents = std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("failed to read config file {}: {e}", path.display()))?;

    let mut config: FileConfig = toml::from_str(&contents)
        .map_err(|e| anyhow::anyhow!("failed to parse config file {}: {e}", path.display()))?;

    expand_env(&mut config)?;

    Ok(config)
}

/// Expand `${VAR}` and a leading `~` in the TOML config fields listed
/// below. Missing env vars produce a contextual error naming the offending
/// field. Enables a single TOML template to be reused across container and
/// Kubernetes deployments without file mutation.
///
/// Bare `$VAR` (without braces) is intentionally **not** expanded: many
/// legitimate config values contain a literal `$` (URLs with basic-auth
/// passwords, query parameters, contract addresses), and eager expansion
/// would turn those into spurious "undefined env var" errors. Operators
/// wanting substitution must use the explicit `${VAR}` form.
///
/// The substitution has **no escape semantics** — backslashes pass through
/// verbatim. This matters on Windows, where values like
/// `C:\Users\${USER}\data` must still have `${USER}` expanded; a shell-style
/// escape interpreter would treat `\$` as a literal `$` and skip the
/// expansion.
fn expand_env(cfg: &mut FileConfig) -> anyhow::Result<()> {
    if let Some(i) = cfg.identity.as_mut() {
        expand_path(&mut i.data_dir, "identity.data_dir")?;
        expand_str(&mut i.region, "identity.region")?;
    }
    if let Some(n) = cfg.network.as_mut() {
        if let Some(urls) = n.relay_urls.as_mut() {
            for (idx, url) in urls.iter_mut().enumerate() {
                *url = expand_value(url, &format!("network.relay_urls[{idx}]"))?;
            }
        }
        // #863: `[network.discovery]` fields were skipped by `${VAR}` expansion
        // unlike their `relay_urls` sibling — `dns_origin = "${DNS_ORIGIN}"`
        // passed the non-empty check and reached bring-up as a literal string,
        // silently breaking peer resolution. Mirror the relay loop here.
        if let Some(d) = n.discovery.as_mut() {
            expand_str(&mut d.pkarr_url, "network.discovery.pkarr_url")?;
            expand_str(&mut d.dns_origin, "network.discovery.dns_origin")?;
            if let Some(peers) = d.peers.as_mut() {
                // `HashMap` iteration order is nondeterministic; sort by peer id
                // so that when two peers both carry a bad `${VAR}`, the `?`
                // surfaces a stable error across runs. Mirrors the sort in
                // `resolve_discovery_into`. Unstable sort: keys are unique.
                let mut entries: Vec<_> = peers.iter_mut().collect();
                entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
                for (id, peer) in entries {
                    expand_str(
                        &mut peer.relay_url,
                        &format!("network.discovery.peers[{id}].relay_url"),
                    )?;
                    for (idx, addr) in peer.addrs.iter_mut().enumerate() {
                        *addr = expand_value(
                            addr,
                            &format!("network.discovery.peers[{id}].addrs[{idx}]"),
                        )?;
                    }
                }
            }
        }
    }
    if let Some(b) = cfg.blockchain.as_mut() {
        expand_str(&mut b.rpc_url, "blockchain.rpc_url")?;
        expand_path(&mut b.eth_keystore, "blockchain.eth_keystore")?;
        expand_str(
            &mut b.payment_pool_address,
            "blockchain.payment_pool_address",
        )?;
        expand_str(
            &mut b.capacity_bond_address,
            "blockchain.capacity_bond_address",
        )?;
        expand_str(
            &mut b.origin_assignment_address,
            "blockchain.origin_assignment_address",
        )?;
        expand_str(
            &mut b.publisher_registry_address,
            "blockchain.publisher_registry_address",
        )?;
        expand_str(&mut b.slash_judge_address, "blockchain.slash_judge_address")?;
        expand_str(
            &mut b.slash_appeal_address,
            "blockchain.slash_appeal_address",
        )?;
        expand_str(
            &mut b.content_blacklist_address,
            "blockchain.content_blacklist_address",
        )?;
    }
    if let Some(c) = cfg.cache.as_mut() {
        expand_path(&mut c.cache_dir, "cache.cache_dir")?;
        if let Some(origin) = c.origin.as_mut() {
            expand_origin(origin, "cache.origin")?;
        }
        if let Some(list) = c.origins.as_mut() {
            for (idx, entry) in list.iter_mut().enumerate() {
                let path_ctx = format!("cache.origins[{idx}]");
                expand_origin(entry, &path_ctx)?;
            }
        }
        expand_str(&mut c.user_agent, "cache.user_agent")?;
    }
    if let Some(o) = cfg.observability.as_mut() {
        expand_str(&mut o.otlp_endpoint, "observability.otlp_endpoint")?;
    }
    Ok(())
}

fn expand_str(field: &mut Option<String>, ctx: &str) -> anyhow::Result<()> {
    if let Some(s) = field.as_mut() {
        *s = expand_value(s, ctx)?;
    }
    Ok(())
}

/// Walk an [`types::OriginConfig`] and run `${VAR}` / leading-`~`
/// expansion on every URL and path field. Sibling of the per-section
/// expansion blocks in [`expand_env`]; lifted out because the cache
/// origin is a tagged enum with backend-specific shape. `prefix` names
/// the TOML path of the containing table (`"cache.origin"` for the
/// singular form, `"cache.origins[i]"` for the array form) so error
/// messages carry the operator-visible field path.
fn expand_origin(origin: &mut types::OriginConfig, prefix: &str) -> anyhow::Result<()> {
    match origin {
        types::OriginConfig::Http { url, .. } => {
            *url = expand_value(url, &format!("{prefix}.url"))?;
        }
        types::OriginConfig::Fs { path } => {
            let as_str = path.to_string_lossy();
            let expanded = expand_value(&as_str, &format!("{prefix}.path"))?;
            *path = PathBuf::from(expanded);
        }
        types::OriginConfig::S3(s3) => {
            s3.bucket = expand_value(&s3.bucket, &format!("{prefix}.bucket"))?;
            s3.region = expand_value(&s3.region, &format!("{prefix}.region"))?;
            expand_str(&mut s3.endpoint_url, &format!("{prefix}.endpoint_url"))?;
            expand_str(&mut s3.prefix, &format!("{prefix}.prefix"))?;
            if let Some(creds) = s3.credentials.as_mut() {
                match creds {
                    types::S3Credentials::Static {
                        access_key_id,
                        secret_access_key,
                        session_token,
                    } => {
                        expand_secret(
                            access_key_id,
                            &format!("{prefix}.credentials.access_key_id"),
                        )?;
                        expand_secret(
                            secret_access_key,
                            &format!("{prefix}.credentials.secret_access_key"),
                        )?;
                        if let Some(token) = session_token.as_mut() {
                            expand_secret(token, &format!("{prefix}.credentials.session_token"))?;
                        }
                    }
                    types::S3Credentials::DefaultChain { profile } => {
                        expand_str(profile, &format!("{prefix}.credentials.profile"))?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn expand_path(field: &mut Option<PathBuf>, ctx: &str) -> anyhow::Result<()> {
    if let Some(p) = field.as_mut() {
        let as_str = p.to_string_lossy();
        let expanded = expand_value(&as_str, ctx)?;
        *p = PathBuf::from(expanded);
    }
    Ok(())
}

/// `${VAR}` expansion for a [`secret::SecretString`] field. The
/// expansion runs against the raw secret value via
/// [`secret::SecretString::expose`], then re-wraps the result so the
/// redacted-Debug invariant is preserved at every other call site.
///
/// `expand_value`'s error chain only mentions the field's dotted path
/// and the missing env-var name — never the partially-expanded value
/// — so a missing env-var error here cannot leak any cleartext that
/// happens to precede the `${VAR}` marker (verified via the existing
/// `expand_value` contract; see `expand_braces` in
/// `crates/common/src/cli/common.rs`).
fn expand_secret(field: &mut secret::SecretString, ctx: &str) -> anyhow::Result<()> {
    let expanded = expand_value(field.expose(), ctx)?;
    *field = secret::SecretString::new(expanded);
    Ok(())
}

fn expand_value(raw: &str, ctx: &str) -> anyhow::Result<String> {
    if !needs_expansion(raw) {
        return Ok(raw.to_string());
    }
    let tilde_expanded = expand_tilde_prefix(raw, ctx)?;
    expand_braces(&tilde_expanded, ctx)
}

/// Returns true if `raw` contains an expansion marker (`${` or leading `~`).
/// Values without a marker are passed through verbatim so literal `$` stays
/// literal (see [`expand_env`] for rationale).
fn needs_expansion(raw: &str) -> bool {
    raw.contains("${") || raw.starts_with('~')
}

/// Replace a leading `~`, `~/`, or (on Windows) `~\` with the user's home
/// directory. Other occurrences of `~` (e.g. in the middle of a string) are
/// left alone. Errors contextually if the home directory is not resolvable,
/// rather than silently returning a literal `~` that downstream file I/O
/// would later fail on with an unrelated error.
fn expand_tilde_prefix(raw: &str, ctx: &str) -> anyhow::Result<String> {
    if raw == "~" {
        let home = dirs::home_dir().ok_or_else(|| missing_home_err(ctx))?;
        return Ok(home.to_string_lossy().into_owned());
    }
    let rest = raw.strip_prefix("~/").or_else(|| {
        if cfg!(windows) {
            raw.strip_prefix(r"~\")
        } else {
            None
        }
    });
    if let Some(rest) = rest {
        let home = dirs::home_dir().ok_or_else(|| missing_home_err(ctx))?;
        return Ok(home.join(rest).to_string_lossy().into_owned());
    }
    Ok(raw.to_string())
}

fn missing_home_err(ctx: &str) -> anyhow::Error {
    anyhow::anyhow!("config field `{ctx}` uses `~` but home directory is not available")
}

/// Substitute `${VAR}` sequences with the corresponding env var value. No
/// escape semantics — backslashes, single `$`, and any other character pass
/// through verbatim. This is important for Windows paths like
/// `C:\Users\${USER}\data`, where a shell-style escape interpreter would
/// swallow the `\` before `$` and disable the substitution.
fn expand_braces(raw: &str, ctx: &str) -> anyhow::Result<String> {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some((before, after)) = rest.split_once("${") {
        out.push_str(before);
        let (name, tail) = after.split_once('}').ok_or_else(|| {
            anyhow::anyhow!("config field `{ctx}` has unterminated `${{` sequence")
        })?;
        let value = match std::env::var(name) {
            Ok(v) => v,
            Err(std::env::VarError::NotPresent) => {
                anyhow::bail!("config field `{ctx}` references undefined env var `{name}`")
            }
            Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!(
                "config field `{ctx}` references env var `{name}` whose value is not valid UTF-8"
            ),
        };
        out.push_str(&value);
        rest = tail;
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
