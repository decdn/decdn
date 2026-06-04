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

pub use errors::ConfigErrorBag;
pub use resolved::{
    ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedDht, ResolvedGossip,
    ResolvedIdentity, ResolvedNetwork, ResolvedObservability, ResolvedOrigin, ResolvedPayment,
    ResolvedReceipts, ResolvedS3Config, ResolvedS3Credentials, ResolvedSecurity,
};
pub use types::FileConfig;

/// Default QUIC bind port.
const DEFAULT_BIND_PORT: u16 = 4433;
/// Default for the QUIC 0-RTT master switch (ADR 015). 0-RTT for
/// `cdn/probe/v1` is on by default; operators kill it via
/// `network.enable_0rtt = false`.
const DEFAULT_ENABLE_0RTT: bool = true;
/// Default maximum cache size in megabytes (10 GB).
const DEFAULT_CACHE_SIZE_MB: u64 = 10_240;
/// Default maximum single blob size in megabytes (1 GB).
///
/// Deliberately well below `DEFAULT_CACHE_SIZE_MB` so a single oversized
/// blob can't saturate the entire cache and evict all other content in
/// one fetch. The `max_blob_size_mb < cache_size_mb` invariant is enforced
/// at config load (see `resolve_cache_into`).
const DEFAULT_MAX_BLOB_SIZE_MB: u64 = 1_024;
/// Default rate per MB in USDC base units ($0.00001/MB).
const DEFAULT_RATE_PER_MB: u64 = 10;
/// Default Prometheus metrics port. Exposed publicly so `decdn node top`
/// (issue #275) can fall back to the same number the daemon binds on
/// without duplicating the constant.
pub const DEFAULT_METRICS_PORT: u16 = 9090;
/// Default metrics bind address (loopback). Operators in containerised
/// deployments override to `0.0.0.0` via CLI/env/config.
const DEFAULT_METRICS_BIND: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
/// Default loopback admin HTTP port (ADR 025). Exposed to the rest of
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

/// Default accrued-claim redemption threshold: 1 USDC (`1_000_000` `µUSDC`).
/// At this size the ~$0.10 `withdraw` gas is a few percent of the redeemed
/// amount while bounding unsettled exposure to ~1 USDC per channel (#327).
const DEFAULT_REDEEM_THRESHOLD_MICRO_USDC: u64 = 1_000_000;
/// Default buyer-side channel deposit: 10 USDC (`10_000_000` `µUSDC`). ADR 003
/// § Deposit Economics recommends a 10 USDC practical minimum (gas overhead
/// ~2.3%); the on-chain `minDeposit` floor still applies and the resolved value
/// is clamped up to it at open time (#744).
const DEFAULT_BUYER_DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// Default interval between outgoing `NodeAnnounce` messages (ADR 001).
const DEFAULT_ANNOUNCE_INTERVAL_SEC: u64 = 60;
/// Default peer-table entry TTL after which a stale entry is evicted.
const DEFAULT_PEER_TTL_SEC: u64 = 600;
/// Default hard cap on `PeerTable` entry count (#577 H3). Sized for ~tens
/// of MB of resident memory at the ~few-hundred-byte `PeerEntry` size,
/// which is plenty of headroom for the tens-of-nodes `PoC` while still
/// capping the fresh-keypair memory-DoS that the empty-allowlist
/// stand-in would otherwise leave unbounded.
const DEFAULT_MAX_PEER_TABLE_ENTRIES: u64 = 100_000;
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

/// Default EIP-712 `chainId` for the `slash_sig` domain separator (ADR 014).
/// Arbitrum Sepolia — the initial network target; matches the chain id bound
/// on the runtime `PrivateKeySigner` (`decdn_incentive::eth_identity`). To
/// target a different chain, override via `blockchain.chain_id` (see
/// `appendix-poc-production-seams.md` §Seam 8).
pub const DEFAULT_CHAIN_ID: u64 = 421_614;

/// Default maximum concurrently held (eviction-exempt) blobs for the
/// probe-triggered hold (ADR 005 §Hold budget, #318). Per-blob holds: many
/// peers probing one hash share a single slot. Re-exported from the
/// `decdn_config_types` leaf crate (the canonical home since #578) so
/// the config default and the cache engine's own default (used by
/// direct `CacheEngine::open` callers) cannot drift apart.
pub const DEFAULT_MAX_PROBE_HOLDS: usize = decdn_config_types::DEFAULT_MAX_PROBE_HOLDS;

/// Default probe-hold slots reserved for the stake lane (#757). `0` keeps
/// the stake-lane reservation off by default, so a single-lane node behaves
/// exactly as before — the reservation is strictly operator opt-in.
pub const DEFAULT_STAKE_LANE_RESERVED_HOLDS: usize = 0;

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

/// Load config from file (if present) and merge with CLI args.
///
/// CLI args take precedence over file values; defaults fill gaps.
///
/// # Errors
///
/// Returns an error if:
/// - The config file exists but cannot be read or parsed.
/// - A required field (`rpc_url`, `payment_channel_address`,
///   `capacity_bond_address`) is not provided by any source.
/// - The home directory cannot be determined for default paths.
pub fn resolve_config(config_path: Option<&Path>, cli: &RunArgs) -> anyhow::Result<ResolvedConfig> {
    // `load_file_config` stays fail-fast: a file we could not read, parse,
    // or env-expand never produced a `FileConfig`, so there is nothing to
    // validate. Everything *after* this accumulates into one `bag` so an
    // operator sees every problem in a single pass.
    let file = load_file_config(config_path)?;

    let mut bag = ConfigErrorBag::new();

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
    let network = resolve_network(&cli.network, file.network.as_ref());
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
    let gossip = resolve_gossip_into(file.gossip.as_ref(), &mut bag);
    let security = resolve_security_into(file.security.as_ref(), &mut bag);
    let dht = resolve_dht_into(file.dht.as_ref(), &mut bag);
    let receipts = resolve_receipts_into(file.receipts.as_ref(), &mut bag);

    ensure_region_when_publishing_global_into(&identity, &gossip, &mut bag);
    validate_port_layout_into(&network, &observability, &mut bag);

    bag.into_result()?;

    Ok(ResolvedConfig {
        identity,
        network,
        blockchain,
        cache,
        payment,
        observability,
        gossip,
        security,
        dht,
        receipts,
    })
}

/// Reject configurations that would publish a region-less `NodeAnnounce` on
/// the global gossip topic — every peer drops those as `BadRegion`.
///
/// Region subscription is separately gated on `identity.region.is_some()` in
/// `GossipService::spawn`, so only the global-topic case needs an interlock:
/// if global is off and no region is set, the service runs as a no-op.
// Single-section shim preserving the `anyhow::Result` API the unit tests
// call directly; `resolve_config` uses the `*_into` worker with the
// shared bag instead, so this is test-only.
#[cfg(test)]
fn ensure_region_when_publishing_global(
    identity: &ResolvedIdentity,
    gossip: &ResolvedGossip,
) -> anyhow::Result<()> {
    one_section(|bag| ensure_region_when_publishing_global_into(identity, gossip, bag))
}

fn ensure_region_when_publishing_global_into(
    identity: &ResolvedIdentity,
    gossip: &ResolvedGossip,
    bag: &mut ConfigErrorBag,
) {
    // If the operator *did* supply a region but it failed
    // `normalize_region`, that single problem is already in the bag under
    // `identity.region`. Reporting "region must be set when subscribe_global
    // is true" on top of it would be misleading double-counting — they set
    // it, it was just malformed. Suppress the cascade.
    if bag.has_field(IDENTITY_REGION) {
        return;
    }
    bag.check(
        identity.region.is_some() || !gossip.subscribe_global,
        IDENTITY_REGION,
        "identity.region must be set when gossip.subscribe_global is true \
         (it signs every NodeAnnounce); set identity.region or disable the \
         global topic by setting gossip.subscribe_global = false",
    );
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
///
/// Uses `eprintln!` rather than `tracing::warn!` because `tracing` is not
/// yet initialized at `resolve_config` time (see `commands::run`).
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
    bag: &mut ConfigErrorBag,
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
            eprintln!(
                "warning: {name} = {p} is in the well-known range (<1024); \
                 requires elevated privilege to bind on Unix and may collide with \
                 a standardized service"
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
    bag: &mut ConfigErrorBag,
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
/// otherwise cause the node to publish announces that it and its peers
/// all reject at validation time, or — worse for unassigned codes that
/// slipped the bare ASCII check — partition the regional gossip topology.
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

/// Resolve network fields.
fn resolve_network(
    cli: &crate::cli::run::NetworkArgs,
    file: Option<&types::NetworkConfig>,
) -> ResolvedNetwork {
    let bind_port = cli
        .bind_port
        .or_else(|| file.and_then(|n| n.bind_port))
        .unwrap_or(DEFAULT_BIND_PORT);

    let relay_url = cli
        .relay_url
        .clone()
        .or_else(|| file.and_then(|n| n.relay_url.clone()));

    // No CLI flag: 0-RTT is an operational kill switch, not a per-invocation
    // tuning knob. File `network.enable_0rtt` > built-in default (`true`).
    let enable_0rtt = file
        .and_then(|n| n.enable_0rtt)
        .unwrap_or(DEFAULT_ENABLE_0RTT);

    ResolvedNetwork {
        bind_port,
        relay_url,
        enable_0rtt,
    }
}

/// Validate a user-supplied EVM contract address string.
///
/// Requires `0x` prefix, 40 hex characters, and a correct EIP-55 checksum.
/// Returns the canonical checksummed form.
fn parse_contract_address(flag_name: &str, raw: &str) -> anyhow::Result<String> {
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
    bag: &mut ConfigErrorBag,
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
    bag: &mut ConfigErrorBag,
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

    let payment_channel_address = resolve_contract_address(
        "blockchain.payment_channel_address",
        "payment_channel_address",
        "missing required option: --payment-channel-address \
         (or blockchain.payment_channel_address in config file)",
        cli.payment_channel_address
            .clone()
            .or_else(|| file.and_then(|b| b.payment_channel_address.clone())),
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

    let buyer_deposit_micro_usdc = file
        .and_then(|b| b.buyer_deposit_micro_usdc)
        .unwrap_or(DEFAULT_BUYER_DEPOSIT_MICRO_USDC);
    // A `0` buyer deposit would open dust channels (and revert below the
    // on-chain `minDeposit` floor). Reject it; the on-chain floor is the
    // authority on the lower bound, but a configured 0 is always an operator
    // mistake worth catching at load time.
    bag.check_with(
        buyer_deposit_micro_usdc > 0,
        "blockchain.buyer_deposit_micro_usdc",
        || {
            "blockchain.buyer_deposit_micro_usdc must be > 0 (a 0 deposit opens \
             dust channels and reverts below the on-chain minDeposit floor)"
                .to_string()
        },
    );
    // Default-on: the one-time max approval is what lets the buyer path open
    // channels without a manual approve step (ADR 003 § Deposit Economics).
    let buyer_max_approve = file.and_then(|b| b.buyer_max_approve).unwrap_or(true);

    // Auto-settlement triggers (#742). Both default to disabled (`None`) so
    // behavior is unchanged unless an operator opts in. A configured `0` is an
    // operator mistake: a 0 µUSDC value threshold would close on the first
    // voucher, and a 0 nonce-span threshold would close before any voucher
    // accrues — both burn gas on a guaranteed dust/no-op close. Reject either.
    let settlement_auto_threshold_micro_usdc =
        file.and_then(|b| b.settlement_auto_threshold_micro_usdc);
    bag.check_with(
        settlement_auto_threshold_micro_usdc != Some(0),
        "blockchain.settlement_auto_threshold_micro_usdc",
        || {
            "blockchain.settlement_auto_threshold_micro_usdc must be > 0 when set \
             (a 0 threshold closes the channel on the first voucher); omit the key \
             to disable auto-settlement"
                .to_string()
        },
    );
    let settlement_auto_by_voucher_nonce_span =
        file.and_then(|b| b.settlement_auto_by_voucher_nonce_span);
    bag.check_with(
        settlement_auto_by_voucher_nonce_span != Some(0),
        "blockchain.settlement_auto_by_voucher_nonce_span",
        || {
            "blockchain.settlement_auto_by_voucher_nonce_span must be > 0 when set \
             (a 0 span closes the channel before any voucher accrues); omit the key \
             to disable the voucher-nonce-span trigger"
                .to_string()
        },
    );

    // CLI/env only — no TOML field. `expand_tilde` for parity with the
    // keystore path itself. Existence check is intentionally deferred to
    // the runtime loader: if the operator passes a stale path the failure
    // surfaces as "no keystore password source available", which is
    // clearer than a config-resolution-time stat() error.
    let keystore_password_file = cli.keystore_password_file.clone().map(|p| expand_tilde(&p));

    ResolvedBlockchain {
        rpc_url,
        eth_keystore,
        keystore_password_file,
        payment_channel_address,
        capacity_bond_address,
        slash_judge_address,
        chain_id,
        rpc_watchdog_interval_sec,
        redeem_threshold_micro_usdc,
        buyer_deposit_micro_usdc,
        buyer_max_approve,
        settlement_auto_threshold_micro_usdc,
        settlement_auto_by_voucher_nonce_span,
    }
}

/// Resolve cache fields.
///
/// Enforces `max_blob_size_mb < cache_size_mb`: a single blob equal to or
/// larger than the cache would saturate the store on one fetch and evict
/// every other entry, making the node a one-shot download target rather
/// than a useful cache. Equality is rejected along with the greater-than
/// case because a cache that can hold exactly one blob has the same
/// failure mode as one that overflows.
#[cfg(test)]
fn resolve_cache(
    cli: &crate::cli::run::CacheArgs,
    file: Option<&types::CacheConfig>,
    data_dir: &std::path::Path,
) -> anyhow::Result<ResolvedCache> {
    one_section(|bag| resolve_cache_into(cli, file, data_dir, bag))
}

fn resolve_cache_into(
    cli: &crate::cli::run::CacheArgs,
    file: Option<&types::CacheConfig>,
    data_dir: &std::path::Path,
    bag: &mut ConfigErrorBag,
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

    let max_blob_size_mb = cli
        .max_blob_size_mb
        .or_else(|| file.and_then(|c| c.max_blob_size_mb))
        .unwrap_or(DEFAULT_MAX_BLOB_SIZE_MB);

    bag.check_with(
        max_blob_size_mb < cache_size_mb,
        "cache.max_blob_size_mb",
        || {
            format!(
                "cache.max_blob_size_mb ({max_blob_size_mb}) must be strictly less than \
             cache.cache_size_mb ({cache_size_mb}); otherwise a single oversized blob \
             can saturate the cache on one fetch"
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

    ResolvedCache {
        cache_dir,
        cache_size_mb,
        max_blob_size_mb,
        origins,
        pinned_hashes,
        origin_retry,
        user_agent,
        gc_interval_sec,
        max_probe_holds,
        stake_lane_reserved_holds,
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
/// Duplicate entries (same kind + identity key) are permitted with a
/// `tracing::warn!` log. Two HTTP origins pointing at the same URL is
/// legitimate for connection-pool sharding, but is more often a
/// copy-paste mistake worth flagging in the startup log.
fn resolve_origins_into(
    file: Option<&types::CacheConfig>,
    bag: &mut ConfigErrorBag,
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
            warn_on_duplicate_origins(&resolved);
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
fn warn_on_duplicate_origins(origins: &[crate::config::ResolvedOrigin]) {
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(origins.len());
    for (idx, origin) in origins.iter().enumerate() {
        let key = origin_identity_key(origin);
        if !seen.insert(key.clone()) {
            tracing::warn!(
                origin_index = idx,
                identity = %key,
                "cache.origins[{idx}] duplicates an earlier entry — \
                 the cache engine will dispatch the same backend twice \
                 in the fallback chain (intentional for connection-pool \
                 sharding, otherwise a likely copy-paste mistake)",
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
/// AWS-permissive choices we deliberately accept (but stricter
/// frontends like virtual-hosted-style URLs may reject): names
/// shorter than 3 chars are rejected (per AWS rule), but `xn--`
/// prefix and `--ol-s3` suffix are not rejected here — they're
/// reserved by AWS to never be assigned and the operator-typo case
/// is rare enough not to warrant the extra code.
fn validate_s3_bucket_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!name.is_empty(), "bucket name must not be empty");
    let len = name.len();
    anyhow::ensure!(
        (3..=63).contains(&len),
        "bucket name must be 3..=63 chars (got {len})"
    );
    // AWS: "Bucket names must begin and end with a letter or number"
    // — i.e. no leading/trailing `.` or `-`. The dot rule used to be
    // the only one here; the hyphen rule was missed in the first
    // pass. Names like `-foo` or `foo-` would parse but fail
    // virtual-hosted-style URLs at request time.
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
    use std::str::FromStr;

    let mut out = std::collections::HashSet::new();
    let Some(entries) = raw else {
        return Ok(decdn_config_types::PinnedHashes::empty());
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
            "cache.pinned_hashes[{idx}] must be 64 hex chars (BLAKE3); got {} chars",
            trimmed.len()
        );
        anyhow::ensure!(
            trimmed.chars().all(|c| c.is_ascii_hexdigit())
                && !trimmed.chars().any(|c| c.is_ascii_uppercase()),
            "cache.pinned_hashes[{idx}] must be lowercase hex (0-9, a-f)"
        );
        let parsed = decdn_config_types::Hash::from_str(trimmed).with_context(|| {
            format!("cache.pinned_hashes[{idx}] failed to parse as a BLAKE3 hash")
        })?;
        out.insert(parsed);
    }
    Ok(decdn_config_types::PinnedHashes::new(out))
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
pub fn resolve_payment(
    cli: &crate::cli::run::PaymentArgs,
    file: Option<&types::PaymentConfig>,
) -> anyhow::Result<ResolvedPayment> {
    one_section(|bag| resolve_payment_into(cli, file, bag))
}

/// Bag-threading variant of [`resolve_payment`]. Used by both
/// [`resolve_config`] (single bag across every section at startup) and the
/// SIGHUP hot-reload path in `runtime::reload` (single bag across every
/// reloadable section), so an operator sees every problem in one error
/// instead of fixing them one SIGHUP at a time. Always returns a
/// [`ResolvedPayment`] (with placeholder values for fields that failed
/// validation) so later checks can still run against it.
pub fn resolve_payment_into(
    cli: &crate::cli::run::PaymentArgs,
    file: Option<&types::PaymentConfig>,
    bag: &mut ConfigErrorBag,
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
    // Locally enforced stand-in for the on-chain `getRateBounds()` (ADR 005
    // §Rate bounds validation). Defaults (`0` .. `MAX_RATE_PER_MB`) make the
    // clamp a no-op so existing deployments see no behavior change.
    let delivery_floor = cli
        .delivery_floor
        .or_else(|| file.and_then(|p| p.delivery_floor))
        .unwrap_or(0);
    let delivery_ceiling = cli
        .delivery_ceiling
        .or_else(|| file.and_then(|p| p.delivery_ceiling))
        .unwrap_or(decdn_protocol::MAX_RATE_PER_MB);
    bag.check_with(
        delivery_floor <= delivery_ceiling,
        "payment.delivery_floor",
        || {
            format!(
                "payment.delivery_floor ({delivery_floor}) must be <= \
             payment.delivery_ceiling ({delivery_ceiling})"
            )
        },
    );
    // A ceiling of 0 would clamp every quoted rate to 0, bypassing the
    // `rate_per_mb > 0` guard above and making the node advertise a
    // free/selection-winning rate (ADR 001). With ceiling >= 1 and the
    // validated `rate_per_mb >= 1`, `clamp(rate, floor, ceiling)` is always
    // >= 1, so the signed rate can never collapse to 0.
    bag.check(
        delivery_ceiling >= 1,
        "payment.delivery_ceiling",
        "payment.delivery_ceiling must be >= 1 (clamping to 0 would sign a \
         free rate and bypass the rate_per_mb > 0 guard, ADR 001)",
    );
    bag.check_with(
        delivery_ceiling <= decdn_protocol::MAX_RATE_PER_MB,
        "payment.delivery_ceiling",
        || {
            format!(
                "payment.delivery_ceiling {delivery_ceiling} exceeds protocol \
             MAX_RATE_PER_MB ({}); clamping to it could still emit a rate honest \
             clients reject",
                decdn_protocol::MAX_RATE_PER_MB,
            )
        },
    );
    // Voucher cadence advertised in `StreamResponse` (ADR 003 §Voucher Interval
    // Negotiation). Default 1 MB; governable range 1..=1024. File-only (no CLI
    // override) — it is read once at handler construction, not hot-reloadable.
    let voucher_interval_mb = file
        .and_then(|p| p.voucher_interval_mb)
        .unwrap_or(decdn_protocol::DEFAULT_VOUCHER_INTERVAL_MB);
    // Lower bound is the literal minimum cadence (1 MB), not the default const:
    // a future change to DEFAULT_VOUCHER_INTERVAL_MB must not narrow the valid
    // governable range (ADR 003 §Voucher Interval Negotiation: 1..=1024).
    bag.check_with(
        (1..=decdn_protocol::MAX_VOUCHER_INTERVAL_MB).contains(&voucher_interval_mb),
        "payment.voucher_interval_mb",
        || {
            format!(
                "payment.voucher_interval_mb {voucher_interval_mb} out of range \
                 [1, {}] (ADR 003 §Voucher Interval Negotiation)",
                decdn_protocol::MAX_VOUCHER_INTERVAL_MB,
            )
        },
    );
    ResolvedPayment {
        rate_per_mb,
        delivery_floor,
        delivery_ceiling,
        voucher_interval_mb,
    }
}

/// Resolve observability fields.
///
/// The admin port is merged with `0` as a first-class "disable" value so
/// operators can turn the surface off without removing the line from their
/// config. Cross-port collision checks (bind/metrics/admin) live in
/// `validate_port_layout`, which sees all three sections at once — see
/// there for the full ruleset.
pub fn resolve_observability(
    cli: &crate::cli::run::ObservabilityArgs,
    file: Option<&types::ObservabilityConfig>,
) -> anyhow::Result<ResolvedObservability> {
    one_section(|bag| resolve_observability_into(cli, file, bag))
}

/// Bag-threading variant of [`resolve_observability`]. Shares a bag with
/// other sections during startup ([`resolve_config`]) and SIGHUP reload
/// (`runtime::reload`); see [`resolve_payment_into`] for the rationale.
pub fn resolve_observability_into(
    cli: &crate::cli::run::ObservabilityArgs,
    file: Option<&types::ObservabilityConfig>,
    bag: &mut ConfigErrorBag,
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
        let lower = ep.to_ascii_lowercase();
        bag.check_with(
            lower.starts_with("http://") || lower.starts_with("https://"),
            "observability.otlp_endpoint",
            || {
                format!(
                    "observability.otlp_endpoint must start with http:// or https:// \
                 (got {ep:?}); gRPC/OTLP collectors require an HTTP-scheme URL"
                )
            },
        );
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

/// Resolve gossip fields. Every allowlist entry is parsed as a 64-character
/// hex node ID (either case accepted); each malformed entry is dropped from
/// the returned list and recorded under `gossip.allowlist[<idx>]` (so an
/// operator sees every bad ID at once), and resolution still fails at startup
/// before the partial allowlist is used — never silently degrading to
/// accept-all mode later.
#[cfg(test)]
fn resolve_gossip(file: Option<&types::GossipConfig>) -> anyhow::Result<ResolvedGossip> {
    one_section(|bag| resolve_gossip_into(file, bag))
}

fn resolve_gossip_into(
    file: Option<&types::GossipConfig>,
    bag: &mut ConfigErrorBag,
) -> ResolvedGossip {
    let announce_interval_sec = file
        .and_then(|g| g.announce_interval_sec)
        .unwrap_or(DEFAULT_ANNOUNCE_INTERVAL_SEC);
    bag.check(
        announce_interval_sec > 0,
        "gossip.announce_interval_sec",
        "gossip.announce_interval_sec must be > 0",
    );

    let peer_ttl_sec = file
        .and_then(|g| g.peer_ttl_sec)
        .unwrap_or(DEFAULT_PEER_TTL_SEC);
    bag.check(
        peer_ttl_sec > 0,
        "gossip.peer_ttl_sec",
        "gossip.peer_ttl_sec must be > 0",
    );

    let max_peer_table_entries = file
        .and_then(|g| g.max_peer_table_entries)
        .unwrap_or(DEFAULT_MAX_PEER_TABLE_ENTRIES);
    bag.check(
        max_peer_table_entries > 0,
        "gossip.max_peer_table_entries",
        "gossip.max_peer_table_entries must be > 0",
    );

    let subscribe_global = file.and_then(|g| g.subscribe_global).unwrap_or(true);

    let allowlist = file
        .and_then(|g| g.allowlist.as_ref())
        .map(|v| {
            v.iter()
                .enumerate()
                .filter_map(|(idx, s)| {
                    bag.try_with(format!("gossip.allowlist[{idx}]"), parse_node_id_hex(s))
                })
                .collect()
        })
        .unwrap_or_default();

    ResolvedGossip {
        announce_interval_sec,
        peer_ttl_sec,
        subscribe_global,
        allowlist,
        max_peer_table_entries,
    }
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
    bag: &mut ConfigErrorBag,
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
pub fn resolve_security(file: Option<&types::SecurityConfig>) -> anyhow::Result<ResolvedSecurity> {
    one_section(|bag| resolve_security_into(file, bag))
}

/// Bag-threading variant of [`resolve_security`]. Shares a bag with other
/// sections during startup ([`resolve_config`]) and SIGHUP reload
/// (`runtime::reload`); see [`resolve_payment_into`] for the rationale.
#[allow(clippy::cognitive_complexity)]
pub fn resolve_security_into(
    file: Option<&types::SecurityConfig>,
    bag: &mut ConfigErrorBag,
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

    // `eprintln!` not `tracing::{info,warn}!`: tracing is not initialized
    // at `resolve_config` time (see `commands::run` and the rationale on
    // `validate_port_layout_into`).
    if max_concurrent_handlers == 0 {
        eprintln!("info: security.max_concurrent_handlers = 0: global concurrency cap disabled");
    }
    if per_source_rate_per_sec == 0.0 {
        eprintln!("info: security.per_source_rate_per_sec = 0: per-source rate-limit disabled");
    }
    if max_tracked_sources == 0 {
        eprintln!(
            "warning: security.max_tracked_sources = 0: rate-limit bookkeeping map is unbounded; \
             an attacker churning sources can grow it without limit"
        );
    }

    ResolvedSecurity {
        max_concurrent_handlers,
        per_source_rate_per_sec,
        per_source_burst,
        max_tracked_sources,
    }
}

/// Resolve `cdn/dht/v1` rate-limit settings (ADR 022 §DHT Rate Limiting).
///
/// Each `*_rate_per_sec == 0.0` paired with `*_burst == 0` disables that
/// layer (operator opt-out). Mixing `rate > 0` with `burst == 0` is
/// rejected as a deny-all corner case — the resolver treats it the same
/// way [`resolve_security_into`] handles the `per_source` pairing.
///
/// Trusted IPs are parsed once at resolution; malformed entries fail
/// fast under the bag pattern.
#[allow(clippy::cognitive_complexity)] // linear "default-or-file → validate" rows.
pub fn resolve_dht_into(file: Option<&types::DhtConfig>, bag: &mut ConfigErrorBag) -> ResolvedDht {
    // ADR 022 nests the rate-limit knobs under `dht.rate_limit.*` (see
    // §Trusted-IP exemption — "Configuration key: dht.rate_limit.trusted_ips").
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

    let trusted_ips = parse_trusted_ips(rate_limit.and_then(|r| r.trusted_ips.as_deref()), bag);

    let max_tracked_per_ip = rate_limit
        .and_then(|r| r.max_tracked_per_ip)
        .unwrap_or(DEFAULT_DHT_MAX_TRACKED_PER_IP);
    let max_tracked_per_peer = rate_limit
        .and_then(|r| r.max_tracked_per_peer)
        .unwrap_or(DEFAULT_DHT_MAX_TRACKED_PER_PEER);
    // `eprintln!` not `tracing::warn!`: tracing is not initialized at
    // `resolve_config` time (see `commands::run` and the rationale on
    // `validate_port_layout_into`).
    if max_tracked_per_ip == 0 {
        eprintln!(
            "warning: dht.rate_limit.max_tracked_per_ip = 0: per-IP bookkeeping map is unbounded; \
             an attacker churning source IPs can grow it without limit"
        );
    }
    if max_tracked_per_peer == 0 {
        eprintln!(
            "warning: dht.rate_limit.max_tracked_per_peer = 0: per-peer bookkeeping map is unbounded; \
             an attacker churning NodeIds can grow it without limit"
        );
    }

    ResolvedDht {
        per_peer_rate_per_sec,
        per_peer_burst,
        per_ip_rate_per_sec,
        per_ip_burst,
        global_rate_per_sec,
        global_burst,
        trusted_ips,
        max_tracked_per_ip,
        max_tracked_per_peer,
    }
}

/// Convenience wrapper for [`resolve_dht_into`] that takes a fresh
/// `ConfigErrorBag`. Test-only.
#[cfg(test)]
pub fn resolve_dht(file: Option<&types::DhtConfig>) -> anyhow::Result<ResolvedDht> {
    one_section(|bag| resolve_dht_into(file, bag))
}

fn parse_trusted_ips(
    raw: Option<&[String]>,
    bag: &mut ConfigErrorBag,
) -> std::collections::HashSet<std::net::IpAddr> {
    let mut out = std::collections::HashSet::new();
    let Some(entries) = raw else {
        return out;
    };
    for entry in entries {
        match entry.parse::<std::net::IpAddr>() {
            Ok(ip) => {
                out.insert(ip);
            }
            Err(e) => {
                bag.check_with(false, "dht.rate_limit.trusted_ips", || {
                    format!(
                        "dht.rate_limit.trusted_ips entry {entry:?} is not a valid IP address: {e}"
                    )
                });
            }
        }
    }
    out
}

/// Parse a 64-character hex (case-insensitive) node ID into 32 raw bytes.
///
/// The error message is field-agnostic — callers (currently
/// `gossip.allowlist`) wrap the result with the field path of the
/// offending entry.
fn parse_node_id_hex(s: &str) -> anyhow::Result<[u8; 32]> {
    anyhow::ensure!(
        s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()),
        "must be 64 hex chars, got {s:?}"
    );
    let mut out = [0u8; 32];
    let bytes = s.as_bytes();
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = bytes
            .get(i * 2)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let lo = bytes
            .get(i * 2 + 1)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        *slot = (hex_val(hi)? << 4) | hex_val(lo)?;
    }
    Ok(out)
}

fn hex_val(b: u8) -> anyhow::Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => anyhow::bail!("invalid hex digit: {}", b as char),
    }
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
        expand_str(&mut n.relay_url, "network.relay_url")?;
    }
    if let Some(b) = cfg.blockchain.as_mut() {
        expand_str(&mut b.rpc_url, "blockchain.rpc_url")?;
        expand_path(&mut b.eth_keystore, "blockchain.eth_keystore")?;
        expand_str(
            &mut b.payment_channel_address,
            "blockchain.payment_channel_address",
        )?;
        expand_str(
            &mut b.capacity_bond_address,
            "blockchain.capacity_bond_address",
        )?;
        expand_str(&mut b.slash_judge_address, "blockchain.slash_judge_address")?;
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
mod tests {
    use super::*;
    use crate::cli::run::BlockchainArgs;
    use tempfile::TempDir;

    #[test]
    fn normalize_region_accepts_and_uppercases() -> anyhow::Result<()> {
        assert_eq!(normalize_region("US")?, "US");
        assert_eq!(normalize_region("us")?, "US");
        assert_eq!(normalize_region("Us")?, "US");
        Ok(())
    }

    #[test]
    fn normalize_region_rejects_wrong_length_or_charset() {
        // Length / charset failures and unassigned codes. Adversarial
        // inputs that the wire layer also rejects are pinned here too so
        // a config-resolver regression can't shift the only effective
        // check onto the receive path.
        let bad_inputs = [
            "usa", "u1", "", "U", "U S", "Ü1", "12", "U-", "OO", "JJ", "BX", "U/", "U\0",
        ];
        for bad in bad_inputs {
            assert!(
                normalize_region(bad).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn normalize_region_accepts_reserved_codes() {
        // Reserved-for-user-assignment codes (ISO 3166-1 §8.1.3) must
        // round-trip unchanged so air-gapped / testnet operators can use
        // them. Spot-check each range.
        for code in ["AA", "QM", "QZ", "XA", "XK", "XZ", "ZZ"] {
            assert_eq!(
                normalize_region(code).expect("accepted"),
                code,
                "{code} should round-trip"
            );
        }
    }

    #[test]
    fn parse_node_id_hex_accepts_either_case() -> anyhow::Result<()> {
        let lower = "0".repeat(64);
        let upper = "A".repeat(64);
        let mixed: String = "Aa".repeat(32);
        assert_eq!(parse_node_id_hex(&lower)?, [0u8; 32]);
        assert_eq!(parse_node_id_hex(&upper)?, [0xAA; 32]);
        assert_eq!(parse_node_id_hex(&mixed)?, [0xAA; 32]);
        Ok(())
    }

    #[test]
    fn parse_node_id_hex_round_trips_nibble_order() -> anyhow::Result<()> {
        let hex = "0123456789abcdef".repeat(4);
        let bytes = parse_node_id_hex(&hex)?;
        // First byte should be 0x01 — high nibble from '0', low from '1'.
        assert_eq!(bytes[0], 0x01);
        assert_eq!(bytes[1], 0x23);
        assert_eq!(bytes[31], 0xef);
        Ok(())
    }

    #[test]
    fn parse_node_id_hex_rejects_bad_input() {
        assert!(parse_node_id_hex(&"0".repeat(63)).is_err());
        assert!(parse_node_id_hex(&"0".repeat(65)).is_err());
        assert!(parse_node_id_hex(&"g".repeat(64)).is_err());
        assert!(parse_node_id_hex("").is_err());
    }

    // vitalik.eth, known-good EIP-55 checksum.
    const GOOD_ADDR: &str = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
    const ALT_ADDR_1: &str = "0x0000000000000000000000000000000000000001";
    const ALT_ADDR_2: &str = "0x0000000000000000000000000000000000000002";
    const ALT_ADDR_3: &str = "0x0000000000000000000000000000000000000003";

    #[test]
    fn parse_contract_address_accepts_checksummed() -> anyhow::Result<()> {
        let out = parse_contract_address("x", GOOD_ADDR)?;
        assert_eq!(out, GOOD_ADDR);
        Ok(())
    }

    #[test]
    fn parse_contract_address_trims_whitespace() -> anyhow::Result<()> {
        let padded = format!("  {GOOD_ADDR}\n");
        let out = parse_contract_address("x", &padded)?;
        assert_eq!(out, GOOD_ADDR);
        Ok(())
    }

    #[test]
    fn parse_contract_address_rejects_missing_0x_prefix() -> anyhow::Result<()> {
        let s = GOOD_ADDR
            .get(2..)
            .ok_or_else(|| anyhow::anyhow!("GOOD_ADDR shorter than expected"))?;
        assert!(parse_contract_address("x", s).is_err());
        Ok(())
    }

    #[test]
    fn parse_contract_address_rejects_wrong_length() {
        assert!(parse_contract_address("x", "0xabc").is_err());
        assert!(parse_contract_address("x", &format!("{GOOD_ADDR}00")).is_err());
    }

    #[test]
    fn parse_contract_address_rejects_empty_and_bare_prefix() {
        assert!(parse_contract_address("x", "").is_err());
        assert!(parse_contract_address("x", "0x").is_err());
    }

    #[test]
    fn parse_contract_address_rejects_all_lowercase() {
        let lower = GOOD_ADDR.to_lowercase();
        assert!(parse_contract_address("x", &lower).is_err());
    }

    #[test]
    fn parse_contract_address_rejects_bad_checksum() {
        let mut bad = String::from(GOOD_ADDR);
        // Flip the case of the first hex digit so the EIP-55 checksum no longer matches.
        bad.replace_range(2..3, "D");
        assert!(parse_contract_address("x", &bad).is_err());
    }

    #[test]
    fn parse_contract_address_rejects_non_hex() {
        let bad = "0xZZZZ6BF26964aF9D7eEd9e03E53415D37aA96045";
        assert!(parse_contract_address("x", bad).is_err());
    }

    #[test]
    fn parse_contract_address_error_names_field_and_format() -> anyhow::Result<()> {
        let lower = GOOD_ADDR.to_lowercase();
        let Err(err) = parse_contract_address("payment_channel_address", &lower) else {
            anyhow::bail!("expected parse_contract_address to fail on lowercase input");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("invalid payment_channel_address"),
            "missing flag name: {msg}"
        );
        assert!(msg.contains("EIP-55"), "missing format hint: {msg}");
        Ok(())
    }

    #[test]
    fn resolve_gossip_allowlist_happy_path() -> anyhow::Result<()> {
        // "0123456789abcdef" repeated 4× = 64 hex chars.
        // Decodes pairwise to 8 bytes (01 23 45 67 89 ab cd ef), repeated 4×.
        let cfg = types::GossipConfig {
            allowlist: Some(vec!["0123456789abcdef".repeat(4)]),
            ..Default::default()
        };
        let g = resolve_gossip(Some(&cfg))?;
        let pattern = [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef];
        let expected: [u8; 32] = std::array::from_fn(|i| pattern[i % 8]);
        assert!(g.allowlist.contains(&expected));
        assert_eq!(g.allowlist.len(), 1);
        Ok(())
    }

    fn ident(region: Option<&str>) -> ResolvedIdentity {
        ResolvedIdentity {
            data_dir: PathBuf::from("/tmp/unused"),
            region: region.map(String::from),
        }
    }

    fn gossip_cfg(subscribe_global: bool) -> ResolvedGossip {
        ResolvedGossip {
            announce_interval_sec: 60,
            peer_ttl_sec: 600,
            subscribe_global,
            allowlist: Vec::new(),
            max_peer_table_entries: DEFAULT_MAX_PEER_TABLE_ENTRIES,
        }
    }

    #[test]
    fn ensure_region_when_publishing_global_rejects_missing_region() {
        let err = ensure_region_when_publishing_global(&ident(None), &gossip_cfg(true))
            .expect_err("expected error when global is on but region is absent")
            .to_string();
        assert!(
            err.contains("identity.region"),
            "error missing field context: {err}"
        );
    }

    #[test]
    fn ensure_region_when_publishing_global_accepts_region_set() -> anyhow::Result<()> {
        ensure_region_when_publishing_global(&ident(Some("US")), &gossip_cfg(true))?;
        Ok(())
    }

    #[test]
    fn ensure_region_when_publishing_global_accepts_subscribe_only() -> anyhow::Result<()> {
        // `subscribe_global = false` + no region = subscribe-only noop; fine.
        ensure_region_when_publishing_global(&ident(None), &gossip_cfg(false))?;
        Ok(())
    }

    #[test]
    fn resolve_gossip_applies_positive_values() -> anyhow::Result<()> {
        let cfg = types::GossipConfig {
            announce_interval_sec: Some(42),
            peer_ttl_sec: Some(123),
            subscribe_global: Some(false),
            allowlist: None,
            max_peer_table_entries: Some(7),
        };
        let g = resolve_gossip(Some(&cfg))?;
        assert_eq!(g.announce_interval_sec, 42);
        assert_eq!(g.peer_ttl_sec, 123);
        assert!(!g.subscribe_global);
        assert!(g.allowlist.is_empty());
        assert_eq!(g.max_peer_table_entries, 7);
        Ok(())
    }

    #[test]
    fn resolve_gossip_applies_defaults_when_absent() -> anyhow::Result<()> {
        let g = resolve_gossip(None)?;
        assert_eq!(g.announce_interval_sec, DEFAULT_ANNOUNCE_INTERVAL_SEC);
        assert_eq!(g.peer_ttl_sec, DEFAULT_PEER_TTL_SEC);
        assert!(g.subscribe_global);
        assert!(g.allowlist.is_empty());
        assert_eq!(g.max_peer_table_entries, DEFAULT_MAX_PEER_TABLE_ENTRIES);
        Ok(())
    }

    #[test]
    fn resolve_receipts_applies_defaults_when_absent() -> anyhow::Result<()> {
        let r = resolve_receipts(None)?;
        assert_eq!(r.max_file_bytes, DEFAULT_RECEIPT_MAX_FILE_BYTES);
        assert_eq!(r.retained_files, DEFAULT_RECEIPT_RETAINED_FILES);
        Ok(())
    }

    #[test]
    fn resolve_receipts_applies_explicit_values() -> anyhow::Result<()> {
        let cfg = types::ReceiptsConfig {
            max_file_bytes: Some(8 << 20),
            retained_files: Some(0),
        };
        let r = resolve_receipts(Some(&cfg))?;
        assert_eq!(r.max_file_bytes, 8 << 20);
        assert_eq!(r.retained_files, 0);
        Ok(())
    }

    #[test]
    fn resolve_receipts_accepts_boundary_values() -> anyhow::Result<()> {
        for bytes in [MIN_RECEIPT_MAX_FILE_BYTES, MAX_RECEIPT_MAX_FILE_BYTES] {
            let cfg = types::ReceiptsConfig {
                max_file_bytes: Some(bytes),
                retained_files: Some(MAX_RECEIPT_RETAINED_FILES),
            };
            let r = resolve_receipts(Some(&cfg))?;
            assert_eq!(r.max_file_bytes, bytes);
            assert_eq!(r.retained_files, MAX_RECEIPT_RETAINED_FILES);
        }
        Ok(())
    }

    #[test]
    fn resolve_receipts_rejects_max_file_bytes_below_floor() {
        let cfg = types::ReceiptsConfig {
            max_file_bytes: Some(MIN_RECEIPT_MAX_FILE_BYTES - 1),
            retained_files: None,
        };
        let err = resolve_receipts(Some(&cfg))
            .expect_err("expected error")
            .to_string();
        assert!(
            err.contains("receipts.max_file_bytes"),
            "error missing field context: {err}"
        );
    }

    #[test]
    fn resolve_receipts_rejects_max_file_bytes_above_ceiling() {
        let cfg = types::ReceiptsConfig {
            max_file_bytes: Some(MAX_RECEIPT_MAX_FILE_BYTES + 1),
            retained_files: None,
        };
        let err = resolve_receipts(Some(&cfg))
            .expect_err("expected error")
            .to_string();
        assert!(
            err.contains("receipts.max_file_bytes"),
            "error missing field context: {err}"
        );
    }

    #[test]
    fn resolve_receipts_rejects_retained_files_above_cap() {
        let cfg = types::ReceiptsConfig {
            max_file_bytes: None,
            retained_files: Some(MAX_RECEIPT_RETAINED_FILES + 1),
        };
        let err = resolve_receipts(Some(&cfg))
            .expect_err("expected error")
            .to_string();
        assert!(
            err.contains("receipts.retained_files"),
            "error missing field context: {err}"
        );
    }

    #[test]
    fn resolve_gossip_rejects_zero_max_peer_table_entries() {
        let cfg = types::GossipConfig {
            max_peer_table_entries: Some(0),
            ..Default::default()
        };
        let err = resolve_gossip(Some(&cfg))
            .expect_err("expected error")
            .to_string();
        assert!(
            err.contains("max_peer_table_entries"),
            "error missing field context: {err}"
        );
    }

    #[test]
    fn resolve_gossip_max_peer_table_entries_file_override() -> anyhow::Result<()> {
        let cfg = types::GossipConfig {
            max_peer_table_entries: Some(42_000),
            ..Default::default()
        };
        let g = resolve_gossip(Some(&cfg))?;
        assert_eq!(g.max_peer_table_entries, 42_000);
        Ok(())
    }

    #[test]
    fn resolve_gossip_max_peer_table_entries_default_when_field_absent() -> anyhow::Result<()> {
        let cfg = types::GossipConfig {
            announce_interval_sec: Some(30),
            ..Default::default()
        };
        let g = resolve_gossip(Some(&cfg))?;
        assert_eq!(g.max_peer_table_entries, DEFAULT_MAX_PEER_TABLE_ENTRIES);
        Ok(())
    }

    #[test]
    fn resolve_gossip_rejects_zero_announce_interval() {
        let cfg = types::GossipConfig {
            announce_interval_sec: Some(0),
            ..Default::default()
        };
        let err = resolve_gossip(Some(&cfg))
            .expect_err("expected error")
            .to_string();
        assert!(
            err.contains("announce_interval_sec"),
            "error missing field context: {err}"
        );
    }

    #[test]
    fn resolve_gossip_rejects_zero_peer_ttl() {
        let cfg = types::GossipConfig {
            peer_ttl_sec: Some(0),
            ..Default::default()
        };
        let err = resolve_gossip(Some(&cfg))
            .expect_err("expected error")
            .to_string();
        assert!(
            err.contains("peer_ttl_sec"),
            "error missing field context: {err}"
        );
    }

    #[test]
    fn resolve_gossip_rejects_bad_allowlist_entry() {
        let cfg = types::GossipConfig {
            allowlist: Some(vec!["0".repeat(63)]),
            ..Default::default()
        };
        let err = resolve_gossip(Some(&cfg))
            .expect_err("expected error")
            .to_string();
        assert!(
            err.contains("64 hex chars"),
            "error missing field context: {err}"
        );
    }

    // Per-field merge coverage for `[gossip]` (#434). The companion to
    // `resolve_security` per-field tests above. Each field gets a
    // file-leg "override" test and a "default when absent" test, with
    // the allowlist hex-validation error path covered alongside.

    #[test]
    fn resolve_gossip_announce_interval_file_override() -> anyhow::Result<()> {
        let cfg = types::GossipConfig {
            announce_interval_sec: Some(123),
            ..Default::default()
        };
        let g = resolve_gossip(Some(&cfg))?;
        assert_eq!(g.announce_interval_sec, 123);
        Ok(())
    }

    #[test]
    fn resolve_gossip_announce_interval_default_when_field_absent() -> anyhow::Result<()> {
        // Other field populated, this one absent — proves the file
        // leg's `unwrap_or` arm fires for this field independently.
        let cfg = types::GossipConfig {
            peer_ttl_sec: Some(900),
            ..Default::default()
        };
        let g = resolve_gossip(Some(&cfg))?;
        assert_eq!(g.announce_interval_sec, DEFAULT_ANNOUNCE_INTERVAL_SEC);
        Ok(())
    }

    #[test]
    fn resolve_gossip_peer_ttl_file_override() -> anyhow::Result<()> {
        let cfg = types::GossipConfig {
            peer_ttl_sec: Some(1234),
            ..Default::default()
        };
        let g = resolve_gossip(Some(&cfg))?;
        assert_eq!(g.peer_ttl_sec, 1234);
        Ok(())
    }

    #[test]
    fn resolve_gossip_peer_ttl_default_when_field_absent() -> anyhow::Result<()> {
        let cfg = types::GossipConfig {
            announce_interval_sec: Some(30),
            ..Default::default()
        };
        let g = resolve_gossip(Some(&cfg))?;
        assert_eq!(g.peer_ttl_sec, DEFAULT_PEER_TTL_SEC);
        Ok(())
    }

    #[test]
    fn resolve_gossip_subscribe_global_file_override_to_false() -> anyhow::Result<()> {
        // Default is `true` (see `resolve_gossip`); the override path
        // is the operator-actionable case (turning off global pub/sub).
        let cfg = types::GossipConfig {
            subscribe_global: Some(false),
            ..Default::default()
        };
        let g = resolve_gossip(Some(&cfg))?;
        assert!(!g.subscribe_global);
        Ok(())
    }

    #[test]
    fn resolve_gossip_subscribe_global_default_when_field_absent_is_true() -> anyhow::Result<()> {
        // `None` => `true` per `resolve_gossip`. Distinct from "field
        // explicitly set to true" (also true), but documents the
        // omitted-field default for an absent TOML key.
        let cfg = types::GossipConfig::default();
        let g = resolve_gossip(Some(&cfg))?;
        assert!(g.subscribe_global);
        Ok(())
    }

    /// Allowlist hex validation: non-hex characters in an otherwise
    /// 64-char entry must be rejected. Sibling to the existing
    /// `resolve_gossip_rejects_bad_allowlist_entry` (wrong length)
    /// test — together they cover the two failure modes
    /// `parse_node_id_hex` raises on the allowlist path so a regression
    /// in either won't slip past CI silently (#434).
    #[test]
    fn resolve_gossip_rejects_non_hex_allowlist_entry() {
        let cfg = types::GossipConfig {
            allowlist: Some(vec!["g".repeat(64)]),
            ..Default::default()
        };
        let err = resolve_gossip(Some(&cfg))
            .expect_err("non-hex entry should be rejected")
            .to_string();
        assert!(
            err.contains("64 hex chars") || err.contains("hex"),
            "error missing hex-context: {err}"
        );
    }

    /// Every malformed allowlist entry is dropped from the returned vec
    /// *and* recorded under its own `gossip.allowlist[<idx>]` label, so
    /// resolution fails at startup before the partial list is used (never
    /// silently degrading to accept-all). Two invalid entries at indices
    /// 0 and 2 (valid hex at 1) must both surface — a regression to
    /// fail-fast on the first bad entry would only report one.
    #[test]
    fn resolve_gossip_accumulates_every_invalid_allowlist_entry() {
        let cfg = types::GossipConfig {
            allowlist: Some(vec![
                "z".repeat(64),               // idx 0: invalid (non-hex)
                "0123456789abcdef".repeat(4), // idx 1: valid
                "q".repeat(64),               // idx 2: invalid (non-hex)
            ]),
            ..Default::default()
        };
        let err = resolve_gossip(Some(&cfg))
            .expect_err("every invalid allowlist entry must be reported")
            .to_string();
        assert!(
            err.contains("gossip.allowlist[0]") && err.contains("gossip.allowlist[2]"),
            "both bad entries must be named by index: {err}"
        );
        assert!(
            !err.contains("gossip.allowlist[1]"),
            "the valid entry must not be reported: {err}"
        );
        assert!(
            err.contains("hex"),
            "error must surface hex-validation failure: {err}"
        );
    }

    /// Independent gossip field overrides: setting one field to a
    /// non-default value must leave the others at their defaults.
    /// Catches a regression that copy-pasted the wrong source field
    /// into a `unwrap_or(DEFAULT_*)` arm.
    #[test]
    fn resolve_gossip_field_overrides_are_independent() -> anyhow::Result<()> {
        let cfg = types::GossipConfig {
            announce_interval_sec: Some(7),
            ..Default::default()
        };
        let g = resolve_gossip(Some(&cfg))?;
        assert_eq!(g.announce_interval_sec, 7);
        assert_eq!(g.peer_ttl_sec, DEFAULT_PEER_TTL_SEC);
        assert!(g.subscribe_global);
        assert!(g.allowlist.is_empty());
        Ok(())
    }

    // HOME is guaranteed set in Rust test harness on Linux/macOS and used here
    // to exercise ${VAR} expansion without mutating the process environment
    // (std::env::set_var is `unsafe` in edition 2024, and workspace lints
    // forbid `unsafe_code`).
    fn home_str() -> anyhow::Result<String> {
        Ok(dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("test requires home dir"))?
            .to_string_lossy()
            .into_owned())
    }

    fn cfg_with_rpc(raw: &str) -> FileConfig {
        FileConfig {
            blockchain: Some(types::BlockchainConfig {
                rpc_url: Some(raw.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn expand_env_substitutes_string_field() -> anyhow::Result<()> {
        let home = home_str()?;
        let mut cfg = cfg_with_rpc("${HOME}/rpc");
        expand_env(&mut cfg)?;
        let url = cfg
            .blockchain
            .as_ref()
            .and_then(|b| b.rpc_url.as_deref())
            .ok_or_else(|| anyhow::anyhow!("rpc_url missing"))?;
        anyhow::ensure!(url == format!("{home}/rpc"), "got: {url}");
        Ok(())
    }

    #[test]
    fn expand_env_substitutes_slash_judge_address() -> anyhow::Result<()> {
        // Regression for the wiring line in `expand_env`: a `${VAR}` in
        // blockchain.slash_judge_address must be expanded before
        // `parse_contract_address`, like the other contract-address fields.
        let home = home_str()?;
        let mut cfg = FileConfig {
            blockchain: Some(types::BlockchainConfig {
                slash_judge_address: Some("${HOME}/judge".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let got = cfg
            .blockchain
            .as_ref()
            .and_then(|b| b.slash_judge_address.as_deref())
            .ok_or_else(|| anyhow::anyhow!("slash_judge_address missing"))?;
        anyhow::ensure!(got == format!("{home}/judge"), "got: {got}");
        Ok(())
    }

    #[test]
    fn expand_env_substitutes_path_field() -> anyhow::Result<()> {
        let home = home_str()?;
        let mut cfg = FileConfig {
            identity: Some(types::IdentityConfig {
                data_dir: Some(PathBuf::from("${HOME}/node")),
                region: None,
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let dd = cfg
            .identity
            .as_ref()
            .and_then(|i| i.data_dir.as_deref())
            .ok_or_else(|| anyhow::anyhow!("data_dir missing"))?
            .to_path_buf();
        let expected = format!("{home}/node");
        anyhow::ensure!(dd == Path::new(&expected), "got: {}", dd.display());
        Ok(())
    }

    #[test]
    fn expand_env_errors_on_missing_var_naming_field() -> anyhow::Result<()> {
        // Var name unlikely to exist; if it does, the test is meaningless —
        // skip loudly rather than producing a false pass.
        let missing = "DECDN_DEFINITELY_UNSET_VAR_QZX_223";
        anyhow::ensure!(
            std::env::var_os(missing).is_none(),
            "test precondition violated: {missing} is set in the environment"
        );
        let mut cfg = cfg_with_rpc(&format!("${{{missing}}}"));
        let err = expand_env(&mut cfg)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected expansion error"))?
            .to_string();
        anyhow::ensure!(
            err.contains("blockchain.rpc_url") && err.contains(missing),
            "error missing context, got: {err}"
        );
        Ok(())
    }

    #[test]
    fn expand_env_leaves_plain_values_untouched() -> anyhow::Result<()> {
        let mut cfg = cfg_with_rpc("https://plain.example");
        expand_env(&mut cfg)?;
        let url = cfg
            .blockchain
            .as_ref()
            .and_then(|b| b.rpc_url.as_deref())
            .ok_or_else(|| anyhow::anyhow!("rpc_url missing"))?;
        anyhow::ensure!(url == "https://plain.example", "got: {url}");
        Ok(())
    }

    // A literal `$` (e.g. in basic-auth passwords or query strings) must
    // pass through untouched; only the explicit `${VAR}` form triggers
    // expansion. Otherwise operators lose access to values containing `$`.
    #[test]
    fn expand_env_preserves_literal_dollar_without_braces() -> anyhow::Result<()> {
        let raw = "https://user:p$w0rd@host/path?token=abc$def";
        let mut cfg = cfg_with_rpc(raw);
        expand_env(&mut cfg)?;
        let url = cfg
            .blockchain
            .as_ref()
            .and_then(|b| b.rpc_url.as_deref())
            .ok_or_else(|| anyhow::anyhow!("rpc_url missing"))?;
        anyhow::ensure!(url == raw, "got: {url}");
        Ok(())
    }

    // Windows-style paths with backslashes must keep their backslashes and
    // still expand `${VAR}` — shell-style escape interpreters would swallow
    // `\` before `$` and disable the expansion on real Windows paths.
    #[test]
    fn expand_env_handles_backslash_before_brace() -> anyhow::Result<()> {
        let home = home_str()?;
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                cache_dir: Some(PathBuf::from(r"C:\data\${HOME}\cache")),
                cache_size_mb: None,
                max_blob_size_mb: None,
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let dir = cfg
            .cache
            .as_ref()
            .and_then(|c| c.cache_dir.as_deref())
            .ok_or_else(|| anyhow::anyhow!("cache_dir missing"))?
            .to_path_buf();
        let expected = PathBuf::from(format!(r"C:\data\{home}\cache"));
        anyhow::ensure!(dir == expected, "got: {}", dir.display());
        Ok(())
    }

    // Unterminated `${` should surface a clear error rather than silently
    // consume the rest of the string.
    #[test]
    fn expand_env_errors_on_unterminated_brace() -> anyhow::Result<()> {
        let mut cfg = cfg_with_rpc("https://${HOST/api");
        let err = expand_env(&mut cfg)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected error"))?
            .to_string();
        anyhow::ensure!(
            err.contains("blockchain.rpc_url") && err.contains("unterminated"),
            "got: {err}"
        );
        Ok(())
    }

    #[test]
    fn expand_env_expands_tilde_in_path_field() -> anyhow::Result<()> {
        let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("test requires home dir"))?;
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                cache_dir: Some(PathBuf::from("~/decdn-cache")),
                cache_size_mb: None,
                max_blob_size_mb: None,
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let dir = cfg
            .cache
            .as_ref()
            .and_then(|c| c.cache_dir.as_deref())
            .ok_or_else(|| anyhow::anyhow!("cache_dir missing"))?
            .to_path_buf();
        anyhow::ensure!(dir == home.join("decdn-cache"), "got: {}", dir.display());
        Ok(())
    }

    #[test]
    fn expand_env_substitutes_multiple_vars_in_one_value() -> anyhow::Result<()> {
        let home = home_str()?;
        let mut cfg = cfg_with_rpc("${HOME}/a/${HOME}/b");
        expand_env(&mut cfg)?;
        let url = cfg
            .blockchain
            .as_ref()
            .and_then(|b| b.rpc_url.as_deref())
            .ok_or_else(|| anyhow::anyhow!("rpc_url missing"))?;
        anyhow::ensure!(url == format!("{home}/a/{home}/b"), "got: {url}");
        Ok(())
    }

    #[test]
    fn expand_env_expands_bare_tilde_path() -> anyhow::Result<()> {
        let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("test requires home dir"))?;
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                cache_dir: Some(PathBuf::from("~")),
                cache_size_mb: None,
                max_blob_size_mb: None,
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let dir = cfg
            .cache
            .as_ref()
            .and_then(|c| c.cache_dir.as_deref())
            .ok_or_else(|| anyhow::anyhow!("cache_dir missing"))?
            .to_path_buf();
        anyhow::ensure!(dir == home, "got: {}", dir.display());
        Ok(())
    }

    type FieldSetter = fn(&mut FileConfig, &str);

    // Guards against copy-paste mislabeling in the 8-arm wiring of
    // `expand_env` — every expandable field must surface its own dotted
    // path in the error message.
    #[test]
    // Length crept past 100 lines after `..Default::default()` was
    // added to every CacheConfig literal in the field-setter table
    // (#312/#276 PR). The body is a flat list of cases — splitting
    // wouldn't compress information density.
    #[allow(clippy::too_many_lines)]
    fn expand_env_per_field_error_context() -> anyhow::Result<()> {
        let missing = "DECDN_UNSET_PER_FIELD_VAR_ZZZ";
        anyhow::ensure!(
            std::env::var_os(missing).is_none(),
            "test precondition violated: {missing} is set in the environment"
        );
        let placeholder = format!("${{{missing}}}");

        let cases: &[(&str, FieldSetter)] = &[
            ("identity.data_dir", |c, v| {
                c.identity = Some(types::IdentityConfig {
                    data_dir: Some(PathBuf::from(v)),
                    region: None,
                });
            }),
            ("identity.region", |c, v| {
                c.identity = Some(types::IdentityConfig {
                    data_dir: None,
                    region: Some(v.to_string()),
                });
            }),
            ("network.relay_url", |c, v| {
                c.network = Some(types::NetworkConfig {
                    bind_port: None,
                    relay_url: Some(v.to_string()),
                    enable_0rtt: None,
                });
            }),
            ("blockchain.rpc_url", |c, v| {
                c.blockchain = Some(types::BlockchainConfig {
                    rpc_url: Some(v.to_string()),
                    ..Default::default()
                });
            }),
            ("blockchain.eth_keystore", |c, v| {
                c.blockchain = Some(types::BlockchainConfig {
                    eth_keystore: Some(PathBuf::from(v)),
                    ..Default::default()
                });
            }),
            ("blockchain.payment_channel_address", |c, v| {
                c.blockchain = Some(types::BlockchainConfig {
                    payment_channel_address: Some(v.to_string()),
                    ..Default::default()
                });
            }),
            ("blockchain.capacity_bond_address", |c, v| {
                c.blockchain = Some(types::BlockchainConfig {
                    capacity_bond_address: Some(v.to_string()),
                    ..Default::default()
                });
            }),
            ("cache.cache_dir", |c, v| {
                c.cache = Some(types::CacheConfig {
                    cache_dir: Some(PathBuf::from(v)),
                    cache_size_mb: None,
                    max_blob_size_mb: None,
                    ..Default::default()
                });
            }),
            ("cache.origin.url", |c, v| {
                c.cache = Some(types::CacheConfig {
                    origin: Some(types::OriginConfig::Http {
                        url: v.to_string(),
                        decompress: None,
                    }),
                    ..Default::default()
                });
            }),
            ("cache.origin.path", |c, v| {
                c.cache = Some(types::CacheConfig {
                    origin: Some(types::OriginConfig::Fs {
                        path: PathBuf::from(v),
                    }),
                    ..Default::default()
                });
            }),
            ("cache.user_agent", |c, v| {
                c.cache = Some(types::CacheConfig {
                    user_agent: Some(v.to_string()),
                    ..Default::default()
                });
            }),
            ("observability.otlp_endpoint", |c, v| {
                c.observability = Some(types::ObservabilityConfig {
                    log_level: None,
                    log_format: None,
                    metrics_port: None,
                    metrics_bind: None,
                    admin_port: None,
                    otlp_endpoint: Some(v.to_string()),
                });
            }),
        ];

        for (expected_ctx, setter) in cases {
            let mut cfg = FileConfig::default();
            setter(&mut cfg, &placeholder);
            let err = expand_env(&mut cfg)
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected error for {expected_ctx}"))?
                .to_string();
            anyhow::ensure!(
                err.contains(expected_ctx),
                "field `{expected_ctx}` missing from error: {err}"
            );
        }
        Ok(())
    }

    // Same guard as `expand_env_substitutes_cache_origin_url`, for the
    // user_agent field. Operators commonly want to embed `${HOSTNAME}` or
    // a build-tag env var into the UA, and silently shipping the literal
    // `${...}` would be a confusing wire-level surprise.
    #[test]
    fn expand_env_substitutes_cache_user_agent() -> anyhow::Result<()> {
        let home = home_str()?;
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                user_agent: Some("decdn-${HOME}/test".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let ua = cfg
            .cache
            .as_ref()
            .and_then(|c| c.user_agent.as_deref())
            .ok_or_else(|| anyhow::anyhow!("user_agent missing"))?;
        let expected = format!("decdn-{home}/test");
        anyhow::ensure!(ua == expected, "got: {ua}");
        Ok(())
    }

    // Guards against the classic "added a field, forgot to wire expansion"
    // regression — the HTTP-origin URL is URL-shaped and must get the
    // same `${VAR}` treatment as sibling URL fields (rpc_url, relay_url,
    // etc).
    #[test]
    fn expand_env_substitutes_cache_origin_url() -> anyhow::Result<()> {
        let home = home_str()?;
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                origin: Some(types::OriginConfig::Http {
                    url: "https://origin.example/${HOME}/bucket".to_string(),
                    decompress: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let url = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
            Some(types::OriginConfig::Http { url, .. }) => url.clone(),
            other => anyhow::bail!("expected Http origin, got: {other:?}"),
        };
        let expected = format!("https://origin.example/{home}/bucket");
        anyhow::ensure!(url == expected, "got: {url}");
        Ok(())
    }

    // Sibling of `expand_env_substitutes_cache_origin_url` — the FS
    // origin path is a path-shaped field and must get the same
    // `${VAR}` treatment so an operator can write
    // `path = "${HOME}/cache-origin"` in their TOML and have it
    // resolve correctly.
    #[test]
    fn expand_env_substitutes_cache_origin_fs_path() -> anyhow::Result<()> {
        let home = home_str()?;
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                origin: Some(types::OriginConfig::Fs {
                    path: PathBuf::from("${HOME}/cache-origin"),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let path = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
            Some(types::OriginConfig::Fs { path }) => path.clone(),
            other => anyhow::bail!("expected Fs origin, got: {other:?}"),
        };
        let expected = PathBuf::from(format!("{home}/cache-origin"));
        anyhow::ensure!(path == expected, "got: {}", path.display());
        Ok(())
    }

    // The FS arm of `resolve_origin` calls `expand_tilde` so a TOML
    // like `path = "~/origin"` resolves to `<home>/origin`. The
    // expansion happens inside resolution (not in `expand_env`),
    // because `~` is filesystem-shaped and the `expand_env`
    // contract only handles `${VAR}` substitution.
    #[test]
    fn resolve_cache_origin_fs_expands_tilde_in_path() -> anyhow::Result<()> {
        let home_dir = TempDir::new()?;
        let home = home_dir.path().to_path_buf();
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origin: Some(types::OriginConfig::Fs {
                path: PathBuf::from("~/origin"),
            }),
            ..Default::default()
        };
        let resolved = common::test_support::with_home_override(Some(&home), || {
            resolve_cache(&cli, Some(&toml), Path::new("/data-dir"))
        })?;
        match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::Fs { path }) => {
                anyhow::ensure!(
                    path == home.join("origin"),
                    "tilde should have expanded; got: {}",
                    path.display()
                );
            }
            other => anyhow::bail!("expected Fs origin, got: {other:?}"),
        }
        Ok(())
    }

    // resolve_cache must reject an HTTP origin variant whose `url`
    // is the empty string. The error is raised by the explicit
    // `anyhow::ensure!(!url.is_empty(), ...)` in `resolve_origin`,
    // separate from `parse_origin_url`'s scheme/format checks. The
    // existing `resolve_cache_rejects_non_http_origin_url` test
    // covers the parser path; this one covers the
    // empty-string-fails-fast path so a future refactor (e.g.
    // pushing the empty check inside the parser) can't silently
    // drop the contract.
    #[test]
    fn resolve_cache_rejects_empty_http_origin_url() -> anyhow::Result<()> {
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origin: Some(types::OriginConfig::Http {
                url: String::new(),
                decompress: None,
            }),
            ..Default::default()
        };
        let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected empty-url rejection"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("cache.origin.url") && msg.contains("must not be empty"),
            "error lacked context: {msg}"
        );
        Ok(())
    }

    // Sibling of the empty-URL test for the FS variant. The
    // emptiness check runs *after* tilde expansion (the operator
    // wrote `path = ""`), so an empty PathBuf reaches the
    // `as_os_str().is_empty()` guard.
    #[test]
    fn resolve_cache_rejects_empty_fs_origin_path() -> anyhow::Result<()> {
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origin: Some(types::OriginConfig::Fs {
                path: PathBuf::new(),
            }),
            ..Default::default()
        };
        let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected empty-path rejection"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("cache.origin.path") && msg.contains("must not be empty"),
            "error lacked context: {msg}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------
    // Multi-origin fallback config validation (#284)
    //
    // The resolver collapses both wire forms — singular `[cache.origin]`
    // and plural `[[cache.origins]]` — into a single canonical
    // `Vec<ResolvedOrigin>` in `ResolvedCache`. These tests pin the
    // invariants the chain-walk engine relies on: declared order is
    // preserved, both forms cannot be set simultaneously, and an
    // empty plural array fails fast rather than silently degrading
    // to "no pull-through".
    // -------------------------------------------------------------------

    #[test]
    fn resolve_cache_accepts_array_of_origins_preserving_order() -> anyhow::Result<()> {
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origins: Some(vec![
                types::OriginConfig::Http {
                    url: "https://primary.example/".to_string(),
                    decompress: None,
                },
                types::OriginConfig::Fs {
                    path: PathBuf::from("/var/lib/decdn/mirror"),
                },
            ]),
            ..Default::default()
        };
        let resolved = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.origins.len() == 2,
            "expected 2 origins, got {}",
            resolved.origins.len()
        );
        // Order is operator-controlled and load-bearing — assert the
        // first slot is the HTTP entry and the second is the Fs one,
        // matching the TOML declaration order.
        anyhow::ensure!(
            matches!(
                resolved.origins[0],
                crate::config::ResolvedOrigin::Http { .. }
            ),
            "expected origins[0] to be Http, got {:?}",
            resolved.origins[0]
        );
        anyhow::ensure!(
            matches!(
                resolved.origins[1],
                crate::config::ResolvedOrigin::Fs { .. }
            ),
            "expected origins[1] to be Fs, got {:?}",
            resolved.origins[1]
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_rejects_both_origin_and_origins() -> anyhow::Result<()> {
        // Each wire form names a different fallback policy. Setting
        // both is an operator mistake the resolver must surface, not
        // silently pick a winner.
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origin: Some(types::OriginConfig::Http {
                url: "https://one.example/".to_string(),
                decompress: None,
            }),
            origins: Some(vec![types::OriginConfig::Http {
                url: "https://two.example/".to_string(),
                decompress: None,
            }]),
            ..Default::default()
        };
        let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected mutual-exclusion rejection"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("cache.origin")
                && msg.contains("cache.origins")
                && msg.contains("mutually exclusive"),
            "error lacked both keys and exclusivity marker: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_rejects_empty_origins_array() -> anyhow::Result<()> {
        // `origins = []` is almost certainly a half-finished config
        // edit (operator meant to populate it later). Reject at load
        // so the surprise lands at startup, not at the first cache
        // miss hours later.
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origins: Some(Vec::new()),
            ..Default::default()
        };
        let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected empty-array rejection"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("cache.origins") && msg.contains("at least one entry"),
            "error lacked the at-least-one-entry guidance: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_origin_singular_resolves_to_one_element_vec() -> anyhow::Result<()> {
        // Back-compat: pre-#284 operators with a single `[cache.origin]`
        // table must observe identical behaviour after the resolver
        // collapses both wire forms into a vec. A length-1 vec is the
        // canonical representation of the singular form.
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origin: Some(types::OriginConfig::Http {
                url: "https://only.example/".to_string(),
                decompress: None,
            }),
            ..Default::default()
        };
        let resolved = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.origins.len() == 1,
            "singular [cache.origin] must produce a 1-element vec, got len {}",
            resolved.origins.len(),
        );
        anyhow::ensure!(
            matches!(
                resolved.origins[0],
                crate::config::ResolvedOrigin::Http { .. }
            ),
            "expected the single origin to be Http"
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_no_origin_resolves_to_empty_vec() -> anyhow::Result<()> {
        // Back-compat: absent both `[cache.origin]` and `[[cache.origins]]`
        // means "no pull-through configured". The resolver returns an
        // empty vec; engine's `pull_through` short-circuits to NoOrigin.
        let cli = empty_cache_args();
        let toml = types::CacheConfig::default();
        let resolved = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.origins.is_empty(),
            "absent origin section must yield empty vec, got len {}",
            resolved.origins.len(),
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_accepts_duplicate_origins_without_erroring() -> anyhow::Result<()> {
        // Duplicate entries are logged as `tracing::warn!` but must
        // not fail config resolution — operators legitimately use
        // duplicates for connection-pool sharding. Pinning the
        // non-erroring contract here protects against a future PR
        // promoting the warning to a hard error. (Tracing-event
        // capture isn't asserted; that would require a new dev-dep
        // for a single assertion. Code review of
        // `warn_on_duplicate_origins` covers the log emission.)
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origins: Some(vec![
                types::OriginConfig::Http {
                    url: "https://shared.example/".to_string(),
                    decompress: None,
                },
                types::OriginConfig::Http {
                    url: "https://shared.example/".to_string(),
                    decompress: None,
                },
            ]),
            ..Default::default()
        };
        let resolved = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.origins.len() == 2,
            "duplicate origins must both survive into the resolved vec"
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_origins_propagates_per_entry_error_with_index() -> anyhow::Result<()> {
        // The second entry has an empty URL — the existing
        // `resolve_origin` validator should reject it, and the wrapper
        // must thread the `cache.origins[1]` index into the error
        // context so an operator with three entries can identify which
        // one is broken.
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origins: Some(vec![
                types::OriginConfig::Http {
                    url: "https://good.example/".to_string(),
                    decompress: None,
                },
                types::OriginConfig::Http {
                    url: String::new(),
                    decompress: None,
                },
            ]),
            ..Default::default()
        };
        let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected per-entry validation rejection"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("cache.origins[1]") && msg.contains("must not be empty"),
            "error lacked the indexed context: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_origins_accumulates_every_bad_entry() -> anyhow::Result<()> {
        // Intra-section accumulation: two malformed origins at indices 0
        // and 2 (valid at 1) must both be reported by index. A regression
        // to fail-fast on the first bad entry would only report `[0]`.
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origins: Some(vec![
                types::OriginConfig::Http {
                    url: String::new(), // idx 0: empty URL
                    decompress: None,
                },
                types::OriginConfig::Http {
                    url: "https://good.example/".to_string(), // idx 1: valid
                    decompress: None,
                },
                types::OriginConfig::Fs {
                    path: std::path::PathBuf::new(), // idx 2: empty path
                },
            ]),
            ..Default::default()
        };
        let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected per-entry validation rejection"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("cache.origins[0]") && msg.contains("cache.origins[2]"),
            "both bad entries must be named by index: {msg}"
        );
        anyhow::ensure!(
            !msg.contains("cache.origins[1]"),
            "the valid entry must not be reported: {msg}"
        );
        Ok(())
    }

    // resolve_cache must reject a non-http(s) URL at config resolution
    // instead of deferring the check to engine wiring. This locks in the
    // "single parser" invariant introduced by `parse_origin_url`.
    #[test]
    fn resolve_cache_rejects_non_http_origin_url() -> anyhow::Result<()> {
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origin: Some(types::OriginConfig::Http {
                url: "file:///etc/passwd".to_string(),
                decompress: None,
            }),
            ..Default::default()
        };
        let err = resolve_cache(&cli, Some(&toml), std::path::Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected scheme rejection"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("invalid cache.origin") || msg.contains("unsupported origin URL scheme"),
            "error lacked context: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_payment_rejects_zero_from_cli() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: Some(0),
            delivery_floor: None,
            delivery_ceiling: None,
        };
        let err = resolve_payment(&cli, None)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection for rate_per_mb=0"))?
            .to_string();
        anyhow::ensure!(
            err.contains("rate_per_mb") && err.contains("> 0"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[test]
    fn resolve_payment_rejects_zero_delivery_ceiling() -> anyhow::Result<()> {
        // ceiling=0 would clamp every quoted rate to 0, signing a free
        // selection-winning rate and bypassing the rate_per_mb > 0 guard.
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: Some(10),
            delivery_floor: Some(0),
            delivery_ceiling: Some(0),
        };
        let err = resolve_payment(&cli, None)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection for delivery_ceiling=0"))?
            .to_string();
        anyhow::ensure!(
            err.contains("delivery_ceiling") && err.contains(">= 1"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[test]
    fn resolve_payment_rejects_floor_above_ceiling() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: Some(10),
            delivery_floor: Some(100),
            delivery_ceiling: Some(50),
        };
        let err = resolve_payment(&cli, None)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection for floor>ceiling"))?
            .to_string();
        anyhow::ensure!(
            err.contains("delivery_floor") && err.contains("delivery_ceiling"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[test]
    fn resolve_payment_rejects_ceiling_above_protocol_max() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: Some(10),
            delivery_floor: None,
            delivery_ceiling: Some(decdn_protocol::MAX_RATE_PER_MB + 1),
        };
        let err = resolve_payment(&cli, None)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection for ceiling>MAX"))?
            .to_string();
        anyhow::ensure!(
            err.contains("delivery_ceiling") && err.contains("MAX_RATE_PER_MB"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[test]
    fn resolve_payment_accepts_and_threads_explicit_bounds() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: Some(10),
            delivery_floor: Some(5),
            delivery_ceiling: Some(100),
        };
        let resolved = resolve_payment(&cli, None)?;
        anyhow::ensure!(
            resolved.delivery_floor == 5 && resolved.delivery_ceiling == 100,
            "bounds not threaded: floor={} ceiling={}",
            resolved.delivery_floor,
            resolved.delivery_ceiling
        );
        Ok(())
    }

    // Origin variant from TOML resolves into a typed `ResolvedOrigin::Http`
    // that round-trips the parsed URL (#437). Replaces the CLI-vs-TOML
    // precedence test that lived here before — the origin no longer has a
    // CLI flag, so precedence is moot, but we still want a smoke test that
    // the TOML form makes it through resolution without dropping anything.
    #[test]
    fn resolve_cache_origin_http_from_toml_round_trips() -> anyhow::Result<()> {
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origin: Some(types::OriginConfig::Http {
                url: "https://origin.example/".to_string(),
                decompress: None,
            }),
            ..Default::default()
        };
        let resolved = resolve_cache(&cli, Some(&toml), std::path::Path::new("/tmp"))?;
        let url = match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::Http { url, .. }) => url,
            other => anyhow::bail!("expected Http origin, got: {other:?}"),
        };
        anyhow::ensure!(
            url.as_url().as_str() == "https://origin.example/",
            "got: {url}"
        );
        Ok(())
    }

    #[test]
    fn resolve_payment_rejects_zero_from_file() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: None,
            delivery_floor: None,
            delivery_ceiling: None,
        };
        let file = types::PaymentConfig {
            rate_per_mb: Some(0),
            delivery_floor: None,
            delivery_ceiling: None,
            voucher_interval_mb: None,
        };
        let err = resolve_payment(&cli, Some(&file))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection for rate_per_mb=0"))?
            .to_string();
        anyhow::ensure!(
            err.contains("rate_per_mb") && err.contains("> 0"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    // Filesystem origin variant resolves into the typed
    // `ResolvedOrigin::Fs` that the runtime's `build_cache` enum match
    // dispatches on (#437).
    #[test]
    fn resolve_cache_origin_fs_round_trips() -> anyhow::Result<()> {
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            origin: Some(types::OriginConfig::Fs {
                path: PathBuf::from("/tmp/origin"),
            }),
            ..Default::default()
        };
        let resolved = resolve_cache(&cli, Some(&toml), std::path::Path::new("/tmp"))?;
        match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::Fs { path }) => {
                anyhow::ensure!(path == Path::new("/tmp/origin"), "path: {}", path.display());
            }
            other => anyhow::bail!("expected Fs origin, got: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn resolve_payment_cli_overrides_file_and_passes_nonzero() -> anyhow::Result<()> {
        // Regression guard for the merge order: a zero file value must not
        // short-circuit the CLI override that would otherwise be valid.
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: Some(42),
            delivery_floor: None,
            delivery_ceiling: None,
        };
        let file = types::PaymentConfig {
            rate_per_mb: Some(0),
            delivery_floor: None,
            delivery_ceiling: None,
            voucher_interval_mb: None,
        };
        let resolved = resolve_payment(&cli, Some(&file))?;
        anyhow::ensure!(resolved.rate_per_mb == 42, "got: {}", resolved.rate_per_mb);
        Ok(())
    }

    #[test]
    fn resolve_payment_defaults_when_unset() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: None,
            delivery_floor: None,
            delivery_ceiling: None,
        };
        let resolved = resolve_payment(&cli, None)?;
        anyhow::ensure!(
            resolved.rate_per_mb == DEFAULT_RATE_PER_MB,
            "got: {}",
            resolved.rate_per_mb
        );
        anyhow::ensure!(
            resolved.voucher_interval_mb == decdn_protocol::DEFAULT_VOUCHER_INTERVAL_MB,
            "voucher_interval_mb default, got: {}",
            resolved.voucher_interval_mb
        );
        Ok(())
    }

    #[test]
    fn resolve_payment_threads_explicit_voucher_interval() -> anyhow::Result<()> {
        let file = types::PaymentConfig {
            rate_per_mb: Some(10),
            delivery_floor: None,
            delivery_ceiling: None,
            voucher_interval_mb: Some(64),
        };
        let resolved = resolve_payment(&empty_payment_args(), Some(&file))?;
        anyhow::ensure!(resolved.voucher_interval_mb == 64);
        Ok(())
    }

    #[test]
    fn resolve_payment_rejects_voucher_interval_out_of_range() -> anyhow::Result<()> {
        for bad in [0u64, decdn_protocol::MAX_VOUCHER_INTERVAL_MB + 1] {
            let file = types::PaymentConfig {
                rate_per_mb: Some(10),
                delivery_floor: None,
                delivery_ceiling: None,
                voucher_interval_mb: Some(bad),
            };
            let err = resolve_payment(&empty_payment_args(), Some(&file))
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected rejection for interval {bad}"))?
                .to_string();
            anyhow::ensure!(
                err.contains("voucher_interval_mb"),
                "error lacked field context for {bad}: {err}"
            );
        }
        Ok(())
    }

    // Issue #378: configuring a rate above the protocol-level wire ceiling
    // is a startup error — otherwise the node would publish ProbeResponses
    // that every honest client decoder rejects, silently dropping itself
    // out of the candidate pool.
    #[test]
    fn resolve_payment_rejects_rate_above_protocol_max() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: Some(decdn_protocol::MAX_RATE_PER_MB + 1),
            delivery_floor: None,
            delivery_ceiling: None,
        };
        let err = resolve_payment(&cli, None)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection above MAX_RATE_PER_MB"))?
            .to_string();
        anyhow::ensure!(
            err.contains("MAX_RATE_PER_MB") && err.contains("#378"),
            "error lacked MAX_RATE_PER_MB / issue #378 context: {err}"
        );
        Ok(())
    }

    #[test]
    fn resolve_payment_accepts_rate_at_protocol_max() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: Some(decdn_protocol::MAX_RATE_PER_MB),
            delivery_floor: None,
            delivery_ceiling: None,
        };
        let resolved = resolve_payment(&cli, None)?;
        anyhow::ensure!(
            resolved.rate_per_mb == decdn_protocol::MAX_RATE_PER_MB,
            "expected MAX_RATE_PER_MB; got {}",
            resolved.rate_per_mb,
        );
        Ok(())
    }

    fn cache_cli(
        cache_size_mb: Option<u64>,
        max_blob_size_mb: Option<u64>,
    ) -> crate::cli::run::CacheArgs {
        crate::cli::run::CacheArgs {
            cache_dir: None,
            cache_size_mb,
            max_blob_size_mb,
            max_probe_holds: None,
            stake_lane_reserved_holds: None,
        }
    }

    #[test]
    fn resolve_cache_rejects_max_blob_equal_to_cache_size() -> anyhow::Result<()> {
        let cli = cache_cli(Some(100), Some(100));
        let err = resolve_cache(&cli, None, Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection for max == cache"))?
            .to_string();
        anyhow::ensure!(
            err.contains("max_blob_size_mb") && err.contains("cache_size_mb"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_rejects_max_blob_greater_than_cache_size() -> anyhow::Result<()> {
        let cli = cache_cli(Some(100), Some(200));
        let err = resolve_cache(&cli, None, Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection for max > cache"))?
            .to_string();
        anyhow::ensure!(
            err.contains("strictly less than"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_accepts_max_blob_below_cache_size() -> anyhow::Result<()> {
        let cli = cache_cli(Some(1024), Some(512));
        let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.cache_size_mb == 1024,
            "cache_size: {}",
            resolved.cache_size_mb
        );
        anyhow::ensure!(
            resolved.max_blob_size_mb == 512,
            "max_blob: {}",
            resolved.max_blob_size_mb
        );
        Ok(())
    }

    // ----- pinned_hashes / decompress (#276, #312) -----

    fn make_hex_hash(seed: u8) -> String {
        use std::fmt::Write as _;

        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            // 32-element array, so usize→u8 always fits.
            let i_u8 = u8::try_from(i).unwrap_or(0);
            *b = i_u8.wrapping_add(seed);
        }
        // 64 lowercase hex chars — matches the BLAKE3 wire form.
        let mut s = String::with_capacity(64);
        for b in bytes {
            // write! to a String is infallible; the `_` swallows the
            // formal Result without invoking the workspace's expect_used
            // lint.
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    #[test]
    fn parse_pinned_hashes_accepts_valid_lowercase_hex() -> anyhow::Result<()> {
        let h1 = make_hex_hash(1);
        let h2 = make_hex_hash(2);
        let raw = vec![h1.clone(), h2.clone()];
        let parsed = parse_pinned_hashes(Some(&raw))?;
        anyhow::ensure!(parsed.len() == 2, "expected 2 hashes, got {}", parsed.len());
        Ok(())
    }

    #[test]
    fn parse_pinned_hashes_deduplicates() -> anyhow::Result<()> {
        let h = make_hex_hash(7);
        let raw = vec![h.clone(), h.clone(), h];
        let parsed = parse_pinned_hashes(Some(&raw))?;
        anyhow::ensure!(parsed.len() == 1, "duplicates should collapse");
        Ok(())
    }

    #[test]
    fn parse_pinned_hashes_rejects_wrong_length() -> anyhow::Result<()> {
        let raw = vec!["abcd".to_string()]; // 4 chars, not 64
        let err = parse_pinned_hashes(Some(&raw))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected error"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("64 hex chars"),
            "error should mention length: {msg}"
        );
        Ok(())
    }

    #[test]
    fn parse_pinned_hashes_rejects_non_hex_chars() -> anyhow::Result<()> {
        // 64 chars but contains 'z' which is not hex.
        let bad: String = std::iter::repeat_n('z', 64).collect();
        let raw = vec![bad];
        let err = parse_pinned_hashes(Some(&raw))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected error"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("lowercase hex"),
            "error should mention hex: {msg}"
        );
        Ok(())
    }

    #[test]
    fn parse_pinned_hashes_rejects_uppercase_hex() -> anyhow::Result<()> {
        // A copy-paste from a UI that upper-cased the digest is the most
        // likely operator mistake. Reject explicitly so they get a clear
        // error rather than a half-pinned set.
        let bad: String = std::iter::repeat_n('A', 64).collect();
        let raw = vec![bad];
        let err = parse_pinned_hashes(Some(&raw))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected error"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(msg.contains("lowercase"), "got: {msg}");
        Ok(())
    }

    #[test]
    fn parse_pinned_hashes_empty_or_none_yields_empty_set() -> anyhow::Result<()> {
        anyhow::ensure!(parse_pinned_hashes(None)?.is_empty());
        let raw: Vec<String> = vec![];
        anyhow::ensure!(parse_pinned_hashes(Some(&raw))?.is_empty());
        Ok(())
    }

    #[test]
    fn resolve_cache_no_origin_and_pinned_empty_by_default() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.origins.is_empty(),
            "no [cache.origin]/[[cache.origins]] => no pull-through"
        );
        anyhow::ensure!(resolved.pinned_hashes.is_empty());
        Ok(())
    }

    #[test]
    fn resolve_cache_user_agent_defaults_to_workspace_constant() -> anyhow::Result<()> {
        // Absent => DEFAULT_USER_AGENT (#435).
        let cli = cache_cli(None, None);
        let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.user_agent == decdn_config_types::DEFAULT_USER_AGENT,
            "expected default UA, got: {}",
            resolved.user_agent
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_user_agent_from_file_overrides_default() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            user_agent: Some("MyCdn/1.0 (+ops@example.com)".to_string()),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.user_agent == "MyCdn/1.0 (+ops@example.com)",
            "got: {}",
            resolved.user_agent
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_user_agent_invalid_falls_back_to_default() {
        // Present-but-invalid UA must both record the `cache.user_agent`
        // problem AND leave the resolved struct carrying
        // `DEFAULT_USER_AGENT`. The fallback matters because
        // `resolve_config` keeps accumulating across sections after this
        // call; a `ResolvedCache` carrying the invalid bytes would smuggle
        // them past the resolver into the eventual `reqwest::Client::builder`.
        // The shim `resolve_cache` returns `Err` and drops the partial
        // value, so the test drives `resolve_cache_into` directly to
        // observe the field.
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            user_agent: Some("evil\r\nX-Inject: 1".to_string()),
            ..types::CacheConfig::default()
        };
        let mut bag = ConfigErrorBag::new();
        let resolved = resolve_cache_into(&cli, Some(&file), Path::new("/tmp"), &mut bag);
        assert!(
            bag.has_field("cache.user_agent"),
            "expected cache.user_agent problem to be recorded"
        );
        assert_eq!(
            resolved.user_agent,
            decdn_config_types::DEFAULT_USER_AGENT,
            "invalid UA must fall back to DEFAULT_USER_AGENT, got: {}",
            resolved.user_agent
        );
    }

    #[test]
    fn resolve_cache_gc_interval_defaults_when_absent() -> anyhow::Result<()> {
        // Absent => DEFAULT_GC_INTERVAL_SEC (#518).
        let cli = cache_cli(None, None);
        let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.gc_interval_sec == DEFAULT_GC_INTERVAL_SEC,
            "expected default {}, got: {}",
            DEFAULT_GC_INTERVAL_SEC,
            resolved.gc_interval_sec
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_gc_interval_from_file_overrides_default() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            gc_interval_sec: Some(42),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.gc_interval_sec == 42,
            "expected 42, got: {}",
            resolved.gc_interval_sec
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_gc_interval_zero_disables() -> anyhow::Result<()> {
        // `0` is the documented "disable periodic GC" sentinel — no
        // clamp/floor should turn it back on. A regression that
        // saturated to a minimum would silently re-enable GC for
        // operators who explicitly opted out.
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            gc_interval_sec: Some(0),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.gc_interval_sec == 0,
            "0 must round-trip as 0 (disabled); got: {}",
            resolved.gc_interval_sec
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_stake_lane_reserved_holds_defaults_to_zero() -> anyhow::Result<()> {
        // Absent everywhere => reservation off (#757). The default MUST be 0
        // so a node that never opted in behaves exactly as pre-#757.
        let cli = cache_cli(None, None);
        let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.stake_lane_reserved_holds == DEFAULT_STAKE_LANE_RESERVED_HOLDS,
            "expected default {DEFAULT_STAKE_LANE_RESERVED_HOLDS}, got: {}",
            resolved.stake_lane_reserved_holds
        );
        anyhow::ensure!(
            resolved.stake_lane_reserved_holds == 0,
            "the #757 default must be 0 (reservation off)"
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_stake_lane_reserved_holds_from_file() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            stake_lane_reserved_holds: Some(8),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.stake_lane_reserved_holds == 8,
            "expected 8 from file, got: {}",
            resolved.stake_lane_reserved_holds
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_stake_lane_reserved_holds_cli_overrides_file() -> anyhow::Result<()> {
        // CLI wins over file, mirroring the `max_probe_holds` precedence.
        let cli = crate::cli::run::CacheArgs {
            cache_dir: None,
            cache_size_mb: None,
            max_blob_size_mb: None,
            max_probe_holds: None,
            stake_lane_reserved_holds: Some(3),
        };
        let file = types::CacheConfig {
            stake_lane_reserved_holds: Some(8),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.stake_lane_reserved_holds == 3,
            "CLI flag must override the file value; expected 3, got: {}",
            resolved.stake_lane_reserved_holds
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_rejects_empty_user_agent() {
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            user_agent: Some(String::new()),
            ..types::CacheConfig::default()
        };
        let err =
            resolve_cache(&cli, Some(&file), Path::new("/tmp")).expect_err("empty UA must reject");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("cache.user_agent"),
            "missing field name: {msg}"
        );
    }

    /// Reject control-byte bytes at config load — most importantly CR/LF, which
    /// would let an operator-supplied (or env-expanded) value smuggle a second
    /// header onto every origin request. Also covers NUL, other C0 controls,
    /// DEL, and non-ASCII obs-text. Without this, the value reaches
    /// `reqwest::Client::builder` and surfaces as a generic build error from
    /// inside `HttpOrigin::new_with_user_agent` at startup, which doesn't tell
    /// the operator which config field is to blame.
    #[test]
    fn resolve_cache_rejects_user_agent_with_control_bytes() {
        let cli = cache_cli(None, None);
        for bad in [
            "evil\r\nX-Inject: 1",
            "has\nlf",
            "has\rcr",
            "nul\0byte",
            "del\x7fbyte",
            "non-ascii-\u{00e9}",
        ] {
            let file = types::CacheConfig {
                user_agent: Some(bad.to_string()),
                ..types::CacheConfig::default()
            };
            let result = resolve_cache(&cli, Some(&file), Path::new("/tmp"));
            assert!(result.is_err(), "UA `{bad:?}` must reject but resolved OK");
        }
        // Pin the message shape on one representative case so a regression in
        // error context is caught (e.g. losing the field name or the byte
        // position).
        let file = types::CacheConfig {
            user_agent: Some("crlf\r\ninjection".to_string()),
            ..types::CacheConfig::default()
        };
        let err =
            resolve_cache(&cli, Some(&file), Path::new("/tmp")).expect_err("CRLF UA must reject");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("cache.user_agent") && msg.contains("invalid byte"),
            "error should name the field and describe the byte, got: {msg}"
        );
    }

    /// Tab is part of the legal HTTP header-value byte set and shows up in
    /// real-world UAs occasionally; make sure the validator doesn't over-reject.
    #[test]
    fn resolve_cache_accepts_user_agent_with_tab() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            user_agent: Some("MyCdn/1.0\t(ops@example.com)".to_string()),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.user_agent == "MyCdn/1.0\t(ops@example.com)",
            "got: {}",
            resolved.user_agent
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_http_decompress_strict_via_file() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            origin: Some(types::OriginConfig::Http {
                url: "https://origin.example/".to_string(),
                decompress: Some(decdn_config_types::DecompressMode::Strict),
            }),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::Http { decompress, .. }) => {
                anyhow::ensure!(matches!(
                    decompress,
                    decdn_config_types::DecompressMode::Strict
                ));
            }
            other => anyhow::bail!("expected Http origin, got: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn resolve_cache_s3_decompress_strict_via_file() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = cache_with_s3(types::S3OriginConfig {
            decompress: Some(decdn_config_types::DecompressMode::Strict),
            ..s3_cfg("decdn-blobs")
        });
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::S3(s3)) => {
                anyhow::ensure!(matches!(
                    s3.decompress,
                    decdn_config_types::DecompressMode::Strict
                ));
            }
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn resolve_cache_s3_decompress_defaults_to_auto() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = cache_with_s3(s3_cfg("decdn-blobs"));
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::S3(s3)) => {
                anyhow::ensure!(matches!(
                    s3.decompress,
                    decdn_config_types::DecompressMode::Auto
                ));
            }
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn resolve_cache_http_decompress_defaults_to_auto() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            origin: Some(types::OriginConfig::Http {
                url: "https://origin.example/".to_string(),
                decompress: None,
            }),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::Http { decompress, .. }) => {
                anyhow::ensure!(matches!(
                    decompress,
                    decdn_config_types::DecompressMode::Auto
                ));
            }
            other => anyhow::bail!("expected Http origin, got: {other:?}"),
        }
        Ok(())
    }

    // ----- #437: S3 origin schema -----
    //
    // The tests below pin the shape of the schema validators and the
    // resolved-form normalization. The runtime backend (`S3Origin`)
    // lives in `decdn-cache` and is exercised separately by
    // `crates/cache/tests/s3_origin.rs`; these tests stay focused on
    // the resolver contract that feeds it.

    /// Build a TOML S3 origin with sane defaults; tests override
    /// individual fields. Avoids 6-line struct literals at every call
    /// site.
    fn s3_cfg(bucket: &str) -> types::S3OriginConfig {
        types::S3OriginConfig {
            bucket: bucket.to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            path_style: None,
            prefix: None,
            credentials: None,
            decompress: None,
        }
    }

    fn cache_with_s3(s3: types::S3OriginConfig) -> types::CacheConfig {
        types::CacheConfig {
            origin: Some(types::OriginConfig::S3(s3)),
            ..types::CacheConfig::default()
        }
    }

    #[test]
    fn resolve_cache_origin_s3_happy_path() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = cache_with_s3(types::S3OriginConfig {
            bucket: "decdn-blobs".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: Some("https://r2.cloudflarestorage.com".to_string()),
            path_style: Some(true),
            prefix: Some("blobs".to_string()),
            credentials: None,
            decompress: None,
        });
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::S3(s3)) => {
                anyhow::ensure!(s3.bucket == "decdn-blobs", "bucket: {}", s3.bucket);
                anyhow::ensure!(s3.region == "us-east-1", "region: {}", s3.region);
                anyhow::ensure!(s3.path_style, "path_style should round-trip true");
                // Trailing-slash auto-append.
                anyhow::ensure!(s3.prefix == "blobs/", "prefix: {}", s3.prefix);
                let endpoint = s3
                    .endpoint_url
                    .ok_or_else(|| anyhow::anyhow!("endpoint missing"))?;
                anyhow::ensure!(
                    endpoint.as_url().as_str() == "https://r2.cloudflarestorage.com/",
                    "endpoint: {endpoint}"
                );
            }
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn resolve_s3_origin_prefix_trailing_slash_already_present_unchanged() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let mut s3 = s3_cfg("decdn-blobs");
        s3.prefix = Some("foo/bar/".to_string());
        let resolved = resolve_cache(&cli, Some(&cache_with_s3(s3)), Path::new("/tmp"))?;
        match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::S3(s3)) => {
                anyhow::ensure!(s3.prefix == "foo/bar/", "prefix: {}", s3.prefix);
            }
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn resolve_s3_origin_empty_prefix_stays_empty() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let mut s3 = s3_cfg("decdn-blobs");
        s3.prefix = Some(String::new());
        let resolved = resolve_cache(&cli, Some(&cache_with_s3(s3)), Path::new("/tmp"))?;
        match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::S3(s3)) => {
                anyhow::ensure!(s3.prefix.is_empty(), "prefix should remain empty");
            }
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn resolve_s3_origin_path_style_none_collapses_to_false() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let resolved = resolve_cache(
            &cli,
            Some(&cache_with_s3(s3_cfg("decdn-blobs"))),
            Path::new("/tmp"),
        )?;
        match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::S3(s3)) => {
                anyhow::ensure!(
                    !s3.path_style,
                    "path_style absent => false (SDK default = virtual-hosted)"
                );
            }
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        }
        Ok(())
    }

    /// Helper: assert that a TOML-form S3 config rejects with an
    /// error whose chained message contains every required fragment.
    fn assert_s3_rejects(s3: types::S3OriginConfig, fragments: &[&str]) -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let err = resolve_cache(&cli, Some(&cache_with_s3(s3)), Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection"))?;
        let msg = format!("{err:#}");
        for fragment in fragments {
            anyhow::ensure!(
                msg.contains(fragment),
                "error did not contain `{fragment}`: {msg}"
            );
        }
        Ok(())
    }

    #[test]
    fn validate_bucket_rejects_too_short() -> anyhow::Result<()> {
        assert_s3_rejects(s3_cfg("ab"), &["cache.origin.bucket", "3..=63"])
    }

    #[test]
    fn validate_bucket_accepts_min_length_three() -> anyhow::Result<()> {
        // Boundary: exactly 3 chars must pass.
        let cli = cache_cli(None, None);
        let _ = resolve_cache(&cli, Some(&cache_with_s3(s3_cfg("abc"))), Path::new("/tmp"))?;
        Ok(())
    }

    #[test]
    fn validate_bucket_rejects_too_long() -> anyhow::Result<()> {
        let name: String = std::iter::repeat_n('a', 64).collect();
        assert_s3_rejects(s3_cfg(&name), &["cache.origin.bucket", "3..=63"])
    }

    #[test]
    fn validate_bucket_accepts_max_length_sixty_three() -> anyhow::Result<()> {
        let name: String = std::iter::repeat_n('a', 63).collect();
        let cli = cache_cli(None, None);
        let _ = resolve_cache(&cli, Some(&cache_with_s3(s3_cfg(&name))), Path::new("/tmp"))?;
        Ok(())
    }

    #[test]
    fn validate_bucket_rejects_uppercase() -> anyhow::Result<()> {
        assert_s3_rejects(s3_cfg("MyBucket"), &["lowercase"])
    }

    #[test]
    fn validate_bucket_rejects_underscore() -> anyhow::Result<()> {
        assert_s3_rejects(s3_cfg("my_bucket"), &["lowercase"])
    }

    #[test]
    fn validate_bucket_rejects_leading_dot() -> anyhow::Result<()> {
        assert_s3_rejects(s3_cfg(".mybucket"), &["begin"])
    }

    #[test]
    fn validate_bucket_rejects_trailing_dot() -> anyhow::Result<()> {
        assert_s3_rejects(s3_cfg("mybucket."), &["end"])
    }

    #[test]
    fn validate_bucket_rejects_leading_hyphen() -> anyhow::Result<()> {
        // Documented AWS rule: "Bucket names must begin and end with a
        // letter or number." Pre-#437-PR1-review the validator only
        // checked dots; the hyphen-edge case slipped through.
        assert_s3_rejects(s3_cfg("-mybucket"), &["begin"])
    }

    #[test]
    fn validate_bucket_rejects_trailing_hyphen() -> anyhow::Result<()> {
        assert_s3_rejects(s3_cfg("mybucket-"), &["end"])
    }

    #[test]
    fn validate_bucket_rejects_consecutive_dots() -> anyhow::Result<()> {
        assert_s3_rejects(s3_cfg("my..bucket"), &["consecutive dots"])
    }

    #[test]
    fn validate_bucket_rejects_ipv4_literal() -> anyhow::Result<()> {
        assert_s3_rejects(s3_cfg("192.168.1.1"), &["IPv4"])
    }

    #[test]
    fn validate_region_rejects_empty_string() -> anyhow::Result<()> {
        let mut s3 = s3_cfg("decdn-blobs");
        s3.region = String::new();
        assert_s3_rejects(s3, &["cache.origin.region", "must not be empty"])
    }

    #[test]
    fn validate_region_rejects_whitespace_only() -> anyhow::Result<()> {
        let mut s3 = s3_cfg("decdn-blobs");
        s3.region = "   ".to_string();
        assert_s3_rejects(s3, &["cache.origin.region", "must not be empty"])
    }

    #[test]
    fn validate_endpoint_rejects_empty_string() -> anyhow::Result<()> {
        let mut s3 = s3_cfg("decdn-blobs");
        s3.endpoint_url = Some(String::new());
        assert_s3_rejects(s3, &["cache.origin.endpoint_url", "omit the key instead"])
    }

    #[test]
    fn validate_endpoint_rejects_non_http_scheme() -> anyhow::Result<()> {
        let mut s3 = s3_cfg("decdn-blobs");
        s3.endpoint_url = Some("ftp://endpoint.example/".to_string());
        assert_s3_rejects(s3, &["cache.origin.endpoint_url"])
    }

    #[test]
    fn validate_prefix_rejects_leading_slash() -> anyhow::Result<()> {
        let mut s3 = s3_cfg("decdn-blobs");
        s3.prefix = Some("/foo/".to_string());
        assert_s3_rejects(s3, &["cache.origin.prefix", "must not start with `/`"])
    }

    #[test]
    fn validate_prefix_rejects_dotdot() -> anyhow::Result<()> {
        let mut s3 = s3_cfg("decdn-blobs");
        s3.prefix = Some("foo/../bar/".to_string());
        assert_s3_rejects(s3, &["cache.origin.prefix", "must not contain `..`"])
    }

    #[test]
    fn validate_prefix_rejects_backslash() -> anyhow::Result<()> {
        let mut s3 = s3_cfg("decdn-blobs");
        s3.prefix = Some("foo\\bar/".to_string());
        assert_s3_rejects(s3, &["cache.origin.prefix", "must not contain `\\`"])
    }

    #[test]
    fn validate_prefix_rejects_control_chars() -> anyhow::Result<()> {
        // \n / \t / \0 inside the prefix would corrupt the eventual
        // S3 key; reject loud at config load instead of letting the
        // SDK URL-encode them into a "key not found".
        let mut s3 = s3_cfg("decdn-blobs");
        s3.prefix = Some("foo\nbar/".to_string());
        assert_s3_rejects(s3, &["cache.origin.prefix", "control characters"])
    }

    #[test]
    fn validate_prefix_rejects_whitespace() -> anyhow::Result<()> {
        let mut s3 = s3_cfg("decdn-blobs");
        s3.prefix = Some("foo bar/".to_string());
        assert_s3_rejects(s3, &["cache.origin.prefix", "whitespace"])
    }

    // ----- #437: deny_unknown_fields catches typos on Http variant -----
    //
    // The first-pass review caught that the OriginConfig enum lacked
    // deny_unknown_fields, allowing typos like `decompres = "auto"`
    // to silently no-op. These two tests lock the contract.

    #[test]
    fn http_origin_rejects_unknown_field_typo() -> anyhow::Result<()> {
        let toml = r#"
            kind = "http"
            url = "https://origin.example/"
            decompres = "auto"
        "#;
        let err = toml::from_str::<types::OriginConfig>(toml)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected unknown-field error"))?;
        let msg = format!("{err}");
        anyhow::ensure!(
            msg.contains("unknown field") && msg.contains("decompres"),
            "got: {msg}"
        );
        Ok(())
    }

    #[test]
    fn fs_origin_rejects_unknown_field_typo() -> anyhow::Result<()> {
        let toml = r#"
            kind = "fs"
            paht = "/var/decdn"
        "#;
        let err = toml::from_str::<types::OriginConfig>(toml)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected unknown-field error"))?;
        let msg = format!("{err}");
        anyhow::ensure!(
            msg.contains("unknown field") && msg.contains("paht"),
            "got: {msg}"
        );
        Ok(())
    }

    /// Helper: assert that a TOML deserialization fails with a
    /// message containing every required fragment. Used by the
    /// missing-required-field and unknown-tag-value tests.
    fn assert_origin_toml_rejects(toml: &str, fragments: &[&str]) -> anyhow::Result<()> {
        let err = toml::from_str::<types::OriginConfig>(toml)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection for: {toml}"))?;
        let msg = format!("{err}");
        for fragment in fragments {
            anyhow::ensure!(
                msg.contains(fragment),
                "error did not contain `{fragment}`: {msg}"
            );
        }
        Ok(())
    }

    // serde-tagged-enum invariant: the `kind` field is required and
    // names which variant is being deserialized. Without it,
    // serde can't disambiguate. Operators who copy-paste the inner
    // table without the `kind = "..."` line need a clear error.
    #[test]
    fn origin_rejects_missing_kind_tag() -> anyhow::Result<()> {
        assert_origin_toml_rejects(
            r#"url = "https://origin.example/""#,
            &["missing field", "kind"],
        )
    }

    // An unknown `kind` value (e.g. operator typo `httpx` or a
    // forward-looking `gcs` someone speculatively wrote) must
    // surface a clear error instead of being silently dropped.
    #[test]
    fn origin_rejects_unknown_kind_value() -> anyhow::Result<()> {
        assert_origin_toml_rejects(
            r#"
                kind = "httpx"
                url = "https://origin.example/"
            "#,
            &["unknown variant", "httpx"],
        )
    }

    // HTTP variant requires a `url` field. serde reports a
    // missing-field error for the inner struct payload of the
    // tagged enum.
    #[test]
    fn http_origin_rejects_missing_url_field() -> anyhow::Result<()> {
        assert_origin_toml_rejects(
            r#"
                kind = "http"
                decompress = "auto"
            "#,
            &["missing field", "url"],
        )
    }

    // FS variant requires a `path` field.
    #[test]
    fn fs_origin_rejects_missing_path_field() -> anyhow::Result<()> {
        assert_origin_toml_rejects(r#"kind = "fs""#, &["missing field", "path"])
    }

    // S3 variant requires `bucket` and `region`. Two siblings —
    // missing each in turn — so future tag-renames or shape changes
    // can't silently drop either requirement.
    #[test]
    fn s3_origin_rejects_missing_bucket_field() -> anyhow::Result<()> {
        assert_origin_toml_rejects(
            r#"
                kind = "s3"
                region = "us-east-1"
            "#,
            &["missing field", "bucket"],
        )
    }

    #[test]
    fn s3_origin_rejects_missing_region_field() -> anyhow::Result<()> {
        assert_origin_toml_rejects(
            r#"
                kind = "s3"
                bucket = "decdn-blobs"
            "#,
            &["missing field", "region"],
        )
    }

    // ----- #437: S3 credentials end-to-end resolution -----

    // Static credentials round-trip through `resolve_cache` into
    // `ResolvedS3Credentials::Static` with the secret values
    // preserved (as `SecretString`s, exposing only via `expose()`).
    // Also exercises the validator path that converts wire-form
    // `S3Credentials::Static` into resolved form.
    #[test]
    fn resolve_cache_origin_s3_static_credentials_round_trip() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = cache_with_s3(types::S3OriginConfig {
            bucket: "decdn-blobs".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            path_style: None,
            prefix: None,
            credentials: Some(types::S3Credentials::Static {
                access_key_id: secret::SecretString::new("AKIA-test-id"),
                secret_access_key: secret::SecretString::new("test-secret-value"),
                session_token: Some(secret::SecretString::new("STS-token")),
            }),
            decompress: None,
        });
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        let creds = match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::S3(s3)) => s3
                .credentials
                .ok_or_else(|| anyhow::anyhow!("credentials missing"))?,
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        };
        match creds {
            crate::config::ResolvedS3Credentials::Static {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                anyhow::ensure!(access_key_id.expose() == "AKIA-test-id");
                anyhow::ensure!(secret_access_key.expose() == "test-secret-value");
                anyhow::ensure!(
                    session_token.as_ref().map(secret::SecretString::expose) == Some("STS-token")
                );
            }
            crate::config::ResolvedS3Credentials::DefaultChain { .. } => {
                anyhow::bail!("expected Static credentials, got DefaultChain")
            }
        }
        Ok(())
    }

    // DefaultChain with no profile — the most common shape for AWS
    // operators using IAM roles or AWS_PROFILE. Resolves to
    // `ResolvedS3Credentials::DefaultChain { profile: None }`.
    #[test]
    fn resolve_cache_origin_s3_default_chain_no_profile() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = cache_with_s3(types::S3OriginConfig {
            bucket: "decdn-blobs".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            path_style: None,
            prefix: None,
            credentials: Some(types::S3Credentials::DefaultChain { profile: None }),
            decompress: None,
        });
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        let creds = match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::S3(s3)) => s3
                .credentials
                .ok_or_else(|| anyhow::anyhow!("credentials missing"))?,
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        };
        match creds {
            crate::config::ResolvedS3Credentials::DefaultChain { profile } => {
                anyhow::ensure!(profile.is_none(), "profile should be None");
            }
            crate::config::ResolvedS3Credentials::Static { .. } => {
                anyhow::bail!("expected DefaultChain, got Static")
            }
        }
        Ok(())
    }

    // `DefaultChain { profile = "" }` (e.g. from a `${PROFILE}` env-var
    // that resolved to empty, or a literal empty string in TOML) is
    // collapsed to `profile = None` at the resolver. Without this
    // normalization the runtime would call `loader.profile_name("")`
    // and the SDK would surface a confusing "profile '' not found" at
    // first credential need. Pinned so a future refactor of
    // `resolve_s3_origin` can't drop the filter.
    #[test]
    fn resolve_cache_origin_s3_default_chain_empty_profile_collapses_to_none() -> anyhow::Result<()>
    {
        let cli = cache_cli(None, None);
        let file = cache_with_s3(types::S3OriginConfig {
            bucket: "decdn-blobs".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            path_style: None,
            prefix: None,
            credentials: Some(types::S3Credentials::DefaultChain {
                profile: Some(String::new()),
            }),
            decompress: None,
        });
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        let creds = match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::S3(s3)) => s3
                .credentials
                .ok_or_else(|| anyhow::anyhow!("credentials missing"))?,
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        };
        match creds {
            crate::config::ResolvedS3Credentials::DefaultChain { profile } => {
                anyhow::ensure!(profile.is_none(), "empty profile should collapse to None");
            }
            crate::config::ResolvedS3Credentials::Static { .. } => {
                anyhow::bail!("expected DefaultChain, got Static")
            }
        }
        Ok(())
    }

    // Absent `[cache.origin.credentials]` => resolved credentials
    // are `None` (the runtime then falls back to the AWS default
    // credential chain). Pin this default so a future refactor of
    // `resolve_s3_origin` can't accidentally synthesize a
    // `DefaultChain` placeholder where one wasn't requested.
    #[test]
    fn resolve_cache_origin_s3_no_credentials_resolves_to_none() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = cache_with_s3(s3_cfg("decdn-blobs"));
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        match resolved.origins.into_iter().next() {
            Some(ResolvedOrigin::S3(s3)) => {
                anyhow::ensure!(
                    s3.credentials.is_none(),
                    "absent credentials must resolve to None, not synthesized DefaultChain"
                );
            }
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        }
        Ok(())
    }

    // ----- #437: expand_origin walks every URL/path/secret field -----

    #[test]
    fn expand_env_substitutes_s3_bucket_and_region() -> anyhow::Result<()> {
        // Operators routinely template region by env var across
        // multi-region deployments.
        let home = home_str()?;
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                origin: Some(types::OriginConfig::S3(types::S3OriginConfig {
                    bucket: "blobs-${HOME}".to_string(),
                    region: "${HOME}-east-1".to_string(),
                    endpoint_url: None,
                    path_style: None,
                    prefix: None,
                    credentials: None,
                    decompress: None,
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let s3 = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
            Some(types::OriginConfig::S3(s3)) => s3,
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        };
        anyhow::ensure!(s3.bucket == format!("blobs-{home}"));
        anyhow::ensure!(s3.region == format!("{home}-east-1"));
        Ok(())
    }

    #[test]
    fn expand_env_substitutes_s3_endpoint_and_prefix() -> anyhow::Result<()> {
        let home = home_str()?;
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                origin: Some(types::OriginConfig::S3(types::S3OriginConfig {
                    bucket: "decdn-blobs".to_string(),
                    region: "us-east-1".to_string(),
                    endpoint_url: Some("https://${HOME}.example/".to_string()),
                    path_style: None,
                    prefix: Some("blobs-${HOME}/".to_string()),
                    credentials: None,
                    decompress: None,
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let s3 = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
            Some(types::OriginConfig::S3(s3)) => s3,
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        };
        anyhow::ensure!(
            s3.endpoint_url.as_deref() == Some(&format!("https://{home}.example/")[..])
        );
        anyhow::ensure!(s3.prefix.as_deref() == Some(&format!("blobs-{home}/")[..]));
        Ok(())
    }

    #[test]
    fn expand_env_substitutes_s3_static_credentials() -> anyhow::Result<()> {
        // The whole point of supporting `${VAR}` in TOML is to keep
        // secrets out of the file: operators write
        // `access_key_id = "${AWS_ACCESS_KEY_ID}"` and the env var
        // supplies the value. This test locks the wiring so a future
        // refactor of expand_origin can't silently regress it.
        let home = home_str()?;
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                origin: Some(types::OriginConfig::S3(types::S3OriginConfig {
                    bucket: "decdn-blobs".to_string(),
                    region: "us-east-1".to_string(),
                    endpoint_url: None,
                    path_style: None,
                    prefix: None,
                    credentials: Some(types::S3Credentials::Static {
                        access_key_id: secret::SecretString::new("${HOME}-access"),
                        secret_access_key: secret::SecretString::new("${HOME}-secret"),
                        session_token: Some(secret::SecretString::new("${HOME}-token")),
                    }),
                    decompress: None,
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let creds = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
            Some(types::OriginConfig::S3(s3)) => s3.credentials.as_ref(),
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        }
        .ok_or_else(|| anyhow::anyhow!("credentials missing"))?;
        match creds {
            types::S3Credentials::Static {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                anyhow::ensure!(access_key_id.expose() == format!("{home}-access"));
                anyhow::ensure!(secret_access_key.expose() == format!("{home}-secret"));
                anyhow::ensure!(
                    session_token
                        .as_ref()
                        .map(secret::SecretString::expose)
                        .map(str::to_string)
                        == Some(format!("{home}-token"))
                );
            }
            types::S3Credentials::DefaultChain { .. } => {
                anyhow::bail!("expected Static, got DefaultChain")
            }
        }
        Ok(())
    }

    #[test]
    fn expand_env_substitutes_s3_default_chain_profile() -> anyhow::Result<()> {
        let home = home_str()?;
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                origin: Some(types::OriginConfig::S3(types::S3OriginConfig {
                    bucket: "decdn-blobs".to_string(),
                    region: "us-east-1".to_string(),
                    endpoint_url: None,
                    path_style: None,
                    prefix: None,
                    credentials: Some(types::S3Credentials::DefaultChain {
                        profile: Some("${HOME}-prof".to_string()),
                    }),
                    decompress: None,
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let profile = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
            Some(types::OriginConfig::S3(s3)) => match s3.credentials.as_ref() {
                Some(types::S3Credentials::DefaultChain { profile }) => profile.clone(),
                other => anyhow::bail!("expected DefaultChain, got: {other:?}"),
            },
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        };
        anyhow::ensure!(profile == Some(format!("{home}-prof")));
        Ok(())
    }

    #[test]
    fn expand_secret_error_on_missing_env_var_does_not_leak_partial_value() -> anyhow::Result<()> {
        // The safety claim is: an undefined `${VAR}` in a secret
        // field surfaces only the field's dotted-path context and
        // the env-var name — never any cleartext that might be
        // adjacent to the marker in the TOML. This test pins that
        // contract for the credential path so a refactor of
        // expand_value cannot silently regress it.
        let missing = "DECDN_UNSET_SECRET_VAR_XYZ";
        anyhow::ensure!(
            std::env::var_os(missing).is_none(),
            "test prereq: unset env var"
        );
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                origin: Some(types::OriginConfig::S3(types::S3OriginConfig {
                    bucket: "decdn-blobs".to_string(),
                    region: "us-east-1".to_string(),
                    endpoint_url: None,
                    path_style: None,
                    prefix: None,
                    credentials: Some(types::S3Credentials::Static {
                        access_key_id: secret::SecretString::new(format!(
                            "AKIA-prefix-${{{missing}}}-suffix"
                        )),
                        secret_access_key: secret::SecretString::new("does-not-matter"),
                        session_token: None,
                    }),
                    decompress: None,
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = expand_env(&mut cfg)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected env-var error"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("cache.origin.credentials.access_key_id"),
            "error must name the field: {msg}"
        );
        anyhow::ensure!(
            !msg.contains("AKIA") && !msg.contains("suffix"),
            "error must not echo any cleartext from the secret value: {msg}"
        );
        Ok(())
    }

    // ----- #437: legacy decompress field rejection -----

    #[test]
    fn http_origin_legacy_top_level_decompress_field_rejected() -> anyhow::Result<()> {
        // Pre-#437 schemas put `decompress` at `[cache]` directly.
        // Sibling of the legacy `origin_url`/`origin_path`
        // rejection tests below.
        let toml_body = format!(
            "{}\n\n[cache]\ndecompress = \"strict\"\n",
            complete_toml_body()
        );
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(&dir, &toml_body)?;
        let args = run_args_with_data_dir(dir.path());
        let err = resolve_config(Some(&path), &args)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected unknown-field error"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("decompress") && msg.contains("unknown field"),
            "error must call out the legacy `decompress` key: {msg}"
        );
        Ok(())
    }

    // ----- #437: regression lock — `decdn config validate` accepts S3 today -----

    #[test]
    fn resolve_config_accepts_s3_origin_today() -> anyhow::Result<()> {
        // `decdn config validate` (which goes through `resolve_config`)
        // must succeed for a valid S3 TOML — operator-side validation
        // is the schema + resolver layer, not the runtime backend.
        // Pinned so a future refactor that moved the S3 backend's
        // construction-time validation upstream into `resolve_origin`
        // can't silently start rejecting configs that `build_cache`
        // would otherwise accept.
        let toml_body = format!(
            "{}\n\n[cache.origin]\nkind = \"s3\"\n\
             bucket = \"decdn-blobs\"\nregion = \"us-east-1\"\n",
            complete_toml_body()
        );
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(&dir, &toml_body)?;
        let args = run_args_with_data_dir(dir.path());
        let resolved = resolve_config(Some(&path), &args)?;
        match resolved.cache.origins.into_iter().next() {
            Some(ResolvedOrigin::S3(s3)) => {
                anyhow::ensure!(s3.bucket == "decdn-blobs");
            }
            other => anyhow::bail!("expected S3 origin, got: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn resolve_cache_pinned_hashes_propagate_through_file() -> anyhow::Result<()> {
        let h = make_hex_hash(5);
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            pinned_hashes: Some(vec![h.clone()]),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.pinned_hashes.len() == 1,
            "expected one pinned hash, got {}",
            resolved.pinned_hashes.len()
        );
        Ok(())
    }

    #[test]
    fn resolve_cache_invalid_pinned_hash_fails_resolution() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            pinned_hashes: Some(vec!["not-a-hash".to_string()]),
            ..types::CacheConfig::default()
        };
        let err = resolve_cache(&cli, Some(&file), Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected error"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("invalid cache.pinned_hashes"),
            "error should be contextualized: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_origin_retry_defaults_when_absent() -> anyhow::Result<()> {
        // Absent `cache.origin_retry` section => defaults from
        // RetryPolicy::default(). Pin the contract here so a future
        // default change has to update this test deliberately.
        let cli = cache_cli(None, None);
        let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
        let p = resolved.origin_retry;
        anyhow::ensure!(p.max_retries == 3, "default max_retries");
        anyhow::ensure!(p.initial_backoff_ms == 100, "default initial_backoff_ms");
        anyhow::ensure!(p.max_backoff_ms == 10_000, "default max_backoff_ms");
        anyhow::ensure!(
            (p.jitter_ratio - 0.1).abs() < f64::EPSILON,
            "default jitter_ratio"
        );
        Ok(())
    }

    #[test]
    fn resolve_origin_retry_parses_full_section() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            origin_retry: Some(decdn_config_types::RetryPolicy {
                max_retries: 7,
                initial_backoff_ms: 50,
                max_backoff_ms: 2_000,
                jitter_ratio: 0.25,
                buffered_max_bytes: 8 << 20, // 8 MiB
            }),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        let p = resolved.origin_retry;
        anyhow::ensure!(p.max_retries == 7);
        anyhow::ensure!(p.initial_backoff_ms == 50);
        anyhow::ensure!(p.max_backoff_ms == 2_000);
        anyhow::ensure!((p.jitter_ratio - 0.25).abs() < f64::EPSILON);
        anyhow::ensure!(p.buffered_max_bytes == 8 << 20);
        Ok(())
    }

    #[test]
    fn resolve_origin_retry_partial_section_inherits_defaults() -> anyhow::Result<()> {
        // `#[serde(default)]` on RetryPolicy fills missing fields from
        // Default. Pinning the contract here so a future struct-level
        // attribute change doesn't silently break partial TOML.
        let toml = "[cache.origin_retry]\nmax_retries = 5\n";
        let file: crate::config::FileConfig = ::toml::from_str(toml)?;
        let p = file
            .cache
            .as_ref()
            .and_then(|c| c.origin_retry.as_ref())
            .ok_or_else(|| anyhow::anyhow!("origin_retry missing"))?;
        anyhow::ensure!(p.max_retries == 5);
        anyhow::ensure!(p.initial_backoff_ms == 100, "default carried through");
        anyhow::ensure!(p.max_backoff_ms == 10_000, "default carried through");
        Ok(())
    }

    #[test]
    fn resolve_origin_retry_max_retries_zero_is_valid() -> anyhow::Result<()> {
        // `0` opts out and is the documented disable knob; resolution
        // must not reject it.
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            origin_retry: Some(decdn_config_types::RetryPolicy {
                max_retries: 0,
                ..decdn_config_types::RetryPolicy::default()
            }),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        anyhow::ensure!(resolved.origin_retry.max_retries == 0);
        Ok(())
    }

    #[test]
    fn resolve_origin_retry_rejects_initial_above_max() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            origin_retry: Some(decdn_config_types::RetryPolicy {
                initial_backoff_ms: 2_000,
                max_backoff_ms: 1_000,
                ..decdn_config_types::RetryPolicy::default()
            }),
            ..types::CacheConfig::default()
        };
        let err = resolve_cache(&cli, Some(&file), Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected error"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("schedule never grows"),
            "error message should explain why initial>max is rejected, got: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_origin_retry_rejects_jitter_out_of_range() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        for bad in [-0.1, 1.5, f64::NAN, f64::INFINITY] {
            let file = types::CacheConfig {
                origin_retry: Some(decdn_config_types::RetryPolicy {
                    jitter_ratio: bad,
                    ..decdn_config_types::RetryPolicy::default()
                }),
                ..types::CacheConfig::default()
            };
            let err = resolve_cache(&cli, Some(&file), Path::new("/tmp"))
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected rejection for jitter={bad}"))?;
            let msg = format!("{err:#}");
            anyhow::ensure!(
                msg.contains("jitter_ratio"),
                "error message should reference jitter_ratio, got: {msg}"
            );
        }
        Ok(())
    }

    #[test]
    fn resolve_origin_retry_rejects_buffered_max_bytes_above_ceiling() -> anyhow::Result<()> {
        // #519 hard ceiling: buffered_max_bytes > 64 MiB is almost
        // certainly an operator typo. The streaming abort+restart
        // path covers any blob size without raising this knob.
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            origin_retry: Some(decdn_config_types::RetryPolicy {
                buffered_max_bytes: MAX_BUFFERED_MAX_BYTES + 1,
                ..decdn_config_types::RetryPolicy::default()
            }),
            ..types::CacheConfig::default()
        };
        let err = resolve_cache(&cli, Some(&file), Path::new("/tmp"))
            .err()
            .ok_or_else(|| {
                anyhow::anyhow!("expected rejection for too-large buffered_max_bytes")
            })?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("buffered_max_bytes"),
            "error message should reference buffered_max_bytes, got: {msg}"
        );
        anyhow::ensure!(
            msg.contains("hard ceiling"),
            "error message should explain the ceiling rationale, got: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_origin_retry_accepts_buffered_max_bytes_at_ceiling() -> anyhow::Result<()> {
        // Boundary: exactly the ceiling is allowed; only > ceiling is
        // rejected. Pins the inclusive-bound semantics so a future
        // edit can't accidentally flip the inequality.
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            origin_retry: Some(decdn_config_types::RetryPolicy {
                buffered_max_bytes: MAX_BUFFERED_MAX_BYTES,
                ..decdn_config_types::RetryPolicy::default()
            }),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        anyhow::ensure!(resolved.origin_retry.buffered_max_bytes == MAX_BUFFERED_MAX_BYTES);
        Ok(())
    }

    #[test]
    fn resolve_origin_retry_accepts_buffered_max_bytes_zero() -> anyhow::Result<()> {
        // `0` disables the buffer path entirely (operator opt-out;
        // documented in `RetryPolicy::buffered_max_bytes`). Must
        // resolve cleanly.
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            origin_retry: Some(decdn_config_types::RetryPolicy {
                buffered_max_bytes: 0,
                ..decdn_config_types::RetryPolicy::default()
            }),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        anyhow::ensure!(resolved.origin_retry.buffered_max_bytes == 0);
        Ok(())
    }

    #[test]
    fn resolve_cache_defaults_satisfy_invariant() -> anyhow::Result<()> {
        // Regression guard: if either default changes, the pair must still
        // satisfy `max_blob < cache_size`. Lives here so a future edit to
        // the DEFAULT_* constants can't silently reintroduce the #221 bug.
        let cli = cache_cli(None, None);
        let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
        anyhow::ensure!(
            resolved.max_blob_size_mb < resolved.cache_size_mb,
            "defaults violate invariant: max_blob={} cache_size={}",
            resolved.max_blob_size_mb,
            resolved.cache_size_mb
        );
        Ok(())
    }

    fn obs_cli(
        metrics_port: Option<u16>,
        admin_port: Option<u16>,
    ) -> crate::cli::run::ObservabilityArgs {
        crate::cli::run::ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port,
            metrics_bind: None,
            admin_port,
            otlp_endpoint: None,
        }
    }

    fn net(port: u16) -> ResolvedNetwork {
        ResolvedNetwork {
            bind_port: port,
            relay_url: None,
            enable_0rtt: true,
        }
    }

    fn obs(port: u16) -> ResolvedObservability {
        ResolvedObservability {
            log_level: crate::cli::common::LogLevel::default(),
            log_format: crate::cli::common::LogFormat::default(),
            metrics_port: port,
            metrics_bind: DEFAULT_METRICS_BIND,
            admin_port: None,
            otlp_endpoint: None,
        }
    }

    fn obs_with_admin(metrics: u16, admin: u16) -> ResolvedObservability {
        ResolvedObservability {
            log_level: crate::cli::common::LogLevel::default(),
            log_format: crate::cli::common::LogFormat::default(),
            metrics_port: metrics,
            metrics_bind: DEFAULT_METRICS_BIND,
            admin_port: Some(admin),
            otlp_endpoint: None,
        }
    }

    #[test]
    fn resolve_observability_defaults_admin_port_to_9191() -> anyhow::Result<()> {
        let obs = resolve_observability(&obs_cli(None, None), None)?;
        anyhow::ensure!(
            obs.admin_port == Some(DEFAULT_ADMIN_PORT),
            "got: {:?}",
            obs.admin_port
        );
        anyhow::ensure!(obs.metrics_port == DEFAULT_METRICS_PORT);
        Ok(())
    }

    #[test]
    fn validate_port_layout_rejects_bind_equal_metrics() {
        let err = validate_port_layout(&net(9090), &obs(9090))
            .expect_err("equal bind and metrics ports should be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("network.bind_port") && msg.contains("metrics_port"),
            "error should name both fields: {msg}"
        );
    }

    #[test]
    fn validate_port_layout_accepts_distinct_ports() -> anyhow::Result<()> {
        validate_port_layout(&net(4433), &obs(9090))?;
        Ok(())
    }

    #[test]
    fn resolve_observability_admin_port_zero_disables() -> anyhow::Result<()> {
        let obs = resolve_observability(&obs_cli(None, Some(0)), None)?;
        anyhow::ensure!(obs.admin_port.is_none(), "got: {:?}", obs.admin_port);
        Ok(())
    }

    #[test]
    fn resolve_observability_file_admin_port_zero_disables() -> anyhow::Result<()> {
        // Closes #300. `Option<u16>` distinguishes absent (None) from
        // explicit zero (Some(0)) under serde+toml, so the file leg of the
        // merge can carry the operator's "disable" intent through to the
        // resolved config without ambiguity.
        let file = types::ObservabilityConfig {
            admin_port: Some(0),
            ..Default::default()
        };
        let obs = resolve_observability(&obs_cli(None, None), Some(&file))?;
        anyhow::ensure!(obs.admin_port.is_none(), "got: {:?}", obs.admin_port);
        Ok(())
    }

    #[test]
    fn file_admin_port_zero_deserializes_as_some_zero() -> anyhow::Result<()> {
        // Locks the deserializer invariant #300 was filed against: an
        // explicit `admin_port = 0` must round-trip to Some(0), distinct
        // from a missing key which round-trips to None.
        let absent: types::ObservabilityConfig = toml::from_str("")?;
        anyhow::ensure!(absent.admin_port.is_none(), "got: {:?}", absent.admin_port);
        let zero: types::ObservabilityConfig = toml::from_str("admin_port = 0")?;
        anyhow::ensure!(zero.admin_port == Some(0), "got: {:?}", zero.admin_port);
        Ok(())
    }

    #[test]
    fn validate_port_layout_allows_well_known_port() -> anyhow::Result<()> {
        // Well-known range only warns (via eprintln), never hard-fails —
        // operators have legitimate reasons to bind there (QUIC on 443,
        // privileged setup scripts, CAP_NET_BIND_SERVICE).
        validate_port_layout(&net(443), &obs(9090))?;
        Ok(())
    }

    #[test]
    fn validate_port_layout_rejects_admin_eq_metrics() {
        let err = validate_port_layout(&net(4433), &obs_with_admin(9090, 9090))
            .expect_err("equal admin and metrics ports should be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("admin_port") && msg.contains("metrics_port"),
            "error should name both fields: {msg}"
        );
    }

    #[test]
    fn validate_port_layout_rejects_bind_equal_admin() {
        let err = validate_port_layout(&net(9191), &obs_with_admin(9090, 9191))
            .expect_err("equal bind and admin ports should be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("bind_port") && msg.contains("admin_port"),
            "error should name both fields: {msg}"
        );
    }

    #[test]
    fn validate_port_layout_skips_admin_when_disabled() -> anyhow::Result<()> {
        // admin_port = None means admin surface is disabled; collision
        // rules that involve admin must be skipped even when other
        // numbers happen to coincide.
        validate_port_layout(&net(4433), &obs(9090))?;
        Ok(())
    }

    #[test]
    fn resolve_observability_cli_admin_port_overrides_file() -> anyhow::Result<()> {
        let file = types::ObservabilityConfig {
            admin_port: Some(1111),
            ..Default::default()
        };
        let obs = resolve_observability(&obs_cli(None, Some(2222)), Some(&file))?;
        anyhow::ensure!(obs.admin_port == Some(2222), "got: {:?}", obs.admin_port);
        Ok(())
    }

    #[test]
    fn validate_port_layout_allows_both_zero() -> anyhow::Result<()> {
        // Port 0 requests an OS-assigned ephemeral port, so two zeros
        // resolve to two distinct ports at bind time and cannot collide.
        // The equality check must not fire here.
        validate_port_layout(&net(0), &obs(0))?;
        Ok(())
    }

    #[test]
    fn validate_port_layout_allows_zero_with_nonzero() -> anyhow::Result<()> {
        // One ephemeral + one fixed is unambiguously collision-free.
        validate_port_layout(&net(0), &obs(9090))?;
        validate_port_layout(&net(4433), &obs(0))?;
        Ok(())
    }

    #[test]
    fn validate_port_layout_aggregates_three_way_collision() {
        // All three ports set to the same number: each pair surfaces its
        // own problem (bind/metrics, bind/admin, admin/metrics) instead of
        // the first collision masking the other two. Locks the comment in
        // `validate_port_layout_into` that promises accumulation across
        // pairs, and verifies each pair carries a distinct field label so
        // the bag can hold all three without one overwriting another.
        let mut bag = ConfigErrorBag::new();
        validate_port_layout_into(&net(7000), &obs_with_admin(7000, 7000), &mut bag);
        let err = bag
            .into_result()
            .expect_err("three colliding ports must report problems");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("3 problem(s):"),
            "expected three pair problems: {msg}"
        );
        for needle in [
            "network.bind_port vs observability.metrics_port",
            "network.bind_port vs observability.admin_port",
            "observability.admin_port vs observability.metrics_port",
        ] {
            assert!(msg.contains(needle), "missing pair label {needle:?}: {msg}");
        }
    }

    #[test]
    fn resolve_observability_metrics_bind_defaults_to_localhost() -> anyhow::Result<()> {
        let obs = resolve_observability(&obs_cli(None, None), None)?;
        assert_eq!(
            obs.metrics_bind,
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );
        Ok(())
    }

    #[test]
    fn resolve_observability_metrics_bind_from_cli() -> anyhow::Result<()> {
        let mut cli = obs_cli(None, None);
        cli.metrics_bind = Some(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
        let obs = resolve_observability(&cli, None)?;
        assert_eq!(
            obs.metrics_bind,
            std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        );
        Ok(())
    }

    #[test]
    fn resolve_observability_metrics_bind_from_file() -> anyhow::Result<()> {
        let file = types::ObservabilityConfig {
            metrics_bind: Some(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)),
            ..Default::default()
        };
        let obs = resolve_observability(&obs_cli(None, None), Some(&file))?;
        assert_eq!(
            obs.metrics_bind,
            std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        );
        Ok(())
    }

    #[test]
    fn resolve_observability_metrics_bind_cli_overrides_file() -> anyhow::Result<()> {
        let mut cli = obs_cli(None, None);
        cli.metrics_bind = Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        let file = types::ObservabilityConfig {
            metrics_bind: Some(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)),
            ..Default::default()
        };
        let obs = resolve_observability(&cli, Some(&file))?;
        assert_eq!(
            obs.metrics_bind,
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );
        Ok(())
    }

    #[test]
    fn resolve_observability_rejects_otlp_endpoint_bad_scheme() {
        let mut cli = obs_cli(None, None);
        cli.otlp_endpoint = Some("grpc://collector:4317".to_string());
        let err =
            resolve_observability(&cli, None).expect_err("non-http scheme should be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("http://") && msg.contains("https://"),
            "error should mention valid schemes: {msg}"
        );
    }

    #[test]
    fn resolve_observability_accepts_valid_otlp_endpoint() -> anyhow::Result<()> {
        let mut cli = obs_cli(None, None);
        cli.otlp_endpoint = Some("http://collector:4317".to_string());
        let obs = resolve_observability(&cli, None)?;
        assert_eq!(obs.otlp_endpoint.as_deref(), Some("http://collector:4317"));
        Ok(())
    }

    // Closes #268. The three-layer merge is CLI/env > TOML file > default;
    // `resolve_*_cli_overrides_file*` tests cover the "Option::Some on
    // RunArgs beats file" leg. The remaining leg — that clap populates
    // RunArgs from `DECDN_*` env vars so those Option::Some values are
    // there to win — lives in this test.
    //
    // Done declaratively (via clap's `Command` introspection) rather than
    // by setting process env vars: `std::env::set_var` is `unsafe` under
    // edition 2024 and the workspace lints forbid `unsafe_code`. A dev-
    // dependency like `temp-env` would work but costs more than the
    // regression risk we're pinning here — a dropped `env = "DECDN_*"`
    // attribute or a field rename fails this test immediately.
    #[test]
    fn run_subcommand_args_are_wired_to_decdn_env_vars() {
        use clap::{Args, CommandFactory, Parser};

        // The user CLI no longer has a `run` subcommand (#421 — the
        // daemon binary `decdn-node` owns it). `RunArgs` itself
        // remains in `decdn-common` because `decdn config validate`
        // flattens it for env-var parity with the daemon. Wrap
        // `RunArgs` in a local `Parser` and walk *its* args — this
        // is the same set of env mappings the daemon's
        // `decdn-node run` exposes and that `decdn config validate`
        // honours.
        #[derive(Parser, Debug)]
        struct RunWrap {
            #[command(flatten)]
            run: crate::cli::RunArgs,
        }

        let _ = RunWrap::command(); // surface a parse error if RunArgs is broken
        let run = <crate::cli::RunArgs as Args>::augment_args(clap::Command::new("run"));

        // One line per DECDN_* env var operators may set. Adding a new
        // `#[arg(env = "DECDN_*")]` field without adding it here is a test
        // failure — which is the point. Arg IDs are the Rust field name
        // (underscored), not the `--long` form, because that's what clap
        // stores on the `Arg` struct.
        let expected: &[(&str, &str)] = &[
            ("data_dir", "DECDN_DATA_DIR"),
            ("region", "DECDN_REGION"),
            ("bind_port", "DECDN_BIND_PORT"),
            ("relay_url", "DECDN_RELAY_URL"),
            ("rpc_url", "DECDN_RPC_URL"),
            ("eth_keystore", "DECDN_ETH_KEYSTORE"),
            ("keystore_password_file", "DECDN_KEYSTORE_PASSWORD_FILE"),
            ("payment_channel_address", "DECDN_PAYMENT_CHANNEL_ADDRESS"),
            ("capacity_bond_address", "DECDN_CAPACITY_BOND_ADDRESS"),
            ("slash_judge_address", "DECDN_SLASH_JUDGE_ADDRESS"),
            ("chain_id", "DECDN_CHAIN_ID"),
            ("cache_dir", "DECDN_CACHE_DIR"),
            ("cache_size_mb", "DECDN_CACHE_SIZE_MB"),
            ("max_blob_size_mb", "DECDN_MAX_BLOB_SIZE_MB"),
            ("max_probe_holds", "DECDN_MAX_PROBE_HOLDS"),
            (
                "stake_lane_reserved_holds",
                "DECDN_STAKE_LANE_RESERVED_HOLDS",
            ),
            ("rate_per_mb", "DECDN_RATE_PER_MB"),
            ("delivery_floor", "DECDN_DELIVERY_FLOOR"),
            ("delivery_ceiling", "DECDN_DELIVERY_CEILING"),
            ("log_level", "DECDN_LOG_LEVEL"),
            ("log_format", "DECDN_LOG_FORMAT"),
            ("metrics_port", "DECDN_METRICS_PORT"),
            ("metrics_bind", "DECDN_METRICS_BIND"),
            ("admin_port", "DECDN_ADMIN_PORT"),
            ("otlp_endpoint", "DECDN_OTLP_ENDPOINT"),
        ];

        for (arg_id, env_name) in expected {
            let arg = run
                .get_arguments()
                .find(|a| a.get_id() == arg_id)
                .unwrap_or_else(|| panic!("run subcommand missing arg {arg_id:?}"));
            let env = arg.get_env().unwrap_or_else(|| {
                panic!("arg {arg_id:?} has no env mapping (expected {env_name:?})")
            });
            assert_eq!(
                env.to_str(),
                Some(*env_name),
                "arg {arg_id:?} env mapping drifted"
            );
        }

        // Reverse direction: catch a newly-added `#[arg(env = "DECDN_*")]`
        // that wasn't added to `expected`. Otherwise this test only
        // enforces "don't remove env mappings", not "don't silently add
        // undocumented ones".
        let all_env_args: Vec<String> = run
            .get_arguments()
            .filter(|a| a.get_env().is_some())
            .map(|a| a.get_id().to_string())
            .collect();
        assert_eq!(
            all_env_args.len(),
            expected.len(),
            "env-bearing args drifted; declared: {all_env_args:?}, expected {}",
            expected.len()
        );
    }

    fn data_dir_with_keystore() -> anyhow::Result<TempDir> {
        let dir = TempDir::new()?;
        std::fs::write(dir.path().join("keystore.json"), "")?;
        Ok(dir)
    }

    #[test]
    fn resolve_blockchain_names_correct_field_for_bad_address() -> anyhow::Result<()> {
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some("0xNOTHEX".to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let dir = data_dir_with_keystore()?;
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected resolve_blockchain to fail on bad staking address");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("capacity_bond_address"),
            "error should name capacity_bond_address: {msg}"
        );
        assert!(
            !msg.contains("payment_channel_address"),
            "error must not name the valid field: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_names_correct_field_for_bad_payment_address() -> anyhow::Result<()> {
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some("0xNOTHEX".to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let dir = data_dir_with_keystore()?;
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected resolve_blockchain to fail on bad payment address");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("payment_channel_address"),
            "error should name payment_channel_address: {msg}"
        );
        assert!(
            !msg.contains("capacity_bond_address"),
            "error must not name the valid field: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_fails_when_keystore_missing() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected resolve_blockchain to fail on missing keystore");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("invalid eth_keystore"),
            "error should name eth_keystore: {msg}"
        );
        assert!(
            msg.contains("cannot access"),
            "error should describe access failure: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_fails_when_cli_keystore_override_missing() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let bogus = dir.path().join("does-not-exist.json");
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: Some(bogus),
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected resolve_blockchain to fail on bogus --eth-keystore");
        };
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid eth_keystore"), "{msg}");
        assert!(
            msg.contains("does-not-exist.json"),
            "error should cite the overridden path: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_rejects_directory_as_keystore() -> anyhow::Result<()> {
        // `File::open` accepts a directory on Linux, so the explicit `is_file`
        // check is the only thing standing between the node and a later panic.
        let dir = TempDir::new()?;
        std::fs::create_dir(dir.path().join("keystore.json"))?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected resolve_blockchain to fail on directory keystore");
        };
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid eth_keystore"), "{msg}");
        assert!(
            msg.contains("not a regular file"),
            "error should say 'not a regular file': {msg}"
        );
        Ok(())
    }

    // -- helpers for full RunArgs construction (issue #217) ---------------

    fn empty_identity_args() -> crate::cli::run::IdentityArgs {
        crate::cli::run::IdentityArgs {
            data_dir: None,
            region: None,
        }
    }

    fn empty_network_args() -> crate::cli::run::NetworkArgs {
        crate::cli::run::NetworkArgs {
            bind_port: None,
            relay_url: None,
        }
    }

    fn empty_blockchain_args() -> crate::cli::run::BlockchainArgs {
        crate::cli::run::BlockchainArgs {
            rpc_url: None,
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: None,
            capacity_bond_address: None,
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        }
    }

    fn empty_cache_args() -> crate::cli::run::CacheArgs {
        crate::cli::run::CacheArgs {
            cache_dir: None,
            cache_size_mb: None,
            max_blob_size_mb: None,
            max_probe_holds: None,
            stake_lane_reserved_holds: None,
        }
    }

    fn empty_payment_args() -> crate::cli::run::PaymentArgs {
        crate::cli::run::PaymentArgs {
            rate_per_mb: None,
            delivery_floor: None,
            delivery_ceiling: None,
        }
    }

    fn empty_observability_args() -> crate::cli::run::ObservabilityArgs {
        crate::cli::run::ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        }
    }

    fn empty_run_args() -> RunArgs {
        RunArgs {
            identity: empty_identity_args(),
            network: empty_network_args(),
            blockchain: empty_blockchain_args(),
            cache: empty_cache_args(),
            payment: empty_payment_args(),
            observability: empty_observability_args(),
        }
    }

    // ---- resolve_identity: CLI > file, file used when CLI omits ----------

    #[test]
    fn resolve_identity_cli_region_overrides_file() -> anyhow::Result<()> {
        let mut cli = empty_identity_args();
        cli.region = Some("de".to_string());
        cli.data_dir = Some(PathBuf::from("/tmp/cli-data"));
        let file = types::IdentityConfig {
            data_dir: Some(PathBuf::from("/tmp/file-data")),
            region: Some("us".to_string()),
        };
        let resolved = resolve_identity(&cli, Some(&file))?;
        assert_eq!(resolved.region.as_deref(), Some("DE"));
        assert_eq!(resolved.data_dir, PathBuf::from("/tmp/cli-data"));
        Ok(())
    }

    #[test]
    fn resolve_identity_falls_back_to_file_when_cli_absent() -> anyhow::Result<()> {
        let cli = empty_identity_args();
        let file = types::IdentityConfig {
            data_dir: Some(PathBuf::from("/tmp/file-data")),
            region: Some("sg".to_string()),
        };
        let resolved = resolve_identity(&cli, Some(&file))?;
        // Region is normalized to upper case.
        assert_eq!(resolved.region.as_deref(), Some("SG"));
        assert_eq!(resolved.data_dir, PathBuf::from("/tmp/file-data"));
        Ok(())
    }

    // ---- resolve_network: CLI > file, default when both omit -------------

    #[test]
    fn resolve_network_cli_bind_port_overrides_file() {
        let mut cli = empty_network_args();
        cli.bind_port = Some(5555);
        let file = types::NetworkConfig {
            bind_port: Some(6666),
            relay_url: None,
            enable_0rtt: None,
        };
        let resolved = resolve_network(&cli, Some(&file));
        assert_eq!(resolved.bind_port, 5555);
    }

    #[test]
    fn resolve_network_uses_file_when_cli_absent() {
        let cli = empty_network_args();
        let file = types::NetworkConfig {
            bind_port: Some(6666),
            relay_url: Some("https://relay.example".to_string()),
            enable_0rtt: None,
        };
        let resolved = resolve_network(&cli, Some(&file));
        assert_eq!(resolved.bind_port, 6666);
        assert_eq!(resolved.relay_url.as_deref(), Some("https://relay.example"));
    }

    #[test]
    fn resolve_network_enable_0rtt_defaults_true_and_file_overrides() {
        let cli = empty_network_args();

        // Absent in file => built-in default (0-RTT on).
        let none = types::NetworkConfig {
            bind_port: None,
            relay_url: None,
            enable_0rtt: None,
        };
        assert!(resolve_network(&cli, Some(&none)).enable_0rtt);
        assert!(resolve_network(&cli, None).enable_0rtt);

        // Explicit `false` in file is the operational kill switch.
        let off = types::NetworkConfig {
            bind_port: None,
            relay_url: None,
            enable_0rtt: Some(false),
        };
        assert!(!resolve_network(&cli, Some(&off)).enable_0rtt);
    }

    #[test]
    fn resolve_network_default_bind_port_when_unset() {
        let cli = empty_network_args();
        let resolved = resolve_network(&cli, None);
        assert_eq!(resolved.bind_port, DEFAULT_BIND_PORT);
        assert!(resolved.relay_url.is_none());
    }

    // ---- resolve_blockchain: CLI > file, missing-required errors ---------

    #[test]
    fn resolve_blockchain_cli_rpc_url_overrides_file() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://cli-wins.example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let file = types::BlockchainConfig {
            rpc_url: Some("https://file-loses.example/rpc".to_string()),
            eth_keystore: None,
            payment_channel_address: None,
            capacity_bond_address: None,
            rpc_watchdog_interval_sec: None,
            redeem_threshold_micro_usdc: None,
            buyer_deposit_micro_usdc: None,
            buyer_max_approve: None,
            settlement_auto_threshold_micro_usdc: None,
            settlement_auto_by_voucher_nonce_span: None,
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
        // url::Url normalisation appends a trailing path on bare-host URLs;
        // both inputs already include `/rpc`, so the prefix match suffices
        // and is robust against future normalisation tweaks.
        assert!(
            resolved.rpc_url.starts_with("https://cli-wins.example/rpc"),
            "expected CLI rpc_url to win, got {}",
            resolved.rpc_url,
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_uses_file_rpc_url_when_cli_absent() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = empty_blockchain_args();
        let file = types::BlockchainConfig {
            rpc_url: Some("https://file-only.example/rpc".to_string()),
            eth_keystore: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            rpc_watchdog_interval_sec: None,
            redeem_threshold_micro_usdc: None,
            buyer_deposit_micro_usdc: None,
            buyer_max_approve: None,
            settlement_auto_threshold_micro_usdc: None,
            settlement_auto_by_voucher_nonce_span: None,
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
        assert!(
            resolved
                .rpc_url
                .starts_with("https://file-only.example/rpc"),
            "expected file rpc_url to be used, got {}",
            resolved.rpc_url,
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_errors_when_rpc_url_missing() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: None,
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected error when rpc_url missing");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("--rpc-url") && msg.contains("rpc_url"),
            "error should mention rpc_url and the flag form: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_errors_when_payment_channel_address_missing() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: None,
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected error when payment_channel_address missing");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("payment_channel_address"),
            "error should mention payment_channel_address: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_errors_when_capacity_bond_address_missing() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: None,
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected error when capacity_bond_address missing");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("capacity_bond_address"),
            "error should mention capacity_bond_address: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_errors_when_slash_judge_address_missing() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: None,
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected error when slash_judge_address missing");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("slash_judge_address"),
            "error should mention slash_judge_address: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_rejects_zero_slash_judge_address() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some("0x0000000000000000000000000000000000000000".to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected error for zero slash_judge_address");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("slash_judge_address") && msg.contains("zero address"),
            "error should reject the zero address: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_rejects_zero_chain_id() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: Some(0),
        };
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected error for chain_id=0");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("chain_id") && msg.contains("must not be 0"),
            "error should reject chain_id=0: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_treats_empty_string_rpc_url_as_missing() -> anyhow::Result<()> {
        // Mirror the `.filter(|s| !s.is_empty())` guard: a blank value (e.g.
        // `DECDN_RPC_URL=""`) must surface the same "missing" diagnostic as
        // an absent value rather than silently passing `""` to url::Url.
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some(String::new()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected error when rpc_url is empty string");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("--rpc-url"),
            "empty rpc_url should surface as missing: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_rejects_small_nonzero_watchdog_interval() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let file = types::BlockchainConfig {
            rpc_url: None,
            eth_keystore: None,
            payment_channel_address: None,
            capacity_bond_address: None,
            rpc_watchdog_interval_sec: Some(1),
            redeem_threshold_micro_usdc: None,
            buyer_deposit_micro_usdc: None,
            buyer_max_approve: None,
            settlement_auto_threshold_micro_usdc: None,
            settlement_auto_by_voucher_nonce_span: None,
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
            anyhow::bail!("expected error when watchdog interval is below the minimum");
        };
        let msg = format!("{err:#}");
        let expected_min = format!("minimum {MIN_RPC_WATCHDOG_INTERVAL_SEC}s");
        assert!(
            msg.contains("rpc_watchdog_interval_sec") && msg.contains(&expected_min),
            "error should mention the field and the {expected_min} floor: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_rejects_zero_redeem_threshold() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let file = types::BlockchainConfig {
            rpc_url: None,
            eth_keystore: None,
            payment_channel_address: None,
            capacity_bond_address: None,
            rpc_watchdog_interval_sec: None,
            redeem_threshold_micro_usdc: Some(0),
            buyer_deposit_micro_usdc: None,
            buyer_max_approve: None,
            settlement_auto_threshold_micro_usdc: None,
            settlement_auto_by_voucher_nonce_span: None,
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
            anyhow::bail!("expected error when redeem threshold is 0");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("redeem_threshold_micro_usdc"),
            "error should name the field: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_rejects_zero_auto_settlement_threshold() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let file = types::BlockchainConfig {
            rpc_url: None,
            eth_keystore: None,
            payment_channel_address: None,
            capacity_bond_address: None,
            rpc_watchdog_interval_sec: None,
            redeem_threshold_micro_usdc: None,
            buyer_deposit_micro_usdc: None,
            buyer_max_approve: None,
            settlement_auto_threshold_micro_usdc: Some(0),
            settlement_auto_by_voucher_nonce_span: None,
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
            anyhow::bail!("expected error when auto-settlement value threshold is 0");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("settlement_auto_threshold_micro_usdc"),
            "error should name the field: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_rejects_zero_auto_settlement_voucher_nonce_span() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let file = types::BlockchainConfig {
            rpc_url: None,
            eth_keystore: None,
            payment_channel_address: None,
            capacity_bond_address: None,
            rpc_watchdog_interval_sec: None,
            redeem_threshold_micro_usdc: None,
            buyer_deposit_micro_usdc: None,
            buyer_max_approve: None,
            settlement_auto_threshold_micro_usdc: None,
            settlement_auto_by_voucher_nonce_span: Some(0),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
            anyhow::bail!("expected error when auto-settlement voucher nonce span is 0");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("settlement_auto_by_voucher_nonce_span"),
            "error should name the field: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_auto_settlement_disabled_by_default() -> anyhow::Result<()> {
        // Absent keys => both auto-settlement triggers disabled (`None`), so a
        // node that never sets them behaves exactly as before #742.
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let resolved = resolve_blockchain(&cli, None, dir.path())?;
        assert_eq!(resolved.settlement_auto_threshold_micro_usdc, None);
        assert_eq!(resolved.settlement_auto_by_voucher_nonce_span, None);
        Ok(())
    }

    #[test]
    fn resolve_blockchain_accepts_positive_auto_settlement_thresholds() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let file = types::BlockchainConfig {
            rpc_url: None,
            eth_keystore: None,
            payment_channel_address: None,
            capacity_bond_address: None,
            rpc_watchdog_interval_sec: None,
            redeem_threshold_micro_usdc: None,
            buyer_deposit_micro_usdc: None,
            buyer_max_approve: None,
            settlement_auto_threshold_micro_usdc: Some(50_000_000),
            settlement_auto_by_voucher_nonce_span: Some(1_000),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
        assert_eq!(
            resolved.settlement_auto_threshold_micro_usdc,
            Some(50_000_000)
        );
        assert_eq!(resolved.settlement_auto_by_voucher_nonce_span, Some(1_000));
        Ok(())
    }

    #[test]
    fn resolve_blockchain_accepts_zero_watchdog_interval() -> anyhow::Result<()> {
        // `0` is the documented disable sentinel and must bypass the floor.
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let file = types::BlockchainConfig {
            rpc_url: None,
            eth_keystore: None,
            payment_channel_address: None,
            capacity_bond_address: None,
            rpc_watchdog_interval_sec: Some(0),
            redeem_threshold_micro_usdc: None,
            buyer_deposit_micro_usdc: None,
            buyer_max_approve: None,
            settlement_auto_threshold_micro_usdc: None,
            settlement_auto_by_voucher_nonce_span: None,
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
        assert_eq!(resolved.rpc_watchdog_interval_sec, 0);
        Ok(())
    }

    #[test]
    fn resolve_blockchain_accepts_min_watchdog_interval() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let file = types::BlockchainConfig {
            rpc_url: None,
            eth_keystore: None,
            payment_channel_address: None,
            capacity_bond_address: None,
            rpc_watchdog_interval_sec: Some(MIN_RPC_WATCHDOG_INTERVAL_SEC),
            redeem_threshold_micro_usdc: None,
            buyer_deposit_micro_usdc: None,
            buyer_max_approve: None,
            settlement_auto_threshold_micro_usdc: None,
            settlement_auto_by_voucher_nonce_span: None,
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
        assert_eq!(
            resolved.rpc_watchdog_interval_sec,
            MIN_RPC_WATCHDOG_INTERVAL_SEC
        );
        Ok(())
    }

    #[test]
    fn resolve_blockchain_applies_default_watchdog_interval_when_absent() -> anyhow::Result<()> {
        // Pins the no-config bootstrap path: if a future change moved
        // DEFAULT below MIN (or to 0), every operator without an explicit
        // setting would silently lose the watchdog. Mirrors the gossip
        // analogue at `resolve_gossip_applies_defaults_when_absent`.
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            keystore_password_file: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            capacity_bond_address: Some(GOOD_ADDR.to_string()),
            slash_judge_address: Some(GOOD_ADDR.to_string()),
            chain_id: None,
        };
        let resolved = resolve_blockchain(&cli, None, dir.path())?;
        assert_eq!(
            resolved.rpc_watchdog_interval_sec,
            DEFAULT_RPC_WATCHDOG_INTERVAL_SEC
        );
        Ok(())
    }

    // ---- resolve_cache: CLI > file, defaults, tilde expansion ------------

    #[test]
    fn resolve_cache_cli_cache_dir_overrides_file_and_expands_tilde() -> anyhow::Result<()> {
        // Hermetic: inject a stub home so the assertion holds whether or
        // not the host's `dirs::home_dir()` returns Some, and so the
        // assertion exercises the documented behaviour (tilde-expand
        // against $HOME).
        let home_dir = TempDir::new()?;
        let home = home_dir.path().to_path_buf();
        let mut cli = empty_cache_args();
        cli.cache_dir = Some(PathBuf::from("~/from-cli"));
        let file = types::CacheConfig {
            cache_dir: Some(PathBuf::from("/from/file")),
            cache_size_mb: None,
            max_blob_size_mb: None,
            ..Default::default()
        };
        let resolved = common::test_support::with_home_override(Some(&home), || {
            resolve_cache(&cli, Some(&file), Path::new("/data-dir"))
        })?;
        assert_eq!(resolved.cache_dir, home.join("from-cli"));
        Ok(())
    }

    #[test]
    fn resolve_cache_cli_cache_dir_passes_through_when_home_unavailable() -> anyhow::Result<()> {
        // Sibling of the above: the documented contract is "log + leave
        // path unchanged" when `dirs::home_dir()` is None (see
        // `cli::common::expand_tilde`). Verify resolve_cache honours it.
        let mut cli = empty_cache_args();
        cli.cache_dir = Some(PathBuf::from("~/from-cli"));
        let resolved = common::test_support::with_home_override(None, || {
            resolve_cache(&cli, None, Path::new("/data-dir"))
        })?;
        assert_eq!(resolved.cache_dir, PathBuf::from("~/from-cli"));
        Ok(())
    }

    #[test]
    fn resolve_cache_uses_file_cache_dir_when_cli_absent() -> anyhow::Result<()> {
        let cli = empty_cache_args();
        let file = types::CacheConfig {
            cache_dir: Some(PathBuf::from("/from/file")),
            cache_size_mb: None,
            max_blob_size_mb: None,
            ..Default::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/data-dir"))?;
        assert_eq!(resolved.cache_dir, PathBuf::from("/from/file"));
        Ok(())
    }

    #[test]
    fn resolve_cache_falls_back_to_data_dir_subdirectory() -> anyhow::Result<()> {
        // No CLI, no file: the documented fallback is `<data-dir>/cache`.
        let cli = empty_cache_args();
        let resolved = resolve_cache(&cli, None, Path::new("/data-dir"))?;
        assert_eq!(resolved.cache_dir, PathBuf::from("/data-dir/cache"));
        Ok(())
    }

    #[test]
    fn resolve_cache_default_size_constants_apply_when_unset() -> anyhow::Result<()> {
        let cli = empty_cache_args();
        let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
        assert_eq!(resolved.cache_size_mb, DEFAULT_CACHE_SIZE_MB);
        assert_eq!(resolved.max_blob_size_mb, DEFAULT_MAX_BLOB_SIZE_MB);
        Ok(())
    }

    #[test]
    fn resolve_cache_cli_size_overrides_file_size() -> anyhow::Result<()> {
        let mut cli = empty_cache_args();
        cli.cache_size_mb = Some(2_048);
        cli.max_blob_size_mb = Some(256);
        let file = types::CacheConfig {
            cache_dir: None,
            cache_size_mb: Some(99_999),
            max_blob_size_mb: Some(50_000),
            ..Default::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        assert_eq!(resolved.cache_size_mb, 2_048);
        assert_eq!(resolved.max_blob_size_mb, 256);
        Ok(())
    }

    #[test]
    fn resolve_cache_uses_file_size_when_cli_absent() -> anyhow::Result<()> {
        let cli = empty_cache_args();
        let file = types::CacheConfig {
            cache_dir: None,
            cache_size_mb: Some(2_048),
            max_blob_size_mb: Some(256),
            ..Default::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        assert_eq!(resolved.cache_size_mb, 2_048);
        assert_eq!(resolved.max_blob_size_mb, 256);
        Ok(())
    }

    // ---- resolve_payment: CLI > file (positive value path) ---------------

    #[test]
    fn resolve_payment_cli_rate_overrides_file_rate() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: Some(99),
            delivery_floor: None,
            delivery_ceiling: None,
        };
        let file = types::PaymentConfig {
            rate_per_mb: Some(1),
            delivery_floor: None,
            delivery_ceiling: None,
            voucher_interval_mb: None,
        };
        let resolved = resolve_payment(&cli, Some(&file))?;
        assert_eq!(resolved.rate_per_mb, 99);
        Ok(())
    }

    #[test]
    fn resolve_payment_uses_file_when_cli_absent() -> anyhow::Result<()> {
        let cli = empty_payment_args();
        let file = types::PaymentConfig {
            rate_per_mb: Some(50),
            delivery_floor: None,
            delivery_ceiling: None,
            voucher_interval_mb: None,
        };
        let resolved = resolve_payment(&cli, Some(&file))?;
        assert_eq!(resolved.rate_per_mb, 50);
        Ok(())
    }

    // ---- resolve_observability: CLI > file, defaults ---------------------

    #[test]
    fn resolve_observability_cli_log_level_overrides_file() -> anyhow::Result<()> {
        let mut cli = empty_observability_args();
        cli.log_level = Some(crate::cli::common::LogLevel::Trace);
        let file = types::ObservabilityConfig {
            log_level: Some(crate::cli::common::LogLevel::Error),
            ..Default::default()
        };
        let resolved = resolve_observability(&cli, Some(&file))?;
        assert_eq!(resolved.log_level, crate::cli::common::LogLevel::Trace);
        Ok(())
    }

    #[test]
    fn resolve_observability_uses_file_log_level_when_cli_absent() -> anyhow::Result<()> {
        let cli = empty_observability_args();
        let file = types::ObservabilityConfig {
            log_level: Some(crate::cli::common::LogLevel::Debug),
            ..Default::default()
        };
        let resolved = resolve_observability(&cli, Some(&file))?;
        assert_eq!(resolved.log_level, crate::cli::common::LogLevel::Debug);
        Ok(())
    }

    #[test]
    fn resolve_observability_default_metrics_port_when_unset() -> anyhow::Result<()> {
        let cli = empty_observability_args();
        let resolved = resolve_observability(&cli, None)?;
        assert_eq!(resolved.metrics_port, DEFAULT_METRICS_PORT);
        Ok(())
    }

    #[test]
    fn resolve_observability_cli_metrics_port_overrides_file() -> anyhow::Result<()> {
        let mut cli = empty_observability_args();
        cli.metrics_port = Some(8888);
        let file = types::ObservabilityConfig {
            metrics_port: Some(7777),
            ..Default::default()
        };
        let resolved = resolve_observability(&cli, Some(&file))?;
        assert_eq!(resolved.metrics_port, 8888);
        Ok(())
    }

    // ---- end-to-end resolve_config: file path, default lookup, e2e -------

    fn write_minimal_toml(dir: &TempDir, body: &str) -> anyhow::Result<PathBuf> {
        let path = dir.path().join("node.toml");
        std::fs::write(&path, body)?;
        Ok(path)
    }

    fn complete_toml_body() -> &'static str {
        r#"
[blockchain]
rpc_url = "https://example/rpc"
payment_channel_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

[gossip]
subscribe_global = false
"#
    }

    fn blockchain_toml_body(
        rpc_url: &str,
        payment_channel_address: &str,
        capacity_bond_address: &str,
        rpc_watchdog_interval_sec: Option<u64>,
    ) -> String {
        let watchdog = rpc_watchdog_interval_sec
            .map(|value| format!("rpc_watchdog_interval_sec = {value}\n"))
            .unwrap_or_default();
        format!(
            r#"
[blockchain]
rpc_url = "{rpc_url}"
payment_channel_address = "{payment_channel_address}"
capacity_bond_address = "{capacity_bond_address}"
{watchdog}[gossip]
subscribe_global = false
"#
        )
    }

    /// Build a minimal `RunArgs` that, combined with `complete_toml_body`,
    /// produces a successfully-resolving config.  `data_dir` points at a
    /// tempdir holding a fake `keystore.json`, which lets `resolve_blockchain`
    /// validate the keystore without touching `$HOME`.
    fn run_args_with_data_dir(data_dir: &Path) -> RunArgs {
        let mut args = empty_run_args();
        args.identity.data_dir = Some(data_dir.to_path_buf());
        args
    }

    #[test]
    fn resolve_config_end_to_end_three_layer_merge() -> anyhow::Result<()> {
        // CLI > file > default exercised together: TOML supplies blockchain
        // required fields and disables global gossip; CLI overrides the bind
        // port; defaults fill metrics_port + admin_port.
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(&dir, complete_toml_body())?;
        let mut args = run_args_with_data_dir(dir.path());
        args.network.bind_port = Some(31_337);

        let resolved = resolve_config(Some(&path), &args)?;

        // CLI value wins for bind_port.
        assert_eq!(resolved.network.bind_port, 31_337);
        // File supplied required blockchain values.
        assert!(
            resolved
                .blockchain
                .rpc_url
                .starts_with("https://example/rpc")
        );
        // Defaults fill in.
        assert_eq!(resolved.observability.metrics_port, DEFAULT_METRICS_PORT);
        assert_eq!(resolved.observability.admin_port, Some(DEFAULT_ADMIN_PORT));
        assert_eq!(resolved.cache.cache_size_mb, DEFAULT_CACHE_SIZE_MB);
        assert_eq!(resolved.payment.rate_per_mb, DEFAULT_RATE_PER_MB);
        assert_eq!(
            resolved.gossip.announce_interval_sec,
            DEFAULT_ANNOUNCE_INTERVAL_SEC
        );
        assert_eq!(resolved.gossip.peer_ttl_sec, DEFAULT_PEER_TTL_SEC);
        // gossip.subscribe_global = false in the TOML => identity.region is
        // not required (covers `ensure_region_when_publishing_global` happy
        // path through resolve_config).
        assert!(!resolved.gossip.subscribe_global);
        Ok(())
    }

    #[test]
    fn resolve_config_blockchain_override_layer_beats_file_for_required_fields()
    -> anyhow::Result<()> {
        // Env vars and CLI flags both populate the same top `RunArgs` layer;
        // `run_subcommand_args_are_wired_to_decdn_env_vars` pins the env
        // mapping, while this test pins that the populated override layer wins
        // for every required blockchain field in one end-to-end resolve.
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(
            &dir,
            &blockchain_toml_body(
                "https://file.example/rpc",
                ALT_ADDR_1,
                ALT_ADDR_2,
                Some(DEFAULT_RPC_WATCHDOG_INTERVAL_SEC),
            ),
        )?;
        let mut args = run_args_with_data_dir(dir.path());
        args.blockchain.rpc_url = Some("https://override.example/rpc".to_string());
        args.blockchain.payment_channel_address = Some(GOOD_ADDR.to_string());
        args.blockchain.capacity_bond_address = Some(ALT_ADDR_3.to_string());

        let resolved = resolve_config(Some(&path), &args)?;

        assert!(
            resolved
                .blockchain
                .rpc_url
                .starts_with("https://override.example/rpc"),
            "override-layer rpc_url should win, got {}",
            resolved.blockchain.rpc_url,
        );
        assert_eq!(resolved.blockchain.payment_channel_address, GOOD_ADDR);
        assert_eq!(resolved.blockchain.capacity_bond_address, ALT_ADDR_3);
        Ok(())
    }

    #[test]
    fn resolve_config_accepts_file_only_blockchain_fields_at_watchdog_min() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(
            &dir,
            &blockchain_toml_body(
                "https://file-only.example/rpc",
                ALT_ADDR_1,
                ALT_ADDR_2,
                Some(MIN_RPC_WATCHDOG_INTERVAL_SEC),
            ),
        )?;
        let args = run_args_with_data_dir(dir.path());

        let resolved = resolve_config(Some(&path), &args)?;

        assert!(
            resolved
                .blockchain
                .rpc_url
                .starts_with("https://file-only.example/rpc"),
            "file-only rpc_url should be used, got {}",
            resolved.blockchain.rpc_url,
        );
        assert_eq!(resolved.blockchain.payment_channel_address, ALT_ADDR_1);
        assert_eq!(resolved.blockchain.capacity_bond_address, ALT_ADDR_2);
        assert_eq!(
            resolved.blockchain.rpc_watchdog_interval_sec,
            MIN_RPC_WATCHDOG_INTERVAL_SEC
        );
        Ok(())
    }

    #[test]
    fn resolve_config_rejects_file_only_blockchain_watchdog_below_minimum() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(
            &dir,
            &blockchain_toml_body(
                "https://file-only.example/rpc",
                ALT_ADDR_1,
                ALT_ADDR_2,
                Some(MIN_RPC_WATCHDOG_INTERVAL_SEC - 1),
            ),
        )?;
        let args = run_args_with_data_dir(dir.path());

        let Err(err) = resolve_config(Some(&path), &args) else {
            anyhow::bail!("expected resolve_config to reject a too-small watchdog interval");
        };
        let msg = format!("{err:#}");
        let expected_min = format!("minimum {MIN_RPC_WATCHDOG_INTERVAL_SEC}s");
        assert!(
            msg.contains("blockchain.rpc_watchdog_interval_sec") && msg.contains(&expected_min),
            "error should mention the watchdog field and floor: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_config_errors_when_subscribe_global_set_without_region() -> anyhow::Result<()> {
        // subscribe_global defaults to true; without identity.region the
        // cross-section invariant fires through resolve_config.
        let body = r#"
[blockchain]
rpc_url = "https://example/rpc"
payment_channel_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
"#;
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(&dir, body)?;
        let args = run_args_with_data_dir(dir.path());
        let Err(err) = resolve_config(Some(&path), &args) else {
            anyhow::bail!("expected resolve_config to error on missing region");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("identity.region") && msg.contains("gossip.subscribe_global"),
            "error should reference both fields: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_config_errors_when_bind_port_equals_metrics_port() -> anyhow::Result<()> {
        // Drive validate_port_layout through resolve_config end-to-end —
        // separate from the helper-level coverage above.
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(&dir, complete_toml_body())?;
        let mut args = run_args_with_data_dir(dir.path());
        args.network.bind_port = Some(9090);
        args.observability.metrics_port = Some(9090);
        let Err(err) = resolve_config(Some(&path), &args) else {
            anyhow::bail!("expected port-collision error");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("network.bind_port") && msg.contains("metrics_port"),
            "error should name both colliding ports: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_config_aggregates_all_problems_in_one_pass() -> anyhow::Result<()> {
        // Four independent problems across three sections (bad region,
        // missing rpc_url, max_blob >= cache, rate_per_mb = 0) must all
        // surface in a single error so the operator fixes them in one
        // edit cycle.
        let body = r#"
[identity]
region = "USA"

[blockchain]
payment_channel_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
slash_judge_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

[cache]
cache_size_mb = 100
max_blob_size_mb = 500

[payment]
rate_per_mb = 0
"#;
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(&dir, body)?;
        let args = run_args_with_data_dir(dir.path());
        let Err(err) = resolve_config(Some(&path), &args) else {
            anyhow::bail!("expected resolve_config to fail with multiple problems");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("configuration has 4 problem(s):"),
            "expected aggregated header with count: {msg}"
        );
        for needle in [
            "identity.region",
            "rpc_url",
            "max_blob_size_mb",
            "rate_per_mb",
        ] {
            assert!(
                msg.contains(needle),
                "aggregated error missing {needle:?}: {msg}"
            );
        }
        // A present-but-invalid region must not also trigger the
        // subscribe_global cross-section cascade (only the 4 real
        // problems, no double-count).
        assert!(
            !msg.contains("must be set when gossip.subscribe_global"),
            "region cascade should be suppressed: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_config_missing_rpc_url_emits_no_parse_cascade() -> anyhow::Result<()> {
        // Cascade guard: a missing `rpc_url` records exactly the
        // "missing required option" problem and skips the URL-parse +
        // scheme checks (a synthesized placeholder must not also emit
        // "is not a valid URL"). subscribe_global=false keeps region
        // out of it so this is a single, clean problem.
        let body = r#"
[blockchain]
payment_channel_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
slash_judge_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

[gossip]
subscribe_global = false
"#;
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(&dir, body)?;
        let args = run_args_with_data_dir(dir.path());
        let Err(err) = resolve_config(Some(&path), &args) else {
            anyhow::bail!("expected resolve_config to fail on missing rpc_url");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("configuration has 1 problem(s):"),
            "missing rpc_url must be the only problem (no parse cascade): {msg}"
        );
        assert!(
            msg.contains("missing required option: --rpc-url"),
            "expected the missing-option message: {msg}"
        );
        assert!(
            !msg.contains("is not a valid URL"),
            "URL-parse cascade must be suppressed: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_config_malformed_slash_judge_emits_no_zero_address_cascade() -> anyhow::Result<()> {
        // Cascade guard: a slash_judge_address that fails to parse
        // records the parse/checksum problem and skips the zero-address
        // check (the empty placeholder must not also emit "must not be
        // the zero address"). The address is set via CLI args because
        // `empty_blockchain_args` defaults `slash_judge_address` to a
        // valid one (CLI > file), so a TOML value would be ignored.
        let body = r#"
[blockchain]
rpc_url = "https://example/rpc"
payment_channel_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

[gossip]
subscribe_global = false
"#;
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(&dir, body)?;
        let mut args = run_args_with_data_dir(dir.path());
        args.blockchain.slash_judge_address = Some("0xnothex".to_string());
        let Err(err) = resolve_config(Some(&path), &args) else {
            anyhow::bail!("expected resolve_config to fail on bad slash_judge_address");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("configuration has 1 problem(s):"),
            "bad slash_judge must be the only problem (no zero-addr cascade): {msg}"
        );
        assert!(
            msg.contains("slash_judge_address"),
            "error should name slash_judge_address: {msg}"
        );
        assert!(
            !msg.contains("must not be the zero address"),
            "zero-address cascade must be suppressed: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_config_missing_home_dir_emits_no_keystore_cascade() -> anyhow::Result<()> {
        // Cascade guard for `IDENTITY_DATA_DIR`: when `data_dir` cannot be
        // resolved (no CLI/file path and `dirs::home_dir()` returns None),
        // `resolve_identity_into` records the `identity.data_dir` problem
        // and stamps a `/nonexistent` placeholder. The downstream
        // `eth_keystore` existence check (which would otherwise fail on
        // `/nonexistent/keystore.json`) must be suppressed via
        // `data_dir_valid` so the operator sees the real problem rather
        // than a stack of cascading filesystem errors.
        let body = r#"
[blockchain]
rpc_url = "https://example/rpc"
payment_channel_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
slash_judge_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

[gossip]
subscribe_global = false
"#;
        let dir = TempDir::new()?;
        let path = write_minimal_toml(&dir, body)?;
        // `empty_run_args` leaves `identity.data_dir = None`, so resolution
        // falls through to `default_data_dir()`; the override forces
        // `dirs::home_dir()` to None, triggering the placeholder path.
        let args = empty_run_args();
        let err =
            common::test_support::with_home_override(None, || resolve_config(Some(&path), &args))
                .expect_err("expected resolve_config to fail with data_dir problem");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("configuration has 1 problem(s):"),
            "data_dir failure must be the only problem (no keystore cascade): {msg}"
        );
        assert!(
            msg.contains("identity.data_dir"),
            "expected identity.data_dir problem: {msg}"
        );
        assert!(
            !msg.contains("eth_keystore"),
            "keystore cascade must be suppressed when data_dir is invalid: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_config_aggregates_cross_and_intra_section_problems() -> anyhow::Result<()> {
        // Cross-section + intra-section accumulation compose with a
        // correct count: bad region (1) + two malformed cache.origins
        // (2) + rate_per_mb=0 (1) = 4. The present-but-invalid region
        // also suppresses the subscribe_global cascade.
        let body = r#"
[identity]
region = "USA"

[blockchain]
rpc_url = "https://example/rpc"
payment_channel_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
slash_judge_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

[[cache.origins]]
kind = "http"
url = ""

[[cache.origins]]
kind = "http"
url = "https://good.example/"

[[cache.origins]]
kind = "fs"
path = ""

[payment]
rate_per_mb = 0
"#;
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(&dir, body)?;
        let args = run_args_with_data_dir(dir.path());
        let Err(err) = resolve_config(Some(&path), &args) else {
            anyhow::bail!("expected resolve_config to fail with 4 problems");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("configuration has 4 problem(s):"),
            "expected exactly 4 problems: {msg}"
        );
        for needle in [
            "identity.region",
            "cache.origins[0]",
            "cache.origins[2]",
            "rate_per_mb",
        ] {
            assert!(
                msg.contains(needle),
                "aggregated error missing {needle:?}: {msg}"
            );
        }
        assert!(
            !msg.contains("cache.origins[1]"),
            "the valid origin must not be reported: {msg}"
        );
        assert!(
            !msg.contains("must be set when gossip.subscribe_global"),
            "region cascade should be suppressed: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_config_errors_on_legacy_origin_url_field() -> anyhow::Result<()> {
        // Pre-#437 schemas placed `origin_url` directly under `[cache]`.
        // The new schema lives under the tagged `[cache.origin]` table
        // and `CacheConfig` has `deny_unknown_fields`, so an operator
        // who hasn't migrated their TOML must get a clear "unknown
        // field" error at config load instead of silently dropping the
        // origin and missing every cache pull.
        let dir = data_dir_with_keystore()?;
        let toml_body = format!(
            "{}\n\n[cache]\norigin_url = \"https://origin.example/\"\n",
            complete_toml_body()
        );
        let path = write_minimal_toml(&dir, &toml_body)?;
        let args = run_args_with_data_dir(dir.path());
        let Err(err) = resolve_config(Some(&path), &args) else {
            anyhow::bail!("expected unknown-field error for legacy origin_url");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("origin_url") && msg.contains("unknown field"),
            "error should call out the legacy `origin_url` key: {msg}"
        );
        Ok(())
    }

    #[test]
    fn resolve_config_errors_on_legacy_origin_path_field() -> anyhow::Result<()> {
        // Sibling of the above: the second pre-#437 flat field also
        // gets the loud `deny_unknown_fields` rejection.
        let dir = data_dir_with_keystore()?;
        let toml_body = format!(
            "{}\n\n[cache]\norigin_path = \"/var/cache/decdn/origin\"\n",
            complete_toml_body()
        );
        let path = write_minimal_toml(&dir, &toml_body)?;
        let args = run_args_with_data_dir(dir.path());
        let Err(err) = resolve_config(Some(&path), &args) else {
            anyhow::bail!("expected unknown-field error for legacy origin_path");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("origin_path") && msg.contains("unknown field"),
            "error should call out the legacy `origin_path` key: {msg}"
        );
        Ok(())
    }

    #[test]
    fn load_file_config_returns_default_when_path_is_none_and_default_absent() -> anyhow::Result<()>
    {
        // Implicit-path branch of load_file_config: when no explicit
        // path is given and the documented default location does not
        // exist, resolution must fall back to FileConfig::default() —
        // operators running `decdn` without a config file rely on this.
        //
        // Hermetic: pin the home directory to a fresh TempDir so the
        // default config path (`<home>/.decdn/node.toml`) is guaranteed
        // absent regardless of the test host's real `~/.decdn/`.
        let home_dir = TempDir::new()?;
        let cfg = common::test_support::with_home_override(Some(home_dir.path()), || {
            load_file_config(None)
        })?;
        // FileConfig::default() leaves every section as None.
        assert!(cfg.identity.is_none());
        assert!(cfg.network.is_none());
        assert!(cfg.blockchain.is_none());
        assert!(cfg.cache.is_none());
        assert!(cfg.payment.is_none());
        assert!(cfg.observability.is_none());
        assert!(cfg.gossip.is_none());
        Ok(())
    }

    #[test]
    fn load_file_config_returns_default_when_home_unavailable() -> anyhow::Result<()> {
        // Sibling of the above: when `dirs::home_dir()` is None (e.g.
        // minimal containers) `default_config_path()` is None, and the
        // implicit-path branch must still fall back to defaults rather
        // than erroring.
        let cfg = common::test_support::with_home_override(None, || load_file_config(None))?;
        assert!(cfg.identity.is_none());
        assert!(cfg.network.is_none());
        assert!(cfg.blockchain.is_none());
        assert!(cfg.cache.is_none());
        assert!(cfg.payment.is_none());
        assert!(cfg.observability.is_none());
        assert!(cfg.gossip.is_none());
        Ok(())
    }

    #[test]
    fn load_file_config_errors_when_explicit_path_missing() {
        // Sibling of the implicit-path test above: an *explicit* missing
        // path must error rather than silently fall through to defaults.
        let dir = TempDir::new().expect("tempdir");
        let bogus = dir.path().join("does-not-exist.toml");
        let err = load_file_config(Some(&bogus)).expect_err("explicit missing path should error");
        let msg = err.to_string();
        assert!(
            msg.contains("does-not-exist.toml"),
            "error should name the missing file: {msg}"
        );
    }

    #[test]
    fn load_file_config_reads_and_parses_explicit_path() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("node.toml");
        std::fs::write(
            &path,
            r"
[network]
bind_port = 12345
",
        )?;
        let cfg = load_file_config(Some(&path))?;
        let bind = cfg
            .network
            .as_ref()
            .and_then(|n| n.bind_port)
            .ok_or_else(|| anyhow::anyhow!("expected network.bind_port to deserialise"))?;
        assert_eq!(bind, 12345);
        Ok(())
    }

    #[test]
    fn load_file_config_errors_on_bad_toml() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("node.toml");
        std::fs::write(&path, "this is not = valid = toml = at = all").expect("write");
        let err = load_file_config(Some(&path)).expect_err("invalid TOML should fail to parse");
        let msg = err.to_string();
        assert!(
            msg.contains("failed to parse config file"),
            "error should describe the parse failure: {msg}"
        );
    }

    // --- resolve_security ----------------------------------------------------

    fn sec_with(mutate: impl FnOnce(&mut types::SecurityConfig)) -> types::SecurityConfig {
        let mut s = types::SecurityConfig::default();
        mutate(&mut s);
        s
    }

    #[test]
    fn resolve_security_populates_defaults_when_absent() {
        let resolved = resolve_security(None).expect("defaults must be valid");
        assert_eq!(
            resolved.max_concurrent_handlers,
            DEFAULT_MAX_CONCURRENT_HANDLERS
        );
        assert!(
            (resolved.per_source_rate_per_sec - DEFAULT_PER_SOURCE_RATE_PER_SEC).abs()
                < f64::EPSILON
        );
        assert_eq!(resolved.per_source_burst, DEFAULT_PER_SOURCE_BURST);
        assert_eq!(resolved.max_tracked_sources, DEFAULT_MAX_TRACKED_SOURCES);
    }

    #[test]
    fn resolve_security_accepts_zero_max_concurrent_handlers_as_disabled() {
        let s = sec_with(|s| s.max_concurrent_handlers = Some(0));
        let resolved = resolve_security(Some(&s)).expect("0 disables the global cap");
        assert_eq!(resolved.max_concurrent_handlers, 0);
    }

    #[test]
    fn resolve_security_rejects_non_finite_or_negative_per_source_rate() {
        // 0.0 is now valid (disabled); only NaN, ±inf, and strictly-negative are rejected.
        for bad in [-1.0_f64, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let s = sec_with(|s| s.per_source_rate_per_sec = Some(bad));
            assert!(
                resolve_security(Some(&s)).is_err(),
                "per_source_rate_per_sec={bad} should be rejected"
            );
        }
    }

    #[test]
    fn resolve_security_accepts_zero_per_source_pair_as_disabled() {
        // rate=0 + burst=0 disables the per-source layer.
        let s = sec_with(|s| {
            s.per_source_rate_per_sec = Some(0.0);
            s.per_source_burst = Some(0);
        });
        let resolved = resolve_security(Some(&s)).expect("0/0 disables per-source");
        assert_eq!(resolved.per_source_burst, 0);
    }

    #[test]
    fn resolve_security_rejects_zero_per_source_burst_with_positive_rate() {
        // rate>0 with burst=0 is the deny-all corner; reject it.
        let s = sec_with(|s| {
            s.per_source_rate_per_sec = Some(10.0);
            s.per_source_burst = Some(0);
        });
        let err = resolve_security(Some(&s)).expect_err("rate>0+burst=0 must reject");
        assert!(format!("{err:#}").contains("per_source_burst"));
    }

    #[test]
    fn resolve_security_accepts_zero_max_tracked_sources_as_unbounded() {
        let s = sec_with(|s| s.max_tracked_sources = Some(0));
        let resolved = resolve_security(Some(&s)).expect("0 makes the map unbounded");
        assert_eq!(resolved.max_tracked_sources, 0);
    }

    #[test]
    fn resolve_security_partial_override_keeps_other_defaults() {
        let s = sec_with(|s| s.max_concurrent_handlers = Some(512));
        let resolved = resolve_security(Some(&s)).expect("valid override");
        assert_eq!(resolved.max_concurrent_handlers, 512);
        assert_eq!(resolved.per_source_burst, DEFAULT_PER_SOURCE_BURST);
    }

    // Per-field merge coverage (#438). The four `[security]` fields all
    // resolve through the same file-vs-default path; one happy-path test
    // per field plus an "absent => default" companion documents the
    // contract for each field individually so a regression that
    // accidentally applies the wrong default doesn't slip past the
    // existing aggregate `_populates_defaults_when_absent` assertion.
    //
    // There is no `[security]` CLI surface — `resolve_security` takes
    // only the file argument. Operators tune these fields by editing
    // the config file (with hot reload via SIGHUP). Per-field CLI
    // overrides could land later without changing the test surface
    // here.

    #[test]
    fn resolve_security_max_concurrent_handlers_file_override() {
        let s = sec_with(|s| s.max_concurrent_handlers = Some(1024));
        let resolved = resolve_security(Some(&s)).expect("valid override");
        assert_eq!(resolved.max_concurrent_handlers, 1024);
    }

    #[test]
    fn resolve_security_max_concurrent_handlers_default_when_field_absent() {
        // Other fields populated, this one absent — companion to the
        // aggregate-defaults test, isolating this single field.
        let s = sec_with(|s| {
            s.per_source_rate_per_sec = Some(50.0);
            s.per_source_burst = Some(100);
            s.max_tracked_sources = Some(2048);
        });
        let resolved = resolve_security(Some(&s)).expect("valid partial config");
        assert_eq!(
            resolved.max_concurrent_handlers,
            DEFAULT_MAX_CONCURRENT_HANDLERS
        );
    }

    #[test]
    fn resolve_security_per_source_rate_file_override() {
        let s = sec_with(|s| s.per_source_rate_per_sec = Some(42.5));
        let resolved = resolve_security(Some(&s)).expect("valid override");
        assert!((resolved.per_source_rate_per_sec - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    fn resolve_security_per_source_rate_default_when_field_absent() {
        let s = sec_with(|s| s.max_concurrent_handlers = Some(128));
        let resolved = resolve_security(Some(&s)).expect("valid partial config");
        assert!(
            (resolved.per_source_rate_per_sec - DEFAULT_PER_SOURCE_RATE_PER_SEC).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn resolve_security_per_source_burst_file_override() {
        let s = sec_with(|s| {
            s.per_source_rate_per_sec = Some(50.0);
            s.per_source_burst = Some(500);
        });
        let resolved = resolve_security(Some(&s)).expect("valid override");
        assert_eq!(resolved.per_source_burst, 500);
    }

    #[test]
    fn resolve_security_per_source_burst_default_when_field_absent() {
        let s = sec_with(|s| s.per_source_rate_per_sec = Some(50.0));
        let resolved = resolve_security(Some(&s)).expect("valid partial config");
        assert_eq!(resolved.per_source_burst, DEFAULT_PER_SOURCE_BURST);
    }

    #[test]
    fn resolve_security_max_tracked_sources_file_override() {
        let s = sec_with(|s| s.max_tracked_sources = Some(8192));
        let resolved = resolve_security(Some(&s)).expect("valid override");
        assert_eq!(resolved.max_tracked_sources, 8192);
    }

    #[test]
    fn resolve_security_max_tracked_sources_default_when_field_absent() {
        let s = sec_with(|s| s.max_concurrent_handlers = Some(128));
        let resolved = resolve_security(Some(&s)).expect("valid partial config");
        assert_eq!(resolved.max_tracked_sources, DEFAULT_MAX_TRACKED_SOURCES);
    }

    /// Independent fields don't bleed: setting one to a non-default
    /// value must leave the others at their defaults. Catches a
    /// regression that copy-pasted `s.max_concurrent_handlers` into
    /// the wrong field's `unwrap_or(DEFAULT_*)` arm.
    #[test]
    fn resolve_security_field_overrides_are_independent() {
        let s = sec_with(|s| s.per_source_burst = Some(777));
        let resolved = resolve_security(Some(&s)).expect("valid partial config");
        assert_eq!(resolved.per_source_burst, 777);
        assert_eq!(
            resolved.max_concurrent_handlers,
            DEFAULT_MAX_CONCURRENT_HANDLERS
        );
        assert!(
            (resolved.per_source_rate_per_sec - DEFAULT_PER_SOURCE_RATE_PER_SEC).abs()
                < f64::EPSILON
        );
        assert_eq!(resolved.max_tracked_sources, DEFAULT_MAX_TRACKED_SOURCES);
    }

    // --- resolve_dht: keyspace caps (#645) -----------------------------------
    //
    // Mirrors the `resolve_security_max_tracked_sources_*` triple: each of
    // the two new `[dht.rate_limit]` knobs gets a file-override, a
    // default-when-absent, and a zero-as-unbounded test. A typo of the
    // shape `unwrap_or(0)` instead of `unwrap_or(DEFAULT_DHT_MAX_TRACKED_PER_*)`
    // would silently re-introduce the unbounded-keyspace DoS this PR fixes —
    // these tests are the resolver-layer regression guard.

    fn dht_rl_with(mutate: impl FnOnce(&mut types::DhtRateLimitConfig)) -> types::DhtConfig {
        let mut r = types::DhtRateLimitConfig::default();
        mutate(&mut r);
        types::DhtConfig {
            rate_limit: Some(r),
        }
    }

    #[test]
    fn resolve_dht_max_tracked_per_ip_file_override() {
        let d = dht_rl_with(|r| r.max_tracked_per_ip = Some(8192));
        let resolved = resolve_dht(Some(&d)).expect("valid override");
        assert_eq!(resolved.max_tracked_per_ip, 8192);
    }

    #[test]
    fn resolve_dht_max_tracked_per_ip_default_when_field_absent() {
        let d = dht_rl_with(|r| r.per_peer_burst = Some(50));
        let resolved = resolve_dht(Some(&d)).expect("valid partial config");
        assert_eq!(resolved.max_tracked_per_ip, DEFAULT_DHT_MAX_TRACKED_PER_IP);
    }

    #[test]
    fn resolve_dht_accepts_zero_max_tracked_per_ip_as_unbounded() {
        let d = dht_rl_with(|r| r.max_tracked_per_ip = Some(0));
        let resolved = resolve_dht(Some(&d)).expect("0 makes the map unbounded");
        assert_eq!(resolved.max_tracked_per_ip, 0);
    }

    #[test]
    fn resolve_dht_max_tracked_per_peer_file_override() {
        let d = dht_rl_with(|r| r.max_tracked_per_peer = Some(8192));
        let resolved = resolve_dht(Some(&d)).expect("valid override");
        assert_eq!(resolved.max_tracked_per_peer, 8192);
    }

    #[test]
    fn resolve_dht_max_tracked_per_peer_default_when_field_absent() {
        let d = dht_rl_with(|r| r.per_ip_burst = Some(150));
        let resolved = resolve_dht(Some(&d)).expect("valid partial config");
        assert_eq!(
            resolved.max_tracked_per_peer,
            DEFAULT_DHT_MAX_TRACKED_PER_PEER
        );
    }

    #[test]
    fn resolve_dht_accepts_zero_max_tracked_per_peer_as_unbounded() {
        let d = dht_rl_with(|r| r.max_tracked_per_peer = Some(0));
        let resolved = resolve_dht(Some(&d)).expect("0 makes the map unbounded");
        assert_eq!(resolved.max_tracked_per_peer, 0);
    }
}
