//! Configuration loading and resolution.
//!
//! Three-layer merge: CLI flags > TOML config file > built-in defaults.

pub mod resolved;
pub mod types;

use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use anyhow::Context;

use crate::cli::common::{self, expand_tilde};
use crate::cli::run::RunArgs;

pub use resolved::{
    ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedGossip, ResolvedIdentity,
    ResolvedNetwork, ResolvedObservability, ResolvedPayment, ResolvedSecurity,
};
pub use types::FileConfig;

/// Default QUIC bind port.
const DEFAULT_BIND_PORT: u16 = 4433;
/// Default maximum cache size in megabytes (10 GB).
const DEFAULT_CACHE_SIZE_MB: u64 = 10_240;
/// Default maximum single blob size in megabytes (1 GB).
///
/// Deliberately well below `DEFAULT_CACHE_SIZE_MB` so a single oversized
/// blob can't saturate the entire cache and evict all other content in
/// one fetch. See [`resolve_cache`] for the accompanying invariant.
const DEFAULT_MAX_BLOB_SIZE_MB: u64 = 1_024;
/// Default rate per MB in USDC base units ($0.00001/MB).
const DEFAULT_RATE_PER_MB: u64 = 10;
/// Default Prometheus metrics port.
const DEFAULT_METRICS_PORT: u16 = 9090;
/// Default metrics bind address (loopback). Operators in containerised
/// deployments override to `0.0.0.0` via CLI/env/config.
const DEFAULT_METRICS_BIND: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
/// Default loopback admin HTTP port (ADR 025). Exposed to the rest of
/// the `node` crate so `decdn node <sub>` clients can fall back to the
/// same default the server binds on, without duplicating the number.
pub(crate) const DEFAULT_ADMIN_PORT: u16 = 9191;
/// Default interval between RPC connectivity watchdog probes. `0`
/// disables the watchdog; absent in config => this value.
const DEFAULT_RPC_WATCHDOG_INTERVAL_SEC: u64 = 30;
/// Default interval between outgoing `NodeAnnounce` messages (ADR 001).
const DEFAULT_ANNOUNCE_INTERVAL_SEC: u64 = 60;
/// Default peer-table entry TTL after which a stale entry is evicted.
const DEFAULT_PEER_TTL_SEC: u64 = 600;
/// Default global cap on concurrent in-flight QUIC handler tasks (issue #235).
const DEFAULT_MAX_CONCURRENT_HANDLERS: u32 = 256;
/// Default per-NodeID token-bucket rate (tokens/second). Matches ADR 001's
/// "20 probe requests per peer per second" inbound limit.
const DEFAULT_PER_NODE_RATE_PER_SEC: f64 = 20.0;
/// Default per-NodeID burst.
const DEFAULT_PER_NODE_BURST: u32 = 20;
/// Default per-IP token-bucket rate. More generous than per-NodeID because a
/// single IP may legitimately host a fleet of nodes.
const DEFAULT_PER_IP_RATE_PER_SEC: f64 = 100.0;
/// Default per-IP burst.
const DEFAULT_PER_IP_BURST: u32 = 200;
/// Default hard cap on tracked source entries (per-NodeID map and per-IP map).
const DEFAULT_MAX_TRACKED_SOURCES: usize = 4096;

/// Load config from file (if present) and merge with CLI args.
///
/// CLI args take precedence over file values; defaults fill gaps.
///
/// # Errors
///
/// Returns an error if:
/// - The config file exists but cannot be read or parsed.
/// - A required field (`rpc_url`, `payment_channel_address`,
///   `staking_registry_address`) is not provided by any source.
/// - The home directory cannot be determined for default paths.
pub fn resolve_config(config_path: Option<&Path>, cli: &RunArgs) -> anyhow::Result<ResolvedConfig> {
    let file = load_file_config(config_path)?;

    let identity = resolve_identity(&cli.identity, file.identity.as_ref())?;
    let network = resolve_network(&cli.network, file.network.as_ref());
    let blockchain = resolve_blockchain(
        &cli.blockchain,
        file.blockchain.as_ref(),
        &identity.data_dir,
    )?;
    let cache = resolve_cache(&cli.cache, file.cache.as_ref(), &identity.data_dir)?;
    let payment = resolve_payment(&cli.payment, file.payment.as_ref())?;
    let observability = resolve_observability(&cli.observability, file.observability.as_ref())?;
    let gossip = resolve_gossip(file.gossip.as_ref())?;
    let security = resolve_security(file.security.as_ref())?;

    ensure_region_when_publishing_global(&identity, &gossip)?;
    validate_port_layout(&network, &observability)?;

    Ok(ResolvedConfig {
        identity,
        network,
        blockchain,
        cache,
        payment,
        observability,
        gossip,
        security,
    })
}

/// Reject configurations that would publish a region-less `NodeAnnounce` on
/// the global gossip topic — every peer drops those as `BadRegion`.
///
/// Region subscription is separately gated on `identity.region.is_some()` in
/// `GossipService::spawn`, so only the global-topic case needs an interlock:
/// if global is off and no region is set, the service runs as a no-op.
fn ensure_region_when_publishing_global(
    identity: &ResolvedIdentity,
    gossip: &ResolvedGossip,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        identity.region.is_some() || !gossip.subscribe_global,
        "identity.region must be set when gossip.subscribe_global is true \
         (it signs every NodeAnnounce); set identity.region or disable the \
         global topic by setting gossip.subscribe_global = false"
    );
    Ok(())
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
fn validate_port_layout(
    network: &ResolvedNetwork,
    observability: &ResolvedObservability,
) -> anyhow::Result<()> {
    let bind = network.bind_port;
    let metrics = observability.metrics_port;
    let admin = observability.admin_port;

    // bind vs metrics — UDP/TCP, same-number operator typo.
    if bind != 0 && metrics != 0 {
        anyhow::ensure!(
            bind != metrics,
            "network.bind_port ({bind}) must differ from observability.metrics_port ({metrics}); \
             QUIC (UDP) and metrics (TCP) would not collide at bind time, but sharing \
             the same port number is almost certainly an operator typo",
        );
    }

    // bind vs admin — UDP/TCP, same-number operator typo.
    if let Some(admin) = admin
        && bind != 0
        && admin != 0
    {
        anyhow::ensure!(
            bind != admin,
            "network.bind_port ({bind}) must differ from observability.admin_port ({admin}); \
             QUIC (UDP) and admin (TCP) would not collide at bind time, but sharing \
             the same port number is almost certainly an operator typo",
        );
    }

    // metrics vs admin — both TCP, second bind would fail silently.
    if let Some(admin) = admin
        && metrics != 0
        && admin != 0
    {
        anyhow::ensure!(
            admin != metrics,
            "observability.admin_port ({admin}) must differ from observability.metrics_port ({metrics}); \
             the two servers cannot share a TCP port",
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

    Ok(())
}

/// Resolve identity fields.
fn resolve_identity(
    cli: &crate::cli::run::IdentityArgs,
    file: Option<&types::IdentityConfig>,
) -> anyhow::Result<ResolvedIdentity> {
    let data_dir = cli
        .data_dir
        .clone()
        .map(|p| expand_tilde(&p))
        .or_else(|| {
            file.and_then(|i| i.data_dir.clone())
                .map(|p| expand_tilde(&p))
        })
        .or_else(common::default_data_dir)
        .ok_or_else(|| anyhow::anyhow!("cannot determine data directory: home dir not found"))?;

    let region = cli
        .region
        .clone()
        .or_else(|| file.and_then(|i| i.region.clone()))
        .map(|r| normalize_region(&r))
        .transpose()?;

    Ok(ResolvedIdentity { data_dir, region })
}

/// Normalize an operator-supplied region code: uppercase it and require
/// exactly two ASCII letters (ISO 3166-1 alpha-2 per ADR 001). A bad value
/// here would otherwise cause the node to publish announces that it and
/// its peers all reject at validation time — fail loudly at startup.
fn normalize_region(raw: &str) -> anyhow::Result<String> {
    let upper = raw.to_ascii_uppercase();
    anyhow::ensure!(
        upper.len() == 2 && upper.bytes().all(|b| b.is_ascii_uppercase()),
        "identity.region must be 2 ASCII letters (ISO 3166-1 alpha-2), got {raw:?}"
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

    ResolvedNetwork {
        bind_port,
        relay_url,
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

/// Resolve blockchain fields.
fn resolve_blockchain(
    cli: &crate::cli::run::BlockchainArgs,
    file: Option<&types::BlockchainConfig>,
    data_dir: &std::path::Path,
) -> anyhow::Result<ResolvedBlockchain> {
    let rpc_url = cli
        .rpc_url
        .clone()
        .or_else(|| file.and_then(|b| b.rpc_url.clone()))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing required option: --rpc-url (or blockchain.rpc_url in config file)"
            )
        })?;

    let parsed = url::Url::parse(&rpc_url).context("blockchain.rpc_url is not a valid URL")?;
    anyhow::ensure!(
        parsed.scheme() == "http" || parsed.scheme() == "https",
        "blockchain.rpc_url must use http or https scheme (got {:?})",
        parsed.scheme()
    );
    // Store the normalized form (lowercase scheme, trailing slash, etc.).
    // Userinfo (basic auth) is preserved by url::Url::to_string and we
    // depend on that for RPC providers that require it.
    let rpc_url = parsed.to_string();

    let eth_keystore = cli
        .eth_keystore
        .clone()
        .map(|p| expand_tilde(&p))
        .or_else(|| {
            file.and_then(|b| b.eth_keystore.clone())
                .map(|p| expand_tilde(&p))
        })
        .unwrap_or_else(|| data_dir.join("keystore.json"));

    // Fail fast: otherwise a bad keystore path only surfaces at first sign.
    // `metadata`/`is_file` catches missing paths, broken symlinks, and
    // directories (which `File::open` silently accepts on Linux); `File::open`
    // then proves read permission.
    let meta = std::fs::metadata(&eth_keystore).with_context(|| {
        format!(
            "invalid eth_keystore: cannot access {}",
            eth_keystore.display()
        )
    })?;
    anyhow::ensure!(
        meta.is_file(),
        "invalid eth_keystore: {} is not a regular file",
        eth_keystore.display()
    );
    std::fs::File::open(&eth_keystore).with_context(|| {
        format!(
            "invalid eth_keystore: cannot open {} for reading",
            eth_keystore.display()
        )
    })?;

    let payment_channel_address = cli
        .payment_channel_address
        .clone()
        .or_else(|| file.and_then(|b| b.payment_channel_address.clone()))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing required option: --payment-channel-address \
                 (or blockchain.payment_channel_address in config file)"
            )
        })?;
    let payment_channel_address =
        parse_contract_address("payment_channel_address", &payment_channel_address)?;

    let staking_registry_address = cli
        .staking_registry_address
        .clone()
        .or_else(|| file.and_then(|b| b.staking_registry_address.clone()))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing required option: --staking-registry-address \
                 (or blockchain.staking_registry_address in config file)"
            )
        })?;
    let staking_registry_address =
        parse_contract_address("staking_registry_address", &staking_registry_address)?;

    let rpc_watchdog_interval_sec = file
        .and_then(|b| b.rpc_watchdog_interval_sec)
        .unwrap_or(DEFAULT_RPC_WATCHDOG_INTERVAL_SEC);

    Ok(ResolvedBlockchain {
        rpc_url,
        eth_keystore,
        payment_channel_address,
        staking_registry_address,
        rpc_watchdog_interval_sec,
    })
}

/// Resolve cache fields.
///
/// Enforces `max_blob_size_mb < cache_size_mb`: a single blob equal to or
/// larger than the cache would saturate the store on one fetch and evict
/// every other entry, making the node a one-shot download target rather
/// than a useful cache. Equality is rejected along with the greater-than
/// case because a cache that can hold exactly one blob has the same
/// failure mode as one that overflows.
fn resolve_cache(
    cli: &crate::cli::run::CacheArgs,
    file: Option<&types::CacheConfig>,
    data_dir: &std::path::Path,
) -> anyhow::Result<ResolvedCache> {
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

    anyhow::ensure!(
        max_blob_size_mb < cache_size_mb,
        "cache.max_blob_size_mb ({max_blob_size_mb}) must be strictly less than \
         cache.cache_size_mb ({cache_size_mb}); otherwise a single oversized blob \
         can saturate the cache on one fetch"
    );

    let origin_url_raw = cli
        .origin_url
        .clone()
        .or_else(|| file.and_then(|c| c.origin_url.clone()))
        .filter(|s| !s.is_empty());
    let origin_url = origin_url_raw
        .as_deref()
        .map(decdn_cache::parse_origin_url)
        .transpose()
        .context("invalid cache.origin_url")?;

    let origin_path = cli
        .origin_path
        .clone()
        .map(|p| expand_tilde(&p))
        .or_else(|| {
            file.and_then(|c| c.origin_path.clone())
                .map(|p| expand_tilde(&p))
        })
        .filter(|p| !p.as_os_str().is_empty());

    // origin_url and origin_path are different backends for the same slot
    // (pull-through on miss). Accepting both would require choosing one
    // silently; operators almost never want that, so fail loudly.
    if origin_url.is_some() && origin_path.is_some() {
        anyhow::bail!(
            "cache.origin_url and cache.origin_path are mutually exclusive; set only one"
        );
    }

    // Decompression defaults to Auto — most object stores serve
    // compressed bodies and the BLAKE3 verify in the engine runs over
    // the canonical (decompressed) form, so silently passing through
    // compressed bytes would always fail. CLI has no override for
    // this knob (no operator policy reason to flip it ad-hoc);
    // file-only is sufficient.
    let decompress = file.and_then(|c| c.decompress).unwrap_or_default();

    let pinned_hashes = parse_pinned_hashes(file.and_then(|c| c.pinned_hashes.as_deref()))
        .context("invalid cache.pinned_hashes")?;

    Ok(ResolvedCache {
        cache_dir,
        cache_size_mb,
        max_blob_size_mb,
        origin_url,
        origin_path,
        decompress,
        pinned_hashes,
    })
}

/// Parse the operator-supplied `cache.pinned_hashes` list (#276) into a
/// [`decdn_cache::PinnedHashes`]. Each entry must be 64 lowercase hex
/// chars (BLAKE3 digest size); anything else fails resolution. Duplicates
/// are silently de-duplicated — they're harmless.
///
/// `None` and the empty list both resolve to the empty set, so an absent
/// or empty `pinned_hashes` key just means "no pinning".
pub(crate) fn parse_pinned_hashes(
    raw: Option<&[String]>,
) -> anyhow::Result<decdn_cache::PinnedHashes> {
    use std::str::FromStr;

    let mut out = std::collections::HashSet::new();
    let Some(entries) = raw else {
        return Ok(decdn_cache::PinnedHashes::empty());
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
        let parsed = decdn_cache::Hash::from_str(trimmed).with_context(|| {
            format!("cache.pinned_hashes[{idx}] failed to parse as a BLAKE3 hash")
        })?;
        out.insert(parsed);
    }
    Ok(decdn_cache::PinnedHashes::new(out))
}

/// Resolve payment fields.
///
/// Rejects `rate_per_mb == 0`: the value participates in the node selection
/// score (`rate_per_mb × rtt_ms × …`, ADR 001 § Node Selection Algorithm) and
/// feeds the on-chain rate-mismatch evidence path (ADR 014). A zero rate would
/// make this node trivially win every client selection while earning no
/// payable revenue — an obvious misconfiguration that should fail startup, not
/// silently degrade the network.
pub(crate) fn resolve_payment(
    cli: &crate::cli::run::PaymentArgs,
    file: Option<&types::PaymentConfig>,
) -> anyhow::Result<ResolvedPayment> {
    let rate_per_mb = cli
        .rate_per_mb
        .or_else(|| file.and_then(|p| p.rate_per_mb))
        .unwrap_or(DEFAULT_RATE_PER_MB);
    anyhow::ensure!(
        rate_per_mb > 0,
        "payment.rate_per_mb must be > 0 (used in the node selection score, \
         ADR 001); got 0"
    );
    Ok(ResolvedPayment { rate_per_mb })
}

/// Resolve observability fields.
///
/// The admin port is merged with `0` as a first-class "disable" value so
/// operators can turn the surface off without removing the line from their
/// config. Cross-port collision checks (bind/metrics/admin) live in
/// [`validate_port_layout`], which sees all three sections at once — see
/// there for the full ruleset.
pub(crate) fn resolve_observability(
    cli: &crate::cli::run::ObservabilityArgs,
    file: Option<&types::ObservabilityConfig>,
) -> anyhow::Result<ResolvedObservability> {
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
        anyhow::ensure!(
            lower.starts_with("http://") || lower.starts_with("https://"),
            "observability.otlp_endpoint must start with http:// or https:// \
             (got {ep:?}); gRPC/OTLP collectors require an HTTP-scheme URL"
        );
    }

    Ok(ResolvedObservability {
        log_level,
        log_format,
        metrics_port,
        metrics_bind,
        admin_port,
        otlp_endpoint,
    })
}

/// Resolve gossip fields. Allowlist entries are parsed as 64-character hex
/// node IDs (either case accepted); bad entries fail loudly at startup
/// rather than silently degrading to accept-all mode later.
fn resolve_gossip(file: Option<&types::GossipConfig>) -> anyhow::Result<ResolvedGossip> {
    let announce_interval_sec = file
        .and_then(|g| g.announce_interval_sec)
        .unwrap_or(DEFAULT_ANNOUNCE_INTERVAL_SEC);
    anyhow::ensure!(
        announce_interval_sec > 0,
        "gossip.announce_interval_sec must be > 0"
    );

    let peer_ttl_sec = file
        .and_then(|g| g.peer_ttl_sec)
        .unwrap_or(DEFAULT_PEER_TTL_SEC);
    anyhow::ensure!(peer_ttl_sec > 0, "gossip.peer_ttl_sec must be > 0");

    let subscribe_global = file.and_then(|g| g.subscribe_global).unwrap_or(true);

    let allowlist = file
        .and_then(|g| g.allowlist.as_ref())
        .map(|v| v.iter().map(|s| parse_node_id_hex(s)).collect())
        .transpose()?
        .unwrap_or_default();

    Ok(ResolvedGossip {
        announce_interval_sec,
        peer_ttl_sec,
        subscribe_global,
        allowlist,
    })
}

/// Resolve security / rate-limiting fields (issue #235).
fn resolve_security(file: Option<&types::SecurityConfig>) -> anyhow::Result<ResolvedSecurity> {
    let max_concurrent_handlers = file
        .and_then(|s| s.max_concurrent_handlers)
        .unwrap_or(DEFAULT_MAX_CONCURRENT_HANDLERS);
    anyhow::ensure!(
        max_concurrent_handlers > 0,
        "security.max_concurrent_handlers must be > 0"
    );

    let per_node_rate_per_sec = file
        .and_then(|s| s.per_node_rate_per_sec)
        .unwrap_or(DEFAULT_PER_NODE_RATE_PER_SEC);
    anyhow::ensure!(
        per_node_rate_per_sec.is_finite() && per_node_rate_per_sec > 0.0,
        "security.per_node_rate_per_sec must be a finite positive number"
    );

    let per_node_burst = file
        .and_then(|s| s.per_node_burst)
        .unwrap_or(DEFAULT_PER_NODE_BURST);
    anyhow::ensure!(per_node_burst > 0, "security.per_node_burst must be > 0");

    let per_ip_rate_per_sec = file
        .and_then(|s| s.per_ip_rate_per_sec)
        .unwrap_or(DEFAULT_PER_IP_RATE_PER_SEC);
    anyhow::ensure!(
        per_ip_rate_per_sec.is_finite() && per_ip_rate_per_sec > 0.0,
        "security.per_ip_rate_per_sec must be a finite positive number"
    );

    let per_ip_burst = file
        .and_then(|s| s.per_ip_burst)
        .unwrap_or(DEFAULT_PER_IP_BURST);
    anyhow::ensure!(per_ip_burst > 0, "security.per_ip_burst must be > 0");

    let max_tracked_sources = file
        .and_then(|s| s.max_tracked_sources)
        .unwrap_or(DEFAULT_MAX_TRACKED_SOURCES);
    anyhow::ensure!(
        max_tracked_sources > 0,
        "security.max_tracked_sources must be > 0"
    );

    Ok(ResolvedSecurity {
        max_concurrent_handlers,
        per_node_rate_per_sec,
        per_node_burst,
        per_ip_rate_per_sec,
        per_ip_burst,
        max_tracked_sources,
    })
}

/// Parse a 64-character hex (case-insensitive) node ID into 32 raw bytes.
fn parse_node_id_hex(s: &str) -> anyhow::Result<[u8; 32]> {
    anyhow::ensure!(
        s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()),
        "gossip.allowlist entry must be 64 hex chars, got {s:?}"
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
pub(crate) fn load_file_config(explicit_path: Option<&Path>) -> anyhow::Result<FileConfig> {
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
            &mut b.staking_registry_address,
            "blockchain.staking_registry_address",
        )?;
    }
    if let Some(c) = cfg.cache.as_mut() {
        expand_path(&mut c.cache_dir, "cache.cache_dir")?;
        expand_str(&mut c.origin_url, "cache.origin_url")?;
        expand_path(&mut c.origin_path, "cache.origin_path")?;
    }
    if let Some(o) = cfg.observability.as_mut() {
        expand_str(&mut o.otlp_endpoint, "observability.otlp_endpoint")?;
    }
    Ok(())
}

fn expand_str(field: &mut Option<String>, ctx: &'static str) -> anyhow::Result<()> {
    if let Some(s) = field.as_mut() {
        *s = expand_value(s, ctx)?;
    }
    Ok(())
}

fn expand_path(field: &mut Option<PathBuf>, ctx: &'static str) -> anyhow::Result<()> {
    if let Some(p) = field.as_mut() {
        let as_str = p.to_string_lossy();
        let expanded = expand_value(&as_str, ctx)?;
        *p = PathBuf::from(expanded);
    }
    Ok(())
}

fn expand_value(raw: &str, ctx: &'static str) -> anyhow::Result<String> {
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
fn expand_tilde_prefix(raw: &str, ctx: &'static str) -> anyhow::Result<String> {
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

fn missing_home_err(ctx: &'static str) -> anyhow::Error {
    anyhow::anyhow!("config field `{ctx}` uses `~` but home directory is not available")
}

/// Substitute `${VAR}` sequences with the corresponding env var value. No
/// escape semantics — backslashes, single `$`, and any other character pass
/// through verbatim. This is important for Windows paths like
/// `C:\Users\${USER}\data`, where a shell-style escape interpreter would
/// swallow the `\` before `$` and disable the substitution.
fn expand_braces(raw: &str, ctx: &'static str) -> anyhow::Result<String> {
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
        for bad in ["usa", "u1", "", "U", "U S", "Ü1", "12", "U-"] {
            assert!(
                normalize_region(bad).is_err(),
                "expected {bad:?} to be rejected"
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
        };
        let g = resolve_gossip(Some(&cfg))?;
        assert_eq!(g.announce_interval_sec, 42);
        assert_eq!(g.peer_ttl_sec, 123);
        assert!(!g.subscribe_global);
        assert!(g.allowlist.is_empty());
        Ok(())
    }

    #[test]
    fn resolve_gossip_applies_defaults_when_absent() -> anyhow::Result<()> {
        let g = resolve_gossip(None)?;
        assert_eq!(g.announce_interval_sec, DEFAULT_ANNOUNCE_INTERVAL_SEC);
        assert_eq!(g.peer_ttl_sec, DEFAULT_PEER_TTL_SEC);
        assert!(g.subscribe_global);
        assert!(g.allowlist.is_empty());
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
                origin_url: None,
                origin_path: None,
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
                origin_url: None,
                origin_path: None,
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
                origin_url: None,
                origin_path: None,
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
            ("blockchain.staking_registry_address", |c, v| {
                c.blockchain = Some(types::BlockchainConfig {
                    staking_registry_address: Some(v.to_string()),
                    ..Default::default()
                });
            }),
            ("cache.cache_dir", |c, v| {
                c.cache = Some(types::CacheConfig {
                    cache_dir: Some(PathBuf::from(v)),
                    cache_size_mb: None,
                    max_blob_size_mb: None,
                    origin_url: None,
                    origin_path: None,
                    ..Default::default()
                });
            }),
            ("cache.origin_url", |c, v| {
                c.cache = Some(types::CacheConfig {
                    cache_dir: None,
                    cache_size_mb: None,
                    max_blob_size_mb: None,
                    origin_url: Some(v.to_string()),
                    origin_path: None,
                    ..Default::default()
                });
            }),
            ("cache.origin_path", |c, v| {
                c.cache = Some(types::CacheConfig {
                    cache_dir: None,
                    cache_size_mb: None,
                    max_blob_size_mb: None,
                    origin_url: None,
                    origin_path: Some(PathBuf::from(v)),
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

    // Guards against the classic "added a field, forgot to wire expansion"
    // regression — cache.origin_url is URL-shaped and must get the same
    // `${VAR}` treatment as sibling URL fields (rpc_url, relay_url, etc).
    #[test]
    fn expand_env_substitutes_cache_origin_url() -> anyhow::Result<()> {
        let home = home_str()?;
        let mut cfg = FileConfig {
            cache: Some(types::CacheConfig {
                cache_dir: None,
                cache_size_mb: None,
                max_blob_size_mb: None,
                origin_url: Some("https://origin.example/${HOME}/bucket".to_string()),
                origin_path: None,
                ..Default::default()
            }),
            ..Default::default()
        };
        expand_env(&mut cfg)?;
        let url = cfg
            .cache
            .as_ref()
            .and_then(|c| c.origin_url.as_deref())
            .ok_or_else(|| anyhow::anyhow!("origin_url missing"))?;
        let expected = format!("https://origin.example/{home}/bucket");
        anyhow::ensure!(url == expected, "got: {url}");
        Ok(())
    }

    // resolve_cache must reject a non-http(s) URL at config resolution
    // instead of deferring the check to engine wiring. This locks in the
    // "single parser" invariant introduced by `parse_origin_url`.
    #[test]
    fn resolve_cache_rejects_non_http_origin_url() -> anyhow::Result<()> {
        let cli = crate::cli::run::CacheArgs {
            cache_dir: None,
            cache_size_mb: None,
            max_blob_size_mb: None,
            origin_url: Some("file:///etc/passwd".to_string()),
            origin_path: None,
        };
        let err = resolve_cache(&cli, None, std::path::Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected scheme rejection"))?
            .to_string();
        anyhow::ensure!(
            err.contains("invalid cache.origin_url")
                || err.contains("unsupported origin URL scheme"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[test]
    fn resolve_payment_rejects_zero_from_cli() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: Some(0),
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

    // CLI > TOML precedence for origin_url specifically — mirrors the
    // existing precedence pattern elsewhere in the config layer.
    #[test]
    fn resolve_cache_cli_origin_url_overrides_toml() -> anyhow::Result<()> {
        let cli = crate::cli::run::CacheArgs {
            cache_dir: None,
            cache_size_mb: None,
            max_blob_size_mb: None,
            origin_url: Some("https://cli-wins.example/".to_string()),
            origin_path: None,
        };
        let toml = types::CacheConfig {
            cache_dir: None,
            cache_size_mb: None,
            max_blob_size_mb: None,
            origin_url: Some("https://toml-loses.example/".to_string()),
            origin_path: None,
            ..Default::default()
        };
        let resolved = resolve_cache(&cli, Some(&toml), std::path::Path::new("/tmp"))?;
        let url = resolved
            .origin_url
            .ok_or_else(|| anyhow::anyhow!("origin_url missing"))?;
        anyhow::ensure!(
            url.as_url().as_str() == "https://cli-wins.example/",
            "got: {url}"
        );
        Ok(())
    }

    // Both backends for the same miss-pull slot cannot be set at once —
    // picking one silently would almost certainly violate operator intent.
    #[test]
    fn resolve_cache_rejects_both_origin_url_and_path() -> anyhow::Result<()> {
        let cli = crate::cli::run::CacheArgs {
            cache_dir: None,
            cache_size_mb: None,
            max_blob_size_mb: None,
            origin_url: Some("https://origin.example/".to_string()),
            origin_path: Some(PathBuf::from("/var/cache/decdn/origin")),
        };
        let err = resolve_cache(&cli, None, std::path::Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected mutual-exclusion error"))?
            .to_string();
        anyhow::ensure!(
            err.contains("mutually exclusive"),
            "error lacked mutual-exclusion context: {err}"
        );
        Ok(())
    }

    #[test]
    fn resolve_payment_rejects_zero_from_file() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs { rate_per_mb: None };
        let file = types::PaymentConfig {
            rate_per_mb: Some(0),
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

    // origin_path alone resolves cleanly and leaves origin_url absent —
    // pairs with the runtime's (Some, None) / (None, Some) dispatch match.
    #[test]
    fn resolve_cache_origin_path_only() -> anyhow::Result<()> {
        let cli = crate::cli::run::CacheArgs {
            cache_dir: None,
            cache_size_mb: None,
            max_blob_size_mb: None,
            origin_url: None,
            origin_path: Some(PathBuf::from("/tmp/origin")),
        };
        let resolved = resolve_cache(&cli, None, std::path::Path::new("/tmp"))?;
        anyhow::ensure!(resolved.origin_url.is_none(), "origin_url should be None");
        anyhow::ensure!(
            resolved.origin_path == Some(PathBuf::from("/tmp/origin")),
            "origin_path: {:?}",
            resolved.origin_path,
        );
        Ok(())
    }

    #[test]
    fn resolve_payment_cli_overrides_file_and_passes_nonzero() -> anyhow::Result<()> {
        // Regression guard for the merge order: a zero file value must not
        // short-circuit the CLI override that would otherwise be valid.
        let cli = crate::cli::run::PaymentArgs {
            rate_per_mb: Some(42),
        };
        let file = types::PaymentConfig {
            rate_per_mb: Some(0),
        };
        let resolved = resolve_payment(&cli, Some(&file))?;
        anyhow::ensure!(resolved.rate_per_mb == 42, "got: {}", resolved.rate_per_mb);
        Ok(())
    }

    #[test]
    fn resolve_payment_defaults_when_unset() -> anyhow::Result<()> {
        let cli = crate::cli::run::PaymentArgs { rate_per_mb: None };
        let resolved = resolve_payment(&cli, None)?;
        anyhow::ensure!(
            resolved.rate_per_mb == DEFAULT_RATE_PER_MB,
            "got: {}",
            resolved.rate_per_mb
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
            origin_url: None,
            origin_path: None,
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
    fn resolve_cache_defaults_decompress_auto_and_pinned_empty() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
        anyhow::ensure!(
            matches!(resolved.decompress, decdn_cache::DecompressMode::Auto),
            "decompress should default to Auto"
        );
        anyhow::ensure!(resolved.pinned_hashes.is_empty());
        Ok(())
    }

    #[test]
    fn resolve_cache_decompress_strict_via_file() -> anyhow::Result<()> {
        let cli = cache_cli(None, None);
        let file = types::CacheConfig {
            decompress: Some(decdn_cache::DecompressMode::Strict),
            ..types::CacheConfig::default()
        };
        let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
        anyhow::ensure!(matches!(
            resolved.decompress,
            decdn_cache::DecompressMode::Strict
        ));
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
        use clap::CommandFactory;

        let cmd = crate::cli::Cli::command();
        let run = cmd
            .find_subcommand("run")
            .expect("Cli has a `run` subcommand");

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
            ("payment_channel_address", "DECDN_PAYMENT_CHANNEL_ADDRESS"),
            ("staking_registry_address", "DECDN_STAKING_REGISTRY_ADDRESS"),
            ("cache_dir", "DECDN_CACHE_DIR"),
            ("cache_size_mb", "DECDN_CACHE_SIZE_MB"),
            ("max_blob_size_mb", "DECDN_MAX_BLOB_SIZE_MB"),
            ("origin_url", "DECDN_ORIGIN_URL"),
            ("origin_path", "DECDN_ORIGIN_PATH"),
            ("rate_per_mb", "DECDN_RATE_PER_MB"),
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
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            staking_registry_address: Some("0xNOTHEX".to_string()),
        };
        let dir = data_dir_with_keystore()?;
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected resolve_blockchain to fail on bad staking address");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("staking_registry_address"),
            "error should name staking_registry_address: {msg}"
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
            payment_channel_address: Some("0xNOTHEX".to_string()),
            staking_registry_address: Some(GOOD_ADDR.to_string()),
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
            !msg.contains("staking_registry_address"),
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
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            staking_registry_address: Some(GOOD_ADDR.to_string()),
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
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            staking_registry_address: Some(GOOD_ADDR.to_string()),
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
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            staking_registry_address: Some(GOOD_ADDR.to_string()),
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
            payment_channel_address: None,
            staking_registry_address: None,
        }
    }

    fn empty_cache_args() -> crate::cli::run::CacheArgs {
        crate::cli::run::CacheArgs {
            cache_dir: None,
            cache_size_mb: None,
            max_blob_size_mb: None,
            origin_url: None,
            origin_path: None,
        }
    }

    fn empty_payment_args() -> crate::cli::run::PaymentArgs {
        crate::cli::run::PaymentArgs { rate_per_mb: None }
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
        };
        let resolved = resolve_network(&cli, Some(&file));
        assert_eq!(resolved.bind_port, 6666);
        assert_eq!(resolved.relay_url.as_deref(), Some("https://relay.example"));
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
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            staking_registry_address: Some(GOOD_ADDR.to_string()),
        };
        let file = types::BlockchainConfig {
            rpc_url: Some("https://file-loses.example/rpc".to_string()),
            eth_keystore: None,
            payment_channel_address: None,
            staking_registry_address: None,
            rpc_watchdog_interval_sec: None,
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
            staking_registry_address: Some(GOOD_ADDR.to_string()),
            rpc_watchdog_interval_sec: None,
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
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            staking_registry_address: Some(GOOD_ADDR.to_string()),
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
            payment_channel_address: None,
            staking_registry_address: Some(GOOD_ADDR.to_string()),
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
    fn resolve_blockchain_errors_when_staking_registry_address_missing() -> anyhow::Result<()> {
        let dir = data_dir_with_keystore()?;
        let cli = BlockchainArgs {
            rpc_url: Some("https://example/rpc".to_string()),
            eth_keystore: None,
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            staking_registry_address: None,
        };
        let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
            anyhow::bail!("expected error when staking_registry_address missing");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("staking_registry_address"),
            "error should mention staking_registry_address: {msg}"
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
            payment_channel_address: Some(GOOD_ADDR.to_string()),
            staking_registry_address: Some(GOOD_ADDR.to_string()),
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

    // ---- resolve_cache: CLI > file, defaults, tilde expansion ------------

    #[test]
    fn resolve_cache_cli_cache_dir_overrides_file_and_expands_tilde() -> anyhow::Result<()> {
        // Hermetic: inject a stub home so the assertion holds whether or
        // not the host's `dirs::home_dir()` returns Some, and so the
        // assertion exercises the documented production behaviour
        // (tilde-expand against $HOME).
        let home_dir = TempDir::new()?;
        let home = home_dir.path().to_path_buf();
        let mut cli = empty_cache_args();
        cli.cache_dir = Some(PathBuf::from("~/from-cli"));
        let file = types::CacheConfig {
            cache_dir: Some(PathBuf::from("/from/file")),
            cache_size_mb: None,
            max_blob_size_mb: None,
            origin_url: None,
            origin_path: None,
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
        // Sibling of the above: the production contract is "log + leave
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
            origin_url: None,
            origin_path: None,
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
            origin_url: None,
            origin_path: None,
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
            origin_url: None,
            origin_path: None,
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
        };
        let file = types::PaymentConfig {
            rate_per_mb: Some(1),
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
staking_registry_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

[gossip]
subscribe_global = false
"#
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
    fn resolve_config_errors_when_subscribe_global_set_without_region() -> anyhow::Result<()> {
        // subscribe_global defaults to true; without identity.region the
        // cross-section invariant fires through resolve_config.
        let body = r#"
[blockchain]
rpc_url = "https://example/rpc"
payment_channel_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
staking_registry_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
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
    fn resolve_config_errors_when_origin_url_and_path_both_set() -> anyhow::Result<()> {
        // resolve_cache enforces mutual exclusion; this end-to-end check
        // confirms the same diagnostic surfaces from resolve_config so
        // operators see it at startup.
        let dir = data_dir_with_keystore()?;
        let path = write_minimal_toml(&dir, complete_toml_body())?;
        let mut args = run_args_with_data_dir(dir.path());
        args.cache.origin_url = Some("https://origin.example/".to_string());
        args.cache.origin_path = Some(PathBuf::from("/var/cache/decdn/origin"));
        let Err(err) = resolve_config(Some(&path), &args) else {
            anyhow::bail!("expected mutex error");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("mutually exclusive"),
            "error should call out mutual exclusion: {msg}"
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
}
