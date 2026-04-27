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
    ResolvedNetwork, ResolvedObservability, ResolvedPayment,
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
/// Default loopback admin HTTP port (ADR 025). Exposed to the rest of
/// the `node` crate so `decdn node <sub>` clients can fall back to the
/// same default the server binds on, without duplicating the number.
pub(crate) const DEFAULT_ADMIN_PORT: u16 = 9191;
/// Default interval between outgoing `NodeAnnounce` messages (ADR 001).
const DEFAULT_ANNOUNCE_INTERVAL_SEC: u64 = 60;
/// Default peer-table entry TTL after which a stale entry is evicted.
const DEFAULT_PEER_TTL_SEC: u64 = 600;

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
    let observability = resolve_observability(&cli.observability, file.observability.as_ref());
    let gossip = resolve_gossip(file.gossip.as_ref())?;

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

/// Resolve blockchain fields.
pub fn resolve_blockchain(
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

    Ok(ResolvedBlockchain {
        rpc_url,
        eth_keystore,
        payment_channel_address,
        staking_registry_address,
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

    Ok(ResolvedCache {
        cache_dir,
        cache_size_mb,
        max_blob_size_mb,
        origin_url,
        origin_path,
    })
}

/// Resolve payment fields.
///
/// Rejects `rate_per_mb == 0`: the value participates in the node selection
/// score (`rate_per_mb × rtt_ms × …`, ADR 001 § Node Selection Algorithm) and
/// feeds the on-chain rate-mismatch evidence path (ADR 014). A zero rate would
/// make this node trivially win every client selection while earning no
/// payable revenue — an obvious misconfiguration that should fail startup, not
/// silently degrade the network.
fn resolve_payment(
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
fn resolve_observability(
    cli: &crate::cli::run::ObservabilityArgs,
    file: Option<&types::ObservabilityConfig>,
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
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));

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
        .or_else(|| file.and_then(|o| o.otlp_endpoint.clone()));

    ResolvedObservability {
        log_level,
        log_format,
        metrics_port,
        metrics_bind,
        admin_port,
        otlp_endpoint,
    }
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
fn load_file_config(explicit_path: Option<&Path>) -> anyhow::Result<FileConfig> {
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
                });
            }),
            ("cache.origin_url", |c, v| {
                c.cache = Some(types::CacheConfig {
                    cache_dir: None,
                    cache_size_mb: None,
                    max_blob_size_mb: None,
                    origin_url: Some(v.to_string()),
                    origin_path: None,
                });
            }),
            ("cache.origin_path", |c, v| {
                c.cache = Some(types::CacheConfig {
                    cache_dir: None,
                    cache_size_mb: None,
                    max_blob_size_mb: None,
                    origin_url: None,
                    origin_path: Some(PathBuf::from(v)),
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
            metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            admin_port: None,
            otlp_endpoint: None,
        }
    }

    fn obs_with_admin(metrics: u16, admin: u16) -> ResolvedObservability {
        ResolvedObservability {
            log_level: crate::cli::common::LogLevel::default(),
            log_format: crate::cli::common::LogFormat::default(),
            metrics_port: metrics,
            metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            admin_port: Some(admin),
            otlp_endpoint: None,
        }
    }

    #[test]
    fn resolve_observability_defaults_admin_port_to_9191() -> anyhow::Result<()> {
        let obs = resolve_observability(&obs_cli(None, None), None);
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
        let obs = resolve_observability(&obs_cli(None, Some(0)), None);
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
        let obs = resolve_observability(&obs_cli(None, None), Some(&file));
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
        let obs = resolve_observability(&obs_cli(None, Some(2222)), Some(&file));
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
    fn resolve_observability_metrics_bind_defaults_to_localhost() {
        let obs = resolve_observability(&obs_cli(None, None), None);
        assert_eq!(
            obs.metrics_bind,
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );
    }

    #[test]
    fn resolve_observability_metrics_bind_from_cli() {
        let mut cli = obs_cli(None, None);
        cli.metrics_bind = Some(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
        let obs = resolve_observability(&cli, None);
        assert_eq!(
            obs.metrics_bind,
            std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        );
    }

    #[test]
    fn resolve_observability_metrics_bind_from_file() {
        let file = types::ObservabilityConfig {
            metrics_bind: Some(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)),
            ..Default::default()
        };
        let obs = resolve_observability(&obs_cli(None, None), Some(&file));
        assert_eq!(
            obs.metrics_bind,
            std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        );
    }

    #[test]
    fn resolve_observability_metrics_bind_cli_overrides_file() {
        let mut cli = obs_cli(None, None);
        cli.metrics_bind = Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        let file = types::ObservabilityConfig {
            metrics_bind: Some(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)),
            ..Default::default()
        };
        let obs = resolve_observability(&cli, Some(&file));
        assert_eq!(
            obs.metrics_bind,
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );
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
}
