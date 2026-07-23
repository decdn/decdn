//! `decdn config init` and `decdn config validate` — manage the deCDN
//! node TOML config file from the operator side.

use decdn_common::{cli, config};

/// Validate a configuration and print a resolved summary.
///
/// Runs the daemon's resolver against the same TOML + CLI + env-var
/// inputs `decdn-node run` would use, then prints the result. Does not
/// bind any ports, does not connect to the JSON-RPC endpoint, and does
/// not open the cache. Does touch the filesystem to verify the config
/// itself: reads the TOML, and stats / opens the configured
/// `blockchain.eth_keystore` for readability (the resolver rejects
/// missing paths, directories, and broken symlinks at this point so
/// the daemon doesn't fail later in startup).
pub fn config_validate(
    config_path: Option<&std::path::Path>,
    args: &cli::ConfigValidateArgs,
) -> anyhow::Result<()> {
    let resolved = config::resolve_config(config_path, &args.run)?;
    let source = effective_source(config_path, cli::common::default_config_path)?;
    let mut stdout = std::io::stdout().lock();
    write_validate_summary(&mut stdout, source.as_deref(), &resolved)
        .map_err(|e| anyhow::anyhow!("failed to write summary: {e}"))
}

/// Resolve the config source actually consulted. When `--config` is omitted,
/// the default path is auto-loaded only if present, so report the file that
/// `resolve_config` will actually read — not just the explicit flag.
///
/// Uses `try_exists` rather than `exists`: a permission error on the default
/// path must be surfaced, not silently reported as "no config file" — the
/// whole point of `validate` is telling the operator what the node sees.
pub fn effective_source(
    config_path: Option<&std::path::Path>,
    default: impl FnOnce() -> Option<std::path::PathBuf>,
) -> anyhow::Result<Option<std::path::PathBuf>> {
    if let Some(p) = config_path {
        return Ok(Some(p.to_path_buf()));
    }
    let Some(default) = default() else {
        return Ok(None);
    };
    match default.try_exists() {
        Ok(true) => Ok(Some(default)),
        Ok(false) => Ok(None),
        Err(e) => Err(anyhow::anyhow!(
            "cannot determine whether default config {} exists: {e}",
            default.display()
        )),
    }
}

/// Render the validation summary. Separated from [`config_validate`] so tests
/// can capture the output into a buffer and assert on the printed contract —
/// in particular that `rpc_url` and `otlp_endpoint` values never appear.
// Linear "print each resolved config field" flow sitting right at the 100-line
// boundary; splitting the writeln! sequence across helpers would obscure the
// field-by-field narrative more than the length does.
#[allow(clippy::too_many_lines)]
pub fn write_validate_summary<W: std::io::Write>(
    w: &mut W,
    source: Option<&std::path::Path>,
    resolved: &config::ResolvedConfig,
) -> std::io::Result<()> {
    writeln!(w, "config valid")?;
    match source {
        Some(path) => writeln!(w, "  source:                   {}", path.display())?,
        None => writeln!(
            w,
            "  source:                   (defaults + env only, no config file)"
        )?,
    }
    writeln!(
        w,
        "  data_dir:                 {}",
        resolved.identity.data_dir.display()
    )?;
    if let Some(region) = &resolved.identity.region {
        writeln!(w, "  region:                   {region}")?;
    }
    writeln!(
        w,
        "  bind_port:                {}",
        resolved.network.bind_port
    )?;
    writeln!(
        w,
        "  enable_0rtt:              {}",
        resolved.network.enable_0rtt
    )?;
    // One line per configured relay; nothing when the list is empty (the node
    // then falls back to the n0 default relays). Relay URLs can carry
    // `user:pass@` userinfo, so redact it (host stays visible) — the same
    // treatment pkarr_url gets below, and the reason rpc_url/otlp_endpoint are
    // fully hidden in this same summary (#862).
    for relay in &resolved.network.relay_urls {
        writeln!(
            w,
            "  relay_url:                {}",
            decdn_common::redact::redact_userinfo(relay)
        )?;
    }
    // Operator-configurable discovery (#818); nothing printed when unset (the
    // node then uses the n0 pkarr/DNS default). A pkarr_url is an infra relay
    // endpoint — pkarr authenticates by the Ed25519-signed packet, not a URL
    // token — so its credentials, if any, are normally `user:pass@` userinfo.
    // Redact that (host stays visible for diagnostics) rather than fully hiding
    // it like rpc_url/otlp_endpoint, whose secret commonly lives in the path or
    // query. Caveat: `redact_userinfo` does NOT scrub a path/query secret, so a
    // nonstandard relay that put one there would still print it — acceptable
    // given pkarr's auth model and that relays themselves redact userinfo
    // above. dns_origin is a plain domain. Peer entries (id/relay/addrs) are
    // not echoed (verbose) — only the count.
    let discovery = &resolved.network.discovery;
    if let Some(pkarr) = &discovery.pkarr_url {
        writeln!(
            w,
            "  discovery.pkarr_url:      {}",
            decdn_common::redact::redact_userinfo(pkarr)
        )?;
    }
    if let Some(origin) = &discovery.dns_origin {
        writeln!(w, "  discovery.dns_origin:     {origin}")?;
    }
    if !discovery.peers.is_empty() {
        writeln!(w, "  discovery.peers:          {}", discovery.peers.len())?;
    }
    writeln!(
        w,
        "  rpc_url:                  <redacted> ({} chars)",
        resolved.blockchain.rpc_url.len()
    )?;
    writeln!(
        w,
        "  eth_keystore:             {}",
        resolved.blockchain.eth_keystore.display()
    )?;
    writeln!(
        w,
        "  payment_channel_address:  {}",
        resolved.blockchain.payment_channel_address
    )?;
    writeln!(
        w,
        "  capacity_bond_address: {}",
        resolved.blockchain.capacity_bond_address
    )?;
    writeln!(
        w,
        "  origin_assignment_address: {}",
        resolved
            .blockchain
            .origin_assignment_address
            .as_deref()
            .unwrap_or("(unset — origin directory empty: pull-through gate and FIND_VALUE fallback find no origins)")
    )?;
    writeln!(
        w,
        "  publisher_registry_address: {}",
        resolved
            .blockchain
            .publisher_registry_address
            .as_deref()
            .unwrap_or("(unset — origin directory empty: pull-through gate and FIND_VALUE fallback find no origins)")
    )?;
    writeln!(
        w,
        "  origin_directory_from_block: {}",
        resolved.blockchain.origin_directory_from_block
    )?;
    writeln!(
        w,
        "  rpc_watchdog_interval_sec: {}",
        resolved.blockchain.rpc_watchdog_interval_sec
    )?;
    writeln!(
        w,
        "  event_poll_interval_ms: {}",
        resolved.blockchain.event_poll_interval_ms
    )?;
    writeln!(
        w,
        "  rate_bounds_poll_interval_sec: {}",
        resolved.blockchain.rate_bounds_poll_interval_sec
    )?;
    match resolved.blockchain.settlement_auto_threshold_micro_usdc {
        Some(v) => writeln!(w, "  settlement_auto_threshold_micro_usdc: {v}")?,
        None => writeln!(w, "  settlement_auto_threshold_micro_usdc: disabled")?,
    }
    match resolved.blockchain.settlement_auto_by_voucher_nonce_span {
        Some(v) => writeln!(w, "  settlement_auto_by_voucher_nonce_span: {v}")?,
        None => writeln!(w, "  settlement_auto_by_voucher_nonce_span: disabled")?,
    }
    writeln!(
        w,
        "  cache_dir:                {}",
        resolved.cache.cache_dir.display()
    )?;
    writeln!(
        w,
        "  cache_size_mb:            {}",
        resolved.cache.cache_size_mb
    )?;
    writeln!(
        w,
        "  max_blob_size_mb:         {}",
        resolved.cache.max_blob_size_mb
    )?;
    match resolved.cache.gc_interval_sec {
        0 => writeln!(w, "  gc_interval_sec:          disabled")?,
        n => writeln!(w, "  gc_interval_sec:          {n}")?,
    }
    writeln!(
        w,
        "  eviction:                 high_water_pct={}, target_pct={}, per_sweep_budget={}, tick_secs={}",
        resolved.cache.eviction_high_water_pct,
        resolved.cache.eviction_target_pct,
        resolved.cache.eviction_per_sweep_budget,
        resolved.cache.eviction_tick_secs
    )?;
    if resolved.cache.node_to_node_pull_through_enabled {
        writeln!(
            w,
            "  node_to_node_pull:        enabled (probe_fanout={}, pull_timeout_sec={}, stall_timeout_sec={})",
            resolved.cache.node_pull_probe_fanout,
            resolved.cache.node_pull_timeout_sec,
            resolved.cache.node_pull_stall_timeout_sec
        )?;
        writeln!(
            w,
            "  pull_through_caps:        pull_ahead_bytes={}, max_unrecouped_leech_bytes={}, share_ratio_percent={}",
            resolved.cache.pull_ahead_bytes,
            resolved.cache.max_unrecouped_leech_bytes,
            resolved.cache.pull_share_ratio_percent
        )?;
    } else {
        writeln!(w, "  node_to_node_pull:        disabled")?;
    }
    writeln!(
        w,
        "  rate_per_mb:              {}",
        resolved.payment.rate_per_mb
    )?;
    writeln!(
        w,
        "  voucher_interval_mb:      {}",
        resolved.payment.voucher_interval_mb
    )?;
    writeln!(
        w,
        "  log_level:                {}",
        resolved.observability.log_level
    )?;
    writeln!(
        w,
        "  metrics_port:             {}",
        resolved.observability.metrics_port
    )?;
    writeln!(
        w,
        "  metrics_bind:             {}",
        resolved.observability.metrics_bind
    )?;
    match resolved.observability.admin_port {
        Some(p) => writeln!(w, "  admin_port:               {p}")?,
        None => writeln!(w, "  admin_port:               disabled")?,
    }
    match resolved.observability.region_accounting_interval_sec {
        0 => writeln!(w, "  region_accounting_interval_sec: disabled")?,
        n => writeln!(w, "  region_accounting_interval_sec: {n}")?,
    }
    // otlp_endpoint URLs commonly carry bearer tokens or API keys in the
    // path or query, so redact like rpc_url.
    if let Some(otlp) = &resolved.observability.otlp_endpoint {
        writeln!(
            w,
            "  otlp_endpoint:            <redacted> ({} chars)",
            otlp.len()
        )?;
    }
    // Surface the kill-switch / rate-limit knobs of the otherwise-silent
    // sections — the throttles an operator most needs to confirm, not every
    // field. All numeric/bool/bind-addr, so no secret-redaction concern.
    writeln!(
        w,
        "  security.max_concurrent_handlers: {}",
        resolved.security.max_concurrent_handlers
    )?;
    writeln!(
        w,
        "  security.per_source_rate_per_sec: {}",
        resolved.security.per_source_rate_per_sec
    )?;
    writeln!(
        w,
        "  security.per_source_burst: {}",
        resolved.security.per_source_burst
    )?;
    writeln!(
        w,
        "  dht.per_peer_rate_per_sec: {}",
        resolved.dht.per_peer_rate_per_sec
    )?;
    writeln!(
        w,
        "  dht.global_rate_per_sec:  {}",
        resolved.dht.global_rate_per_sec
    )?;
    writeln!(
        w,
        "  probe.per_peer_rate_per_sec: {}",
        resolved.probe.per_peer_rate_per_sec
    )?;
    writeln!(
        w,
        "  probe.global_rate_per_sec: {}",
        resolved.probe.global_rate_per_sec
    )?;
    writeln!(
        w,
        "  gossip.subscribe_global:  {}",
        resolved.gossip.subscribe_global
    )?;
    writeln!(
        w,
        "  gossip.max_peer_entries: {}",
        resolved
            .gossip
            .max_peer_entries
            .map_or_else(|| "unlimited".to_string(), |n| n.to_string())
    )?;
    // Download-receipt audit log (#802). The log lives at a fixed filename
    // inside data_dir; surface the resolved path so an operator can confirm
    // where receipts land without reading the daemon source, alongside the
    // rotation cap and retained-backup count that bound its disk use. The path
    // is derived (not a configurable field) — the daemon writes it to this
    // canonical name under data_dir.
    writeln!(
        w,
        "  receipts.log_path:        {}",
        resolved
            .identity
            .data_dir
            .join(config::RECEIPT_LOG_FILE)
            .display()
    )?;
    writeln!(
        w,
        "  receipts.max_file_bytes:  {}",
        resolved.receipts.max_file_bytes
    )?;
    writeln!(
        w,
        "  receipts.retained_files:  {}",
        resolved.receipts.retained_files
    )?;
    // Counts, not contents. An operator running `config validate` after adding a
    // takedown wants confirmation the entries were accepted — and a zero here is
    // the tell that a `[content]` section landed in the wrong file. Printing the
    // hashes themselves would put the subject of a legal order into terminal
    // scrollback and any CI log that captures it.
    writeln!(
        w,
        "  content.denied_hashes:    {}",
        resolved.content.denied_hashes.len()
    )?;
    writeln!(
        w,
        "  content.denied_origins:   {}",
        resolved.content.denied_origins.len()
    )?;
    Ok(())
}

/// Write a default TOML configuration file.
pub fn config_init(args: &cli::ConfigInitArgs) -> anyhow::Result<()> {
    let output = args
        .output
        .as_deref()
        .map(cli::common::expand_tilde)
        .or_else(cli::default_config_path)
        .ok_or_else(|| anyhow::anyhow!("cannot determine config path: home dir not found"))?;

    if output.exists() && !args.force {
        anyhow::bail!(
            "config file already exists at {}; use --force to overwrite",
            output.display()
        );
    }

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("failed to create directory {}: {e}", parent.display()))?;
    }

    std::fs::write(&output, DEFAULT_CONFIG)
        .map_err(|e| anyhow::anyhow!("failed to write config file {}: {e}", output.display()))?;

    println!("wrote default config to {}", output.display());
    Ok(())
}

/// Default TOML config file content.
const DEFAULT_CONFIG: &str = r#"# deCDN node configuration
# CLI flags override values in this file.

[identity]
# data_dir = "~/.decdn"
# region = "US"

[network]
# bind_port = 4433
# enable_0rtt = true                 # QUIC 0-RTT for cdn/probe/v1 (ADR 015); set false to require a full handshake
# Multiple relays give redundancy/failover; reachability is probed at bring-up
# and logged but never fatal (the node proceeds and iroh retries in the background).
# relay_urls = ["https://relay-a.example.", "https://relay-b.example."]
# Deprecated single-relay alias (folded into relay_urls when set):
# relay_url = "https://relay.iroh.network."
# Operator-configurable address discovery (#818). Absent => n0-hosted pkarr/DNS.
# Present => the node drops the n0 discovery leg and uses only what is set here.
# [network.discovery]
# pkarr_url = "https://pkarr.example./"      # publish this node's signed address record here
# dns_origin = "discovery.example."          # resolve peers via DNS TXT under this origin
# Static peer address book (fully offline; keyed by NodeId — 64-char lowercase hex):
# [network.discovery.peers.0000000000000000000000000000000000000000000000000000000000000000]
# relay_url = "https://relay.example./"
# addrs = ["203.0.113.4:4433"]

[blockchain]
# rpc_url = ""                       # REQUIRED: Arbitrum Sepolia JSON-RPC URL
# eth_keystore = "~/.decdn/keystore.json"
# payment_channel_address = ""       # REQUIRED: 0x-prefixed hex
# capacity_bond_address = ""        # REQUIRED: 0x-prefixed hex
# origin_assignment_address = ""     # OPTIONAL: 0x-prefixed hex; `decdn publish assign` target, and the chain-backed origin directory for cache-miss pull-through fallback (ADR 022).
# publisher_registry_address = ""    # OPTIONAL: 0x-prefixed hex; `decdn publish namespace create` target (#1029).
# origin_directory_from_block = 0    # OPTIONAL: AssignmentActivated log-replay start; set to the OriginAssignment deploy block (default 0 scans the whole chain)
# slash_judge_address = ""           # REQUIRED: 0x-prefixed hex (EIP-712 verifyingContract, ADR 014)
# chain_id = 421614                  # EIP-712 chain id; default Arbitrum Sepolia
# rpc_watchdog_interval_sec = 30     # 0 disables the connectivity watchdog
# event_poll_interval_ms = 7000      # eth_getLogs tick cadence for chain watchers + pending-tx receipt polling (#1011/#1106); default 7000ms, min 250ms (lower for a local anvil)
# rate_bounds_poll_interval_sec = 3600 # authoritative getRateBounds() re-read cadence, safety net beside the RateBoundsUpdated subscription (#1172); default 3600s, must be > 0
# redeem_threshold_micro_usdc = 1000000          # seller redeems accrued vouchers on-chain at this µUSDC balance (#327); default 1 USDC
# buyer_deposit_micro_usdc = 10000000            # deposit when the buyer opens a node-to-node PaymentChannel on a miss (#744); default 10 USDC
# buyer_max_approve = true                       # unlimited USDC approval for PaymentChannel (#744); node default true, decdn client default false (exact deposit-sized approval); set true on the client to opt into unlimited
# settlement_auto_threshold_micro_usdc = 50000000   # auto-closeChannel once un-redeemed µUSDC reaches this (#742); leave unset/commented to disable — when set it must be > 0
# settlement_auto_by_voucher_nonce_span = 1000       # auto-closeChannel once the un-redeemed nonce span reaches this (#742); leave unset/commented to disable — when set it must be > 0

[cache]
# cache_dir = "~/.decdn/cache"
# cache_size_mb = 10240
# max_blob_size_mb = 1024
# pinned_hashes = []                       # blob hashes (hex) exempted from LRU eviction (#276)
# user_agent = "decdn-node/<version>"      # User-Agent on HTTP origin pull-through (#435); default embeds the crate version
# Pull-through origin (singular). Mutually exclusive with the plural [[cache.origins]] form below.
# Empty => no pull-through; cache misses return NoOrigin.
# [cache.origin]
# kind = "http"
# url = "https://origin.example/"
# Multi-origin fallback (#284), tried in order on a miss:
# [[cache.origins]]
# kind = "http"
# url = "https://primary.example/"
# Origin pull-through retry policy (#285); restart-required.
# [cache.origin_retry]
# max_retries = 3
# NOTE: the keys below belong to [cache], NOT to the [cache.origin_retry] table
# above — uncomment this header along with them or TOML will nest them wrongly.
# [cache]
# gc_interval_sec = 300                    # iroh-blobs GC sweep cadence; 0 disables (#518). NOTE: the eviction driver only drops GC protection, so with 0 it can never reclaim disk and cache_size_mb is unenforceable (#1173)
# eviction_high_water_pct = 90             # LRU driver evicts above this % of cache_size_mb (#1173); bounds [60,95]
# eviction_target_pct = 80                 # LRU driver evicts down to this % (#1173); bounds [40,90], must be <= high_water-5
# eviction_per_sweep_budget = 16           # max LRU victims per tick before yielding (#1173); bounds [1,256]
# eviction_tick_secs = 1                   # LRU driver wakeup cadence in seconds (#1173); bounds [1,60]
# max_probe_holds = 256                    # probe eviction-hold budget (ADR 005 §Hold budget); 0 disables has_blob:true
# stake_lane_reserved_holds = 0            # hold slots reserved for node-to-node probes (#757, ADR 003 §Admission); 0 = off
# node_to_node_pull_through_enabled = false # paid cache-miss pull from upstream nodes (#831, ADR 001/022); OFF by default
# node_pull_probe_fanout = 5               # providers probed before ranking on a node-to-node pull (#831)
# node_pull_timeout_sec = 20               # per-upstream STREAM-OPEN timeout (connect/handshake/response) on a node-to-node miss; NOT the channel open, which has its own 5s budget. The overall pull-through deadline is derived from this, the channel-open budget, and the stall timeout, so every ranked upstream can be tried before falling back (#831, #859)
# node_pull_stall_timeout_sec = 20         # per-upstream INACTIVITY timeout while streaming (#1134); the clock resets on every byte, so it trips only on a silent upstream — not on a large blob or a slow link. Budgeted per candidate, so raising it raises the worst-case client wait ~3x (167.5s at defaults)
# pull_ahead_bytes = 1048576               # window-paced pull-through pipeline window (#856, ADR 037); per-request speculative loss is bounded to this many bytes
# max_unrecouped_leech_bytes = 268435456   # node-wide unrecouped-leech budget in bytes (#856, ADR 037); aggregate speculative spend above this pauses until served bytes recoup it; 0 disables
# pull_share_ratio_percent = 400           # per-peer pull ceiling as a percent of bytes served to that peer (#856, ADR 037); 100 == 1.0x, plus an opening pull_ahead_bytes allowance

[payment]
# rate_per_mb = 10
# delivery_floor = 0                       # PRE-CHAIN SEED ONLY (#1172): overwritten from on-chain getRateBounds() before serving; governance owns the live floor
# delivery_ceiling = 1000000000000         # PRE-CHAIN SEED ONLY (#1172): overwritten from on-chain getRateBounds(); must be >= 1
# voucher_interval_mb = 1                  # voucher cadence advertised on cdn/client/v1 (ADR 003); range 1..=MAX_VOUCHER_INTERVAL_MB

[observability]
# log_level = "info"
# log_format = "pretty"
# metrics_port = 9090
# metrics_bind = "127.0.0.1"               # IP the metrics HTTP server binds; default loopback only
# admin_port = 9191                        # loopback-only; 0 disables (ADR 025)
# region_accounting_interval_sec = 3600    # 0 disables the per-region bandwidth log (#750)
# otlp_endpoint = "http://localhost:4317"  # requires --features otlp

[gossip]
# announce_interval_sec = 60                # seconds between outgoing NodeAnnounce messages (ADR 001)
# peer_ttl_sec = 600                        # evict a peer-table entry after this long unrefreshed
# subscribe_global = true                   # subscribe/publish on cdn/global/v1
# max_peer_entries = 100000                 # optional hard cap on PeerTable entries; omit = unlimited; must be > 0 when set

[security]
# max_concurrent_handlers = 256             # global cap on in-flight QUIC handler tasks; 0 disables the cap
# per_source_rate_per_sec = 100.0           # per-source rate-limit refill (cells/sec); 0.0 disables the layer
# per_source_burst = 200                    # per-source burst capacity; required > 0 when the rate is > 0
# max_tracked_sources = 4096                # cap on tracked sources in the keyed limiter; 0 = unbounded

[dht.rate_limit]
# per_peer_rate_per_sec = 20.0              # per-peer (NodeId) sustained rate (ADR 022); 0.0 disables the layer
# per_peer_burst = 40                       # per-peer burst capacity; required > 0 when the rate is > 0
# per_ip_rate_per_sec = 100.0               # per-IP sustained rate; 0.0 disables
# per_ip_burst = 200                        # per-IP burst capacity
# global_rate_per_sec = 1000.0              # global inbound DHT sustained rate; 0.0 disables
# global_burst = 2000                       # global inbound DHT burst capacity
# trusted_ips = []                          # IPs that bypass the per-IP layer only (ADR 022 §Trusted-IP exemption)
# max_tracked_per_ip = 4096                 # cap on the per-IP keyed-limiter map (#645); 0 = unbounded
# max_tracked_per_peer = 4096               # cap on the per-peer keyed-limiter map (#645); 0 = unbounded

[probe.rate_limit]
# per_peer_rate_per_sec = 5.0               # per-peer (NodeId) sustained rate (ADR 005); 0.0 disables the layer
# per_peer_burst = 5                        # per-peer burst capacity; required > 0 when the rate is > 0
# per_ip_rate_per_sec = 50.0                # per-IP sustained rate; 0.0 disables
# per_ip_burst = 200                        # per-IP burst capacity
# global_rate_per_sec = 1000.0              # global inbound probe sustained rate; 0.0 disables
# global_burst = 2000                       # global inbound probe burst capacity
# trusted_ips = []                          # IPs that bypass the per-IP layer only (ADR 005 §Trusted-IP exemption)
# max_tracked_per_ip = 4096                 # cap on the per-IP keyed-limiter map (#645); 0 = unbounded
# max_tracked_per_peer = 4096               # cap on the per-peer keyed-limiter map (#645); 0 = unbounded

[receipts]
# max_file_bytes = 134217728                # rotate the download-receipt log at this size (#802); default 128 MiB
# retained_files = 4                        # rotated backup receipt files retained (#802); 0 keeps none

[content]
# ADR 011 local denylist — this operator's own removal lever, independent of
# governance. Entries take effect on `decdn node reload` (no restart), are never
# gossiped, and bind only this node. This is the fastest removal path the
# protocol offers and the one sized to a sub-day statutory deadline (e.g. the EU
# TCO one-hour clock), because it is entirely within the order recipient's
# control. Refused requests are signed as HashBlacklisted / OriginBlacklisted,
# which do not reveal whether the entry is local or on-chain.
#
# Hashes are bare 64-char lowercase hex — the same spelling as
# cache.pinned_hashes. An invalid entry FAILS startup rather than being skipped:
# a typo in a takedown must not silently leave content served.
# denied_hashes = ["0000000000000000000000000000000000000000000000000000000000000000"]
# denied_origins = ["0x000000000000000000000000000000000000dEaD"]   # operator addresses whose channels are refused (the zero address is rejected)
"#;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // The shipped template must parse under `deny_unknown_fields`: every
    // uncommented line is a real section header or field. Locks the four
    // new sections' header names (`[gossip]`, `[security]`,
    // `[dht.rate_limit]`, `[receipts]`) against typos that would only
    // surface when an operator uncommented a knob.
    #[test]
    fn default_config_template_parses() {
        let parsed: config::FileConfig = toml::from_str(DEFAULT_CONFIG)
            .expect("DEFAULT_CONFIG template must parse as FileConfig");
        // Only bare section headers are uncommented, so every section is
        // Some but each field stays at its built-in default (None on the
        // wire form). Spot-check the four added sections are recognized.
        assert!(parsed.gossip.is_some(), "[gossip] header parsed");
        assert!(parsed.security.is_some(), "[security] header parsed");
        assert!(parsed.dht.is_some(), "[dht.rate_limit] header parsed");
        assert!(parsed.probe.is_some(), "[probe.rate_limit] header parsed");
        assert!(parsed.receipts.is_some(), "[receipts] header parsed");
        assert!(parsed.content.is_some(), "[content] header parsed");
    }
}
