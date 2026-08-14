//! `decdn config init` and `decdn config validate` — manage the deCDN
//! node TOML config file from the operator side.

use anyhow::Context;
use decdn_common::{cli, config};

use crate::known_chains;

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
        "  payment_pool_address:  {}",
        resolved.blockchain.payment_pool_address
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
        "  content_blacklist_address: {}",
        resolved
            .blockchain
            .content_blacklist_address
            .as_deref()
            .unwrap_or("(unset — REQUIRED: the node refuses to start without it)")
    )?;
    writeln!(
        w,
        "  content_blacklist_poll_interval_sec: {}",
        resolved.blockchain.content_blacklist_poll_interval_sec
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
    writeln!(
        w,
        "  redeem_threshold_micro_usdc: {}",
        resolved.blockchain.redeem_threshold_micro_usdc
    )?;
    writeln!(
        w,
        "  redeem_interval_secs: {}",
        resolved.blockchain.redeem_interval_secs
    )?;
    writeln!(
        w,
        "  buyer_initial_deposit_micro_usdc: {}",
        resolved.blockchain.buyer_initial_deposit_micro_usdc
    )?;
    writeln!(
        w,
        "  buyer_working_deposit_micro_usdc: {}",
        resolved.blockchain.buyer_working_deposit_micro_usdc
    )?;
    writeln!(
        w,
        "  buyer_max_approve: {}",
        resolved.blockchain.buyer_max_approve
    )?;
    writeln!(
        w,
        "  pool_min_remaining_deposit_micro_usdc: {}",
        resolved.blockchain.pool_min_remaining_deposit_micro_usdc
    )?;
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
    match resolved.cache.max_rate_per_mb {
        0 => writeln!(w, "  max_rate_per_mb:          unlimited (buyer)")?,
        n => writeln!(w, "  max_rate_per_mb:          {n} (buyer ceiling)")?,
    }
    match resolved.cache.gc_interval_sec {
        0 => writeln!(w, "  gc_interval_sec:          disabled")?,
        n => writeln!(w, "  gc_interval_sec:          {n}")?,
    }
    match resolved.cache.fs_rescan_interval_sec {
        0 => writeln!(
            w,
            "  fs_rescan_interval_sec:   disabled (startup + reload only)"
        )?,
        n => writeln!(w, "  fs_rescan_interval_sec:   {n}")?,
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

    let chain = known_chains::resolve(args.chain.as_deref())?;
    let contents = render_config(chain)?;

    std::fs::write(&output, &contents)
        .map_err(|e| anyhow::anyhow!("failed to write config file {}: {e}", output.display()))?;

    match chain {
        Some(c) => println!(
            "wrote {} config to {} (run `decdn key-gen` to create the keystore)",
            c.label,
            output.display()
        ),
        None => println!("wrote blank config template to {}", output.display()),
    }
    Ok(())
}

/// Render the config `config init` writes for the selected chain.
///
/// `None` returns the blank generic template ([`DEFAULT_CONFIG`]) verbatim.
/// `Some(chain)` bakes that chain's id, RPC, and manifest contract addresses
/// into the `[blockchain]` section, leaving every other section at its
/// commented defaults — so the file runs out of the box once a keystore exists.
fn render_config(chain: Option<&known_chains::KnownChain>) -> anyhow::Result<String> {
    let Some(chain) = chain else {
        return Ok(DEFAULT_CONFIG.to_string());
    };
    let filled = render_blockchain_section(chain)?;
    splice_blockchain_section(DEFAULT_CONFIG, &filled)
}

/// Build the filled `[blockchain]` section for a known chain.
fn render_blockchain_section(chain: &known_chains::KnownChain) -> anyhow::Result<String> {
    let a = chain.addresses()?;
    Ok(format!(
        "[blockchain]\n\
         # Baked in for {label} (chain {chain_id}) from the shipped deployment\n\
         # manifest (contracts/deployments/{chain_id}.json). Ready to run as-is;\n\
         # `eth_keystore` defaults to <data_dir>/keystore.json — create it with\n\
         # `decdn key-gen`. Point `rpc_url` at your own provider for production.\n\
         rpc_url = \"{rpc}\"\n\
         chain_id = {chain_id}\n\
         # eth_keystore = \"~/.decdn/keystore.json\"   # defaults to <data_dir>/keystore.json\n\
         # --- Contract addresses (from deployments/{chain_id}.json) ---\n\
         payment_pool_address       = \"{payment_pool}\"\n\
         capacity_bond_address      = \"{capacity_bond}\"\n\
         slash_judge_address        = \"{slash_judge}\"\n\
         content_blacklist_address  = \"{content_blacklist}\"\n\
         origin_assignment_address  = \"{origin_assignment}\"\n\
         publisher_registry_address = \"{publisher_registry}\"\n\
         slash_appeal_address       = \"{slash_appeal}\"\n\
         usdc_address               = \"{usdc}\"\n\
         # Optional tuning knobs (economics, poll intervals) and the DEX swap_*\n\
         # keys for `decdn setup --pay-bond-with usdc` are commented in the blank\n\
         # template — see `decdn config init --chain none`. Swap addresses are\n\
         # venue-specific and are not baked in.\n\
         \n",
        label = chain.label,
        chain_id = chain.chain_id,
        rpc = chain.public_rpc,
        payment_pool = a.payment_pool,
        capacity_bond = a.capacity_bond,
        slash_judge = a.slash_judge,
        content_blacklist = a.content_blacklist,
        origin_assignment = a.origin_assignment,
        publisher_registry = a.publisher_registry,
        slash_appeal = a.slash_appeal,
        usdc = a.usdc,
    ))
}

/// Replace the blank template's `[blockchain]` block with a filled one.
///
/// The generic template's `[blockchain]` section runs from its header to the
/// start of the next top-level section, `[cache]`. Both headers are guaranteed
/// present by the template guard tests; a missing marker is a hard error rather
/// than a silent mis-splice.
fn splice_blockchain_section(template: &str, filled: &str) -> anyhow::Result<String> {
    let start = template
        .find("\n[blockchain]\n")
        .map(|i| i + 1)
        .context("template is missing the [blockchain] section header")?;
    let rest = template
        .get(start..)
        .context("template [blockchain] slice out of bounds")?;
    let cache_rel = rest
        .find("\n[cache]\n")
        .map(|i| i + 1)
        .context("template is missing the [cache] section header after [blockchain]")?;
    let end = start + cache_rel;
    let head = template
        .get(..start)
        .context("template head slice out of bounds")?;
    let tail = template
        .get(end..)
        .context("template tail slice out of bounds")?;
    Ok(format!("{head}{filled}{tail}"))
}

/// Default TOML config file content, written by `decdn config init`.
///
/// This is the **canonical, fully-commented node config template** — the one
/// place that enumerates every section and knob. The other hand-maintained
/// copies (the `examples/configs/*.toml` samples and the e2e `render_config`
/// template) are not derived from it, so CI guards them against drift instead:
///
/// - `default_config_template_parses_with_every_section` — this template
///   parses under `deny_unknown_fields` (no stale/typo'd section headers) and
///   carries a header for *every* top-level `FileConfig` section, so adding a
///   schema section without surfacing it here fails CI.
/// - `examples_use_only_template_sections` — the shipped example configs use
///   only sections that also exist here (a sample can't reference a section the
///   canonical template lacks).
/// - `arbitrum_sepolia_{operator,client}_sample_config_matches_schema` (via the
///   `assert_sample_config_matches_schema` helper in `decdn_common::config`) —
///   the samples parse against the live `FileConfig` schema and pass the
///   resolver's chain-id / EIP-55 address checks.
/// - `render_config_emits_parseable_toml` (in `decdn_e2e`) — the e2e template
///   parses as valid TOML and spot-checks the daemon-critical keys.
///
/// When adding a config knob, update this template first — the
/// `default_config_template_covers_every_wired_field` guard below enforces
/// field-level coverage for the daemon-config sections — then the samples; the
/// guards keep them honest.
const DEFAULT_CONFIG: &str = r#"# deCDN node configuration
# CLI flags override values in this file.

[identity]
# data_dir = "~/.decdn"
# region = "US"

[network]
# bind_port = 4433
# Multiple relays give redundancy/failover; reachability is probed at bring-up
# and logged but never fatal (the node proceeds and iroh retries in the background).
# relay_urls = ["https://relay-a.example.", "https://relay-b.example."]
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
# payment_pool_address = ""          # REQUIRED: 0x-prefixed hex
# capacity_bond_address = ""        # REQUIRED: 0x-prefixed hex
# origin_assignment_address = ""     # OPTIONAL: 0x-prefixed hex; `decdn publish assign` target, and the chain-backed origin directory for cache-miss pull-through fallback (ADR 022).
# publisher_registry_address = ""    # OPTIONAL: 0x-prefixed hex; `decdn publish namespace create` target (#1029).
# slash_judge_address = ""           # REQUIRED: 0x-prefixed hex (EIP-712 verifyingContract, ADR 014)
# content_blacklist_address = ""     # REQUIRED: 0x-prefixed hex; deployed ContentBlacklist (ADR 011/031). Absent => startup fails before any ALPN accepts; the zero address is rejected (it is a fail-open compliance trap).
# content_blacklist_poll_interval_sec = 600  # blacklist watcher periodic replay + re-scope cadence (ADR 011); must be > 0; default 600s
# chain_id = 421614                  # EIP-712 chain id; default Arbitrum Sepolia
# rpc_watchdog_interval_sec = 30     # 0 disables the connectivity watchdog
# event_poll_interval_ms = 7000      # eth_getLogs tick cadence for chain watchers + pending-tx receipt polling (#1011/#1106); default 7000ms, min 250ms (lower for a local anvil)
# rate_bounds_poll_interval_sec = 3600 # authoritative getRateBounds() re-read cadence, safety net beside the RateBoundsUpdated subscription (#1172); default 3600s, must be > 0
# redeem_threshold_micro_usdc = 1000000          # seller redeems accrued vouchers on-chain at this µUSDC balance (#327); default 1 USDC
# redeem_interval_secs = 300                      # redeemer self-tick sweep cadence, the backstop beside the per-voucher hints (#327/#751); default 300s, must be > 0
# buyer_initial_deposit_micro_usdc = 500000     # deposit when OPENING a pool (first-contact lock); default 0.5 USDC
# buyer_working_deposit_micro_usdc = 10000000    # refill target on reuse or mid-transfer shortfall; 0 disables top-up; default 10 USDC
# buyer_max_approve = true                       # unlimited USDC approval for PaymentPool (#744); node default true, decdn client default false (exact deposit-sized approval); set true on the client to opt into unlimited
# pool_min_remaining_deposit_micro_usdc = 1000000 # refundable floor M the node keeps in reserve on a pool it serves (ADR 003 § Sizing); default 1 USDC
# CLI-only [blockchain] keys — consumed by `decdn setup` / `decdn appeal`, NOT the daemon.
# They live here because [blockchain] denies unknown fields and a node's node.toml is
# shared with those CLIs, so a config that drives them must still pass `config validate`.
# slash_appeal_address = ""          # `decdn appeal slash` target (ADR 028)
# swap_venue = "uniswap-v3"          # `decdn setup --pay-bond-with usdc` venue: uniswap-v3 | balancer-v3
# swap_router_address = ""           # router for swap_venue (Uniswap SwapRouter02 / Balancer V3 Router); required when swap_venue is set
# swap_quoter_address = ""           # Uniswap QuoterV2 (Uniswap venue only)
# usdc_address = ""                  # USDC token the swap spends; required when swap_venue is set
# swap_fee_tier = 3000               # Uniswap V3 pool fee tier, e.g. 500/3000/10000 (Uniswap venue only)
# swap_balancer_pool = ""            # Balancer V3 pool address (Balancer venue only)
# swap_pool_address = ""             # Uniswap V3 TOKEN/USDC pool for the advisory price-impact check (optional)

[cache]
# cache_dir = "~/.decdn/cache"
# cache_size_mb = 10240
# max_blob_size_mb = 10240             # largest single blob admitted; unset => cache_size_mb (the disk budget). Must be <= cache_size_mb; 0 = unlimited
# max_rate_per_mb = 0                      # buyer-side per-MB rate ceiling for paid cache-miss pulls (USDC base units); 0 = unlimited (#1375). Refuses a provider quote above the lower of this and the candidate's probe rate, before paying. Distinct from the seller-side [payment] delivery_floor clamp, which raises this node's own quote
# pinned_hashes = []                       # blob hashes (hex) exempted from LRU eviction (#276)
# user_agent = "decdn-node/<version>"      # User-Agent on HTTP origin pull-through (#435); default embeds the crate version
# Pull-through origin (singular). Mutually exclusive with the plural [[cache.origins]] form below.
# Empty => no pull-through; cache misses return NoOrigin.
# [cache.origin]
# kind = "http"
# url = "https://origin.example/"
# decompress = "auto"                      # optional; "auto" decompresses gzip/zstd, "strict" refuses non-identity encodings
# — or a local filesystem origin (blobs at {path}/{hex[0..2]}/{hex}):
# [cache.origin]
# kind = "fs"
# path = "/var/lib/decdn/origin"
# — or an S3-compatible origin (AWS S3 / Cloudflare R2 / Backblaze B2 / MinIO):
# [cache.origin]
# kind = "s3"
# bucket = "decdn-blobs"
# region = "us-east-1"                      # required even with a custom endpoint_url (SigV4 signing)
# endpoint_url = "https://<accountid>.r2.cloudflarestorage.com"  # for R2/B2/MinIO; omit for AWS
# path_style = true                         # required true for MinIO; AWS/R2 default to virtual-hosted-style
# prefix = "blobs/"                         # optional key prefix; final key is {prefix}{hex[0..2]}/{hex}
# decompress = "auto"                       # optional; mirrors the HTTP origin's knob
# [cache.origin.credentials]               # omit entirely to use the AWS default credential chain (env / ~/.aws / IAM role)
# source = "default-chain"                  # or "static" with access_key_id = "..." and secret_access_key = "..."
# Multi-origin fallback (#284), tried in order on a miss:
# [[cache.origins]]
# kind = "http"
# url = "https://primary.example/"
# Origin pull-through retry policy (#285); restart-required.
# [cache.origin_retry]
# max_retries = 3
# Per-origin circuit breaker (#963); restart-required. Fronts the retry loop: after
# failure_threshold consecutive origin-unavailable failures it trips OPEN and fast-fails
# every miss for cooldown_ms, then admits half_open_max_calls trial pulls. Structurally
# identical to [cache.origin_retry]. Set enabled = false (or failure_threshold = 0) to opt out.
# [cache.circuit_breaker]
# enabled = true
# failure_threshold = 5
# cooldown_ms = 30000
# half_open_max_calls = 1
# NOTE: the keys below belong to [cache], NOT to the [cache.origin_retry] table
# above — uncomment this header along with them or TOML will nest them wrongly.
# [cache]
# gc_interval_sec = 300                    # iroh-blobs GC sweep cadence; 0 disables (#518). NOTE: the eviction driver only drops GC protection, so with 0 it can never reclaim disk and cache_size_mb is unenforceable (#1173)
# fs_rescan_interval_sec = 60              # re-walk the fs origin + re-check pins into the origin-held index, so a file dropped into the origin becomes probe-answerable and DHT-announced within one interval (#1130); 0 disables the timer (startup and `decdn node reload` still rescan)
# origin_probe_ttl_sec = 15                # TTL for a memoised live-origin probe answer — a hash absent from the fs/pins index falls back to a HEAD/HeadObject against the http/s3 origin, cached this long (#1130 pt3)
# origin_probe_timeout_ms = 2000           # per-probe ceiling on the live-origin HEAD/HeadObject; on timeout the probe answers has_blob:false and the miss is memoised absent for one TTL (#1130 pt3)
# origin_probe_memo_capacity = 4096        # max distinct hashes held in the live-origin probe memo; bounds memo memory under a random-hash probe flood (#1130 pt3)
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
# credit_window_bytes = 8388608            # downstream credit window in bytes (ADR 003 §Credit window); how far past cleared payment the node streams before collecting a voucher; default 8 MiB; floored at one voucher accounting interval (4 MiB)
# voucher_commit_interval_ms = 5           # group-commit interval for durable voucher persistence (ADR 003, #1483); 0 commits each batch immediately; default 5ms

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
# max_tracked_per_ip = 4096                 # cap on the per-IP keyed-limiter map (#645); 0 = unbounded
# max_tracked_per_peer = 4096               # cap on the per-peer keyed-limiter map (#645); 0 = unbounded

[probe.rate_limit]
# per_peer_rate_per_sec = 5.0               # per-peer (NodeId) sustained rate (ADR 005); 0.0 disables the layer
# per_peer_burst = 5                        # per-peer burst capacity; required > 0 when the rate is > 0
# per_ip_rate_per_sec = 50.0                # per-IP sustained rate; 0.0 disables
# per_ip_burst = 200                        # per-IP burst capacity
# global_rate_per_sec = 1000.0              # global inbound probe sustained rate; 0.0 disables
# global_burst = 2000                       # global inbound probe burst capacity
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

    // The shipped template must parse under `deny_unknown_fields` (every
    // uncommented line is a real section header, so a typo'd header fails
    // here) AND carry a header for *every* top-level schema section. The
    // exhaustive destructure below is the load-bearing part: adding a section
    // to `FileConfig` fails to compile until it is named here, and the
    // matching `is_some` assertion then fails until the template gains the
    // header — so "add a schema section but forget the canonical template"
    // is caught at CI, not left to drift (#1402).
    #[test]
    fn default_config_template_parses_with_every_section() {
        let parsed: config::FileConfig = toml::from_str(DEFAULT_CONFIG)
            .expect("DEFAULT_CONFIG template must parse as FileConfig");
        // Exhaustive (no `..`): a new `FileConfig` field breaks this line
        // until the author accounts for it in the template + list below.
        let config::FileConfig {
            identity,
            network,
            blockchain,
            cache,
            payment,
            observability,
            gossip,
            security,
            dht,
            probe,
            receipts,
            content,
        } = &parsed;
        // Every uncommented line is a section header with no field values —
        // some nested (`[dht.rate_limit]`, `[probe.rate_limit]`) but each
        // mapping to a top-level section — so every section parses to `Some`
        // with its fields left at their built-in defaults.
        for (section, present) in [
            ("identity", identity.is_some()),
            ("network", network.is_some()),
            ("blockchain", blockchain.is_some()),
            ("cache", cache.is_some()),
            ("payment", payment.is_some()),
            ("observability", observability.is_some()),
            ("gossip", gossip.is_some()),
            ("security", security.is_some()),
            ("dht", dht.is_some()),
            ("probe", probe.is_some()),
            ("receipts", receipts.is_some()),
            ("content", content.is_some()),
        ] {
            assert!(
                present,
                "DEFAULT_CONFIG is missing a header for the [{section}] schema section"
            );
        }
    }

    // Field-level coverage for the daemon-config sections where wired knobs
    // recurringly drifted out of the template (#1554): every field of
    // `BlockchainConfig` / `CacheConfig` / `PaymentConfig` must appear (at least
    // commented) in DEFAULT_CONFIG. The exhaustive destructures are the
    // load-bearing part — adding a field to any of these structs fails to
    // compile until it is named here, and the `contains` assertion then fails
    // until the template surfaces it. `.is_none()` on each binding is just how
    // the destructured field is referenced; the token is the substring the
    // template must carry (a `key =` line for scalars, a table header for the
    // sub-table types).
    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive destructure + key list per config section reads best unsplit"
    )]
    fn default_config_template_covers_every_wired_field() {
        let config::types::BlockchainConfig {
            rpc_url,
            eth_keystore,
            payment_pool_address,
            capacity_bond_address,
            origin_assignment_address,
            publisher_registry_address,
            slash_judge_address,
            slash_appeal_address,
            content_blacklist_address,
            content_blacklist_poll_interval_sec,
            chain_id,
            rpc_watchdog_interval_sec,
            event_poll_interval_ms,
            rate_bounds_poll_interval_sec,
            redeem_threshold_micro_usdc,
            redeem_interval_secs,
            buyer_initial_deposit_micro_usdc,
            buyer_working_deposit_micro_usdc,
            buyer_max_approve,
            pool_min_remaining_deposit_micro_usdc,
            swap_venue,
            swap_router_address,
            swap_quoter_address,
            usdc_address,
            swap_fee_tier,
            swap_balancer_pool,
            swap_pool_address,
        } = &config::types::BlockchainConfig::default();
        let blockchain = [
            ("rpc_url =", rpc_url.is_none()),
            ("eth_keystore =", eth_keystore.is_none()),
            ("payment_pool_address =", payment_pool_address.is_none()),
            ("capacity_bond_address =", capacity_bond_address.is_none()),
            (
                "origin_assignment_address =",
                origin_assignment_address.is_none(),
            ),
            (
                "publisher_registry_address =",
                publisher_registry_address.is_none(),
            ),
            ("slash_judge_address =", slash_judge_address.is_none()),
            ("slash_appeal_address =", slash_appeal_address.is_none()),
            (
                "content_blacklist_address =",
                content_blacklist_address.is_none(),
            ),
            (
                "content_blacklist_poll_interval_sec =",
                content_blacklist_poll_interval_sec.is_none(),
            ),
            ("chain_id =", chain_id.is_none()),
            (
                "rpc_watchdog_interval_sec =",
                rpc_watchdog_interval_sec.is_none(),
            ),
            ("event_poll_interval_ms =", event_poll_interval_ms.is_none()),
            (
                "rate_bounds_poll_interval_sec =",
                rate_bounds_poll_interval_sec.is_none(),
            ),
            (
                "redeem_threshold_micro_usdc =",
                redeem_threshold_micro_usdc.is_none(),
            ),
            ("redeem_interval_secs =", redeem_interval_secs.is_none()),
            (
                "buyer_initial_deposit_micro_usdc =",
                buyer_initial_deposit_micro_usdc.is_none(),
            ),
            (
                "buyer_working_deposit_micro_usdc =",
                buyer_working_deposit_micro_usdc.is_none(),
            ),
            ("buyer_max_approve =", buyer_max_approve.is_none()),
            (
                "pool_min_remaining_deposit_micro_usdc =",
                pool_min_remaining_deposit_micro_usdc.is_none(),
            ),
            ("swap_venue =", swap_venue.is_none()),
            ("swap_router_address =", swap_router_address.is_none()),
            ("swap_quoter_address =", swap_quoter_address.is_none()),
            ("usdc_address =", usdc_address.is_none()),
            ("swap_fee_tier =", swap_fee_tier.is_none()),
            ("swap_balancer_pool =", swap_balancer_pool.is_none()),
            ("swap_pool_address =", swap_pool_address.is_none()),
        ];

        let config::types::CacheConfig {
            cache_dir,
            cache_size_mb,
            max_blob_size_mb,
            max_rate_per_mb,
            origin,
            origins,
            pinned_hashes,
            origin_retry,
            circuit_breaker,
            user_agent,
            gc_interval_sec,
            fs_rescan_interval_sec,
            origin_probe_ttl_sec,
            origin_probe_timeout_ms,
            origin_probe_memo_capacity,
            eviction_high_water_pct,
            eviction_target_pct,
            eviction_per_sweep_budget,
            eviction_tick_secs,
            max_probe_holds,
            stake_lane_reserved_holds,
            node_to_node_pull_through_enabled,
            node_pull_probe_fanout,
            node_pull_timeout_sec,
            node_pull_stall_timeout_sec,
            pull_ahead_bytes,
            max_unrecouped_leech_bytes,
            pull_share_ratio_percent,
        } = &config::types::CacheConfig::default();
        let cache = [
            ("cache_dir =", cache_dir.is_none()),
            ("cache_size_mb =", cache_size_mb.is_none()),
            ("max_blob_size_mb =", max_blob_size_mb.is_none()),
            ("max_rate_per_mb =", max_rate_per_mb.is_none()),
            ("[cache.origin]", origin.is_none()),
            ("[[cache.origins]]", origins.is_none()),
            ("pinned_hashes =", pinned_hashes.is_none()),
            ("[cache.origin_retry]", origin_retry.is_none()),
            ("[cache.circuit_breaker]", circuit_breaker.is_none()),
            ("user_agent =", user_agent.is_none()),
            ("gc_interval_sec =", gc_interval_sec.is_none()),
            ("fs_rescan_interval_sec =", fs_rescan_interval_sec.is_none()),
            ("origin_probe_ttl_sec =", origin_probe_ttl_sec.is_none()),
            (
                "origin_probe_timeout_ms =",
                origin_probe_timeout_ms.is_none(),
            ),
            (
                "origin_probe_memo_capacity =",
                origin_probe_memo_capacity.is_none(),
            ),
            (
                "eviction_high_water_pct =",
                eviction_high_water_pct.is_none(),
            ),
            ("eviction_target_pct =", eviction_target_pct.is_none()),
            (
                "eviction_per_sweep_budget =",
                eviction_per_sweep_budget.is_none(),
            ),
            ("eviction_tick_secs =", eviction_tick_secs.is_none()),
            ("max_probe_holds =", max_probe_holds.is_none()),
            (
                "stake_lane_reserved_holds =",
                stake_lane_reserved_holds.is_none(),
            ),
            (
                "node_to_node_pull_through_enabled =",
                node_to_node_pull_through_enabled.is_none(),
            ),
            ("node_pull_probe_fanout =", node_pull_probe_fanout.is_none()),
            ("node_pull_timeout_sec =", node_pull_timeout_sec.is_none()),
            (
                "node_pull_stall_timeout_sec =",
                node_pull_stall_timeout_sec.is_none(),
            ),
            ("pull_ahead_bytes =", pull_ahead_bytes.is_none()),
            (
                "max_unrecouped_leech_bytes =",
                max_unrecouped_leech_bytes.is_none(),
            ),
            (
                "pull_share_ratio_percent =",
                pull_share_ratio_percent.is_none(),
            ),
        ];

        let config::types::PaymentConfig {
            rate_per_mb,
            delivery_floor,
            credit_window_bytes,
            voucher_commit_interval_ms,
        } = &config::types::PaymentConfig::default();
        let payment = [
            ("rate_per_mb =", rate_per_mb.is_none()),
            ("delivery_floor =", delivery_floor.is_none()),
            ("credit_window_bytes =", credit_window_bytes.is_none()),
            (
                "voucher_commit_interval_ms =",
                voucher_commit_interval_ms.is_none(),
            ),
        ];

        for (section, keys) in [
            ("blockchain", blockchain.as_slice()),
            ("cache", cache.as_slice()),
            ("payment", payment.as_slice()),
        ] {
            for &(token, _referenced) in keys {
                assert!(
                    DEFAULT_CONFIG.contains(token),
                    "DEFAULT_CONFIG template is missing wired [{section}] key `{token}` \
                     — surface it (at least commented) so init→run does not fail on an \
                     undocumented key"
                );
            }
        }
    }

    /// The `examples/configs/*.toml` samples are hand-maintained copies of the
    /// schema, separate from `DEFAULT_CONFIG`. Each is parse-guarded in
    /// isolation (this test + `assert_sample_config_matches_schema` in
    /// `decdn_common`), but nothing otherwise asserts the samples and the
    /// canonical template agree on which sections exist. Guard that here:
    /// every top-level section a sample uses must also appear in
    /// `DEFAULT_CONFIG`, so adding a schema section to a sample without
    /// surfacing it in the template fails CI instead of drifting silently.
    ///
    /// Compared as raw `toml::Value` tables (the section headers literally
    /// present in the text), not `FileConfig` (whose fields are all-`Option`
    /// and so always "present"). Nested headers like `[dht.rate_limit]` and
    /// `[[cache.origins]]` collapse to their top-level key (`dht`, `cache`).
    #[test]
    fn examples_use_only_template_sections() {
        use std::collections::BTreeSet;

        fn top_level_sections(toml_src: &str) -> BTreeSet<String> {
            let value: toml::Value =
                toml::from_str(toml_src).expect("config must parse as a TOML table");
            value
                .as_table()
                .expect("config must be a TOML table")
                .keys()
                .cloned()
                .collect()
        }

        let template = top_level_sections(DEFAULT_CONFIG);
        let label = "arbitrum-sepolia.toml";
        let sample = include_str!("../../../../examples/configs/arbitrum-sepolia.toml");
        let sample_sections = top_level_sections(sample);
        let missing: Vec<&String> = sample_sections.difference(&template).collect();
        assert!(
            missing.is_empty(),
            "example {label} uses section(s) absent from DEFAULT_CONFIG: {missing:?} \
             (add them to the canonical template)"
        );
    }

    /// `config init` with no `--chain` (the sole-chain default) must emit a
    /// config that parses, carries the seeded chain id, and has every baked
    /// contract address active — i.e. runs out of the box modulo a keystore.
    #[test]
    fn render_config_for_default_chain_is_runnable() {
        let chain = known_chains::resolve(None)
            .expect("resolve")
            .expect("sole chain");
        let rendered = render_config(Some(chain)).expect("render");

        let cfg: config::FileConfig =
            toml::from_str(&rendered).expect("rendered --chain config must parse as FileConfig");
        let bc = cfg.blockchain.expect("[blockchain] present");

        assert_eq!(bc.chain_id, Some(chain.chain_id));
        assert_eq!(bc.rpc_url.as_deref(), Some(chain.public_rpc));

        // Every manifest-derived address is filled in (not left commented), and
        // equals the manifest exactly.
        let a = chain.addresses().expect("addresses");
        assert_eq!(bc.payment_pool_address, Some(a.payment_pool));
        assert_eq!(bc.capacity_bond_address, Some(a.capacity_bond));
        assert_eq!(bc.slash_judge_address, Some(a.slash_judge));
        assert_eq!(bc.content_blacklist_address, Some(a.content_blacklist));
        assert_eq!(bc.origin_assignment_address, Some(a.origin_assignment));
        assert_eq!(bc.publisher_registry_address, Some(a.publisher_registry));
        assert_eq!(bc.slash_appeal_address, Some(a.slash_appeal));
        assert_eq!(bc.usdc_address, Some(a.usdc));

        // Splicing preserved the other sections.
        for header in ["[identity]", "[cache]", "[payment]", "[content]"] {
            assert!(rendered.contains(header), "rendered config lost {header}");
        }
    }

    /// `--chain none` reproduces the blank template byte-for-byte, so the
    /// generic path is unchanged and every drift guard above still applies.
    #[test]
    fn render_config_none_equals_default_template() {
        let rendered = render_config(None).expect("render");
        assert_eq!(rendered, DEFAULT_CONFIG);
    }
}
