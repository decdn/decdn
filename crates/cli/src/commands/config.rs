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
    // One line per configured relay; nothing when the list is empty (the node
    // then falls back to the n0 default relays).
    for relay in &resolved.network.relay_urls {
        writeln!(w, "  relay_url:                {relay}")?;
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
            .unwrap_or("(unset — file-config origin directory)")
    )?;
    writeln!(
        w,
        "  publisher_registry_address: {}",
        resolved
            .blockchain
            .publisher_registry_address
            .as_deref()
            .unwrap_or("(unset — file-config origin directory)")
    )?;
    writeln!(
        w,
        "  rpc_watchdog_interval_sec: {}",
        resolved.blockchain.rpc_watchdog_interval_sec
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
    match resolved.cache.gc_interval_sec {
        0 => writeln!(w, "  gc_interval_sec:          disabled")?,
        n => writeln!(w, "  gc_interval_sec:          {n}")?,
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
    writeln!(
        w,
        "  prefetch_enabled:         {}",
        resolved.prefetch.enabled
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
# Multiple relays give redundancy/failover; reachability is probed at bring-up
# and logged but never fatal (the node proceeds and iroh retries in the background).
# relay_urls = ["https://relay-a.example.", "https://relay-b.example."]
# Deprecated single-relay alias (folded into relay_urls when set):
# relay_url = "https://relay.iroh.network."

[blockchain]
# rpc_url = ""                       # REQUIRED: Arbitrum Sepolia JSON-RPC URL
# eth_keystore = "~/.decdn/keystore.json"
# payment_channel_address = ""       # REQUIRED: 0x-prefixed hex
# capacity_bond_address = ""        # REQUIRED: 0x-prefixed hex
# origin_assignment_address = ""     # OPTIONAL: 0x-prefixed hex; enables chain-backed origin directory for DHT prefetch (ADR 022). Set WITH publisher_registry_address.
# publisher_registry_address = ""    # OPTIONAL: 0x-prefixed hex; pairs with origin_assignment_address
# slash_judge_address = ""           # REQUIRED: 0x-prefixed hex (EIP-712 verifyingContract, ADR 014)
# chain_id = 421614                  # EIP-712 chain id; default Arbitrum Sepolia
# rpc_watchdog_interval_sec = 30     # 0 disables the connectivity watchdog

[cache]
# cache_dir = "~/.decdn/cache"
# cache_size_mb = 10240
# max_blob_size_mb = 1024
# gc_interval_sec = 300                    # iroh-blobs GC sweep cadence; 0 disables (#518)
# max_probe_holds = 256                    # probe eviction-hold budget (ADR 005 §Hold budget); 0 disables has_blob:true
# stake_lane_reserved_holds = 0            # hold slots reserved for node-to-node probes (#757, ADR 003 §Admission); 0 = off

[payment]
# rate_per_mb = 10
# delivery_floor = 0                       # local rate-bounds clamp lower bound (ADR 005)
# delivery_ceiling = 1000000000000         # local rate-bounds clamp upper bound; must be >= 1

[observability]
# log_level = "info"
# log_format = "pretty"
# metrics_port = 9090
# admin_port = 9191                        # loopback-only; 0 disables (ADR 025)
# region_accounting_interval_sec = 3600    # 0 disables the per-region bandwidth log (#750)
# otlp_endpoint = "http://localhost:4317"  # requires --features otlp

[prefetch]
# ADR 022 speculative-prefetch operator policy. Disabled by default.
# NOTE: this slice only *meters* — it records demand, runs the decision gates,
# and exports metrics, but does not yet acquire content (the live acquisition
# is the #650 follow-up). Setting enabled = true before then exercises the
# decision/metrics path only; no prefetch bytes are fetched and no USDC spent.
# enabled = false
# require_authorized_origin = true          # require an authorized origin in the FIND_VALUE candidate set
# budget_usdc_per_hour = 0                  # micro-USDC rolling-1h spend cap; 0 = never prefetch
# find_value_threshold = 5                  # FIND_VALUE queries within the window that trip the trigger
# threshold_window_secs = 300               # rolling-window length for the trigger
# demand_quality_min_ratio = 0.1            # served/acquired auto-throttle floor
# demand_quality_window_secs = 3600         # rolling-window length for the demand-quality predicate
"#;
