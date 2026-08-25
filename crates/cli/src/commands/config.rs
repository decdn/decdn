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
        "  fee_shares_poll_interval_sec: {}",
        resolved.blockchain.fee_shares_poll_interval_sec
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
    } else {
        writeln!(w, "  node_to_node_pull:        disabled")?;
    }
    if resolved.cache.relay_foreign_namespaces {
        writeln!(w, "  relay_foreign_namespaces: true")?;
    } else {
        writeln!(w, "  relay_foreign_namespaces: false (origin-only node)")?;
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

/// The config shape `config init` writes, implied by the role flags (#1772).
///
/// Role is implied by `--origin`, not a separate flag: presence of an origin
/// backend IS the role signal, exactly mirroring the runtime's role-derived
/// `cache.relay_foreign_namespaces` default — so the generated config and the
/// resolver agree by construction. `--client` is a distinct role, not the
/// absence of the others: a consumer that fetches and pays, with none of the
/// serving sections.
#[derive(Debug)]
pub enum Role {
    /// No role flag: a serving node with no origin backend — relays foreign
    /// namespaces for pay (`relay_foreign_namespaces` derives to `true`).
    Relay,
    /// `--origin <URL>`: a serving node with an active `[cache.origin]`
    /// backend (`relay_foreign_namespaces` derives to `false`, origin-only).
    Origin(decdn_config_types::OriginUrl),
    /// `--client`: a fetch-only consumer — identity + chain coordinates,
    /// none of the cache/origin/serving sections.
    Client,
}

/// Map the `config init` role flags to a [`Role`]. `--origin` and `--client`
/// are mutually exclusive at the clap layer (`conflicts_with`); the URL is
/// validated and normalized through the same [`decdn_config_types::parse_origin_url`]
/// the daemon's resolver uses, so a URL this accepts cannot fail resolution.
fn resolve_role(args: &cli::ConfigInitArgs) -> anyhow::Result<Role> {
    if args.client {
        return Ok(Role::Client);
    }
    match args.origin.as_deref() {
        Some(raw) => Ok(Role::Origin(decdn_config_types::parse_origin_url(raw)?)),
        None => Ok(Role::Relay),
    }
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

    let role = resolve_role(args)?;
    let chain = known_chains::resolve(args.chain.as_deref())?;
    let contents = render_config(chain, &role)?;

    std::fs::write(&output, &contents)
        .map_err(|e| anyhow::anyhow!("failed to write config file {}: {e}", output.display()))?;

    let role_word = match &role {
        Role::Relay => "relay-node",
        Role::Origin(_) => "origin-node",
        Role::Client => "client",
    };
    match chain {
        Some(c) => println!(
            "wrote {} {role_word} config to {} (run `decdn key-gen` to create the keystore)",
            c.label,
            output.display()
        ),
        None => println!(
            "wrote blank {role_word} config template to {}",
            output.display()
        ),
    }
    if let Role::Origin(url) = &role {
        println!(
            "origin backend: {} — cache.relay_foreign_namespaces derives to false (origin-only)",
            decdn_config_types::redact_for_log(url.as_url())
        );
    }
    Ok(())
}

/// Render the config `config init` writes for the selected chain and role.
///
/// Chain `None` starts from the blank template; `Some(chain)` bakes that
/// chain's id, RPC, and manifest contract addresses into the `[blockchain]`
/// section — so the file runs out of the box once a keystore exists. The role
/// then picks the shape: [`Role::Client`] uses the trimmed [`CLIENT_CONFIG`]
/// template, [`Role::Origin`] additionally activates a `[cache.origin]` block,
/// and [`Role::Relay`] leaves the node template as-is.
fn render_config(chain: Option<&known_chains::KnownChain>, role: &Role) -> anyhow::Result<String> {
    if let Role::Client = role {
        let Some(chain) = chain else {
            return Ok(CLIENT_CONFIG.to_string());
        };
        let filled = render_client_blockchain_section(chain)?;
        // The client template ends with `[blockchain]`, so the splice runs to EOF.
        return splice_section(CLIENT_CONFIG, &filled, "\n[blockchain]\n", None);
    }
    let base = match chain {
        None => DEFAULT_CONFIG.to_string(),
        Some(chain) => {
            let filled = render_blockchain_section(chain)?;
            splice_section(
                DEFAULT_CONFIG,
                &filled,
                "\n[blockchain]\n",
                Some("\n[cache]\n"),
            )?
        }
    };
    match role {
        Role::Origin(url) => insert_origin_block(&base, url),
        Role::Relay | Role::Client => Ok(base),
    }
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

/// Build the filled `[blockchain]` section for the client shape of a known
/// chain. The client consumes `rpc_url`, `chain_id`, `eth_keystore`,
/// `payment_pool_address`, and `usdc_address`; the remaining contract
/// addresses are daemon/operator keys the client ignores, kept active so the
/// same file also passes `decdn config validate` (whose resolver requires
/// `capacity_bond_address` and `slash_judge_address`).
fn render_client_blockchain_section(chain: &known_chains::KnownChain) -> anyhow::Result<String> {
    let a = chain.addresses()?;
    Ok(format!(
        "[blockchain]\n\
         # Baked in for {label} (chain {chain_id}) from the shipped deployment\n\
         # manifest (contracts/deployments/{chain_id}.json). The client consumes the\n\
         # first block of keys; the remaining contract addresses are daemon/operator\n\
         # keys it ignores, kept active so this file also passes `decdn config\n\
         # validate`. Ready to fetch as-is once the keystore exists (`decdn key-gen`)\n\
         # and holds USDC. Point `rpc_url` at your own provider for production.\n\
         rpc_url = \"{rpc}\"\n\
         chain_id = {chain_id}\n\
         # eth_keystore = \"~/.decdn/keystore.json\"   # defaults to <data_dir>/keystore.json\n\
         payment_pool_address       = \"{payment_pool}\"\n\
         usdc_address               = \"{usdc}\"\n\
         # buyer_working_deposit_micro_usdc = 10000000  # deposit when OPENING a pool and refill target; must be > 0; default 10 USDC\n\
         # buyer_max_approve = false          # exact deposit-sized USDC approvals (client default); true opts into an unlimited standing approval\n\
         # --- Daemon/operator addresses (ignored by the client) ---\n\
         capacity_bond_address      = \"{capacity_bond}\"\n\
         slash_judge_address        = \"{slash_judge}\"\n\
         content_blacklist_address  = \"{content_blacklist}\"\n\
         origin_assignment_address  = \"{origin_assignment}\"\n\
         publisher_registry_address = \"{publisher_registry}\"\n\
         slash_appeal_address       = \"{slash_appeal}\"\n",
        label = chain.label,
        chain_id = chain.chain_id,
        rpc = chain.public_rpc,
        payment_pool = a.payment_pool,
        usdc = a.usdc,
        capacity_bond = a.capacity_bond,
        slash_judge = a.slash_judge,
        content_blacklist = a.content_blacklist,
        origin_assignment = a.origin_assignment,
        publisher_registry = a.publisher_registry,
        slash_appeal = a.slash_appeal,
    ))
}

/// Replace one top-level section of a template with a filled block.
///
/// The section runs from `start_header` to `end_header` — or to the end of the
/// template when `end_header` is `None` (for a template whose spliced section
/// is last). The needed headers are guaranteed present by the template guard
/// tests; a missing marker is a hard error rather than a silent mis-splice.
fn splice_section(
    template: &str,
    filled: &str,
    start_header: &str,
    end_header: Option<&str>,
) -> anyhow::Result<String> {
    let start = template
        .find(start_header)
        .map(|i| i + 1)
        .with_context(|| {
            format!(
                "template is missing the {} section header",
                start_header.trim()
            )
        })?;
    let rest = template
        .get(start..)
        .context("template section slice out of bounds")?;
    let end = match end_header {
        Some(header) => {
            let rel = rest.find(header).map(|i| i + 1).with_context(|| {
                format!(
                    "template is missing the {} section header after {}",
                    header.trim(),
                    start_header.trim()
                )
            })?;
            start + rel
        }
        None => template.len(),
    };
    let head = template
        .get(..start)
        .context("template head slice out of bounds")?;
    let tail = template
        .get(end..)
        .context("template tail slice out of bounds")?;
    Ok(format!("{head}{filled}{tail}"))
}

/// Splice the active `[cache.origin]` block `--origin <URL>` asked for into a
/// rendered node config, at the end of its `[cache]` section — immediately
/// before the `[payment]` header.
///
/// Everything between the `[cache]` header and `[payment]` in the template is
/// comments, so the last active header above the insertion point is `[cache]`
/// itself. That placement is why the commented `relay_foreign_namespaces`
/// override line sits *above* the `[cache.origin]` header: uncommented in
/// place it lands in `[cache]`, where the key belongs — below the header it
/// would parse as an unknown `[cache.origin]` field and fail resolution.
fn insert_origin_block(
    template: &str,
    url: &decdn_config_types::OriginUrl,
) -> anyhow::Result<String> {
    let at = template
        .find("\n[payment]\n")
        .map(|i| i + 1)
        .context("rendered config is missing the [payment] section header")?;
    let head = template
        .get(..at)
        .context("rendered config head slice out of bounds")?;
    let tail = template
        .get(at..)
        .context("rendered config tail slice out of bounds")?;
    // `url` came through `parse_origin_url`, whose normalization
    // percent-encodes characters that could break out of a TOML basic string.
    let block = format!(
        "# --- Origin backend (written by `decdn config init --origin`) ---\n\
         # An active origin backend makes this an ORIGIN node: the runtime derives\n\
         # relay_foreign_namespaces = false (serve own namespaces only). Uncomment the\n\
         # next line to override the derived value — set true to also relay foreign\n\
         # content for pay while running an origin backend.\n\
         # relay_foreign_namespaces = false\n\
         [cache.origin]\n\
         kind = \"http\"\n\
         url = \"{url}\"\n\
         \n"
    );
    Ok(format!("{head}{block}{tail}"))
}

/// Trimmed TOML template `decdn config init --client` writes.
///
/// A client is a fetch-only consumer (`decdn fetch`, `decdn bundle pull`): it
/// needs an identity, chain coordinates, and USDC to deposit — none of the
/// cache/origin/serving sections a `decdn-node` daemon reads. The
/// `client_config_template_is_trimmed_and_parses` guard keeps this template
/// parsing against the live `FileConfig` schema with only the `[identity]`
/// and `[blockchain]` sections present.
const CLIENT_CONFIG: &str = r#"# deCDN client configuration — fetch-only consumer.
# A client fetches and pays (`decdn fetch`, `decdn bundle pull`); it needs an
# identity, chain coordinates, and USDC to deposit. The cache/origin/serving
# sections a `decdn-node` daemon reads do not apply and are omitted.
# CLI flags override values in this file.

[identity]
# data_dir = "~/.decdn"              # holds the eth keystore and the buyer-channel store

[blockchain]
# rpc_url = ""                       # REQUIRED: JSON-RPC URL for the payment chain
# chain_id = 421614                  # EIP-712 chain id; default Arbitrum Sepolia
# eth_keystore = "~/.decdn/keystore.json"   # defaults to <data_dir>/keystore.json; create with `decdn key-gen`
# payment_pool_address = ""          # REQUIRED: 0x-prefixed hex; pool the client opens payment channels through
# usdc_address = ""                  # USDC token deposits spend; also required for `decdn setup` swaps
# buyer_working_deposit_micro_usdc = 10000000  # deposit when OPENING a pool and refill target; must be > 0; default 10 USDC
# buyer_max_approve = false          # exact deposit-sized USDC approvals (client default); true opts into an unlimited standing approval
"#;

/// Default TOML config file content, written by `decdn config init`.
///
/// This is the **canonical, fully-commented node config template** — the one
/// place that enumerates every section and knob. There is no hand-maintained
/// sample config beside it: `config init` (role flags + `--chain` presets) is
/// the single source of a valid config. The one other hand-maintained copy
/// (the e2e `render_config` template) is not derived from it, so CI guards
/// against drift instead:
///
/// - `default_config_template_parses_with_every_section` — this template
///   parses under `deny_unknown_fields` (no stale/typo'd section headers) and
///   carries a header for *every* top-level `FileConfig` section, so adding a
///   schema section without surfacing it here fails CI.
/// - `render_config_emits_parseable_toml` (in `decdn_e2e`) — the e2e template
///   parses as valid TOML and spot-checks the daemon-critical keys.
///
/// When adding a config knob, update this template first — the
/// `default_config_template_covers_every_wired_field` guard below enforces
/// field-level coverage for the daemon-config sections.
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
# origin_directory_positive_ttl_sec = 300  # lazy origin directory: how long a resolved, non-empty getOrigins() set is served before a re-read; only consulted when origin_assignment_address is set; 0 disables caching; default 300s
# origin_directory_negative_ttl_sec = 30   # lazy origin directory: how long "namespace has no origins" is cached — the DoS bound against permissionless/free namespace creation; only consulted when origin_assignment_address is set; 0 disables caching; default 30s
# origin_directory_cache_capacity = 4096   # lazy origin directory: max distinct namespaces held (LRU eviction); only consulted when origin_assignment_address is set; default 4096
# publisher_registry_address = ""    # OPTIONAL: 0x-prefixed hex; `decdn publish namespace create` target (#1029).
# slash_judge_address = ""           # REQUIRED: 0x-prefixed hex (EIP-712 verifyingContract, ADR 014)
# content_blacklist_address = ""     # REQUIRED: 0x-prefixed hex; deployed ContentBlacklist (ADR 011/031). Absent => startup fails before any ALPN accepts; the zero address is rejected (it is a fail-open compliance trap).
# content_blacklist_poll_interval_sec = 600  # blacklist watcher periodic replay + re-scope cadence (ADR 011); must be > 0; default 600s
# chain_id = 421614                  # EIP-712 chain id; default Arbitrum Sepolia
# rpc_watchdog_interval_sec = 30     # 0 disables the connectivity watchdog
# event_poll_interval_ms = 7000      # eth_getLogs tick cadence for chain watchers + pending-tx receipt polling (#1011/#1106); default 7000ms, min 250ms (lower for a local anvil)
# rate_bounds_poll_interval_sec = 3600 # authoritative getRateBounds() re-read cadence, safety net beside the RateBoundsUpdated subscription (#1172); default 3600s, must be > 0
# fee_shares_poll_interval_sec = 3600  # authoritative getShares() re-read cadence, safety net beside the SharesUpdated subscription (ADR 041); default 3600s, must be > 0
# redeem_threshold_micro_usdc = 1000000          # seller redeems accrued vouchers on-chain at this µUSDC balance (#327); default 1 USDC
# redeem_max_vouchers_per_tx = 300  # max vouchers per redeemMany tx; the redeemer chunks a sweep to stay under the block gas limit (default 300)
# redeem_interval_secs = 300                      # redeemer self-tick sweep cadence, the backstop beside the per-voucher hints (#327/#751); default 300s, must be > 0
# buyer_working_deposit_micro_usdc = 10000000    # deposit when OPENING a pool and refill target on reuse or mid-transfer shortfall; must be > 0; default 10 USDC
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
# origin_probe_ttl_sec = 15                # TTL for a memoised positive live-origin probe answer — a hash absent from the fs/pins index falls back to a HEAD/HeadObject against the http/s3 origin, a present answer cached this long (#1130 pt3)
# origin_probe_negative_ttl_sec = 2        # TTL for a memoised negative (absent) live-origin probe answer; short on purpose so a stale absent cannot hide newly-available own content for long (#1130 pt3)
# origin_probe_timeout_ms = 2000           # per-probe ceiling on the live-origin HEAD/HeadObject; on timeout the probe answers has_blob:false and the miss is memoised absent for one negative TTL (#1130 pt3)
# origin_probe_memo_capacity = 4096        # max distinct hashes held in the live-origin probe memo; bounds memo memory under a random-hash probe flood (#1130 pt3)
# eviction_high_water_pct = 90             # LRU driver evicts above this % of cache_size_mb (#1173); bounds [60,95]
# eviction_target_pct = 80                 # LRU driver evicts down to this % (#1173); bounds [40,90], must be <= high_water-5
# eviction_per_sweep_budget = 16           # max LRU victims per tick before yielding (#1173); bounds [1,256]
# eviction_tick_secs = 1                   # LRU driver wakeup cadence in seconds (#1173); bounds [1,60]
# max_probe_holds = 256                    # probe eviction-hold budget (ADR 005 §Hold budget); 0 disables has_blob:true
# stake_lane_reserved_holds = 0            # hold slots reserved for node-to-node probes (#757, ADR 003 §Admission); 0 = off
# node_to_node_pull_through_enabled = false # paid cache-miss pull from upstream nodes (#831, ADR 001/022); OFF by default
# relay_foreign_namespaces = true          # relay content this node's own backend does not hold (#1759); default is role-derived — origin-only (false) when a [cache.origin]/[[cache.origins]] backend is configured, relay (true) when none is; set explicitly to override
# node_pull_probe_fanout = 5               # providers probed before ranking on a node-to-node pull (#831)
# node_pull_timeout_sec = 20               # per-upstream STREAM-OPEN timeout (connect/handshake/response) on a node-to-node miss; NOT the channel open, which has its own 5s budget. The overall pull-through deadline is derived from this, the channel-open budget, and the stall timeout, so every ranked upstream can be tried before falling back (#831, #859)
# node_pull_stall_timeout_sec = 20         # per-upstream INACTIVITY timeout while streaming (#1134); the clock resets on every byte, so it trips only on a silent upstream — not on a large blob or a slow link. Budgeted per candidate, so raising it raises the worst-case client wait ~3x (167.5s at defaults)
# eviction_policy = "lru"                  # ADR 040: "lru" or "tinylfu"; restart-required
# admission_policy = "always"              # ADR 040: "always" or "tinylfu"; restart-required
# [cache.tinylfu]                          # W-TinyLFU tuning (ADR 040); consulted only when eviction_policy or admission_policy above is "tinylfu"
# sketch_bytes = 262144                    # count-min sketch size, shared by admission and eviction
# promotion_threshold = 2                  # prior sightings before a probation member promotes to main
# probation_target_pct = 10                # % of cache_size_mb the probation segment is capped to; bounds [1,50]
# aging_halflife_sec = 600                 # reserved: not yet consulted by the shipped sketch (fixed sample-count reset)
# [cache.serve_economics]                  # refuse-to-serve economics (ADR 041)
# policy = "margin"                        # "off" or "margin"
# discount = 0.5                           # (0.0, 1.0]; discount applied to the sell price for the margin gate
# n_max = 64                               # max concurrent speculative-warming sources; >= 1
# warming_budget = 5000000                 # per-source warming allowance, in payment base units; > 0
# warming_refill = 58                      # per-source allowance refill rate, base units/sec; 0 disables time-based refill

[payment]
# rate_per_mb = 10
# delivery_floor = 0                       # PRE-CHAIN SEED ONLY (#1172): overwritten from on-chain getRateBounds() before serving; governance owns the live floor
# credit_max = 67108864                    # downstream credit-window ceiling in bytes (ADR 003 §Credit window); the window ramps toward this cap as the stream pays; default 64 MiB; floored at one chunk (1 MiB)
# credit_ramp_divisor = 2                  # ramp divisor (ADR 003 §Credit window); window is paid/credit_ramp_divisor, capped at credit_max; 0 opens the full ceiling immediately
# frame_target_bytes = 1048576              # serve-path wire-frame target in bytes (ADR 005 §cdn/client/v1); node-local, never negotiated; clamped down to the credit window's remaining room; must be in 1..=1048576 (one payment chunk); default 1 MiB
# voucher_commit_interval_ms = 5000        # background flush period (ms) for durable voucher persistence (ADR 003); must be > 0; default 5000 (5s)

[observability]
# log_level = "info"
# log_format = "pretty"
# metrics_port = 9090
# metrics_bind = "127.0.0.1"               # IP the metrics HTTP server binds; default loopback only
# admin_port = 9191                        # loopback-only; 0 disables (ADR 025)
# otlp_endpoint = "http://localhost:4317"  # requires --features otlp

[security]
# max_concurrent_handlers = 256             # global cap on in-flight QUIC handler tasks; 0 disables the cap
# per_source_rate_per_sec = 100.0           # per-source rate-limit refill (cells/sec); 0.0 disables the layer
# per_source_burst = 200                    # per-source burst capacity; required > 0 when the rate is > 0
# max_tracked_sources = 4096                # cap on tracked sources in the keyed limiter; 0 = unbounded

[load_shed]
# policy = "resource-pressure"              # "resource-pressure" (default) or "always-admit"
# egress_budget_mbps = 0                    # serving egress budget in Mbps; 0 disables the egress ceiling
# max_concurrent_serves_high = 256          # concurrency high-water mark; start shedding misses at/above
# max_concurrent_serves_low = 192           # concurrency low-water mark; resume at/below
# per_client_serve_cap = 32                 # per-client concurrent-serve cap under pressure; 0 disables

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
# governance. Entries take effect on `decdn node reload` (no restart) and bind
# only this node. This is the fastest removal path the protocol offers and the
# one sized to a sub-day statutory deadline (e.g. the EU TCO one-hour clock),
# because it is entirely within the order recipient's control. Refused
# requests are signed as HashBlacklisted / OriginBlacklisted, which do not
# reveal whether the entry is local or on-chain.
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
            security,
            load_shed,
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
            ("security", security.is_some()),
            ("load_shed", load_shed.is_some()),
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
            origin_directory_positive_ttl_sec,
            origin_directory_negative_ttl_sec,
            origin_directory_cache_capacity,
            publisher_registry_address,
            slash_judge_address,
            slash_appeal_address,
            content_blacklist_address,
            content_blacklist_poll_interval_sec,
            chain_id,
            rpc_watchdog_interval_sec,
            event_poll_interval_ms,
            rate_bounds_poll_interval_sec,
            fee_shares_poll_interval_sec,
            redeem_threshold_micro_usdc,
            redeem_max_vouchers_per_tx,
            redeem_interval_secs,
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
                "origin_directory_positive_ttl_sec =",
                origin_directory_positive_ttl_sec.is_none(),
            ),
            (
                "origin_directory_negative_ttl_sec =",
                origin_directory_negative_ttl_sec.is_none(),
            ),
            (
                "origin_directory_cache_capacity =",
                origin_directory_cache_capacity.is_none(),
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
                "fee_shares_poll_interval_sec =",
                fee_shares_poll_interval_sec.is_none(),
            ),
            (
                "redeem_threshold_micro_usdc =",
                redeem_threshold_micro_usdc.is_none(),
            ),
            (
                "redeem_max_vouchers_per_tx =",
                redeem_max_vouchers_per_tx.is_none(),
            ),
            ("redeem_interval_secs =", redeem_interval_secs.is_none()),
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
            origin_probe_negative_ttl_sec,
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
            node_pull_stall_timeout_sec,
            eviction_policy,
            admission_policy,
            tinylfu,
            serve_economics,
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
                "origin_probe_negative_ttl_sec =",
                origin_probe_negative_ttl_sec.is_none(),
            ),
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
            (
                "relay_foreign_namespaces =",
                relay_foreign_namespaces.is_none(),
            ),
            ("node_pull_probe_fanout =", node_pull_probe_fanout.is_none()),
            ("node_pull_timeout_sec =", node_pull_timeout_sec.is_none()),
            (
                "node_pull_stall_timeout_sec =",
                node_pull_stall_timeout_sec.is_none(),
            ),
            ("eviction_policy =", eviction_policy.is_none()),
            ("admission_policy =", admission_policy.is_none()),
            ("[cache.tinylfu]", tinylfu.is_none()),
            ("[cache.serve_economics]", serve_economics.is_none()),
        ];

        let config::types::PaymentConfig {
            rate_per_mb,
            delivery_floor,
            credit_max,
            credit_ramp_divisor,
            frame_target_bytes,
            voucher_commit_interval_ms,
        } = &config::types::PaymentConfig::default();
        let payment = [
            ("rate_per_mb =", rate_per_mb.is_none()),
            ("delivery_floor =", delivery_floor.is_none()),
            ("credit_max =", credit_max.is_none()),
            ("frame_target_bytes =", frame_target_bytes.is_none()),
            ("credit_ramp_divisor =", credit_ramp_divisor.is_none()),
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

    /// `config init` with no `--chain` (the sole-chain default) must emit a
    /// config that parses, carries the seeded chain id, and has every baked
    /// contract address active — i.e. runs out of the box modulo a keystore.
    #[test]
    fn render_config_for_default_chain_is_runnable() {
        let chain = known_chains::resolve(None)
            .expect("resolve")
            .expect("sole chain");
        let rendered = render_config(Some(chain), &Role::Relay).expect("render");

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
        let rendered = render_config(None, &Role::Relay).expect("render");
        assert_eq!(rendered, DEFAULT_CONFIG);
    }

    /// `--origin <URL>` must activate a `[cache.origin]` HTTP backend carrying
    /// the normalized URL, state the derived `relay_foreign_namespaces = false`
    /// as an explicit commented line, and leave every other section intact.
    #[test]
    fn render_origin_config_activates_backend_and_notes_role() {
        let chain = known_chains::resolve(None)
            .expect("resolve")
            .expect("sole chain");
        // `parse_origin_url` appends the trailing slash the runtime needs; the
        // rendered file must carry the normalized form.
        let url = decdn_config_types::parse_origin_url("https://origin.example/v1")
            .expect("valid origin URL");
        let rendered = render_config(Some(chain), &Role::Origin(url)).expect("render");

        let cfg: config::FileConfig =
            toml::from_str(&rendered).expect("rendered --origin config must parse as FileConfig");
        let cache = cfg.cache.expect("[cache] present");
        assert!(
            cache.origin.is_some(),
            "--origin must write an active [cache.origin] backend"
        );
        assert!(
            rendered.contains("url = \"https://origin.example/v1/\""),
            "origin URL must be the normalized (trailing-slash) form"
        );
        // The derived role is stated explicitly (commented) so nothing flips
        // silently (#1772); the line sits in the [cache] scalar scope.
        assert!(
            rendered.contains("# relay_foreign_namespaces = false"),
            "derived origin-only default must be written as an explicit commented line"
        );
        // The splice must not displace the sections around it.
        for header in ["[identity]", "[blockchain]", "[payment]", "[content]"] {
            assert!(rendered.contains(header), "rendered config lost {header}");
        }
    }

    /// `--origin` composes with the blank template too (`--chain none`).
    #[test]
    fn render_origin_config_without_chain_parses() {
        let url =
            decdn_config_types::parse_origin_url("https://origin.example/").expect("valid URL");
        let rendered = render_config(None, &Role::Origin(url)).expect("render");
        let cfg: config::FileConfig = toml::from_str(&rendered).expect("must parse as FileConfig");
        assert!(cfg.cache.expect("[cache] present").origin.is_some());
    }

    /// The client template is the trimmed fetch-only shape: it parses against
    /// the live `FileConfig` schema and carries ONLY the `[identity]` and
    /// `[blockchain]` sections — none of the daemon's cache/serving sections.
    #[test]
    fn client_config_template_is_trimmed_and_parses() {
        let parsed: config::FileConfig =
            toml::from_str(CLIENT_CONFIG).expect("CLIENT_CONFIG template must parse as FileConfig");
        assert!(parsed.identity.is_some(), "client template lost [identity]");
        assert!(
            parsed.blockchain.is_some(),
            "client template lost [blockchain]"
        );
        // Compare as raw TOML tables (headers literally present in the text),
        // not `FileConfig` fields — those are all-`Option` and always "absent"
        // here, which would pass vacuously if a daemon section were added.
        // Set comparison, because key iteration order is a `toml` feature
        // choice (sorted today, insertion-ordered under `preserve_order`).
        let value: toml::Value = toml::from_str(CLIENT_CONFIG).expect("client template is TOML");
        let sections: std::collections::BTreeSet<String> = value
            .as_table()
            .expect("client template is a TOML table")
            .keys()
            .cloned()
            .collect();
        let expected: std::collections::BTreeSet<String> =
            ["blockchain".to_string(), "identity".to_string()].into();
        assert_eq!(
            sections, expected,
            "client template must carry exactly [identity] + [blockchain]"
        );
    }

    /// `--client` with the default chain bakes the client-consumed coordinates
    /// (pool + USDC) AND the operator addresses the daemon resolver requires,
    /// so the emitted file still passes `decdn config validate`.
    #[test]
    fn render_client_config_for_default_chain_bakes_addresses() {
        let chain = known_chains::resolve(None)
            .expect("resolve")
            .expect("sole chain");
        let rendered = render_config(Some(chain), &Role::Client).expect("render");

        let cfg: config::FileConfig =
            toml::from_str(&rendered).expect("rendered --client config must parse as FileConfig");
        let bc = cfg.blockchain.expect("[blockchain] present");
        let a = chain.addresses().expect("addresses");

        assert_eq!(bc.chain_id, Some(chain.chain_id));
        assert_eq!(bc.rpc_url.as_deref(), Some(chain.public_rpc));
        assert_eq!(bc.payment_pool_address, Some(a.payment_pool));
        assert_eq!(bc.usdc_address, Some(a.usdc));
        // Required by the daemon resolver — kept active so `config validate`
        // accepts the client file.
        assert_eq!(bc.capacity_bond_address, Some(a.capacity_bond));
        assert_eq!(bc.slash_judge_address, Some(a.slash_judge));
        assert_eq!(bc.content_blacklist_address, Some(a.content_blacklist));

        // Still the trimmed shape: no daemon sections appear.
        assert!(cfg.cache.is_none(), "client config must not gain [cache]");
        assert!(
            cfg.payment.is_none(),
            "client config must not gain [payment]"
        );
    }

    /// Role resolution: `--client` wins its branch, a bad `--origin` URL is
    /// rejected up front (same validation the daemon resolver applies), and no
    /// flags mean relay.
    #[test]
    fn resolve_role_maps_flags() {
        let args = |origin: Option<&str>, client: bool| cli::ConfigInitArgs {
            output: None,
            force: false,
            chain: None,
            origin: origin.map(str::to_string),
            client,
        };
        assert!(matches!(
            resolve_role(&args(None, false)).expect("relay"),
            Role::Relay
        ));
        assert!(matches!(
            resolve_role(&args(None, true)).expect("client"),
            Role::Client
        ));
        assert!(matches!(
            resolve_role(&args(Some("https://origin.example/"), false)).expect("origin"),
            Role::Origin(_)
        ));
        // ftp:// fails the http/https scheme check in `parse_origin_url`.
        assert!(resolve_role(&args(Some("ftp://origin.example/"), false)).is_err());
    }
}
