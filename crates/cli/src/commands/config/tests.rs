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
    let parsed: config::FileConfig =
        toml::from_str(DEFAULT_CONFIG).expect("DEFAULT_CONFIG template must parse as FileConfig");
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
        client,
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
        ("client", client.is_some()),
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
        usdc_address,
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
        (
            "chain_staleness_grace_sec =",
            chain_staleness_grace_sec.is_none(),
        ),
        ("chain_id =", chain_id.is_none()),
        (
            "rpc_watchdog_interval_sec =",
            rpc_watchdog_interval_sec.is_none(),
        ),
        ("event_poll_interval_ms =", event_poll_interval_ms.is_none()),
        (
            "get_logs_max_block_span =",
            get_logs_max_block_span.is_none(),
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
        (
            "pool_floor_signer_live_windows =",
            pool_floor_signer_live_windows.is_none(),
        ),
        ("usdc_address =", usdc_address.is_none()),
    ];

    let config::types::CacheConfig {
        cache_dir,
        cache_size_mb,
        disk_headroom_mb,
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
        tinylfu,
        serve_economics,
    } = &config::types::CacheConfig::default();
    let cache = [
        ("cache_dir =", cache_dir.is_none()),
        ("cache_size_mb =", cache_size_mb.is_none()),
        ("disk_headroom_mb =", disk_headroom_mb.is_none()),
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
            "origin_probe_fault_ttl_sec =",
            origin_probe_fault_ttl_sec.is_none(),
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
            "node_pull_stall_window_sec =",
            node_pull_stall_window_sec.is_none(),
        ),
        (
            "node_pull_min_throughput_bps =",
            node_pull_min_throughput_bps.is_none(),
        ),
        ("eviction_policy =", eviction_policy.is_none()),
        ("admission_policy =", admission_policy.is_none()),
        ("[cache.tinylfu]", tinylfu.is_none()),
        ("[cache.serve_economics]", serve_economics.is_none()),
    ];

    let config::types::PaymentConfig {
        rate_per_mb,
        credit_max,
        credit_ramp_divisor,
        frame_target_bytes,
        voucher_commit_interval_ms,
    } = &config::types::PaymentConfig::default();
    let payment = [
        ("rate_per_mb =", rate_per_mb.is_none()),
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
    let spec = parse_origin_spec("https://origin.example/v1").expect("valid origin URL");
    let rendered = render_config(Some(chain), &Role::Origin(spec)).expect("render");

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
    let spec = parse_origin_spec("https://origin.example/").expect("valid URL");
    let rendered = render_config(None, &Role::Origin(spec)).expect("render");
    let cfg: config::FileConfig = toml::from_str(&rendered).expect("must parse as FileConfig");
    assert!(cfg.cache.expect("[cache] present").origin.is_some());
}

/// `--origin file:///path` writes an fs origin carrying the decoded local
/// path, with a quote-bearing path escaped rather than breaking the TOML.
#[test]
fn render_fs_origin_config_carries_the_path() {
    // %20 decodes to a space; the quote exercises `toml_basic_string`.
    let spec = parse_origin_spec("file:///var/lib/decdn%20blobs/ori%22gin").expect("valid URL");
    assert!(
        matches!(&spec, OriginSpec::Fs(p) if p.to_str() == Some("/var/lib/decdn blobs/ori\"gin"))
    );
    let rendered = render_config(None, &Role::Origin(spec)).expect("render");
    let cfg: config::FileConfig = toml::from_str(&rendered).expect("must parse as FileConfig");
    match cfg.cache.expect("[cache] present").origin {
        Some(config::types::OriginConfig::Fs { path }) => {
            assert_eq!(
                path,
                std::path::PathBuf::from("/var/lib/decdn blobs/ori\"gin")
            );
        }
        other => panic!("expected an fs origin, got {other:?}"),
    }
}

/// Bare `--origin s3` writes an S3 block with a placeholder bucket that
/// still parses and would pass resolution (DNS-safe bucket, non-empty
/// region); `s3://<bucket>` fills the bucket in.
#[test]
fn render_s3_origin_config_places_bucket() {
    for (flag, bucket) in [
        ("s3", "your-origin-bucket"),
        ("s3://decdn-blobs", "decdn-blobs"),
    ] {
        let spec = parse_origin_spec(flag).expect("valid s3 spelling");
        let rendered = render_config(None, &Role::Origin(spec)).expect("render");
        let cfg: config::FileConfig = toml::from_str(&rendered).expect("must parse as FileConfig");
        match cfg.cache.expect("[cache] present").origin {
            Some(config::types::OriginConfig::S3(s3)) => {
                assert_eq!(s3.bucket, bucket, "for --origin {flag}");
                assert_eq!(s3.region, "us-east-1", "for --origin {flag}");
            }
            other => panic!("expected an s3 origin for --origin {flag}, got {other:?}"),
        }
    }
}

/// The `--origin` spellings the parser must refuse, each with the reason
/// the error names: a remote-host file URL, an s3 URL with a key prefix
/// (the commented `prefix` key owns that), a bucket failing the shared
/// DNS-safety guard, and an unknown scheme.
#[test]
fn parse_origin_spec_rejects_bad_spellings() {
    for (flag, expected) in [
        ("file://host.example/var/blobs", "local absolute path"),
        ("s3://decdn-blobs/some/prefix", "no key prefix"),
        ("s3://Bad_Bucket", "invalid --origin s3 bucket"),
        ("ftp://origin.example/", "unsupported --origin scheme"),
    ] {
        let err = format!("{:#}", parse_origin_spec(flag).expect_err(flag));
        assert!(err.contains(expected), "--origin {flag}: {err}");
    }
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
    // ftp:// fails the scheme classification in `parse_origin_spec`.
    assert!(resolve_role(&args(Some("ftp://origin.example/"), false)).is_err());
}
