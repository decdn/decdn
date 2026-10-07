use super::*;

/// A malformed `render_config` template only surfaces behind the `anvil-e2e`
/// feature (Foundry + a built binary). Parse the rendered TOML here so a
/// template regression (bad `format!`, dropped/mis-nested key) is caught in
/// a normal `cargo nextest run -p decdn-e2e`, no chain required.
#[test]
fn render_config_emits_parseable_toml() {
    let addrs = ContractAddrs {
        capacity_bond: Address::from([0x11; 20]),
        payment_pool: Address::from([0x22; 20]),
        fee_router: Address::from([0x33; 20]),
        token: Address::from([0x44; 20]),
        slash_judge: Address::from([0x55; 20]),
        slash_appeal: Address::from([0x66; 20]),
        governor: Address::from([0x77; 20]),
        timelock: Address::from([0x88; 20]),
        publisher_registry: Address::from([0x99; 20]),
        origin_assignment: Address::from([0xAA; 20]),
        manual_vetting_policy: Address::from([0xAB; 20]),
        content_blacklist: Address::from([0xBB; 20]),
    };
    let discovery_peers = [(iroh::SecretKey::from_bytes(&[0xCC; 32]).public(), 4434)];
    let rendered = render_config(&RenderConfig {
        data_dir: PathBuf::from("/var/lib/decdn"),
        region: "US",
        bind_port: 4433,
        admin_port: 9944,
        metrics_port: 9100,
        rpc_url: "http://127.0.0.1:8545",
        keystore: std::path::Path::new("/var/lib/decdn/keystore.json"),
        cache_dir: std::path::Path::new("/var/lib/decdn/cache"),
        origin_dir: std::path::Path::new("/var/lib/decdn/origin"),
        chain_id: 31_337,
        addrs,
        node_to_node_pull_through: true,
        discovery_peers: &discovery_peers,
    });

    // The core check: the whole template parses as TOML.
    let doc: toml::Value = toml::from_str(&rendered).expect("render_config must emit valid TOML");

    // Guard the tables/keys the daemon depends on against silent drift.
    assert_eq!(doc["blockchain"]["chain_id"].as_integer(), Some(31_337));
    assert_eq!(
        doc["blockchain"]["capacity_bond_address"].as_str(),
        Some(addrs.capacity_bond.to_string().as_str())
    );
    assert_eq!(
        doc["blockchain"]["redeem_threshold_micro_usdc"].as_integer(),
        Some(10)
    );
    assert_eq!(doc["cache"]["origin"]["kind"].as_str(), Some("fs"));
    assert!(doc["cache"]["origin"].get("path").is_some());
    assert_eq!(
        doc["cache"]["node_to_node_pull_through_enabled"].as_bool(),
        Some(true)
    );
    assert_eq!(
        doc["network"]["discovery"]["peers"]
            .as_table()
            .map(toml::Table::len),
        Some(1)
    );
    assert_eq!(doc["payment"]["rate_per_mb"].as_integer(), Some(10));
    assert_eq!(doc["observability"]["admin_port"].as_integer(), Some(9944));
    assert_eq!(
        doc["observability"]["metrics_port"].as_integer(),
        Some(9100)
    );
}

/// Build a representative rendered node config for the TOML-round-trip tests.
fn sample_rendered_config() -> String {
    let addrs = ContractAddrs {
        capacity_bond: Address::from([0x11; 20]),
        payment_pool: Address::from([0x22; 20]),
        fee_router: Address::from([0x33; 20]),
        token: Address::from([0x44; 20]),
        slash_judge: Address::from([0x55; 20]),
        slash_appeal: Address::from([0x66; 20]),
        governor: Address::from([0x77; 20]),
        timelock: Address::from([0x88; 20]),
        publisher_registry: Address::from([0x99; 20]),
        origin_assignment: Address::from([0xAA; 20]),
        manual_vetting_policy: Address::from([0xAB; 20]),
        content_blacklist: Address::from([0xBB; 20]),
    };
    render_config(&RenderConfig {
        data_dir: PathBuf::from("/var/lib/decdn"),
        region: "US",
        bind_port: 4433,
        admin_port: 9944,
        metrics_port: 9100,
        rpc_url: "http://127.0.0.1:8545",
        keystore: std::path::Path::new("/var/lib/decdn/keystore.json"),
        cache_dir: std::path::Path::new("/var/lib/decdn/cache"),
        origin_dir: std::path::Path::new("/var/lib/decdn/origin"),
        chain_id: 31_337,
        addrs,
        node_to_node_pull_through: true,
        discovery_peers: &[],
    })
}

/// `set_rate_per_mb`'s parse → mutate → serialize step (`rewrite_rate_per_mb`)
/// must round-trip: the new rate lands and every other section survives. The
/// `[cache]` block is the one at risk — it holds a scalar
/// (`node_to_node_pull_through_enabled`) after a sub-table (`[cache.origin]`),
/// the `ValueAfterTable` shape a `toml` serializer bug would mangle (#1378).
#[test]
fn set_rate_per_mb_round_trips_through_toml() {
    let rewritten = rewrite_rate_per_mb(&sample_rendered_config(), 4242)
        .expect("rewrite_rate_per_mb must succeed");
    let doc: toml::Value = toml::from_str(&rewritten).expect("rewritten config must be valid TOML");

    assert_eq!(doc["payment"]["rate_per_mb"].as_integer(), Some(4242));
    // Everything around the mutation is intact — notably the `[cache]`
    // scalar-after-subtable that trips the serializer hazard.
    assert_eq!(doc["cache"]["origin"]["kind"].as_str(), Some("fs"));
    assert_eq!(
        doc["cache"]["node_to_node_pull_through_enabled"].as_bool(),
        Some(true)
    );
    assert_eq!(doc["blockchain"]["chain_id"].as_integer(), Some(31_337));
}

/// `set_buyer_working_deposit`'s parse → mutate → serialize step
/// (`rewrite_buyer_working_deposit`) must round-trip: the new deposit lands
/// under `[blockchain]` and every other section — notably the `[cache]`
/// scalar-after-subtable that trips the serializer hazard — survives.
#[test]
fn set_buyer_working_deposit_round_trips_through_toml() {
    let rewritten = rewrite_buyer_working_deposit(&sample_rendered_config(), 4_000_000)
        .expect("rewrite_buyer_working_deposit must succeed");
    let doc: toml::Value = toml::from_str(&rewritten).expect("rewritten config must be valid TOML");

    assert_eq!(
        doc["blockchain"]["buyer_working_deposit_micro_usdc"].as_integer(),
        Some(4_000_000)
    );
    // Everything around the mutation is intact.
    assert_eq!(doc["blockchain"]["chain_id"].as_integer(), Some(31_337));
    assert_eq!(doc["cache"]["origin"]["kind"].as_str(), Some("fs"));
    assert_eq!(
        doc["cache"]["node_to_node_pull_through_enabled"].as_bool(),
        Some(true)
    );
    assert_eq!(doc["payment"]["rate_per_mb"].as_integer(), Some(10));
}

/// `set_pool_floor_signer`'s parse → mutate → serialize step
/// (`rewrite_pool_floor_signer`) must round-trip: the `[blockchain]` knob
/// lands and every other section — notably the `[cache]`
/// scalar-after-subtable that trips the serializer hazard — survives.
#[test]
fn set_pool_floor_signer_round_trips_through_toml() {
    let rewritten = rewrite_pool_floor_signer(&sample_rendered_config(), 8)
        .expect("rewrite_pool_floor_signer must succeed");
    let doc: toml::Value = toml::from_str(&rewritten).expect("rewritten config must be valid TOML");

    assert_eq!(
        doc["blockchain"]["pool_floor_signer_live_windows"].as_integer(),
        Some(8)
    );
    // Everything around the mutation is intact.
    assert_eq!(doc["blockchain"]["chain_id"].as_integer(), Some(31_337));
    assert_eq!(doc["cache"]["origin"]["kind"].as_str(), Some("fs"));
    assert_eq!(
        doc["cache"]["node_to_node_pull_through_enabled"].as_bool(),
        Some(true)
    );
}

/// `set_pool_min_remaining_deposit`'s parse → mutate → serialize step
/// (`rewrite_pool_min_remaining_deposit`) must round-trip: the `[blockchain]`
/// knob lands and every other section — notably the `[cache]`
/// scalar-after-subtable that trips the serializer hazard — survives.
#[test]
fn set_pool_min_remaining_deposit_round_trips_through_toml() {
    let rewritten = rewrite_pool_min_remaining_deposit(&sample_rendered_config(), 500)
        .expect("rewrite_pool_min_remaining_deposit must succeed");
    let doc: toml::Value = toml::from_str(&rewritten).expect("rewritten config must be valid TOML");

    assert_eq!(
        doc["blockchain"]["pool_min_remaining_deposit_micro_usdc"].as_integer(),
        Some(500)
    );
    // Everything around the mutation is intact.
    assert_eq!(doc["blockchain"]["chain_id"].as_integer(), Some(31_337));
    assert_eq!(doc["cache"]["origin"]["kind"].as_str(), Some("fs"));
    assert_eq!(
        doc["cache"]["node_to_node_pull_through_enabled"].as_bool(),
        Some(true)
    );
    assert_eq!(doc["payment"]["rate_per_mb"].as_integer(), Some(10));
}

/// `repoint_payment_pool`'s parse → mutate → serialize step
/// (`rewrite_payment_pool`) must round-trip: the new `PaymentPool` lands, the
/// discovery peers are replaced rather than merged, and every other section —
/// notably the `[cache]` scalar-after-subtable that trips the serializer
/// hazard — survives.
#[test]
fn repoint_payment_pool_round_trips_through_toml() {
    let old_peer = iroh::SecretKey::from_bytes(&[0xCC; 32]).public();
    let new_peer = iroh::SecretKey::from_bytes(&[0xDD; 32]).public();
    let redeployed = Address::from([0x23; 20]);
    // The sample renders no peers, so the first rewrite also creates
    // `[network.discovery]`; the second replaces what the first wrote.
    let with_old = rewrite_payment_pool(
        &sample_rendered_config(),
        Address::from([0x22; 20]),
        &[(old_peer, 4434)],
    )
    .expect("first rewrite_payment_pool must succeed");
    let rewritten = rewrite_payment_pool(&with_old, redeployed, &[(new_peer, 4435)])
        .expect("second rewrite_payment_pool must succeed");
    let doc: toml::Value = toml::from_str(&rewritten).expect("rewritten config must be valid TOML");

    assert_eq!(
        doc["blockchain"]["payment_pool_address"].as_str(),
        Some(redeployed.to_string().as_str())
    );
    let peers = doc["network"]["discovery"]["peers"]
        .as_table()
        .expect("discovery peers must be a table");
    assert_eq!(peers.len(), 1, "peers are replaced, not merged: {peers:?}");
    assert_eq!(
        peers[&new_peer.to_string()]["addrs"][0].as_str(),
        Some("127.0.0.1:4435")
    );
    // Everything around the mutation is intact.
    assert_eq!(doc["network"]["bind_port"].as_integer(), Some(4433));
    assert_eq!(doc["blockchain"]["chain_id"].as_integer(), Some(31_337));
    assert_eq!(doc["cache"]["origin"]["kind"].as_str(), Some("fs"));
    assert_eq!(
        doc["cache"]["node_to_node_pull_through_enabled"].as_bool(),
        Some(true)
    );
}

/// `set_payment_pool_address`'s parse → mutate → serialize step
/// (`rewrite_payment_pool_address`) sets only the address: the discovery
/// peers and the `[cache]` table survive the round-trip.
#[test]
fn rewrite_payment_pool_address_keeps_peers_and_cache() {
    let peer = iroh::SecretKey::from_bytes(&[0xCC; 32]).public();
    let with_peer = rewrite_payment_pool(
        &sample_rendered_config(),
        Address::from([0x22; 20]),
        &[(peer, 4434)],
    )
    .expect("rewrite_payment_pool must succeed");
    let codeless = Address::repeat_byte(0xC0);
    let rewritten = rewrite_payment_pool_address(&with_peer, codeless)
        .expect("rewrite_payment_pool_address must succeed");
    let doc: toml::Value = toml::from_str(&rewritten).expect("rewritten config must be valid TOML");

    assert_eq!(
        doc["blockchain"]["payment_pool_address"].as_str(),
        Some(codeless.to_string().as_str())
    );
    let peers = doc["network"]["discovery"]["peers"]
        .as_table()
        .expect("discovery peers must be a table");
    assert_eq!(peers.len(), 1, "peers are kept as they were: {peers:?}");
    assert_eq!(
        peers[&peer.to_string()]["addrs"][0].as_str(),
        Some("127.0.0.1:4434")
    );
    assert_eq!(doc["blockchain"]["chain_id"].as_integer(), Some(31_337));
    assert_eq!(doc["cache"]["origin"]["kind"].as_str(), Some("fs"));
    assert_eq!(
        doc["cache"]["node_to_node_pull_through_enabled"].as_bool(),
        Some(true)
    );
}

/// The daemon's pretty log format styles a field as italic name, dimmed
/// `=`, plain value. Stripping must leave the bare `name=value` a needle
/// matches, and keep text outside the escapes byte for byte.
#[test]
fn strip_ansi_removes_sgr_styling() {
    assert_eq!(
        strip_ansi(b"\x1b[33m WARN\x1b[0m \x1b[3mpool_id\x1b[0m\x1b[2m=\x1b[0m0xab\n"),
        " WARN pool_id=0xab\n"
    );
    assert_eq!(strip_ansi(b"plain = text"), "plain = text");
}

/// Only a CSI sequence is consumed past its ESC: a lone ESC goes alone,
/// and a CSI cut off at a newline keeps the newline.
#[test]
fn strip_ansi_removes_a_lone_esc_alone_and_stops_a_cut_csi_at_newline() {
    assert_eq!(strip_ansi(b"a\x1bcb"), "acb");
    assert_eq!(strip_ansi(b"a\x1b"), "a");
    assert_eq!(strip_ansi(b"a\x1b[3\nb"), "a\nb");
}

/// A `Write` whose bytes the test reads back after the tee thread ends.
#[derive(Clone, Default)]
struct SharedBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The tee echoes the daemon's bytes unchanged, logs them without styling,
/// and ends a line the daemon never finished, so the next spawn's first
/// line starts on its own.
#[test]
fn tee_echoes_verbatim_and_logs_whole_plain_lines() {
    let raw: &[u8] = b"\x1b[3mpool_id\x1b[0m=1\ncut off";
    let log = DaemonLog::default();
    let echo = SharedBuf::default();
    log.tee(std::io::Cursor::new(raw.to_vec()), echo.clone(), "test")
        .expect("spawn tee")
        .join()
        .expect("tee thread");
    log.append("next spawn\n");

    assert_eq!(
        echo.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_slice(),
        raw
    );
    assert_eq!(log.tail(3), "pool_id=1\ncut off\nnext spawn");
    assert_eq!(log.tail(1), "next spawn");
}

/// A match needs every needle on the same line, so two fields logged by
/// different events never combine into a false hit.
#[test]
fn line_with_all_needs_every_needle_on_one_line() {
    let log = "a pool_id=1\nforeign_payment_pool=2 other\nboth pool_id=1 foreign_payment_pool=2\n";
    assert_eq!(
        line_with_all(log, &["pool_id=1", "foreign_payment_pool=2"]),
        Some("both pool_id=1 foreign_payment_pool=2")
    );
    assert_eq!(line_with_all(log, &["pool_id=1", "absent"]), None);
}
