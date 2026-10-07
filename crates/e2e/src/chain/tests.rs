use super::*;

#[test]
fn forge_body_completion_gates_on_the_success_marker() {
    // A non-zero exit *after* forge prints this marker is a broadcast-phase
    // hiccup (retryable, #883); its absence means a revert before the body
    // ran (fail fast). The classifier keys purely off the marker string, so
    // pin both branches — if forge changes the wording this test catches it.
    assert!(forge_script_body_completed(
        b"...\nScript ran successfully.\n== Logs ==\n"
    ));
    assert!(!forge_script_body_completed(
        b"Error: script failed: revert: Ownable: caller is not the owner"
    ));
    assert!(!forge_script_body_completed(b""));
}

// The shared-deployment cache helpers are pure (no anvil/forge), so pin their
// correctness here rather than only through the gated journeys.
#[allow(clippy::expect_used, clippy::panic)]
fn write_json(path: &Path, json: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    std::fs::write(path, json).expect("write json fixture");
}

/// A minimal deploy manifest with distinct, valid addresses for each field.
fn sample_manifest_json() -> String {
    let addr = |n: u8| format!("0x{n:0>40x}");
    format!(
        r#"{{
              "contracts": {{
                "CapacityBond": "{}", "PaymentPool": "{}", "FeeRouter": "{}",
                "Token": "{}", "SlashJudge": "{}", "SlashAppeal": "{}",
                "DecdnGovernor": "{}", "TimelockController": "{}",
                "PublisherRegistry": "{}", "OriginAssignment": "{}",
                "ManualVettingPolicy": "{}", "ContentBlacklist": "{}"
              }},
              "externalDeps": {{ "usdc": "{}" }}
            }}"#,
        addr(1),
        addr(2),
        addr(3),
        addr(4),
        addr(5),
        addr(6),
        addr(7),
        addr(8),
        addr(9),
        addr(10),
        addr(11),
        addr(12),
        addr(0xff),
    )
}

#[test]
#[allow(clippy::expect_used, clippy::panic)]
fn short_hash_is_stable_distinct_and_hex() {
    let a = short_hash(b"alpha");
    assert_eq!(a, short_hash(b"alpha"), "same input, same hash");
    assert_ne!(a, short_hash(b"beta"), "different input, different hash");
    assert_eq!(a.len(), 16, "8 bytes as hex");
    assert!(
        a.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
}

#[test]
#[allow(clippy::expect_used, clippy::panic)]
fn write_atomic_commits_whole_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.json");
    assert!(!nonempty_file(&path), "absent before write");
    write_atomic(&path, b"0xdeadbeef").expect("atomic write");
    assert!(nonempty_file(&path), "present after write");
    assert_eq!(std::fs::read(&path).expect("read back"), b"0xdeadbeef");
    // No sibling temp file is left behind.
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .expect("read dir")
        .filter_map(Result::ok)
        .filter(|e| e.file_name() != "state.json")
        .collect();
    assert!(
        leftovers.is_empty(),
        "temp file renamed away, found {leftovers:?}"
    );
}

#[test]
#[allow(clippy::expect_used, clippy::panic)]
fn read_manifest_parses_contracts_and_usdc() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("manifest.json");
    write_json(&path, &sample_manifest_json());
    let manifest = read_manifest(&path).expect("parse manifest");
    assert_eq!(manifest.addrs.capacity_bond, Address::with_last_byte(1));
    assert_eq!(
        manifest.addrs.content_blacklist,
        Address::with_last_byte(12)
    );
    assert_eq!(manifest.usdc, Address::with_last_byte(0xff));
    // Every `contracts` entry, name-sorted, including those with no typed
    // field.
    assert_eq!(manifest.contracts.len(), 12);
    assert_eq!(
        manifest.contracts.first(),
        Some(&("CapacityBond".to_string(), Address::with_last_byte(1)))
    );
    assert!(
        manifest.contracts.is_sorted_by(|a, b| a.0 < b.0),
        "manifest contracts must be sorted by name: {:?}",
        manifest.contracts
    );
}

#[test]
#[allow(clippy::expect_used, clippy::panic)]
fn read_manifest_requires_usdc() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("manifest.json");
    // Strip externalDeps: the settlement token must be present.
    write_json(&path, r#"{ "contracts": {} }"#);
    assert!(read_manifest(&path).is_err());
}

#[test]
#[allow(clippy::expect_used, clippy::panic)]
fn load_cached_deployment_needs_both_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state = dir.path().join("state.json");
    let manifest = dir.path().join("manifest.json");

    // Neither present, or only one present, is a miss.
    assert!(
        load_cached_deployment(&state, &manifest)
            .expect("miss")
            .is_none()
    );
    write_atomic(&state, b"0x00").expect("write state");
    assert!(
        load_cached_deployment(&state, &manifest)
            .expect("half-written miss")
            .is_none(),
        "manifest is the commit marker; state alone is not a hit"
    );

    // Both present is a hit that surfaces the parsed addresses.
    write_json(&manifest, &sample_manifest_json());
    let hit = load_cached_deployment(&state, &manifest)
        .expect("hit")
        .expect("some");
    assert_eq!(hit.manifest.usdc, Address::with_last_byte(0xff));
    assert_eq!(hit.state_path, state);
}

#[test]
#[allow(clippy::expect_used, clippy::panic)]
fn artifact_cache_key_invalidates_on_bytecode_change() {
    let dir = tempfile::tempdir().expect("tempdir");
    let contracts = dir.path();
    let usdc = contracts.join("out/MintableUSDC.sol/MintableUSDC.json");
    let deploy = contracts.join("out/DeployProtocol.s.sol/DeployProtocol.json");
    write_json(&usdc, r#"{ "bytecode": { "object": "0x6001" } }"#);
    write_json(&deploy, r#"{ "bytecode": { "object": "0x6002" } }"#);

    let key1 = artifact_cache_key(contracts).expect("key1");
    assert_eq!(
        key1,
        artifact_cache_key(contracts).expect("key1 again"),
        "deterministic"
    );

    // A recompiled deploy script (embeds every `new`d contract) rekeys the cache.
    write_json(&deploy, r#"{ "bytecode": { "object": "0x6003" } }"#);
    assert_ne!(key1, artifact_cache_key(contracts).expect("key2"));
}

#[test]
fn cache_dir_is_per_checkout() {
    assert_ne!(
        cache_dir(Path::new("/a/contracts")),
        cache_dir(Path::new("/b/contracts"))
    );
}

/// Regression test for the deploy-retry death spiral (#785).
///
/// Models the state a SIGKILLed `forge script` leaves behind: a transaction
/// already on the wire but *not yet mined*. Before the drain, `evm_revert`
/// left that straggler in the pool, so the deployer nonce read clean at
/// snapshot time and then advanced once the straggler mined — poisoning the
/// fresh snapshot and making every later attempt fail fast with `nonce too
/// low` / `transaction already imported`.
///
/// anvil runs on a fixed block time so the straggler is *provably* still
/// pending while the lane is reset. That is the deterministic stand-in for
/// the CI condition: a starved anvil that has not drained its pool by the
/// time the stalled forge is SIGKILLed. The block time must exceed the drain
/// budget, or the straggler mines before the revert and the revert alone
/// would clean it up — which is precisely the case that never reproduced.
#[cfg(feature = "anvil-e2e")]
#[tokio::test]
// Test scaffolding legitimately uses expect/panic; the workspace anti-panic
// policy targets runtime code (matches the journey files' crate-level allow).
#[allow(clippy::expect_used, clippy::panic)]
async fn reset_broadcast_lane_survives_a_straggler_from_a_killed_forge() {
    // Must stay comfortably above the drain budget (POOL_DRAIN_POLLS *
    // POOL_DRAIN_SETTLE) so the straggler is still pending through the reset.
    const BLOCK_TIME: u64 = 4;

    let port = crate::free_port().expect("free port");
    let mut anvil = Command::new("anvil")
        .args([
            "--port",
            &port.to_string(),
            "--block-time",
            &BLOCK_TIME.to_string(),
            "--silent",
        ])
        .spawn()
        .expect("spawn anvil (is foundry installed?)");

    let url: reqwest::Url = format!("http://127.0.0.1:{port}")
        .parse()
        .expect("parse anvil rpc url");
    let deployer: PrivateKeySigner = DEPLOYER_KEY.parse().expect("parse deployer key");
    let deployer_addr = deployer.address();
    let provider: DynProvider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(deployer))
        .connect_http(url)
        .erased();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while provider.get_chain_id().await.is_err() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "anvil RPC never came up"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let snapshot = evm_snapshot(&provider).await.expect("snapshot");
    let before = provider
        .get_transaction_count(deployer_addr)
        .await
        .expect("nonce before");

    // Submit without awaiting the receipt: on a fixed block time this leaves
    // the transaction sitting in the pool — exactly what a SIGKILLed forge
    // leaves behind.
    let pending = provider
        .send_transaction(
            TransactionRequest::default()
                .with_to(Address::ZERO)
                .with_value(U256::from(1)),
        )
        .await
        .expect("submit straggler");
    drop(pending);
    assert!(
        !pool_is_empty(&provider).await.expect("pool status"),
        "straggler was mined before the reset — the test is not exercising the race"
    );

    let _fresh = reset_broadcast_lane(&provider, snapshot)
        .await
        .expect("reset broadcast lane");

    // Past the next block: if the straggler survived the reset it has mined
    // by now and the nonce has moved past the fresh snapshot.
    tokio::time::sleep(Duration::from_secs(BLOCK_TIME * 2)).await;
    let after = provider
        .get_transaction_count(deployer_addr)
        .await
        .expect("nonce after");
    assert_eq!(
        after, before,
        "a straggler from the killed forge advanced the deployer nonce past the \
         fresh snapshot — every retry will now die with `nonce too low`"
    );
    assert!(
        pool_is_empty(&provider).await.expect("pool status"),
        "anvil's pool still holds transactions after the lane reset"
    );

    let _ = anvil.kill();
    let _ = anvil.wait();
}

#[test]
fn ci_scaling_widens_the_budget_only_under_ci() {
    // Local runs keep the tighter base budget so a genuine hang fails fast;
    // CI multiplies it to absorb runner contention (#1384).
    let base = Duration::from_secs(60);
    assert_eq!(scale_for_ci(base, false), base);
    assert_eq!(scale_for_ci(base, true), base * CI_TIMEOUT_MULTIPLIER);
}
