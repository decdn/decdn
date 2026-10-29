use super::*;

#[test]
fn parse_http_date_epoch() {
    // 1970-01-01T00:00:00 GMT → 0.
    assert_eq!(parse_http_date("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
}

#[test]
fn parse_http_date_known_value() {
    // RFC 7231's own example: 784111777.
    assert_eq!(
        parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"),
        Some(784_111_777)
    );
}

#[test]
fn registration_status_unbound_is_due() {
    let local = B256::repeat_byte(0xAA);
    // No binding yet (`nodeId == 0`): a fresh registration is due.
    assert!(matches!(
        registration_status(B256::ZERO, false, local),
        RegistrationStatus::Due
    ));
}

#[test]
fn registration_status_same_node_active_is_already_registered() {
    let local = B256::repeat_byte(0xAA);
    // Bound to this key and active: registration is skipped.
    assert!(matches!(
        registration_status(local, true, local),
        RegistrationStatus::AlreadyRegistered
    ));
}

#[test]
fn registration_status_same_node_inactive_is_due() {
    let local = B256::repeat_byte(0xAA);
    // Deregistered: the binding to this key survives but `active` is false,
    // so a re-registration is due rather than skipped. Skipping it would
    // strand the node inactive.
    assert!(matches!(
        registration_status(local, false, local),
        RegistrationStatus::Due
    ));
}

#[test]
fn registration_status_different_node_is_divergent_regardless_of_active() {
    let local = B256::repeat_byte(0xAA);
    let other = B256::repeat_byte(0xBB);
    // A binding to a different, non-zero node key is a divergence whether or
    // not it is active — setup never overwrites the operator's bound key.
    assert!(matches!(
        registration_status(other, true, local),
        RegistrationStatus::Divergent
    ));
    assert!(matches!(
        registration_status(other, false, local),
        RegistrationStatus::Divergent
    ));
}

#[test]
fn parse_http_date_rejects_garbage() {
    assert_eq!(parse_http_date("not a date"), None);
    assert_eq!(parse_http_date("Sun, 06 Foo 1994 08:49:37 GMT"), None);
    assert_eq!(parse_http_date(""), None);
}

#[test]
fn parse_http_date_boundary_fields() {
    // Leap second (sec=60) is deliberately accepted.
    assert_eq!(parse_http_date("Thu, 01 Jan 1970 00:00:60 GMT"), Some(60));
    // Out-of-range fields are rejected.
    assert_eq!(parse_http_date("Thu, 01 Jan 1970 00:00:61 GMT"), None);
    assert_eq!(parse_http_date("Thu, 01 Jan 1970 00:60:00 GMT"), None);
    assert_eq!(parse_http_date("Thu, 01 Jan 1970 24:00:00 GMT"), None);
    assert_eq!(parse_http_date("Thu, 00 Jan 1970 00:00:00 GMT"), None);
    assert_eq!(parse_http_date("Thu, 32 Jan 1970 00:00:00 GMT"), None);
    // Out-of-band years are rejected so `days * 86_400` can't overflow i64.
    assert_eq!(parse_http_date("Thu, 01 Jan 1969 00:00:00 GMT"), None);
    assert_eq!(parse_http_date("Thu, 01 Jan 10000 00:00:00 GMT"), None);
    assert_eq!(
        parse_http_date("Thu, 01 Jan 292471210647 00:00:00 GMT"),
        None
    );
    // In-band edges still parse (guards against an off-by-one in `..=9999`;
    // the 1970 lower edge is covered by `parse_http_date_epoch`).
    assert!(parse_http_date("Fri, 31 Dec 9999 23:59:59 GMT").is_some());
}

#[test]
fn parse_http_date_rejects_short_token_strings() {
    // Missing the time token entirely → None (no panic on `tokens.get(4)`).
    assert_eq!(parse_http_date("Sun, 06 Nov 1994"), None);
    assert_eq!(parse_http_date("Sun 06"), None);
    // Missing seconds in the time token → None.
    assert_eq!(parse_http_date("Sun, 06 Nov 1994 08:49 GMT"), None);
}

#[test]
fn parse_http_date_trusts_zone_label_as_gmt() {
    // The zone token is ignored (trusted as GMT), by deliberate design —
    // a non-GMT label parses identically rather than erroring.
    assert_eq!(
        parse_http_date("Sun, 06 Nov 1994 08:49:37 UTC"),
        Some(784_111_777)
    );
}

#[test]
fn days_from_civil_matches_known_epochs() {
    assert_eq!(days_from_civil(1970, 1, 1), 0);
    assert_eq!(days_from_civil(1970, 1, 2), 1);
    assert_eq!(days_from_civil(1969, 12, 31), -1);
    // 2000-03-01 is 11017 days after the epoch.
    assert_eq!(days_from_civil(2000, 3, 1), 11_017);
}

fn pf_with(skew: Option<i64>, token: u64, shortfall: u64, native: u64, gas: u64) -> Preflight {
    Preflight {
        rpc_chain_id: 42,
        signing_chain_id: 42,
        clock_skew: skew,
        token_balance: U256::from(token),
        shortfall: U256::from(shortfall),
        native_balance: U256::from(native),
        gas_needed: U256::from(gas),
        buyer_usdc: None,
    }
}

#[test]
fn clock_status_tri_state() {
    assert_eq!(pf_with(Some(0), 0, 0, 0, 0).clock_status(), CheckStatus::Ok);
    assert_eq!(
        pf_with(Some(10), 0, 0, 0, 0).clock_status(),
        CheckStatus::Ok
    );
    assert_eq!(
        pf_with(Some(-10), 0, 0, 0, 0).clock_status(),
        CheckStatus::Ok
    );
    assert_eq!(
        pf_with(Some(11), 0, 0, 0, 0).clock_status(),
        CheckStatus::Fail
    );
    assert_eq!(
        pf_with(Some(-61), 0, 0, 0, 0).clock_status(),
        CheckStatus::Fail
    );
    // Undetermined → WARN, which does NOT block.
    assert_eq!(pf_with(None, 0, 0, 0, 0).clock_status(), CheckStatus::Warn);
    assert!(!pf_with(None, 0, 0, 0, 0).clock_status().blocks());
}

#[test]
fn all_ok_requires_every_blocking_check() {
    // All good, clock undetermined (WARN, non-blocking) → passes.
    assert!(pf_with(None, 100, 10, 100, 10).all_ok());
    // Insufficient token → fails.
    assert!(!pf_with(Some(0), 5, 10, 100, 10).all_ok());
    // Insufficient native gas → fails.
    assert!(!pf_with(Some(0), 100, 10, 5, 10).all_ok());
    // Blocking clock skew → fails even with funds.
    assert!(!pf_with(Some(999), 100, 10, 100, 10).all_ok());
    // chain id mismatch → fails.
    let mut pf = pf_with(Some(0), 100, 10, 100, 10);
    pf.signing_chain_id = 1;
    assert!(!pf.all_ok());
}

fn buyer(working_deposit: u64, balance: BuyerBalance) -> BuyerUsdc {
    BuyerUsdc {
        working_deposit,
        balance,
    }
}

fn read(balance: u64) -> BuyerBalance {
    BuyerBalance::Read(balance)
}

fn read_error(reason: &str) -> BuyerBalance {
    BuyerBalance::ReadError(reason.to_owned())
}

fn file(body: &str) -> chain_ctx::FileConfig {
    toml::from_str(body).unwrap()
}

/// The wallet must cover the whole working deposit; anything less, a wallet
/// not created yet, or no balance at all warns. The row never fails.
#[test]
fn buyer_usdc_status_warns_below_the_working_deposit() {
    assert_eq!(buyer(10, read(10)).status(), CheckStatus::Ok);
    assert_eq!(buyer(10, read(11)).status(), CheckStatus::Ok);
    assert_eq!(buyer(10, read(9)).status(), CheckStatus::Warn);
    assert_eq!(buyer(10, read(0)).status(), CheckStatus::Warn);
    assert_eq!(
        buyer(10, BuyerBalance::NoWallet).status(),
        CheckStatus::Warn
    );
    assert_eq!(
        buyer(10, read_error("rpc down")).status(),
        CheckStatus::Warn
    );
    assert_eq!(
        buyer(10, BuyerBalance::ConfigError("x".to_owned())).status(),
        CheckStatus::Warn
    );
    assert_eq!(buyer(10, read(4)).shortfall(), Some(6));
    assert_eq!(buyer(10, read(40)).shortfall(), Some(0));
    assert_eq!(buyer(10, BuyerBalance::NoWallet).shortfall(), Some(10));
    assert_eq!(buyer(10, read_error("rpc down")).shortfall(), None);
}

/// An unfunded buyer wallet warns but does not gate the live run: the node
/// serves without it.
#[test]
fn all_ok_ignores_an_unfunded_buyer_wallet() {
    let mut pf = pf_with(Some(0), 100, 10, 100, 10);
    pf.buyer_usdc = Some(buyer(10_000_000, read(0)));
    assert!(pf.all_ok());
    pf.buyer_usdc = Some(buyer(10_000_000, read_error("rpc down")));
    assert!(pf.all_ok());
}

/// The shortfall row names the deposit, the balance, the shortfall, where to
/// send USDC and the faucet: what the operator needs to fund the node. It also
/// says that an open pool draws only refills, so a re-run on a working node
/// does not read as a node that cannot buy.
#[test]
fn buyer_usdc_detail_names_the_shortfall_and_the_funding_pointer() {
    let operator = Address::repeat_byte(0x42);
    let detail = buyer_usdc_detail(
        &buyer(10_000_000, read(2_500_000)),
        Some(operator),
        Some("https://faucet.example"),
    );
    assert!(
        detail.contains("working deposit 10.000000 USDC"),
        "{detail}"
    );
    assert!(detail.contains("hold 2.500000"), "{detail}");
    assert!(detail.contains("short 7.500000"), "{detail}");
    assert!(detail.contains("draws only refills"), "{detail}");
    assert!(detail.contains(&format!("{operator:#x}")), "{detail}");
    assert!(
        detail.contains("(faucet: https://faucet.example)"),
        "{detail}"
    );

    // An unknown chain has no faucet; the keyless run has no address yet.
    let detail = buyer_usdc_detail(&buyer(10_000_000, BuyerBalance::NoWallet), None, None);
    assert!(detail.contains("hold nothing (no keys yet)"), "{detail}");
    assert!(detail.contains("short 10.000000"), "{detail}");
    assert!(
        detail.contains("`decdn whoami`'s `eth address:` line"),
        "{detail}"
    );
    assert!(!detail.contains("faucet"), "{detail}");

    let funded = buyer_usdc_detail(&buyer(10_000_000, read(10_000_000)), Some(operator), None);
    assert_eq!(
        funded,
        "pull-through on: working deposit 10.000000 USDC, hold 10.000000"
    );

    let unread = buyer_usdc_detail(&buyer(10_000_000, read_error("rpc down")), None, None);
    assert!(
        unread.contains("could not read the wallet's USDC (rpc down)"),
        "{unread}"
    );
    assert!(unread.contains("re-run setup"), "{unread}");

    let config = buyer_usdc_detail(
        &buyer(
            10_000_000,
            BuyerBalance::ConfigError("pool unset".to_owned()),
        ),
        None,
        None,
    );
    assert!(
        config.contains("pool unset: decdn-node refuses to start"),
        "{config}"
    );
}

/// `preflight.buyer_usdc` is present and `null` with pull-through off, and
/// otherwise carries the state, the deposit, the balance, the shortfall, `ok`,
/// and the reason.
#[test]
fn build_summary_reports_the_buyer_usdc_row() {
    let summary = |pf: &Preflight| {
        build_summary(
            pf,
            false,
            &sample_plan(),
            &bond::Outcome::default(),
            None,
            true,
            None,
            None,
            None,
            "US",
            0,
        )
    };
    let mut pf = pf_with(None, 0, 0, 0, 0);
    assert_eq!(
        summary(&pf)["preflight"].get("buyer_usdc"),
        Some(&serde_json::Value::Null),
        "the key is present and null with pull-through off"
    );

    pf.buyer_usdc = Some(buyer(10_000_000, read(4_000_000)));
    assert_eq!(
        summary(&pf)["preflight"]["buyer_usdc"],
        serde_json::json!({
            "state": "short",
            "working_deposit_micro_usdc": 10_000_000,
            "balance_micro_usdc": 4_000_000,
            "shortfall_micro_usdc": 6_000_000,
            "ok": false,
            "error": null,
        })
    );

    pf.buyer_usdc = Some(buyer(10_000_000, read(10_000_000)));
    assert_eq!(
        summary(&pf)["preflight"]["buyer_usdc"]["state"],
        serde_json::json!("covered")
    );
    assert_eq!(
        summary(&pf)["preflight"]["buyer_usdc"]["ok"],
        serde_json::json!(true)
    );

    pf.buyer_usdc = Some(buyer(10_000_000, read_error("rpc down")));
    assert_eq!(
        summary(&pf)["preflight"]["buyer_usdc"],
        serde_json::json!({
            "state": "read_error",
            "working_deposit_micro_usdc": 10_000_000,
            "balance_micro_usdc": null,
            "shortfall_micro_usdc": null,
            "ok": false,
            "error": "rpc down",
        })
    );
}

/// The keyless dry run reports a wallet not created yet, distinct in `--json`
/// from a chain read of an empty wallet.
#[test]
fn keyless_buyer_usdc_reports_no_wallet_without_a_read() {
    assert!(keyless_buyer_usdc(BuyerUsdcPlan::Off).is_none());
    let row = keyless_buyer_usdc(BuyerUsdcPlan::Read {
        working_deposit: 10_000_000,
        pool: Address::repeat_byte(0x11),
    })
    .unwrap();
    assert_eq!(
        buyer_usdc_json(Some(&row)),
        serde_json::json!({
            "state": "no_wallet",
            "working_deposit_micro_usdc": 10_000_000,
            "balance_micro_usdc": 0,
            "shortfall_micro_usdc": 10_000_000,
            "ok": false,
            "error": null,
        })
    );
    // A config error is not hidden behind the missing wallet.
    let settled = keyless_buyer_usdc(buyer_usdc_plan(&file(""), None)).unwrap();
    assert_eq!(
        buyer_usdc_json(Some(&settled))["state"],
        serde_json::json!("config_error")
    );
}

const POOL_A: &str = "0x00000000000000000000000000000000000000A1";
const POOL_B: &str = "0x00000000000000000000000000000000000000B2";

fn plan_pool(plan: &BuyerUsdcPlan) -> Option<Address> {
    match plan {
        BuyerUsdcPlan::Read { pool, .. } => Some(*pool),
        BuyerUsdcPlan::Off | BuyerUsdcPlan::Settled(_) => None,
    }
}

fn plan_error(plan: &BuyerUsdcPlan) -> Option<&str> {
    match plan {
        BuyerUsdcPlan::Settled(row) => row.error(),
        BuyerUsdcPlan::Off | BuyerUsdcPlan::Read { .. } => None,
    }
}

/// The pool resolves as `decdn-node` resolves it: the env var, else the
/// config. A missing, zero or zero-deposit config settles the row as a config
/// error; pull-through off plans no row at all.
#[test]
fn buyer_usdc_plan_resolves_the_pool_like_the_daemon() {
    let with_pool = file(&format!(
        "[blockchain]\npayment_pool_address = \"{POOL_A}\"\n"
    ));
    let plan = buyer_usdc_plan(&with_pool, None);
    assert_eq!(plan_pool(&plan), Some(POOL_A.parse().unwrap()));
    assert!(matches!(
        plan,
        BuyerUsdcPlan::Read {
            working_deposit: 10_000_000,
            ..
        }
    ));

    // The env var wins over the file, as the daemon's clap `env` does.
    let plan = buyer_usdc_plan(&with_pool, Some(POOL_B));
    assert_eq!(plan_pool(&plan), Some(POOL_B.parse().unwrap()));

    // No pool anywhere: the daemon refuses to start. No known-chain fallback,
    // even on a known chain.
    let unset = file("[blockchain]\nchain_id = 421614\n");
    let plan = buyer_usdc_plan(&unset, None);
    let reason = plan_error(&plan).unwrap();
    assert!(
        reason.contains("blockchain.payment_pool_address is not set"),
        "{reason}"
    );
    assert!(reason.contains(PAYMENT_POOL_ENV), "{reason}");

    let zero = file(&format!(
        "[blockchain]\npayment_pool_address = \"{}\"\n",
        Address::ZERO
    ));
    let reason = plan_error(&buyer_usdc_plan(&zero, None))
        .map(str::to_owned)
        .unwrap();
    assert!(reason.contains("zero address"), "{reason}");

    let zero_deposit = file(&format!(
        "[blockchain]\npayment_pool_address = \"{POOL_A}\"\nbuyer_working_deposit_micro_usdc = 0\n"
    ));
    let reason = plan_error(&buyer_usdc_plan(&zero_deposit, None))
        .map(str::to_owned)
        .unwrap();
    assert!(reason.contains("must be > 0"), "{reason}");

    let off = file("[cache]\nnode_to_node_pull_through_enabled = false\n");
    assert!(matches!(
        buyer_usdc_plan(&off, Some(POOL_B)),
        BuyerUsdcPlan::Off
    ));
}

/// A failed chain read is a reason, not an abort, and the reason never echoes
/// the RPC URL: it reaches stdout past `main`'s sanitizer (#954).
#[tokio::test]
async fn read_wallet_usdc_redacts_a_failed_read() {
    decdn_common::tls::install_crypto_provider();
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let url = format!("http://127.0.0.1:{port}/v3/SECRETKEY123");
    let provider = ProviderBuilder::new().connect_http(url.parse().unwrap());
    let reason = read_wallet_usdc(
        &provider,
        Address::repeat_byte(0x11),
        Address::repeat_byte(0x22),
    )
    .await
    .unwrap_err();
    assert!(reason.contains("PaymentPool.usdc()"), "{reason}");
    assert!(!reason.contains("SECRETKEY123"), "{reason}");
    assert!(!reason.contains(&format!("127.0.0.1:{port}")), "{reason}");
}

/// The testnet row points at its faucet; an unknown chain (anvil) gets none.
#[test]
fn usdc_faucet_follows_the_known_chain() {
    assert!(usdc_faucet(421_614).is_some());
    assert!(usdc_faucet(31_337).is_none());
}

#[test]
fn precheck_keys_decides_action() {
    let def = Path::new("/data/keystore.json");
    // Both present.
    assert_eq!(
        precheck_keys(true, true, def, def).unwrap(),
        KeyAction::Present
    );
    // Both absent, default keystore → generate.
    assert_eq!(
        precheck_keys(false, false, def, def).unwrap(),
        KeyAction::Generate
    );
    // Partial material → error.
    assert!(precheck_keys(true, false, def, def).is_err());
    assert!(precheck_keys(false, true, def, def).is_err());
    // Both absent but a custom keystore path → error (can't auto-provision).
    let custom = Path::new("/elsewhere/ks.json");
    assert!(precheck_keys(false, false, custom, def).is_err());
}

fn sample_plan() -> bond::Plan {
    bond::Plan {
        mbps: 1000,
        token: Address::repeat_byte(0x11),
        capacity_bond: Address::repeat_byte(0x22),
        required: U256::from(50_000u64),
        target: U256::from(50_000u64),
        prior: U256::ZERO,
        shortfall: U256::from(50_000u64),
        needs_declare: true,
    }
}

fn sample_readiness() -> Readiness {
    Readiness {
        active: true,
        active_bond: U256::from(50_000u64),
        declared_mbps: U256::from(1000u64),
        node_id: B256::repeat_byte(0xAB),
    }
}

/// #1355 review — the partial-failure summary must be the SAME shape as the
/// success one, carrying whatever landed, so a `--json` consumer parses one
/// schema and branches on `partial`. The bond hash is the case that matters
/// most: it is an irreversible on-chain spend, and the aggregated object is
/// its only machine-readable carrier (`hline` is a no-op under `--json`).
#[test]
fn build_summary_partial_keeps_the_shape_and_the_landed_hashes() {
    let bond_outcome = bond::Outcome {
        approve: Some(B256::repeat_byte(0xA1)),
        bond: Some(B256::repeat_byte(0xB2)),
        declare: None,
    };
    let v = build_summary(
        &pf_with(None, 0, 50_000, 100, 10),
        false,
        &sample_plan(),
        &bond_outcome,
        None,
        false,
        None,
        Some(true),
        None,
        "US",
        1,
    );

    assert_eq!(v["partial"], serde_json::json!(true));
    assert!(v["readiness"].is_null(), "no read-back on the failure path");
    // Everything that actually happened is still reported.
    assert_eq!(
        v["bond"]["approve_tx"],
        serde_json::json!(format!("{:#x}", B256::repeat_byte(0xA1)))
    );
    assert_eq!(
        v["bond"]["bond_tx"],
        serde_json::json!(format!("{:#x}", B256::repeat_byte(0xB2)))
    );
    // Same keys as the success shape, so one parser handles both.
    let ok = build_summary(
        &pf_with(None, 0, 50_000, 100, 10),
        false,
        &sample_plan(),
        &bond_outcome,
        None,
        false,
        Some(&sample_readiness()),
        Some(true),
        None,
        "US",
        1,
    );
    assert_eq!(ok["partial"], serde_json::json!(false));
    let keys = |v: &serde_json::Value| {
        let mut k: Vec<String> = v
            .as_object()
            .expect("summary is an object")
            .keys()
            .cloned()
            .collect();
        k.sort();
        k
    };
    assert_eq!(keys(&v), keys(&ok), "partial and success shapes must match");
}

/// #1355 review — a `registerNode` that was broadcast but whose receipt
/// could not be read must still reach the partial summary. It cannot travel
/// in `register_outcome`: `submit_registration` returns that by value, so
/// the `Err` takes it with them. The caller-owned slot is the only carrier,
/// and this pins that it is actually rendered rather than merely threaded.
#[test]
fn build_summary_reports_an_unresolved_registration_tx() {
    let in_flight = B256::repeat_byte(0x7E);
    let v = build_summary(
        &pf_with(None, 100, 10, 100, 10),
        false,
        &sample_plan(),
        &bond::Outcome::default(),
        // No outcome: the registration did not resolve.
        None,
        false,
        None,
        Some(true),
        Some(in_flight),
        "US",
        1,
    );

    assert_eq!(v["partial"], serde_json::json!(true));
    assert_eq!(
        v["register"]["tx"],
        serde_json::json!(format!("{in_flight:#x}")),
        "the in-flight registration must be named: {v}"
    );
    // `due` is what separates "owed but unresolved" from "already
    // registered", which `skipped` alone cannot express.
    assert_eq!(v["register"]["skipped"], serde_json::json!(true));
    assert_eq!(v["register"]["due"], serde_json::json!(true));

    // The genuinely-not-due case must stay distinguishable from it.
    let not_due = build_summary(
        &pf_with(None, 100, 10, 100, 10),
        false,
        &sample_plan(),
        &bond::Outcome::default(),
        None,
        false,
        Some(&sample_readiness()),
        Some(false),
        None,
        "US",
        1,
    );
    assert_eq!(not_due["register"]["due"], serde_json::json!(false));
    assert!(not_due["register"]["tx"].is_null());
}

#[test]
fn build_summary_register_skipped() {
    let pf = pf_with(None, 100, 10, 100, 10);
    let v = build_summary(
        &pf,
        false,
        &sample_plan(),
        &bond::Outcome::default(),
        None,
        false,
        Some(&sample_readiness()),
        Some(true),
        None,
        "US",
        1,
    );
    assert_eq!(v["register"]["skipped"], serde_json::json!(true));
    assert!(v["register"].get("submitted").is_none());
    // Undetermined clock serializes as JSON null and clock_ok stays true.
    assert!(v["preflight"]["clock_skew_secs"].is_null());
    assert_eq!(v["preflight"]["clock_ok"], serde_json::json!(true));
    assert_eq!(v["readiness"]["registry_active"], serde_json::json!(true));
}

#[test]
fn build_summary_register_submitted() {
    let outcome = register::RegisterOutcome {
        node_id: B256::repeat_byte(0xCD),
        operator: Address::repeat_byte(0x01),
        chain_id: 42,
        capacity_bond: Address::repeat_byte(0x22),
        region: "DE".to_string(),
        binding_nonce: 0,
        registration_nonce: 0,
        multiaddr_count: 1,
        binding_sig: vec![0x11; 65],
        ed25519_sig: vec![0x22; 64],
        tx: Some(B256::repeat_byte(0x55)),
    };
    let pf = pf_with(Some(2), 100, 10, 100, 10);
    let v = build_summary(
        &pf,
        true,
        &sample_plan(),
        &bond::Outcome::default(),
        Some(&outcome),
        false,
        Some(&sample_readiness()),
        Some(true),
        None,
        "DE",
        2,
    );
    assert_eq!(v["register"]["skipped"], serde_json::json!(false));
    assert_eq!(v["register"]["submitted"], serde_json::json!(true));
    assert!(
        v["register"]["tx"]
            .as_str()
            .is_some_and(|s| s.starts_with("0x5555"))
    );
    assert_eq!(v["preflight"]["clock_skew_secs"], serde_json::json!(2));
    assert_eq!(v["keys_generated"], serde_json::json!(true));
}

#[test]
fn present_label_maps() {
    assert_eq!(present_label(true), "present");
    assert_eq!(present_label(false), "absent");
}

mod earnings_key {
    use decdn_common::admin::{EarningsPeriod, EarningsResponse};

    use super::*;

    fn period(net: u64) -> EarningsPeriod {
        EarningsPeriod {
            from_unix: 0,
            net_micro_usdc: net,
            gross_micro_usdc: net,
            bytes_settled: 0,
            settlements: 1,
        }
    }

    #[test]
    fn attach_earnings_adds_null_without_a_daemon_and_the_response_with_one() {
        let mut summary = serde_json::json!({ "partial": false, "keys_generated": true });
        attach_earnings(&mut summary, &Ok(None));
        assert!(summary["earnings"].is_null());
        assert_eq!(
            summary["partial"],
            serde_json::json!(false),
            "other keys untouched"
        );

        let resp = EarningsResponse {
            operator_address: None,
            covered_from_unix: 5,
            last_24h: period(1),
            last_7d: period(2),
            since_ledger_start: period(3),
            since: None,
            last_settlement: None,
            pending: None,
        };
        attach_earnings(&mut summary, &Ok(Some(resp)));
        assert_eq!(
            summary["earnings"]["since_ledger_start"]["net_micro_usdc"],
            3
        );
        assert_eq!(summary["earnings"]["covered_from_unix"], 5);
        assert!(summary["earnings"]["since"].is_null());
        attach_earnings(&mut summary, &Err("ledger corrupt".to_owned()));
        assert_eq!(summary["earnings"]["error"], "ledger corrupt");
    }
}
