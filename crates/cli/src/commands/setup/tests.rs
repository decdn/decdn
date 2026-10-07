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
