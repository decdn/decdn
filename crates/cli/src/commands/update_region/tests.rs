use super::*;

/// A submittable plan: active and cooldown elapsed. Individual tests knock
/// out one field to exercise each gate.
fn plan() -> Plan {
    Plan {
        capacity_bond: Address::repeat_byte(0xCB),
        operator: Address::repeat_byte(0x0E),
        node_id: B256::repeat_byte(0xAB),
        active: true,
        current_region: "US".to_string(),
        new_region: "DE".to_string(),
        last_changed: 1_000,
        stability_window_secs: 3_600,
        ready_at: 4_600,
        head_timestamp: 10_000,
    }
}

fn rendered(p: &Plan, json: bool, tx: Option<&B256>, dry_run: bool) -> String {
    let mut buf = Vec::new();
    write_plan(&mut buf, p, json, tx, dry_run).expect("write to a Vec cannot fail");
    String::from_utf8(buf).expect("output is ASCII")
}

#[test]
fn valid_region_is_accepted_and_normalized() {
    let region = validate_region("de").expect("a lowercase code is accepted");
    assert_eq!(region.as_str(), "DE", "the code is uppercased");
    let region = validate_region("  us  ").expect("surrounding whitespace is trimmed");
    assert_eq!(region.as_str(), "US");
}

/// The contract only length-checks, so a garbage code must be rejected
/// CLI-side — the whole reason this command validates.
#[test]
fn garbage_region_is_rejected() {
    let err = validate_region("OO").expect_err("a non-allowlisted code must not proceed");
    assert!(
        format!("{err}").contains("ISO 3166-1 alpha-2"),
        "names the expected format: {err}"
    );
}

#[test]
fn non_alpha2_region_is_rejected() {
    validate_region("USA").expect_err("a three-letter code is not alpha-2");
    validate_region("").expect_err("an empty code is not a region");
    validate_region("GLOBAL").expect_err("the GLOBAL sentinel is not an alpha-2 code");
}

#[test]
fn submittable_plan_passes() {
    ensure_submittable(&plan()).expect("an active, cooled-down plan is submittable");
}

/// An ejected operator cannot register — `deregister` sends them to
/// `unbond --all`, and so must this command, not to a `register` that reverts.
#[test]
fn inactive_message_routes_ejected_to_unbond_not_register_only() {
    let mut p = plan();
    p.active = false;
    let err = ensure_submittable(&p).expect_err("inactive must not proceed");
    let msg = format!("{err}");
    assert!(msg.contains("NodeNotActive"), "names the revert: {msg}");
    assert!(msg.contains("node register"), "names re-entry: {msg}");
    assert!(
        msg.contains("unbond --all"),
        "names the ejected exit: {msg}"
    );
}

#[test]
fn active_cooldown_reports_ready_at() {
    let mut p = plan();
    p.last_changed = 9_000;
    p.stability_window_secs = 3_600;
    p.ready_at = 12_600;
    p.head_timestamp = 10_000; // before ready_at
    let err = ensure_submittable(&p).expect_err("within cooldown must not proceed");
    let msg = format!("{err}");
    assert!(
        msg.contains("RegionCooldownActive"),
        "names the revert: {msg}"
    );
    assert!(msg.contains("12600"), "reports the ready-at time: {msg}");
    assert!(msg.contains("2600"), "reports the remaining wait: {msg}");
}

/// Cooldown boundary: the contract accepts `head == ready_at` (its check is
/// `block.timestamp < readyAt`), so the pre-check must too.
#[test]
fn head_equal_to_ready_at_passes() {
    let mut p = plan();
    p.ready_at = 10_000;
    p.head_timestamp = 10_000;
    ensure_submittable(&p).expect("head == ready_at clears the cooldown");
}

#[test]
fn dry_run_reports_no_tx_and_is_distinguishable_from_a_failed_send() {
    let dry = rendered(&plan(), false, None, true);
    assert!(dry.contains("update_tx=skipped"), "{dry}");
    assert!(dry.contains("submitted=false dry_run=true"), "{dry}");
    assert!(dry.contains("new_region=DE"), "{dry}");
    assert!(dry.contains("current_region=US"), "{dry}");

    let failed = rendered(&plan(), false, None, false);
    assert!(
        failed.contains("submitted=false dry_run=false"),
        "a failed send is not a preview: {failed}"
    );
}

/// An operator that never set a region reads back an empty `regionHint`;
/// render a sentinel rather than a blank value so the line stays parseable.
#[test]
fn unset_current_region_renders_sentinel() {
    let mut p = plan();
    p.current_region = String::new();
    let s = rendered(&p, false, None, true);
    assert!(s.contains("current_region=(unset)"), "{s}");
}

#[test]
fn json_carries_the_plan_and_the_tx() {
    let tx = B256::repeat_byte(0xAB);
    let s = rendered(&plan(), true, Some(&tx), false);
    let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
    assert_eq!(
        v.get("submitted").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        v.get("new_region").and_then(serde_json::Value::as_str),
        Some("DE")
    );
    assert_eq!(
        v.get("current_region").and_then(serde_json::Value::as_str),
        Some("US")
    );
    assert_eq!(
        v.get("stability_window_secs")
            .and_then(serde_json::Value::as_u64),
        Some(3_600)
    );
    assert_eq!(
        v.get("update_tx").and_then(serde_json::Value::as_str),
        Some(format!("{tx:#x}").as_str())
    );
}

#[test]
fn json_null_tx_on_a_dry_run() {
    let s = rendered(&plan(), true, None, true);
    let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
    assert!(
        v.get("update_tx").is_some_and(serde_json::Value::is_null),
        "the key is present-and-null, not absent: {s}"
    );
    assert_eq!(
        v.get("dry_run").and_then(serde_json::Value::as_bool),
        Some(true)
    );
}
