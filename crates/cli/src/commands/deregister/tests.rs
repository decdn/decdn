use alloy::primitives::U256;

use super::*;

fn plan(active: bool, ejected: bool, declared_mbps: u64) -> Plan {
    Plan {
        capacity_bond: Address::repeat_byte(0xCB),
        active,
        ejected,
        declared_mbps,
        retained_bond: U256::from(5_000u64),
    }
}

fn rendered(p: &Plan, json: bool, tx: Option<&B256>, dry_run: bool) -> String {
    let mut buf = Vec::new();
    write_plan(&mut buf, p, json, tx, dry_run).expect("write to a Vec cannot fail");
    String::from_utf8(buf).expect("output is ASCII")
}

#[test]
fn headless_without_yes_refuses() {
    assert_eq!(decide_confirmation(false, false), Confirmation::NeedFlag);
    assert_eq!(decide_confirmation(false, true), Confirmation::Prompt);
    // `--yes` bypasses the prompt on a terminal AND headless — the flag is
    // what makes a scripted run legal.
    assert_eq!(decide_confirmation(true, false), Confirmation::Bypassed);
    assert_eq!(decide_confirmation(true, true), Confirmation::Bypassed);
}

#[test]
fn ejected_operator_is_pointed_at_unbond_not_register() {
    let err = ensure_active(&plan(false, true, 1000)).expect_err("ejected must not proceed");
    let msg = format!("{err}");
    assert!(msg.contains("unbond --all"), "names the exit: {msg}");
    assert!(
        !msg.contains("node register"),
        "re-registering is exactly what an ejected operator cannot do: {msg}"
    );
}

#[test]
fn inactive_operator_gets_both_routes() {
    let err = ensure_active(&plan(false, false, 0)).expect_err("inactive must not proceed");
    let msg = format!("{err}");
    assert!(msg.contains("node register"), "names re-entry: {msg}");
    assert!(msg.contains("unbond --all"), "names the exit: {msg}");
}

#[test]
fn active_operator_proceeds() {
    ensure_active(&plan(true, false, 1000)).expect("an active node is deregisterable");
}

/// The bond disclosure is the point of the gate, not decoration: an
/// operator reading "deregister" as "refund" is the failure this prevents.
#[test]
fn disclosure_says_the_bond_is_not_returned() {
    let mut buf = Vec::new();
    write_disclosure(&mut buf, &plan(true, false, 1000)).expect("write to a Vec cannot fail");
    let s = String::from_utf8(buf).expect("output is ASCII");
    assert!(s.contains("NOT returned"), "{s}");
    assert!(s.contains("slashable"), "{s}");
    assert!(s.contains("unbond --all"), "names the next leg: {s}");
    assert!(s.contains("1000 Mbps"), "names the tier being cleared: {s}");
}

/// A never-declared operator has no tier line to print — reporting
/// "the declared tier of 0 Mbps is cleared" would invent a state change.
#[test]
fn disclosure_omits_the_tier_line_when_none_was_declared() {
    let mut buf = Vec::new();
    write_disclosure(&mut buf, &plan(true, false, 0)).expect("write to a Vec cannot fail");
    let s = String::from_utf8(buf).expect("output is ASCII");
    assert!(!s.contains("declared tier"), "{s}");
    assert!(
        s.contains("NOT returned"),
        "the bond line always prints: {s}"
    );
}

#[test]
fn dry_run_reports_no_tx_and_is_distinguishable_from_a_failed_send() {
    let dry = rendered(&plan(true, false, 1000), false, None, true);
    assert!(dry.contains("deregister_tx=skipped"), "{dry}");
    assert!(dry.contains("submitted=false dry_run=true"), "{dry}");

    let failed = rendered(&plan(true, false, 1000), false, None, false);
    assert!(
        failed.contains("submitted=false dry_run=false"),
        "a failed send is not a preview: {failed}"
    );
}

#[test]
fn json_carries_the_state_and_the_tx() {
    let tx = B256::repeat_byte(0xAB);
    let s = rendered(&plan(true, false, 1000), true, Some(&tx), false);
    let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
    assert_eq!(
        v.get("submitted").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        v.get("declared_mbps").and_then(serde_json::Value::as_u64),
        Some(1000)
    );
    assert_eq!(
        v.get("retained_bond_base")
            .and_then(serde_json::Value::as_str),
        Some("5000"),
        "base units are a decimal STRING — 1e18-scaled values overflow a JSON number"
    );
    assert_eq!(
        v.get("deregister_tx").and_then(serde_json::Value::as_str),
        Some(format!("{tx:#x}").as_str())
    );
}

#[test]
fn json_null_tx_on_a_dry_run() {
    let s = rendered(&plan(true, false, 0), true, None, true);
    let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
    assert!(
        v.get("deregister_tx")
            .is_some_and(serde_json::Value::is_null),
        "the key is present-and-null, not absent: {s}"
    );
    assert_eq!(
        v.get("dry_run").and_then(serde_json::Value::as_bool),
        Some(true)
    );
}
