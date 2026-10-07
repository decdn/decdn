use super::*;

/// A submittable plan: active, comfortably under the ceiling, cooldown
/// elapsed. Individual tests knock out one field to exercise each gate.
fn plan() -> Plan {
    Plan {
        capacity_bond: Address::repeat_byte(0xCB),
        operator: Address::repeat_byte(0x0E),
        node_id: B256::repeat_byte(0xAB),
        active: true,
        multiaddr_count: 1,
        packed_size: 36,
        max_multiaddr_size: 1_024,
        last_update: 1_000,
        cooldown_secs: 3_600,
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
fn submittable_plan_passes() {
    ensure_submittable(&plan()).expect("an active, in-bounds, cooled-down plan is submittable");
}

#[test]
fn non_blank_multiaddrs_pass() {
    reject_blank_multiaddrs(&["/ip4/203.0.113.10/udp/4433/quic-v1".to_string()])
        .expect("a real address is accepted");
}

/// clap requires at least one `--multiaddr`, but `--multiaddr ""` slips
/// through and would pack to a zero-length entry — the relay-pinned footgun.
#[test]
fn empty_multiaddr_is_rejected() {
    let err = reject_blank_multiaddrs(&["ok".to_string(), String::new()])
        .expect_err("an empty entry must not proceed");
    assert!(
        format!("{err}").contains("#2"),
        "names the offending index: {err}"
    );
}

#[test]
fn whitespace_only_multiaddr_is_rejected() {
    reject_blank_multiaddrs(&["   ".to_string()])
        .expect_err("a whitespace-only entry must not proceed");
}

/// An ejected operator cannot register — `deregister` sends them to
/// `unbond --all`, and so must this command, not to a `register` that reverts.
#[test]
fn inactive_message_routes_ejected_to_unbond_not_register_only() {
    let mut p = plan();
    p.active = false;
    let err = ensure_submittable(&p).expect_err("inactive must not proceed");
    let msg = format!("{err}");
    assert!(
        msg.contains("unbond --all"),
        "names the ejected exit: {msg}"
    );
}

#[test]
fn inactive_operator_is_pointed_at_register() {
    let mut p = plan();
    p.active = false;
    let err = ensure_submittable(&p).expect_err("inactive must not proceed");
    let msg = format!("{err}");
    assert!(msg.contains("NodeNotActive"), "names the revert: {msg}");
    assert!(msg.contains("node register"), "names re-entry: {msg}");
}

#[test]
fn oversized_set_reports_size_and_ceiling() {
    let mut p = plan();
    p.packed_size = 2_048;
    p.max_multiaddr_size = 1_024;
    let err = ensure_submittable(&p).expect_err("over the ceiling must not proceed");
    let msg = format!("{err}");
    assert!(
        msg.contains("MultiaddrsTooLarge"),
        "names the revert: {msg}"
    );
    assert!(msg.contains("2048"), "reports the packed size: {msg}");
    assert!(msg.contains("1024"), "reports the ceiling: {msg}");
}

/// The packed size is allowed to hit the ceiling exactly — the contract's
/// check is `> maxMultiaddrSize`, so equality must pass, not trip.
#[test]
fn packed_size_equal_to_ceiling_passes() {
    let mut p = plan();
    p.packed_size = 1_024;
    p.max_multiaddr_size = 1_024;
    ensure_submittable(&p).expect("size == ceiling is within bounds");
}

#[test]
fn active_cooldown_reports_ready_at() {
    let mut p = plan();
    p.last_update = 9_000;
    p.cooldown_secs = 3_600;
    p.ready_at = 12_600;
    p.head_timestamp = 10_000; // before ready_at
    let err = ensure_submittable(&p).expect_err("within cooldown must not proceed");
    let msg = format!("{err}");
    assert!(
        msg.contains("MultiaddrCooldownActive"),
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

    let failed = rendered(&plan(), false, None, false);
    assert!(
        failed.contains("submitted=false dry_run=false"),
        "a failed send is not a preview: {failed}"
    );
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
        v.get("multiaddrs").and_then(serde_json::Value::as_u64),
        Some(1)
    );
    assert_eq!(
        v.get("packed_bytes").and_then(serde_json::Value::as_u64),
        Some(36)
    );
    assert_eq!(
        v.get("max_multiaddr_size")
            .and_then(serde_json::Value::as_u64),
        Some(1_024)
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
