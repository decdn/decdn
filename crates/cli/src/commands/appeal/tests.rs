use super::*;

fn plan(needs_approve: bool) -> Plan {
    Plan {
        slash_id: U256::from(7u64),
        slash_appeal: Address::repeat_byte(0x11),
        token: Address::repeat_byte(0x22),
        evidence: B256::repeat_byte(0xAB),
        bond: U256::from(1000u64),
        needs_approve,
    }
}

#[test]
fn parse_bytes32_accepts_prefixed_32_bytes() {
    let h = parse_bytes32("0x00000000000000000000000000000000000000000000000000000000000000ab")
        .unwrap();
    assert_eq!(h, B256::with_last_byte(0xab));
}

#[test]
fn parse_bytes32_rejects_wrong_length() {
    assert!(parse_bytes32("0xabcd").is_err());
}

#[test]
fn parse_slash_id_accepts_values_above_u64_max() {
    // u64::MAX + 1 — must not overflow (the whole point of U256).
    let big = "18446744073709551616";
    assert_eq!(
        parse_slash_id(big).unwrap(),
        U256::from(u64::MAX) + U256::from(1u64)
    );
    assert!(parse_slash_id("not-a-number").is_err());
}

#[test]
fn dry_run_reports_dry_run_true() {
    let p = plan(true);
    let mut buf = Vec::new();
    write_plan(&mut buf, &p, false, &Outcome::default(), true).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("submitted=false dry_run=true"), "{s}");
    assert!(s.contains("appeal_bond_base=1000"), "{s}");
    assert!(s.contains("open_tx=skipped"), "{s}");
}

#[test]
fn json_output_carries_fields() {
    let p = plan(false);
    let mut buf = Vec::new();
    write_plan(&mut buf, &p, true, &Outcome::default(), true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    // slash_id is emitted as a decimal string (uint256 can exceed u64).
    assert_eq!(
        v.get("slash_id").and_then(serde_json::Value::as_str),
        Some("7")
    );
    assert_eq!(
        v.get("needs_approve").and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert_eq!(
        v.get("dry_run").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert!(v.get("approve_tx").is_some_and(serde_json::Value::is_null));
}
