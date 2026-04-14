use decdn_node::config::parse_contract_address;

// vitalik.eth, known-good EIP-55 checksum
const GOOD: &str = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";

#[test]
fn accepts_checksummed_address() -> anyhow::Result<()> {
    let out = parse_contract_address("x", GOOD)?;
    assert_eq!(out, GOOD);
    Ok(())
}

#[test]
fn trims_whitespace() -> anyhow::Result<()> {
    let padded = format!("  {GOOD}\n");
    let out = parse_contract_address("x", &padded)?;
    assert_eq!(out, GOOD);
    Ok(())
}

#[test]
fn rejects_missing_0x_prefix() {
    let s = GOOD.get(2..).unwrap_or_default();
    assert!(parse_contract_address("x", s).is_err());
}

#[test]
fn rejects_wrong_length() {
    assert!(parse_contract_address("x", "0xabc").is_err());
    assert!(parse_contract_address("x", &format!("{GOOD}00")).is_err());
}

#[test]
fn rejects_all_lowercase() {
    let lower = GOOD.to_lowercase();
    assert!(parse_contract_address("x", &lower).is_err());
}

#[test]
fn rejects_bad_checksum() {
    let mut bad = String::from(GOOD);
    bad.replace_range(2..3, "D");
    assert!(parse_contract_address("x", &bad).is_err());
}

#[test]
fn rejects_non_hex() {
    let bad = "0xZZZZ6BF26964aF9D7eEd9e03E53415D37aA96045";
    assert!(parse_contract_address("x", bad).is_err());
}
