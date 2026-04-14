use decdn_node::cli::run::BlockchainArgs;
use decdn_node::config::{parse_contract_address, resolve_blockchain};

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
fn rejects_missing_0x_prefix() -> anyhow::Result<()> {
    let s = GOOD
        .get(2..)
        .ok_or_else(|| anyhow::anyhow!("GOOD constant shorter than expected"))?;
    assert!(parse_contract_address("x", s).is_err());
    Ok(())
}

#[test]
fn rejects_wrong_length() {
    assert!(parse_contract_address("x", "0xabc").is_err());
    assert!(parse_contract_address("x", &format!("{GOOD}00")).is_err());
}

#[test]
fn rejects_empty_and_bare_prefix() {
    assert!(parse_contract_address("x", "").is_err());
    assert!(parse_contract_address("x", "0x").is_err());
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

#[test]
fn resolve_blockchain_names_correct_field_for_bad_address() -> anyhow::Result<()> {
    let cli = BlockchainArgs {
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        payment_channel_address: Some(GOOD.to_string()),
        staking_registry_address: Some("0xNOTHEX".to_string()),
    };
    let data_dir = std::env::temp_dir();
    let Err(err) = resolve_blockchain(&cli, None, &data_dir) else {
        anyhow::bail!("expected resolve_blockchain to fail on bad staking address");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("staking_registry_address"),
        "error should name staking_registry_address: {msg}"
    );
    assert!(
        !msg.contains("payment_channel_address"),
        "error must not name the valid field: {msg}"
    );
    Ok(())
}

#[test]
fn error_message_names_field_and_format() -> anyhow::Result<()> {
    let lower = GOOD.to_lowercase();
    let Err(err) = parse_contract_address("payment_channel_address", &lower) else {
        anyhow::bail!("expected parse_contract_address to fail on lowercase input");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("invalid payment_channel_address"),
        "missing flag name: {msg}"
    );
    assert!(msg.contains("EIP-55"), "missing format hint: {msg}");
    Ok(())
}
