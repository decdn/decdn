use std::fs;

use decdn_node::cli::run::BlockchainArgs;
use decdn_node::config::{parse_contract_address, resolve_blockchain};
use tempfile::TempDir;

// vitalik.eth, known-good EIP-55 checksum
const GOOD: &str = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";

fn data_dir_with_keystore() -> anyhow::Result<TempDir> {
    let dir = TempDir::new()?;
    fs::write(dir.path().join("keystore.json"), "")?;
    Ok(dir)
}

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
    let dir = data_dir_with_keystore()?;
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
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
fn resolve_blockchain_names_correct_field_for_bad_payment_address() -> anyhow::Result<()> {
    let cli = BlockchainArgs {
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        payment_channel_address: Some("0xNOTHEX".to_string()),
        staking_registry_address: Some(GOOD.to_string()),
    };
    let dir = data_dir_with_keystore()?;
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected resolve_blockchain to fail on bad payment address");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("payment_channel_address"),
        "error should name payment_channel_address: {msg}"
    );
    assert!(
        !msg.contains("staking_registry_address"),
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

#[test]
fn resolve_blockchain_fails_when_keystore_missing() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cli = BlockchainArgs {
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        payment_channel_address: Some(GOOD.to_string()),
        staking_registry_address: Some(GOOD.to_string()),
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected resolve_blockchain to fail on missing keystore");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("invalid eth_keystore"),
        "error should name eth_keystore: {msg}"
    );
    assert!(
        msg.contains("cannot access"),
        "error should describe access failure: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_fails_when_cli_keystore_override_missing() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let bogus = dir.path().join("does-not-exist.json");
    let cli = BlockchainArgs {
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: Some(bogus.clone()),
        payment_channel_address: Some(GOOD.to_string()),
        staking_registry_address: Some(GOOD.to_string()),
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected resolve_blockchain to fail on bogus --eth-keystore");
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("invalid eth_keystore"), "{msg}");
    assert!(
        msg.contains("does-not-exist.json"),
        "error should cite the overridden path: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_directory_as_keystore() -> anyhow::Result<()> {
    // `File::open` accepts a directory on Linux, so the explicit `is_file`
    // check is the only thing standing between the node and a later panic.
    let dir = TempDir::new()?;
    fs::create_dir(dir.path().join("keystore.json"))?;
    let cli = BlockchainArgs {
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        payment_channel_address: Some(GOOD.to_string()),
        staking_registry_address: Some(GOOD.to_string()),
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected resolve_blockchain to fail on directory keystore");
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("invalid eth_keystore"), "{msg}");
    assert!(
        msg.contains("not a regular file"),
        "error should say 'not a regular file': {msg}"
    );
    Ok(())
}
