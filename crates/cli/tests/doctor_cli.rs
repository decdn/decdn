//! CLI-level checks for `decdn node doctor`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::process::Command;

fn decdn() -> Command {
    // CARGO_BIN_EXE_<name> is set by cargo for integration tests.
    Command::new(env!("CARGO_BIN_EXE_decdn"))
}

#[test]
fn offline_json_report_is_wellformed() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("node.toml");
    // A minimal but resolvable config (checksummed placeholder addresses
    // reused from `config_validate.rs`, which already exercises them
    // against the same resolver). `--data-dir` points the resolver at the
    // tempdir; the defaulted `blockchain.eth_keystore` then lands at
    // `<data_dir>/keystore.json`, which must exist and be readable.
    std::fs::write(&cfg, MINIMAL_CONFIG).unwrap();
    std::fs::write(dir.path().join("keystore.json"), "").unwrap();

    let out = decdn()
        .args([
            "--config",
            cfg.to_str().unwrap(),
            "node",
            "doctor",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--offline",
            "--json",
        ])
        .output()
        .unwrap();

    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(v["findings"].is_array());
    assert!(v["summary"]["pass"].as_u64().is_some());

    // The offline run resolves config against a real (empty) data dir, so
    // the config group itself must pass — a `config.resolves` failure would
    // mean the fixture is wrong, not that the doctor command is broken.
    let config_finding = v["findings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["id"] == "config.resolves")
        .expect("doctor always emits a config.resolves finding");
    assert_eq!(config_finding["severity"], "pass");

    // Exit code reflects fail count.
    let fail = v["summary"]["fail"].as_u64().unwrap();
    if fail == 0 {
        assert!(out.status.success());
    } else {
        assert!(!out.status.success());
    }
}

const MINIMAL_CONFIG: &str = r#"
[blockchain]
rpc_url = "http://127.0.0.1:8545"
payment_pool_address = "0x0000000000000000000000000000000000000001"
capacity_bond_address = "0x0000000000000000000000000000000000000002"
slash_judge_address = "0x0000000000000000000000000000000000000003"
content_blacklist_address = "0x0000000000000000000000000000000000000004"

[cache]
cache_size_mb = 1024
"#;
