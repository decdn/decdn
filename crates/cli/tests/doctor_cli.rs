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

/// The whole notice path, end to end through the real binary: a resolver
/// records it, `resolve_config` hands it back, `check_config` turns it into a
/// finding, and it lands in the JSON report. The unit tests either side of this
/// cover the resolver and the renderer; nothing else covers the wiring, which
/// is where #1902 went wrong in the first place.
///
/// `--strict` is what makes a notice change the exit status, so both codes are
/// pinned here: an unbounded bookkeeping map is a warning, not a failure.
#[test]
fn a_resolve_notice_becomes_a_doctor_finding() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("node.toml");
    // `0` is the documented "unbounded" escape hatch — it resolves fine and
    // records a `Warn` notice, which is exactly the shape doctor exists to
    // surface: invisible in the resolved values themselves.
    std::fs::write(
        &cfg,
        format!("{MINIMAL_CONFIG}\n[security]\nmax_tracked_sources = 0\n"),
    )
    .unwrap();
    std::fs::write(dir.path().join("keystore.json"), "").unwrap();

    let run = |strict: bool| {
        let mut cmd = decdn();
        cmd.args([
            "--config",
            cfg.to_str().unwrap(),
            "node",
            "doctor",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--offline",
            "--json",
        ]);
        if strict {
            cmd.arg("--strict");
        }
        cmd.output().unwrap()
    };

    let out = run(false);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let notice = v["findings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["id"] == "config.notice")
        .expect("the zeroed bookkeeping cap must reach the report");
    assert_eq!(notice["severity"], "warn");
    assert!(
        notice["title"].as_str().unwrap().contains("unbounded"),
        "{notice}"
    );
    assert_eq!(notice["detail"], "field=security.max_tracked_sources");
    assert!(notice["remediation"].as_str().is_some(), "{notice}");

    // A warning alone is not a failure.
    assert!(out.status.success(), "a Warn must not fail a plain run");
    assert!(
        !run(true).status.success(),
        "--strict is what turns a Warn into a nonzero exit"
    );
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
