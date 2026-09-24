//! End-to-end `flow::pull` against a mock gateway and a stub `decdn` script
//! that records its arguments and exits with a chosen status: a shell script
//! on Unix, a `.cmd` batch file on Windows.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, B256};
use decdn_incentive::CapabilityGrant;
use decdn_sponsored::config::WrapperConfig;
use decdn_sponsored::flow;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const HASH: &str = "9194000d7b356650e6924a7746ec4afb0b705838b65913c0e46cba6b15af69e5";

fn token(expiry: u64) -> String {
    CapabilityGrant {
        pool_id: B256::repeat_byte(0x11),
        signer: Address::repeat_byte(0x22),
        spending_cap: 5_000_000,
        expiry,
        owner_signature: vec![0u8; 65],
    }
    .to_token()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A stub `decdn` that appends its argv to `<dir>/calls` and exits `code`.
#[cfg(unix)]
fn stub_decdn(dir: &Path, code: i32) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let bin = dir.join(format!("decdn-{code}"));
    let log = dir.join("calls");
    std::fs::write(
        &bin,
        format!(
            "#!/bin/sh\necho \"$@\" >> '{}'\nexit {code}\n",
            log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    bin
}

/// A stub `decdn` that appends its argv to `<dir>/calls` and exits `code`.
#[cfg(windows)]
fn stub_decdn(dir: &Path, code: i32) -> PathBuf {
    let bin = dir.join(format!("decdn-{code}.cmd"));
    let log = dir.join("calls");
    std::fs::write(
        &bin,
        format!(
            "@echo off\r\necho %*>> \"{}\"\r\nexit /b {code}\r\n",
            log.display()
        ),
    )
    .unwrap();
    bin
}

fn config(gateway: &str, decdn_bin: &Path, data_dir: &Path) -> WrapperConfig {
    WrapperConfig {
        gateway_base: gateway.to_string(),
        decdn_bin: decdn_bin.display().to_string(),
        data_dir: data_dir.to_path_buf(),
        rpc_url: "http://rpc.invalid".into(),
        payment_pool: Address::repeat_byte(0x01),
        capacity_bond: Some(Address::repeat_byte(0x02)),
        slash_judge: None,
        chain_id: 421_614,
    }
}

fn state_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("downloads").join(HASH)
}

async fn gateway_with_capability(expiry: u64, expected_calls: u64) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capability"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "token": token(expiry)
        })))
        .expect(expected_calls)
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn success_runs_bundle_pull_and_discards_state() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("sponsored");
    let gateway = gateway_with_capability(now() + 3600, 1).await;
    let cfg = config(&gateway.uri(), &stub_decdn(tmp.path(), 0), &data);

    flow::pull(&format!("b3:{HASH}"), &tmp.path().join("out"), &cfg)
        .await
        .unwrap();

    let calls = std::fs::read_to_string(tmp.path().join("calls")).unwrap();
    assert!(calls.starts_with(&format!("bundle pull --hash {HASH} -o ")));
    assert!(calls.contains("--capability-file"));
    assert!(calls.contains("--keystore-password-file"));
    assert!(!state_dir(&data).exists());
}

#[tokio::test]
async fn failure_keeps_state_and_rerun_reuses_capability() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("sponsored");
    let gateway = gateway_with_capability(now() + 3600, 1).await;

    let failing = config(&gateway.uri(), &stub_decdn(tmp.path(), 1), &data);
    let err = flow::pull(HASH, &tmp.path().join("out"), &failing)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Re-run the same command"));
    let key_before = std::fs::read(state_dir(&data).join("keystore.json")).unwrap();

    // The mock allows exactly one GET /capability, so this run must reuse
    // the saved capability and key rather than asking the gateway again.
    let ok = config(&gateway.uri(), &stub_decdn(tmp.path(), 0), &data);
    flow::pull(HASH, &tmp.path().join("out"), &ok)
        .await
        .unwrap();
    let calls = std::fs::read_to_string(tmp.path().join("calls")).unwrap();
    assert_eq!(calls.lines().count(), 2);
    assert!(!state_dir(&data).exists());
    assert!(!key_before.is_empty());
}

#[tokio::test]
async fn expired_capability_rotates_to_a_fresh_key() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("sponsored");

    // First run leaves state behind holding an already-expired capability.
    let stale = gateway_with_capability(now().saturating_sub(10), 1).await;
    let failing = config(&stale.uri(), &stub_decdn(tmp.path(), 1), &data);
    flow::pull(HASH, &tmp.path().join("out"), &failing)
        .await
        .unwrap_err();
    let old_key = std::fs::read(state_dir(&data).join("keystore.json")).unwrap();

    // Second run must throw the old key away and fetch a new capability.
    let fresh = gateway_with_capability(now() + 3600, 1).await;
    let failing_again = config(&fresh.uri(), &stub_decdn(tmp.path(), 1), &data);
    flow::pull(HASH, &tmp.path().join("out"), &failing_again)
        .await
        .unwrap_err();
    let new_key = std::fs::read(state_dir(&data).join("keystore.json")).unwrap();
    assert_ne!(old_key, new_key);
}
