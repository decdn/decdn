//! End-to-end `pull::pull` against a mock onramp and a stub `decdn` script
//! that records its arguments and exits with a chosen status: a shell script
//! on Unix, a `.cmd` batch file on Windows.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, B256};
use decdn_incentive::CapabilityGrant;
use decdn_sponsored::config::Config;
use decdn_sponsored::pull;
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

/// Expiry of a freshly issued capability: the onramp's 30-day default TTL.
fn fresh_expiry() -> u64 {
    now() + 30 * 86_400
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

fn config(onramp: &str, decdn_bin: &Path, data_dir: &Path) -> Config {
    Config {
        onramp_url: onramp.to_string(),
        decdn_bin: decdn_bin.display().to_string(),
        data_dir: data_dir.to_path_buf(),
    }
}

fn profile_json() -> serde_json::Value {
    serde_json::json!({
        "chain_id": 421_614,
        "rpc_url": "http://rpc.invalid",
        "payment_pool": Address::repeat_byte(0x01),
        "capacity_bond": Address::repeat_byte(0x02),
    })
}

async fn mount_profile(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/v1/profile"))
        .respond_with(ResponseTemplate::new(200).set_body_json(profile_json()))
        .mount(server)
        .await;
}

fn state_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("downloads").join(HASH)
}

async fn onramp_with_capability(expiry: u64, expected_calls: u64) -> MockServer {
    let server = MockServer::start().await;
    mount_profile(&server).await;
    Mock::given(method("GET"))
        .and(path("/v1/capability"))
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
    let onramp = onramp_with_capability(fresh_expiry(), 1).await;
    let cfg = config(&onramp.uri(), &stub_decdn(tmp.path(), 0), &data);

    pull::pull(
        &format!("b3:{HASH}"),
        &tmp.path().join("out"),
        NonZeroU64::new(1),
        &cfg,
    )
    .await
    .unwrap();

    let calls = std::fs::read_to_string(tmp.path().join("calls")).unwrap();
    assert!(calls.starts_with(&format!("bundle pull --hash {HASH} -o ")));
    assert!(calls.contains("--capability-file"));
    assert!(calls.contains("--keystore-password-file"));
    assert!(calls.contains("--namespace 1"));
    assert!(
        calls.contains("--chain-id 421614"),
        "chain from /v1/profile"
    );
    assert!(calls.contains(&format!(
        "--payment-pool-address {}",
        Address::repeat_byte(0x01)
    )));
    // Windows quotes path arguments to a `.cmd` stub, and its echo keeps them.
    let unquoted = calls.replace('"', "");
    assert!(
        unquoted.contains(&format!("--data-dir {}", data.join("decdn").display())),
        "{calls}"
    );
    assert!(
        unquoted.contains(&format!("--keystore {}", state_dir(&data).display())),
        "{calls}"
    );
    assert!(!state_dir(&data).exists());
    assert!(
        data.join("decdn").is_dir(),
        "the shared data dir outlives it"
    );
}

#[tokio::test]
async fn failure_keeps_state_and_rerun_reuses_capability() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("sponsored");
    let onramp = onramp_with_capability(fresh_expiry(), 1).await;

    let failing = config(&onramp.uri(), &stub_decdn(tmp.path(), 1), &data);
    let err = pull::pull(HASH, &tmp.path().join("out"), None, &failing)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Re-run the same command"));
    let key_before = std::fs::read(state_dir(&data).join("keystore.json")).unwrap();
    assert!(!key_before.is_empty(), "the failed run left its key behind");

    // The mock allows exactly one GET /v1/capability, so this run must reuse
    // the saved capability and key rather than asking the onramp again.
    let ok = config(&onramp.uri(), &stub_decdn(tmp.path(), 0), &data);
    pull::pull(HASH, &tmp.path().join("out"), None, &ok)
        .await
        .unwrap();
    let calls = std::fs::read_to_string(tmp.path().join("calls")).unwrap();
    assert_eq!(calls.lines().count(), 2);
    assert!(!state_dir(&data).exists());
}

#[tokio::test]
async fn expired_capability_rotates_to_a_fresh_key() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("sponsored");

    // First run leaves state behind holding an already-expired capability.
    let stale = onramp_with_capability(now().saturating_sub(10), 1).await;
    let failing = config(&stale.uri(), &stub_decdn(tmp.path(), 1), &data);
    pull::pull(HASH, &tmp.path().join("out"), None, &failing)
        .await
        .unwrap_err();
    let old_key = std::fs::read(state_dir(&data).join("keystore.json")).unwrap();

    // Second run must throw the old key away and fetch a new capability.
    let fresh = onramp_with_capability(fresh_expiry(), 1).await;
    let failing_again = config(&fresh.uri(), &stub_decdn(tmp.path(), 1), &data);
    pull::pull(HASH, &tmp.path().join("out"), None, &failing_again)
        .await
        .unwrap_err();
    let new_key = std::fs::read(state_dir(&data).join("keystore.json")).unwrap();
    assert_ne!(old_key, new_key);
}

#[tokio::test]
async fn capability_inside_node_margin_rotates_to_a_fresh_key() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("sponsored");

    // Still unexpired, but a default node already refuses vouchers under it.
    let near = onramp_with_capability(now() + pull::NODE_EXPIRY_MARGIN_SECS - 60, 1).await;
    let failing = config(&near.uri(), &stub_decdn(tmp.path(), 1), &data);
    pull::pull(HASH, &tmp.path().join("out"), None, &failing)
        .await
        .unwrap_err();
    let old_key = std::fs::read(state_dir(&data).join("keystore.json")).unwrap();

    // The re-run must not reuse it: a new key asks the onramp again.
    let fresh = onramp_with_capability(fresh_expiry(), 1).await;
    let failing_again = config(&fresh.uri(), &stub_decdn(tmp.path(), 1), &data);
    pull::pull(HASH, &tmp.path().join("out"), None, &failing_again)
        .await
        .unwrap_err();
    let new_key = std::fs::read(state_dir(&data).join("keystore.json")).unwrap();
    assert_ne!(old_key, new_key);
}

#[tokio::test]
async fn a_rerun_resumes_with_the_saved_profile_when_the_onramp_is_down() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("sponsored");
    let onramp = onramp_with_capability(fresh_expiry(), 1).await;
    let failing = config(&onramp.uri(), &stub_decdn(tmp.path(), 1), &data);
    pull::pull(HASH, &tmp.path().join("out"), None, &failing)
        .await
        .unwrap_err();

    // The onramp is gone; the saved capability and profile carry the re-run.
    let down = config("http://127.0.0.1:1", &stub_decdn(tmp.path(), 0), &data);
    pull::pull(HASH, &tmp.path().join("out"), None, &down)
        .await
        .unwrap();
    let calls = std::fs::read_to_string(tmp.path().join("calls")).unwrap();
    assert_eq!(calls.lines().count(), 2);
}

#[tokio::test]
async fn an_onramp_needing_a_newer_cli_is_refused_before_any_key_is_made() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("sponsored");
    let server = MockServer::start().await;
    let mut profile = profile_json();
    profile["min_cli_version"] = "999.0.0".into();
    Mock::given(method("GET"))
        .and(path("/v1/profile"))
        .respond_with(ResponseTemplate::new(200).set_body_json(profile))
        .mount(&server)
        .await;
    let cfg = config(&server.uri(), &stub_decdn(tmp.path(), 0), &data);
    let err = pull::pull(HASH, &tmp.path().join("out"), None, &cfg)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("re-run its installer"), "{err}");
    assert!(!state_dir(&data).join("keystore.json").exists());
}
