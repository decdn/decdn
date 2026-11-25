//! A bad `network.external_addrs` entry stops `decdn node register` before the
//! password prompt or any chain read, and stops `decdn setup` on a node with
//! no key before key generation or any chain read (ADR 019 § Phase 2). Setup
//! on a node with a key checks the entries after it reads the registration
//! state; `crates/e2e/tests/cli_setup_external_addrs.rs` covers that.
//!
//! Drives the real `decdn` binary against a refused RPC port: a command that
//! reached the chain would fail with an RPC error instead of naming the entry.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Output;

use tempfile::TempDir;

/// Bind then drop an ephemeral loopback port, so a connect to it refuses fast.
fn refused_rpc_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    format!("http://127.0.0.1:{port}")
}

fn write_config(dir: &Path, external_addrs: &str) -> PathBuf {
    let path = dir.join("node.toml");
    std::fs::write(
        &path,
        format!("[network]\nexternal_addrs = {external_addrs}\n"),
    )
    .expect("write config");
    path
}

fn run(data_dir: &Path, config: &Path, args: &[&str]) -> Output {
    common::decdn_command(data_dir)
        .args(args)
        .arg("--config")
        .arg(config)
        .arg("--rpc-url")
        .arg(refused_rpc_url())
        .args([
            "--capacity-bond-address",
            "0x0000000000000000000000000000000000000002",
        ])
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run decdn binary")
}

#[test]
fn setup_refuses_a_private_external_addr_before_generating_keys() {
    let data_dir = TempDir::new().expect("temp data dir");
    let config = write_config(data_dir.path(), r#"["10.0.0.5:4433"]"#);

    let output = run(
        data_dir.path(),
        &config,
        &["setup", "--mbps", "100", "--region", "US"],
    );

    assert!(
        !output.status.success(),
        "setup must fail on a private entry"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("network.external_addrs[0]"), "{stderr}");
    assert!(stderr.contains("10.0.0.5"), "{stderr}");
    assert!(
        !stderr.contains("RPC reachable?"),
        "no chain read happened: {stderr}"
    );
    assert!(
        !decdn_common::identity::key_path(data_dir.path()).exists(),
        "no node key is generated before the check"
    );
    assert!(
        !decdn_incentive::eth_identity::keystore_path(data_dir.path()).exists(),
        "no eth keystore is created before the check"
    );
}

#[test]
fn setup_with_explicit_multiaddr_skips_the_external_addrs_check() {
    let data_dir = TempDir::new().expect("temp data dir");
    let config = write_config(data_dir.path(), r#"["10.0.0.5:4433"]"#);

    let output = run(
        data_dir.path(),
        &config,
        &[
            "setup",
            "--mbps",
            "100",
            "--region",
            "US",
            "--dry-run",
            "--multiaddr",
            "/ip4/8.8.8.8/udp/4433/quic-v1",
        ],
    );

    assert!(
        !output.status.success(),
        "the refused RPC still fails setup"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("network.external_addrs"),
        "--multiaddr skips the key: {stderr}"
    );
    assert!(
        stderr.contains("RPC reachable?"),
        "the run got past the check to the chain read: {stderr}"
    );
}

#[test]
fn register_refuses_a_private_external_addr_before_loading_the_signer() {
    let data_dir = TempDir::new().expect("temp data dir");
    let config = write_config(data_dir.path(), r#"["10.0.0.5:4433"]"#);

    let output = run(
        data_dir.path(),
        &config,
        &["node", "register", "--region", "US"],
    );

    assert!(
        !output.status.success(),
        "register must fail on a private entry"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("network.external_addrs[0]"), "{stderr}");
    assert!(
        !stderr.to_lowercase().contains("keystore"),
        "the entry fails before the signer loads: {stderr}"
    );
}
