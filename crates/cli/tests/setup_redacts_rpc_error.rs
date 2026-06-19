//! Regression test for issue #954: a chain-RPC transport failure must never
//! echo the configured `rpc_url` (Infura/Alchemy-style API keys live in its
//! path/query) to operator output.
//!
//! Drives the real `decdn` binary so the `main()` sanitizer boundary
//! (`crates/cli/src/main.rs`, [`decdn_common::redact::sanitize_err_chain`]) is
//! exercised end-to-end: `setup --dry-run` on a clean slate falls into
//! `dry_run_without_keys`, whose first chain read (`minCapacityMbps`) fails
//! against an unreachable endpoint. The underlying `alloy`/`reqwest` error
//! carries the URL through the `anyhow` chain until `main` strips it.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::TcpListener;
use std::process::Command;

use tempfile::TempDir;

/// Embedded where an Infura/Alchemy project key would sit (URL path segment).
const FAKE_KEY: &str = "SECRETKEY123abcXYZ";

/// Bind then immediately drop an ephemeral loopback port. The OS just confirmed
/// it free, so a connect to it refuses fast and deterministically — avoiding the
/// privileged-port-1 hang risk in restrictive CI sandboxes.
fn refused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

#[test]
fn setup_dry_run_does_not_leak_rpc_url_on_unreachable_endpoint() {
    // Empty data dir → no node key / keystore yet → the `--dry-run` clean-slate
    // path (`dry_run_without_keys`), which needs no keystore.
    let data_dir = TempDir::new().expect("temp data dir");
    // Unreachable endpoint; the secret sits in the path where an
    // Infura/Alchemy key would live.
    let rpc_url = format!("http://127.0.0.1:{}/v3/{FAKE_KEY}", refused_port());

    let output = Command::new(env!("CARGO_BIN_EXE_decdn"))
        .args(["setup", "--mbps", "100", "--region", "US", "--dry-run"])
        .arg("--rpc-url")
        .arg(&rpc_url)
        .args([
            "--capacity-bond-address",
            "0x0000000000000000000000000000000000000002",
        ])
        .arg("--data-dir")
        .arg(data_dir.path())
        .output()
        .expect("run decdn binary");

    assert!(
        !output.status.success(),
        "expected `setup --dry-run` to fail against an unreachable RPC"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // The secret — and the whole URL — must not appear in either stream.
    assert!(
        !stdout.contains(FAKE_KEY) && !stderr.contains(FAKE_KEY),
        "rpc_url secret leaked.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        !stdout.contains("127.0.0.1:1") && !stderr.contains("127.0.0.1:1"),
        "rpc_url host:port leaked.\nstdout: {stdout}\nstderr: {stderr}"
    );

    // Positive checks: the error went through the new `main()` sanitizer
    // boundary (the `Error:` prefix), and the failure is still actionable —
    // sanitizing strips the URL, not the context naming which read failed.
    assert!(
        stderr.contains("Error:"),
        "expected the sanitized `main()` error prefix.\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("minCapacityMbps") || stderr.contains("RPC"),
        "sanitized error dropped the actionable context.\nstderr: {stderr}"
    );
}
