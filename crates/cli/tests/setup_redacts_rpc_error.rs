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
    let host_port = format!("127.0.0.1:{}", refused_port());
    let rpc_url = format!("http://{host_port}/v3/{FAKE_KEY}");

    let output = Command::new(env!("CARGO_BIN_EXE_decdn"))
        // Pin `$HOME` at the temp dir so default config resolution
        // (`~/.decdn/node.toml`) can't reach a real config on a developer
        // machine. Without this, a present `~/.decdn/node.toml` with a custom
        // `eth_keystore` makes `setup --dry-run` abort on the keystore check
        // before it ever reaches the RPC path this test exercises — passing in
        // CI (clean HOME) but failing locally.
        .env("HOME", data_dir.path())
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

    // The secret — and the whole URL (including its actual host:port) — must
    // not appear in either stream.
    for (name, stream) in [("stdout", &stdout), ("stderr", &stderr)] {
        assert!(
            !stream.contains(FAKE_KEY),
            "rpc_url secret leaked on {name}: {stream}"
        );
        assert!(
            !stream.contains(&host_port) && !stream.contains(&*rpc_url),
            "rpc_url host:port leaked on {name}: {stream}"
        );
    }

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
