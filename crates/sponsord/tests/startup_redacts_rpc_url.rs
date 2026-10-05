//! A `sponsord` that cannot reach its RPC endpoint exits with an error that
//! names the failed read but not the RPC URL, whose path carries the
//! provider's API key (#38).
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use decdn_incentive::eth_identity;

/// Boot the binary against a closed port with an API key in the RPC URL's
/// path, `RUST_LOG` set to `rust_log`, and wait for it to exit. Returns the
/// port, stdout and stderr.
fn boot_against_a_closed_port(rust_log: Option<&str>) -> (u16, String, String) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    eth_identity::generate_and_persist(dir.path(), "pw", false).unwrap();
    let keystore = eth_identity::keystore_path(dir.path());

    // Bound and dropped: nothing listens on the port, so the boot read is
    // refused.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_sponsord"));
    cmd.env_clear()
        .env("SPONSORD_BIND", "127.0.0.1:0")
        .env("SPONSORD_API_TOKEN", "0123456789abcdef0123456789abcdef")
        .env(
            "SPONSORD_RPC_URL",
            format!("http://127.0.0.1:{port}/v3/SECRET-API-KEY"),
        )
        .env(
            "SPONSORD_PAYMENT_POOL_ADDR",
            "0x2222222222222222222222222222222222222222",
        )
        .env("SPONSORD_POOL_ID", format!("0x{}", "11".repeat(32)))
        .env("SPONSORD_TREASURY_KEYSTORE", &keystore)
        .env("SPONSORD_TREASURY_PASSWORD", "pw")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(filter) = rust_log {
        cmd.env("RUST_LOG", filter);
    }
    let mut child = cmd.spawn().unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("sponsord did not exit");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(!out.status.success(), "{stderr}");
    (
        port,
        String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr,
    )
}

#[test]
fn an_unreachable_rpc_url_stays_out_of_the_exit_error() {
    let (port, stdout, stderr) = boot_against_a_closed_port(None);
    assert!(stderr.starts_with("Error: "), "{stderr}");
    assert!(stderr.contains("usdc()"), "{stderr}");
    assert!(
        stderr.contains("tcp connect error"),
        "the cause is kept: {stderr}"
    );
    for text in [&stdout, &stderr] {
        assert!(!text.contains("SECRET-API-KEY"), "{text}");
        assert!(!text.contains(&format!(":{port}")), "{text}");
    }
}

/// alloy records the URL on a debug span around every request, which the
/// events inside it (reqwest's, hyper's) would print. At trace, hyper still
/// names the host and port it connects to, but nothing names the path.
#[test]
fn trace_logging_does_not_print_the_rpc_url() {
    for filter in ["trace", "alloy_transport_http=trace,trace"] {
        let (_, stdout, stderr) = boot_against_a_closed_port(Some(filter));
        assert!(stdout.contains("TRACE"), "{filter} logs at trace: {stdout}");
        for text in [&stdout, &stderr] {
            assert!(!text.contains("SECRET-API-KEY"), "{filter}: {text}");
        }
    }
}
