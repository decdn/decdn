//! The `sponsord-onramp` binary boots far enough to build its HTTP client and
//! read the daemon. The workspace builds reqwest with `rustls-no-provider`, so
//! a `main` that built a client before installing the provider would panic on
//! every start; this test runs the real binary, not the library.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn the_binary_reaches_the_daemon_read_and_names_an_unreachable_daemon() {
    // Bound and dropped: nothing listens on the port, so the boot read is
    // refused and the binary exits with that error.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let daemon_url = format!("http://127.0.0.1:{port}");
    let mut child = Command::new(env!("CARGO_BIN_EXE_sponsord-onramp"))
        .env_clear()
        .env("ONRAMP_BIND", "127.0.0.1:0")
        .env("ONRAMP_PUBLIC_URL", "https://up.example.org")
        .env("ONRAMP_DAEMON_URL", &daemon_url)
        .env("ONRAMP_DAEMON_TOKEN", "0123456789abcdef0123456789abcdef")
        .env("ONRAMP_RPC_URL", "https://rpc.example.org/")
        .env(
            "ONRAMP_CAPACITY_BOND_ADDR",
            "0x2222222222222222222222222222222222222222",
        )
        .env("ONRAMP_TURNSTILE_SITEKEY", "KEY")
        .env("ONRAMP_TURNSTILE_SECRET", "secret")
        .env("ONRAMP_DECDN_RELEASE", "v0.2.0")
        .env("ONRAMP_DECDN_SUMS_SHA256", "ab".repeat(32))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_mins(1);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("sponsord-onramp did not exit");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(
        stderr.contains(&daemon_url),
        "the daemon read is reached and its error names the daemon: {stderr}"
    );
    for marker in ["panicked", "CryptoProvider", "rustls"] {
        assert!(!stderr.contains(marker), "{marker}: {stderr}");
    }
}
