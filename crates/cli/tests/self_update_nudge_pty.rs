//! The once-a-day update notice, driven over a pty: it prints only when stderr
//! is a terminal, so an ordinary piped integration test never sees it.

#![cfg(unix)] // pty forking is POSIX-only.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::Path;

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TIMEOUT_MS: u64 = 10_000;
const NOTICE: &str = "decdn 99.0.0 is available; run `decdn self-update`";

/// A release page whose "latest" is v99.0.0.
async fn serve_latest() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/releases/latest/download/SHA256SUMS"))
        .respond_with(ResponseTemplate::new(302).insert_header(
            "location",
            format!("{}/releases/download/v99.0.0/SHA256SUMS", server.uri()).as_str(),
        ))
        .mount(&server)
        .await;
    server
}

/// Run `decdn` on a pty with the notice turned on and return its output.
/// `config init` is cheap, offline, and succeeds, which is what the notice
/// waits for.
fn run_on_pty(home: &Path, base: &str, args: &[&str], env: &[(&str, &str)]) -> String {
    let mut cmd = common::decdn_command(home);
    cmd.args(args)
        .env("DECDN_SELF_UPDATE_CHECK", "1")
        .env("DECDN_RELEASES_BASE", base);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut session = rexpect::session::spawn_command(cmd, Some(TIMEOUT_MS)).unwrap();
    session.exp_eof().unwrap()
}

async fn requests(server: &MockServer) -> usize {
    server.received_requests().await.unwrap().len()
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // one sequential scenario: each step depends on the stamp the last left
async fn notice_prints_once_a_day_after_a_successful_command() {
    let server = serve_latest().await;
    let home = tempfile::tempdir().unwrap();
    let home_path = home.path().to_path_buf();
    let base = server.uri();
    let stamp = home_path.join(".decdn").join("self-update-check");

    // First successful command on a terminal: one lookup, one notice line.
    let out = tokio::task::spawn_blocking({
        let (h, b) = (home_path.clone(), base.clone());
        move || {
            let cfg = h.join("one.toml");
            run_on_pty(
                &h,
                &b,
                &[
                    "config",
                    "init",
                    "--client",
                    "--output",
                    cfg.to_str().unwrap(),
                ],
                &[],
            )
        }
    })
    .await
    .unwrap();
    assert!(out.contains(NOTICE), "{out}");
    assert_eq!(requests(&server).await, 1);
    assert!(stamp.is_file());

    // The same day: no lookup, no notice.
    let out = tokio::task::spawn_blocking({
        let (h, b) = (home_path.clone(), base.clone());
        move || {
            let cfg = h.join("two.toml");
            run_on_pty(
                &h,
                &b,
                &[
                    "config",
                    "init",
                    "--client",
                    "--output",
                    cfg.to_str().unwrap(),
                ],
                &[],
            )
        }
    })
    .await
    .unwrap();
    assert!(!out.contains("is available"), "{out}");
    assert_eq!(requests(&server).await, 1);

    // A command that fails: no lookup even when a check is due.
    std::fs::remove_file(&stamp).unwrap();
    let out = tokio::task::spawn_blocking({
        let (h, b) = (home_path.clone(), base.clone());
        move || {
            let missing = h.join("missing.toml");
            run_on_pty(
                &h,
                &b,
                &["config", "validate", "--config", missing.to_str().unwrap()],
                &[],
            )
        }
    })
    .await
    .unwrap();
    assert!(!out.contains("is available"), "{out}");
    assert_eq!(requests(&server).await, 1);

    // The env var turns it off.
    let out = tokio::task::spawn_blocking({
        let (h, b) = (home_path.clone(), base.clone());
        move || {
            let cfg = h.join("three.toml");
            run_on_pty(
                &h,
                &b,
                &[
                    "config",
                    "init",
                    "--client",
                    "--output",
                    cfg.to_str().unwrap(),
                ],
                &[("DECDN_SELF_UPDATE_CHECK", "0")],
            )
        }
    })
    .await
    .unwrap();
    assert!(!out.contains("is available"), "{out}");
    assert_eq!(requests(&server).await, 1);
    assert!(!stamp.exists());

    // `[self_update] check = false` in the config file turns it off too; the
    // env var is left unset so the file decides.
    let config = home_path.join(".decdn").join("node.toml");
    std::fs::write(&config, "[self_update]\ncheck = false\n").unwrap();
    let out = tokio::task::spawn_blocking({
        let (h, b) = (home_path.clone(), base.clone());
        move || {
            let cfg = h.join("four.toml");
            let mut cmd = common::decdn_command(&h);
            cmd.args([
                "config",
                "init",
                "--client",
                "--output",
                cfg.to_str().unwrap(),
            ])
            .env_remove("DECDN_SELF_UPDATE_CHECK")
            .env("DECDN_RELEASES_BASE", &b);
            let mut session = rexpect::session::spawn_command(cmd, Some(TIMEOUT_MS)).unwrap();
            session.exp_eof().unwrap()
        }
    })
    .await
    .unwrap();
    assert!(!out.contains("is available"), "{out}");
    assert_eq!(requests(&server).await, 1);
}
