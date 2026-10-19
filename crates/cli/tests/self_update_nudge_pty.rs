//! The once-a-day update notice, driven over a pty: it prints only when stdout
//! and stderr are terminals, so an ordinary piped integration test never sees
//! it.

#![cfg(unix)] // pty forking is POSIX-only.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::{Path, PathBuf};

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

/// One `decdn` run on a pty with the notice turned on unless `env` says
/// otherwise. `config init` is cheap, offline, and succeeds, which is what
/// the notice waits for; `config validate` on a missing file fails.
struct Run {
    home: PathBuf,
    base: String,
}

impl Run {
    fn on_pty(&self, args: &[&str], env: &[(&str, &str)], unset_check: bool) -> String {
        let mut cmd = common::decdn_command(&self.home);
        cmd.args(args).env("DECDN_RELEASES_BASE", &self.base);
        if unset_check {
            cmd.env_remove("DECDN_SELF_UPDATE_CHECK");
        } else {
            cmd.env("DECDN_SELF_UPDATE_CHECK", "1");
        }
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut session = rexpect::session::spawn_command(cmd, Some(TIMEOUT_MS)).unwrap();
        session.exp_eof().unwrap()
    }

    fn init(&self, name: &str) -> String {
        let cfg = self.home.join(name);
        self.on_pty(
            &[
                "config",
                "init",
                "--client",
                "--output",
                cfg.to_str().unwrap(),
            ],
            &[],
            false,
        )
    }

    /// `decdn … | cat` on the pty: stdout a pipe, stderr the terminal.
    fn init_piped(&self, name: &str) -> String {
        let cfg = self.home.join(name);
        let mut cmd = std::process::Command::new("sh");
        cmd.arg("-c")
            .arg(format!(
                "{} config init --client --output {} | cat",
                env!("CARGO_BIN_EXE_decdn"),
                cfg.display()
            ))
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &self.home)
            .env("DECDN_SELF_UPDATE_CHECK", "1")
            .env("DECDN_RELEASES_BASE", &self.base);
        let mut session = rexpect::session::spawn_command(cmd, Some(TIMEOUT_MS)).unwrap();
        session.exp_eof().unwrap()
    }
}

async fn requests(server: &MockServer) -> usize {
    server.received_requests().await.unwrap().len()
}

fn reset(stamp: &Path) {
    let _ = std::fs::remove_file(stamp);
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // one sequential scenario per stamp state
async fn notice_prints_once_a_day_after_a_successful_command() {
    let server = serve_latest().await;
    let home = tempfile::tempdir().unwrap();
    let run = Run {
        home: home.path().to_path_buf(),
        base: server.uri(),
    };
    let stamp = run.home.join(".decdn").join("self-update-check");

    // First successful command on a terminal: one lookup, one notice line.
    let out = tokio::task::spawn_blocking({
        let run = Run {
            home: run.home.clone(),
            base: run.base.clone(),
        };
        move || run.init("one.toml")
    })
    .await
    .unwrap();
    assert!(out.contains(NOTICE), "{out}");
    assert_eq!(requests(&server).await, 1);
    assert!(stamp.is_file());

    // The same day: no lookup, no notice.
    let before = requests(&server).await;
    let out = tokio::task::spawn_blocking({
        let run = Run {
            home: run.home.clone(),
            base: run.base.clone(),
        };
        move || run.init("two.toml")
    })
    .await
    .unwrap();
    assert!(!out.contains("is available"), "{out}");
    assert_eq!(requests(&server).await, before);

    // A command that fails prints no notice, whatever the lookup did; the
    // lookup runs alongside the command, so it may or may not have asked.
    reset(&stamp);
    let before = requests(&server).await;
    let out = tokio::task::spawn_blocking({
        let run = Run {
            home: run.home.clone(),
            base: run.base.clone(),
        };
        move || {
            let missing = run.home.join("missing.toml");
            run.on_pty(
                &["config", "validate", "--config", missing.to_str().unwrap()],
                &[],
                false,
            )
        }
    })
    .await
    .unwrap();
    assert!(!out.contains("is available"), "{out}");
    assert!(requests(&server).await <= before + 1);

    // stdout piped, stderr still the terminal: no lookup and no notice, so a
    // pipeline never waits on GitHub and never gets a line it did not ask for.
    reset(&stamp);
    let before = requests(&server).await;
    let out = tokio::task::spawn_blocking({
        let run = Run {
            home: run.home.clone(),
            base: run.base.clone(),
        };
        move || run.init_piped("piped.toml")
    })
    .await
    .unwrap();
    assert!(out.contains("wrote"), "{out}");
    assert!(!out.contains("is available"), "{out}");
    assert_eq!(requests(&server).await, before);
    assert!(!stamp.exists());

    // The env var turns it off: no lookup, no stamp.
    reset(&stamp);
    let before = requests(&server).await;
    let out = tokio::task::spawn_blocking({
        let run = Run {
            home: run.home.clone(),
            base: run.base.clone(),
        };
        move || {
            let cfg = run.home.join("three.toml");
            run.on_pty(
                &[
                    "config",
                    "init",
                    "--client",
                    "--output",
                    cfg.to_str().unwrap(),
                ],
                &[("DECDN_SELF_UPDATE_CHECK", "0")],
                false,
            )
        }
    })
    .await
    .unwrap();
    assert!(!out.contains("is available"), "{out}");
    assert_eq!(requests(&server).await, before);
    assert!(!stamp.exists());

    // `[self_update] check = false` in the config file turns it off too; the
    // env var is left unset so the file decides.
    reset(&stamp);
    std::fs::write(
        run.home.join(".decdn").join("node.toml"),
        "[self_update]\ncheck = false\n",
    )
    .unwrap();
    let before = requests(&server).await;
    let out = tokio::task::spawn_blocking({
        let run = Run {
            home: run.home.clone(),
            base: run.base.clone(),
        };
        move || {
            let cfg = run.home.join("four.toml");
            run.on_pty(
                &[
                    "config",
                    "init",
                    "--client",
                    "--output",
                    cfg.to_str().unwrap(),
                ],
                &[],
                true,
            )
        }
    })
    .await
    .unwrap();
    assert!(!out.contains("is available"), "{out}");
    assert_eq!(requests(&server).await, before);
    assert!(!stamp.exists());
}
