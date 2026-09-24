//! The `decdn-node` binary with `log_format = "json"` writes the node's
//! `node_id` as a top-level key on every log event, from the first one (ADR
//! appendix-observability § Structured Logging).
//!
//! The unit tests in `commands` pin the formatter. This test pins the wiring:
//! the real daemon start path, with the real key on disk. The node needs no
//! chain: the RPC URL points at a closed port, and the test stops the daemon
//! once it logs "loaded node identity", which comes before the RPC preflight
//! gives up.
//!
//! Gated `#[cfg(unix)]`: the data dir and keystore need owner-only modes.

#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::Context;

/// How long one daemon start may take to log its node identity.
const IDENTITY_LOG_TIMEOUT: Duration = Duration::from_secs(30);

/// The message `commands::run` logs once the subscriber and the key exist.
const LOADED: &str = "loaded node identity";

/// The message `commands::run` logs when the start wrote a new key.
const GENERATED: &str = "generated new node secret key";

/// Placeholder contract address: config resolution needs one, and the node
/// stops before it reads any contract.
const ADDR: &str = "0x0000000000000000000000000000000000000001";

/// Kills the daemon when the test ends, on every path.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Start the daemon against `root`, and return its stdout lines up to and
/// including the "loaded node identity" event, parsed as JSON.
fn run_until_identity(root: &Path) -> anyhow::Result<Vec<serde_json::Value>> {
    let data_dir = root.join("data");
    let mut child = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_decdn-node"))
            .arg("--config")
            .arg(root.join("node.toml"))
            .arg("run")
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--cache-dir")
            .arg(root.join("cache"))
            .args(["--rpc-url", "http://127.0.0.1:1"])
            .args(["--log-format", "json", "--log-level", "info"])
            .args(["--payment-pool-address", ADDR])
            .args(["--capacity-bond-address", ADDR])
            .args(["--slash-judge-address", ADDR])
            .args(["--content-blacklist-address", ADDR])
            .env("HOME", root)
            .env_remove("RUST_LOG")
            .env_remove("OTEL_RESOURCE_ATTRIBUTES")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn decdn-node")?,
    );
    let stdout = child.0.stdout.take().context("no daemon stdout")?;

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    let deadline = Instant::now() + IDENTITY_LOG_TIMEOUT;
    let mut events = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let line = rx
            .recv_timeout(left)
            .with_context(|| format!("no {LOADED:?} line; got {events:?}"))??;
        let event: serde_json::Value =
            serde_json::from_str(&line).with_context(|| format!("not JSON: {line}"))?;
        let done = message(&event) == Some(LOADED);
        events.push(event);
        if done {
            return Ok(events);
        }
    }
}

/// An event's `fields.message`.
fn message(event: &serde_json::Value) -> Option<&str> {
    event
        .pointer("/fields/message")
        .and_then(serde_json::Value::as_str)
}

/// The position of the event with `msg`, if any.
fn position(events: &[serde_json::Value], msg: &str) -> Option<usize> {
    events.iter().position(|e| message(e) == Some(msg))
}

/// Every event carries the on-disk key's id at the top level. A first start
/// logs "generated" before "loaded". A second start on the same data dir
/// reads the same key and logs no "generated" line.
#[test]
fn every_json_event_carries_the_on_disk_node_id() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let data_dir = root.path().join("data");
    std::fs::create_dir(&data_dir)?;
    std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o700))?;
    // Config resolution checks that the keystore file exists. The node stops
    // before it decrypts it.
    let keystore = data_dir.join("keystore.json");
    std::fs::write(&keystore, "{}")?;
    std::fs::set_permissions(&keystore, std::fs::Permissions::from_mode(0o600))?;
    std::fs::write(root.path().join("node.toml"), "")?;

    let first = run_until_identity(root.path())?;
    let second = run_until_identity(root.path())?;
    let node_id = decdn_common::identity::load(&data_dir)?
        .public()
        .to_string();

    for events in [&first, &second] {
        for event in events {
            anyhow::ensure!(
                event
                    .pointer("/node_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(node_id.as_str()),
                "event without the on-disk node_id {node_id}: {event}"
            );
        }
    }

    let generated = position(&first, GENERATED)
        .with_context(|| format!("first start logged no {GENERATED:?}: {first:?}"))?;
    let loaded = position(&first, LOADED).context("no loaded line")?;
    anyhow::ensure!(
        generated < loaded,
        "{GENERATED:?} at {generated}, after {LOADED:?} at {loaded}"
    );

    anyhow::ensure!(
        position(&second, GENERATED).is_none(),
        "second start generated a key again: {second:?}"
    );
    Ok(())
}
