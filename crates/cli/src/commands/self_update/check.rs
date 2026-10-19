//! The once-a-day notice that a newer signed release exists.
//!
//! After a command's own work succeeds, and at most once per day, the CLI
//! asks GitHub for the latest release tag and prints one line on stderr when
//! it is newer than this build. The check never runs for machine-read
//! (`--json`) or non-terminal output, is bounded by a short timeout, and is
//! silent on every failure. `[self_update] check = false` in the config file
//! or `DECDN_SELF_UPDATE_CHECK=0` turns it off.

use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use decdn_common::config::load_file_config;

use super::release::{
    Clients, Upgrade, decide, releases_base, resolve_latest, running_version, version_from_tag,
};

/// How long after one check the next one is due.
const INTERVAL: Duration = Duration::from_hours(24);

/// The whole-request bound on the lookup, so no command waits on it.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

/// The environment variable that overrides `[self_update] check`.
pub(crate) const ENV_CHECK: &str = "DECDN_SELF_UPDATE_CHECK";

/// What the finished command tells the notice about its output.
#[derive(Clone, Copy, Debug)]
pub struct NudgePolicy {
    /// stderr is a terminal.
    pub tty: bool,
    /// The command rendered JSON for a program to read.
    pub json: bool,
    /// The command is `self-update` itself.
    pub excluded: bool,
}

/// The stamp recording the last check, under the data directory.
pub(crate) fn stamp_path(data_dir: &Path) -> PathBuf {
    data_dir.join("self-update-check")
}

/// Whether a check is due at `now_secs` given the stamp's contents. A
/// missing, unreadable, or future-dated stamp means due.
pub(crate) fn is_due(stamp: Option<&str>, now_secs: u64) -> bool {
    let Some(last) = stamp.and_then(|s| s.trim().parse::<u64>().ok()) else {
        return true;
    };
    last > now_secs || now_secs - last >= INTERVAL.as_secs()
}

/// `1|true|yes|on` → `Some(true)`, `0|false|no|off` → `Some(false)`, case
/// folded and trimmed; anything else is ignored.
pub(crate) fn env_flag(raw: Option<&str>) -> Option<bool> {
    match raw?.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Resolve the knob: the env var wins, then `[self_update] check`, then on.
/// A config file that fails to parse (`Err`) turns the check off.
pub(crate) fn check_enabled(env: Option<bool>, file_check: Result<Option<bool>, ()>) -> bool {
    match (env, file_check) {
        (Some(v), _) => v,
        (None, Ok(v)) => v.unwrap_or(true),
        (None, Err(())) => false,
    }
}

/// Stamp the check at `now_secs`, look the latest release up, and return the
/// notice line when it is newer than this build. The stamp is written
/// first: a lookup that hangs or fails must not repeat on every command.
/// When the stamp cannot be written, nothing is looked up.
pub(crate) async fn notice(
    stamp: &Path,
    base: &str,
    timeout: Duration,
    now_secs: u64,
) -> Option<String> {
    write_stamp(stamp, now_secs).ok()?;
    let clients = Clients::build(timeout).ok()?;
    let tag = tokio::time::timeout(timeout, resolve_latest(&clients, base))
        .await
        .ok()?
        .ok()?;
    let latest = version_from_tag(&tag).ok()?;
    let running = running_version().ok()?;
    match decide(&running, &latest) {
        Upgrade::Newer => Some(format!(
            "decdn {latest} is available; run `decdn self-update`"
        )),
        Upgrade::UpToDate | Upgrade::Downgrade | Upgrade::PreRelease => None,
    }
}

fn write_stamp(stamp: &Path, now_secs: u64) -> std::io::Result<()> {
    if let Some(dir) = stamp.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut file = std::fs::File::create(stamp)?;
    writeln!(file, "{now_secs}")
}

/// Run the daily check for a command that just succeeded, printing the
/// notice on stderr when a newer release exists. Silent otherwise.
pub async fn maybe_nudge(policy: NudgePolicy, config_path: Option<&Path>) {
    if policy.excluded || policy.json || !policy.tty || !std::io::stderr().is_terminal() {
        return;
    }
    let env = env_flag(std::env::var(ENV_CHECK).ok().as_deref());
    let file_check = load_file_config(config_path)
        .map(|f| f.self_update.and_then(|s| s.check))
        .map_err(|_| ());
    if !check_enabled(env, file_check) {
        return;
    }
    let Some(data_dir) = decdn_common::cli::default_data_dir() else {
        return;
    };
    let stamp = stamp_path(&data_dir);
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    if !is_due(std::fs::read_to_string(&stamp).ok().as_deref(), now_secs) {
        return;
    }
    if let Some(line) = notice(&stamp, &releases_base(), LOOKUP_TIMEOUT, now_secs).await {
        eprintln!("{line}");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
