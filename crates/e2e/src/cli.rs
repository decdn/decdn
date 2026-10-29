//! Spawning the user-facing `decdn` CLI as a subprocess, hermetically.
//!
//! The counterpart to [`crate::node`]'s daemon spawning. See [`decdn_command`]
//! for why journeys must not hand-roll their own [`std::process::Command`].

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use alloy::primitives::{Address, U256};
use anyhow::Context;
use decdn_bao_range::align_range;
use serde_json::Value;

/// The `RUST_LOG` a journey sets on one `decdn` invocation whose stderr its
/// money assertions print on failure. The paid-pull client at `debug` logs every
/// leg, rejection, heal and resume behind a billed figure, so a load-dependent
/// under-pay or double-pay arrives with the trail that produced it. Set it with
/// `Command::env("RUST_LOG", CLIENT_DEBUG_LOG)` on that one invocation: it
/// overrides the [`decdn_command`] default for that child alone.
pub const CLIENT_DEBUG_LOG: &str = "info,decdn_client=debug";

/// Assert the `decdn` CLI binary has been built, so a journey fails with an
/// actionable message before it spends a minute launching anvil.
///
/// Deliberately returns no path: the binary location is private so a test
/// cannot spawn the CLI except through [`decdn_command`], which is what keeps
/// the `HOME` isolation below unforgettable (#1332).
pub fn ensure_decdn_cli_built() -> anyhow::Result<()> {
    decdn_cli_bin().map(|_| ())
}

/// Locate the built `decdn` CLI binary relative to the current test executable
/// (`target/<profile>/decdn`), falling back to `DECDN_CLI_BIN`. The sibling of
/// [`crate::node`]'s private `decdn_node_bin`.
fn decdn_cli_bin() -> anyhow::Result<PathBuf> {
    let overridden = std::env::var_os("DECDN_CLI_BIN");
    let bin = if let Some(p) = &overridden {
        // Expand `~` as the production CLI does for every user-supplied path
        // (`cli::common::expand_tilde`): a quoted `~/…` in a shell rc reaches
        // us literally, since only unquoted tildes are expanded by the shell.
        decdn_common::cli::common::expand_tilde(Path::new(p))
    } else {
        let exe = std::env::current_exe().context("current_exe")?;
        // .../target/<profile>/deps/<test-bin>  → .../target/<profile>/decdn
        let profile_dir = exe
            .parent()
            .and_then(|deps| deps.parent())
            .context("resolve target profile dir")?;
        profile_dir.join(if cfg!(windows) { "decdn.exe" } else { "decdn" })
    };
    // Check both branches: an unvalidated `DECDN_CLI_BIN` would defer a stale
    // path to a bare "No such file or directory" at spawn time, naming neither
    // the variable nor the path tried. Report it absolute — a relative override
    // resolves against the test process's cwd (the *crate* root, not the
    // workspace root), so the bare string is not enough to debug the miss.
    anyhow::ensure!(
        bin.exists(),
        "decdn binary not found at {}{}",
        std::path::absolute(&bin)
            .unwrap_or_else(|_| bin.clone())
            .display(),
        if overridden.is_some() {
            " (from DECDN_CLI_BIN — stale or misspelled?)"
        } else {
            "; run `cargo build -p decdn-cli` first (or set DECDN_CLI_BIN)"
        }
    );
    Ok(bin)
}

/// Build a command that runs the `decdn` CLI hermetically.
///
/// A hand-rolled `Command` inherits the developer's environment, and two
/// inherited channels reach into their real `~/.decdn`:
///
/// - **`HOME`**, which is where the CLI resolves its default data dir and
///   config file from (`dirs::home_dir()`). Config *fills in* coordinates the
///   caller left unset — flags always win (`cli::commands::chain_ctx`,
///   flag > config > default) — so inheriting `HOME` means a real
///   `~/.decdn/node.toml` supplies `blockchain.eth_keystore` to any invocation
///   that passes no `--keystore`, and the CLI then decrypts the developer's
///   real keystore with the test password. That is #1332: invisible in CI where
///   `$HOME` is fresh, and only reproducible on a used workstation.
/// - **the `DECDN_*` namespace**, which clap folds into args at CLI precedence
///   — *above* a fixture's rendered `node.toml`. An exported `DECDN_DATA_DIR`
///   or `DECDN_RPC_URL` silently displaces the fixture's own config. Same
///   defect class, one variable over, so both are closed here.
///
/// `home` must be absolute. An empty or relative path is the dangerous input,
/// not a harmless one: `dirs` treats an empty `HOME` as *absent* and falls back
/// to `getpwuid`, landing back on the real home — a guard that silently
/// restores the bug is worse than none.
///
/// `keystore_password` is a parameter rather than a constant because journeys
/// mint their own throwaway keystores rather than all sharing
/// [`crate::node::KEYSTORE_PASSWORD`]. It reaches the child through the env
/// source the CLI checks before prompting, so no test blocks on a TTY, and it
/// goes through `Command::env` — never `set_var`, which the workspace forbids.
///
/// `RUST_LOG` defaults to `warn`; raise it with `DECDN_NODE_LOG` when debugging
/// (the same knob the daemon fixture reads), or for one invocation with
/// [`CLIENT_DEBUG_LOG`].
///
/// Returns a [`std::process::Command`] so sync and async callers share one
/// helper: `tokio::process::Command` is `From<std::process::Command>`, and
/// tokio-only builders such as `.kill_on_drop(true)` still chain after the
/// conversion.
///
/// Unix-only isolation: on Windows `dirs::home_dir()` reads `USERPROFILE`, not
/// `HOME`. The whole crate is effectively Unix-only (anvil/forge-gated, and
/// every permission hardening is `#[cfg(unix)]`), but the distinction matters
/// if that ever changes.
pub fn decdn_command(
    home: &Path,
    keystore_password: &str,
) -> anyhow::Result<std::process::Command> {
    hermetic_command(decdn_cli_bin()?, home, keystore_password)
}

/// The env wiring of [`decdn_command`], split from the binary lookup so the
/// regression guards below exercise it without a built `decdn` on disk. Also
/// used by [`crate::node`] for the daemon, which needs the identical treatment.
pub(crate) fn hermetic_command(
    bin: PathBuf,
    home: &Path,
    keystore_password: &str,
) -> anyhow::Result<std::process::Command> {
    anyhow::ensure!(
        home.is_absolute(),
        "hermetic HOME must be absolute, got {}: an empty or relative HOME makes \
         dirs fall back to getpwuid — the developer's real ~/.decdn, which is \
         the #1332 defect this helper exists to prevent",
        home.display()
    );
    let mut cmd = std::process::Command::new(bin);
    // Strip before setting: `DECDN_KEYSTORE_PASSWORD` is itself in the stripped
    // namespace, so the reverse order would remove the password we just set
    // (`HOME` and `RUST_LOG` are unaffected either way).
    strip_decdn_env(&mut cmd, std::env::vars_os().map(|(k, _)| k));
    cmd.env("HOME", home)
        .env(
            decdn_incentive::eth_identity::KEYSTORE_PASSWORD_ENV,
            keystore_password,
        )
        .env(
            "RUST_LOG",
            std::env::var("DECDN_NODE_LOG").unwrap_or_else(|_| "warn".into()),
        );
    Ok(cmd)
}

/// Check the dashboard record of the one `decdn get` run that succeeded under
/// the `HOME` `home`: a `kind` run writing to `output`, with the blob's `len`
/// content bytes expected, verified and downloaded, none resumed or reused,
/// and `wire` bytes paid to `provider` alone. Every record in the directory
/// must parse, and exactly one may be `done`: the runs a journey retried end
/// `failed`.
pub fn check_pull_record(
    home: &Path,
    kind: &str,
    output: &Path,
    len: u64,
    wire: U256,
    provider: Address,
) -> anyhow::Result<()> {
    let dir = home.join(".decdn/activity/pulls");
    let mut records = Vec::new();
    for entry in std::fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
        let path = entry
            .with_context(|| format!("list {}", dir.display()))?
            .path();
        // The writer's temp files carry no `.json` extension until the rename.
        if path.extension().is_none_or(|ext| ext != "json") {
            continue;
        }
        let bytes = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
        let record: Value =
            serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
        records.push(record);
    }
    let done: Vec<&Value> = records.iter().filter(|r| r["state"] == "done").collect();
    let [done] = done.as_slice() else {
        anyhow::bail!("expected exactly one done record among {records:?}");
    };
    let u64_at = |pointer: &str| {
        done.pointer(pointer)
            .and_then(Value::as_u64)
            .with_context(|| format!("{pointer} is not a u64: {done}"))
    };
    anyhow::ensure!(done["kind"] == kind, "a {kind} run: {done}");
    anyhow::ensure!(
        done["output"] == output.display().to_string(),
        "a run writing to {}: {done}",
        output.display()
    );
    anyhow::ensure!(
        done["expected_bytes"] == len && done["received_bytes"] == len,
        "{done}"
    );
    anyhow::ensure!(
        done["downloaded_bytes"] == len && done["resumed_bytes"] == 0 && done["reused_bytes"] == 0,
        "a fresh fetch downloads every byte and resumes or reuses none: {done}"
    );
    anyhow::ensure!(done["finished_unix_ms"].is_u64(), "{done}");
    let paid_bytes = u64_at("/paid/bytes")?;
    anyhow::ensure!(
        U256::from(paid_bytes) == wire,
        "the record pays {wire} wire bytes: {done}"
    );
    let paid_amount = u64_at("/paid/amount_micro_usdc")?;
    anyhow::ensure!(paid_amount > 0, "{done}");
    anyhow::ensure!(
        done["nodes"].as_array().map(Vec::len) == Some(1)
            && done.pointer("/nodes/0/provider").and_then(Value::as_str)
                == Some(provider.to_checksum(None).as_str()),
        "one node, {provider}: {done}"
    );
    anyhow::ensure!(
        u64_at("/nodes/0/bytes")? == paid_bytes
            && u64_at("/nodes/0/amount_micro_usdc")? == paid_amount,
        "the one node is paid the whole run: {done}"
    );
    Ok(())
}

/// The exact wire bytes a whole-blob paid pull vouchers for: the bao-encoded
/// size (content plus interleaved proof) of the 16 KiB chunk-group-aligned
/// whole range (ADR 038), the quantity the node meters and the buyer records as
/// the lane watermark. The whole range `(0, 0)` aligns for every `total`, so an
/// error here means `align_range` itself broke.
pub fn whole_blob_wire_bytes(total: u64) -> anyhow::Result<u64> {
    Ok(align_range(0, 0, total)
        .with_context(|| format!("align the whole {total}-byte blob"))?
        .wire_len())
}

/// Remove every inherited `DECDN_*` variable from `cmd`. Takes the key set as
/// an iterator rather than reading the environment directly so the guard below
/// can drive it with a synthetic one (`set_var` is forbidden workspace-wide).
fn strip_decdn_env(cmd: &mut std::process::Command, keys: impl Iterator<Item = OsString>) {
    for key in keys {
        if key.to_string_lossy().starts_with("DECDN_") {
            cmd.env_remove(&key);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
