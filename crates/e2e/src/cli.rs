//! Spawning the user-facing `decdn` CLI as a subprocess, hermetically.
//!
//! The counterpart to [`crate::node`]'s daemon spawning. See [`decdn_command`]
//! for why journeys must not hand-roll their own [`std::process::Command`].

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::Context;

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
/// (the same knob the daemon fixture reads).
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
mod tests {
    use super::*;

    /// What a `Command` does to one variable. `get_envs` reports a set var as
    /// `Some(value)` and an explicitly removed one as `None`; a var it never
    /// mentions is absent from the iterator entirely. All three are distinct
    /// outcomes here, so name them rather than nesting `Option`s.
    #[derive(Debug, PartialEq, Eq)]
    enum EnvState {
        Set(OsString),
        Removed,
        Untouched,
    }

    fn env_of(cmd: &std::process::Command, key: &str) -> EnvState {
        cmd.get_envs()
            .find(|(k, _)| *k == std::ffi::OsStr::new(key))
            .map_or(EnvState::Untouched, |(_, v)| {
                v.map_or(EnvState::Removed, |v| EnvState::Set(v.to_owned()))
            })
    }

    /// The point of routing every spawn through [`decdn_command`] is that the
    /// isolation cannot be omitted. Assert the wiring directly (no chain, no
    /// built binary) so dropping it fails a plain
    /// `cargo nextest run -p decdn-e2e` rather than silently re-pointing the
    /// gated journeys at a developer's real `~/.decdn`.
    #[test]
    fn hermetic_command_isolates_home_and_carries_the_password() {
        let cmd = hermetic_command(PathBuf::from("decdn"), Path::new("/tmp/decdn-home"), "pw")
            .expect("absolute home accepted");
        assert_eq!(
            env_of(&cmd, "HOME"),
            EnvState::Set("/tmp/decdn-home".into()),
            "HOME must be pinned to the caller's tempdir"
        );
        assert_eq!(
            env_of(&cmd, decdn_incentive::eth_identity::KEYSTORE_PASSWORD_ENV),
            EnvState::Set("pw".into()),
            "the keystore password must reach the child without a prompt"
        );
        assert_eq!(cmd.get_program(), std::ffi::OsStr::new("decdn"));
    }

    /// An empty or relative `HOME` is not a harmless input: `dirs` treats empty
    /// as absent and falls back to `getpwuid`, i.e. straight back to the real
    /// home. Reject it loudly instead of silently reproducing #1332.
    #[test]
    fn hermetic_command_rejects_a_non_absolute_home() {
        for bad in ["", "relative/dir", "node.toml"] {
            let err = hermetic_command(PathBuf::from("decdn"), Path::new(bad), "pw")
                .expect_err("a non-absolute HOME must be rejected");
            assert!(
                format!("{err:#}").contains("must be absolute"),
                "unexpected error for {bad:?}: {err:#}"
            );
        }
    }

    /// An inherited `DECDN_*` var outranks the fixture's rendered config, so a
    /// developer's exported `DECDN_DATA_DIR` would point a journey back at
    /// their real data dir — #1332 one variable over.
    #[test]
    fn strip_decdn_env_removes_the_namespace_and_nothing_else() {
        let mut cmd = std::process::Command::new("decdn");
        cmd.env("PATH", "/usr/bin");
        strip_decdn_env(
            &mut cmd,
            ["DECDN_DATA_DIR", "DECDN_RPC_URL", "PATH", "HOME"]
                .into_iter()
                .map(OsString::from),
        );
        assert_eq!(
            env_of(&cmd, "DECDN_DATA_DIR"),
            EnvState::Removed,
            "an inherited DECDN_* var must be removed"
        );
        assert_eq!(env_of(&cmd, "DECDN_RPC_URL"), EnvState::Removed);
        assert_eq!(
            env_of(&cmd, "PATH"),
            EnvState::Set("/usr/bin".into()),
            "a non-DECDN var must survive untouched"
        );
        assert_eq!(
            env_of(&cmd, "HOME"),
            EnvState::Untouched,
            "the strip must not invent entries for non-DECDN keys"
        );
    }
}
