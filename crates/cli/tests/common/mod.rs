//! Shared helpers for the `decdn` CLI integration tests.
//!
//! Cargo compiles every file in `tests/` as its own crate, so this module is
//! `mod common;`-included per test file and not every helper is used by every
//! one — hence the blanket `dead_code` allow.

#![allow(dead_code)]

use std::path::Path;
use std::process::Command;

/// Spawn the built `decdn` binary with the developer's environment sealed off.
///
/// Two inherited channels reach the real `~/.decdn` and must be closed at every
/// spawn site, not just the ones that currently happen to care:
///
/// - **`HOME`**, from which the CLI resolves its default data dir and config
///   file (`dirs::home_dir()`). A flag always beats config
///   (`commands::chain_ctx`, flag > config > default), but config *fills in*
///   whatever the caller left unset — so a real `~/.decdn/node.toml` can supply
///   `blockchain.eth_keystore` to any invocation passing no `--keystore`, and
///   the binary then reads a developer's actual key material.
/// - **the `DECDN_*` namespace**, which clap folds into args at CLI precedence,
///   above any config file. An exported `DECDN_DATA_DIR` displaces the
///   `--data-dir` these tests pass.
///
/// Neither is a live defect in this crate's tests today: `channel list`
/// short-circuits config loading when `--data-dir` is given
/// (`commands::channel::list`), and `bundle create` is never handed a
/// `config_path` at all (`commands::bundle::bundle_dispatch`). That safety is a
/// property of those two subcommands, though, not of the tests — it evaporates
/// if a subcommand changes or a new test drops the flag. `decdn-e2e` had the
/// same latent shape and it became a real, environment-dependent failure
/// (#1332); `setup_redacts_rpc_error` had already hit it here.
///
/// `home` must be absolute: `dirs` treats an empty `HOME` as *absent* and falls
/// back to `getpwuid`, landing straight back on the real home directory.
pub fn decdn_command(home: &Path) -> Command {
    assert!(
        home.is_absolute(),
        "isolated HOME must be absolute, got {}: an empty or relative value \
         makes dirs fall back to getpwuid — the developer's real home",
        home.display()
    );
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_decdn"));
    // Strip before setting, so a `DECDN_*` var we set below is not removed.
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("DECDN_") {
            cmd.env_remove(&key);
        }
    }
    cmd.env("HOME", home);
    cmd
}
