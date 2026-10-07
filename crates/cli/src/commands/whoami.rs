//! `decdn whoami` — print the local identity read-only.
//!
//! Reports the persistent identity under the data dir and generates, stages,
//! rotates, and overwrites nothing.
//!
//! Two identities can share one data dir, and the command reports both:
//!  - the **node** identity at the top level: the persisted `node.secret` iroh
//!    key (its Ed25519 public key — the value a serving node logs as `remote`
//!    on `cdn/client/v1`) and the eth keystore at `<data_dir>/keystore.json`.
//!  - the **client** identity in the `client/` subdir: only its eth keystore at
//!    `<data_dir>/client/keystore.json`. A client's iroh key is ephemeral (a
//!    fresh key per fetch), so a client has no persistent node id to report. The
//!    client line prints only when that keystore exists.
//!
//! An absent node key is a note, not an error: a client-only install has no
//! `node.secret`, and the command still reports its client eth address. The
//! node id and paths print first and unconditionally, so the command stays
//! useful for the node-id diagnosis even when no password is at hand.
//!
//! An `eth address:` line always prints; only the decrypted address *value* is
//! gated by a password. The address lives inside the encrypted keystore, so it
//! shows only when a keystore password is available (the
//! `DECDN_KEYSTORE_PASSWORD` env var, a `--keystore-password-file`, or an
//! interactive prompt on a TTY); otherwise the line is a note (no keystore, or a
//! password is needed).
//!
//! One resolved password is tried against whichever keystores are present, and
//! the two can hold different ones — `key-gen` writes them at different times
//! and prompts for each. A keystore that does not open degrades its own line to
//! a note naming the reason; every other line still prints, and the command
//! still exits 0. On a TTY the failing keystore gets one more prompt of its own
//! before the note.

use std::path::{Path, PathBuf};

use decdn_common::{cli, identity};
use decdn_incentive::eth_identity::{self, PasswordSource, PasswordUse};
use zeroize::Zeroizing;

/// Whether a keystore file is there to decrypt. A stat error other than
/// `NotFound` is neither `Present` nor `Absent` — it propagates as a command
/// error rather than being silently reported as "no keystore".
enum KeystoreState {
    /// A keystore file exists at the path.
    Present,
    /// No keystore file exists (the path is definitively `NotFound`).
    Absent,
}

/// How the keystore password was resolved before printing the eth address.
enum KeystorePassword {
    /// A password source was present and supplied this value.
    Supplied(Zeroizing<String>),
    /// No source was present (no env var, no readable file, and stdin is not a
    /// TTY to prompt on), so the eth address cannot be decrypted.
    Unavailable,
}

/// Print the local identity read-only: node id, key paths, and eth addresses.
///
/// The data directory resolves with the same precedence the daemon uses, so
/// `whoami` reports the paths the running node actually reads: an explicit
/// `--output-dir` wins, then `identity.data_dir` from the config file (the
/// global `--config`, or the default `~/.decdn/node.toml`), and finally the
/// `~/.decdn` default. Without the config-file step, a node whose `data_dir` is
/// set in `node.toml` — the common service layout, where the `decdn` user's home
/// is the data dir itself — would report a phantom `~/.decdn` path the node
/// never uses.
///
/// The config file is read only when `--output-dir` is absent: the flag wins
/// outright, so an explicit `--output-dir` must not fail on an unrelated broken
/// `node.toml` it never consults.
pub fn whoami(args: &cli::WhoamiArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let data_dir = resolve_data_dir(args.output_dir.as_deref(), config_path)?
        .ok_or_else(|| anyhow::anyhow!("cannot determine data directory: home dir not found"))?;

    for line in report(&data_dir, args.keystore_password_file.as_deref())? {
        println!("{line}");
    }
    Ok(())
}

/// Resolve the data directory with the daemon's precedence: an explicit
/// `--output-dir` first, then `identity.data_dir` from the config file, then the
/// `~/.decdn` default. Returns `Ok(None)` only when every source is absent and
/// the home directory cannot be found. A leading `~` in either explicit source
/// is expanded, mirroring `resolve_identity_into`.
///
/// The config file is loaded (and parsed) only when `output_dir` is `None`, so a
/// caller that passed `--output-dir` never surfaces a config-parse error for a
/// file whose value the flag overrides anyway.
fn resolve_data_dir(
    output_dir: Option<&Path>,
    config_path: Option<&Path>,
) -> anyhow::Result<Option<PathBuf>> {
    if let Some(dir) = output_dir {
        return Ok(Some(cli::common::expand_tilde(dir)));
    }
    let file = decdn_common::config::load_file_config(config_path)?;
    Ok(file
        .identity
        .and_then(|i| i.data_dir)
        .map(|p| cli::common::expand_tilde(&p))
        .or_else(cli::default_data_dir))
}

/// Build the read-only whoami report for `data_dir` as the lines to print.
///
/// The node id and paths come first, before any keystore password resolution
/// that could prompt on a TTY: a stuck-fetch diagnosis needs the node id even
/// when no password is at hand, so gating it behind the prompt would defeat the
/// command's point. An absent node key is a note here, not an error — a
/// client-only install has no persistent node key, and the client eth address
/// still reports.
///
/// The password is resolved once, and only when a keystore is present, then
/// tried against whichever keystores exist. A keystore the shared password does
/// not open gets one prompt of its own on a TTY, then degrades to a note — the
/// two keystores can legitimately hold different passwords (#2008), and one
/// wrong password must not take the whole report down. Split from [`whoami`] so
/// the ordering and the client/node sections are testable without capturing
/// stdout.
fn report(data_dir: &Path, password_file: Option<&Path>) -> anyhow::Result<Vec<String>> {
    let mut lines = Vec::new();

    let node_keystore = eth_identity::keystore_path(data_dir);
    // The client identity is scoped to the `client/` subdir; only its eth
    // keystore persists (the client iroh key is ephemeral, so no client node id).
    let client_keystore = eth_identity::keystore_path(&data_dir.join("client"));
    let key_path = identity::key_path(data_dir);

    // Node id — read-only. A definitively-absent `node.secret` is a note; any
    // other stat/load failure (insecure permissions, wrong size, IO) still
    // propagates so a real problem is not hidden behind the note.
    match std::fs::symlink_metadata(&key_path) {
        Ok(_) => {
            let secret = identity::load(data_dir)?;
            lines.push(format!("node id: {}", secret.public()));
            lines.push(format!("key path: {}", key_path.display()));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            lines.push(format!(
                "node id: (none — no node key at {}; run `decdn key-gen` to create one)",
                key_path.display()
            ));
        }
        Err(e) => {
            return Err(anyhow::Error::new(e)
                .context(format!("failed to stat node key at {}", key_path.display())));
        }
    }
    lines.push(format!("keystore path: {}", node_keystore.display()));

    // The eth addresses live inside the encrypted keystores, so they resolve
    // last: a missing keystore or absent password degrades to a note, while a
    // supplied password decrypts them. One password unlocks either keystore.
    let node_state = keystore_state(&node_keystore)?;
    let client_state = keystore_state(&client_keystore)?;
    let password = if matches!(node_state, KeystoreState::Present)
        || matches!(client_state, KeystoreState::Present)
    {
        resolve_password(password_file)?
    } else {
        KeystorePassword::Unavailable
    };

    lines.push(unlock_line(
        &node_keystore,
        &node_state,
        &password,
        "node eth keystore password",
    ));
    if matches!(client_state, KeystoreState::Present) {
        lines.push(format!(
            "client keystore path: {}",
            client_keystore.display()
        ));
        lines.push(format!(
            "client {}",
            unlock_line(
                &client_keystore,
                &client_state,
                &password,
                "client eth keystore password",
            )
        ));
    }

    Ok(lines)
}

/// One keystore's `eth address:` line, with a second attempt when the shared
/// password does not open it and a TTY is there to ask on.
///
/// The retry is what makes two keystores with two passwords reportable. The
/// shared password is the common case and is tried first, so an install whose
/// keystores share a password prompts once, as it would with no retry at all.
///
/// The first attempt's note goes to stderr before the prompt. Without it the
/// prompt appears with no explanation — the report is buffered and printed
/// after this returns — and an operator who set `DECDN_KEYSTORE_PASSWORD`
/// cannot see why it was not used. It also keeps a failure no password can fix,
/// such as a keystore at `0o644`, from reading as a password problem: the
/// reason is on screen before anything is typed.
///
/// A failed retry, or no TTY to retry on, leaves a note — the reason is on the
/// line either way, and the rest of the report is unaffected.
fn unlock_line(
    keystore: &Path,
    state: &KeystoreState,
    password: &KeystorePassword,
    prompt_label: &str,
) -> String {
    let attempt = eth_address_line(keystore, state, password);
    if !attempt.retryable || !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
        return attempt.line;
    }
    eprintln!("warning: {}", attempt.line);
    let sources = [PasswordSource::Prompt {
        usage: PasswordUse::Unlock,
    }];
    match super::chain_ctx::read_keystore_password(&sources, prompt_label) {
        Ok(resolved) => {
            let retry = KeystorePassword::Supplied(resolved.into_secret());
            eth_address_line(keystore, state, &retry).line
        }
        // A prompt that cannot be read is not a reason to lose the report. Say
        // that it was the terminal, though: the first attempt's note blames
        // "this password", and after a prompt an operator reads that as the one
        // they just typed.
        Err(err) => format!("{} [no second attempt: {}]", attempt.line, err.root_cause()),
    }
}

/// Classify the keystore path without letting a non-`NotFound` stat error read
/// as "no keystore": `symlink_metadata` (not [`Path::exists`], which collapses
/// every error to `false`) so a permission or IO problem surfaces instead of
/// being mistaken for an absent file.
fn keystore_state(keystore: &Path) -> anyhow::Result<KeystoreState> {
    match std::fs::symlink_metadata(keystore) {
        Ok(_) => Ok(KeystoreState::Present),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(KeystoreState::Absent),
        Err(e) => Err(anyhow::Error::new(e).context(format!(
            "failed to stat eth keystore at {}",
            keystore.display()
        ))),
    }
}

/// Resolve the keystore password from the standard sources, but only when one
/// is actually present — otherwise `read_password` would error on exhausted
/// sources, which must instead degrade to a note on the eth line.
fn resolve_password(password_file: Option<&Path>) -> anyhow::Result<KeystorePassword> {
    let sources = super::chain_ctx::password_sources(password_file, PasswordUse::Unlock);
    if password_source_present(&sources) {
        Ok(KeystorePassword::Supplied(
            super::chain_ctx::read_keystore_password(&sources, "eth keystore password")?
                .into_secret(),
        ))
    } else {
        Ok(KeystorePassword::Unavailable)
    }
}

/// True when at least one source would supply a value without further input we
/// do not have: the env var is set, a password file is there, or stdin is a TTY
/// the prompt can read from. Mirrors the presence rule
/// [`eth_identity::read_password`] applies, so a `true` here means that call
/// will not exhaust its sources.
fn password_source_present(sources: &[PasswordSource]) -> bool {
    sources.iter().any(|source| match source {
        // `var_os`, not `var`: a set-but-non-UTF-8 value is still present, and
        // `read_password` surfaces the UTF-8 error rather than silently skipping.
        PasswordSource::Env(name) => std::env::var_os(name).is_some(),
        // Present unless the path is definitively `NotFound`: a permission/IO
        // error counts as present so `read_password` surfaces it rather than
        // silently skipping the source, matching its own File handling.
        PasswordSource::File(path) => {
            !matches!(std::fs::symlink_metadata(path), Err(e) if e.kind() == std::io::ErrorKind::NotFound)
        }
        PasswordSource::Prompt { .. } => std::io::IsTerminal::is_terminal(&std::io::stdin()),
    })
}

/// One attempt at an eth-address line, and whether another password could
/// still change the answer.
struct EthLine {
    /// The line to print — an address, or a note saying why there is none.
    line: String,
    /// True when a keystore is present and the password tried did not open it.
    /// Only then is a second prompt worth the operator's time.
    retryable: bool,
}

/// Format the eth-address line: decrypt the keystore when a password was
/// supplied, report it absent when there is no keystore, and otherwise note
/// that a password is needed.
///
/// A keystore that does not decrypt is a note, not an error. The node and
/// client keystores are written at different times under separate prompts, so
/// one password failing on one of them says nothing about the other, and
/// aborting would print neither (#2008). The failure text rides on the line, so
/// a corrupt file is as visible as a wrong password.
///
/// Split from [`whoami`] so the decrypt branch and the notes are testable
/// without touching the environment or a TTY.
fn eth_address_line(
    keystore: &Path,
    state: &KeystoreState,
    password: &KeystorePassword,
) -> EthLine {
    let note = |line: String| EthLine {
        line,
        retryable: false,
    };
    match state {
        KeystoreState::Absent => note(format!(
            "eth address: (no keystore at {})",
            keystore.display()
        )),
        KeystoreState::Present => match password {
            KeystorePassword::Unavailable => note(format!(
                "eth address: (keystore present at {}; set DECDN_KEYSTORE_PASSWORD or pass \
                 --keystore-password-file to show it)",
                keystore.display()
            )),
            KeystorePassword::Supplied(pw) => {
                match eth_identity::load_signer(keystore, pw.as_str()) {
                    Ok(signer) => note(format!("eth address: {}", signer.address())),
                    Err(err) => EthLine {
                        // The root cause, not the whole chain: every wrapping
                        // layer repeats this path, and the innermost message is
                        // the one that separates a wrong password from a
                        // keystore that is broken.
                        line: format!(
                            "eth address: (keystore at {} did not open with this password: {})",
                            keystore.display(),
                            err.root_cause()
                        ),
                        retryable: true,
                    },
                }
            }
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
