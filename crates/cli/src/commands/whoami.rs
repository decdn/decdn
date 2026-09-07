//! `decdn whoami` — print the local identity read-only.
//!
//! Loads the persisted node key and reports the iroh node id (its Ed25519
//! public key — the value a serving node logs as `remote` on `cdn/client/v1`),
//! the eth address, and the resolved key paths. It generates, stages, rotates,
//! and overwrites nothing: an absent node key is an error, not a trigger to
//! mint one.
//!
//! The eth address lives inside the encrypted keystore, so it prints only when
//! a keystore password is available (the `DECDN_KEYSTORE_PASSWORD` env var, a
//! `--keystore-password-file`, or an interactive prompt on a TTY). The node id
//! and paths print first and unconditionally, so the command stays useful for
//! the node-id diagnosis even when no password is at hand.

use std::path::Path;

use anyhow::Context;
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

/// Print the local identity read-only: node id, key paths, and eth address.
pub fn whoami(args: &cli::WhoamiArgs) -> anyhow::Result<()> {
    let data_dir = args
        .output_dir
        .as_deref()
        .map(cli::common::expand_tilde)
        .or_else(cli::default_data_dir)
        .ok_or_else(|| anyhow::anyhow!("cannot determine data directory: home dir not found"))?;

    let keystore = eth_identity::keystore_path(&data_dir);

    // Print the primary diagnostic — the node id and paths — FIRST, before any
    // keystore password resolution that could prompt on a TTY. A stuck-fetch
    // diagnosis needs the node id even when no password is at hand or the prompt
    // is unwanted; gating it behind the prompt would defeat the command's point.
    let secret = identity::load(&data_dir)?;
    println!("node id: {}", secret.public());
    println!("key path: {}", identity::key_path(&data_dir).display());
    println!("keystore path: {}", keystore.display());

    // The eth address lives inside the encrypted keystore, so it is resolved
    // last: a missing keystore or absent password degrades to a note, while a
    // supplied password decrypts it.
    let state = keystore_state(&keystore)?;
    let password = match state {
        KeystoreState::Present => resolve_password(args.keystore_password_file.as_deref())?,
        KeystoreState::Absent => KeystorePassword::Unavailable,
    };
    println!("{}", eth_address_line(&keystore, &state, &password)?);
    Ok(())
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
        Ok(KeystorePassword::Supplied(eth_identity::read_password(
            &sources,
            "eth keystore password",
        )?))
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

/// Format the eth-address line: decrypt the keystore when a password was
/// supplied, report it absent when there is no keystore, and otherwise note
/// that a password is needed.
///
/// Split from [`whoami`] so the decrypt branch and the notes are testable
/// without touching the environment or a TTY.
fn eth_address_line(
    keystore: &Path,
    state: &KeystoreState,
    password: &KeystorePassword,
) -> anyhow::Result<String> {
    match state {
        KeystoreState::Absent => Ok(format!(
            "eth address: (no keystore at {})",
            keystore.display()
        )),
        KeystoreState::Present => match password {
            KeystorePassword::Unavailable => Ok(format!(
                "eth address: (keystore present at {}; set DECDN_KEYSTORE_PASSWORD or pass \
                 --keystore-password-file to show it)",
                keystore.display()
            )),
            KeystorePassword::Supplied(pw) => {
                let signer =
                    eth_identity::load_signer(keystore, pw.as_str()).with_context(|| {
                        format!("failed to load keystore at {}", keystore.display())
                    })?;
                Ok(format!("eth address: {}", signer.address()))
            }
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;
    use decdn_incentive::eth_identity::generate_and_persist;

    const TEST_PASSWORD: &str = "hunter2";

    #[cfg(unix)]
    fn secure_tempdir() -> tempfile::TempDir {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        dir
    }

    #[cfg(not(unix))]
    fn secure_tempdir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    /// The set of filenames in `dir`, to prove `whoami` mutates nothing.
    fn file_names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// A supplied password yields the eth address `key-gen` persisted, and
    /// reads — not writes — the keystore (whoami must mutate nothing).
    #[test]
    fn address_line_shows_generated_address_and_mutates_nothing() {
        let dir = secure_tempdir();
        identity::load_or_generate(dir.path()).unwrap();
        let address = generate_and_persist(dir.path(), TEST_PASSWORD, false).unwrap();
        let keystore = eth_identity::keystore_path(dir.path());

        let before = file_names(dir.path());
        let line = eth_address_line(
            &keystore,
            &KeystoreState::Present,
            &KeystorePassword::Supplied(Zeroizing::new(TEST_PASSWORD.to_owned())),
        )
        .unwrap();

        assert_eq!(line, format!("eth address: {address}"));
        assert_eq!(
            before,
            file_names(dir.path()),
            "whoami must not add or remove files"
        );
    }

    /// With no password available, the eth line is a note rather than an error.
    #[test]
    fn address_line_without_password_notes_one_is_needed() {
        let keystore = Path::new("/data/keystore.json");
        let line = eth_address_line(
            keystore,
            &KeystoreState::Present,
            &KeystorePassword::Unavailable,
        )
        .unwrap();
        assert!(
            line.starts_with("eth address:") && line.contains("password"),
            "expected a password note, got {line:?}"
        );
    }

    /// No keystore at all is reported on the eth line, not treated as an error.
    #[test]
    fn address_line_without_keystore_notes_it_is_absent() {
        let keystore = Path::new("/data/keystore.json");
        let line = eth_address_line(
            keystore,
            &KeystoreState::Absent,
            &KeystorePassword::Unavailable,
        )
        .unwrap();
        assert!(
            line.starts_with("eth address:") && line.contains("no keystore"),
            "expected a 'no keystore' note, got {line:?}"
        );
    }

    /// A wrong password is a hard error on the eth line, not a silent note.
    #[test]
    fn address_line_with_wrong_password_errors() {
        let dir = secure_tempdir();
        generate_and_persist(dir.path(), TEST_PASSWORD, false).unwrap();
        let keystore = eth_identity::keystore_path(dir.path());

        let err = eth_address_line(
            &keystore,
            &KeystoreState::Present,
            &KeystorePassword::Supplied(Zeroizing::new("definitely-wrong".to_owned())),
        )
        .expect_err("a wrong password must error");
        assert!(format!("{err:#}").contains("keystore"), "got: {err:#}");
    }

    /// `keystore_state` classifies an absent path as `Absent` (not an error)
    /// and a present one as `Present`.
    #[test]
    fn keystore_state_classifies_present_and_absent() {
        let dir = secure_tempdir();
        let keystore = eth_identity::keystore_path(dir.path());
        assert!(
            matches!(keystore_state(&keystore).unwrap(), KeystoreState::Absent),
            "absent keystore must classify as Absent"
        );
        generate_and_persist(dir.path(), TEST_PASSWORD, false).unwrap();
        assert!(
            matches!(keystore_state(&keystore).unwrap(), KeystoreState::Present),
            "present keystore must classify as Present"
        );
    }

    /// The node id comes from `identity::load`, which errors (never generates)
    /// on an absent key — so a `whoami` against an empty dir never mints one.
    #[test]
    fn node_id_load_errors_on_absent_key_without_writing() {
        let dir = secure_tempdir();
        let err = identity::load(dir.path()).expect_err("absent node key must error");
        assert!(
            format!("{err:#}").contains("key-gen"),
            "error should point at key-gen: {err:#}"
        );
        assert!(
            !identity::key_path(dir.path()).exists(),
            "a failed whoami must not create node.secret"
        );
    }
}
