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

/// How the keystore password was resolved before building the report.
enum KeystorePassword {
    /// A password source was present and supplied this value.
    Supplied(Zeroizing<String>),
    /// No source was present (no env var, no readable file, and stdin is not a
    /// TTY to prompt on), so the eth address cannot be decrypted.
    Unavailable,
}

/// Print the local identity read-only: node id, eth address, and key paths.
pub fn whoami(args: &cli::WhoamiArgs) -> anyhow::Result<()> {
    let data_dir = args
        .output_dir
        .as_deref()
        .map(cli::common::expand_tilde)
        .or_else(cli::default_data_dir)
        .ok_or_else(|| anyhow::anyhow!("cannot determine data directory: home dir not found"))?;

    let keystore = eth_identity::keystore_path(&data_dir);

    // Resolve the keystore password before building the report, but only from a
    // source that is actually present — `read_password` would otherwise fail
    // when none is, which must degrade to a note rather than sink the whole
    // command. The node id is the primary diagnostic and never depends on it.
    let sources = super::chain_ctx::password_sources(
        args.keystore_password_file.as_deref(),
        PasswordUse::Unlock,
    );
    let password = if keystore.exists() && password_source_present(&sources) {
        KeystorePassword::Supplied(eth_identity::read_password(
            &sources,
            "eth keystore password",
        )?)
    } else {
        KeystorePassword::Unavailable
    };

    for line in build_report(&data_dir, &keystore, &password)? {
        println!("{line}");
    }
    Ok(())
}

/// True when at least one source would supply a value without further input we
/// do not have: the env var is set, a password file exists, or stdin is a TTY
/// the prompt can read from. Mirrors the presence rule
/// [`eth_identity::read_password`] applies, so a `true` here means that call
/// will not exhaust its sources.
fn password_source_present(sources: &[PasswordSource]) -> bool {
    sources.iter().any(|source| match source {
        // `var_os`, not `var`: a set-but-non-UTF-8 value is still present, and
        // `read_password` surfaces the UTF-8 error rather than silently skipping.
        PasswordSource::Env(name) => std::env::var_os(name).is_some(),
        PasswordSource::File(path) => path.exists(),
        PasswordSource::Prompt { .. } => std::io::IsTerminal::is_terminal(&std::io::stdin()),
    })
}

/// Assemble the report lines for `data_dir`. Loading the node key is read-only
/// and fails (rather than generating) when the key is absent. The eth address
/// line depends on `password`: it decrypts the keystore when a password was
/// supplied, reports the keystore absent when there is none, and otherwise
/// notes that a password is needed.
///
/// Split from [`whoami`] so the formatting and the decrypt branch are testable
/// without touching the environment or a TTY.
fn build_report(
    data_dir: &Path,
    keystore: &Path,
    password: &KeystorePassword,
) -> anyhow::Result<Vec<String>> {
    let secret = identity::load(data_dir)?;
    let key_path = identity::key_path(data_dir);

    let eth_address = if keystore.exists() {
        match password {
            KeystorePassword::Unavailable => format!(
                "eth address: (keystore present at {}; set DECDN_KEYSTORE_PASSWORD or pass \
                 --keystore-password-file to show it)",
                keystore.display()
            ),
            KeystorePassword::Supplied(pw) => {
                let signer =
                    eth_identity::load_signer(keystore, pw.as_str()).with_context(|| {
                        format!("failed to load keystore at {}", keystore.display())
                    })?;
                format!("eth address: {}", signer.address())
            }
        }
    } else {
        format!("eth address: (no keystore at {})", keystore.display())
    };

    Ok(vec![
        format!("node id: {}", secret.public()),
        eth_address,
        format!("key path: {}", key_path.display()),
        format!("keystore path: {}", keystore.display()),
    ])
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

    /// A supplied password shows the same node id and eth address `key-gen`
    /// persisted, and reads — not writes — the keystore.
    #[test]
    fn report_shows_node_id_and_address_and_mutates_nothing() {
        let dir = secure_tempdir();
        let node_key = identity::load_or_generate(dir.path()).unwrap();
        let address = generate_and_persist(dir.path(), TEST_PASSWORD, false).unwrap();
        let keystore = eth_identity::keystore_path(dir.path());

        let before = file_names(dir.path());
        let lines = build_report(
            dir.path(),
            &keystore,
            &KeystorePassword::Supplied(Zeroizing::new(TEST_PASSWORD.to_owned())),
        )
        .unwrap();

        assert!(
            lines.contains(&format!("node id: {}", node_key.public())),
            "node id line missing from {lines:?}"
        );
        assert!(
            lines.contains(&format!("eth address: {address}")),
            "eth address line missing from {lines:?}"
        );
        assert_eq!(
            before,
            file_names(dir.path()),
            "whoami must not add or remove files"
        );
    }

    /// With no password available, the node id still prints and the eth line is
    /// a note rather than an error.
    #[test]
    fn report_without_password_notes_address_needs_one() {
        let dir = secure_tempdir();
        let node_key = identity::load_or_generate(dir.path()).unwrap();
        generate_and_persist(dir.path(), TEST_PASSWORD, false).unwrap();
        let keystore = eth_identity::keystore_path(dir.path());

        let lines = build_report(dir.path(), &keystore, &KeystorePassword::Unavailable).unwrap();

        assert!(lines.contains(&format!("node id: {}", node_key.public())));
        assert!(
            lines
                .iter()
                .any(|l| l.starts_with("eth address:") && l.contains("password")),
            "expected an eth-address note about the password, got {lines:?}"
        );
    }

    /// No keystore at all is reported on the eth line, not treated as an error.
    #[test]
    fn report_without_keystore_notes_it_is_absent() {
        let dir = secure_tempdir();
        identity::load_or_generate(dir.path()).unwrap();
        let keystore = eth_identity::keystore_path(dir.path());

        let lines = build_report(dir.path(), &keystore, &KeystorePassword::Unavailable).unwrap();
        assert!(
            lines
                .iter()
                .any(|l| l.starts_with("eth address:") && l.contains("no keystore")),
            "expected a 'no keystore' note, got {lines:?}"
        );
    }

    /// An absent node key is a hard error — `whoami` never mints an identity.
    #[test]
    fn report_errors_when_node_key_absent() {
        let dir = secure_tempdir();
        let keystore = eth_identity::keystore_path(dir.path());
        let err = build_report(dir.path(), &keystore, &KeystorePassword::Unavailable)
            .expect_err("absent node key must error");
        assert!(
            format!("{err:#}").contains("key-gen"),
            "error should point at key-gen: {err:#}"
        );
        assert!(
            !identity::key_path(dir.path()).exists(),
            "a failed whoami must not create node.secret"
        );
    }

    /// A wrong password is a hard error on the eth line, not a silent note.
    #[test]
    fn report_with_wrong_password_errors() {
        let dir = secure_tempdir();
        identity::load_or_generate(dir.path()).unwrap();
        generate_and_persist(dir.path(), TEST_PASSWORD, false).unwrap();
        let keystore = eth_identity::keystore_path(dir.path());

        let err = build_report(
            dir.path(),
            &keystore,
            &KeystorePassword::Supplied(Zeroizing::new("definitely-wrong".to_owned())),
        )
        .expect_err("a wrong password must error");
        assert!(format!("{err:#}").contains("keystore"), "got: {err:#}");
    }
}
