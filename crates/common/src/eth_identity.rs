//! Ethereum keystore: generate, persist, and load a Web3 Secret Storage v3
//! file into a live `alloy::signers::local::PrivateKeySigner`.
//!
//! Companion to [`crate::identity`]. Same security model — the keystore file
//! is encrypted at rest, but we still enforce `0o600` on the file and `0o700`
//! on `data_dir` (defense-in-depth: a leaked ciphertext lowers the bar to an
//! offline KDF attack, so we reuse [`crate::identity`]'s `validate_data_dir`
//! and `create_data_dir_secure` helpers for permission parity).
//!
//! Atomic write: encrypted into a temp filename via alloy's
//! `LocalSigner::encrypt_keystore`, then chmodded `0o600` and renamed into
//! `keystore.json`. The encryption KDF (scrypt by default) takes hundreds of
//! milliseconds; runtime callers must wrap [`load_signer`] in
//! `tokio::task::spawn_blocking`.

use std::fs;
use std::io::IsTerminal;
use std::io::Read;
use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use alloy::signers::k256::elliptic_curve::rand_core::OsRng;
use alloy::signers::local::{LocalSigner, PrivateKeySigner};
use anyhow::{Context, anyhow};
use rand::Rng;

const KEYSTORE_FILE_NAME: &str = "keystore.json";
/// Forbid any group/world bit on the keystore file. Encrypted at rest, but a
/// readable ciphertext + a leaked or weak password is enough for offline
/// dictionary attacks. Mirrors [`crate::identity`]'s `0o077` mask.
#[cfg(unix)]
const FORBIDDEN_BITS: u32 = 0o077;
#[cfg(unix)]
const KEYSTORE_FILE_MODE: u32 = 0o600;

/// Source for the keystore password, in precedence order. The first source in
/// the slice that yields a non-empty secret wins.
#[derive(Debug, Clone)]
pub enum PasswordSource {
    /// Process environment variable. Skipped if unset or empty.
    Env(&'static str),
    /// File whose contents are the password. A single trailing `\n` is
    /// stripped (passwords may legitimately contain other whitespace).
    File(PathBuf),
    /// Interactive prompt via `rpassword`. Errors if `stdin` is not a TTY.
    /// `confirm = true` re-prompts and verifies the entries match.
    Prompt {
        /// When true, prompt twice and require both entries to match.
        confirm: bool,
    },
}

/// Path of the persistent Ethereum keystore within `data_dir`.
pub fn keystore_path(data_dir: &Path) -> PathBuf {
    data_dir.join(KEYSTORE_FILE_NAME)
}

/// Generate a fresh secp256k1 keypair, encrypt it as a Web3 Secret Storage v3
/// JSON keystore, and persist it to `<data_dir>/keystore.json` with mode
/// `0o600`. Returns the EIP-55 `Address` derived from the new key.
///
/// # Errors
///
/// Returns an error if `data_dir` exists with insecure permissions; if the
/// keystore file already exists and `force == false`; or if encryption,
/// chmod, or the atomic rename fails. The temp file is cleaned up on any
/// failure path; partial writes never accumulate in `data_dir`.
pub fn generate_and_persist(
    data_dir: &Path,
    password: &str,
    force: bool,
) -> anyhow::Result<Address> {
    let target = keystore_path(data_dir);

    // Same data_dir existence/permissions semantics as
    // `identity::load_or_generate`: validate if present, create securely if
    // not. Sharing the helpers (rather than re-implementing them) keeps the
    // node.secret and keystore.json paths in lock-step.
    match fs::symlink_metadata(data_dir) {
        Ok(_) => crate::identity::validate_data_dir(data_dir)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            crate::identity::create_data_dir_secure(data_dir)?;
            crate::identity::validate_data_dir(data_dir)?;
        }
        Err(e) => {
            return Err(anyhow::Error::new(e)
                .context(format!("failed to stat data_dir {}", data_dir.display())));
        }
    }

    if target.exists() {
        if !force {
            anyhow::bail!(
                "eth keystore already exists at {}; pass --force to overwrite",
                target.display()
            );
        }
        fs::remove_file(&target)
            .map_err(|e| anyhow!("failed to remove {}: {e}", target.display()))?;
    }

    // Alloy's `encrypt_keystore` writes the file in-place inside `data_dir`
    // with the given name. Use a unique temp name + chmod + rename so we end
    // with a properly-permissioned `keystore.json` even under concurrent
    // writers and crashed-mid-encrypt runs.
    let temp_name = temp_filename();

    // 32 cryptographically random bytes for the secp256k1 secret. We sample
    // via `rand::rng()` (workspace rand 0.10) and feed alloy's `OsRng`
    // (rand_core 0.6, the version alloy-signer-local 2.0 expects in its
    // `Rng + CryptoRng` bound) for the keystore's KDF salt + IV. Two
    // separate RNGs is fine — both are CSPRNGs backed by the OS entropy
    // pool.
    let mut key_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut key_bytes);

    let result: anyhow::Result<Address> = (|| {
        let mut alloy_rng = OsRng;
        let (signer, _emitted_name) = PrivateKeySigner::encrypt_keystore(
            data_dir,
            &mut alloy_rng,
            key_bytes,
            password,
            Some(&temp_name),
        )
        .with_context(|| format!("failed to encrypt eth keystore at {}", data_dir.display()))?;

        let temp_path = data_dir.join(&temp_name);
        chmod_keystore_file(&temp_path)?;

        fs::rename(&temp_path, &target).with_context(|| {
            format!(
                "failed to rename {} -> {}",
                temp_path.display(),
                target.display()
            )
        })?;

        Ok(signer.address())
    })();

    if result.is_err() {
        // Best-effort cleanup. The real error is already in flight; if
        // cleanup also fails (e.g. permission denied on a broken setup),
        // surfacing both would obscure the root cause.
        let _ = fs::remove_file(data_dir.join(&temp_name));
    }

    // Wipe the in-memory key material we don't need anymore. Not a hard
    // guarantee against a sufficiently determined attacker (the optimizer
    // can elide writes to soon-to-be-dropped buffers), but cheap and
    // useful against casual heap inspection.
    key_bytes.fill(0);

    result
}

/// Decrypt the keystore at `path` with `password` and return a live signer.
///
/// **Blocks** for the duration of the KDF (scrypt or argon2 — typically
/// hundreds of milliseconds, configurable up to multiple seconds). Async
/// callers must wrap this in [`tokio::task::spawn_blocking`].
///
/// # Errors
///
/// Returns an error if `path` is not a regular file, has insecure
/// permissions (group/world bits set), is missing, or contains a JSON that
/// is not a valid v3 keystore decryptable with `password`. The error
/// context never echoes the password.
pub fn load_signer(path: &Path, password: &str) -> anyhow::Result<PrivateKeySigner> {
    validate_keystore_file(path)?;
    LocalSigner::decrypt_keystore(path, password)
        .with_context(|| format!("failed to decrypt eth keystore at {}", path.display()))
}

/// Resolve a password from the first matching source. Empty `Env` values and
/// missing `File` sources fall through to the next entry.
///
/// # Errors
///
/// - `Prompt` with mismatched confirmations after 3 attempts.
/// - `Prompt` when `stdin` is not a TTY.
/// - All sources exhausted without producing a value.
pub fn read_password(sources: &[PasswordSource], prompt_label: &str) -> anyhow::Result<String> {
    let mut last_skip_reason: Option<String> = None;
    for source in sources {
        match source {
            PasswordSource::Env(name) => match std::env::var(name) {
                Ok(value) if !value.is_empty() => return Ok(value),
                Ok(_) => last_skip_reason = Some(format!("env {name} is empty")),
                Err(_) => last_skip_reason = Some(format!("env {name} unset")),
            },
            PasswordSource::File(path) => match read_password_file(path) {
                Ok(Some(value)) => return Ok(value),
                Ok(None) => {
                    last_skip_reason = Some(format!("password file {} is empty", path.display()));
                }
                Err(e) => return Err(e),
            },
            PasswordSource::Prompt { confirm } => {
                if !std::io::stdin().is_terminal() {
                    last_skip_reason = Some("stdin is not a TTY".to_owned());
                    continue;
                }
                return prompt_password(prompt_label, *confirm);
            }
        }
    }
    Err(anyhow!(
        "no keystore password source available ({})",
        last_skip_reason.unwrap_or_else(|| "no sources configured".to_owned())
    ))
}

fn read_password_file(path: &Path) -> anyhow::Result<Option<String>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read password file {}", path.display()))?;
    // Strip a single trailing `\n` (or `\r\n`). `String::trim_end` would also
    // eat trailing spaces — passwords legitimately contain whitespace, so we
    // strip exactly the one newline that nearly every editor and `echo`
    // appends.
    let trimmed = raw.strip_suffix("\r\n").or_else(|| raw.strip_suffix('\n'));
    let value = trimmed.map_or(raw.as_str(), |s| s).to_owned();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

fn prompt_password(label: &str, confirm: bool) -> anyhow::Result<String> {
    const MAX_ATTEMPTS: u8 = 3;
    let mut attempts: u8 = 0;
    loop {
        attempts = attempts.saturating_add(1);
        let pw = rpassword::prompt_password(format!("{label}: "))
            .with_context(|| "failed to read password from terminal")?;
        if !confirm {
            return Ok(pw);
        }
        let again = rpassword::prompt_password(format!("{label} (confirm): "))
            .with_context(|| "failed to read password confirmation from terminal")?;
        if pw == again {
            return Ok(pw);
        }
        if attempts >= MAX_ATTEMPTS {
            return Err(anyhow!(
                "passwords did not match after {MAX_ATTEMPTS} attempts"
            ));
        }
        eprintln!("passwords did not match; please try again");
    }
}

#[cfg(unix)]
fn validate_keystore_file(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::symlink_metadata(path)
        .with_context(|| format!("invalid eth keystore: cannot access {}", path.display()))?;
    anyhow::ensure!(
        meta.file_type().is_file(),
        "invalid eth keystore: {} is not a regular file",
        path.display()
    );
    let mode = meta.mode() & 0o777;
    anyhow::ensure!(
        mode & FORBIDDEN_BITS == 0,
        "invalid eth keystore: {} has insecure permissions {:#o} \
         (must be user-only, e.g. {:#o})",
        path.display(),
        mode,
        KEYSTORE_FILE_MODE
    );
    // Prove read permission early: alloy's `decrypt_keystore` would also
    // surface this, but its error mentions the JSON parser, not the open
    // step. A direct check produces a clearer diagnostic for operators.
    let mut probe = fs::File::open(path)
        .with_context(|| format!("invalid eth keystore: cannot open {}", path.display()))?;
    let mut byte = [0u8; 1];
    let _ = probe.read(&mut byte).with_context(|| {
        format!(
            "invalid eth keystore: cannot read first byte of {}",
            path.display()
        )
    })?;
    Ok(())
}

#[cfg(not(unix))]
fn validate_keystore_file(path: &Path) -> anyhow::Result<()> {
    let meta = fs::symlink_metadata(path)
        .with_context(|| format!("invalid eth keystore: cannot access {}", path.display()))?;
    anyhow::ensure!(
        meta.file_type().is_file(),
        "invalid eth keystore: {} is not a regular file",
        path.display()
    );
    Ok(())
}

#[cfg(unix)]
fn chmod_keystore_file(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(KEYSTORE_FILE_MODE))
        .with_context(|| format!("failed to chmod {} to 0o600", path.display()))
}

#[cfg(not(unix))]
fn chmod_keystore_file(_path: &Path) -> anyhow::Result<()> {
    // No POSIX mode bits on Windows; relying on the parent dir / NTFS ACLs.
    Ok(())
}

fn temp_filename() -> String {
    let mut salt = [0u8; 4];
    rand::rng().fill_bytes(&mut salt);
    format!(
        "{KEYSTORE_FILE_NAME}.tmp.{:08x}{:02x}{:02x}{:02x}{:02x}",
        std::process::id(),
        salt[0],
        salt[1],
        salt[2],
        salt[3],
    )
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use alloy::primitives::{B256, keccak256};
    use alloy::signers::{Signer, SignerSync};
    use alloy::sol;
    use alloy::sol_types::{SolStruct, eip712_domain};
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    const TEST_PASSWORD: &str = "hunter2";

    fn make_data_dir() -> TempDir {
        let tmp = TempDir::new().unwrap();
        // TempDir defaults are usually 0o700 already, but CI may run under
        // umask 002 — force the mode explicitly so the validation passes.
        fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        tmp
    }

    #[test]
    fn generates_keystore_with_secure_perms() {
        let tmp = make_data_dir();
        let _addr = generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
        let path = keystore_path(tmp.path());
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "keystore file should be 0o600, was {mode:#o}");
        let dir_mode = fs::metadata(tmp.path()).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            dir_mode, 0o700,
            "data_dir should be 0o700, was {dir_mode:#o}"
        );
    }

    #[test]
    fn skips_when_keystore_exists_and_force_false() {
        let tmp = make_data_dir();
        generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
        let err = generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("already exists"), "got: {msg}");
        assert!(msg.contains("--force"), "got: {msg}");
    }

    #[test]
    fn force_overwrites_existing_keystore() {
        let tmp = make_data_dir();
        let first = generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
        let second = generate_and_persist(tmp.path(), TEST_PASSWORD, true).unwrap();
        assert_ne!(
            first, second,
            "force should produce a fresh key with a different address"
        );
    }

    #[test]
    fn round_trip_address_recovery() {
        let tmp = make_data_dir();
        let written = generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
        let signer = load_signer(&keystore_path(tmp.path()), TEST_PASSWORD).unwrap();
        assert_eq!(
            signer.address(),
            written,
            "loaded signer must derive to the address returned by generate"
        );
    }

    #[test]
    fn wrong_password_rejected() {
        let tmp = make_data_dir();
        generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
        let err = load_signer(&keystore_path(tmp.path()), "definitely-wrong").unwrap_err();
        let chain = format!("{err:#}");
        assert!(
            chain.contains("decrypt"),
            "error should mention decrypt failure, got: {chain}"
        );
        assert!(
            !chain.contains(TEST_PASSWORD),
            "error must not echo the correct password, got: {chain}"
        );
        assert!(
            !chain.contains("definitely-wrong"),
            "error must not echo the attempted password, got: {chain}"
        );
    }

    #[test]
    fn missing_keystore_path_rejected() {
        let tmp = make_data_dir();
        let path = tmp.path().join("does-not-exist.json");
        let err = load_signer(&path, TEST_PASSWORD).unwrap_err();
        assert!(format!("{err:#}").contains("cannot access"), "got: {err:#}");
    }

    #[test]
    fn malformed_json_rejected() {
        let tmp = make_data_dir();
        let path = keystore_path(tmp.path());
        fs::write(&path, b"{not json").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let err = load_signer(&path, TEST_PASSWORD).unwrap_err();
        assert!(format!("{err:#}").contains("decrypt"), "got: {err:#}");
    }

    #[test]
    fn non_v3_keystore_rejected() {
        let tmp = make_data_dir();
        let path = keystore_path(tmp.path());
        fs::write(&path, br#"{"version":1,"address":"0x0"}"#).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let err = load_signer(&path, TEST_PASSWORD).unwrap_err();
        assert!(format!("{err:#}").contains("decrypt"), "got: {err:#}");
    }

    #[test]
    fn rejects_keystore_with_group_perms() {
        let tmp = make_data_dir();
        generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
        let path = keystore_path(tmp.path());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        let err = load_signer(&path, TEST_PASSWORD).unwrap_err();
        assert!(
            format!("{err:#}").contains("insecure permissions"),
            "got: {err:#}"
        );
    }

    #[test]
    fn rejects_symlinked_keystore() {
        let tmp = make_data_dir();
        // Generate a real keystore, then point a symlink at it.
        let real = tmp.path().join("real.json");
        fs::write(&real, b"placeholder").unwrap();
        fs::set_permissions(&real, fs::Permissions::from_mode(0o600)).unwrap();
        let link = keystore_path(tmp.path());
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let err = load_signer(&link, TEST_PASSWORD).unwrap_err();
        assert!(
            format!("{err:#}").contains("not a regular file"),
            "got: {err:#}"
        );
    }

    #[test]
    fn first_matching_source_wins() {
        // `read_password` honors source order. Using two `File` sources
        // exercises the same precedence loop as `Env` would; the
        // `unsafe_code = "forbid"` workspace lint blocks
        // `std::env::set_var` (edition 2024), so we drive the loop via
        // files instead. The "Env > File > Prompt" priority is a property
        // of how callers construct the slice, not of `read_password`.
        let tmp = make_data_dir();
        let first = tmp.path().join("first.txt");
        let second = tmp.path().join("second.txt");
        fs::write(&first, b"first-wins").unwrap();
        fs::write(&second, b"second-loses").unwrap();
        let pw = read_password(
            &[PasswordSource::File(first), PasswordSource::File(second)],
            "ignored",
        )
        .unwrap();
        assert_eq!(pw, "first-wins");
    }

    #[test]
    fn skips_empty_source_and_continues() {
        // First source is an empty file (skipped), second has content.
        let tmp = make_data_dir();
        let empty = tmp.path().join("empty.txt");
        let real = tmp.path().join("real.txt");
        fs::write(&empty, b"").unwrap();
        fs::write(&real, b"actual-pw").unwrap();
        let pw = read_password(
            &[PasswordSource::File(empty), PasswordSource::File(real)],
            "ignored",
        )
        .unwrap();
        assert_eq!(pw, "actual-pw");
    }

    #[test]
    fn password_file_trailing_newline_stripped() {
        let tmp = make_data_dir();
        let pw_file = tmp.path().join("pw.txt");
        fs::write(&pw_file, b"hunter2\n").unwrap();
        let pw = read_password(&[PasswordSource::File(pw_file)], "ignored").unwrap();
        assert_eq!(pw, "hunter2");
    }

    #[test]
    fn password_file_preserves_internal_newlines() {
        let tmp = make_data_dir();
        let pw_file = tmp.path().join("pw.txt");
        fs::write(&pw_file, b"line1\nline2").unwrap();
        let pw = read_password(&[PasswordSource::File(pw_file)], "ignored").unwrap();
        // No trailing newline to strip; internal newline preserved.
        assert_eq!(pw, "line1\nline2");
    }

    #[test]
    fn password_file_strips_crlf() {
        let tmp = make_data_dir();
        let pw_file = tmp.path().join("pw.txt");
        fs::write(&pw_file, b"hunter2\r\n").unwrap();
        let pw = read_password(&[PasswordSource::File(pw_file)], "ignored").unwrap();
        assert_eq!(pw, "hunter2");
    }

    #[test]
    fn prompt_skipped_when_not_a_tty() {
        // Under `cargo test` / `cargo nextest` stdin is not a TTY, so
        // `Prompt` should fall through and the empty source list path
        // surfaces the standard error.
        let err =
            read_password(&[PasswordSource::Prompt { confirm: false }], "ignored").unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("no keystore password source") && msg.contains("TTY"),
            "got: {msg}"
        );
    }

    #[test]
    fn empty_password_file_falls_through() {
        let tmp = make_data_dir();
        let empty = tmp.path().join("empty.txt");
        fs::write(&empty, b"").unwrap();
        // Only an empty file source — should error with "empty" in the
        // skip-reason chain.
        let err = read_password(&[PasswordSource::File(empty)], "ignored").unwrap_err();
        assert!(format!("{err:#}").contains("empty"), "got: {err:#}");
    }

    sol! {
        #[allow(missing_docs)]
        struct TinyVoucher {
            uint256 nonce;
            uint256 amount;
        }
    }

    #[test]
    fn eip712_loopback_sign_and_recover() {
        let tmp = make_data_dir();
        let written = generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
        // Pin to Arbitrum Sepolia chain id (decdn's PoC target). Pure
        // smoke test: the chain id only matters for the EIP-712 domain
        // separator, which the recover must use the same.
        let signer = load_signer(&keystore_path(tmp.path()), TEST_PASSWORD)
            .unwrap()
            .with_chain_id(Some(421_614));

        let domain = eip712_domain! {
            name: "decdn-test",
            version: "1",
            chain_id: 421_614,
            verifying_contract: Address::ZERO,
        };
        let voucher = TinyVoucher {
            nonce: alloy::primitives::U256::from(1u64),
            amount: alloy::primitives::U256::from(123_456u64),
        };
        let signing_hash: B256 = voucher.eip712_signing_hash(&domain);

        let sig = signer.sign_hash_sync(&signing_hash).unwrap();
        let recovered = sig.recover_address_from_prehash(&signing_hash).unwrap();

        assert_eq!(
            recovered, written,
            "recovered EIP-712 signer must match the keystore-derived address"
        );

        // A trivial keccak check to ensure we linked the EIP-712 helpers
        // correctly (not strictly necessary for the round-trip, but
        // catches a class of "wrong domain hashed" bugs cheaply).
        assert_ne!(
            keccak256(TinyVoucher::eip712_root_type().as_bytes()),
            B256::ZERO
        );
    }
}
