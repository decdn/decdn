//! Ethereum keystore: generate, persist, and load a Web3 Secret Storage v3
//! file into a live `alloy::signers::local::PrivateKeySigner`.
//!
//! Lives in `decdn-incentive` (per `appendix-poc-production-seams.md`
//! §Seam 1) because the keystore surface is shared between nodes (slash
//! signing, on-chain settlement) and clients (voucher signing per ADR 003
//! §EIP-712 Voucher Signature).
//!
//! Same security model as [`decdn_common::identity`] — the keystore file is
//! encrypted at rest, but we still enforce `0o600` on the file and `0o700`
//! on `data_dir` (defense-in-depth: a leaked ciphertext lowers the bar to an
//! offline KDF attack, so we delegate `data_dir` setup to
//! [`decdn_common::identity::ensure_data_dir`] for permission parity).
//!
//! Atomic write: encrypted into a temp filename via alloy's
//! `LocalSigner::encrypt_keystore`, then chmodded `0o600` and renamed into
//! `keystore.json`. The encryption KDF (scrypt by default) takes hundreds of
//! milliseconds; runtime callers must wrap [`load_signer`] in
//! `tokio::task::spawn_blocking`.

use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use alloy::signers::k256::elliptic_curve::rand_core::OsRng;
use alloy::signers::local::{LocalSigner, PrivateKeySigner};
use anyhow::{Context, anyhow};
use rand::Rng;
use zeroize::Zeroizing;

/// Process environment variable that supplies the keystore password.
/// [`standard_sources`] places it first in the [`PasswordSource`] list, ahead
/// of a password file and an interactive prompt.
pub const KEYSTORE_PASSWORD_ENV: &str = "DECDN_KEYSTORE_PASSWORD";

const KEYSTORE_FILE_NAME: &str = "keystore.json";
/// Forbid any group/world bit on the keystore file. Encrypted at rest, but a
/// readable ciphertext + a leaked or weak password is enough for offline
/// dictionary attacks. Mirrors [`decdn_common::identity`]'s `0o077` mask.
#[cfg(unix)]
const FORBIDDEN_BITS: u32 = 0o077;
#[cfg(unix)]
const KEYSTORE_FILE_MODE: u32 = 0o600;

/// What the caller is about to do with the keystore password, which is what
/// decides whether the interactive prompt asks once or twice.
///
/// Each call site names which one it means, so the reason for the double entry
/// travels with the argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PasswordUse {
    /// The command CREATES the keystore. The prompt asks twice and requires the
    /// entries to match: a password typed once has nothing to check it against,
    /// so a typo would be encrypted into the file and stay undiscovered until
    /// the next unlock fails, by which point the key is unrecoverable.
    Create,
    /// The command opens a keystore that already exists. One entry is enough —
    /// a wrong password simply fails to decrypt, and says so immediately.
    Unlock,
}

/// Source for the keystore password, in precedence order. The first source in
/// the slice that is **present** wins, and it supplies its value as-is — the
/// empty string included, because an empty password is a legitimate Web3 Secret
/// Storage password. Presence, not emptiness, decides: a source that is not
/// there falls through to the next entry.
#[derive(Debug, Clone)]
pub enum PasswordSource {
    /// Process environment variable. Present when the variable is set; its
    /// value is the password, empty included. An unset variable falls through;
    /// a value that is not valid UTF-8 is an error, because the variable is
    /// there and the operator meant it to be read.
    Env(&'static str),
    /// File whose contents are the password. A single trailing `\n` (or
    /// `\r\n`) is stripped; other whitespace is part of the password. Present
    /// when the file exists — an empty file is an empty password. A path that
    /// does not exist falls through; any other read failure is an error.
    File(PathBuf),
    /// Interactive prompt via `rpassword`. Present when `stdin` is a TTY; a
    /// non-TTY `stdin` falls through. An empty entry is an empty password.
    /// [`PasswordUse::Create`] re-prompts and verifies the entries match.
    Prompt {
        /// What the password is for; see [`PasswordUse`]. `Create` prompts
        /// twice and requires both entries to match, `Unlock` takes the single
        /// entry as given.
        usage: PasswordUse,
    },
}

/// Which [`PasswordSource`] supplied the password that [`read_password`]
/// returned. Lets a caller name the winning source in its own messages —
/// `key-gen` uses it to refuse an empty password that came from the
/// environment (a shell expanding an unset variable), and operator commands
/// use it to say which source won.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PasswordOrigin {
    /// The environment variable named here supplied the password.
    Env(&'static str),
    /// This file supplied the password.
    File(PathBuf),
    /// An interactive terminal prompt supplied the password.
    Prompt,
}

impl PasswordOrigin {
    /// A human phrase naming the source, for operator-facing messages — "the
    /// environment variable" followed by the variable name, "the password file"
    /// followed by the path, or "an interactive prompt". Plain text with no
    /// backticks, and never the password itself.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            PasswordOrigin::Env(name) => format!("the environment variable {name}"),
            PasswordOrigin::File(path) => format!("the password file {}", path.display()),
            PasswordOrigin::Prompt => "an interactive prompt".to_owned(),
        }
    }
}

/// The outcome of [`read_password`]: the password, the source that won, and any
/// operator-facing warnings about sources that were configured but not used.
///
/// The warnings exist because a mistyped `--keystore-password-file` on a TTY
/// silently falls through to an interactive prompt — correct at the terminal,
/// but a different mode than the headless unit the operator ultimately deploys.
/// The caller decides where the warnings go: the CLI prints them to stderr
/// (`decdn-incentive` cannot, it denies `print_stderr`), the daemon logs them
/// through `tracing`.
#[derive(Debug)]
pub struct ResolvedPassword {
    secret: Zeroizing<String>,
    origin: PasswordOrigin,
    warnings: Vec<String>,
}

impl ResolvedPassword {
    /// The resolved password. Borrowed as `&str` for the common case of
    /// passing it straight to [`load_signer`] or [`stage_keystore`].
    #[must_use]
    pub fn secret(&self) -> &str {
        &self.secret
    }

    /// Whether the resolved password is the empty string.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.secret.is_empty()
    }

    /// Which source supplied the password.
    #[must_use]
    pub const fn origin(&self) -> &PasswordOrigin {
        &self.origin
    }

    /// Operator-facing warnings about configured-but-unused sources, in source
    /// order. Empty in the common case. Each is a complete sentence with no
    /// `warning:` prefix and never contains the password.
    #[must_use]
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// Consume the outcome and yield the password, still zeroized on drop. Use
    /// when the secret must be moved (e.g. into a `spawn_blocking` closure)
    /// after the warnings and origin have been inspected.
    #[must_use]
    pub fn into_secret(self) -> Zeroizing<String> {
        self.secret
    }
}

/// The keystore password sources every binary consults, in precedence order:
/// the `DECDN_KEYSTORE_PASSWORD` env var, then `password_file` when the
/// caller passed one, then an interactive prompt on a TTY. Presence decides
/// at each step (see [`read_password`]), so an env var set to the empty
/// string is the password rather than a skipped source.
///
/// `usage` reaches the [`PasswordSource::Prompt`] entry and nothing else:
/// [`PasswordUse::Create`] makes that prompt ask twice and require a match,
/// [`PasswordUse::Unlock`] makes it ask once. A password arriving from the env
/// var or the file is used as given under either.
///
/// `password_file` is used as given; no expansion happens here. Tilde
/// expansion is the caller's concern. The CLI expands at its boundary
/// (`chain_ctx::password_sources`), the daemon resolves it in `decdn-common`
/// config resolution.
pub fn standard_sources(password_file: Option<PathBuf>, usage: PasswordUse) -> Vec<PasswordSource> {
    let mut sources = vec![PasswordSource::Env(KEYSTORE_PASSWORD_ENV)];
    if let Some(path) = password_file {
        sources.push(PasswordSource::File(path));
    }
    sources.push(PasswordSource::Prompt { usage });
    sources
}

/// Path of the persistent Ethereum keystore within `data_dir`.
pub fn keystore_path(data_dir: &Path) -> PathBuf {
    data_dir.join(KEYSTORE_FILE_NAME)
}

/// Generate a fresh secp256k1 keypair, encrypt it as a Web3 Secret Storage v3
/// JSON keystore, and persist it to `<data_dir>/keystore.json` with mode
/// `0o600`. Returns the EIP-55 `Address` derived from the new key.
///
/// A thin wrapper over [`stage_keystore`] + [`StagedKeystore::commit`]: all the
/// generation (RNG, encryption, chmod) happens before any existing keystore is
/// archived, so an encryption failure leaves the prior `keystore.json` in place.
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
    if !force && target.exists() {
        anyhow::bail!(
            "eth keystore already exists at {}; pass --force to overwrite",
            target.display()
        );
    }
    let staged = stage_keystore(data_dir, password)?;
    let address = staged.address();
    // Commit archives any existing keystore (the `force` case) and renames the
    // staged temp into place. The bak path is dropped here; the `decdn key-gen`
    // CLI uses the staged API directly to log it.
    staged.commit()?;
    Ok(address)
}

/// A freshly-generated, fully-encrypted keystore written to a temp file in
/// `data_dir`, not yet committed to `keystore.json`.
///
/// Staging (generate + encrypt + chmod the temp) is separated from committing
/// (archive-old + rename) so a multi-file key rotation can generate **all** key
/// material before touching any canonical file — an encryption failure then
/// leaves the prior keystore (and the sibling `node.secret`) untouched rather
/// than half-rotated (#844). Produced by [`stage_keystore`]; finished with
/// [`Self::commit`].
///
/// Dropping a [`StagedKeystore`] without committing removes the temp file, so an
/// abandoned stage never litters `data_dir`.
#[must_use = "a staged keystore must be committed (or it is discarded on drop)"]
// `derive(Debug)` is safe only because every field is non-secret: the secret key
// is already encrypted into the temp file and `key_bytes` is zeroed in
// `stage_keystore`. If a secret field is ever added, hand-write `Debug` to omit
// it (as `StagedNodeKey` does).
#[derive(Debug)]
pub struct StagedKeystore {
    tmp: PathBuf,
    target: PathBuf,
    address: Address,
    committed: bool,
}

impl StagedKeystore {
    /// The EIP-55 address derived from the staged key, for logging before the
    /// commit lands.
    #[must_use]
    pub const fn address(&self) -> Address {
        self.address
    }

    /// Commit the staged keystore: archive any existing `keystore.json` (so the
    /// operator key-rotation runbook keeps the prior ciphertext) and atomically
    /// rename the temp into place.
    ///
    /// Returns the archive path if a prior keystore was moved aside, or `None`
    /// when there was nothing to archive (fresh install).
    ///
    /// # Errors
    ///
    /// Returns an error if archiving the prior keystore or the install rename
    /// fails. The commit is **fail-safe** via
    /// [`decdn_common::identity::install_staged`]: if the install rename fails
    /// after the prior keystore was archived, the archive is restored
    /// (best-effort) so the old keystore stays live, and the error states whether
    /// the restore succeeded or `keystore.json` is now missing. On any error the
    /// staged temp is removed by the `Drop` handler (`committed` stays `false`).
    pub fn commit(mut self) -> anyhow::Result<Option<PathBuf>> {
        // Archiving (inside `install_staged`) keeps the prior ciphertext rather
        // than destroying it: the operator key-rotation runbook
        // (`appendix-operator-key-rotation.md` §5) relies on rollback to the prior
        // key, and the offline-archive requirement applies symmetrically here.
        let bak = decdn_common::identity::install_staged(&self.tmp, &self.target)?;
        self.committed = true;
        Ok(bak)
    }
}

impl Drop for StagedKeystore {
    fn drop(&mut self) {
        if !self.committed {
            // Best-effort: an abandoned stage must not leave a keystore behind.
            let _ = fs::remove_file(&self.tmp);
        }
    }
}

/// Generate a fresh secp256k1 keypair and encrypt it into a temp keystore file
/// in `data_dir` (mode `0o600` on Unix) **without** touching `keystore.json`.
/// Validates or securely creates `data_dir` with the same `0o700` semantics as
/// [`decdn_common::identity::load_or_generate`].
///
/// The returned [`StagedKeystore`] is committed via [`StagedKeystore::commit`].
/// Used by `decdn key-gen` so the keystore and the node key are both generated
/// before either canonical file is replaced (#844).
///
/// # Errors
///
/// Returns an error if `data_dir` cannot be validated or created, or if
/// encryption or the chmod fails. The temp file is cleaned up on any failure
/// path; partial writes never accumulate in `data_dir`.
pub fn stage_keystore(data_dir: &Path, password: &str) -> anyhow::Result<StagedKeystore> {
    // Validate (or create+validate) `data_dir` with the same semantics as
    // `identity::load_or_generate`: shared via the public helper so the
    // node.secret and keystore.json paths stay in lock-step.
    decdn_common::identity::ensure_data_dir(data_dir)?;

    // Alloy's `encrypt_keystore` writes the file in-place inside `data_dir`
    // with the given name. Use a unique temp name + chmod so we end with a
    // properly-permissioned file that the caller renames into place.
    let temp_name = temp_filename();

    // 32 cryptographically random bytes for the secp256k1 secret. We sample
    // via `rand::rng()` (workspace rand 0.10) and feed alloy's `OsRng`
    // (rand_core 0.6, the version alloy-signer-local 2.0 expects in its
    // `Rng + CryptoRng` bound) for the keystore's KDF salt + IV. Two
    // separate RNGs is fine — both are CSPRNGs backed by the OS entropy
    // pool.
    let mut key_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut key_bytes);

    let result: anyhow::Result<StagedKeystore> = (|| {
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
        // alloy's `encrypt_keystore` writes the temp file with default umask
        // perms (typically ~0o644). The chmod below tightens it before the
        // commit's rename. The window is bounded by `data_dir = 0o700`, so only
        // the owner can traverse the directory — equivalent to legitimate-user
        // access.
        chmod_keystore_file(&temp_path)?;

        Ok(StagedKeystore {
            tmp: temp_path,
            target: keystore_path(data_dir),
            address: signer.address(),
            committed: false,
        })
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

/// Resolve a password from the first source that is present. An unset `Env`, a
/// `File` path that does not exist, and a `Prompt` without a TTY fall through to
/// the next entry; a source that is present supplies its value, the empty string
/// included. When every source falls through, the error lists why each one did.
///
/// Returns a [`ResolvedPassword`]: the secret, which source won, and any
/// warnings about configured-but-unused sources for the caller to surface. The
/// secret is held in a `Zeroizing<String>` so it is overwritten in memory on
/// drop — defense-in-depth against post-mortem heap inspection. The `String`
/// itself is still subject to allocator reuse, but the wrapper guarantees the
/// bytes are scrubbed before that reuse becomes possible.
///
/// # Errors
///
/// - `Env` set to a value that is not valid UTF-8.
/// - A `File` that exists but cannot be read: permissions, a directory, or
///   contents that are not valid UTF-8.
/// - `Prompt` when the terminal read itself fails.
/// - `Prompt` with mismatched confirmations after 3 attempts.
/// - All sources exhausted without a present one.
pub fn read_password(
    sources: &[PasswordSource],
    prompt_label: &str,
) -> anyhow::Result<ResolvedPassword> {
    // Every skipped source is reported, not just the last: a mistyped
    // `--keystore-password-file` falls through, and if only the final skip survived
    // (`stdin is not a TTY`) the path the operator got wrong would never reach
    // them.
    let mut skipped: Vec<String> = Vec::new();
    for (idx, source) in sources.iter().enumerate() {
        let won: Option<(Zeroizing<String>, PasswordOrigin)> = match source {
            PasswordSource::Env(name) => match std::env::var(name) {
                // A set variable is a deliberate value, so an empty one is an
                // empty password rather than a reason to try the next source.
                Ok(value) => Some((Zeroizing::new(value), PasswordOrigin::Env(name))),
                Err(std::env::VarError::NotPresent) => {
                    skipped.push(format!("env {name} unset"));
                    None
                }
                // Set but unreadable is a misconfiguration, not an absent
                // source. The message never echoes the value — it is a password.
                Err(std::env::VarError::NotUnicode(_)) => {
                    return Err(anyhow!("env {name} is set but is not valid UTF-8"));
                }
            },
            PasswordSource::File(path) => match read_password_file(path) {
                Ok(Some(value)) => Some((value, PasswordOrigin::File(path.clone()))),
                Ok(None) => {
                    skipped.push(format!(
                        "password file {} not found (missing path or broken symlink)",
                        path.display()
                    ));
                    None
                }
                Err(e) => return Err(e),
            },
            PasswordSource::Prompt { usage } => {
                if std::io::stdin().is_terminal() {
                    Some((
                        prompt_password(prompt_label, *usage)?,
                        PasswordOrigin::Prompt,
                    ))
                } else {
                    skipped.push("stdin is not a TTY".to_owned());
                    None
                }
            }
        };
        if let Some((secret, origin)) = won {
            let warnings = unused_file_warnings(sources, idx, &origin);
            return Ok(ResolvedPassword {
                secret,
                origin,
                warnings,
            });
        }
    }
    let why = if skipped.is_empty() {
        "no sources configured".to_owned()
    } else {
        skipped.join("; ")
    };
    Err(anyhow!("no keystore password source available ({why})"))
}

/// Build the operator warnings for a successful resolve: a `File` source the
/// caller configured but that did not win is worth a word, because the usual
/// reason is a typo.
///
/// Two shapes, told apart by position relative to the winner at `win_idx`:
/// a file *before* the winner was reached and fell through as not-found (so a
/// later source quietly won — the 3am-under-systemd trap); a file *after* the
/// winner was never consulted because a higher-precedence source was present
/// (so the flag is inert). A file read fatally (permissions, a directory)
/// never reaches here — that path returns an error instead.
fn unused_file_warnings(
    sources: &[PasswordSource],
    win_idx: usize,
    origin: &PasswordOrigin,
) -> Vec<String> {
    let winner = origin.describe();
    let mut warnings = Vec::new();
    for (idx, source) in sources.iter().enumerate() {
        let PasswordSource::File(path) = source else {
            continue;
        };
        match idx.cmp(&win_idx) {
            // The file itself won; nothing to warn about.
            std::cmp::Ordering::Equal => {}
            // Reached and fell through as not-found; a later source won.
            std::cmp::Ordering::Less => warnings.push(format!(
                "password file {} not found; used {winner} instead",
                path.display()
            )),
            // Never consulted because a higher-precedence source was present.
            std::cmp::Ordering::Greater => warnings.push(format!(
                "password file {} was not used; {winner} took precedence",
                path.display()
            )),
        }
    }
    warnings
}

/// Read `path` as a password. `Ok(None)` means the file is not there, which is
/// an absent source; `Ok(Some(_))` carries the file's contents with a single
/// trailing `\n` or `\r\n` removed, the empty string included.
fn read_password_file(path: &Path) -> anyhow::Result<Option<Zeroizing<String>>> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => Zeroizing::new(raw),
        // A path that is not there is an absent source and falls through. Every
        // other read failure (unreadable, a directory) names a real mistake, so
        // it stays fatal rather than silently selecting another source.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(anyhow::Error::new(e))
                .with_context(|| format!("failed to read password file {}", path.display()));
        }
    };
    // Strip a single trailing `\n` (or `\r\n`). `String::trim_end` would also
    // eat trailing spaces — passwords legitimately contain whitespace, so we
    // strip exactly the one newline that nearly every editor and `echo`
    // appends.
    let trimmed = raw.strip_suffix("\r\n").or_else(|| raw.strip_suffix('\n'));
    Ok(Some(Zeroizing::new(
        trimmed.map_or(raw.as_str(), |s| s).to_owned(),
    )))
}

#[expect(
    clippy::print_stderr,
    reason = "interactive terminal prompt; there is no subscriber to route this to"
)]
fn prompt_password(label: &str, usage: PasswordUse) -> anyhow::Result<Zeroizing<String>> {
    const MAX_ATTEMPTS: u8 = 3;
    let mut attempts: u8 = 0;
    loop {
        attempts = attempts.saturating_add(1);
        let pw = Zeroizing::new(
            rpassword::prompt_password(format!("{label}: "))
                .with_context(|| "failed to read password from terminal")?,
        );
        // Matched, not compared, so a variant added later fails the build here
        // rather than inheriting `Create`'s double entry by default.
        match usage {
            PasswordUse::Unlock => return Ok(pw),
            PasswordUse::Create => {}
        }
        let again = Zeroizing::new(
            rpassword::prompt_password(format!("{label} (confirm): "))
                .with_context(|| "failed to read password confirmation from terminal")?,
        );
        if *pw == *again {
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
    use std::io::Read;
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
#[allow(
    clippy::unnecessary_wraps,
    reason = "the Unix variant can fail; the shared call sites take a Result"
)]
#[allow(
    clippy::missing_const_for_fn,
    reason = "the Unix variant does filesystem I/O and cannot be const; the shared call sites are not const contexts"
)]
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

    /// The builder every binary routes through: `Env` first, the `File` only
    /// when the caller passed a path, `Prompt` last carrying `usage`.
    /// Dropping the `Prompt` push would make every interactive command
    /// headless-only, and pushing `File` ahead of `Env` would invert the
    /// documented precedence. Neither shows up in the [`read_password`] tests
    /// below: those drive a list handed to them, so they pin how a list is
    /// consumed, never how this one is built.
    #[test]
    fn password_sources_orders_env_then_file_then_prompt() {
        let with_file = standard_sources(Some(PathBuf::from("/abs/pw.txt")), PasswordUse::Unlock);
        assert!(
            matches!(
                with_file.as_slice(),
                [
                    PasswordSource::Env(name),
                    PasswordSource::File(p),
                    PasswordSource::Prompt {
                        usage: PasswordUse::Unlock
                    },
                ] if *name == KEYSTORE_PASSWORD_ENV
                    && p == Path::new("/abs/pw.txt")
            ),
            "got: {with_file:?}"
        );

        // No path => no `File` entry at all, so an operator who passed no flag
        // never sees a missing-file skip reason.
        let without = standard_sources(None, PasswordUse::Create);
        assert!(
            matches!(
                without.as_slice(),
                [
                    PasswordSource::Env(_),
                    PasswordSource::Prompt {
                        usage: PasswordUse::Create
                    },
                ]
            ),
            "got: {without:?}"
        );
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

    /// `--force` must archive (not delete) the prior keystore so the
    /// operator key-rotation runbook (`appendix-operator-key-rotation.md`
    /// §1 step 9, §5 rollback) has the previous ciphertext to fall back on.
    #[test]
    fn force_archives_old_keystore_to_bak() {
        let tmp = make_data_dir();
        generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
        let original = fs::read(keystore_path(tmp.path())).unwrap();

        generate_and_persist(tmp.path(), TEST_PASSWORD, true).unwrap();

        let mut bak: Option<PathBuf> = None;
        for entry in fs::read_dir(tmp.path()).unwrap() {
            let p = entry.unwrap().path();
            let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if name.starts_with("keystore.json.bak.") {
                bak = Some(p);
                break;
            }
        }
        let bak = bak.expect("force should produce a `keystore.json.bak.<ts>` file");
        let archived = fs::read(&bak).unwrap();
        assert_eq!(
            archived, original,
            "bak file must hold the byte-for-byte prior keystore"
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

    /// The empty-password twin of [`round_trip_address_recovery`]: a keystore
    /// created with `""` opens with `""` and derives the same address (#1931).
    #[test]
    fn empty_password_round_trip() {
        let tmp = make_data_dir();
        let written = generate_and_persist(tmp.path(), "", false).unwrap();
        let signer = load_signer(&keystore_path(tmp.path()), "").unwrap();
        assert_eq!(
            signer.address(),
            written,
            "an empty password must round-trip like any other"
        );
    }

    /// Proves the empty password is really applied to the KDF rather than
    /// treated as "no password set": a non-empty guess must not open it.
    #[test]
    fn wrong_password_rejected_for_empty_keystore() {
        let tmp = make_data_dir();
        generate_and_persist(tmp.path(), "", false).unwrap();
        let err = load_signer(&keystore_path(tmp.path()), TEST_PASSWORD).unwrap_err();
        assert!(format!("{err:#}").contains("decrypt"), "got: {err:#}");
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
        // `read_password` stops at the first PRESENT source. Using two
        // `File` sources exercises the same precedence loop as `Env` would; the
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
        assert_eq!(pw.secret(), "first-wins");
    }

    #[test]
    fn missing_file_source_falls_through() {
        // First source is a path that does not exist (an absent source),
        // second has content.
        let tmp = make_data_dir();
        let missing = tmp.path().join("not-there.txt");
        let real = tmp.path().join("real.txt");
        fs::write(&real, b"actual-pw").unwrap();
        let pw = read_password(
            &[PasswordSource::File(missing), PasswordSource::File(real)],
            "ignored",
        )
        .unwrap();
        assert_eq!(pw.secret(), "actual-pw");
    }

    /// A file that fell through as not-found before a later source won is worth
    /// a warning naming the path (#1934): the usual cause is a typo that looks
    /// fine at the terminal and fails headless. The warning names the skipped
    /// path and the source that actually supplied the password.
    #[test]
    fn a_skipped_file_before_the_winner_warns_naming_the_path() {
        let tmp = make_data_dir();
        let missing = tmp.path().join("typo.txt");
        let real = tmp.path().join("real.txt");
        fs::write(&real, b"actual-pw").unwrap();
        let resolved = read_password(
            &[
                PasswordSource::File(missing.clone()),
                PasswordSource::File(real.clone()),
            ],
            "ignored",
        )
        .unwrap();
        assert_eq!(resolved.warnings().len(), 1, "{:?}", resolved.warnings());
        let warning = resolved.warnings().first().unwrap();
        assert!(
            warning.contains(&missing.display().to_string()),
            "{warning}"
        );
        assert!(warning.contains(&real.display().to_string()), "{warning}");
        assert!(warning.contains("not found"), "{warning}");
        assert!(
            !warning.contains("actual-pw"),
            "must not echo the password: {warning}"
        );
    }

    /// A file configured after the source that won was never consulted, so its
    /// flag is inert — warn that it was not used (#1934, the shadowed case).
    #[test]
    fn a_file_after_the_winner_warns_it_was_unused() {
        let tmp = make_data_dir();
        let winner = tmp.path().join("winner.txt");
        let shadowed = tmp.path().join("shadowed.txt");
        fs::write(&winner, b"winning-pw").unwrap();
        fs::write(&shadowed, b"never-read").unwrap();
        let resolved = read_password(
            &[
                PasswordSource::File(winner),
                PasswordSource::File(shadowed.clone()),
            ],
            "ignored",
        )
        .unwrap();
        let warning = resolved.warnings().first().expect("one warning");
        assert!(
            warning.contains(&shadowed.display().to_string()),
            "{warning}"
        );
        assert!(warning.contains("was not used"), "{warning}");
    }

    /// The common case: the single source that wins produces no warning and
    /// reports itself as the origin.
    #[test]
    fn a_clean_single_source_resolve_has_no_warnings() {
        let tmp = make_data_dir();
        let pw_file = tmp.path().join("pw.txt");
        fs::write(&pw_file, b"hunter2").unwrap();
        let resolved = read_password(&[PasswordSource::File(pw_file.clone())], "ignored").unwrap();
        assert!(resolved.warnings().is_empty(), "{:?}", resolved.warnings());
        assert_eq!(resolved.origin(), &PasswordOrigin::File(pw_file));
    }

    #[test]
    fn password_file_trailing_newline_stripped() {
        let tmp = make_data_dir();
        let pw_file = tmp.path().join("pw.txt");
        fs::write(&pw_file, b"hunter2\n").unwrap();
        let pw = read_password(&[PasswordSource::File(pw_file)], "ignored").unwrap();
        assert_eq!(pw.secret(), "hunter2");
    }

    #[test]
    fn password_file_preserves_internal_newlines() {
        let tmp = make_data_dir();
        let pw_file = tmp.path().join("pw.txt");
        fs::write(&pw_file, b"line1\nline2").unwrap();
        let pw = read_password(&[PasswordSource::File(pw_file)], "ignored").unwrap();
        // No trailing newline to strip; internal newline preserved.
        assert_eq!(pw.secret(), "line1\nline2");
    }

    #[test]
    fn password_file_strips_crlf() {
        let tmp = make_data_dir();
        let pw_file = tmp.path().join("pw.txt");
        fs::write(&pw_file, b"hunter2\r\n").unwrap();
        let pw = read_password(&[PasswordSource::File(pw_file)], "ignored").unwrap();
        assert_eq!(pw.secret(), "hunter2");
    }

    #[test]
    fn prompt_skipped_when_not_a_tty() {
        // Under `cargo test` / `cargo nextest` stdin is not a TTY, so
        // `Prompt` should fall through and the empty source list path
        // surfaces the standard error.
        let err = read_password(
            &[PasswordSource::Prompt {
                usage: PasswordUse::Unlock,
            }],
            "ignored",
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("no keystore password source") && msg.contains("TTY"),
            "got: {msg}"
        );
    }

    /// An empty file is a file that is there, so it supplies an empty password
    /// rather than falling through. This is what lets an operator load a
    /// keystore written with no password without a TTY (#1931).
    #[test]
    fn empty_password_file_is_an_empty_password() {
        let tmp = make_data_dir();
        let empty = tmp.path().join("empty.txt");
        fs::write(&empty, b"").unwrap();
        let pw = read_password(&[PasswordSource::File(empty)], "ignored").unwrap();
        assert_eq!(pw.secret(), "");
    }

    /// Presence, not content, decides precedence: an empty file earlier in the
    /// slice beats a populated one later rather than falling through to it.
    #[test]
    fn empty_password_file_beats_a_later_populated_source() {
        let tmp = make_data_dir();
        let empty = tmp.path().join("empty.txt");
        let real = tmp.path().join("real.txt");
        fs::write(&empty, b"").unwrap();
        fs::write(&real, b"never-reached").unwrap();
        let pw = read_password(
            &[PasswordSource::File(empty), PasswordSource::File(real)],
            "ignored",
        )
        .unwrap();
        assert_eq!(pw.secret(), "");
    }

    /// A path that does not exist falls through, and the exhausted-sources
    /// error names it — the only thing that keeps a typo'd
    /// `--keystore-password-file` diagnosable, because a missing path falls
    /// through rather than erroring at the read.
    /// Every skipped source reaches the error, not just the last one. With a
    /// single source this cannot be told apart from keeping only the last, so
    /// the test drives all three fall-through legs at once.
    #[test]
    fn exhausted_sources_error_lists_every_skip_reason() {
        // A name no environment sets, rather than `KEYSTORE_PASSWORD_ENV`: a
        // developer who exports the real variable would otherwise fail this
        // test for a reason that has nothing to do with accumulation.
        const UNSET: &str = "DECDN_KEYSTORE_PASSWORD_ABSENT_IN_TESTS";
        let tmp = make_data_dir();
        let missing = tmp.path().join("nope.txt");
        // Stdin is not a TTY under the test harness, so all three fall through.
        let err = read_password(
            &[
                PasswordSource::Env(UNSET),
                PasswordSource::File(missing.clone()),
                PasswordSource::Prompt {
                    usage: PasswordUse::Unlock,
                },
            ],
            "ignored",
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains(UNSET), "got: {msg}");
        assert!(msg.contains(&missing.display().to_string()), "got: {msg}");
        assert!(msg.contains("TTY"), "got: {msg}");
    }

    #[test]
    fn missing_password_file_falls_through_and_names_the_path() {
        let tmp = make_data_dir();
        let missing = tmp.path().join("nope.txt");
        let err = read_password(&[PasswordSource::File(missing.clone())], "ignored").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("no keystore password source"), "got: {msg}");
        assert!(msg.contains(&missing.display().to_string()), "got: {msg}");
    }

    #[test]
    fn password_file_with_only_a_newline_is_an_empty_password() {
        let tmp = make_data_dir();
        let pw_file = tmp.path().join("pw.txt");
        fs::write(&pw_file, b"\n").unwrap();
        let pw = read_password(&[PasswordSource::File(pw_file)], "ignored").unwrap();
        assert_eq!(pw.secret(), "");
    }

    /// A file that exists but cannot be read names a real misconfiguration, so
    /// it is fatal — silently selecting the next source would hide it. A
    /// directory is the portable way to provoke a non-`NotFound` read error: a
    /// mode-000 file is still readable by root, and CI may run as root.
    #[test]
    fn unreadable_password_file_is_fatal() {
        let tmp = make_data_dir();
        let dir = tmp.path().join("a-directory");
        fs::create_dir(&dir).unwrap();
        let fallback = tmp.path().join("fallback.txt");
        fs::write(&fallback, b"never-reached").unwrap();
        let err = read_password(
            &[
                PasswordSource::File(dir.clone()),
                PasswordSource::File(fallback),
            ],
            "ignored",
        )
        .unwrap_err();
        let chain = format!("{err:#}");
        assert!(
            chain.contains("failed to read password file"),
            "got: {chain}"
        );
        assert!(
            chain.contains(&dir.display().to_string()),
            "error must name the offending path, got: {chain}"
        );
        assert!(
            !chain.contains("no keystore password source"),
            "an unreadable file must not fall through to the next source: {chain}"
        );
    }

    // Canonical EIP-712 schema from ADR 003 §EIP-712 NodeId-to-Ethereum
    // Binding (`BindNodeId(bytes32 nodeId,uint64 nonce)`). Inlined rather
    // than imported from another module: this test asserts the schema we
    // build against the keystore-loaded signer matches the ADR exactly,
    // so any drift here surfaces independently of the on-chain contract
    // wiring (#327).
    sol! {
        #[allow(missing_docs)]
        struct BindNodeId {
            bytes32 nodeId;
            uint64 nonce;
        }
    }

    #[test]
    fn eip712_loopback_sign_and_recover() {
        let tmp = make_data_dir();
        let written = generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
        let signer = load_signer(&keystore_path(tmp.path()), TEST_PASSWORD)
            .unwrap()
            .with_chain_id(Some(421_614));

        let domain = eip712_domain! {
            name: "decdn-test",
            version: "1",
            chain_id: 421_614,
            verifying_contract: Address::ZERO,
        };
        let binding = BindNodeId {
            nodeId: B256::from([0xaau8; 32]),
            nonce: 1,
        };
        let signing_hash: B256 = binding.eip712_signing_hash(&domain);

        let sig = signer.sign_hash_sync(&signing_hash).unwrap();
        let recovered = sig.recover_address_from_prehash(&signing_hash).unwrap();

        assert_eq!(
            recovered, written,
            "recovered EIP-712 signer must match the keystore-derived address"
        );
    }

    /// Lock the EIP-712 type hash to ADR 003's canonical wording. If this
    /// breaks, either the ADR changed or the `sol!` macro's canonical
    /// encoding shifted — both warrant a coordinated update with the
    /// Solidity contract. Mirrors the equivalent test in
    /// `voucher::voucher_type_hash_matches_adr_003`.
    #[test]
    fn bind_node_id_type_hash_matches_adr_003() {
        // Single space between Solidity type and field name; no other
        // whitespace; fields in declaration order. Per ADR 003 §606.
        let canonical: &[u8] = b"BindNodeId(bytes32 nodeId,uint64 nonce)";
        let expected = keccak256(canonical);
        let actual = keccak256(BindNodeId::eip712_root_type().as_bytes());
        assert_eq!(
            actual, expected,
            "BindNodeId type hash drifted from ADR 003 §EIP-712 NodeId-to-Ethereum Binding"
        );
    }
}
