//! Node identity: load or generate a persistent ed25519 `SecretKey`.
//!
//! The key is stored as 32 raw bytes at `<data_dir>/node.secret`. On Unix the
//! temp file is opened with mode `0600` from creation (no umask-window where
//! the key material is world-readable) and atomically renamed into place.
//!
//! On Unix, permissions are validated on every load to block key-replacement
//! attacks: `data_dir` must be a directory with no group or world permission
//! bits (e.g. `0o700`), and `node.secret` must be a regular file with no group
//! or world permission bits (e.g. `0o600`). Setuid/setgid/sticky bits are not
//! inspected. A world- or group-writable `data_dir` would let any local
//! process replace `node.secret` between load and use. On first run the
//! directory is created with `0o700` via `DirBuilder::mode` (no umask window).
//! Non-Unix platforms skip these checks — POSIX mode bits do not map
//! meaningfully to NTFS ACLs.
//!
//! New keys are built via [`SecretKey::generate`], which draws from
//! the auto-seeded thread-local CSPRNG backed by OS entropy.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use iroh::{PublicKey, SecretKey, Signature};
use rand::Rng;

const KEY_FILE_NAME: &str = "node.secret";
const KEY_LEN: usize = 32;

/// Mode used when creating `data_dir` on first run.
#[cfg(unix)]
const DATA_DIR_MODE: u32 = 0o700;
/// Any group or other bit set on `data_dir` or the key file is rejected. A
/// single mask for both: key material must be owner-only, and so must its
/// containing directory (a group-writable dir allows the same key-replacement
/// attack as a world-writable one, just with a smaller attacker set).
#[cfg(unix)]
const FORBIDDEN_BITS: u32 = 0o077;

/// Location of the persistent node key within `data_dir`.
pub fn key_path(data_dir: &Path) -> PathBuf {
    data_dir.join(KEY_FILE_NAME)
}

/// Load the node's `SecretKey` from disk, generating and persisting one if absent.
///
/// # Errors
///
/// Returns an error if the file exists but is a symlink or other non-regular
/// file, has the wrong size, or has insecure permissions; if `data_dir` is
/// not a directory or has insecure permissions; if the key path can't be
/// stat'd for reasons other than non-existence; if the directory cannot be
/// created; or if reading/writing fails.
pub fn load_or_generate(data_dir: &Path) -> anyhow::Result<SecretKey> {
    let path = key_path(data_dir);

    // Check `data_dir` first so a clear "invalid data_dir" error beats a
    // confusing "failed to stat key path" when `data_dir` is e.g. a regular
    // file (the `join` would make the key path unstat'able with ENOTDIR).
    // NotFound is fine — the create path below handles it.
    let data_dir_exists = match fs::symlink_metadata(data_dir) {
        Ok(_) => {
            validate_data_dir(data_dir)?;
            true
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => {
            return Err(anyhow::Error::new(e)
                .context(format!("failed to stat data_dir {}", data_dir.display())));
        }
    };

    // `symlink_metadata` rather than `path.exists()`: it returns `Ok` for a
    // dangling symlink (so `validate_key_file` rejects it instead of `rename`
    // overwriting the target), and we distinguish `NotFound` from other errors
    // so a storage EIO doesn't silently mask an existing key and trigger a
    // rogue regeneration.
    if data_dir_exists {
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                validate_key_file(&path)?;
                return load_from(&path);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(anyhow::Error::new(e)
                    .context(format!("failed to stat key path {}", path.display())));
            }
        }
    }

    create_data_dir_secure(data_dir)?;
    // Re-validate even after we just validated above: `create_data_dir_secure`
    // is a no-op on a pre-existing dir, so this branch runs on already-checked
    // state — but if the dir did not exist before, this is the *only* check.
    validate_data_dir(data_dir)?;

    let key = fresh_secret_key();
    write_atomic(&path, &key.to_bytes())?;
    tracing::info!(path = %path.display(), "generated new node secret key");
    Ok(key)
}

/// Validate (or create+validate) `data_dir` with `0o700` semantics: if it
/// exists, the permission policy is enforced; if it's `NotFound`, it's
/// created securely and then validated. Other stat errors propagate.
///
/// Public surface so `decdn_incentive::eth_identity::generate_and_persist`
/// (the eth keystore writer, #406) can share the same `data_dir` hardening
/// path as `node.secret`. The lower-level `validate_data_dir` /
/// `create_data_dir_secure` helpers stay `pub(crate)` — callers outside
/// this crate should reach for `ensure_data_dir` instead.
pub fn ensure_data_dir(data_dir: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(data_dir) {
        Ok(_) => validate_data_dir(data_dir),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            create_data_dir_secure(data_dir)?;
            validate_data_dir(data_dir)
        }
        Err(e) => Err(anyhow::Error::new(e)
            .context(format!("failed to stat data_dir {}", data_dir.display()))),
    }
}

/// Rename `path` to `<filename>.bak.<unix_ts>` so an in-place key rotation
/// preserves the prior key material rather than destroying it. Required by
/// `appendix-operator-key-rotation.md` §1 step 9 ("Archive the old iroh
/// keystore offline. Retain it for at least `MAX_EVIDENCE_AGE_US`") and the
/// §5 rollback path ("Repoint config at old keystore"). The operator can
/// later move the `.bak` file offline; deleting it is up to them.
///
/// Bails if the bak path already exists (two `key-gen` invocations within
/// the same second) rather than silently clobbering — operator removes the
/// stale archive manually and retries.
///
/// Returns the new bak path on success.
pub fn move_aside(path: &Path) -> anyhow::Result<PathBuf> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let unix_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("invalid path (no UTF-8 filename): {}", path.display()))?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("path has no parent directory: {}", path.display()))?;
    let bak = parent.join(format!("{file_name}.bak.{unix_ts}"));
    anyhow::ensure!(
        !bak.exists(),
        "refusing to overwrite existing archive {}; \
         move it aside manually and retry",
        bak.display()
    );
    fs::rename(path, &bak)
        .with_context(|| format!("failed to archive {} -> {}", path.display(), bak.display()))?;
    Ok(bak)
}

/// Install the staged temp file `tmp` at `target`, archiving any existing
/// `target` first. Shared commit primitive for the staged-key types
/// ([`StagedNodeKey`] and `decdn_incentive`'s `StagedKeystore`) so their
/// archive → rename → fail-safe-rollback logic stays in one place.
///
/// **Fail-safe.** If the install rename fails after the prior file was archived,
/// the archive is restored (best-effort) so `target` keeps the prior file rather
/// than going missing. The returned error distinguishes the two outcomes:
/// - restore succeeded → the prior file is live at `target`, safe to retry;
/// - restore also failed → `target` is now **missing**, and the error names the
///   surviving `.bak` archive the operator must move back manually.
///
/// Does **not** remove `tmp` on failure — the caller's `Drop` owns temp cleanup,
/// so the temp is reclaimed whether the caller `?`-propagates or not.
///
/// Returns the archive path if a prior file was moved aside, or `None` (fresh
/// install).
///
/// # Errors
///
/// Returns an error if archiving the prior file fails, or if the install rename
/// fails (with the best-effort restore outcome folded into the message).
pub fn install_staged(tmp: &Path, target: &Path) -> anyhow::Result<Option<PathBuf>> {
    let bak = if target.exists() {
        Some(move_aside(target)?)
    } else {
        None
    };
    let Err(rename_err) = fs::rename(tmp, target) else {
        return Ok(bak);
    };
    let base = format!("failed to rename {} -> {}", tmp.display(), target.display());
    // The install rename failed *after* any prior file was archived, which would
    // otherwise leave `target` missing. Restore the archive (best-effort) so a
    // failed rotation is fail-safe. The caller's `Drop` reclaims `tmp`.
    let Some(bak_path) = &bak else {
        // Fresh install: nothing was archived, so nothing to restore — `target`
        // was already absent and stays absent. Clean failure.
        return Err(anyhow::Error::new(rename_err).context(base));
    };
    match fs::rename(bak_path, target) {
        Ok(()) => Err(anyhow::Error::new(rename_err).context(format!(
            "{base}; the prior file was restored to {} and stays live (safe to retry)",
            target.display()
        ))),
        Err(restore_err) => Err(anyhow::Error::new(rename_err).context(format!(
            "{base}, AND restoring the archived prior file failed ({restore_err}): {} is now \
             MISSING — its only surviving copy is the archive at {}. Move it back manually before \
             starting the node (appendix-operator-key-rotation.md §5 rollback)",
            target.display(),
            bak_path.display()
        ))),
    }
}

/// A freshly-generated node key written to a temp file in `data_dir`, not yet
/// committed to `node.secret`.
///
/// Staging (generate + write the temp) is separated from committing
/// (archive-old + rename) so a multi-file rotation can generate **all** key
/// material before touching any canonical file — a failure while generating the
/// *second* secret then leaves the first untouched rather than half-rotated
/// (#844). Produced by [`stage_node_key`]; finished with [`Self::commit`].
///
/// Dropping a [`StagedNodeKey`] without committing removes the temp file, so an
/// abandoned stage never litters `data_dir`.
#[must_use = "a staged node key must be committed (or it is discarded on drop)"]
pub struct StagedNodeKey {
    tmp: PathBuf,
    final_path: PathBuf,
    key: SecretKey,
    committed: bool,
}

impl std::fmt::Debug for StagedNodeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never format the secret key.
        f.debug_struct("StagedNodeKey")
            .field("final_path", &self.final_path)
            .field("committed", &self.committed)
            .finish_non_exhaustive()
    }
}

impl StagedNodeKey {
    /// The public key of the staged secret, for logging the node id before the
    /// commit lands.
    #[must_use]
    pub fn public(&self) -> PublicKey {
        self.key.public()
    }

    /// Sign `msg` with the staged secret, without committing it.
    ///
    /// `decdn node rotate-key` needs this: `CapacityBond.bindNodeId` demands an
    /// ed25519 ownership proof from the key being bound, and that proof has to
    /// exist *before* the transaction is submitted — while committing
    /// `node.secret` has to happen *after* it confirms, or a reverted bind
    /// would leave the daemon serving under an unbound identity. Signing from
    /// the stage is what lets those two orders coexist.
    ///
    /// The secret itself stays unreachable: this is deliberately a signing
    /// oracle rather than a `secret()` accessor, so a staged key cannot be
    /// copied out and persisted somewhere the archive-on-commit discipline
    /// does not cover.
    #[must_use]
    pub fn sign(&self, msg: &[u8]) -> Signature {
        self.key.sign(msg)
    }

    /// Commit the staged key: archive any existing `node.secret` (so the
    /// operator key-rotation runbook keeps the prior material, mirroring the
    /// no-stage path) and atomically rename the temp into place.
    ///
    /// Returns the archive path if a prior key was moved aside, or `None` when
    /// there was nothing to archive (fresh install).
    ///
    /// Takes `&mut self` rather than `self` **so a failed commit does not
    /// destroy the key**. Consuming `self` meant the `?` below dropped the
    /// stage with `committed == false`, and `Drop` deleted the temp — on a
    /// caller path (`decdn node rotate-key`) that only reaches this call after
    /// proving on-chain that the binding moved to this key. The caller must
    /// stay able to [`Self::keep`] it instead. The cost is that a caller can
    /// now observe a committed stage; `commit` is idempotent-safe in that a
    /// second call would simply fail on the already-renamed temp.
    ///
    /// # Errors
    ///
    /// Returns an error if archiving the prior key or the install rename fails.
    /// The commit is **fail-safe** via [`install_staged`]: if the install rename
    /// fails after the prior key was archived, the archive is restored
    /// (best-effort) so the old key stays live, and the error states whether the
    /// restore succeeded or `node.secret` is now missing. On error `committed`
    /// stays `false`, so the caller may still [`Self::keep`] the staged key —
    /// and if it does not, `Drop` reclaims the temp.
    pub fn commit(&mut self) -> anyhow::Result<Option<PathBuf>> {
        let bak = install_staged(&self.tmp, &self.final_path)?;
        self.committed = true;
        Ok(bak)
    }

    /// Preserve the staged key *without* installing it, at
    /// `<data_dir>/node.secret.pending.<node_id>`. Returns the path it landed at.
    ///
    /// The third outcome, between [`Self::commit`] and the `Drop` that discards
    /// an abandoned stage. It exists for one situation, reached three ways: a
    /// key-rotation transaction may have taken effect, but the caller cannot
    /// confirm the binding moved — the receipt could not be read at all, or it
    /// mined carrying no matching `NodeIdBound`, or it mined and the install of
    /// the confirmed key then failed. Whether the chain now names this key is
    /// **unknown** in the first two, and *known* in the third — where the key is
    /// even more valuable. Discarding the secret there is unrecoverable — if the
    /// transaction did land, the operator is bound to an identity whose private
    /// key no longer exists anywhere, and the daemon keeps serving under a key
    /// nothing binds. Installing it is equally wrong, because the transaction
    /// may instead have failed.
    ///
    /// So the key is parked under a name that is deliberately *not*
    /// `node.secret`: no daemon will load it (`load_or_generate` reads exactly
    /// `<data_dir>/node.secret`), and the operator can install it once they have
    /// checked whether the transaction confirmed.
    ///
    /// # Errors
    ///
    /// Returns an error if the destination has no parent directory, or if the
    /// rename fails. In **both** cases the staged temp is deliberately left in
    /// place and its path is named in the error, rather than being reclaimed by
    /// `Drop`: this is only ever called when the key may already be bound
    /// on-chain, so an awkwardly-named surviving file beats an unrecoverable
    /// identity. The caller is expected to print that path.
    pub fn keep(mut self) -> anyhow::Result<PathBuf> {
        // Set FIRST, not after the rename: every exit from here on must leave
        // the temp on disk. The success path renames it (so `Drop` must not
        // chase the old path), and both failure paths need it preserved for the
        // reason in the doc above. This is the inverse of `commit`, where a
        // failure means the key is genuinely worthless.
        self.committed = true;
        let parent = self.final_path.parent().ok_or_else(|| {
            anyhow!(
                "staged key path has no parent directory: {}; the staged key is at {}",
                self.final_path.display(),
                self.tmp.display()
            )
        })?;
        let dest = parent.join(format!("{KEY_FILE_NAME}.pending.{}", self.key.public()));
        fs::rename(&self.tmp, &dest).with_context(|| {
            format!(
                "failed to park the staged node key at {}; it has been LEFT IN PLACE at {} — \
                 move it there yourself before starting the node",
                dest.display(),
                self.tmp.display()
            )
        })?;
        Ok(dest)
    }
}

impl Drop for StagedNodeKey {
    fn drop(&mut self) {
        if !self.committed {
            // Best-effort: an abandoned stage must not leave key material behind.
            let _ = fs::remove_file(&self.tmp);
        }
    }
}

/// Generate a fresh node key and write it to a temp file in `data_dir` (mode
/// `0o600` on Unix) **without** touching `node.secret`. Validates or securely
/// creates `data_dir` with the same `0o700` semantics as [`load_or_generate`].
///
/// The returned [`StagedNodeKey`] is committed via [`StagedNodeKey::commit`].
/// This is the staged counterpart of [`load_or_generate`]'s generate path, used
/// by `decdn key-gen` so the node key and the eth keystore are both generated
/// before either canonical file is replaced (#844).
///
/// # Errors
///
/// Returns an error if `data_dir` cannot be validated or created, or if writing
/// the temp file fails.
pub fn stage_node_key(data_dir: &Path) -> anyhow::Result<StagedNodeKey> {
    ensure_data_dir(data_dir)?;
    let final_path = key_path(data_dir);
    let key = fresh_secret_key();
    let tmp = write_temp(&final_path, &key.to_bytes())?;
    Ok(StagedNodeKey {
        tmp,
        final_path,
        key,
        committed: false,
    })
}

/// Reject `data_dir` if it isn't a directory or if any group/other permission
/// bit is set.
///
/// Uses `symlink_metadata` so symlinks at `data_dir` are rejected outright.
/// `metadata()` would follow the link and evaluate the target's mode, which
/// leaves a race where the symlink can be repointed between validation and
/// use — exercised by `rejects_symlinked_data_dir`.
#[cfg(unix)]
pub(crate) fn validate_data_dir(data_dir: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::symlink_metadata(data_dir)
        .with_context(|| format!("invalid data_dir: cannot access {}", data_dir.display()))?;
    anyhow::ensure!(
        meta.file_type().is_dir(),
        "invalid data_dir: {} is not a directory",
        data_dir.display()
    );
    let mode = meta.mode() & 0o777;
    anyhow::ensure!(
        mode & FORBIDDEN_BITS == 0,
        "invalid data_dir: {} has insecure permissions {:#o} \
         (group/other bits must be clear; expected {:#o})",
        data_dir.display(),
        mode,
        DATA_DIR_MODE
    );
    Ok(())
}

/// Reject the key file if it isn't a regular file or if any group/other
/// permission bit is set.
///
/// There is a small TOCTOU window between this check and `load_from`'s
/// `fs::read`, but exploiting it requires write access to `data_dir` — which
/// `validate_data_dir` already denies, so the two checks compose. Tightening
/// to an `O_NOFOLLOW` open would require a libc dep; deferred.
#[cfg(unix)]
fn validate_key_file(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::symlink_metadata(path)
        .with_context(|| format!("invalid node.secret: cannot access {}", path.display()))?;
    anyhow::ensure!(
        meta.file_type().is_file(),
        "invalid node.secret: {} is not a regular file",
        path.display()
    );
    let mode = meta.mode() & 0o777;
    anyhow::ensure!(
        mode & FORBIDDEN_BITS == 0,
        "invalid node.secret: {} has insecure permissions {:#o} \
         (must be user-only, e.g. {:#o})",
        path.display(),
        mode,
        0o600
    );
    Ok(())
}

/// Create `data_dir` (and any missing parents) with mode `0o700` on the leaf
/// created by this call. `DirBuilder::mode` only applies to directories this
/// call creates — pre-existing parents like `~/.local/share` keep their own
/// permissions.
#[cfg(unix)]
pub(crate) fn create_data_dir_secure(data_dir: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new()
        .recursive(true)
        .mode(DATA_DIR_MODE)
        .create(data_dir)
        .with_context(|| format!("failed to create data dir {}", data_dir.display()))
}

#[cfg(not(unix))]
pub(crate) fn validate_data_dir(_data_dir: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn validate_key_file(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn create_data_dir_secure(data_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create data dir {}", data_dir.display()))
}

/// Load the node's `SecretKey` from an existing file.
///
/// Callers must validate `path` via [`validate_key_file`] first — this
/// function follows symlinks via `fs::read`.
///
/// # Errors
///
/// Returns an error if the file cannot be read or is not exactly 32 bytes.
fn load_from(path: &Path) -> anyhow::Result<SecretKey> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read secret key file {}", path.display()))?;
    let len = bytes.len();
    let arr: [u8; KEY_LEN] = bytes.as_slice().try_into().map_err(|_| {
        anyhow!(
            "secret key file {} must be {KEY_LEN} bytes, got {len}",
            path.display()
        )
    })?;
    Ok(SecretKey::from_bytes(&arr))
}

pub fn fresh_secret_key() -> SecretKey {
    SecretKey::generate()
}

/// Write `bytes` to `path` atomically. On Unix the temp file is created with
/// mode 0600 from the start (via `OpenOptionsExt::mode`) so the key material is
/// never briefly exposed under a permissive umask.
fn write_atomic(path: &Path, bytes: &[u8; KEY_LEN]) -> anyhow::Result<()> {
    let tmp = write_temp(path, bytes)?;
    if let Err(e) = fs::rename(&tmp, path)
        .with_context(|| format!("failed to rename {} -> {}", tmp.display(), path.display()))
    {
        // The rename never happened, so the staged temp is still on disk —
        // remove it so a partial write doesn't accumulate in `data_dir`.
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

/// Write `bytes` to a freshly-created `<path>.tmp.<suffix>` sibling (mode 0600
/// on Unix from creation, then fsync'd) and return that temp path **without**
/// renaming it into place. The caller commits by renaming `tmp` → `path`, or
/// drops it by removing the file. This is the staging half of [`write_atomic`],
/// shared with [`stage_node_key`] so a multi-file rotation can generate all key
/// material before touching any canonical file (#844).
///
/// On any error the temp file is cleaned up before returning.
fn write_temp(path: &Path, bytes: &[u8; KEY_LEN]) -> anyhow::Result<PathBuf> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("key path {} has no parent directory", path.display()))?;
    // Unique temp name per writer: pid + 8 random hex chars. Avoids the race
    // where two concurrent writers clobber each other's temp file. The rename
    // step (in the caller) is atomic on Unix, so only one final `path` will exist.
    let suffix = {
        let mut s = [0u8; 4];
        rand::rng().fill_bytes(&mut s);
        format!(
            "{:08x}{:02x}{:02x}{:02x}{:02x}",
            std::process::id(),
            s[0],
            s[1],
            s[2],
            s[3],
        )
    };
    let tmp = parent.join(format!(
        "{}.tmp.{suffix}",
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(KEY_FILE_NAME),
    ));

    // Closure lets us funnel every fallible step through a single cleanup
    // path: on any error after `create_new`, remove the tmp file so partial
    // writes don't accumulate in `data_dir`. Cleanup failures are ignored —
    // the real error is already in flight.
    let result: anyhow::Result<()> = (|| {
        #[cfg(unix)]
        {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&tmp)
                .with_context(|| format!("failed to create {}", tmp.display()))?;
            file.write_all(bytes)
                .with_context(|| format!("failed to write {}", tmp.display()))?;
            file.sync_all()
                .with_context(|| format!("failed to sync {}", tmp.display()))?;
        }

        #[cfg(not(unix))]
        {
            fs::write(&tmp, bytes).with_context(|| format!("failed to write {}", tmp.display()))?;
        }

        Ok(())
    })();

    match result {
        Ok(()) => Ok(tmp),
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e)
        }
    }
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

    #[cfg(unix)]
    fn chmod(path: &Path, mode: u32) -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
        Ok(())
    }

    // Some CI runners (and this dev workstation) run under umask 002, so
    // `tempfile::tempdir()` comes up with mode 0o775 — which the new validator
    // rejects. Return a tempdir that's already 0o700 so tests exercising the
    // happy path don't need to repeat the chmod.
    fn secure_tempdir() -> anyhow::Result<tempfile::TempDir> {
        let dir = tempfile::tempdir()?;
        #[cfg(unix)]
        chmod(dir.path(), 0o700)?;
        Ok(dir)
    }

    /// Every `*.tmp.*` staging file left in `dir`.
    fn tmp_files(dir: &Path) -> Vec<PathBuf> {
        fs::read_dir(dir)
            .expect("read dir")
            .filter_map(|e| {
                let p = e.expect("dir entry").path();
                let name = p.file_name()?.to_str()?.to_string();
                name.contains(".tmp.").then_some(p)
            })
            .collect()
    }

    /// The core `keep()` contract, and the one that fails silently if the
    /// `committed`-flag ordering ever regresses: `keep` would still return
    /// `Ok(path)` and the caller would still print "preserved at …", pointing
    /// at a file `Drop` had just deleted.
    #[test]
    fn keep_parks_the_key_and_it_survives_drop() -> anyhow::Result<()> {
        let dir = secure_tempdir()?;
        let staged = stage_node_key(dir.path())?;
        let public = staged.public();

        let parked = staged.keep()?;
        // `staged` is consumed and dropped by here — this is the assertion.
        assert!(parked.exists(), "the parked key must outlive the stage");
        assert_eq!(
            parked.file_name().and_then(|s| s.to_str()),
            Some(format!("{KEY_FILE_NAME}.pending.{public}").as_str()),
        );
        assert!(
            !key_path(dir.path()).exists(),
            "parking must NOT install the key as node.secret — the whole point \
             is that no daemon loads it"
        );
        assert!(tmp_files(dir.path()).is_empty(), "no staging litter");

        // The parked bytes must be the secret for the id in the filename, or
        // "move this over node.secret" bricks the node.
        let bytes = fs::read(&parked)?;
        let raw: [u8; KEY_LEN] = bytes.as_slice().try_into().expect("32 raw bytes");
        assert_eq!(SecretKey::from_bytes(&raw).public(), public);
        Ok(())
    }

    /// The inverse of `commit`'s policy: `keep` is only ever called when the key
    /// may already be bound on-chain, so a failure must leave the material on
    /// disk and name it, not reclaim it.
    #[test]
    fn keep_leaves_the_temp_in_place_when_the_rename_fails() -> anyhow::Result<()> {
        let dir = secure_tempdir()?;
        let staged = stage_node_key(dir.path())?;
        let tmp = staged.tmp.clone();

        // Make the destination un-renameable by putting a *directory* where the
        // parked file would go. `fs::rename` of a file onto a non-empty dir path
        // fails on every platform we target.
        let dest = dir
            .path()
            .join(format!("{KEY_FILE_NAME}.pending.{}", staged.public()));
        fs::create_dir(&dest)?;
        fs::write(dest.join("occupied"), b"x")?;

        let err = staged
            .keep()
            .expect_err("rename onto a non-empty dir fails");
        assert!(
            tmp.exists(),
            "a key that may already be bound on-chain must not be deleted when parking fails"
        );
        assert!(
            format!("{err:#}").contains(&tmp.display().to_string()),
            "the error must name the surviving path: {err:#}"
        );
        Ok(())
    }

    /// A stage that is neither committed nor kept must leave nothing behind —
    /// the pre-existing `Drop` contract, stated directly now that there is a
    /// third outcome it has to stay distinct from.
    #[test]
    fn an_abandoned_stage_leaves_nothing() -> anyhow::Result<()> {
        let dir = secure_tempdir()?;
        drop(stage_node_key(dir.path())?);
        assert!(tmp_files(dir.path()).is_empty());
        assert!(!key_path(dir.path()).exists());
        Ok(())
    }

    /// `commit` keeps the opposite policy to `keep`, and the difference is
    /// deliberate: a failed install means the key was never bound on-chain, so
    /// reclaiming it is right. Pinned so the two policies cannot be "unified"
    /// by mistake — `keep`'s whole point is that it does NOT do this.
    #[cfg(unix)]
    #[test]
    fn a_failed_commit_still_reclaims_the_temp() -> anyhow::Result<()> {
        let dir = secure_tempdir()?;
        let mut staged = stage_node_key(dir.path())?;
        let tmp = staged.tmp.clone();
        // A pre-existing `node.secret` forces `install_staged` through
        // `move_aside` first, and a read-only data dir makes that archive
        // rename fail — the realistic shape (a permissions problem), rather
        // than deleting the temp, which would make the assertion vacuous.
        fs::write(key_path(dir.path()), [0u8; KEY_LEN])?;
        chmod(dir.path(), 0o500)?;

        let failed = staged.commit();

        // Restore write access BEFORE the drop, or `Drop`'s cleanup is the
        // thing that fails and the assertion below tests the chmod, not the
        // policy.
        chmod(dir.path(), 0o700)?;
        failed.expect_err("archiving into a read-only dir must fail");
        drop(staged);
        assert!(!tmp.exists(), "a never-bound key is reclaimed on drop");
        Ok(())
    }

    #[test]
    fn generates_and_reloads() -> anyhow::Result<()> {
        let dir = secure_tempdir()?;
        let k1 = load_or_generate(dir.path())?;
        let k2 = load_or_generate(dir.path())?;
        assert_eq!(k1.to_bytes(), k2.to_bytes());
        Ok(())
    }

    #[test]
    fn rejects_wrong_size() -> anyhow::Result<()> {
        let dir = secure_tempdir()?;
        let path = key_path(dir.path());
        fs::write(&path, b"too short")?;
        // `fs::write` respects umask, typically producing 0o644 — which the
        // new `validate_key_file` rejects before the size check runs. Tighten
        // to 0o600 so this test continues to exercise the size-rejection path.
        #[cfg(unix)]
        chmod(&path, 0o600)?;
        let err = load_or_generate(dir.path()).expect_err("should reject wrong-sized key file");
        assert!(
            err.to_string().contains("32 bytes")
                || err.chain().any(|c| c.to_string().contains("32 bytes")),
            "expected size error, got: {err:#}"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn accepts_0700_data_dir() -> anyhow::Result<()> {
        let dir = secure_tempdir()?;
        let k1 = load_or_generate(dir.path())?;
        let k2 = load_or_generate(dir.path())?;
        assert_eq!(k1.to_bytes(), k2.to_bytes());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_world_writable_data_dir() -> anyhow::Result<()> {
        assert_data_dir_mode_rejected(0o777)
    }

    // Locks in the strict-mode policy: `0o770` still allows any group member
    // to replace `node.secret`, which is the same attack class as 0o777.
    #[cfg(unix)]
    #[test]
    fn rejects_group_writable_data_dir() -> anyhow::Result<()> {
        assert_data_dir_mode_rejected(0o770)
    }

    // The next three lock the `FORBIDDEN_BITS = 0o077` mask against a
    // regression that narrows it to writable-only (which would silently
    // accept group/world-*readable* but not writable dirs). Each mode sets
    // exactly one forbidden bit — no overlap.
    #[cfg(unix)]
    #[test]
    fn rejects_group_readable_data_dir() -> anyhow::Result<()> {
        assert_data_dir_mode_rejected(0o740)
    }

    #[cfg(unix)]
    #[test]
    fn rejects_world_readable_data_dir() -> anyhow::Result<()> {
        assert_data_dir_mode_rejected(0o704)
    }

    #[cfg(unix)]
    #[test]
    fn rejects_world_exec_only_data_dir() -> anyhow::Result<()> {
        assert_data_dir_mode_rejected(0o701)
    }

    #[cfg(unix)]
    fn assert_data_dir_mode_rejected(mode: u32) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        chmod(dir.path(), mode)?;
        let err = load_or_generate(dir.path())
            .expect_err(&format!("should reject data_dir mode {mode:#o}"));
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid data_dir"), "unexpected: {msg}");
        assert!(msg.contains("insecure permissions"), "unexpected: {msg}");
        Ok(())
    }

    // Catches a refactor that replaced `is_dir()` with `!is_file()` — a
    // regular file would pass the latter even though it's not a directory.
    #[cfg(unix)]
    #[test]
    fn rejects_file_as_data_dir() -> anyhow::Result<()> {
        let parent = secure_tempdir()?;
        let file = parent.path().join("not_a_dir");
        fs::write(&file, b"")?;
        chmod(&file, 0o600)?;
        let err = load_or_generate(&file).expect_err("should reject file as data_dir");
        let msg = format!("{err:#}");
        assert!(msg.contains("is not a directory"), "unexpected: {msg}");
        Ok(())
    }

    // Symmetric counterpart to `rejects_group_readable_data_dir`: guards the
    // key-file side of the `0o077` mask against a writable-only narrowing.
    #[cfg(unix)]
    #[test]
    fn rejects_world_readable_key_file() -> anyhow::Result<()> {
        let dir = secure_tempdir()?;
        let path = key_path(dir.path());
        fs::write(&path, [0u8; KEY_LEN])?;
        chmod(&path, 0o604)?;
        let err = load_or_generate(dir.path()).expect_err("should reject world-readable key");
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid node.secret"), "unexpected: {msg}");
        assert!(msg.contains("0o604"), "expected mode in message: {msg}");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_key_file_with_group_access() -> anyhow::Result<()> {
        let dir = secure_tempdir()?;
        let path = key_path(dir.path());
        fs::write(&path, [0u8; KEY_LEN])?;
        chmod(&path, 0o640)?;
        let err = load_or_generate(dir.path()).expect_err("should reject group-readable key");
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid node.secret"), "unexpected: {msg}");
        assert!(msg.contains("0o640"), "expected mode in message: {msg}");
        Ok(())
    }

    // A symlink at the key path — even to a well-formed 32-byte file with
    // mode 0o600 — must be rejected. Without this check, an attacker with
    // write access to `data_dir` could replace `node.secret` with a symlink
    // pointing at another user's key.
    #[cfg(unix)]
    #[test]
    fn rejects_non_regular_key_file() -> anyhow::Result<()> {
        let dir = secure_tempdir()?;
        let target_dir = secure_tempdir()?;
        let target = target_dir.path().join("attacker_key");
        let sentinel = [0xAAu8; KEY_LEN];
        fs::write(&target, sentinel)?;
        chmod(&target, 0o600)?;
        std::os::unix::fs::symlink(&target, key_path(dir.path()))?;
        let err = load_or_generate(dir.path()).expect_err("should reject symlinked key");
        let msg = format!("{err:#}");
        assert!(msg.contains("not a regular file"), "unexpected: {msg}");
        // Confirm the symlink target wasn't overwritten by a key-gen fallback.
        assert_eq!(fs::read(&target)?, sentinel, "target was modified");
        Ok(())
    }

    // Documents the "check the path, not the target" tradeoff: a symlink at
    // `data_dir` itself is rejected by the `is_dir()` check on
    // `symlink_metadata`, even if the target is a valid 0o700 directory.
    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_data_dir() -> anyhow::Result<()> {
        let parent = secure_tempdir()?;
        let real = parent.path().join("real");
        fs::create_dir(&real)?;
        chmod(&real, 0o700)?;
        let link = parent.path().join("link");
        std::os::unix::fs::symlink(&real, &link)?;
        let err = load_or_generate(&link).expect_err("should reject symlinked data_dir");
        let msg = format!("{err:#}");
        assert!(msg.contains("is not a directory"), "unexpected: {msg}");
        Ok(())
    }

    // Guards against a regression where a dangling symlink at `node.secret`
    // would be treated as "not existing" by a plain `path.exists()` check and
    // then silently overwritten via `rename`, destroying whatever the
    // attacker pointed at.
    #[cfg(unix)]
    #[test]
    fn rejects_dangling_symlink_at_key_path() -> anyhow::Result<()> {
        let dir = secure_tempdir()?;
        let nowhere = dir.path().join("does_not_exist");
        std::os::unix::fs::symlink(&nowhere, key_path(dir.path()))?;
        let err = load_or_generate(dir.path()).expect_err("should reject dangling symlink");
        let msg = format!("{err:#}");
        assert!(msg.contains("not a regular file"), "unexpected: {msg}");
        // Confirm nothing was written to the symlink target.
        assert!(
            !nowhere.exists(),
            "load_or_generate must not follow the symlink"
        );
        Ok(())
    }

    // Guards the `DirBuilder::mode(0o700)` call against a future refactor
    // that drops `DirBuilderExt`. Stat the created leaf explicitly rather
    // than trusting the OS umask.
    #[cfg(unix)]
    #[test]
    fn creates_dir_with_0700_on_first_run() -> anyhow::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let parent = secure_tempdir()?;
        let sub = parent.path().join("sub");
        let leaf = sub.join("data");
        load_or_generate(&leaf)?;
        // Both the leaf and any intermediate dir this call created must be
        // 0o700 — `DirBuilderExt::mode` applies to every dir the single
        // `create` call materializes. Checking both guards against a future
        // refactor that switched to `create_dir_all` (which ignores the mode
        // on intermediates).
        let leaf_mode = fs::metadata(&leaf)?.mode() & 0o777;
        let sub_mode = fs::metadata(&sub)?.mode() & 0o777;
        assert_eq!(leaf_mode, 0o700, "leaf: expected 0o700, got {leaf_mode:#o}");
        assert_eq!(sub_mode, 0o700, "sub: expected 0o700, got {sub_mode:#o}");
        Ok(())
    }
}
