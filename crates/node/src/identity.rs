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
//! New keys are built by drawing 32 random bytes from `rand::rng()`
//! (rand 0.10's auto-seeded `ThreadRng`, backed by OS entropy via
//! `getrandom`) and feeding them to [`SecretKey::from_bytes`]. We go
//! through raw bytes rather than `SecretKey::generate(&mut rand::rng())`
//! because iroh 0.97 still pins `rand_core 0.9`, so rand 0.10's
//! `ThreadRng` does not satisfy iroh's `CryptoRng` bound — the
//! `rand_core` trait lives in two incompatible versions in the dep
//! graph.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use iroh::SecretKey;
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

/// Reject `data_dir` if it isn't a directory or if any group/other permission
/// bit is set.
///
/// Uses `symlink_metadata` so symlinks at `data_dir` are rejected outright.
/// `metadata()` would follow the link and evaluate the target's mode, which
/// leaves a race where the symlink can be repointed between validation and
/// use — exercised by `rejects_symlinked_data_dir`.
#[cfg(unix)]
fn validate_data_dir(data_dir: &Path) -> anyhow::Result<()> {
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
fn create_data_dir_secure(data_dir: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new()
        .recursive(true)
        .mode(DATA_DIR_MODE)
        .create(data_dir)
        .with_context(|| format!("failed to create data dir {}", data_dir.display()))
}

#[cfg(not(unix))]
fn validate_data_dir(_data_dir: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn validate_key_file(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn create_data_dir_secure(data_dir: &Path) -> anyhow::Result<()> {
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

/// Build a fresh `SecretKey` from 32 random bytes drawn from
/// [`rand::rng()`]. See module-level docs for why we don't use
/// `SecretKey::generate` directly.
pub(crate) fn fresh_secret_key() -> SecretKey {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    SecretKey::from_bytes(&bytes)
}

/// Write `bytes` to `path` atomically. On Unix the temp file is created with
/// mode 0600 from the start (via `OpenOptionsExt::mode`) so the key material is
/// never briefly exposed under a permissive umask.
fn write_atomic(path: &Path, bytes: &[u8; KEY_LEN]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("key path {} has no parent directory", path.display()))?;
    // Unique temp name per writer: pid + 8 random hex chars. Avoids the race
    // where two concurrent writers clobber each other's temp file. The rename
    // step is still atomic on Unix, so only one final `path` will exist.
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

        fs::rename(&tmp, path)
            .with_context(|| format!("failed to rename {} -> {}", tmp.display(), path.display()))?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
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
