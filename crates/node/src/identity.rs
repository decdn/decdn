//! Node identity: load or generate a persistent ed25519 `SecretKey`.
//!
//! The key is stored as 32 raw bytes at `<data_dir>/node.secret`. On Unix the
//! temp file is opened with mode `0600` from creation (no umask-window where
//! the key material is world-readable) and atomically renamed into place.
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

/// Location of the persistent node key within `data_dir`.
pub fn key_path(data_dir: &Path) -> PathBuf {
    data_dir.join(KEY_FILE_NAME)
}

/// Load the node's `SecretKey` from disk, generating and persisting one if absent.
///
/// # Errors
///
/// Returns an error if the file exists but has the wrong size, if the directory
/// cannot be created, or if reading/writing fails.
pub fn load_or_generate(data_dir: &Path) -> anyhow::Result<SecretKey> {
    let path = key_path(data_dir);
    if path.exists() {
        return load_from(&path);
    }

    fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create data dir {}", data_dir.display()))?;

    let key = fresh_secret_key();
    write_atomic(&path, &key.to_bytes())?;
    tracing::info!(path = %path.display(), "generated new node secret key");
    Ok(key)
}

/// Load the node's `SecretKey` from an existing file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or is not exactly 32 bytes.
pub fn load_from(path: &Path) -> anyhow::Result<SecretKey> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read secret key file {}", path.display()))?;
    let arr: [u8; KEY_LEN] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("secret key file {} must be {KEY_LEN} bytes", path.display()))?;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_and_reloads() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let k1 = load_or_generate(dir.path())?;
        let k2 = load_or_generate(dir.path())?;
        assert_eq!(k1.to_bytes(), k2.to_bytes());
        Ok(())
    }

    #[test]
    fn rejects_wrong_size() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        fs::write(key_path(dir.path()), b"too short")?;
        assert!(load_or_generate(dir.path()).is_err());
        Ok(())
    }
}
