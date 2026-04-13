//! Node identity: load or generate a persistent ed25519 `SecretKey`.
//!
//! The key is stored as 32 raw bytes at `<data_dir>/node.secret` with file
//! mode 0600 on Unix. If the file does not exist, a new key is generated using
//! `OsRng` and written atomically.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use iroh::SecretKey;

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

    let key = SecretKey::generate(&mut rand::rng());
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

/// Write `bytes` to `path` atomically with mode 0600 on Unix.
fn write_atomic(path: &Path, bytes: &[u8; KEY_LEN]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("key path {} has no parent directory", path.display()))?;
    let tmp = parent.join(format!(
        "{}.tmp",
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(KEY_FILE_NAME)
    ));

    fs::write(&tmp, bytes).with_context(|| format!("failed to write {}", tmp.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o600);
        fs::set_permissions(&tmp, perms)
            .with_context(|| format!("failed to chmod {}", tmp.display()))?;
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
