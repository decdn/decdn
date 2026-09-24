//! Per-download state: a throwaway voucher-signing key and the capability
//! the sponsor issued to it, kept under `<data_dir>/downloads/<hash>/`.
//!
//! Each download gets its own key, so the user never manages a wallet: the
//! key holds no funds, is never shown, and its password is random and stored
//! beside it. The directory survives an interrupted pull, so re-running the
//! same command resumes with the same key and capability (no new captcha),
//! and is deleted once the pull succeeds. It is also the `--data-dir` handed
//! to `decdn`, so the buyer-channel store for this key goes with it.

use std::io::Write;
use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use anyhow::Context;
use decdn_incentive::CapabilityGrant;
use decdn_incentive::eth_identity;

const PASSWORD_FILE: &str = "password";
const CAPABILITY_FILE: &str = "capability";

/// Normalize a BLAKE3 hash as the website prints it (`b3:<hex>`, `0x<hex>`,
/// or bare hex) to 64 lowercase hex characters.
///
/// # Errors
///
/// Returns an error unless the remainder is exactly 64 hex characters.
pub fn normalize_hash(raw: &str) -> anyhow::Result<String> {
    let trimmed = raw.trim();
    let hex = trimmed
        .strip_prefix("b3:")
        .or_else(|| trimmed.strip_prefix("0x"))
        .unwrap_or(trimmed)
        .to_ascii_lowercase();
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        anyhow::bail!(
            "not a BLAKE3 hash (expected 64 hex characters, optionally b3:-prefixed): {raw}"
        );
    }
    Ok(hex)
}

/// One download's state directory.
#[derive(Debug)]
pub struct Session {
    dir: PathBuf,
}

impl Session {
    /// Open (creating with mode `0700` if needed) the state directory for
    /// `hash` under `root`. `hash` must already be normalized.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or secured.
    pub fn open(root: &Path, hash: &str) -> anyhow::Result<Self> {
        let dir = root.join("downloads").join(hash);
        create_private_dir(&dir)?;
        Ok(Self { dir })
    }

    /// The directory itself; `decdn`'s `--data-dir` for this download.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    #[must_use]
    pub fn keystore_path(&self) -> PathBuf {
        eth_identity::keystore_path(&self.dir)
    }

    #[must_use]
    pub fn password_path(&self) -> PathBuf {
        self.dir.join(PASSWORD_FILE)
    }

    #[must_use]
    pub fn capability_path(&self) -> PathBuf {
        self.dir.join(CAPABILITY_FILE)
    }

    /// Return this download's key address, generating the key (and its
    /// random password) on first use. A key whose password file is missing
    /// cannot sign, so it is replaced, together with the capability bound to
    /// it, instead of being handed to `decdn` to fail on.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be generated, written, or read.
    pub fn ensure_key(&self) -> anyhow::Result<Address> {
        let keystore = self.keystore_path();
        if keystore.exists() {
            if self.password_path().is_file() {
                return crate::keystore::read_address(&keystore);
            }
            remove_if_present(&keystore)?;
            remove_if_present(&self.capability_path())?;
        }
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|e| anyhow::anyhow!("read OS randomness: {e}"))?;
        let password = hex::encode(secret);
        write_private(&self.password_path(), password.as_bytes())?;
        eth_identity::generate_and_persist(&self.dir, &password, false)
            .context("generate throwaway download key")
    }

    /// The capability saved for this download, if any. A file whose contents
    /// are not a `dcap1:` token reads as `None`, so the caller requests a
    /// fresh one.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read.
    pub fn capability(&self) -> anyhow::Result<Option<CapabilityGrant>> {
        let path = self.capability_path();
        if !path.exists() {
            return Ok(None);
        }
        let token =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        Ok(CapabilityGrant::from_token(token.trim()).ok())
    }

    /// Persist the capability token issued to this download's key.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be written.
    pub fn save_capability(&self, token: &str) -> anyhow::Result<()> {
        write_private(&self.capability_path(), token.as_bytes())
    }

    /// Delete this download's state: the key, its password, its capability,
    /// and the buyer-channel store.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be removed.
    pub fn discard(self) -> anyhow::Result<()> {
        std::fs::remove_dir_all(&self.dir).with_context(|| format!("remove {}", self.dir.display()))
    }
}

/// Create `dir` (and missing parents) as mode `0700` from the start, so the
/// directory holding key material is never briefly world-readable. An
/// existing `dir` is tightened to `0700` as well.
fn create_private_dir(dir: &Path) -> anyhow::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(dir)
        .with_context(|| format!("create {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("chmod 0700 {}", dir.display()))?;
    }
    Ok(())
}

fn remove_if_present(path: &Path) -> anyhow::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            Err(e).with_context(|| format!("remove {}", path.display()))
        }
        _ => Ok(()),
    }
}

fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write {}", path.display()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const HEX: &str = "9194000d7b356650e6924a7746ec4afb0b705838b65913c0e46cba6b15af69e5";

    #[test]
    fn normalize_accepts_website_forms() {
        assert_eq!(normalize_hash(&format!("b3:{HEX}")).unwrap(), HEX);
        assert_eq!(normalize_hash(&format!("0x{HEX}")).unwrap(), HEX);
        assert_eq!(normalize_hash(&HEX.to_uppercase()).unwrap(), HEX);
    }

    #[test]
    fn normalize_rejects_non_hashes() {
        assert!(normalize_hash("mistral-7b").is_err());
        assert!(normalize_hash(&format!("b3:{}", &HEX[..63])).is_err());
        assert!(normalize_hash(&format!("b3:{}g", &HEX[..63])).is_err());
    }

    #[test]
    fn key_is_generated_once_and_reused() {
        let root = tempfile::tempdir().unwrap();
        let session = Session::open(root.path(), HEX).unwrap();
        let first = session.ensure_key().unwrap();
        let again = Session::open(root.path(), HEX)
            .unwrap()
            .ensure_key()
            .unwrap();
        assert_eq!(first, again);
        let pw = std::fs::read_to_string(session.password_path()).unwrap();
        assert_eq!(pw.len(), 64);
    }

    #[test]
    fn key_without_password_is_replaced_with_its_capability() {
        let root = tempfile::tempdir().unwrap();
        let session = Session::open(root.path(), HEX).unwrap();
        let first = session.ensure_key().unwrap();
        session.save_capability("dcap1:bound-to-first").unwrap();
        std::fs::remove_file(session.password_path()).unwrap();

        let second = session.ensure_key().unwrap();
        assert_ne!(first, second);
        assert!(session.password_path().is_file());
        assert!(!session.capability_path().exists());
    }

    #[cfg(unix)]
    #[test]
    fn state_dirs_are_created_private() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let session = Session::open(root.path(), HEX).unwrap();
        for dir in [session.dir(), root.path().join("downloads").as_path()] {
            let mode = std::fs::metadata(dir).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{}", dir.display());
        }
    }

    #[test]
    fn malformed_capability_reads_as_none_and_discard_removes_state() {
        let root = tempfile::tempdir().unwrap();
        let session = Session::open(root.path(), HEX).unwrap();
        assert!(session.capability().unwrap().is_none());
        session.save_capability("dcap1:not-a-token").unwrap();
        assert!(session.capability().unwrap().is_none());
        let dir = session.dir().to_path_buf();
        session.discard().unwrap();
        assert!(!dir.exists());
    }
}
