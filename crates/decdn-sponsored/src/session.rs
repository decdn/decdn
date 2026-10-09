//! Per-download state: a throwaway voucher-signing key and the capability
//! the sponsor issued to it, kept under `<data_dir>/downloads/<hash>/`.
//!
//! Each download gets its own key, so the user never manages a wallet: the
//! key holds no funds, is never shown, and its password is random and stored
//! beside it. The directory survives an interrupted pull, so re-running the
//! same command resumes with the same key and capability (no new trip through the gate),
//! and is deleted once the pull succeeds.
//!
//! `decdn`'s `--data-dir` is `<data_dir>/decdn/`, one directory shared by
//! downloads, so the peers one download discovers serve the next. It holds
//! `decdn`'s peer store and buyer-channel store, never key material, and
//! outlives every download. `decdn` holds an exclusive lock on its
//! buyer-channel store for a whole pull, so a download reserves the shared
//! dir through `<data_dir>/decdn.lock`, and one started while another holds
//! it runs in its own directory instead.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use anyhow::Context;
use decdn_incentive::CapabilityGrant;
use decdn_incentive::eth_identity;
use sponsord_api::onramp::Profile;

const PASSWORD_FILE: &str = "password";
const ADDRESS_FILE: &str = "address";
const CAPABILITY_FILE: &str = "capability";
const PROFILE_FILE: &str = "profile.json";

/// Subdirectory of the root that downloads share as `decdn`'s `--data-dir`.
const DECDN_DATA_DIR: &str = "decdn";

/// File beside it whose lock reserves the shared dir for one `decdn` run.
const DECDN_LOCK_FILE: &str = "decdn.lock";

/// One download's state directory.
#[derive(Debug)]
pub struct Session {
    root: PathBuf,
    dir: PathBuf,
}

/// `decdn`'s `--data-dir` for one run, reserved until this is dropped.
#[derive(Debug)]
pub struct DecdnDataDir {
    path: PathBuf,
    /// Held (and released on drop) only for the shared dir.
    lock: Option<File>,
}

impl DecdnDataDir {
    /// The directory to pass to `decdn --data-dir`.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether this is the shared dir rather than the download's own.
    #[must_use]
    pub const fn is_shared(&self) -> bool {
        self.lock.is_some()
    }
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
        Ok(Self {
            root: root.to_path_buf(),
            dir,
        })
    }

    /// This download's own directory: key, password, capability, profile.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Reserve `decdn`'s `--data-dir` for one run: the shared
    /// `<root>/decdn/` (created `0700` if missing) when no other download
    /// holds it, otherwise this download's own directory. Keep the result
    /// alive until `decdn` exits. [`Session::discard`] never touches the
    /// shared dir.
    ///
    /// # Errors
    ///
    /// Returns an error if the shared directory cannot be created or secured.
    pub fn reserve_decdn_data_dir(&self) -> anyhow::Result<DecdnDataDir> {
        let shared = self.root.join(DECDN_DATA_DIR);
        create_private_dir(&shared)?;
        match lock_exclusive(&self.root.join(DECDN_LOCK_FILE)) {
            Some(lock) => Ok(DecdnDataDir {
                path: shared,
                lock: Some(lock),
            }),
            None => Ok(DecdnDataDir {
                path: self.dir.clone(),
                lock: None,
            }),
        }
    }

    /// This download's encrypted key (`keystore.json`), for `decdn --keystore`.
    #[must_use]
    pub fn keystore_path(&self) -> PathBuf {
        eth_identity::keystore_path(&self.dir)
    }

    /// The file holding the key's random password, for
    /// `decdn --keystore-password-file`.
    #[must_use]
    pub fn password_path(&self) -> PathBuf {
        self.dir.join(PASSWORD_FILE)
    }

    /// The file holding the `dcap1:` token issued to this download's key,
    /// for `decdn --capability-file`.
    #[must_use]
    pub fn capability_path(&self) -> PathBuf {
        self.dir.join(CAPABILITY_FILE)
    }

    /// Return this download's key address, generating the key (and its
    /// random password) on first use. The address is written beside the key
    /// at generation, so reading it needs no password. A key missing its
    /// password or address file is replaced, together with the capability
    /// bound to it, instead of being handed to `decdn` to fail on.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be generated, written, or read.
    pub fn ensure_key(&self) -> anyhow::Result<Address> {
        let keystore = self.keystore_path();
        if keystore.exists()
            && self.password_path().is_file()
            && let Some(address) = self.address()?
        {
            return Ok(address);
        }
        for stale in [keystore, self.address_path(), self.capability_path()] {
            remove_if_present(&stale)?;
        }
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|e| anyhow::anyhow!("read OS randomness: {e}"))?;
        let password = hex::encode(secret);
        write_private(&self.password_path(), password.as_bytes())?;
        let address = eth_identity::generate_and_persist(&self.dir, &password, false)
            .context("generate throwaway download key")?;
        write_private(&self.address_path(), address.to_string().as_bytes())?;
        Ok(address)
    }

    fn address_path(&self) -> PathBuf {
        self.dir.join(ADDRESS_FILE)
    }

    /// The address written beside the key, if it is there and parses.
    fn address(&self) -> anyhow::Result<Option<Address>> {
        let path = self.address_path();
        if !path.exists() {
            return Ok(None);
        }
        let text =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        Ok(text.trim().parse().ok())
    }

    /// The onramp profile this download last ran with, if saved: a fallback
    /// for resuming while the onramp is unreachable.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read.
    pub fn profile(&self) -> anyhow::Result<Option<Profile>> {
        let path = self.dir.join(PROFILE_FILE);
        if !path.exists() {
            return Ok(None);
        }
        let text =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        Ok(serde_json::from_str(&text).ok())
    }

    /// Save the onramp profile this download runs with.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be written.
    pub fn save_profile(&self, profile: &Profile) -> anyhow::Result<()> {
        let json = serde_json::to_vec(profile)?;
        write_private(&self.dir.join(PROFILE_FILE), &json)
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

    /// Delete this download's state: the key, its password, its capability
    /// and its profile. The shared `decdn` data dir stays.
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

/// Open `path` (mode `0600`) and take its exclusive lock without waiting.
/// `None` when another process holds it, or when it cannot be opened or
/// locked at all: the caller then falls back to a directory of its own.
fn lock_exclusive(path: &Path) -> Option<File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let file = opts.open(path).ok()?;
    file.try_lock().is_ok().then_some(file)
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
        let shared = session.reserve_decdn_data_dir().unwrap();
        for dir in [
            session.dir(),
            shared.path(),
            root.path().join("downloads").as_path(),
        ] {
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

    #[test]
    fn discard_keeps_the_shared_decdn_data_dir() {
        let root = tempfile::tempdir().unwrap();
        let session = Session::open(root.path(), HEX).unwrap();
        session.ensure_key().unwrap();
        let reserved = session.reserve_decdn_data_dir().unwrap();
        assert!(reserved.is_shared());
        let shared = reserved.path().to_path_buf();
        assert_eq!(shared, root.path().join(DECDN_DATA_DIR));
        assert!(!shared.starts_with(session.dir()));
        std::fs::create_dir(shared.join("peers")).unwrap();
        drop(reserved);

        let dir = session.dir().to_path_buf();
        session.discard().unwrap();
        assert!(!dir.exists());
        assert!(shared.join("peers").is_dir());
    }

    #[test]
    fn downloads_in_turn_share_the_data_dir_but_not_the_key() {
        let root = tempfile::tempdir().unwrap();
        let a = Session::open(root.path(), HEX).unwrap();
        let b = Session::open(root.path(), &"ab".repeat(32)).unwrap();
        let a_dir = a.reserve_decdn_data_dir().unwrap().path().to_path_buf();
        let b_dir = b.reserve_decdn_data_dir().unwrap().path().to_path_buf();
        assert_eq!(a_dir, b_dir);
        assert_ne!(a.keystore_path(), b.keystore_path());
        assert_ne!(a.ensure_key().unwrap(), b.ensure_key().unwrap());

        // A fresh key after discard (success or near expiry) is a new signer.
        let first = a.ensure_key().unwrap();
        a.discard().unwrap();
        let again = Session::open(root.path(), HEX).unwrap();
        assert_ne!(again.ensure_key().unwrap(), first);
    }

    #[test]
    fn a_concurrent_download_runs_in_its_own_dir() {
        let root = tempfile::tempdir().unwrap();
        let a = Session::open(root.path(), HEX).unwrap();
        let b = Session::open(root.path(), &"ab".repeat(32)).unwrap();
        let held = a.reserve_decdn_data_dir().unwrap();
        assert!(held.is_shared());

        let busy = b.reserve_decdn_data_dir().unwrap();
        assert!(!busy.is_shared());
        assert_eq!(busy.path(), b.dir());

        drop(held);
        assert!(b.reserve_decdn_data_dir().unwrap().is_shared());
    }

    #[test]
    fn key_without_its_address_file_is_replaced() {
        let root = tempfile::tempdir().unwrap();
        let session = Session::open(root.path(), HEX).unwrap();
        let first = session.ensure_key().unwrap();
        assert_eq!(
            std::fs::read_to_string(session.dir().join(ADDRESS_FILE)).unwrap(),
            first.to_string()
        );
        std::fs::remove_file(session.dir().join(ADDRESS_FILE)).unwrap();
        assert_ne!(session.ensure_key().unwrap(), first);
    }

    #[test]
    fn profile_round_trips() {
        let root = tempfile::tempdir().unwrap();
        let session = Session::open(root.path(), HEX).unwrap();
        assert!(session.profile().unwrap().is_none());
        let profile = Profile {
            chain_id: 1,
            rpc_url: "https://rpc".into(),
            payment_pool: Address::repeat_byte(1),
            capacity_bond: Address::repeat_byte(2),
            slash_judge: None,
            min_cli_version: None,
        };
        session.save_profile(&profile).unwrap();
        assert_eq!(session.profile().unwrap(), Some(profile));
    }
}
