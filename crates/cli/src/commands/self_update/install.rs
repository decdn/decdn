//! Replacing the running binary: whose install it is, what the archive
//! holds, and the staged swap.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use tempfile::NamedTempFile;

use super::release::ArchiveKind;

/// The file name the release archive holds at its root.
const BIN_NAME: &str = if cfg!(windows) { "decdn.exe" } else { "decdn" };

/// Whether `decdn self-update` may replace the running binary.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Ownership {
    /// The user can write the binary's directory and no package manager
    /// owns it: replace it in place.
    Replaceable,
    /// `cargo install` placed the binary; the user re-runs that instead.
    CargoBin(PathBuf),
    /// The binary's directory cannot be written by this user (the
    /// container image, an operator-managed fleet path, a distro package).
    NotWritable {
        /// The running binary.
        path: PathBuf,
        /// The OS reason the directory refused a write.
        reason: String,
    },
}

/// Decide [`Ownership`] for the running binary at `exe`. `cargo_bins` are
/// the cargo bin directories to refuse; `probe_writable` tries a write in a
/// directory and reports the OS error when it fails.
pub(crate) fn classify(
    exe: &Path,
    cargo_bins: &[PathBuf],
    probe_writable: impl FnOnce(&Path) -> std::io::Result<()>,
) -> Ownership {
    if cargo_bins.iter().any(|dir| exe.starts_with(dir)) {
        return Ownership::CargoBin(exe.to_path_buf());
    }
    let dir = exe.parent().unwrap_or(exe);
    match probe_writable(dir) {
        Ok(()) => Ownership::Replaceable,
        Err(e) => Ownership::NotWritable {
            path: exe.to_path_buf(),
            reason: e.to_string(),
        },
    }
}

/// The directories `cargo install` writes binaries to: `~/.cargo/bin` and,
/// when set, `$CARGO_HOME/bin`.
pub(crate) fn cargo_bin_dirs(home: &Path, cargo_home: Option<&Path>) -> Vec<PathBuf> {
    let mut dirs = vec![home.join(".cargo").join("bin")];
    if let Some(cargo_home) = cargo_home {
        let dir = cargo_home.join("bin");
        if !dirs.contains(&dir) {
            dirs.push(dir);
        }
    }
    dirs
}

/// The running binary's path, with symlinks resolved so the directory
/// probed and renamed into is the one that holds the file.
pub(crate) fn running_exe() -> anyhow::Result<PathBuf> {
    let exe = std::env::current_exe().context("locate the running executable")?;
    exe.canonicalize()
        .with_context(|| format!("resolve {}", exe.display()))
}

/// Try to create a file in `dir`, as the swap will.
pub(crate) fn probe_writable(dir: &Path) -> std::io::Result<()> {
    tempfile::Builder::new()
        .prefix(".decdn-update-probe-")
        .tempfile_in(dir)
        .map(drop)
}

/// The bare binary a release archive holds at its root, at most `max_bytes`.
pub(crate) fn extract_binary(
    archive: &[u8],
    kind: ArchiveKind,
    max_bytes: u64,
) -> anyhow::Result<Vec<u8>> {
    match kind {
        ArchiveKind::TarGz => extract_tar_gz(archive, max_bytes),
        ArchiveKind::Zip => extract_zip(archive, max_bytes),
    }
}

fn extract_tar_gz(archive: &[u8], max_bytes: u64) -> anyhow::Result<Vec<u8>> {
    let gz = flate2::read::GzDecoder::new(archive);
    let mut tar = tar::Archive::new(gz);
    for entry in tar.entries().context("read the release archive")? {
        let mut entry = entry.context("read the release archive")?;
        let path = entry.path().context("read an archive entry name")?;
        let name = path.to_string_lossy();
        if name.strip_prefix("./").unwrap_or(&name) != BIN_NAME {
            continue;
        }
        let size = entry.size();
        if size > max_bytes {
            bail!(
                "{BIN_NAME} in the archive is {size} bytes, larger than the {max_bytes}-byte limit"
            );
        }
        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .context("read the binary out of the archive")?;
        return Ok(bytes);
    }
    bail!("the release archive holds no `{BIN_NAME}` entry at its root")
}

#[cfg(windows)]
fn extract_zip(archive: &[u8], max_bytes: u64) -> anyhow::Result<Vec<u8>> {
    let mut zip =
        zip::ZipArchive::new(std::io::Cursor::new(archive)).context("read the release archive")?;
    let mut entry = match zip.by_name(BIN_NAME) {
        Ok(entry) => entry,
        Err(zip::result::ZipError::FileNotFound) => {
            bail!("the release archive holds no `{BIN_NAME}` entry at its root")
        }
        Err(e) => return Err(e).context("read the release archive"),
    };
    let size = entry.size();
    if size > max_bytes {
        bail!("{BIN_NAME} in the archive is {size} bytes, larger than the {max_bytes}-byte limit");
    }
    let mut bytes = Vec::new();
    entry
        .read_to_end(&mut bytes)
        .context("read the binary out of the archive")?;
    Ok(bytes)
}

#[cfg(not(windows))]
fn extract_zip(_archive: &[u8], _max_bytes: u64) -> anyhow::Result<Vec<u8>> {
    bail!("zip release archives are for Windows builds only")
}

/// Write `bytes` to a new executable file beside the running binary, so the
/// final rename stays on one filesystem and is atomic.
pub(crate) fn stage(dir: &Path, bytes: &[u8]) -> anyhow::Result<NamedTempFile> {
    let mut staged = tempfile::Builder::new()
        .prefix(".decdn-update-")
        .tempfile_in(dir)
        .with_context(|| format!("create a temporary file in {}", dir.display()))?;
    staged
        .write_all(bytes)
        .and_then(|()| staged.as_file().sync_all())
        .with_context(|| format!("write {}", staged.path().display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(staged.path(), std::fs::Permissions::from_mode(0o755))
            .with_context(|| format!("mark {} executable", staged.path().display()))?;
    }
    Ok(staged)
}

/// Replace the running executable with the staged file. On Unix this is a
/// rename over the running path; on Windows the running `.exe` is moved
/// aside first, because it cannot be overwritten while it runs.
pub(crate) fn swap(staged: &Path) -> anyhow::Result<()> {
    self_replace::self_replace(staged)
        .with_context(|| format!("replace the running binary with {}", staged.display()))
}

/// The sponsored installer's marker for the `decdn` binary under `data_dir`.
pub(crate) fn installer_marker_path(data_dir: &Path) -> PathBuf {
    data_dir.join("installer").join("decdn")
}

/// The marker line the sponsored installer writes: the SHA-256 of the
/// release's `SHA256SUMS`, a space, the target triple. The installer skips
/// a release whose marker matches, so a self-updated binary stays.
pub(crate) fn marker_line(sums_sha256: &str, triple: &str) -> String {
    format!("{sums_sha256} {triple}\n")
}

/// Rewrite `marker` atomically when it exists. Returns whether it did.
pub(crate) fn rewrite_marker_if_present(marker: &Path, line: &str) -> std::io::Result<bool> {
    if !marker.is_file() {
        return Ok(false);
    }
    let dir = marker.parent().unwrap_or(marker);
    let mut tmp = NamedTempFile::new_in(dir)?;
    tmp.write_all(line.as_bytes())?;
    tmp.as_file().sync_all()?;
    tmp.persist(marker).map_err(|e| e.error)?;
    Ok(true)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
