//! Replacing the running binary: whose install it is, what the archive
//! holds, and the staged swap.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use tempfile::NamedTempFile;

use super::release::ArchiveKind;

/// Whether `decdn self-update` may replace the running binary.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Ownership {
    /// The user can write the binary's directory and no package manager
    /// owns it: replace it in place.
    Replaceable,
    /// `cargo install` or `cargo binstall` placed the binary; the user
    /// re-runs that instead.
    CargoBin,
    /// The binary's directory cannot be written by this user (the
    /// container image, an operator-managed fleet path, a distro package).
    /// Carries the OS reason.
    NotWritable(String),
}

/// Decide [`Ownership`] for the running binary at `exe`, which must be
/// canonical. `cargo_bins` are the canonical cargo bin directories to
/// refuse; `probe_writable` tries a write in a directory and reports the OS
/// reason when it fails.
pub(crate) fn classify(
    exe: &Path,
    cargo_bins: &[PathBuf],
    probe_writable: impl FnOnce(&Path) -> std::io::Result<()>,
) -> Ownership {
    if cargo_bins.iter().any(|dir| exe.starts_with(dir)) {
        return Ownership::CargoBin;
    }
    let dir = exe.parent().unwrap_or(exe);
    match probe_writable(dir) {
        Ok(()) => Ownership::Replaceable,
        Err(e) => Ownership::NotWritable(e.to_string()),
    }
}

/// The directories `cargo install` and `cargo binstall` write binaries to:
/// `~/.cargo/bin`, and `$CARGO_HOME/bin` and `$CARGO_INSTALL_ROOT/bin` when
/// set. Each one that exists is canonicalised, so it compares with the
/// canonical path of the running binary even through a symlinked cargo home.
pub(crate) fn cargo_bin_dirs(
    home: &Path,
    cargo_home: Option<&Path>,
    install_root: Option<&Path>,
) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    for root in [
        Some(home.join(".cargo")),
        cargo_home.map(Path::to_path_buf),
        install_root.map(Path::to_path_buf),
    ]
    .into_iter()
    .flatten()
    {
        let dir = root.join("bin");
        let dir = dir.canonicalize().unwrap_or(dir);
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

/// Try to create a file in `dir`, as the swap will. The error carries the
/// OS reason alone; the caller names the directory.
pub(crate) fn probe_writable(dir: &Path) -> std::io::Result<()> {
    tempfile::Builder::new()
        .prefix(".decdn-update-probe-")
        .tempfile_in(dir)
        .map(drop)
        .map_err(|e| match e.raw_os_error() {
            Some(code) => std::io::Error::from_raw_os_error(code),
            None => std::io::Error::from(e.kind()),
        })
}

/// The bare binary a release archive holds at its root, at most `max_bytes`.
pub(crate) fn extract_binary(
    archive: &[u8],
    kind: ArchiveKind,
    max_bytes: u64,
) -> anyhow::Result<Vec<u8>> {
    let bytes = match kind {
        ArchiveKind::TarGz => extract_tar_gz(archive, max_bytes)?,
        ArchiveKind::Zip => extract_zip(archive, max_bytes)?,
    };
    if bytes.is_empty() {
        bail!("`{}` in the archive is empty", kind.binary_name());
    }
    Ok(bytes)
}

fn extract_tar_gz(archive: &[u8], max_bytes: u64) -> anyhow::Result<Vec<u8>> {
    let bin = ArchiveKind::TarGz.binary_name();
    let gz = flate2::read::GzDecoder::new(archive);
    let mut tar = tar::Archive::new(gz);
    for entry in tar.entries().context("read the release archive")? {
        let entry = entry.context("read the release archive")?;
        let path = entry.path().context("read an archive entry name")?;
        let name = path.to_string_lossy();
        if name.strip_prefix("./").unwrap_or(&name) != bin {
            continue;
        }
        if !entry.header().entry_type().is_file() {
            bail!("`{bin}` in the archive is not a regular file");
        }
        let size = entry.size();
        return read_binary(entry, size, bin, max_bytes);
    }
    bail!("the release archive holds no `{bin}` entry at its root")
}

#[cfg(any(windows, test))]
fn extract_zip(archive: &[u8], max_bytes: u64) -> anyhow::Result<Vec<u8>> {
    let bin = ArchiveKind::Zip.binary_name();
    let mut zip =
        zip::ZipArchive::new(std::io::Cursor::new(archive)).context("read the release archive")?;
    let entry = match zip.by_name(bin) {
        Ok(entry) => entry,
        Err(zip::result::ZipError::FileNotFound) => {
            bail!("the release archive holds no `{bin}` entry at its root")
        }
        Err(e) => return Err(e).context("read the release archive"),
    };
    if !entry.is_file() {
        bail!("`{bin}` in the archive is not a regular file");
    }
    let size = entry.size();
    read_binary(entry, size, bin, max_bytes)
}

/// Read the archive entry `bin`, which declares `size` bytes, refusing it
/// when that exceeds `max_bytes`.
fn read_binary(
    mut entry: impl Read,
    size: u64,
    bin: &str,
    max_bytes: u64,
) -> anyhow::Result<Vec<u8>> {
    if size > max_bytes {
        bail!("`{bin}` in the archive is {size} bytes, larger than the {max_bytes}-byte limit");
    }
    let mut bytes = Vec::new();
    entry
        .read_to_end(&mut bytes)
        .context("read the binary out of the archive")?;
    Ok(bytes)
}

#[cfg(not(any(windows, test)))]
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

/// Replace the binary at `exe` with the staged file. On Unix this is one
/// rename of the staged sibling over `exe`, so the path always holds either
/// the old or the new binary. On Windows the running `.exe` cannot be
/// overwritten: it is moved aside and deleted once this process exits. A
/// swap that fails keeps the staged file and names it in the error, so the
/// verified download is not lost.
pub(crate) fn swap(staged: NamedTempFile, exe: &Path) -> anyhow::Result<()> {
    match swap_inner(&staged, exe) {
        Ok(()) => {
            // The staged file is already gone on Unix; on Windows it was
            // copied, and dropping the handle removes it.
            drop(staged);
            Ok(())
        }
        Err(e) => {
            let staged_path = staged.path().to_path_buf();
            let kept = staged.keep().map_or(staged_path, |(_, path)| path);
            Err(e.context(format!(
                "the verified new binary is kept at {}; move it over {} by hand",
                kept.display(),
                exe.display()
            )))
        }
    }
}

#[cfg(unix)]
fn swap_inner(staged: &NamedTempFile, exe: &Path) -> anyhow::Result<()> {
    std::fs::rename(staged.path(), exe)
        .with_context(|| format!("rename {} over {}", staged.path().display(), exe.display()))
}

#[cfg(windows)]
fn swap_inner(staged: &NamedTempFile, exe: &Path) -> anyhow::Result<()> {
    self_replace::self_replace(staged.path())
        .with_context(|| format!("replace {} with {}", exe.display(), staged.path().display()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
