//! Where a bundle pull lands its entries: the output directory, held open as a
//! handle that every destination resolves beneath (#2439).

use std::ffi::OsStr;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, anyhow};
use cap_std::fs::Dir;
use rand::distr::SampleString as _;

/// How many `.decdn-link-<rand>` names a hard link tries before it falls back
/// to a copy.
const LINK_NAME_TRIES: usize = 16;

/// The output directory a run lands its entries in, held open once per pass.
/// Every destination resolves beneath the handle, never as a path: a symlink
/// in an entry's parent chain that leads out of the root fails that entry
/// instead of writing outside the directory, and a swap of a directory for a
/// symlink while the pull runs cannot redirect a write either. The root
/// itself may be a symlink. A relative symlink that stays inside the root is
/// followed. A symlink with an absolute target is an escape, even one that
/// points back inside.
///
/// The staging directory (`.decdn-partial/`) and the pull's own state files
/// are read by their own paths: a staging directory that is a symlink still
/// lands its blobs, by a copy.
pub(super) struct Landing {
    /// The output directory, as the caller named it.
    root: PathBuf,
    /// The open handle on `root`.
    dir: Dir,
}

impl Landing {
    /// Open `out_root`, creating it when absent.
    ///
    /// # Errors
    ///
    /// `out_root` cannot be created or opened as a directory.
    pub(super) fn open(out_root: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(out_root)
            .with_context(|| format!("create {}", out_root.display()))?;
        let dir = Dir::open_ambient_dir(out_root, cap_std::ambient_authority())
            .with_context(|| format!("open {}", out_root.display()))?;
        Ok(Self {
            root: out_root.to_path_buf(),
            dir,
        })
    }

    /// The output directory, as the caller named it.
    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// Refuse `dest` before its fetch is paid for, when its parent chain
    /// already leads out of the root. A parent that does not exist yet
    /// passes: the landing creates it beneath the root. The landing itself
    /// stays the gate, so a chain that changes after this check still cannot
    /// redirect the write.
    ///
    /// # Errors
    ///
    /// `dest` is not under the root, or a symlink in its parent chain leads
    /// out of the root.
    pub(super) fn check(&self, dest: &Path) -> anyhow::Result<()> {
        let rel = self.relative(dest)?;
        let Some(parent) = rel.parent().filter(|p| !p.as_os_str().is_empty()) else {
            return Ok(());
        };
        match self.dir.metadata(parent) {
            Err(e) if is_escape(&e) => Err(self.escape(rel)),
            // A missing parent is created at landing; any other fault is
            // reported there, with the write it fails.
            Ok(_) | Err(_) => Ok(()),
        }
    }

    /// Copy the already-fetched, already-verified blob at `staging` to `dest`,
    /// returning the byte count. `staging` is read-only here (not consumed) — a
    /// retry after a failed first write calls this again for the next writable
    /// path — so the copy goes through a unique temp file beside `dest` and an
    /// atomic rename, the same source-must-survive shape
    /// [`Self::link_or_copy_atomic`] uses for every later duplicate.
    ///
    /// # Errors
    ///
    /// `dest` leaves the root, or the copy or its rename fails.
    pub(super) fn materialize(&self, staging: &Path, dest: &Path) -> anyhow::Result<u64> {
        let rel = self.relative(dest)?;
        let (parent, name) = self.parent_of(rel)?;
        Self::copy_into(staging, &parent, name, dest)
    }

    /// Move the finalized `staging` blob to `dest`, returning its byte count. A
    /// non-donor blob has no reader after its first destination lands, so a
    /// rename replaces the full copy [`Self::materialize`] makes — no second
    /// write of the blob and no second copy on disk. `staging` is already
    /// durable: the ranged store's finalize, and the dedup splice before
    /// promotion, sync it. A rename the handle refuses (a staging directory
    /// that is a symlink out of the root) or the filesystem cannot do (a
    /// destination on another filesystem, `EXDEV`) falls back to
    /// [`Self::materialize`], which leaves `staging` in place for the next
    /// writable path.
    ///
    /// # Errors
    ///
    /// `dest` leaves the root, or both the move and the copy fail.
    pub(super) fn move_into_place(&self, staging: &Path, dest: &Path) -> anyhow::Result<u64> {
        let len = std::fs::metadata(staging)
            .with_context(|| format!("stat {}", staging.display()))?
            .len();
        let rel = self.relative(dest)?;
        let (parent, name) = self.parent_of(rel)?;
        let moved = self
            .relative(staging)
            .and_then(|from| Ok(self.dir.rename(from, &parent, name)?));
        match moved {
            Ok(()) => Ok(len),
            Err(_) => self.materialize(staging, dest),
        }
    }

    /// Materialize `src`'s content at `dest` without re-reading it over the
    /// network: a hard link where the filesystem allows it, else a full copy
    /// (cross-device `EXDEV`, a filesystem that can't link, or a source the
    /// handle refuses). Staged in `dest`'s directory and renamed into place so
    /// `dest` is only ever absent or complete — the atomic-replace invariant
    /// [`Self::materialize`] upholds, which the disk pre-pass and the slot
    /// plan rely on when deciding a path is skip-safe.
    ///
    /// # Errors
    ///
    /// `src` or `dest` is not under the root, `dest` leaves the root, or both
    /// the link and the copy fail.
    pub(super) fn link_or_copy_atomic(&self, src: &Path, dest: &Path) -> anyhow::Result<()> {
        let rel = self.relative(dest)?;
        let (parent, name) = self.parent_of(rel)?;
        let src_rel = self.relative(src)?;
        let mut tries = 0;
        let staged = loop {
            let candidate = link_name();
            match self.dir.hard_link(src_rel, &parent, &candidate) {
                Ok(()) => break Some(candidate),
                // A fresh random name colliding: draw another.
                Err(e) if e.kind() == ErrorKind::AlreadyExists && tries < LINK_NAME_TRIES => {
                    tries += 1;
                }
                // EXDEV, an unsupported link, or a refused source: a full copy.
                Err(_) => break None,
            }
        };
        match staged {
            Some(candidate) => parent
                .rename(&candidate, &parent, name)
                .inspect_err(|_| {
                    // The staged link is ours alone: drop it rather than leave
                    // a `.decdn-link-*` name beside the entry.
                    let _ = parent.remove_file(&candidate);
                })
                .with_context(|| format!("write {}", dest.display())),
            None => Self::copy_into(src, &parent, name, dest).map(|_| ()),
        }
    }

    /// Land a group's finalized `staging` blob at its first destination,
    /// returning its byte count: a non-donor moves
    /// ([`Self::move_into_place`]); a donor — a splice source the run-end
    /// sweep still owns — is hard-linked (a copy only across filesystems), so
    /// its staging name survives for later recipients.
    ///
    /// # Errors
    ///
    /// As [`Self::move_into_place`] and [`Self::link_or_copy_atomic`].
    pub(super) fn first_write(
        &self,
        staging: &Path,
        dest: &Path,
        is_donor: bool,
    ) -> anyhow::Result<u64> {
        if !is_donor {
            return self.move_into_place(staging, dest);
        }
        let len = std::fs::metadata(staging)
            .with_context(|| format!("stat {}", staging.display()))?
            .len();
        self.link_or_copy_atomic(staging, dest).map(|()| len)
    }

    /// Copy `src` (read by its own path) to `name` in `parent` through a temp
    /// file there and an atomic rename, returning the byte count. `dest` names
    /// the destination in errors.
    fn copy_into(src: &Path, parent: &Dir, name: &OsStr, dest: &Path) -> anyhow::Result<u64> {
        let mut tmp = cap_tempfile::TempFile::new(parent)
            .with_context(|| format!("stage {}", dest.display()))?;
        let mut from =
            std::fs::File::open(src).with_context(|| format!("open {}", src.display()))?;
        let written = std::io::copy(&mut from, &mut tmp)
            .with_context(|| format!("copy {} -> {}", src.display(), dest.display()))?;
        tmp.as_file()
            .sync_all()
            .with_context(|| format!("sync staged copy for {}", dest.display()))?;
        tmp.replace(name)
            .with_context(|| format!("write {}", dest.display()))?;
        Ok(written)
    }

    /// `path` as the root-relative path the handle resolves.
    fn relative<'p>(&self, path: &'p Path) -> anyhow::Result<&'p Path> {
        let rel = path.strip_prefix(&self.root).map_err(|_| {
            anyhow!(
                "{} is not under the output directory {}",
                path.display(),
                self.root.display()
            )
        })?;
        anyhow::ensure!(
            !rel.as_os_str().is_empty(),
            "{} is the output directory itself",
            path.display()
        );
        Ok(rel)
    }

    /// `rel`'s directory, created beneath the root, and `rel`'s file name.
    fn parent_of<'p>(&self, rel: &'p Path) -> anyhow::Result<(Dir, &'p OsStr)> {
        let name = rel
            .file_name()
            .ok_or_else(|| anyhow!("{} has no file name", rel.display()))?;
        let parent = match rel.parent().filter(|p| !p.as_os_str().is_empty()) {
            Some(p) => {
                if let Err(e) = self.dir.create_dir_all(p) {
                    // A symlink that leads out is reported as the name it
                    // occupies, not as the escape behind it: resolve it to
                    // name the cause.
                    if let Err(probe) = self.dir.metadata(p)
                        && is_escape(&probe)
                    {
                        return Err(self.escape(rel));
                    }
                    return Err(self.beneath(rel, "create the directory of", e));
                }
                self.dir
                    .open_dir(p)
                    .map_err(|e| self.beneath(rel, "open the directory of", e))?
            }
            None => self
                .dir
                .try_clone()
                .with_context(|| format!("reopen {}", self.root.display()))?,
        };
        Ok((parent, name))
    }

    /// `e`, from `what` on `rel` beneath the root: an escape names the
    /// symlink that leads out, anything else keeps its own text.
    fn beneath(&self, rel: &Path, what: &str, e: std::io::Error) -> anyhow::Error {
        if is_escape(&e) {
            self.escape(rel)
        } else {
            anyhow::Error::new(e).context(format!(
                "{what} {} beneath {}",
                rel.display(),
                self.root.display()
            ))
        }
    }

    /// The entry at `rel` resolves outside the root.
    fn escape(&self, rel: &Path) -> anyhow::Error {
        anyhow!(
            "{} resolves outside the output directory {}: a symlink in its path leads out of it",
            rel.display(),
            self.root.display()
        )
    }
}

/// Whether `e` is the handle's refusal of a path that leads out of its
/// directory.
fn is_escape(e: &std::io::Error) -> bool {
    e.kind() == ErrorKind::PermissionDenied && e.to_string().contains("led outside")
}

/// A fresh `.decdn-link-<rand>` staging name for a hard link.
fn link_name() -> String {
    format!(
        ".decdn-link-{}",
        rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 8)
    )
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
