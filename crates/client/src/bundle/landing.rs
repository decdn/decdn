//! Where a bundle pull lands its entries: the output directory, held open as a
//! handle that every destination resolves beneath (#2439).

use std::ffi::OsStr;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, anyhow};
use cap_std::fs::{Dir, OpenOptions};
use rand::distr::SampleString as _;

/// How many fresh `<prefix><rand>` names a staging file or link draws when
/// each one collides, before the collision is an error.
const NAME_TRIES: usize = 16;

/// The output directory a run lands its entries in, held open for one pass of
/// the pull. Every destination resolves beneath the handle, never as a path: a
/// symlink in an entry's parent chain that leads out of the root fails that
/// entry instead of writing outside the directory, and a swap of a directory
/// for a symlink while the pull runs cannot redirect a write either. The root
/// itself may be a symlink. A relative symlink that stays inside the root is
/// followed. A symlink with an absolute target is an escape, even one that
/// points back inside.
///
/// The staging directory (`.decdn-partial/`) and the pull's own state files
/// are read by their own paths: a staging directory that is a symlink the
/// handle refuses still lands its blobs, by a copy.
#[derive(Debug)]
pub(super) struct Landing {
    /// The output directory, as the caller named it.
    root: PathBuf,
    /// The open handle on `root`.
    dir: Dir,
}

impl Landing {
    /// Open `out_root`, creating it when absent. An empty `out_root` is the
    /// current directory.
    ///
    /// # Errors
    ///
    /// `out_root` cannot be created or opened as a directory.
    pub(super) fn open(out_root: &Path) -> anyhow::Result<Self> {
        let out_root = if out_root.as_os_str().is_empty() {
            Path::new(".")
        } else {
            out_root
        };
        std::fs::create_dir_all(out_root)
            .with_context(|| format!("create the output directory {}", out_root.display()))?;
        let dir = Dir::open_ambient_dir(out_root, cap_std::ambient_authority())
            .with_context(|| format!("open the output directory {}", out_root.display()))?;
        Ok(Self {
            root: out_root.to_path_buf(),
            dir,
        })
    }

    /// The output directory, as the caller named it: for the pull's own state
    /// (the staging directory, the skip cache, the manifest cache) and for
    /// reads. A manifest-controlled destination never takes this path; it
    /// lands through the handle.
    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// Refuse `dest` before its fetch is paid for, when its parent chain
    /// already cannot take the write: a symlink in it leads out of the root,
    /// or a component of it is not a directory. A parent that does not exist
    /// yet passes: the landing creates it beneath the root. The landing itself
    /// stays the gate, so a chain that changes after this check still cannot
    /// redirect the write.
    ///
    /// # Errors
    ///
    /// `dest` is not under the root, a symlink in its parent chain leads out
    /// of the root, a component of the chain is not a directory, or the chain
    /// cannot be read.
    pub(super) fn check(&self, dest: &Path) -> anyhow::Result<()> {
        let rel = self.relative(dest)?;
        let Some(parent) = rel.parent().filter(|p| !p.as_os_str().is_empty()) else {
            return Ok(());
        };
        match self.dir.metadata(parent) {
            Ok(meta) if meta.is_dir() => Ok(()),
            Ok(_) => Err(anyhow!(
                "{} cannot land: {} is not a directory",
                rel.display(),
                parent.display()
            )),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(self.beneath(rel, "read the directory of", e)),
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
    /// `dest` is not under the root or its parent chain leads out of it, its
    /// directory cannot be created, or the copy or its rename fails.
    pub(super) fn materialize(&self, staging: &Path, dest: &Path) -> anyhow::Result<u64> {
        let rel = self.relative(dest)?;
        let (parent, name) = self.parent_of(rel)?;
        copy_into(staging, &parent, name, dest)
    }

    /// Move the finalized `staging` blob to `dest`, returning its byte count. A
    /// non-donor blob has no reader after its first destination lands, so a
    /// rename replaces the full copy [`Self::materialize`] makes — no second
    /// write of the blob and no second copy on disk. `staging` is already
    /// durable: the ranged store's finalize, and the dedup splice before
    /// promotion, sync it. A rename the handle refuses (a staging directory
    /// that is a symlink the handle refuses) or the filesystem cannot do (a
    /// destination on another filesystem, `EXDEV`) falls back to
    /// [`Self::materialize`], which leaves `staging` in place for the next
    /// writable path.
    ///
    /// # Errors
    ///
    /// `staging` cannot be read, `dest` is not under the root or its parent
    /// chain leads out of it, its directory cannot be created, or both the
    /// move and the copy fail.
    pub(super) fn move_into_place(&self, staging: &Path, dest: &Path) -> anyhow::Result<u64> {
        let len = file_len(staging)?;
        let rel = self.relative(dest)?;
        let (parent, name) = self.parent_of(rel)?;
        let moved = self.relative(staging).and_then(|from| {
            self.dir
                .rename(from, &parent, name)
                .map_err(anyhow::Error::from)
        });
        match moved {
            Ok(()) => Ok(len),
            Err(refused) => {
                tracing::debug!(
                    staging = %staging.display(),
                    dest = %dest.display(),
                    error = %refused,
                    "move refused; copying the blob into place"
                );
                self.materialize(staging, dest)
                    .with_context(|| format!("copy after the move failed ({refused:#})"))
            }
        }
    }

    /// Materialize `src`'s content at `dest` without re-reading it over the
    /// network: a hard link where the filesystem allows it, else a full copy
    /// (cross-device `EXDEV`, a filesystem that can't link, a source the
    /// handle refuses, or a spent name budget). Staged in `dest`'s directory
    /// and renamed into place so `dest` is only ever absent or complete — the
    /// atomic-replace invariant [`Self::materialize`] upholds, which the disk
    /// pre-pass and the slot plan rely on when deciding a path is skip-safe.
    ///
    /// # Errors
    ///
    /// `src` or `dest` is not under the root, `dest`'s parent chain leads out
    /// of it, its directory cannot be created, or both the link and the copy
    /// fail.
    pub(super) fn link_or_copy_atomic(&self, src: &Path, dest: &Path) -> anyhow::Result<()> {
        let rel = self.relative(dest)?;
        let (parent, name) = self.parent_of(rel)?;
        let src_rel = self.relative(src)?;
        let linked = fresh_name(".decdn-link-", |candidate| {
            self.dir.hard_link(src_rel, &parent, candidate)
        });
        let candidate = match linked {
            Ok((candidate, ())) => candidate,
            Err(refused) => {
                tracing::debug!(
                    src = %src.display(),
                    dest = %dest.display(),
                    error = %refused,
                    "hard link refused; copying the blob into place"
                );
                return copy_into(src, &parent, name, dest).map(|_| ());
            }
        };
        parent
            .rename(&candidate, &parent, name)
            .inspect_err(|_| remove_staged(&parent, &candidate))
            .with_context(|| format!("write {}", dest.display()))
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
        let len = file_len(staging)?;
        self.link_or_copy_atomic(staging, dest).map(|()| len)
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
                self.dir.create_dir_all(p).map_err(|e| {
                    // A symlink that leads out is reported as the name it
                    // occupies, not as the escape behind it: resolve it to
                    // name the cause.
                    let cause = match self.dir.metadata(p) {
                        Err(probe) if is_escape(&probe) => probe,
                        _ => e,
                    };
                    self.beneath(rel, "create the directory of", cause)
                })?;
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

/// Copy `src` (read by its own path) to `name` in `parent` through a
/// `.decdn-tmp-<rand>` file there and an atomic rename, returning the byte
/// count. `dest` names the destination in errors. The temp file is created
/// through the handle, so the copy lands beneath the root like a link does.
fn copy_into(src: &Path, parent: &Dir, name: &OsStr, dest: &Path) -> anyhow::Result<u64> {
    let mut from = std::fs::File::open(src).with_context(|| format!("open {}", src.display()))?;
    let (staged, mut file) = fresh_name(".decdn-tmp-", |candidate| {
        parent.open_with(candidate, OpenOptions::new().write(true).create_new(true))
    })
    .with_context(|| format!("stage {}", dest.display()))?;
    std::io::copy(&mut from, &mut file)
        .with_context(|| format!("copy {} -> {}", src.display(), dest.display()))
        .and_then(|written| {
            file.sync_all()
                .with_context(|| format!("sync staged copy for {}", dest.display()))?;
            parent
                .rename(&staged, parent, name)
                .with_context(|| format!("write {}", dest.display()))?;
            Ok(written)
        })
        .inspect_err(|_| remove_staged(parent, &staged))
}

/// `path`'s length in bytes.
fn file_len(path: &Path) -> anyhow::Result<u64> {
    let meta = std::fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    Ok(meta.len())
}

/// Drop the staged `name` in `parent` after its landing failed, so no
/// `.decdn-link-*` or `.decdn-tmp-*` name is left beside the entry.
fn remove_staged(parent: &Dir, name: &str) {
    if let Err(e) = parent.remove_file(name) {
        tracing::warn!(name, error = %e, "failed to remove a staged file after its landing failed");
    }
}

/// A `<prefix><rand>` name under which `create` succeeded, with what `create`
/// returned. A name that already exists draws another, up to [`NAME_TRIES`]
/// times; any other error, or a spent budget, is returned.
fn fresh_name<T, F>(prefix: &str, mut create: F) -> std::io::Result<(String, T)>
where
    F: FnMut(&str) -> std::io::Result<T>,
{
    let mut last = None;
    for _ in 0..NAME_TRIES {
        let candidate = format!(
            "{prefix}{}",
            rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 8)
        );
        match create(&candidate) {
            Ok(created) => return Ok((candidate, created)),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => last = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| std::io::Error::new(ErrorKind::AlreadyExists, "no free name")))
}

/// Whether `e` is the handle's refusal of a path that leads out of its
/// directory. cap-primitives has no typed error for it: the refusal is
/// `PermissionDenied` with the text "a path led outside of the filesystem",
/// so this keys on the text, and a cap-std bump must re-check it (the
/// landing tests assert the message the refusal becomes).
fn is_escape(e: &std::io::Error) -> bool {
    e.kind() == ErrorKind::PermissionDenied && e.to_string().contains("led outside")
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
