//! The local bundle-manifest cache `decdn bundle pull --hash` keeps under an
//! output directory. It stores each fetched bundle manifest blob verbatim, keyed
//! by its BLAKE3 content address, so a repeat pull of the same bundle into the
//! same directory reads the manifest from disk instead of paying to fetch the
//! blob again over `cdn/client/v1`.
//!
//! The cache is content-addressed and self-verifying: a load re-hashes the bytes
//! and returns them only when the digest still equals the requested hash, so a
//! corrupt or tampered file is never trusted. A load tells two cases apart: a
//! missing file is a plain miss, and any other failure (a read error, or bytes
//! that fail the hash check) is an error that names the path, for the caller to
//! act on. A real pull is fail-open: it prints that error as a warning and
//! fetches the manifest fresh, so a bad cache file only costs one manifest
//! fetch. A write failure prints a warning that names the path and never fails
//! the pull. A real pull under `--overwrite` skips the read to force a fresh
//! fetch. The cache also lets `bundle pull --hash --dry-run` list a bundle's
//! entries with no network; the dry run reads it even under `--overwrite`,
//! because verified bytes equal what a fresh fetch returns, and it fails on a
//! cache file it cannot use, because the cache is its whole answer.
//!
//! Only the `--hash` path uses it: an `-i` local manifest is already on disk for
//! free, with nothing to cache or re-pay.

use std::path::{Path, PathBuf};

/// Directory under the output root that holds cached bundle manifests, one file
/// per bundle keyed by hash. Sits beside the `.decdn-manifest.json` skip-cache.
pub(crate) const CACHE_DIR: &str = ".decdn-bundles";

/// The on-disk path a bundle manifest with content address `hash` caches to:
/// `<out_root>/.decdn-bundles/<hex>.json`. Keyed by the hash so two different
/// bundles pulled into one directory never clobber each other.
pub(crate) fn cache_path(out_root: &Path, hash: [u8; 32]) -> PathBuf {
    let name = blake3::Hash::from_bytes(hash).to_hex();
    out_root.join(CACHE_DIR).join(format!("{name}.json"))
}

/// Load the cached manifest bytes for `hash`. Returns `Ok(None)` when no cache
/// file exists. Returns an error that names the path for any other read failure
/// (for example permission denied, or a directory in the way) and when the
/// file's BLAKE3 digest does not equal `hash`. The returned bytes always hash
/// to `hash`.
pub(crate) fn load(out_root: &Path, hash: [u8; 32]) -> anyhow::Result<Option<Vec<u8>>> {
    use anyhow::Context as _;

    let path = cache_path(out_root, hash);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e)
                .with_context(|| format!("read cached bundle manifest {}", path.display()));
        }
    };
    if *blake3::hash(&bytes).as_bytes() != hash {
        anyhow::bail!(
            "cached bundle manifest {} does not hash to the bundle",
            path.display()
        );
    }
    Ok(Some(bytes))
}

/// Cache `bytes` (a verified bundle manifest with content address `hash`) under
/// `out_root`. Best-effort and advisory: a filesystem error prints a warning and
/// is swallowed, because a pull must not fail over a cache write.
pub(crate) fn store(out_root: &Path, hash: [u8; 32], bytes: &[u8]) {
    if let Err(e) = try_store(out_root, hash, bytes) {
        eprintln!("warning: bundle manifest cache not written: {e:#}");
    }
}

/// The fallible core of [`store`]: create the cache directory and write the
/// manifest bytes atomically (temp file in the same directory, `sync_all`, then
/// rename), so a reader never sees a partial file. Every error names the cache
/// directory or the destination file.
fn try_store(out_root: &Path, hash: [u8; 32], bytes: &[u8]) -> anyhow::Result<()> {
    use anyhow::Context as _;

    let dest = cache_path(out_root, hash);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let shown = dest.display();
    let mut tmp = super::fetch::temp_in_parent(&dest).with_context(|| format!("stage {shown}"))?;
    std::io::Write::write_all(tmp.as_file_mut(), bytes)
        .with_context(|| format!("write {shown}"))?;
    tmp.as_file()
        .sync_all()
        .with_context(|| format!("sync {shown}"))?;
    tmp.persist(&dest)
        .map_err(|e| e.error)
        .with_context(|| format!("persist {shown}"))?;
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
