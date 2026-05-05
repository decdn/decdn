//! Filesystem origin backend.
//!
//! Reads blobs from a sharded local directory at
//! `{base}/{hex[0..2]}/{hex}`. Useful for local dev, pre-seeded caches,
//! and operators who prefer to hand-curate the origin surface without
//! standing up an HTTP server.
//!
//! The sharded layout mirrors git's object store — it keeps directory
//! cardinality manageable as the content set grows, and a pre-seed script
//! is a trivial `cp` loop.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use anyhow::Context;
use bytes::Bytes;
use iroh_blobs::Hash;

use super::{Origin, OriginFetch};

/// Origin backed by a local filesystem directory. Blobs live at
/// `{base}/{hex[0..2]}/{hex}`; the engine is responsible for BLAKE3
/// verification after the bytes come back (same contract as every other
/// [`Origin`] impl).
#[derive(Debug, Clone)]
pub struct FilesystemOrigin {
    /// Canonicalized at construction so the per-fetch containment check
    /// compares two resolved paths — see [`Self::new`] and the
    /// canonicalize step in [`Origin::fetch`].
    base: PathBuf,
}

impl FilesystemOrigin {
    /// Construct an origin rooted at `base`. Fails fast if `base` doesn't
    /// exist or isn't a directory — a typo in the config shouldn't surface
    /// as a per-request miss.
    ///
    /// `base` is canonicalized so the per-request containment check in
    /// [`Origin::fetch`] can compare a resolved blob path against a
    /// resolved root. Without this, an operator who configured the origin
    /// via a symlinked path would have every fetch look "outside" itself.
    pub async fn new(base: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let base = base.into();
        let meta = tokio::fs::metadata(&base)
            .await
            .with_context(|| format!("cache.origin_path {} is not accessible", base.display()))?;
        if !meta.is_dir() {
            anyhow::bail!("cache.origin_path {} is not a directory", base.display());
        }
        let base = tokio::fs::canonicalize(&base).await.with_context(|| {
            format!(
                "cache.origin_path {} could not be canonicalized",
                base.display()
            )
        })?;
        Ok(Self { base })
    }

    /// Expose the configured base for diagnostics / logging.
    pub fn base(&self) -> &Path {
        &self.base
    }

    /// Build the per-hash path: `{base}/{hex[0..2]}/{hex}`. The first
    /// two hex chars shard the directory so a content set with millions
    /// of entries doesn't land in a single unix dirent list.
    fn path_for(&self, hash: Hash) -> PathBuf {
        let hex = hash.to_hex();
        // BLAKE3 hex is always 64 lowercase chars; `get(..2)` is defensive
        // against an unexpected iroh_blobs::Hash format change, and the
        // workspace's anti-indexing lint forbids `&hex[..2]` here.
        let shard = hex.get(..2).unwrap_or("");
        self.base.join(shard).join(hex.as_str())
    }
}

impl Origin for FilesystemOrigin {
    fn fetch(
        &self,
        hash: Hash,
        max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<OriginFetch>> + Send + '_>> {
        Box::pin(async move {
            let path = self.path_for(hash);

            // Resolve symlinks before touching the file. A symlink dropped
            // into the shard tree by an operator mistake or compromised
            // tooling could otherwise turn a content-addressed read into
            // an arbitrary-file read of anything the process can see —
            // and the engine's BLAKE3 check happens *after* the bytes
            // already left the disk, so it is not a defense for what got
            // read in the first place. We canonicalize and require the
            // result to sit under the (already-canonical) base.
            let canonical = match tokio::fs::canonicalize(&path).await {
                Ok(p) => p,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OriginFetch::NotFound);
                }
                Err(err) => {
                    return Err(anyhow::Error::from(err).context(format!(
                        "cache.origin_path canonicalize failed for {}",
                        path.display()
                    )));
                }
            };
            if !canonical.starts_with(&self.base) {
                anyhow::bail!(
                    "cache.origin_path entry {} resolves to {} which is outside base {}",
                    path.display(),
                    canonical.display(),
                    self.base.display()
                );
            }

            // Stat the canonical path so a known-oversize file is rejected
            // without ever reading it into memory — mirrors the HTTP
            // origin's Content-Length fast-path. Using the canonical
            // path also means the metadata + read pair operate on the
            // same already-resolved target, not the symlink we started
            // from.
            let meta = tokio::fs::metadata(&canonical).await.with_context(|| {
                format!("cache.origin_path stat failed for {}", canonical.display())
            })?;

            if !meta.is_file() {
                anyhow::bail!(
                    "cache.origin_path entry {} is not a regular file",
                    canonical.display()
                );
            }

            let len = meta.len();
            if len > max_bytes {
                anyhow::bail!(
                    "cache.origin_path entry {} is {len} bytes, exceeds max {max_bytes}",
                    canonical.display()
                );
            }

            let data = tokio::fs::read(&canonical).await.with_context(|| {
                format!("cache.origin_path read failed for {}", canonical.display())
            })?;
            Ok(OriginFetch::Found(Bytes::from(data)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_rejects_missing_path() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let missing = tmp.path().join("does-not-exist");
        let err = FilesystemOrigin::new(&missing)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("missing path should have been rejected"))?
            .to_string();
        anyhow::ensure!(
            err.contains("not accessible"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn new_rejects_non_directory() -> anyhow::Result<()> {
        let tmp = tempfile::NamedTempFile::new()?;
        let err = FilesystemOrigin::new(tmp.path())
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("file path should have been rejected"))?
            .to_string();
        anyhow::ensure!(
            err.contains("not a directory"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn path_for_uses_two_char_shard() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let hash = Hash::new(b"marker");
        let hex = hash.to_hex();
        let path = origin.path_for(hash);
        let expected_shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        // Compare against the canonicalized tmp dir — on macOS the
        // tempdir lives under /var, which is itself a symlink to
        // /private/var, so origin.base differs from tmp.path().
        let canonical_tmp = tokio::fs::canonicalize(tmp.path()).await?;
        let expected = canonical_tmp.join(expected_shard).join(hex.as_str());
        anyhow::ensure!(path == expected, "got: {}", path.display());
        Ok(())
    }

    /// A symlink in the shard directory pointing outside the base must
    /// be rejected — that's the whole point of the per-fetch
    /// containment check (see issue #374).
    #[cfg(unix)]
    #[tokio::test]
    async fn fetch_rejects_symlink_pointing_outside_base() -> anyhow::Result<()> {
        let outside = tempfile::tempdir()?;
        let secret = outside.path().join("secret");
        tokio::fs::write(&secret, b"top secret").await?;

        let inside = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(inside.path()).await?;
        let hash = Hash::new(b"marker");
        let hex = hash.to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let shard_dir = inside.path().join(shard);
        tokio::fs::create_dir_all(&shard_dir).await?;
        let link = shard_dir.join(hex.as_str());
        tokio::fs::symlink(&secret, &link).await?;

        let err = origin
            .fetch(hash, 1024)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("symlink outside base should have been rejected"))?
            .to_string();
        anyhow::ensure!(err.contains("outside base"), "error lacked context: {err}");
        Ok(())
    }

    /// A symlink that still resolves to a regular file inside the base
    /// is fine — we're guarding against escape, not symlinks per se.
    #[cfg(unix)]
    #[tokio::test]
    async fn fetch_follows_symlink_inside_base() -> anyhow::Result<()> {
        let inside = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(inside.path()).await?;

        // Drop the real file in a sibling directory under base, then
        // place a symlink at the expected sharded location that points
        // at it. canonicalize() resolves to the real file, which is
        // still under base, so the fetch should succeed.
        let real_dir = inside.path().join("real");
        tokio::fs::create_dir_all(&real_dir).await?;
        let real_file = real_dir.join("blob");
        tokio::fs::write(&real_file, b"hello").await?;

        let hash = Hash::new(b"marker");
        let hex = hash.to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let shard_dir = inside.path().join(shard);
        tokio::fs::create_dir_all(&shard_dir).await?;
        let link = shard_dir.join(hex.as_str());
        tokio::fs::symlink(&real_file, &link).await?;

        let fetched = origin.fetch(hash, 1024).await?;
        match fetched {
            OriginFetch::Found(bytes) => {
                anyhow::ensure!(bytes.as_ref() == b"hello", "got: {bytes:?}");
            }
            OriginFetch::NotFound => anyhow::bail!("expected Found, got NotFound"),
        }
        Ok(())
    }

    /// A missing file should still surface as `NotFound`, even though
    /// `canonicalize` is the first syscall and errors when the leaf
    /// doesn't exist.
    #[tokio::test]
    async fn fetch_missing_file_is_not_found() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let hash = Hash::new(b"marker");
        match origin.fetch(hash, 1024).await? {
            OriginFetch::NotFound => Ok(()),
            OriginFetch::Found(_) => anyhow::bail!("expected NotFound"),
        }
    }
}
