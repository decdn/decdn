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
    base: PathBuf,
}

impl FilesystemOrigin {
    /// Construct an origin rooted at `base`. Fails fast if `base` doesn't
    /// exist or isn't a directory — a typo in the config shouldn't surface
    /// as a per-request miss.
    pub async fn new(base: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let base = base.into();
        let meta = tokio::fs::metadata(&base)
            .await
            .with_context(|| format!("cache.origin_path {} is not accessible", base.display()))?;
        if !meta.is_dir() {
            anyhow::bail!("cache.origin_path {} is not a directory", base.display());
        }
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

            // Stat first so a known-oversize file is rejected without
            // ever reading it into memory — mirrors the HTTP origin's
            // Content-Length fast-path.
            let meta = match tokio::fs::metadata(&path).await {
                Ok(m) => m,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OriginFetch::NotFound);
                }
                Err(err) => {
                    return Err(anyhow::Error::from(err).context(format!(
                        "cache.origin_path stat failed for {}",
                        path.display()
                    )));
                }
            };

            if !meta.is_file() {
                anyhow::bail!(
                    "cache.origin_path entry {} is not a regular file",
                    path.display()
                );
            }

            let len = meta.len();
            if len > max_bytes {
                anyhow::bail!(
                    "cache.origin_path entry {} is {len} bytes, exceeds max {max_bytes}",
                    path.display()
                );
            }

            let data = tokio::fs::read(&path)
                .await
                .with_context(|| format!("cache.origin_path read failed for {}", path.display()))?;
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
        let expected = tmp.path().join(expected_shard).join(hex.as_str());
        anyhow::ensure!(path == expected, "got: {}", path.display());
        Ok(())
    }
}
