//! The local bundle-manifest cache `decdn bundle pull --hash` keeps under an
//! output directory. It stores each fetched bundle manifest blob verbatim, keyed
//! by its BLAKE3 content address, so a repeat pull of the same bundle into the
//! same directory reads the manifest from disk instead of paying to fetch the
//! blob again over `cdn/client/v1`.
//!
//! The cache is content-addressed and self-verifying: a load re-hashes the bytes
//! and returns them only when the digest still equals the requested hash, so a
//! corrupt or tampered file is ignored (the manifest is fetched fresh) rather
//! than trusted. It is advisory — a missing, unreadable, or mismatched file only
//! costs one manifest fetch — so a write failure is logged and never fails the
//! pull, and `--overwrite` skips the read to force a fresh fetch.
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

/// Load the cached manifest bytes for `hash`, or `None` when there is no usable
/// cache hit. A missing or unreadable file, or bytes whose BLAKE3 digest no
/// longer equals `hash`, all yield `None` — the caller then fetches the manifest
/// fresh. The returned bytes are guaranteed to hash to `hash`.
pub(crate) fn load(out_root: &Path, hash: [u8; 32]) -> Option<Vec<u8>> {
    let bytes = std::fs::read(cache_path(out_root, hash)).ok()?;
    (*blake3::hash(&bytes).as_bytes() == hash).then_some(bytes)
}

/// Cache `bytes` (a verified bundle manifest with content address `hash`) under
/// `out_root`. Best-effort and advisory: any filesystem error is logged and
/// swallowed, because a pull must not fail over a caching miss.
pub(crate) fn store(out_root: &Path, hash: [u8; 32], bytes: &[u8]) {
    if let Err(e) = try_store(out_root, hash, bytes) {
        tracing::debug!("bundle manifest cache write skipped: {e:#}");
    }
}

/// The fallible core of [`store`]: create the cache directory and write the
/// manifest bytes atomically (temp file in the same directory, `sync_all`, then
/// rename), so a reader never sees a partial file.
fn try_store(out_root: &Path, hash: [u8; 32], bytes: &[u8]) -> anyhow::Result<()> {
    use anyhow::Context as _;

    let dest = cache_path(out_root, hash);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut tmp = super::fetch::temp_in_parent(&dest).context("stage bundle manifest cache")?;
    std::io::Write::write_all(tmp.as_file_mut(), bytes).context("write bundle manifest cache")?;
    tmp.as_file()
        .sync_all()
        .context("sync bundle manifest cache")?;
    tmp.persist(&dest)
        .map_err(|e| e.error)
        .context("persist bundle manifest cache")?;
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    #[test]
    fn store_then_load_returns_the_cached_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = b"{\"version\":1,\"entries\":[]}".to_vec();
        let hash = *blake3::hash(&bytes).as_bytes();

        store(dir.path(), hash, &bytes);

        assert_eq!(load(dir.path(), hash), Some(bytes));
    }

    #[test]
    fn load_is_none_when_no_cache_file_exists() {
        let dir = tempfile::tempdir().unwrap();
        let hash = *blake3::hash(b"absent").as_bytes();

        assert_eq!(load(dir.path(), hash), None);
    }

    #[test]
    fn load_rejects_bytes_that_do_not_hash_to_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = b"original".to_vec();
        let hash = *blake3::hash(&bytes).as_bytes();
        store(dir.path(), hash, &bytes);

        // Corrupt the cached file in place; its bytes no longer match `hash`.
        std::fs::write(cache_path(dir.path(), hash), b"tampered").unwrap();

        assert_eq!(load(dir.path(), hash), None);
    }

    #[test]
    fn cache_path_is_hash_keyed_under_the_cache_dir() {
        let hash = *blake3::hash(b"x").as_bytes();
        let p = cache_path(Path::new("/out"), hash);
        let hex = blake3::Hash::from_bytes(hash).to_hex();
        assert_eq!(
            p,
            Path::new("/out")
                .join(CACHE_DIR)
                .join(format!("{hex}.json"))
        );
    }
}
