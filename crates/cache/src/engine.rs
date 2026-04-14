//! Cache engine: local iroh-blobs store fronted by an [`Origin`] for misses.

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use iroh_blobs::Hash;
use iroh_blobs::store::fs::FsStore;

use crate::error::{CacheError, CacheResult};
use crate::origin::{Origin, OriginFetch};

/// Engine bundling a filesystem-backed iroh-blobs store with an optional
/// origin backend. Lookups hit the store first; on miss and when an origin is
/// configured, bytes are pulled, BLAKE3-verified, and inserted before being
/// returned to the caller.
#[derive(Debug, Clone)]
pub struct CacheEngine {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    store: FsStore,
    origin: Option<Arc<dyn Origin>>,
    max_blob_bytes: u64,
}

/// Coarse-grained cache statistics.
///
/// All fields are stubs for now — the gossip crate will read from here once
/// eviction + accounting land. Kept as a struct (not a tuple) so adding fields
/// is non-breaking.
#[derive(Debug, Default, Clone, Copy)]
pub struct CacheStats {
    /// Estimated on-disk bytes consumed by cached blobs.
    pub bytes_stored: u64,
    /// Number of distinct blobs currently in the store.
    pub blob_count: u64,
}

impl CacheEngine {
    /// Open or create the store at `cache_dir`. `max_blob_mb` caps the size
    /// of any single blob pulled from the origin. Oversize payloads typically
    /// surface as [`CacheError::OriginError`] (the HTTP origin trips the cap
    /// mid-stream before the engine sees the bytes), or as
    /// [`CacheError::BlobTooLarge`] when a custom `Origin` impl returns bytes
    /// that exceed the cap without self-enforcement.
    pub async fn open(
        cache_dir: &Path,
        origin: Option<Arc<dyn Origin>>,
        max_blob_mb: u64,
    ) -> CacheResult<Self> {
        tokio::fs::create_dir_all(cache_dir)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;

        let store = FsStore::load(cache_dir)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;

        // Saturate-on-overflow: an operator setting `max_blob_mb = u64::MAX`
        // as a de-facto "unlimited" value should still yield a usable byte cap
        // rather than overflow-wrap to zero.
        let max_blob_bytes = max_blob_mb.saturating_mul(1024 * 1024);

        Ok(Self {
            inner: Arc::new(Inner {
                store,
                origin,
                max_blob_bytes,
            }),
        })
    }

    /// Is this blob already present in the local store?
    pub async fn has(&self, hash: Hash) -> CacheResult<bool> {
        self.inner
            .store
            .blobs()
            .has(hash)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))
    }

    /// Fetch the blob by hash. Hits the local store on a cache hit; on a miss
    /// pulls from the configured origin, BLAKE3-verifies, and inserts before
    /// returning.
    ///
    /// Error variants callers commonly handle:
    /// - [`CacheError::NoOrigin`] — miss with no origin configured.
    /// - [`CacheError::NotFound`] — origin returned a definitive not-found
    ///   (e.g. HTTP 404).
    /// - [`CacheError::HashMismatch`] — origin returned bytes whose BLAKE3
    ///   hash didn't match the request; bytes are dropped, not cached.
    /// - [`CacheError::BlobTooLarge`] / [`CacheError::OriginError`] — size
    ///   cap tripped in the engine or in the origin, respectively.
    /// - [`CacheError::Store`] — local iroh-blobs store I/O failure.
    pub async fn get(&self, hash: Hash) -> CacheResult<Bytes> {
        if self.has(hash).await? {
            return self.read_local(hash).await;
        }
        self.pull_through(hash).await
    }

    /// Flush ephemeral state to disk. The iroh-blobs store does its own
    /// cleanup on drop, but only an explicit
    /// [`iroh_blobs::store::fs::FsStore`] shutdown guarantees that in-flight
    /// writes survive a crash of the surrounding process, so the runtime
    /// calls this during graceful shutdown.
    pub async fn shutdown(&self) -> CacheResult<()> {
        tracing::debug!("flushing cache engine store");
        self.inner
            .store
            .shutdown()
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))
    }

    /// Coarse stats for gossip / observability. MVP returns zeros; the
    /// method exists so callers don't have to change once real accounting
    /// lands (it'll read `self.inner` at that point).
    pub const fn stats(&self) -> CacheStats {
        let _ = self;
        CacheStats {
            bytes_stored: 0,
            blob_count: 0,
        }
    }

    async fn read_local(&self, hash: Hash) -> CacheResult<Bytes> {
        self.inner
            .store
            .blobs()
            .get_bytes(hash)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))
    }

    async fn pull_through(&self, hash: Hash) -> CacheResult<Bytes> {
        let origin = self
            .inner
            .origin
            .as_ref()
            .ok_or(CacheError::NoOrigin { hash })?;

        let fetch = origin
            .fetch(hash, self.inner.max_blob_bytes)
            .await
            .map_err(|source| CacheError::OriginError { hash, source })?;

        let bytes = match fetch {
            OriginFetch::NotFound => return Err(CacheError::NotFound { hash }),
            OriginFetch::Found(b) => b,
        };

        // Enforce the size cap on actual payload — even if the origin
        // omitted `Content-Length`, the blob can't silently exceed the cap.
        let len_u64: u64 = bytes
            .len()
            .try_into()
            .map_err(|_| CacheError::Store(anyhow::anyhow!("payload length overflows u64")))?;
        if len_u64 > self.inner.max_blob_bytes {
            return Err(CacheError::BlobTooLarge {
                hash,
                limit_bytes: self.inner.max_blob_bytes,
            });
        }

        let actual = Hash::new(&bytes);
        if actual != hash {
            return Err(CacheError::HashMismatch {
                expected: hash,
                actual,
            });
        }

        // Hash is verified — now insert. `add_bytes(..).await` runs to
        // completion and yields the tagged info; we discard the tag because
        // a lifecycle policy isn't in scope for the MVP.
        //
        // TODO: once iroh-blobs exposes a verified-insert API that accepts
        // an expected hash, we can drop the explicit `Hash::new(&bytes)`
        // above and pay BLAKE3 only once instead of twice on the happy path.
        if let Err(err) = self.inner.store.blobs().add_bytes(bytes.clone()).await {
            // Verified bytes failed to land in the store: distinct from a
            // generic store error because the caller just spent origin
            // egress and a retry will re-pay it. Surface as an error log so
            // operators can spot this failure mode separately.
            tracing::error!(
                %hash,
                bytes = bytes.len(),
                %err,
                "verified blob failed to insert into cache store",
            );
            return Err(CacheError::Store(anyhow::Error::from(err)));
        }

        Ok(bytes)
    }
}
