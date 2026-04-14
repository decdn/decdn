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
    /// of any single blob pulled from the origin; larger payloads are rejected
    /// with [`CacheError::BlobTooLarge`].
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
    /// returning. Returns [`CacheError::NoOrigin`] if there is no origin and
    /// the blob is not cached.
    pub async fn get(&self, hash: Hash) -> CacheResult<Bytes> {
        if self.has(hash).await? {
            return self.read_local(hash).await;
        }
        self.pull_through(hash).await
    }

    /// Drop all ephemeral state and flush the store.
    ///
    /// Safe to call on shutdown; the store's `FsStore::Drop` runs its own
    /// cleanup but a log line here makes delayed writes visible.
    pub async fn shutdown(&self) -> CacheResult<()> {
        self.inner
            .store
            .shutdown()
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))
    }

    /// Coarse stats for gossip / observability. MVP returns zeros — the
    /// hook exists so call sites don't have to change once real accounting
    /// lands.
    #[allow(clippy::unused_self)] // Signature is part of the public seam.
    pub const fn stats(&self) -> CacheStats {
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
        self.inner
            .store
            .blobs()
            .add_bytes(bytes.clone())
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;

        Ok(bytes)
    }
}
