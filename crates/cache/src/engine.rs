//! Cache engine: local iroh-blobs store fronted by an [`Origin`] for misses.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

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
    /// Per-hash last-access timestamps for LRU eviction ordering.
    access_times: Mutex<HashMap<Hash, Instant>>,
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
                access_times: Mutex::new(HashMap::new()),
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
        let bytes = if self.has(hash).await? {
            self.read_local(hash).await?
        } else {
            self.pull_through(hash).await?
        };
        self.touch(hash);
        Ok(bytes)
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
    // `&self` is intentional — the signature is load-bearing across the
    // eventual accounting implementation, and keeping it spares callers a
    // churn commit. The `allow` is narrowly scoped to this method.
    #[allow(clippy::unused_self)]
    pub const fn stats(&self) -> CacheStats {
        CacheStats {
            bytes_stored: 0,
            blob_count: 0,
        }
    }

    /// Return the last access time for `hash`, or `None` if the hash has
    /// never been accessed through [`Self::get`].
    pub fn last_accessed(&self, hash: Hash) -> Option<Instant> {
        self.inner
            .access_times
            .lock()
            .ok()
            .and_then(|guard| guard.get(&hash).copied())
    }

    /// Return a snapshot of all recorded access times. Eviction logic can
    /// sort by value to determine LRU ordering.
    pub fn access_times_snapshot(&self) -> HashMap<Hash, Instant> {
        let Ok(guard) = self.inner.access_times.lock() else {
            return HashMap::new();
        };
        guard.clone()
    }

    /// Record an access for `hash` at the current instant.
    fn touch(&self, hash: Hash) {
        if let Ok(mut guard) = self.inner.access_times.lock() {
            guard.insert(hash, Instant::now());
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

    /// BLAKE3 of a 10 GB blob takes seconds of 100% CPU; running it on the
    /// async executor would block one worker and starve other tasks. Small
    /// blobs don't need the `spawn_blocking` round-trip (≤ 1 MiB hashes in
    /// sub-millisecond on a modern core), so `pull_through` uses the inline
    /// path when cheap and `spawn_blocking` above this threshold.
    const BLOCKING_HASH_THRESHOLD: usize = 1 << 20; // 1 MiB

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

        let actual = if bytes.len() <= Self::BLOCKING_HASH_THRESHOLD {
            Hash::new(&bytes)
        } else {
            let bytes_for_hash = bytes.clone();
            tokio::task::spawn_blocking(move || Hash::new(&bytes_for_hash))
                .await
                .map_err(|e| {
                    // JoinError fires on panic or cancellation — don't
                    // lie about which one happened.
                    let note = if e.is_panic() {
                        "blake3 hash task panicked"
                    } else if e.is_cancelled() {
                        "blake3 hash task cancelled"
                    } else {
                        "blake3 hash task failed to join"
                    };
                    CacheError::Store(anyhow::Error::from(e).context(note))
                })?
        };
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
        // TODO(#233): once iroh-blobs exposes a verified-insert API that
        // accepts an expected hash, drop the explicit `Hash::new(&bytes)`
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;

    use crate::origin::{Origin, OriginFetch};

    /// A trivial in-memory origin for tests. Stores exactly one blob.
    #[derive(Debug)]
    struct StubOrigin {
        data: Bytes,
        hash: Hash,
    }

    impl StubOrigin {
        fn new(payload: &[u8]) -> Self {
            Self {
                hash: Hash::new(payload),
                data: Bytes::from(payload.to_vec()),
            }
        }
    }

    impl Origin for StubOrigin {
        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<OriginFetch>> + Send + '_>> {
            let result = if hash == self.hash {
                Ok(OriginFetch::Found(self.data.clone()))
            } else {
                Ok(OriginFetch::NotFound)
            };
            Box::pin(async move { result })
        }
    }

    #[tokio::test]
    async fn get_cache_hit_records_access_time() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello cache hit";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 10).await?;

        // Prime the cache via pull-through.
        let _ = engine.get(hash).await?;

        // Clear the access time so the next get proves a cache-hit path.
        if let Ok(mut guard) = engine.inner.access_times.lock() {
            guard.clear();
        }

        // Read again — this time it's a local hit.
        let _ = engine.get(hash).await?;

        anyhow::ensure!(
            engine.last_accessed(hash).is_some(),
            "expected Some(Instant) after cache-hit get"
        );
        Ok(())
    }

    #[tokio::test]
    async fn pull_through_records_access_time() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello pull-through";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 10).await?;

        // First get triggers pull-through.
        let _ = engine.get(hash).await?;

        anyhow::ensure!(
            engine.last_accessed(hash).is_some(),
            "expected Some(Instant) after pull-through get"
        );
        Ok(())
    }

    #[tokio::test]
    async fn second_get_updates_access_time() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello update";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 10).await?;

        // First access (pull-through).
        let _ = engine.get(hash).await?;
        let first = engine
            .last_accessed(hash)
            .ok_or_else(|| anyhow::anyhow!("expected Some after first get"))?;

        // Burn a tiny bit of real wall-clock time so Instant::now() advances.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Second access (cache hit).
        let _ = engine.get(hash).await?;
        let second = engine
            .last_accessed(hash)
            .ok_or_else(|| anyhow::anyhow!("expected Some after second get"))?;

        anyhow::ensure!(
            second > first,
            "access time should advance: first={first:?}, second={second:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn access_times_snapshot_contains_accessed_hash() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello snapshot";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 10).await?;

        let _ = engine.get(hash).await?;

        let snap = engine.access_times_snapshot();
        anyhow::ensure!(
            snap.contains_key(&hash),
            "snapshot should contain the accessed hash"
        );
        Ok(())
    }

    #[tokio::test]
    async fn last_accessed_returns_none_for_unknown_hash() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), None, 10).await?;
        let unknown = Hash::new(b"never accessed");

        anyhow::ensure!(
            engine.last_accessed(unknown).is_none(),
            "expected None for a hash that was never accessed"
        );
        Ok(())
    }
}
