//! Cache engine: local iroh-blobs store fronted by an [`Origin`] for misses.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arc_swap::ArcSwap;
use bytes::Bytes;
use iroh_blobs::Hash;
use iroh_blobs::store::fs::FsStore;
use tokio::sync::Notify;

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
    /// In-flight pull-through requests. When a pull is in progress for a hash,
    /// subsequent callers wait on the [`Notify`] rather than issuing a
    /// duplicate origin fetch (coalescing, fixes #305).
    inflight: Mutex<HashMap<Hash, Arc<Notify>>>,
    /// Operator-pinned blob hashes (#276). Pinned hashes are excluded from
    /// the eviction-candidates snapshot and therefore survive any LRU
    /// pressure. Held in [`ArcSwap`] so SIGHUP reloads can swap in a new
    /// set atomically without rebuilding the engine — the pattern mirrors
    /// the `Arc<AtomicU64>` used for `payment.rate_per_mb` (commit
    /// 166ae41); pinning sets aren't `Copy`, so `ArcSwap` is the
    /// non-blocking equivalent for `HashSet<Hash>`.
    pinned: ArcSwap<HashSet<Hash>>,
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

/// RAII cleanup for an inflight pull-through entry. Removing the entry and
/// waking waiters in `Drop` keeps the coalescing map consistent even if the
/// owning task is cancelled (or panics) mid-fetch — without this, a cancelled
/// pull would leave the entry in place and every subsequent request for the
/// same hash would block forever on a `Notify` that never fires.
struct InflightGuard<'a> {
    hash: Hash,
    inflight: &'a Mutex<HashMap<Hash, Arc<Notify>>>,
    notify: &'a Arc<Notify>,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.inflight.lock() {
            guard.remove(&self.hash);
        }
        self.notify.notify_waiters();
    }
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
        Self::open_with_pinned(cache_dir, origin, max_blob_mb, HashSet::new()).await
    }

    /// Open the cache with an initial pinning set. The set is held in an
    /// [`ArcSwap`] internally so subsequent SIGHUP reloads can call
    /// [`Self::set_pinned`] without rebuilding the engine.
    pub async fn open_with_pinned(
        cache_dir: &Path,
        origin: Option<Arc<dyn Origin>>,
        max_blob_mb: u64,
        pinned: HashSet<Hash>,
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
                inflight: Mutex::new(HashMap::new()),
                pinned: ArcSwap::from_pointee(pinned),
            }),
        })
    }

    /// Atomically swap the pinned-hashes set. Called by the runtime's
    /// SIGHUP handler when `cache.pinned_hashes` changes — readers (the
    /// eviction-candidate snapshot) observe either the old or the new set,
    /// never a partial mix. Returns the previous pinned set so callers can
    /// log a diff if useful.
    pub fn set_pinned(&self, pinned: HashSet<Hash>) -> Arc<HashSet<Hash>> {
        self.inner.pinned.swap(Arc::new(pinned))
    }

    /// Borrow a snapshot of the current pinned set. Cheap (one
    /// `Arc::clone`); the underlying [`ArcSwap`] returns a `Guard` that
    /// resolves to an `Arc<HashSet<Hash>>` we then own.
    pub fn pinned_snapshot(&self) -> Arc<HashSet<Hash>> {
        self.inner.pinned.load_full()
    }

    /// Is `hash` currently pinned? Cheap O(1) lookup against the live set.
    pub fn is_pinned(&self, hash: Hash) -> bool {
        self.inner.pinned.load().contains(&hash)
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
            self.touch(hash);
            return self.read_local(hash).await;
        }

        // Coalesce concurrent pull-through requests for the same hash (#305).
        // A single lock acquisition atomically checks and inserts to avoid the
        // race where multiple tasks see an empty map and all proceed to pull.
        let bytes = loop {
            let state = self.inner.inflight.lock().ok().map(|mut guard| {
                if let Some(n) = guard.get(&hash) {
                    Err(Arc::clone(n))
                } else {
                    let n = Arc::new(Notify::new());
                    guard.insert(hash, Arc::clone(&n));
                    Ok(n)
                }
            });

            match state {
                // Another task owns the pull — wait, then retry from the top.
                Some(Err(notify)) => {
                    notify.notified().await;
                    if self.has(hash).await? {
                        break self.read_local(hash).await?;
                    }
                    // First attempt failed — loop back and either wait on a
                    // new owner or become the owner ourselves.
                }
                // We are the owner — perform the pull. The guard's Drop impl
                // removes the inflight entry and wakes waiters even if this
                // task is cancelled mid-await, preventing the leak that would
                // otherwise hang every future request for `hash`.
                Some(Ok(notify)) => {
                    let _guard = InflightGuard {
                        hash,
                        inflight: &self.inner.inflight,
                        notify: &notify,
                    };
                    break self.pull_through(hash).await?;
                }
                // Mutex poisoned — fall through to a direct pull.
                None => break self.pull_through(hash).await?,
            }
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
    ///
    /// **Note:** this snapshot is the *raw* access map and includes pinned
    /// hashes. Eviction implementations should use
    /// [`Self::eviction_candidates`] instead, which filters pinned hashes
    /// out so they survive LRU pressure (#276). The raw snapshot is still
    /// exposed because tests and observability paths sometimes want the
    /// unfiltered view.
    pub fn access_times_snapshot(&self) -> HashMap<Hash, Instant> {
        let Ok(guard) = self.inner.access_times.lock() else {
            return HashMap::new();
        };
        guard.clone()
    }

    /// Return a snapshot of access times **excluding pinned hashes**.
    /// This is the canonical input to LRU eviction (#276): a pinned hash
    /// never appears here, so any candidate-picking sort or top-K query
    /// run against the result inherently respects the pinning policy.
    ///
    /// The pinned set is loaded once at the start of the call so a
    /// concurrent `set_pinned` swap doesn't change which hashes get
    /// filtered mid-iteration — the snapshot is consistent against
    /// *some* pinned generation, just not necessarily the very latest.
    pub fn eviction_candidates(&self) -> HashMap<Hash, Instant> {
        let pinned = self.inner.pinned.load();
        let Ok(guard) = self.inner.access_times.lock() else {
            return HashMap::new();
        };
        guard
            .iter()
            .filter_map(|(h, t)| {
                if pinned.contains(h) {
                    None
                } else {
                    Some((*h, *t))
                }
            })
            .collect()
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    /// An origin that sleeps before returning, counting how many times
    /// `fetch` was invoked. Used to verify coalescing of concurrent pulls.
    #[derive(Debug)]
    struct SlowCountingOrigin {
        data: Bytes,
        hash: Hash,
        fetch_count: AtomicUsize,
        delay: std::time::Duration,
    }

    impl SlowCountingOrigin {
        fn new(payload: &[u8], delay: std::time::Duration) -> Self {
            Self {
                hash: Hash::new(payload),
                data: Bytes::from(payload.to_vec()),
                fetch_count: AtomicUsize::new(0),
                delay,
            }
        }
    }

    impl Origin for SlowCountingOrigin {
        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<OriginFetch>> + Send + '_>> {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);
            let result = if hash == self.hash {
                Ok(OriginFetch::Found(self.data.clone()))
            } else {
                Ok(OriginFetch::NotFound)
            };
            let delay = self.delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                result
            })
        }
    }

    #[tokio::test]
    async fn concurrent_gets_coalesce_into_single_origin_fetch() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"coalesce me";
        let hash = Hash::new(payload);
        let origin = Arc::new(SlowCountingOrigin::new(
            payload,
            std::time::Duration::from_millis(50),
        ));

        let engine =
            CacheEngine::open(tmp.path(), Some(origin.clone() as Arc<dyn Origin>), 10).await?;

        // Spawn several concurrent gets for the same hash.
        let mut handles = Vec::new();
        for _ in 0..5 {
            let e = engine.clone();
            handles.push(tokio::spawn(async move { e.get(hash).await }));
        }

        // Await all — they should all succeed.
        for handle in handles {
            let result = handle
                .await
                .map_err(|e| anyhow::anyhow!("task join: {e}"))?;
            anyhow::ensure!(result.is_ok(), "expected Ok, got {result:?}");
        }

        // The origin should have been called at most once (coalesced).
        let count = origin.fetch_count.load(Ordering::SeqCst);
        anyhow::ensure!(count == 1, "expected exactly 1 origin fetch, got {count}");
        Ok(())
    }

    #[tokio::test]
    async fn inflight_map_is_empty_after_pull_completes() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"cleanup check";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 10).await?;

        let _ = engine.get(hash).await?;

        let inflight_len = engine.inner.inflight.lock().ok().map_or(0, |g| g.len());
        anyhow::ensure!(
            inflight_len == 0,
            "inflight map should be empty after pull, had {inflight_len} entries"
        );
        Ok(())
    }

    /// Cancelling the owner mid-pull must not leave the inflight entry
    /// orphaned — otherwise every subsequent `get()` for the same hash hangs
    /// on a `Notify` that never fires. The `InflightGuard`'s `Drop` impl
    /// wakes waiters and clears the entry even on cancellation.
    #[tokio::test]
    async fn cancelled_owner_does_not_orphan_inflight_entry() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"cancel test";
        let hash = Hash::new(payload);
        let origin = SlowCountingOrigin::new(payload, std::time::Duration::from_secs(10));

        let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 10).await?;

        // Spawn the owner with a tiny timeout so it gets cancelled mid-pull.
        let owner_engine = engine.clone();
        let owner = tokio::spawn(async move {
            tokio::time::timeout(std::time::Duration::from_millis(50), owner_engine.get(hash)).await
        });
        // Wait for the owner task to finish (timeout fires → future dropped).
        let _ = owner.await?;

        // The inflight map must be empty — InflightGuard::drop ran on cancel.
        let inflight_len = engine.inner.inflight.lock().ok().map_or(0, |g| g.len());
        anyhow::ensure!(
            inflight_len == 0,
            "inflight map should be empty after cancellation, had {inflight_len} entries"
        );
        Ok(())
    }

    // ----- Pinning (#276) -----

    #[tokio::test]
    async fn pinned_hash_is_excluded_from_eviction_candidates() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let pinned_payload = b"pinned blob";
        let evictable_payload = b"evictable blob";
        let pinned_hash = Hash::new(pinned_payload);
        let evictable_hash = Hash::new(evictable_payload);

        // Build the engine with pinned_hash in the pinning set.
        let mut pinned_set = HashSet::new();
        pinned_set.insert(pinned_hash);
        let engine = CacheEngine::open_with_pinned(tmp.path(), None, 10, pinned_set).await?;

        // Touch both hashes via direct access-time insertion (we don't
        // need actual blob content for this test).
        if let Ok(mut g) = engine.inner.access_times.lock() {
            g.insert(pinned_hash, Instant::now());
            g.insert(evictable_hash, Instant::now());
        }

        let raw = engine.access_times_snapshot();
        anyhow::ensure!(raw.len() == 2, "raw snapshot must include pinned");

        let candidates = engine.eviction_candidates();
        anyhow::ensure!(
            candidates.len() == 1,
            "candidates should exclude pinned, got {} entries",
            candidates.len()
        );
        anyhow::ensure!(
            candidates.contains_key(&evictable_hash),
            "evictable hash should be a candidate"
        );
        anyhow::ensure!(
            !candidates.contains_key(&pinned_hash),
            "pinned hash must NOT be a candidate"
        );
        anyhow::ensure!(engine.is_pinned(pinned_hash));
        anyhow::ensure!(!engine.is_pinned(evictable_hash));
        Ok(())
    }

    #[tokio::test]
    async fn set_pinned_atomically_updates_filter() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let h1 = Hash::new(b"one");
        let h2 = Hash::new(b"two");

        let engine = CacheEngine::open(tmp.path(), None, 10).await?;
        if let Ok(mut g) = engine.inner.access_times.lock() {
            g.insert(h1, Instant::now());
            g.insert(h2, Instant::now());
        }

        // No pinning yet — both candidates.
        anyhow::ensure!(engine.eviction_candidates().len() == 2);

        // Pin h1.
        let mut s = HashSet::new();
        s.insert(h1);
        let prev = engine.set_pinned(s);
        anyhow::ensure!(
            prev.is_empty(),
            "previous pinned set should have been empty"
        );

        let candidates = engine.eviction_candidates();
        anyhow::ensure!(candidates.len() == 1, "h1 should now be excluded");
        anyhow::ensure!(candidates.contains_key(&h2));

        // Replace with empty set — h1 becomes a candidate again.
        let prev2 = engine.set_pinned(HashSet::new());
        anyhow::ensure!(prev2.contains(&h1));
        anyhow::ensure!(engine.eviction_candidates().len() == 2);
        Ok(())
    }
}
