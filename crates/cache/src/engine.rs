//! Cache engine: local iroh-blobs store fronted by an [`Origin`] for misses.

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
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
    ///
    /// Pinning interacts with the `evicted` field in one direction only:
    /// pinning prevents *LRU* eviction (issue #276) but does not protect
    /// against an explicit operator [`CacheEngine::evict`] (#279) — an
    /// operator running a DMCA takedown on a pinned hash gets the
    /// takedown, full stop. The pin just keeps the hash off the LRU
    /// candidate list.
    pinned: ArcSwap<HashSet<Hash>>,
    /// Hashes the operator has explicitly evicted via [`CacheEngine::evict`]
    /// (issue #279). Membership is honored by [`CacheEngine::has`] and
    /// [`CacheEngine::get`] so an evicted blob is not served, even though
    /// the underlying iroh-blobs store may still hold the bytes — iroh-blobs
    /// 0.99 does not expose a public delete (`Blobs::delete` is `pub(crate)`,
    /// reserved for the GC task, see issue #233). Reclaim of disk bytes
    /// happens on the next GC sweep once that lands.
    ///
    /// Persisted alongside the iroh-blobs store at `<cache_dir>/evicted.log`
    /// on every successful [`CacheEngine::evict`] call so DMCA takedowns and
    /// corruption-recovery evicts survive a process restart — an
    /// in-memory-only set would silently let evicted content resume serving
    /// after `decdn run` is restarted, which is exactly the failure mode
    /// #279 needs to prevent.
    evicted: Mutex<HashSet<Hash>>,
    /// Append-only file holding lowercase-hex evicted hashes, one per line.
    /// Loaded on [`CacheEngine::open`]; appended to (with `fsync`) on every
    /// successful [`CacheEngine::evict`]. Lives at `<cache_dir>/evicted.log`.
    /// The format is intentionally trivial so operators can grep / inspect /
    /// hand-edit it during incident response; duplicate lines are tolerated
    /// (loading deduplicates via the `HashSet`).
    evicted_log_path: PathBuf,
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

/// Operator-pinned blob hashes (#276). Hashes here are excluded from the
/// LRU eviction-candidate snapshot. Constructing this type is the only
/// way to feed pinned hashes into [`CacheEngine::open_with_pinned`] or
/// [`CacheEngine::set_pinned`], so a future "blocklist" or similar
/// `HashSet<Hash>`-shaped feature can't be silently passed into the
/// pinning slot.
///
/// Held as `Arc<HashSet<Hash>>` internally so reload paths that swap the
/// active set don't need to clone the underlying map.
#[derive(Debug, Clone)]
pub struct PinnedHashes(Arc<HashSet<Hash>>);

impl PinnedHashes {
    /// Build a [`PinnedHashes`] from a freshly parsed set.
    #[must_use]
    pub fn new(set: HashSet<Hash>) -> Self {
        Self(Arc::new(set))
    }

    /// The empty pinned set.
    #[must_use]
    pub fn empty() -> Self {
        Self(Arc::new(HashSet::new()))
    }

    /// Number of pinned hashes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Is the pinned set empty?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Is `hash` pinned?
    #[must_use]
    pub fn contains(&self, hash: &Hash) -> bool {
        self.0.contains(hash)
    }

    /// Iterate over the pinned hashes.
    pub fn iter(&self) -> std::collections::hash_set::Iter<'_, Hash> {
        self.0.iter()
    }

    /// Compute counts of additions / removals / unchanged hashes between
    /// `prev` (older snapshot) and `self` (newer). Used by the SIGHUP
    /// reload path to log a diff line — operators pin/unpin individual
    /// hashes and want to see the delta in the success log without
    /// scraping the full set.
    #[must_use]
    pub fn diff(&self, prev: &Self) -> PinDiff {
        let added = self.0.iter().filter(|h| !prev.0.contains(*h)).count();
        let removed = prev.0.iter().filter(|h| !self.0.contains(*h)).count();
        PinDiff { added, removed }
    }
}

impl<'a> IntoIterator for &'a PinnedHashes {
    type Item = &'a Hash;
    type IntoIter = std::collections::hash_set::Iter<'a, Hash>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl Default for PinnedHashes {
    fn default() -> Self {
        Self::empty()
    }
}

/// Cheap diff of two [`PinnedHashes`] snapshots, for the reload log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinDiff {
    /// Number of hashes present in the new set but not the old.
    pub added: usize,
    /// Number of hashes present in the old set but not the new.
    pub removed: usize,
}

/// Read-only snapshot of a hash's local-cache state, returned by
/// [`CacheEngine::inspect`]. Backs `decdn node evict --dry-run`
/// (issue #379): operators running DMCA takedowns or
/// corruption-recovery want to confirm the blob's size, last-access
/// time, pin status, and already-evicted flag before mutating state.
///
/// All fields reflect the *underlying* cache state — `size_bytes` reads
/// from the iroh-blobs store directly, so a hash that has already been
/// logically evicted (and whose bytes are still on disk pending the
/// follow-up GC sweep in #233) still reports its on-disk size here.
/// That keeps dry-run honest about disk reclaim potential rather than
/// hiding it once the operator has flipped the evicted flag.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EvictionPreview {
    /// Bytes the iroh-blobs store reports for this hash. `None` when the
    /// blob isn't in the store; matches `BlobStatus::NotFound`. Partial
    /// blobs (`BlobStatus::Partial { size }`) report whatever size the
    /// store has so far — the operator sees how much disk a partial
    /// pull is occupying.
    pub size_bytes: Option<u64>,
    /// Microseconds elapsed since the blob was last served via
    /// [`CacheEngine::get`]. `None` when no access has been recorded —
    /// typical for a hash that was just inserted but never re-served,
    /// or for one that has been logically evicted (eviction clears the
    /// access entry).
    pub last_accessed_us_ago: Option<u64>,
    /// Whether the hash is in the operator-pinned set (#276). Pinning
    /// protects against LRU eviction but **not** against an explicit
    /// [`CacheEngine::evict`]; surfaced here so a dry-run operator can
    /// spot the case before running the takedown.
    pub pinned: bool,
    /// Whether the hash already lives in `<cache_dir>/evicted.log`.
    /// `true` means a real [`CacheEngine::evict`] would short-circuit
    /// at the idempotency guard at the top of `evict()` — the dry-run
    /// is reporting on a no-op.
    pub already_evicted: bool,
    /// Whether [`CacheEngine::has`] would currently return `true` for
    /// this hash — equivalently, `BlobStatus::Complete` *and* not
    /// already evicted. Pre-computed inside `inspect()` so the admin
    /// RPC doesn't need a second `has()` round-trip to fill the
    /// `was_present` field on its response.
    pub served: bool,
}

/// Snapshot of access times for blobs that are eligible for LRU
/// eviction — i.e. **pinned hashes are already excluded**. Returned by
/// [`CacheEngine::eviction_candidates`].
///
/// The newtype makes "pinned-already-excluded" a *type-level* property:
/// any future eviction-policy implementation that takes
/// `EvictionCandidates` is guaranteed by the compiler not to evict
/// pinned hashes. With a raw `HashMap<Hash, Instant>` return that
/// guarantee would live only in a doc comment, and a future caller
/// could substitute [`CacheEngine::access_times_snapshot`] (which
/// includes pinned) by mistake.
#[derive(Debug, Default)]
pub struct EvictionCandidates(HashMap<Hash, Instant>);

impl EvictionCandidates {
    /// Number of eviction candidates.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Are there no eviction candidates?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Does `hash` appear in the candidate set?
    #[must_use]
    pub fn contains_key(&self, hash: &Hash) -> bool {
        self.0.contains_key(hash)
    }

    /// Iterate `(hash, last-access)` pairs.
    pub fn iter(&self) -> std::collections::hash_map::Iter<'_, Hash, Instant> {
        self.0.iter()
    }

    /// Consume into the underlying `HashMap`. Eviction policies that
    /// need to sort by access time and pop top-K can call this once at
    /// the start of their loop. The newtype's invariant
    /// (pinned-already-excluded) is preserved by the time this returns
    /// — the caller just gets a plain map to work with.
    #[must_use]
    pub fn into_inner(self) -> HashMap<Hash, Instant> {
        self.0
    }
}

impl<'a> IntoIterator for &'a EvictionCandidates {
    type Item = (&'a Hash, &'a Instant);
    type IntoIter = std::collections::hash_map::Iter<'a, Hash, Instant>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
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

/// Hard cap on the number of distinct hashes the in-memory `evicted`
/// set may hold. Bounds (a) the resident memory of the set itself and
/// (b) the unbounded growth of `<cache_dir>/evicted.log` under e.g. a
/// mass-evict automation gone wrong. At ~64 bytes per `HashSet<Hash>`
/// entry the cap is ~64 MB resident. Operators legitimately hitting
/// this are operating well outside normal DMCA-takedown caseloads and
/// should investigate before raising it. Loading from `evicted.log` on
/// `open()` is *not* gated by this cap — entries persisted by a previous
/// run always replay (silently dropping a persisted DMCA takedown to
/// stay under cap is the worst-case the cap was meant to prevent).
pub(crate) const MAX_EVICTED_ENTRIES: usize = 1_000_000;

/// Read the evicted-hash log into a [`HashSet`]. A missing file is the
/// normal "no evictions yet" case and yields an empty set; any other I/O
/// or parse error is fatal because silently dropping persisted evictions
/// would resume serving DMCA-flagged content (issue #279). Lines that
/// don't parse as a 64-character hex hash are skipped with a `warn` log
/// — operators may have hand-edited the file during incident response,
/// and one corrupt line shouldn't take the whole log down.
fn load_evicted_log(path: &Path) -> CacheResult<HashSet<Hash>> {
    let contents = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(err) => {
            return Err(CacheError::Store(anyhow::Error::from(err).context(
                format!("failed to read evicted-hash log at {}", path.display()),
            )));
        }
    };
    let mut out = HashSet::new();
    for raw in contents.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match parse_hex_hash(line) {
            Some(h) => {
                out.insert(h);
            }
            None => {
                tracing::warn!(
                    line = %line,
                    path = %path.display(),
                    "skipping malformed entry in evicted-hash log",
                );
            }
        }
    }
    Ok(out)
}

/// Parse a 64-char hex BLAKE3 hash. Accepts either case so hand-edited
/// log entries (operators pasting a hash from access logs / takedown
/// notices, which may use either case) round-trip through the same
/// parser the admin RPC accepts. The persisted format is canonically
/// lowercase via `Hash::Display`'s `to_hex()`, so this only relaxes the
/// read path — writes remain lowercase, and load-then-save normalizes
/// silently. Strict on length: a corrupted log line surfaces as `None`
/// (logged + skipped at load time) rather than silently turning into
/// the wrong hash.
fn parse_hex_hash(s: &str) -> Option<Hash> {
    if s.len() != 64 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        // First nibble is the high nibble (bits 7..4): hex `ab` decodes
        // to `0xab`, not `0xba`. Naming follows that semantic so an
        // audit-sensitive DMCA-takedown codepath isn't decoded against
        // mis-labeled variables.
        let hi = s.as_bytes().get(i * 2)?;
        let lo = s.as_bytes().get(i * 2 + 1)?;
        *byte = (hex_digit(*hi)? << 4) | hex_digit(*lo)?;
    }
    Some(Hash::from_bytes(bytes))
}

const fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Append `hash` to the evicted log with `fsync` before returning. The
/// caller relies on the durability guarantee — a return-without-error
/// means a crash now will replay the eviction on the next `open`.
fn append_evicted_log(path: &Path, hash: Hash) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    // POSIX guarantees `write` calls under PIPE_BUF (typically 4 KiB) are
    // atomic on append-mode file handles. A 64-char hash + newline is
    // 65 bytes, well under the limit, so concurrent writers from a
    // misconfigured shared cache_dir won't interleave bytes mid-line.
    let line = format!("{hash}\n");
    file.write_all(line.as_bytes())?;
    file.sync_all()?;
    Ok(())
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
        Self::open_with_pinned(cache_dir, origin, max_blob_mb, PinnedHashes::empty()).await
    }

    /// Open the cache with an initial pinning set. The set is held in an
    /// [`ArcSwap`] internally so subsequent SIGHUP reloads can call
    /// [`Self::set_pinned`] without rebuilding the engine.
    pub async fn open_with_pinned(
        cache_dir: &Path,
        origin: Option<Arc<dyn Origin>>,
        max_blob_mb: u64,
        pinned: PinnedHashes,
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

        let evicted_log_path = cache_dir.join("evicted.log");
        let evicted = load_evicted_log(&evicted_log_path)?;

        Ok(Self {
            inner: Arc::new(Inner {
                store,
                origin,
                max_blob_bytes,
                access_times: Mutex::new(HashMap::new()),
                inflight: Mutex::new(HashMap::new()),
                pinned: ArcSwap::from(pinned.0),
                evicted: Mutex::new(evicted),
                evicted_log_path,
            }),
        })
    }

    /// Atomically swap the pinned-hashes set. Called by the runtime's
    /// SIGHUP handler when `cache.pinned_hashes` changes — readers (the
    /// eviction-candidate snapshot) observe either the old or the new set,
    /// never a partial mix. Returns a [`PinDiff`] so callers can log
    /// "added X, removed Y" without re-walking either set.
    ///
    /// Takes `&PinnedHashes` rather than ownership: the inner `Arc`
    /// is cheap to clone for the swap, and callers commonly want to
    /// compute the diff without giving up their own copy.
    pub fn set_pinned(&self, new: &PinnedHashes) -> PinDiff {
        let prev_arc = self.inner.pinned.swap(Arc::clone(&new.0));
        let prev = PinnedHashes(prev_arc);
        new.diff(&prev)
    }

    /// Borrow a snapshot of the current pinned set. Cheap (one
    /// `Arc::clone`); the underlying [`ArcSwap`] returns a `Guard` that
    /// resolves to an `Arc<HashSet<Hash>>` we then own.
    pub fn pinned_snapshot(&self) -> PinnedHashes {
        PinnedHashes(self.inner.pinned.load_full())
    }

    /// Is `hash` currently pinned? Cheap O(1) lookup against the live set.
    pub fn is_pinned(&self, hash: Hash) -> bool {
        self.inner.pinned.load().contains(&hash)
    }

    /// Is this blob already present in the local store?
    ///
    /// Returns `Ok(false)` when the hash has been logically evicted (issue
    /// #279) even if the underlying store still holds the bytes — operators
    /// who call `evict` expect the node to stop serving immediately, so
    /// `has` reports the blob as absent.
    pub async fn has(&self, hash: Hash) -> CacheResult<bool> {
        if self.is_evicted(hash) {
            return Ok(false);
        }
        self.inner
            .store
            .blobs()
            .has(hash)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))
    }

    /// Mark `hash` as evicted: subsequent [`Self::has`] / [`Self::get`] calls
    /// behave as if the blob is absent (returning `false` / `NotFound`
    /// respectively). The corresponding [`Self::access_times_snapshot`] entry
    /// is cleared so future LRU sweeps don't re-surface the hash.
    ///
    /// This is a *logical* evict: iroh-blobs 0.99 does not expose a public
    /// `delete` API (see issue #233), so the bytes remain on disk until the
    /// internal GC sweep reclaims them. The operator-visible behavior — the
    /// node stops serving the blob immediately — is what `decdn node evict`
    /// (issue #279) needs for use cases like DMCA takedown and corruption
    /// recovery.
    ///
    /// Persisted: the eviction is appended (with `fsync`) to
    /// `<cache_dir>/evicted.log` before this call returns successfully, so
    /// the takedown survives a process restart. A persistence failure is
    /// surfaced as `Err` rather than silently downgrading to in-memory-only —
    /// for DMCA-driven evicts the operator must be able to tell whether
    /// the takedown is durable, otherwise a node restart could resume
    /// serving the content.
    pub fn evict(&self, hash: Hash) -> CacheResult<()> {
        // Pre-check under one lock acquisition: short-circuit on
        // already-evicted (idempotent — don't grow `evicted.log` with a
        // duplicate line) and reject on cap (DoS bound on an unbounded
        // public-ish surface). The cap check is racy against concurrent
        // evicts but the cap itself is a soft DoS bound, not a hard
        // invariant — going +ε over by a handful of races is fine.
        {
            let guard = self
                .inner
                .evicted
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if guard.contains(&hash) {
                return Ok(());
            }
            if guard.len() >= MAX_EVICTED_ENTRIES {
                return Err(CacheError::EvictionLimitExceeded {
                    limit: MAX_EVICTED_ENTRIES,
                });
            }
        }

        // Persist FIRST, then commit to the in-memory set: a crash between
        // these two steps will at worst replay a successful evict on the
        // next open, which is idempotent. The opposite ordering would
        // briefly stop serving but lose durability if the fsync failed —
        // the worst-case scenario for a takedown.
        append_evicted_log(&self.inner.evicted_log_path, hash).map_err(|err| {
            CacheError::Store(anyhow::Error::from(err).context("persist eviction"))
        })?;

        // `unwrap_or_else(PoisonError::into_inner)` rather than the project's
        // usual `if let Ok(...) = lock()` pattern: a poisoned lock here
        // would silently skip the in-memory commit and the node would keep
        // serving the supposedly-evicted blob until the next restart loaded
        // `evicted.log`. For a DMCA takedown that is the canonical worst
        // case. Recovering the inner guard preserves the contract that
        // `evict() -> Ok(())` implies the in-memory set was updated.
        self.inner
            .evicted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(hash);
        self.inner
            .access_times
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&hash);
        Ok(())
    }

    /// Read-only inspection of a hash's local cache state, used by
    /// `admin_v1_evict { dry_run: true }` (issue #379) to preview what
    /// an [`Self::evict`] call would touch without mutating any state.
    ///
    /// Performs a single `BlobStatus` query against the underlying
    /// store and reuses the cheap in-memory lookups for pin / access /
    /// already-evicted flags. The returned [`EvictionPreview`] also
    /// pre-computes a `served` bool so admin can derive `was_present`
    /// without a follow-up [`Self::has`] call.
    pub async fn inspect(&self, hash: Hash) -> CacheResult<EvictionPreview> {
        let status = self
            .inner
            .store
            .blobs()
            .status(hash)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;

        // Decompose `BlobStatus` once: `complete` feeds `served`,
        // `size_bytes` feeds the wire response. Keeping the `match` here
        // (rather than splitting into two helpers) makes the mapping
        // between iroh-blobs states and our preview fields auditable in
        // one place.
        let (size_bytes, complete) = match status {
            iroh_blobs::api::blobs::BlobStatus::NotFound => (None, false),
            iroh_blobs::api::blobs::BlobStatus::Partial { size } => (size, false),
            iroh_blobs::api::blobs::BlobStatus::Complete { size } => (Some(size), true),
        };

        let already_evicted = self.is_evicted(hash);
        // `served` mirrors the `has()` semantic: complete AND not
        // evicted. Pre-computed here so admin doesn't issue a second
        // round-trip to compute `was_present`.
        let served = complete && !already_evicted;

        let last_accessed_us_ago = self.last_accessed(hash).map(|inst| {
            // Saturate-on-overflow rather than panic. `Instant::elapsed`
            // can theoretically exceed u64::MAX microseconds on a
            // process that's been up for ~580k years; defensive against
            // a future test that builds an `Instant` from a stub.
            u64::try_from(inst.elapsed().as_micros()).unwrap_or(u64::MAX)
        });

        Ok(EvictionPreview {
            size_bytes,
            last_accessed_us_ago,
            pinned: self.is_pinned(hash),
            already_evicted,
            served,
        })
    }

    /// Has this hash been logically evicted via [`Self::evict`]?
    ///
    /// Recovers from a poisoned mutex via [`PoisonError::into_inner`]
    /// rather than treating poison as "not evicted": a poisoned lock
    /// returning `false` here would let evicted DMCA-flagged content
    /// resume serving — exactly what `<cache_dir>/evicted.log`'s
    /// durability guarantee was designed to prevent.
    pub fn is_evicted(&self, hash: Hash) -> bool {
        self.inner
            .evicted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&hash)
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

        // Logical-eviction guard (#279): once an operator has run
        // `decdn node evict <hash>`, a subsequent `get` must not silently
        // re-pull from the origin and undo the eviction. The eviction is
        // sticky for the life of `<cache_dir>/evicted.log` — there is no
        // "unevict" path; an operator who needs to re-cache a previously
        // evicted hash hand-edits the log and restarts.
        if self.is_evicted(hash) {
            return Err(CacheError::NotFound { hash });
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
    /// The return type ([`EvictionCandidates`]) is a newtype with no
    /// public constructor — callers can iterate or `into_inner` but
    /// cannot fabricate one. This makes "pinned-already-excluded" a
    /// type-level invariant rather than a documentation claim.
    ///
    /// The pinned set is loaded once at the start of the call so a
    /// concurrent `set_pinned` swap doesn't change which hashes get
    /// filtered mid-iteration — the snapshot is consistent against
    /// *some* pinned generation, just not necessarily the very latest.
    pub fn eviction_candidates(&self) -> EvictionCandidates {
        let pinned = self.inner.pinned.load();
        let Ok(guard) = self.inner.access_times.lock() else {
            return EvictionCandidates(HashMap::new());
        };
        let map = guard
            .iter()
            .filter_map(|(h, t)| {
                if pinned.contains(h) {
                    None
                } else {
                    Some((*h, *t))
                }
            })
            .collect();
        EvictionCandidates(map)
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
        let engine =
            CacheEngine::open_with_pinned(tmp.path(), None, 10, PinnedHashes::new(pinned_set))
                .await?;

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
        let diff = engine.set_pinned(&PinnedHashes::new(s));
        anyhow::ensure!(
            diff.added == 1 && diff.removed == 0,
            "expected diff (added=1, removed=0), got {diff:?}"
        );

        let candidates = engine.eviction_candidates();
        anyhow::ensure!(candidates.len() == 1, "h1 should now be excluded");
        anyhow::ensure!(candidates.contains_key(&h2));

        // Replace with empty set — h1 becomes a candidate again.
        let diff2 = engine.set_pinned(&PinnedHashes::empty());
        anyhow::ensure!(
            diff2.added == 0 && diff2.removed == 1,
            "expected diff (added=0, removed=1), got {diff2:?}"
        );
        anyhow::ensure!(engine.eviction_candidates().len() == 2);
        Ok(())
    }

    #[tokio::test]
    async fn pinned_hashes_diff_counts_added_and_removed() -> anyhow::Result<()> {
        // Direct unit test of the diff helper, independent of the
        // engine swap path. Locks the API: a future caller stitching
        // log messages from `PinDiff` shouldn't break silently if the
        // counting changes shape.
        let h1 = Hash::new(b"one");
        let h2 = Hash::new(b"two");
        let h3 = Hash::new(b"three");

        let mut prev_set = HashSet::new();
        prev_set.insert(h1);
        prev_set.insert(h2);
        let prev = PinnedHashes::new(prev_set);

        let mut new_set = HashSet::new();
        new_set.insert(h2);
        new_set.insert(h3);
        let new = PinnedHashes::new(new_set);

        let diff = new.diff(&prev);
        anyhow::ensure!(diff.added == 1 && diff.removed == 1, "got {diff:?}");

        // Same set on both sides: zero diff.
        let no_change = new.diff(&new);
        anyhow::ensure!(no_change.added == 0 && no_change.removed == 0);
        Ok(())
    }

    // ----- Operator-evict (#279) -----

    /// `evict` must take a previously-cached hash off the served set. After
    /// evict, `has` reports false and `get` returns `NotFound` rather than
    /// silently re-pulling from the origin (which would defeat the point of
    /// the takedown / corruption-recovery use case behind issue #279).
    #[tokio::test]
    async fn evict_blocks_subsequent_serve() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"evict me";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 10).await?;

        // Prime the cache so we evict a real, served blob — covers the
        // hot path the operator would actually be evicting.
        let _ = engine.get(hash).await?;
        anyhow::ensure!(
            engine.has(hash).await?,
            "expected blob present before evict"
        );

        engine.evict(hash)?;

        anyhow::ensure!(engine.is_evicted(hash), "evict flag not set");
        anyhow::ensure!(
            !engine.has(hash).await?,
            "has() should report absent after evict"
        );
        match engine.get(hash).await {
            Err(CacheError::NotFound { .. }) => Ok(()),
            other => Err(anyhow::anyhow!(
                "expected NotFound after evict, got {other:?}"
            )),
        }
    }

    /// Eviction must survive a process restart — operators running DMCA
    /// takedowns rely on the evict being durable. The on-disk
    /// `evicted.log` is replayed by a fresh `open()`. Without this test,
    /// a regression that dropped the persistence path (e.g. moved to
    /// in-memory-only) would let evicted content silently resume serving
    /// after a restart.
    #[tokio::test]
    async fn evict_persists_across_open() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"persist me";
        let hash = Hash::new(payload);

        // First open: prime + evict.
        {
            let origin = Arc::new(StubOrigin::new(payload));
            let engine = CacheEngine::open(tmp.path(), Some(origin), 10).await?;
            let _ = engine.get(hash).await?;
            engine.evict(hash)?;
            engine.shutdown().await?;
        }

        // Second open: same cache_dir, no origin so a re-pull would fail
        // loudly. The evicted set must reload from disk.
        let engine2 = CacheEngine::open(tmp.path(), None, 10).await?;
        anyhow::ensure!(
            engine2.is_evicted(hash),
            "evict flag should reload from <cache_dir>/evicted.log"
        );
        anyhow::ensure!(
            !engine2.has(hash).await?,
            "has() should report absent after restart"
        );
        Ok(())
    }

    /// Malformed lines in `evicted.log` (manual edit gone wrong, partial
    /// write from an old crash) must not stop the engine from opening;
    /// they get logged + skipped, and the well-formed lines still load.
    #[tokio::test]
    async fn evicted_log_skips_malformed_lines() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let log_path = tmp.path().join("evicted.log");
        let good = Hash::new(b"good entry");
        std::fs::write(
            &log_path,
            format!("\n# operator note\n{good}\nnot-a-hash\n{good}\n"),
        )?;

        let engine = CacheEngine::open(tmp.path(), None, 10).await?;
        anyhow::ensure!(engine.is_evicted(good), "valid hash line not loaded");
        Ok(())
    }

    #[tokio::test]
    async fn evict_unknown_hash_is_a_no_op() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), None, 10).await?;
        let unknown = Hash::new(b"never seen");
        // Evicting a hash we've never cached is fine — operators may run
        // `decdn node evict` ahead of time as a precaution.
        engine.evict(unknown)?;
        anyhow::ensure!(engine.is_evicted(unknown), "evict flag not set");
        Ok(())
    }

    /// Evicting the same hash twice must not append a duplicate line to
    /// `<cache_dir>/evicted.log`. Without this contract a stuck
    /// automation that mass-replays the same DMCA-takedown hash would
    /// grow the log unboundedly. The first evict appends one line, the
    /// second short-circuits via the `contains(&hash)` check at the top
    /// of `evict()`.
    #[tokio::test]
    async fn evict_is_idempotent_and_does_not_grow_log() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let log_path = tmp.path().join("evicted.log");
        let engine = CacheEngine::open(tmp.path(), None, 10).await?;
        let hash = Hash::new(b"dup-evict");

        engine.evict(hash)?;
        let after_first = std::fs::read_to_string(&log_path)?;
        let lines_first = after_first.lines().count();

        engine.evict(hash)?;
        engine.evict(hash)?;
        let after_third = std::fs::read_to_string(&log_path)?;
        let lines_third = after_third.lines().count();

        anyhow::ensure!(
            lines_first == 1 && lines_third == 1,
            "expected 1 log line both times, got first={lines_first}, third={lines_third}"
        );
        Ok(())
    }

    /// Hand-edited uppercase hex in `evicted.log` must be tolerated by
    /// `parse_hex_hash` — the persisted format is canonically lowercase
    /// (`Hash::Display` calls `to_hex()`), but operators pasting from
    /// access logs / takedown notices may use either case. Without this,
    /// a mixed-case hand-edit would silently get dropped at next open
    /// and the takedown would resume serving content.
    #[tokio::test]
    async fn evicted_log_accepts_uppercase_hex() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let log_path = tmp.path().join("evicted.log");
        let hash = Hash::new(b"upper-hex");
        let upper = hash.to_string().to_uppercase();
        std::fs::write(&log_path, format!("{upper}\n"))?;

        let engine = CacheEngine::open(tmp.path(), None, 10).await?;
        anyhow::ensure!(
            engine.is_evicted(hash),
            "uppercase hex line should load as the same hash",
        );
        Ok(())
    }

    /// `evict()` must surface a persistence failure as `Err` rather than
    /// silently degrading to in-memory-only — for DMCA-driven evicts the
    /// operator must be able to tell whether the takedown is durable.
    /// Forcing the failure: place a *directory* at the `evicted.log`
    /// path so `OpenOptions::open(...)` fails (`EISDIR`) when
    /// `append_evicted_log` runs. Avoids relying on filesystem
    /// permission games that may not work uniformly across CI hosts.
    #[tokio::test]
    async fn evict_returns_err_when_persistence_fails() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), None, 10).await?;
        // Engine opened cleanly with no log file yet. Now plant a
        // directory at the path the engine will try to append to.
        std::fs::create_dir(tmp.path().join("evicted.log"))?;

        let hash = Hash::new(b"persist-fail");
        match engine.evict(hash) {
            Err(CacheError::Store(_)) => Ok(()),
            other => Err(anyhow::anyhow!(
                "expected Store error from persistence failure, got {other:?}"
            )),
        }?;

        // And the in-memory set must NOT have been updated — otherwise
        // an operator seeing the error would (correctly) assume the
        // takedown didn't land, but the running node would actually have
        // already stopped serving. Either contract is reasonable on its
        // own; mixing them is the worst case.
        anyhow::ensure!(
            !engine.is_evicted(hash),
            "in-memory set must not commit when persistence fails",
        );
        Ok(())
    }

    // ----- inspect / dry-run preview (#379) -----

    /// `inspect` on a freshly-opened cache must report all the
    /// "absent" sentinels: no size, no last access, not pinned, not
    /// evicted, not served. Locks the wire-shape contract so a
    /// regression that defaulted `served` to `true` (or that swallowed
    /// the iroh-blobs `NotFound` arm) is caught on every CI run.
    #[tokio::test]
    async fn inspect_unknown_hash_reports_absent() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), None, 10).await?;
        let unknown = Hash::new(b"never seen");

        let preview = engine.inspect(unknown).await?;
        anyhow::ensure!(preview.size_bytes.is_none(), "expected size_bytes=None");
        anyhow::ensure!(
            preview.last_accessed_us_ago.is_none(),
            "expected last_accessed_us_ago=None"
        );
        anyhow::ensure!(!preview.pinned, "expected pinned=false");
        anyhow::ensure!(!preview.already_evicted, "expected already_evicted=false");
        anyhow::ensure!(!preview.served, "expected served=false");
        Ok(())
    }

    /// After a successful pull-through `get`, `inspect` reports the
    /// concrete blob size, a finite `last_accessed_us_ago`, and
    /// `served=true`. Asserts the size matches the payload exactly so
    /// a regression that returned the partial-blob size (or a wrong
    /// match arm in the `BlobStatus` decode) fails loudly.
    #[tokio::test]
    async fn inspect_after_get_reports_size_and_served() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"inspect me";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 10).await?;
        let _ = engine.get(hash).await?;

        let preview = engine.inspect(hash).await?;
        anyhow::ensure!(
            preview.size_bytes == Some(payload.len() as u64),
            "expected size_bytes={:?}, got {:?}",
            Some(payload.len() as u64),
            preview.size_bytes,
        );
        anyhow::ensure!(
            preview.last_accessed_us_ago.is_some(),
            "expected Some(last_accessed_us_ago) after get()"
        );
        anyhow::ensure!(preview.served, "expected served=true");
        anyhow::ensure!(!preview.already_evicted, "expected already_evicted=false");
        anyhow::ensure!(!preview.pinned, "expected pinned=false");
        Ok(())
    }

    /// `inspect` after `evict` must still report the on-disk
    /// `size_bytes` (until #233 reclaims) but flip `already_evicted`
    /// to `true` and `served` to `false`. The size-still-reported part
    /// is the load-bearing assertion: dry-run callers want to see
    /// disk-reclaim potential, not a clean `None` that hides the bytes.
    #[tokio::test]
    async fn inspect_after_evict_keeps_size_but_flips_served() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"evicted blob";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 10).await?;
        let _ = engine.get(hash).await?;
        engine.evict(hash)?;

        let preview = engine.inspect(hash).await?;
        anyhow::ensure!(
            preview.size_bytes == Some(payload.len() as u64),
            "expected size_bytes still reported post-evict, got {:?}",
            preview.size_bytes,
        );
        anyhow::ensure!(preview.already_evicted, "expected already_evicted=true");
        anyhow::ensure!(!preview.served, "expected served=false post-evict");
        // Eviction clears the access-time entry, so this should now be None.
        anyhow::ensure!(
            preview.last_accessed_us_ago.is_none(),
            "expected last_accessed cleared by evict, got {:?}",
            preview.last_accessed_us_ago,
        );
        Ok(())
    }

    /// `inspect` reflects the operator-pinned set without needing a
    /// blob to be cached. The pinned flag must be observable for
    /// hashes the operator hasn't fetched yet — that's the whole
    /// point of pre-flight dry-run: confirm policy state before
    /// committing to evict.
    #[tokio::test]
    async fn inspect_reports_pinned_flag() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let pinned_hash = Hash::new(b"pinned");
        let other_hash = Hash::new(b"other");
        let mut set = HashSet::new();
        set.insert(pinned_hash);
        let engine =
            CacheEngine::open_with_pinned(tmp.path(), None, 10, PinnedHashes::new(set)).await?;

        let pinned_preview = engine.inspect(pinned_hash).await?;
        anyhow::ensure!(pinned_preview.pinned, "expected pinned=true");
        anyhow::ensure!(
            !pinned_preview.served,
            "pinned-but-uncached blob should not be served"
        );

        let other_preview = engine.inspect(other_hash).await?;
        anyhow::ensure!(!other_preview.pinned, "unrelated hash must not be pinned");
        Ok(())
    }
}
