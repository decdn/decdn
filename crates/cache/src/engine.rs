//! Cache engine: local iroh-blobs store fronted by an [`Origin`] for misses.

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::Bytes;
use futures_util::StreamExt;
use iroh_blobs::Hash;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::fs::options::Options as FsStoreOptions;
use iroh_blobs::store::{GcConfig, ProtectOutcome};
use tokio::sync::{Notify, broadcast};

use decdn_config_types::{PinDiff, PinnedHashes, RetryPolicy};

use crate::error::{CacheError, CacheResult, OriginPullError};
use crate::metrics::CacheMetrics;
use crate::origin::{Origin, OriginKind};
use crate::probe_hold::ProbeHoldOutcome;
use crate::retry::{classify_io_error, drain_to_bytes, run_with_retry, should_buffer};
use crate::{from_store_hash, to_store_hash};

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
    /// Ordered list of origin backends consulted on cache misses
    /// (#284). Empty vec => no pull-through configured; `pull_through`
    /// short-circuits to `CacheError::NoOrigin`. A single-element vec
    /// preserves the pre-#284 single-origin semantics. Entries are
    /// tried in operator-supplied order; the next entry is consulted
    /// on `NotFound`, permanent error, or retry-budget exhaustion.
    /// Deterministic per-origin failures (`HashMismatch`,
    /// `BlobTooLarge`, and local-store errors classified as
    /// `Store`) deliberately do not fall back — they indicate a
    /// misbehaving backend or a degraded local store that must
    /// surface, not be masked by trying a different mirror.
    origins: Vec<Arc<dyn Origin>>,
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
    /// the underlying iroh-blobs store may still hold the bytes —
    /// `Blobs::delete` is `pub(crate)` in iroh-blobs and reserved for the
    /// GC task. Reclaim of disk bytes happens on the next iroh-blobs GC
    /// sweep, configured via `cache.gc_interval_sec` (#518). On-demand
    /// reclamation is tracked under #520, blocked on upstream exposing
    /// the sweep API.
    ///
    /// Persisted alongside the iroh-blobs store at `<cache_dir>/evicted.log`
    /// on every successful [`CacheEngine::evict`] call so DMCA takedowns and
    /// corruption-recovery evicts survive a process restart — an
    /// in-memory-only set would silently let evicted content resume serving
    /// after `decdn run` is restarted, which is exactly the failure mode
    /// #279 needs to prevent.
    evicted: Mutex<HashSet<Hash>>,
    /// Probe-triggered eviction holds (#318, ADR 005 §Probe-triggered
    /// eviction hold). Maps a held hash to its hold *expiry* instant; a
    /// held hash is invisible to [`CacheEngine::eviction_candidates`] until
    /// expiry, composing *above* the LRU layer. Holds are per-blob, not
    /// per-probe: many peers probing the same hash share (and refresh) one
    /// entry, so the slot count is bounded by distinct held blobs, not
    /// probe volume. Expired entries are swept lazily on every hold
    /// admission and every `eviction_candidates` call (no background task).
    ///
    /// Like [`Self::pinned`] this only blocks *LRU* eviction — an explicit
    /// operator [`CacheEngine::evict`] still wins (ADR
    /// appendix-blob-cache-eviction.md §4: DMCA always wins), enforced
    /// because [`CacheEngine::try_probe_hold`] gates on [`CacheEngine::has`]
    /// which already
    /// honors the evicted set.
    probe_holds: Mutex<HashMap<Hash, Instant>>,
    /// Hold-budget cap (ADR 005 §Hold budget). `0` disables `has_blob: true`
    /// entirely. Set once from `cache.max_probe_holds` via
    /// [`CacheEngine::set_max_probe_holds`] at runtime bring-up — `cache.*`
    /// is restart-required (not hot-reloaded), so an atomic written once is
    /// sufficient and avoids threading the value through every `open_*`
    /// constructor and its many test call sites.
    max_probe_holds: AtomicUsize,
    /// Append-only file holding lowercase-hex evicted hashes, one per line.
    /// Loaded on [`CacheEngine::open`]; appended to (with `fsync`) on every
    /// successful [`CacheEngine::evict`]. Lives at `<cache_dir>/evicted.log`.
    /// The format is intentionally trivial so operators can grep / inspect /
    /// hand-edit it during incident response; duplicate lines are tolerated
    /// (loading deduplicates via the `HashSet`).
    evicted_log_path: PathBuf,
    /// Origin pull-through retry policy (#285). Set once at construction;
    /// changes require a restart. `RetryPolicy: Copy` so the per-fetch
    /// read is a single struct copy.
    retry_policy: RetryPolicy,
    /// Optional handle to the cache-side `OpenMetrics` counters (#285).
    /// The engine bumps `origin_fetches` once per pull-through and the
    /// retry loop bumps `origin_retry_exhausted` on terminal exhaustion.
    /// `None` in tests / non-metrics builds — bumps short-circuit.
    metrics: Option<Arc<CacheMetrics>>,
    /// Broadcast channel announcing successful blob commits to interested
    /// subscribers (DHT republish per ADR 022 §STORE Flow). Producers (the
    /// `pull_through` success arm) call `send` and ignore the
    /// "no-active-receivers" error — broadcast is fire-and-forget. The
    /// channel is bounded; lagged receivers see [`broadcast::error::RecvError::Lagged`]
    /// on the next recv and decide for themselves whether to backfill —
    /// the DHT-side subscriber treats it as "force a republish sweep on
    /// the next tick", so a brief stall in the consumer doesn't lose
    /// blobs from the republish set.
    inserts_tx: broadcast::Sender<Hash>,
    /// Strong reference to the GC callback's late-bound `FsStore`
    /// handle (#518). `Some` when `gc_interval > 0` was passed to
    /// [`CacheEngine::open_full`], `None` when GC is disabled.
    ///
    /// **Why this lives here:** iroh-blobs spawns its GC loop on its
    /// internal runtime when `Options.gc` is set. The loop's cb captures
    /// a [`Weak<OnceLock<FsStore>>`] rather than a strong `Arc`, so the
    /// cb itself contributes no strong refcount to the iroh-blobs
    /// `FsStore` handle. When this `Inner` drops, the strong `Arc` here
    /// drops with it; subsequent cb fires see `Weak::upgrade -> None`
    /// and silently no-op (no metric writes, no work) — the cb is no
    /// longer wedged in a cycle that keeps it alive.
    ///
    /// **What this does NOT do:** the iroh-blobs GC loop itself
    /// (`run_gc(store: Store, ...)`) owns its own `Store` clone for the
    /// lifetime of its `loop`, separate from anything in `Inner`. So
    /// `Inner.drop()` does not shut iroh-blobs' internal runtime down;
    /// the runtime sits resident-but-idle until the surrounding process
    /// exits (or until `gc_run_once` errors and breaks the loop). For
    /// the current single-engine, process-lifetime model this is
    /// benign. #520 tracks driving the loop ourselves once iroh-blobs
    /// exposes `gc_run_once`, at which point engine drop will be able
    /// to abort the loop directly.
    ///
    /// **Invariant:** no `Arc::clone` of this field may escape `Inner`.
    /// Cloning the strong `Arc` into a longer-lived owner would re-
    /// introduce a cb-side strong ref via the round-trip and defeat
    /// the cycle-break, leaving the cb running with a populated
    /// `Weak` past engine drop.
    ///
    /// `dead_code` is allowed because nothing *reads* this field —
    /// its only purpose is keeping the `Arc` strong-ref alive for the
    /// lifetime of `Inner`. The `Weak` captured into the cb is the
    /// reading party.
    #[allow(dead_code)]
    gc_store_handle: Option<Arc<OnceLock<FsStore>>>,
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

// `PinnedHashes` and `PinDiff` moved to the `decdn-config-types` leaf
// crate (#578) and are imported above. The engine holds its pinned set
// internally as `HashSet<Hash>` (the iroh-blobs store hash) and converts
// at the public boundary via `to_store_hash` / `from_store_hash`.

/// Read-only snapshot of a hash's local-cache state, returned by
/// [`CacheEngine::inspect`]. Backs `decdn node evict --dry-run`
/// (issue #379): operators running DMCA takedowns or
/// corruption-recovery want to confirm the blob's size, last-access
/// time, pin status, and already-evicted flag before mutating state.
///
/// All fields reflect the *underlying* cache state — `size_bytes` reads
/// from the iroh-blobs store directly, so a hash that has already been
/// logically evicted (and whose bytes are still on disk pending the
/// follow-up GC sweep in #518) still reports its on-disk size here.
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
    /// Ordered list of backend kinds the engine would consult on a
    /// post-eviction miss (#439, #284). Empty when no origin is
    /// configured (cache-only mode); otherwise one [`OriginKind`] per
    /// entry of the operator-supplied fallback chain, in order.
    /// Operators running takedowns or LRU sweeps use this to estimate
    /// the worst-case origin egress cost — re-pulling from a
    /// `Filesystem` origin is a local read; re-pulling from `Http` or
    /// `S3` may consume metered bandwidth, and a chain of three S3
    /// origins multiplies the bill on a deep fallback.
    pub origin_kinds: Vec<OriginKind>,
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

/// `add_protected` body for [`CacheEngine::open_full`]'s GC wiring (#518).
///
/// Runs once per iroh-blobs sweep cycle, before `gc_run_once`. Snapshots
/// the current blob set, attributes "what disappeared since the last
/// snapshot" to the previous sweep's reclaim, bumps the cache metrics,
/// and saves the snapshot for the next cycle's diff. Always returns
/// without adding any hashes to `live` — named-tag promotion in
/// [`CacheEngine::pull_through`] and `TempTag` lifetimes are what protect
/// cached / in-progress blobs; this callback is purely instrumentation.
///
/// **Why the spawn-and-wait dance:** iroh-blobs requires the
/// `ProtectCb` future to be `Send + Sync`, but its own RPC layer
/// (`blobs().list().stream()`, `blobs().status(...)`) returns futures
/// that are only `Send`. Awaiting those directly here would leak the
/// non-Sync constraint into our outer future. Spawning the snapshot
/// work on a separate task and awaiting a [`tokio::sync::oneshot`]
/// receiver (Send + Sync as long as `T: Send`) gives the outer future
/// the auto-trait shape iroh-blobs demands.
///
/// **Errors are logged and dropped, never propagated as `Abort`.** Any
/// future refactor that wants to surface them should keep returning
/// [`ProtectOutcome::Continue`] — `Abort` would skip the sweep itself,
/// letting the disk-leak threat the GC was added to mitigate keep
/// growing. The next cycle's snapshot recovers the count attribution as
/// long as the store eventually services the list/status calls.
///
/// **Engine-drop semantics:** `store_handle` is a [`Weak`] of the
/// `Arc<OnceLock<FsStore>>` that lives in `Inner.gc_store_handle`.
/// When the engine drops, that strong `Arc` drops with it, the
/// `Weak::upgrade` here returns `None`, and the cb returns silently —
/// no metric writes, no work, no contribution to the cb's surviving
/// strong-ref graph. iroh-blobs' GC loop itself owns its own `Store`
/// clone (via `run_gc(store: Store, ...)`) and keeps running for the
/// rest of the process lifetime; that's an upstream design constraint
/// tracked under #520. What this `Weak` ensures is that the cb body
/// no-ops cleanly past engine drop rather than spuriously snapshotting
/// or bumping metrics on a drained engine.
async fn gc_protect_callback(
    store_handle: Weak<OnceLock<FsStore>>,
    prev_pre_sweep: Arc<Mutex<HashMap<Hash, u64>>>,
    metrics: Option<Arc<CacheMetrics>>,
) {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        gc_protect_inner(store_handle, prev_pre_sweep, metrics).await;
        let _ = tx.send(());
    });
    if let Err(err) = rx.await {
        // The spawned task panicked or was dropped before sending.
        // Surface it: a panic in `gc_protect_inner` would otherwise be
        // invisible (this cb just returns `Continue` either way).
        tracing::warn!(%err, "gc protect spawn dropped without completing; metrics may have skipped a cycle");
    }
}

async fn gc_protect_inner(
    store_handle: Weak<OnceLock<FsStore>>,
    prev_pre_sweep: Arc<Mutex<HashMap<Hash, u64>>>,
    metrics: Option<Arc<CacheMetrics>>,
) {
    // `Weak::upgrade` returning `None` is the post-engine-drop steady
    // state: `Inner.gc_store_handle` (the strong `Arc`) has dropped,
    // the iroh-blobs runtime is in the process of being aborted, and
    // any further cb fires before the abort lands have nothing to
    // measure against. Quietly skip — this is not an error.
    let Some(store_arc) = store_handle.upgrade() else {
        return;
    };
    let Some(store) = store_arc.get() else {
        // Practically unreachable: iroh-blobs' `run_gc` calls `sleep`
        // first, and we `set` the OnceLock immediately after
        // `load_with_opts` returns. A `None` here would mean a future
        // refactor enabled near-zero intervals or moved the `set`. Log
        // and skip rather than panic.
        tracing::warn!("gc protect callback fired before store handle was set");
        return;
    };

    let current = match snapshot_blob_sizes(store).await {
        Ok(snap) => snap,
        Err(err) => {
            tracing::warn!(%err, "gc snapshot failed; skipping reclaim attribution this cycle");
            return;
        }
    };

    let reclaimed_bytes: u64 = {
        // Surface poison: this mutex guards metrics-only state, so a
        // poisoned lock means a *prior* invocation of this function
        // panicked while holding the guard (i.e., somewhere in the
        // diff/fold loop). Different from the operational mutexes
        // (`evicted`, `access_times`) where silent recovery is correct.
        // Recover the inner state to keep metrics flowing — the next
        // cycle re-establishes a baseline — but log once so the panic
        // doesn't hide.
        let mut guard = match prev_pre_sweep.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                tracing::warn!("gc prev_pre_sweep mutex poisoned; recovering inner state");
                poisoned.into_inner()
            }
        };
        let bytes = guard
            .iter()
            .filter(|(hash, _)| !current.contains_key(*hash))
            .fold(0u64, |acc, (_, size)| acc.saturating_add(*size));
        *guard = current;
        bytes
    };

    if let Some(m) = metrics.as_ref() {
        m.gc_runs.inc();
        m.gc_bytes_reclaimed.inc_by(reclaimed_bytes);
    }
}

/// Snapshot the iroh-blobs blob set keyed by hash, with each entry's
/// size in bytes. Used by [`gc_protect_callback`] to diff sweep cycles
/// (#518). Hashes that race the snapshot (deleted between `list` and
/// `status`) report `BlobStatus::NotFound` and are dropped — they
/// cannot have contributed bytes either way.
///
/// `Partial` blobs report whatever size iroh-blobs has on disk so far
/// (`None` when the store can't tell us, treated as zero). Including
/// partials matters: the threat model that motivated #518 is exactly
/// the partial-import case (`add_stream` errored mid-flight, the
/// `TempTag` was dropped, but the bytes already on disk are what we
/// want GC to reclaim).
async fn snapshot_blob_sizes(store: &FsStore) -> CacheResult<HashMap<Hash, u64>> {
    let blobs = store.blobs();
    let mut stream = blobs
        .list()
        .stream()
        .await
        .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
    let mut out = HashMap::new();
    while let Some(hash) = stream.next().await {
        let hash = hash.map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        let status = blobs
            .status(hash)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        let size = match status {
            iroh_blobs::api::blobs::BlobStatus::NotFound => continue,
            iroh_blobs::api::blobs::BlobStatus::Partial { size } => size.unwrap_or(0),
            iroh_blobs::api::blobs::BlobStatus::Complete { size } => size,
        };
        out.insert(hash, size);
    }
    Ok(out)
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
        origins: Vec<Arc<dyn Origin>>,
        max_blob_mb: u64,
    ) -> CacheResult<Self> {
        Self::open_with_pinned(cache_dir, origins, max_blob_mb, PinnedHashes::empty()).await
    }

    /// Open the cache with an initial pinning set. The set is held in an
    /// [`ArcSwap`] internally so subsequent SIGHUP reloads can call
    /// [`Self::set_pinned`] without rebuilding the engine.
    ///
    /// Defaults the retry policy to [`RetryPolicy::default`], wires no
    /// metrics handle, and disables periodic GC. Use [`Self::open_full`] to
    /// override any of those — the node crate plugs in its
    /// `Arc<CacheMetrics>`, the operator-configured retry policy, and the
    /// `cache.gc_interval_sec` through that path.
    pub async fn open_with_pinned(
        cache_dir: &Path,
        origins: Vec<Arc<dyn Origin>>,
        max_blob_mb: u64,
        pinned: PinnedHashes,
    ) -> CacheResult<Self> {
        Self::open_full(
            cache_dir,
            origins,
            max_blob_mb,
            pinned,
            RetryPolicy::default(),
            None,
            Duration::ZERO,
        )
        .await
    }

    /// Open the cache with full control over policy, metrics, and GC wiring.
    /// The runtime's `build_cache` uses this directly; tests usually want
    /// [`Self::open`] or [`Self::open_with_pinned`] with their defaults.
    ///
    /// `gc_interval` controls iroh-blobs' built-in GC sweep loop (#518).
    /// [`Duration::ZERO`] disables periodic GC; any other value is forwarded
    /// to [`iroh_blobs::store::GcConfig`] and iroh-blobs spawns its own GC
    /// task on its internal runtime. Reclaimable bytes accumulate between
    /// sweeps — set the interval based on how much disk you're willing to
    /// lose to a hostile-origin amplification window.
    ///
    /// **Why iroh-blobs drives the loop instead of us:** the sweep
    /// function (`gc::gc_run_once`) lives in iroh-blobs 0.100's private
    /// `store::gc` module and is not re-exported, and `Blobs::delete`
    /// is `pub(crate)`. The only externally-reachable trigger is
    /// `Options::gc`. #520 tracks switching to a runtime-driven loop
    /// with manual on-demand GC (`admin_v1_cacheGc` / `decdn node gc`)
    /// once upstream exposes the sweep API.
    pub async fn open_full(
        cache_dir: &Path,
        origins: Vec<Arc<dyn Origin>>,
        max_blob_mb: u64,
        pinned: PinnedHashes,
        retry_policy: RetryPolicy,
        metrics: Option<Arc<CacheMetrics>>,
        gc_interval: Duration,
    ) -> CacheResult<Self> {
        tokio::fs::create_dir_all(cache_dir)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;

        // Late-bound store handle for the GC `add_protected` callback. The
        // callback is captured into `Options.gc` *before* `FsStore` exists,
        // but the cb needs to query `blobs().list()` to compute the
        // pre-sweep snapshot. Solution: build `Arc<OnceLock<FsStore>>` here,
        // capture a `Weak` into the cb, and `set` the OnceLock as soon as
        // the store handle is in hand. The strong `Arc` is moved into
        // `Inner.gc_store_handle` so its lifetime tracks the engine; on
        // engine drop the cb's `Weak::upgrade` returns `None`, which is
        // exactly what breaks the cb → `FsStore` clone → iroh-blobs actor
        // → GC task → cb cycle.
        //
        // `None` when GC is disabled — no cb is registered, so no late-
        // bind handle is needed.
        let gc_store_handle: Option<Arc<OnceLock<FsStore>>> = if gc_interval.is_zero() {
            None
        } else {
            Some(Arc::new(OnceLock::new()))
        };

        // Previous-cycle pre-sweep snapshot. The cb runs once per cycle
        // *before* `gc_run_once`, so the diff between the snapshot saved
        // last cycle and the snapshot taken this cycle is exactly the
        // hash set the previous sweep deleted (the cache crate has no
        // other public delete path: `Blobs::delete` is `pub(crate)` in
        // iroh-blobs 0.100). Bytes for that diff is what the previous
        // sweep reclaimed; we attribute it on the *current* cb fire.
        // First-cycle fires bump the runs counter but emit zero on
        // the reclaim counter because there is no prior snapshot.
        let prev_pre_sweep: Arc<Mutex<HashMap<Hash, u64>>> = Arc::new(Mutex::new(HashMap::new()));

        let mut options = FsStoreOptions::new(cache_dir);
        if let Some(strong) = gc_store_handle.as_ref() {
            let store_weak: Weak<OnceLock<FsStore>> = Arc::downgrade(strong);
            let prev_for_cb = Arc::clone(&prev_pre_sweep);
            let metrics_for_cb = metrics.clone();
            options.gc = Some(GcConfig {
                interval: gc_interval,
                add_protected: Some(Arc::new(move |_live: &mut HashSet<Hash>| {
                    let store_weak = store_weak.clone();
                    let prev_for_cb = Arc::clone(&prev_for_cb);
                    let metrics_for_cb = metrics_for_cb.clone();
                    Box::pin(async move {
                        gc_protect_callback(store_weak, prev_for_cb, metrics_for_cb).await;
                        ProtectOutcome::Continue
                    })
                })),
            });
        }

        let db_path = cache_dir.join("blobs.db");
        let store = FsStore::load_with_opts(db_path, options)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;

        // Late-bind: the cb's `Weak<OnceLock<FsStore>>` can now upgrade
        // and `.get()` to reach the store handle. `set` only fails if
        // the OnceLock was already populated, which can't happen on
        // this code path (we just constructed it).
        if let Some(strong) = gc_store_handle.as_ref() {
            let _ = strong.set(store.clone());
        }

        // Saturate-on-overflow: an operator setting `max_blob_mb = u64::MAX`
        // as a de-facto "unlimited" value should still yield a usable byte cap
        // rather than overflow-wrap to zero.
        let max_blob_bytes = max_blob_mb.saturating_mul(1024 * 1024);

        let evicted_log_path = cache_dir.join("evicted.log");
        let evicted = load_evicted_log(&evicted_log_path)?;

        Ok(Self {
            inner: Arc::new(Inner {
                store,
                origins,
                max_blob_bytes,
                access_times: Mutex::new(HashMap::new()),
                inflight: Mutex::new(HashMap::new()),
                pinned: ArcSwap::from(Arc::new(
                    pinned
                        .iter()
                        .map(|h| to_store_hash(*h))
                        .collect::<HashSet<Hash>>(),
                )),
                evicted: Mutex::new(evicted),
                probe_holds: Mutex::new(HashMap::new()),
                max_probe_holds: AtomicUsize::new(crate::probe_hold::DEFAULT_MAX_PROBE_HOLDS),
                evicted_log_path,
                retry_policy,
                metrics,
                // Bounded channel — slow consumers (e.g. a republish
                // scheduler under load) lag instead of backpressuring
                // the cache hot path. Cap of 1024 matches the dispatch
                // limiter's similar in-flight slot count; sized
                // generously enough that a steady-state pull rate above
                // 1000/s would have to also lose all subscribers for
                // the lag to actually fire.
                inserts_tx: broadcast::channel(1024).0,
                gc_store_handle,
            }),
        })
    }

    /// Subscribe to a stream of `Hash`es announcing every blob that
    /// successfully landed in the local store via the cache's
    /// pull-through path (the private `pull_through` is the single
    /// convergence point).
    /// Used by the DHT republish scheduler (ADR 022 §STORE Flow line
    /// 124 — "When a node caches blob H ...") to schedule the first
    /// publish-set to the K+3 closest peers.
    ///
    /// The channel is bounded and best-effort: lagged receivers see a
    /// [`broadcast::error::RecvError::Lagged`] on the next recv and
    /// MUST treat it as "force a full republish sweep" rather than try
    /// to backfill — the cache does not retain the missed hashes.
    #[must_use]
    pub fn subscribe_inserts(&self) -> broadcast::Receiver<Hash> {
        self.inner.inserts_tx.subscribe()
    }

    /// Atomically swap the pinned-hashes set. Called by the runtime's
    /// SIGHUP handler when `cache.pinned_hashes` changes — readers (the
    /// eviction-candidate snapshot) observe either the old or the new set,
    /// never a partial mix. Returns a [`PinDiff`] so callers can log
    /// "added X, removed Y" without re-walking either set.
    ///
    /// Takes `&PinnedHashes` rather than ownership: callers commonly
    /// want to compute the diff without giving up their own copy. The
    /// leaf [`PinnedHashes`] is converted to the engine-internal
    /// `HashSet<Hash>` (iroh-blobs store hash) here — the cold SIGHUP
    /// path, so the O(n) conversion is acceptable; the hot
    /// `is_pinned`/`eviction_candidates` lookups stay on the store hash.
    pub fn set_pinned(&self, new: &PinnedHashes) -> PinDiff {
        let new_arc = Arc::new(
            new.iter()
                .map(|h| to_store_hash(*h))
                .collect::<HashSet<Hash>>(),
        );
        let prev_arc = self.inner.pinned.swap(Arc::clone(&new_arc));
        // Same set-difference arithmetic as `PinnedHashes::diff` (in
        // `decdn-config-types`), recomputed here over the store-hash
        // sets instead of round-tripping back to leaf hashes. The two
        // must stay behaviorally identical — `to_store_hash` is a
        // bijection on the 32 bytes, so cardinalities and membership are
        // preserved; if `PinnedHashes::diff` ever reports more than
        // counts (e.g. a sample of changed hashes), update both.
        let added = new_arc.iter().filter(|h| !prev_arc.contains(*h)).count();
        let removed = prev_arc.iter().filter(|h| !new_arc.contains(*h)).count();
        PinDiff { added, removed }
    }

    /// Borrow a snapshot of the current pinned set, rebuilt as the leaf
    /// [`PinnedHashes`] (config-vocabulary hash) from the engine-internal
    /// store-hash set.
    ///
    /// **Cost:** O(n) — unlike the pre-#578 single `Arc::clone`, this
    /// allocates a fresh `HashSet` and converts every hash across the
    /// store↔leaf boundary. Call it off the hot path (it backs SIGHUP
    /// reload logging and admin snapshots, not per-request lookups).
    pub fn pinned_snapshot(&self) -> PinnedHashes {
        let set = self
            .inner
            .pinned
            .load()
            .iter()
            .map(|h| from_store_hash(*h))
            .collect::<HashSet<decdn_config_types::Hash>>();
        PinnedHashes::new(set)
    }

    /// Snapshot of the active retry policy. Used by tests and startup
    /// logging. The policy is fixed at construction; changes require
    /// a restart.
    pub fn retry_policy(&self) -> RetryPolicy {
        self.inner.retry_policy
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
    /// This is a *logical* evict: `Blobs::delete` is `pub(crate)` in
    /// iroh-blobs and reserved for the GC task, so the bytes remain on
    /// disk until the next iroh-blobs GC sweep reclaims them (#518; the
    /// sweep cadence is `cache.gc_interval_sec`, default 5min).
    /// The operator-visible behavior — the
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
    ///
    /// Lock structure: the three in-memory probes hit independent
    /// synchronization primitives — `evicted` (`Mutex<HashSet>`),
    /// `access_times` (`Mutex<HashMap>`), and `pinned` (`ArcSwap`).
    /// Each is held for an O(1) lookup; merging them into a single
    /// lock acquisition would require either combining the underlying
    /// data structures (a much larger refactor that would couple
    /// unrelated invariants) or holding a coarser lock across the
    /// async `BlobStatus` call (which would block the `get()` hot
    /// path on whichever store backend is slower). Same pattern as
    /// [`Self::eviction_candidates`].
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

        let origin_kinds = self.inner.origins.iter().map(|o| o.kind()).collect();

        Ok(EvictionPreview {
            size_bytes,
            last_accessed_us_ago,
            pinned: self.is_pinned(hash),
            already_evicted,
            served,
            origin_kinds,
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

    /// Set the probe-hold budget cap from `cache.max_probe_holds` (ADR 005
    /// §Hold budget, #318). Called once by the runtime at bring-up. `0`
    /// disables the hold path so [`Self::try_probe_hold`] always returns
    /// `false` (the node then answers `has_blob: false` to every probe).
    pub fn set_max_probe_holds(&self, max: usize) {
        self.inner.max_probe_holds.store(max, Ordering::Relaxed);
    }

    /// Attempt to take (or refresh) a probe-triggered eviction hold on
    /// `hash` for [`crate::probe_hold::PROBE_HOLD_DURATION`] (ADR 005
    /// §Probe-triggered eviction hold).
    ///
    /// Returns [`ProbeHoldOutcome::Held`] only when the node may safely sign
    /// `has_blob: true`: the blob is present, not operator-evicted, **and** a
    /// hold is guaranteed for the full slashing window. Otherwise returns
    /// [`ProbeHoldOutcome::Unavailable`] (absent/evicted) or
    /// [`ProbeHoldOutcome::BudgetExhausted`] (present but `max == 0` or all
    /// slots in use) so the caller signs `has_blob: false` without a second
    /// cache lookup to classify the miss.
    ///
    /// Per-blob semantics: a hash already held has its expiry refreshed and
    /// consumes no additional slot, so many peers probing one popular blob
    /// share a single hold (ADR 005 §Hold budget). The common
    /// already-held case is an O(1) lookup that skips the O(N) expiry
    /// sweep; the sweep runs only when no live hold exists, bounding map
    /// growth without a background task.
    ///
    /// The operator-evicted set is re-checked **while holding the
    /// `probe_holds` lock**, closing the TOCTOU window where a concurrent
    /// [`Self::evict`] (DMCA takedown) could land between the initial
    /// [`Self::has`] check and granting the hold — a takedown always wins
    /// (ADR appendix-blob-cache-eviction.md §4).
    pub async fn try_probe_hold(&self, hash: Hash) -> CacheResult<ProbeHoldOutcome> {
        // `has` returns false for operator-evicted hashes too.
        if !self.has(hash).await? {
            return Ok(ProbeHoldOutcome::Unavailable);
        }
        let max = self.inner.max_probe_holds.load(Ordering::Relaxed);
        let now = Instant::now();
        // `Instant + Duration` panics on overflow; saturate instead to keep
        // the workspace anti-panic policy (clippy `unwrap_used`/`panic`).
        let expiry = now
            .checked_add(crate::probe_hold::PROBE_HOLD_DURATION)
            .unwrap_or(now);
        let mut guard = self
            .inner
            .probe_holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        // Fast path for popular re-probed blobs: a live hold already exists.
        // O(1), no sweep. Still re-check the takedown set under the lock so a
        // refresh can't resurrect just-evicted content.
        if guard.get(&hash).is_some_and(|exp| *exp > now) {
            if self.is_evicted(hash) {
                return Ok(ProbeHoldOutcome::Unavailable);
            }
            guard.insert(hash, expiry);
            return Ok(ProbeHoldOutcome::Held);
        }

        // No live hold — sweep expired entries before consulting the budget.
        guard.retain(|_, exp| *exp > now);
        // TOCTOU re-check: a concurrent `evict()` may have completed after
        // the `has()` above. Under the lock, an evicted hash is never held.
        if self.is_evicted(hash) {
            return Ok(ProbeHoldOutcome::Unavailable);
        }
        if guard.len() >= max {
            // Covers both budget exhaustion and the `max == 0` (holds
            // disabled) case (ADR 005 §Hold budget).
            return Ok(ProbeHoldOutcome::BudgetExhausted);
        }
        guard.insert(hash, expiry);
        Ok(ProbeHoldOutcome::Held)
    }

    /// Number of currently-active (non-expired) probe holds, for the
    /// `probe_hold_slots_used` metric (ADR 005). Sweeps expired entries as
    /// a side effect so the gauge reflects live holds even with no probe
    /// traffic.
    pub fn probe_hold_slots_used(&self) -> usize {
        let now = Instant::now();
        let mut guard = self
            .inner
            .probe_holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        guard.retain(|_, exp| *exp > now);
        guard.len()
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
            let bytes = self.read_local(hash).await?;
            if let Some(m) = &self.inner.metrics {
                m.hits.inc();
                m.bytes_returned
                    .inc_by(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
            }
            return Ok(bytes);
        }

        // Logical-eviction guard (#279): once an operator has run
        // `decdn node evict <hash>`, a subsequent `get` must not silently
        // re-pull from the origin and undo the eviction. The eviction is
        // sticky for the life of `<cache_dir>/evicted.log` — there is no
        // "unevict" path; an operator who needs to re-cache a previously
        // evicted hash hand-edits the log and restarts.
        if self.is_evicted(hash) {
            if let Some(m) = &self.inner.metrics {
                m.misses.inc();
            }
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
                        let bytes = self.read_local(hash).await?;
                        // Waiter found the blob after the owner inserted it:
                        // semantically a hit. `bytes_returned` is bumped
                        // once after the loop for both arms; bump only
                        // `hits` here.
                        if let Some(m) = &self.inner.metrics {
                            m.hits.inc();
                        }
                        break bytes;
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
        if let Some(m) = &self.inner.metrics {
            m.bytes_returned
                .inc_by(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        }
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
    ///
    /// A third filter layer (after pinned, before the LRU sort) drops any
    /// hash under an active probe-triggered eviction hold (#318, ADR 005
    /// §Probe-triggered eviction hold; appendix-blob-cache-eviction.md §4:
    /// "a held hash is invisible to the LRU driver until the hold
    /// expires"). The held set is swept of expired entries here too, so a
    /// node with no probe traffic still releases stale holds.
    pub fn eviction_candidates(&self) -> EvictionCandidates {
        let pinned = self.inner.pinned.load();
        let now = Instant::now();
        let held: HashSet<Hash> = {
            let mut g = self
                .inner
                .probe_holds
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            g.retain(|_, exp| *exp > now);
            g.keys().copied().collect()
        };
        let Ok(guard) = self.inner.access_times.lock() else {
            return EvictionCandidates(HashMap::new());
        };
        let map = guard
            .iter()
            .filter_map(|(h, t)| {
                if pinned.contains(h) || held.contains(h) {
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

    async fn pull_through(&self, hash: Hash) -> CacheResult<Bytes> {
        // Every pull_through entry is a `get()` cache miss, regardless
        // of how the pull resolves. Coalesced waiters that find a hit
        // on retry never call `pull_through`, so they never reach this
        // bump (their `hits` increment lives in the waiter branch of
        // `get`).
        if let Some(m) = &self.inner.metrics {
            m.misses.inc();
        }
        // Reject pulls with no origin configured early — keeps the
        // per-attempt closure pure with respect to the origin handle.
        if self.inner.origins.is_empty() {
            return Err(CacheError::NoOrigin { hash });
        }

        // Bump the per-fetch denominator once per `pull_through` —
        // preserving #285 semantics where the counter is per
        // cache-miss-call, not per origin attempted. Alerts that
        // page on `origin_retry_exhausted_total / origin_fetches_total`
        // continue to be a per-call ratio; the new
        // `origin_fallback_total` separately counts chain-walk steps.
        if let Some(m) = &self.inner.metrics {
            m.origin_fetches.inc();
        }
        let max_blob_bytes = self.inner.max_blob_bytes;
        let policy = self.inner.retry_policy;

        // Fallback chain walk (#284). Each origin gets its own retry
        // budget; on retry-exhaustion / Permanent / NotFound we advance
        // to the next entry. Deterministic per-origin failures
        // (`HashMismatch`, `BlobTooLarge`, `Store`) intentionally do
        // not fall back — they indicate a misbehaving backend that
        // must surface, not be masked by trying a different one.
        let mut last_err: Option<OriginPullError> = None;
        let mut any_not_found = false;
        let total = self.inner.origins.len();
        for (idx, origin) in self.inner.origins.iter().enumerate() {
            let origin = Arc::clone(origin);
            let outcome = run_with_retry(policy, self.inner.metrics.as_ref(), hash, || {
                self.pull_through_attempt(Arc::clone(&origin), hash, max_blob_bytes, policy)
            })
            .await;

            // Track *this iteration's* outcome class so the post-match
            // log records the correct cause. `last_err.is_some()` is
            // cumulative across iterations and would mislabel a later
            // NotFound advance as "primary failed" once any earlier
            // origin had errored.
            let advance_was_error = match outcome {
                Ok(PullThroughOutcome::Bytes(bytes)) => {
                    // Announce the successful commit to any DHT
                    // republish-scheduler subscribers (ADR 022 §STORE
                    // Flow). `broadcast::send` returns `Err(SendError)`
                    // only when there are no active subscribers, which
                    // is the normal state when no DHT republish task
                    // exists — ignore. We deliberately do NOT emit on
                    // the local-store hit short-circuit at line ~1090:
                    // the consumer cares about *fresh* commits (which
                    // start a new TTL cycle), and a get-from-local
                    // doesn't change the holder's relationship with the
                    // blob.
                    let _ = self.inner.inserts_tx.send(hash);
                    return Ok(bytes);
                }
                Ok(PullThroughOutcome::NotFound) => {
                    any_not_found = true;
                    false
                }
                Ok(PullThroughOutcome::BlobTooLarge) => {
                    return Err(CacheError::BlobTooLarge {
                        hash,
                        limit_bytes: max_blob_bytes,
                    });
                }
                Ok(PullThroughOutcome::HashMismatch { actual }) => {
                    return Err(CacheError::HashMismatch {
                        expected: hash,
                        actual,
                    });
                }
                Ok(PullThroughOutcome::Store(err)) => return Err(CacheError::Store(err)),
                Err(e) => {
                    last_err = Some(e);
                    true
                }
            };

            // Final entry already tried; don't log a "fallback" for a
            // chain that has nowhere left to advance.
            if idx + 1 < total {
                self.emit_chain_advance(
                    hash,
                    idx,
                    origin.kind(),
                    advance_was_error,
                    last_err.as_ref(),
                );
            }
        }

        // All origins exhausted. Any non-NotFound failure beats a pure
        // NotFound because "known backend errored" is more diagnostic
        // than "no backend had it" — operators triaging a 5xx benefit
        // from the underlying error, and a real NotFound only fires
        // when every origin agreed the blob is absent.
        if let Some(e) = last_err {
            Err(CacheError::OriginError {
                hash,
                source: e.into_inner(),
            })
        } else if any_not_found {
            Err(CacheError::NotFound { hash })
        } else {
            // Structurally unreachable: every iteration of the loop
            // above takes exactly one match arm. The five non-`Err`
            // arms all `return`; the `NotFound` arm sets
            // `any_not_found`; the `Err` arm sets `last_err`. To reach
            // this branch the chain must be non-empty (`is_empty()`
            // check at the top of `pull_through`) and have produced
            // no `last_err` and no `any_not_found` — impossible under
            // the current `PullThroughOutcome` taxonomy. Reaching it
            // would mean a future variant was added without wiring
            // the corresponding flag, and a debug-only assert would
            // compile out in release builds. Emit an operator-visible
            // log and fall through to a `NotFound` surface so the
            // observable behaviour stays bounded (`unreachable!()`
            // would also be correct but the workspace policy prefers
            // a logged fallback over a release-panic in a hot path).
            tracing::error!(
                hash = %hash,
                "internal invariant violated: non-empty origin chain produced \
                 neither a NotFound nor an Err — likely a missing flag on a new \
                 PullThroughOutcome variant",
            );
            Err(CacheError::NotFound { hash })
        }
    }

    /// Bump the `origin_fallback` counter and emit a structured log for
    /// the chain-advance event (#284). Extracted from `pull_through`
    /// so the hot-path loop body stays small enough for clippy's
    /// cognitive-complexity lint, and so the WHY of the
    /// `warn!`-vs-`info!` branch decision lives in one named place
    /// rather than inline with retry control flow.
    ///
    /// `advance_was_error` is the *current iteration's* outcome class
    /// (not the cumulative `last_err.is_some()`), so a chain like
    /// `[Permanent, NotFound, Found]` logs `warn!` at the 0→1 step
    /// and `info!` at the 1→2 step rather than mislabeling the second
    /// step as "primary failed" by virtue of an earlier error still
    /// living in `last_err`. The caller is responsible for the
    /// `idx + 1 < total` guard that prevents a final-entry log; this
    /// method assumes a real advance is about to happen.
    fn emit_chain_advance(
        &self,
        hash: Hash,
        idx: usize,
        origin_kind: OriginKind,
        advance_was_error: bool,
        last_err: Option<&OriginPullError>,
    ) {
        if let Some(m) = &self.inner.metrics {
            m.origin_fallback.inc();
        }
        if advance_was_error {
            // Carry this origin's error in the log so operators
            // triaging the user-visible 5xx see *this* backend's
            // failure mode, not just the chain-final one surfaced via
            // `CacheError::OriginError`. `last_err` was assigned by
            // the `Err(e)` arm of this iteration and is `Some` here.
            tracing::warn!(
                hash = %hash,
                origin_index = idx,
                origin_kind = ?origin_kind,
                error = last_err.map(ToString::to_string).unwrap_or_default(),
                "advancing to next origin in fallback chain (origin failed)",
            );
        } else {
            tracing::info!(
                hash = %hash,
                origin_index = idx,
                origin_kind = ?origin_kind,
                "advancing to next origin in fallback chain (NotFound)",
            );
        }
    }

    /// A single end-to-end pull-through attempt: origin.fetch +
    /// (buffer-then-commit | stream-and-commit) + hash verify + tag
    /// promote. Body-phase errors classified as
    /// [`OriginPullError::Transient`] re-enter the retry loop; the
    /// non-[`OriginPullError`] outcomes (`Store` / `HashMismatch` /
    /// `BlobTooLarge`) ride out through [`PullThroughOutcome`] because
    /// they are deterministic and retry would not help.
    ///
    /// Why this method instead of inlining into `pull_through`: the
    /// retry loop ([`run_with_retry`]) needs a callable that produces
    /// a fresh attempt on each invocation — the side-channel `Arc`s,
    /// `TempTag`s, and origin futures all have to be re-created per
    /// attempt and can't be reused across iterations.
    #[allow(clippy::too_many_lines)] // Linear per-attempt flow; the failure-classification arms each need their own context comment, and splitting them across functions would obscure the sequence more than the length.
    async fn pull_through_attempt(
        &self,
        origin: Arc<dyn Origin>,
        hash: Hash,
        max_blob_bytes: u64,
        policy: RetryPolicy,
    ) -> Result<PullThroughOutcome, OriginPullError> {
        // The origin handle is now plumbed in by the caller
        // (`pull_through`'s fallback-chain loop, #284) so this method
        // is agnostic to chain position and works identically for the
        // singular-origin (chain length 1) and multi-origin paths.
        let fetch = origin.fetch(hash, max_blob_bytes).await?;
        let (stream, size_hint) = match fetch {
            crate::origin::OriginFetch::NotFound => return Ok(PullThroughOutcome::NotFound),
            crate::origin::OriginFetch::Found { stream, size_hint } => (stream, size_hint),
        };

        // Pre-stream cap: if the adapter advertised a length, reject
        // before reading the first byte. The post-stream cap below is
        // load-bearing too — origins can lie or omit the hint. This
        // is a deterministic operator-visible cap breach; surface as
        // `BlobTooLarge` directly without burning retry budget.
        if let Some(advertised) = size_hint
            && advertised > max_blob_bytes
        {
            return Ok(PullThroughOutcome::BlobTooLarge);
        }

        if should_buffer(size_hint, policy.buffered_max_bytes) {
            let drain_cap = policy.buffered_max_bytes.min(max_blob_bytes);
            let bytes = match drain_to_bytes(stream, drain_cap, self.inner.metrics.as_ref()).await {
                Ok(b) => b,
                // Cap-breach: surface as `BlobTooLarge` directly (typed
                // outcome). Routing through `classify_io_error` would
                // collapse it to a generic `OriginError` and operators
                // alerting on the typed `BlobTooLarge` metric would
                // lose visibility.
                Err(e) if is_blob_too_large_marker(&e) => {
                    return Ok(PullThroughOutcome::BlobTooLarge);
                }
                Err(e) => return Err(classify_io_error(e)),
            };
            return self.commit_buffered_bytes(hash, bytes).await;
        }

        // Streaming path: hand the stream to `iroh-blobs::add_stream`
        // and capture mid-stream errors via the side channel.
        // `iroh-blobs::add_stream` swallows the upstream `io::Error`
        // (it `?`-propagates inside an async block whose error is
        // discarded), so without this capture the engine sees only
        // "unexpected end of stream" and operators lose the
        // actionable upstream message. Failed attempts strand a
        // partial `TempTag` worth of bytes; iroh-blobs GC reclaims
        // them at `cache.gc_interval_sec` cadence.
        let captured_err: Arc<Mutex<Option<std::io::Error>>> = Arc::new(Mutex::new(None));
        let counted = count_and_cap_stream(
            stream,
            max_blob_bytes,
            self.inner.metrics.clone(),
            captured_err.clone(),
        );
        let progress = self.inner.store.blobs().add_stream(counted).await;
        let temp_tag_result = progress.temp_tag().await;

        // Side-channel-recorded error wins over both the iroh-blobs
        // Err arm AND a "successful" partial import, because the
        // latter's hash is deterministically wrong and we'd rather
        // surface the real cause ("body read stalled", "decompression
        // failed") than a confusing `HashMismatch`. Drop the temp tag
        // (regardless of inner Ok/Err) so iroh-blobs GC reclaims the
        // partial bytes.
        let captured = captured_err
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(upstream) = captured {
            drop(temp_tag_result);
            // Cap-breach mid-stream: typed `BlobTooLargeMarker` is the
            // documented escape hatch — surface as the typed
            // `BlobTooLarge` outcome rather than routing through
            // `classify_io_error` which would collapse it to a
            // generic `OriginError`.
            if is_blob_too_large_marker(&upstream) {
                return Ok(PullThroughOutcome::BlobTooLarge);
            }
            // Otherwise classify via the shared body-phase
            // classifier: typed `OriginError::*` inners surface as
            // Permanent (decompression failures, etc.);
            // `io::ErrorKind`-Transient kinds (ConnectionReset,
            // TimedOut, …) surface as Transient and re-enter the
            // retry loop for abort+restart.
            return Err(classify_io_error(upstream));
        }

        let temp_tag = match temp_tag_result {
            Ok(tt) => tt,
            Err(err) => {
                // No upstream-captured error: the failure is on
                // iroh-blobs' side (disk write, actor crash,
                // serialization-task panic, etc.). Surface as
                // `Store` so operators routing on origin-vs-store
                // don't misclassify a local store problem as a
                // remote origin one. Not retry-class.
                return Ok(PullThroughOutcome::Store(
                    anyhow::Error::from(err)
                        .context("iroh-blobs add_stream failed during pull-through"),
                ));
            }
        };

        let actual = temp_tag.hash();
        if actual != hash {
            // Drop the temp tag without promotion → bytes become
            // GC-eligible inside iroh-blobs (they're not protected
            // once the `TempTag` drops). Deterministic protocol
            // violation: a clean stream that hashed wrong is not a
            // transport failure — retry won't help.
            drop(temp_tag);
            // Cache-poisoning mitigation: between the drop above
            // and the next iroh-blobs GC sweep, the wrong-hash
            // bytes are still resident in the store and
            // `Blobs::has(actual)` would return `true`. An
            // adversary who chose the bytes also chose `actual`,
            // so a follow-up request for `actual` could otherwise
            // serve content the operator never authorized. Add
            // `actual` to the engine's logical-evicted set so
            // `engine::has(actual)` and `engine::get(actual)`
            // return absent regardless of what iroh-blobs
            // currently has on disk. Best-effort: a poisoned
            // mutex or a full evicted-set cap surfaces only as a
            // log line — the primary error returned to the
            // caller is still `HashMismatch`.
            if let Err(evict_err) = self.evict(actual) {
                tracing::warn!(
                    expected = %hash,
                    %actual,
                    err = %evict_err,
                    "hash-mismatch logical-evict failed; engine.has(actual) may surface partial-import bytes until iroh-blobs GC runs",
                );
            }
            return Ok(PullThroughOutcome::HashMismatch { actual });
        }

        // Promote the temp tag to a named tag — same effect as
        // `add_bytes(...).await`, which goes through `with_tag()` (
        // iroh-blobs `blobs.rs:624-632`). The name is opaque; the
        // store auto-assigns it. Tag-create failure is store-side, not
        // origin-side: surface as `Store` so retry-class taxonomy
        // doesn't pick it up.
        let haf = temp_tag.hash_and_format();
        if let Err(err) = self.inner.store.tags().create(haf).await {
            return Ok(PullThroughOutcome::Store(anyhow::Error::from(err)));
        }
        drop(temp_tag);

        // The engine's existing `get()` callers (admin RPC, metrics
        // tests) want the full payload as `Bytes`. Re-read it from
        // the local store: with iroh-blobs' `fs-store` this is one
        // mmap'd read with no extra origin egress. Stream-shaped
        // `get()` is in scope for #317 (cdn/client/v1 paid delivery),
        // not this issue.
        match self.read_local(hash).await {
            Ok(bytes) => Ok(PullThroughOutcome::Bytes(bytes)),
            Err(CacheError::Store(err)) => Ok(PullThroughOutcome::Store(err)),
            Err(other) => {
                // `read_local` only surfaces `Store`; any other
                // variant is a logic regression. Map to `Store` so
                // the outer `pull_through` still surfaces a coherent
                // error; the inner anyhow chain preserves the cause.
                Ok(PullThroughOutcome::Store(anyhow::Error::msg(format!(
                    "read_local returned unexpected variant after successful commit: {other}"
                ))))
            }
        }
    }

    /// Commit a fully-buffered payload from the drain path. Drains do
    /// not go through `count_and_cap_stream` (their cap was already
    /// enforced inline), so this just hands the buffered `Bytes` to
    /// iroh-blobs and verifies the resulting hash. Splitting the
    /// commit out of `pull_through_attempt` keeps the two body-phase
    /// paths (drain vs. streaming) symmetric: each is followed by the
    /// same commit-and-verify sequence.
    async fn commit_buffered_bytes(
        &self,
        hash: Hash,
        bytes: Bytes,
    ) -> Result<PullThroughOutcome, OriginPullError> {
        // iroh-blobs `add_bytes` returns `Ok(NamedTag)` directly,
        // skipping the `TempTag` intermediary used by `add_stream`.
        // The named tag protects the blob from GC and is the same
        // shape the streaming path produces after `tags().create()`.
        let tag = match self.inner.store.blobs().add_bytes(bytes.clone()).await {
            Ok(t) => t,
            Err(err) => {
                return Ok(PullThroughOutcome::Store(
                    anyhow::Error::from(err)
                        .context("iroh-blobs add_bytes failed during pull-through"),
                ));
            }
        };
        let actual = tag.hash;
        if actual != hash {
            // Same cache-poisoning mitigation as the streaming path:
            // log-evict the wrong hash so `engine.has(actual)` doesn't
            // surface attacker-chosen bytes between now and the next
            // GC sweep.
            if let Err(evict_err) = self.evict(actual) {
                tracing::warn!(
                    expected = %hash,
                    %actual,
                    err = %evict_err,
                    "hash-mismatch logical-evict failed (drain path); engine.has(actual) may surface partial-import bytes until iroh-blobs GC runs",
                );
            }
            return Ok(PullThroughOutcome::HashMismatch { actual });
        }
        Ok(PullThroughOutcome::Bytes(bytes))
    }
}

/// Per-attempt outcomes that ride out of the retry loop without
/// classification: each is a deterministic, non-retry-class result
/// the engine maps directly to a `CacheError` variant.
#[derive(Debug)]
enum PullThroughOutcome {
    Bytes(Bytes),
    NotFound,
    BlobTooLarge,
    HashMismatch { actual: Hash },
    Store(anyhow::Error),
}

/// True when the body-phase `io::Error` wraps a typed
/// [`BlobTooLargeMarker`] — meaning the cap (engine-level
/// `count_and_cap_stream`, adapter-level HTTP chunk cap, or the
/// drain-path running total) tripped. Lets the engine surface
/// `CacheError::BlobTooLarge` directly instead of routing through
/// [`classify_io_error`] which would collapse the typed marker into a
/// generic `OriginError`.
fn is_blob_too_large_marker(e: &std::io::Error) -> bool {
    e.get_ref()
        .is_some_and(<dyn std::error::Error + Send + Sync>::is::<BlobTooLargeMarker>)
}

pub(crate) use crate::origin::BlobTooLargeMarker;

/// Adapter that bumps the `pull_through_bytes` metric per chunk,
/// captures any upstream error into `captured_err`, and *terminates
/// the stream cleanly* (yields `None`, never `Err`) on cap breach
/// or upstream error.
///
/// The engine pipes the returned stream directly into
/// [`iroh_blobs::api::blobs::Blobs::add_stream`], so the I/O bound
/// becomes the chunk size from the origin (typically a few KiB to a
/// few MiB depending on the backend) — a 10 GB blob no longer pins
/// 10 GB of process RSS (issue #271).
///
/// **Why `None`-on-error rather than `Err`:** iroh-blobs'
/// `add_stream` send loop propagates a yielded `Err` via `?`,
/// dropping the bidi-channel sender. Its server-side companion
/// actor then waits forever for `Done` before yielding any
/// progress item, hanging the whole import. Yielding `None` lets
/// `add_stream` send the `Done` marker so the import commits
/// (under whatever hash the partial bytes produce); the engine
/// reads `captured_err` to surface the *real* failure instead of
/// the misleading `HashMismatch` the partial-import would
/// otherwise produce. The partial blob is left as an unprotected
/// `TempTag` and reclaimed by iroh-blobs' GC.
///
/// **Wrapping the inner stream in `Option`** ensures polling
/// returns `None` once we've terminated. Polling a stream after
/// a terminal error is undefined; without the sentinel, a
/// consumer that resumes polling could re-enter the adapter and
/// re-poll an already-errored upstream.
///
/// The `Send + Sync + 'static` bound on the returned stream is fixed
/// by `add_stream`'s signature.
fn count_and_cap_stream<S>(
    stream: S,
    max_bytes: u64,
    metrics: Option<Arc<CacheMetrics>>,
    captured_err: Arc<Mutex<Option<std::io::Error>>>,
) -> impl futures_util::Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static
where
    S: futures_util::Stream<Item = std::io::Result<Bytes>> + Send + Sync + Unpin + 'static,
{
    use futures_util::StreamExt;
    futures_util::stream::unfold(
        (Some(stream), 0u64, metrics, captured_err),
        move |(maybe_s, total, metrics, captured)| async move {
            let mut s = maybe_s?;
            let next = s.next().await?;
            match next {
                Err(e) => {
                    // Move the real error into the side channel
                    // (preserves any typed inner like
                    // `io::Error::other(OriginError::*)` that
                    // `HttpOrigin` packs in) and *terminate the
                    // stream cleanly* by returning `None` on the
                    // next poll. We deliberately do **not** yield
                    // the error to iroh-blobs' `add_stream`: when
                    // we yield `Err` from the source stream,
                    // `add_stream`'s send loop returns early via
                    // `?`, dropping the bidi-channel sender — but
                    // its companion server-side actor then waits
                    // forever for `Done` before yielding any
                    // progress item, hanging the whole import.
                    // Yielding `None` instead lets `add_stream`
                    // send the `Done` marker, the import commits
                    // with whatever bytes were already received
                    // (under a wrong hash), and the engine reads
                    // `captured_err` to surface the *real* failure
                    // instead of the misleading `HashMismatch` the
                    // partial-import would otherwise produce. The
                    // partial blob is left as an unprotected
                    // `TempTag` and reclaimed by iroh-blobs' GC.
                    *captured.lock().unwrap_or_else(PoisonError::into_inner) = Some(e);
                    None
                }
                Ok(chunk) => {
                    // Bill the chunk before the cap check: the
                    // origin already sent us the bytes, so the
                    // operator-visible egress meter must count them
                    // (matches the pre-streaming "every byte fetched
                    // is paid" intent at engine.rs:952-960). A
                    // chunk that lands the running total past
                    // `max_bytes` is still counted in
                    // `pull_through_bytes` even though we then
                    // abort the import.
                    if let Some(m) = metrics.as_ref() {
                        m.pull_through_bytes
                            .inc_by(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
                    }
                    let new_total = total.saturating_add(chunk.len() as u64);
                    if new_total > max_bytes {
                        // Pack a typed `BlobTooLargeMarker` into the
                        // io::Error so the engine surfaces
                        // `CacheError::BlobTooLarge` (recovered via
                        // `io::Error::get_ref` downcast).
                        let err = std::io::Error::other(BlobTooLargeMarker { max_bytes });
                        *captured.lock().unwrap_or_else(PoisonError::into_inner) = Some(err);
                        return None;
                    }
                    Some((Ok(chunk), (Some(s), new_total, metrics, captured)))
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::origin::{Origin, OriginFetch, OriginKind};

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
        fn kind(&self) -> OriginKind {
            // Stand in for an HTTP origin in tests so callers reasoning
            // about preview-side `origin_kinds` behaviour see a
            // non-empty `Vec<OriginKind>` entry (post-#284 the field is
            // a vec, not an `Option`). The choice is arbitrary —
            // `Origin::kind` is a tag, not a behavioural switch.
            OriginKind::Http
        }

        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
        {
            let result = if hash == self.hash {
                Ok(OriginFetch::found_one_shot(self.data.clone()))
            } else {
                Ok(OriginFetch::NotFound)
            };
            Box::pin(async move { result })
        }
    }

    /// Serves several blobs from one origin so a single engine can hold
    /// multiple distinct hashes (a second `CacheEngine::open` on the same
    /// dir would deadlock on iroh-blobs' single-writer file lock).
    #[derive(Debug)]
    struct MultiStubOrigin {
        blobs: std::collections::HashMap<Hash, Bytes>,
    }

    impl MultiStubOrigin {
        fn new(payloads: &[&[u8]]) -> Self {
            let blobs = payloads
                .iter()
                .map(|p| (Hash::new(p), Bytes::from(p.to_vec())))
                .collect();
            Self { blobs }
        }
    }

    impl Origin for MultiStubOrigin {
        fn kind(&self) -> OriginKind {
            OriginKind::Http
        }

        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
        {
            let result = self
                .blobs
                .get(&hash)
                .map_or(Ok(OriginFetch::NotFound), |b| {
                    Ok(OriginFetch::found_one_shot(b.clone()))
                });
            Box::pin(async move { result })
        }
    }

    #[tokio::test]
    async fn get_cache_hit_records_access_time() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello cache hit";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

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

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

        // First get triggers pull-through.
        let _ = engine.get(hash).await?;

        anyhow::ensure!(
            engine.last_accessed(hash).is_some(),
            "expected Some(Instant) after pull-through get"
        );
        Ok(())
    }

    #[tokio::test]
    async fn subscribe_inserts_emits_on_pull_through_success() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello dht hook";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
        let mut rx = engine.subscribe_inserts();

        // Successful pull-through must announce the hash to the
        // subscriber. The DHT republish scheduler (PR 4 of #320)
        // consumes this stream to drive `Store` fan-out to the K+3
        // closest peers.
        let _ = engine.get(hash).await?;
        let announced = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for subscribe_inserts emit"))?
            .map_err(|e| anyhow::anyhow!("recv: {e}"))?;
        anyhow::ensure!(
            announced == hash,
            "expected announced hash to equal committed hash"
        );
        Ok(())
    }

    #[tokio::test]
    async fn subscribe_inserts_does_not_emit_on_cache_hit() -> anyhow::Result<()> {
        // A `get` that hits the local store (no pull-through, no new
        // commit) must NOT emit on the channel — only fresh commits
        // do, because the consumer's job is to schedule a NEW publish
        // cycle for newly-cached blobs.
        let tmp = tempfile::tempdir()?;
        let payload = b"hello cached-hit";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

        // First get: pull-through, should emit. Drain that emission so
        // the channel is empty before the second get.
        let mut rx = engine.subscribe_inserts();
        let _ = engine.get(hash).await?;
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("first pull-through emission missing"))?
            .map_err(|e| anyhow::anyhow!("recv first: {e}"))?;

        // Second get: cache hit. No emission expected.
        let _ = engine.get(hash).await?;
        let r = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await;
        anyhow::ensure!(
            r.is_err(),
            "cache hit must not emit on subscribe_inserts; got {r:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn second_get_updates_access_time() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello update";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

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

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

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
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
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
        fn kind(&self) -> OriginKind {
            OriginKind::Http
        }

        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
        {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);
            let result = if hash == self.hash {
                Ok(OriginFetch::found_one_shot(self.data.clone()))
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

        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![origin.clone() as Arc<dyn Origin>],
            10,
            crate::PinnedHashes::empty(),
            crate::RetryPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

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

        // Counter accounting under coalescing (#418): exactly one task
        // becomes the owner and counts as a miss; the other four are
        // waiter-retry hits. A regression that moved the waiter-hit
        // bump out of `engine.rs::get`'s waiter branch would silently
        // mis-classify every coalesced workload as a miss-storm.
        anyhow::ensure!(
            cm.misses.get() == 1,
            "owner pulls once → exactly 1 miss, got {}",
            cm.misses.get()
        );
        anyhow::ensure!(
            cm.hits.get() == 4,
            "4 waiters retry into a hit, got {} hits",
            cm.hits.get()
        );
        anyhow::ensure!(
            cm.pull_through_bytes.get() == payload.len() as u64,
            "single origin fetch → pull_through_bytes == payload.len()"
        );
        anyhow::ensure!(
            cm.bytes_returned.get() == (payload.len() as u64) * 5,
            "all 5 callers got the bytes back, so bytes_returned == 5 * payload.len()"
        );
        Ok(())
    }

    #[tokio::test]
    async fn inflight_map_is_empty_after_pull_completes() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"cleanup check";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

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

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

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

        // Build the engine with pinned_hash in the pinning set. The
        // leaf `PinnedHashes` is keyed on the config-vocabulary hash, so
        // convert the store hash at the boundary (#578) — this also
        // regression-covers the leaf↔store conversion in `open_full`.
        let pinned_set = [from_store_hash(pinned_hash)].into_iter().collect();
        let engine = CacheEngine::open_with_pinned(
            tmp.path(),
            Vec::new(),
            10,
            PinnedHashes::new(pinned_set),
        )
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

        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
        if let Ok(mut g) = engine.inner.access_times.lock() {
            g.insert(h1, Instant::now());
            g.insert(h2, Instant::now());
        }

        // No pinning yet — both candidates.
        anyhow::ensure!(engine.eviction_candidates().len() == 2);

        // Pin h1 (convert the store hash to the leaf config-vocabulary
        // hash at the boundary, #578).
        let s = [from_store_hash(h1)].into_iter().collect();
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

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

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
            let engine = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 10).await?;
            let _ = engine.get(hash).await?;
            engine.evict(hash)?;
            engine.shutdown().await?;
        }

        // Second open: same cache_dir, no origin so a re-pull would fail
        // loudly. The evicted set must reload from disk.
        let engine2 = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
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

        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
        anyhow::ensure!(engine.is_evicted(good), "valid hash line not loaded");
        Ok(())
    }

    #[tokio::test]
    async fn evict_unknown_hash_is_a_no_op() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
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
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
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

        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
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
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
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
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
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

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
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
    /// `size_bytes` (until iroh-blobs' periodic GC sweep reclaims, #518)
    /// but flip `already_evicted` to `true` and `served` to `false`.
    /// The size-still-reported part
    /// is the load-bearing assertion: dry-run callers want to see
    /// disk-reclaim potential, not a clean `None` that hides the bytes.
    #[tokio::test]
    async fn inspect_after_evict_keeps_size_but_flips_served() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"evicted blob";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
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
        // Convert the store hash to the leaf config-vocabulary hash at
        // the `PinnedHashes` boundary (#578).
        let set = [from_store_hash(pinned_hash)].into_iter().collect();
        let engine =
            CacheEngine::open_with_pinned(tmp.path(), Vec::new(), 10, PinnedHashes::new(set))
                .await?;

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

    /// `inspect` (#439) reports the configured origin's backend kind so
    /// admin dry-run callers can estimate origin egress cost before
    /// committing to an eviction. Engine constructed with no origin
    /// reports an empty vec.
    #[tokio::test]
    async fn inspect_reports_origin_kinds_when_origin_configured() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = StubOrigin::new(b"egress-cost preview");
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
        let preview = engine.inspect(Hash::new(b"never-fetched")).await?;
        anyhow::ensure!(
            preview.origin_kinds == vec![OriginKind::Http],
            "expected [Http], got {:?}",
            preview.origin_kinds,
        );
        Ok(())
    }

    #[tokio::test]
    async fn inspect_reports_no_origin_kinds_when_cache_only() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
        let preview = engine.inspect(Hash::new(b"absent")).await?;
        anyhow::ensure!(
            preview.origin_kinds.is_empty(),
            "expected empty Vec for cache-only mode, got {:?}",
            preview.origin_kinds,
        );
        Ok(())
    }

    // -------------------------------------------------------------------
    // Cache hit/miss + bytes counters (#418)
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn hits_plus_misses_equals_total_gets() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello invariant";
        let hash = Hash::new(payload);
        let unknown = Hash::new(b"never present");
        let origin = StubOrigin::new(payload);
        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![Arc::new(origin) as Arc<dyn Origin>],
            10,
            crate::PinnedHashes::empty(),
            crate::RetryPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        // 1 miss (pull-through), 2 hits, 1 miss (origin NotFound), 1 miss (evicted).
        let _ = engine.get(hash).await?; // miss
        let _ = engine.get(hash).await?; // hit
        let _ = engine.get(hash).await?; // hit
        let _ = engine.get(unknown).await; // miss (origin NotFound)
        engine.evict(hash)?;
        let _ = engine.get(hash).await; // miss (evicted)

        anyhow::ensure!(cm.hits.get() == 2, "hits = {}", cm.hits.get());
        anyhow::ensure!(cm.misses.get() == 3, "misses = {}", cm.misses.get());
        anyhow::ensure!(
            cm.hits.get() + cm.misses.get() == 5,
            "across cache-domain outcomes (no store-I/O errors), every get bumps exactly one of hits/misses"
        );
        // Only the priming Found bumped pull_through_bytes; the unknown
        // get took the origin-NotFound branch which returns before the
        // bump. Pin both, so a stray bump in either error path fails
        // the test.
        anyhow::ensure!(
            cm.pull_through_bytes.get() == payload.len() as u64,
            "pull_through_bytes = {}, expected {}",
            cm.pull_through_bytes.get(),
            payload.len()
        );
        Ok(())
    }

    #[tokio::test]
    async fn pull_through_success_bumps_bytes_returned() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello pull-through bytes returned";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);
        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![Arc::new(origin) as Arc<dyn Origin>],
            10,
            crate::PinnedHashes::empty(),
            crate::RetryPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        let bytes = engine.get(hash).await?;
        anyhow::ensure!(
            cm.bytes_returned.get() == bytes.len() as u64,
            "bytes_returned should match payload length after a successful pull-through"
        );
        Ok(())
    }

    #[tokio::test]
    async fn pull_through_bumps_pull_through_bytes() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello pull-through bytes";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);
        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![Arc::new(origin) as Arc<dyn Origin>],
            10,
            crate::PinnedHashes::empty(),
            crate::RetryPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        let bytes = engine.get(hash).await?;
        anyhow::ensure!(cm.misses.get() == 1, "first get is a miss");
        anyhow::ensure!(cm.hits.get() == 0, "no hits on first get");
        anyhow::ensure!(
            cm.pull_through_bytes.get() == bytes.len() as u64,
            "pull_through_bytes should equal payload length on a Found origin"
        );
        Ok(())
    }

    #[tokio::test]
    async fn no_origin_increments_misses_only() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            Vec::new(),
            10,
            crate::PinnedHashes::empty(),
            crate::RetryPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        let hash = Hash::new(b"missing payload");
        let Err(err) = engine.get(hash).await else {
            anyhow::bail!("expected NoOrigin, got Ok");
        };
        anyhow::ensure!(
            matches!(err, CacheError::NoOrigin { .. }),
            "expected NoOrigin"
        );
        anyhow::ensure!(cm.misses.get() == 1, "exactly one miss for a NoOrigin get");
        anyhow::ensure!(cm.hits.get() == 0, "no hits");
        anyhow::ensure!(
            cm.pull_through_bytes.get() == 0,
            "no origin bytes since origin not configured"
        );
        anyhow::ensure!(
            cm.bytes_returned.get() == 0,
            "no bytes returned on error path"
        );
        Ok(())
    }

    #[tokio::test]
    async fn evicted_hash_increments_misses() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello evicted miss";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);
        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![Arc::new(origin) as Arc<dyn Origin>],
            10,
            crate::PinnedHashes::empty(),
            crate::RetryPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        // Prime then evict so the next get hits the evicted branch in get().
        let _ = engine.get(hash).await?;
        engine.evict(hash)?;
        let misses_before = cm.misses.get();

        let Err(err) = engine.get(hash).await else {
            anyhow::bail!("expected NotFound, got Ok");
        };
        anyhow::ensure!(
            matches!(err, CacheError::NotFound { .. }),
            "evicted get must surface NotFound"
        );
        anyhow::ensure!(
            cm.misses.get() == misses_before + 1,
            "misses should bump by exactly 1 on an evicted-hash get"
        );
        Ok(())
    }

    // ---- Probe-triggered eviction hold (#318, ADR 005) ----

    #[tokio::test]
    async fn try_probe_hold_unavailable_when_blob_absent() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), vec![], 10).await?;
        let absent = Hash::new(b"never fetched");
        anyhow::ensure!(
            engine.try_probe_hold(absent).await? == ProbeHoldOutcome::Unavailable,
            "absent blob must not be holdable (would risk a phantom slash)"
        );
        Ok(())
    }

    #[tokio::test]
    async fn try_probe_hold_true_when_cached_and_excluded_from_eviction() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"holdable blob";
        let hash = Hash::new(payload);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;
        let _ = engine.get(hash).await?;

        anyhow::ensure!(
            engine.try_probe_hold(hash).await? == ProbeHoldOutcome::Held,
            "cached blob should hold"
        );
        anyhow::ensure!(engine.probe_hold_slots_used() == 1, "one slot used");
        anyhow::ensure!(
            !engine
                .eviction_candidates()
                .into_inner()
                .contains_key(&hash),
            "held hash must be invisible to the LRU driver"
        );
        Ok(())
    }

    #[tokio::test]
    async fn probe_hold_is_shared_per_blob_not_per_probe() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"popular blob";
        let hash = Hash::new(payload);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;
        let _ = engine.get(hash).await?;
        engine.set_max_probe_holds(1);

        // Many "peers" probing the same hash share one slot.
        for _ in 0..3 {
            anyhow::ensure!(engine.try_probe_hold(hash).await? == ProbeHoldOutcome::Held);
        }
        anyhow::ensure!(
            engine.probe_hold_slots_used() == 1,
            "shared per-blob slot must not grow with probe volume"
        );
        Ok(())
    }

    #[tokio::test]
    async fn probe_hold_budget_exhaustion_reports_exhausted() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let a: &[u8] = b"blob a";
        let b: &[u8] = b"blob bee";
        let (ha, hb) = (Hash::new(a), Hash::new(b));
        // One engine, one multi-blob origin: opening a second engine on the
        // same dir would deadlock on iroh-blobs' single-writer lock.
        let origin = Arc::new(MultiStubOrigin::new(&[a, b]));
        let engine = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 10).await?;
        let _ = engine.get(ha).await?;
        let _ = engine.get(hb).await?;
        engine.set_max_probe_holds(1);

        anyhow::ensure!(
            engine.try_probe_hold(ha).await? == ProbeHoldOutcome::Held,
            "first hold fits budget"
        );
        anyhow::ensure!(
            engine.try_probe_hold(hb).await? == ProbeHoldOutcome::BudgetExhausted,
            "second distinct hold must be refused when budget is exhausted"
        );
        Ok(())
    }

    #[tokio::test]
    async fn probe_hold_disabled_when_budget_zero() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"unhold me";
        let hash = Hash::new(payload);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;
        let _ = engine.get(hash).await?;
        engine.set_max_probe_holds(0);
        anyhow::ensure!(
            engine.try_probe_hold(hash).await? == ProbeHoldOutcome::BudgetExhausted,
            "max_probe_holds=0 must disable has_blob:true entirely"
        );
        Ok(())
    }

    #[tokio::test]
    async fn operator_evict_overrides_probe_hold() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"dmca target";
        let hash = Hash::new(payload);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;
        let _ = engine.get(hash).await?;
        anyhow::ensure!(
            engine.try_probe_hold(hash).await? == ProbeHoldOutcome::Held,
            "held before evict"
        );

        engine.evict(hash)?;
        anyhow::ensure!(
            engine.try_probe_hold(hash).await? == ProbeHoldOutcome::Unavailable,
            "operator evict (DMCA) must win over a probe hold"
        );
        Ok(())
    }

    #[tokio::test]
    async fn expired_probe_hold_is_swept_and_re_evictable() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"expiring blob";
        let hash = Hash::new(payload);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;
        let _ = engine.get(hash).await?;
        engine.touch(hash); // make it an LRU candidate

        // Inject an already-expired hold directly (the real 35s duration is
        // impractical to sleep, and std `Instant` ignores tokio time pause).
        {
            let mut g = engine
                .inner
                .probe_holds
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let past = Instant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(Instant::now);
            g.insert(hash, past);
        }

        anyhow::ensure!(
            engine.probe_hold_slots_used() == 0,
            "expired hold must be swept from the slot count"
        );
        anyhow::ensure!(
            engine
                .eviction_candidates()
                .into_inner()
                .contains_key(&hash),
            "an expired hold must no longer shield the hash from LRU"
        );
        Ok(())
    }

    #[tokio::test]
    async fn hit_increments_hits_and_bytes_returned() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello hit metrics";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);
        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![Arc::new(origin) as Arc<dyn Origin>],
            10,
            crate::PinnedHashes::empty(),
            crate::RetryPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        // First get is a pull-through (miss); prime the cache.
        let _ = engine.get(hash).await?;
        let hits_before = cm.hits.get();
        let bytes_before = cm.bytes_returned.get();

        // Second get must be a local hit.
        let bytes = engine.get(hash).await?;
        anyhow::ensure!(
            cm.hits.get() == hits_before + 1,
            "hits should increment by 1 on a cache hit"
        );
        anyhow::ensure!(
            cm.bytes_returned.get() == bytes_before + bytes.len() as u64,
            "bytes_returned should increase by bytes.len() on a cache hit"
        );
        Ok(())
    }
}
