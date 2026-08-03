//! Cache engine: local iroh-blobs store fronted by an [`Origin`] for misses.

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bao_tree::io::BaoContentItem;
use bao_tree::io::fsm::{ResponseDecoder, ResponseDecoderNext};
use bao_tree::{BaoTree, ChunkRanges};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use iroh_blobs::Hash;
use iroh_blobs::api::blobs::EncodedItem;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::fs::options::Options as FsStoreOptions;
use iroh_blobs::store::{GcConfig, ProtectOutcome};
use iroh_blobs::util::{RecvStream, RecvStreamAsyncStreamReader};
use tokio::sync::{Notify, broadcast};

use decdn_config_types::{CircuitBreakerPolicy, DeniedHashes, PinDiff, PinnedHashes, RetryPolicy};

use crate::circuit_breaker::{
    Admission, Clock, OriginBreaker, OriginOutcome, SystemClock, TrialGuard,
};
use crate::error::{CacheError, CacheResult, OriginPullError};
use crate::local_outboard_pull::{LocalOutboardHeader, LocalOutboardPull};
use crate::metrics::CacheMetrics;
use crate::origin::{
    Origin, OriginFetch, OriginKind, OriginRangeFetch, OriginRangeRequest, OutboardFetch,
};
use crate::origin_probe::{OriginProbeMemo, Presence};
use crate::probe_hold::ProbeHoldOutcome;
use crate::range_pull::{AlignedRange, align_range, encode_verified_range};
use crate::retry::{
    TerminalFailure, classify_io_error, drain_to_bytes, run_with_retry_classified, should_buffer,
};
use crate::{from_store_hash, to_store_hash};

/// Engine bundling a filesystem-backed iroh-blobs store with an optional
/// origin backend. Lookups hit the store first; on miss and when an origin is
/// configured, bytes are pulled and BLAKE3-verified. Insert-before-return is a
/// property of the buffered path ([`Self::get`] / [`Self::populate`]), not of
/// this type: [`Self::open_local_outboard_pull`] streams and tees concurrently,
/// and [`Self::pull_through_range`] commits only a verified sub-range.
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
    /// Per-origin circuit-breakers (#963), parallel to `origins` by
    /// index. `breakers[i]` fronts `origins[i]`'s pull-through retry
    /// loop so a sustained outage on one backend fast-fails its misses
    /// without burning retry/backoff, while the chain still advances to
    /// the next backend. Always the same length as `origins` (built
    /// together in `open_full`); an empty `origins` yields an empty
    /// `breakers` and the `NoOrigin` short-circuit never reaches them.
    breakers: Vec<OriginBreaker>,
    max_blob_bytes: u64,
    /// Per-hash last-access timestamps for LRU eviction ordering.
    access_times: Mutex<HashMap<Hash, Instant>>,
    /// In-flight pull-through requests. When a pull is in progress for a hash,
    /// subsequent callers wait on the [`Notify`] rather than issuing a
    /// duplicate origin fetch (coalescing, fixes #305).
    ///
    /// Production code locks it only through [`Inner::lock_inflight`] —
    /// never `.lock()` directly. See that method for why poison must be
    /// recovered here rather than skipped. (Tests below reach for `.lock()`
    /// deliberately, to poison it and to read its length through
    /// `PoisonError::into_inner`.)
    inflight: Mutex<HashMap<Hash, Arc<Notify>>>,
    /// One-shot latch for the [`Inner::lock_inflight`] poison log (#1517).
    /// The counter carries the true count of poisonings; this bounds the
    /// log to one line even under a pathological panic loop.
    inflight_poison_logged: AtomicBool,
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
    /// Origin-held discovery index (#1130): hashes this node can serve from a
    /// configured origin *before* any pull-through, each mapped to its total
    /// byte size. Rebuilt wholesale by [`CacheEngine::rescan_origins`] (fs
    /// directory enumeration ∪ present operator pins) and swapped atomically,
    /// mirroring `pinned`. Probe and DHT-announce read it so cold origin
    /// content is discoverable on the first request rather than only after a
    /// warm pulls it into the store. Never includes refused/denied hashes.
    origin_held: ArcSwap<HashMap<Hash, u64>>,
    /// Live-origin probe memo (#1130 pt3). The `origin_held` index only covers
    /// fs enumeration ∪ pins — http/s3 do not list, so a non-pinned bucket
    /// object is absent from it. [`CacheEngine::origin_probe_size`] falls back to
    /// a live `HEAD`/`HeadObject` for such hashes and memoises the answer here
    /// (positive AND negative) under `cache.origin_probe_ttl_sec`, so a probe
    /// flood costs at most one origin round-trip per hash per TTL window rather
    /// than one per probe. Restart-configured like `max_probe_holds`; swapped in
    /// wholesale by [`CacheEngine::set_origin_probe_config`] at bring-up.
    origin_probe_memo: Mutex<OriginProbeMemo>,
    /// Hashes this node refuses to serve, announce, or acquire because the
    /// operator's own `[content] denied_hashes` names them, and which a later
    /// config reload can UN-refuse (ADR 011 § Local Denylist). The governance
    /// half lives in [`Self::chain_denied`].
    ///
    /// Distinct from [`Self::evicted`] on exactly one axis: reversibility.
    /// Eviction is a durable, sticky operator act recorded in `evicted.log`;
    /// this set is a live policy view swapped wholesale from the current
    /// denylist. A wrongful takedown must be reversible without editing a log
    /// file and restarting, and ADR 011 § One-hour removal orders puts this
    /// mechanism on a statutory clock in both directions.
    ///
    /// It lives HERE rather than beside the origin deny-set in `decdn-node`
    /// because "will this node serve/announce/acquire `hash`" has five
    /// consumers — the serve path, `try_probe_hold`, the DHT republisher,
    /// `populate`, and the per-MB in-flight re-check that terminates a stream
    /// already running when a takedown lands — and the ADR requires one answer
    /// for all of them. Keeping it next to `evicted` is what lets
    /// [`CacheEngine::refuses`] be the single predicate they all call, so the
    /// next consumer inherits the check instead of forgetting it. The in-flight
    /// one is exactly that: it was added later and needed no wiring of its own.
    denied: ArcSwap<HashSet<Hash>>,
    /// Hashes refused because *governance* blacklisted them — the blacklist
    /// watcher's live projection of `ContentBlacklist`, kept in its own slot
    /// beside [`Self::denied`] for the same reason the node keeps local and
    /// on-chain origins apart: the two have independent lifecycles, and a config
    /// reload calling [`CacheEngine::set_denied`] must not clobber what the
    /// chain watcher learned (nor the reverse).
    ///
    /// Membership is not what stops the serving — the watcher also
    /// [`CacheEngine::evict`]s, which is sticky and durable. What this set adds
    /// is the *reason*, and the reason picks the wire refusal code. ADR 011
    /// §`StreamRequest` Response requires a governance takedown and this
    /// operator's own denylist to be indistinguishable on the wire, so both must
    /// answer `HashBlacklisted`; without this slot a governance entry falls
    /// through to the eviction arm and answers `EvictedSinceProbe` instead,
    /// which uniquely fingerprints a local entry by elimination.
    ///
    /// It is also the earlier of the two gates: the watcher denies *before* it
    /// evicts, so a takedown whose durable eviction fails still stops serving on
    /// the same tick rather than waiting for the retry.
    chain_denied: ArcSwap<HashSet<Hash>>,
    /// Hashes the operator has explicitly evicted via [`CacheEngine::evict`]
    /// (issue #279). Membership is honored by [`CacheEngine::has`] and
    /// [`CacheEngine::get`] so an evicted blob is not served, even though
    /// the underlying iroh-blobs store may still hold the bytes —
    /// `Blobs::delete` is `pub(crate)` in iroh-blobs and reserved for the
    /// GC task. [`CacheEngine::evict`] deletes the blob's protecting named
    /// tag(s) (#860), so reclaim of disk bytes then happens on the next
    /// iroh-blobs GC sweep, configured via `cache.gc_interval_sec` (#518).
    /// Reclaim is therefore best-effort: it requires GC to be enabled *and*
    /// the tag deletion to have succeeded — a failed delete leaves the bytes
    /// GC-protected and is surfaced via `tag_drop_failures` (serving is
    /// blocked regardless). On-demand (synchronous) reclamation is tracked
    /// under #520, blocked on upstream exposing the sweep API.
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

impl Inner {
    /// The only way to lock [`Inner::inflight`] (#1517).
    ///
    /// **Poison is recovered and cleared, not skipped.** This mutex guards
    /// the fill-coalescing map, which is what stops N concurrent requests
    /// for one missing blob from opening N origin pulls. Skipping the
    /// critical section — what `.lock().ok()` did before #1517 — loses
    /// coalescing, and because nothing cleared the poison, it lost it for
    /// the rest of the process. On a metered `http`/`s3` origin that is an
    /// unbounded multiplier on the egress bill; on the `Peer` origin
    /// reached via `populate` it is a double-spend of USDC vouchers to
    /// upstream nodes — the hazard [`TeeOpen::InFlight`] exists to prevent.
    /// Recovering the guard keeps that invariant intact, and matches the
    /// reasoning already written down for `evicted` (see
    /// [`CacheEngine::evict`] and [`CacheEngine::is_evicted`]).
    ///
    /// The map is only ever read, inserted into, and removed from under
    /// this lock — no user code runs inside the critical section, at any of
    /// the six call sites — so a recovered guard cannot observe a torn
    /// `HashMap`. Having established that, [`Mutex::clear_poison`] (stable
    /// since 1.77; MSRV is 1.95) returns the mutex to a healthy state. That
    /// is what makes the counter below mean *"how many tasks panicked in
    /// here"* rather than *"how many times we locked since one did"* — the
    /// latter climbs at request rate forever and reads on a `rate()` panel
    /// as a raging ongoing incident long after a single panic.
    ///
    /// **It is still reported.** The anti-panic policy makes poison close
    /// to unreachable, so a firing here is a genuine bug: it bumps
    /// `decdn_cache_inflight_mutex_poisoned_total` once per poisoning, and
    /// logs on the first one (latched via `inflight_poison_logged` — with
    /// the clear above, a repeat means a *new* panic rather than an echo of
    /// the old one, but the latch still bounds a pathological panic loop).
    ///
    /// Note the coalescing wait this guards (`notify.notified().await` in
    /// [`CacheEngine::get`]) has no deadline of its own; callers impose
    /// their own (the node wraps `populate` in `tokio::time::timeout`).
    fn lock_inflight(&self) -> MutexGuard<'_, HashMap<Hash, Arc<Notify>>> {
        match self.inflight.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                if let Some(m) = &self.metrics {
                    m.inflight_mutex_poisoned.inc();
                }
                if !self.inflight_poison_logged.swap(true, Ordering::Relaxed) {
                    tracing::error!(
                        "inflight coalescing mutex poisoned; recovering inner state and \
                         clearing the poison. A task panicked while holding it — this is a \
                         bug, not an operational condition. Coalescing is preserved and no \
                         restart is needed; any further poisonings are counted by \
                         decdn_cache_inflight_mutex_poisoned_total but not re-logged."
                    );
                }
                self.inflight.clear_poison();
                poisoned.into_inner()
            }
        }
    }
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
    inner: &'a Inner,
    notify: &'a Arc<Notify>,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        // Removal goes through `lock_inflight`, which recovers a poisoned
        // guard (#1517). Dropping the removal on poison — what the previous
        // `if let Ok(..)` did — leaked the entry, and since `notify_waiters`
        // only wakes *current* waiters, every later request for this hash
        // would then park on a `Notify` that never fires again: the exact
        // permanent hang this guard exists to prevent.
        self.inner.lock_inflight().remove(&self.hash);
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
        // diff/fold loop). Recover the inner state to keep metrics
        // flowing — the next cycle re-establishes a baseline — but log
        // once so the panic doesn't hide.
        //
        // Poison handling in this file is decided **per lock site**, not
        // per mutex, so the honest summary is a spectrum rather than a
        // taxonomy:
        // - `inflight` recovers, clears the poison, `error!`s once and
        //   counts it at all six sites, because losing its invariant costs
        //   origin egress and USDC. See [`Inner::lock_inflight`].
        // - `evicted` and `probe_holds` recover silently everywhere; for
        //   `evicted` that is load-bearing (skipping would resume serving
        //   a DMCA-takedown hash) and written up at `evict`/`is_evicted`.
        // - `access_times` is mixed: it recovers where a lost update is
        //   durability-relevant (`evict`, the LRU delete path) and skips on
        //   the read/observability paths (`last_accessed`,
        //   `access_times_snapshot`, `eviction_candidates`, `touch`). That
        //   split is a gap, not a design — a poisoned `access_times` makes
        //   `eviction_candidates` return empty, which stops LRU eviction
        //   and fills the disk. Tracked separately from #1517.
        // - `origin_probe_memo` recovers silently via `probe_memo_lock`.
        // - `prev_pre_sweep` (here) is the only metrics-only one: recover
        //   + `warn!`, since the next cycle re-establishes a baseline.
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

/// Outcome of [`CacheEngine::pull_through_range`] (#823, [ADR 037 §Origin-tier
/// pull-through](../../../adr/037-regional-proxy-warming.md)).
///
/// The range-scoped origin pull is **best-effort**: only [`Self::Served`] means
/// the requested byte span is now present as a verified partial blob. Every
/// other variant is a degrade-to-whole-blob signal — the caller falls back to
/// [`CacheEngine::populate`] / [`CacheEngine::get`], which is never a
/// correctness or availability failure (ADR 037 §"Fallback is always correct").
#[derive(Debug)]
pub enum RangePullOutcome {
    /// The requested `[byte_offset, byte_offset + byte_len)` was fetched from
    /// origin as a chunk-group-aligned span, verified against the root `H` via
    /// the untrusted `{H}.obao4` outboard, and imported as a partial blob. The
    /// node can now serve the range via iroh-blobs `export_ranges` without a
    /// whole-blob origin pull.
    Served,
    /// No configured origin could serve a range pull (none published
    /// `{H}.obao4`, none honored `Range`, the outboard was short/absent, or
    /// `origin_range_pull_enabled` was off at the call site). The caller MUST
    /// fall back to a whole-blob pull.
    Unsupported,
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
            CircuitBreakerPolicy::default(),
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
    /// function (`gc::gc_run_once`) lives in iroh-blobs 0.103's private
    /// `store::gc` module and is not re-exported, and `Blobs::delete`
    /// is `pub(crate)`. The only externally-reachable trigger is
    /// `Options::gc`. #520 tracks switching to a runtime-driven loop
    /// with manual on-demand GC (`admin_v1_cacheGc` / `decdn node gc`)
    /// once upstream exposes the sweep API.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_full(
        cache_dir: &Path,
        origins: Vec<Arc<dyn Origin>>,
        max_blob_mb: u64,
        pinned: PinnedHashes,
        retry_policy: RetryPolicy,
        circuit_breaker: CircuitBreakerPolicy,
        metrics: Option<Arc<CacheMetrics>>,
        gc_interval: Duration,
    ) -> CacheResult<Self> {
        Self::open_full_with_clock(
            cache_dir,
            origins,
            max_blob_mb,
            pinned,
            retry_policy,
            circuit_breaker,
            metrics,
            gc_interval,
            Arc::new(SystemClock::new()),
        )
        .await
    }

    /// [`Self::open_full`] with an injectable [`Clock`] driving the
    /// per-origin circuit-breaker cooldown (#963). Production goes
    /// through `open_full` (which supplies a [`SystemClock`]); tests use
    /// this to drive the breaker's cooldown deterministically with a
    /// [`crate::ManualClock`].
    #[allow(clippy::too_many_arguments)]
    pub async fn open_full_with_clock(
        cache_dir: &Path,
        origins: Vec<Arc<dyn Origin>>,
        max_blob_mb: u64,
        pinned: PinnedHashes,
        retry_policy: RetryPolicy,
        circuit_breaker: CircuitBreakerPolicy,
        metrics: Option<Arc<CacheMetrics>>,
        gc_interval: Duration,
        clock: Arc<dyn Clock>,
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
        // iroh-blobs 0.103). Bytes for that diff is what the previous
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

        // One breaker per origin, parallel by index. Each shares the
        // single injected clock and the cache metrics handle so trip /
        // recovery / short-circuit counters land in the same encoder
        // output as the rest of the cache group (#963).
        let breakers = origins
            .iter()
            .map(|_| OriginBreaker::new(circuit_breaker, Arc::clone(&clock), metrics.clone()))
            .collect::<Vec<_>>();

        Ok(Self {
            inner: Arc::new(Inner {
                store,
                origins,
                breakers,
                max_blob_bytes,
                access_times: Mutex::new(HashMap::new()),
                inflight: Mutex::new(HashMap::new()),
                inflight_poison_logged: AtomicBool::new(false),
                pinned: ArcSwap::from(Arc::new(
                    pinned
                        .iter()
                        .map(|h| to_store_hash(*h))
                        .collect::<HashSet<Hash>>(),
                )),
                origin_held: ArcSwap::from(Arc::new(HashMap::new())),
                origin_probe_memo: Mutex::new(OriginProbeMemo::default()),
                denied: ArcSwap::from(Arc::new(HashSet::new())),
                chain_denied: ArcSwap::from(Arc::new(HashSet::new())),
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
    /// 126 — "When a node caches blob H ...") to schedule the first
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

    /// Rebuild the origin-held discovery index (#1130): the hashes this node
    /// can serve from a configured origin *before* any pull-through, so probe
    /// and DHT-announce can advertise them. The set is the union of
    ///
    /// - every enumerable origin's [`Origin::enumerate`] listing (the local
    ///   filesystem walks its sharded tree; HTTP/S3 enumerate to nothing), and
    /// - operator `pinned_hashes` the origin chain actually has (the remote
    ///   discovery path — HTTP has no listing, S3's is deliberately not walked,
    ///   so the pin list is their advertisement set),
    ///
    /// each mapped to its total byte size and filtered through [`Self::refuses`]
    /// so denied/blacklisted content is never advertised. Rebuilt wholesale and
    /// swapped atomically (like `pinned`): a concurrent reader sees the old or
    /// the new map, never a partial one.
    ///
    /// **Cost:** one [`Self::origin_size`] probe per held hash — a local
    /// `metadata()` stat per fs entry, one HTTP `HEAD` / S3 `HeadObject` per
    /// pinned remote hash. Runs off the hot path at the configured rescan
    /// cadence (startup / interval / reload), never per request.
    pub async fn rescan_origins(&self) {
        // Gather candidate hashes: every enumerable origin's listing plus the
        // operator pin set (snapshotted before any await, so we never hold the
        // `ArcSwap` guard across a `size()` probe).
        let mut candidates: Vec<Hash> = Vec::new();
        for origin in &self.inner.origins {
            match origin.enumerate().await {
                Ok(hashes) => candidates.extend(hashes),
                Err(err) => tracing::warn!(
                    origin = ?origin.kind(),
                    error = %err,
                    "rescan_origins: enumerate failed; skipping this origin"
                ),
            }
        }
        candidates.extend(self.inner.pinned.load().iter().copied());

        // Resolve size for each present, non-refused candidate, deduping so we
        // probe each hash once. `origin_size` is a local stat (fs) or one HEAD
        // (pinned remote); `None`/error means "origin doesn't have it" → skip.
        let mut held: HashMap<Hash, u64> = HashMap::new();
        for hash in candidates {
            if self.refuses(hash) || held.contains_key(&hash) {
                continue;
            }
            if let Ok(Some(size)) = self.origin_size(hash).await {
                held.insert(hash, size);
            }
        }

        let count = held.len();
        self.inner.origin_held.store(Arc::new(held));
        tracing::debug!(count, "rescan_origins: refreshed origin-held index");
    }

    /// Snapshot of the hashes in the origin-held index (#1130), for seeding the
    /// DHT announce / republish set alongside [`Self::iter_hashes`].
    ///
    /// Filtered through the **live** [`Self::refuses`] set: the index is only a
    /// per-rescan snapshot, so a hash blacklisted / evicted / denied *after* the
    /// last rescan is still in it — but must never be announced. The live filter
    /// closes that window without waiting for the next rescan.
    pub fn origin_held_hashes(&self) -> Vec<Hash> {
        self.inner
            .origin_held
            .load()
            .keys()
            .copied()
            .filter(|h| !self.refuses(*h))
            .collect()
    }

    /// Total byte size of `hash` if this node can serve it from a configured
    /// origin (#1130), else `None`. Backs the probe `has_blob` / `total_bytes`
    /// answer for origin-held content — `Some` means "advertise and serve" —
    /// resolved from the last [`Self::rescan_origins`] with no per-probe I/O.
    ///
    /// Returns `None` for a [`Self::refuses`]-listed hash even if the snapshot
    /// still holds it: the index is rebuilt only on rescan, so a hash
    /// blacklisted / evicted / denied since the last rescan would otherwise be
    /// advertised (probe `has_blob: true`) for content the serve path would
    /// refuse. The live refusal check is the authority.
    pub fn origin_held_size(&self, hash: Hash) -> Option<u64> {
        if self.refuses(hash) {
            return None;
        }
        self.inner.origin_held.load().get(&hash).copied()
    }

    /// Total byte size of `hash` if a configured origin can serve it, resolved
    /// by a **live** `HEAD`/`HeadObject`/stat and memoised (#1130 pt3). This is
    /// the per-probe fallback for the http/s3 discovery gap: [`Self::rescan_origins`]
    /// can only index what an origin `enumerate`s, and http/s3 enumerate to
    /// nothing, so a non-pinned bucket object is absent from
    /// [`Self::origin_held_size`]. Callers should consult the in-memory index
    /// first (zero I/O) and only fall back here on its miss.
    ///
    /// A memo hit returns with no I/O. On a miss the origin chain is probed via
    /// [`Self::origin_size`] under a `cache.origin_probe_timeout_ms` ceiling, and
    /// the answer — `Present(size)` OR `Absent` — is memoised for
    /// `cache.origin_probe_ttl_sec`. Caching the negative is deliberate: it is
    /// what stops a random-hash probe flood from issuing a `HeadObject` per
    /// probe. A timeout, a transport error, an unknown size, and a genuine 404
    /// all fold to `Absent`/`None` — the safe "do not advertise" answer, which
    /// post-#1512 is never slashable (an unservable `has_blob:false`, or a later
    /// `NotFound` on the serve, carries only local reputation, not a bond slash).
    ///
    /// **Never fetches the body** — existence and size only.
    pub async fn origin_probe_size(&self, hash: Hash) -> Option<u64> {
        if self.refuses(hash) {
            return None;
        }
        let now = Instant::now();
        let timeout = {
            let mut memo = self.probe_memo_lock();
            if let Some(presence) = memo.get(hash, now) {
                return presence.size();
            }
            memo.timeout()
        };
        // Live probe off the memo lock (never hold it across the await). A
        // `NoOrigin` error (no origins configured), any transport error, an
        // unknown size, or the timeout all collapse to `Absent`.
        let presence = match tokio::time::timeout(timeout, self.origin_size(hash)).await {
            Ok(Ok(Some(size))) => Presence::Present(size),
            Ok(Ok(None) | Err(_)) | Err(_) => Presence::Absent,
        };
        self.probe_memo_lock().insert(hash, presence, now);
        presence.size()
    }

    /// Lock the origin-probe memo, recovering a poisoned mutex rather than
    /// panicking (the anti-panic policy) — a poisoned memo only means a prior
    /// holder panicked mid-update, and a best-effort existence cache is safe to
    /// keep using.
    fn probe_memo_lock(&self) -> std::sync::MutexGuard<'_, OriginProbeMemo> {
        self.inner
            .origin_probe_memo
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Install the live-origin-probe configuration at runtime bring-up (#1130
    /// pt3). `cache.*` is restart-required, so this is called once from the
    /// runtime wiring and swaps the memo wholesale (dropping any warm entries) —
    /// the same "set once, no threading through every test constructor" pattern
    /// as [`Self::set_max_probe_holds`].
    pub fn set_origin_probe_config(&self, ttl: Duration, timeout: Duration, capacity: usize) {
        *self.probe_memo_lock() = OriginProbeMemo::new(ttl, timeout, capacity);
    }

    /// Swap in the live *local* denied set from `[content] denied_hashes` (ADR
    /// 011 § Local Denylist). Returns the delta for the reload log line, same
    /// shape as [`Self::set_pinned`].
    ///
    /// Wholesale replacement, not a merge: removing an entry from the denylist
    /// and reloading must un-deny it. Touches only the local slot — a reload
    /// must not drop what the blacklist watcher put in [`Self::set_chain_denied`].
    pub fn set_denied(&self, new: &DeniedHashes) -> PinDiff {
        let new_arc = Arc::new(
            new.iter()
                .map(|h| to_store_hash(*h))
                .collect::<HashSet<Hash>>(),
        );
        let prev_arc = self.inner.denied.swap(Arc::clone(&new_arc));
        let added = new_arc.difference(&prev_arc).count();
        let removed = prev_arc.difference(&new_arc).count();
        PinDiff { added, removed }
    }

    /// Is `hash` on the live denied set? Cheap O(1) lookup.
    ///
    /// Prefer [`Self::refuses`] unless you specifically need to tell a denial
    /// apart from an eviction — the serve path does, to pick the right refusal
    /// code and metric; nothing else should care.
    pub fn is_denied(&self, hash: Hash) -> bool {
        self.inner.denied.load().contains(&hash)
    }

    /// Replace the governance deny-set wholesale.
    ///
    /// The set records *why* a hash is refused — governance-sourced, which selects
    /// the wire refusal code — a fact `evicted.log` (which records only *that* a
    /// hash was evicted) cannot express. The blacklist watcher rebuilds it each
    /// boot by re-checking every enumerated hash through
    /// `isHashBlacklistedForOperator` and writing survivors one at a time via
    /// [`Self::set_chain_denied_one`]; this bulk setter is retained for tests and
    /// any wholesale reseed.
    pub fn set_chain_denied(&self, hashes: HashSet<Hash>) {
        self.inner.chain_denied.store(Arc::new(hashes));
    }

    /// Add or drop one governance-denied hash, returning whether the set
    /// actually changed so a caller can skip logging a no-op replay (the watcher
    /// re-scans a block range after a restart and re-delivers events it has
    /// already applied).
    ///
    /// Read-modify-write rather than in-place mutation, because [`ArcSwap`] has
    /// no such thing. Blacklist events are governance actions and therefore
    /// rare, so the clone costs nothing next to keeping the read side — which is
    /// on the request hot path — lock-free.
    pub fn set_chain_denied_one(&self, hash: Hash, denied: bool) -> bool {
        let current = self.inner.chain_denied.load();
        if current.contains(&hash) == denied {
            return false;
        }
        let mut next = HashSet::clone(&current);
        if denied {
            next.insert(hash);
        } else {
            next.remove(&hash);
        }
        self.inner.chain_denied.store(Arc::new(next));
        true
    }

    /// Is `hash` on the live governance deny-set? Cheap O(1) lookup.
    ///
    /// Prefer [`Self::refuses`] unless you specifically need to tell a
    /// governance takedown apart from an eviction or a local denylist entry —
    /// the serve path does, to pick the operator-side metric; nothing else
    /// should care, and the *wire* code deliberately cannot tell this apart from
    /// [`Self::is_denied`].
    pub fn is_chain_denied(&self, hash: Hash) -> bool {
        self.inner.chain_denied.load().contains(&hash)
    }

    /// Does this node refuse to serve, announce, or acquire `hash`?
    ///
    /// The single predicate every "should I expose this blob" decision must
    /// call. `is_evicted` alone is NOT sufficient and using it directly is the
    /// bug this exists to prevent: a denied-but-still-held hash would keep
    /// being probe-answered `has_blob: true` and DHT-republished while the
    /// serve path refused it — which under ADR 005 § Probe response is exactly
    /// the signed evidence pair that makes the operator slashable for a
    /// takedown they were discharging.
    pub fn refuses(&self, hash: Hash) -> bool {
        self.is_denied(hash) || self.is_chain_denied(hash) || self.is_evicted(hash)
    }

    /// Is this blob already present in the local store?
    ///
    /// Returns `Ok(false)` when the hash has been logically evicted (issue
    /// #279) even if the underlying store still holds the bytes — operators
    /// who call `evict` expect the node to stop serving immediately, so
    /// `has` reports the blob as absent.
    pub async fn has(&self, hash: Hash) -> CacheResult<bool> {
        if self.refuses(hash) {
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
    /// Serving stops immediately via the logical-evicted set (`Blobs::delete`
    /// is `pub(crate)` in iroh-blobs and reserved for the GC task, so the
    /// bytes are not removed synchronously). `evict` deletes the blob's
    /// protecting named tag(s) (#860) so the bytes become GC-eligible; the
    /// disk is then reclaimed on the next iroh-blobs GC sweep — cadence
    /// `cache.gc_interval_sec`, default 5min (#518) — *when periodic GC is
    /// enabled*. With GC disabled (`gc_interval_sec == 0`) serving still stops
    /// but the bytes stay on disk until a sweep is configured. Reclaim is
    /// best-effort in the other direction too: the tag deletion is not
    /// allowed to fail the takedown, so if it errors the bytes remain
    /// GC-protected (surfaced via `tag_drop_failures`) while serving stays
    /// blocked. The operator-visible behavior — the node stops serving the
    /// blob immediately, and reclaims its disk on the next sweep when GC is
    /// enabled and the tag delete succeeded — is what `decdn node evict`
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
    ///
    /// Async because the durable append runs the blocking `fsync` on a
    /// `spawn_blocking` thread rather than on the async runtime (#845): one
    /// caller is the hash-mismatch path in pull-through, which executes on a
    /// request-serving worker that must not stall on disk I/O.
    pub async fn evict(&self, hash: Hash) -> CacheResult<()> {
        // Pre-check under one lock acquisition: short-circuit on
        // already-evicted (a sequential repeat-evict of the same hash returns
        // here and never re-appends) and reject on cap (DoS bound on an
        // unbounded public-ish surface). Both checks are best-effort against
        // concurrency: the lock is released before the append below, so two
        // evict() calls racing the *same* new hash can each pass and append a
        // duplicate `evicted.log` line — harmless, since replay folds the log
        // into a `HashSet`. The cap is likewise a soft DoS bound, not a hard
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
        //
        // The append's `fsync` is blocking, so run it on a `spawn_blocking`
        // thread (#845) to keep it off the async runtime — `evict` is reached
        // from the request-serving hash-mismatch path. A panic in the blocking
        // task (JoinError) and an I/O failure are both surfaced as `Err` so the
        // operator never gets an `Ok` that silently skipped durable persistence.
        let log_path = self.inner.evicted_log_path.clone();
        tokio::task::spawn_blocking(move || append_evicted_log(&log_path, hash))
            .await
            .map_err(|join_err| {
                CacheError::Store(anyhow::Error::new(join_err).context("persist eviction task"))
            })?
            .map_err(|err| {
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

        // Disk reclaim (#860): serving has already stopped via the logical
        // set above, but a successfully pulled-through blob carries a named
        // tag that protects it from the iroh-blobs GC sweep forever. Delete
        // that tag so the next sweep can actually reclaim the bytes — without
        // this a DMCA evict retains the content on disk indefinitely. Two
        // caveats on the reclaim, both intentional: GC must be enabled
        // (`cache.gc_interval_sec > 0`); and a pull-through for the same hash
        // that races this evict can re-create a protecting tag *after* the
        // delete below (the commit path does not re-check `is_evicted`), in
        // which case reclaim waits until that tag is itself cleared — serving
        // still stays blocked via the logical set either way.
        //
        // Best-effort: the durable serve-blocking guarantee is already in
        // place, so a tag-delete failure only delays reclaim (it can never
        // resurrect serving) and must not turn a successful takedown into an
        // `Err`. The failure is surfaced via `tag_drop_failures` + a warn so
        // an operator can act; note nothing auto-retries it — a re-run of
        // `evict()` short-circuits at the `contains` check above, and open()
        // replay only reloads the logical set, so recovery is manual.
        match self.drop_named_tags_for(hash).await {
            Ok(deleted) => tracing::debug!(%hash, deleted, "evict: dropped protecting tags"),
            Err(err) => {
                if let Some(m) = &self.inner.metrics {
                    m.tag_drop_failures.inc();
                }
                tracing::warn!(
                    %hash,
                    %err,
                    "evict: failed to drop protecting tags; bytes stay GC-protected (not auto-retried)",
                );
            }
        }
        // Operator-evict (DMCA/corruption) counter — distinct from the LRU
        // `evictions` counter the eviction driver bumps (#1173,
        // appendix-blob-cache-eviction.md § Observability). Bumped after the
        // durable append + logical-set commit succeeded above, so the count
        // tracks takedowns that actually stopped serving.
        if let Some(m) = &self.inner.metrics {
            m.evicted_operator.inc();
        }
        Ok(())
    }

    /// Delete every named tag pointing at `hash`, making the underlying
    /// bytes eligible for the iroh-blobs GC sweep.
    ///
    /// Pull-through promotes each cached blob to a named tag
    /// ([`iroh_blobs::api::tags::Tags::create`] on the streaming path,
    /// `add_bytes` on the drain path); iroh-blobs GC protects any blob
    /// reachable from a tag, so the bytes are never reclaimed while a tag
    /// survives. Tag names are opaque and store-assigned, so matches are
    /// found by enumerating tags and comparing hashes. A hash can carry more
    /// than one tag, so every match is removed (in practice the engine creates
    /// one tag per cached hash — `get`/`populate` short-circuit on a hit and
    /// in-flight pulls coalesce (#305) — but the loop is robust to a future
    /// caller that tags the same hash twice). Returns the number of tags
    /// deleted.
    ///
    /// Cost scales with the *total* tag set: iroh-blobs has no hash-indexed
    /// tag lookup, so this lists every tag and filters in Rust. Fine at
    /// operator-evict / mismatch rates; not something to call on a hot path.
    async fn drop_named_tags_for(&self, hash: Hash) -> CacheResult<u64> {
        let tags = self.inner.store.tags();
        // Collect matching names before deleting so the (immutable) list
        // stream is fully drained before any delete call — keeps the two
        // store interactions sequential and easy to reason about.
        let mut to_delete = Vec::new();
        {
            let mut stream = tags
                .list()
                .await
                .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
            while let Some(info) = stream.next().await {
                let info = info.map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
                if info.hash == hash {
                    to_delete.push(info.name);
                }
            }
        }
        let mut deleted = 0u64;
        for name in to_delete {
            deleted += tags
                .delete(name)
                .await
                .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        }
        Ok(deleted)
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
    /// disables the hold path so [`Self::try_probe_hold`] returns
    /// [`ProbeHoldOutcome::HoldsDisabled`] for any present blob (the node then
    /// answers `has_blob: false` to every probe).
    pub fn set_max_probe_holds(&self, max: usize) {
        self.inner.max_probe_holds.store(max, Ordering::Relaxed);
    }

    /// Attempt to take (or refresh) a probe-triggered eviction hold on
    /// `hash` for [`crate::probe_hold::PROBE_HOLD_DURATION`] (ADR 005
    /// §Probe-triggered eviction hold).
    ///
    /// Returns [`ProbeHoldOutcome::Held`] when the blob is present, not
    /// operator-evicted, and a hold was placed for the full window. The other
    /// three variants classify why no hold happened, so the caller need not
    /// re-inspect the cache:
    /// - [`ProbeHoldOutcome::Unavailable`] — blob absent or operator-evicted.
    /// - [`ProbeHoldOutcome::HoldsDisabled`] — holds disabled by config
    ///   (`max_probe_holds == 0`); an intentional operator decision.
    /// - [`ProbeHoldOutcome::BudgetExhausted`] — present but every hold slot
    ///   is live (`max_probe_holds > 0`); genuine budget pressure.
    ///
    /// Placing a hold and advertising the blob are **not** the same decision:
    /// `BudgetExhausted` is advertised despite holding no slot. Callers should
    /// read [`ProbeHoldOutcome::advertises`] / [`ProbeHoldOutcome::hold_placed`]
    /// rather than comparing against `Held`.
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
        // `has` returns false for operator-evicted hashes too. This stays
        // *before* the `max == 0` check below so an absent/evicted blob is a
        // true negative (`Unavailable`), not a config-disable event.
        if !self.has(hash).await? {
            return Ok(ProbeHoldOutcome::Unavailable);
        }
        let max = self.inner.max_probe_holds.load(Ordering::Relaxed);
        // Holds disabled by config (#739): answer `HoldsDisabled` before
        // touching the lock. This is the "never advertise store-backed content"
        // path — it must override an existing live hold so that
        // if the budget is ever lowered to 0 while a hold is live, the disable
        // wins instead of the fast path below refreshing and re-signing the
        // hold. (Today only the tests lower it post-startup;
        // `cache.max_probe_holds` is restart-required, not hot-reloaded.) Also
        // skips the O(N) expiry sweep entirely while holds are off. Returning
        // before the under-lock `is_evicted` re-check is harmless: a blob
        // evicted concurrently here is still answered `has_blob: false` (the
        // misclassification is `Unavailable`→`HoldsDisabled`, a metric-only
        // miscount between two counters — never a false `has_blob: true`).
        if max == 0 {
            return Ok(ProbeHoldOutcome::HoldsDisabled);
        }
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
        // refresh can't resurrect just-refused content.
        if guard.get(&hash).is_some_and(|exp| *exp > now) {
            if self.refuses(hash) {
                return Ok(ProbeHoldOutcome::Unavailable);
            }
            guard.insert(hash, expiry);
            return Ok(ProbeHoldOutcome::Held);
        }

        // No live hold — sweep expired entries before consulting the budget.
        guard.retain(|_, exp| *exp > now);
        // TOCTOU re-check: a concurrent `evict()` or denylist reload may have
        // completed after the `has()` above. Under the lock, a refused hash is
        // never held — a hold is what authorises signing `has_blob: true`, and
        // signing that for a *blacklisted* hash is the ADR 014
        // blacklist-violation evidence (and for a locally-evicted one, an
        // advertisement the serve path would only refuse).
        if self.refuses(hash) {
            return Ok(ProbeHoldOutcome::Unavailable);
        }
        if guard.len() >= max {
            // `max == 0` was already handled above, so reaching the budget
            // ceiling here is always genuine pressure: every slot is live
            // (#739). This is the "increase max_probe_holds" signal (ADR 005
            // §Hold budget) — never a config disable.
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
        if self.refuses(hash) {
            if let Some(m) = &self.inner.metrics {
                m.misses.inc();
            }
            return Err(CacheError::NotFound { hash });
        }

        // Coalesce concurrent pull-through requests for the same hash (#305).
        // A single lock acquisition atomically checks and inserts to avoid the
        // race where multiple tasks see an empty map and all proceed to pull.
        let bytes = loop {
            let state = {
                let mut guard = self.inner.lock_inflight();
                if let Some(n) = guard.get(&hash) {
                    Err(Arc::clone(n))
                } else {
                    let n = Arc::new(Notify::new());
                    guard.insert(hash, Arc::clone(&n));
                    Ok(n)
                }
            };

            match state {
                // Another task owns the pull — wait, then retry from the top.
                Err(notify) => {
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
                Ok(notify) => {
                    let _guard = InflightGuard {
                        hash,
                        inner: &self.inner,
                        notify: &notify,
                    };
                    break self.pull_through_bytes(hash).await?;
                }
            }
        };
        self.touch(hash);
        if let Some(m) = &self.inner.metrics {
            m.bytes_returned
                .inc_by(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        }
        Ok(bytes)
    }

    /// Ensure `hash` is present locally, pulling it through the origin chain on
    /// a miss — like [`Self::get`] but WITHOUT returning the bytes or bumping
    /// the `bytes_returned` / `hits` counters (which measure bytes served to a
    /// `get` caller). Use this to fill the cache as a *side effect* — e.g. the
    /// node-to-node cache-miss pull-through hook (#831) — so an internal fill is
    /// not miscounted as client-facing egress and the whole blob is not
    /// re-assembled into a buffer the caller would just drop. A hit is a no-op.
    ///
    /// That second guarantee is enforced, not merely intended: this path is
    /// `FillMode::CommitOnly` all the way down, and neither commit arm can hand a
    /// payload back under it — the streaming arm skips the `read_local`, the
    /// buffered arm drops its drain buffer (#1132). Before that the blob *was*
    /// read back and dropped, which is how the serve path's miss leg came to hold
    /// ~708 MB for a 708 MB blob.
    ///
    /// Note the buffered arm's cost is bounded by
    /// `cache.origin_retry.buffered_max_bytes` (4 MiB default) rather than by the
    /// mode: an origin that advertises a `size_hint` at or under it is drained
    /// before commit either way (a `None` hint always streams, and `0` disables
    /// buffering). `CommitOnly` guarantees no blob is handed *back*, not that no
    /// blob is ever briefly buffered.
    ///
    /// The origin-egress metric (`pull_through_bytes`) is still bumped by the
    /// pull, which is correct — those bytes really did leave an origin.
    ///
    /// # Errors
    ///
    /// Same set as [`Self::get`] (`NoOrigin` / `NotFound` / `HashMismatch` /
    /// `BlobTooLarge` / `OriginError` / `Store`).
    pub async fn populate(&self, hash: Hash) -> CacheResult<()> {
        self.populate_inner(hash, false).await
    }

    /// Like [`Self::populate`], but restricted to the node's OWN configured
    /// origins (fs/http/s3): the `Peer` node→node origin is never consulted, so
    /// this fronts no upstream USDC (#1116). The serve path uses it to
    /// reactively fill from a local origin a blob a paying, channel-owning
    /// client asked for — independent of `node_to_node_pull_through_enabled` —
    /// and to prefer a local origin over the paid peer window path. A hit is a
    /// no-op; absence from every local origin surfaces `NotFound`, and a chain
    /// with no non-`Peer` origin surfaces `NoOrigin`.
    ///
    /// # Errors
    ///
    /// Same set as [`Self::populate`].
    pub async fn populate_local(&self, hash: Hash) -> CacheResult<()> {
        self.populate_inner(hash, true).await
    }

    /// Shared body of [`Self::populate`] / [`Self::populate_local`]. `local_only`
    /// threads through the coalescing loop into [`Self::pull_through`], where it
    /// skips the `Peer` origin.
    async fn populate_inner(&self, hash: Hash, local_only: bool) -> CacheResult<()> {
        if self.has(hash).await? {
            self.touch(hash);
            return Ok(());
        }
        // Logical-eviction guard (#279): never re-pull a deliberately evicted
        // hash (mirrors `get`).
        if self.refuses(hash) {
            if let Some(m) = &self.inner.metrics {
                m.misses.inc();
            }
            return Err(CacheError::NotFound { hash });
        }
        // Coalesce concurrent fills for the same hash (#305), mirroring `get`'s
        // loop but bumping no `get`-caller metrics: `populate` fills as a side
        // effect, so it counts neither a hit nor returned bytes.
        loop {
            let state = {
                let mut guard = self.inner.lock_inflight();
                if let Some(n) = guard.get(&hash) {
                    Err(Arc::clone(n))
                } else {
                    let n = Arc::new(Notify::new());
                    guard.insert(hash, Arc::clone(&n));
                    Ok(n)
                }
            };
            match state {
                // Another task owns the pull — wait, then re-check presence.
                Err(notify) => {
                    notify.notified().await;
                    if self.has(hash).await? {
                        break;
                    }
                }
                // We own the pull — the guard wakes waiters + clears the entry
                // even on cancellation.
                Ok(notify) => {
                    let _guard = InflightGuard {
                        hash,
                        inner: &self.inner,
                        notify: &notify,
                    };
                    // Fill-only: the payload is dropped, so the whole blob is
                    // never read back out of the store (#1132). The wrapper
                    // returns `()`, so there is nothing here to drop by accident.
                    self.pull_through_fill(hash, local_only).await?;
                    break;
                }
            }
        }
        self.touch(hash);
        Ok(())
    }

    /// Attempt a **range-scoped** origin pull-through for `[byte_offset,
    /// byte_offset + byte_len)` of `hash` (`byte_len == 0` = to the blob end),
    /// per [ADR 037 §Origin-tier pull-through](../../../adr/037-regional-proxy-warming.md)
    /// (#823). `blob_size` is the trusted total size — sourced from the signed
    /// `StreamResponse.total_bytes` (or the manifest `ChunkEntry.size`), never
    /// from the origin — and is what frames the bao tree.
    ///
    /// On success the requested span is fetched chunk-group-aligned, the
    /// fetched bytes + the untrusted `{H}.obao4` outboard are verified against
    /// the root `H` ([`crate::range_pull::encode_verified_range`]), and the
    /// verified span is imported as a **partial** blob via iroh-blobs
    /// `import_bao_bytes` — no whole-blob origin egress. The node then serves
    /// the range via `export_ranges` ([ADR 038 §Serve side](../../../adr/038-bao-verified-range-streaming.md)).
    /// Only the actually-pulled bytes (span + outboard) are metered as origin
    /// egress, tightening the seed-leech caps rather than the whole blob (ADR
    /// 037 §"Range-scoped origin pulls only tighten the caps").
    ///
    /// Returns [`RangePullOutcome::Served`] when the partial range is present,
    /// or [`RangePullOutcome::Unsupported`] when no origin could range-pull
    /// (no `{H}.obao4`, no `Range`, short outboard) — in which case the caller
    /// MUST fall back to a whole-blob [`Self::populate`] / [`Self::get`]. The
    /// fallback is always correct; the optimization only reduces the origin
    /// hop's cost.
    ///
    /// This is **partial**-blob population: unlike [`Self::populate`] it does
    /// not promote a named tag or make [`Self::has`] return `true` (which
    /// requires a `Complete` blob), and it does not announce a DHT insert — a
    /// node holding only a range is not advertised as a full holder (ADR 037
    /// §"partial warming copies are not advertised"). A subsequent whole-blob
    /// pull-through (or further range pulls) completes the blob.
    ///
    /// # Errors
    ///
    /// - [`CacheError::NoOrigin`] — no origin configured.
    /// - [`CacheError::NotFound`] — the hash is logically evicted (operator
    ///   takedown / DMCA); a range pull must not silently re-fetch and re-cache
    ///   evicted content (mirrors [`Self::get`] / [`Self::populate`]).
    /// - [`CacheError::OriginError`] — the requested range is out of bounds for
    ///   `blob_size` (ADR 005: reject, don't clamp).
    /// - [`CacheError::Store`] — a local store fault while importing the
    ///   verified partial blob (disk full / IO). Like the whole-blob
    ///   pull-through path, a store fault fails fast rather than masking a
    ///   misbehaving local store behind the next origin.
    ///
    /// A genuine origin *transport / verify* fault is NOT surfaced as an error:
    /// it is recorded in `last_err`, logged, and the chain advances; if every
    /// origin declines or errors the call returns [`RangePullOutcome::Unsupported`]
    /// so the caller falls back to a whole-blob pull (which re-surfaces the real
    /// fault if the blob is genuinely unreachable). A *missing-outboard* /
    /// *no-range* origin likewise returns [`RangePullOutcome::Unsupported`].
    pub async fn pull_through_range(
        &self,
        hash: Hash,
        byte_offset: u64,
        byte_len: u64,
        blob_size: u64,
    ) -> CacheResult<RangePullOutcome> {
        if self.inner.origins.is_empty() {
            return Err(CacheError::NoOrigin { hash });
        }
        // Logical-eviction guard (#279): once an operator has run
        // `decdn node evict <hash>` (e.g. a DMCA takedown), a subsequent range
        // pull must not silently re-fetch the evicted span from the origin and
        // undo the eviction — exactly as `get` / `populate` refuse. The
        // eviction is sticky for the life of `<cache_dir>/evicted.log`.
        if self.refuses(hash) {
            if let Some(m) = &self.inner.metrics {
                m.misses.inc();
            }
            return Err(CacheError::NotFound { hash });
        }
        // Reject an out-of-bounds request up front (ADR 005 §Bounded byte
        // ranges: reject, never silently clamp). `align_range` owns the bound
        // check; map its typed error onto the engine's origin-error surface so
        // the caller sees a coherent `CacheError` rather than a cache-internal
        // type.
        let aligned =
            align_range(byte_offset, byte_len, blob_size).map_err(|e| CacheError::OriginError {
                hash,
                source: anyhow::Error::new(e).context("range pull-through: invalid byte range"),
            })?;

        if let Some(m) = &self.inner.metrics {
            m.origin_fetches.inc();
        }
        let root = *hash.as_bytes();
        let req = OriginRangeRequest {
            fetch_start: aligned.fetch_start(),
            fetch_end: aligned.fetch_end(),
        };

        // Walk the origin fallback chain (#284). A per-origin `Unsupported`
        // (no outboard / no range) advances to the next origin; a genuine
        // origin transport / verify fault is recorded and the chain advances.
        // A local-store fault (`CacheError::Store`: disk full / IO) fails fast
        // and is NOT masked by trying another origin — consistent with
        // whole-blob `pull_through`, where `Store` short-circuits the chain
        // (a misbehaving *local* store is not fixed by a different *origin*).
        // The first origin that serves and verifies a range wins.
        let mut last_err: Option<CacheError> = None;
        for origin in &self.inner.origins {
            match self
                .range_pull_attempt(Arc::clone(origin), hash, root, blob_size, &aligned, req)
                .await
            {
                Ok(RangePullOutcome::Served) => return Ok(RangePullOutcome::Served),
                Ok(RangePullOutcome::Unsupported) => {}
                // Local store fault: fail fast, do not advance the chain.
                Err(e @ CacheError::Store(_)) => return Err(e),
                Err(e) => last_err = Some(e),
            }
        }
        // Every origin declined the optimization. If any errored, the caller
        // still falls back to a whole-blob pull (which will surface the real
        // error if the blob is genuinely unreachable), so prefer the degrade
        // signal — but log a genuine fault so it isn't silently swallowed.
        if let Some(e) = last_err {
            tracing::warn!(
                %hash,
                error = %e,
                "range pull-through attempt errored on every origin; degrading to whole-blob pull",
            );
        }
        Ok(RangePullOutcome::Unsupported)
    }

    /// One range-pull attempt against a single origin: fetch the aligned span
    /// plus outboard, verify against `root`, then import the verified partial
    /// blob. Returns [`RangePullOutcome::Unsupported`] (degrade) for any
    /// non-error decline; `Err` only for genuine transport / store / verify
    /// faults.
    async fn range_pull_attempt(
        &self,
        origin: Arc<dyn Origin>,
        hash: Hash,
        root: [u8; 32],
        blob_size: u64,
        aligned: &AlignedRange,
        req: OriginRangeRequest,
    ) -> CacheResult<RangePullOutcome> {
        let outboard_max = expected_outboard_len(blob_size).saturating_add(64);
        let (data, outboard) =
            match origin
                .fetch_range(hash, req, outboard_max)
                .await
                .map_err(|e| CacheError::OriginError {
                    hash,
                    source: e.into_inner(),
                })? {
                OriginRangeFetch::Ranged { data, outboard } => (data, outboard),
                // Missing outboard / no range support / object absent → degrade.
                OriginRangeFetch::Unsupported | OriginRangeFetch::NotFound => {
                    return Ok(RangePullOutcome::Unsupported);
                }
            };

        // Meter the actually-pulled bytes (span + outboard) as origin egress —
        // the bytes really did leave an origin. This is what ADR 037 counts
        // against the seed-leech caps: the pulled side, not the whole blob.
        if let Some(m) = &self.inner.metrics {
            let pulled = u64::try_from(data.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(outboard.len()).unwrap_or(u64::MAX));
            m.pull_through_bytes.inc_by(pulled);
        }

        // Verify the untrusted range + outboard against the root `H` and
        // produce the bao interleaved encoding for `import_bao_bytes`. A
        // verification failure (tampered range/outboard, wrong root) is a
        // deterministic protocol violation — degrade to a whole-blob pull
        // (which re-verifies whole-blob against `H`) rather than erroring, so
        // a single misbehaving origin can't deny the range entirely. A
        // wrong-length outboard is the same degrade.
        let encoded = match encode_verified_range(root, aligned, &data, outboard) {
            Ok(bytes) => bytes,
            Err(err) => {
                tracing::warn!(
                    %hash,
                    kind = ?origin.kind(),
                    error = %err,
                    "origin range failed bao verification; degrading to whole-blob pull",
                );
                return Ok(RangePullOutcome::Unsupported);
            }
        };

        // Import the verified span as a partial blob. The chunk ranges scope
        // exactly what was verified; iroh-blobs writes them as a partial blob
        // anchored at `hash`. A store fault here is a real error (local disk /
        // actor problem), surfaced as `Store`.
        self.inner
            .store
            .blobs()
            .import_bao_bytes(hash, aligned.chunk_ranges().clone(), encoded)
            .await
            .map_err(|e| {
                CacheError::Store(
                    anyhow::Error::from(e).context("import_bao_bytes failed for verified range"),
                )
            })?;

        Ok(RangePullOutcome::Served)
    }

    /// Best-effort total byte size of `hash` from the configured origins, for
    /// scoping a range pull ([`Self::pull_through_range`] needs the exact blob
    /// size to align + verify a sub-range against the root `H`, and the
    /// `{H}.obao4` outboard alone doesn't pin the final chunk's length). Walks
    /// the origin fallback chain ([`Origin::size`] — HTTP `HEAD` / S3
    /// `HeadObject` / `fs` metadata) and returns the first known size; a
    /// per-origin `Ok(None)` (no object / compressed / unsupported) or a
    /// transport error advances the chain.
    ///
    /// Returns `Ok(None)` when no origin can answer — the caller MUST then
    /// degrade to a whole-blob [`Self::populate`] / [`Self::get`]. This is a
    /// metadata probe only: it never fetches or caches bytes, so unlike
    /// [`Self::pull_through_range`] it carries no logical-eviction guard (the
    /// caller's range pull and whole-blob fallback both enforce it).
    ///
    /// # Errors
    ///
    /// [`CacheError::NoOrigin`] when no origin is configured (mirrors
    /// [`Self::pull_through_range`], so the caller sees a coherent "can't
    /// range-pull" signal rather than a silent `None`).
    pub async fn origin_size(&self, hash: Hash) -> CacheResult<Option<u64>> {
        if self.inner.origins.is_empty() {
            return Err(CacheError::NoOrigin { hash });
        }
        for origin in &self.inner.origins {
            match origin.size(hash).await {
                Ok(Some(size)) => return Ok(Some(size)),
                Ok(None) => {}
                Err(e) => {
                    tracing::debug!(
                        %hash,
                        kind = ?origin.kind(),
                        error = %e,
                        "origin size probe failed; trying next origin",
                    );
                }
            }
        }
        Ok(None)
    }

    /// Open a [`LocalOutboardPull`] (#1130 stream-while-store): stream an
    /// origin's PLAINTEXT bytes through the header-less bao encoder as they
    /// arrive, verifying against a locally-fetched `{H}.obao4` outboard, so a
    /// paying client's cache-miss serve can start before the whole blob has
    /// finished pulling through — rather than waiting on a whole-blob
    /// [`Self::populate`] first.
    ///
    /// Walks the origin chain twice, mirroring [`Self::origin_size`] /
    /// [`Self::pull_through_range`]'s degrade discipline: first for the
    /// blob's total size ([`Self::origin_size`]), then for the first origin
    /// that serves the outboard ([`Origin::fetch_outboard`]). Any decline —
    /// unknown size, no origin publishes the outboard, or the winning origin
    /// no longer has the data itself — returns `Ok(None)` so the caller
    /// degrades to [`Self::populate`] and serves from the store as usual.
    /// This is never a correctness or availability failure, only a forgone
    /// optimization (same contract as [`Self::pull_through_range`]).
    ///
    /// Unlike [`Self::populate_local`] the size probe carries no `local_only`
    /// filter, so it relies on the node→node `Peer` origin overriding neither
    /// [`Origin::size`] nor [`Origin::fetch_outboard`] (both default to
    /// `Ok(None)` / `Unsupported`). That is what keeps a paid peer from
    /// winning the outboard or setting the `total_bytes` the serving node
    /// signs into its `StreamResponse`. Give `NodeOrigin` either impl and this
    /// path needs an explicit local-only filter first.
    ///
    /// # Errors
    ///
    /// [`CacheError::NoOrigin`] is folded into `Ok(None)` (no origins
    /// configured means nothing to stream — not an error on this
    /// best-effort path). A genuine transport fault surfaced by
    /// [`Self::origin_size`] or the winning origin's [`Origin::fetch`] call
    /// propagates as `Err`.
    pub async fn open_local_outboard_pull(
        &self,
        hash: Hash,
    ) -> anyhow::Result<Option<(LocalOutboardHeader, LocalOutboardPull)>> {
        let total_bytes = match self.origin_size(hash).await {
            Ok(Some(n)) => n,
            Ok(None) | Err(CacheError::NoOrigin { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let outboard_max = expected_outboard_len(total_bytes).saturating_add(64);
        let mut winner: Option<(Arc<dyn Origin>, Bytes)> = None;
        for origin in &self.inner.origins {
            match origin.fetch_outboard(hash, outboard_max).await {
                Ok(OutboardFetch::Found(ob)) => {
                    winner = Some((Arc::clone(origin), ob));
                    break;
                }
                Ok(OutboardFetch::NotFound | OutboardFetch::Unsupported) => {}
                Err(e) => {
                    tracing::debug!(
                        %hash,
                        kind = ?origin.kind(),
                        error = %e,
                        "origin outboard fetch failed; trying next origin",
                    );
                }
            }
        }
        let Some((origin, outboard)) = winner else {
            return Ok(None);
        };

        let max_bytes = self.inner.max_blob_bytes;
        let (stream, _size_hint) = match origin
            .fetch(hash, max_bytes)
            .await
            .map_err(crate::error::OriginPullError::into_inner)?
        {
            OriginFetch::Found { stream, size_hint } => (stream, size_hint),
            // Outboard published but the origin no longer has the data
            // itself — degrade to a whole-blob populate, which will
            // re-surface a persistent absence at its proper severity.
            OriginFetch::NotFound => return Ok(None),
        };

        let pull = LocalOutboardPull::spawn(hash, total_bytes, outboard, stream);
        Ok(Some((LocalOutboardHeader { total_bytes }, pull)))
    }

    /// Begin a node-driven *tee* fill of `hash` (#856): the caller pushes the bao
    /// verified-stream it forwards from an upstream node→node pull via
    /// [`TeeSink::write`] (the header-less interleaved bao — ADR 038; the content
    /// size goes to [`TeeReservation::begin`], not in band), while the engine
    /// decodes + verifies it against
    /// the content root and streams the plaintext into the store; [`TeeSink::finish`]
    /// promotes the blob if every chunk group verified. This lets the
    /// node's `cdn/client/v1` handler fuse the upstream pull with downstream
    /// delivery — forwarding each chunk to the paying client as it arrives —
    /// rather than buffering the whole blob via [`Self::populate`] before serving
    /// (the prepay-the-whole-blob exposure of #856).
    ///
    /// Coalescing (#305): the tee participates in the SAME in-flight map as
    /// [`Self::populate`] / [`Self::get`]. If another fill for `hash` is already
    /// in progress this returns [`TeeOpen::InFlight`] and the caller MUST NOT open
    /// a second upstream pull (no double spend) — it can instead wait on the
    /// existing fill via `populate` and serve from the store. The returned
    /// [`TeeSink`] holds the in-flight claim for `hash`; dropping it (via
    /// `finish` / `abandon`, or an early return on the error path) releases the
    /// claim and wakes waiters.
    ///
    /// Unlike `populate`, this does NOT itself check `is_evicted` or `has`: the
    /// `cdn/client/v1` miss path that calls it has already gated on both. The
    /// caller owns the upstream pull, so the engine stays payment-agnostic.
    #[must_use]
    pub fn open_tee_sink(&self, hash: Hash) -> TeeOpen {
        let mut guard = self.inner.lock_inflight();
        if guard.contains_key(&hash) {
            return TeeOpen::InFlight;
        }
        let notify = Arc::new(Notify::new());
        guard.insert(hash, Arc::clone(&notify));
        drop(guard);

        // The in-flight claim is taken now (coalescing #305), but the verifying
        // decoder can only be framed once the whole-blob size is known — the
        // caller learns it from the upstream's signed `total_bytes` AFTER this
        // point. So opening yields a [`TeeReservation`] holding the claim; the
        // caller calls [`TeeReservation::begin`] with the content size to spawn
        // the import task and get the writable [`TeeSink`]. Dropping the
        // reservation (an upstream that failed before the size was known)
        // releases the claim.
        TeeOpen::Owner(TeeReservation {
            engine: self.clone(),
            hash,
            notify,
            active: true,
        })
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

    /// Walk every committed blob in the local iroh-blobs store and return its
    /// hash, excluding operator-evicted blobs ([ADR 011](../../../adr/011-content-takedown.md))
    /// and partial-import bytes (`BlobStatus != Complete`). Consumed by the
    /// DHT republish scheduler at startup (ADR 022 §Bootstrap AC 16): every
    /// cached blob's first re-publish time is drawn from `uniform(0, 40 min)`
    /// per record, so the bootstrap `Store` rate matches steady-state by
    /// construction. The complementary [`Self::subscribe_inserts`] stream
    /// handles fresh pull-through commits during steady state; the two
    /// together cover every blob the node holds.
    ///
    /// Unlike [`Self::access_times_snapshot`], this accessor reflects on-disk
    /// state and is non-empty on cold start. `access_times` maps `Hash →
    /// Instant` and is initialised empty on every [`Self::open`] — only
    /// hashes touched since process start appear there — so it is the wrong
    /// input for the cold-start seed.
    ///
    /// Returns a [`Vec`] rather than an async stream because the consumer
    /// drains the input strictly into a hash set, so streaming saves nothing
    /// downstream. The transient is small: at C = 100k blobs the allocation
    /// is roughly 100k × 32 bytes = 3.2 MiB.
    ///
    /// Race semantics: a blob evicted between the iroh-blobs `list()`
    /// emission and the per-hash [`Self::is_evicted`] filter would be
    /// included, but downstream publish paths re-check the evicted set, so a
    /// stale entry in the scheduler's heap fails the membership check at
    /// publish time rather than re-advertising a takedown.
    ///
    /// Error semantics: a failure on the iroh-blobs cursor itself
    /// (`list()` or `stream.next()`) aborts the walk — we can't trust
    /// subsequent yields once the cursor is broken. But a per-blob
    /// `status()` failure logs-and-skips so a single inaccessible blob
    /// doesn't deny the seed for every other healthy blob. The
    /// republish surface is best-effort by design; partial completion
    /// strictly beats total failure.
    pub async fn iter_hashes(&self) -> CacheResult<Vec<Hash>> {
        let blobs = self.inner.store.blobs();
        let mut stream = blobs
            .list()
            .stream()
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        let mut out = Vec::new();
        while let Some(hash) = stream.next().await {
            let hash = hash.map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
            if self.refuses(hash) {
                continue;
            }
            let status = match blobs.status(hash).await {
                Ok(s) => s,
                Err(err) => {
                    tracing::warn!(
                        hash = %hash,
                        error = %err,
                        "iter_hashes: blob status() failed; skipping (cold-start seed continues with remaining blobs)"
                    );
                    continue;
                }
            };
            if matches!(status, iroh_blobs::api::blobs::BlobStatus::Complete { .. }) {
                out.push(hash);
            }
        }
        Ok(out)
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
        // Hoisted once, like `pinned`: a deny-listed hash stays an eviction
        // candidate even when pinned, so "deny wins over pin" holds on the space
        // path too, not only the takedown `evict()` actuator — otherwise a pinned
        // + governance-denied hash would sit on disk (unservable, since `refuses`
        // blocks it) until the watcher's `evict()` happened to run. These are the
        // lock-free deny halves of `refuses`; `is_evicted` is deliberately NOT
        // consulted — it would take a second mutex under the `access_times` guard
        // below for nothing, since `evict` removes the `access_times` entry, so an
        // already-evicted hash is never in this map to begin with.
        let denied = self.inner.denied.load();
        let chain_denied = self.inner.chain_denied.load();
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
                // `held` short-circuits BEFORE the deny carve-out, so a probe-held
                // hash stays excluded even when denied: a probe-hold is transient
                // (seconds, self-expiring) and the takedown `evict()` is the
                // reclaim actuator for a denied hash regardless, so the space path
                // need not race the hold. The carve-out targets the *pin* (the
                // hold-forever case), which is the actual "worst combination".
                let deny_listed = denied.contains(h) || chain_denied.contains(h);
                if held.contains(h) || (pinned.contains(h) && !deny_listed) {
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

    /// Snapshot every on-disk blob keyed by hash with its byte size
    /// (`Complete` and `Partial` alike), the public form of the internal
    /// `snapshot_blob_sizes` helper. This is the authoritative disk-usage
    /// input for the capacity-eviction driver (#1173): unlike
    /// [`Self::eviction_candidates`] — which sees only hashes *touched since
    /// process start* — this walks the store, so a node that boots with a
    /// disk already full measures the real usage rather than an empty
    /// access map.
    ///
    /// Cost scales with the total blob set (one `status()` per blob); call
    /// it on the eviction sweep cadence, not per request.
    pub async fn size_snapshot(&self) -> CacheResult<HashMap<Hash, u64>> {
        snapshot_blob_sizes(&self.inner.store).await
    }

    /// Total on-disk bytes across all blobs, the sum of
    /// [`Self::size_snapshot`]. Saturating so an implausibly large store can
    /// never wrap. Drives the eviction driver's high-water comparison and the
    /// current-size cache-health reporting (#1173).
    pub async fn total_bytes(&self) -> CacheResult<u64> {
        Ok(self
            .size_snapshot()
            .await?
            .values()
            .fold(0u64, |acc, sz| acc.saturating_add(*sz)))
    }

    /// Release `hash` for capacity eviction: drop its protecting named tag(s)
    /// so the bytes become eligible for the next iroh-blobs GC sweep, and
    /// forget its [`Self::access_times_snapshot`] entry so a later LRU sweep
    /// doesn't re-surface it. Returns the number of tags dropped.
    ///
    /// This is the LRU-driver counterpart to [`Self::evict`] and is
    /// deliberately *not* the same path (#1173). `evict` is the permanent,
    /// `fsync`'d, `evicted.log`-backed DMCA takedown: it records the hash in a
    /// durable logical-evicted set that blocks serving forever and survives
    /// restart. Capacity eviction must not do that — a blob dropped only for
    /// space pressure has to be freely re-pullable, and an unbounded takedown
    /// log per LRU eviction would be a durability leak. So this method only
    /// releases GC protection; the blob keeps serving until GC actually
    /// reclaims it, after which [`Self::has`] reports it absent naturally
    /// (the bytes are gone from the store, not logically masked).
    ///
    /// Pinned hashes are refused (returns `Ok(0)` without touching state) as a
    /// defense in depth — [`Self::eviction_candidates`] already excludes them,
    /// but the caller is a background loop and the pinned set can change
    /// between candidate selection and this call.
    ///
    /// The pin exemption is itself carved out for a **deny-listed** hash
    /// ([`Self::refuses`]): "deny wins over pin" on every path, so a pinned +
    /// governance/local-denied hash is still reclaimable here rather than being
    /// held on disk (unservable) until the takedown `evict()` runs. This only
    /// drops GC protection — the durable takedown record stays `evict`'s job.
    ///
    /// Best-effort against the GC cadence: like `evict`, actual disk reclaim
    /// only happens when periodic GC is enabled (`cache.gc_interval_sec > 0`).
    pub async fn release_for_eviction(&self, hash: Hash) -> CacheResult<u64> {
        if self.is_pinned(hash) && !self.refuses(hash) {
            return Ok(0);
        }
        // Drop the protecting tag(s) FIRST, and only forget the access-time
        // entry once that succeeded. The reverse order looks tempting (it would
        // stop a concurrent `eviction_candidates` re-picking the hash during the
        // slower tag walk) but it loses the blob on failure: `eviction_candidates`
        // iterates `access_times`, so a hash removed from it before an erroring
        // tag walk can never be re-selected, leaving bytes on disk *and*
        // GC-protected forever. A transient double-selection is harmless by
        // comparison — the second release is a no-op `Ok(0)`.
        let deleted = match self.drop_named_tags_for(hash).await {
            Ok(deleted) => deleted,
            Err(err) => {
                // Same observability as `evict`'s tag-drop failure: without this
                // the LRU path could burn down the candidate pool silently.
                if let Some(m) = &self.inner.metrics {
                    m.tag_drop_failures.inc();
                }
                return Err(err);
            }
        };
        self.inner
            .access_times
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&hash);
        Ok(deleted)
    }

    async fn read_local(&self, hash: Hash) -> CacheResult<Bytes> {
        self.inner
            .store
            .blobs()
            .get_bytes(hash)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))
    }

    /// Read `[byte_offset, byte_offset + byte_len)` of `hash` from the local
    /// store ([ADR 038 §Serve side](../../../adr/038-bao-verified-range-streaming.md)).
    /// `byte_len == 0` reads to the blob end. Works against a **partial** blob
    /// imported by [`Self::pull_through_range`] — only the bytes covered by an
    /// imported (and thus already-verified) range are readable; asking for
    /// bytes outside the imported span surfaces a [`CacheError::Store`] from
    /// iroh-blobs rather than zero-filling.
    ///
    /// Concatenates the exported range into a contiguous [`Bytes`]; for the
    /// streaming serve path the node drives iroh-blobs' `export_ranges`
    /// directly. Provided here so the engine owns the local-read seam for both
    /// whole-blob ([`Self::get`]) and range serves.
    ///
    /// # Errors
    ///
    /// [`CacheError::Store`] if the store cannot satisfy the range (blob
    /// absent, the requested bytes were never imported / verified, or a
    /// read-to-end (`byte_len == 0`) was requested against a partial blob whose
    /// size the store cannot yet report).
    pub async fn export_range(
        &self,
        hash: Hash,
        byte_offset: u64,
        byte_len: u64,
    ) -> CacheResult<Bytes> {
        // `byte_len == 0` means "to end"; iroh-blobs `export_ranges` takes a
        // half-open `Range<u64>`, so resolve the end against the store's known
        // size. A `RangeFull`-style read isn't directly expressible, so query
        // the size and build the explicit bound.
        let end = if byte_len == 0 {
            match self
                .inner
                .store
                .blobs()
                .status(hash)
                .await
                .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?
            {
                iroh_blobs::api::blobs::BlobStatus::NotFound => {
                    return Err(CacheError::Store(anyhow::anyhow!(
                        "export_range: blob {hash} not present"
                    )));
                }
                // A partial blob whose size the store cannot yet report is an
                // explicit failure: silently treating `None` as "ends at
                // `byte_offset`" would return an empty range for a `byte_len ==
                // 0` ("to end") read and mask the real cause. Surface it so the
                // caller retries or falls back to a whole-blob serve.
                iroh_blobs::api::blobs::BlobStatus::Partial { size: None } => {
                    return Err(CacheError::Store(anyhow::anyhow!(
                        "export_range: store cannot report size for partial blob {hash}; \
                         cannot resolve a read-to-end bound"
                    )));
                }
                iroh_blobs::api::blobs::BlobStatus::Partial { size: Some(size) }
                | iroh_blobs::api::blobs::BlobStatus::Complete { size } => size,
            }
        } else {
            byte_offset.saturating_add(byte_len)
        };
        let bytes = self
            .inner
            .store
            .blobs()
            .export_ranges(hash, byte_offset..end)
            .concatenate()
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        Ok(Bytes::from(bytes))
    }

    /// Whole-blob-in-memory form of [`Self::export_bao_range_stream`]: drains the
    /// stream into one contiguous `Bytes`. See that method for the wire format and
    /// the range semantics.
    ///
    /// **Peak memory is the whole aligned range.** The paid serve path must NOT
    /// use this — it drives the stream directly, so a large blob costs one frame
    /// of RAM rather than a copy of the blob (#1132). Every caller today is a test
    /// that needs a buffer to assert against; it is kept for them, and because the
    /// ADR 038 round-trip tests are more legible over a buffer than a stream.
    ///
    /// # Errors
    ///
    /// Everything [`Self::export_bao_range_stream`] can fail with, except that a
    /// fault discovered while exporting — including the truncation refusal —
    /// surfaces here as an `Err` return rather than as a terminal stream item,
    /// because nothing has been handed to a consumer yet.
    pub async fn export_bao_range(
        &self,
        hash: Hash,
        byte_offset: u64,
        byte_len: u64,
        blob_size: u64,
    ) -> CacheResult<Bytes> {
        let mut stream = self
            .export_bao_range_stream(hash, byte_offset, byte_len, blob_size)
            .await?;
        // Pre-size to the exact wire length (proof + data) so the buffer never
        // reallocates; `wire_len` walks the same node set the export stream emits.
        // The range re-validated cleanly inside the call above, so a re-alignment
        // fault here is unreachable — degrade to an unsized buffer rather than
        // duplicating the error mapping.
        let cap = align_range(byte_offset, byte_len, blob_size)
            .map_or(0, |a| usize::try_from(a.wire_len()).unwrap_or(0));
        let mut out = Vec::with_capacity(cap);
        while let Some(item) = stream.next().await {
            out.extend_from_slice(&item?);
        }
        Ok(Bytes::from(out))
    }

    /// Export `[byte_offset, byte_offset + byte_len)` of `hash` as the
    /// **header-less bao interleaved verified-stream encoding** that travels on
    /// `cdn/client/v1` ([ADR 038 §Serve side](../../../adr/038-bao-verified-range-streaming.md)),
    /// yielded incrementally as the store produces it. `byte_len == 0` exports to
    /// the blob end. This is the *only* client-facing delivery path — there is no
    /// raw-byte fallback (ADR 038 AC#4) — so a whole-blob serve passes
    /// `byte_offset == 0, byte_len == 0`.
    ///
    /// The bytes are proof nodes (64 B each) interleaved with chunk-group data in
    /// tree order, **without** the 8-byte size header: the signed
    /// `StreamResponse.body.total_bytes` is the authoritative size, so the
    /// receiver builds its own `BaoTree` and never reads a header off the wire
    /// (avoids a second, unauthenticated size source — ADR 038 §Wire format).
    ///
    /// The range widens to enclosing 16 KiB chunk-group boundaries
    /// ([`align_range`]) because a bao proof anchors whole groups; the serve side
    /// does **not** trim back to the requested offset (trimming would break
    /// verification). The receiver discards the group-aligned prefix. The outboard
    /// is read from the store (built at import); no held content is re-hashed.
    /// Works against a **partial** blob (only imported/verified chunk groups are
    /// exportable, exactly like [`Self::export_range`]).
    ///
    /// Each item is one export item's serialization — a 64-byte proof pair or one
    /// chunk group's data — so a consumer that writes items straight to the wire
    /// holds O(chunk group) rather than O(blob). This is what the paid serve path
    /// drives (#1132); a 708 MB blob previously cost ~708 MB resident per
    /// concurrent serve.
    ///
    /// # Truncation is reported mid-stream
    ///
    /// The `!done` refusal below (the store's item channel closing without a
    /// terminal `Done`/`Error` — an actor crash or shutdown race) can only be
    /// detected at the END of the stream, by which point a streaming consumer has
    /// already put earlier bytes on the wire. It therefore surfaces as a terminal
    /// `Err` **item** rather than as a pre-flight error, and the consumer must
    /// abort the delivery on it. The billing invariant is unchanged: the client
    /// sees a short delivery, rejects it, and never pays the closing voucher — but
    /// the detection point moved from before the first byte to after the last
    /// (#915 review, #1132).
    ///
    /// # Errors
    ///
    /// [`CacheError::Store`] if the requested range is out of bounds, or (for the
    /// 0-byte case) the blob is absent. Faults discovered while exporting —
    /// including the truncation refusal — arrive as `Err` items in the stream.
    pub async fn export_bao_range_stream(
        &self,
        hash: Hash,
        byte_offset: u64,
        byte_len: u64,
        blob_size: u64,
    ) -> CacheResult<Pin<Box<dyn futures_util::Stream<Item = CacheResult<Bytes>> + Send>>> {
        // `blob_size` is the authoritative whole-blob size supplied by the caller
        // (the signed `total_bytes`), NOT resolved from the store. An origin-tier
        // range pull (#823) imports a *partial* blob whose `status()` size is
        // `None`, so re-deriving the size here would fail the very ranged serve the
        // partial import enabled. The BaoTree geometry (and hence the proof) is a
        // function of the whole-blob size, so the partial and the caller agree by
        // construction (#915, ADR 038).

        // Snap to chunk-group boundaries (ADR 005: reject, never clamp, an
        // out-of-bounds range). `align_range` owns the bound check.
        let aligned = align_range(byte_offset, byte_len, blob_size).map_err(|e| {
            CacheError::Store(anyhow::Error::from(e).context("export_bao_range: range alignment"))
        })?;

        // A 0-byte blob (#1054) has no chunk groups and no proof: the header-less
        // wire form is empty. Return an empty stream directly rather than driving an
        // empty `export_bao` stream, whose terminal `Done` we would otherwise depend
        // on to clear the `!done` guard. Still confirm presence first: the documented
        // contract errors on an absent blob, the non-empty path below faults on
        // `export_bao` for a missing hash, and `has` honors a logical eviction
        // (#279) — so a present-only early return keeps behavior consistent and
        // never serves an empty body for a hash this node has taken down.
        //
        // An empty stream (rather than a single empty item) is what keeps the serve
        // path's "the empty blob goes straight to `StreamEnd`" property: `ChunkData`
        // cannot hold an empty payload (#1088), so a zero-length item would be
        // unsendable.
        if blob_size == 0 {
            if !self.has(hash).await? {
                return Err(CacheError::Store(anyhow::anyhow!(
                    "export_bao_range: blob {hash} not present"
                )));
            }
            return Ok(Box::pin(futures_util::stream::empty()));
        }

        let stream = self
            .inner
            .store
            .blobs()
            .export_bao(hash, aligned.chunk_ranges().clone())
            .stream();
        // Serialize the header-less wire form: Parent → 64 bytes (left‖right
        // hashes), Leaf → its data; skip the `Size` item (the header) and stop on
        // `Done`. Mirrors iroh-blobs' `ExportBaoProgress::write` minus the header.
        //
        // The `bool` in the unfold state is "this stream is finished" — set after
        // yielding a terminal error so the consumer cannot poll past it into a
        // second, spurious truncation error.
        Ok(Box::pin(futures_util::stream::unfold(
            (stream, false),
            move |(mut stream, finished)| async move {
                if finished {
                    return None;
                }
                loop {
                    match stream.next().await {
                        Some(EncodedItem::Size(_)) => {}
                        Some(EncodedItem::Parent(parent)) => {
                            let mut frame = BytesMut::with_capacity(64);
                            frame.extend_from_slice(parent.pair.0.as_bytes());
                            frame.extend_from_slice(parent.pair.1.as_bytes());
                            return Some((Ok(frame.freeze()), (stream, false)));
                        }
                        Some(EncodedItem::Leaf(leaf)) => {
                            return Some((Ok(leaf.data), (stream, false)));
                        }
                        Some(EncodedItem::Done) => return None,
                        Some(EncodedItem::Error(cause)) => {
                            let err = CacheError::Store(
                                anyhow::Error::from(cause).context("export_bao stream failed"),
                            );
                            return Some((Err(err), (stream, true)));
                        }
                        // The store's item channel closing without a terminal
                        // `Done`/`Error` (actor crash / shutdown race) would
                        // otherwise yield a silently TRUNCATED wire that the serve
                        // path bills the client for and the client rejects as a
                        // short delivery — with no server-side signal. Refuse
                        // instead (#915 review).
                        None => {
                            let err = CacheError::Store(anyhow::anyhow!(
                                "export_bao stream for {hash} ended without Done; \
                                 refusing truncated export"
                            ));
                            return Some((Err(err), (stream, true)));
                        }
                    }
                }
            },
        )))
    }

    /// Whether the origin chain has any origin `pull_through` would actually try
    /// for the given mode: any origin at all normally, or any non-`Peer` origin
    /// under `local_only` (#1116).
    fn has_eligible_origin(&self, local_only: bool) -> bool {
        self.inner
            .origins
            .iter()
            .any(|o| !local_only || o.kind() != OriginKind::Peer)
    }

    /// [`Self::pull_through`] for a caller that needs the bytes ([`Self::get`]).
    ///
    /// Exists so no caller has to name a [`FillMode`] or unwrap the `Option`: the
    /// mode↔shape correspondence is resolved here, once. A `None` under
    /// [`FillMode::ReturnBytes`] is a logic regression, not a runtime condition —
    /// surface it as a `Store` fault, since the anti-panic policy rules out an
    /// `expect` and a silent empty `Bytes` would be worse (the caller would serve
    /// a zero-length blob).
    async fn pull_through_bytes(&self, hash: Hash) -> CacheResult<Bytes> {
        match self
            .pull_through(hash, false, FillMode::ReturnBytes)
            .await?
        {
            Some(bytes) => Ok(bytes),
            None => Err(CacheError::Store(anyhow::anyhow!(
                "pull_through returned no bytes for {hash} under FillMode::ReturnBytes"
            ))),
        }
    }

    /// [`Self::pull_through`] for a caller that only wants the blob present
    /// ([`Self::populate`] / [`Self::populate_local`]).
    ///
    /// The `()` return is the point: under [`FillMode::CommitOnly`] there is no
    /// payload to hand back, and a caller that cannot receive one cannot
    /// accidentally keep a whole blob alive (#1132).
    async fn pull_through_fill(&self, hash: Hash, local_only: bool) -> CacheResult<()> {
        self.pull_through(hash, local_only, FillMode::CommitOnly)
            .await
            .map(drop)
    }

    /// Walk the origin chain to fill `hash`.
    ///
    /// The mode↔return-shape correspondence is exact in both directions:
    /// [`FillMode::ReturnBytes`] always yields `Some` on success and
    /// [`FillMode::CommitOnly`] always yields `None`. Prefer the
    /// [`Self::pull_through_bytes`] / [`Self::pull_through_fill`] wrappers, which
    /// are total and hide the `Option` entirely.
    #[allow(clippy::too_many_lines)] // One linear chain walk; each outcome arm carries the rationale for its own fallback/return decision, and splitting the match out would separate those from the loop state (`last_err`, `any_not_found`, `any_short_circuit`) they exist to explain.
    async fn pull_through(
        &self,
        hash: Hash,
        local_only: bool,
        mode: FillMode,
    ) -> CacheResult<Option<Bytes>> {
        // Every pull_through entry is a `get()` cache miss, regardless
        // of how the pull resolves. Coalesced waiters that find a hit
        // on retry never call `pull_through`, so they never reach this
        // bump (their `hits` increment lives in the waiter branch of
        // `get`).
        if let Some(m) = &self.inner.metrics {
            m.misses.inc();
        }
        // Reject pulls with no *eligible* origin early. With `local_only`
        // (#1116) the `Peer` origin (the paid node→node fallback) is skipped, so
        // a chain of nothing but `Peer` origins is a fast `NoOrigin`, like an
        // empty chain — checked before the `origin_fetches` bump so that metric
        // still counts only real local fetch attempts.
        if !self.has_eligible_origin(local_only) {
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
        let mut any_short_circuit = false;
        let total = self.inner.origins.len();
        for (idx, origin) in self.inner.origins.iter().enumerate() {
            // #1116: a `local_only` populate never touches the `Peer` origin
            // (the paid node→node fallback), so an operator serving its OWN
            // configured fs/http/s3 origin fronts no upstream USDC. Skipped
            // before the breaker/retry machinery so it costs nothing.
            if local_only && origin.kind() == OriginKind::Peer {
                continue;
            }
            let origin = Arc::clone(origin);
            // Per-origin circuit-breaker (#963). An OPEN breaker
            // short-circuits this origin *before* the retry/backoff
            // loop runs, so a sustained outage on this backend costs no
            // backoff — the chain just advances to the next origin (or,
            // if every origin is OPEN, surfaces a fast `OriginError`).
            // `breakers` is parallel to `origins` by index; `get` keeps
            // the access non-panicking per the workspace anti-panic
            // policy (a missing slot would be a construction bug, in
            // which case we degrade to "no breaker" rather than panic).
            let breaker = self.inner.breakers.get(idx);
            // Admit (or short-circuit) under the breaker. The `Proceed`
            // arm carries a `TrialGuard` that owns any HALF-OPEN trial
            // slot; it MUST stay alive across the `.await` below so that
            // a cancelled future (client disconnect / timeout) drops it
            // and reclaims the slot rather than leaking it (#963). A
            // `None` breaker (construction degraded to "no breaker")
            // admits unconditionally with no guard.
            let trial_guard = match breaker.map(OriginBreaker::acquire) {
                Some(Admission::ShortCircuit) => {
                    any_short_circuit = true;
                    self.emit_breaker_short_circuit_advance(hash, idx, total, origin.kind());
                    continue;
                }
                Some(Admission::Proceed(_state, guard)) => Some(guard),
                None => None,
            };

            let (outcome, terminal) =
                run_with_retry_classified(policy, self.inner.metrics.as_ref(), hash, || {
                    self.pull_through_attempt(
                        Arc::clone(&origin),
                        hash,
                        max_blob_bytes,
                        policy,
                        mode,
                    )
                })
                .await;
            // Commit the breaker outcome through the guard (defusing its
            // cancellation-release path). A `None` guard is the degraded
            // "no breaker" case and records nothing.
            Self::record_breaker_outcome(trial_guard, terminal);

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
                    return Ok(Some(bytes));
                }
                // Same successful commit, minus the read-back the caller did not
                // want (#1132) — so it must broadcast the insert identically.
                Ok(PullThroughOutcome::Committed) => {
                    let _ = self.inner.inserts_tx.send(hash);
                    return Ok(None);
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
        } else if any_short_circuit {
            // Every origin that wasn't a definitive NotFound was
            // short-circuited by an OPEN breaker (#963). There is no
            // `last_err` to surface (we never ran the retry loop for
            // those origins), but returning `NotFound` would be wrong —
            // the blob may well exist; we just refused to pull it while
            // the origin is shedding load. Surface a fast `OriginError`
            // so the caller sees "origin unavailable" rather than a
            // spurious 404, *without* having incurred any backoff.
            Err(CacheError::OriginError {
                hash,
                source: anyhow::anyhow!(
                    "origin circuit-breaker open: all eligible origins are \
                     fast-failing during a sustained outage (#963)"
                ),
            })
        } else if any_not_found {
            Err(CacheError::NotFound { hash })
        } else {
            // Structurally unreachable: every iteration of the loop
            // above takes exactly one match arm. The five non-`Err`
            // arms all `return`; the `NotFound` arm sets
            // `any_not_found`; the breaker short-circuit sets
            // `any_short_circuit`; the `Err` arm sets `last_err`. To
            // reach this branch the chain must be non-empty (`is_empty()`
            // check at the top of `pull_through`) and have produced
            // no `last_err`, no `any_not_found`, and no
            // `any_short_circuit` — impossible under the current
            // `PullThroughOutcome` taxonomy. Reaching it
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

    /// Commit a per-origin breaker the health verdict for a completed
    /// attempt (#963) by recording it through the admission's
    /// [`crate::circuit_breaker::TrialGuard`].
    ///
    /// A transient that exhausted the retry budget is `Unavailable`
    /// (counts toward the trip threshold); everything else — bytes,
    /// `NotFound`, permanent per-object errors (404/4xx/decode/cap), and
    /// even a `Store`/`HashMismatch`/`BlobTooLarge` (the origin DID
    /// respond) — is `Available` and resets the failure count.
    ///
    /// Recording defuses the guard's cancellation-release path, so the
    /// HALF-OPEN trial slot it may own is resolved exactly once. A `None`
    /// guard is the degraded "no breaker" case (construction produced no
    /// breaker for this index) and records nothing.
    fn record_breaker_outcome(guard: Option<TrialGuard<'_>>, terminal: Option<TerminalFailure>) {
        let Some(guard) = guard else { return };
        let outcome = match terminal {
            Some(TerminalFailure::TransientExhausted) => OriginOutcome::Unavailable,
            Some(TerminalFailure::Permanent) | None => OriginOutcome::Available,
        };
        guard.record(outcome);
    }

    /// Handle a per-origin circuit-breaker short-circuit (#963) that
    /// advances the fallback chain: bump `origin_fallback` and emit the
    /// operator-visible advance breadcrumb, both gated by `idx + 1 <
    /// total` so a short-circuit on the chain-final origin neither
    /// logs nor counts a non-existent advance.
    ///
    /// The `origin_fallback` bump matches the non-breaker advances in
    /// [`Self::emit_chain_advance`] — a short-circuit IS a real
    /// fallback-chain step, so omitting it would make
    /// `origin_fallback_total` undercount. The short-circuit *load-shed*
    /// counter itself is bumped separately inside
    /// [`crate::circuit_breaker::OriginBreaker::acquire`] (so it counts
    /// even on the chain-final origin, where no advance fires).
    fn emit_breaker_short_circuit_advance(
        &self,
        hash: Hash,
        idx: usize,
        total: usize,
        origin_kind: OriginKind,
    ) {
        if idx + 1 >= total {
            return;
        }
        if let Some(m) = &self.inner.metrics {
            m.origin_fallback.inc();
        }
        tracing::warn!(
            hash = %hash,
            origin_index = idx,
            origin_kind = ?origin_kind,
            "advancing to next origin in fallback chain (circuit-breaker open)",
        );
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
    /// retry loop ([`run_with_retry_classified`]) needs a callable that
    /// produces a fresh attempt on each invocation — the side-channel `Arc`s,
    /// `TempTag`s, and origin futures all have to be re-created per
    /// attempt and can't be reused across iterations.
    ///
    /// Under [`FillMode::CommitOnly`] (the `populate` fill path) this returns
    /// [`PullThroughOutcome::Committed`] rather than a payload — the streaming arm
    /// by skipping its `read_local`, the buffered arm by dropping its drain
    /// buffer. See that variant's docs (#1132).
    #[allow(clippy::too_many_lines)] // Linear per-attempt flow; the failure-classification arms each need their own context comment, and splitting them across functions would obscure the sequence more than the length.
    async fn pull_through_attempt(
        &self,
        origin: Arc<dyn Origin>,
        hash: Hash,
        max_blob_bytes: u64,
        policy: RetryPolicy,
        mode: FillMode,
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
            return self.commit_buffered_bytes(hash, bytes, mode).await;
        }

        // Streaming path: drive the origin stream into the store, verify the
        // hash, and promote — shared verbatim with the node-driven tee sink
        // (#856) via `import_and_verify_stream`. On a committed blob the engine's
        // existing `get()` callers (admin RPC, metrics tests) want the full
        // payload as `Bytes`, so re-read it from the local store (one mmap'd read
        // with `fs-store`, no extra origin egress).
        match self
            .import_and_verify_stream(hash, stream, max_blob_bytes)
            .await?
        {
            // The fill-only caller (`populate` / `populate_local`) drops the bytes,
            // so reading them back would re-materialise the whole blob for nothing
            // — the exact allocation the streaming import above just avoided
            // (#1132). Skip it.
            StreamCommitOutcome::Committed if mode == FillMode::CommitOnly => {
                Ok(PullThroughOutcome::Committed)
            }
            StreamCommitOutcome::Committed => match self.read_local(hash).await {
                Ok(bytes) => Ok(PullThroughOutcome::Bytes(bytes)),
                Err(CacheError::Store(err)) => Ok(PullThroughOutcome::Store(err)),
                Err(other) => {
                    // `read_local` only surfaces `Store`; any other variant is a
                    // logic regression. Map to `Store` so the outer
                    // `pull_through` still surfaces a coherent error; the inner
                    // anyhow chain preserves the cause.
                    Ok(PullThroughOutcome::Store(anyhow::Error::msg(format!(
                        "read_local returned unexpected variant after successful commit: {other}"
                    ))))
                }
            },
            StreamCommitOutcome::HashMismatch { actual } => {
                Ok(PullThroughOutcome::HashMismatch { actual })
            }
            // `VerifyFailed` is emitted only by the tee's bao-decoding source
            // (`bao_decoded_source`); the origin pull-through feeds raw bytes, so
            // it can never fire here. Map defensively to `Store` — if it ever does,
            // that is a logic regression worth surfacing, not silent success.
            StreamCommitOutcome::VerifyFailed => Ok(PullThroughOutcome::Store(anyhow::Error::msg(
                "import_and_verify_stream returned VerifyFailed on the non-tee origin path",
            ))),
            StreamCommitOutcome::BlobTooLarge => Ok(PullThroughOutcome::BlobTooLarge),
            StreamCommitOutcome::Store(err) => Ok(PullThroughOutcome::Store(err)),
        }
    }

    /// Drive a chunk stream into the iroh-blobs store, capturing mid-stream
    /// errors via the side channel, verify the committed hash against `hash`,
    /// and on match promote the temp tag to a named tag. This is the shared
    /// commit-and-verify tail used by both the origin pull-through
    /// ([`Self::pull_through_attempt`]) and the node-driven tee sink
    /// ([`Self::open_tee_sink`], #856) — the only difference between the two is
    /// the source of `stream` (an `Origin::fetch` stream vs. a caller-fed
    /// channel). It does NOT broadcast the insert or read the bytes back; the
    /// caller owns those (the tee broadcasts on `finish`, the pull-through
    /// re-reads for its `Bytes` return).
    ///
    /// `count_and_cap_stream` enforces `max_blob_bytes` and bumps the
    /// origin-egress metric per chunk, so a tee fill is metered as the upstream
    /// egress it genuinely is (those bytes left an origin) without touching the
    /// `get`-caller hit/returned counters. On the tee path `stream` is the
    /// DECODED plaintext (`bao_decoded_source` strips the interleaved proof), so
    /// `pull_through_bytes` here meters content bytes — slightly under the bao
    /// wire that actually left the upstream (the proof overhead the buyer paid for
    /// is counted on the receive side, not here).
    async fn import_and_verify_stream<S>(
        &self,
        hash: Hash,
        stream: S,
        max_blob_bytes: u64,
    ) -> Result<StreamCommitOutcome, OriginPullError>
    where
        S: futures_util::Stream<Item = std::io::Result<Bytes>> + Send + Sync + Unpin + 'static,
    {
        // `iroh-blobs::add_stream` swallows the upstream `io::Error` (it
        // `?`-propagates inside an async block whose error is discarded), so
        // without this side-channel capture the engine sees only "unexpected end
        // of stream" and operators lose the actionable upstream message. Failed
        // attempts strand a partial `TempTag` worth of bytes; iroh-blobs GC
        // reclaims them at `cache.gc_interval_sec` cadence.
        let captured_err: Arc<Mutex<Option<std::io::Error>>> = Arc::new(Mutex::new(None));
        let counted = count_and_cap_stream(
            stream,
            max_blob_bytes,
            self.inner.metrics.clone(),
            captured_err.clone(),
        );
        let progress = self.inner.store.blobs().add_stream(counted).await;
        let temp_tag_result = progress.temp_tag().await;

        // Side-channel-recorded error wins over both the iroh-blobs Err arm AND a
        // "successful" partial import, because the latter's hash is
        // deterministically wrong and we'd rather surface the real cause than a
        // confusing `HashMismatch`. Drop the temp tag (regardless of inner
        // Ok/Err) so iroh-blobs GC reclaims the partial bytes.
        let captured = captured_err
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(upstream) = captured {
            drop(temp_tag_result);
            // Cap-breach mid-stream: typed `BlobTooLargeMarker` is the documented
            // escape hatch — surface as the typed `BlobTooLarge` outcome rather
            // than routing through `classify_io_error` which would collapse it to
            // a generic `OriginError`.
            if is_blob_too_large_marker(&upstream) {
                return Ok(StreamCommitOutcome::BlobTooLarge);
            }
            // A bao verify failure (#915): the teed verified-stream did not check
            // out against the content root — a corrupt/lying upstream. Surface it
            // as the dedicated verify-failure outcome so the window serve path
            // scores it as an upstream-verify failure rather than a generic
            // transport error. The failure is at an interior chunk group, so there
            // is no meaningful whole-blob "actual" hash to report.
            if is_bao_verify_marker(&upstream) {
                return Ok(StreamCommitOutcome::VerifyFailed);
            }
            // Otherwise classify via the shared body-phase classifier: typed
            // `OriginError::*` inners surface as Permanent (decompression
            // failures, etc.); `io::ErrorKind`-Transient kinds (ConnectionReset,
            // TimedOut, …) surface as Transient and re-enter the retry loop.
            return Err(classify_io_error(upstream));
        }

        let temp_tag = match temp_tag_result {
            Ok(tt) => tt,
            Err(err) => {
                // No upstream-captured error: the failure is on iroh-blobs' side
                // (disk write, actor crash, serialization-task panic, etc.).
                // Surface as `Store` so operators routing on origin-vs-store
                // don't misclassify a local store problem as a remote origin one.
                return Ok(StreamCommitOutcome::Store(
                    anyhow::Error::from(err)
                        .context("iroh-blobs add_stream failed during pull-through"),
                ));
            }
        };

        let actual = temp_tag.hash();
        if actual != hash {
            // Drop the temp tag without promotion → the wrong-hash bytes are
            // never tagged, so they are GC-eligible inside iroh-blobs and the
            // next sweep reclaims them. Deterministic protocol violation: a clean
            // stream that hashed wrong is not a transport failure — retry won't
            // help.
            //
            // We deliberately do NOT logically evict `actual`. The persisted
            // evicted set is reserved for operator DMCA/corruption takedowns
            // (#279); reusing it here would durably censor `actual` — and since
            // content is BLAKE3-addressed, bytes whose hash is `actual` *are* the
            // authorized content for `actual`. A malicious upstream that answers
            // a pull for `hash` with a victim blob's bytes could otherwise make
            // us permanently blacklist that legitimate blob (#853). The requested
            // hash `hash` is correctly never committed.
            drop(temp_tag);
            return Ok(StreamCommitOutcome::HashMismatch { actual });
        }

        // Promote the temp tag to a named tag — same effect as
        // `add_bytes(...).await`, which goes through `with_tag()` (iroh-blobs
        // `blobs.rs:624-632`). The name is opaque; the store auto-assigns it.
        // Tag-create failure is store-side, not origin-side: surface as `Store`
        // so retry-class taxonomy doesn't pick it up.
        let haf = temp_tag.hash_and_format();
        if let Err(err) = self.inner.store.tags().create(haf).await {
            return Ok(StreamCommitOutcome::Store(anyhow::Error::from(err)));
        }
        drop(temp_tag);
        Ok(StreamCommitOutcome::Committed)
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
        mode: FillMode,
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
            // Unlike the streaming path (which drops an unpromoted `TempTag`),
            // `add_bytes` already created a *persistent named tag* protecting
            // these wrong-hash bytes from GC. Delete it so the bytes become
            // GC-eligible — matching the streaming path's drop semantics and
            // avoiding the unbounded-disk-growth leak (#837). Best-effort: a
            // delete failure only delays reclaim, so it is logged, not
            // propagated over the `HashMismatch` we owe the caller.
            //
            // As on the streaming path we do NOT logically evict `actual`:
            // the persisted evicted set is for deliberate takedowns only, and
            // logically evicting a content-addressed hash here is the
            // durable-censorship vector in #853.
            if let Err(err) = self.inner.store.tags().delete(tag.name).await {
                if let Some(m) = &self.inner.metrics {
                    m.tag_drop_failures.inc();
                }
                tracing::warn!(
                    expected = %hash,
                    %actual,
                    %err,
                    "hash-mismatch tag delete failed (drain path); wrong-hash bytes stay GC-protected (not auto-retried)",
                );
            }
            return Ok(PullThroughOutcome::HashMismatch { actual });
        }
        match mode {
            FillMode::ReturnBytes => Ok(PullThroughOutcome::Bytes(bytes)),
            // The drain already holds these bytes, so unlike the streaming path
            // there is nothing to *avoid* re-reading here — dropping them is not a
            // memory win. It is a CONTRACT win: it makes "CommitOnly ⟺ None" true
            // in both directions, so the wrappers are total and a reader of
            // `FillMode::CommitOnly` can trust it end to end rather than
            // discovering this arm as an exception.
            FillMode::CommitOnly => {
                drop(bytes);
                Ok(PullThroughOutcome::Committed)
            }
        }
    }
}

/// Whether a pull-through hands the committed blob back to its caller.
///
/// Replaces a `want_bytes: bool` that sat directly beside `local_only: bool` in
/// `pull_through`'s argument list — two adjacent booleans the compiler would let
/// you swap silently, in a chain where getting it wrong either reinstates the
/// #1132 whole-blob read-back or stops `get` consulting peer origins. The
/// `local_only` half would at least be caught — `populate_local_skips_peer_origin_and_fills_from_local`
/// asserts `peer_fetches == 0`. The read-back half is the silent one, and is what
/// the enum is really buying.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FillMode {
    /// [`CacheEngine::get`] — the caller needs the payload. The streaming arm
    /// reads the committed blob back out of the store to produce it; the buffered
    /// arm already holds it and returns its drain buffer.
    ReturnBytes,
    /// [`CacheEngine::populate`] — the caller drops the payload, so it is never
    /// read back (#1132). Serving a 708 MB blob used to cost that much again on
    /// the miss leg for a buffer nobody looked at.
    CommitOnly,
}

/// Per-attempt outcomes that ride out of the retry loop without
/// classification: each is a deterministic, non-retry-class result
/// the engine maps directly to a `CacheError` variant.
#[derive(Debug)]
enum PullThroughOutcome {
    Bytes(Bytes),
    /// The blob committed to the local store, but the caller asked not to have it
    /// read back ([`FillMode::CommitOnly`] — the fill-only path, #1132). Carrying no
    /// payload is the entire point: re-reading a 708 MB blob to hand it to
    /// [`CacheEngine::populate`], which drops it, was a whole-blob allocation on
    /// the serve path's miss leg.
    Committed,
    NotFound,
    BlobTooLarge,
    HashMismatch {
        actual: Hash,
    },
    Store(anyhow::Error),
}

/// Outcome of [`CacheEngine::import_and_verify_stream`] — the commit-and-verify
/// tail shared by origin pull-through and the tee sink (#856). Mirrors
/// [`PullThroughOutcome`] minus the `Bytes`/`NotFound` arms: a stream import
/// either commits, fails bao verification, hashes wrong, overruns the cap, or
/// hits a store fault.
#[derive(Debug)]
enum StreamCommitOutcome {
    Committed,
    BlobTooLarge,
    /// The teed bao verified-stream failed to decode against the content root —
    /// a corrupt/lying upstream (#915). Distinct from [`Self::HashMismatch`]:
    /// the failure is at an interior chunk group, so there is no meaningful
    /// whole-blob "actual" hash to report.
    VerifyFailed,
    HashMismatch {
        actual: Hash,
    },
    Store(anyhow::Error),
}

/// Backpressure bound on the [`TeeSink`] feeder channel: at most this many
/// caller-pushed chunks may be in flight to the store-import task before
/// [`TeeSink::write`] awaits. Small enough to cap resident memory (a handful of
/// `cdn/client/v1` chunks), large enough that the store import and the network
/// forward overlap rather than ping-ponging one chunk at a time.
const TEE_SINK_CHANNEL_CAP: usize = 8;

/// Result of [`CacheEngine::open_tee_sink`] (#856).
#[derive(Debug)]
pub enum TeeOpen {
    /// This caller owns the fill: name the content size via
    /// [`TeeReservation::begin`] to get the writable [`TeeSink`].
    Owner(TeeReservation),
    /// Another task is already filling this hash (coalescing, #305). The caller
    /// MUST NOT open a competing upstream pull — wait on the existing fill via
    /// [`CacheEngine::populate`] / [`CacheEngine::get`] and serve from the store.
    InFlight,
}

/// The in-flight claim for a tee fill, held between [`CacheEngine::open_tee_sink`]
/// (which takes the claim for coalescing #305) and [`Self::begin`] (which frames
/// the verifying decoder once the whole-blob size is known from the upstream's
/// signed `total_bytes`). Dropping it without calling `begin` — an upstream that
/// failed before the size was known — releases the claim and wakes waiters.
#[derive(Debug)]
pub struct TeeReservation {
    engine: CacheEngine,
    hash: Hash,
    notify: Arc<Notify>,
    /// Cleared by [`Self::begin`] so the produced [`TeeSink`] owns the claim and
    /// this reservation's `Drop` becomes a no-op (no double release).
    active: bool,
}

impl TeeReservation {
    /// Frame the verifying decoder with `content_size` (the signed whole-blob
    /// size) and spawn the store-import task, handing back the writable
    /// [`TeeSink`]. The content size is passed here, at open, rather than as an
    /// in-band header — the wire the caller forwards is the header-less bao
    /// interleaved stream (ADR 038).
    #[must_use]
    pub fn begin(mut self, content_size: u64) -> TeeSink {
        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(TEE_SINK_CHANNEL_CAP);
        // Decode + verify the forwarded header-less bao stream into plaintext,
        // which the existing commit path imports — keeping the cap
        // (`count_and_cap_stream`) and the `StreamCommitOutcome` taxonomy intact
        // while verifying the cached copy against the root. The channel closing
        // (all senders dropped) ends the stream — that is how `finish` / `abandon`
        // signal end-of-blob.
        let source: Pin<
            Box<dyn futures_util::Stream<Item = std::io::Result<Bytes>> + Send + Sync>,
        > = Box::pin(bao_decoded_source(self.hash, content_size, rx));
        let engine = self.engine.clone();
        let hash = self.hash;
        let max_blob_bytes = engine.inner.max_blob_bytes;
        let import = tokio::spawn(async move {
            engine
                .import_and_verify_stream(hash, source, max_blob_bytes)
                .await
        });
        // Transfer the in-flight claim to the sink; this reservation's Drop is now
        // a no-op.
        self.active = false;
        TeeSink {
            engine: self.engine.clone(),
            hash: self.hash,
            notify: Arc::clone(&self.notify),
            tx: Some(tx),
            import: Some(import),
        }
    }

    /// Release the claim without ever framing a decoder (e.g. the upstream pull
    /// failed before the size was known). Equivalent to dropping the reservation;
    /// named for symmetry with [`TeeSink::abandon`] at the call sites.
    pub fn abandon(self) {
        drop(self);
    }
}

impl Drop for TeeReservation {
    fn drop(&mut self) {
        // Only release if `begin` never consumed the claim; otherwise the
        // produced `TeeSink` owns it and will release on its own drop.
        if !self.active {
            return;
        }
        self.engine.inner.lock_inflight().remove(&self.hash);
        self.notify.notify_waiters();
    }
}

/// A caller-driven tee fill of one blob into the cache (#856). The owner pushes
/// the header-less bao verified-stream via [`Self::write`] as it arrives from an
/// upstream node→node pull (the content size was named at [`TeeReservation::begin`]
/// — ADR 038); the engine decodes + verifies it against the content root and
/// streams the plaintext into the store concurrently. [`Self::finish`] promotes
/// the blob when every chunk group verified (making it a discoverable holder),
/// and surfaces a [`CacheError::VerifyFailed`] when the forwarded bao failed
/// verification (a corrupt/lying upstream); [`Self::abandon`] drops a partial
/// fill (e.g. the downstream client disconnected). Either way — or on an early
/// drop from any error path — the in-flight claim is released and waiters woken.
#[derive(Debug)]
pub struct TeeSink {
    engine: CacheEngine,
    hash: Hash,
    /// The per-hash in-flight notifier (shared with [`CacheEngine::populate`]
    /// waiters); woken on drop so a coalesced waiter re-checks presence.
    notify: Arc<Notify>,
    /// Feeder into the store-import task. `take`n / set to `None` by
    /// `finish`/`abandon` to end the stream; dropping it closes the channel.
    tx: Option<tokio::sync::mpsc::Sender<Bytes>>,
    /// The spawned store-import task; `take`n by `finish` (awaited) or
    /// `abandon`/`Drop` (aborted).
    import: Option<tokio::task::JoinHandle<Result<StreamCommitOutcome, OriginPullError>>>,
}

impl TeeSink {
    /// Push one chunk into the fill. Awaits if the bounded feeder channel is
    /// full (store backpressure, which also paces the caller's downstream
    /// forward). Errors if the import task has already ended — the caller should
    /// then [`Self::abandon`].
    ///
    /// # Errors
    ///
    /// - [`CacheError::VerifyFailed`] — the import ended because the tee's bao
    ///   decoder REJECTED a chunk group: the bytes being forwarded are corrupt
    ///   (a lying upstream), not a local fault. Callers route this to their
    ///   corruption handling, not their store-fault handling (#915).
    /// - [`CacheError::BlobTooLarge`] / [`CacheError::OriginError`] — the import
    ///   ended on the cap or a captured transport-class fault.
    /// - [`CacheError::Store`] — the sink is already finished, or the import
    ///   task ended/failed for a genuinely local reason.
    pub async fn write(&mut self, chunk: &[u8]) -> CacheResult<()> {
        let Some(tx) = self.tx.as_ref() else {
            return Err(CacheError::Store(anyhow::anyhow!(
                "tee sink write after finish/abandon"
            )));
        };
        if tx.send(Bytes::copy_from_slice(chunk)).await.is_ok() {
            return Ok(());
        }
        // The import task ended before this write landed — its outcome IS the
        // reason the send failed, so surface it instead of a generic "store
        // fault" (#915 review): a bao verify rejection mid-stream must classify
        // as CORRUPTION (`CacheError::VerifyFailed`), not as a failing local
        // disk, or the caller meters/scores the wrong party. After this the
        // sink is spent; the caller should [`Self::abandon`] (a no-op then).
        self.tx = None;
        let Some(import) = self.import.take() else {
            return Err(CacheError::Store(anyhow::anyhow!(
                "tee sink import task ended before write completed"
            )));
        };
        let outcome = import.await.map_err(|e| {
            CacheError::Store(anyhow::anyhow!("tee sink import task join failed: {e}"))
        })?;
        match self.verdict(outcome) {
            // A "clean" early exit with bytes still unwritten cannot be a real
            // commit of the full blob — report the early termination itself.
            Ok(()) => Err(CacheError::Store(anyhow::anyhow!(
                "tee sink import task ended before write completed"
            ))),
            Err(e) => Err(e),
        }
    }

    /// Map a finished import task's outcome to what the caller sees. Shared by
    /// [`Self::finish`] (the normal verdict point) and [`Self::write`]'s
    /// task-ended-early path, so a mid-stream bao rejection surfaces as the same
    /// [`CacheError::VerifyFailed`] from either.
    fn verdict(&self, outcome: Result<StreamCommitOutcome, OriginPullError>) -> CacheResult<()> {
        match outcome {
            Ok(StreamCommitOutcome::Committed) => Ok(()),
            Ok(StreamCommitOutcome::VerifyFailed) => Err(CacheError::VerifyFailed {
                expected: self.hash,
            }),
            Ok(StreamCommitOutcome::HashMismatch { actual }) => Err(CacheError::HashMismatch {
                expected: self.hash,
                actual,
            }),
            Ok(StreamCommitOutcome::BlobTooLarge) => Err(CacheError::BlobTooLarge {
                hash: self.hash,
                limit_bytes: self.engine.inner.max_blob_bytes,
            }),
            Ok(StreamCommitOutcome::Store(err)) => Err(CacheError::Store(err)),
            Err(e) => Err(CacheError::OriginError {
                hash: self.hash,
                source: e.into_inner(),
            }),
        }
    }

    /// Close the stream and finalize: verify the committed hash and, on match,
    /// promote the blob and announce the insert (ADR 022 §STORE Flow), so the
    /// node becomes a discoverable holder for future requests.
    ///
    /// # Errors
    ///
    /// - [`CacheError::VerifyFailed`] — a forwarded chunk group did not verify
    ///   against the content root (corrupt upstream); the blob is NOT promoted.
    /// - [`CacheError::BlobTooLarge`] — the fill exceeded `max_blob_bytes`.
    /// - [`CacheError::Store`] — store-write or import-task-join fault.
    /// - [`CacheError::OriginError`] — a mid-stream transport error was captured.
    pub async fn finish(mut self) -> CacheResult<()> {
        // Drop the sender → the import stream ends → the task finalizes.
        self.tx = None;
        let Some(import) = self.import.take() else {
            return Err(CacheError::Store(anyhow::anyhow!(
                "tee sink finished twice"
            )));
        };
        let outcome = import.await.map_err(|e| {
            CacheError::Store(anyhow::anyhow!("tee sink import task join failed: {e}"))
        })?;
        let result = self.verdict(outcome);
        if result.is_ok() {
            // Announce the fresh commit to DHT-republish subscribers
            // (ADR 022 §STORE Flow), mirroring the origin pull-through path.
            // No active subscriber → `SendError`, the normal state; ignore.
            let _ = self.engine.inner.inserts_tx.send(self.hash);
            self.engine.touch(self.hash);
        }
        result
        // `self` drops here → the in-flight claim is released AFTER the commit,
        // so a coalesced waiter that wakes sees the blob present.
    }

    /// Drop a partial fill without promoting it (e.g. the downstream client
    /// disconnected, so we stop pulling). The partial temp tag is reclaimed by
    /// iroh-blobs GC; the in-flight claim releases on drop.
    pub fn abandon(mut self) {
        self.tx = None;
        if let Some(import) = self.import.take() {
            import.abort();
        }
    }
}

impl Drop for TeeSink {
    fn drop(&mut self) {
        // Release the in-flight claim and wake waiters (#305) on EVERY exit —
        // `finish`, `abandon`, or an early drop on the caller's error path. If
        // the import task is still live (dropped without finish/abandon), abort
        // it so its partial temp tag becomes GC-eligible.
        if let Some(import) = self.import.take() {
            import.abort();
        }
        self.engine.inner.lock_inflight().remove(&self.hash);
        self.notify.notify_waiters();
    }
}

/// Typed marker: the bao verifying decoder driving a [`TeeSink`] fill rejected a
/// chunk group (or the root) — the upstream forwarded bytes that do not verify
/// against the content hash. Carries the decoder's own description of WHICH
/// parent/leaf failed (the first question in a corruption postmortem), mirroring
/// [`BlobTooLargeMarker`]'s payload-carrying shape. Wrapped in the import
/// stream's `io::Error` so [`CacheEngine::import_and_verify_stream`] surfaces a
/// corruption outcome ([`StreamCommitOutcome::VerifyFailed`]) instead of
/// collapsing it into a generic transport [`OriginPullError`] (#915, ADR 038).
/// Only genuine hash mismatches carry this marker — a truncated feed (EOF,
/// `*NotFound`) stays a transport-class error; see [`bao_decoded_source`].
#[derive(Debug)]
struct BaoVerifyMarker {
    /// The failing `DecodeError` rendered (`ParentHashMismatch(node)` /
    /// `LeafHashMismatch(chunk)`).
    detail: String,
}

impl std::fmt::Display for BaoVerifyMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bao verified-stream decode failed (content did not verify against root): {}",
            self.detail
        )
    }
}

impl std::error::Error for BaoVerifyMarker {}

/// True when the import-stream `io::Error` wraps a [`BaoVerifyMarker`] — the teed
/// bao failed verification against the content root (corrupt/lying upstream).
fn is_bao_verify_marker(e: &std::io::Error) -> bool {
    e.get_ref()
        .is_some_and(<dyn std::error::Error + Send + Sync>::is::<BaoVerifyMarker>)
}

/// A [`RecvStream`] backed by the [`TeeSink`] feeder channel. The window
/// pull-through producer writes the header-less bao interleaved stream it
/// forwards from the upstream (the content size is passed at open, not in-band);
/// this adapter hands those bytes to a [`ResponseDecoder`] so the tee verifies +
/// decodes them to plaintext for the store import (#915, ADR 038 §Serve side).
struct ChannelRecvStream {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
    buf: BytesMut,
}

impl ChannelRecvStream {
    fn new(rx: tokio::sync::mpsc::Receiver<Bytes>) -> Self {
        Self {
            rx,
            buf: BytesMut::new(),
        }
    }

    /// Pull from the channel until `buf` holds at least `n` bytes or it closes.
    async fn fill_to(&mut self, n: usize) {
        while self.buf.len() < n {
            match self.rx.recv().await {
                Some(b) => self.buf.extend_from_slice(&b),
                None => break,
            }
        }
    }

    fn eof() -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "tee feeder channel closed before the requested bytes arrived",
        )
    }
}

impl RecvStream for ChannelRecvStream {
    async fn recv_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        if self.buf.is_empty()
            && let Some(b) = self.rx.recv().await
        {
            self.buf.extend_from_slice(&b);
        }
        // A drained-and-closed channel yields a zero-length `Bytes`, which the
        // `bao-tree` reader reads as clean EOF (not an error) — the correct signal
        // for a fill that ended (`finish`/`abandon` dropped all senders). A feed
        // that ends mid-tree surfaces as this same short read to the decoder, which
        // then fails with `ParentNotFound`/`LeafNotFound` — the transport-class
        // `bao stream truncated mid-tree` path in `bao_decoded_source`, distinct
        // from a genuine group hash mismatch.
        let take = self.buf.len().min(len);
        Ok(self.buf.split_to(take).freeze())
    }

    async fn recv_bytes_exact(&mut self, len: usize) -> std::io::Result<Bytes> {
        self.fill_to(len).await;
        if self.buf.len() < len {
            return Err(Self::eof());
        }
        Ok(self.buf.split_to(len).freeze())
    }

    async fn recv_exact(&mut self, target: &mut [u8]) -> std::io::Result<()> {
        self.fill_to(target.len()).await;
        if self.buf.len() < target.len() {
            return Err(Self::eof());
        }
        let head = self.buf.split_to(target.len());
        target.copy_from_slice(&head);
        Ok(())
    }

    // `stop`/`id` are inert by design: this reader is backed by an in-process
    // mpsc channel, not a real QUIC stream. There is no peer to send a STOP_SENDING
    // frame to (the producer ends the fill by dropping its sender), and there is no
    // wire stream id — `0` is a stable placeholder the decoder never keys on.
    fn stop(&mut self, _code: iroh::endpoint::VarInt) -> std::io::Result<()> {
        Ok(())
    }

    fn id(&self) -> u64 {
        0
    }
}

/// Build the plaintext byte stream a [`TeeSink`] imports from a window
/// pull-through fill: drive a bao [`ResponseDecoder`] over the feeder channel,
/// yielding each verified chunk-group's plaintext (proof `Parent` nodes are
/// skipped). `content_size` — the signed whole-blob size passed to
/// [`TeeReservation::begin`] once the upstream header is known — frames the bao
/// tree, so the fed wire is the header-less interleaved stream (no in-band 8-byte
/// size header). The error
/// taxonomy matches the client-side decoder (ADR 038): a genuine group/parent
/// hash mismatch surfaces as an `io::Error` wrapping [`BaoVerifyMarker`] (then
/// the stream ends), so the import reports a CORRUPTION outcome; a truncated
/// feed mid-tree (`ParentNotFound`/`LeafNotFound`) or a reader fault surfaces as
/// a transport-class `io::Error` (`UnexpectedEof`/`Io`), which must NOT be
/// blamed on the upstream as corruption. Feeding plaintext through the existing
/// `import_and_verify_stream` keeps the `count_and_cap_stream` cap and the
/// structured `StreamCommitOutcome` taxonomy intact (#915, ADR 038).
fn bao_decoded_source(
    hash: Hash,
    content_size: u64,
    rx: tokio::sync::mpsc::Receiver<Bytes>,
) -> impl futures_util::Stream<Item = std::io::Result<Bytes>> + Send + Sync {
    enum State {
        Decoding(ResponseDecoder<RecvStreamAsyncStreamReader<ChannelRecvStream>>),
        Done,
    }
    let tree = BaoTree::new(content_size, crate::range_pull::IROH_BLOCK_SIZE);
    let decoder = ResponseDecoder::new(
        hash.into(),
        ChunkRanges::all(),
        tree,
        RecvStreamAsyncStreamReader::new(ChannelRecvStream::new(rx)),
    );
    futures_util::stream::unfold(State::Decoding(decoder), move |state| async move {
        let mut decoder = match state {
            State::Decoding(d) => d,
            State::Done => return None,
        };
        // Advance to the next leaf (yield its plaintext), a decode failure
        // (yield a classified error, then end), or the end of the stream.
        loop {
            match decoder.next().await {
                ResponseDecoderNext::More((rest, Ok(BaoContentItem::Leaf(leaf)))) => {
                    return Some((Ok(leaf.data), State::Decoding(rest)));
                }
                ResponseDecoderNext::More((rest, Ok(BaoContentItem::Parent(_)))) => {
                    decoder = rest;
                }
                ResponseDecoderNext::More((_rest, Err(decode_err))) => {
                    // Split the taxonomy exactly as the client-side decoder
                    // does (decdn-client-pull `decode_verified_range`): only a
                    // genuine hash mismatch is CORRUPTION (the marker →
                    // `StreamCommitOutcome::VerifyFailed` → the upstream is
                    // scored); an EOF mid-tree (`*NotFound`) or reader fault
                    // (`Io`) is a TRUNCATED/faulted feed — transport-class, not
                    // provably corruption — and must not tar the upstream as a
                    // liar.
                    use bao_tree::io::DecodeError;
                    let err = match decode_err {
                        DecodeError::ParentHashMismatch(_) | DecodeError::LeafHashMismatch(_) => {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                BaoVerifyMarker {
                                    detail: decode_err.to_string(),
                                },
                            )
                        }
                        DecodeError::Io(io_err) => io_err,
                        not_found @ (DecodeError::ParentNotFound(_)
                        | DecodeError::LeafNotFound(_)) => std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            format!("bao stream truncated mid-tree: {not_found}"),
                        ),
                    };
                    return Some((Err(err), State::Done));
                }
                ResponseDecoderNext::Done(_reader) => return None,
            }
        }
    })
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

/// Exact byte length a correct pre-order bao outboard for a `blob_size`-byte
/// blob has, under iroh-blobs' canonical `IROH_BLOCK_SIZE` (#823). Used to
/// bound the untrusted `{H}.obao4` read on the range-pull path before
/// `encode_verified_range`'s authoritative length check rejects a malformed
/// one. Saturates to `u64::MAX` only if the upstream `outboard_size` ever
/// exceeds `u64` (it cannot for any real blob).
fn expected_outboard_len(blob_size: u64) -> u64 {
    bao_tree::BaoTree::new(blob_size, crate::range_pull::IROH_BLOCK_SIZE).outboard_size()
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

    /// A stub origin that also answers [`Origin::size`] and
    /// [`Origin::fetch_outboard`], for [`LocalOutboardPull`] tests. The
    /// outboard answer is configurable so a test can exercise the
    /// `NotFound`/`Unsupported` degrade.
    #[derive(Debug)]
    struct OutboardStubOrigin {
        data: Bytes,
        hash: Hash,
        outboard: Option<Bytes>,
    }

    impl OutboardStubOrigin {
        fn new(payload: &[u8], outboard: Option<Bytes>) -> Self {
            Self {
                hash: Hash::new(payload),
                data: Bytes::from(payload.to_vec()),
                outboard,
            }
        }
    }

    impl Origin for OutboardStubOrigin {
        fn kind(&self) -> OriginKind {
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

        fn size(
            &self,
            hash: Hash,
        ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, crate::OriginPullError>> + Send + '_>>
        {
            let matches = hash == self.hash;
            let len = u64::try_from(self.data.len()).unwrap_or(u64::MAX);
            Box::pin(async move { Ok(matches.then_some(len)) })
        }

        fn fetch_outboard(
            &self,
            hash: Hash,
            _outboard_max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, crate::OriginPullError>> + Send + '_>>
        {
            let result = if hash == self.hash {
                match &self.outboard {
                    Some(ob) => OutboardFetch::Found(ob.clone()),
                    None => OutboardFetch::NotFound,
                }
            } else {
                OutboardFetch::NotFound
            };
            Box::pin(async move { Ok(result) })
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

    /// Origin that answers `size()` (a `HEAD`/`HeadObject` stand-in) for one
    /// hash and COUNTS the calls, so a test can prove `origin_probe_size`
    /// memoises rather than re-probing the backend on every probe. An optional
    /// delay exercises the per-probe timeout.
    #[derive(Debug)]
    struct CountingSizeOrigin {
        hash: Hash,
        size: u64,
        calls: Arc<AtomicUsize>,
        delay: Option<Duration>,
    }

    impl CountingSizeOrigin {
        fn new(hash: Hash, size: u64) -> (Self, Arc<AtomicUsize>) {
            let calls = Arc::new(AtomicUsize::new(0));
            let origin = Self {
                hash,
                size,
                calls: Arc::clone(&calls),
                delay: None,
            };
            (origin, calls)
        }

        fn slow(hash: Hash, size: u64, delay: Duration) -> (Self, Arc<AtomicUsize>) {
            let (mut origin, calls) = Self::new(hash, size);
            origin.delay = Some(delay);
            (origin, calls)
        }
    }

    impl Origin for CountingSizeOrigin {
        fn kind(&self) -> OriginKind {
            OriginKind::Http
        }

        fn fetch(
            &self,
            _hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
        {
            // Never exercised by the probe path (existence + size only).
            Box::pin(async { Ok(OriginFetch::NotFound) })
        }

        fn size(
            &self,
            hash: Hash,
        ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, crate::OriginPullError>> + Send + '_>>
        {
            let matches = hash == self.hash;
            let size = self.size;
            let calls = Arc::clone(&self.calls);
            let delay = self.delay;
            Box::pin(async move {
                // Count the *attempt* before any await, so a probe that the
                // caller times out still registers as a backend hit.
                calls.fetch_add(1, Ordering::SeqCst);
                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                }
                Ok(matches.then_some(size))
            })
        }
    }

    /// A present remote object is discovered by a live `HEAD` and the answer is
    /// memoised: a second probe for the same hash issues NO further backend call.
    #[tokio::test]
    async fn origin_probe_size_hits_origin_then_memoises_positive() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let hash = Hash::new(b"remote object");
        let (origin, calls) = CountingSizeOrigin::new(hash, 4096);
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

        assert_eq!(
            engine.origin_probe_size(hash).await,
            Some(4096),
            "live HEAD finds it"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1, "one backend HEAD");
        assert_eq!(
            engine.origin_probe_size(hash).await,
            Some(4096),
            "served from memo"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "memo hit issues no second HEAD"
        );
        Ok(())
    }

    /// A 404 is memoised too — the whole point of the negative cache is that a
    /// random-hash probe flood does not re-`HeadObject` the origin every time.
    #[tokio::test]
    async fn origin_probe_size_memoises_absent() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let present = Hash::new(b"present");
        let absent = Hash::new(b"absent");
        let (origin, calls) = CountingSizeOrigin::new(present, 100);
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

        assert_eq!(
            engine.origin_probe_size(absent).await,
            None,
            "not in origin"
        );
        assert_eq!(engine.origin_probe_size(absent).await, None, "still absent");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "negative answer is cached");
        Ok(())
    }

    /// A refused (denied/blacklisted/evicted) hash is never advertised, and the
    /// guard short-circuits BEFORE any backend probe.
    #[tokio::test]
    async fn origin_probe_size_refused_never_probes() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let hash = Hash::new(b"denied object");
        let (origin, calls) = CountingSizeOrigin::new(hash, 100);
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
        engine.set_chain_denied_one(hash, true);

        assert_eq!(
            engine.origin_probe_size(hash).await,
            None,
            "refused stays hidden"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "no HEAD for a refused hash"
        );
        Ok(())
    }

    /// A slow origin must not stall the probe: the live HEAD is bounded by
    /// `origin_probe_timeout_ms` and a timeout folds to `None` (safe — never
    /// slashable post-#1512).
    #[tokio::test]
    async fn origin_probe_size_times_out_to_absent() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let hash = Hash::new(b"slow object");
        let (origin, _calls) = CountingSizeOrigin::slow(hash, 100, Duration::from_millis(400));
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
        // Tight timeout so the 400 ms origin overruns it.
        engine.set_origin_probe_config(Duration::from_secs(15), Duration::from_millis(20), 16);

        assert_eq!(
            engine.origin_probe_size(hash).await,
            None,
            "a HEAD slower than the ceiling folds to absent",
        );
        Ok(())
    }

    /// `total_bytes` must sum the on-disk footprint — it is the eviction
    /// driver's entire input, so a wrong sum silently mis-sizes every decision.
    #[tokio::test]
    async fn total_bytes_sums_populated_blobs() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"total-bytes payload";
        let hash = Hash::new(payload);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;

        anyhow::ensure!(engine.total_bytes().await? == 0, "empty cache is 0 bytes");
        let _ = engine.get(hash).await?;

        let total = engine.total_bytes().await?;
        anyhow::ensure!(
            total >= payload.len() as u64,
            "total_bytes {total} should cover the {}-byte blob",
            payload.len()
        );
        let sizes = engine.size_snapshot().await?;
        anyhow::ensure!(
            sizes.get(&hash).copied() == Some(payload.len() as u64),
            "size_snapshot should report the blob's exact size"
        );
        Ok(())
    }

    /// The load-bearing distinction from `evict`: capacity eviction must NOT
    /// write the durable takedown log, or every LRU victim would be permanently
    /// un-servable and the log would grow without bound (#1173).
    #[tokio::test]
    async fn release_for_eviction_does_not_write_the_evicted_log() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"soft evict payload";
        let hash = Hash::new(payload);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;
        let _ = engine.get(hash).await?;

        engine.release_for_eviction(hash).await?;

        anyhow::ensure!(
            !engine.is_evicted(hash),
            "soft evict must not enter the logical-evicted set"
        );
        anyhow::ensure!(
            !tmp.path().join("evicted.log").exists(),
            "soft evict must not create the durable takedown log"
        );
        // The access-time entry is forgotten, so the LRU driver won't re-pick it.
        anyhow::ensure!(
            engine.last_accessed(hash).is_none(),
            "soft evict should forget the access-time entry"
        );
        anyhow::ensure!(
            !engine.eviction_candidates().contains_key(&hash),
            "released hash should leave the candidate set"
        );
        Ok(())
    }

    /// The origin-held index (#1130) advertises fs-origin content by directory
    /// enumeration, includes only *present* pins, and excludes refused hashes.
    #[tokio::test]
    async fn rescan_origins_indexes_fs_and_present_pins() -> anyhow::Result<()> {
        use crate::origin::FilesystemOrigin;

        async fn seed(base: &Path, payload: Vec<u8>) -> anyhow::Result<(Hash, u64)> {
            let hash = Hash::new(&payload);
            let hex = hash.to_hex();
            let shard = hex.get(..2).unwrap_or("");
            let dir = base.join(shard);
            tokio::fs::create_dir_all(&dir).await?;
            tokio::fs::write(dir.join(hex.as_str()), &payload).await?;
            let len = u64::try_from(payload.len()).unwrap_or(u64::MAX);
            Ok((hash, len))
        }

        let origin_dir = tempfile::tempdir()?;
        let base = tokio::fs::canonicalize(origin_dir.path()).await?;
        let (h1, len1) = seed(&base, vec![1u8; 5000]).await?;
        let (h2, len2) = seed(&base, vec![2u8; 9000]).await?;

        let cache_dir = tempfile::tempdir()?;
        let origin = Arc::new(FilesystemOrigin::new(&base).await?) as Arc<dyn Origin>;
        let engine = CacheEngine::open(cache_dir.path(), vec![origin], 10).await?;

        // Pin one present hash and one absent hash; only the present one is held.
        let absent = Hash::new(b"never-on-disk");
        engine.set_pinned(&PinnedHashes::new(
            [from_store_hash(h1), from_store_hash(absent)]
                .into_iter()
                .collect(),
        ));

        engine.rescan_origins().await;

        let held: HashSet<Hash> = engine.origin_held_hashes().into_iter().collect();
        anyhow::ensure!(
            held.contains(&h1) && held.contains(&h2),
            "fs blobs must be enumerated into the held index",
        );
        anyhow::ensure!(!held.contains(&absent), "absent pin must not be held");
        anyhow::ensure!(engine.origin_held_size(h1) == Some(len1), "h1 size wrong");
        anyhow::ensure!(engine.origin_held_size(h2) == Some(len2), "h2 size wrong");
        anyhow::ensure!(
            engine.origin_held_size(absent).is_none(),
            "absent hash must not be servable from origin",
        );

        // A hash denied AFTER the last rescan must drop out of the *live* reads
        // immediately — the snapshot still lists it, but `refuses` is the
        // authority. This guards the #1130 blacklist-compliance interaction:
        // without it the probe would sign has_blob:true for a just-blacklisted
        // blob (g_node_04). No rescan between the deny and the assertions.
        engine.set_denied(&crate::DeniedHashes::new(
            [from_store_hash(h1)].into_iter().collect(),
        ));
        anyhow::ensure!(
            engine.origin_held_size(h1).is_none(),
            "a hash denied since the last rescan must not be servable from origin",
        );
        anyhow::ensure!(
            !engine.origin_held_hashes().contains(&h1),
            "a hash denied since the last rescan must not be announced",
        );

        // And it also drops from the index proper on the next rescan.
        engine.set_denied(&crate::DeniedHashes::new(
            [from_store_hash(h1), from_store_hash(h2)]
                .into_iter()
                .collect(),
        ));
        engine.rescan_origins().await;
        anyhow::ensure!(
            engine.origin_held_size(h2).is_none(),
            "denied hash must not be advertised",
        );
        Ok(())
    }

    /// A pinned hash must be refused with `Ok(0)` and left completely untouched.
    /// The driver relies on the `0` to avoid crediting a no-op as freed bytes.
    #[tokio::test]
    async fn release_for_eviction_refuses_pinned_hash() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"pinned payload";
        let hash = Hash::new(payload);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;
        let _ = engine.get(hash).await?;

        engine.set_pinned(&PinnedHashes::new(
            [from_store_hash(hash)].into_iter().collect(),
        ));

        let released = engine.release_for_eviction(hash).await?;
        anyhow::ensure!(released == 0, "pinned hash must report 0 released");
        anyhow::ensure!(
            engine.last_accessed(hash).is_some(),
            "pinned refusal must not forget the access-time entry"
        );
        anyhow::ensure!(engine.has(hash).await?, "pinned blob must still be present");
        Ok(())
    }

    /// The pin exemption is carved out for a deny-listed hash: "deny wins over
    /// pin" on the space path too, so a pinned + governance-denied hash is
    /// reclaimable here rather than held on disk (unservable) until the takedown
    /// `evict()` runs.
    #[tokio::test]
    async fn release_for_eviction_reclaims_a_deny_listed_pinned_hash() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"pinned then denied";
        let hash = Hash::new(payload);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;
        let _ = engine.get(hash).await?;
        engine.set_pinned(&PinnedHashes::new(
            [from_store_hash(hash)].into_iter().collect(),
        ));

        // Pinned + clean: still exempt from space reclaim.
        anyhow::ensure!(
            engine.release_for_eviction(hash).await? == 0,
            "a pinned clean hash stays exempt"
        );
        anyhow::ensure!(
            engine.last_accessed(hash).is_some(),
            "the clean exemption must keep the access-time entry"
        );

        // Governance denies it → the pin no longer exempts it.
        anyhow::ensure!(
            engine.set_chain_denied_one(hash, true),
            "deny must change the set"
        );
        engine.release_for_eviction(hash).await?;
        anyhow::ensure!(
            engine.last_accessed(hash).is_none(),
            "a pinned + governance-denied hash must go through the reclaim path (which \
             forgets the access-time entry), not the pin early-return"
        );
        Ok(())
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

    /// The `FillMode` ↔ return-shape correspondence, asserted where the private
    /// types are visible — the integration tests in `tests/pull_through.rs`
    /// cannot see `PullThroughOutcome`, so they can only observe that the fill
    /// happened, not which arm produced it.
    ///
    /// This is the property the enum exists to carry, and it must hold in BOTH
    /// directions: `ReturnBytes` always yields `Some`, `CommitOnly` always yields
    /// `None`. Before `FillMode` the second half was false on the buffered arm,
    /// which handed its drain buffer back regardless — an exception that made the
    /// wrappers partial and the mode untrustworthy to read.
    ///
    /// Deliberately run through the BUFFERED arm: `StubOrigin` advertises a size
    /// hint well under `buffered_max_bytes`, so `should_buffer` routes here. The
    /// streaming arm never had the defect.
    #[tokio::test]
    async fn fill_mode_determines_the_return_shape_in_both_directions() -> anyhow::Result<()> {
        let payload = b"drained, not streamed";
        let hash = Hash::new(payload);

        let tmp_fill = tempfile::tempdir()?;
        let engine = CacheEngine::open(
            tmp_fill.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;
        let committed = engine
            .pull_through(hash, false, FillMode::CommitOnly)
            .await?;
        anyhow::ensure!(
            committed.is_none(),
            "CommitOnly must yield no payload, got {committed:?}"
        );

        let tmp_bytes = tempfile::tempdir()?;
        let engine = CacheEngine::open(
            tmp_bytes.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;
        let returned = engine
            .pull_through(hash, false, FillMode::ReturnBytes)
            .await?;
        anyhow::ensure!(
            returned.as_deref() == Some(payload.as_slice()),
            "ReturnBytes must yield the payload, got {returned:?}"
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

    /// Unwrap a [`TeeOpen::Owner`], failing the test on `InFlight`.
    fn owner(open: TeeOpen) -> anyhow::Result<TeeReservation> {
        match open {
            TeeOpen::Owner(res) => Ok(res),
            TeeOpen::InFlight => Err(anyhow::anyhow!("expected Owner, got InFlight")),
        }
    }

    /// Strip the 8-byte LE size header `encode_verified_range` prepends, leaving
    /// the header-less bao wire the window pull-through forwards (and the tee now
    /// consumes, since the content size is passed at `begin`).
    fn header_less(combined: &[u8]) -> anyhow::Result<&[u8]> {
        combined
            .get(8..)
            .ok_or_else(|| anyhow::anyhow!("combined encoding shorter than its 8-byte header"))
    }

    #[tokio::test]
    async fn tee_sink_commits_promotes_and_announces() -> anyhow::Result<()> {
        // A teed fill (#856) must end up cached, hash-verified, and announced to
        // DHT-republish subscribers exactly like an origin pull-through.
        let tmp = tempfile::tempdir()?;
        let payload: Vec<u8> = (0..200_000u32)
            .map(|i| u8::try_from(i % 256).unwrap_or(0))
            .collect();
        let hash = Hash::new(&payload);

        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
        let mut rx = engine.subscribe_inserts();

        // The tee imports the header-less bao verified-stream (ADR 038): the
        // content size is passed at `begin`, so feed the interleaved proof/data
        // without the 8-byte size header — the shape `window_forward_loop`
        // forwards, not the raw payload.
        let blob_size = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        let aligned = crate::range_pull::align_range(0, 0, blob_size)
            .map_err(|e| anyhow::anyhow!("align: {e}"))?;
        let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
            &payload,
            crate::range_pull::IROH_BLOCK_SIZE,
        );
        let combined = crate::range_pull::encode_verified_range(
            *hash.as_bytes(),
            &aligned,
            &payload,
            Bytes::from(ob.data),
        )
        .map_err(|e| anyhow::anyhow!("encode bao: {e}"))?;

        let mut sink = owner(engine.open_tee_sink(hash))?.begin(blob_size);
        for chunk in header_less(combined.as_ref())?.chunks(64 * 1024) {
            sink.write(chunk).await?;
        }
        sink.finish().await?;

        anyhow::ensure!(engine.has(hash).await?, "blob must be present after finish");
        let got = engine.get(hash).await?;
        anyhow::ensure!(
            got.as_ref() == payload.as_slice(),
            "served bytes must match"
        );

        let announced = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for tee insert announce"))?
            .map_err(|e| anyhow::anyhow!("recv: {e}"))?;
        anyhow::ensure!(
            announced == hash,
            "announced hash must equal committed hash"
        );
        Ok(())
    }

    #[tokio::test]
    async fn tee_sink_verify_failure_does_not_promote() -> anyhow::Result<()> {
        // Corrupt upstream (#853/#915): a WELL-FORMED bao stream of the WRONG
        // content — a valid encoding, but of different bytes than the requested
        // hash names — must fail the tee's verifying decoder with a genuine
        // group hash mismatch, surface `CacheError::VerifyFailed` from `finish`,
        // and never be promoted. (A truncated feed is a different failure class —
        // see `tee_sink_truncated_feed_is_not_corruption`.)
        let tmp = tempfile::tempdir()?;
        let genuine: Vec<u8> = (0..200_000u32)
            .map(|i| u8::try_from(i % 256).unwrap_or(0))
            .collect();
        let wanted = Hash::new(&genuine);
        // Same length, different bytes: a valid bao encoding of OTHER content.
        let wrong: Vec<u8> = (0..200_000u32)
            .map(|i| u8::try_from((i + 7) % 251).unwrap_or(0))
            .collect();
        anyhow::ensure!(Hash::new(&wrong) != wanted, "fixtures must differ");
        let blob_size = u64::try_from(wrong.len()).unwrap_or(u64::MAX);
        let aligned = crate::range_pull::align_range(0, 0, blob_size)
            .map_err(|e| anyhow::anyhow!("align: {e}"))?;
        let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
            &wrong,
            crate::range_pull::IROH_BLOCK_SIZE,
        );
        let combined = crate::range_pull::encode_verified_range(
            *Hash::new(&wrong).as_bytes(),
            &aligned,
            &wrong,
            Bytes::from(ob.data),
        )
        .map_err(|e| anyhow::anyhow!("encode bao: {e}"))?;

        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
        let mut sink = owner(engine.open_tee_sink(wanted))?.begin(blob_size);
        let mut write_err = None;
        for chunk in header_less(combined.as_ref())?.chunks(64 * 1024) {
            if let Err(e) = sink.write(chunk).await {
                // The decoder may reject mid-feed (the import ends and a later
                // write fails) — that surfaced verdict must be the same
                // VerifyFailed `finish` would report.
                write_err = Some(e);
                break;
            }
        }
        let err = match write_err {
            Some(e) => {
                sink.abandon();
                e
            }
            None => sink
                .finish()
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected VerifyFailed, got Ok"))?,
        };
        anyhow::ensure!(
            matches!(err, CacheError::VerifyFailed { expected } if expected == wanted),
            "expected VerifyFailed for {wanted}, got {err:?}"
        );
        anyhow::ensure!(
            !engine.has(wanted).await?,
            "mismatched bytes must not be promoted"
        );
        Ok(())
    }

    #[tokio::test]
    async fn tee_sink_empty_blob_commits_and_promotes() -> anyhow::Result<()> {
        // A 0-byte blob teed through the window path (#1054): `content_size` 0 and
        // zero wire bytes. `finish` must promote the empty blob under the empty
        // root `Hash::new(&[])` — the window-forward loop's finalize on a size-0
        // reservation.
        let tmp = tempfile::tempdir()?;
        let empty: Vec<u8> = Vec::new();
        let hash = Hash::new(&empty);
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;

        let sink = owner(engine.open_tee_sink(hash))?.begin(0);
        // No writes: the empty blob has no chunk group. `finish` drops the feeder,
        // closing the stream with no data.
        sink.finish().await?;

        anyhow::ensure!(
            engine.has(hash).await?,
            "empty blob must be present after finish"
        );
        let got = engine.get(hash).await?;
        anyhow::ensure!(got.as_ref().is_empty(), "served empty bytes");
        Ok(())
    }

    #[tokio::test]
    async fn tee_sink_empty_claim_for_nonempty_hash_does_not_promote() -> anyhow::Result<()> {
        // A lying upstream that claims `content_size == 0` (feeds nothing) for a
        // NON-empty requested hash must NOT promote: the empty root must be proven,
        // never accepted for an arbitrary hash (#1054, the tee's fail-closed leg).
        let tmp = tempfile::tempdir()?;
        let genuine: Vec<u8> = (0..200_000u32)
            .map(|i| u8::try_from(i % 256).unwrap_or(0))
            .collect();
        let wanted = Hash::new(&genuine);
        let empty: Vec<u8> = Vec::new();
        anyhow::ensure!(Hash::new(&empty) != wanted, "fixtures must differ");

        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
        let sink = owner(engine.open_tee_sink(wanted))?.begin(0);
        let res = sink.finish().await;
        anyhow::ensure!(
            res.is_err(),
            "an empty feed for a non-empty hash must fail closed, got Ok"
        );
        anyhow::ensure!(
            !engine.has(wanted).await?,
            "an empty feed for a non-empty hash must not be promoted"
        );
        Ok(())
    }

    #[tokio::test]
    async fn tee_sink_truncated_feed_is_not_corruption() -> anyhow::Result<()> {
        // A short feed: the decoder is framed (at `begin`) for a large blob but
        // the stream ends mid-tree. This must FAIL (not commit a partial blob)
        // and must NOT be branded a verify failure — `VerifyFailed` is reserved
        // for a group that provably hashes wrong; a truncated feed is
        // transport-class (EOF mid-tree), which must not tar the upstream as a
        // liar (#915 review).
        let tmp = tempfile::tempdir()?;
        let payload: Vec<u8> = (0..200_000u32)
            .map(|i| u8::try_from(i % 256).unwrap_or(0))
            .collect();
        let wanted = Hash::new(&payload);
        let content_size = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;

        // Frame for a 200 KB blob, then feed only a few bytes and stop: the bao
        // decoder starves mid-tree.
        let mut sink = owner(engine.open_tee_sink(wanted))?.begin(content_size);
        let write_result = sink
            .write(b"only a few bytes, nowhere near a full tree")
            .await;
        let err = match write_result {
            Err(e) => {
                sink.abandon();
                e
            }
            Ok(()) => sink
                .finish()
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected a truncated feed to fail, got Ok"))?,
        };
        anyhow::ensure!(
            !matches!(err, CacheError::VerifyFailed { .. }),
            "a truncated feed must not classify as corruption, got {err:?}"
        );
        anyhow::ensure!(
            !engine.has(wanted).await?,
            "a truncated fill must not be promoted"
        );
        Ok(())
    }

    #[tokio::test]
    async fn tee_sink_coalesces_concurrent_fills() -> anyhow::Result<()> {
        // Two concurrent fills for the same hash: the first owns it, the second
        // is told it is in flight (no double upstream pull, #305). Releasing the
        // owner lets a later caller own it again.
        let tmp = tempfile::tempdir()?;
        let hash = Hash::new(b"coalesce me");
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;

        let first = owner(engine.open_tee_sink(hash))?;
        anyhow::ensure!(
            matches!(engine.open_tee_sink(hash), TeeOpen::InFlight),
            "second concurrent fill must report InFlight"
        );

        // Abandon the owner (e.g. downstream client dropped) → claim released.
        first.abandon();
        // Drop runs synchronously on `abandon`'s move; the claim is now free.
        anyhow::ensure!(
            matches!(engine.open_tee_sink(hash), TeeOpen::Owner(_)),
            "after abandon, a new fill must be able to own the hash"
        );
        anyhow::ensure!(
            !engine.has(hash).await?,
            "an abandoned fill must not have committed the blob"
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

    #[tokio::test]
    async fn iter_hashes_returns_empty_on_empty_store() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
        let hashes = engine.iter_hashes().await?;
        anyhow::ensure!(
            hashes.is_empty(),
            "expected empty iter_hashes on a fresh store, got {hashes:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn iter_hashes_returns_all_committed_blobs() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payloads: &[&[u8]] = &[b"iter-a", b"iter-b", b"iter-c"];
        let origin = MultiStubOrigin::new(payloads);

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

        let mut expected: HashSet<Hash> = HashSet::new();
        for p in payloads {
            let h = Hash::new(*p);
            let _ = engine.get(h).await?;
            expected.insert(h);
        }

        // Use a HashSet for the comparison — iroh-blobs `list()` order is
        // not contractually stable.
        let actual: HashSet<Hash> = engine.iter_hashes().await?.into_iter().collect();
        anyhow::ensure!(actual == expected, "expected {expected:?}, got {actual:?}");
        Ok(())
    }

    #[tokio::test]
    async fn iter_hashes_excludes_evicted_blobs() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payloads: &[&[u8]] = &[b"evict-a", b"evict-b", b"evict-c", b"evict-d"];
        let origin = MultiStubOrigin::new(payloads);

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

        let hashes: Vec<Hash> = payloads.iter().map(|p| Hash::new(*p)).collect();
        for h in &hashes {
            let _ = engine.get(*h).await?;
        }

        // Evict half — odd indices.
        let mut kept: Vec<Hash> = Vec::new();
        let mut evicted: Vec<Hash> = Vec::new();
        for (i, h) in hashes.iter().enumerate() {
            if i % 2 == 0 {
                kept.push(*h);
            } else {
                evicted.push(*h);
            }
        }
        for h in &evicted {
            engine.evict(*h).await?;
        }

        let actual: HashSet<Hash> = engine.iter_hashes().await?.into_iter().collect();
        let kept_set: HashSet<Hash> = kept.iter().copied().collect();

        anyhow::ensure!(
            actual == kept_set,
            "iter_hashes must return exactly the non-evicted set; expected {kept_set:?}, got {actual:?}"
        );
        Ok(())
    }

    /// Pinned blobs MUST appear in `iter_hashes`: pinning protects against
    /// LRU eviction, not against DHT republish. They're the most valuable
    /// content to surface, so they have to seed the cold-start scheduler
    /// alongside everything else. Locks the contract against a future
    /// "exclude pinned for symmetry with `access_times_snapshot`" refactor.
    #[tokio::test]
    async fn iter_hashes_includes_pinned_blobs() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload_pinned: &[u8] = b"pin-a";
        let payload_plain: &[u8] = b"pin-b";
        let origin = MultiStubOrigin::new(&[payload_pinned, payload_plain]);

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

        let h_pinned = Hash::new(payload_pinned);
        let h_plain = Hash::new(payload_plain);
        let _ = engine.get(h_pinned).await?;
        let _ = engine.get(h_plain).await?;

        let s = [from_store_hash(h_pinned)].into_iter().collect();
        let diff = engine.set_pinned(&PinnedHashes::new(s));
        // Without this guard, a future bug where set_pinned silently
        // no-ops would let the test pass on the strength of pre-pin
        // presence alone.
        anyhow::ensure!(
            diff.added == 1 && diff.removed == 0,
            "set_pinned must apply the pin to make this test meaningful, got {diff:?}"
        );

        let actual: HashSet<Hash> = engine.iter_hashes().await?.into_iter().collect();
        let expected: HashSet<Hash> = [h_pinned, h_plain].into_iter().collect();
        anyhow::ensure!(
            actual == expected,
            "iter_hashes must include pinned blobs (they're the highest-value DHT advertisements); expected {expected:?}, got {actual:?}"
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
            CircuitBreakerPolicy::default(),
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

        let inflight_len = inflight_len(&engine);
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
        let inflight_len = inflight_len(&engine);
        anyhow::ensure!(
            inflight_len == 0,
            "inflight map should be empty after cancellation, had {inflight_len} entries"
        );
        Ok(())
    }

    // ----- In-flight mutex poisoning (#1517) -----

    /// Poison `inner.inflight` the only way a `std::sync::Mutex` can be
    /// poisoned: panic while holding the guard.
    ///
    /// This is synthetic by construction — the workspace anti-panic policy
    /// (`unwrap_used` / `expect_used` / `panic` denied) is precisely why no
    /// production path can do this, and precisely why a real firing would be
    /// a bug worth an `error!` rather than a condition worth tuning.
    #[allow(clippy::panic)]
    fn poison_inflight(engine: &CacheEngine) {
        let inner = Arc::clone(&engine.inner);
        let handle = std::thread::spawn(move || {
            let _guard = inner.inflight.lock();
            panic!("deliberately poisoning the inflight mutex");
        });
        // Panicked by construction, so `join` returns the panic payload.
        drop(handle.join());
    }

    /// Read the map length without laundering poison into a false `0` —
    /// `lock().ok()` would report an empty map on a poisoned mutex and make
    /// the orphan assertion below vacuously true.
    fn inflight_len(engine: &CacheEngine) -> usize {
        engine
            .inner
            .inflight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// The load-bearing half of #1517: a poisoned coalescing mutex must not
    /// cost origin egress. Before the fix both `get` call sites discarded the
    /// `PoisonError` and fell through to a *direct* pull, and std poison is
    /// sticky for the process lifetime — so one panic permanently turned every
    /// concurrent request for a missing blob into its own origin fetch. On a
    /// metered `http`/`s3` origin that is an unbounded egress multiplier; on
    /// the `Peer` origin reached via `populate` it is a USDC double-spend.
    ///
    /// `fetch_count == 1` under a poisoned mutex is the whole assertion.
    #[tokio::test]
    async fn poisoned_inflight_mutex_still_coalesces_and_is_counted() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"coalesce under poison";
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
            CircuitBreakerPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        poison_inflight(&engine);

        let mut handles = Vec::new();
        for _ in 0..5 {
            let e = engine.clone();
            handles.push(tokio::spawn(async move { e.get(hash).await }));
        }
        for handle in handles {
            let result = handle
                .await
                .map_err(|e| anyhow::anyhow!("task join: {e}"))?;
            anyhow::ensure!(result.is_ok(), "expected Ok, got {result:?}");
        }

        let count = origin.fetch_count.load(Ordering::SeqCst);
        anyhow::ensure!(
            count == 1,
            "coalescing must survive poison — expected exactly 1 origin fetch, got {count}"
        );
        // Exactly one: `lock_inflight` clears the poison, so a single
        // panic is a single bump no matter how many locks follow it.
        // Asserting the exact value is what pins that — `> 0` would pass
        // just as happily if the clear were dropped and the counter
        // climbed with request volume.
        let bumps = cm.inflight_mutex_poisoned.get();
        anyhow::ensure!(
            bumps == 1,
            "one poisoning must count exactly once, got {bumps}"
        );
        anyhow::ensure!(
            inflight_len(&engine) == 0,
            "the owner's guard must still clear its entry under poison"
        );
        Ok(())
    }

    /// Two separate poisonings count twice while the log stays latched at one.
    ///
    /// Two poisonings, not one, is the whole point: because `lock_inflight`
    /// clears the poison, a single panic yields a single bump, so only a
    /// *second* panic can show the counter advancing past the latch. This is
    /// also the test that pins "counts poisonings, not locks-since-a-poisoning"
    /// — drop the `clear_poison` and the first `get` alone drives the counter
    /// to 2, which the exact-value assertions below reject.
    ///
    /// The latch itself is asserted through the private `AtomicBool` rather
    /// than by capturing log output: no crate in the workspace carries a
    /// `tracing` capture layer in dev-deps, and that is a weak proxy — it
    /// cannot catch a mutation that logs unconditionally *and* sets the flag.
    /// Recorded rather than papered over; the counter assertions are the ones
    /// carrying real weight here.
    #[tokio::test]
    async fn separate_poisonings_each_count_while_the_log_latches() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"latch check";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![Arc::new(origin) as Arc<dyn Origin>],
            10,
            crate::PinnedHashes::empty(),
            crate::RetryPolicy::default(),
            CircuitBreakerPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        poison_inflight(&engine);

        anyhow::ensure!(
            !engine.inner.inflight_poison_logged.load(Ordering::Relaxed),
            "the latch must not be set before any lock is taken"
        );

        // One miss takes the lock twice (claim the entry, then release it in
        // `InflightGuard::drop`) but sees the poison only on the first, since
        // that lock clears it.
        let _ = engine.get(hash).await?;
        let after_first = cm.inflight_mutex_poisoned.get();
        anyhow::ensure!(
            after_first == 1,
            "one poisoning, one bump — the poison must have been cleared; got {after_first}"
        );
        anyhow::ensure!(
            engine.inner.inflight_poison_logged.load(Ordering::Relaxed),
            "the first poisoned lock must latch the log"
        );

        // A second, independent panic. A second `get` would add nothing on its
        // own — it is a cache hit and returns above the coalescing loop without
        // locking at all — so poison directly and take one more lock.
        poison_inflight(&engine);
        drop(engine.inner.lock_inflight());
        let after_second = cm.inflight_mutex_poisoned.get();

        anyhow::ensure!(
            after_second == 2,
            "the counter must count the second poisoning too, got {after_second}"
        );
        anyhow::ensure!(
            engine.inner.inflight_poison_logged.load(Ordering::Relaxed),
            "the latch must stay set — the second poisoning must not re-log"
        );
        Ok(())
    }

    /// `populate_inner`'s coalescing loop is a near-verbatim *copy* of `get`'s,
    /// not a shared helper, and the pre-#1517 defect was per-copy — each had its
    /// own poison fall-through. So covering `get` does not cover this, and a
    /// mutation that reverts only `populate_inner` survives a `get`-only suite.
    ///
    /// This is also the copy that matters most: `populate` (unlike
    /// `populate_local`) walks the `Peer` origin, so a lost claim here is the
    /// duplicate *paid* upstream pull — the USDC double-spend #1517 names.
    /// `get`, by contrast, has no production caller in the daemon; the serve
    /// path fills via `populate`/`populate_local` and `open_tee_sink`.
    #[tokio::test]
    async fn poisoned_populate_still_coalesces_into_one_fill() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"populate under poison";
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
            CircuitBreakerPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        poison_inflight(&engine);

        let mut handles = Vec::new();
        for _ in 0..5 {
            let e = engine.clone();
            handles.push(tokio::spawn(async move { e.populate(hash).await }));
        }
        for handle in handles {
            let result = handle
                .await
                .map_err(|e| anyhow::anyhow!("task join: {e}"))?;
            anyhow::ensure!(result.is_ok(), "expected Ok, got {result:?}");
        }

        let count = origin.fetch_count.load(Ordering::SeqCst);
        anyhow::ensure!(
            count == 1,
            "populate must coalesce under poison — expected 1 origin fetch, got {count}"
        );
        let bumps = cm.inflight_mutex_poisoned.get();
        anyhow::ensure!(bumps == 1, "one poisoning, one bump; got {bumps}");
        anyhow::ensure!(inflight_len(&engine) == 0, "the claim must be released");
        Ok(())
    }

    /// The cross-path case, and the one that maps most directly onto the USDC
    /// hazard: `open_tee_sink` answers `TeeOpen::InFlight` off the *same* map
    /// that `populate`/`get` claim into.
    ///
    /// `open_tee_sink` itself already recovered poison before #1517, so it was
    /// never broken from its own side — it was defeated from the other. A
    /// poisoned `populate` fell through to a direct pull **without inserting the
    /// entry**; a concurrent `open_tee_sink` then saw an empty map, returned
    /// `Owner`, and opened a second paid upstream pull. Neither site alone
    /// exhibits that, which is why it needs its own test.
    #[tokio::test]
    async fn poisoned_claim_is_still_visible_to_the_tee_path() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"tee sees the claim";
        let hash = Hash::new(payload);
        let origin = Arc::new(SlowCountingOrigin::new(
            payload,
            std::time::Duration::from_secs(10),
        ));

        let engine =
            CacheEngine::open(tmp.path(), vec![origin.clone() as Arc<dyn Origin>], 10).await?;

        poison_inflight(&engine);

        // Claim the hash from the populate side and leave it in flight.
        let filler = engine.clone();
        let owner = tokio::spawn(async move { filler.populate(hash).await });
        // Wait for the claim to land rather than sleeping a fixed interval —
        // but with a deadline. An unbounded spin here would *hang* under the
        // very regression this test exists to catch (a poisoned `populate` that
        // never inserts the claim), and a hung test blocks CI instead of
        // reporting. Fail fast and say which it was.
        let deadline = std::time::Duration::from_secs(5);
        tokio::time::timeout(deadline, async {
            while inflight_len(&engine) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!("populate never claimed the hash within {deadline:?} under poison")
        })?;

        anyhow::ensure!(
            matches!(engine.open_tee_sink(hash), TeeOpen::InFlight),
            "a claim taken under poison must still block a second paid upstream pull"
        );

        owner.abort();
        Ok(())
    }

    /// The drop-path half of #1517. `InflightGuard::drop` used to swallow the
    /// `PoisonError` and skip the removal; since `notify_waiters` only wakes
    /// *current* waiters, the leaked entry made every later request for that
    /// hash park on a `Notify` that would never fire again — a permanent hang,
    /// which is the exact failure the guard exists to prevent.
    ///
    /// The unpoisoned cancellation case is covered by
    /// [`cancelled_owner_does_not_orphan_inflight_entry`]; this is its poisoned
    /// twin, and it reads the map with `into_inner` so poison cannot fake a
    /// pass.
    #[tokio::test]
    async fn poisoned_mutex_does_not_orphan_inflight_entry() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"poisoned cancel";
        let hash = Hash::new(payload);
        let origin_handle = Arc::new(SlowCountingOrigin::new(
            payload,
            std::time::Duration::from_secs(10),
        ));

        let engine = CacheEngine::open(
            tmp.path(),
            vec![origin_handle.clone() as Arc<dyn Origin>],
            10,
        )
        .await?;

        poison_inflight(&engine);

        let owner_engine = engine.clone();
        let owner = tokio::spawn(async move {
            tokio::time::timeout(std::time::Duration::from_millis(50), owner_engine.get(hash)).await
        });
        let _ = owner.await?;

        // Prove the owner actually claimed the entry before asserting it was
        // released. Without this the test passes vacuously whenever the 50 ms
        // timeout lands before the pull starts — `get` does `refuses()` and
        // `has()` (fs store I/O) first — and an empty map then proves nothing,
        // even under the pre-#1517 drop impl. A loaded CI box makes that a
        // silent false pass rather than a visible flake.
        let claimed = origin_handle.fetch_count.load(Ordering::SeqCst);
        anyhow::ensure!(
            claimed == 1,
            "owner must have reached the pull (and so held the entry), got {claimed} fetches"
        );

        // An empty map is the assertion proper, and it reads through
        // `into_inner` so poison cannot fake it. Under the pre-#1517 drop impl
        // the entry survives here and `len == 1`. There is deliberately no
        // "now issue another get and time it" probe: this origin sleeps for ten
        // seconds by design, so any such timeout would measure the stub rather
        // than the orphaned `Notify`.
        let len = inflight_len(&engine);
        anyhow::ensure!(
            len == 0,
            "InflightGuard::drop must clear the entry under poison, had {len}"
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

    /// A pinned hash that is ALSO governance-denied stays an eviction candidate —
    /// "deny wins over pin" on the LRU path, not only the takedown `evict()`. A
    /// pinned clean hash is still excluded. The pin flag itself is unchanged.
    #[tokio::test]
    async fn deny_listed_pinned_hash_is_an_eviction_candidate() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let clean_hash = Hash::new(b"pinned clean blob");
        let denied_hash = Hash::new(b"pinned denied blob");

        let pinned_set = [from_store_hash(clean_hash), from_store_hash(denied_hash)]
            .into_iter()
            .collect();
        let engine = CacheEngine::open_with_pinned(
            tmp.path(),
            Vec::new(),
            10,
            PinnedHashes::new(pinned_set),
        )
        .await?;
        if let Ok(mut g) = engine.inner.access_times.lock() {
            g.insert(clean_hash, Instant::now());
            g.insert(denied_hash, Instant::now());
        }

        anyhow::ensure!(
            engine.set_chain_denied_one(denied_hash, true),
            "deny must change the set"
        );

        let candidates = engine.eviction_candidates();
        anyhow::ensure!(
            candidates.contains_key(&denied_hash),
            "a pinned + governance-denied hash must remain an eviction candidate"
        );
        anyhow::ensure!(
            !candidates.contains_key(&clean_hash),
            "a pinned clean hash must still be excluded"
        );
        anyhow::ensure!(
            engine.is_pinned(denied_hash) && engine.is_pinned(clean_hash),
            "the pin flag itself is unchanged — only the eviction carve-out differs"
        );
        Ok(())
    }

    /// The carve-out fires for the LOCAL (`content.denied_hashes` → `set_denied`)
    /// deny half too, not only the on-chain half — the `is_denied` side of the
    /// `is_denied || is_chain_denied` disjunction in `eviction_candidates`.
    #[tokio::test]
    async fn local_denied_pinned_hash_is_also_an_eviction_candidate() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let hash = Hash::new(b"pinned + locally denied");
        let engine = CacheEngine::open_with_pinned(
            tmp.path(),
            Vec::new(),
            10,
            PinnedHashes::new([from_store_hash(hash)].into_iter().collect()),
        )
        .await?;
        if let Ok(mut g) = engine.inner.access_times.lock() {
            g.insert(hash, Instant::now());
        }
        engine.set_denied(&denied(&[hash]));
        anyhow::ensure!(
            engine.eviction_candidates().contains_key(&hash),
            "a pinned + locally-denied hash must also be an eviction candidate"
        );
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

        engine.evict(hash).await?;

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
            engine.evict(hash).await?;
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
        engine.evict(unknown).await?;
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

        engine.evict(hash).await?;
        let after_first = std::fs::read_to_string(&log_path)?;
        let lines_first = after_first.lines().count();

        engine.evict(hash).await?;
        engine.evict(hash).await?;
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
        match engine.evict(hash).await {
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
        engine.evict(hash).await?;

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
            CircuitBreakerPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        // 1 miss (pull-through), 2 hits, 1 miss (origin NotFound), 1 miss (evicted).
        let _ = engine.get(hash).await?; // miss
        let _ = engine.get(hash).await?; // hit
        let _ = engine.get(hash).await?; // hit
        let _ = engine.get(unknown).await; // miss (origin NotFound)
        engine.evict(hash).await?;
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
            CircuitBreakerPolicy::default(),
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
            CircuitBreakerPolicy::default(),
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
    async fn populate_fills_without_bumping_bytes_returned() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"populate must not count as bytes returned";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);
        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![Arc::new(origin) as Arc<dyn Origin>],
            10,
            crate::PinnedHashes::empty(),
            crate::RetryPolicy::default(),
            CircuitBreakerPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        engine.populate(hash).await?;
        anyhow::ensure!(engine.has(hash).await?, "populate must fill the store");
        // Origin egress IS counted (the bytes really left an origin)...
        anyhow::ensure!(
            cm.pull_through_bytes.get() == payload.len() as u64,
            "populate must still bump origin-egress pull_through_bytes"
        );
        // ...but it is NOT a `get` caller, so no served-bytes / hit accounting.
        anyhow::ensure!(
            cm.bytes_returned.get() == 0,
            "populate must NOT bump bytes_returned (#831: internal fill, not client egress)"
        );
        anyhow::ensure!(cm.hits.get() == 0, "populate must not count a hit");

        // A populate on an already-present hash is a no-op (no second pull).
        engine.populate(hash).await?;
        anyhow::ensure!(
            cm.pull_through_bytes.get() == payload.len() as u64,
            "a populate for an already-present blob must not re-pull"
        );
        anyhow::ensure!(cm.bytes_returned.get() == 0, "still no bytes_returned");
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
            CircuitBreakerPolicy::default(),
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
            CircuitBreakerPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        // Prime then evict so the next get hits the evicted branch in get().
        let _ = engine.get(hash).await?;
        engine.evict(hash).await?;
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

    // ---- Governance deny-set (ADR 011 §StreamRequest Response) ----

    async fn empty_engine(tmp: &std::path::Path) -> anyhow::Result<CacheEngine> {
        Ok(
            CacheEngine::open_with_pinned(tmp, Vec::new(), 10, crate::PinnedHashes::empty())
                .await?,
        )
    }

    /// The governance set has to feed `refuses`, or the takedown suppresses
    /// nothing before the (separate, slower) eviction lands.
    #[tokio::test]
    async fn chain_denied_hash_is_refused() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = empty_engine(tmp.path()).await?;
        let hash = Hash::new(b"governance takedown");

        anyhow::ensure!(!engine.refuses(hash), "nothing refused before the deny");
        anyhow::ensure!(engine.set_chain_denied_one(hash, true), "set changed");
        anyhow::ensure!(engine.is_chain_denied(hash));
        anyhow::ensure!(engine.refuses(hash), "a governance deny must refuse");
        anyhow::ensure!(
            !engine.is_denied(hash),
            "and must NOT masquerade as a local denylist entry — the two feed \
             different operator metrics"
        );
        anyhow::ensure!(!engine.has(hash).await?, "refused hashes report absent");
        Ok(())
    }

    /// A no-op replay must be reported as such: the watcher re-scans a block
    /// range after a restart and re-delivers events it already applied, and
    /// every one of those would otherwise log as a fresh takedown.
    #[tokio::test]
    async fn set_chain_denied_one_reports_whether_it_changed_anything() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = empty_engine(tmp.path()).await?;
        let hash = Hash::new(b"replayed");

        anyhow::ensure!(engine.set_chain_denied_one(hash, true));
        anyhow::ensure!(
            !engine.set_chain_denied_one(hash, true),
            "replay is a no-op"
        );
        anyhow::ensure!(engine.set_chain_denied_one(hash, false));
        anyhow::ensure!(!engine.set_chain_denied_one(hash, false));
        anyhow::ensure!(
            !engine.refuses(hash),
            "a de-listed hash stops being refused"
        );
        Ok(())
    }

    /// The reason the two deny-sets are separate slots: their lifecycles are
    /// independent. A config reload must not drop a governance takedown, and the
    /// watcher must not drop the operator's own list.
    #[tokio::test]
    async fn local_and_chain_deny_sets_do_not_clobber_each_other() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = empty_engine(tmp.path()).await?;
        let local = Hash::new(b"local entry");
        let governance = Hash::new(b"governance entry");

        engine.set_denied(&denied(&[local]));
        engine.set_chain_denied_one(governance, true);

        // A reload that drops the local entry leaves the governance one standing.
        engine.set_denied(&crate::DeniedHashes::empty());
        anyhow::ensure!(!engine.refuses(local), "local entry lifted by the reload");
        anyhow::ensure!(
            engine.refuses(governance),
            "a config reload must not lift a governance takedown"
        );

        // ...and a governance removal leaves a re-added local entry standing.
        engine.set_denied(&denied(&[local]));
        engine.set_chain_denied_one(governance, false);
        anyhow::ensure!(engine.refuses(local));
        anyhow::ensure!(!engine.refuses(governance));
        Ok(())
    }

    /// A hash on BOTH lists must survive removal from one. A single shared set
    /// would drop it and silently resume serving content still under a takedown.
    #[tokio::test]
    async fn hash_on_both_deny_sets_survives_removal_from_one() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = empty_engine(tmp.path()).await?;
        let hash = Hash::new(b"both lists");

        engine.set_denied(&denied(&[hash]));
        engine.set_chain_denied_one(hash, true);
        engine.set_chain_denied_one(hash, false);
        anyhow::ensure!(engine.refuses(hash), "the local entry still stands");
        Ok(())
    }

    /// The watcher's boot restore replaces wholesale — it is reloading a
    /// projection, not merging events into one.
    #[tokio::test]
    async fn set_chain_denied_replaces_wholesale() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = empty_engine(tmp.path()).await?;
        let stale = Hash::new(b"stale");
        let restored = Hash::new(b"restored");

        engine.set_chain_denied_one(stale, true);
        engine.set_chain_denied([restored].into_iter().collect());
        anyhow::ensure!(!engine.refuses(stale));
        anyhow::ensure!(engine.refuses(restored));
        Ok(())
    }

    fn denied(hashes: &[Hash]) -> crate::DeniedHashes {
        crate::DeniedHashes::new(hashes.iter().map(|h| from_store_hash(*h)).collect())
    }

    // ---- Probe-triggered eviction hold (#318, ADR 005) ----

    #[tokio::test]
    async fn try_probe_hold_unavailable_when_blob_absent() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), vec![], 10).await?;
        let absent = Hash::new(b"never fetched");
        anyhow::ensure!(
            engine.try_probe_hold(absent).await? == ProbeHoldOutcome::Unavailable,
            "absent blob must not be holdable (an absent blob is never advertised)"
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
        // `max == 0` is an operator config decision (holds turned off), not
        // budget pressure — it must report a cause distinct from
        // `BudgetExhausted` (#739) so the "increase max_probe_holds" alert
        // isn't tripped by an intentional disable.
        anyhow::ensure!(
            engine.try_probe_hold(hash).await? == ProbeHoldOutcome::HoldsDisabled,
            "max_probe_holds=0 must disable has_blob:true entirely"
        );
        Ok(())
    }

    #[tokio::test]
    async fn runtime_disable_overrides_existing_probe_hold() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"held then disabled";
        let hash = Hash::new(payload);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;
        let _ = engine.get(hash).await?;
        engine.set_max_probe_holds(1);
        anyhow::ensure!(
            engine.try_probe_hold(hash).await? == ProbeHoldOutcome::Held,
            "hold should be granted while the budget is positive"
        );
        // Lowering the budget to 0 while a hold is live must take effect
        // immediately: a re-probe for the already-held blob must NOT refresh
        // the hold and re-sign has_blob:true (#739). `max == 0` means "never
        // sign has_blob:true", unconditionally.
        engine.set_max_probe_holds(0);
        anyhow::ensure!(
            engine.try_probe_hold(hash).await? == ProbeHoldOutcome::HoldsDisabled,
            "a runtime disable must override an existing live hold"
        );
        Ok(())
    }

    #[tokio::test]
    async fn absent_blob_is_unavailable_even_when_holds_disabled() -> anyhow::Result<()> {
        // Precedence pin (#739): the `max == 0` early return must sit *after*
        // the `has()` check, so an absent blob is a true negative
        // (`Unavailable`), never a `HoldsDisabled` config-disable event. Guards
        // against a future reorder that moves the cheap `max == 0` load above
        // the store lookup and silently inflates
        // `probe_hold_unavailable{reason="disabled"}` with
        // probes for content the node never had.
        let tmp = tempfile::tempdir()?;
        let absent = Hash::new(b"never fetched");
        let engine = CacheEngine::open(tmp.path(), vec![], 10).await?;
        engine.set_max_probe_holds(0);
        anyhow::ensure!(
            engine.try_probe_hold(absent).await? == ProbeHoldOutcome::Unavailable,
            "absent blob must win over holds-disabled"
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

        engine.evict(hash).await?;
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
            CircuitBreakerPolicy::default(),
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

    /// A blob spanning several chunk groups plus a partial final group, so
    /// the bao tree has real interior nodes (matches
    /// `decdn-bao-range::streaming`'s test rationale).
    fn local_outboard_pull_test_blob() -> Vec<u8> {
        let size = 5 * crate::CHUNK_GROUP_BYTES + 123;
        (0..size).map(|i| (i % 251) as u8).collect()
    }

    #[tokio::test]
    async fn local_outboard_pull_streams_full_wire() -> anyhow::Result<()> {
        use bao_tree::io::outboard::PreOrderMemOutboard;

        let data = local_outboard_pull_test_blob();
        let ob = PreOrderMemOutboard::create(&data, crate::range_pull::IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let outboard = Bytes::from(ob.data.clone());

        let origin = OutboardStubOrigin::new(&data, Some(outboard.clone()));
        let hash = Hash::new(&data);
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

        let Some((header, mut pull)) = engine.open_local_outboard_pull(hash).await? else {
            anyhow::bail!("expected Some((header, pull)) — outboard + data are both served");
        };
        anyhow::ensure!(
            header.total_bytes == data.len() as u64,
            "header.total_bytes should be the plaintext length"
        );
        let expected_wire_bytes = pull.expected_wire_bytes();

        let mut wire = Vec::new();
        while let Some(chunk) = pull.next_chunk().await? {
            wire.extend_from_slice(&chunk);
        }
        pull.finish().await?;

        anyhow::ensure!(
            wire.len() as u64 == expected_wire_bytes,
            "sum(chunk.len()) should equal expected_wire_bytes(): got {} vs {}",
            wire.len(),
            expected_wire_bytes,
        );

        let mut reference = Vec::new();
        decdn_bao_range::streaming::encode_whole_blob_headerless(
            root,
            data.len() as u64,
            outboard,
            &data[..],
            &mut reference,
        )?;
        anyhow::ensure!(
            wire == reference,
            "streamed header-less bao wire should match the reference whole-blob encoding"
        );

        Ok(())
    }

    #[tokio::test]
    async fn local_outboard_pull_none_without_outboard() -> anyhow::Result<()> {
        let data = local_outboard_pull_test_blob();
        let hash = Hash::new(&data);
        let origin = OutboardStubOrigin::new(&data, None);
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

        let result = engine.open_local_outboard_pull(hash).await?;
        anyhow::ensure!(
            result.is_none(),
            "no origin serves the outboard; open_local_outboard_pull should degrade to Ok(None)"
        );
        Ok(())
    }
}
