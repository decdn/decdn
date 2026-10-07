//! Cache engine: local iroh-blobs store fronted by an [`Origin`] for misses.

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bao_tree::ChunkRanges;
use bao_tree::io::BaoContentItem;
use bao_tree::io::fsm::{ResponseDecoder, ResponseDecoderNext};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures_util::StreamExt;
use iroh_blobs::api::blobs::EncodedItem;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::fs::options::Options as FsStoreOptions;
use iroh_blobs::store::{GcConfig, ProtectOutcome};
use iroh_blobs::{Hash, HashAndFormat};
use iroh_io::AsyncStreamReader;
use tokio::sync::{Notify, broadcast};

use decdn_config_types::{CircuitBreakerPolicy, DeniedHashes, PinDiff, PinnedHashes, RetryPolicy};

use crate::CHUNK_GROUP_BYTES;
use crate::circuit_breaker::{
    Admission, Clock, OriginBreaker, OriginOutcome, SystemClock, TrialGuard,
};
use crate::error::{CacheError, CacheResult, OriginPullError};
use crate::fill_session::{FillClaim, FillRegistry, FillSession};
use crate::metrics::CacheMetrics;
use crate::origin::{Origin, OriginKind, OutboardFetch};
use crate::origin_probe::{OriginProbeMemo, OriginProbePolicy, Presence};
use crate::origin_range::{
    MAX_CONCURRENT_RANGE_PULLS, OriginRangeCursor, OriginRangeWire, OriginReadBudget,
    RANGE_PULL_PERMIT_WARN_AFTER, within_origin_timeout,
};
use crate::outboard_cache::{OUTBOARD_CACHE_BYTES, OUTBOARD_CACHE_ENTRIES, OutboardCache};
use crate::probe_hold::ProbeHoldOutcome;
use crate::range_pull::{AlignedRange, align_range, align_range_clamped};
use crate::retry::{
    TerminalFailure, classify_io_error, drain_to_bytes, run_with_retry_classified, should_buffer,
};
use crate::{from_store_hash, to_store_hash};

/// Rescan slot: no pass running, and none requested.
const RESCAN_IDLE: u8 = 0;
/// A pass owns the slot; nothing queued behind it.
const RESCAN_RUNNING: u8 = 1;
/// A pass owns the slot and a further pass is queued behind it.
const RESCAN_QUEUED: u8 = 2;

/// Releases the rescan slot if a pass leaves without a clean release CAS —
/// today, only by panicking. Without it the slot stays claimed and every later
/// rescan returns immediately, so the announce set freezes at whatever the
/// panicking pass had last published.
struct RescanSlotGuard<'a> {
    slot: &'a AtomicU8,
    armed: bool,
}

impl Drop for RescanSlotGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.slot.store(RESCAN_IDLE, Ordering::Release);
        }
    }
}

/// Per-candidate ceiling for an origin rescan's existence probes.
///
/// Deliberately not `cache.origin_probe_timeout_ms`: that knob is sized so a
/// slow origin cannot stall the probe serve path, and a rescan is a bulk walk
/// with nothing waiting on it. Borrowing the tighter budget would classify a
/// merely slow origin as faulting on every candidate, which on a cold boot —
/// where nothing can be carried forward — leaves the announce set empty for the
/// process lifetime.
const RESCAN_PROBE_TIMEOUT: Duration = Duration::from_secs(30);

/// How far before [`CacheEngine::open`] the open-time recency seed sits.
///
/// Every blob the open-time store walk finds enters `access_times` at
/// `open - COLD_SEED_AGE`. Every access after open records `Instant::now()`,
/// which is later. So both eviction policies see an unaccessed pre-open blob as
/// older than anything touched since open: [`crate::policy::LruEviction`]
/// releases it first, and [`crate::policy::TinyLfuEviction`] releases it first
/// among blobs of equal frequency. The pre-open blobs tie on recency, and both
/// policies break that tie largest first.
const COLD_SEED_AGE: Duration = Duration::from_secs(1);

/// The origin-held index and what the rescan that built it could not resolve.
///
/// One `ArcSwap` payload rather than an index plus separate counters: a reader
/// that saw a fault-truncated index alongside a later rescan's zero fault count
/// would report a short announce set as healthy, which is the failure the counts
/// exist to expose. Published together, read together.
#[derive(Debug, Default)]
struct OriginHeldIndex {
    /// Hash -> total byte size, for everything a configured origin can serve.
    held: HashMap<Hash, u64>,
    /// Candidates whose size probe faulted rather than answering. Each either
    /// kept a size carried from the previous index or is missing from `held`.
    probe_faults: u64,
    /// Origins whose `enumerate` failed. Their listings contributed nothing to
    /// this pass, so every hash discoverable only through one of them is absent
    /// from `held` — with no per-hash fault to count, since none was probed.
    enumerate_failures: u64,
}

/// What one [`CacheEngine::rescan_origins`] probe pass resolved.
#[derive(Debug, Default)]
struct RescanResolution {
    /// The rebuilt index: hash -> total byte size.
    held: HashMap<Hash, u64>,
    /// Candidates whose probe faulted rather than answering.
    faults: u64,
    /// Faulted candidates that kept an entry from the previous index. Always
    /// `<= faults`, hence the same width.
    carried: u64,
}

/// The origin-held announce set plus what the rescan behind it could not
/// resolve, read as one consistent view — see
/// [`CacheEngine::origin_held_snapshot`].
#[derive(Debug, Default)]
pub struct OriginHeldReport {
    /// Hashes a configured origin can serve, minus anything currently refused.
    pub hashes: HashSet<Hash>,
    /// Size probes that faulted on the rescan that built this set.
    pub probe_faults: u64,
    /// Origins whose listing failed on that rescan, contributing nothing.
    pub enumerate_failures: u64,
}

/// A live origin-existence answer (#1766), distinguishing a genuine negative
/// from a backend fault. It is the target of the two-way collapse in
/// `crate::origin_probe::Presence`: `Present`/`Absent` map straight across,
/// and `Fault` is folded into "don't advertise" — see
/// [`CacheEngine::origin_probe_size`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginPresence {
    /// A configured origin holds the blob; carries the total byte size.
    Present(u64),
    /// No configured origin holds the blob: a genuine `HEAD`/`HeadObject`
    /// 404, or no origin is configured at all ([`CacheError::NoOrigin`] — a
    /// node with nothing configured genuinely holds nothing; that is not a
    /// transient condition).
    Absent,
    /// The probe could not get an authoritative answer: a transport error, or
    /// the live `HEAD` overran `cache.origin_probe_timeout_ms`. Distinct from
    /// `Absent` on purpose — a caller that would otherwise sign an
    /// authoritative `NotFound` must not do so on a fault; see the
    /// origin-only serve gate (`dispatch.rs`).
    Fault,
}

impl From<Presence> for OriginPresence {
    fn from(presence: Presence) -> Self {
        match presence {
            Presence::Present(size) => OriginPresence::Present(size),
            Presence::Absent => OriginPresence::Absent,
            Presence::Fault => OriginPresence::Fault,
        }
    }
}

/// Engine bundling a filesystem-backed iroh-blobs store with an optional
/// origin backend. Lookups hit the store first; on miss and when an origin is
/// configured, bytes are pulled and BLAKE3-verified. Insert-before-return is a
/// property of the buffered path ([`Self::get`] / [`Self::populate`]), not of
/// this type: [`Self::admit_bao`] commits only a verified sub-range.
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
    /// is the single-origin case. Entries are tried in operator-supplied
    /// order; the next entry is consulted on `NotFound`, permanent error,
    /// or retry-budget exhaustion.
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
    /// Per-hash recency for eviction ordering: the instant of the last access,
    /// or the open-time seed.
    ///
    /// Holds every blob the open-time store walk finds, seeded at
    /// [`Inner::cold_seed_at`], plus every access since open at the instant of
    /// that access. The seed makes a
    /// blob from before a restart an eviction candidate without traffic.
    ///
    /// Sharded. A serve completion's [`CacheEngine::record_access`] takes one
    /// shard's lock, so its cost is independent of the cached-blob count and it
    /// never queues behind a full-map scan. The scans — `eviction_candidates`
    /// and `access_times_snapshot` — walk shard by shard and hold one shard at a
    /// time, so a completion contends only when it lands on the shard the scan
    /// is inside at that moment.
    ///
    /// A scan therefore reads a shard-by-shard view rather than one instant: a
    /// record that lands while the walk is in progress may or may not appear in
    /// its result. Recency is advisory input to eviction ordering, and a hash
    /// missed by one sweep is seen by the next.
    access_times: DashMap<Hash, Instant>,
    /// The single instant the open-time seed writes into `access_times`
    /// ([`COLD_SEED_AGE`] before open). An entry that still holds this value has
    /// had no access since open, so [`CacheEngine::last_accessed`] reports none
    /// for it.
    cold_seed_at: Instant,
    /// Hashes whose `decdn-partial-` protecting tag this process has already
    /// written and not observed dropped. A partial's protection only needs the
    /// tag to EXIST, and the name is deterministic per hash, so re-admitting more
    /// ranges of the same blob need not re-issue the store write — one fill can
    /// admit hundreds of ranges. [`CacheEngine::protect_partial`] short-circuits on
    /// a present entry; [`CacheEngine::drop_named_tags_for`] removes the entry
    /// before deleting the tag, so the next admit re-protects. Lock-free
    /// (`DashMap`) so it never serializes the admit hot path. Starts empty each
    /// process, so the first admit after restart harmlessly re-sets the tag once.
    partial_protected: DashMap<Hash, ()>,
    /// Present-byte count of each partial blob, shared by
    /// [`CacheEngine::size_snapshot`] and the GC callback's walk. See
    /// [`PARTIAL_SIZE_TTL`] for why the walks do not re-observe every
    /// partial every time.
    partial_sizes: Arc<PartialSizeMemo>,
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
    origin_held: ArcSwap<OriginHeldIndex>,
    /// Single-flight slot for [`CacheEngine::rescan_origins`], with one queued
    /// rerun. See `RESCAN_IDLE` / `RESCAN_RUNNING` / `RESCAN_QUEUED`.
    ///
    /// A rescan reads the current index (to carry a faulted candidate forward),
    /// probes every candidate, and only then publishes — a read-modify-write
    /// spanning the whole walk. Both production triggers fire detached, and a
    /// walk gets slower exactly when the origin is faulting, so overlapping
    /// passes would let the slower one publish a payload derived from a
    /// pre-empted index and drop whatever the fresher pass found.
    ///
    /// Excluding is not enough on its own: queueing every trigger behind a lock
    /// would pile up one waiter per tick for as long as a walk outruns the
    /// cadence, then run that backlog of obsolete passes back to back. The slot
    /// collapses any number of triggers into a single rerun, which is all a
    /// rerun can be worth — the next pass re-derives everything from scratch.
    rescan_slot: AtomicU8,
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
    /// other operator evicts survive a process restart — an
    /// in-memory-only set would silently let evicted content resume serving
    /// after `decdn run` is restarted, which is exactly the failure mode
    /// #279 needs to prevent.
    ///
    /// Append-only, so the serve-path membership check in
    /// [`CacheEngine::is_evicted`] is a lock-free load (#1789 item 5) and
    /// readers never block on the rarer writer. Unlike [`Self::denied`] and
    /// [`Self::chain_denied`], which are replaced wholesale on a config reload,
    /// this set may only grow: an operator eviction is a takedown, and a hash
    /// that stopped being served must not start again. [`MonotoneHashSet`]
    /// carries that difference.
    evicted: MonotoneHashSet,
    /// Held hashes whose own serve export failed bao validation against the
    /// content root: the stored bytes or outboard diverged after admission
    /// (disk rot or tampering). [`CacheEngine::refuses`] honors membership, so
    /// the node stops serving, announcing, and re-acquiring the hash while the
    /// corrupt entry is on disk. The quarantine drops the protecting tags, so
    /// the next GC sweep reclaims the entry. The membership lifts on the next
    /// lookup or origin rescan that finds the store no longer holds the hash
    /// (see [`CacheEngine::is_quarantined`]). A later pull-through then admits
    /// a freshly verified copy.
    ///
    /// In memory only. A restart before the sweep forgets the entry, and the
    /// next serve of the corrupt bytes trips the validation and quarantines it
    /// again. A durable record is unnecessary: the validated export aborts
    /// every serve at the first mismatching chunk group, so no buyer pays for
    /// a corrupt group in the window.
    quarantined: DashMap<Hash, ()>,
    /// Probe-triggered eviction holds (#318, ADR 005 §Probe-triggered
    /// eviction hold). Maps a held hash to its hold *expiry* instant; a
    /// held hash is invisible to [`CacheEngine::eviction_candidates`] until
    /// expiry, composing *above* the LRU layer. Holds are per-blob, not
    /// per-probe: many peers probing the same hash share (and refresh) one
    /// entry, so the slot count is bounded by distinct held blobs, not
    /// probe volume. Expired entries are swept lazily on every hold
    /// admission, every `eviction_candidates` call, and every
    /// [`CacheEngine::probe_hold_slots_used`] read (no background task).
    /// Expiries run on the tokio clock so a paused test clock drives them.
    ///
    /// Like [`Self::pinned`] this only blocks *LRU* eviction — an explicit
    /// operator [`CacheEngine::evict`] still wins (ADR 040 §Pinning, durable
    /// operator-evict, and the probe-hold stay engine-enforced: DMCA always
    /// wins), enforced
    /// because [`CacheEngine::try_probe_hold`] gates on [`CacheEngine::has`]
    /// which already
    /// honors the evicted set.
    probe_holds: Mutex<HashMap<Hash, tokio::time::Instant>>,
    /// Hold-budget cap (ADR 005 §Hold budget). `0` disables `has_blob: true`
    /// entirely. Set once from `cache.max_probe_holds` via
    /// [`CacheEngine::set_max_probe_holds`] at runtime bring-up — `cache.*`
    /// is restart-required (not hot-reloaded), so an atomic written once is
    /// sufficient and avoids threading the value through every `open_*`
    /// constructor and its many test call sites.
    max_probe_holds: AtomicUsize,
    /// Optional shared frequency signal (ADR 040). `None` for the `lru`/`always`
    /// default — the observe call is skipped, so recency-only pays nothing.
    frequency: ArcSwap<Option<Arc<dyn crate::policy::FrequencyEstimator>>>,
    /// Admission policy consulted at store-time (ADR 040). Always present —
    /// defaults to [`crate::policy::AlwaysAdmit`] (store to
    /// [`crate::policy::Segment::Main`]), so behavior is unchanged until an
    /// operator selects a different policy. `ArcSwap<Arc<dyn Trait>>` rather
    /// than `ArcSwap<dyn Trait>`: the latter needs `RefCnt: Sized`, which
    /// `arc-swap` 1.9.2 does not give a `dyn` trait object.
    admission: ArcSwap<Arc<dyn crate::policy::AdmissionPolicy>>,
    /// Generic `hash -> Segment` membership the engine tracks with no meaning
    /// attached (ADR 040 §1). Admission stores the label it chose; the eviction
    /// policy reads and moves it at the sweep. Pure in-memory metadata — the
    /// blob's normal commit tag still provides GC protection, so no tag I/O is
    /// tied to it. Only non-default (`Probation`) entries are stored; an absent
    /// hash reads back as [`crate::policy::Segment::Main`], so under the default
    /// `AlwaysAdmit` (always `Main`) the map stays empty and inert.
    segments: Mutex<HashMap<Hash, crate::policy::Segment>>,
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
    /// Range-aware in-flight fill registry (ADR 038). Coalesces concurrent
    /// serve-misses for the same hash: a request whose range an in-flight pull
    /// already covers attaches an observer instead of opening a duplicate pull,
    /// and a partial overlap opens a pull for only its remainder. Also holds the
    /// per-hash captured outboard every serve leg reads. Purely synchronous range
    /// math; holds no blob bytes. Driven through [`CacheEngine::claim_fill`].
    fill_registry: Arc<FillRegistry>,
    /// Bounds the [`CacheEngine::origin_range_wire`] encodes that run at once
    /// (#2065); each holds a permit for its whole span and `O(window + outboard)`
    /// bytes, so the bound caps their sum.
    own_origin_range_pulls: Arc<tokio::sync::Semaphore>,
    /// Per configured origin (same index as `origins`): whether the origin has
    /// served a clean first range window. Set on the first such window, by the
    /// serviceability probe ([`CacheEngine::origin_range_serviceable`]) or a
    /// draw, and never cleared, so the probe's range half reads nothing for a
    /// confirmed origin. An origin that later stops serving ranges fails its
    /// draw like any other mid-life origin fault.
    origin_range_confirmed: Box<[AtomicBool]>,
    /// Origin `{H}.obao4` outboards already read, keyed by hash, tagged with
    /// the serving origin, and bounded by bytes. The serviceability probe's
    /// outboard half ([`CacheEngine::origin_fetch_outboard_bytes`]) and every
    /// [`CacheEngine::origin_range_wire`] draw read it and fill it. Each
    /// [`OriginRangeWire`] holds a handle and evicts the copy it used when the
    /// range fails bao verification.
    outboards: OutboardCache,
    /// One async lock per hash whose outboard an origin read is fetching, so
    /// concurrent cold misses of one hash wait for the first read and reuse its
    /// cached copy instead of each downloading it. Held as `Weak`: an entry
    /// lives while a fetch holds its lock, and dead entries are pruned on the
    /// next lookup.
    outboard_flights: Mutex<HashMap<Hash, Weak<tokio::sync::Mutex<()>>>>,
    /// Head start, in milliseconds, of the time budget for one origin read of
    /// the range-pull path. `0` means no budget. Set once at bring-up by
    /// [`CacheEngine::set_origin_read_budget`].
    origin_read_head_start_ms: AtomicU64,
    /// Throughput floor, in bytes per second, that the rest of that budget
    /// scales with. `0` means no budget.
    origin_read_min_bps: AtomicU64,
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
    /// upstream nodes — the hazard the in-flight coalescing map exists to prevent.
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

// `PinnedHashes` and `PinDiff` moved to the `decdn-config-types` leaf
// crate (#578) and are imported above. The engine holds its pinned set
// internally as `HashSet<Hash>` (the iroh-blobs store hash) and converts
// at the public boundary via `to_store_hash` / `from_store_hash`.

/// Append-only set of hashes with a lock-free read path.
///
/// Every published snapshot is a superset of its predecessor. That is the
/// property [`CacheEngine::is_evicted`] relies on: a takedown observed once is
/// observed by every later reader, so a lock-free read can never answer
/// not-evicted for a hash the operator has already evicted.
///
/// Writers serialize on `writer`, so the publish is a single uncontended
/// `Arc` clone-and-swap rather than a CAS retry loop that re-clones the whole
/// set on every lost race. The clone itself is O(n) in the set's size, which is
/// the cost this shape accepts to keep the read side free — writes are operator
/// takedowns and blacklist enforcement, reads are on every serve.
#[derive(Debug)]
struct MonotoneHashSet {
    snapshot: ArcSwap<HashSet<Hash>>,
    /// Serializes writers so the read-modify-write below is atomic: without it
    /// the cap check and the "was it already present" answer are both racy.
    writer: std::sync::Mutex<()>,
}

impl MonotoneHashSet {
    fn new(initial: HashSet<Hash>) -> Self {
        Self {
            snapshot: ArcSwap::from(Arc::new(initial)),
            writer: std::sync::Mutex::new(()),
        }
    }

    fn contains(&self, hash: Hash) -> bool {
        self.snapshot.load().contains(&hash)
    }

    /// The current snapshot, for a caller that needs several questions answered
    /// against one consistent view.
    fn snapshot(&self) -> arc_swap::Guard<Arc<HashSet<Hash>>> {
        self.snapshot.load()
    }

    /// Add `hash` unless the set already holds it or is at `cap`. Returns
    /// `false` only when `cap` would be exceeded — an already-present hash is
    /// a success, since the set already says what the caller wants it to say.
    ///
    /// Atomic with respect to other writers, so two concurrent inserts cannot
    /// both read a set one below `cap` and both land.
    fn insert_if_absent(&self, hash: Hash, cap: usize) -> bool {
        let _writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let current = self.snapshot.load();
        if current.contains(&hash) {
            return true;
        }
        if current.len() >= cap {
            return false;
        }
        let mut next = HashSet::clone(&current);
        next.insert(hash);
        drop(current);
        self.snapshot.store(Arc::new(next));
        true
    }
}

/// Serve-path verdict for one hash, from a single store `status()` call.
///
/// Answers in one store contact what the delivery path asks in two —
/// [`CacheEngine::has`] for presence and [`CacheEngine::inspect`] for size
/// (#1789 item 7 part B). The variants are the delivery path's own branches,
/// so a size is reachable only where it is serveable and a refusal can never
/// be paired with a wire size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServeAudit {
    /// The store reports the blob complete and no gate refuses it (denied /
    /// chain-denied / evicted) — the same condition [`CacheEngine::has`]
    /// reports. `size` is the whole-blob wire size, and `0` means a genuinely
    /// empty blob.
    Serveable {
        /// Whole-blob byte size to advertise on the wire.
        size: u64,
    },
    /// Nothing serveable from the local store: the blob is absent, partial, or
    /// refused by a gate.
    Unavailable {
        /// Whether this node withdrew the hash on its own authority: an
        /// operator eviction ([`CacheEngine::is_evicted`], #279) or a
        /// stored-corruption quarantine ([`CacheEngine::is_quarantined`]). The
        /// miss path must not fill a withdrawn hash: a fill of an evicted hash
        /// would undo the takedown, and a fill of a quarantined hash would land
        /// on the corrupt entry and pay for bytes it can never serve. A
        /// quarantined hash becomes fillable again only after GC reclaims the
        /// entry and the quarantine lifts; an evicted hash never does.
        withdrawn: bool,
    },
}

impl ServeAudit {
    /// Wire size for a warm hit; `None` when the delivery path must fill and
    /// then size the blob itself.
    #[must_use]
    pub const fn hit_size(&self) -> Option<u64> {
        match self {
            Self::Serveable { size } => Some(*size),
            Self::Unavailable { .. } => None,
        }
    }

    /// Whether the blob can be served from the local store right now.
    #[must_use]
    pub const fn is_serveable(&self) -> bool {
        matches!(self, Self::Serveable { .. })
    }

    /// Whether this node withdrew the hash: evicted or quarantined. `false`
    /// for a serveable hash, since both are gates that refuse a serve.
    #[must_use]
    pub const fn is_withdrawn(&self) -> bool {
        matches!(self, Self::Unavailable { withdrawn: true })
    }
}

/// Read-only snapshot of a hash's local-cache state, returned by
/// [`CacheEngine::inspect`]. Backs `decdn node evict --dry-run`
/// (issue #379): operators running DMCA takedowns want to confirm the blob's size, last-access
/// time, pin status, and already-evicted flag before mutating state.
///
/// All fields reflect the *underlying* cache state — `size_bytes` reads
/// from the iroh-blobs store directly, so a hash that has already been
/// logically evicted (and whose bytes are still on disk pending the
/// follow-up GC sweep in #518) still reports its size here. That keeps
/// dry-run honest about disk reclaim potential rather than hiding it once
/// the operator has flipped the evicted flag. For a complete blob the size
/// is its byte length; for a partial blob it is the validated total (see
/// [`Self::size_bytes`]), and [`CacheEngine::size_snapshot`] gives the bytes
/// it holds on disk.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EvictionPreview {
    /// Bytes the iroh-blobs store reports for this hash. `None` when the
    /// blob isn't in the store; matches `BlobStatus::NotFound`. A partial
    /// blob reports `BlobStatus::Partial { size }` as is: the whole blob's
    /// size once its last chunk validates, `None` before. That is the blob's
    /// total, not the bytes it holds on disk — the delivery path signs it as
    /// the blob's total and the probe path advertises it, and
    /// [`CacheEngine::size_snapshot`] is the on-disk measure.
    pub size_bytes: Option<u64>,
    /// Microseconds elapsed since the last recorded access to the blob: a
    /// serve, a hit, or a fill. `None` when the blob has had no access since
    /// open — including a blob on disk at open that nothing has touched since
    /// (its open-time recency seed is not an access) — or when eviction or
    /// quarantine has cleared its entry.
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

/// The chunk ranges of a hash currently present in the store.
///
/// This is the presence oracle the range-aware cache API builds on: `read`
/// serves only present ranges, resume re-fetches only missing ones, and disk
/// admission accounts present bytes. Absent and logically-evicted hashes report
/// empty + not-complete, matching [`CacheEngine::has`]'s eviction masking.
#[derive(Debug, Clone)]
pub struct PresentRanges {
    ranges: ChunkRanges,
    complete: bool,
    size: u64,
}

impl PresentRanges {
    fn absent() -> Self {
        Self {
            ranges: ChunkRanges::empty(),
            complete: false,
            size: 0,
        }
    }

    /// The whole blob is present.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.complete
    }

    /// No range of this hash is present (absent or evicted).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// The present ranges, in chunk units.
    #[must_use]
    pub const fn chunk_ranges(&self) -> &ChunkRanges {
        &self.ranges
    }

    /// The blob's total size in bytes, from the `observe()` bitfield.
    ///
    /// Unlike `status()`, which iroh-blobs leaves unknown for a partial blob
    /// until its LAST chunk validates, the bitfield knows the full size as
    /// soon as any chunk carries it (a front-prefix partial with no tail
    /// still reports it here). `0` for an absent/evicted/refused hash.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }
}

/// Bytes of a `size`-byte blob that `ranges` marks present.
///
/// `ranges` counts 1 KiB chunks ([`bao_tree::ChunkNum`]). An open span ends at
/// `size`: iroh-blobs gives a bitfield that holds the blob's last chunk an open
/// end ([`ChunkRanges::all`] once complete). The `.min(size)` clamp bounds every
/// other span and counts a short last chunk at its true length. iroh-blobs'
/// `Bitfield::total_bytes` computes the same sum with unchecked subtraction,
/// which underflows on a span that starts past `size`.
fn present_byte_count(ranges: &ChunkRanges, size: u64) -> u64 {
    const CHUNK_BYTES: u64 = 1024;
    let byte = |chunk: &bao_tree::ChunkNum| chunk.0.saturating_mul(CHUNK_BYTES).min(size);
    ranges.boundaries().chunks(2).fold(0u64, |acc, span| {
        let start = span.first().map_or(size, byte);
        let end = span.get(1).map_or(size, byte);
        acc.saturating_add(end.saturating_sub(start))
    })
}

/// The chunk span of discovery block `index` of a `size`-byte blob.
///
/// One bao leaf chunk is 1024 bytes ([`bao_tree::ChunkNum`]'s unit), so one
/// 64 MiB [`decdn_protocol::discovery_block_bytes`] block is 65536 chunks. The
/// last block ends at the blob's last chunk. The block size is the same
/// accessor [`decdn_protocol::num_blocks`] reads, so the two agree when a test
/// overrides it.
fn discovery_block_span(index: u32, size: u64) -> ChunkRanges {
    const BAO_CHUNK_BYTES: u64 = 1024;
    let chunks_per_block = decdn_protocol::discovery_block_bytes() / BAO_CHUNK_BYTES;
    let total_chunks = size.div_ceil(BAO_CHUNK_BYTES);
    let start = u64::from(index) * chunks_per_block;
    let end = (start + chunks_per_block).min(total_chunks);
    ChunkRanges::from(bao_tree::ChunkNum(start)..bao_tree::ChunkNum(end))
}

/// Indices of the discovery blocks of a `size`-byte blob that `present` fully
/// covers. A block with any missing chunk is not covered.
fn covered_blocks(size: u64, present: &ChunkRanges) -> impl Iterator<Item = u32> + '_ {
    // `span - present` is empty iff `present` fully contains `span`, i.e.
    // every chunk in this block is on disk.
    (0..decdn_protocol::num_blocks(size))
        .filter(move |&i| (discovery_block_span(i, size) - present).is_empty())
}

/// Bytes of `hash` present on disk, from its `observe()` bitfield.
///
/// `status()` cannot size a partial blob: iroh-blobs leaves the size unknown
/// until the last chunk validates, then reports the whole blob's size.
/// Awaiting `observe()` returns only its first item, the current bitfield,
/// which the store sends at once, so the call never waits on a later write. A
/// hash that GC removes after its `status()` reads as 0 bytes.
async fn observed_present_bytes(
    blobs: &iroh_blobs::api::blobs::Blobs,
    hash: Hash,
) -> CacheResult<u64> {
    let bitfield = blobs.observe(hash).await.map_err(|e| {
        CacheError::Store(anyhow::Error::from(e).context(format!("observe partial {hash}")))
    })?;
    Ok(present_byte_count(&bitfield.ranges, bitfield.size()))
}

/// How long a partial blob's present-byte count in [`PartialSizeMemo`] stays
/// fresh.
///
/// Each `observe()` of an idle partial loads its store entry, and the entry's
/// idle shutdown fsyncs its data, outboard and sizes files and rewrites its
/// bitfield file. The eviction driver walks the store every tick (1 s by
/// default), so the walk re-observes a partial at most once per TTL. A growing
/// fill reads up to one TTL of progress low.
const PARTIAL_SIZE_TTL: Duration = Duration::from_secs(30);

/// Present-byte count of each partial blob, with the instant the walk
/// measured it. [`snapshot_blob_sizes`] reads and refreshes it; the eviction
/// driver's walk and the GC callback's walk share one memo.
type PartialSizeMemo = Mutex<HashMap<Hash, (u64, Instant)>>;

/// Snapshot of access times for blobs that are eligible for LRU
/// eviction — i.e. **pinned hashes are already excluded**. Returned by
/// [`CacheEngine::eviction_candidates`].
///
/// The one carve-out is a deny-listed hash: it stays a candidate even when
/// pinned ("deny wins over pin"), so the space path reclaims an unservable
/// blob instead of holding it on disk.
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

    /// Wrap a hand-built map so policy tests can construct a candidate set
    /// without running a real sweep.
    #[cfg(test)]
    #[must_use]
    pub const fn from_map_for_test(map: HashMap<Hash, Instant>) -> Self {
        Self(map)
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
    partial_sizes: Arc<PartialSizeMemo>,
    metrics: Option<Arc<CacheMetrics>>,
) {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        gc_protect_inner(store_handle, prev_pre_sweep, partial_sizes, metrics).await;
        let _ = tx.send(());
    });
    if let Err(err) = rx.await {
        // The spawned task panicked or was dropped before sending.
        // Surface it: a panic in `gc_protect_inner` would otherwise be
        // invisible (this cb just returns `Continue` either way).
        tracing::warn!(error = %err, "gc protect spawn dropped without completing; metrics may have skipped a cycle");
    }
}

async fn gc_protect_inner(
    store_handle: Weak<OnceLock<FsStore>>,
    prev_pre_sweep: Arc<Mutex<HashMap<Hash, u64>>>,
    partial_sizes: Arc<PartialSizeMemo>,
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

    let current = match snapshot_blob_sizes(
        store,
        &partial_sizes,
        Instant::now(),
        metrics.as_deref(),
    )
    .await
    {
        Ok(snap) => snap,
        Err(err) => {
            tracing::warn!(error = %err.display_chain(), "gc snapshot failed; skipping reclaim attribution this cycle");
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
        // - `access_times` has no entry here: it is a `DashMap`, so no
        //   poisoning decision exists to make. A panic while one of its shards
        //   is locked leaves that shard usable, and every reader and writer
        //   takes one shard at a time.
        // - `origin_probe_memo` recovers silently via `probe_memo_lock`.
        // - `partial_sizes` recovers silently: it caches store reads, and the
        //   next walk re-observes any entry it lost.
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
/// (#518) and by [`CacheEngine::size_snapshot`]. Hashes that race the
/// snapshot (deleted between `list` and `status`) report
/// `BlobStatus::NotFound` and are dropped — they cannot have contributed
/// bytes either way.
///
/// A `Partial` blob reports the bytes its bitfield marks present
/// ([`observed_present_bytes`]), not `status()`'s size. Including
/// partials matters: the threat model that motivated #518 is exactly
/// the partial-import case (`add_stream` errored mid-flight, the
/// `TempTag` was dropped, but the bytes already on disk are what we
/// want GC to reclaim). A partial that GC removes between `status` and
/// `observe` stays in the map at 0 bytes.
///
/// `memo` holds each partial's last count. A count younger than
/// [`PARTIAL_SIZE_TTL`] at `now` is reused without an `observe()`. A failed
/// `observe()` falls back to the last count, else to `status()`'s size, and
/// the fallback is memoized for one TTL so a dead store entry is retried and
/// logged once per TTL, not every walk. Each failure ticks
/// `partial_size_observe_failures`. Only a `list` or `status` failure, which
/// is store-wide, fails the walk. The walk drops memo entries for hashes that
/// are no longer partial.
async fn snapshot_blob_sizes(
    store: &FsStore,
    memo: &PartialSizeMemo,
    now: Instant,
    metrics: Option<&CacheMetrics>,
) -> CacheResult<HashMap<Hash, u64>> {
    let blobs = store.blobs();
    let mut stream = blobs
        .list()
        .stream()
        .await
        .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
    let mut out = HashMap::new();
    let mut partials = HashSet::new();
    while let Some(hash) = stream.next().await {
        let hash = hash.map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        let status = blobs
            .status(hash)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        let size = match status {
            iroh_blobs::api::blobs::BlobStatus::NotFound => continue,
            iroh_blobs::api::blobs::BlobStatus::Partial { size } => {
                partials.insert(hash);
                partial_size(blobs, hash, size, memo, now, metrics).await
            }
            iroh_blobs::api::blobs::BlobStatus::Complete { size } => size,
        };
        out.insert(hash, size);
    }
    memo.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .retain(|hash, _| partials.contains(hash));
    Ok(out)
}

/// One partial blob's entry in [`snapshot_blob_sizes`]: the memoized count
/// while fresh, else a new `observe()`, else a fallback. `status_size` is
/// `status()`'s size for the blob. See [`snapshot_blob_sizes`] for the rules.
async fn partial_size(
    blobs: &iroh_blobs::api::blobs::Blobs,
    hash: Hash,
    status_size: Option<u64>,
    memo: &PartialSizeMemo,
    now: Instant,
    metrics: Option<&CacheMetrics>,
) -> u64 {
    let cached = memo
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&hash)
        .copied();
    if let Some((bytes, at)) = cached
        && now.saturating_duration_since(at) < PARTIAL_SIZE_TTL
    {
        return bytes;
    }
    let bytes = match observed_present_bytes(blobs, hash).await {
        Ok(bytes) => bytes,
        Err(err) => {
            let fallback = cached.map_or_else(|| status_size.unwrap_or(0), |(bytes, _)| bytes);
            tracing::warn!(
                %hash,
                error = %err.display_chain(),
                fallback,
                "partial blob size: observe failed; using the last count or the status() size"
            );
            if let Some(m) = metrics {
                m.partial_size_observe_failures.inc();
            }
            fallback
        }
    };
    memo.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(hash, (bytes, now));
    bytes
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

/// One origin's answer to an outboard read, after the exact-length gate.
enum GatedOutboard {
    /// The origin served an outboard of the exact length.
    Found(Bytes),
    /// The origin has no outboard for the hash, or does not publish outboards.
    Declined,
    /// The origin served an outboard of the wrong length, which cannot verify.
    WrongLength,
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
        let partial_sizes: Arc<PartialSizeMemo> = Arc::new(Mutex::new(HashMap::new()));

        let mut options = FsStoreOptions::new(cache_dir);
        if let Some(strong) = gc_store_handle.as_ref() {
            let store_weak: Weak<OnceLock<FsStore>> = Arc::downgrade(strong);
            let prev_for_cb = Arc::clone(&prev_pre_sweep);
            let sizes_for_cb = Arc::clone(&partial_sizes);
            let metrics_for_cb = metrics.clone();
            options.gc = Some(GcConfig {
                interval: gc_interval,
                add_protected: Some(Arc::new(move |_live: &mut HashSet<Hash>| {
                    let store_weak = store_weak.clone();
                    let prev_for_cb = Arc::clone(&prev_for_cb);
                    let sizes_for_cb = Arc::clone(&sizes_for_cb);
                    let metrics_for_cb = metrics_for_cb.clone();
                    Box::pin(async move {
                        gc_protect_callback(store_weak, prev_for_cb, sizes_for_cb, metrics_for_cb)
                            .await;
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
        let origin_range_confirmed = origins.iter().map(|_| AtomicBool::new(false)).collect();

        // `checked_sub` fails only within `COLD_SEED_AGE` of the monotonic
        // clock's origin (near boot). Seeding at `opened` then still sorts at or
        // before every later access.
        let opened = Instant::now();
        let cold_seed_at = opened.checked_sub(COLD_SEED_AGE).unwrap_or(opened);

        let engine = Self {
            inner: Arc::new(Inner {
                store,
                origins,
                breakers,
                max_blob_bytes,
                access_times: DashMap::new(),
                cold_seed_at,
                partial_protected: DashMap::new(),
                partial_sizes,
                inflight: Mutex::new(HashMap::new()),
                inflight_poison_logged: AtomicBool::new(false),
                pinned: ArcSwap::from(Arc::new(
                    pinned
                        .iter()
                        .map(|h| to_store_hash(*h))
                        .collect::<HashSet<Hash>>(),
                )),
                origin_held: ArcSwap::from(Arc::new(OriginHeldIndex::default())),
                rescan_slot: AtomicU8::new(RESCAN_IDLE),
                origin_probe_memo: Mutex::new(OriginProbeMemo::default()),
                denied: ArcSwap::from(Arc::new(HashSet::new())),
                chain_denied: ArcSwap::from(Arc::new(HashSet::new())),
                evicted: MonotoneHashSet::new(evicted),
                quarantined: DashMap::new(),
                probe_holds: Mutex::new(HashMap::new()),
                max_probe_holds: AtomicUsize::new(crate::probe_hold::DEFAULT_MAX_PROBE_HOLDS),
                frequency: ArcSwap::from_pointee(None),
                admission: ArcSwap::from_pointee(
                    Arc::new(crate::policy::AlwaysAdmit) as Arc<dyn crate::policy::AdmissionPolicy>
                ),
                segments: Mutex::new(HashMap::new()),
                evicted_log_path,
                retry_policy,
                fill_registry: Arc::new(FillRegistry::with_metrics(metrics.clone())),
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
                own_origin_range_pulls: Arc::new(tokio::sync::Semaphore::new(
                    MAX_CONCURRENT_RANGE_PULLS,
                )),
                origin_range_confirmed,
                outboards: OutboardCache::new(OUTBOARD_CACHE_BYTES, OUTBOARD_CACHE_ENTRIES),
                outboard_flights: Mutex::new(HashMap::new()),
                origin_read_head_start_ms: AtomicU64::new(0),
                origin_read_min_bps: AtomicU64::new(0),
            }),
        };
        engine.seed_access_times_from_store().await;
        Ok(engine)
    }

    /// Enter every blob already on disk into `access_times` at
    /// [`Inner::cold_seed_at`], so eviction can release content from before a
    /// restart without waiting for traffic to touch it.
    ///
    /// The seed runs before `open` returns the engine, so no access can land
    /// first; `or_insert` is defensive. The walk is [`Self::iter_hashes`], which
    /// skips refused hashes. At open only the durable evicted set is populated,
    /// so operator-evicted hashes stay out and every other stored blob enters
    /// the map. [`Self::eviction_candidates`] then filters pinned and probe-held
    /// hashes as for any other entry.
    ///
    /// A failed walk logs a WARN, increments `recency_seed_failures`, and leaves
    /// the map unseeded. Open still succeeds and the cache still serves every
    /// blob, but until the next restart eviction sees only blobs accessed after
    /// open.
    async fn seed_access_times_from_store(&self) {
        let hashes = match self.iter_hashes().await {
            Ok(hashes) => hashes,
            Err(err) => {
                if let Some(m) = &self.inner.metrics {
                    m.recency_seed_failures.inc();
                }
                tracing::warn!(
                    error = %err.display_chain(),
                    "cache open: store walk failed; blobs from before this start are not \
                     eviction candidates until accessed, until the next restart \
                     (alert on decdn_cache_recency_seed_failures_total)"
                );
                return;
            }
        };
        let seeded = hashes.len();
        let seed = self.inner.cold_seed_at;
        for hash in hashes {
            self.inner.access_times.entry(hash).or_insert(seed);
        }
        tracing::info!(seeded, "cache open: seeded eviction recency from the store");
    }

    /// Peek the in-flight fill registry for a LIVE fill of `hash`, returning its
    /// blob `total_bytes` if one runs. ADVISORY: the serve-miss path uses it to skip
    /// the upstream header handshake when it will coalesce onto a live pull, but the
    /// authoritative own-vs-attach decision stays in [`Self::claim_fill`]. See
    /// [`FillRegistry::in_flight_total`].
    #[must_use]
    pub fn in_flight_total(&self, hash: Hash) -> Option<u64> {
        self.inner.fill_registry.in_flight_total(hash)
    }

    /// Atomically claim a serve-miss of `[offset, offset+len)` (`len == 0` = to end)
    /// of the `total`-byte blob `hash`: decide attach/own/mixed AND register any new
    /// owner session under one map-lock acquisition (no plan-then-register TOCTOU
    /// race). Returns [`FillClaim::Attach`] to coalesce wholly onto a live pull,
    /// [`FillClaim::Owner`] with a freshly-registered session covering the whole
    /// request, or [`FillClaim::Mixed`] to own a pull for a contiguous remainder while
    /// attaching a sibling for the overlap. `make_session` builds the session only on
    /// an owning branch (never built-and-dropped on a pure attach). See
    /// [`FillRegistry::claim`].
    pub fn claim_fill(
        &self,
        hash: Hash,
        offset: u64,
        len: u64,
        total: u64,
        make_session: impl FnOnce() -> Arc<FillSession>,
    ) -> FillClaim {
        self.inner
            .fill_registry
            .claim(hash, offset, len, total, make_session)
    }

    /// Subscribe to a stream of `Hash`es announcing hashes as they become
    /// advertisable in the local store: a whole blob that the private
    /// `pull_through` committed, or a ranged admit ([`Self::admit_bao`],
    /// [`Self::admit_bao_stream`]) that touched a fully present discovery
    /// block, whether the admit succeeded or failed partway. Used by the DHT republish scheduler (ADR
    /// 022 §STORE Flow — "When a node verifies its first 64 MiB block of blob
    /// H") to send the first publish-set to the K+3 closest peers.
    ///
    /// One hash can arrive more than once — a ranged fill announces once per
    /// block it completes — so a consumer dedupes against its own state.
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
    /// **Cost:** O(n) — this allocates a fresh `HashSet` and converts
    /// every hash across the store↔leaf boundary. Call it off the hot
    /// path (it backs SIGHUP reload logging and admin snapshots, not
    /// per-request lookups).
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
    /// A candidate whose probe *faults* — a transport error, or the walk
    /// overrunning the probe timeout — keeps whatever size the previous index
    /// held for it instead of being dropped alongside the genuinely-absent. A
    /// fault is not an authoritative absence, and dropping on one shrinks the
    /// announce set for a whole rescan interval on a throttle window the origin
    /// recovers from in seconds. An origin that faults indefinitely therefore
    /// keeps a carried entry for as long as it keeps faulting; that is the
    /// intended trade, and the serve path answers from the live origin either
    /// way. Only a *retry-eligible* fault carries forward — a permanent one (a
    /// revoked ACL, a symlink escape) reads the same on every pass, so carrying
    /// it would advertise content this node can never serve until an operator
    /// intervenes. Faults are counted
    /// on `decdn_cache_origin_probe_failures_total` and reported to the DHT seed
    /// paths through [`Self::origin_held_snapshot`].
    ///
    /// An origin whose `enumerate` fails is the coarser version of the same
    /// thing: it contributes no candidates, so its hashes leave the index with
    /// no per-hash fault and nothing to carry forward. Only operator pins naming
    /// them survive. That leg counts on
    /// `decdn_cache_origin_enumerate_failures_total` and rides the same
    /// snapshot.
    ///
    /// Every adapter reserves [`Origin::size`]'s `Ok(None)` for an
    /// authoritative absence (a genuine 404): an HTTP or S3 5xx/408/429
    /// surfaces as a transient fault and carries forward, a permission decline
    /// as a permanent one — so no outage is dropped here as an absence.
    ///
    /// **Cost:** one origin probe per deduped candidate — every listed entry and
    /// every pin, including the ones that resolve absent and never enter the
    /// index. A local `metadata()` stat per fs entry, one HTTP `HEAD` / S3
    /// `HeadObject` per remote one. Runs off the hot path at the configured
    /// rescan cadence (startup / interval / reload), never per request.
    ///
    /// One pass at a time, with any number of triggers arriving during a pass
    /// collapsing into a single rerun. A trigger that finds a pass in flight
    /// returns immediately rather than awaiting it, so a walk that outruns the
    /// cadence cannot accumulate a backlog of waiters — and one rerun covers
    /// every trigger it coalesced, because the next pass re-derives everything
    /// from scratch.
    pub async fn rescan_origins(&self) {
        if !self.claim_rescan_slot() {
            // A pass owns the slot and will take another one for this request.
            return;
        }
        // RAII: a panic inside the walk would otherwise strand the slot and
        // disable every later rescan for the process lifetime.
        let mut guard = RescanSlotGuard {
            slot: &self.inner.rescan_slot,
            armed: true,
        };
        loop {
            self.rescan_once().await;
            if self
                .inner
                .rescan_slot
                .compare_exchange(
                    RESCAN_RUNNING,
                    RESCAN_IDLE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                // Released cleanly with nothing queued. The guard must not store
                // again: a trigger may already have claimed the slot.
                guard.armed = false;
                return;
            }
            // The CAS can only fail because a trigger arrived, so consume it and
            // take another pass.
            self.inner
                .rescan_slot
                .store(RESCAN_RUNNING, Ordering::Release);
        }
    }

    /// Take the rescan slot, or register a rerun behind whoever holds it.
    ///
    /// Returns whether the caller owns the slot and must do the work. A request
    /// is never lost: it either claims the slot or moves it to `RESCAN_QUEUED`,
    /// and the running pass's release is a compare-exchange that fails if one
    /// landed first.
    fn claim_rescan_slot(&self) -> bool {
        loop {
            match self.inner.rescan_slot.compare_exchange_weak(
                RESCAN_IDLE,
                RESCAN_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(RESCAN_RUNNING) => {
                    if self
                        .inner
                        .rescan_slot
                        .compare_exchange_weak(
                            RESCAN_RUNNING,
                            RESCAN_QUEUED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return false;
                    }
                }
                // Already queued — that pass has not taken its snapshot yet, so
                // it covers this request too.
                Err(RESCAN_QUEUED) => return false,
                // Spurious failure or a state change under us; re-read and retry.
                Err(_) => {}
            }
        }
    }

    /// One rescan pass: gather candidates, resolve them, publish the index.
    async fn rescan_once(&self) {
        self.lift_reclaimed_quarantines().await;
        let (candidates, enumerate_failures) = self.rescan_candidates().await;
        let RescanResolution {
            held,
            faults,
            carried,
        } = self.resolve_candidates(candidates).await;

        let count = held.len();
        self.inner.origin_held.store(Arc::new(OriginHeldIndex {
            held,
            probe_faults: faults,
            enumerate_failures,
        }));
        if let Some(m) = &self.inner.metrics {
            if faults > 0 {
                m.origin_probe_failures.inc_by(faults);
            }
            if enumerate_failures > 0 {
                m.origin_enumerate_failures.inc_by(enumerate_failures);
            }
        }
        if faults > 0 {
            tracing::warn!(
                faults,
                carried,
                count,
                "rescan_origins: origin size probes faulted; carried the previous \
                 index entry forward where there was one. Candidates with no \
                 previous entry are absent from the announce set until a rescan \
                 resolves them"
            );
        }
        tracing::debug!(count, "rescan_origins: refreshed origin-held index");
    }

    /// Every hash [`Self::rescan_origins`] considers: each enumerable origin's
    /// listing plus the operator pin set.
    ///
    /// The pin set is snapshotted here rather than read inside the probe loop,
    /// so its `ArcSwap` guard is not held across a `size()` await.
    ///
    /// Also returns how many origins failed to enumerate. Such an origin
    /// contributes nothing this pass — its hashes are never probed, so they
    /// carry no per-hash fault and simply leave the index. Only the operator
    /// pins naming them survive.
    async fn rescan_candidates(&self) -> (Vec<Hash>, u64) {
        let mut candidates: Vec<Hash> = Vec::new();
        let mut enumerate_failures = 0u64;
        for origin in &self.inner.origins {
            match origin.enumerate().await {
                Ok(hashes) => candidates.extend(hashes),
                Err(err) => {
                    enumerate_failures = enumerate_failures.saturating_add(1);
                    tracing::warn!(
                        origin = ?origin.kind(),
                        error = %format_args!("{err:#}"),
                        "rescan_origins: enumerate failed; every hash discoverable \
                         only through this origin leaves the announce set until a \
                         later rescan lists it"
                    );
                }
            }
        }
        candidates.extend(self.inner.pinned.load().iter().copied());
        (candidates, enumerate_failures)
    }

    /// Resolve each candidate against the origin chain for
    /// [`Self::rescan_origins`], deduping so a hash is probed once.
    ///
    /// One local stat (fs) or one `HEAD` (pinned remote) per deduped candidate,
    /// through [`Self::probe_origin_chain_classified`] rather than
    /// [`Self::origin_size`], which folds a transport error into "origin doesn't
    /// have it".
    ///
    /// The probe memo is deliberately bypassed: an enumeration is as large as
    /// the origin's listing, and the memo is a bounded map that evicts an
    /// arbitrary live entry once full, so walking a rescan through it would push
    /// out the serve path's warm answers. The store walk in the DHT seed does go
    /// through the memo, because it asks the same question the serve gate asks
    /// about the same hashes moments later.
    ///
    /// Each candidate is bounded by [`RESCAN_PROBE_TIMEOUT`], not by
    /// `cache.origin_probe_timeout_ms` — that knob exists to stop a slow origin
    /// stalling the serve hot path, and a bulk walk answering nothing on the
    /// critical path has no reason to be that impatient. A candidate that
    /// overruns is a fault like any other, so borrowing the tighter budget would
    /// turn a merely slow origin into an empty index on a cold boot.
    async fn resolve_candidates(&self, candidates: Vec<Hash>) -> RescanResolution {
        // `load_full` rather than holding the `ArcSwap` guard: the guard would
        // be held across every probe await below, which is the long-lived-guard
        // case arc-swap tells callers to avoid.
        let previous = self.inner.origin_held.load_full();
        let mut out = RescanResolution::default();
        // Dedupe on a `seen` set, not on `held`: a candidate that resolves
        // `Absent`, or faults with nothing to carry forward, never lands in
        // `held`, and every origin's listing is concatenated with the pin set —
        // so keying off `held` re-probes those hashes once per duplicate and
        // counts one fault per probe.
        let mut seen: HashSet<Hash> = HashSet::new();
        for hash in candidates {
            if self.refuses(hash) || !seen.insert(hash) {
                continue;
            }
            match self
                .probe_origin_chain_classified(hash, RESCAN_PROBE_TIMEOUT)
                .await
            {
                (OriginPresence::Present(size), _) => {
                    out.held.insert(hash, size);
                }
                (OriginPresence::Absent, _) => {}
                (OriginPresence::Fault, transient) => {
                    out.faults = out.faults.saturating_add(1);
                    // Carry forward only what a later pass might resolve. A
                    // permanent fault — a revoked ACL, a symlink escape — will
                    // read the same way on every rescan, so carrying it keeps the
                    // hash advertised for the process lifetime while the serve
                    // path refuses every request for it.
                    if !transient {
                        continue;
                    }
                    if let Some(size) = previous.held.get(&hash).copied() {
                        out.held.insert(hash, size);
                        out.carried = out.carried.saturating_add(1);
                    }
                }
            }
        }
        out
    }

    /// The hashes in the origin-held index (#1130) together with what the rescan
    /// that built it could not resolve, read from one load of the index.
    ///
    /// The only way to read the announce set, deliberately: the origin-held half
    /// of a DHT seed has no other error channel. A probe fault either carries an
    /// older entry forward or leaves the candidate out of the set, a failed
    /// enumeration drops a whole origin's listing, and the seed cannot tell
    /// either from an origin that genuinely stopped holding the content.
    /// Returning the counts with the hashes — from one load, so a truncated set
    /// can never be paired with a later healthy rescan's counts — is what stops a
    /// caller from reporting a short set as a complete one.
    ///
    /// `hashes` is filtered through the **live** [`Self::refuses`] set: the index
    /// is only a per-rescan snapshot, so a hash blacklisted / evicted / denied
    /// *after* the last rescan is still in it — but must never be announced. The
    /// live filter closes that window without waiting for the next rescan.
    pub fn origin_held_snapshot(&self) -> OriginHeldReport {
        let index = self.inner.origin_held.load();
        OriginHeldReport {
            hashes: index
                .held
                .keys()
                .copied()
                .filter(|h| !self.refuses(*h))
                .collect(),
            probe_faults: index.probe_faults,
            enumerate_failures: index.enumerate_failures,
        }
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
        self.inner.origin_held.load().held.get(&hash).copied()
    }

    /// Total byte size of `hash` if a configured origin can serve it, resolved
    /// by a **live** `HEAD`/`HeadObject`/stat and memoised (#1130 pt3). This is
    /// the per-probe fallback for the http/s3 discovery gap: [`Self::rescan_origins`]
    /// can only index what an origin `enumerate`s, and http/s3 enumerate to
    /// nothing, so a non-pinned bucket object is absent from
    /// [`Self::origin_held_size`]. Callers should consult the in-memory index
    /// first (zero I/O) and only fall back here on its miss.
    ///
    /// A thin wrapper over [`Self::origin_probe_presence`]: `Present(size)`
    /// advertises `Some(size)`, and both `Absent` and `Fault` advertise `None`
    /// — a probing caller (the `probe` handler's `has_blob`, DHT announce)
    /// never distinguishes "genuinely missing" from "backend unreachable
    /// right now"; either way it must not advertise the blob.
    ///
    /// **Never fetches the body** — existence and size only.
    pub async fn origin_probe_size(&self, hash: Hash) -> Option<u64> {
        match self.origin_probe_presence(hash).await {
            OriginPresence::Present(size) => Some(size),
            OriginPresence::Absent | OriginPresence::Fault => None,
        }
    }

    /// Walk the origin chain for `hash`, distinguishing a genuine negative from
    /// a backend fault, with no memo read or write.
    ///
    /// Deliberately not routed through [`Self::origin_size`]: that helper's
    /// per-origin error handling is swallow-and-advance, so other callers can
    /// fall through a dead origin to a live one, and its terminal `Ok(None)`
    /// folds a fault into a negative. A fault-aware caller needs the raw
    /// per-origin outcomes instead.
    ///
    /// - ANY origin answering `Ok(Some(size))` is `Present(size)` — the first
    ///   such answer wins, same order as [`Self::origin_size`];
    /// - failing that, ANY origin answering `Err(_)` (a transport error), or
    ///   the whole walk overrunning `timeout`, is `Fault`;
    /// - only when every configured origin answered `Ok(None)` — or no origin
    ///   is configured at all, a node that genuinely holds nothing — is the
    ///   answer `Absent`. This is deliberately the lowest-precedence outcome:
    ///   if at least one origin is unreachable and none confirmed the object,
    ///   the honest answer is "unknown", never an authoritative `NotFound`.
    ///
    /// One `timeout` covers the whole chain rather than resetting per origin,
    /// so a chain of slow origins cannot add up past the caller's budget.
    ///
    /// **Never fetches the body** — existence and size only.
    async fn probe_origin_chain(&self, hash: Hash, timeout: Duration) -> OriginPresence {
        self.probe_origin_chain_classified(hash, timeout).await.0
    }

    /// [`Self::probe_origin_chain`] plus whether a `Fault` is worth waiting out.
    ///
    /// The second element is meaningful only for `Fault`, and is `true` when at
    /// least one origin failed with a retry-eligible error (or the walk hit its
    /// ceiling). A `Fault` built only from
    /// [`OriginPullError::Permanent`] — a symlink escape, a permission denied,
    /// an HTTP 4xx — will still be there on the next pass and the one after, so
    /// a caller that would otherwise hold state open on the strength of "this
    /// might come back" must not.
    async fn probe_origin_chain_classified(
        &self,
        hash: Hash,
        timeout: Duration,
    ) -> (OriginPresence, bool) {
        let walk = async {
            let mut any_fault = false;
            let mut any_transient = false;
            for origin in &self.inner.origins {
                match origin.size(hash).await {
                    Ok(Some(size)) => return (OriginPresence::Present(size), false),
                    Ok(None) => {}
                    Err(e) => {
                        tracing::debug!(
                            %hash,
                            kind = ?origin.kind(),
                            error = %format_args!("{e:#}"),
                            transient = e.is_transient(),
                            "origin-probe HEAD faulted; checking remaining origins",
                        );
                        any_fault = true;
                        any_transient |= e.is_transient();
                    }
                }
            }
            if any_fault {
                (OriginPresence::Fault, any_transient)
            } else {
                (OriginPresence::Absent, false)
            }
        };
        match tokio::time::timeout(timeout, walk).await {
            Ok(outcome) => outcome,
            // A ceiling the origin overran says nothing about whether it would
            // answer given longer, so treat it as the waitable kind.
            Err(_) => (OriginPresence::Fault, true),
        }
    }

    /// Live existence probe against the configured origins, distinguishing a
    /// genuine negative from a backend fault (#1766). This is the primitive
    /// behind [`Self::origin_probe_size`]; callers that must NOT treat a fault
    /// as an authoritative absence (the origin-only serve gate) use this
    /// directly instead of the size-only wrapper.
    ///
    /// A memo hit returns with no I/O. On a miss the origin chain is walked
    /// under a `cache.origin_probe_timeout_ms` ceiling — deliberately not via
    /// [`Self::origin_size`], whose per-origin error handling is
    /// swallow-and-advance so other callers can fall through a dead origin to a
    /// live one — and this layer decides what to remember:
    ///
    /// - `Present(size)` is memoised under the positive TTL;
    /// - `Fault` is memoised under the fault TTL (#1789 item 6). The memo is
    ///   keyed per hash, so this bounds repeat probes OF THE SAME HASH to one
    ///   live `HEAD` per fault TTL — the common shape when a client retries a
    ///   request against a failing origin. It does not bound namespace-wide
    ///   load: during an outage, N distinct hashes still cost N live `HEAD`s
    ///   per window. The TTL is graded against its neighbours and the config
    ///   resolver enforces `negative <= fault <= positive`: patient enough that
    ///   a retried hash is not re-probed as often as an absence, eager enough
    ///   that a recovered origin is noticed soon — a memoised fault costs
    ///   client-visible availability, since the origin-only serve gate answers
    ///   `InternalError` and the probe/DHT paths report this node holds nothing
    ///   for as long as it stands.
    /// - `Absent` — every configured origin answered `Ok(None)`, or none is
    ///   configured at all (a node with nothing configured genuinely holds
    ///   nothing, which is not transient) — is memoised under the short
    ///   negative TTL. Every adapter reserves `Ok(None)` for an authoritative
    ///   absence (a genuine 404) — an outage or a permission decline is an
    ///   `Err`, which reaches the `Fault` arm instead — so this arm never
    ///   caches a 5xx as an absence.
    ///
    /// **Never fetches the body** — existence and size only.
    pub async fn origin_probe_presence(&self, hash: Hash) -> OriginPresence {
        if self.refuses(hash) {
            return OriginPresence::Absent;
        }
        let now = Instant::now();
        let timeout = {
            let mut memo = self.probe_memo_lock();
            if let Some(presence) = memo.get(hash, now) {
                return presence.into();
            }
            memo.timeout()
        };
        // Live probe off the memo lock (never hold it across the await).
        let presence = self.probe_origin_chain(hash, timeout).await;
        let memo_presence = match presence {
            OriginPresence::Present(size) => Presence::Present(size),
            OriginPresence::Absent => Presence::Absent,
            OriginPresence::Fault => Presence::Fault,
        };
        if matches!(presence, OriginPresence::Fault) {
            // Warn rather than debug: the memo answers the repeats, so this
            // fires at most once per hash per fault TTL — and it is the only
            // record
            // that this node is about to refuse serves and answer probes with
            // "not held" for content it may well hold. The per-origin errors
            // behind the fault stay at debug.
            tracing::warn!(
                %hash,
                fault_ttl_secs = self.probe_memo_lock().fault_ttl().as_secs(),
                "origin probe faulted; memoising the fault — serves for this hash \
                 answer InternalError and probes answer not-held until it expires"
            );
        }
        self.probe_memo_lock().insert(hash, memo_presence, now);
        presence
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
    pub fn set_origin_probe_config(&self, policy: OriginProbePolicy) {
        *self.probe_memo_lock() = OriginProbeMemo::new(policy);
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
    ///
    /// A [quarantined](Self::is_quarantined) hash is refused too. Its stored
    /// bytes failed validation, so a serve or an announce only fails every
    /// buyer, and a re-acquisition only lands on the corrupt entry until GC
    /// reclaims it.
    pub fn refuses(&self, hash: Hash) -> bool {
        self.is_denied(hash)
            || self.is_chain_denied(hash)
            || self.is_evicted(hash)
            || self.is_quarantined(hash)
    }

    /// Is this blob already present in the local store?
    ///
    /// Returns `Ok(false)` when the hash has been logically evicted (issue
    /// #279) even if the underlying store still holds the bytes — operators
    /// who call `evict` expect the node to stop serving immediately, so
    /// `has` reports the blob as absent.
    pub async fn has(&self, hash: Hash) -> CacheResult<bool> {
        self.lift_reclaimed_quarantine(hash).await;
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

    /// Serve-path presence + size audit for `hash` in ONE store actor call.
    ///
    /// Answers from a single `BlobStatus` what the delivery path otherwise asks
    /// in two hops — [`Self::has`] for presence, then [`Self::inspect`] for size
    /// (#1789 item 7, part B) — saving one store round-trip on every cache hit.
    /// Presence matches [`Self::has`] exactly: the blob must be `Complete` and
    /// no gate may refuse it, so a logically-evicted hash is
    /// [`ServeAudit::Unavailable`] even while the store still holds its bytes.
    ///
    /// Unlike [`Self::inspect`], a partial blob reports no size here. `inspect`
    /// exists to tell an operator how much disk a partial pull occupies; this
    /// audit answers what may go on the wire, and a partial blob's byte count
    /// is not that.
    pub async fn serve_audit(&self, hash: Hash) -> CacheResult<ServeAudit> {
        self.lift_reclaimed_quarantine(hash).await;
        let withdrawn = self.is_evicted(hash) || self.is_quarantined(hash);
        let refused = self.refuses(hash);
        let status = self
            .inner
            .store
            .blobs()
            .status(hash)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        match status {
            iroh_blobs::api::blobs::BlobStatus::Complete { size } if !refused => {
                Ok(ServeAudit::Serveable { size })
            }
            iroh_blobs::api::blobs::BlobStatus::Complete { .. }
            | iroh_blobs::api::blobs::BlobStatus::NotFound
            | iroh_blobs::api::blobs::BlobStatus::Partial { .. } => {
                Ok(ServeAudit::Unavailable { withdrawn })
            }
        }
    }

    /// Which chunk ranges of `hash` are present on disk right now.
    ///
    /// Classifies via `status()` first (so an absent hash returns immediately),
    /// then snapshots the current bitfield by awaiting `observe` directly — the
    /// FIRST `observe` item is the current state. It deliberately does NOT call
    /// `ObserveProgress::await_completion`, which blocks until the blob is
    /// *complete* and would hang forever on a partial or absent blob.
    /// Logically-evicted hashes report absent, mirroring [`Self::has`]. Pure
    /// query: does not touch access times and changes no serving behavior.
    pub async fn present_ranges(&self, hash: Hash) -> CacheResult<PresentRanges> {
        if self.refuses(hash) {
            return Ok(PresentRanges::absent());
        }
        // Resolve absence without `observe`: a hash the store has never seen has
        // no defined current bitfield to await, and a size-0 bitfield reports
        // `is_complete() == true` vacuously — both are wrong answers here.
        let status = self
            .inner
            .store
            .blobs()
            .status(hash)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        if matches!(status, iroh_blobs::api::blobs::BlobStatus::NotFound) {
            return Ok(PresentRanges::absent());
        }
        // The blob exists (Partial or Complete): its current bitfield is the
        // first item `observe` yields, available immediately.
        let bitfield = self
            .inner
            .store
            .blobs()
            .observe(hash)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        Ok(PresentRanges {
            complete: bitfield.is_complete(),
            size: bitfield.size(),
            ranges: bitfield.ranges,
        })
    }

    /// A live watch of which chunk ranges of `hash` are present, for progressive
    /// serve-while-filling (#1621). Yields the current bitfield's
    /// [`bao_tree::ChunkRanges`] first, then further updates as the blob fills.
    ///
    /// Mirrors [`Self::present_ranges`]'s guards (refuse an evicted/blacklisted
    /// hash, gate a never-seen hash via `status()` before observing — a hash
    /// with no defined current state has nothing to watch), but stays a live
    /// stream instead of a point-in-time snapshot. Deliberately uses
    /// `ObserveProgress::stream()`, NEVER `await_completion`, which blocks
    /// until the blob is complete and would hang forever on a partial blob.
    pub async fn observe_present_ranges(
        &self,
        hash: Hash,
    ) -> CacheResult<Pin<Box<dyn futures_util::Stream<Item = bao_tree::ChunkRanges> + Send>>> {
        use futures_util::StreamExt;
        // Same guards as `present_ranges`: never observe an evicted/blacklisted
        // hash, and gate a never-seen hash (observe has no defined current state).
        if self.refuses(hash) {
            return Err(CacheError::Store(anyhow::anyhow!(
                "observe_present_ranges: hash is evicted/blacklisted"
            )));
        }
        let status = self
            .inner
            .store
            .blobs()
            .status(hash)
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        if matches!(status, iroh_blobs::api::blobs::BlobStatus::NotFound) {
            return Err(CacheError::Store(anyhow::anyhow!(
                "observe_present_ranges: blob not present"
            )));
        }
        // `.stream()` yields the current bitfield first, then updates. NEVER
        // `.await_completion()` — it loops until complete and hangs on a partial.
        let stream = self
            .inner
            .store
            .blobs()
            .observe(hash)
            .stream()
            .await
            .map_err(|e| CacheError::Store(anyhow::Error::from(e)))?;
        Ok(Box::pin(stream.map(|bf| bf.ranges)))
    }

    /// Which [`decdn_protocol::DISCOVERY_BLOCK_BYTES`] discovery blocks of
    /// `hash` this node can serve from its own cache right now.
    ///
    /// Cached blocks only — an origin-backed node that could re-pull the
    /// whole blob on demand does NOT get all-ones here; that capability-aware
    /// widening is the probe handler's job (it alone knows whether `hash` has
    /// a configured origin), not this cache-only derivation's. Absent,
    /// evicted, or otherwise refused hashes report [`decdn_protocol::Coverage::empty`],
    /// mirroring [`Self::present_ranges`]'s guards.
    ///
    /// A block is covered iff every chunk in its byte span is present: this
    /// reads the blob's size from the `observe()` bitfield (via
    /// [`Self::present_ranges`]), then diffs each block's chunk range against
    /// the present ranges the same way [`Self::missing_ranges`] diffs a
    /// requested range — a block with any missing chunk is not covered. The
    /// bitfield knows the size as soon as any chunk carries it, so a
    /// front-prefix partial with no validated tail still reports its covered
    /// blocks (where `status()` would leave the size unknown until the last
    /// chunk). A `NotFound`, evicted, or refused hash reports no blocks.
    pub async fn coverage(&self, hash: Hash) -> CacheResult<decdn_protocol::Coverage> {
        Ok(self.coverage_sized(hash).await?.0)
    }

    /// [`Self::coverage`] plus the blob size it was derived from, so a partial
    /// holder can advertise its size alongside its blocks. The size is `None`
    /// exactly when the coverage is empty for want of a known size: an absent,
    /// evicted, or refused hash.
    ///
    /// The size comes from the same `observe()` bitfield read as the coverage,
    /// so the pair costs one read and cannot disagree.
    pub async fn coverage_sized(
        &self,
        hash: Hash,
    ) -> CacheResult<(decdn_protocol::Coverage, Option<u64>)> {
        if self.refuses(hash) {
            return Ok((decdn_protocol::Coverage::empty(), None));
        }
        // The size comes from the `observe()` bitfield (via `present_ranges`),
        // not `status()`: iroh-blobs leaves a partial blob's size unknown
        // until its LAST chunk validates, but the bitfield already knows it
        // as soon as any chunk carries it — so a front-prefix partial with no
        // tail still derives a non-empty coverage here. `present_ranges`
        // already handles the absent/evicted guards (size 0 there too).
        let present = self.present_ranges(hash).await?;
        let size = present.size();
        let coverage = decdn_protocol::Coverage::from_block_indices(
            decdn_protocol::num_blocks(size),
            covered_blocks(size, present.chunk_ranges()),
        );
        Ok((coverage, (size > 0).then_some(size)))
    }

    /// Announce `hash` on [`Self::subscribe_inserts`] when the admit of
    /// `admitted` completed at least one discovery block — a block that
    /// `admitted` touches and that is now fully present (ADR 022 §STORE Flow:
    /// a partial holder publishes once it verifies a 64 MiB block).
    ///
    /// One admit fills a window, not a block, so most admits touch no fully
    /// present block and announce nothing. A front-to-back fill announces about
    /// once per block, and the republisher publishes eagerly only once.
    ///
    /// Fails open: when the coverage query faults, it announces anyway. No
    /// later event may name this hash — a blob under 64 MiB has one block, and
    /// a finished fill admits nothing more — so a dropped announcement would
    /// last until the next lag sweep or restart. The consumer reads coverage
    /// itself and publishes nothing for a hash that covers no block. With no
    /// subscriber (no DHT task) it skips the store query entirely.
    async fn announce_completed_blocks(&self, hash: Hash, admitted: &ChunkRanges) {
        if self.inner.inserts_tx.receiver_count() == 0 {
            return;
        }
        let present = match self.present_ranges(hash).await {
            Ok(present) => present,
            Err(err) => {
                tracing::warn!(
                    %hash,
                    error = %err.display_chain(),
                    "admit: coverage query failed; announcing the hash so the republisher \
                     decides from its own coverage read"
                );
                let _ = self.inner.inserts_tx.send(hash);
                return;
            }
        };
        let size = present.size();
        let completed = covered_blocks(size, present.chunk_ranges())
            .any(|i| !discovery_block_span(i, size).is_disjoint(admitted));
        if completed {
            // `Err` only means no subscriber is left; nothing to announce to.
            let _ = self.inner.inserts_tx.send(hash);
        }
    }

    /// The chunk-aligned sub-ranges of `[byte_offset, byte_offset + byte_len)`
    /// (`byte_len == 0` = to `blob_size`, an end past `blob_size` clamped to
    /// it) that are NOT present on disk.
    ///
    /// Empty ⇒ the requested span is fully present: a completeness-aware read can
    /// serve it with no fetch, and a resumed pull is a no-op. `blob_size` is
    /// caller-supplied (the signed `total_bytes`), the same contract as
    /// [`Self::export_bao_range_stream`].
    pub async fn missing_ranges(
        &self,
        hash: Hash,
        byte_offset: u64,
        byte_len: u64,
        blob_size: u64,
    ) -> CacheResult<ChunkRanges> {
        // Same align_range_clamped error mapping as `export_bao_range_stream`:
        // only a `byte_offset` at or past `blob_size` is an argument error, not an
        // origin fault; an end past `blob_size` clamps rather than erroring, so a
        // partial holder does not refuse its own already-complete partial hit.
        let aligned = align_range_clamped(byte_offset, byte_len, blob_size).map_err(|e| {
            CacheError::Store(anyhow::Error::from(e).context("missing_ranges: range alignment"))
        })?;
        let present = self.present_ranges(hash).await?;
        // requested \ present. `ChunkRanges` is a `RangeSet2`, which implements
        // `Sub` (`owned - &ref -> owned`).
        Ok(aligned.chunk_ranges().clone() - present.chunk_ranges())
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
    /// (issue #279) needs for use cases like DMCA takedown. A stored hash
    /// mismatch that a serve detects does not come here: it takes the
    /// non-durable [quarantine](Self::is_quarantined), because a corrupt
    /// local copy must stay re-acquirable.
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
    /// `spawn_blocking` thread rather than on the async runtime (#845), so a
    /// caller on a request-serving worker does not stall on disk I/O.
    pub async fn evict(&self, hash: Hash) -> CacheResult<()> {
        // Lock-free pre-check on a single snapshot: short-circuit on
        // already-evicted (a sequential repeat-evict of the same hash returns
        // here and never re-appends) and reject on cap (DoS bound on an
        // unbounded public-ish surface). Both checks are advisory against
        // concurrency — the snapshot is read before the durable append below —
        // and `insert_if_absent` re-decides both under the write lock, so the
        // cap is exact and a race on the same new hash appends at most one
        // duplicate `evicted.log` line (harmless: replay folds the log into a
        // `HashSet`).
        let evicted = self.inner.evicted.snapshot();
        if evicted.contains(&hash) {
            return Ok(());
        }
        if evicted.len() >= MAX_EVICTED_ENTRIES {
            return Err(CacheError::EvictionLimitExceeded {
                limit: MAX_EVICTED_ENTRIES,
            });
        }
        drop(evicted);

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

        // Commit to the in-memory set (#1789 item 5). The publish is a single
        // atomic swap of a superset, so no reader can observe `Ok(())` here
        // with the set still excluding `hash`, and the read side never has a
        // lock that could be poisoned into a "still serving" fallback.
        if !self
            .inner
            .evicted
            .insert_if_absent(hash, MAX_EVICTED_ENTRIES)
        {
            return Err(CacheError::EvictionLimitExceeded {
                limit: MAX_EVICTED_ENTRIES,
            });
        }
        self.inner.access_times.remove(&hash);
        self.inner
            .segments
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
                    error = %err.display_chain(),
                    "evict: failed to drop protecting tags; bytes stay GC-protected (not auto-retried)",
                );
            }
        }
        // Operator-evict (DMCA/corruption) counter — distinct from the LRU
        // `evictions` counter the eviction driver bumps (#1173, ADR 040). Bumped after the
        // durable append + logical-set commit succeeded above, so the count
        // tracks takedowns that actually stopped serving.
        if let Some(m) = &self.inner.metrics {
            m.evicted_operator.inc();
        }
        Ok(())
    }

    /// Protect a partial (range-admitted) blob from GC by giving its raw hash a
    /// deterministic named tag. Idempotent — `set` overwrites the same name, so
    /// re-admitting more ranges never proliferates tags. The `decdn-partial-`
    /// prefix is opaque to iroh-blobs; `drop_named_tags_for` (evict) matches by
    /// hash and removes it. A GC sweep in the sub-second window between
    /// `import_bao_bytes` and this `set` is bounded by the GC interval and
    /// self-heals on the next admit. #1607.
    ///
    /// The tag only needs to EXIST, so once this process has written it (tracked
    /// in `partial_protected`) the store write is skipped — a single fill admits
    /// many ranges, and only the first need pay the tag write. The memo is cleared
    /// whenever the tag is dropped (`drop_named_tags_for`), so a re-admit after an
    /// eviction re-protects.
    ///
    /// The memo is a best-effort optimization, not a memo↔tag invariant: the
    /// `contains_key`/`insert` here and the `remove` in `drop_named_tags_for` are
    /// not atomic against the store, so a `protect_partial` racing a concurrent
    /// evict of the same hash can leave the memo set while the tag was deleted.
    /// That never affects served correctness — served bytes are hash-verified, and
    /// an operator takedown blocks serving through the logical evicted set, not
    /// through this tag (see `evict`). Its only cost is that such a partial may go
    /// unprotected and be GC-reclaimed, which self-corrects on the next pull.
    /// Concurrent first-admits merely repeat one idempotent `set`.
    ///
    /// A [quarantined](Self::is_quarantined) hash gets no tag: the tag would
    /// protect the corrupt entry from the GC sweep that ends the quarantine. A
    /// fill already past this check when the quarantine starts can still write
    /// the tag. The origin rescan drops it again
    /// ([`Self::lift_reclaimed_quarantines`]).
    async fn protect_partial(&self, hash: Hash) -> CacheResult<()> {
        if self.is_quarantined(hash) || self.inner.partial_protected.contains_key(&hash) {
            return Ok(());
        }
        let name = format!("decdn-partial-{hash}");
        self.inner
            .store
            .tags()
            .set(name.as_bytes(), HashAndFormat::raw(hash))
            .await
            .map_err(|e| {
                CacheError::Store(anyhow::Error::from(e).context("protect_partial: tags().set"))
            })?;
        self.inner.partial_protected.insert(hash, ());
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
        // Invalidate the partial-protection memo before touching the store, so a
        // later admit re-issues the protecting tag rather than trusting a stale
        // "already protected" entry for the tag we are removing. Clearing it up
        // front (not after the deletes) also means a delete failure mid-loop still
        // leaves the memo clear, so a re-admit re-protects. This is best-effort,
        // not atomic against a concurrent `protect_partial` of the same hash — the
        // race is benign for the reasons documented on `protect_partial`.
        self.inner.partial_protected.remove(&hash);
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
    /// The three in-memory probes are each an O(1) lookup and none of them
    /// blocks: `evicted` and `pinned` are `ArcSwap` loads and `access_times` is
    /// one `DashMap` shard. Same pattern as [`Self::eviction_candidates`].
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
    /// Lock-free (#1789 item 5): one `ArcSwap` load and a hash-set probe, so
    /// the serve-path gate takes no mutex and has no lock-poisoning failure
    /// mode that could answer not-evicted for a taken-down hash. The write side
    /// only ever publishes a superset, so an evicted hash is observed evicted
    /// by every reader that linearizes after the swap.
    pub fn is_evicted(&self, hash: Hash) -> bool {
        self.inner.evicted.contains(hash)
    }

    /// Is this hash quarantined because its stored bytes failed validation on
    /// a serve export?
    ///
    /// A hash mismatch or a short read over held content in
    /// [`Self::export_bao_range_stream`] or [`Self::outboard_pairs`] quarantines
    /// the hash. [`Self::refuses`] then withholds it from serving, announcing,
    /// and re-acquisition. The engine drops its protecting tags, even when the
    /// hash is pinned: the pin does not keep corrupt bytes from GC. The
    /// quarantine lifts on the next [`Self::has`], [`Self::serve_audit`], or
    /// origin rescan that finds the store no longer holds the hash. The quarantine is in memory only.
    pub fn is_quarantined(&self, hash: Hash) -> bool {
        self.inner.quarantined.contains_key(&hash)
    }

    /// Quarantine a held `hash` whose serve export failed bao validation
    /// against the content root.
    ///
    /// The store bytes or outboard diverged after admission, so every later
    /// serve fails the same way. The quarantine makes [`Self::refuses`]
    /// withhold the hash from serving, announcing, and re-acquisition. It
    /// drops the protecting named tags, so the next GC sweep reclaims the
    /// entry, and it forgets the access-time and segment entries. The tags
    /// drop for a pinned hash too: a pin protects content, and these bytes
    /// are not that content. [`Self::lift_reclaimed_quarantine`] ends the
    /// quarantine when the store no longer holds the hash.
    ///
    /// Idempotent: only the first trip per hash counts
    /// `held_corruption_quarantined` and walks the tag store. A tag-drop
    /// failure counts `tag_drop_failures` and leaves the bytes GC-protected
    /// until [`Self::lift_reclaimed_quarantines`] retries the drop on the next
    /// origin rescan. The hash stays refused regardless.
    async fn quarantine_corrupt(&self, hash: Hash) {
        if self.inner.quarantined.insert(hash, ()).is_some() {
            return;
        }
        if let Some(m) = &self.inner.metrics {
            m.held_corruption_quarantined.inc();
        }
        if self.inner.gc_store_handle.is_some() {
            tracing::warn!(
                %hash,
                "held blob failed bao validation on export; quarantined it and released it to GC"
            );
        } else {
            tracing::warn!(
                %hash,
                "held blob failed bao validation on export; quarantined it. GC is disabled \
                 (cache.gc_interval_sec = 0), so the corrupt bytes stay on disk and the hash \
                 stays withdrawn until a restart"
            );
        }
        self.inner.access_times.remove(&hash);
        self.inner
            .segments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&hash);
        self.drop_quarantined_tags(hash).await;
    }

    /// Drop the protecting tags of quarantined `hash`. Returns how many tags
    /// were dropped; a failure counts `tag_drop_failures`, logs, and returns 0.
    async fn drop_quarantined_tags(&self, hash: Hash) -> u64 {
        match self.drop_named_tags_for(hash).await {
            Ok(dropped) => dropped,
            Err(err) => {
                if let Some(m) = &self.inner.metrics {
                    m.tag_drop_failures.inc();
                }
                tracing::warn!(
                    %hash,
                    error = %err.display_chain(),
                    "quarantine: dropping the protecting tags failed; the corrupt bytes stay on \
                     disk until the next origin rescan retries"
                );
                0
            }
        }
    }

    /// End the quarantine on `hash` when the store no longer holds it. Returns
    /// `true` when `hash` is not quarantined after the call.
    ///
    /// GC reclaims a quarantined entry because [`Self::quarantine_corrupt`]
    /// dropped its tags. After the sweep nothing corrupt remains, so a
    /// pull-through may admit a freshly verified copy. A store fault keeps the
    /// quarantine, and a later call retries.
    async fn lift_reclaimed_quarantine(&self, hash: Hash) -> bool {
        if !self.is_quarantined(hash) {
            return true;
        }
        match self.inner.store.blobs().status(hash).await {
            Ok(iroh_blobs::api::blobs::BlobStatus::NotFound) => {
                self.inner.quarantined.remove(&hash);
                tracing::info!(%hash, "quarantined blob reclaimed; lifted the quarantine");
                true
            }
            Ok(_) => false,
            Err(err) => {
                tracing::debug!(
                    %hash,
                    error = %err,
                    "quarantine: store status failed; the quarantine stays"
                );
                false
            }
        }
    }

    /// Lift every quarantine whose entry GC has reclaimed, and drop the tags
    /// again for every quarantined entry the store still holds.
    ///
    /// The periodic origin rescan calls this, so a reclaimed hash rejoins the
    /// origin-held announce set even when no request touches it. The second
    /// tag drop covers a fill or pull-through that was already past its
    /// quarantine check when the quarantine started and then protected the
    /// corrupt entry. Without it, that tag would keep the entry from GC and the
    /// quarantine would never lift.
    async fn lift_reclaimed_quarantines(&self) {
        let quarantined: Vec<Hash> = self.inner.quarantined.iter().map(|e| *e.key()).collect();
        for hash in quarantined {
            if self.lift_reclaimed_quarantine(hash).await {
                continue;
            }
            let dropped = self.drop_quarantined_tags(hash).await;
            if dropped > 0 {
                tracing::warn!(
                    %hash,
                    dropped,
                    "quarantine: a fill re-protected the corrupt entry; dropped its tags again"
                );
            }
        }
    }

    /// Quarantine `hash` when the export error `cause` proves that held content
    /// diverged from the root.
    ///
    /// Three errors can prove it:
    /// - `LeafHashMismatch` and `ParentHashMismatch` fail the hash check.
    /// - `Io` with `UnexpectedEof` means a data or outboard file is shorter
    ///   than the size the store recorded.
    ///
    /// Each counts only over content the store holds. A partial blob's data
    /// file is sparse, so an absent range reads back as zeros or ends early,
    /// and fails exactly like corruption. A complete blob holds every range,
    /// so any of the three errors quarantines it. A partial blob is
    /// quarantined only when the failing chunks lie inside its present ranges.
    /// A hash mismatch names the failing leaf or node. A short read names no
    /// location, so it counts only when the whole `requested` export range is
    /// present. Every other error is a store fault or an absent blob, not
    /// corruption.
    async fn quarantine_on_mismatch(
        &self,
        hash: Hash,
        cause: &bao_tree::io::EncodeError,
        requested: &ChunkRanges,
    ) {
        use bao_tree::ChunkNum;
        use bao_tree::io::EncodeError;

        let chunk_log = decdn_bao_range::IROH_BLOCK_SIZE.chunk_log();
        // Leaf and node ranges are in 1 KiB chunks, the unit of `ChunkRanges`.
        let failing = match cause {
            EncodeError::LeafHashMismatch(start) => {
                ChunkRanges::from(*start..ChunkNum(start.0.saturating_add(1 << chunk_log)))
            }
            EncodeError::ParentHashMismatch(node) => ChunkRanges::from(node.chunk_range()),
            EncodeError::Io(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                requested.clone()
            }
            _ => return,
        };
        let Ok(present) = self.present_ranges(hash).await else {
            return;
        };
        let held = present.is_complete() || {
            // Clip the failing groups to the blob end: the last group is short.
            let failing = failing & ChunkRanges::from(..ChunkNum::chunks(present.size()));
            !failing.is_empty() && (failing - present.chunk_ranges()).is_empty()
        };
        if held {
            self.quarantine_corrupt(hash).await;
        }
    }

    /// Set the probe-hold budget cap from `cache.max_probe_holds` (ADR 005
    /// §Hold budget, #318). Called once by the runtime at bring-up. `0`
    /// disables the hold path so [`Self::try_probe_hold`] returns
    /// [`ProbeHoldOutcome::HoldsDisabled`] for any present blob (the node then
    /// answers `has_blob: false` to every probe).
    pub fn set_max_probe_holds(&self, max: usize) {
        self.inner.max_probe_holds.store(max, Ordering::Relaxed);
    }

    /// Set the time budget for each origin read of the range-pull path — one
    /// `{H}.obao4` fetch or one data window (ADR 037). Called once by the
    /// runtime at bring-up, from `cache.node_pull_stall_window_sec` and
    /// `cache.node_pull_min_throughput_bps`. A read of `len` bytes gets
    /// `head_start + len / min_bps`, so the budget scales with the read and
    /// never caps the blob size: the origin must only sustain `min_bps` on
    /// average after the head start. A read past its budget fails as an origin
    /// transport fault and bumps `origin_range_timeouts`, so a stuck origin ends
    /// the fill instead of parking it until the client gives up.
    ///
    /// A zero `head_start` or a zero `min_bps` leaves the reads unbounded. A
    /// whole-body read has no stream to watch for idle gaps, so there is no
    /// idle-only form of the budget. A non-zero `head_start` below one
    /// millisecond rounds up to one millisecond.
    pub fn set_origin_read_budget(&self, head_start: Duration, min_bps: u64) {
        let ms = if head_start.is_zero() {
            0
        } else {
            u64::try_from(head_start.as_millis())
                .unwrap_or(u64::MAX)
                .max(1)
        };
        self.inner
            .origin_read_head_start_ms
            .store(ms, Ordering::Relaxed);
        self.inner
            .origin_read_min_bps
            .store(min_bps, Ordering::Relaxed);
    }

    /// The budget [`Self::set_origin_read_budget`] set, or `None` for none.
    fn origin_read_budget(&self) -> Option<OriginReadBudget> {
        let ms = self.inner.origin_read_head_start_ms.load(Ordering::Relaxed);
        let min_bps = self.inner.origin_read_min_bps.load(Ordering::Relaxed);
        (ms > 0 && min_bps > 0).then(|| OriginReadBudget {
            head_start: Duration::from_millis(ms),
            min_bps,
        })
    }

    /// The cached outboard for `hash`, if it has the length a `total_bytes`-byte
    /// blob needs.
    #[cfg(test)]
    fn cached_outboard(&self, hash: Hash, total_bytes: u64) -> Option<Bytes> {
        self.inner
            .outboards
            .get(hash, expected_outboard_len(total_bytes))
            .map(|c| c.bytes)
    }

    /// The async lock that serializes origin outboard reads of `hash`
    /// ([`Inner::outboard_flights`]). The caller locks it, re-checks the cache,
    /// and only then reads from the origin.
    fn outboard_flight(&self, hash: Hash) -> Arc<tokio::sync::Mutex<()>> {
        let mut flights = self
            .inner
            .outboard_flights
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(live) = flights.get(&hash).and_then(Weak::upgrade) {
            return live;
        }
        flights.retain(|_, w| w.strong_count() > 0);
        let flight = Arc::new(tokio::sync::Mutex::new(()));
        flights.insert(hash, Arc::downgrade(&flight));
        flight
    }

    /// The origin read budget as `(head_start, min_bps)`, or `None` when
    /// [`Self::set_origin_read_budget`] left the reads unbounded. For the
    /// runtime's wiring tests.
    #[must_use]
    pub fn origin_read_budget_parts(&self) -> Option<(Duration, u64)> {
        self.origin_read_budget().map(|b| (b.head_start, b.min_bps))
    }

    /// [`Self::fetch_gated_outboard`] under `hash`'s flight lock: a concurrent
    /// draw that already cached this origin's copy answers from the cache.
    async fn flighted_outboard(
        &self,
        origin_ix: usize,
        origin: &Arc<dyn Origin>,
        hash: Hash,
        expected_len: u64,
    ) -> CacheResult<GatedOutboard> {
        let flight = self.outboard_flight(hash);
        let _reading = flight.lock().await;
        match self.inner.outboards.get(hash, expected_len) {
            Some(c) if c.origin_ix == origin_ix => Ok(GatedOutboard::Found(c.bytes)),
            _ => {
                self.fetch_gated_outboard(origin_ix, origin, hash, expected_len)
                    .await
            }
        }
    }

    /// Read `hash`'s outboard from the origin at `origin_ix` under the read
    /// budget and gate it on `expected_len`. A copy of that exact length is
    /// cached, tagged with `origin_ix`. Meters every outboard byte the origin
    /// sends as `pull_through_bytes`, before the gate, because that egress is
    /// paid either way. Logs a transport fault or timeout at `warn`.
    ///
    /// # Errors
    ///
    /// [`CacheError::OriginError`] for a transport fault or a read past the
    /// budget.
    async fn fetch_gated_outboard(
        &self,
        origin_ix: usize,
        origin: &Arc<dyn Origin>,
        hash: Hash,
        expected_len: u64,
    ) -> CacheResult<GatedOutboard> {
        let outboard_max = expected_len.saturating_add(64);
        let fetched = within_origin_timeout(
            self.origin_read_budget(),
            expected_len,
            hash,
            "outboard fetch",
            self.inner.metrics.as_deref(),
            origin.fetch_outboard(hash, outboard_max),
        )
        .await
        .and_then(|r| {
            r.map_err(|e| CacheError::OriginError {
                hash,
                source: e.into_inner(),
            })
        });
        let ob = match fetched {
            Ok(OutboardFetch::Found(ob)) => ob,
            Ok(OutboardFetch::NotFound | OutboardFetch::Unsupported) => {
                return Ok(GatedOutboard::Declined);
            }
            Err(e) => {
                tracing::warn!(
                    %hash,
                    kind = ?origin.kind(),
                    error = %e.display_chain(),
                    "origin outboard fetch failed; trying next origin",
                );
                return Err(e);
            }
        };
        if let Some(m) = &self.inner.metrics {
            m.pull_through_bytes
                .inc_by(u64::try_from(ob.len()).unwrap_or(u64::MAX));
        }
        // Exact-length gate. A wrong-length `{H}.obao4` (a truncated upload, an
        // HTML error body under the cap) can never verify against `H`, and a
        // broken origin here must not shadow a healthy later one.
        if u64::try_from(ob.len()).unwrap_or(u64::MAX) != expected_len {
            tracing::warn!(
                %hash,
                kind = ?origin.kind(),
                got = ob.len(),
                expected = expected_len,
                "origin served a wrong-length outboard; trying next origin",
            );
            return Ok(GatedOutboard::WrongLength);
        }
        if !self.inner.outboards.insert(hash, ob.clone(), origin_ix) {
            tracing::debug!(
                %hash,
                len = ob.len(),
                budget = OUTBOARD_CACHE_BYTES,
                "outboard is larger than the outboard cache; each draw reads it again",
            );
        }
        Ok(GatedOutboard::Found(ob))
    }

    /// Install the shared frequency estimator. Called once at bring-up when a
    /// `tinylfu` policy is selected; absent otherwise.
    pub fn set_frequency_estimator(&self, est: Arc<dyn crate::policy::FrequencyEstimator>) {
        self.inner.frequency.store(Arc::new(Some(est)));
    }

    /// Whether a shared frequency estimator is installed (ADR 040 / ADR 041).
    /// Introspection for bring-up wiring tests: the estimator is shared by any
    /// consumer that needs a heat signal — `tinylfu` eviction/admission and the
    /// `margin` serve-economics policy — so this only reports whether *some*
    /// consumer requested one, not which.
    #[must_use]
    pub fn has_frequency_estimator(&self) -> bool {
        self.inner.frequency.load().is_some()
    }

    /// Install the admission policy consulted at store-time (ADR 040). Called
    /// once at bring-up when a non-default policy is selected; otherwise the
    /// engine keeps [`crate::policy::AlwaysAdmit`].
    pub fn set_admission_policy(&self, policy: Arc<dyn crate::policy::AdmissionPolicy>) {
        self.inner.admission.store(Arc::new(policy));
    }

    /// Consult the admission policy for `ctx`, mapping its verdict onto a
    /// [`crate::policy::Segment`]. `PassThrough` is reserved: no
    /// shipped policy returns it yet, and no pass-through-without-storing leg
    /// exists, so it is treated as `Store { Probation }` until one does.
    fn admission_segment(&self, ctx: &crate::policy::AdmissionContext) -> crate::policy::Segment {
        match self.inner.admission.load().admit(ctx) {
            crate::policy::AdmissionDecision::Store { segment } => segment,
            crate::policy::AdmissionDecision::PassThrough => crate::policy::Segment::Probation,
        }
    }

    /// `admission_segment` under a test-visible name, so policy wiring can be
    /// asserted without reaching through a fill.
    #[cfg(test)]
    pub fn admission_segment_for_test(
        &self,
        ctx: &crate::policy::AdmissionContext,
    ) -> crate::policy::Segment {
        self.admission_segment(ctx)
    }

    /// Set `hash`'s generic segment membership (ADR 040 §1). Pure in-memory
    /// metadata — the blob's commit tag still protects it from GC, so this does
    /// no tag I/O. [`crate::policy::Segment::Main`] is the absent default, so
    /// setting `Main` removes any entry; under `AlwaysAdmit` (always `Main`)
    /// this is a no-op and the map stays empty.
    pub fn set_segment(&self, hash: Hash, segment: crate::policy::Segment) {
        let mut guard = self
            .inner
            .segments
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match segment {
            crate::policy::Segment::Main => {
                guard.remove(&hash);
            }
            crate::policy::Segment::Probation => {
                guard.insert(hash, segment);
            }
        }
    }

    /// Read `hash`'s segment membership; an untracked hash is
    /// [`crate::policy::Segment::Main`].
    #[must_use]
    pub fn segment_of(&self, hash: Hash) -> crate::policy::Segment {
        self.inner
            .segments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&hash)
            .copied()
            .unwrap_or(crate::policy::Segment::Main)
    }

    /// Sum the sizes of the members of `seg`, using `sizes` for per-hash bytes.
    /// For `Main` (the untracked default) this sums every hash in `sizes` not
    /// present in the segment map.
    #[must_use]
    pub fn segment_bytes(&self, seg: crate::policy::Segment, sizes: &HashMap<Hash, u64>) -> u64 {
        let guard = self
            .inner
            .segments
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        sizes
            .iter()
            .filter(|(h, _)| {
                guard
                    .get(*h)
                    .copied()
                    .unwrap_or(crate::policy::Segment::Main)
                    == seg
            })
            .fold(0u64, |acc, (_, sz)| acc.saturating_add(*sz))
    }

    /// Snapshot the generic segment membership for the sweep's
    /// [`crate::policy::EvictionContext`]. Only non-default (`Probation`)
    /// entries are present.
    #[must_use]
    pub fn segments_snapshot(&self) -> HashMap<Hash, crate::policy::Segment> {
        self.inner
            .segments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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
    /// (ADR 040 §Pinning, durable operator-evict, and the probe-hold stay
    /// engine-enforced).
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
        let now = tokio::time::Instant::now();
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
    /// a side effect, so a caller that samples it on its own schedule (the
    /// node's `/metrics` scrape) reads live holds even with no probe traffic.
    pub fn probe_hold_slots_used(&self) -> usize {
        let now = tokio::time::Instant::now();
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
            self.observe_hit(hash);
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
        self.observe_hit(hash);
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
            // Recency only — a fill is not a hit sighting. The paired serve
            // emits the one `observe` through `observe_hit` (ADR 040).
            self.record_access(hash);
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
                    // ADR 040: consult the admission policy now that the fill
                    // succeeded and record the chosen segment. Under the default
                    // `AlwaysAdmit` the segment is `Main`, so `set_segment` is a
                    // no-op and the tag path below is unaffected — membership is
                    // pure in-memory metadata, no tag I/O.
                    let admission_ctx = crate::policy::AdmissionContext {
                        hash,
                        known_size: None,
                    };
                    let segment = self.admission_segment(&admission_ctx);
                    self.set_segment(hash, segment);
                    break;
                }
            }
        }
        // Recency only — the fill's admission read above already consulted the
        // estimate; the paired serve emits the one hit sighting (ADR 040).
        self.record_access(hash);
        Ok(())
    }

    /// A permit from the own-origin draw pool ([`MAX_CONCURRENT_RANGE_PULLS`]).
    /// A full pool bumps `range_pull_permit_waits` and waits rather than
    /// degrading, because the degrade is a whole-blob origin pull — more egress,
    /// not less. A wait past [`RANGE_PULL_PERMIT_WARN_AFTER`] logs one warning.
    async fn range_pull_permit(&self) -> CacheResult<tokio::sync::OwnedSemaphorePermit> {
        let pool = &self.inner.own_origin_range_pulls;
        if let Ok(permit) = Arc::clone(pool).try_acquire_owned() {
            return Ok(permit);
        }
        if let Some(m) = &self.inner.metrics {
            m.range_pull_permit_waits.inc();
        }
        tracing::debug!(
            bound = MAX_CONCURRENT_RANGE_PULLS,
            "origin range-pull pool is full; waiting for a permit",
        );
        let closed =
            |e| CacheError::Internal(anyhow::Error::new(e).context("range-pull bound closed"));
        let mut acquire = std::pin::pin!(Arc::clone(pool).acquire_owned());
        if let Ok(permit) = tokio::time::timeout(RANGE_PULL_PERMIT_WARN_AFTER, &mut acquire).await {
            return permit.map_err(closed);
        }
        tracing::warn!(
            bound = MAX_CONCURRENT_RANGE_PULLS,
            waited_secs = RANGE_PULL_PERMIT_WARN_AFTER.as_secs(),
            "origin range-pull pool still full; a slow or hung origin may be holding \
             permits for whole draws, stalling this fill",
        );
        acquire.await.map_err(closed)
    }

    /// The `{H}.obao4` outboard for `hash` (a `total_bytes`-byte blob): the
    /// cached copy of the exact length, or else the first origin's copy that
    /// passes the exact-length gate (`expected_outboard_len`, read with 64
    /// bytes of slack), which is then cached. `Ok(None)` when no origin serves
    /// one (or none are configured) — the caller degrades exactly as with an
    /// unsupported range. Each origin read runs under
    /// [`Self::set_origin_read_budget`]'s budget. A per-origin decline, wrong
    /// length, transport fault or timeout advances the chain.
    ///
    /// This is the outboard half of the own-origin serviceability probe
    /// ([`Self::origin_range_serviceable`]), which the serve-miss path runs
    /// before it signs a `StreamResponse`, so a blob no origin can prove is
    /// never advertised as serviceable. A cached copy answers without an origin
    /// read.
    /// The returned outboard is UNTRUSTED until it verifies against the root `H`
    /// (the range encode in [`Self::origin_range_wire`] is where that happens).
    ///
    /// # Errors
    ///
    /// [`CacheError::OriginError`] when no origin serves the outboard and at
    /// least one failed with a transport fault or a timeout (the last one).
    pub async fn origin_fetch_outboard_bytes(
        &self,
        hash: Hash,
        total_bytes: u64,
    ) -> CacheResult<Option<Bytes>> {
        Ok(self
            .outboard_with_origin(hash, total_bytes)
            .await?
            .map(|(ob, _)| ob))
    }

    /// [`Self::origin_fetch_outboard_bytes`], with the index of the origin whose
    /// copy it is — the cached entry's origin, or the origin that served it.
    async fn outboard_with_origin(
        &self,
        hash: Hash,
        total_bytes: u64,
    ) -> CacheResult<Option<(Bytes, usize)>> {
        let expected_len = expected_outboard_len(total_bytes);
        if let Some(cached) = self.inner.outboards.get(hash, expected_len) {
            return Ok(Some((cached.bytes, cached.origin_ix)));
        }
        // A concurrent cold miss of this hash may be reading the outboard now:
        // wait for it, then take its cached copy.
        let flight = self.outboard_flight(hash);
        let _reading = flight.lock().await;
        if let Some(cached) = self.inner.outboards.get(hash, expected_len) {
            return Ok(Some((cached.bytes, cached.origin_ix)));
        }
        // A genuine transport fault on an origin (as opposed to a clean
        // `NotFound`/`Unsupported` decline) is remembered so it can be surfaced when
        // NO origin serves the outboard. The serviceability caller latches this into
        // `fault_seen` (#1129): an own-origin miss that fails because the operator's
        // origin is degraded must terminate as `InternalError`, not a bare
        // `NotFound`. A clean decline stays `Ok(None)` so the caller degrades
        // silently (ADR 037 §"Fallback is always correct").
        let mut last_err: Option<CacheError> = None;
        for (ix, origin) in self.inner.origins.iter().enumerate() {
            match self
                .fetch_gated_outboard(ix, origin, hash, expected_len)
                .await
            {
                Ok(GatedOutboard::Found(ob)) => return Ok(Some((ob, ix))),
                Ok(GatedOutboard::Declined | GatedOutboard::WrongLength) => {}
                Err(e) => last_err = Some(e),
            }
        }
        // No origin served the outboard. If any errored on the way, that transport
        // fault is the answer (degraded, not absent); otherwise it is a clean
        // absence and the caller degrades to the buffered path.
        match last_err {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }

    /// Stream the header-less interleaved bao **wire** (ADR 038) for
    /// `aligned`'s span out of the configured origins, verified against the
    /// root `H` — the raw-fetch half of a range pull WITHOUT the import (the
    /// node's `NodeAdmitStore` sink admits, fed by
    /// `decdn_client::BlobSource`).
    ///
    /// Each origin serves both halves of its own attempt: the outboard and the
    /// data. The origin whose outboard is cached goes first and reuses that copy,
    /// so a draw reads no outboard from the origin while the hash stays cached.
    /// Every other origin, in chain order, reads its own outboard through the
    /// exact-length gate and caches it. The first origin that serves the first
    /// window wins; a per-origin decline or transport fault advances the chain.
    /// Each origin read runs under [`Self::set_origin_read_budget`]'s budget. A
    /// verify fault evicts the copy the draw used, so the next draw reads the
    /// outboard again. The wire is produced
    /// window by window ([`crate::RANGE_PULL_WINDOW_BYTES`]) by a background
    /// encode that holds a permit from the engine-wide pool of
    /// [`crate::MAX_CONCURRENT_RANGE_PULLS`], so memory stays
    /// `O(window + outboard)` whatever the span (#2065).
    ///
    /// A verify failure here is a HARD fault ([`CacheError::VerifyFailed`]), NOT a
    /// degrade: by the time this runs the node has signed a
    /// `StreamResponse` committing to serve under `H`, so a corrupt or
    /// misconfigured OWN origin is a local-origin fault to surface, not upstream
    /// corruption to route around (there is no upstream, and no fallback still
    /// honours `H`). A fault after the wire has started ends it with a terminal
    /// `Err` from [`OriginRangeWire::next_chunk`].
    ///
    /// # Errors
    ///
    /// - [`CacheError::VerifyFailed`] — no origin opened the range, and an origin
    ///   served a wrong-length outboard or a wrong-length first window; neither
    ///   can verify against `H`.
    /// - [`CacheError::OriginError`] — no origin opened the range, and at least
    ///   one failed with a transport fault or a timeout (the last such fault).
    ///
    /// `Ok(None)` when every origin cleanly declines.
    pub async fn origin_range_wire(
        &self,
        hash: Hash,
        aligned: &AlignedRange,
    ) -> CacheResult<Option<OriginRangeWire>> {
        let permit = self.range_pull_permit().await?;
        let Some(cursor) = self.open_range_cursor(hash, aligned, None).await? else {
            return Ok(None);
        };
        OriginRangeWire::spawn(cursor, aligned, permit, self.inner.outboards.clone()).map(Some)
    }

    /// The own-origin serviceability probe the serve-miss path runs before it
    /// signs a `StreamResponse`: whether an origin furnishes both the `{H}.obao4`
    /// outboard for `hash` (a `total_bytes`-byte blob) and ranged reads of its
    /// data. `false` sends the caller to the buffered whole-blob degrade
    /// (ADR 037 §"Fallback is always correct"), so an origin that publishes an
    /// outboard but declines `Range` does not have a signed stream fail on its
    /// first draw. The exception is an origin that served ranges earlier and
    /// has stopped since: it fails its draw like any other mid-life origin
    /// fault.
    ///
    /// The range half reads the first chunk group through the same origin
    /// chain a draw walks, starting at the origin that served the outboard. It
    /// reads nothing when that origin has already served a clean range window.
    /// The 0-byte blob has no data to range, so its outboard alone answers.
    ///
    /// # Errors
    ///
    /// - [`CacheError::OriginError`] — no origin serves both halves, and at
    ///   least one failed with a transport fault or a timeout.
    /// - [`CacheError::VerifyFailed`] — no origin serves both halves, and an
    ///   origin served a wrong-length outboard or first window, which cannot
    ///   verify.
    pub async fn origin_range_serviceable(
        &self,
        hash: Hash,
        total_bytes: u64,
    ) -> CacheResult<bool> {
        let Some((outboard, ix)) = self.outboard_with_origin(hash, total_bytes).await? else {
            return Ok(false);
        };
        if total_bytes == 0 || self.range_confirmed(ix) {
            return Ok(true);
        }
        let first_group =
            align_range(0, CHUNK_GROUP_BYTES.min(total_bytes), total_bytes).map_err(|e| {
                CacheError::Internal(anyhow::Error::from(e).context("range probe alignment"))
            })?;
        let opened = self
            .open_range_cursor(hash, &first_group, Some((ix, outboard)))
            .await?
            .is_some();
        if !opened {
            tracing::debug!(
                %hash,
                "no own origin serves ranged reads of this blob; the miss degrades to a \
                 whole-blob origin pull",
            );
        }
        Ok(opened)
    }

    /// Whether the origin at `ix` has served a clean first range window.
    fn range_confirmed(&self, ix: usize) -> bool {
        self.inner
            .origin_range_confirmed
            .get(ix)
            .is_some_and(|c| c.load(Ordering::Relaxed))
    }

    /// Open a cursor on the first origin that serves both the outboard and a
    /// first window of `aligned` of the right length. `preferred` (an origin
    /// index and the outboard it served) goes first and reuses that copy; with
    /// `None`, the origin whose outboard is cached goes first and reuses the
    /// cached copy. Every other origin, in chain order, reads its own outboard
    /// through the exact-length gate. A per-origin decline, transport fault,
    /// wrong-length outboard or wrong-length first window advances the chain,
    /// so a broken origin never shadows a healthy later one. The opened origin
    /// is marked range-confirmed. Errors as [`Self::origin_range_wire`].
    async fn open_range_cursor(
        &self,
        hash: Hash,
        aligned: &AlignedRange,
        preferred: Option<(usize, Bytes)>,
    ) -> CacheResult<Option<OriginRangeCursor>> {
        let expected_len = expected_outboard_len(aligned.blob_size());
        let cached = preferred.or_else(|| {
            self.inner
                .outboards
                .get(hash, expected_len)
                .map(|c| (c.origin_ix, c.bytes))
        });
        let first = cached.as_ref().map(|(ix, _)| *ix);
        let order = first
            .into_iter()
            .chain((0..self.inner.origins.len()).filter(|ix| Some(*ix) != first));
        let budget = self.origin_read_budget();
        let mut last_err: Option<CacheError> = None;
        let mut wrong_length = false;
        for ix in order {
            let Some(origin) = self.inner.origins.get(ix) else {
                continue;
            };
            let outboard = match &cached {
                Some((cached_ix, bytes)) if *cached_ix == ix => bytes.clone(),
                _ => match self.flighted_outboard(ix, origin, hash, expected_len).await {
                    Ok(GatedOutboard::Found(ob)) => ob,
                    Ok(GatedOutboard::Declined) => continue,
                    Ok(GatedOutboard::WrongLength) => {
                        wrong_length = true;
                        continue;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        continue;
                    }
                },
            };
            let opened = OriginRangeCursor::open(
                Arc::clone(origin),
                hash,
                aligned,
                outboard,
                budget,
                self.inner.metrics.clone(),
            )
            .await;
            match opened {
                Ok(Some(cursor)) if cursor.first_is_wrong_length() => {
                    tracing::warn!(
                        %hash,
                        kind = ?origin.kind(),
                        "own origin served a wrong-length first range window; it cannot \
                         verify against H, trying next origin",
                    );
                    wrong_length = true;
                }
                Ok(Some(cursor)) => {
                    if let Some(confirmed) = self.inner.origin_range_confirmed.get(ix) {
                        confirmed.store(true, Ordering::Relaxed);
                    }
                    return Ok(Some(cursor));
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(
                        %hash,
                        kind = ?origin.kind(),
                        error = %e.display_chain(),
                        "own origin range open failed; trying next origin",
                    );
                    last_err = Some(e);
                }
            }
        }
        match (last_err, wrong_length) {
            (Some(e), _) => Err(e),
            (None, true) => Err(CacheError::VerifyFailed { expected: hash }),
            (None, false) => Ok(None),
        }
    }

    /// Import an already-encoded interleaved bao range for `hash`, verified
    /// against the root on import (iroh-blobs `import_bao_bytes`). Thin
    /// wrapper over the store call, exposed so `NodeRangedStore::admit` need
    /// not reach into the private store handle. Records recency, like every
    /// fill path, so the partial it leaves is an eviction candidate. An admit
    /// that completes a discovery block announces the hash on
    /// [`Self::subscribe_inserts`].
    pub async fn admit_bao(
        &self,
        hash: Hash,
        chunk_ranges: bao_tree::ChunkRanges,
        bao_bytes: bytes::Bytes,
    ) -> CacheResult<()> {
        let imported = self
            .inner
            .store
            .blobs()
            .import_bao_bytes(hash, chunk_ranges.clone(), bao_bytes)
            .await
            .map_err(|e| {
                CacheError::Store(anyhow::Error::from(e).context("admit_bao: import_bao_bytes"))
            });
        let finished = match imported {
            Ok(()) => self
                .protect_partial(hash)
                .await
                .map(|()| self.record_access(hash)),
            Err(e) => Err(e),
        };
        // On success and failure alike, as in `admit_bao_stream`: items the store
        // verified before a fault stay on disk.
        self.announce_completed_blocks(hash, &chunk_ranges).await;
        finished
    }

    /// Stream the header-less bao for `chunk_ranges` of `hash` (a `total_bytes`
    /// blob) off `reader` into the cache as a **partial**, O(chunk-group),
    /// verifying against the root incrementally. Returns the drained `reader`
    /// (for `BlobSource::finish`).
    ///
    /// Drives `bao_tree`'s [`ResponseDecoder`] directly over the header-less wire
    /// (ADR 038 — the size is the trusted `total_bytes`, not an in-band prefix),
    /// forwarding each decoded [`BaoContentItem`] to iroh-blobs' `import_bao`
    /// handle. The decoder pulls one chunk at a time and the import channel
    /// backpressures, so memory stays O(one item), not O(range size).
    ///
    /// When a serve leg shares this fill (`session`), the decoder's `Parent` items
    /// ARE the outboard proof nodes, so they are captured into the shared session
    /// in the SAME decode pass — no post-admit `export_bao` read-back that would
    /// re-stream the whole range through the store actor a second time (#1790 item
    /// 4). Each leaf's run of proof nodes goes into the session in one batch just
    /// before that leaf goes to the store, so a parked serve leg encodes its first
    /// frame after the first leaf, not after the whole range, and wakes at most once
    /// per leaf. Front-to-back admits union to the whole tree.
    ///
    /// # Errors
    ///
    /// - [`CacheError::VerifyFailed`] — the decoder rejected a chunk group or
    ///   parent hash against the root `hash`: the forwarded bytes are corrupt
    ///   (a lying upstream). Nothing is admitted.
    /// - [`CacheError::Feed`] — a truncated or failed feed off `reader`, or a zero
    ///   `total_bytes` for a non-empty `hash`: the sender's fault (distinct from
    ///   corruption — see `classify_admit_decode_error`).
    /// - [`CacheError::Store`] — a fault of this node's store or its import
    ///   channel.
    ///
    /// The `reader` is carried on BOTH result arms: `Ok(reader)` on success and
    /// `Err((reader, err))` on failure. The error arm hands it back so the
    /// caller can recover a typed peer fault the reader parked while filling
    /// (`PullStalled`/`PullTimeout`/`UpstreamRefused`/`UpstreamVoucherRejected`/
    /// buyer-side `LocalPullFault`) — that parked fault is the real reason the
    /// stream stopped, and it beats the generic truncated-feed `CacheError` the
    /// decoder sees. This crate does not read the fault itself (it must not
    /// depend on `decdn-client`); it only returns the reader so the node ingest
    /// path can.
    pub async fn admit_bao_stream<R>(
        &self,
        hash: Hash,
        chunk_ranges: ChunkRanges,
        total_bytes: u64,
        reader: R,
        session: Option<&Arc<crate::FillSession>>,
    ) -> Result<R, (R, CacheError)>
    where
        R: AsyncStreamReader + Send,
    {
        let Some(size) = NonZeroU64::new(total_bytes) else {
            // An empty blob has no bao to decode. Mirror iroh-blobs' own
            // `import_bao_reader`: the canonical empty hash is a no-op success
            // (nothing to import, store, or protect), while a zero size under any
            // other hash is an upstream inconsistency (the signed `total_bytes`
            // disagrees with a non-empty content hash) — the sender's fault.
            if hash == Hash::EMPTY {
                return Ok(reader);
            }
            return Err((
                reader,
                CacheError::Feed(anyhow::anyhow!(
                    "admit_bao_stream: zero total_bytes for non-empty hash {hash}"
                )),
            ));
        };
        let tree = bao_tree::BaoTree::new(total_bytes, crate::range_pull::IROH_BLOCK_SIZE);
        let capture_into = session.cloned();
        let admitted = chunk_ranges.clone();

        let handle = match self
            .inner
            .store
            .blobs()
            .import_bao(hash, size, ADMIT_BAO_LOCAL_UPDATE_CAP)
            .await
        {
            Ok(handle) => handle,
            Err(e) => {
                return Err((
                    reader,
                    CacheError::Store(
                        anyhow::Error::from(e).context("admit_bao_stream: import_bao"),
                    ),
                ));
            }
        };
        // Split the handle so the decode driver owns the item sender while the store
        // result is awaited concurrently. The sender is bounded, so both halves must
        // make progress together — fully draining the driver before awaiting the
        // result would deadlock once the channel fills.
        let tx = handle.tx;
        let rx = handle.rx;

        let driver = async move {
            let mut decoder = ResponseDecoder::new(hash.into(), chunk_ranges, tree, reader);
            let mut pairs = Vec::new();
            loop {
                match decoder.next().await {
                    ResponseDecoderNext::More((rest, item)) => {
                        let item = match item {
                            Ok(item) => item,
                            // A decode/verify fault (corrupt bytes) or a truncated
                            // feed. Recover the reader and surface the decoder's
                            // `io::Error` for classification.
                            Err(e) => break (rest.finish(), Err(std::io::Error::from(e))),
                        };
                        // The `Parent` items ARE the outboard proof nodes; capture
                        // them for the serve leg here, in the one decode pass, rather
                        // than re-reading them back out of the store afterwards. The
                        // decoder yields a leaf's proof nodes, already verified, right
                        // before that leaf, so each run is published in one batch as
                        // its leaf arrives.
                        if let Some(session) = &capture_into {
                            if let BaoContentItem::Parent(parent) = &item {
                                pairs.push((parent.node, parent.pair));
                            } else if !pairs.is_empty() {
                                session.capture_many(pairs.drain(..));
                            }
                        }
                        if tx.send(item).await.is_err() {
                            // The store import ended before this item landed — its
                            // result (awaited below) explains why. Recover the reader.
                            break (rest.finish(), Ok(pairs));
                        }
                        decoder = rest;
                    }
                    ResponseDecoderNext::Done(reader) => break (reader, Ok(pairs)),
                }
            }
            // `tx` drops here, ending the fed item stream so the store finalizes.
        };
        // Await the store's result concurrently with the decode: the item channel is
        // bounded, so the store must drain it while the driver fills it.
        let ((reader, decode_res), store_res) = tokio::join!(driver, rx);

        let finished: CacheResult<()> = 'finish: {
            // A decode/verify fault names the real cause (corrupt upstream vs
            // truncated feed) and wins over the store side.
            let pairs = match decode_res {
                Ok(pairs) => pairs,
                Err(io_err) => break 'finish Err(classify_admit_decode_error(hash, io_err)),
            };
            // Then the store's own result, or a dropped receiver (the store task died).
            match store_res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    break 'finish Err(CacheError::Store(
                        anyhow::Error::from(e).context("admit_bao_stream: store import"),
                    ));
                }
                Err(_recv) => {
                    break 'finish Err(CacheError::Store(anyhow::anyhow!(
                        "admit_bao_stream: import result channel dropped"
                    )));
                }
            }

            // ADR 040: consult the admission policy, then label the segment only
            // after `protect_partial` succeeds, so a failed protect leaves no stale
            // membership entry for an unprotected blob. Under the default
            // `AlwaysAdmit` the segment is `Main`, so `set_segment` is a no-op —
            // membership is pure in-memory metadata, no tag I/O.
            let admission_ctx = crate::policy::AdmissionContext {
                hash,
                known_size: Some(total_bytes),
            };
            let segment = self.admission_segment(&admission_ctx);
            if let Err(e) = self.protect_partial(hash).await {
                break 'finish Err(e);
            }
            self.set_segment(hash, segment);
            // Recency only, like every fill path: the partial becomes an eviction
            // candidate even when no serve follows (an aborted or unpaid serve leg
            // never reaches `observe_hit`), so the bytes `size_snapshot` counts can
            // be released.
            self.record_access(hash);
            // Capture any proof nodes left after the last leaf. `pairs` is empty
            // when no serve leg shares this fill.
            if let Some(session) = session {
                session.capture_many(pairs);
            }
            Ok(())
        };
        // On success and failure alike: the store keeps every item it verified
        // before a fault, and a gap-fill retry admits only the missing ranges, so
        // a block this admit completed is announced here or not at all.
        self.announce_completed_blocks(hash, &admitted).await;
        match finished {
            Ok(()) => Ok(reader),
            Err(e) => Err((reader, e)),
        }
    }

    /// Best-effort total byte size of `hash` from the configured origins, for
    /// scoping a range pull ([`Self::origin_range_wire`] needs the exact blob
    /// size to align + verify a sub-range against the root `H`, and the
    /// `{H}.obao4` outboard alone doesn't pin the final chunk's length). Walks
    /// the origin fallback chain ([`Origin::size`] — HTTP `HEAD` / S3
    /// `HeadObject` / `fs` metadata) and returns the first known size; a
    /// per-origin `Ok(None)` (no object / compressed / unsupported) or a
    /// transport error advances the chain.
    ///
    /// Returns `Ok(None)` when every origin cleanly declines — the caller MUST
    /// then degrade to a whole-blob [`Self::populate`] / [`Self::get`]. This is
    /// a metadata probe only: it never fetches or caches bytes, so it carries no
    /// logical-eviction guard (the caller's serve path and whole-blob fallback
    /// both enforce it).
    ///
    /// # Errors
    ///
    /// - [`CacheError::NoOrigin`] when no origin is configured, so the caller
    ///   sees a coherent "can't range-pull" signal rather than a silent `None`.
    /// - [`CacheError::OriginError`] when NO origin answers and at least one
    ///   failed with a transport fault (the last such fault). A per-origin
    ///   fault still advances the chain — a later origin's answer wins — but a
    ///   probe that ends on faults reports a degraded node, not an empty one,
    ///   so the serve path's #1129 latch can turn the terminal miss into
    ///   `InternalError` instead of `NotFound`. Mirrors
    ///   [`Self::origin_fetch_outboard_bytes`].
    pub async fn origin_size(&self, hash: Hash) -> CacheResult<Option<u64>> {
        if self.inner.origins.is_empty() {
            return Err(CacheError::NoOrigin { hash });
        }
        let mut last_err: Option<CacheError> = None;
        for origin in &self.inner.origins {
            match origin.size(hash).await {
                Ok(Some(size)) => return Ok(Some(size)),
                Ok(None) => {}
                Err(e) => {
                    tracing::debug!(
                        %hash,
                        kind = ?origin.kind(),
                        error = %format_args!("{e:#}"),
                        "origin size probe failed; trying next origin",
                    );
                    last_err = Some(CacheError::OriginError {
                        hash,
                        source: e.into_inner(),
                    });
                }
            }
        }
        match last_err {
            Some(e) => Err(e),
            None => Ok(None),
        }
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

    /// Return the last access time for `hash`, or `None` if the hash has had
    /// no access since open.
    ///
    /// A blob that was on disk at open and has had no access since holds the
    /// open-time recency seed in `access_times`. The seed is not an access, so
    /// this returns `None` for it.
    pub fn last_accessed(&self, hash: Hash) -> Option<Instant> {
        self.inner
            .access_times
            .get(&hash)
            .map(|e| *e.value())
            .filter(|t| *t != self.inner.cold_seed_at)
    }

    /// Collect every recency entry. Eviction logic can sort by value to
    /// determine LRU ordering.
    ///
    /// Not a point-in-time snapshot: the map is sharded and this walks it shard
    /// by shard, so a record that lands mid-walk may or may not appear (see the
    /// `access_times` field docs). Recency is advisory input to eviction
    /// ordering, so a hash missed by one walk is seen by the next.
    ///
    /// **Note:** this snapshot is the *raw* access map. It includes the
    /// open-time recency seed of every blob on disk at open that has had no
    /// access since, and it includes pinned hashes. Eviction implementations
    /// should use [`Self::eviction_candidates`] instead, which filters pinned
    /// hashes out so they survive LRU pressure (#276). The raw snapshot is
    /// still exposed because tests and observability paths sometimes want the
    /// unfiltered view.
    pub fn access_times_snapshot(&self) -> HashMap<Hash, Instant> {
        self.inner
            .access_times
            .iter()
            .map(|e| (*e.key(), *e.value()))
            .collect()
    }

    /// Walk every blob in the local iroh-blobs store, complete or partial, and
    /// return its hash, excluding operator-evicted blobs
    /// ([ADR 011](../../../adr/011-content-takedown.md)). Consumed by the DHT
    /// republish scheduler at startup and by its lag sweep (ADR 022 §Bootstrap;
    /// AC 15 cold start, AC 20 lag re-seed): every held blob's first re-publish time is drawn from
    /// `uniform(0, 40 min)` per record, so the bootstrap `Store` rate matches
    /// steady-state by construction. The complementary
    /// [`Self::subscribe_inserts`] stream handles fresh commits and completed
    /// discovery blocks during steady state; the two together cover every blob
    /// the node may advertise.
    ///
    /// A partial is listed whether or not it covers a whole discovery block
    /// (ADR 022 §STORE Flow: a partial holder publishes once it verifies one).
    /// The walk reads only `status()`: deciding coverage here would `observe()`
    /// every partial at boot, and each `observe()` of an idle partial costs an
    /// fsync on the entry's idle shutdown. The republisher's due-time gate reads
    /// coverage instead, one hash at a time across the jittered window, and
    /// drops a partial that covers no block.
    ///
    /// This reads on-disk state. [`Self::open`] calls it to seed
    /// `access_times`, so every blob it returns at open is an eviction
    /// candidate. Boot therefore pays this walk twice: once for that seed and
    /// once for the DHT republish seed.
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
                        "iter_hashes: blob status() failed; skipping (seed continues with remaining blobs)"
                    );
                    continue;
                }
            };
            if !matches!(status, iroh_blobs::api::blobs::BlobStatus::NotFound) {
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
    /// The exception is a pinned hash that is deny-listed (local or
    /// governance deny): it stays a candidate, because deny wins over pin.
    /// The deny set itself never removes a hash from the snapshot.
    ///
    /// The return type ([`EvictionCandidates`]) is a newtype with no
    /// public constructor — callers can iterate or `into_inner` but
    /// cannot fabricate one. This makes "pinned-already-excluded" a
    /// type-level invariant rather than a documentation claim.
    ///
    /// Every blob the open-time store walk finds is a candidate from open.
    /// Until it is accessed, it ranks older than every blob accessed since
    /// open.
    ///
    /// The pinned set is loaded once at the start of the call so a
    /// concurrent `set_pinned` swap doesn't change which hashes get
    /// filtered mid-iteration — the snapshot is consistent against
    /// *some* pinned generation, just not necessarily the very latest.
    ///
    /// A third filter layer (after pinned, before the LRU sort) drops any
    /// hash under an active probe-triggered eviction hold, even a deny-listed
    /// one (#318, ADR 005
    /// §Probe-triggered eviction hold; ADR 040 §Pinning, durable operator-evict,
    /// and the probe-hold stay engine-enforced:
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
        // consulted — it would take a mutex per candidate during the shard walk
        // below for nothing, since `evict` removes the `access_times` entry, so an
        // already-evicted hash is never in this map to begin with, and the
        // open-time seed skips evicted hashes via `iter_hashes`.
        let denied = self.inner.denied.load();
        let chain_denied = self.inner.chain_denied.load();
        let now = tokio::time::Instant::now();
        let held: HashSet<Hash> = {
            let mut g = self
                .inner
                .probe_holds
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            g.retain(|_, exp| *exp > now);
            g.keys().copied().collect()
        };
        let map = self
            .inner
            .access_times
            .iter()
            .filter_map(|entry| {
                let (h, t) = (entry.key(), entry.value());
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

    /// Emit the hit signal for `hash`: bump its LRU recency AND forward one
    /// sighting to the shared frequency estimator (ADR 040 §Hit signal). This is
    /// the one hit sighting a served or `get` request produces.
    ///
    /// Every client-facing serve chokepoint calls this exactly once per served
    /// request — [`crate::CacheEngine::get`] on its own path, and the `node`
    /// crate's `deliver` / `serve_leg` on the paid serve paths. The fill paths
    /// ([`Self::populate`], [`Self::admit_bao_stream`]) deliberately do NOT emit
    /// it; they only record recency, so a miss that fills and then serves counts
    /// as ONE sighting (the serve's), never two. This also
    /// preserves the admission ordering invariant: a fill reads the frequency
    /// estimate for its admission decision before the paired serve emits this
    /// request's own `observe`.
    pub fn observe_hit(&self, hash: Hash) {
        self.record_access(hash);
        // Clone the estimator Arc out only when one is installed, dropping the
        // arc-swap guard before calling `observe`. The default (no-estimator)
        // path pays nothing — no clone, no refcount roundtrip — and the
        // estimator's own work never runs under the read guard.
        let est = (**self.inner.frequency.load()).clone();
        if let Some(est) = est {
            est.observe(hash);
        }
    }

    /// Mark `hash` as in use now — recency only, no frequency observe. A serve
    /// calls this when it starts, so a blob carrying the open-time recency seed
    /// does not rank oldest, and first in line for eviction, while its bytes are
    /// on the wire. The serve's one frequency sighting still comes from
    /// [`Self::observe_hit`] at clean completion.
    pub fn touch_recency(&self, hash: Hash) {
        self.record_access(hash);
    }

    /// Record an access for `hash` at the current instant — LRU recency only, no
    /// frequency observe. The fill paths use this so the blob becomes an eviction
    /// candidate without counting as a hit sighting; the paired serve emits the
    /// one sighting through [`Self::observe_hit`].
    fn record_access(&self, hash: Hash) {
        self.inner.access_times.insert(hash, Instant::now());
    }

    /// Snapshot every on-disk blob keyed by hash with its byte size
    /// (`Complete` and `Partial` alike; a partial counts only its present
    /// bytes), the public form of the internal
    /// `snapshot_blob_sizes` helper. This is the authoritative disk-usage
    /// input for the capacity-eviction driver (#1173): unlike
    /// [`Self::eviction_candidates`] — which drops pinned and probe-held hashes
    /// and carries no sizes — this walks the store, so it counts every on-disk
    /// byte, including bytes released but not yet reclaimed by GC.
    ///
    /// Cost scales with the total blob set: one `status()` per blob, plus one
    /// `observe()` per partial blob whose memoized count is older than 30 s
    /// (each `observe()` of an idle partial fsyncs its store files). A
    /// partial's count can therefore trail its disk use by up to 30 s. Call it
    /// on the eviction sweep cadence, not per request.
    pub async fn size_snapshot(&self) -> CacheResult<HashMap<Hash, u64>> {
        snapshot_blob_sizes(
            &self.inner.store,
            &self.inner.partial_sizes,
            Instant::now(),
            self.inner.metrics.as_deref(),
        )
        .await
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
        self.inner.access_times.remove(&hash);
        self.inner
            .segments
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
    /// admitted by [`Self::admit_bao`] — only the bytes covered by an
    /// admitted (and thus already-verified) range are readable; asking for
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
        // duplicating the error mapping. `align_range_clamped` sizes the buffer
        // for an end past the blob too, rather than degrading to 0.
        let cap = align_range_clamped(byte_offset, byte_len, blob_size)
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
    /// ([`align_range_clamped`]) because a bao proof anchors whole groups; an
    /// end past the blob clamps to it rather than being refused: a claimed
    /// size is a hint. The serve side does **not** trim back to the requested offset
    /// (trimming would break verification). The receiver discards the
    /// group-aligned prefix. The outboard is read from the store (built at
    /// import).
    /// Works against a **partial** blob (only imported/verified chunk groups are
    /// exportable, exactly like [`Self::export_range`]).
    ///
    /// Each item is one export item's serialization — a 64-byte proof pair or one
    /// chunk group's data — so a consumer that writes items straight to the wire
    /// holds O(chunk group) rather than O(blob). This is what the paid serve path
    /// drives (#1132), so a 708 MB blob costs O(chunk group) resident per
    /// concurrent serve, not ~708 MB.
    ///
    /// # Held content is validated as it is exported
    ///
    /// The store validates each exported item against the content root before
    /// it yields the item: every parent pair against the hash its parent
    /// expects, and every leaf's data against its parent's hash. Bytes or an
    /// outboard that diverged on disk after admission therefore never reach
    /// the stream. The export ends at the first mismatching chunk group with a
    /// terminal `Err` item, and the engine
    /// [quarantines](Self::is_quarantined) the hash.
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
    /// the detection point is after the last byte, not before the first
    /// (#915 review, #1132).
    ///
    /// # Errors
    ///
    /// [`CacheError::Store`] if `byte_offset` is at or past the blob end, or
    /// (for the 0-byte case) the blob is absent. An end past the blob clamps.
    /// Faults discovered while exporting, the truncation refusal included,
    /// arrive as `Err` items in the stream.
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

        // Snap to chunk-group boundaries. Only a start at or past the blob end
        // is rejected; an end past the blob clamps to it instead.
        // `align_range_clamped` owns the bound check, the same one the
        // dispatch-tier gate applies before this runs.
        let aligned = align_range_clamped(byte_offset, byte_len, blob_size).map_err(|e| {
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
        let engine = self.clone();
        let requested = aligned.chunk_ranges().clone();
        Ok(Box::pin(futures_util::stream::unfold(
            (stream, false),
            move |(mut stream, finished)| {
                let engine = engine.clone();
                let requested = requested.clone();
                async move {
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
                                engine
                                    .quarantine_on_mismatch(hash, &cause, &requested)
                                    .await;
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
                }
            },
        )))
    }

    /// Collect the outboard `(node, (left, right))` hash pairs iroh-blobs emits for
    /// `chunk_ranges` of `hash`, read from the store's outboard.
    ///
    /// The store validates each pair and each leaf of the range against the content
    /// root as it exports them, like [`Self::export_bao_range_stream`]. A stored
    /// hash mismatch returns `Err` and [quarantines](Self::is_quarantined) the
    /// hash.
    ///
    /// The serve leg's shared outboard (ADR 038) is fed from these so it can drive
    /// a coherent whole-range bao encode while the pull fills the cache
    /// incrementally. Call it for each range as it is admitted (and for the held
    /// part of the serve's range at serve start). It reads and verifies every leaf
    /// of `chunk_ranges`, so its cost grows with the held bytes it is given.
    /// `export_bao` emits every proof `Parent` on the path to the range PLUS the
    /// right-siblings covering
    /// still-absent content, so the union over a front-to-back admit sequence is the
    /// whole tree's internal nodes. Leaf data and the size header are skipped.
    ///
    /// `chunk_ranges` must be PRESENT (a just-admitted or held range): the proof
    /// nodes for absent siblings are emitted from the outboard regardless, but a
    /// range whose own leaves are absent faults `export_bao`. On a partial blob
    /// that fault can be a hash mismatch, because an absent range reads back as
    /// zeros. The quarantine ignores a mismatch outside the present ranges, so
    /// a precondition breach returns `Err` without withdrawing the hash.
    pub async fn outboard_pairs(
        &self,
        hash: Hash,
        chunk_ranges: &bao_tree::ChunkRanges,
    ) -> CacheResult<
        Vec<(
            bao_tree::TreeNode,
            (bao_tree::blake3::Hash, bao_tree::blake3::Hash),
        )>,
    > {
        let mut stream = self
            .inner
            .store
            .blobs()
            .export_bao(hash, chunk_ranges.clone())
            .stream();
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                EncodedItem::Parent(parent) => out.push((parent.node, parent.pair)),
                EncodedItem::Leaf(_) | EncodedItem::Size(_) => {}
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => {
                    self.quarantine_on_mismatch(hash, &cause, chunk_ranges)
                        .await;
                    return Err(CacheError::Store(
                        anyhow::Error::from(cause)
                            .context("outboard_pairs: export_bao stream failed"),
                    ));
                }
            }
        }
        Ok(out)
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
    ///
    /// Runs inside an `origin_pull` span: one per chain walk, with the paid
    /// node→node fallback's `node_pull` span nested inside when the walk reaches
    /// the `Peer` origin.
    #[allow(clippy::too_many_lines)]
    // One linear chain walk; each outcome arm carries the rationale for its own fallback/return decision, and splitting the match out would separate those from the loop state (`last_err`, `any_not_found`, `any_short_circuit`) they exist to explain.
    #[tracing::instrument(
        name = "origin_pull",
        skip_all,
        fields(
            %hash,
            local_only = local_only,
            outcome = tracing::field::Empty,
            error = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        )
    )]
    async fn pull_through(
        &self,
        hash: Hash,
        local_only: bool,
        mode: FillMode,
    ) -> CacheResult<Option<Bytes>> {
        let result: CacheResult<Option<Bytes>> = async {
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
                        // exists — ignore. A local-store hit in `get` does
                        // not emit: a hit changes nothing the republisher
                        // advertises; commits and completed blocks do.
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
        .await;
        record_origin_pull(&tracing::Span::current(), &result);
        result
    }

    /// Bump the `origin_fallback` counter and emit a structured log for
    /// the chain-advance event (#284). Kept out of `pull_through`'s
    /// hot-path loop body so it stays small enough for clippy's
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
                error = last_err.map(|e| format!("{e:#}")).unwrap_or_default(),
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
            crate::origin::OriginFetch::AlreadyAdmitted => {
                // The origin admitted + verified the blob into the store itself
                // (node-to-node pull, #1682). Skip the drain / import_and_verify_stream
                // and reuse the same post-commit tail the streamed path runs.
                return match mode {
                    FillMode::CommitOnly => Ok(PullThroughOutcome::Committed),
                    FillMode::ReturnBytes => match self.read_local(hash).await {
                        Ok(bytes) => Ok(PullThroughOutcome::Bytes(bytes)),
                        Err(CacheError::Store(err)) => Ok(PullThroughOutcome::Store(err)),
                        Err(other) => Ok(PullThroughOutcome::Store(
                            anyhow::Error::from(other).context(
                                "read_local returned unexpected variant after AlreadyAdmitted",
                            ),
                        )),
                    },
                };
            }
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
                    Ok(PullThroughOutcome::Store(
                        anyhow::Error::from(other).context(
                            "read_local returned unexpected variant after successful commit",
                        ),
                    ))
                }
            },
            StreamCommitOutcome::HashMismatch { actual } => {
                Ok(PullThroughOutcome::HashMismatch { actual })
            }
            StreamCommitOutcome::BlobTooLarge => Ok(PullThroughOutcome::BlobTooLarge),
            StreamCommitOutcome::Store(err) => Ok(PullThroughOutcome::Store(err)),
        }
    }

    /// Drive a chunk stream into the iroh-blobs store, capturing mid-stream
    /// errors via the side channel, verify the committed hash against `hash`,
    /// and on match promote the temp tag to a named tag. This is the
    /// commit-and-verify tail of the origin pull-through
    /// ([`Self::pull_through_attempt`]); `stream` is an `Origin::fetch` stream.
    /// It does NOT broadcast the insert or read the bytes back; the caller owns
    /// those (the pull-through re-reads for its `Bytes` return).
    ///
    /// `count_and_cap_stream` enforces `max_blob_bytes` and bumps the
    /// origin-egress metric per chunk, so the pulled bytes are metered as the
    /// upstream egress they genuinely are (those bytes left an origin) without
    /// touching the `get`-caller hit/returned counters.
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
        // A pull that started before a quarantine must not protect the corrupt
        // entry the quarantine released to GC. The import landed on that entry,
        // so nothing verified is lost.
        if self.is_quarantined(hash) {
            drop(temp_tag);
            return Ok(StreamCommitOutcome::Committed);
        }
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
                    error = %err,
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
    /// read back (#1132). Serving a 708 MB blob would otherwise cost that much
    /// again on the miss leg for a buffer nobody looks at.
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
/// tail of the origin pull-through. Mirrors [`PullThroughOutcome`] minus the
/// `Bytes`/`NotFound` arms: a stream import either commits, hashes wrong,
/// overruns the cap, or hits a store fault.
#[derive(Debug)]
enum StreamCommitOutcome {
    Committed,
    BlobTooLarge,
    HashMismatch { actual: Hash },
    Store(anyhow::Error),
}

/// Bound on the store's `import_bao` local update queue for
/// [`CacheEngine::admit_bao_stream`]: at most this many decoded [`BaoContentItem`]s
/// may be in flight to the store before the decode driver awaits. Small enough to
/// cap resident memory (a handful of `cdn/client/v1` chunks), large enough that the
/// store import and the decode overlap rather than ping-ponging one item at a time.
const ADMIT_BAO_LOCAL_UPDATE_CAP: usize = 8;

/// Classify the `io::Error` [`CacheEngine::admit_bao_stream`]'s decoder surfaces. A
/// genuine bao verify rejection (chunk-group or parent hash mismatch) is an
/// `io::Error` of kind `InvalidData` (see `bao_tree::io::error::DecodeError`'s
/// `From<DecodeError> for io::Error`); a truncated/short feed instead surfaces
/// `UnexpectedEof`, and any other failure is the feed's own read or transport
/// fault. Only the mismatch is a corrupt-upstream `VerifyFailed`; everything else
/// is a `Feed` fault. Neither is this node's store.
fn classify_admit_decode_error(hash: Hash, e: std::io::Error) -> CacheError {
    if e.kind() == std::io::ErrorKind::InvalidData {
        CacheError::VerifyFailed { expected: hash }
    } else {
        CacheError::Feed(anyhow::Error::from(e).context("admit_bao_stream: decode/feed failed"))
    }
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
pub(crate) fn expected_outboard_len(blob_size: u64) -> u64 {
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
/// few MiB depending on the backend) — a 10 GB blob never pins
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

/// Record the end of an `origin_pull` span once. A
/// clean miss (`NotFound` / `NoOrigin`) is `not_found` with no error status,
/// since walking a chain that lacks the blob is normal; every other failure
/// is `failed` with its error text and an error status.
fn record_origin_pull<T>(span: &tracing::Span, result: &CacheResult<T>) {
    match result {
        Ok(_) => {
            span.record("outcome", "filled");
        }
        Err(CacheError::NotFound { .. } | CacheError::NoOrigin { .. }) => {
            span.record("outcome", "not_found");
        }
        Err(e) => {
            span.record("outcome", "failed");
            span.record("error", tracing::field::display(e.display_chain()));
            span.record("otel.status_code", "ERROR");
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation
)]
mod tests;
