//! Cache engine: local iroh-blobs store fronted by an [`Origin`] for misses.

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
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

use crate::circuit_breaker::{
    Admission, Clock, OriginBreaker, OriginOutcome, SystemClock, TrialGuard,
};
use crate::error::{CacheError, CacheResult, OriginPullError};
use crate::fill_session::{FillClaim, FillRegistry, FillSession};
use crate::metrics::CacheMetrics;
use crate::origin::{Origin, OriginKind, OriginRangeFetch, OriginRangeRequest, OutboardFetch};
use crate::origin_probe::{OriginProbeMemo, OriginProbePolicy, Presence};
use crate::probe_hold::ProbeHoldOutcome;
use crate::range_pull::{AlignedRange, align_range, encode_verified_range};
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
/// this type: [`Self::pull_through_range`] commits only a verified sub-range.
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
    /// corruption-recovery evicts survive a process restart — an
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
    /// operator [`CacheEngine::evict`] still wins (ADR 040 §Pinning, durable
    /// operator-evict, and the probe-hold stay engine-enforced: DMCA always
    /// wins), enforced
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
        /// Whether the hash is logically evicted (#279). Mirrors
        /// [`CacheEngine::is_evicted`], so the miss path tells an eviction
        /// from a plain miss without a second call.
        evicted: bool,
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

    /// Whether the hash is logically evicted (#279). `false` for a serveable
    /// hash, since eviction is one of the gates that refuses a serve.
    #[must_use]
    pub const fn is_evicted(&self) -> bool {
        matches!(self, Self::Unavailable { evicted: true })
    }
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
}

impl PresentRanges {
    fn absent() -> Self {
        Self {
            ranges: ChunkRanges::empty(),
            complete: false,
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
        // - `access_times` has no entry here: it is a `DashMap`, so no
        //   poisoning decision exists to make. A panic while one of its shards
        //   is locked leaves that shard usable, and every reader and writer
        //   takes one shard at a time.
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
    /// `{H}.obao4`, none honored `Range`, or the outboard was short/absent).
    /// The caller MUST fall back to a whole-blob pull.
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
                access_times: DashMap::new(),
                partial_protected: DashMap::new(),
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
                probe_holds: Mutex::new(HashMap::new()),
                max_probe_holds: AtomicUsize::new(crate::probe_hold::DEFAULT_MAX_PROBE_HOLDS),
                frequency: ArcSwap::from_pointee(None),
                admission: ArcSwap::from_pointee(
                    Arc::new(crate::policy::AlwaysAdmit) as Arc<dyn crate::policy::AdmissionPolicy>
                ),
                segments: Mutex::new(HashMap::new()),
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
                fill_registry: Arc::new(FillRegistry::new()),
            }),
        })
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
                        error = %err,
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
                            error = %e,
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
        let evicted = self.is_evicted(hash);
        let refused = self.is_denied(hash) || self.is_chain_denied(hash) || evicted;
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
                Ok(ServeAudit::Unavailable { evicted })
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

    /// The chunk-aligned sub-ranges of `[byte_offset, byte_offset + byte_len)`
    /// (`byte_len == 0` = to `blob_size`) that are NOT present on disk.
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
        // Same align_range error mapping as `export_bao_range_stream`: a range
        // that does not fit `blob_size` is an argument error, not an origin fault.
        let aligned = align_range(byte_offset, byte_len, blob_size).map_err(|e| {
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
                    %err,
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
    async fn protect_partial(&self, hash: Hash) -> CacheResult<()> {
        if self.inner.partial_protected.contains_key(&hash) {
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

    /// Set the probe-hold budget cap from `cache.max_probe_holds` (ADR 005
    /// §Hold budget, #318). Called once by the runtime at bring-up. `0`
    /// disables the hold path so [`Self::try_probe_hold`] returns
    /// [`ProbeHoldOutcome::HoldsDisabled`] for any present blob (the node then
    /// answers `has_blob: false` to every probe).
    pub fn set_max_probe_holds(&self, max: usize) {
        self.inner.max_probe_holds.store(max, Ordering::Relaxed);
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
    /// [`crate::policy::Segment`]. `PassThrough` is reserved (spec §3) — no
    /// shipped policy returns it yet, and no pass-through-without-storing leg
    /// exists, so it is treated as `Store { Probation }` until one does.
    fn admission_segment(&self, ctx: &crate::policy::AdmissionContext) -> crate::policy::Segment {
        match self.inner.admission.load().admit(ctx) {
            crate::policy::AdmissionDecision::Store { segment } => segment,
            crate::policy::AdmissionDecision::PassThrough => crate::policy::Segment::Probation,
        }
    }

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
    /// Only the actually-pulled bytes leave the origin — the range span the ramped
    /// credit window paces, plus the small `{H}.obao4` outboard as additional
    /// origin egress beyond that content window — not the whole blob, tightening
    /// the node's exposure (ADR 037 §"Origin-tier pull-through").
    ///
    /// Returns [`RangePullOutcome::Served`] when the partial range is present,
    /// or [`RangePullOutcome::Unsupported`] when no origin could range-pull
    /// (no `{H}.obao4`, no `Range`, short outboard) — in which case the caller
    /// MUST fall back to a whole-blob [`Self::populate`] / [`Self::get`]. The
    /// fallback is always correct; the optimization only reduces the origin
    /// hop's cost.
    ///
    /// This is **partial**-blob population. It installs a deterministic
    /// `decdn-partial-<hash>` named tag so the imported range survives GC
    /// (#1607, via `protect_partial`) — but, unlike [`Self::populate`],
    /// it does not make [`Self::has`] return `true` (which requires a `Complete`
    /// blob), and it does not announce a DHT insert — a node holding only a
    /// range is not advertised as a full holder (ADR 037 §"partial warming
    /// copies are not advertised"). A subsequent whole-blob pull-through (or
    /// further range pulls) completes the blob.
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
        let Some((data, outboard)) = self
            .origin_fetch_range_bytes(&origin, hash, blob_size, req)
            .await?
        else {
            // Missing outboard / no range support / object absent → degrade.
            return Ok(RangePullOutcome::Unsupported);
        };

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

        self.protect_partial(hash).await?;

        Ok(RangePullOutcome::Served)
    }

    /// Fetch one origin's chunk-group-aligned range span (`req`) plus its
    /// sibling `{H}.obao4` outboard, and meter the pulled bytes as
    /// `pull_through_bytes`. Returns the raw, still-UNVERIFIED `(data, outboard)`
    /// on a hit, or `Ok(None)` for a per-origin decline
    /// ([`OriginRangeFetch::Unsupported`] / [`OriginRangeFetch::NotFound`]) so the
    /// caller can advance the fallback chain.
    ///
    /// Deliberately stops at the fetch+meter boundary and does NOT verify against
    /// the root: the two callers apply OPPOSITE verify-failure policies over the
    /// same fetched bytes, so the verify cannot be shared. [`Self::range_pull_attempt`]
    /// DEGRADES a range that fails bao verification to a whole-blob pull (a single
    /// misbehaving origin must not deny the range), while [`Self::origin_encode_range`]
    /// treats the same failure as a HARD local-origin fault (Flow A has already
    /// committed to serving under `H`, so there is no safe degrade). Sharing the
    /// fetch keeps the origin transport / metering path DRY without forcing one
    /// policy on both.
    async fn origin_fetch_range_bytes(
        &self,
        origin: &Arc<dyn Origin>,
        hash: Hash,
        blob_size: u64,
        req: OriginRangeRequest,
    ) -> CacheResult<Option<(Bytes, Bytes)>> {
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
                OriginRangeFetch::Unsupported | OriginRangeFetch::NotFound => return Ok(None),
            };

        // Meter the actually-pulled bytes (span + outboard) as origin egress —
        // the bytes really did leave an origin. The ramped credit window paces the
        // content span; the outboard is additional origin egress beyond it. Either
        // way this is the pulled side, not the whole blob.
        if let Some(m) = &self.inner.metrics {
            let pulled = u64::try_from(data.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(outboard.len()).unwrap_or(u64::MAX));
            m.pull_through_bytes.inc_by(pulled);
        }

        Ok(Some((data, outboard)))
    }

    /// Fetch the sibling `{H}.obao4` outboard for `hash` (a `total_bytes`-byte
    /// blob) from the first configured origin that publishes it, returning the raw
    /// outboard bytes. `Ok(None)` when no origin serves it (or none are
    /// configured) — the caller degrades exactly as with an unsupported range.
    ///
    /// This is the Flow A serviceability probe: the node's own-origin serve-miss
    /// path (FA.3) confirms an origin can furnish the outboard for `H` before it
    /// signs a `StreamResponse` and spins up the two-leg driver, so a blob no
    /// origin can prove is never advertised as serviceable. It is a standalone
    /// outboard walk — same
    /// `outboard_max` derivation (`expected_outboard_len` plus a 64-byte slack
    /// for the final partial group), same "a per-origin decline or transport fault
    /// advances the chain" discipline. The returned outboard is UNTRUSTED until it
    /// verifies against the root `H` (the range encode in
    /// [`Self::origin_encode_range`] is where that happens).
    pub async fn origin_fetch_outboard_bytes(
        &self,
        hash: Hash,
        total_bytes: u64,
    ) -> CacheResult<Option<Bytes>> {
        let outboard_max = expected_outboard_len(total_bytes).saturating_add(64);
        // A genuine transport fault on an origin (as opposed to a clean
        // `NotFound`/`Unsupported` decline) is remembered so it can be surfaced when
        // NO origin serves the outboard. The serviceability caller latches this into
        // `fault_seen` (#1129): an own-origin miss that fails because the operator's
        // origin is degraded must terminate as `InternalError`, not a bare
        // `NotFound`. A clean decline stays `Ok(None)` so the caller degrades
        // silently (ADR 037 §"Fallback is always correct").
        let mut last_err: Option<CacheError> = None;
        for origin in &self.inner.origins {
            match origin.fetch_outboard(hash, outboard_max).await {
                Ok(OutboardFetch::Found(ob)) => return Ok(Some(ob)),
                Ok(OutboardFetch::NotFound | OutboardFetch::Unsupported) => {}
                Err(e) => {
                    tracing::debug!(
                        %hash,
                        kind = ?origin.kind(),
                        error = %e,
                        "origin outboard fetch failed; trying next origin",
                    );
                    last_err = Some(CacheError::OriginError {
                        hash,
                        source: e.into_inner(),
                    });
                }
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

    /// Fetch `aligned`'s span from the configured origins and return the
    /// header-full interleaved bao **wire** for it, verified against the root `H`
    /// — the raw-fetch half of a range pull WITHOUT the import
    /// (`range_pull_attempt` imports; here the node's `NodeAdmitStore` sink
    /// does). The returned bytes keep their leading 8-byte little-endian size
    /// header (the shape [`encode_verified_range`] produces); the node-side
    /// `decdn_client_pull::BlobSource` caller strips it before feeding the
    /// header-less wire (ADR 038) to the driver.
    ///
    /// The first origin that SERVES the range wins. A per-origin decline
    /// ([`OriginRangeFetch::Unsupported`] / [`OriginRangeFetch::NotFound`])
    /// advances the chain; all origins exhausted → `Ok(None)`.
    ///
    /// # The load-bearing difference from `range_pull_attempt`
    ///
    /// A verify failure here is a HARD fault ([`CacheError::VerifyFailed`]), NOT a
    /// degrade. `range_pull_attempt` can degrade a range that fails bao
    /// verification to a whole-blob pull because it is only OPTIMIZING a cold miss
    /// — the whole-blob path re-verifies against `H` and still serves correct
    /// bytes. Flow A cannot: by the time this runs the node has signed a
    /// `StreamResponse` committing to serve under `H`, so a corrupt or
    /// misconfigured OWN origin is a local-origin fault to surface, not upstream
    /// corruption to route around (there is no upstream, and no fallback still
    /// honours `H`).
    ///
    /// # Errors
    ///
    /// - [`CacheError::VerifyFailed`] — the winning origin's range/outboard did
    ///   not verify against the root `H` (a bad `{H}.obao4`, a corrupt span, a
    ///   wrong-length body). Mapped from [`encode_verified_range`]'s
    ///   [`RangeVerifyError`](decdn_bao_range::RangeVerifyError) — the same shape
    ///   [`Self::admit_bao_stream`] reports on a mid-stream group mismatch.
    /// - [`CacheError::OriginError`] — an origin transport fault while fetching the
    ///   range (propagated from `origin_fetch_range_bytes`).
    pub async fn origin_encode_range(
        &self,
        hash: Hash,
        aligned: &AlignedRange,
    ) -> CacheResult<Option<Bytes>> {
        let root = *hash.as_bytes();
        let req = OriginRangeRequest {
            fetch_start: aligned.fetch_start(),
            fetch_end: aligned.fetch_end(),
        };
        let blob_size = aligned.blob_size();
        for origin in &self.inner.origins {
            let Some((data, outboard)) = self
                .origin_fetch_range_bytes(origin, hash, blob_size, req)
                .await?
            else {
                continue;
            };
            return match encode_verified_range(root, aligned, &data, outboard) {
                Ok(wire) => Ok(Some(wire)),
                Err(err) => {
                    tracing::warn!(
                        %hash,
                        kind = ?origin.kind(),
                        error = %err,
                        "own origin served a range that failed bao verification against H; \
                         hard local-origin fault (no degrade — committed to serving under H)",
                    );
                    Err(CacheError::VerifyFailed { expected: hash })
                }
            };
        }
        Ok(None)
    }

    /// Import an already-encoded interleaved bao range for `hash`, verified
    /// against the root on import (iroh-blobs `import_bao_bytes`). Thin
    /// wrapper over the same store call `pull_through_range` makes, exposed so
    /// `NodeRangedStore::admit` need not reach into the private store handle.
    pub async fn admit_bao(
        &self,
        hash: Hash,
        chunk_ranges: bao_tree::ChunkRanges,
        bao_bytes: bytes::Bytes,
    ) -> CacheResult<()> {
        self.inner
            .store
            .blobs()
            .import_bao_bytes(hash, chunk_ranges, bao_bytes)
            .await
            .map_err(|e| {
                CacheError::Store(anyhow::Error::from(e).context("admit_bao: import_bao_bytes"))
            })?;
        self.protect_partial(hash).await?;
        Ok(())
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
    /// 4). Front-to-back admits union to the whole tree.
    ///
    /// # Errors
    ///
    /// - [`CacheError::VerifyFailed`] — the decoder rejected a chunk group or
    ///   parent hash against the root `hash`: the forwarded bytes are corrupt
    ///   (a lying upstream). Nothing is admitted.
    /// - [`CacheError::Store`] — a local store fault, an import-channel fault, or a
    ///   truncated/short feed off `reader` (distinct from corruption — see
    ///   `classify_admit_decode_error`).
    ///
    /// The `reader` is carried on BOTH result arms: `Ok(reader)` on success and
    /// `Err((reader, err))` on failure. The error arm hands it back so the
    /// caller can recover a typed peer fault the reader parked while filling
    /// (`PullStalled`/`PullTimeout`/`UpstreamRefused`/`UpstreamVoucherRejected`/
    /// buyer-side `LocalPullFault`) — that parked fault is the real reason the
    /// stream stopped, and it beats the generic truncated-feed `CacheError` the
    /// decoder sees. This crate does not read the fault itself (it must not
    /// depend on `client-pull`); it only returns the reader so the node ingest
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
            // disagrees with a non-empty content hash) — a Store-class fault, as
            // before.
            if hash == Hash::EMPTY {
                return Ok(reader);
            }
            return Err((
                reader,
                CacheError::Store(anyhow::anyhow!(
                    "admit_bao_stream: zero total_bytes for non-empty hash {hash}"
                )),
            ));
        };
        let tree = bao_tree::BaoTree::new(total_bytes, crate::range_pull::IROH_BLOCK_SIZE);
        let capture = session.is_some();

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
                        // than re-reading them back out of the store afterwards.
                        if capture && let BaoContentItem::Parent(parent) = &item {
                            pairs.push((parent.node, parent.pair));
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

        // A decode/verify fault names the real cause (corrupt upstream vs truncated
        // feed) and wins over the store side.
        let pairs = match decode_res {
            Ok(pairs) => pairs,
            Err(io_err) => return Err((reader, classify_admit_decode_error(hash, io_err))),
        };
        // Then the store's own result, or a dropped receiver (the store task died).
        match store_res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                return Err((
                    reader,
                    CacheError::Store(
                        anyhow::Error::from(e).context("admit_bao_stream: store import"),
                    ),
                ));
            }
            Err(_recv) => {
                return Err((
                    reader,
                    CacheError::Store(anyhow::anyhow!(
                        "admit_bao_stream: import result channel dropped"
                    )),
                ));
            }
        }

        // ADR 040: consult the admission policy, then label the segment only after
        // `protect_partial` succeeds, so a failed protect leaves no stale membership
        // entry for an unprotected blob. Under the default `AlwaysAdmit` the segment
        // is `Main`, so `set_segment` is a no-op — membership is pure in-memory
        // metadata, no tag I/O.
        let admission_ctx = crate::policy::AdmissionContext {
            hash,
            known_size: Some(total_bytes),
        };
        let segment = self.admission_segment(&admission_ctx);
        if let Err(e) = self.protect_partial(hash).await {
            return Err((reader, e));
        }
        self.set_segment(hash, segment);
        // Wake parked serve legs once for the whole admit, not per node: a large
        // range carries many proof nodes, and a per-node notify storm scales the
        // wakeups with proof-node count for no gain. `pairs` is empty when no serve
        // leg shares this fill.
        if let Some(session) = session {
            session.capture_many(pairs);
        }
        Ok(reader)
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

    /// Return the last access time for `hash`, or `None` if the hash has
    /// never been accessed through [`Self::get`].
    pub fn last_accessed(&self, hash: Hash) -> Option<Instant> {
        self.inner.access_times.get(&hash).map(|e| *e.value())
    }

    /// Collect every recorded access time. Eviction logic can sort by value to
    /// determine LRU ordering.
    ///
    /// Not a point-in-time snapshot: the map is sharded and this walks it shard
    /// by shard, so a record that lands mid-walk may or may not appear (see the
    /// `access_times` field docs). Recency is advisory input to eviction
    /// ordering, so a hash missed by one walk is seen by the next.
    ///
    /// **Note:** this snapshot is the *raw* access map and includes pinned
    /// hashes. Eviction implementations should use
    /// [`Self::eviction_candidates`] instead, which filters pinned hashes
    /// out so they survive LRU pressure (#276). The raw snapshot is still
    /// exposed because tests and observability paths sometimes want the
    /// unfiltered view.
    pub fn access_times_snapshot(&self) -> HashMap<Hash, Instant> {
        self.inner
            .access_times
            .iter()
            .map(|e| (*e.key(), *e.value()))
            .collect()
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

    /// Record an access for `hash` at the current instant — LRU recency only, no
    /// frequency observe. The fill paths use this so the blob becomes an eviction
    /// candidate without counting as a hit sighting; the paired serve emits the
    /// one sighting through [`Self::observe_hit`].
    fn record_access(&self, hash: Hash) {
        self.inner.access_times.insert(hash, Instant::now());
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
    /// drives (#1132), so a 708 MB blob costs O(chunk group) resident per
    /// concurrent serve, not ~708 MB.
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

    /// Collect the outboard `(node, (left, right))` hash pairs iroh-blobs emits for
    /// `chunk_ranges` of `hash`, straight from the store's outboard — NO re-hashing.
    ///
    /// The serve leg's shared outboard (ADR 038) is fed
    /// from these so it can drive a coherent whole-range bao encode while the pull
    /// fills the cache incrementally. Call it for each range as it is admitted (and
    /// for the already-held ranges at serve start): `export_bao` emits every proof
    /// `Parent` on the path to the range PLUS the right-siblings covering
    /// still-absent content, so the union over a front-to-back admit sequence is the
    /// whole tree's internal nodes. Leaf data and the size header are skipped.
    ///
    /// `chunk_ranges` must be PRESENT (a just-admitted or held range): the proof
    /// nodes for absent siblings are emitted from the outboard regardless, but a
    /// range whose own leaves are absent faults `export_bao`.
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
            crate::origin::OriginFetch::AlreadyAdmitted => {
                // The origin admitted + verified the blob into the store itself
                // (node-to-node pull, #1682). Skip the drain / import_and_verify_stream
                // and reuse the same post-commit tail the streamed path runs.
                return match mode {
                    FillMode::CommitOnly => Ok(PullThroughOutcome::Committed),
                    FillMode::ReturnBytes => match self.read_local(hash).await {
                        Ok(bytes) => Ok(PullThroughOutcome::Bytes(bytes)),
                        Err(CacheError::Store(err)) => Ok(PullThroughOutcome::Store(err)),
                        Err(other) => Ok(PullThroughOutcome::Store(anyhow::Error::msg(format!(
                            "read_local returned unexpected variant after AlreadyAdmitted: {other}"
                        )))),
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
                    Ok(PullThroughOutcome::Store(anyhow::Error::msg(format!(
                        "read_local returned unexpected variant after successful commit: {other}"
                    ))))
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
/// `UnexpectedEof`, and any other failure is a genuinely local read/transport
/// fault. Only the mismatch is a corrupt-upstream `VerifyFailed`; everything else
/// is transport-class `Store`.
fn classify_admit_decode_error(hash: Hash, e: std::io::Error) -> CacheError {
    if e.kind() == std::io::ErrorKind::InvalidData {
        CacheError::VerifyFailed { expected: hash }
    } else {
        CacheError::Store(anyhow::Error::from(e).context("admit_bao_stream: decode/feed failed"))
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation
)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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
    /// [`Origin::fetch_outboard`], for outboard-fetch tests. The
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

    /// Origin whose `size()` (a `HEAD`/`HeadObject` stand-in) always fails
    /// with a transport error, and COUNTS the calls — the `Fault`-arm
    /// counterpart to [`CountingSizeOrigin`].
    #[derive(Debug)]
    struct FailingSizeOrigin {
        calls: Arc<AtomicUsize>,
    }

    impl FailingSizeOrigin {
        fn new() -> (Self, Arc<AtomicUsize>) {
            let calls = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    calls: Arc::clone(&calls),
                },
                calls,
            )
        }
    }

    impl Origin for FailingSizeOrigin {
        fn kind(&self) -> OriginKind {
            OriginKind::Http
        }

        fn fetch(
            &self,
            _hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
        {
            Box::pin(async { Ok(OriginFetch::NotFound) })
        }

        fn size(
            &self,
            _hash: Hash,
        ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, crate::OriginPullError>> + Send + '_>>
        {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(crate::OriginPullError::Transient(anyhow::anyhow!(
                    "synthetic HEAD outage (connection refused)"
                )))
            })
        }
    }

    /// A per-origin transport error on the live `HEAD` — not just a timeout —
    /// is `Fault`, never `Absent` (#1766 follow-up): `origin_probe_presence`
    /// walks the origin chain itself rather than going through
    /// [`CacheEngine::origin_size`], whose swallow-and-advance per-origin
    /// error handling would otherwise launder a connection-refused/5xx/DNS
    /// outage into a false `Ok(None)`. Also confirms the fault is never
    /// memoised: a second probe re-walks the chain (call count 1 -> 2).
    #[tokio::test]
    async fn origin_probe_presence_transport_error_is_fault_not_absent() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let hash = Hash::new(b"faulting origin probe");
        let (origin, calls) = FailingSizeOrigin::new();
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

        assert_eq!(
            engine.origin_probe_presence(hash).await,
            OriginPresence::Fault,
            "a per-origin transport error must be Fault, not Absent",
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1, "one backend HEAD attempt");
        assert_eq!(
            engine.origin_probe_presence(hash).await,
            OriginPresence::Fault,
            "a fault is memoised under the fault TTL (#1789 item 6)",
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the second probe served the cached fault rather than re-walking \
             the origin chain — a steady state re-probes once per fault TTL, \
             not once per request",
        );
        assert_eq!(
            engine.origin_probe_size(hash).await,
            None,
            "the thin size wrapper still folds Fault to None",
        );
        Ok(())
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
        engine.set_origin_probe_config(OriginProbePolicy {
            positive_ttl: Duration::from_secs(15),
            negative_ttl: Duration::from_secs(2),
            fault_ttl: Duration::from_secs(5),
            timeout: Duration::from_millis(20),
            capacity: 16,
        });

        assert_eq!(
            engine.origin_probe_size(hash).await,
            None,
            "a HEAD slower than the ceiling folds to absent",
        );
        Ok(())
    }

    /// `origin_probe_presence` distinguishes `Present`/`Absent`/`Fault`
    /// (#1766): a hash the origin holds is `Present(size)`, a hash it does not
    /// is `Absent`, and a `HEAD` slower than the probe ceiling is `Fault` —
    /// NOT `Absent`, so a caller can never sign an authoritative `NotFound`
    /// off a backend blip.
    #[tokio::test]
    async fn origin_probe_presence_distinguishes_present_absent_and_fault() -> anyhow::Result<()> {
        // `Present`/`Absent` against a fast origin.
        let tmp = tempfile::tempdir()?;
        let present = Hash::new(b"present for presence");
        let absent = Hash::new(b"absent for presence");
        let (present_origin, _present_calls) = CountingSizeOrigin::new(present, 777);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(present_origin) as Arc<dyn Origin>],
            10,
        )
        .await?;
        assert_eq!(
            engine.origin_probe_presence(present).await,
            OriginPresence::Present(777),
            "a live HEAD hit is Present, not folded away",
        );
        assert_eq!(
            engine.origin_probe_presence(absent).await,
            OriginPresence::Absent,
            "a genuine miss is Absent",
        );

        // `Fault` against a separate engine whose only origin overruns the
        // probe ceiling (a shared origin would delay the Present/Absent
        // probes above too, since `CountingSizeOrigin`'s delay is unconditional).
        let tmp2 = tempfile::tempdir()?;
        let slow = Hash::new(b"slow for presence");
        let (slow_origin, slow_calls) =
            CountingSizeOrigin::slow(slow, 100, Duration::from_millis(400));
        let fault_engine = CacheEngine::open(
            tmp2.path(),
            vec![Arc::new(slow_origin) as Arc<dyn Origin>],
            10,
        )
        .await?;
        // Tight timeout so the 400 ms origin overruns it.
        fault_engine.set_origin_probe_config(OriginProbePolicy {
            positive_ttl: Duration::from_secs(15),
            negative_ttl: Duration::from_secs(2),
            fault_ttl: Duration::from_secs(5),
            timeout: Duration::from_millis(20),
            capacity: 16,
        });
        assert_eq!(
            fault_engine.origin_probe_presence(slow).await,
            OriginPresence::Fault,
            "a HEAD slower than the ceiling is Fault, not Absent",
        );
        assert_eq!(
            slow_calls.load(Ordering::SeqCst),
            1,
            "one backend attempt for the first faulting probe",
        );
        assert_eq!(
            fault_engine.origin_probe_presence(slow).await,
            OriginPresence::Fault,
            "a memoised fault still answers Fault",
        );
        assert_eq!(
            slow_calls.load(Ordering::SeqCst),
            1,
            "the second probe served the cached fault instead of re-hitting \
             the backend (#1789 item 6)",
        );
        Ok(())
    }

    /// `origin_probe_size` stays a thin `Present -> Some`, `Absent`/`Fault ->
    /// None` wrapper (#1766): callers that only care about advertising size
    /// (probe `has_blob`, DHT announce) must not change behavior when a fault
    /// is now distinguishable one layer down.
    #[tokio::test]
    async fn origin_probe_size_folds_fault_to_none_like_absent() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let hash = Hash::new(b"slow object for size wrapper");
        let (origin, _calls) = CountingSizeOrigin::slow(hash, 100, Duration::from_millis(400));
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
        engine.set_origin_probe_config(OriginProbePolicy {
            positive_ttl: Duration::from_secs(15),
            negative_ttl: Duration::from_secs(2),
            fault_ttl: Duration::from_secs(5),
            timeout: Duration::from_millis(20),
            capacity: 16,
        });

        assert_eq!(
            engine.origin_probe_size(hash).await,
            None,
            "a fault still folds to None through the size wrapper",
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

        let held: HashSet<Hash> = engine.origin_held_snapshot().hashes;
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
            !engine.origin_held_snapshot().hashes.contains(&h1),
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

    /// Origin that enumerates a fixed set and whose `size()` can be switched
    /// from answering to faulting, standing in for a `HeadObject` throttle
    /// window that opens between two rescans.
    #[derive(Debug)]
    struct ThrottlableOrigin {
        held: Vec<(Hash, u64)>,
        faulting: Arc<AtomicBool>,
        /// The fault is `Permanent` rather than `Transient` — a revoked ACL or a
        /// symlink escape, which no later rescan resolves.
        permanent: Arc<AtomicBool>,
        enumerate_fails: Arc<AtomicBool>,
        /// The origin still *lists* `held` but no longer serves it, so `size`
        /// answers an authoritative `Ok(None)`.
        holds: Arc<AtomicBool>,
        /// Milliseconds each `size` takes, so a test can keep a pass in flight
        /// while other triggers arrive.
        slow_ms: Arc<AtomicU64>,
        size_calls: Arc<AtomicUsize>,
    }

    impl ThrottlableOrigin {
        fn new(held: Vec<(Hash, u64)>) -> (Self, ThrottleControls) {
            let controls = ThrottleControls {
                faulting: Arc::new(AtomicBool::new(false)),
                permanent: Arc::new(AtomicBool::new(false)),
                enumerate_fails: Arc::new(AtomicBool::new(false)),
                holds: Arc::new(AtomicBool::new(true)),
                slow_ms: Arc::new(AtomicU64::new(0)),
                size_calls: Arc::new(AtomicUsize::new(0)),
            };
            (
                Self {
                    held,
                    faulting: Arc::clone(&controls.faulting),
                    permanent: Arc::clone(&controls.permanent),
                    enumerate_fails: Arc::clone(&controls.enumerate_fails),
                    holds: Arc::clone(&controls.holds),
                    slow_ms: Arc::clone(&controls.slow_ms),
                    size_calls: Arc::clone(&controls.size_calls),
                },
                controls,
            )
        }
    }

    /// The switches and the probe counter a test drives [`ThrottlableOrigin`] by.
    struct ThrottleControls {
        faulting: Arc<AtomicBool>,
        permanent: Arc<AtomicBool>,
        enumerate_fails: Arc<AtomicBool>,
        holds: Arc<AtomicBool>,
        slow_ms: Arc<AtomicU64>,
        size_calls: Arc<AtomicUsize>,
    }

    impl Origin for ThrottlableOrigin {
        fn kind(&self) -> OriginKind {
            OriginKind::S3
        }

        fn fetch(
            &self,
            _hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
        {
            Box::pin(async { Ok(OriginFetch::NotFound) })
        }

        fn size(
            &self,
            hash: Hash,
        ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, crate::OriginPullError>> + Send + '_>>
        {
            Box::pin(async move {
                self.size_calls.fetch_add(1, Ordering::SeqCst);
                let slow = self.slow_ms.load(Ordering::SeqCst);
                if slow > 0 {
                    tokio::time::sleep(Duration::from_millis(slow)).await;
                }
                if self.faulting.load(Ordering::SeqCst) {
                    return Err(if self.permanent.load(Ordering::SeqCst) {
                        crate::OriginPullError::Permanent(anyhow::anyhow!(
                            "synthetic HeadObject access denied"
                        ))
                    } else {
                        crate::OriginPullError::Transient(anyhow::anyhow!(
                            "synthetic HeadObject throttle (503 SlowDown)"
                        ))
                    });
                }
                if !self.holds.load(Ordering::SeqCst) {
                    return Ok(None);
                }
                Ok(self
                    .held
                    .iter()
                    .find(|(h, _)| *h == hash)
                    .map(|(_, len)| *len))
            })
        }

        fn enumerate(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<Hash>, crate::OriginPullError>> + Send + '_>>
        {
            Box::pin(async {
                if self.enumerate_fails.load(Ordering::SeqCst) {
                    return Err(crate::OriginPullError::Transient(anyhow::anyhow!(
                        "synthetic ListObjectsV2 outage"
                    )));
                }
                Ok(self.held.iter().map(|(h, _)| *h).collect())
            })
        }
    }

    /// A faulted size probe must not evict a hash from the origin-held index.
    ///
    /// The index is what probe and DHT-announce advertise. A transport fault is
    /// not an authoritative absence, so dropping on one makes a throttle window
    /// the origin recovers from in seconds cost a whole rescan interval of
    /// announce coverage — with every downstream signal reporting success.
    #[tokio::test]
    async fn rescan_origins_carries_a_faulted_probe_forward() -> anyhow::Result<()> {
        let indexed = Hash::new(b"throttle-indexed");
        let fresh = Hash::new(b"throttle-fresh");
        let (origin, controls) = ThrottlableOrigin::new(vec![(indexed, 5000), (fresh, 9000)]);
        let origin = Arc::new(origin) as Arc<dyn Origin>;

        let tmp = tempfile::tempdir()?;
        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![origin],
            10,
            // `fresh` is also pinned, so it reaches the probe loop twice — once
            // from the listing and once from the pin set. It must be probed once.
            crate::PinnedHashes::new([from_store_hash(fresh)].into_iter().collect()),
            crate::RetryPolicy::default(),
            CircuitBreakerPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        // First rescan resolves `indexed` only: `fresh` is enumerated but the
        // origin is asked about it after the throttle opens.
        engine.set_denied(&crate::DeniedHashes::new(
            [from_store_hash(fresh)].into_iter().collect(),
        ));
        engine.rescan_origins().await;
        anyhow::ensure!(
            engine.origin_held_size(indexed) == Some(5000),
            "the healthy rescan must index the enumerated hash",
        );
        anyhow::ensure!(
            engine.origin_held_snapshot().probe_faults == 0,
            "a healthy rescan reports no faults",
        );

        // The throttle opens, and `fresh` becomes a candidate for the first time.
        engine.set_denied(&crate::DeniedHashes::new(HashSet::new()));
        controls.faulting.store(true, Ordering::SeqCst);
        controls.size_calls.store(0, Ordering::SeqCst);
        engine.rescan_origins().await;

        anyhow::ensure!(
            controls.size_calls.load(Ordering::SeqCst) == 2,
            "each candidate is probed once however many times it is listed, or a \
             duplicate inflates the fault count: {} probes for 2 candidates",
            controls.size_calls.load(Ordering::SeqCst),
        );

        anyhow::ensure!(
            engine.origin_held_size(indexed) == Some(5000),
            "a faulted probe must keep the previous index entry, not drop it",
        );
        anyhow::ensure!(
            engine.origin_held_size(fresh).is_none(),
            "a candidate first seen inside the fault window has nothing to carry forward",
        );
        anyhow::ensure!(
            engine.origin_held_snapshot().probe_faults == 2,
            "both faulted probes must be reported to the DHT seed paths, got {}",
            engine.origin_held_snapshot().probe_faults,
        );

        // The rescan is the metric's only source, so the counter and the
        // per-rescan report must agree.
        anyhow::ensure!(
            cm.origin_probe_failures.get() == 2,
            "faulted probes must surface on their counter, got {}",
            cm.origin_probe_failures.get(),
        );

        // A hash denied since the last rescan must not reach the announce set,
        // even though the index still lists it. `origin_held_snapshot` is what
        // the DHT seed reads, so the live refusal filter has to be applied there
        // and not only on the per-hash lookup.
        engine.set_denied(&crate::DeniedHashes::new(
            [from_store_hash(indexed)].into_iter().collect(),
        ));
        anyhow::ensure!(
            !engine.origin_held_snapshot().hashes.contains(&indexed),
            "a hash denied since the last rescan must not be announced",
        );
        engine.set_denied(&crate::DeniedHashes::new(HashSet::new()));

        // The throttle closes: the next rescan resolves both.
        controls.faulting.store(false, Ordering::SeqCst);
        engine.rescan_origins().await;
        anyhow::ensure!(
            engine.origin_held_size(fresh) == Some(9000),
            "a recovered origin must index what the fault window missed",
        );
        anyhow::ensure!(
            engine.origin_held_snapshot().probe_faults == 0,
            "a recovered rescan clears the fault report",
        );
        Ok(())
    }

    /// Carry-forward must not make an index entry immortal.
    ///
    /// The counterpart risk of keeping a faulted candidate: a hash the origin
    /// genuinely stopped holding has to leave, or the node advertises content it
    /// cannot serve and every request for it becomes a refusal. Only a fault
    /// carries an entry forward — an authoritative `Ok(None)` drops it.
    #[tokio::test]
    async fn rescan_origins_evicts_a_hash_the_origin_no_longer_holds() -> anyhow::Result<()> {
        let goes_away = Hash::new(b"origin-drops-it");
        let (origin, controls) = ThrottlableOrigin::new(vec![(goes_away, 4242)]);
        let origin = Arc::new(origin) as Arc<dyn Origin>;

        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), vec![origin], 10).await?;

        engine.rescan_origins().await;
        anyhow::ensure!(
            engine.origin_held_size(goes_away) == Some(4242),
            "the healthy rescan must index it",
        );

        // The origin still lists it but no longer holds it: `size` answers
        // `Ok(None)`, which is authoritative, not a fault.
        controls.holds.store(false, Ordering::SeqCst);
        engine.rescan_origins().await;

        anyhow::ensure!(
            engine.origin_held_size(goes_away).is_none(),
            "an authoritative absence must drop the entry, not carry it",
        );
        anyhow::ensure!(
            engine.origin_held_snapshot().probe_faults == 0,
            "a clean `not held` answer is not a fault",
        );
        Ok(())
    }

    /// Triggers arriving during a pass collapse into one rerun rather than
    /// queueing.
    ///
    /// A rescan gets slower exactly when the origin is faulting, which is when
    /// the periodic trigger is most likely to fire on top of one. Queueing every
    /// trigger would stack a waiter per tick and then run that backlog of
    /// obsolete passes back to back, against an origin already struggling.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_rescans_collapse_into_one_rerun() -> anyhow::Result<()> {
        let listed = Hash::new(b"rescan-single-flight");
        let (origin, controls) = ThrottlableOrigin::new(vec![(listed, 11)]);
        let origin = Arc::new(origin) as Arc<dyn Origin>;

        let tmp = tempfile::tempdir()?;
        let engine = Arc::new(CacheEngine::open(tmp.path(), vec![origin], 10).await?);

        // Slow enough that the later triggers land while the first pass is
        // still probing.
        controls.slow_ms.store(50, Ordering::SeqCst);

        // Four triggers at once: one claims the slot, the rest collapse into a
        // single queued rerun — two passes over the one candidate, not four.
        controls.size_calls.store(0, Ordering::SeqCst);
        let mut joins = Vec::new();
        for _ in 0..4 {
            let engine = Arc::clone(&engine);
            joins.push(tokio::spawn(async move { engine.rescan_origins().await }));
        }
        for j in joins {
            j.await?;
        }

        let probes = controls.size_calls.load(Ordering::SeqCst);
        anyhow::ensure!(
            (1..=2).contains(&probes),
            "four triggers must collapse into at most one rerun, so at most two \
             passes probe the single candidate; saw {probes}",
        );
        anyhow::ensure!(
            engine.origin_held_size(listed) == Some(11),
            "and the index is still published",
        );

        // The slot is released, so a later trigger still runs.
        controls.slow_ms.store(0, Ordering::SeqCst);
        controls.size_calls.store(0, Ordering::SeqCst);
        engine.rescan_origins().await;
        anyhow::ensure!(
            controls.size_calls.load(Ordering::SeqCst) == 1,
            "a rescan after the burst must still run, or the slot is stranded",
        );
        Ok(())
    }

    /// A permanent fault must not carry an entry forward.
    ///
    /// A revoked ACL or a symlink escape reads the same on every rescan, so
    /// carrying it would hold the hash in the announce set until an operator
    /// intervened — advertising content the serve path refuses, indefinitely.
    #[tokio::test]
    async fn rescan_origins_does_not_carry_a_permanent_fault() -> anyhow::Result<()> {
        let revoked = Hash::new(b"origin-acl-revoked");
        let (origin, controls) = ThrottlableOrigin::new(vec![(revoked, 777)]);
        let origin = Arc::new(origin) as Arc<dyn Origin>;

        let tmp = tempfile::tempdir()?;
        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![origin],
            10,
            crate::PinnedHashes::empty(),
            crate::RetryPolicy::default(),
            CircuitBreakerPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        engine.rescan_origins().await;
        anyhow::ensure!(
            engine.origin_held_size(revoked) == Some(777),
            "the healthy rescan must index it",
        );

        controls.faulting.store(true, Ordering::SeqCst);
        controls.permanent.store(true, Ordering::SeqCst);
        engine.rescan_origins().await;

        let report = engine.origin_held_snapshot();
        anyhow::ensure!(
            !report.hashes.contains(&revoked),
            "a permanent fault must drop the entry rather than advertise content \
             the serve path will refuse for the process lifetime",
        );
        anyhow::ensure!(
            report.probe_faults == 1,
            "it is still a probe that could not answer, so it still counts",
        );
        Ok(())
    }

    /// An origin that cannot be listed drops every hash discoverable only
    /// through it, and must say so.
    ///
    /// This is the more severe half of the same silent shrink: no candidate is
    /// produced, so there is no per-hash fault and nothing to carry forward. A
    /// seed reading only the probe-fault count would call the truncated set
    /// healthy.
    #[tokio::test]
    async fn rescan_origins_reports_an_origin_it_could_not_list() -> anyhow::Result<()> {
        let listed = Hash::new(b"listing-outage");
        let (origin, controls) = ThrottlableOrigin::new(vec![(listed, 1234)]);
        let origin = Arc::new(origin) as Arc<dyn Origin>;

        let tmp = tempfile::tempdir()?;
        let cm = Arc::new(CacheMetrics::default());
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![origin],
            10,
            crate::PinnedHashes::empty(),
            crate::RetryPolicy::default(),
            CircuitBreakerPolicy::default(),
            Some(Arc::clone(&cm)),
            Duration::ZERO,
        )
        .await?;

        engine.rescan_origins().await;
        anyhow::ensure!(
            engine.origin_held_size(listed) == Some(1234),
            "the healthy rescan must index the listed hash",
        );

        controls.enumerate_fails.store(true, Ordering::SeqCst);
        engine.rescan_origins().await;

        let report = engine.origin_held_snapshot();
        anyhow::ensure!(
            report.enumerate_failures == 1,
            "the failed listing must be reported, got {}",
            report.enumerate_failures,
        );
        anyhow::ensure!(
            report.probe_faults == 0,
            "no candidate was produced, so there is no probe to fault",
        );
        anyhow::ensure!(
            !report.hashes.contains(&listed),
            "an unlisted, unpinned hash genuinely leaves the announce set — the \
             point is that the report says so",
        );
        anyhow::ensure!(
            cm.origin_enumerate_failures.get() == 1,
            "the failed listing must surface on its counter, got {}",
            cm.origin_enumerate_failures.get(),
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
        engine.inner.access_times.clear();

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
    async fn observe_hit_forwards_to_frequency_estimator() -> anyhow::Result<()> {
        use crate::policy::FrequencyEstimator;
        use std::sync::atomic::{AtomicU32, Ordering};

        #[derive(Debug, Default)]
        struct Counter(AtomicU32);
        impl FrequencyEstimator for Counter {
            fn observe(&self, _h: Hash) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
            fn estimate(&self, _h: Hash) -> u32 {
                self.0.load(Ordering::Relaxed)
            }
        }

        let tmp = tempfile::tempdir()?;
        let payload = b"hello frequency";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

        let counter = Arc::new(Counter::default());
        engine.set_frequency_estimator(counter.clone());

        let _ = engine.get(hash).await?;

        anyhow::ensure!(counter.estimate(hash) >= 1, "observe should fire on access");
        Ok(())
    }

    /// ADR 040 serve-hit signal: the fill path emits NO hit sighting (so a
    /// fill-and-serve miss never double-counts), and each served request emits
    /// exactly one sighting through the serve chokepoint's
    /// [`CacheEngine::observe_hit`]. A hot RESIDENT blob served repeatedly must
    /// therefore accumulate frequency and become promotable — the case that was
    /// inverted before serve paths emitted the signal.
    #[tokio::test]
    async fn serve_hit_signal_fires_once_per_serve_and_promotes_a_hot_resident_blob()
    -> anyhow::Result<()> {
        use crate::policy::{FrequencyEstimator, ProbationAdmission, Segment, TinyLfuEstimator};

        let tmp = tempfile::tempdir()?;
        let payload = b"hot resident blob";
        let hash = Hash::new(payload);
        let engine = CacheEngine::open(
            tmp.path(),
            vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
            10,
        )
        .await?;

        let promotion_threshold = 3u32;
        let freq: Arc<dyn FrequencyEstimator> = Arc::new(TinyLfuEstimator::new(4096));
        engine.set_frequency_estimator(freq.clone());
        engine.set_admission_policy(Arc::new(ProbationAdmission {
            freq: freq.clone(),
            promotion_threshold,
        }));

        // Fill as a miss. The fill path reads the estimate for admission (0 ->
        // Probation) but must NOT emit the hit signal, so estimate stays 0 — this
        // is what keeps a fill-and-serve miss at one sighting, not two.
        engine.populate_local(hash).await?;
        anyhow::ensure!(
            freq.estimate(hash) == 0,
            "the fill alone must not observe (no double-count with the serve)"
        );
        anyhow::ensure!(
            engine.segment_of(hash) == Segment::Probation,
            "first sight lands in probation"
        );

        // Serve the resident blob repeatedly. Each served request is exactly one
        // sighting via the serve chokepoint's `observe_hit`.
        for i in 1..=promotion_threshold {
            engine.observe_hit(hash);
            anyhow::ensure!(
                freq.estimate(hash) == i,
                "each serve must be exactly one sighting"
            );
        }
        anyhow::ensure!(
            freq.estimate(hash) >= promotion_threshold,
            "a hot resident blob served repeatedly must become promotable"
        );
        Ok(())
    }

    #[tokio::test]
    async fn default_admission_is_main_segment() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"hello admission";
        let hash = Hash::new(payload);
        let origin = StubOrigin::new(payload);

        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

        let ctx = crate::policy::AdmissionContext {
            hash,
            known_size: None,
        };
        assert_eq!(
            engine.admission_segment_for_test(&ctx),
            crate::policy::Segment::Main
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

    /// A serve completion's access record must not wait on a scan of the rest of
    /// the access map.
    ///
    /// `observe_hit` -> `record_access` is the terminal bookkeeping of every
    /// completed serve, and `eviction_candidates` walks every entry on the
    /// eviction sweep. The map is sharded, so the two meet on one shard at a
    /// time: with a scan pinned inside one shard, a record for a hash that lives
    /// in another shard still lands. Under one map-wide lock that record waits
    /// for the whole scan, and the bounded wait below expires.
    #[tokio::test]
    async fn record_access_does_not_wait_on_a_scan_of_another_shard() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;

        // Stand in for a scan sitting inside one shard by holding a guard on
        // that shard. `get_mut` takes the write guard rather than the read guard
        // `eviction_candidates`' walk takes, which is the stronger hold: if a
        // record can land against an exclusive guard on another shard, it can
        // land against a shared one. What is pinned is the shard, which is the
        // property under test.
        let scanned = Hash::new(b"the shard under scan");
        engine.inner.access_times.insert(scanned, Instant::now());
        let Some(scan_guard) = engine.inner.access_times.get_mut(&scanned) else {
            anyhow::bail!("the seeded access-time entry must be present");
        };

        // Find a hash that lives outside the pinned shard: `try_get` reports
        // `Locked` for that shard alone and `Absent` for every other one.
        let completing = (0..1024u32).map(|i| Hash::new(i.to_le_bytes())).find(|h| {
            !matches!(
                engine.inner.access_times.try_get(h),
                dashmap::try_result::TryResult::Locked
            )
        });
        let Some(completing) = completing else {
            anyhow::bail!("no candidate hash landed outside the pinned shard");
        };

        // Record the completion from another thread so a wait is observable as a
        // timeout rather than a hung test.
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let recorder = {
            let engine = engine.clone();
            std::thread::spawn(move || {
                engine.observe_hit(completing);
                let _ = tx.send(());
            })
        };
        let landed = rx.recv_timeout(Duration::from_secs(10)).is_ok();

        // Release the scan and reap the recorder before asserting, so a failure
        // reports rather than leaks the thread.
        drop(scan_guard);
        let joined = recorder.join().is_ok();

        anyhow::ensure!(
            landed,
            "a serve completion's access record waited on a scan of another shard"
        );
        anyhow::ensure!(joined, "the recording thread panicked");
        anyhow::ensure!(
            engine.last_accessed(completing).is_some(),
            "the recorded access must be readable once the scan releases"
        );
        Ok(())
    }

    /// A concurrent record must not make the eviction sweep *lose* a candidate.
    ///
    /// The sharded walk is not point-in-time, by design: a record landing
    /// mid-walk may or may not appear. What it must never do is drop a
    /// hash that was already in the map when the walk started, because
    /// `eviction_candidates` is the only source of eviction candidates — a hash
    /// silently skipped by every sweep is a blob that is never reclaimed, which
    /// is unbounded disk growth. `DashMap::iter` holds each shard's read guard
    /// for that shard's traversal, so a concurrent insert can add to a shard the
    /// walk has not reached but cannot remove from one it has. This pins that.
    ///
    /// A round only counts once its result carries a hash the writer produced
    /// *after the walk began* — the writer's counter is sampled either side of
    /// the walk, and only that window's keys are accepted as witnesses. A key
    /// written before the walk started proves nothing: the walk would find it in
    /// a quiet map too. Scheduling decides whether a given round lands one, so
    /// rounds repeat until one does; the no-candidate-lost invariant is checked
    /// on every round either way.
    #[tokio::test]
    async fn a_scan_never_loses_a_candidate_to_a_concurrent_record() -> anyhow::Result<()> {
        const WRITER_BASE: u32 = 1_000_000;
        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;

        // Seed enough hashes to spread across every shard and to give the walk
        // enough work that a concurrent writer can get inside it.
        let seeded: Vec<Hash> = (0..4096u32).map(|i| Hash::new(i.to_le_bytes())).collect();
        for h in &seeded {
            engine.inner.access_times.insert(*h, Instant::now());
        }

        let mut overlapped = false;
        for _ in 0..16 {
            // Hammer the map with fresh hashes for the duration of the walk.
            let stop = Arc::new(AtomicBool::new(false));
            let written = Arc::new(AtomicU64::new(0));
            let writer = {
                let engine = engine.clone();
                let stop = Arc::clone(&stop);
                let written = Arc::clone(&written);
                std::thread::spawn(move || {
                    let mut i = WRITER_BASE;
                    while !stop.load(Ordering::Relaxed) {
                        engine.observe_hit(Hash::new(i.to_le_bytes()));
                        written.fetch_add(1, Ordering::Relaxed);
                        i = i.saturating_add(1);
                    }
                })
            };

            // Do not start the walk until the writer is provably running, or the
            // scan can finish before the thread is even scheduled. Bounded, so a
            // writer that dies before its first record fails the test instead of
            // hanging it with the panic trapped in an unjoined thread.
            let spin_deadline = Instant::now() + Duration::from_secs(10);
            while written.load(Ordering::Relaxed) == 0 {
                anyhow::ensure!(
                    Instant::now() < spin_deadline,
                    "the recording thread never recorded an access"
                );
                std::thread::yield_now();
            }

            // The writer bumps its counter *after* the insert lands, so keys
            // `[before, after)` are exactly those it could have written while
            // the walk was running.
            let before = written.load(Ordering::Relaxed);
            let candidates = engine.eviction_candidates();
            let after = written.load(Ordering::Relaxed);
            stop.store(true, Ordering::Relaxed);
            anyhow::ensure!(writer.join().is_ok(), "the recording thread panicked");

            let missing = seeded
                .iter()
                .filter(|h| !candidates.contains_key(h))
                .count();
            anyhow::ensure!(
                missing == 0,
                "the sweep dropped {missing} of {} pre-existing candidates",
                seeded.len()
            );

            // Did this round's walk actually see a record that landed inside it?
            let during: HashSet<Hash> = (before..after)
                .filter_map(|n| u32::try_from(n).ok())
                .map(|n| Hash::new(WRITER_BASE.saturating_add(n).to_le_bytes()))
                .collect();
            overlapped |= candidates.iter().any(|(h, _)| during.contains(h));
            if overlapped {
                break;
            }
        }
        anyhow::ensure!(
            overlapped,
            "no round overlapped the writer, so the walk was never concurrent"
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
    /// path fills via `populate`/`populate_local`.
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

    /// The drop-path half of #1517. If `InflightGuard::drop` swallowed the
    /// `PoisonError` and skipped the removal, then — since `notify_waiters`
    /// only wakes *current* waiters — the leaked entry would make every later
    /// request for that hash park on a `Notify` that never fires again — a
    /// permanent hang, which is the exact failure the guard exists to prevent.
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
        engine
            .inner
            .access_times
            .insert(pinned_hash, Instant::now());
        engine
            .inner
            .access_times
            .insert(evictable_hash, Instant::now());

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
        engine.inner.access_times.insert(clean_hash, Instant::now());
        engine
            .inner
            .access_times
            .insert(denied_hash, Instant::now());

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
        engine.inner.access_times.insert(hash, Instant::now());
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
        engine.inner.access_times.insert(h1, Instant::now());
        engine.inner.access_times.insert(h2, Instant::now());

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

    /// `serve_audit` folds presence + size + eviction into one store contact
    /// (#1789 item 7 part B) and matches the `has`/`inspect` pairing the
    /// delivery path would otherwise make: a complete blob is `Serveable` with
    /// its size, an absent hash is `Unavailable`, and an evicted hash is
    /// `Unavailable` with `evicted` set so the serve path tells an eviction
    /// from a plain miss without a second call.
    #[tokio::test]
    async fn serve_audit_reports_presence_size_and_eviction() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"a served blob";
        let origin = StubOrigin::new(payload);
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
        let hash = Hash::new(payload);
        let unseen = Hash::new(b"never cached");

        anyhow::ensure!(
            engine.serve_audit(unseen).await? == ServeAudit::Unavailable { evicted: false },
            "an absent hash is unavailable and not evicted"
        );

        engine.populate(hash).await?;
        anyhow::ensure!(
            engine.serve_audit(hash).await?
                == ServeAudit::Serveable {
                    size: payload.len() as u64
                },
            "a complete blob is serveable and carries its size"
        );

        engine.evict(hash).await?;
        anyhow::ensure!(
            engine.serve_audit(hash).await? == ServeAudit::Unavailable { evicted: true },
            "evicted content is unavailable with the eviction surfaced"
        );
        Ok(())
    }

    /// A complete blob that a gate refuses is `Unavailable` and carries NO
    /// size, so a caller cannot advertise the wire size of content the serve
    /// path would refuse. `evicted` stays false: a chain-denied hash is a
    /// different refusal from an eviction and the miss path must not report it
    /// as `EvictedSinceProbe`.
    #[tokio::test]
    async fn serve_audit_withholds_the_size_of_a_denied_blob() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let payload = b"denied but on disk";
        let origin = StubOrigin::new(payload);
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
        let hash = Hash::new(payload);
        engine.populate(hash).await?;

        engine.set_chain_denied_one(hash, true);
        let audit = engine.serve_audit(hash).await?;
        anyhow::ensure!(
            audit == ServeAudit::Unavailable { evicted: false },
            "a chain-denied blob is unavailable, un-evicted, and sizeless"
        );
        anyhow::ensure!(
            audit.hit_size().is_none(),
            "a refused blob never yields a wire size"
        );
        Ok(())
    }

    /// A genuinely empty blob is `Serveable { size: 0 }`. The delivery path
    /// keys its "fill then size it yourself" branch on the absence of a size,
    /// so a zero-length blob must not read as a miss.
    #[tokio::test]
    async fn serve_audit_reports_a_zero_length_blob_as_serveable() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = StubOrigin::new(b"");
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
        let hash = Hash::new(b"");
        engine.populate(hash).await?;

        let audit = engine.serve_audit(hash).await?;
        anyhow::ensure!(
            audit == ServeAudit::Serveable { size: 0 },
            "an empty blob is serveable at size 0"
        );
        anyhow::ensure!(
            audit.hit_size() == Some(0),
            "the delivery path reads 0, not a missing size"
        );
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

    /// The lock-free `evicted` set (#1789 item 5) must never lose a write and
    /// never present a torn view.
    ///
    /// CONCURRENT WRITERS are the point: several tasks evict disjoint hashes at
    /// once, and every one of them must be evicted at the end. A publish that
    /// read a snapshot, cloned it and stored it without serializing — the
    /// obvious "one less allocation" rewrite of
    /// [`MonotoneHashSet::insert_if_absent`] — drops whichever writer lost the
    /// race, and `evict` would have returned `Ok(())` while the hash kept
    /// serving. That is a takedown failure, so it is asserted directly.
    ///
    /// Readers run alongside on real worker threads and assert monotonicity: a
    /// hash observed evicted stays evicted. The reader half fails safe (it can
    /// only under-observe under scheduling pressure), so it also asserts it saw
    /// something, otherwise a starved reader would assert nothing at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn evicted_set_stays_consistent_under_concurrent_evict() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let engine = Arc::new(empty_engine(tmp.path()).await?);
        let hashes: Vec<Hash> = (0..64)
            .map(|i| Hash::new(format!("concurrent-evict-{i}").as_bytes()))
            .collect();
        let stop = Arc::new(AtomicBool::new(false));

        // Eight writers over disjoint slices of the hash set, all evicting at
        // once. A lost update leaves one of the slices un-evicted.
        let mut writers = Vec::new();
        for chunk in hashes.chunks(8) {
            let engine = Arc::clone(&engine);
            let chunk: Vec<Hash> = chunk.to_vec();
            writers.push(tokio::spawn(async move {
                for h in &chunk {
                    engine.evict(*h).await?;
                }
                Ok::<_, anyhow::Error>(())
            }));
        }

        let mut readers = Vec::new();
        for _ in 0..8 {
            let engine = Arc::clone(&engine);
            let hashes = hashes.clone();
            let stop = Arc::clone(&stop);
            readers.push(tokio::spawn(async move {
                let mut saw_evicted = HashSet::new();
                for _ in 0..50_000 {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    for h in &hashes {
                        if engine.is_evicted(*h) {
                            saw_evicted.insert(*h);
                        }
                    }
                    tokio::task::yield_now().await;
                }
                Ok::<_, anyhow::Error>(saw_evicted)
            }));
        }

        for w in writers {
            w.await
                .map_err(|e| anyhow::anyhow!("writer task panicked: {e}"))??;
        }
        stop.store(true, Ordering::Relaxed);

        for h in &hashes {
            anyhow::ensure!(
                engine.is_evicted(*h),
                "{h} was evicted by a concurrent writer but is not in the set — lost update"
            );
        }

        let mut any_observed = false;
        for r in readers {
            let saw = r
                .await
                .map_err(|e| anyhow::anyhow!("reader task panicked: {e}"))??;
            any_observed |= !saw.is_empty();
            for h in &saw {
                anyhow::ensure!(
                    engine.is_evicted(*h),
                    "reader once saw {h} evicted but it is not evicted at the end — torn view"
                );
            }
        }
        anyhow::ensure!(
            any_observed,
            "no reader observed any eviction — the monotonicity half asserted nothing"
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
        engine.observe_hit(hash); // make it an LRU candidate

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

    // -- FA.1a: origin_encode_range / origin_fetch_outboard_bytes (Flow A) --

    /// A test origin that serves chunk-group-aligned ranges plus a configurable
    /// `{H}.obao4` outboard, so the Flow A raw fetch+encode surface can be
    /// exercised without an HTTP/S3/fs backend. `data`/`outboard`/`size` are held
    /// independently so a test can serve bytes that do NOT hash to `hash` (a
    /// corrupt / misconfigured OWN origin, the local-origin-fault case).
    /// `support_range == false` models an origin with no `206`/outboard support,
    /// i.e. the [`OriginRangeFetch::Unsupported`] degrade.
    #[derive(Debug)]
    struct RangeStubOrigin {
        hash: Hash,
        data: Bytes,
        outboard: Option<Bytes>,
        size: u64,
        support_range: bool,
        /// When true, [`Origin::fetch_outboard`] returns a transport
        /// [`OriginPullError`] instead of a clean decline — the degraded-origin
        /// case the serviceability probe must surface as a fault (#1129), not as a
        /// clean `Ok(None)` absence.
        fault_outboard: bool,
    }

    impl RangeStubOrigin {
        /// An origin that serves `payload` and its genuine outboard for `hash`.
        fn serving(hash: Hash, payload: &[u8], outboard: Bytes) -> Self {
            Self {
                hash,
                data: Bytes::from(payload.to_vec()),
                outboard: Some(outboard),
                size: u64::try_from(payload.len()).unwrap_or(u64::MAX),
                support_range: true,
                fault_outboard: false,
            }
        }

        /// An origin that knows `hash`'s size but FAULTS its outboard fetch with a
        /// transport error — a degraded own origin, distinct from a clean absence.
        fn outboard_faulting(hash: Hash, size: u64) -> Self {
            Self {
                hash,
                data: Bytes::new(),
                outboard: None,
                size,
                support_range: false,
                fault_outboard: true,
            }
        }
    }

    impl Origin for RangeStubOrigin {
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
            let out = (hash == self.hash).then_some(self.size);
            Box::pin(async move { Ok(out) })
        }

        fn fetch_outboard(
            &self,
            hash: Hash,
            _outboard_max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, crate::OriginPullError>> + Send + '_>>
        {
            if self.fault_outboard && hash == self.hash {
                return Box::pin(async move {
                    Err(crate::OriginPullError::Transient(anyhow::anyhow!(
                        "stub outboard transport fault"
                    )))
                });
            }
            let result = match (&self.outboard, hash == self.hash) {
                (Some(ob), true) => OutboardFetch::Found(ob.clone()),
                _ => OutboardFetch::NotFound,
            };
            Box::pin(async move { Ok(result) })
        }

        fn fetch_range(
            &self,
            hash: Hash,
            req: OriginRangeRequest,
            _outboard_max_bytes: u64,
        ) -> Pin<
            Box<dyn Future<Output = Result<OriginRangeFetch, crate::OriginPullError>> + Send + '_>,
        > {
            let result = match (&self.outboard, hash == self.hash && self.support_range) {
                (Some(ob), true) => {
                    let s = usize::try_from(req.fetch_start).unwrap_or(usize::MAX);
                    let e = usize::try_from(req.fetch_end).unwrap_or(usize::MAX);
                    match self.data.get(s..e) {
                        Some(span) => OriginRangeFetch::Ranged {
                            data: Bytes::copy_from_slice(span),
                            outboard: ob.clone(),
                        },
                        None => OriginRangeFetch::NotFound,
                    }
                }
                _ => OriginRangeFetch::Unsupported,
            };
            Box::pin(async move { Ok(result) })
        }
    }

    /// A correct origin: `origin_encode_range` yields header-full wire whose
    /// header-less body `admit_bao_stream` accepts and stores under `H`.
    #[tokio::test]
    async fn origin_encode_range_yields_admittable_wire() -> anyhow::Result<()> {
        use bao_tree::io::outboard::PreOrderMemOutboard;

        let data = local_outboard_pull_test_blob();
        let ob = PreOrderMemOutboard::create(&data, crate::range_pull::IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let outboard = Bytes::from(ob.data.clone());
        let hash = Hash::from(root);
        let total = u64::try_from(data.len()).unwrap_or(u64::MAX);

        let origin = RangeStubOrigin::serving(hash, &data, outboard);
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

        let aligned = crate::range_pull::align_range(0, 0, total)
            .map_err(|e| anyhow::anyhow!("align: {e}"))?;
        let Some(combined) = engine.origin_encode_range(hash, &aligned).await? else {
            anyhow::bail!("expected Some(wire) — the origin serves the range + outboard");
        };
        anyhow::ensure!(
            combined.len() > 8,
            "origin_encode_range must return the header-full wire (8-byte size header intact)"
        );

        // Round-trip: strip the header (the node-side caller's job) and admit the
        // header-less wire into a FRESH engine, which verifies it against `H`.
        let header_less = combined.slice(8..);
        let tmp2 = tempfile::tempdir()?;
        let engine2 = CacheEngine::open(tmp2.path(), vec![], 64).await?;
        let drained = engine2
            .admit_bao_stream(
                hash,
                aligned.chunk_ranges().clone(),
                total,
                header_less,
                None,
            )
            .await
            .map_err(|(_reader, e)| e)?;
        anyhow::ensure!(drained.is_empty(), "the wire is fully drained by admit");
        anyhow::ensure!(
            engine2.present_ranges(hash).await?.is_complete(),
            "the whole-blob wire must reconstruct a complete blob under H"
        );
        anyhow::ensure!(
            engine2.get(hash).await?.as_ref() == data.as_slice(),
            "the reconstructed content must be byte-exact"
        );
        Ok(())
    }

    /// A corrupt own origin (bytes that do NOT hash to `H`, served with the
    /// genuine outboard) is a HARD [`CacheError::VerifyFailed`] — never degraded.
    #[tokio::test]
    async fn origin_encode_range_hard_faults_on_mismatch() -> anyhow::Result<()> {
        use bao_tree::io::outboard::PreOrderMemOutboard;

        let genuine = local_outboard_pull_test_blob();
        let ob = PreOrderMemOutboard::create(&genuine, crate::range_pull::IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let outboard = Bytes::from(ob.data.clone());
        let hash = Hash::from(root);
        let total = u64::try_from(genuine.len()).unwrap_or(u64::MAX);

        // Same length, different bytes: the served span will not verify against H.
        let corrupt: Vec<u8> = genuine.iter().map(|b| b ^ 0xFF).collect();
        anyhow::ensure!(Hash::new(&corrupt) != hash, "fixtures must differ");
        let origin = RangeStubOrigin {
            hash,
            data: Bytes::from(corrupt),
            outboard: Some(outboard),
            size: total,
            support_range: true,
            fault_outboard: false,
        };
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

        let aligned = crate::range_pull::align_range(0, 0, total)
            .map_err(|e| anyhow::anyhow!("align: {e}"))?;
        let err = engine
            .origin_encode_range(hash, &aligned)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected VerifyFailed, got Ok"))?;
        anyhow::ensure!(
            matches!(err, CacheError::VerifyFailed { expected } if expected == hash),
            "a corrupt own origin must be a hard VerifyFailed, got {err:?}"
        );
        Ok(())
    }

    /// An origin with no range support degrades to `Ok(None)` — the caller then
    /// falls through to a whole-blob path.
    #[tokio::test]
    async fn origin_encode_range_none_when_unsupported() -> anyhow::Result<()> {
        let data = local_outboard_pull_test_blob();
        let hash = Hash::new(&data);
        let total = u64::try_from(data.len()).unwrap_or(u64::MAX);
        let origin = RangeStubOrigin {
            hash,
            data: Bytes::from(data.clone()),
            outboard: None,
            size: total,
            support_range: false,
            fault_outboard: false,
        };
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

        let aligned = crate::range_pull::align_range(0, 0, total)
            .map_err(|e| anyhow::anyhow!("align: {e}"))?;
        anyhow::ensure!(
            engine.origin_encode_range(hash, &aligned).await?.is_none(),
            "an unsupported origin must degrade origin_encode_range to Ok(None)"
        );
        Ok(())
    }

    /// `origin_fetch_outboard_bytes` returns the outboard when an origin
    /// publishes it, and `None` when none do.
    #[tokio::test]
    async fn origin_fetch_outboard_bytes_found_and_absent() -> anyhow::Result<()> {
        use bao_tree::io::outboard::PreOrderMemOutboard;

        let data = local_outboard_pull_test_blob();
        let ob = PreOrderMemOutboard::create(&data, crate::range_pull::IROH_BLOCK_SIZE);
        let outboard = Bytes::from(ob.data.clone());
        let hash = Hash::new(&data);
        let total = u64::try_from(data.len()).unwrap_or(u64::MAX);

        let serving = RangeStubOrigin::serving(hash, &data, outboard.clone());
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(serving) as Arc<dyn Origin>], 64).await?;
        anyhow::ensure!(
            engine.origin_fetch_outboard_bytes(hash, total).await? == Some(outboard),
            "a publishing origin must return its outboard bytes"
        );

        let bare = OutboardStubOrigin::new(&data, None);
        let tmp2 = tempfile::tempdir()?;
        let engine2 =
            CacheEngine::open(tmp2.path(), vec![Arc::new(bare) as Arc<dyn Origin>], 64).await?;
        anyhow::ensure!(
            engine2
                .origin_fetch_outboard_bytes(hash, total)
                .await?
                .is_none(),
            "no origin publishes the outboard; must be Ok(None)"
        );
        Ok(())
    }

    /// A genuine transport fault fetching the outboard (a degraded own origin) is
    /// surfaced as `Err`, NOT collapsed into a clean `Ok(None)` absence — so the
    /// serviceability caller can latch it into `fault_seen` (#1129) and terminate a
    /// resulting miss as `InternalError` rather than a bare `NotFound`.
    #[tokio::test]
    async fn origin_fetch_outboard_bytes_surfaces_a_transport_fault() -> anyhow::Result<()> {
        let data = local_outboard_pull_test_blob();
        let hash = Hash::new(&data);
        let total = u64::try_from(data.len()).unwrap_or(u64::MAX);

        let faulting = RangeStubOrigin::outboard_faulting(hash, total);
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(faulting) as Arc<dyn Origin>], 64).await?;
        anyhow::ensure!(
            engine
                .origin_fetch_outboard_bytes(hash, total)
                .await
                .is_err(),
            "an outboard transport fault must surface as Err, not a clean Ok(None)"
        );
        Ok(())
    }

    // -- #1607: admit_bao tags its partial so it survives GC --

    fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, bytes::Bytes) {
        let mut plaintext = vec![0u8; len];
        let mut x: u32 = 0x9e37_79b9;
        for b in &mut plaintext {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x.to_le_bytes()[0];
        }
        let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
            &plaintext,
            crate::range_pull::IROH_BLOCK_SIZE,
        );
        (*ob.root.as_bytes(), plaintext, bytes::Bytes::from(ob.data))
    }

    fn bao_for(
        root: [u8; 32],
        plaintext: &[u8],
        outboard: bytes::Bytes,
        off: u64,
        len: u64,
        total: u64,
    ) -> (Hash, bao_tree::ChunkRanges, bytes::Bytes) {
        let aligned = crate::range_pull::align_range(off, len, total).unwrap();
        let s = aligned.fetch_start() as usize;
        let e = aligned.fetch_end() as usize;
        let encoded =
            crate::range_pull::encode_verified_range(root, &aligned, &plaintext[s..e], outboard)
                .unwrap();
        (Hash::from(root), aligned.chunk_ranges().clone(), encoded)
    }

    async fn count_tags_for(engine: &CacheEngine, hash: Hash) -> usize {
        let mut stream = engine.inner.store.tags().list().await.unwrap();
        let mut n = 0usize;
        while let Some(info) = stream.next().await {
            if info.unwrap().hash == hash {
                n += 1;
            }
        }
        n
    }

    #[tokio::test]
    async fn admit_bao_tags_the_partial() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
        let group = crate::CHUNK_GROUP_BYTES;
        let total = 4 * group;
        let (root, plaintext, outboard) = synth_blob(total as usize);

        // Admit one interior group -> a genuine partial.
        let (hash, ranges, bao) = bao_for(root, &plaintext, outboard.clone(), group, group, total);
        engine.admit_bao(hash, ranges, bao).await.unwrap();
        assert!(
            !engine.present_ranges(hash).await.unwrap().is_complete(),
            "still partial"
        );
        assert_eq!(
            count_tags_for(&engine, hash).await,
            1,
            "partial admit creates exactly one protecting tag"
        );

        // Idempotent: admit a second group -> still exactly one tag.
        let (h2, r2, b2) = bao_for(root, &plaintext, outboard, 2 * group, group, total);
        engine.admit_bao(h2, r2, b2).await.unwrap();
        assert_eq!(
            count_tags_for(&engine, hash).await,
            1,
            "re-admit does not proliferate tags"
        );
    }

    #[tokio::test]
    async fn protect_partial_skips_store_write_when_memoized() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
        let group = crate::CHUNK_GROUP_BYTES;
        let total = 4 * group;
        let (root, plaintext, outboard) = synth_blob(total as usize);

        // First admit writes the protecting tag and memoizes it.
        let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);
        engine.admit_bao(hash, ranges, bao).await.unwrap();
        assert_eq!(count_tags_for(&engine, hash).await, 1);
        assert!(engine.inner.partial_protected.contains_key(&hash));

        // Delete the tag directly at the store, leaving the memo intact. A
        // memoized `protect_partial` must short-circuit and NOT re-create it —
        // proving it skipped the redundant store write on re-admit.
        let name = format!("decdn-partial-{hash}");
        engine
            .inner
            .store
            .tags()
            .delete(name.as_bytes())
            .await
            .unwrap();
        assert_eq!(count_tags_for(&engine, hash).await, 0);
        engine.protect_partial(hash).await.unwrap();
        assert_eq!(
            count_tags_for(&engine, hash).await,
            0,
            "a memoized protect_partial must skip the store write"
        );
    }

    #[tokio::test]
    async fn dropping_tags_reinvalidates_protect_memo() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
        let group = crate::CHUNK_GROUP_BYTES;
        let total = 4 * group;
        let (root, plaintext, outboard) = synth_blob(total as usize);

        let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);
        engine.admit_bao(hash, ranges, bao).await.unwrap();
        assert!(engine.inner.partial_protected.contains_key(&hash));

        // The tag-drop path clears the memo, so a re-admit re-protects rather
        // than trusting a stale entry for a tag that no longer exists.
        engine.drop_named_tags_for(hash).await.unwrap();
        assert_eq!(count_tags_for(&engine, hash).await, 0);
        assert!(
            !engine.inner.partial_protected.contains_key(&hash),
            "dropping the tag must invalidate the memo"
        );
        engine.protect_partial(hash).await.unwrap();
        assert_eq!(
            count_tags_for(&engine, hash).await,
            1,
            "protect_partial re-creates the tag after the memo is invalidated"
        );
    }

    // -- admit_bao_stream — O(chunk-group) streaming range admit --

    #[tokio::test]
    async fn admit_bao_stream_admits_a_partial_range() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
        let group = crate::CHUNK_GROUP_BYTES;
        let total = 4 * group;
        let (root, plaintext, outboard) = synth_blob(total as usize);
        let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);

        // `bao_for` (via `encode_verified_range`) prepends the 8-byte LE size
        // header the in-memory `import_bao_bytes` path expects. The wire
        // `admit_bao_stream` consumes is header-less (ADR 038) — the size
        // comes from `total_bytes` instead — so strip it here to synthesize
        // that header-less wire for the reader.
        assert!(bao.len() > 8, "bao_for output must carry the 8-byte header");
        let header_less = bao.slice(8..);

        let reader = engine
            .admit_bao_stream(hash, ranges.clone(), total, header_less, None)
            .await
            .map_err(|(_reader, e)| e)
            .unwrap();
        assert_eq!(reader.len(), 0, "the reader is fully drained");

        let present = engine.present_ranges(hash).await.unwrap();
        assert!(!present.is_complete(), "still partial");
        assert!(!present.is_empty(), "the admitted range is present");
        assert_eq!(
            count_tags_for(&engine, hash).await,
            1,
            "streaming admit creates exactly one protecting tag"
        );
    }

    #[tokio::test]
    async fn admit_bao_stream_captures_proof_into_the_session_during_import() {
        // With a serve leg attached, the decode pass captures the range's proof
        // nodes straight into the shared session — no post-admit `export_bao`
        // read-back. Prove the captured set equals exactly what the read-back would
        // have recovered, so a serve leg reads back an identical outboard.
        use bao_tree::io::fsm::Outboard;
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
        let group = crate::CHUNK_GROUP_BYTES;
        let total = 4 * group;
        let (root, plaintext, outboard) = synth_blob(total as usize);
        let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);
        let header_less = bao.slice(8..);

        let session = crate::FillSession::new(bao_tree::blake3::Hash::from(root), total);
        engine
            .admit_bao_stream(hash, ranges.clone(), total, header_less, Some(&session))
            .await
            .map_err(|(_reader, e)| e)
            .unwrap();

        // The proof nodes an `export_bao` read-back would recover for this range.
        let expected = engine.outboard_pairs(hash, &ranges).await.unwrap();
        assert!(
            !expected.is_empty(),
            "the admitted range spans interior proof nodes"
        );
        // Every one was captured into the session during import: a reader minted
        // from the session loads each without awaiting a further fill.
        let mut reader = session.outboard_reader();
        for (node, pair) in expected {
            assert_eq!(
                reader.load(node).await.unwrap(),
                Some(pair),
                "node {node:?} was captured during import"
            );
        }
    }

    #[tokio::test]
    async fn admit_bao_stream_handles_zero_total_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();

        // The canonical empty blob is a no-op success: nothing to decode or admit.
        engine
            .admit_bao_stream(Hash::EMPTY, ChunkRanges::empty(), 0, Bytes::new(), None)
            .await
            .map_err(|(_reader, e)| e)
            .expect("admitting the empty blob is a no-op success");

        // A zero size under any OTHER hash is an upstream inconsistency (the signed
        // total_bytes disagrees with a non-empty content hash) — a Store fault.
        let (_reader, err) = engine
            .admit_bao_stream(
                Hash::from([9u8; 32]),
                ChunkRanges::empty(),
                0,
                Bytes::new(),
                None,
            )
            .await
            .expect_err("zero size under a non-empty hash is rejected");
        assert!(
            matches!(err, CacheError::Store(_)),
            "expected Store, got {err:?}"
        );
    }

    #[tokio::test]
    async fn admit_bao_stream_rejects_corrupt_bao() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
        let group = crate::CHUNK_GROUP_BYTES;
        let total = 4 * group;
        let (root, plaintext, outboard) = synth_blob(total as usize);
        let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);
        let mut corrupt = bao.slice(8..).to_vec();
        let flip_at = corrupt.len() / 2;
        let byte = corrupt.get_mut(flip_at).expect("non-empty header-less bao");
        *byte ^= 0xFF;

        let (_reader, err) = engine
            .admit_bao_stream(hash, ranges, total, Bytes::from(corrupt), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, CacheError::VerifyFailed { expected } if expected == hash),
            "expected VerifyFailed, got {err:?}"
        );
        assert!(
            engine.present_ranges(hash).await.unwrap().is_empty(),
            "nothing admitted from a corrupt bao"
        );
        assert_eq!(
            count_tags_for(&engine, hash).await,
            0,
            "a rejected import must not tag a partial"
        );
    }

    /// An origin that admits the blob into the store itself (as the ported
    /// `NodeOrigin` does) and returns `AlreadyAdmitted`; the engine must then
    /// serve it from the store without re-ingesting.
    ///
    /// The origin needs a handle to the same `CacheEngine` it is registered
    /// on to call `admit_bao_stream`, but `CacheEngine::open` needs the
    /// origin list up front — so the handle is late-bound through a
    /// `OnceLock` set right after `open` returns, mirroring how the node
    /// wires its own origin against the engine it is constructed for.
    #[tokio::test]
    async fn already_admitted_short_circuits_and_serves_from_store() -> anyhow::Result<()> {
        /// Header-less bao wire reader for [`CacheEngine::admit_bao_stream`]
        /// in [`already_admitted_short_circuits_and_serves_from_store`].
        struct AdmitReader(bytes::Bytes);
        impl iroh_io::AsyncStreamReader for AdmitReader {
            async fn read_bytes(&mut self, len: usize) -> std::io::Result<bytes::Bytes> {
                Ok(self.0.split_to(self.0.len().min(len)))
            }
            async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
                if self.0.len() < L {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "short",
                    ));
                }
                let g = self.0.split_to(L);
                let mut out = [0u8; L];
                out.copy_from_slice(&g);
                Ok(out)
            }
        }

        /// An origin whose `fetch` admits the blob into its own engine (via
        /// a late-bound handle — see the test doc comment) and returns
        /// `AlreadyAdmitted`, exactly as the ported `NodeOrigin` will.
        #[derive(Debug)]
        struct AdmittingOrigin {
            engine: std::sync::Arc<std::sync::OnceLock<CacheEngine>>,
            hash: Hash,
            total: u64,
            wire: bytes::Bytes,
            ranges: bao_tree::ChunkRanges,
        }

        impl Origin for AdmittingOrigin {
            fn kind(&self) -> OriginKind {
                OriginKind::Peer
            }

            fn fetch(
                &self,
                hash: Hash,
                _max_bytes: u64,
            ) -> Pin<
                Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>,
            > {
                Box::pin(async move {
                    if hash != self.hash {
                        return Ok(OriginFetch::NotFound);
                    }
                    let engine = self
                        .engine
                        .get()
                        .expect("engine set by the caller right after open");
                    engine
                        .admit_bao_stream(
                            self.hash,
                            self.ranges.clone(),
                            self.total,
                            AdmitReader(self.wire.clone()),
                            None,
                        )
                        .await
                        .map_err(|(_reader, e)| {
                            crate::OriginPullError::Permanent(anyhow::Error::from(e))
                        })?;
                    Ok(OriginFetch::AlreadyAdmitted)
                })
            }
        }

        let group = crate::CHUNK_GROUP_BYTES;
        let total = 5 * group + 321;
        let (root, plaintext, outboard) = synth_blob(total as usize);
        let (hash, ranges, wire) = bao_for(root, &plaintext, outboard, 0, total, total);
        assert!(
            wire.len() > 8,
            "bao_for output must carry the 8-byte header"
        );
        let wire = wire.slice(8..);

        let engine_cell = std::sync::Arc::new(std::sync::OnceLock::<CacheEngine>::new());
        let origin = std::sync::Arc::new(AdmittingOrigin {
            engine: engine_cell.clone(),
            hash,
            total,
            wire,
            ranges: ranges.clone(),
        }) as Arc<dyn Origin>;

        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), vec![origin], 16).await?;
        engine_cell
            .set(engine.clone())
            .map_err(|_| anyhow::anyhow!("engine cell already set"))?;

        // populate (CommitOnly) → blob present without re-ingest.
        engine.populate(hash).await?;
        anyhow::ensure!(
            engine.has(hash).await?,
            "blob must be present after AlreadyAdmitted populate"
        );
        // get (ReturnBytes) → bytes read back from the store equal the content.
        let got = engine.get(hash).await?;
        anyhow::ensure!(
            got.as_ref() == plaintext.as_slice(),
            "served bytes must equal the blob"
        );
        Ok(())
    }

    #[tokio::test]
    async fn tagged_partial_survives_gc_untagged_is_reclaimed() {
        use iroh_blobs::api::blobs::BlobStatus;
        let group = crate::CHUNK_GROUP_BYTES;
        let total = 4 * group;
        let tmp = tempfile::tempdir().unwrap();
        // Short GC interval so the store's internal run_gc loop sweeps quickly.
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![],
            16,
            PinnedHashes::empty(),
            RetryPolicy::disabled(),
            CircuitBreakerPolicy::default(),
            Some(std::sync::Arc::new(CacheMetrics::default())),
            std::time::Duration::from_millis(200),
        )
        .await
        .unwrap();

        // Tagged: normal admit_bao (protect_partial fires).
        let (root_a, pt_a, ob_a) = synth_blob(total as usize);
        let (ha, ra, ba) = bao_for(root_a, &pt_a, ob_a, group, group, total);
        engine.admit_bao(ha, ra, ba).await.unwrap();

        // Control: same shape, distinct hash, imported WITHOUT a tag (pre-#1607).
        let (root_b, pt_b, ob_b) = synth_blob((total + group) as usize); // different len -> different root
        let (hb, rb, bb) = bao_for(root_b, &pt_b, ob_b, group, group, total + group);
        engine
            .inner
            .store
            .blobs()
            .import_bao_bytes(hb, rb, bb)
            .await
            .unwrap();

        assert!(matches!(
            engine.inner.store.blobs().status(ha).await.unwrap(),
            BlobStatus::Partial { .. }
        ));
        assert!(matches!(
            engine.inner.store.blobs().status(hb).await.unwrap(),
            BlobStatus::Partial { .. }
        ));

        // Poll for the control's reclaim rather than sleeping a fixed span:
        // fails fast once GC sweeps (typically the first 200ms interval), and
        // only fails if GC never reclaims the untagged control within a generous
        // budget — robust on slow/loaded CI and independent of the exact GC
        // interval. The control's `NotFound` gates the test, so a genuine GC
        // failure still fails loud; it can never silently pass.
        let deadline = std::time::Duration::from_secs(15);
        let poll = std::time::Duration::from_millis(50);
        let start = std::time::Instant::now();
        loop {
            let reclaimed = matches!(
                engine.inner.store.blobs().status(hb).await.unwrap(),
                BlobStatus::NotFound
            );
            if reclaimed {
                break;
            }
            assert!(
                start.elapsed() < deadline,
                "control never reclaimed within {deadline:?}: GC did not run"
            );
            tokio::time::sleep(poll).await;
        }

        // The tagged partial must STILL be present after the control was swept —
        // proving the tag (not timing) is what protected it.
        assert!(
            matches!(
                engine.inner.store.blobs().status(ha).await.unwrap(),
                BlobStatus::Partial { .. }
            ),
            "tagged partial survives GC"
        );
    }

    /// Drain a serve-leg export stream to completion, prefixed with its
    /// already-pulled `first` item. Every remaining item MUST be `Ok`: an
    /// in-flight reader started before an evict has to keep delivering correct
    /// bytes even after the blob is evicted and GC-swept out of the store.
    /// Consumes (and thus drops) the stream, releasing its handle so the disk
    /// space can free.
    async fn drain_serve_leg(
        first: Bytes,
        mut stream: Pin<Box<dyn futures_util::Stream<Item = CacheResult<Bytes>> + Send>>,
    ) -> Bytes {
        let mut out = bytes::BytesMut::from(&first[..]);
        while let Some(item) = stream.next().await {
            let bytes =
                item.expect("in-flight serve-leg reader must deliver bytes despite evict + GC");
            out.extend_from_slice(&bytes);
        }
        out.freeze()
    }

    /// Poll the store until `hash` reports `NotFound`, or fail after `deadline`.
    /// Used to gate on a GC sweep having reclaimed a blob without pinning the
    /// test to the exact 200ms interval — robust on slow/loaded CI.
    async fn wait_reclaimed(engine: &CacheEngine, hash: Hash, deadline: Duration) {
        use iroh_blobs::api::blobs::BlobStatus;
        let start = std::time::Instant::now();
        loop {
            if matches!(
                engine.inner.store.blobs().status(hash).await.unwrap(),
                BlobStatus::NotFound
            ) {
                return;
            }
            assert!(
                start.elapsed() < deadline,
                "blob {hash} never reclaimed within {deadline:?}: GC did not run"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Multi-observer coalescing (#1656) composes with operator eviction
    /// (#279). A coalesced serve-miss fans one upstream pull out to N serve legs;
    /// each serve leg reads the filling partial through its own in-flight
    /// `export_bao_range_stream` handle. This test reduces that to the cache-level
    /// invariant the node layer relies on: **two in-flight readers over one
    /// partial, evicted mid-serve, both still finish delivering byte-for-byte
    /// correct bytes even after the GC sweep has logically removed the blob.**
    ///
    /// The load-bearing assumption: reader-pinning is
    /// UNCHANGED by coalescing — N serve-leg readers survive an evict + GC sweep
    /// exactly as one reader would, because each holds its own live export handle.
    /// Eviction is a *logical* takedown: it drops the partial's protecting tag and blocks
    /// NEW serves (`has` reports absent), but it does not tear down readers already
    /// in flight.
    ///
    /// One subtlety this pins precisely: the store flips the blob to `NotFound`
    /// the instant the GC sweep deletes it — the logical delete does NOT wait for
    /// the last reader. The in-flight readers still complete correctly because the
    /// bytes they need stay reachable through their open handles until they drop
    /// (the disk space is what frees only after the last handle closes).
    #[tokio::test]
    async fn evict_mid_serve_lets_coalesced_readers_finish_then_reclaims() {
        let group = crate::CHUNK_GROUP_BYTES;
        let total = 8 * group;
        let tmp = tempfile::tempdir().unwrap();
        // Short GC interval so the store's internal run_gc loop sweeps within the
        // test window (same knob as `tagged_partial_survives_gc_...`).
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![],
            16,
            PinnedHashes::empty(),
            RetryPolicy::disabled(),
            CircuitBreakerPolicy::default(),
            Some(Arc::new(CacheMetrics::default())),
            Duration::from_millis(200),
        )
        .await
        .unwrap();

        // The coalesced-fill target: a genuine partial (groups [0, 6g) of an 8g
        // blob), tagged by `admit_bao` exactly as the pull leg tags it.
        let (root, plaintext, outboard) = synth_blob(total as usize);
        let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, 0, 6 * group, total);
        engine.admit_bao(hash, ranges, bao).await.unwrap();
        assert!(
            !engine.present_ranges(hash).await.unwrap().is_complete(),
            "the fill target is a genuine partial"
        );
        assert_eq!(
            count_tags_for(&engine, hash).await,
            1,
            "the partial carries its B0 protecting tag"
        );

        // The exact wire each serve leg must deliver, captured before the evict.
        let expected = engine
            .export_bao_range(hash, 0, 6 * group, total)
            .await
            .unwrap();

        // Two coalesced serve legs: each opens an in-flight verified-range stream
        // and pulls its first frame, so both hold a live export handle when the
        // evict lands.
        let mut leg_a = engine
            .export_bao_range_stream(hash, 0, 6 * group, total)
            .await
            .unwrap();
        let mut leg_b = engine
            .export_bao_range_stream(hash, 0, 6 * group, total)
            .await
            .unwrap();
        let first_a = leg_a.next().await.expect("leg A first frame").unwrap();
        let first_b = leg_b.next().await.expect("leg B first frame").unwrap();

        // Evict mid-serve. Logical takedown: tag dropped, new serves blocked.
        engine.evict(hash).await.unwrap();
        assert!(engine.is_evicted(hash), "evict flag set");
        assert_eq!(
            count_tags_for(&engine, hash).await,
            0,
            "evict drops the B0 protecting tag"
        );
        assert!(
            !engine.has(hash).await.unwrap(),
            "a NEW serve is blocked immediately after evict"
        );

        // Wait — while BOTH serve legs are still held — for the target itself to
        // report `NotFound`. This is the documented subtlety made an assertion:
        // the GC sweep deletes the untagged blob and flips its logical status the
        // instant it runs, without waiting for the in-flight readers. Gating on
        // the target (not a proxy) both proves a sweep ran after the tag drop and
        // pins that the delete does not defer to the last reader.
        wait_reclaimed(&engine, hash, Duration::from_secs(15)).await;

        // The heart of the composition: BOTH in-flight readers, started before the
        // evict, still drain to completion with byte-for-byte identical bytes even
        // though the blob was already logically removed by the sweep above. Each
        // reader's open export handle keeps its bytes reachable — reader-pinning
        // held for two readers exactly as it would for one.
        let served_a = drain_serve_leg(first_a, leg_a).await;
        let served_b = drain_serve_leg(first_b, leg_b).await;
        assert_eq!(
            served_a, expected,
            "serve leg A delivered the full range intact"
        );
        assert_eq!(
            served_b, expected,
            "serve leg B delivered the full range intact"
        );
    }
}
