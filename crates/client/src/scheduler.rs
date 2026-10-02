//! The client's acquire loop (ADR 039 § Dynamic segmentation and
//! tail-stealing): fill a list of byte ranges of one
//! blob from the paid sources a [`SourceSet`] holds, all writing into ONE shared
//! [`IngestStore`], until every byte is present, a fault only the human can fix
//! arrives, the sources agree the blob cannot be had, or the [`StopPolicy`]
//! gives up.
//!
//! # Lanes
//!
//! A lane is one source's paid delivery: a worker future that drives
//! [`fill_gap`] over one range at a time. The loop starts lanes up to
//! [`AcquireEnv::max_lanes`], nearest source first, and only for a source that
//! covers work still to do. A lane ends when it faults or finds nothing to
//! take; the loop reports the fault to the [`SourceSet`], which cools the
//! source, parks it until the deposit rises, or excludes it, and starts
//! another. A cooled source comes back on its cached lane. Lane builds and
//! discovery run as futures inside the loop's one `select!`, so a slow RPC never
//! pauses a streaming lane.
//!
//! # Work
//!
//! The ranges' missing bytes are spread once across the first lanes that start,
//! by discovery-block coverage ([`crate::coverage_plan::spread_segments`]): each
//! block goes to one covering lane, rarest-cover-first, and a run several lanes
//! hold whole is split evenly across them (`split_evenly`). Coverage is a
//! preference (#2225): a lane takes what it covers first, and a block no
//! running lane covers goes to any lane, whose node serves it by pull-through,
//! while discovery keeps looking for a node that covers it. A lane with nothing
//! queued *steals* the aligned second half of the missing remainder of the
//! range in flight it also covers that misses the most ([`steal_split`]), so a
//! fast source keeps helping a slow one, and a lane that joins late starts by
//! stealing. The victim keeps the first half of its remainder, so it still
//! has work after the steal.
//!
//! # Lane correctness: one unit per worker, one worker per lane by default
//!
//! Each worker holds EXACTLY ONE outstanding range at a time (structural: the
//! worker loop drives one `fill_gap` to completion before it picks the next
//! range), and each source has at most one lane. A lane runs one worker, so
//! parallelism comes from having many sources. The one exception (#2231,
//! #2230, #2252): a queued range no idle worker takes, such as a faulted
//! lane's remainder or one of a queue of runs, goes to a busy lane on an
//! extra worker, granted by the lane's [`LaneWiden`], so the range does not
//! wait behind the busy lane's whole range. A lane runs as many extra
//! workers as its `LaneWiden` grants, one per range. The busy lane covers part of the range; or, when part of the range
//! has no running lane that covers it, it is any busy lane not barred from
//! pull-through, and its node serves that part by pull-through. The loop asks
//! as workers end and lanes build, and again after `GROWTH_RETRY` while a
//! `grow` grants nothing, or while a range has no lane to ask and a running
//! lane has a [`LaneWiden`]: a worker that takes its next range wakes no
//! pass (#2252). A lane whose node refuses its extra stream waits,
//! from `GROWTH_RETRY` and doubling, before it is asked again. A lane with a
//! [`LaneWiden`] gives its [`LaneLease`] back when its own worker ends, and
//! starts again only on a stream `grow` grants. The extra worker shares the
//! lane's `(ctx, ledger)`: several streams on one `(signer, provider)` lane
//! share its one voucher ledger, which serializes their issuance (ADR 003 §
//! Concurrent Streams). It takes one piece, never steals, and gives its grant
//! back when it stops. A fault on it stops only that stream and does not cool
//! the node while the lane's own worker runs; once that worker has stopped,
//! the extra is the lane's last live worker and its fault is the lane's,
//! charged once per outage: a node already charged with no verified byte
//! since is not charged again.
//!
//! # Cancellation: closing the double-pay
//!
//! A worker's `fill_gap` runs under a [`tokio::select!`] against a per-lane
//! `CancelHandle` and a progress-relative watchdog, so it can be interrupted
//! mid-fetch. Because [`crate::ClientRangedStore::ingest_stream`] durably
//! checkpoints verified groups every `INGEST_CHECKPOINT_BYTES` as it streams,
//! dropping the `fill_gap` future keeps every checkpointed prefix in the store:
//! [`RangedStore::missing_ranges`](decdn_bao_range::RangedStore::missing_ranges)
//! then reports only the un-checkpointed remainder. A cancel re-fetches at most
//! the unflushed batch plus the detached checkpoints that land after the requeue
//! (`INGEST_MAX_QUEUED_CHECKPOINTS + 1` intervals), never a byte an earlier
//! checkpoint made durable. Two triggers drive one cancellation mechanism:
//!
//! - **Steal.** When a freed worker steals a busy victim's tail `[mid, end)`,
//!   where `mid` splits the victim's missing remainder in half,
//!   `Work::pick` trims the victim's assignment to `[start, mid)`. Once the
//!   stealer confirms the tail still has missing bytes, `Work::cancel_victim`
//!   signals the victim's `CancelHandle`. A tail the victim has already
//!   delivered is not a reason to cancel: the victim finishes its leg
//!   normally. Otherwise the victim stops fetching past `mid`, re-queues the
//!   still-missing part of its trimmed `[start, mid)` to `pending`, and picks
//!   again, so the stolen tail is fetched (and paid for) by exactly ONE source.
//! - **Stall / fault.** A lane with no verified progress for the lane watchdog
//!   ([`LANE_WATCHDOG`]), or whose `fill_gap` returns an `Err`, re-queues the
//!   UN-fetched remainder of its range to `pending` and ends. The loop
//!   classifies the fault ([`crate::classify`]): a fatal one ends the acquire
//!   with that typed error, anything else is the [`SourceSet`]'s to act on.
//!   Verified bytes already stored are never refetched.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use bao_tree::ChunkRanges;
use decdn_bao_range::{AlignedRange, align_range};
use decdn_protocol::{Coverage, num_blocks};
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use futures_util::stream::FuturesUnordered;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::time::Instant;

use crate::coverage_plan::{SourceCoverage, covered_part, covers_byte_range, spread_segments};
use crate::driver::{
    DriveConfig, DriveCounters, PRESENT_RECORD_FLUSH_INTERVAL, PacingWait, SharedPool, WaitReason,
    contiguous_byte_ranges, fill_gap, ranges_content_len,
};
use crate::fault::Fault;
use crate::health::PeerHealth;
use crate::ledgers::LaneLedgers;
use crate::pacer::DownstreamFrontier;
use crate::segment::{split_evenly, steal_split};
use crate::source::{BlobSource, Funder, IngestStore, SourceFuture, SourceStream};
use crate::source_set::{Holder, SourceProvider, SourceSet};
use crate::stop::StopPolicy;
use crate::streamer::StreamCandidate;
use crate::{Pacer, PoolContext, PoolLedger};

/// How long a lane may go without a verified byte, with bytes of its range
/// still missing, before its range moves to other lanes and its source cools.
/// A leg's first byte gets a 30 s grace before this window applies.
/// Off under consumption pacing, where a lane parked on the consumer's cursor
/// is waiting, not stalled.
pub const LANE_WATCHDOG: Duration = Duration::from_secs(10);

/// How long a leg may wait for its first verified byte before the lane
/// watchdog applies. A cold miss makes the node pull from its own upstream
/// first, so its first byte can take longer than [`LANE_WATCHDOG`]. It equals
/// the per-stream idle window a paid leg allows before its first byte.
const FIRST_BYTE_GRACE: Duration = Duration::from_secs(30);

/// How long [`acquire`] waits to ask for growth again after a lane's
/// [`LaneWiden`] granted nothing while a queued range had no taker. A stream
/// that a sibling fetch gives back wakes nothing in the loop, so the loop
/// looks again on this clock until the range has a taker. It is also the
/// wait after a pass that found no lane to ask for a queued range while a
/// running lane has a [`LaneWiden`]: an own worker that takes its next range
/// wakes nothing, so the loop looks again on this clock. It is also the
/// first wait before a lane is asked again after a node refused its extra
/// stream ([`EXTRA_RETRY_CAP`]), and the wait before a lane that found no
/// free stream tries to start again.
const GROWTH_RETRY: Duration = Duration::from_secs(1);

/// The longest wait before a lane is asked for an extra stream again after
/// its node refused extra streams in a row. The wait starts at
/// [`GROWTH_RETRY`] and doubles with each refusal; a verified byte on an
/// extra stream of the lane ends it.
const EXTRA_RETRY_CAP: Duration = Duration::from_secs(30);

/// The refusals in a row of a lane's extra streams at which the loop logs
/// them at info, once per run of refusals.
const EXTRA_REFUSALS_LOGGED: u32 = 3;

/// How often the loop logs, at info, that queued ranges still wait for a
/// stream. A change between those lines logs at debug.
const WAITING_RELOG: Duration = Duration::from_secs(30);

/// The unit the planner grows a size claim by. A claim is a hint: when every
/// byte below the bound is present and no leg has proved the size, the bound
/// grows by at least one `SEED` ([`grown_bound`]). It is one discovery block,
/// the unit the planner assigns to nodes.
pub(crate) const SEED: u64 = decdn_protocol::DISCOVERY_BLOCK_BYTES;

/// The next bound after every byte below `bound` is present and no size is
/// proven: `max(bound + SEED, c0 + 2 * extra)`, rounded up to a multiple of
/// [`SEED`]. `c0` is the first claim and `extra` the verified bytes past it,
/// so a claim that falls far short of the blob grows geometrically. The sum
/// saturates at `u64::MAX`.
#[must_use]
pub(crate) fn grown_bound(bound: u64, c0: u64, extra: u64) -> u64 {
    let want = bound
        .saturating_add(SEED)
        .max(c0.saturating_add(extra.saturating_mul(2)));
    want.div_ceil(SEED).saturating_mul(SEED)
}

/// The end of the blob as the fetch knows it: the proven size, or with none
/// proven the smaller of the first claim `c0` and the store's bound. A piece
/// that starts at or past it can lie past the true end.
fn known_end<St: IngestStore>(store: &St, c0: u64) -> u64 {
    store
        .proven()
        .unwrap_or_else(|| c0.min(store.total_bytes()))
}

/// Bytes of the store's `present` chunks at or past `from`, against `bound`.
fn bytes_past(present: &ChunkRanges, from: u64, bound: u64) -> u64 {
    contiguous_byte_ranges(present, bound)
        .into_iter()
        .map(|(start, len)| start.saturating_add(len).saturating_sub(start.max(from)))
        .fold(0, u64::saturating_add)
}

/// The chunks of `[start, start+len)` the store misses, clipped to its
/// current bound ([`crate::driver::missing_below_bound`]): a range at or past
/// the bound misses nothing, and a bound that shrinks under the query clips
/// it. A store error is this process's fault ([`crate::LocalPullFault`]),
/// never a source's.
///
/// # Errors
///
/// The store's error, marked [`crate::LocalPullFault`].
async fn store_missing<St>(store: &St, start: u64, len: u64) -> anyhow::Result<ChunkRanges>
where
    St: IngestStore,
{
    crate::driver::missing_below_bound(store, start, len)
        .await
        .map(|(missing, _bound)| missing)
}

/// What one [`acquire`] fills: byte ranges of one blob, in one store.
pub struct AcquireTarget<'a, St> {
    /// The ranged store every lane writes into, keyed by `hash`. Its bound is
    /// the size the planner works to.
    pub store: &'a St,
    /// The blob's BLAKE3 root.
    pub hash: [u8; 32],
    /// The first size claim: a hint. A resumed store's bound wins over it,
    /// and a leg that verifies the final chunk proves the size.
    pub total_bytes: u64,
    /// The `(offset, len)` byte ranges to fill. A range that reaches
    /// `total_bytes` or the store's bound, or has a zero length, is a tail: it
    /// follows the bound as it grows or shrinks. Bytes outside the ranges and
    /// their tail are never fetched.
    pub ranges: &'a [(u64, u64)],
}

impl<St> std::fmt::Debug for AcquireTarget<'_, St> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcquireTarget")
            .field("total_bytes", &self.total_bytes)
            .field("ranges", &self.ranges)
            .finish_non_exhaustive()
    }
}

/// How one [`acquire`] runs: pacing, payment, the lane cap and the stop.
pub struct AcquireEnv<'a, Pc, F> {
    /// Decides each leg's draw against the pool's budget (or, under
    /// consumption pacing, against the consumer's window).
    pub pacer: &'a Pc,
    /// Tops the pool's deposit up when a lane's pacer asks for it.
    pub funder: &'a F,
    /// The driver's payment knobs.
    pub drive: &'a DriveConfig,
    /// The most lanes that stream at once.
    pub max_lanes: usize,
    /// When the acquire gives up for lack of progress. Every verified byte
    /// ticks its clock.
    pub stop: &'a StopPolicy,
    /// Called with `(position, total)` as verified bytes land: one monotonic
    /// whole-blob position across every lane.
    pub on_progress: Option<&'a (dyn Fn(u64, u64) + Send + Sync + 'a)>,
    /// The run registry whose lanes share this acquire's pool deposit, or
    /// `None` to gate on this acquire's own lanes. See [`acquire`].
    pub ledgers: Option<&'a LaneLedgers>,
    /// Bounds every lane to a read-ahead window ahead of a live consumer, or
    /// `None` for the eager fetch. See [`ConsumptionPacing`].
    pub pacing: Option<&'a ConsumptionPacing<'a>>,
    /// The largest blob the caller accepts, in bytes, or `0` for no cap. A
    /// size claim above it is clamped to it before any work is sized, and the
    /// bound never grows past it: a blob that holds bytes past it ends the
    /// acquire with [`crate::BlobTooLarge`].
    pub max_blob_bytes: u64,
}

impl<Pc, F> std::fmt::Debug for AcquireEnv<'_, Pc, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcquireEnv")
            .field("max_lanes", &self.max_lanes)
            .field("stop", &self.stop)
            .field("paced", &self.pacing.is_some())
            .finish_non_exhaustive()
    }
}

/// A resource one source lane holds while it takes part in an acquire, such as
/// the caller's stream permit for the lane's provider. For a lane with a
/// [`LaneWiden`], [`acquire`] releases it when the lane's own worker ends, and
/// the lane starts again in that acquire only on a stream `grow` grants. For
/// any lane, [`acquire`] releases it at the latest when it returns or is
/// dropped, including for a lane that cooled mid-fetch. A lease is released
/// once: a lane that takes part in a later acquire holds nothing.
#[derive(Default)]
pub struct LaneLease(Mutex<Option<Box<dyn Send + Sync>>>);

impl LaneLease {
    /// A lease that holds `held` until the acquire it takes part in releases
    /// it: when the lane's own worker ends, for a lane with a [`LaneWiden`],
    /// and at the latest when the acquire returns.
    pub fn new(held: impl Send + Sync + 'static) -> Self {
        Self(Mutex::new(Some(Box::new(held))))
    }

    /// Whether the lease still holds what it was given.
    pub(crate) fn is_held(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Drop what the lease holds. A later call does nothing.
    pub(crate) fn release(&self) {
        let held = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        drop(held);
    }
}

impl std::fmt::Debug for LaneLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let held = self.0.lock().is_ok_and(|held| held.is_some());
        f.debug_struct("LaneLease").field("held", &held).finish()
    }
}

/// What a [`LaneWiden`] `grow` asks a stream for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrowFor {
    /// The stream a lane starts again on after its own worker ended and gave
    /// its [`LaneLease`] back. It stands in for the lane's first stream.
    Restart,
    /// An extra stream beside the lane's own, for a queued range no idle lane
    /// takes. A caller may refuse it a stream that a sibling fetch's first
    /// stream still needs, such as a provider's last free permit.
    Extra,
}

/// How one lane adds a concurrent stream while an [`acquire`] runs
/// ([`StreamCandidate::widen`]): a `grow` hook that grants streams and a
/// `release` hook that gives one back. `grow` grants a lane's extra streams,
/// and the stream a lane starts again on after its own worker ended and gave
/// its [`LaneLease`] back; its [`GrowFor`] says which.
///
/// The acquire relies on two rules. `grow` never waits: it grants a stream
/// only when it can grant one now, such as a stream permit that is free. And
/// `release` is called exactly once for each stream `grow` granted, when that
/// stream's worker stops for any reason.
pub struct LaneWiden {
    /// Grants one stream of the given [`GrowFor`] now, without waiting, and
    /// returns whether it did.
    grow: Box<dyn Fn(GrowFor) -> bool + Send + Sync>,
    /// Gives back one granted stream's hold.
    release: Box<dyn Fn() + Send + Sync>,
}

impl LaneWiden {
    /// A pair of `grow`, which grants one stream of the given [`GrowFor`]
    /// now (an extra stream, or the stream a lane starts again on) and returns
    /// whether it did, and `release`, which gives one back. See the type's
    /// rules for both.
    pub fn new(
        grow: impl Fn(GrowFor) -> bool + Send + Sync + 'static,
        release: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            grow: Box::new(grow),
            release: Box::new(release),
        }
    }

    /// Ask for one stream of kind `kind`: a restart's stream, or an extra
    /// stream. Returns whether it was granted.
    pub(crate) fn grow(&self, kind: GrowFor) -> bool {
        (self.grow)(kind)
    }

    /// Give back one granted stream's hold.
    pub(crate) fn release(&self) {
        (self.release)();
    }
}

impl std::fmt::Debug for LaneWiden {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LaneWiden")
    }
}

/// Gives back one stream's grant ([`LaneWiden::release`]) when its worker's
/// future ends or is dropped unfinished: an extra worker's stream, or the
/// stream a lane's own worker starts again on.
struct ReleaseGrant<S>(Arc<StreamCandidate<S>>);

impl<S> Drop for ReleaseGrant<S> {
    fn drop(&mut self) {
        if let Some(widen) = &self.0.widen {
            widen.release();
        }
    }
}

/// Releases a lane's [`LaneLease`] when its own worker's future ends or is
/// dropped unfinished, for a lane with a [`LaneWiden`]: such a lane takes a
/// stream again through `grow` to start again. A lane without one keeps its
/// lease until [`acquire`] returns.
struct ReleaseLease<S>(Arc<StreamCandidate<S>>);

impl<S> Drop for ReleaseLease<S> {
    fn drop(&mut self) {
        if self.0.widen.is_some() {
            self.0.lease.release();
        }
    }
}

/// What a worker's stream holds. It moves into the worker's future when the
/// future is built, so it is given back when the worker ends, and also when
/// the future is dropped, polled or not.
#[expect(
    dead_code,
    reason = "an extra worker's grant is never read: it acts only when the worker's future \
              drops it"
)]
enum Hold<S> {
    /// A lane's own worker: the lane's lease, and the grant the lane started
    /// again on, if it started on one.
    Own {
        /// Releases the lane's lease as the worker ends.
        lease: ReleaseLease<S>,
        /// The restart's grant.
        grant: Option<ReleaseGrant<S>>,
    },
    /// An extra worker ([`Work::add_extra`]) and its grant.
    Extra(ReleaseGrant<S>),
}

impl<S> Hold<S> {
    /// Give back the stream of a lane's own worker as it parks with nothing
    /// to take, when the lane has a [`LaneWiden`]: the lane's lease, or the
    /// grant it started again on. A parked worker fetches nothing, so a
    /// sibling fetch can use the stream meanwhile (#2252). Other holds keep
    /// their stream.
    fn park(&mut self) {
        if let Self::Own { lease, grant } = self
            && lease.0.widen.is_some()
        {
            lease.0.lease.release();
            *grant = None;
        }
    }

    /// Whether the worker holds a stream to fetch on: an extra worker's
    /// grant, a lane's lease or restart grant, or a stream a parked own
    /// worker takes back now through its lane's `grow`, as a restart.
    fn stream(&mut self) -> bool {
        match self {
            Self::Extra(_) => true,
            Self::Own { lease, grant } => {
                let lane = &lease.0;
                if lane.widen.is_none() || lane.lease.is_held() || grant.is_some() {
                    return true;
                }
                let granted = lane
                    .widen
                    .as_ref()
                    .is_some_and(|widen| widen.grow(GrowFor::Restart));
                if granted {
                    *grant = Some(ReleaseGrant(Arc::clone(lane)));
                }
                granted
            }
        }
    }
}

/// A range [`Work::pick`] handed to a worker. `victim` is set when the range is
/// a stolen tail: the peer it was taken from and that peer's unit number
/// ([`Work::units`]) at the steal, for [`Work::cancel_victim`]. `uncovered` is
/// set when the range lies wholly outside the worker's coverage, for its node
/// to serve by pull-through.
struct Picked {
    range: AlignedRange,
    victim: Option<(usize, u64)>,
    uncovered: bool,
}

/// Per-lane interrupt: an edge-triggered wakeup ([`Notify`]) plus a `flag`
/// that says the wakeup means "cancel", not a stale permit. The stealer sets
/// `flag` and wakes the victim under the `Work` lock; the victim clears it on
/// its next `Work::pick`, also under the lock, so the two never race.
struct CancelHandle {
    /// `true` once a stealer confirms the tail it took from this lane still
    /// has missing bytes ([`Work::cancel_victim`]); the victim must stop.
    flag: AtomicBool,
    /// Wakes the victim's `cancelled` future so it re-reads `flag` promptly.
    notify: Notify,
}

impl CancelHandle {
    fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }
}

/// Resolve once this handle is genuinely cancelled. A [`Notify`] permit can be
/// stored by a stale `notify_one` from a prior unit, so a wakeup alone is not
/// proof: re-read `flag` and keep waiting until it is set. This also closes the
/// lost-wakeup: `notify_one` stores a permit even if it fires before the await,
/// so a cancel signalled before the victim parks here is still observed.
async fn cancelled(handle: &CancelHandle) {
    loop {
        handle.notify.notified().await;
        if handle.flag.load(Ordering::Acquire) {
            return;
        }
    }
}

/// One planned run staged for seeding into `pending`, with the piece count it
/// will split into. `max_pieces` bounds the split to what is useful for THIS run
/// (the number of lanes that hold it whole), while the seeding loop's global
/// budget (one span per lane) decides how much of that headroom each run
/// actually uses. `pieces` starts at one contiguous span and only grows to reach
/// otherwise-idle lanes.
struct RunSeed {
    offset: u64,
    len: u64,
    max_pieces: usize,
    pieces: usize,
}

/// Bytes of `[start, start+len)` the store still misses. This worker's range is
/// disjoint from every peer's, so this reflects only its own delivery frontier.
///
/// # Errors
///
/// A store error ([`store_missing`]): this process's fault, not the source's.
async fn missing_bytes<St>(store: &St, start: u64, len: u64) -> anyhow::Result<u64>
where
    St: IngestStore,
{
    let ranges = store_missing(store, start, len).await?;
    Ok(contiguous_byte_ranges(&ranges, store.total_bytes())
        .iter()
        .map(|(_, l)| *l)
        .fold(0, u64::saturating_add))
}

/// The chunks of `ranges` the store still misses, each clipped to its bound.
async fn missing_chunks<St>(store: &St, ranges: &[(u64, u64)]) -> anyhow::Result<ChunkRanges>
where
    St: IngestStore,
{
    let mut missing = ChunkRanges::empty();
    for &(start, len) in ranges {
        missing |= store_missing(store, start, len).await?;
    }
    Ok(missing)
}

/// Progress-relative stall watchdog: resolve (trip) once a full `deadline`
/// window passes with `verified` unchanged while the store still misses bytes
/// of `[start, len)`, i.e. no verified progress and not yet done. `verified`
/// is the unit's verified-byte counter, which every bao-verified leaf advances
/// as it lands, before the store's 4 MiB checkpoint makes it durable. The
/// watchdog samples the counter once per window, so a source that verifies at
/// least one byte in every window, however slowly or in however small paced
/// draws, never trips; a range the store holds in full (missing == 0) is left
/// to `fill_gap`'s own completion, never tripped. A zero deadline disables the
/// watchdog.
///
/// Before the unit's first verified byte the window is [`FIRST_BYTE_GRACE`]
/// (or `deadline`, if longer), so a cold miss whose first byte waits on the
/// node's own upstream is not a stall. After it, a gap between verified bytes
/// shorter than the deadline never trips; a source that stops trips it within
/// one to two deadlines.
///
/// It resolves with `None` on a trip. A store error while it checks resolves
/// with that error ([`store_missing`]), which is this process's fault.
async fn watchdog<St>(
    store: &St,
    start: u64,
    len: u64,
    deadline: Duration,
    verified: &AtomicU64,
) -> Option<anyhow::Error>
where
    St: IngestStore,
{
    if deadline.is_zero() {
        return std::future::pending().await;
    }
    let mut prev = verified.load(Ordering::Relaxed);
    let mut window = if prev == 0 {
        deadline.max(FIRST_BYTE_GRACE)
    } else {
        deadline
    };
    loop {
        tokio::time::sleep(window).await;
        window = deadline;
        let now = verified.load(Ordering::Relaxed);
        if now != prev {
            prev = now;
            continue;
        }
        match missing_bytes(store, start, len).await {
            // Delivered in full; `fill_gap` will finish paying and return
            // `Completed`. Keep waiting rather than tripping a done range.
            Ok(0) => {}
            // A whole window with no verified byte and bytes still missing: a
            // stall.
            Ok(_) => return None,
            Err(err) => return Some(err),
        }
    }
}

/// How one unit of work ended, so the worker loop knows what to do next.
enum UnitOutcome {
    /// `fill_gap` filled the range: free the lane and pick again.
    Completed,
    /// A steal claimed this lane's tail: re-queue the trimmed remainder and
    /// stay live (pick again).
    Cancelled,
    /// The lane stalled or `fill_gap` failed: re-queue the remainder and end
    /// the lane. `Some(e)` carries the `fill_gap` error; `None` is a watchdog
    /// trip, which has no error by construction.
    Faulted(Option<anyhow::Error>),
}

/// How one worker's run ended.
enum LaneEnd {
    /// The worker found nothing it could take: for a lane's own worker, with
    /// no peer holding work; for an extra worker, once its one piece ended.
    Idle,
    /// The worker faulted on `range`, in the piece that starts at `piece_at`,
    /// at `at`: `err` is `Some(e)` for a `fill_gap` error, `None` for a
    /// watchdog trip. The loop records the fault at `at`, not at the time it
    /// takes the end, so a sibling's byte verified in between orders after it.
    Faulted {
        err: Option<anyhow::Error>,
        range: crate::source_set::LaneRange,
        piece_at: u64,
        at: Instant,
    },
}

/// What [`run_worker`] hands back to the loop.
struct WorkerEnd {
    /// The lane's payee.
    provider: Address,
    /// How the worker ended.
    end: LaneEnd,
    /// Whether the worker verified any byte.
    delivered: bool,
    /// When the worker last verified a byte of a range outside its coverage:
    /// its node serves such ranges by pull-through.
    pulled_through: Option<Instant>,
    /// Whether it was an extra worker ([`Work::add_extra`]): its end touches
    /// neither the lane's running state nor its source's health.
    extra: bool,
}

/// Shared work-state, guarded by one [`AsyncMutex`]. `pending` holds the
/// ranges no lane owns; `in_flight[i]` is lane `i`'s currently-owned range as
/// `(start, len)` (`None` = idle), which is both the tail-steal remaining-set
/// and the "at most one lane owns any range" ledger. Every per-lane `Vec` is
/// indexed by the lane's stable slot ([`Work::add_lane`]).
struct Work {
    /// Ranges no lane owns (drains as workers pick). A worker takes an entry
    /// its own [`Coverage`] includes first (#1506), and [`Work::pick`] skips
    /// past an entry another running lane covers. An entry no running lane
    /// covers goes to any worker, whose node serves it by pull-through (#2225),
    /// while discovery keeps looking for a node that covers it.
    pending: VecDeque<AlignedRange>,
    /// Per-lane current range, `None` when the lane holds nothing.
    in_flight: Vec<Option<(u64, u64)>>,
    /// Per-lane interrupt handles. [`Work::cancel_victim`] signals
    /// `cancel[victim]` after a steal; the victim clears it on its next `pick`.
    cancel: Vec<Arc<CancelHandle>>,
    /// Per-lane count of units started. Every [`Work::pick`] by worker `i`
    /// bumps `units[i]`, so a number names one unit. A steal only trims its
    /// victim's slot and never bumps it, so a victim trimmed by two stealers is
    /// still on the same unit.
    units: Vec<u64>,
    /// `alive[i]` is `true` while lane `i`'s worker runs. [`Work::park`]
    /// clears it when the worker ends; [`Work::revive`] sets it when the loop
    /// starts the lane again.
    alive: Vec<bool>,
    /// Per-lane block coverage: what a lane prefers to pick, and the only
    /// ranges it steals from ([`Work::pick`]).
    coverage: Vec<Coverage>,
    /// `measured[i]` is `true` when lane `i`'s coverage is a probed bitmap.
    /// A lane without one holds the whole blob, so its coverage follows the
    /// bound as it grows ([`Work::regrow`]).
    measured: Vec<bool>,
    /// `lane_of[i]` is the lane slot worker slot `i` fetches for: `i` itself
    /// for a lane's own worker, the lane's slot for an extra worker
    /// ([`Work::add_extra`]).
    lane_of: Vec<usize>,
    /// `extra[i]` is `true` when slot `i` is an extra worker: one more stream
    /// a lane's [`LaneWiden`] granted for a queued range no idle worker takes.
    extra: Vec<bool>,
    /// `providers[i]` is the payee worker slot `i` fetches from: set by
    /// [`Work::set_provider`] when the loop starts a lane, and copied from
    /// the lane for an extra worker. Steal logs name both sides with it.
    providers: Vec<Option<Address>>,
    /// `no_uncovered[lane]` is `true` while the lane's source is barred from
    /// pull-through ([`SourceSet::no_pull_through`]): its workers take only
    /// what the lane covers. The loop sets it ([`Work::set_no_uncovered`]).
    no_uncovered: Vec<bool>,
    /// Pick the lowest-offset pending segment first, not the oldest. Set for a
    /// consumption-paced fetch ([`ConsumptionPacing`]): the consumer reads in
    /// offset order, so the earliest missing range is always the one it waits
    /// on.
    front_first: bool,
}

impl Work {
    /// A work-state with no lanes and `pending` queued.
    const fn new(pending: VecDeque<AlignedRange>, front_first: bool) -> Self {
        Self {
            pending,
            in_flight: Vec::new(),
            cancel: Vec::new(),
            units: Vec::new(),
            alive: Vec::new(),
            coverage: Vec::new(),
            measured: Vec::new(),
            lane_of: Vec::new(),
            extra: Vec::new(),
            providers: Vec::new(),
            no_uncovered: Vec::new(),
            front_first,
        }
    }

    /// Record `provider` as the payee worker slot `slot` fetches from.
    fn set_provider(&mut self, slot: usize, provider: Address) {
        if let Some(named) = self.providers.get_mut(slot) {
            *named = Some(provider);
        }
    }

    /// Bar lane `lane`'s workers from ranges outside its coverage, or lift
    /// the bar.
    fn set_no_uncovered(&mut self, lane: usize, barred: bool) {
        if let Some(flag) = self.no_uncovered.get_mut(lane) {
            *flag = barred;
        }
    }

    /// Whether worker slot `i`'s lane is barred from ranges outside its
    /// coverage.
    fn barred(&self, i: usize) -> bool {
        self.lane_of
            .get(i)
            .and_then(|&lane| self.no_uncovered.get(lane))
            .copied()
            .unwrap_or(false)
    }

    /// Give a new lane a stable slot, alive and holding nothing. Returns the
    /// slot. `coverage` is the lane's probed bitmap, or `None` for a lane that
    /// holds the whole blob of `total_bytes`.
    fn add_lane(&mut self, coverage: Option<Coverage>, total_bytes: u64) -> usize {
        let slot = self.in_flight.len();
        self.in_flight.push(None);
        self.cancel.push(Arc::new(CancelHandle::new()));
        self.units.push(0);
        self.alive.push(true);
        self.measured.push(coverage.is_some());
        self.coverage
            .push(coverage.unwrap_or_else(|| Coverage::full(num_blocks(total_bytes))));
        self.lane_of.push(slot);
        self.extra.push(false);
        self.providers.push(None);
        self.no_uncovered.push(false);
        slot
    }

    /// Give lane `lane` an extra worker's slot, alive and holding nothing.
    /// Returns the slot. A slot of an extra worker of the lane that has ended
    /// is used again, so a lane holds only as many extra slots as it ran extra
    /// workers at once. Its unit count carries on, so a steal made against an
    /// earlier unit cannot cancel it.
    fn add_extra(&mut self, lane: usize) -> usize {
        let ended = (0..self.in_flight.len()).find(|&i| {
            self.extra.get(i) == Some(&true)
                && self.lane_of.get(i) == Some(&lane)
                && self.alive.get(i) == Some(&false)
        });
        if let Some(slot) = ended {
            let coverage = self
                .coverage
                .get(lane)
                .cloned()
                .unwrap_or_else(Coverage::empty);
            let measured = self.measured.get(lane).copied().unwrap_or(true);
            let provider = self.providers.get(lane).copied().flatten();
            if let Some(held) = self.in_flight.get_mut(slot) {
                *held = None;
            }
            if let Some(handle) = self.cancel.get_mut(slot) {
                *handle = Arc::new(CancelHandle::new());
            }
            if let Some(alive) = self.alive.get_mut(slot) {
                *alive = true;
            }
            if let Some(m) = self.measured.get_mut(slot) {
                *m = measured;
            }
            if let Some(c) = self.coverage.get_mut(slot) {
                *c = coverage;
            }
            if let Some(p) = self.providers.get_mut(slot) {
                *p = provider;
            }
            return slot;
        }
        let slot = self.in_flight.len();
        self.in_flight.push(None);
        self.cancel.push(Arc::new(CancelHandle::new()));
        self.units.push(0);
        self.alive.push(true);
        self.measured
            .push(self.measured.get(lane).copied().unwrap_or(true));
        self.coverage.push(
            self.coverage
                .get(lane)
                .cloned()
                .unwrap_or_else(Coverage::empty),
        );
        self.lane_of.push(lane);
        self.extra.push(true);
        self.providers
            .push(self.providers.get(lane).copied().flatten());
        // An extra worker reads its lane's bar through `lane_of`.
        self.no_uncovered.push(false);
        slot
    }

    /// End extra worker slot `i`: it is gone for good, and
    /// [`Work::add_extra`] may use the slot again.
    fn end_extra(&mut self, i: usize) {
        self.park(i);
    }

    /// Who can grow for each pending range no idle worker will take: per such
    /// range, the range and the running lanes that may be asked for an extra
    /// worker for it, in lane order (#2231).
    ///
    /// Ranges are matched to idle workers one to one, in queue order: an idle
    /// worker is an alive slot that holds no range, and it takes a range it
    /// covers, or one that holds a chunk no running lane covers (#2225) unless
    /// its lane is barred from those ([`Work::barred`]), on its next pick. A
    /// lane may be asked for a range only while its own worker runs a range of
    /// its own and `can_grow` allows it; its `LaneWiden` bounds how many extra
    /// workers it runs. It must
    /// also cover part of that range, or, for a range that holds a chunk no
    /// running lane covers, not be barred from pull-through: the same rule as
    /// for an idle worker, so a remainder only the faulted lane covered still
    /// finds a taker (#2230).
    fn growth_wanted(
        &self,
        total_bytes: u64,
        can_grow: impl Fn(usize) -> bool,
    ) -> Vec<(AlignedRange, Vec<usize>)> {
        let covers = |slot: usize, seg: &AlignedRange| {
            self.coverage
                .get(slot)
                .is_some_and(|c| !covered_part(c, seg.chunk_ranges(), total_bytes).is_empty())
        };
        let mut idle: Vec<usize> = (0..self.in_flight.len())
            .filter(|&i| {
                self.alive.get(i) == Some(&true)
                    && self.in_flight.get(i).is_some_and(Option::is_none)
            })
            .collect();
        let mut wanted = Vec::new();
        for seg in &self.pending {
            let orphan = !self.uncovered_part(seg, total_bytes).is_empty();
            if let Some(pos) = idle
                .iter()
                .position(|&i| (orphan && !self.barred(i)) || covers(i, seg))
            {
                idle.swap_remove(pos);
                continue;
            }
            let candidates = (0..self.in_flight.len())
                .filter(|&lane| {
                    self.extra.get(lane) == Some(&false)
                        && self.alive.get(lane) == Some(&true)
                        && self.in_flight.get(lane).is_some_and(Option::is_some)
                        && can_grow(lane)
                        && ((orphan && !self.barred(lane)) || covers(lane, seg))
                })
                .collect();
            wanted.push((seg.clone(), candidates));
        }
        wanted
    }

    /// Plan `missing` afresh under a grown bound of `total_bytes`: every lane
    /// without a probed bitmap now covers the grown region, and `pending` is
    /// replanned across the lanes. Only called while no range is in flight.
    ///
    /// # Errors
    ///
    /// A segmentation alignment error.
    fn regrow(&mut self, missing: &ChunkRanges, total_bytes: u64) -> anyhow::Result<()> {
        self.grow_coverage(total_bytes);
        self.pending = plan_pending(missing, total_bytes, &self.coverage)?;
        Ok(())
    }

    /// Size every lane without a probed bitmap to the whole blob of
    /// `total_bytes`.
    fn grow_coverage(&mut self, total_bytes: u64) {
        for (coverage, &measured) in self.coverage.iter_mut().zip(&self.measured) {
            if !measured {
                *coverage = Coverage::full(num_blocks(total_bytes));
            }
        }
    }

    /// Clip every range to a proven size of `total_bytes`: drop the pending
    /// ranges at or past it and cut the rest there. A lane whose range starts
    /// at or past it is cancelled, so it stops fetching a range the blob does
    /// not have; a lane whose range crosses it is trimmed, and its own drive
    /// ends at the proven size.
    ///
    /// # Errors
    ///
    /// A segmentation alignment error.
    fn clip(&mut self, total_bytes: u64) -> anyhow::Result<()> {
        let mut kept = VecDeque::with_capacity(self.pending.len());
        for seg in self.pending.drain(..) {
            let start = seg.fetch_start();
            if start >= total_bytes {
                continue;
            }
            if seg.fetch_end() > total_bytes {
                kept.push_back(align_range(start, total_bytes - start, total_bytes)?);
            } else {
                kept.push_back(seg);
            }
        }
        self.pending = kept;
        for i in 0..self.in_flight.len() {
            let Some(Some((start, len))) = self.in_flight.get(i).copied() else {
                continue;
            };
            if start >= total_bytes {
                if let Some(unit) = self.units.get(i).copied() {
                    self.cancel_victim(i, unit);
                }
            } else if start.saturating_add(len) > total_bytes
                && let Some(Some(slot)) = self.in_flight.get_mut(i)
            {
                slot.1 = total_bytes - start;
            }
        }
        Ok(())
    }

    /// Mark lane `i`'s worker ended. Every `pending` entry stays: a cooling
    /// lane may return, and a rediscovered holder may cover what no live lane
    /// does.
    fn park(&mut self, i: usize) {
        if let Some(alive) = self.alive.get_mut(i) {
            *alive = false;
        }
    }

    /// Mark lane `i`'s worker running again.
    fn revive(&mut self, i: usize) {
        if let Some(alive) = self.alive.get_mut(i) {
            *alive = true;
        }
    }

    /// Whether some lane holds a range in flight. A worker with nothing to
    /// pick parks while this holds, since a peer's range may yet be re-queued
    /// or become splittable.
    fn busy(&self) -> bool {
        self.in_flight.iter().any(Option::is_some)
    }

    /// Whether a lane over `coverage` has anything to take: a pending entry it
    /// covers at least in part, a pending chunk no running lane covers (any
    /// lane may take that one, #2225, when `take_uncovered` allows it), or a
    /// range in flight it could steal from.
    fn has_work_for(&self, coverage: &Coverage, total_bytes: u64, take_uncovered: bool) -> bool {
        self.pending
            .iter()
            .any(|seg| !covered_part(coverage, seg.chunk_ranges(), total_bytes).is_empty())
            || (take_uncovered && self.uncovered(total_bytes))
            || self
                .in_flight
                .iter()
                .flatten()
                .any(|&(s, l)| covers_byte_range(coverage, s, l, total_bytes))
    }

    /// The chunks of `seg` no running lane covers.
    fn uncovered_part(&self, seg: &AlignedRange, total_bytes: u64) -> ChunkRanges {
        let mut held = ChunkRanges::empty();
        for (_, coverage) in self
            .alive
            .iter()
            .zip(&self.coverage)
            .filter(|&(&alive, _)| alive)
        {
            held |= covered_part(coverage, seg.chunk_ranges(), total_bytes);
        }
        seg.chunk_ranges().clone() - held
    }

    /// Whether a pending entry holds a chunk no running lane covers. Such a
    /// chunk goes to any lane, which serves it by pull-through, and it keeps
    /// discovery looking for a node that covers it.
    fn uncovered(&self, total_bytes: u64) -> bool {
        self.pending
            .iter()
            .any(|seg| !self.uncovered_part(seg, total_bytes).is_empty())
    }

    /// Plan the first batch of lanes: give every lane a slot, and replace
    /// `pending` with `missing` spread across the lanes' `coverages` (`None`
    /// for a lane that holds the whole blob). Returns the lanes' slots, in
    /// order.
    ///
    /// # Errors
    ///
    /// A segmentation alignment error.
    fn seed(
        &mut self,
        missing: &ChunkRanges,
        total_bytes: u64,
        coverages: &[Option<Coverage>],
    ) -> anyhow::Result<Vec<usize>> {
        let slots: Vec<usize> = coverages
            .iter()
            .map(|coverage| self.add_lane(coverage.clone(), total_bytes))
            .collect();
        let planned: Vec<Coverage> = slots
            .iter()
            .filter_map(|&slot| self.coverage.get(slot).cloned())
            .collect();
        self.pending = plan_pending(missing, total_bytes, &planned)?;
        Ok(slots)
    }

    /// Worker `i`'s in-flight slot. An out-of-range `i` is a wiring bug, not a
    /// condition to absorb: silently no-op'ing it would let the worker fetch
    /// (and pay for) a range `in_flight` never records, which a peer then reads
    /// as unowned and steals, so both pay for it.
    fn slot_mut(&mut self, i: usize) -> anyhow::Result<&mut Option<(u64, u64)>> {
        match self.in_flight.get_mut(i) {
            Some(slot) => Ok(slot),
            None => anyhow::bail!("worker index {i} out of range for in-flight slots"),
        }
    }

    /// Under the caller's lock, choose worker `i`'s next range. Pop the FIRST
    /// pending segment `coverage` includes (a worker prefers what it covers,
    /// #1506), else the first covered run of a pending entry, else the first
    /// run of pending chunks no running lane covers (#2225), unless the
    /// worker's lane is barred from those ([`Work::barred`]); when none remain,
    /// steal the aligned second half of the missing remainder of the COVERABLE
    /// range in flight that misses the most ([`steal_split`]), trimming the
    /// victim to end at that split so no other freed worker can re-steal the
    /// same tail. `missing` is the store's missing byte runs, read just before
    /// the pick; it only overstates what is missing, since bytes that land
    /// after the read are never taken away. Records the choice in
    /// `in_flight[i]`. A steal
    /// does NOT cancel the victim here: the caller does that with
    /// [`Work::cancel_victim`] once it knows the tail still has missing bytes.
    /// `Ok(None)` means there is nothing this worker can start right now: it
    /// parks while [`Work::busy`] holds, and ends otherwise.
    ///
    /// With `steal` off, the pick stops before the steal: an extra worker
    /// takes one queued piece and never steals, so it adds at most one stream
    /// beside its lane's own.
    ///
    /// # Errors
    ///
    /// An out-of-range worker index; or the alignment error [`steal_split`]
    /// raises on an out-of-bounds range (never on the ranges this scheduler
    /// feeds it).
    fn pick(
        &mut self,
        i: usize,
        total_bytes: u64,
        coverage: &Coverage,
        steal: bool,
        missing: &[(u64, u64)],
    ) -> anyhow::Result<Option<Picked>> {
        // This worker is starting a fresh unit: clear any cancel signal left from
        // a prior unit, under the lock, so a stale `notify_one` permit cannot
        // spuriously cancel the new unit (see `cancelled`).
        match self.cancel.get(i) {
            Some(handle) => handle.flag.store(false, Ordering::Release),
            None => anyhow::bail!("worker index {i} out of range for cancel handles"),
        }
        match self.units.get_mut(i) {
            Some(unit) => *unit = unit.wrapping_add(1),
            None => anyhow::bail!("worker index {i} out of range for unit counters"),
        }
        let covered = |seg: &AlignedRange| {
            covers_byte_range(coverage, seg.fetch_start(), seg.fetch_len(), total_bytes)
        };
        let coverable = if self.front_first {
            self.pending
                .iter()
                .enumerate()
                .filter(|(_, seg)| covered(seg))
                .min_by_key(|(_, seg)| seg.fetch_start())
                .map(|(pos, _)| pos)
        } else {
            self.pending.iter().position(covered)
        };
        if let Some(pos) = coverable {
            // `pos` came from this same deque's `position`, so it is always
            // in range; `VecDeque::remove` returns `Option`, never panics.
            if let Some(seg) = self.pending.remove(pos) {
                *self.slot_mut(i)? = Some((seg.fetch_start(), seg.fetch_len()));
                return Ok(Some(Picked {
                    range: seg,
                    victim: None,
                    uncovered: false,
                }));
            }
        }
        // An entry this worker covers only in part (queued before any lane's
        // coverage split it): take its first covered run and leave the rest
        // queued in its place.
        // Coverage is a preference (#2225): a pending chunk no running lane
        // covers goes to this worker, whose node serves it by pull-through,
        // rather than waiting for a covering node to join. No pending entry
        // holds a chunk this worker covers by then, so such a part lies wholly
        // outside its coverage. A lane barred from pull-through takes none.
        let taken = match self.take_part(total_bytes, |_, seg| {
            covered_part(coverage, seg.chunk_ranges(), total_bytes)
        })? {
            Some(part) => Some((part, false)),
            None if self.barred(i) => None,
            None => self
                .take_part(total_bytes, |w, seg| w.uncovered_part(seg, total_bytes))?
                .map(|part| (part, true)),
        };
        if let Some((picked, uncovered)) = taken {
            *self.slot_mut(i)? = Some((picked.fetch_start(), picked.fetch_len()));
            return Ok(Some(Picked {
                range: picked,
                victim: None,
                uncovered,
            }));
        }
        if !steal {
            *self.slot_mut(i)? = None;
            return Ok(None);
        }

        // Nothing pending this worker can serve: every remaining byte is
        // either in flight on a busy worker or outside this worker's own
        // coverage. Steal the aligned second half of the missing remainder of
        // the COVERABLE such range that misses the most. `in_flight[i]` is
        // `None` here (cleared before this pick), so this worker is excluded
        // from the remaining set and never steals from itself.
        let (owners, remaining): (Vec<usize>, Vec<(u64, u64)>) = self
            .in_flight
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| slot.map(|r| (idx, r)))
            .unzip();
        // `steal_split` returns WHICH remaining range it split, so the trim below
        // lands on that exact victim: no second argmax to agree with. The
        // predicate excludes any range this worker's `coverage` does not fully
        // include, so a narrow-coverage worker that finds nothing it can serve
        // gets `None` here and parks rather than stealing a range it cannot
        // deliver.
        // The split halves the victim's missing bytes, and declines a steal
        // that would leave the victim none: that steal takes the victim's
        // whole remaining work, and the victim, with nothing left, would steal
        // it straight back.
        let Some((v, half)) = steal_split(&remaining, missing, total_bytes, |s, l| {
            covers_byte_range(coverage, s, l, total_bytes)
        })?
        else {
            *self.slot_mut(i)? = None;
            return Ok(None);
        };

        // Trim the victim to end at the split point, so a later freed worker sees
        // the shortened tail and cannot re-steal the half this worker just took:
        // at most one lane owns any range, by construction, in the work-state.
        // The victim's `requeue_missing` re-queues the missing bytes of this
        // same trimmed range, so the steal and the requeue split the remainder
        // at one point.
        //
        // Every branch that cannot complete that trim DECLINES the steal instead
        // of proceeding. Handing out `half` with the victim untrimmed would leave
        // two workers owning overlapping ranges, and both would pay for the
        // overlap: the exact double-pay the trim exists to prevent.
        let trimmed = owners.get(v).copied().and_then(|victim| {
            if victim == i {
                return None;
            }
            let (start, len) = self.in_flight.get_mut(victim)?.as_mut()?;
            if half.fetch_start() <= *start {
                return None;
            }
            *len = half.fetch_start() - *start;
            Some(victim)
        });
        let Some((victim, unit)) = trimmed.and_then(|v| Some((v, *self.units.get(v)?))) else {
            *self.slot_mut(i)? = None;
            return Ok(None);
        };

        *self.slot_mut(i)? = Some((half.fetch_start(), half.fetch_len()));
        Ok(Some(Picked {
            range: half,
            victim: Some((victim, unit)),
            uncovered: false,
        }))
    }

    /// Take the first run of `part_of` of the first pending entry where it is
    /// not empty (the lowest-offset one under `front_first`), and queue the
    /// entry's other pieces where it stood.
    ///
    /// # Errors
    ///
    /// An alignment error on a piece (never on the ranges this scheduler
    /// queues).
    fn take_part(
        &mut self,
        total_bytes: u64,
        part_of: impl Fn(&Self, &AlignedRange) -> ChunkRanges,
    ) -> anyhow::Result<Option<AlignedRange>> {
        let mut parts = self.pending.iter().enumerate().filter_map(|(pos, seg)| {
            let part = part_of(self, seg);
            let &(start, len) = contiguous_byte_ranges(&part, total_bytes).first()?;
            Some((pos, start, len))
        });
        let found = if self.front_first {
            parts.min_by_key(|&(_, start, _)| start)
        } else {
            parts.next()
        };
        let Some((pos, start, len)) = found else {
            return Ok(None);
        };
        let take = align_range(start, len, total_bytes)?;
        let Some(seg) = self.pending.remove(pos) else {
            return Ok(None);
        };
        let rest = seg.chunk_ranges().clone() - take.chunk_ranges();
        for (k, (s, l)) in contiguous_byte_ranges(&rest, total_bytes)
            .into_iter()
            .enumerate()
        {
            self.pending
                .insert(pos.saturating_add(k), align_range(s, l, total_bytes)?);
        }
        Ok(Some(take))
    }

    /// Signal a steal's victim to STOP fetching past the split. Its
    /// already-running `fill_gap` would otherwise fetch (and pay for) the tail
    /// the stealer took. The signal fires only while the victim still runs
    /// `unit`, the unit the steal trimmed. A victim that has since finished,
    /// faulted, or re-queued holds no slot or runs a later unit, and cancelling
    /// that unit would be wrong. A second steal from the same victim trims its
    /// slot again but leaves `unit` unchanged, so this cancel still fires. Set
    /// the flag then wake it, both under the caller's `Work` lock, serialized
    /// against the victim's own `pick` reset. `notify_one` stores a permit if
    /// the victim is not parked yet, so the signal is never lost.
    fn cancel_victim(&self, victim: usize, unit: u64) {
        let same_unit = self.units.get(victim) == Some(&unit)
            && self.in_flight.get(victim).is_some_and(Option::is_some);
        if !same_unit {
            tracing::debug!(victim, unit, "steal victim moved on; cancel skipped");
            return;
        }
        if let Some(handle) = self.cancel.get(victim) {
            handle.flag.store(true, Ordering::Release);
            handle.notify.notify_one();
        }
    }

    /// Whether a pending segment worker `i` can serve starts before the range
    /// `i` holds now. A consumption-paced worker parked on its window checks
    /// this to hand its range back and take the earlier one (see
    /// [`yield_to_front`]).
    fn earlier_pending(&self, i: usize, coverage: &Coverage, total_bytes: u64) -> bool {
        let Some(Some((start, _))) = self.in_flight.get(i) else {
            return false;
        };
        self.pending.iter().any(|seg| {
            seg.fetch_start() < *start
                && covers_byte_range(coverage, seg.fetch_start(), seg.fetch_len(), total_bytes)
        })
    }

    /// Release worker `i`'s range once its `fill_gap` returns, so a peer's steal
    /// computation stops counting the finished range and this lane can be
    /// re-picked for more work.
    ///
    /// # Errors
    ///
    /// An out-of-range worker index (see [`Work::slot_mut`]).
    fn clear(&mut self, i: usize) -> anyhow::Result<()> {
        *self.slot_mut(i)? = None;
        Ok(())
    }
}

/// `coverage`'s covered discovery blocks as inclusive runs, `0-22,42-66`, for
/// a log line; `none` when it covers no block.
fn block_runs(coverage: &Coverage) -> String {
    let mut runs: Vec<(u32, u32)> = Vec::new();
    for block in coverage.covered_blocks() {
        match runs.last_mut() {
            Some((_, end)) if end.checked_add(1) == Some(block) => *end = block,
            _ => runs.push((block, block)),
        }
    }
    if runs.is_empty() {
        return "none".to_owned();
    }
    runs.iter()
        .map(|&(start, end)| {
            if start == end {
                start.to_string()
            } else {
                format!("{start}-{end}")
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Byte ranges as `offset+len` runs, `0+1543503872,2818572288+872415232`, for
/// a log line.
fn byte_runs(ranges: impl Iterator<Item = (u64, u64)>) -> String {
    ranges
        .map(|(start, len)| format!("{start}+{len}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Spread `missing` across lanes of `coverage` (client planner, #1506): each
/// discovery block goes to one covering lane, rarest-cover-first, in lane order
/// as rank. The runs are then split further ONLY to reach otherwise-idle lanes:
/// the eager fan-out is a GLOBAL budget of at most one contiguous span per lane,
/// not a per-run multiplier. A run is split at most as many ways as lanes hold it
/// WHOLE, so every piece stays inside its holders' coverage; `split_evenly` keeps
/// each piece chunk-group aligned, below `steal_split`'s `MIN_SPLIT_SIZE` if need
/// be, so a small blob still engages every full holder from the start. A block
/// no lane covers is queued whole; any lane takes it and serves it by
/// pull-through (#2225), while discovery keeps looking for a node that covers
/// it.
///
/// # Errors
///
/// A segmentation alignment error.
fn plan_pending(
    missing: &ChunkRanges,
    total_bytes: u64,
    coverage: &[Coverage],
) -> anyhow::Result<VecDeque<AlignedRange>> {
    let sources: Vec<SourceCoverage> = coverage
        .iter()
        .enumerate()
        .map(|(source_ix, coverage)| SourceCoverage {
            source_ix,
            coverage: coverage.clone(),
        })
        .collect();
    let rank: Vec<usize> = (0..coverage.len()).collect();
    let (runs, uncovered) = spread_segments(missing, total_bytes, &sources, &rank);
    // A run spans its blocks' missing extent, which holds the holes between
    // separate requested ranges: keep only its missing pieces.
    let mut pieces = Vec::with_capacity(runs.len());
    for run in &runs {
        let span = align_range(run.offset, run.len, total_bytes)?;
        let run_missing = span.chunk_ranges() & missing;
        pieces.extend(contiguous_byte_ranges(&run_missing, total_bytes));
    }
    let mut seeds: Vec<RunSeed> = pieces
        .iter()
        .map(|&(offset, len)| {
            let holders = coverage
                .iter()
                .filter(|c| covers_byte_range(c, offset, len, total_bytes))
                .count()
                .max(1);
            RunSeed {
                offset,
                len,
                max_pieces: holders,
                pieces: 1,
            }
        })
        .collect();
    let mut spare = coverage.len().saturating_sub(seeds.len());
    while spare > 0 {
        // The still-splittable run whose next split yields the largest piece.
        let Some(seed) = seeds
            .iter_mut()
            .filter(|s| s.pieces < s.max_pieces)
            .max_by_key(|s| s.len / u64::try_from(s.pieces + 1).unwrap_or(u64::MAX))
        else {
            break;
        };
        seed.pieces += 1;
        spare -= 1;
    }
    let mut pending: VecDeque<AlignedRange> = VecDeque::with_capacity(seeds.len());
    for seed in &seeds {
        for seg in split_evenly(seed.offset, seed.len, seed.pieces, total_bytes)? {
            pending.push_back(seg);
        }
    }
    for (start, len) in contiguous_byte_ranges(&uncovered, total_bytes) {
        pending.push_back(align_range(start, len, total_bytes)?);
    }
    Ok(pending)
}

/// Re-queue worker `i`'s still-missing tail so another worker (or, after a
/// steal, this one) covers it. Takes the assignment out of `in_flight` FIRST,
/// under the lock, so no peer can steal it while the remainder is computed
/// off-lock; then pushes only the bytes `missing_ranges` still reports
/// missing. Verified bytes already stored are excluded, so nothing is
/// refetched. Returns what it put back, or `None` when worker `i` held
/// nothing.
async fn requeue_missing<St>(
    store: &St,
    work: &AsyncMutex<Work>,
    i: usize,
) -> anyhow::Result<Option<Requeued>>
where
    St: IngestStore,
{
    let assigned = {
        let mut w = work.lock().await;
        w.in_flight.get_mut(i).and_then(Option::take)
    };
    let Some(held) = assigned else {
        return Ok(None);
    };
    let total_bytes = store.total_bytes();
    let missing = store_missing(store, held.0, held.1).await?;
    let remainder = contiguous_byte_ranges(&missing, total_bytes);
    let bytes = remainder
        .iter()
        .map(|&(_, l)| l)
        .fold(0, u64::saturating_add);
    if !remainder.is_empty() {
        let mut w = work.lock().await;
        for (s, l) in remainder {
            w.pending.push_back(align_range(s, l, total_bytes)?);
        }
    }
    Ok(Some(Requeued { held, bytes }))
}

/// What [`requeue_missing`] put back.
struct Requeued {
    /// The `(start, len)` range the worker held, trimmed by any steal.
    held: (u64, u64),
    /// The missing bytes of `held` re-queued to `pending`.
    bytes: u64,
}

/// A worker's [`PacingWait`] under consumption pacing: the shared consumer wait,
/// plus a flag that says this worker is parked on it. [`yield_to_front`] reads
/// the flag, because a parked worker holds no open leg and can drop its range
/// without losing a paid byte.
struct ParkedWait<'a> {
    /// The consumer wait every lane shares.
    inner: &'a dyn PacingWait,
    /// `true` while this worker is parked in `inner`.
    parked: &'a AtomicBool,
    /// Woken when this worker parks.
    parked_wake: &'a Notify,
}

/// Clears a worker's parked flag when its wait ends or is dropped.
struct Unpark<'a>(&'a AtomicBool);

impl Drop for Unpark<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl PacingWait for ParkedWait<'_> {
    fn wait(
        &self,
        observed: DownstreamFrontier,
        reason: WaitReason,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.parked.store(true, Ordering::Release);
            let _unpark = Unpark(self.parked);
            self.parked_wake.notify_waiters();
            self.inner.wait(observed, reason).await;
        })
    }
}

/// Resolve once worker `i` is parked on the consumer AND a range it can serve
/// waits in `pending` ahead of its own. That happens when a lane nearer the
/// consumer faults and its remainder is re-queued: the consumer cannot read
/// past that gap, so this worker's own wait never ends, and unless it gives its
/// range back to take the earlier one, the fetch hangs.
async fn yield_to_front(
    work: &AsyncMutex<Work>,
    i: usize,
    coverage: &Coverage,
    total_bytes: u64,
    parked: &AtomicBool,
    parked_wake: &Notify,
    progress_wake: &Notify,
) {
    loop {
        // Register for both wakeups BEFORE the check, so a park or a re-queue
        // between the check and the await is not lost.
        let on_park = parked_wake.notified();
        let on_work = progress_wake.notified();
        tokio::pin!(on_park);
        tokio::pin!(on_work);
        on_park.as_mut().enable();
        on_work.as_mut().enable();
        if parked.load(Ordering::Acquire)
            && work.lock().await.earlier_pending(i, coverage, total_bytes)
        {
            return;
        }
        tokio::select! {
            () = on_park => {}
            () = on_work => {}
        }
    }
}

/// Everything a worker shares with the loop and its peers, borrowed from
/// [`acquire`]'s frame.
struct Engine<'a, St, Pc, F> {
    /// The store every lane writes. Its bound is the planner's size, read
    /// fresh at each use: it grows while no size is proven and shrinks to a
    /// proven size.
    store: &'a St,
    hash: [u8; 32],
    pacer: &'a Pc,
    funder: &'a F,
    drive: &'a DriveConfig,
    work: &'a AsyncMutex<Work>,
    /// Wakes workers parked because nothing was pickable, whenever a peer
    /// frees, re-queues, or ends.
    progress_wake: &'a Notify,
    /// ONE monotonic whole-blob delivered-byte counter behind the progress
    /// bar, which every lane folds its leg deltas into.
    progress_agg: &'a AtomicU64,
    /// The caller's progress callback, wrapped to tick the stop clock.
    on_progress: &'a (dyn Fn(u64, u64) + Send + Sync + 'a),
    pool: &'a SharedPool<'a>,
    /// The lane watchdog's window; zero turns it off.
    watchdog: Duration,
    pacing: Option<&'a ConsumptionPacing<'a>>,
}

/// One lane's worker: loop picking a range and driving `fill_gap` over it,
/// under a cancel/stall [`tokio::select!`], until the lane finds nothing to
/// take or faults. Exactly one outstanding range at a time (the loop drives one
/// `fill_gap` to a terminal outcome before the next pick): the
/// one-unit-per-source lane invariant, structurally.
///
/// A worker that cannot pick PARKS on `progress_wake` while a peer holds work,
/// because a peer's range may yet be re-queued or become splittable. It ends
/// [`LaneEnd::Idle`] once no lane holds work, and [`LaneEnd::Faulted`] on a
/// fault, after re-queueing its remainder. Either way it parks its slot
/// ([`Work::park`]) before it returns.
///
/// An `extra` worker (#2231) is one more stream of a busy lane, granted for a
/// faulted lane's remainder: it takes one queued piece, never steals, and ends
/// when that piece ends, when it finds none, or when its lane's own worker
/// has stopped.
///
/// # Errors
///
/// A store I/O failure or an out-of-range slot: faults of this process, not of
/// the source.
#[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
// One pick -> drive -> outcome loop. The three unit outcomes each justify a
// money-relevant decision against the loop state they act on, and an extra
// worker's one-piece exits sit beside them; splitting them out would separate
// the decisions from that state.
async fn run_worker<St, S, Pc, F>(
    engine: &Engine<'_, St, Pc, F>,
    i: usize,
    lane: Arc<StreamCandidate<S>>,
    provider: Address,
    health: &PeerHealth,
    hold: &mut Hold<S>,
) -> anyhow::Result<WorkerEnd>
where
    St: IngestStore,
    S: BlobSource,
    Pc: Pacer,
    F: Funder,
{
    let extra = matches!(hold, Hold::Extra(_));
    let Engine {
        store,
        hash,
        work,
        progress_wake,
        ..
    } = *engine;
    // This lane's coverage and cancel handle, cloned once so `cancelled` can
    // await the handle OUTSIDE the `Work` lock while a peer's
    // `Work::cancel_victim` signals it under the lock.
    let (my_coverage, handle) = {
        let w = work.lock().await;
        match (w.coverage.get(i).cloned(), w.cancel.get(i).map(Arc::clone)) {
            (Some(c), Some(h)) => (c, h),
            _ => anyhow::bail!("worker index {i} out of range for lane slots"),
        }
    };
    let mut delivered = false;
    let mut pulled_through = None;
    // Per-worker resume/quote state. The reactive-top-up budget is NOT in here:
    // it is a property of the one shared pool and lives in `pool`.
    let mut counters = DriveCounters::new();
    // Wake every parked peer: this worker changed the work state.
    let wake = || progress_wake.notify_waiters();
    // Under consumption pacing, this worker's wait records when it is parked, so
    // `yield_to_front` can move it to an earlier re-queued range.
    let lane_parked = AtomicBool::new(false);
    let lane_parked_wake = Notify::new();
    let lane_wait = engine.pacing.map(|p| ParkedWait {
        inner: p.pacing_wait,
        parked: &lane_parked,
        parked_wake: &lane_parked_wake,
    });
    loop {
        // Register for the peer-progress wakeup BEFORE reading the work state, so
        // a peer that changes it between this read and the park below cannot slip
        // between the two and leave this worker asleep on work it could take.
        let parked = progress_wake.notified();
        tokio::pin!(parked);
        parked.as_mut().enable();

        // Tiny critical section: pick a range, then DROP the guard before the
        // `fill_gap` await (the guard does not cross the await point).
        let total_bytes = store.total_bytes();
        let idle = || WorkerEnd {
            provider,
            end: LaneEnd::Idle,
            delivered,
            pulled_through,
            extra,
        };
        // The store's missing runs, which a steal splits by. Read off the
        // lock and before the pick, so it only overstates what is missing.
        let missing = if extra {
            Vec::new()
        } else {
            contiguous_byte_ranges(&store_missing(store, 0, total_bytes).await?, total_bytes)
        };
        let picked = {
            let mut w = work.lock().await;
            // An extra worker whose lane's own worker stopped takes nothing.
            let lane_stopped = extra
                && w.lane_of
                    .get(i)
                    .and_then(|&l| w.alive.get(l))
                    .is_none_or(|alive| !alive);
            if lane_stopped {
                None
            } else if !hold.stream() {
                // A parked own worker gave its stream back and finds none
                // free now. It ends when no lane holds work; otherwise it
                // tries again on a peer's progress or after `GROWTH_RETRY`.
                if !w.busy() {
                    w.park(i);
                    drop(w);
                    wake();
                    return Ok(idle());
                }
                drop(w);
                tokio::select! {
                    () = parked.as_mut() => {}
                    () = tokio::time::sleep(GROWTH_RETRY) => {}
                }
                continue;
            } else {
                let picked = w.pick(i, total_bytes, &my_coverage, !extra, &missing)?;
                if picked.is_none() {
                    // Nothing to take: the stream goes back while the worker
                    // parks or ends.
                    hold.park();
                }
                picked
            }
        };
        let Some(Picked {
            range,
            victim,
            uncovered,
        }) = picked
        else {
            // Nothing to start right now. An extra worker ends here. A lane's
            // own worker ends only when no lane holds work; otherwise it
            // parks: a peer's range is still draining toward a requeue or a
            // splittable size.
            {
                let mut w = work.lock().await;
                if extra {
                    w.end_extra(i);
                    drop(w);
                    wake();
                    return Ok(idle());
                }
                if !w.busy() {
                    w.park(i);
                    drop(w);
                    wake();
                    return Ok(idle());
                }
            }
            parked.await;
            continue;
        };
        let (r_start, r_len) = (range.fetch_start(), range.fetch_len());

        // Present-bytes backstop (scheduling-independent invariant: "never fetch
        // or pay for bytes already present"). Between a peer's `fill_gap`
        // returning `Ok` and its `clear(i)`, that peer's `in_flight` still
        // advertises its just-COMPLETED, already-PAID range as steal-eligible; on
        // a multi-thread runtime this worker can `pick`/steal it in that window.
        // Re-deriving the still-missing sub-ranges OFF-lock and driving ONLY
        // those closes that window structurally: a stolen already-present range
        // yields an empty set and is skipped, so `fill_gap` (which resumes from
        // its paid frontier and would re-pull the whole span) never re-pays for a
        // present byte. Interior holes never arise (a picked range is contiguous
        // and delivered front-to-back), so this is normally one suffix gap or
        // (for a stolen completed range) none.
        let gaps = contiguous_byte_ranges(
            &store_missing(store, r_start, r_len).await?,
            store.total_bytes(),
        );
        if gaps.is_empty() {
            let mut w = work.lock().await;
            w.clear(i)?;
            if extra {
                w.end_extra(i);
                drop(w);
                wake();
                return Ok(idle());
            }
            drop(w);
            wake();
            continue;
        }
        // A stolen tail with missing bytes: stop the victim at the split so the
        // tail is fetched and paid for once. A fully present tail skips this,
        // so a victim that has already delivered its range reaches its own
        // `finish` instead of being dropped just before it.
        if let Some((v, unit)) = victim {
            let w = work.lock().await;
            w.cancel_victim(v, unit);
            tracing::debug!(
                stealer = %provider,
                victim = ?w.providers.get(v).copied().flatten(),
                split = r_start,
                victim_range = ?w.in_flight.get(v).copied().flatten(),
                "stole the second half of a lane's missing remainder"
            );
        }

        // Drive each still-missing gap OUTSIDE the lock, racing it against a steal
        // cancel and the stall watchdog. Dropping the `fill_gap` future on either
        // leaves the store's checkpointed prefix intact. `in_flight[i]` stays the
        // whole picked range, trimmed only by a peer's steal, so the steal's
        // split (read from the store's missing runs) and this worker's
        // `requeue_missing` (the missing bytes of that trimmed range) agree.
        let mut terminal: Option<UnitOutcome> = None;
        // The unit's verified bytes across its gaps, for the fault's log line.
        let mut landed = 0u64;
        // The start of the gap the unit ended on, for an overshoot check.
        let mut piece_at = r_start;
        for (g_start, g_len) in gaps {
            piece_at = g_start;
            let verified = AtomicU64::new(0);
            let outcome = {
                let fill = fill_gap(
                    store,
                    &lane.source,
                    engine.pacer,
                    engine.funder,
                    &lane.ctx,
                    &lane.ledger,
                    hash,
                    g_start,
                    g_len,
                    engine.drive,
                    &mut counters,
                    Some(engine.on_progress),
                    // The shared whole-blob delivered counter: every lane folds its
                    // own leg deltas in, so the bar reads one monotonic position.
                    Some(engine.progress_agg),
                    // This unit's verified bytes, which the watchdog judges.
                    Some(&verified),
                    // Consumption pacing (#1848): with a `WindowPacer`, gate this
                    // lane against the shared consumer cursor so it never runs more
                    // than one read-ahead window ahead of what the consumer read.
                    // `None` keeps the eager, unbounded fan-out.
                    lane_wait.as_ref().map(|w| w as &dyn PacingWait),
                    engine.pacing.map(|p| p.downstream),
                    // Everything this lane must not treat as its own: the
                    // aggregate spend the deposit gate subtracts, the fetch-wide
                    // top-up budget, and the credit path that shows a landed
                    // top-up to EVERY lane.
                    Some(engine.pool),
                );
                tokio::select! {
                    biased;
                    res = fill => match res {
                        Ok(()) => UnitOutcome::Completed,
                        // The loop classifies the fault once the lane ends.
                        Err(e) => UnitOutcome::Faulted(Some(e)),
                    },
                    () = cancelled(&handle) => UnitOutcome::Cancelled,
                    // Parked on the consumer with an earlier range waiting: give
                    // this range back (it re-queues like a steal) and take that
                    // one. Only under consumption pacing.
                    () = yield_to_front(
                        work,
                        i,
                        &my_coverage,
                        total_bytes,
                        &lane_parked,
                        &lane_parked_wake,
                        progress_wake,
                    ), if engine.pacing.is_some() => UnitOutcome::Cancelled,
                    // A watchdog trip carries no error by construction: the
                    // source simply stopped making verified progress. A store
                    // error while it checks is ours (#2213).
                    err = watchdog(store, g_start, g_len, engine.watchdog, &verified) => {
                        UnitOutcome::Faulted(err)
                    }
                }
            };
            let gap_landed = verified.load(Ordering::Relaxed);
            landed = landed.saturating_add(gap_landed);
            if gap_landed > 0 {
                delivered = true;
                if uncovered {
                    pulled_through = Some(Instant::now());
                }
                health.record_progress(provider);
            }
            match outcome {
                UnitOutcome::Completed => {}
                other => {
                    terminal = Some(other);
                    break;
                }
            }
        }

        match terminal {
            // Every gap filled: free the lane and pick again.
            None | Some(UnitOutcome::Completed) => {
                work.lock().await.clear(i)?;
            }
            // Stolen: re-queue the trimmed remainder and stay live. The
            // credit-window tail [paid_frontier, checkpointed_frontier) is NOT
            // re-billed here: resume is checkpoint-frontier via `missing_ranges`,
            // a bounded (<= credit window + INGEST_MAX_QUEUED_CHECKPOINTS + 1
            // 4 MiB INGEST_CHECKPOINT_BYTES intervals: the dropped batch plus
            // the checkpoints still queued), client-favorable gap identical to
            // the single-source cross-invocation resume.
            Some(UnitOutcome::Cancelled) => {
                if let Some(Requeued { held, bytes }) = requeue_missing(store, work, i).await? {
                    tracing::debug!(
                        %provider,
                        held_start = held.0,
                        held_len = held.1,
                        requeued_bytes = bytes,
                        "cancelled leg re-queued the missing bytes of its trimmed range"
                    );
                }
            }
            // Stalled/faulted: re-queue the remainder and end the worker.
            Some(UnitOutcome::Faulted(err)) => {
                let faulted_at = Instant::now();
                requeue_missing(store, work, i).await?;
                {
                    let mut w = work.lock().await;
                    if extra {
                        w.end_extra(i);
                    } else {
                        w.park(i);
                    }
                }
                wake();
                let range = crate::source_set::LaneRange {
                    offset: r_start,
                    len: r_len,
                    landed,
                    // The loop knows the first claim; it sets this.
                    past_end: false,
                    uncovered,
                };
                return Ok(WorkerEnd {
                    provider,
                    end: LaneEnd::Faulted {
                        err,
                        range,
                        piece_at,
                        at: faulted_at,
                    },
                    delivered,
                    pulled_through,
                    extra,
                });
            }
        }
        // An extra worker takes one piece, then gives its stream back.
        if extra {
            work.lock().await.end_extra(i);
            wake();
            return Ok(WorkerEnd {
                provider,
                end: LaneEnd::Idle,
                delivered,
                pulled_through,
                extra,
            });
        }
        wake();
    }
}

/// One worker's future: [`run_worker`], holding `hold`, what its stream
/// holds ([`Hold`]): a lane's own worker releases the lane's lease as it
/// ends, and every grant a [`LaneWiden`] made for it is given back.
async fn worker<St, S, Pc, F>(
    engine: &Engine<'_, St, Pc, F>,
    i: usize,
    lane: Arc<StreamCandidate<S>>,
    provider: Address,
    health: &PeerHealth,
    hold: Hold<S>,
) -> anyhow::Result<WorkerEnd>
where
    St: IngestStore,
    S: BlobSource,
    Pc: Pacer,
    F: Funder,
{
    let mut hold = hold;
    run_worker(engine, i, lane, provider, health, &mut hold).await
}

/// Log, where it happens, that an extra worker's stream faulted on `range`
/// while the lane's own worker runs, or while the node is already charged
/// for this outage. Such a fault is most often a refusal of the additional
/// stream, which is routine, so the line is at debug when nothing landed and
/// at info otherwise. Such a fault does not cool the node.
fn log_extra_fault(
    provider: Address,
    hash: [u8; 32],
    range: crate::source_set::LaneRange,
    err: &anyhow::Error,
) {
    let hash = blake3::Hash::from_bytes(hash).to_hex();
    let crate::source_set::LaneRange {
        offset,
        len,
        landed,
        past_end: _,
        uncovered,
    } = range;
    if landed > 0 {
        tracing::info!(
            %provider,
            %hash,
            offset,
            len,
            landed,
            uncovered,
            error = %format_args!("{err:#}"),
            "an extra stream of a lane faulted; its remainder goes back to the queue"
        );
    } else {
        tracing::debug!(
            %provider,
            %hash,
            offset,
            len,
            uncovered,
            error = %format_args!("{err:#}"),
            "an extra stream of a lane faulted before any verified byte; its range goes back \
             to the queue"
        );
    }
}

/// The consumption-pacing seam for a bounded, consumer-driven fetch (#1848): the
/// downstream read cursor every lane's [`WindowPacer`] gates against, and the
/// wait hook a lane parks on when its read-ahead window is full.
///
/// `None` (every non-streaming caller) is the eager, unbounded fetch: the pacer
/// caps only on budget, and a lane never parks on the consumer. `Some` (the
/// `Streamer`) bounds each lane to one read-ahead window ahead of the consumer's
/// cursor: a lane whose own delivered frontier runs a window past `downstream`
/// waits until the consumer reads more. Pass a [`WindowPacer`] as the fetch's
/// `pacer` to make the bound bite; a [`BudgetPacer`] ignores `downstream`, so the
/// seam is inert without one.
///
/// [`WindowPacer`]: crate::pacer::WindowPacer
/// [`BudgetPacer`]: crate::pacer::BudgetPacer
pub struct ConsumptionPacing<'a> {
    /// The downstream frontier (for the `Streamer`, the consumer's read cursor
    /// as `served_paid`) each lane's `WindowPacer` measures its outstanding
    /// bytes against.
    pub downstream: &'a (dyn Fn() -> DownstreamFrontier + Send + Sync),
    /// The wait hook a lane parks on when its read-ahead window is full, resolved
    /// once the consumer's cursor advances.
    pub pacing_wait: &'a dyn PacingWait,
}

impl std::fmt::Debug for ConsumptionPacing<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumptionPacing").finish_non_exhaustive()
    }
}

/// The byte ranges one [`acquire`] fills, fitted to the store's bound as it
/// moves (ADR 039 § Dynamic segmentation and tail-stealing).
struct Want {
    /// The `(offset, len)` ranges still to fill, each inside `bound`.
    ranges: Vec<(u64, u64)>,
    /// The ranges reach the bound's end: a whole-blob target, or explicit
    /// ranges that hold the tail. Such a target follows the bound as it grows
    /// and needs a proven size to complete.
    tail: bool,
    /// The bound the ranges were last fitted to.
    bound: u64,
}

impl Want {
    /// `ranges` against a first bound of `bound`, cut from a first claim of
    /// `c0`. A range that reaches the end of the blob as the caller knows it,
    /// `min(c0, bound)`, or has a zero length, is a tail: it runs to `bound`.
    /// Every other range is clipped to `bound`. A resumed record's bound can
    /// be past `c0`, so a whole-blob target cut from `c0` still reaches it.
    fn new(ranges: &[(u64, u64)], bound: u64, c0: u64) -> Self {
        let end = c0.min(bound);
        let reaches_end = |start: u64, len: u64| len == 0 || start.saturating_add(len) >= end;
        let tail = ranges.iter().any(|&(start, len)| reaches_end(start, len));
        let ranges: Vec<(u64, u64)> = ranges
            .iter()
            .filter(|&&(start, _)| start < bound)
            .map(|&(start, len)| {
                let room = bound - start;
                let len = if reaches_end(start, len) {
                    room
                } else {
                    len.min(room)
                };
                (start, len)
            })
            .collect();
        Self {
            ranges,
            tail,
            bound,
        }
    }

    /// Fit the ranges to a new `bound`. A tail target gains `[old, bound)`
    /// when the bound grows; every range is clipped when it shrinks. Returns
    /// whether the bound moved.
    fn fit(&mut self, bound: u64) -> bool {
        if bound == self.bound {
            return false;
        }
        if bound > self.bound && self.tail {
            self.ranges.push((self.bound, bound - self.bound));
        }
        if bound < self.bound {
            self.ranges = self
                .ranges
                .iter()
                .filter(|&&(start, _)| start < bound)
                .map(|&(start, len)| (start, len.min(bound - start)))
                .collect();
        }
        self.bound = bound;
        true
    }
}

/// Releases every started lane's [`LaneLease`] when [`acquire`] returns or is
/// dropped: the lease of a lane without a [`LaneWiden`], and any lease a
/// lane's own worker did not release ([`ReleaseLease`]).
struct ReleaseLeases<S>(Vec<Arc<StreamCandidate<S>>>);

impl<S> Drop for ReleaseLeases<S> {
    fn drop(&mut self) {
        for lane in &self.0 {
            lane.lease.release();
        }
    }
}

/// Every started lane's `(ctx, ledger)`: the pool-wide view of an acquire
/// with no run registry.
type PoolLanes = Vec<(Arc<Mutex<PoolContext>>, Arc<PoolLedger>)>;

/// Each lane slot's provider and lane, keyed by the lane's slot in [`Work`].
type LaneAt<S> = HashMap<usize, (Address, Arc<StreamCandidate<S>>)>;

/// Add a lane that starts for the first time to the pool view. A lane that
/// joins late adopts a top-up credited before it did, so it never gates on a
/// stale deposit.
fn join_pool<S>(lane: &StreamCandidate<S>, deposit: U256, pool_lanes: &Mutex<PoolLanes>) {
    if let Ok(mut ctx) = lane.ctx.lock()
        && ctx.deposit < deposit
    {
        ctx.deposit = deposit;
    }
    pool_lanes
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push((Arc::clone(&lane.ctx), Arc::clone(&lane.ledger)));
}

/// Up to `room` holders to start now, nearest first: usable at `now` and
/// `deposit`, not in `busy`, and with work still to do that each may take. A
/// holder barred from pull-through ([`SourceSet::no_pull_through`]) may take
/// only what it covers.
fn lanes_to_start<P: SourceProvider>(
    sources: &SourceSet<'_, P>,
    work: &Work,
    total_bytes: u64,
    now: Instant,
    deposit: U256,
    mut busy: HashSet<Address>,
    room: usize,
) -> Vec<Holder> {
    let mut out = Vec::new();
    while out.len() < room {
        let Some(holder) = sources.next_to_start(now, deposit, &busy) else {
            break;
        };
        busy.insert(holder.provider);
        let take_uncovered = !sources.no_pull_through(holder.provider);
        if work.has_work_for(
            &holder_coverage(&holder, total_bytes),
            total_bytes,
            take_uncovered,
        ) {
            out.push(holder);
        }
    }
    out
}

/// `holder`'s coverage, or every block of a blob of `total_bytes` for a holder
/// of the whole blob.
fn holder_coverage(holder: &Holder, total_bytes: u64) -> Coverage {
    holder
        .coverage
        .clone()
        .unwrap_or_else(|| Coverage::full(num_blocks(total_bytes)))
}

/// Whether no work left lies inside the coverage of a holder barred from
/// pull-through ([`SourceSet::no_pull_through`]): each such holder can serve
/// none of it, so the item ends once the other sources can serve none of it
/// either ([`SourceSet::exhausted`]).
fn only_uncovered_left<P: SourceProvider>(
    sources: &SourceSet<'_, P>,
    work: &Work,
    total_bytes: u64,
) -> bool {
    sources
        .holders()
        .iter()
        .filter(|h| sources.no_pull_through(h.provider))
        .all(|h| !work.has_work_for(&holder_coverage(h, total_bytes), total_bytes, false))
}

/// Whether every byte of `ranges` is present. With no lane running nothing is
/// in flight, so an empty queue while bytes are missing is refilled from the
/// store.
///
/// # Errors
///
/// A store read or alignment error.
async fn all_present<St>(
    store: &St,
    ranges: &[(u64, u64)],
    work: &AsyncMutex<Work>,
) -> anyhow::Result<bool>
where
    St: IngestStore,
{
    let total_bytes = store.total_bytes();
    let gaps = contiguous_byte_ranges(&missing_chunks(store, ranges).await?, total_bytes);
    if gaps.is_empty() {
        return Ok(true);
    }
    let mut w = work.lock().await;
    if w.pending.is_empty() && !w.busy() {
        for (start, len) in gaps {
            w.pending.push_back(align_range(start, len, total_bytes)?);
        }
    }
    Ok(false)
}

/// A lane build in flight: the provider it builds for, and the result.
pub(crate) type Connecting<'p, S> = std::pin::Pin<
    Box<dyn Future<Output = (Address, anyhow::Result<StreamCandidate<S>>)> + Send + 'p>,
>;

/// Build `holder`'s lane through `provider`.
pub(crate) fn connect_future<P: SourceProvider>(
    provider: &P,
    holder: Holder,
) -> Connecting<'_, P::Source> {
    Box::pin(async move {
        let built = provider.connect(&holder).await;
        (holder.provider, built)
    })
}

/// Await the boxed future in `slot` in place, without taking it, so a
/// cancelled `select!` branch leaves it to be polled again. An empty slot
/// never resolves.
async fn poll_opt<T>(slot: &mut Option<SourceFuture<'_, T>>) -> anyhow::Result<T> {
    match slot.as_mut() {
        Some(fut) => fut.await,
        None => std::future::pending().await,
    }
}

/// The next item of `slot`'s stream, or wait forever without one.
async fn next_opt<T>(slot: &mut Option<SourceStream<'_, T>>) -> Option<T> {
    use futures_util::StreamExt as _;
    match slot.as_mut() {
        Some(stream) => stream.next().await,
        None => std::future::pending().await,
    }
}

/// Sleep until `wake`, or forever without one.
pub(crate) async fn sleep_until_opt(wake: Option<Instant>) {
    match wake {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// When [`acquire`] asks its busy lanes for an extra stream (#2231, #2230),
/// and what it last found.
struct Growth {
    /// A worker end or a lane build asks for a growth pass.
    wanted: bool,
    /// When to run a pass with no other trigger.
    retry_at: Option<Instant>,
    /// Each lane slot's wait after its node refused its extra streams.
    extra_backoff: HashMap<usize, crate::source_set::Backoff>,
    /// The queued ranges the last pass left without a taker, as
    /// `(start, len)`.
    waiting: Vec<(u64, u64)>,
    /// When `waiting` last changed.
    waiting_since: Instant,
    /// When `waiting` was last logged at info, or `None` before the first
    /// such line.
    logged_at: Option<Instant>,
}

/// What one growth pass left waiting.
struct GrowthPass {
    /// Each queued range without a taker, as `(start, len)`.
    waiting: Vec<(u64, u64)>,
    /// How many of them had candidate lanes that all refused a stream in
    /// this pass.
    refused: usize,
    /// How many of them had no candidate lane at all.
    no_candidate: usize,
    /// Whether a running lane has a [`LaneWiden`]. A range with no lane to
    /// ask now can find one once such a lane's own worker holds a range, and
    /// that pick wakes no pass.
    growable: bool,
}

impl Growth {
    fn new(now: Instant) -> Self {
        Self {
            wanted: false,
            retry_at: None,
            extra_backoff: HashMap::new(),
            waiting: Vec::new(),
            waiting_since: now,
            logged_at: None,
        }
    }

    /// Whether a pass runs now: a trigger asked for one, or the retry is due.
    fn due(&mut self, now: Instant) -> bool {
        std::mem::take(&mut self.wanted) || self.retry_at.is_some_and(|at| now >= at)
    }

    /// Whether lane `lane` may be asked for an extra stream at `now`: its
    /// node refused no extra stream, or the wait after the refusals ended.
    fn may_ask(&self, lane: usize, now: Instant) -> bool {
        self.extra_backoff
            .get(&lane)
            .is_none_or(|backoff| now >= backoff.next_at)
    }

    /// Run a pass no later than `at`.
    fn retry_by(&mut self, at: Instant) {
        self.retry_at = Some(self.retry_at.map_or(at, |retry| retry.min(at)));
    }

    /// A node refused lane `lane`'s extra stream at `now`: the lane waits
    /// before it is asked again, and a pass runs when the wait ends. Returns
    /// the refusals in a row.
    fn extra_refused(&mut self, lane: usize, now: Instant) -> u32 {
        let backoff = self
            .extra_backoff
            .get(&lane)
            .copied()
            .unwrap_or(crate::source_set::Backoff::new(now))
            .fail(now, GROWTH_RETRY, EXTRA_RETRY_CAP);
        self.extra_backoff.insert(lane, backoff);
        self.retry_by(backoff.next_at);
        backoff.attempts
    }

    /// An extra stream of lane `lane` verified a byte: its node serves extra
    /// streams, so the lane may be asked again at once.
    fn extra_served(&mut self, lane: usize) {
        self.extra_backoff.remove(&lane);
    }

    /// Record what the pass at `now` left waiting, and when to run the next
    /// pass with no other trigger: after [`GROWTH_RETRY`] when a `grow`
    /// granted nothing or a range had no lane to ask while a running lane has
    /// a [`LaneWiden`], and when a lane's wait after a refused extra stream
    /// ends. Log the waiting ranges at info on the first wait and then at most
    /// every [`WAITING_RELOG`] while ranges wait, and at debug when they
    /// change between those lines.
    fn passed(&mut self, now: Instant, hash: [u8; 32], pass: GrowthPass) {
        self.retry_at = None;
        if pass.waiting.is_empty() {
            self.waiting.clear();
            return;
        }
        if pass.refused > 0 || (pass.no_candidate > 0 && pass.growable) {
            self.retry_by(now + GROWTH_RETRY);
        }
        if let Some(ends) = self
            .extra_backoff
            .values()
            .map(|backoff| backoff.next_at)
            .filter(|at| *at > now)
            .min()
        {
            self.retry_by(ends);
        }
        let changed = pass.waiting != self.waiting;
        if changed {
            self.waiting = pass.waiting;
            self.waiting_since = now;
        }
        let ranges = byte_runs(self.waiting.iter().copied());
        let waited_secs = now.saturating_duration_since(self.waiting_since).as_secs();
        if self
            .logged_at
            .is_none_or(|at| now.saturating_duration_since(at) >= WAITING_RELOG)
        {
            self.logged_at = Some(now);
            tracing::info!(
                hash = %blake3::Hash::from_bytes(hash).to_hex(),
                %ranges,
                refused = pass.refused,
                no_candidate = pass.no_candidate,
                waited_secs,
                "a queued range waits for a stream: no idle lane takes it, and no busy lane \
                 could take one more stream for it"
            );
        } else if changed {
            tracing::debug!(
                hash = %blake3::Hash::from_bytes(hash).to_hex(),
                %ranges,
                refused = pass.refused,
                no_candidate = pass.no_candidate,
                waited_secs,
                "a queued range waits for a stream: no idle lane takes it, and no busy lane \
                 could take one more stream for it"
            );
        }
    }
}

/// Flush the present record so the bytes that landed stay recorded for a
/// resume, then hand back `err`. A flush failure is logged: `err` is the
/// reason the acquire ends.
async fn flushed<St>(store: &St, err: anyhow::Error) -> anyhow::Error
where
    St: IngestStore,
{
    if let Err(flush) = store.flush_present_record().await {
        tracing::warn!("flushing the present record failed: {flush:#}");
    }
    err
}

/// The pool deposit as the loop sees it: the larger of the deposit watch and
/// every started lane's context. A bundle entry's lanes share their contexts
/// with sibling entries through the run's `LaneLedgers`, so a top-up a
/// sibling credits reaches this loop through them.
fn pool_deposit<S>(
    watch: &tokio::sync::watch::Receiver<U256>,
    lanes: &[Arc<StreamCandidate<S>>],
) -> U256 {
    lanes
        .iter()
        .filter_map(|lane| lane.ctx.lock().ok().map(|ctx| ctx.deposit))
        .fold(*watch.borrow(), U256::max)
}

/// Seed the deposit watch from `lane`'s pool context while it still holds zero:
/// the first lane that builds names the pool's deposit.
fn seed_deposit<S>(deposit: &tokio::sync::watch::Sender<U256>, lane: &StreamCandidate<S>) {
    let Ok(ctx) = lane.ctx.lock() else {
        return;
    };
    let seen = ctx.deposit;
    drop(ctx);
    deposit.send_if_modified(|current| {
        if current.is_zero() && !seen.is_zero() {
            *current = seen;
            true
        } else {
            false
        }
    });
}

/// Fill `target.ranges` of one blob from `sources` (ADR 039 § Dynamic
/// segmentation and tail-stealing).
///
/// Lanes start nearest-first up to [`AcquireEnv::max_lanes`], each driving
/// `fill_gap` over one range at a time and stealing from the largest range in
/// flight when nothing is queued. A lane that faults re-queues its remainder
/// and ends; [`SourceSet::record_fault`] decides what the fault means for its
/// source, and the loop starts another lane from whatever is usable. When no
/// lane can run, the loop sleeps until a source cools down, a lane build or a
/// discovery may retry, or the deposit rises. It never runs out of lanes: it
/// waits, and [`AcquireEnv::stop`] decides how long.
///
/// The lane watchdog ([`LANE_WATCHDOG`]) is on for an eager fetch and off
/// under consumption pacing.
///
/// # The size is a hint
///
/// `target.total_bytes` is the first size claim, `C0`, from a probe hint, a
/// signed header or a manifest; a resumed store keeps the bound its record
/// holds. The loop corrects the claim as bytes land:
///
/// - **Shrink.** A leg that verifies the final chunk of its sender's claim
///   proves that size, and the store's bound moves to it. The loop clips every
///   pending and in-flight range to it and cancels a lane whose range starts
///   past it.
/// - **Grow.** When every byte below the bound is present and no size is
///   proven, the bound grows to `max(bound + SEED, C0 + 2 * extra)`, rounded
///   up to a multiple of `SEED` (one discovery block), where `extra` is the
///   verified bytes past `C0`. The loop plans the new region.
///
/// A target whose ranges reach the bound's end completes once a size is
/// proven and every byte below it is present. Explicit ranges that stop short
/// of the end complete once they are present.
///
/// # Per-source payment (ADR 039 § Payment)
///
/// Each lane pays with its OWN `(ctx, ledger)`: a voucher is scoped to one
/// on-chain provider and one `(signer, provider)` watermark. One shared pool
/// DEPOSIT backs every lane, and every lane draws through one shared view of it.
/// `ledgers` picks which view: `None` subtracts the committed amount of every
/// lane this acquire has started; `Some(reg)` subtracts `reg.total_committed()`,
/// the sum over every lane a `bundle pull` run has registered. Either way the
/// funder's view of the whole pool ([`crate::Funder::pool_spent`]) wins when it
/// is larger, and the
/// reactive-top-up budget is counted once for the acquire rather than once per
/// lane, and a landed top-up is credited to every lane the view covers. The gate
/// is evaluated at each `fill_gap` leg boundary; the hard backstop against a
/// node redeeming past the deposit stays on-chain.
///
/// Finalization is the caller's job: this only flushes the present record
/// (its single-writer flush point), periodically and once more when it
/// returns. A lane with a [`LaneWiden`] releases its [`LaneLease`] when its
/// own worker ends, and starts again only on a stream its `grow` grants.
/// Every started lane's lease is released at the latest when it returns or
/// is dropped. Before it returns, it stops every lane and awaits each lane
/// build still in flight, for at most 30 s: a build can be in the middle of an
/// on-chain top-up. A lane built there never starts.
///
/// # Errors
///
/// - a fatal fault ([`crate::Fault::Fatal`]) a lane ended with, verbatim;
/// - a fatal lane build, wrapped in [`crate::LaneBuildFault`]: one that may
///   have escrowed USDC no record credits, which a retry would escrow again;
/// - [`crate::GaveUp`] once the stop policy's limit passes without a verified
///   byte;
/// - [`crate::NoAffordableSource`] or [`crate::NoSourceHasBlob`] on a
///   unanimous verdict of the sources;
/// - a store I/O failure or a segmentation alignment error.
pub async fn acquire<St, P, Pc, F>(
    target: AcquireTarget<'_, St>,
    sources: &mut SourceSet<'_, P>,
    env: &AcquireEnv<'_, Pc, F>,
) -> anyhow::Result<()>
where
    St: IngestStore,
    P: SourceProvider,
    Pc: Pacer,
    F: Funder,
{
    let watchdog = if env.pacing.is_none() {
        LANE_WATCHDOG
    } else {
        Duration::ZERO
    };
    acquire_with_watchdog(target, sources, env, watchdog).await
}

/// [`acquire`] with an explicit lane watchdog window; zero turns it off.
#[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
// The loop's setup (the shared pool view, the progress base) and its one
// `select!` over workers, lane builds, discovery and timers. Each arm acts on
// the same loop state, so splitting them apart would scatter that state; the
// `select!` expansion is most of the complexity score.
async fn acquire_with_watchdog<'p, St, P, Pc, F>(
    target: AcquireTarget<'_, St>,
    sources: &mut SourceSet<'p, P>,
    env: &AcquireEnv<'_, Pc, F>,
    watchdog: Duration,
) -> anyhow::Result<()>
where
    St: IngestStore,
    P: SourceProvider,
    Pc: Pacer,
    F: Funder,
{
    let AcquireTarget {
        store,
        hash,
        total_bytes: c0,
        ranges,
    } = target;
    let health = Arc::clone(sources.health());
    // A claim is peer- or manifest-controlled: clamp it to the caller's cap
    // before it sizes any work or coverage bitmap.
    let cap = env.max_blob_bytes;
    let capped = |size: u64| if cap > 0 { size.min(cap) } else { size };
    let c0 = capped(c0);
    if cap > 0 && store.total_bytes() > cap {
        store.set_bound(cap);
    }
    // A resumed record's bound wins over the caller's hint.
    let first_bound = store.total_bytes();
    if cap > 0 && first_bound > cap {
        // Only a proven size is past the cap after the clamp above.
        return Err(anyhow::Error::new(crate::BlobTooLarge {
            reached: first_bound,
            ceiling: cap,
        }));
    }
    let mut want = Want::new(ranges, first_bound, c0);
    let mut pending = VecDeque::new();
    for (start, len) in
        contiguous_byte_ranges(&missing_chunks(store, &want.ranges).await?, first_bound)
    {
        pending.push_back(align_range(start, len, first_bound)?);
    }
    let work = AsyncMutex::new(Work::new(pending, env.pacing.is_some()));
    let progress_wake = Notify::new();

    // The pool deposit every lane draws on, as the loop last saw it. The first
    // built lane seeds it; a landed top-up publishes the new value.
    let (deposit_tx, mut deposit_rx) = tokio::sync::watch::channel(U256::ZERO);
    // Every started lane's `(ctx, ledger)`: the pool-wide view when there is no
    // run registry.
    let pool_lanes: Mutex<PoolLanes> = Mutex::new(Vec::new());
    // The pool's spend: what the loop's own lanes committed, or the funder's
    // view of the whole pool when that is larger (it also counts lanes the
    // loop does not drive).
    let funder = env.funder;
    let lanes = &pool_lanes;
    let own: Box<dyn Fn() -> U256 + Send + Sync + '_> = match env.ledgers {
        Some(reg) => Box::new(move || reg.total_committed()),
        None => Box::new(move || {
            lanes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .map(|(_, ledger)| ledger.committed().amount)
                .fold(U256::ZERO, U256::saturating_add)
        }),
    };
    let spent = move || own().max(funder.pool_spent().unwrap_or(U256::ZERO));
    let credit: Box<dyn Fn(U256) -> anyhow::Result<()> + Send + Sync + '_> = match env.ledgers {
        Some(reg) => Box::new(|new_deposit| {
            reg.credit_all(new_deposit);
            deposit_tx.send_replace(new_deposit);
            Ok(())
        }),
        None => Box::new(|new_deposit| {
            for (ctx, _) in pool_lanes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
            {
                ctx.lock()
                    .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?
                    .deposit = new_deposit;
            }
            deposit_tx.send_replace(new_deposit);
            Ok(())
        }),
    };
    let topups_used = AtomicU32::new(0);
    // With a run registry every fetch of the run tops up the one deposit, so
    // they share the run's lock; a solo acquire serializes only its own lanes.
    let own_topup_lock = tokio::sync::Mutex::new(());
    let pool = SharedPool {
        spent: &spent,
        topups_used: &topups_used,
        credit: &*credit,
        topup_lock: env.ledgers.map_or(&own_topup_lock, LaneLedgers::topup_lock),
    };

    // Seeded with the bytes already present, so a resumed fetch's bar starts
    // where the last run left off; each lane folds in its own leg deltas.
    let base_present = ranges_content_len(&store.present_ranges().await?, first_bound);
    let progress_agg = AtomicU64::new(base_present);
    // Surface the resume base on the bar before any lane opens a channel.
    if let Some(cb) = env.on_progress {
        cb(base_present, first_bound);
    }
    // The bound the loop last fitted its work to. A leg that proves a size
    // moves the store's bound; the progress report sees that first and wakes
    // the loop to clip the work.
    let bound_seen = AtomicU64::new(first_bound);
    let bound_moved = Notify::new();
    // Every verified byte ticks the stop clock, then reaches the caller.
    let on_progress = |position: u64, total: u64| {
        env.stop.clock.tick();
        if store.total_bytes() != bound_seen.load(Ordering::Acquire) {
            bound_moved.notify_one();
        }
        if let Some(cb) = env.on_progress {
            cb(position, total);
        }
    };
    let engine = Engine {
        store,
        hash,
        pacer: env.pacer,
        funder: env.funder,
        drive: env.drive,
        work: &work,
        progress_wake: &progress_wake,
        progress_agg: &progress_agg,
        on_progress: &on_progress,
        pool: &pool,
        watchdog,
        pacing: env.pacing,
    };

    let max_lanes = env.max_lanes.max(1);
    let mut started = ReleaseLeases(Vec::new());
    let mut workers = FuturesUnordered::new();
    let mut connecting: FuturesUnordered<Connecting<'p, P::Source>> = FuturesUnordered::new();
    let mut connecting_set: HashSet<Address> = HashSet::new();
    let mut discovering: Option<SourceFuture<'p, Vec<Holder>>> = None;
    // Holders the provider pushes after the start. While the stream is open
    // the loop asks for no discovery: the pushed holders are the answer a
    // discovery would wait for.
    let mut arrivals: Option<SourceStream<'p, Holder>> = sources.provider().arrivals();
    let mut running: HashSet<Address> = HashSet::new();
    let mut slots: HashMap<Address, usize> = HashMap::new();
    // Each lane slot's provider and lane, for a growth request (#2231).
    let mut lane_at: LaneAt<P::Source> = HashMap::new();
    // Extra workers still running, and when to ask busy lanes for one more
    // stream (#2230).
    let mut extras_running = 0usize;
    let mut growth = Growth::new(Instant::now());
    // Providers charged a fault since their last verified byte: a lane's
    // outage is charged once, however many of its workers fault in it.
    let mut charged: HashSet<Address> = HashSet::new();
    let mut ready: Vec<(Address, Arc<StreamCandidate<P::Source>>)> = Vec::new();
    let mut seeded = false;
    let mut flush = tokio::time::interval_at(
        Instant::now() + PRESENT_RECORD_FLUSH_INTERVAL,
        PRESENT_RECORD_FLUSH_INTERVAL,
    );
    // The record write in flight, if any. It runs beside the workers, never
    // instead of them: awaited inline, a slow record fsync would leave every
    // lane unpolled for as long as it takes (#2211). A tick that finds a write
    // still in flight skips, which keeps this loop the single record writer.
    let mut record: Option<SourceFuture<'_, ()>> = None;
    let result: anyhow::Result<()> = async {
        loop {
            let bound = store.total_bytes();
            if want.fit(bound) {
                bound_seen.store(bound, Ordering::Release);
                let mut w = work.lock().await;
                w.grow_coverage(bound);
                w.clip(bound)?;
            }
            if running.is_empty()
                && ready.is_empty()
                && extras_running == 0
                && all_present(store, &want.ranges, &work).await?
            {
                if let Some(proven) = store.proven()
                    && cap > 0
                    && proven > cap
                {
                    // A cap off the chunk-group grid lets a leg that ends at
                    // it prove a size up to one group past it.
                    let too_large = crate::BlobTooLarge {
                        reached: proven,
                        ceiling: cap,
                    };
                    return Err(anyhow::Error::new(too_large));
                }
                if store.proven().is_some() || !want.tail {
                    // Land the write in flight first, so it cannot rename an
                    // older snapshot over the final one. Its failure is only
                    // logged: the final write below supersedes it, and its
                    // own result decides.
                    if let Some(pending) = record.take()
                        && let Err(err) = pending.await
                    {
                        tracing::warn!(
                            "a periodic present-record write failed before the final one: \
                             {err:#}"
                        );
                    }
                    store.flush_present_record().await?;
                    return Ok(());
                }
                // Every byte below the bound is present and no leg proved a
                // size: the claim was short. Grow and plan the new region, but
                // never past the cap: a bound already at the cap with no proof
                // means the blob holds bytes past it.
                if cap > 0 && bound >= cap {
                    let too_large = crate::BlobTooLarge {
                        reached: bound.saturating_add(1),
                        ceiling: cap,
                    };
                    return Err(anyhow::Error::new(too_large));
                }
                let extra = bytes_past(&store.present_ranges().await?, c0, bound);
                let grown = capped(grown_bound(bound, c0, extra));
                tracing::debug!(
                    hash = %blake3::Hash::from_bytes(hash).to_hex(),
                    bound,
                    grown,
                    extra,
                    "every byte below the size claim is present and none proves the size; \
                     growing the claim"
                );
                store.set_bound(grown);
                want.fit(grown);
                bound_seen.store(grown, Ordering::Release);
                let missing = missing_chunks(store, &want.ranges).await?;
                work.lock().await.regrow(&missing, grown)?;
                if let Some(cb) = env.on_progress {
                    cb(progress_agg.load(Ordering::Relaxed), grown);
                }
                continue;
            }
            let total_bytes = bound;
            let now = Instant::now();
            let deposit = pool_deposit(&deposit_rx, &started.0);

            // Start lanes up to the cap, nearest first, for sources that cover work
            // still to do: cached lanes at once, the rest through `connect`.
            let busy: HashSet<Address> = running
                .iter()
                .chain(&connecting_set)
                .copied()
                .chain(ready.iter().map(|(p, _)| *p))
                .collect();
            let room = max_lanes.saturating_sub(busy.len());
            let starts = {
                let mut w = work.lock().await;
                // A fault, a delivery, a discovery or the end of a bar's time
                // may have set or lifted a source's bar from pull-through since
                // the last pass.
                sources.expire_pull_through_bars(now);
                for (&provider, &slot) in &slots {
                    w.set_no_uncovered(slot, sources.no_pull_through(provider));
                }
                lanes_to_start(sources, &w, total_bytes, now, deposit, busy, room)
            };
            for holder in starts {
                if let Some(lane) = sources.cached_lane(holder.provider) {
                    ready.push((holder.provider, lane));
                } else {
                    connecting_set.insert(holder.provider);
                    connecting.push(connect_future(sources.provider(), holder));
                }
            }

            // Start every ready lane. The first batch plans the work.
            if !ready.is_empty() {
                let batch = std::mem::take(&mut ready);
                let mut w = work.lock().await;
                if !seeded {
                    seeded = true;
                    let coverages: Vec<Option<Coverage>> = batch
                        .iter()
                        .map(|(_, lane)| lane.coverage.clone())
                        .collect();
                    let missing = missing_chunks(store, &want.ranges).await?;
                    for ((provider, _), slot) in
                        batch.iter().zip(w.seed(&missing, total_bytes, &coverages)?)
                    {
                        slots.insert(*provider, slot);
                    }
                    tracing::debug!(
                        hash = %blake3::Hash::from_bytes(hash).to_hex(),
                        total_bytes,
                        lanes = batch.len(),
                        pending = %byte_runs(w.pending.iter().map(|s| (s.fetch_start(), s.fetch_len()))),
                        "planned the missing ranges across the first lanes"
                    );
                }
                for (provider, lane) in batch {
                    // A lane with a `LaneWiden` gave its lease back when its
                    // own worker ended, so it starts again only on a stream
                    // `grow` grants now. When `grow` grants none, its start
                    // waits `GROWTH_RETRY` and tries again (#2230). A lane that
                    // still holds its lease starts on it.
                    let restart = started.0.iter().any(|l| Arc::ptr_eq(l, &lane));
                    let grant = match lane.widen.as_ref() {
                        Some(widen) if restart && !lane.lease.is_held() => {
                            let at = Instant::now();
                            if !widen.grow(GrowFor::Restart) {
                                if sources.start_refused(provider, at + GROWTH_RETRY) {
                                    tracing::info!(
                                        %provider,
                                        hash = %blake3::Hash::from_bytes(hash).to_hex(),
                                        "a lane cannot start again: its provider has no free \
                                         stream; it tries again each second"
                                    );
                                }
                                continue;
                            }
                            sources.start_taken(provider);
                            Some(ReleaseGrant(Arc::clone(&lane)))
                        }
                        _ => None,
                    };
                    let slot = if let Some(&slot) = slots.get(&provider) {
                        w.revive(slot);
                        slot
                    } else {
                        let slot = w.add_lane(lane.coverage.clone(), total_bytes);
                        slots.insert(provider, slot);
                        slot
                    };
                    tracing::debug!(
                        hash = %blake3::Hash::from_bytes(hash).to_hex(),
                        %provider,
                        slot,
                        coverage = %lane.coverage.as_ref().map_or_else(
                            || "whole blob".to_owned(),
                            block_runs,
                        ),
                        "lane starts"
                    );
                    if !started.0.iter().any(|l| Arc::ptr_eq(l, &lane)) {
                        join_pool(&lane, deposit, &pool_lanes);
                        started.0.push(Arc::clone(&lane));
                    }
                    running.insert(provider);
                    w.set_provider(slot, provider);
                    lane_at.insert(slot, (provider, Arc::clone(&lane)));
                    let hold = Hold::Own {
                        lease: ReleaseLease(Arc::clone(&lane)),
                        grant,
                    };
                    workers.push(worker(&engine, slot, lane, provider, &health, hold));
                }
            }

            // A queued range no idle worker takes, such as a faulted lane's
            // remainder, goes to a busy lane on an extra stream, so it does
            // not wait for that lane's whole range (#2231, #2252). The lane covers
            // part of the range, or, when part of the range has no running
            // lane that covers it, is not barred from pull-through. Asked
            // after this pass started its lanes, so a lane that just started
            // counts as a taker. A lane whose node refused its extra stream
            // waits before it is asked again; a pass that a `grow` refused
            // runs again after `GROWTH_RETRY` (#2230). So does a pass that
            // found no lane to ask while a running lane has a `LaneWiden`: a
            // worker that takes its next range wakes no pass, and an own
            // worker drains a whole queue without ending (#2252).
            if growth.due(now) {
                let mut w = work.lock().await;
                let wanted = w.growth_wanted(total_bytes, |lane| {
                    growth.may_ask(lane, now)
                        && lane_at.get(&lane).is_some_and(|(_, l)| l.widen.is_some())
                });
                let mut refused_lanes: HashSet<usize> = HashSet::new();
                let mut pass = GrowthPass {
                    waiting: Vec::new(),
                    refused: 0,
                    no_candidate: 0,
                    growable: lane_at
                        .values()
                        .any(|(provider, l)| running.contains(provider) && l.widen.is_some()),
                };
                for (range, candidates) in wanted {
                    let had_candidates = !candidates.is_empty();
                    // Ask the candidate lanes in lane order until one grants a
                    // stream. `grow` never waits. A lane that grants may be
                    // asked again for the next range, so one pass fills every
                    // stream the lanes grant; a lane that refuses is not asked
                    // again in this pass.
                    let taker = candidates.into_iter().find_map(|lane| {
                        if refused_lanes.contains(&lane) {
                            return None;
                        }
                        let (provider, l) = lane_at.get(&lane)?;
                        let granted = l
                            .widen
                            .as_ref()
                            .is_some_and(|widen| widen.grow(GrowFor::Extra));
                        if !granted {
                            refused_lanes.insert(lane);
                        }
                        granted.then(|| (lane, *provider, Arc::clone(l)))
                    });
                    let Some((lane, provider, l)) = taker else {
                        pass.waiting.push((range.fetch_start(), range.fetch_len()));
                        if had_candidates {
                            pass.refused = pass.refused.saturating_add(1);
                        } else {
                            pass.no_candidate = pass.no_candidate.saturating_add(1);
                        }
                        continue;
                    };
                    // The grant is built before the future and moves into it,
                    // so it is given back however the worker ends.
                    let hold = Hold::Extra(ReleaseGrant(Arc::clone(&l)));
                    let slot = w.add_extra(lane);
                    workers.push(worker(&engine, slot, l, provider, &health, hold));
                    extras_running = extras_running.saturating_add(1);
                    tracing::debug!(
                        %provider,
                        offset = range.fetch_start(),
                        len = range.fetch_len(),
                        "a busy lane takes one extra stream for a queued range"
                    );
                }
                growth.passed(now, hash, pass);
            }

            // A lane ends priced out only once its driver's own top-up path has
            // declined, so the top-up budget cannot revive a source at this deposit.
            if running.is_empty() && connecting.is_empty() && discovering.is_none() {
                let only_uncovered = only_uncovered_left(sources, &*work.lock().await, total_bytes);
                if let Some(err) = sources.exhausted(deposit, false, only_uncovered) {
                    return Err(err);
                }
            }
            let uncovered = work.lock().await.uncovered(total_bytes);
            if discovering.is_none()
                && arrivals.is_none()
                && sources.wants_discovery(now, deposit, running.len(), uncovered)
            {
                discovering = Some(sources.provider().discover(sources.hash()));
            }
            let wake = match (sources.next_wake(now), growth.retry_at) {
                (Some(at), Some(retry)) => Some(at.min(retry)),
                (at, retry) => at.or(retry),
            };

            tokio::select! {
                biased;
                Some(end) = workers.next(), if !workers.is_empty() => {
                    // `Err` here is this process's fault (store I/O, a slot bug).
                    let WorkerEnd { provider, end, delivered, pulled_through, extra } = match end {
                        Ok(end) => end,
                        Err(err) => return Err(err),
                    };
                    // The own worker's lease went with its future
                    // (`ReleaseLease`).
                    let lane_slot = slots.get(&provider).copied();
                    debug_assert!(lane_slot.is_some(), "a worker ended for an unstarted lane");
                    if extra {
                        extras_running = extras_running.saturating_sub(1);
                        if delivered && let Some(lane) = lane_slot {
                            growth.extra_served(lane);
                        }
                    } else {
                        running.remove(&provider);
                    }
                    // Every end can free a stream or a taker for a queued
                    // range, so growth is asked for again; a fault on an
                    // extra stream that the lane outlives is the exception
                    // below.
                    growth.wanted = true;
                    if delivered {
                        sources.record_progress(provider);
                        charged.remove(&provider);
                    }
                    if let Some(at) = pulled_through {
                        sources.record_pull_through(provider, at);
                    }
                    if let LaneEnd::Faulted {
                        err,
                        range,
                        piece_at,
                        at,
                    } = end
                    {
                        let err = err.unwrap_or_else(|| {
                            anyhow::anyhow!("no verified progress for {watchdog:?}")
                        });
                        let range = crate::source_set::LaneRange {
                            past_end: piece_at >= known_end(store, c0),
                            ..range
                        };
                        // A fault on an extra stream stops only that stream: it
                        // is most often a refusal of the additional stream, so
                        // it does not cool the node while the lane's own worker
                        // still runs, and it asks for no growth at once: the
                        // lane waits, from `GROWTH_RETRY` and doubling up to
                        // `EXTRA_RETRY_CAP`, before it is asked again. A
                        // `NotFound` or size-ceiling refusal of a range outside
                        // the lane's coverage counts toward barring it from
                        // pull-through, as on its own stream. A lane's last
                        // live worker faults for the lane, so once the own
                        // worker has stopped, the extra's fault is recorded
                        // like the lane's own, unless the node is already
                        // charged for this outage (no verified byte since its
                        // last recorded fault). A fatal fault is the pool's or
                        // ours, not the stream's, and always ends the acquire.
                        if extra
                            && (running.contains(&provider) || charged.contains(&provider))
                        {
                            if let Fault::Fatal(_) = crate::fault::classify(&err) {
                                return Err(err);
                            }
                            log_extra_fault(provider, hash, range, &err);
                            if range.uncovered {
                                sources.record_extra_refusal(provider, &err, range, at);
                            }
                            growth.wanted = false;
                            if let Some(lane) = lane_slot {
                                let refusals = growth.extra_refused(lane, Instant::now());
                                if refusals == EXTRA_REFUSALS_LOGGED {
                                    tracing::info!(
                                        %provider,
                                        hash = %blake3::Hash::from_bytes(hash).to_hex(),
                                        offset = range.offset,
                                        len = range.len,
                                        uncovered = range.uncovered,
                                        refusals,
                                        error = %format_args!("{err:#}"),
                                        "a node refuses a lane's extra streams; the lane is \
                                         asked less often until one serves"
                                    );
                                }
                            }
                        } else {
                            let deposit = pool_deposit(&deposit_rx, &started.0);
                            charged.insert(provider);
                            if let Fault::Fatal(_) =
                                sources.record_fault(provider, &err, Some(range), at, deposit)
                            {
                                return Err(err);
                            }
                        }
                    }
                    progress_wake.notify_waiters();
                }
                Some((provider, built)) = connecting.next(), if !connecting.is_empty() => {
                    // Every other build that already finished lands in this batch
                    // too, so lanes that build together are planned together.
                    let mut builds = vec![(provider, built)];
                    while let Some(Some(more)) = connecting.next().now_or_never() {
                        builds.push(more);
                    }
                    for (provider, built) in builds {
                        connecting_set.remove(&provider);
                        match sources.lane_built(provider, built, Instant::now()) {
                            Ok(lane) => {
                                seed_deposit(&deposit_tx, &lane);
                                ready.push((provider, lane));
                                // A new lane changes who takes a queued range.
                                growth.wanted = true;
                            }
                            // A build fault is chain-side and retries with
                            // backoff, unless it may have escrowed USDC no
                            // record credits: a retry escrows again.
                            Err(err) => {
                                if let Fault::Fatal(_) = crate::fault::classify(&err) {
                                    return Err(err);
                                }
                                tracing::debug!(%provider, error = %decdn_common::redact::sanitize_err_chain(&err), "lane build failed");
                            }
                        }
                    }
                }
                found = poll_opt(&mut discovering), if discovering.is_some() => {
                    discovering = None;
                    let deposit = pool_deposit(&deposit_rx, &started.0);
                    sources.discovery_done(found, Instant::now(), deposit);
                }
                arrived = next_opt(&mut arrivals), if arrivals.is_some() => {
                    match arrived {
                        Some(holder) => {
                            tracing::debug!(
                                provider = %holder.provider,
                                rtt_ms = holder.rtt_ms,
                                probed_holder = holder.probed_holder,
                                "a late holder joined"
                            );
                            sources.holders_arrived(vec![holder]);
                            growth.wanted = true;
                        }
                        None => arrivals = None,
                    }
                }
                () = sleep_until_opt(wake) => {}
                Ok(()) = deposit_rx.changed() => {}
                // A leg proved a size: the loop top clips the work to it.
                () = bound_moved.notified() => {}
                flushed = poll_opt(&mut record), if record.is_some() => {
                    record = None;
                    flushed?;
                }
                _ = flush.tick() => {
                    if record.is_none() {
                        record = Some(store.flush_present_record());
                    } else {
                        tracing::debug!(
                            hash = %blake3::Hash::from_bytes(hash).to_hex(),
                            "a present-record write is still in flight; this tick skips its \
                             write"
                        );
                    }
                }
                gave_up = env.stop.expired() => {
                    return Err(anyhow::Error::new(gave_up));
                }
            }
        }
    }
    .await;
    // Stop every lane first, so no paid leg sits open while the builds drain.
    drop(workers);
    drop(discovering);
    drop(arrivals);
    // An error exit can leave a record write in flight: land it before the
    // flush below, so it cannot rename an older snapshot over that one.
    if let Some(pending) = record.take()
        && let Err(err) = pending.await
    {
        tracing::warn!("a periodic present-record write failed as the fetch ended: {err:#}");
    }
    // Every error exit keeps the bytes that landed recorded for a resume.
    let result = match result {
        Err(err) => Err(flushed(store, err).await),
        ok => ok,
    };
    drain_builds(&mut connecting).await;
    result
}

/// How long [`acquire`] waits, as it returns, for lane builds still in flight.
const BUILD_DRAIN: Duration = Duration::from_secs(30);

/// Await every lane build still in `connecting`, for at most [`BUILD_DRAIN`].
/// A build can be in the middle of an on-chain top-up; dropping it there
/// would escrow funds its caller never records. A lane that builds here never
/// starts and drops at once.
async fn drain_builds<S>(connecting: &mut FuturesUnordered<Connecting<'_, S>>) {
    if connecting.is_empty() {
        return;
    }
    let drained = tokio::time::timeout(BUILD_DRAIN, async {
        while let Some((provider, built)) = connecting.next().await {
            if let Err(err) = built {
                tracing::debug!(
                    %provider,
                    error = %decdn_common::redact::sanitize_err_chain(&err),
                    "a lane build that ended after the fetch failed"
                );
            }
        }
    })
    .await;
    if drained.is_err() {
        tracing::warn!(
            builds = connecting.len(),
            "lane builds still running {BUILD_DRAIN:?} after the fetch ended are dropped"
        );
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation
)] // tests
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use alloy::primitives::{Address, B256, U256};
    use decdn_incentive::{DepositOutcome, LaneKey};
    use decdn_protocol::client::StreamError;
    use decdn_protocol::{Coverage, DISCOVERY_BLOCK_BYTES, num_blocks};

    use super::{AcquireEnv, AcquireTarget, ConsumptionPacing, LANE_WATCHDOG, LaneLease, acquire};
    use crate::driver::{DriveConfig, PoolExhausted, ranges_content_len};
    use crate::fault::{FatalScope, Fault, classify};
    use crate::health::{Health, PeerHealth};
    use crate::ledgers::{LaneHandle, LaneLedgers};
    use crate::pacer::{BudgetPacer, PaceDecision, PaceState, Pacer};
    use crate::source::{BlobSource, FakeFunder, Funder, ScriptedSource, ctx_with};
    use crate::source_set::{NoAffordableSource, NoSourceHasBlob, SourceSet, StaticSources};
    use crate::stop::{GaveUp, StopPolicy};
    use crate::streamer::StreamCandidate;
    use crate::{
        ClientRangedStore, Cumulative, PoolContext, PoolLedger, UpstreamRefused,
        UpstreamVoucherRejected,
    };
    use decdn_bao_range::RangedStore;

    /// A deterministic blob of `len` bytes: the same synth as the `source` unit
    /// tests, so a `ScriptedSource` over it yields verifiable wire.
    fn blob(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// A shared handle to a healthy buyer context (huge deposit so the pacer never
    /// tops up) paying the given `provider`.
    fn ctx_for(provider: u8) -> Arc<Mutex<PoolContext>> {
        Arc::new(Mutex::new(ctx_with(provider, U256::from(u128::MAX))))
    }

    /// One paid source for [`StaticSources`]: `source` paying `ledger`, with a
    /// context pinned to `provider` and a huge deposit. Hand it a clone of a
    /// [`ScriptedSource`] and keep the original: clones share the opened and
    /// delivered counters the assertions read.
    fn candidate<S>(
        source: S,
        ledger: Arc<PoolLedger>,
        provider: u8,
        coverage: Option<Coverage>,
    ) -> StreamCandidate<S> {
        candidate_ctx(source, ledger, ctx_for(provider), coverage)
    }

    /// [`candidate`] with an explicit pool context.
    fn candidate_ctx<S>(
        source: S,
        ledger: Arc<PoolLedger>,
        ctx: Arc<Mutex<PoolContext>>,
        coverage: Option<Coverage>,
    ) -> StreamCandidate<S> {
        StreamCandidate {
            source,
            ctx,
            ledger,
            coverage,
            lease: LaneLease::new(()),
            widen: None,
        }
    }

    fn drive_config() -> DriveConfig {
        DriveConfig {
            working_deposit: U256::ZERO,
            seller_reserve: U256::ZERO,
            max_settle_waits: 0,
            settle_backoff: Duration::from_millis(1),
        }
    }

    fn no_topups() -> FakeFunder {
        FakeFunder::new(0, DepositOutcome::Added(U256::ZERO))
    }

    /// A `.partial` store whose tempdir path is returned so the test can read
    /// the finalized blob back off disk.
    fn fresh_store(root: [u8; 32], total: u64) -> (ClientRangedStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tmp dir");
        let store = ClientRangedStore::create(dir.path(), "b", root, total).expect("create");
        (store, dir)
    }

    /// Everything [`run_acquire_with`] takes beyond the target and the sources.
    struct Knobs<'a> {
        max_lanes: usize,
        give_up_after: Option<Duration>,
        on_progress: Option<&'a (dyn Fn(u64, u64) + Send + Sync)>,
        ledgers: Option<&'a LaneLedgers>,
        pacing: Option<&'a ConsumptionPacing<'a>>,
        health: Arc<PeerHealth>,
        max_blob_bytes: u64,
    }

    impl Knobs<'_> {
        fn lanes(max_lanes: usize) -> Self {
            Self {
                max_lanes,
                give_up_after: None,
                on_progress: None,
                ledgers: None,
                pacing: None,
                health: Arc::default(),
                max_blob_bytes: 0,
            }
        }
    }

    /// Acquire the whole `root` blob from `provider`'s candidates.
    async fn run_acquire_with<S: BlobSource>(
        store: &ClientRangedStore,
        provider: &StaticSources<S>,
        root: [u8; 32],
        pacer: &impl Pacer,
        funder: &impl Funder,
        knobs: Knobs<'_>,
    ) -> anyhow::Result<()> {
        let total = store.total_bytes();
        let mut set = SourceSet::new(provider, root, knobs.health, provider.holders());
        let stop = StopPolicy::new(
            false,
            knobs.give_up_after.or(Some(Duration::from_hours(1))),
            Arc::default(),
        );
        let drive = drive_config();
        acquire(
            AcquireTarget {
                store,
                hash: root,
                total_bytes: total,
                ranges: &[(0, total)],
            },
            &mut set,
            &AcquireEnv {
                pacer,
                funder,
                drive: &drive,
                max_lanes: knobs.max_lanes,
                stop: &stop,
                on_progress: knobs.on_progress,
                ledgers: knobs.ledgers,
                pacing: knobs.pacing,
                max_blob_bytes: knobs.max_blob_bytes,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_acquire(
        store: &ClientRangedStore,
        provider: &StaticSources<ScriptedSource>,
        root: [u8; 32],
        total: u64,
        pacer: &impl Pacer,
        funder: &impl Funder,
        max_lanes: usize,
        give_up_after: Option<Duration>,
    ) -> anyhow::Result<()> {
        assert_eq!(store.total_bytes(), total);
        run_acquire_with(
            store,
            provider,
            root,
            pacer,
            funder,
            Knobs {
                give_up_after,
                ..Knobs::lanes(max_lanes)
            },
        )
        .await
    }

    #[tokio::test]
    async fn two_sources_fetch_large_blob_byte_identical() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        // Each source pays its OWN lane (its own provider + ledger); one shared
        // pool deposit backs both. Both hold the whole blob.
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(src_a.clone(), ledger_a, 0xA1, None),
            candidate(src_b.clone(), ledger_b, 0xB2, None),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await?;

        store.finalize().await?;

        // Whole blob present and byte-identical.
        let got = std::fs::read(dir.path().join("b"))?;
        assert_eq!(got.len(), data.len(), "assembled length matches");
        assert_eq!(got, data, "assembled bytes byte-identical to source blob");

        // Both sources actually contributed (work was parallelized, not all
        // from one).
        assert!(
            src_a.opened_bytes() > 0 && src_b.opened_bytes() > 0,
            "both sources must have served work: a={} b={}",
            src_a.opened_bytes(),
            src_b.opened_bytes()
        );
        // Coverage: at least the whole blob's bytes were opened across the set
        // (a tail-steal boundary may cause a bounded, idempotent re-fetch).
        assert!(
            src_a.opened_bytes() + src_b.opened_bytes() >= data.len() as u64,
            "the two sources together must cover the whole blob"
        );
        Ok(())
    }

    /// The delivery progress the bar reads is ONE monotonic whole-blob position,
    /// not each lane's divergent local `base_present + received`. Two concurrent
    /// full holders each split the blob and report through the SAME callback; the
    /// callback records every position it is handed. A progress bar must never go
    /// backwards, so the recorded sequence must be non-decreasing and end at the
    /// whole-blob size.
    #[tokio::test]
    async fn progress_positions_are_monotonic_across_lanes() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, _dir) = fresh_store(root, total);

        let samples: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let cb_samples = Arc::clone(&samples);
        let on_progress: Box<crate::ProgressCallback> = Box::new(move |received, expected| {
            if let Ok(mut s) = cb_samples.lock() {
                s.push((received, expected));
            }
        });

        let provider = StaticSources::new(vec![
            candidate(src_a, ledger_a, 0xA1, None),
            candidate(src_b, ledger_b, 0xB2, None),
        ])?;
        run_acquire_with(
            &store,
            &provider,
            root,
            &BudgetPacer::new(),
            &no_topups(),
            Knobs {
                on_progress: Some(&on_progress),
                ..Knobs::lanes(4)
            },
        )
        .await?;

        let samples = samples.lock().expect("samples lock").clone();
        assert!(
            !samples.is_empty(),
            "progress callback must fire at least once"
        );
        // The bar can never move backwards: every reported position is >= the one
        // before it, against a stable whole-blob total.
        let mut prev = 0u64;
        for (received, expected) in &samples {
            assert_eq!(
                *expected, total,
                "the progress total must be the whole-blob size"
            );
            assert!(
                *received >= prev,
                "progress regressed: {received} after {prev}: the bar jumped backwards"
            );
            assert!(
                *received <= total,
                "progress overshot the blob size: {received} > {total}"
            );
            prev = *received;
        }
        // And it reaches the whole blob by the end.
        assert_eq!(
            prev, total,
            "the final reported position must reach the blob size"
        );
        Ok(())
    }

    /// A resumed fetch surfaces the already-present base on the bar BEFORE any
    /// lane opens a channel: the first reported position is the held prefix's
    /// content length, not `0`.
    #[tokio::test]
    async fn resume_base_is_reported_before_the_first_chunk() -> anyhow::Result<()> {
        use decdn_bao_range::{CHUNK_GROUP_BYTES, align_range};

        let total = 8 * CHUNK_GROUP_BYTES;
        let data = blob(total as usize);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, _dir) = fresh_store(root, total);

        // Seed a two-group prefix through the real verified ingest path, using a
        // throwaway source so the lane sources' opened-byte logs stay clean.
        let held = align_range(0, 2 * CHUNK_GROUP_BYTES, total).expect("align held");
        let seed = ScriptedSource::new(data.clone())?;
        let (h, reader) = seed.open(root, held.clone()).await?;
        store
            .ingest_stream(&held, reader, None, h.total_bytes)
            .await?;
        let base_present = ranges_content_len(&store.present_ranges().await?, total);
        assert_eq!(
            base_present,
            2 * CHUNK_GROUP_BYTES,
            "scenario: two groups held"
        );

        let samples: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let cb_samples = Arc::clone(&samples);
        let on_progress: Box<crate::ProgressCallback> = Box::new(move |received, expected| {
            if let Ok(mut s) = cb_samples.lock() {
                s.push((received, expected));
            }
        });

        let provider = StaticSources::new(vec![
            candidate(src_a, ledger_a, 0xA1, None),
            candidate(src_b, ledger_b, 0xB2, None),
        ])?;
        run_acquire_with(
            &store,
            &provider,
            root,
            &BudgetPacer::new(),
            &no_topups(),
            Knobs {
                on_progress: Some(&on_progress),
                ..Knobs::lanes(4)
            },
        )
        .await?;

        let samples = samples.lock().expect("samples lock").clone();
        let first = *samples.first().expect("at least one progress sample");
        assert_eq!(
            first,
            (base_present, total),
            "the first reported position must be the resume base, emitted before \
             any lane opens a channel"
        );
        Ok(())
    }

    /// Fan-out geometry (#1506): a large multi-block gap across N full holders
    /// seeds ~N contiguous spans (one per holder), never `blocks × N`. Every
    /// holder still contributes.
    ///
    /// The check reads each holder's FIRST open, which is its seeded span: every
    /// worker picks from `pending` before any worker can finish. Later opens are
    /// tail steals and requeues, whose count depends on how the lanes interleave.
    #[tokio::test]
    async fn large_multi_block_gap_seeds_about_one_span_per_holder() -> anyhow::Result<()> {
        let total = 3 * DISCOVERY_BLOCK_BYTES;
        let data = blob(total as usize);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_c = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let src_c = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_c));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(src_a.clone(), ledger_a, 0xA1, None),
            candidate(src_b.clone(), ledger_b, 0xB2, None),
            candidate(src_c.clone(), ledger_c, 0xC3, None),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await?;
        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical across the fan-out"
        );
        // N = 3 spans of 64 MiB, one per holder.
        for (name, src) in [("a", &src_a), ("b", &src_b), ("c", &src_c)] {
            let first = src.opened_ranges().first().copied();
            assert!(
                first.is_some_and(|(_, len)| len >= total / 4),
                "fan-out seeds ~N contiguous spans, not blocks × N: holder {name}'s \
                 first open is {first:?} of a {total}-byte blob"
            );
        }
        assert!(
            src_a.opened_bytes() > 0 && src_b.opened_bytes() > 0 && src_c.opened_bytes() > 0,
            "every holder contributes: a={} b={} c={}",
            src_a.opened_bytes(),
            src_b.opened_bytes(),
            src_c.opened_bytes()
        );
        Ok(())
    }

    /// Build a `Coverage` sized for `n` discovery blocks with exactly `blocks`
    /// covered: the same shorthand `coverage_plan`'s own tests use.
    fn cov(n: u32, blocks: &[u32]) -> Coverage {
        Coverage::from_block_indices(n, blocks.iter().copied())
    }

    /// A lane's coverage logs as inclusive block runs, a lone block as itself.
    #[test]
    fn coverage_logs_as_block_runs() {
        assert_eq!(
            super::block_runs(&cov(70, &[0, 1, 2, 5, 42, 43, 67])),
            "0-2,5,42-43,67"
        );
        assert_eq!(super::block_runs(&cov(4, &[])), "none");
        assert_eq!(super::block_runs(&Coverage::full(3)), "0-2");
    }

    /// Disjoint coverage (#1506): source A holds only discovery block 0, source
    /// B holds only block 1. Every byte range A opens falls inside block 0 and
    /// every range B opens falls inside block 1: neither is EVER handed the
    /// other's block.
    #[tokio::test]
    async fn disjoint_coverage_routes_each_block_to_its_only_coverer() -> anyhow::Result<()> {
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let data = blob(total as usize);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);

        let n = num_blocks(total);
        let provider = StaticSources::new(vec![
            candidate(src_a.clone(), ledger_a, 0xA1, Some(cov(n, &[0]))),
            candidate(src_b.clone(), ledger_b, 0xB2, Some(cov(n, &[1]))),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical from two disjoint-coverage sources"
        );

        assert!(
            src_a
                .opened_ranges()
                .iter()
                .all(|&(s, l)| s + l <= DISCOVERY_BLOCK_BYTES),
            "source A covers only block 0 and must never be opened past it: {:?}",
            src_a.opened_ranges()
        );
        assert!(
            src_b
                .opened_ranges()
                .iter()
                .all(|&(s, _)| s >= DISCOVERY_BLOCK_BYTES),
            "source B covers only block 1 and must never be opened before it: {:?}",
            src_b.opened_ranges()
        );
        assert!(src_a.opened_bytes() > 0, "A must have served block 0");
        assert!(src_b.opened_bytes() > 0, "B must have served block 1");
        Ok(())
    }

    /// A third, all-ones holder serves the one block neither of the two
    /// partial holders covers (#1506). A holds only block 0, B holds only
    /// block 1, and only O (full coverage) can serve block 2, so O, and only O,
    /// opens bytes in block 2.
    #[tokio::test]
    async fn a_block_only_the_all_ones_source_covers_is_served_by_it() -> anyhow::Result<()> {
        let total = 3 * DISCOVERY_BLOCK_BYTES;
        let data = blob(total as usize);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_o = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let src_o = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_o));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);

        let n = num_blocks(total);
        let provider = StaticSources::new(vec![
            candidate(src_a.clone(), ledger_a, 0xA1, Some(cov(n, &[0]))),
            candidate(src_b.clone(), ledger_b, 0xB2, Some(cov(n, &[1]))),
            candidate(src_o.clone(), ledger_o, 0xC3, Some(Coverage::full(n))),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical with a partial-coverage trio"
        );

        let block2_start = 2 * DISCOVERY_BLOCK_BYTES;
        assert!(
            src_o
                .opened_ranges()
                .iter()
                .any(|&(s, l)| s < total && s + l > block2_start),
            "only O covers block 2, so O must be the one that opened it: {:?}",
            src_o.opened_ranges()
        );
        assert!(
            src_a.opened_ranges().iter().all(|&(s, _)| s < block2_start),
            "A does not cover block 2 and must never open into it: {:?}",
            src_a.opened_ranges()
        );
        assert!(
            src_b.opened_ranges().iter().all(|&(s, _)| s < block2_start),
            "B does not cover block 2 and must never open into it: {:?}",
            src_b.opened_ranges()
        );
        Ok(())
    }

    /// Coverage-constrained steal (#1506): a fast source that covers ONLY
    /// block 0 finishes its own segment quickly, while the block-1-only holder
    /// is slow to start. The only range left in flight (block 1) is outside the
    /// fast source's coverage, so it PARKS instead of stealing work it cannot
    /// serve. The slow source still finishes block 1 on its own.
    #[tokio::test]
    async fn coverage_constrained_steal_parks_instead_of_taking_uncoverable_work()
    -> anyhow::Result<()> {
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let data = blob(total as usize);
        let ledger_fast = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_slow = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_fast = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_fast));
        // Every leg the slow source opens stalls before its first byte, giving
        // the fast source (block 0 only) time to finish and attempt a steal.
        let src_slow = ScriptedSource::new(data.clone())?
            .slow_to_start(Duration::from_millis(200))
            .paying(Arc::clone(&ledger_slow));
        let root = src_fast.root();
        let (store, dir) = fresh_store(root, total);

        let n = num_blocks(total);
        let provider = StaticSources::new(vec![
            candidate(src_fast.clone(), ledger_fast, 0xA1, Some(cov(n, &[0]))),
            candidate(src_slow.clone(), ledger_slow, 0xB2, Some(cov(n, &[1]))),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            2,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical despite the fast source finding nothing to steal"
        );

        assert!(
            src_fast
                .opened_ranges()
                .iter()
                .all(|&(s, l)| s + l <= DISCOVERY_BLOCK_BYTES),
            "the fast source covers only block 0 and must never open past it: it must \
             have parked rather than stolen block 1: {:?}",
            src_fast.opened_ranges()
        );
        assert!(
            src_slow.opened_bytes() > 0,
            "the slow source must still have served its own block 1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_source_below_two_holders_still_completes() -> anyhow::Result<()> {
        // With a single source the loop degrades to one segment, no steal, and
        // still assembles the whole blob byte-identical.
        let data = blob(20 * 1024 * 1024);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger));
        let root = src.root();
        let total = src.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![candidate(src.clone(), ledger, 0xA1, None)])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await?;

        store.finalize().await?;

        let got = std::fs::read(dir.path().join("b"))?;
        assert_eq!(got, data, "single-source assembly is byte-identical");
        // The lone source opened exactly the whole blob (one segment, no steal,
        // no re-fetch).
        assert_eq!(
            src.opened_bytes(),
            data.len() as u64,
            "one source, one segment: exactly the blob's bytes opened once"
        );
        Ok(())
    }

    /// Fault reassignment: `src_a` faults after ~8 MiB of its segment;
    /// `src_b` holds the whole blob and covers the reassigned remainder. The
    /// blob still assembles byte-identical, and the faulted source's verified
    /// prefix is NOT refetched (the remainder alone is reassigned).
    #[tokio::test(start_paused = true)]
    async fn faulted_source_tail_is_reassigned_and_fetch_completes() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        // src_a returns an `Err` after 8 MiB of wire on any range longer than
        // that; its 32 MiB initial segment therefore delivers only a ~8 MiB
        // prefix then faults. This is the `fill_gap`-error arm, NOT the stall
        // watchdog. src_b is healthy.
        let src_a = ScriptedSource::new(data.clone())?
            .with_fault_after(8 * 1024 * 1024, || anyhow::anyhow!("scripted fault"))
            .paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(src_a.clone(), ledger_a, 0xA1, None),
            candidate(src_b.clone(), ledger_b, 0xB2, None),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical after reassignment"
        );

        // src_a opened its 32 MiB segment but delivered only its pre-fault prefix.
        assert!(
            src_a.opened_bytes() >= 8 * 1024 * 1024,
            "src_a opened its segment: {}",
            src_a.opened_bytes()
        );
        assert!(
            src_a.delivered_bytes() < 32 * 1024 * 1024,
            "src_a faulted, so it did NOT deliver its whole segment: {}",
            src_a.delivered_bytes()
        );
        assert!(
            src_b.opened_bytes() > 0,
            "src_b covered the reassigned tail"
        );
        // No wholesale refetch: the two together delivered about the blob size,
        // not the blob plus a re-pulled 32 MiB segment.
        let total_delivered = src_a.delivered_bytes() + src_b.delivered_bytes();
        assert!(
            total_delivered <= total + 8 * 1024 * 1024,
            "verified bytes must not be refetched: delivered {total_delivered} for a \
             {total}-byte blob"
        );
        Ok(())
    }

    /// A lane whose paced draws leave gaps shorter than the lane watchdog, and
    /// that reports no progress callback, is never reassigned: the watchdog
    /// judges the verified bytes `fill_gap` counts as each leaf lands, not the
    /// store's 4 MiB checkpoints (#2209). Each wait here (the first read, then a
    /// pause after 1 MiB) is half the watchdog, and the first watchdog sample
    /// falls while bytes are still missing.
    #[tokio::test(start_paused = true)]
    async fn a_lane_pausing_under_the_deadline_is_not_reassigned() -> anyhow::Result<()> {
        let data = blob(16 * 1024 * 1024);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(data.clone())?
            .slow_to_start(LANE_WATCHDOG / 2)
            .stall_after(1024 * 1024, LANE_WATCHDOG / 2)
            .paying(Arc::clone(&ledger));
        let root = src.root();
        let total = src.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let health: Arc<PeerHealth> = Arc::default();
        let provider = StaticSources::new(vec![candidate(src, ledger, 0xA1, None)])?;
        run_acquire_with(
            &store,
            &provider,
            root,
            &BudgetPacer::new(),
            &no_topups(),
            Knobs {
                health: Arc::clone(&health),
                ..Knobs::lanes(4)
            },
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        assert_eq!(
            health.health(Address::repeat_byte(0xA1)),
            Health::Healthy { streak: 0 },
            "the lane never tripped the watchdog"
        );
        Ok(())
    }

    /// Stamps the moment it drops, so a test can see when a lane let go of it.
    struct DropStamp(Arc<Mutex<Option<std::time::Instant>>>);

    impl Drop for DropStamp {
        fn drop(&mut self) {
            if let Ok(mut at) = self.0.lock() {
                *at = Some(std::time::Instant::now());
            }
        }
    }

    /// Every started lane's lease, the faulted lane's included, is held until
    /// the acquire returns and released by then, for lanes without a
    /// [`super::LaneWiden`]: such a lane has no way to take a stream back.
    #[tokio::test(start_paused = true)]
    async fn every_lease_is_released_when_acquire_returns() -> anyhow::Result<()> {
        let data = blob(16 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?
            .with_fault_after(1024 * 1024, || anyhow::anyhow!("scripted fault"))
            .paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, _dir) = fresh_store(root, total);
        let dropped_a = Arc::new(Mutex::new(None));
        let dropped_b = Arc::new(Mutex::new(None));
        let mut lane_a = candidate(src_a.clone(), ledger_a, 0xA1, None);
        lane_a.lease = LaneLease::new(DropStamp(Arc::clone(&dropped_a)));
        let mut lane_b = candidate(src_b.clone(), ledger_b, 0xB2, None);
        lane_b.lease = LaneLease::new(DropStamp(Arc::clone(&dropped_b)));
        let provider = StaticSources::new(vec![lane_a, lane_b])?;
        // Past 12 MiB of the 16 MiB blob, lane a (which faults 1 MiB into every
        // open) has faulted; its lease must still be held.
        let a_released_mid_fetch: Arc<Mutex<Vec<bool>>> = Arc::default();
        let on_progress = {
            let seen = Arc::clone(&a_released_mid_fetch);
            let dropped_a = Arc::clone(&dropped_a);
            move |position: u64, _total: u64| {
                if position >= 12 * 1024 * 1024
                    && let (Ok(mut seen), Ok(at)) = (seen.lock(), dropped_a.lock())
                {
                    seen.push(at.is_some());
                }
            }
        };
        run_acquire_with(
            &store,
            &provider,
            root,
            &BudgetPacer::new(),
            &no_topups(),
            Knobs {
                on_progress: Some(&on_progress),
                ..Knobs::lanes(4)
            },
        )
        .await?;
        let ended = std::time::Instant::now();
        let seen = a_released_mid_fetch.lock().expect("seen lock").clone();
        assert!(!seen.is_empty(), "progress passed lane a's fault point");
        assert!(
            seen.iter().all(|released| !released),
            "the faulted lane's lease is held until the acquire returns"
        );
        assert!(
            src_a.delivered_bytes() > 0 && src_a.delivered_bytes() < total,
            "lane a delivered a prefix and faulted"
        );
        let at_a = dropped_a
            .lock()
            .expect("stamp lock")
            .expect("lane a let go");
        let at_b = dropped_b
            .lock()
            .expect("stamp lock")
            .expect("lane b let go");
        assert!(
            at_a <= ended && at_b <= ended,
            "both lanes let go before the acquire returns"
        );
        assert!(
            src_b.delivered_bytes() > src_a.delivered_bytes(),
            "lane b fetched the reassigned tail after lane a faulted"
        );
        Ok(())
    }

    /// An acquire that its caller drops releases each lane's lease with it
    /// rather than leaving it held for the caller.
    #[tokio::test]
    async fn a_dropped_acquire_releases_every_lease() -> anyhow::Result<()> {
        let data = blob(8 * 1024 * 1024);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(data)?
            .stall_after(0, Duration::from_hours(1))
            .paying(Arc::clone(&ledger));
        let root = src.root();
        let total = src.total_bytes();
        let (store, _dir) = fresh_store(root, total);
        let dropped = Arc::new(Mutex::new(None));
        let mut lane = candidate(src, ledger, 0xA1, None);
        lane.lease = LaneLease::new(DropStamp(Arc::clone(&dropped)));
        let provider = StaticSources::new(vec![lane])?;
        let (pacer, funder) = (BudgetPacer::new(), no_topups());
        let fetch = run_acquire(&store, &provider, root, total, &pacer, &funder, 4, None);
        tokio::select! {
            fetched = fetch => panic!("a wedged lane cannot finish: {fetched:?}"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        assert!(
            dropped.lock().expect("stamp lock").is_some(),
            "the dropped acquire's lease is released"
        );
        Ok(())
    }

    /// A parked lane keeps every `pending` entry: a cooling lane may return,
    /// and a rediscovered holder may cover what no live lane does.
    #[test]
    fn park_keeps_every_pending_entry() -> anyhow::Result<()> {
        use std::collections::VecDeque;

        use decdn_bao_range::align_range;

        use super::Work;

        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let in_block0 = align_range(0, 1, total)?;
        let in_block1 = align_range(DISCOVERY_BLOCK_BYTES, 1, total)?;
        let mut work = Work::new(VecDeque::from(vec![in_block0, in_block1]), false);
        let a = work.add_lane(Some(cov(2, &[0])), total);
        let b = work.add_lane(Some(cov(2, &[1])), total);

        work.park(a);
        work.park(b);
        let starts: Vec<u64> = work
            .pending
            .iter()
            .map(decdn_bao_range::AlignedRange::fetch_start)
            .collect();
        assert_eq!(
            starts,
            vec![0, DISCOVERY_BLOCK_BYTES],
            "every entry stays queued"
        );
        assert!(work.uncovered(total), "no running lane covers the queue");
        work.revive(a);
        assert!(work.uncovered(total), "block 1 still has no running lane");
        work.revive(b);
        assert!(!work.uncovered(total));
        Ok(())
    }

    /// A steal trims the victim but does not cancel it: the stealer cancels
    /// only after it confirms the stolen tail still has missing bytes, and
    /// only while the victim still runs the unit the steal trimmed. A second
    /// steal from the same victim does not hide the first stealer's cancel,
    /// and a victim that has moved on to a new unit keeps it.
    #[tokio::test]
    async fn steal_cancels_the_victim_only_through_cancel_victim() -> anyhow::Result<()> {
        use std::collections::VecDeque;
        use std::sync::atomic::Ordering;

        use super::{CancelHandle, Work};

        let total = DISCOVERY_BLOCK_BYTES;
        let coverage = cov(1, &[0]);
        let fresh_work = || Work {
            pending: VecDeque::new(),
            in_flight: vec![Some((0, total)), None, None],
            cancel: (0..3).map(|_| Arc::new(CancelHandle::new())).collect(),
            alive: vec![true, true, true],
            units: vec![1, 0, 0],
            coverage: vec![coverage.clone(), coverage.clone(), coverage.clone()],
            measured: vec![true; 3],
            lane_of: (0..3).collect(),
            extra: vec![false; 3],
            providers: vec![None; 3],
            no_uncovered: vec![false; 3],
            front_first: false,
        };
        // Nothing is present: every byte in flight is missing.
        let all = [(0, total)];
        let victim_flag = |w: &Work| {
            w.cancel
                .first()
                .is_some_and(|h| h.flag.load(Ordering::Acquire))
        };

        // A single steal: `pick` trims but does not cancel; `cancel_victim` does.
        let mut work = fresh_work();
        let first = work
            .pick(1, total, &coverage, true, &all)?
            .ok_or_else(|| anyhow::anyhow!("worker 1 must steal worker 0's tail"))?;
        let (victim, unit) = first
            .victim
            .ok_or_else(|| anyhow::anyhow!("a steal must name its victim"))?;
        assert_eq!(victim, 0);
        assert_eq!(
            work.in_flight.first().copied().flatten(),
            Some((0, first.range.fetch_start()))
        );
        assert!(!victim_flag(&work), "pick alone must not cancel the victim");
        work.cancel_victim(victim, unit);
        assert!(
            victim_flag(&work),
            "the victim must be cancelled at the split"
        );

        // Two steals from one victim before the first stealer cancels: the
        // second trim leaves the victim on the same unit, so the first
        // stealer's cancel still fires even if the second stealer skips its own.
        let mut work = fresh_work();
        let first = work
            .pick(1, total, &coverage, true, &all)?
            .ok_or_else(|| anyhow::anyhow!("worker 1 must steal"))?;
        let (v1, u1) = first
            .victim
            .ok_or_else(|| anyhow::anyhow!("first steal names its victim"))?;
        // A second stealer trims the victim again, to the first quarter.
        *work.slot_mut(v1)? = Some((0, first.range.fetch_start() / 2));
        work.cancel_victim(v1, u1);
        assert!(
            victim_flag(&work),
            "a second trim must not hide the first stealer's cancel"
        );

        // The victim finished and started a new unit: a late cancel must not hit it.
        let mut work = fresh_work();
        let stolen = work
            .pick(1, total, &coverage, true, &all)?
            .ok_or_else(|| anyhow::anyhow!("worker 1 must steal"))?;
        let (victim, unit) = stolen
            .victim
            .ok_or_else(|| anyhow::anyhow!("a steal must name its victim"))?;
        work.clear(victim)?;
        work.pending
            .push_back(decdn_bao_range::align_range(0, 1, total)?);
        work.pick(victim, total, &coverage, true, &all)?
            .ok_or_else(|| anyhow::anyhow!("the victim must pick its new unit"))?;
        work.cancel_victim(victim, unit);
        assert!(
            !victim_flag(&work),
            "a victim on a new unit must not be cancelled"
        );

        // The victim finished and holds nothing: nothing to cancel.
        work.clear(victim)?;
        work.cancel_victim(victim, unit);
        assert!(!victim_flag(&work), "an idle victim must not be cancelled");
        Ok(())
    }

    /// THE double-pay test: a fast source and a held-back one over a 64 MiB
    /// blob, arranged so a steal DEFINITELY fires: every leg of the slow source
    /// waits before its first byte until the test sees the fast source open a
    /// second range, which is the steal of the slow source's tail. With
    /// steal-cancellation the stolen tail is fetched by exactly ONE source, so
    /// total delivered ≈ the blob size.
    #[tokio::test]
    async fn forced_steal_does_not_double_fetch_the_stolen_tail() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let ledger_fast = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_slow = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_fast = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_fast));
        // The slow source delivers nothing until the gate opens, so the fast
        // source always finishes its own 32 MiB first and steals, whatever the
        // machine's speed.
        let (gate, gated) = tokio::sync::watch::channel(false);
        let src_slow = ScriptedSource::new(data.clone())?
            .gated_on(gated)
            .paying(Arc::clone(&ledger_slow));
        let root = src_fast.root();
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(src_fast.clone(), ledger_fast, 0xA1, None),
            candidate(src_slow.clone(), ledger_slow, 0xB2, None),
        ])?;
        let (pacer, funder) = (BudgetPacer::new(), no_topups());
        let fetch = run_acquire(&store, &provider, root, total, &pacer, &funder, 2, None);
        // Open the gate once the fast source has opened a second range: its
        // steal of the slow source's tail.
        let release = async {
            while src_fast.opened_ranges().len() < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            gate.send_replace(true);
        };
        let (fetched, ()) = tokio::time::timeout(
            Duration::from_mins(5),
            futures_util::future::join(fetch, release),
        )
        .await
        .map_err(|_| anyhow::anyhow!("the fetch or the steal never happened"))?;
        fetched?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical despite the forced steal"
        );

        // The steal fired: the fast source delivered strictly more than its own
        // 32 MiB initial segment (it took over the slow source's tail).
        assert!(
            src_fast.delivered_bytes() > 32 * 1024 * 1024,
            "the fast source must have stolen work beyond its own segment: fast={}",
            src_fast.delivered_bytes()
        );
        // THE no-double-pay assertion: total bytes fetched across BOTH sources is
        // within a small bounded slop of the blob size.
        let total_delivered = src_fast.delivered_bytes() + src_slow.delivered_bytes();
        assert!(
            total_delivered <= total + 8 * 1024 * 1024,
            "the stolen tail must NOT be fetched twice: delivered {total_delivered} \
             (fast={}, slow={}) for a {total}-byte blob",
            src_fast.delivered_bytes(),
            src_slow.delivered_bytes()
        );
        Ok(())
    }

    /// Admit `[start, start + len)` of `data` into `store`, as a leg that
    /// delivered it would: the frontier a steal reads.
    async fn admit_prefix(
        store: &ClientRangedStore,
        data: &[u8],
        start: u64,
        len: u64,
    ) -> anyhow::Result<()> {
        let range = decdn_bao_range::align_range(start, len, data.len() as u64)?;
        let outboard = bao_tree::io::outboard::PreOrderMemOutboard::create(
            data,
            decdn_bao_range::IROH_BLOCK_SIZE,
        );
        let slice = data
            .get(usize::try_from(range.fetch_start())?..usize::try_from(range.fetch_end())?)
            .ok_or_else(|| anyhow::anyhow!("admit range out of bounds"))?;
        let bao = decdn_bao_range::encode_verified_range(
            store.root(),
            &range,
            slice,
            outboard.data.into(),
        )?;
        store.admit(range, bao).await?;
        Ok(())
    }

    /// A steal after the victim delivered part of its range: two gated full
    /// holders of a 128 MiB blob each open one half. Once both have opened,
    /// the victim's range is made present up to `frontier_of(start, len)`,
    /// then the thief is released: it finishes its own half and may steal.
    /// The thief's legs stall 1 s before their first byte, so a victim that
    /// steals back does so before the thief's steal leg delivers.
    /// `victim_release` opens the victim's gate. Returns the thief's and the
    /// victim's opens and the victim's first range and frontier.
    async fn steal_after_victim_frontier(
        frontier_of: impl Fn(u64, u64) -> u64,
        victim_release: impl AsyncFn(
            &ScriptedSource,
            &ScriptedSource,
            &tokio::sync::watch::Sender<bool>,
        ),
    ) -> anyhow::Result<StealRun> {
        let data = blob(128 * MIB as usize);
        let lt = Arc::new(PoolLedger::new(Cumulative::default()));
        let lv = Arc::new(PoolLedger::new(Cumulative::default()));
        let (thief_gate, thief_held) = tokio::sync::watch::channel(false);
        let (victim_gate, victim_held) = tokio::sync::watch::channel(false);
        let thief = ScriptedSource::new(data.clone())?
            .gated_on(thief_held)
            .slow_to_start(Duration::from_secs(1))
            .paying(Arc::clone(&lt));
        let victim = ScriptedSource::new(data.clone())?
            .gated_on(victim_held)
            .paying(Arc::clone(&lv));
        let (root, total) = (thief.root(), thief.total_bytes());
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(thief.clone(), lt, 0xA1, None),
            candidate(victim.clone(), lv, 0xB2, None),
        ])?;
        let (pacer, funder) = (BudgetPacer::new(), no_topups());
        let fetch = run_acquire(&store, &provider, root, total, &pacer, &funder, 2, None);
        let script = async {
            while thief.opened_ranges().is_empty() || victim.opened_ranges().is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let (start, len) = victim
                .opened_ranges()
                .first()
                .copied()
                .ok_or_else(|| anyhow::anyhow!("the victim opened a range"))?;
            let frontier = frontier_of(start, len);
            admit_prefix(&store, &data, start, frontier - start).await?;
            thief_gate.send_replace(true);
            victim_release(&thief, &victim, &victim_gate).await;
            anyhow::Ok((start, len, frontier))
        };
        let (fetched, scripted) = tokio::time::timeout(
            Duration::from_mins(5),
            futures_util::future::join(fetch, script),
        )
        .await
        .map_err(|_| anyhow::anyhow!("the fetch or the script stalled"))?;
        fetched?;
        let (start, len, frontier) = scripted?;
        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical after the steal"
        );
        Ok(StealRun {
            thief: thief.opened_ranges(),
            victim: victim.opened_ranges(),
            victim_range: (start, len),
            frontier,
        })
    }

    /// What [`steal_after_victim_frontier`] observed.
    struct StealRun {
        thief: Vec<(u64, u64)>,
        victim: Vec<(u64, u64)>,
        victim_range: (u64, u64),
        frontier: u64,
    }

    /// A steal from a victim that delivered past its picked midpoint splits
    /// the victim's MISSING remainder: the thief takes its second half, the
    /// victim keeps the first half, and no byte after the steal is opened by
    /// both lanes (no steal ping-pong).
    #[tokio::test(start_paused = true)]
    async fn a_steal_splits_the_victims_missing_remainder() -> anyhow::Result<()> {
        let run = steal_after_victim_frontier(
            |start, len| start + len / 8 * 5,
            async |_, victim, gate| open_when(|| victim.opened_ranges().len() >= 2, gate).await,
        )
        .await?;
        let (start, len) = run.victim_range;
        let end = start + len;
        let split = run.frontier + (end - run.frontier) / 2;
        assert_eq!(
            run.thief.get(1).copied(),
            Some((split, end - split)),
            "the thief steals the second half of the victim's missing remainder: {run:?}",
            run = (&run.thief, &run.victim, run.frontier)
        );
        assert_eq!(
            run.victim.get(1).copied(),
            Some((run.frontier, split - run.frontier)),
            "the victim keeps the first half of its missing remainder: {run:?}",
            run = (&run.thief, &run.victim, run.frontier)
        );
        let later: Vec<(u64, u64)> = run
            .thief
            .iter()
            .skip(1)
            .chain(run.victim.iter().skip(1))
            .copied()
            .collect();
        for (k, &(s1, l1)) in later.iter().enumerate() {
            for &(s2, l2) in later.iter().skip(k + 1) {
                assert!(
                    s1 + l1 <= s2 || s2 + l2 <= s1,
                    "a byte after the steal is opened twice: {later:?}"
                );
            }
        }
        Ok(())
    }

    /// A victim whose missing remainder is below the split floor is not
    /// stolen from, however long its picked range: the thief parks and the
    /// victim finishes its own range.
    #[tokio::test(start_paused = true)]
    async fn a_victim_with_a_small_missing_remainder_is_not_stolen_from() -> anyhow::Result<()> {
        let run = steal_after_victim_frontier(
            |start, len| start + len / 16 * 15,
            async |thief, _, gate| {
                while thief.delivered_bytes() < 64 * MIB {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                // Every task idles before virtual time moves on, so the thief
                // has picked by the time this sleep ends.
                tokio::time::sleep(Duration::from_millis(100)).await;
                gate.send_replace(true);
            },
        )
        .await?;
        assert_eq!(
            run.thief.len(),
            1,
            "the thief must not steal a 4 MiB remainder: {:?}",
            run.thief
        );
        assert_eq!(
            run.victim.len(),
            1,
            "the victim keeps its range: {:?}",
            run.victim
        );
        Ok(())
    }

    /// The multi-thread variant of the no-double-pay guard, deterministically
    /// hitting the completed-but-uncleared window on a real 2-thread runtime.
    /// `src_slow_finish` delivers its whole segment then stalls INSIDE `finish`,
    /// so its completed range stays in `in_flight` for the whole stall.
    /// `src_stealer` starts late, finishes its own segment, and (with `pending`
    /// empty) steals that completed, fully-present range. The present-bytes
    /// backstop skips it, so total fetched stays near the blob size.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forced_steal_no_double_pay_on_multi_thread_runtime() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let ledger_finish = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_stealer = Arc::new(PoolLedger::new(Cumulative::default()));
        // Delivers its segment fast, then holds it completed-but-uncleared for
        // 500 ms inside `finish`: the window a peer steals into.
        let src_slow_finish = ScriptedSource::new(data.clone())?
            .slow_finish(Duration::from_millis(500))
            .paying(Arc::clone(&ledger_finish));
        // Starts 100 ms late so the other source is already parked in `finish`
        // by the time this one frees up and steals it.
        let src_stealer = ScriptedSource::new(data.clone())?
            .slow_to_start(Duration::from_millis(100))
            .paying(Arc::clone(&ledger_stealer));
        let root = src_slow_finish.root();
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(
                src_slow_finish.clone(),
                Arc::clone(&ledger_finish),
                0xA1,
                None,
            ),
            candidate(src_stealer.clone(), ledger_stealer, 0xB2, None),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical on the multi-thread runtime"
        );

        let total_delivered = src_slow_finish.delivered_bytes() + src_stealer.delivered_bytes();
        // The backstop makes this exactly the blob size (every present-range steal
        // is skipped); without it the stolen tail is re-fetched, +8 MiB here.
        assert!(
            total_delivered <= total + 4 * 1024 * 1024,
            "a completed-but-uncleared range must not be re-fetched: delivered \
             {total_delivered} (slow_finish={}, stealer={}) for a {total}-byte blob",
            src_slow_finish.delivered_bytes(),
            src_stealer.delivered_bytes(),
        );
        // The stolen range was already present, so the steal must not cancel the
        // victim: its leg reaches `finish` and bills its own lane.
        let finish_committed = ledger_finish.committed();
        assert!(
            finish_committed.bytes > U256::ZERO,
            "a victim whose tail is already present must finish and bill its leg: \
             {finish_committed:?}"
        );
        Ok(())
    }

    /// A node that signs a size of zero for a bounded open fails the wire
    /// bound's alignment. The error is the node's, not ours: the node cools,
    /// and the other source finishes the blob.
    #[tokio::test(start_paused = true)]
    async fn a_zero_size_signed_for_a_bounded_open_cools_only_that_node() -> anyhow::Result<()> {
        let data = blob(8 * 1024 * 1024);
        let total = data.len() as u64;
        let ledger_liar = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_honest = Arc::new(PoolLedger::new(Cumulative::default()));
        let liar = ScriptedSource::new(data.clone())?
            .refusing_opens_from(0, || {
                crate::aligned_wire_len(0, 16_384, 0)
                    .err()
                    .unwrap_or_else(|| anyhow::anyhow!("a zero size must not align"))
            })
            .paying(Arc::clone(&ledger_liar));
        let honest = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_honest));
        let root = honest.root();
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(liar, ledger_liar, 0xA1, None),
            candidate(honest, ledger_honest, 0xB2, None),
        ])?;
        let health: Arc<PeerHealth> = Arc::default();
        run_acquire_with(
            &store,
            &provider,
            root,
            &BudgetPacer::new(),
            &no_topups(),
            Knobs {
                health: Arc::clone(&health),
                ..Knobs::lanes(2)
            },
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        assert!(
            !matches!(
                health.health(Address::repeat_byte(0xA1)),
                Health::Healthy { streak: 0 }
            ),
            "the node that signed zero cooled: {:?}",
            health.health(Address::repeat_byte(0xA1))
        );
        Ok(())
    }

    /// A fatal fault ends the whole acquire with THAT typed error and does NOT
    /// reassign the failed source's range to a peer. `src_terminal` (lane 0)
    /// owns the first segment and faults at byte 0 with a typed
    /// [`UpstreamVoucherRejected`]: a payment-layer rejection the shared pool
    /// hits against every provider, so no other lane can fix it. `src_peer`
    /// (lane 1) is slow to start, so it is still on its OWN second segment when
    /// the fatal fault ends the acquire.
    #[tokio::test]
    async fn terminal_fault_propagates_and_is_not_reassigned() -> anyhow::Result<()> {
        use decdn_protocol::client::VoucherRejectReason;

        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let ledger_terminal = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_peer = Arc::new(PoolLedger::new(Cumulative::default()));
        // A non-`SpendingCapExhausted` reason, so the driver's exhaustion /
        // reseed self-heal does not intercept it: `fill_gap` returns it verbatim.
        let src_terminal = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || {
                anyhow::Error::new(UpstreamVoucherRejected {
                    reason: VoucherRejectReason::CapabilityExpired,
                    bundle: None,
                    proof_generation: None,
                })
            })
            .paying(Arc::clone(&ledger_terminal));
        let src_peer = ScriptedSource::new(data.clone())?
            .slow_to_start(Duration::from_secs(10))
            .paying(Arc::clone(&ledger_peer));
        let root = src_terminal.root();
        let (store, _dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(src_terminal.clone(), ledger_terminal, 0xA1, None),
            candidate(src_peer, ledger_peer, 0xB2, None),
        ])?;
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            run_acquire(
                &store,
                &provider,
                root,
                total,
                &BudgetPacer::new(),
                &no_topups(),
                2,
                None,
            ),
        )
        .await
        .expect("a fatal fault must end the acquire promptly, not hang");

        let err = result.expect_err("a fatal fault must fail the acquire");
        assert!(
            err.downcast_ref::<UpstreamVoucherRejected>().is_some(),
            "the fatal error must propagate verbatim, not be masked: {err:#}"
        );

        // Lane 0 delivered nothing (it faulted at byte 0) and its range was NOT
        // reassigned: a region well inside lane 0's first segment is still entirely
        // missing.
        assert_eq!(
            src_terminal.delivered_bytes(),
            0,
            "the terminal source faulted before delivering a byte"
        );
        let probe = 16 * 1024 * 1024;
        let missing =
            crate::driver::contiguous_byte_ranges(&store.missing_ranges(0, probe).await?, total);
        let missing_bytes: u64 = missing.iter().map(|(_, l)| *l).fold(0, u64::saturating_add);
        assert_eq!(
            missing_bytes, probe,
            "the failed source's range must not be reassigned to a peer"
        );
        Ok(())
    }

    /// The pool-wide gate: a run registry's `total_committed()` sums EVERY
    /// registered lane, including one this acquire never starts: a concurrent
    /// fetch's lane sharing the same deposit. Lane C is registered but is not a
    /// source here; it carries a prior spend of 68 of the 100-unit deposit.
    /// Summed over just this acquire's own lanes the pool looks fully solvent;
    /// summed the pool-wide way (`Some(&reg)`), C's spend already claims most
    /// of the deposit, so A's second leg is refused and A is priced out.
    #[tokio::test(start_paused = true)]
    #[allow(clippy::too_many_lines)] // three registered lanes, then one acquire
    async fn pool_wide_spent_gates_on_other_run_lanes() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let deposit = U256::from(100u64);
        let other_lane_spend = U256::from(68u64);

        let reg = LaneLedgers::new();
        let handle_a = reg.get_or_insert(
            LaneKey {
                pool_id: B256::ZERO,
                signer: Address::ZERO,
                provider: Address::repeat_byte(0xA1),
            },
            || LaneHandle {
                ledger: Arc::new(PoolLedger::new(Cumulative::default())),
                ctx: Arc::new(Mutex::new(ctx_with(0xA1, deposit))),
            },
        );
        let handle_b = reg.get_or_insert(
            LaneKey {
                pool_id: B256::ZERO,
                signer: Address::ZERO,
                provider: Address::repeat_byte(0xB2),
            },
            || LaneHandle {
                ledger: Arc::new(PoolLedger::new(Cumulative::default())),
                ctx: Arc::new(Mutex::new(ctx_with(0xB2, deposit))),
            },
        );
        // Lane C belongs to a DIFFERENT concurrent fetch on the same run.
        reg.get_or_insert(
            LaneKey {
                pool_id: B256::ZERO,
                signer: Address::ZERO,
                provider: Address::repeat_byte(0xC3),
            },
            || LaneHandle {
                ledger: Arc::new(PoolLedger::new(Cumulative {
                    bytes: U256::from(68u64 * 1024 * 1024),
                    amount: other_lane_spend,
                })),
                ctx: Arc::new(Mutex::new(ctx_with(0xC3, deposit))),
            },
        );

        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&handle_a.ledger));
        // Lane B faults at its first byte on every open: it contributes
        // nothing, so its 32 MiB segment goes to lane A as a second leg.
        let src_b = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || anyhow::anyhow!("scripted immediate fault"))
            .paying(Arc::clone(&handle_b.ledger));
        let root = src_a.root();
        let (store, _dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate_ctx(
                src_a,
                Arc::clone(&handle_a.ledger),
                Arc::clone(&handle_a.ctx),
                None,
            ),
            candidate_ctx(
                src_b,
                Arc::clone(&handle_b.ledger),
                Arc::clone(&handle_b.ctx),
                None,
            ),
        ])?;
        let health: Arc<PeerHealth> = Arc::default();
        let err = run_acquire_with(
            &store,
            &provider,
            root,
            &BudgetPacer::new(),
            &no_topups(),
            Knobs {
                give_up_after: Some(Duration::from_mins(1)),
                ledgers: Some(&reg),
                health: Arc::clone(&health),
                ..Knobs::lanes(2)
            },
        )
        .await
        .expect_err("the pool-wide spend must refuse the second leg");
        assert!(err.downcast_ref::<GaveUp>().is_some(), "{err:#}");
        assert!(
            matches!(
                health.health(Address::repeat_byte(0xA1)),
                Health::Unaffordable { .. }
            ),
            "the pool-wide view priced A out: {:?}",
            health.health(Address::repeat_byte(0xA1))
        );

        let missing =
            crate::driver::contiguous_byte_ranges(&store.missing_ranges(0, total).await?, total);
        let missing_bytes: u64 = missing.iter().map(|(_, l)| *l).fold(0, u64::saturating_add);
        assert!(
            missing_bytes >= 30 * 1024 * 1024,
            "the refused segment must stay unfetched (a whole ~32 MiB): only \
             {missing_bytes} bytes missing"
        );
        Ok(())
    }

    /// Per-provider payment lanes. Two sources with DISTINCT providers each pay
    /// their OWN ledger: the fetch assembles byte-identical, and each lane's
    /// cumulative advances INDEPENDENTLY, tracking exactly the wire that source
    /// delivered (never the peer's).
    ///
    /// Each lane covers one discovery block of a two-block blob, so neither can
    /// steal the other's range. The scripted double bills a leg only in
    /// `finish`, so a leg a steal cancels would read as unpaid here and hide
    /// which ledger the lane uses.
    #[tokio::test]
    async fn distinct_providers_each_pay_their_own_lane() -> anyhow::Result<()> {
        let data = blob(usize::try_from(2 * DISCOVERY_BLOCK_BYTES)?);
        let total = data.len() as u64;
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);

        let lane_a = candidate(src_a, Arc::clone(&ledger_a), 0xA1, Some(cov(2, &[0])));
        let lane_b = candidate(src_b, Arc::clone(&ledger_b), 0xB2, Some(cov(2, &[1])));
        let provider_a = lane_a.ctx.lock().expect("ctx").provider;
        let provider_b = lane_b.ctx.lock().expect("ctx").provider;
        assert_ne!(
            provider_a, provider_b,
            "the two lanes must pay two DIFFERENT providers"
        );
        let provider = StaticSources::new(vec![lane_a, lane_b])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await?;
        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical across two distinct-provider lanes"
        );

        let committed_a = ledger_a.committed();
        let committed_b = ledger_b.committed();
        assert!(
            committed_a.amount > U256::ZERO && committed_b.amount > U256::ZERO,
            "both lanes must have advanced their own cumulative: a={committed_a:?} b={committed_b:?}"
        );
        assert!(
            committed_a.bytes < U256::from(total) && committed_b.bytes < U256::from(total),
            "neither lane alone may bill the whole blob; a shared ledger would: \
             a={committed_a:?} b={committed_b:?} total={total}"
        );
        assert!(
            committed_a.bytes.saturating_add(committed_b.bytes) >= U256::from(total),
            "the two independent lanes must together cover the whole blob: a={committed_a:?} \
             b={committed_b:?} total={total}"
        );
        Ok(())
    }

    /// A [`Pacer`] that records the minimum `remaining_deposit` any `decide` saw,
    /// then defers to [`BudgetPacer`]. Proves what balance the workers actually
    /// gated on.
    struct MinRemainingPacer {
        inner: BudgetPacer,
        min_remaining: Mutex<Option<U256>>,
    }

    impl Pacer for MinRemainingPacer {
        fn decide(&self, s: &PaceState) -> PaceDecision {
            if let Ok(mut g) = self.min_remaining.lock() {
                *g = Some(g.map_or(s.remaining_deposit, |m| m.min(s.remaining_deposit)));
            }
            self.inner.decide(s)
        }
    }

    /// Shared-pool aggregate solvency. Two lanes draw on ONE pool deposit `D`.
    /// Each worker's `remaining_deposit` is `D - Σ committed across EVERY lane`,
    /// so the smallest balance any pacing decision saw drops below
    /// `D - max(single-lane committed)`: the floor a per-lane gate could never
    /// go under.
    #[tokio::test]
    async fn concurrent_lanes_gate_on_the_shared_pool_balance() -> anyhow::Result<()> {
        let data = blob(8 * 1024 * 1024);
        let total = data.len() as u64;
        // A pool deposit far above the ~8-unit blob cost, so the fetch always
        // completes; the test reads the observed balances, not a refusal.
        let deposit = U256::from(1_000u64);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);
        let pacer = MinRemainingPacer {
            inner: BudgetPacer::new(),
            min_remaining: Mutex::new(None),
        };

        // Both lanes' ctxs carry the SAME shared pool deposit `D`.
        let provider = StaticSources::new(vec![
            candidate_ctx(
                src_a,
                Arc::clone(&ledger_a),
                Arc::new(Mutex::new(ctx_with(0xA1, deposit))),
                None,
            ),
            candidate_ctx(
                src_b,
                Arc::clone(&ledger_b),
                Arc::new(Mutex::new(ctx_with(0xB2, deposit))),
                None,
            ),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &pacer,
            &no_topups(),
            4,
            None,
        )
        .await?;
        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "the fetch still assembles byte-identical under the shared-solvency gate"
        );

        let committed_a = ledger_a.committed().amount;
        let committed_b = ledger_b.committed().amount;
        assert!(
            committed_a > U256::ZERO && committed_b > U256::ZERO,
            "both lanes must have paid, so the aggregate exceeds either lane alone"
        );
        let max_lane = committed_a.max(committed_b);
        let aggregate = committed_a.saturating_add(committed_b);
        let min_remaining = pacer
            .min_remaining
            .lock()
            .expect("min lock")
            .expect("at least one decide ran");

        // The aggregate gate: the lowest balance a worker saw is `D - Σ committed`.
        assert_eq!(
            min_remaining,
            deposit.saturating_sub(aggregate),
            "a worker must have gated on the SHARED remaining (deposit minus every lane's spend)"
        );
        // And that is strictly below the per-lane floor `D - max_lane`.
        assert!(
            min_remaining < deposit.saturating_sub(max_lane),
            "the shared gate must see less than a single lane's own remaining: \
             min={min_remaining:?} per_lane_floor={:?}",
            deposit.saturating_sub(max_lane)
        );
        Ok(())
    }

    /// The shared gate actually BLOCKS a lane (no collective overspend).
    ///
    /// A 64 MiB blob splits into two 32 MiB segments. Lane B's source faults at
    /// once on every open (delivers nothing), but its ledger is PRE-SEEDED with
    /// a committed amount `prior_spend`: a peer lane that has already drawn
    /// most of the pool on earlier streams. Lane A fetches its OWN 32 MiB
    /// segment (a first leg always draws: voucher cost is unpriced until the
    /// first open), then takes B's re-queued segment as a SECOND leg. By then
    /// the aggregate reader reports `A_committed + prior_spend`, which is at or
    /// over the deposit, so the second leg is REFUSED and A is priced out. The
    /// acquire makes no further progress and gives up, and A never bills a
    /// second segment.
    #[tokio::test(start_paused = true)]
    async fn shared_gate_refuses_a_second_leg_into_a_drained_pool() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let deposit = U256::from(100u64);
        let prior_spend = U256::from(68u64);

        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative {
            bytes: U256::from(68u64 * 1024 * 1024),
            amount: prior_spend,
        }));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || anyhow::anyhow!("scripted immediate fault"))
            .paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, _dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate_ctx(
                src_a.clone(),
                Arc::clone(&ledger_a),
                Arc::new(Mutex::new(ctx_with(0xA1, deposit))),
                None,
            ),
            candidate_ctx(
                src_b,
                Arc::clone(&ledger_b),
                Arc::new(Mutex::new(ctx_with(0xB2, deposit))),
                None,
            ),
        ])?;
        let result = run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            2,
            Some(Duration::from_mins(1)),
        )
        .await;

        // The gate blocked A's second leg, so the blob never completed.
        let err = result.expect_err("the shared gate must refuse A's second leg");
        assert!(err.downcast_ref::<GaveUp>().is_some(), "{err:#}");

        let committed_a = ledger_a.committed().amount;
        let committed_b = ledger_b.committed().amount;
        assert_eq!(
            committed_b, prior_spend,
            "the faulted peer lane billed nothing new"
        );
        // A billed its OWN one segment (~33 units of wire) and NOTHING for the
        // refused second.
        assert!(
            committed_a > U256::ZERO && committed_a < U256::from(50u64),
            "A must bill exactly ONE segment, never the refused second: committed_a={committed_a:?}"
        );
        assert!(
            src_a.delivered_bytes() < total,
            "A must not have delivered the whole blob: its second leg was refused: delivered={}",
            src_a.delivered_bytes()
        );
        // No-collective-overspend bound: combined committed stays within the
        // deposit plus at most one in-flight leg's overshoot.
        let combined = committed_a.saturating_add(committed_b);
        let one_leg_ceiling = deposit.saturating_add(U256::from(40u64));
        assert!(
            combined <= one_leg_ceiling,
            "combined committed must stay within deposit + one in-flight leg: \
             combined={combined:?} ceiling={one_leg_ceiling:?}"
        );
        Ok(())
    }

    // ---- stall watchdog (`watchdog`, selected at the worker's `tokio::select!`) ----

    /// A store double whose `missing_ranges` replays a scripted sequence of
    /// still-missing byte counts, so every branch of [`watchdog`] runs against an
    /// exact progress history under a paused clock.
    ///
    /// Only `total_bytes`/`missing_ranges` are reachable from `watchdog`; the rest
    /// of the [`IngestStore`] surface returns an error rather than panicking, so a
    /// future caller that starts using one gets a failure it can see.
    struct ScriptedMissing {
        total: u64,
        /// Still-missing byte counts, one consumed per call. The LAST entry
        /// repeats forever, which is what lets a "never trips" assertion run to a
        /// timeout instead of running out of script.
        script: Mutex<std::collections::VecDeque<u64>>,
    }

    impl ScriptedMissing {
        fn new(total: u64, script: &[u64]) -> Self {
            Self {
                total,
                script: Mutex::new(script.iter().copied().collect()),
            }
        }

        fn next_missing(&self) -> u64 {
            let mut q = self.script.lock().expect("script lock");
            if q.len() > 1 {
                q.pop_front().unwrap_or(0)
            } else {
                q.front().copied().unwrap_or(0)
            }
        }
    }

    fn unsupported<T>() -> decdn_bao_range::RangedStoreError {
        let _ = std::marker::PhantomData::<T>;
        decdn_bao_range::RangedStoreError::Backend(Box::from("unsupported on ScriptedMissing"))
    }

    impl decdn_bao_range::RangedStore for ScriptedMissing {
        fn total_bytes(&self) -> u64 {
            self.total
        }

        fn present_ranges(&self) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
            Box::pin(async { Err(unsupported::<()>()) })
        }

        fn missing_ranges(
            &self,
            byte_offset: u64,
            _byte_len: u64,
        ) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
            let missing = self.next_missing();
            Box::pin(async move {
                if missing == 0 {
                    return Ok(bao_tree::ChunkRanges::empty());
                }
                // 1 KiB per bao chunk; the byte counts the script names are
                // multiples of that.
                let start = bao_tree::ChunkNum(byte_offset / 1024);
                let end = bao_tree::ChunkNum((byte_offset + missing) / 1024);
                Ok(bao_tree::ChunkRanges::from(start..end))
            })
        }

        fn admit(
            &self,
            _range: decdn_bao_range::AlignedRange,
            _bao_bytes: bytes::Bytes,
        ) -> decdn_bao_range::RangedFuture<'_, ()> {
            Box::pin(async { Err(unsupported::<()>()) })
        }

        fn read(
            &self,
            _byte_offset: u64,
            _byte_len: u64,
        ) -> decdn_bao_range::RangedFuture<'_, bytes::Bytes> {
            Box::pin(async { Err(unsupported::<()>()) })
        }

        fn is_complete(&self) -> decdn_bao_range::RangedFuture<'_, bool> {
            Box::pin(async { Err(unsupported::<()>()) })
        }

        fn finalize(&self) -> decdn_bao_range::RangedFuture<'_, ()> {
            Box::pin(async { Err(unsupported::<()>()) })
        }
    }

    impl crate::source::IngestStore for ScriptedMissing {
        fn ingest_stream<'a, R>(
            &'a self,
            _range: &'a decdn_bao_range::AlignedRange,
            _reader: R,
            _on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
            _claimed_total: u64,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<R>> + 'a>>
        where
            R: crate::BaoRangeReader + 'a,
        {
            Box::pin(async { Err(anyhow::anyhow!("unsupported on ScriptedMissing")) })
        }

        fn flush_present_record(&self) -> crate::source::SourceFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    /// Whether `watchdog` trips within an hour of virtual time. `missing` scripts
    /// the store's still-missing counts; `verified(k)` is the unit's verified-byte
    /// counter during window `k`, set halfway into that window.
    async fn watchdog_trips(
        missing: &[u64],
        verified: impl Fn(u64) -> u64,
        deadline: Duration,
    ) -> bool {
        let store = ScriptedMissing::new(64 * 1024 * 1024, missing);
        let counter = std::sync::atomic::AtomicU64::new(0);
        let feed = async {
            if deadline.is_zero() {
                return std::future::pending().await;
            }
            tokio::time::sleep(deadline / 2).await;
            for k in 0u64.. {
                counter.store(verified(k), std::sync::atomic::Ordering::Relaxed);
                tokio::time::sleep(deadline).await;
            }
        };
        let raced = async {
            tokio::select! {
                err = super::watchdog(&store, 0, 64 * 1024 * 1024, deadline, &counter) => {
                    err.is_none()
                }
                () = feed => false,
            }
        };
        tokio::time::timeout(Duration::from_hours(1), raced)
            .await
            .unwrap_or(false)
    }

    /// A full window with bytes still missing and no verified byte is the
    /// definition of a stall: the watchdog trips and the worker's range is
    /// reassigned.
    #[tokio::test(start_paused = true)]
    async fn watchdog_trips_when_no_verified_byte_lands() {
        assert!(
            watchdog_trips(&[8192], |_| 0, LANE_WATCHDOG).await,
            "no verified byte across a full window must trip the watchdog"
        );
    }

    /// A source that verified bytes and then stops is a stall from the first
    /// window without a verified byte.
    #[tokio::test(start_paused = true)]
    async fn watchdog_trips_when_verified_bytes_stop() {
        assert!(
            watchdog_trips(&[8192], |k| k.min(3) * 1024, LANE_WATCHDOG).await,
            "a source that stops verifying bytes must trip the watchdog"
        );
    }

    /// A fully delivered range (`missing == 0`) is left to `fill_gap`'s own
    /// completion, NEVER tripped: the source is inside `finish`, draining the
    /// vouchers for bytes it already delivered. Tripping here would reassign an
    /// ALREADY-PAID range: a direct double-pay.
    #[tokio::test(start_paused = true)]
    async fn watchdog_never_trips_a_fully_delivered_range() {
        assert!(
            !watchdog_trips(&[0], |_| 0, LANE_WATCHDOG).await,
            "a delivered range must never be tripped while it finishes paying"
        );
    }

    /// A source that keeps verifying bytes resets the window at each sample, even
    /// when it lands less than one 4 MiB ingest checkpoint per window, so the
    /// store's missing count never moves (#2209).
    #[tokio::test(start_paused = true)]
    async fn watchdog_window_resets_on_verified_bytes_below_a_checkpoint() {
        assert!(
            !watchdog_trips(&[8192], |k| (k + 1) * 1024 * 1024, LANE_WATCHDOG).await,
            "verified bytes must reset the window before any checkpoint lands"
        );
    }

    /// A leg's first byte gets the first-byte grace, not the lane watchdog: a
    /// cold miss whose first byte takes 20 s is not a stall.
    #[tokio::test(start_paused = true)]
    async fn watchdog_gives_the_first_byte_its_grace() {
        let store = ScriptedMissing::new(64 * 1024 * 1024, &[8192]);
        let counter = std::sync::atomic::AtomicU64::new(0);
        let start = tokio::time::Instant::now();
        let feed = async {
            tokio::time::sleep(Duration::from_secs(20)).await;
            for k in 1u64.. {
                counter.store(k * 1024, std::sync::atomic::Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };
        let tripped = tokio::time::timeout(Duration::from_mins(2), async {
            tokio::select! {
                err = super::watchdog(&store, 0, 64 * 1024 * 1024, LANE_WATCHDOG, &counter) => {
                    err.is_none()
                }
                () = feed => false,
            }
        })
        .await
        .unwrap_or(false);
        assert!(!tripped, "tripped after {:?}", start.elapsed());

        // With no first byte at all, it trips once the grace ends.
        let silent = std::sync::atomic::AtomicU64::new(0);
        let start = tokio::time::Instant::now();
        let err = super::watchdog(&store, 0, 64 * 1024 * 1024, LANE_WATCHDOG, &silent).await;
        assert!(err.is_none(), "a trip, not a store error");
        assert_eq!(start.elapsed(), super::FIRST_BYTE_GRACE);
    }

    /// Acquire the whole blob into `store` from `provider`'s one lane, under
    /// `stop`, bounded so a regression fails instead of hanging.
    async fn acquire_bounded<St: crate::source::IngestStore>(
        store: &St,
        provider: &StaticSources<ScriptedSource>,
        root: [u8; 32],
        stop: &StopPolicy,
    ) -> anyhow::Result<()> {
        let total = store.total_bytes();
        let mut set = SourceSet::new(provider, root, Arc::default(), provider.holders());
        let drive = drive_config();
        let (pacer, funder) = (BudgetPacer::new(), no_topups());
        let ranges = [(0, total)];
        let env = AcquireEnv {
            pacer: &pacer,
            funder: &funder,
            drive: &drive,
            max_lanes: 1,
            stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        };
        let target = AcquireTarget {
            store,
            hash: root,
            total_bytes: total,
            ranges: &ranges,
        };
        tokio::time::timeout(Duration::from_mins(2), acquire(target, &mut set, &env))
            .await
            .map_err(|_| anyhow::anyhow!("the acquire did not end within 2 min"))?
    }

    /// A record write slower than the lane watchdog never pauses the lanes
    /// (#2211): the write runs beside the workers. The first record tick falls
    /// while the lane waits for its first byte, and its write takes 30 s. The
    /// lane still lands the blob on its first open, 8 s in; awaited inline,
    /// the write would leave it unpolled past its first-byte grace.
    #[tokio::test(start_paused = true)]
    async fn a_slow_record_write_does_not_pause_the_lanes() -> anyhow::Result<()> {
        let data = blob(1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data.clone())?
            .paying(Arc::clone(&la))
            .slow_to_start(Duration::from_secs(8));
        let (root, total) = (a.root(), a.total_bytes());
        let (inner, dir) = fresh_store(root, total);
        let store = crate::source::FlushCountingStore::new(inner, Duration::from_secs(30));
        assert!(store.flush_delay > LANE_WATCHDOG);
        let provider = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None)])?;
        let stop = StopPolicy::new(false, Some(Duration::from_mins(5)), Arc::default());
        acquire_bounded(&store, &provider, root, &stop).await?;

        assert_eq!(a.opened_ranges().len(), 1, "{:?}", a.opened_ranges());
        let (_, opened, finished) = a
            .timeline()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("the lane opened its range"))?;
        let finished = finished.ok_or_else(|| anyhow::anyhow!("the leg finished"))?;
        assert!(
            finished.duration_since(opened) < LANE_WATCHDOG,
            "the leg waited on the record write: it took {:?}",
            finished.duration_since(opened)
        );
        // The tick's write and the final one.
        assert_eq!(store.flushes.load(std::sync::atomic::Ordering::SeqCst), 2);
        store.inner.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        Ok(())
    }

    /// A store whose first record write lands 30 s after its call and every
    /// later one at once, so a final write that does not wait for the first
    /// lands before it. The tempdir is handed back to keep the files alive.
    fn slow_first_write(
        root: [u8; 32],
        total: u64,
    ) -> (crate::source::FlushCountingStore, tempfile::TempDir) {
        let (inner, dir) = fresh_store(root, total);
        let store = crate::source::FlushCountingStore {
            slow_flushes: 1,
            ..crate::source::FlushCountingStore::new(inner, Duration::from_secs(30))
        };
        (store, dir)
    }

    /// Assert that the final record write is the last to land, once every
    /// write has had time to land, whoever awaited it.
    async fn assert_final_record_lands_last(store: &crate::source::FlushCountingStore) {
        tokio::time::sleep(Duration::from_mins(1)).await;
        let flushes = store.flushes.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(flushes, 2, "the tick's write and the final one");
        assert_eq!(
            store.last_landed.load(std::sync::atomic::Ordering::SeqCst),
            flushes,
            "the final snapshot lands last"
        );
    }

    /// The final record write lands last when the fetch completes: the slow
    /// periodic write in flight lands first, so its older snapshot cannot
    /// replace the final one.
    #[tokio::test(start_paused = true)]
    async fn the_final_record_lands_last_when_the_fetch_completes() -> anyhow::Result<()> {
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(blob(1024 * 1024))?
            .paying(Arc::clone(&la))
            .slow_to_start(Duration::from_secs(8));
        let (store, _dir) = slow_first_write(a.root(), a.total_bytes());
        let provider = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None)])?;
        let stop = StopPolicy::new(false, Some(Duration::from_mins(5)), Arc::default());
        acquire_bounded(&store, &provider, a.root(), &stop).await?;
        assert_final_record_lands_last(&store).await;
        Ok(())
    }

    /// The final record write lands last when the fetch ends on an error (here
    /// a give-up): the slow periodic write in flight lands first.
    #[tokio::test(start_paused = true)]
    async fn the_final_record_lands_last_when_the_fetch_gives_up() -> anyhow::Result<()> {
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let (_open, gate) = tokio::sync::watch::channel(false);
        let a = ScriptedSource::new(blob(1024 * 1024))?
            .paying(Arc::clone(&la))
            .gated_on(gate);
        let (store, _dir) = slow_first_write(a.root(), a.total_bytes());
        let provider = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None)])?;
        // Past the first record tick at 5 s, inside the first-byte grace.
        let stop = StopPolicy::new(false, Some(Duration::from_secs(7)), Arc::default());
        let err = acquire_bounded(&store, &provider, a.root(), &stop)
            .await
            .expect_err("a source that never delivers gives up");
        assert!(err.downcast_ref::<GaveUp>().is_some(), "{err:#}");
        assert_final_record_lands_last(&store).await;
        Ok(())
    }

    /// End-to-end: a source whose first byte takes 20 s serves the whole
    /// blob on its first open; the lane watchdog never reassigns it.
    #[tokio::test(start_paused = true)]
    async fn a_slow_first_byte_is_not_a_stall() -> anyhow::Result<()> {
        let data = blob(1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data.clone())?
            .paying(Arc::clone(&la))
            .slow_to_start(Duration::from_secs(20));
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None)])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            1,
            Some(Duration::from_mins(5)),
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        assert_eq!(a.opened_ranges().len(), 1, "{:?}", a.opened_ranges());
        Ok(())
    }

    /// A zero window turns the watchdog off, as consumption pacing does.
    #[tokio::test(start_paused = true)]
    async fn watchdog_zero_deadline_never_trips() {
        assert!(
            !watchdog_trips(&[8192], |_| 0, Duration::ZERO).await,
            "a zero window must disable the watchdog"
        );
    }

    /// End-to-end: a source that opens, delivers a prefix, then WEDGES without
    /// erroring is ended by the lane watchdog ALONE, and its unfetched remainder
    /// is picked up by a healthy peer.
    ///
    /// The segments are deliberately below `MIN_SPLIT_SIZE` (a 16 MiB blob over
    /// two lanes) so the healthy peer CANNOT steal the wedged lane's tail: with
    /// stealing unavailable, the watchdog is the only thing that can end the
    /// wedge.
    #[tokio::test(start_paused = true)]
    async fn wedged_source_is_ended_by_the_watchdog_and_its_tail_reassigned() -> anyhow::Result<()>
    {
        let data = blob(16 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        // A delivers ~4 MiB (one INGEST_CHECKPOINT_BYTES) and then sleeps far
        // past the lane watchdog.
        let src_a = ScriptedSource::new(data.clone())?
            .stall_after(4 * 1024 * 1024, Duration::from_hours(1))
            .paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let health: Arc<PeerHealth> = Arc::default();
        let provider = StaticSources::new(vec![
            candidate(src_a.clone(), ledger_a, 0xA1, None),
            candidate(src_b, ledger_b, 0xB2, None),
        ])?;
        let started = tokio::time::Instant::now();
        run_acquire_with(
            &store,
            &provider,
            root,
            &BudgetPacer::new(),
            &no_topups(),
            Knobs {
                health: Arc::clone(&health),
                ..Knobs::lanes(2)
            },
        )
        .await?;
        assert!(
            started.elapsed() < Duration::from_mins(1),
            "the watchdog, not the hour-long wedge, ended lane a: {:?}",
            started.elapsed()
        );

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical after the wedged source was reassigned"
        );
        assert!(
            src_a.delivered_bytes() < 8 * 1024 * 1024,
            "the wedged source must not have delivered its whole segment: {}",
            src_a.delivered_bytes()
        );
        Ok(())
    }

    /// A dropped acquire keeps the bytes that landed recorded: the periodic
    /// flush writes the present record while lanes run, so a caller that drops
    /// the acquire mid-fetch (Ctrl-C, its own stop) resumes from there.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_acquire_keeps_landed_bytes_recorded() -> anyhow::Result<()> {
        let data = blob(32 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        // Each lane delivers one ingest checkpoint of its half, then sleeps.
        let src_a = ScriptedSource::new(data.clone())?
            .stall_after(4 * 1024 * 1024, Duration::from_hours(1))
            .paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?
            .stall_after(4 * 1024 * 1024, Duration::from_hours(1))
            .paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(src_a, ledger_a, 0xA1, None),
            candidate(src_b, ledger_b, 0xB2, None),
        ])?;
        {
            let (pacer, funder) = (BudgetPacer::new(), no_topups());
            let fetch = run_acquire(&store, &provider, root, total, &pacer, &funder, 2, None);
            // Past one present-record flush, short of the lane watchdog.
            tokio::select! {
                fetched = fetch => panic!("wedged lanes cannot finish: {fetched:?}"),
                () = tokio::time::sleep(LANE_WATCHDOG.saturating_sub(Duration::from_secs(2))) => {}
            }
        }
        drop(store);

        let reopened = ClientRangedStore::open(dir.path(), "b", root)?;
        let recorded = ranges_content_len(&reopened.present_ranges().await?, total);
        assert!(
            recorded >= 8 * 1024 * 1024,
            "both lanes' landed checkpoints are recorded: {recorded}"
        );
        assert!(recorded < total);
        Ok(())
    }

    /// A freed worker with nothing to steal PARKS rather than ending, so it is
    /// still there to take over when a peer faults moments later.
    ///
    /// Shape: a 16 MiB blob splits into two 8 MiB segments, both below the 16 MiB
    /// `MIN_SPLIT_SIZE`, so the fast worker's `pick` finds nothing splittable:
    /// the routine end-of-fetch condition. The slow worker then faults and
    /// re-queues its remainder, which the parked worker takes.
    #[tokio::test]
    async fn a_parked_worker_takes_over_a_later_faulted_peers_remainder() -> anyhow::Result<()> {
        let data = blob(16 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        // A: holds back long enough for B to finish its own segment and find
        // nothing worth stealing, then delivers a prefix and faults.
        let src_a = ScriptedSource::new(data.clone())?
            .slow_to_start(Duration::from_millis(400))
            .with_fault_after(2 * 1024 * 1024, || anyhow::anyhow!("scripted fault"))
            .paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(src_a, ledger_a, 0xA1, None),
            candidate(src_b, ledger_b, 0xB2, None),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            2,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "the parked worker covered the faulted peer's remainder"
        );
        Ok(())
    }

    /// A `WindowPacer` + `ConsumptionPacing` bounds a lane to one read-ahead
    /// window ahead of an injected consumer cursor: the fetch parks when the
    /// window fills and only proceeds as the cursor advances via the pacing hook.
    #[tokio::test]
    async fn window_pacing_gates_a_lane_on_the_consumer_cursor() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicU64, Ordering};

        use crate::driver::{PacingWait, WaitReason};
        use crate::pacer::{DownstreamFrontier, WindowPacer};

        /// Stands in for the consumer reading one window's worth: it bumps the
        /// shared cursor and resolves immediately, so the test is deterministic.
        struct BumpConsumer {
            cursor: Arc<AtomicU64>,
            by: u64,
        }
        impl PacingWait for BumpConsumer {
            fn wait(
                &self,
                _observed: DownstreamFrontier,
                _reason: WaitReason,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
                self.cursor.fetch_add(self.by, Ordering::SeqCst);
                Box::pin(async {})
            }
        }

        let data = blob(4 * 1024 * 1024); // several windows long
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger));
        let root = src.root();
        let total = src.total_bytes();
        let (store, dir) = fresh_store(root, total);

        let window = 1024 * 1024;
        let pacer = WindowPacer::new(window);
        let cursor = Arc::new(AtomicU64::new(0));
        let reader = {
            let cursor = Arc::clone(&cursor);
            move || DownstreamFrontier {
                served_paid: cursor.load(Ordering::SeqCst),
                serve_demand: 0,
            }
        };
        let hook = BumpConsumer {
            cursor: Arc::clone(&cursor),
            by: window,
        };
        let pacing = ConsumptionPacing {
            downstream: &reader,
            pacing_wait: &hook,
        };

        let provider = StaticSources::new(vec![candidate(src, ledger, 0xA1, None)])?;
        run_acquire_with(
            &store,
            &provider,
            root,
            &pacer,
            &no_topups(),
            Knobs {
                pacing: Some(&pacing),
                ..Knobs::lanes(1)
            },
        )
        .await?;
        store.finalize().await?;

        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "byte-identical under window pacing"
        );
        assert!(
            cursor.load(Ordering::SeqCst) >= 2 * window,
            "the window must have parked the lane and been unstuck by the consumer \
             hook (cursor={})",
            cursor.load(Ordering::SeqCst)
        );
        Ok(())
    }

    // ---- acquire over a SourceSet ----

    /// The reported bug: one holder, one reset, then it recovers.
    #[tokio::test(start_paused = true)]
    async fn one_source_one_reset_then_recovery_completes() -> anyhow::Result<()> {
        let data = blob(8 * 1024 * 1024);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(data.clone())?
            .paying(Arc::clone(&ledger))
            .fault_once_after(64 * 1024, || anyhow::anyhow!("connection reset"));
        let (root, total) = (src.root(), src.total_bytes());
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![candidate(src.clone(), ledger, 0xA1, None)])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        assert!(
            src.opened_ranges().len() >= 2,
            "the source faulted once and was reopened: {:?}",
            src.opened_ranges()
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_reserve_source_joins_when_a_lane_cools() -> anyhow::Result<()> {
        let data = blob(8 * 1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data.clone())?
            .paying(Arc::clone(&la))
            .with_fault_after(64 * 1024, || anyhow::anyhow!("reset"));
        let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(a.clone(), la, 0xA1, None),
            candidate(b.clone(), lb, 0xB2, None),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            1,
            None,
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        assert!(b.delivered_bytes() > 0, "the reserve source took over");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_cheaper_source_takes_over_after_one_is_priced_out() -> anyhow::Result<()> {
        // Source A refuses with InsufficientDeposit on open; B serves.
        let data = blob(4 * 1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data.clone())?
            .paying(Arc::clone(&la))
            .with_fault_after(0, || {
                anyhow::Error::new(UpstreamRefused::mid_stream(
                    StreamError::InsufficientDeposit,
                ))
            });
        let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
        let (root, total) = (a.root(), a.total_bytes());
        let (store, _dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(a, la, 0xA1, None),
            candidate(b.clone(), lb, 0xB2, None),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            1,
            None,
        )
        .await?;
        assert!(b.delivered_bytes() > 0);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn every_source_priced_out_stops_with_the_top_up_remedy() -> anyhow::Result<()> {
        let data = blob(1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data)?
            .paying(Arc::clone(&la))
            .with_fault_after(0, || {
                anyhow::Error::new(PoolExhausted {
                    gap_start: 0,
                    gap_len: 1,
                })
            });
        let (root, total) = (a.root(), a.total_bytes());
        let (store, _dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?;
        let err = run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("must stop"))?;
        assert!(
            err.downcast_ref::<NoAffordableSource>().is_some(),
            "{err:#}"
        );
        Ok(())
    }

    /// A node's `SpendingCapExhausted` refusal is genuine only when the pool's
    /// own accounting agrees, and that accounting takes the funder's view of
    /// the whole pool ([`Funder::pool_spent`]). With no spend beyond the loop's
    /// lanes, they have touched almost none of the deposit, so the refusal is a
    /// lie that ends the command. With the deposit spent outside
    /// the loop, the refusal is genuine: with no top-up left, the acquire stops
    /// with the top-up remedy.
    #[tokio::test(start_paused = true)]
    async fn a_cap_refusal_is_genuine_once_the_outside_spend_drains_the_deposit()
    -> anyhow::Result<()> {
        let deposit = U256::from(1_000_000_000u64);
        for (outside, genuine) in [(U256::ZERO, false), (deposit, true)] {
            let la = Arc::new(PoolLedger::new(Cumulative::default()));
            let a = ScriptedSource::new(blob(1024 * 1024))?
                .paying(Arc::clone(&la))
                .with_fault_after(0, || {
                    anyhow::Error::new(UpstreamVoucherRejected {
                        reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
                        bundle: None,
                        proof_generation: None,
                    })
                });
            let (root, total) = (a.root(), a.total_bytes());
            let (store, _dir) = fresh_store(root, total);
            let ctx = Arc::new(Mutex::new(ctx_with(0xA1, deposit)));
            let provider = StaticSources::new(vec![candidate_ctx(a, la, ctx, None)])?;
            let funder = no_topups().with_pool_spent(outside);
            let err = run_acquire(
                &store,
                &provider,
                root,
                total,
                &BudgetPacer::new(),
                &funder,
                1,
                None,
            )
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("outside spend {outside}: must stop"))?;
            assert_eq!(
                err.downcast_ref::<NoAffordableSource>().is_some(),
                genuine,
                "outside spend {outside}: {err:#}"
            );
        }
        Ok(())
    }

    /// A lane build that may have escrowed USDC no record credits ends the
    /// acquire at once. Any other build fault retries with backoff, and a retry
    /// here would escrow again.
    #[tokio::test(start_paused = true)]
    async fn a_lane_build_that_may_have_escrowed_ends_the_acquire() -> anyhow::Result<()> {
        struct EscrowingBuild(StaticSources<ScriptedSource>);

        impl crate::SourceProvider for EscrowingBuild {
            type Source = ScriptedSource;

            fn discover(&self, hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
                self.0.discover(hash)
            }

            fn connect<'a>(
                &'a self,
                _holder: &'a crate::Holder,
            ) -> crate::SourceFuture<'a, StreamCandidate<ScriptedSource>> {
                Box::pin(async {
                    Err(crate::buyer_pool::escrowed_but_untracked(
                        "pool 0x01 topped up by 5 µUSDC",
                        alloy::primitives::TxHash::repeat_byte(0xab),
                        "disk full",
                    ))
                })
            }
        }

        let data = blob(1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data)?.paying(Arc::clone(&la));
        let (root, total) = (a.root(), a.total_bytes());
        let (store, _dir) = fresh_store(root, total);
        let provider = EscrowingBuild(StaticSources::new(vec![candidate(a, la, 0xA1, None)])?);
        let mut set = SourceSet::new(&provider, root, Arc::default(), provider.0.holders());
        // A retried build would run until this gives up instead.
        let stop = StopPolicy::new(false, Some(Duration::from_mins(5)), Arc::default());
        let drive = drive_config();
        let err = acquire(
            AcquireTarget {
                store: &store,
                hash: root,
                total_bytes: total,
                ranges: &[(0, total)],
            },
            &mut set,
            &AcquireEnv {
                pacer: &BudgetPacer::new(),
                funder: &no_topups(),
                drive: &drive,
                max_lanes: 1,
                stop: &stop,
                on_progress: None,
                ledgers: None,
                pacing: None,
                max_blob_bytes: 0,
            },
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("must stop"))?;
        assert_eq!(classify(&err), Fault::Fatal(FatalScope::Command), "{err:#}");
        assert!(
            format!("{err:#}").contains("escrowed but untracked"),
            "{err:#}"
        );
        Ok(())
    }

    /// A source priced out at the pool's deposit stays priced out after a
    /// top-up the driver's own pacer declined, so a set top-up budget does not
    /// keep the acquire waiting: it stops with the top-up remedy.
    #[tokio::test(start_paused = true)]
    async fn every_source_priced_out_stops_even_with_top_ups_left() -> anyhow::Result<()> {
        let data = blob(1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data)?
            .paying(Arc::clone(&la))
            .with_fault_after(0, || {
                anyhow::Error::new(PoolExhausted {
                    gap_start: 0,
                    gap_len: 1,
                })
            });
        let (root, total) = (a.root(), a.total_bytes());
        let (store, _dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?;
        // `run_acquire` drives with `working_deposit = 0`.
        let funder = FakeFunder::new(1, DepositOutcome::Added(U256::ZERO));
        let err = run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &funder,
            4,
            None,
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("must stop"))?;
        assert!(
            err.downcast_ref::<NoAffordableSource>().is_some(),
            "{err:#}"
        );
        Ok(())
    }

    /// An entry two running lanes cover between them is not uncovered, even
    /// though neither covers it whole.
    #[test]
    fn an_entry_covered_only_by_two_lanes_together_is_not_uncovered() -> anyhow::Result<()> {
        use std::collections::VecDeque;

        use decdn_bao_range::align_range;

        use super::Work;

        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let both_blocks = align_range(0, total, total)?;
        let mut work = Work::new(VecDeque::from(vec![both_blocks]), false);
        let a = work.add_lane(Some(cov(2, &[0])), total);
        let b = work.add_lane(Some(cov(2, &[1])), total);
        assert!(!work.uncovered(total), "a and b cover it between them");
        work.park(b);
        assert!(work.uncovered(total), "block 1 has no running lane");
        work.revive(b);
        work.park(a);
        assert!(work.uncovered(total), "block 0 has no running lane");
        Ok(())
    }

    /// A lane barred from pull-through is never handed a range outside its
    /// coverage, and still takes the ranges it covers. Lifting the bar hands
    /// it the uncovered range again.
    #[test]
    fn a_barred_lane_takes_only_what_it_covers() -> anyhow::Result<()> {
        use std::collections::VecDeque;

        use decdn_bao_range::align_range;

        use super::{Picked, Work};

        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let in_block0 = align_range(0, DISCOVERY_BLOCK_BYTES, total)?;
        let in_block1 = align_range(DISCOVERY_BLOCK_BYTES, DISCOVERY_BLOCK_BYTES, total)?;
        let mut work = Work::new(VecDeque::from(vec![in_block1, in_block0]), false);
        let a_cov = cov(2, &[0]);
        let a = work.add_lane(Some(a_cov.clone()), total);
        let b = work.add_lane(Some(cov(2, &[1])), total);
        work.park(b);
        work.set_no_uncovered(a, true);
        let missing = [(0, total)];

        assert!(work.has_work_for(&a_cov, total, false), "block 0 is a's");
        let first = work.pick(a, total, &a_cov, true, &missing)?;
        assert!(
            first.is_some_and(|p| p.range.fetch_start() == 0 && !p.uncovered),
            "a takes the block it covers"
        );
        work.clear(a)?;
        assert!(
            !work.has_work_for(&a_cov, total, false),
            "only block 1 is left"
        );
        assert_eq!(
            work.growth_wanted(total, |_| false).len(),
            1,
            "the idle barred lane does not take block 1"
        );
        assert!(
            work.pick(a, total, &a_cov, true, &missing)?.is_none(),
            "a barred lane takes no uncovered range"
        );

        work.set_no_uncovered(a, false);
        let Some(Picked {
            range, uncovered, ..
        }) = work.pick(a, total, &a_cov, true, &missing)?
        else {
            anyhow::bail!("an unbarred lane takes the uncovered range");
        };
        assert_eq!(range.fetch_start(), DISCOVERY_BLOCK_BYTES);
        assert!(uncovered);
        Ok(())
    }

    /// A faulted lane's remainder that only that lane covered (#2230): once
    /// the lane has stopped, no running lane covers it, so a busy lane not
    /// barred from pull-through may grow for it, and its extra worker takes
    /// it as an uncovered range. A barred busy lane is not asked, and its
    /// extra worker takes nothing outside its coverage.
    #[test]
    fn a_remainder_only_the_faulted_lane_covered_goes_to_a_busy_lanes_extra() -> anyhow::Result<()>
    {
        use std::collections::VecDeque;

        use decdn_bao_range::align_range;

        use super::{Picked, Work};

        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let in_block0 = align_range(0, DISCOVERY_BLOCK_BYTES, total)?;
        let in_block1 = align_range(DISCOVERY_BLOCK_BYTES, DISCOVERY_BLOCK_BYTES, total)?;
        let mut work = Work::new(VecDeque::from(vec![in_block0, in_block1]), false);
        let (a_cov, b_cov) = (cov(2, &[0]), cov(2, &[1]));
        let a = work.add_lane(Some(a_cov.clone()), total);
        let b = work.add_lane(Some(b_cov.clone()), total);
        let missing = [(0, total)];
        let a_took = work.pick(a, total, &a_cov, true, &missing)?;
        assert!(a_took.is_some_and(|p| p.range.fetch_start() == 0));
        let b_took = work.pick(b, total, &b_cov, true, &missing)?;
        assert!(b_took.is_some_and(|p| p.range.fetch_start() == DISCOVERY_BLOCK_BYTES));

        // Lane a faults 1 MiB in: its remainder goes back, and it stops.
        let remainder = align_range(MIB, DISCOVERY_BLOCK_BYTES - MIB, total)?;
        work.clear(a)?;
        work.pending.push_back(remainder);
        work.park(a);
        assert!(
            work.uncovered(total),
            "no running lane covers the remainder"
        );

        let candidates = |work: &Work| -> Vec<Vec<usize>> {
            work.growth_wanted(total, |_| true)
                .into_iter()
                .map(|(_, lanes)| lanes)
                .collect()
        };
        work.set_no_uncovered(b, true);
        assert_eq!(
            candidates(&work),
            vec![Vec::<usize>::new()],
            "a busy lane barred from pull-through is not asked"
        );
        work.set_no_uncovered(b, false);
        assert_eq!(
            candidates(&work),
            vec![vec![b]],
            "the busy lane that does not cover the remainder is asked for it"
        );

        let extra = work.add_extra(b);
        work.set_no_uncovered(b, true);
        assert!(
            work.pick(extra, total, &b_cov, false, &[])?.is_none(),
            "the extra worker of a barred lane takes no uncovered range"
        );
        work.set_no_uncovered(b, false);
        let Some(Picked {
            range, uncovered, ..
        }) = work.pick(extra, total, &b_cov, false, &[])?
        else {
            anyhow::bail!("the extra worker takes the remainder");
        };
        assert_eq!(
            (range.fetch_start(), range.fetch_len()),
            (MIB, DISCOVERY_BLOCK_BYTES - MIB)
        );
        assert!(uncovered, "its node serves the remainder by pull-through");

        // A lane with an extra worker running is asked again for a second
        // orphan: its `LaneWiden` bounds its extra workers (#2252).
        work.pending.push_back(align_range(0, MIB, total)?);
        assert_eq!(
            candidates(&work),
            vec![vec![b]],
            "a lane running an extra stream is asked for another"
        );
        // Once the extra ends, the lane may grow again, on the same slot.
        work.clear(extra)?;
        work.end_extra(extra);
        assert_eq!(candidates(&work), vec![vec![b]]);
        let slots = work.in_flight.len();
        assert_eq!(
            work.add_extra(b),
            extra,
            "the ended extra's slot is used again"
        );
        assert_eq!(work.in_flight.len(), slots, "no slot is added");
        Ok(())
    }

    /// A partial holder that keeps refusing a block outside its coverage is
    /// asked for it at most [`ABSENT_AFTER_NOT_FOUND`] times while its bar
    /// lasts. Its node cannot serve the block by pull-through, so it is barred
    /// from it, and the whole holder serves the block once its lane builds.
    ///
    /// [`ABSENT_AFTER_NOT_FOUND`]: crate::source_set::ABSENT_AFTER_NOT_FOUND
    #[tokio::test(start_paused = true)]
    async fn a_partial_holder_refusing_an_uncovered_block_is_barred_from_it() -> anyhow::Result<()>
    {
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let data = blob(total as usize);
        let n = num_blocks(total);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?
            .paying(Arc::clone(&ledger_a))
            .refusing_blocks(&[1], || {
                anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
            });
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);
        // B's lane builds just inside the bar's time: without the bar, A
        // would be asked for block 1 again each time its cooldown ends.
        let provider = SlowBuild {
            lanes: StaticSources::new(vec![
                candidate(src_a.clone(), ledger_a, 0xA1, Some(cov(n, &[0]))),
                candidate(src_b.clone(), ledger_b, 0xB2, None),
            ])?,
            slow: Address::repeat_byte(0xB2),
            delay: crate::source_set::PULL_THROUGH_BAR.saturating_sub(Duration::from_secs(5)),
            built: std::sync::atomic::AtomicBool::new(false),
        };
        let mut set = SourceSet::new(&provider, root, Arc::default(), provider.lanes.holders());
        let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
        let drive = drive_config();
        acquire(
            AcquireTarget {
                store: &store,
                hash: root,
                total_bytes: total,
                ranges: &[(0, total)],
            },
            &mut set,
            &AcquireEnv {
                pacer: &BudgetPacer::new(),
                funder: &no_topups(),
                drive: &drive,
                max_lanes: 2,
                stop: &stop,
                on_progress: None,
                ledgers: None,
                pacing: None,
                max_blob_bytes: 0,
            },
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);

        let asked = src_a
            .opened_ranges()
            .into_iter()
            .filter(|&(start, _)| start >= DISCOVERY_BLOCK_BYTES)
            .count();
        assert!(
            (1..=3).contains(&asked),
            "A was asked for block 1 {asked} times"
        );
        assert!(
            src_b
                .opened_ranges()
                .iter()
                .any(|&(start, _)| start >= DISCOVERY_BLOCK_BYTES),
            "B serves block 1"
        );
        Ok(())
    }

    /// A sole partial holder barred from the only block left gets one probe
    /// once its bar ends, and ends the item when the probe draws the bar
    /// again and a discovery finds no one else, as a unanimous `NotFound`
    /// does.
    #[tokio::test(start_paused = true)]
    async fn a_sole_barred_partial_holder_ends_only_the_item() -> anyhow::Result<()> {
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(blob(total as usize))?
            .paying(Arc::clone(&ledger))
            .refusing_blocks(&[1], || {
                anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
            });
        let root = src.root();
        let (store, _dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![candidate(
            src.clone(),
            ledger,
            0xA1,
            Some(cov(num_blocks(total), &[0])),
        )])?;
        let err = run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            1,
            None,
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("must stop"))?;
        assert!(err.downcast_ref::<NoSourceHasBlob>().is_some(), "{err:#}");
        assert!(err.downcast_ref::<UpstreamRefused>().is_some(), "{err:#}");
        assert_eq!(classify(&err), Fault::Fatal(FatalScope::Item));
        let asked = src
            .opened_ranges()
            .into_iter()
            .filter(|&(start, _)| start >= DISCOVERY_BLOCK_BYTES)
            .count();
        assert_eq!(
            asked,
            2 * crate::source_set::ABSENT_AFTER_NOT_FOUND as usize,
            "asked for block 1 until barred, then again after the bar ended"
        );
        Ok(())
    }

    /// A pull-through target (not a probed holder) that keeps saying
    /// `NotFound` ends the item, and the stop carries the refusal.
    #[tokio::test(start_paused = true)]
    async fn unanimous_not_found_ends_only_the_item() -> anyhow::Result<()> {
        let data = blob(1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data)?
            .paying(Arc::clone(&la))
            .with_fault_after(0, || {
                anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
            });
        let (root, total) = (a.root(), a.total_bytes());
        let (store, _dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?.not_probed();
        let err = run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("must stop"))?;
        assert!(err.downcast_ref::<NoSourceHasBlob>().is_some(), "{err:#}");
        assert!(err.downcast_ref::<UpstreamRefused>().is_some(), "{err:#}");
        assert_eq!(classify(&err), Fault::Fatal(FatalScope::Item));
        Ok(())
    }

    /// A sole probed holder that says `NotFound` twice (a load shed, say) and
    /// then serves completes the fetch: its `NotFound` only cools it.
    #[tokio::test(start_paused = true)]
    async fn a_probed_holder_saying_not_found_twice_then_serving_completes() -> anyhow::Result<()> {
        let data = blob(1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data.clone())?
            .paying(Arc::clone(&la))
            .fault_times_after(2, 0, || {
                anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
            });
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            4,
            None,
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        Ok(())
    }

    /// Static lanes, one of whose builds takes `delay` and sets `built` when
    /// it finishes: a lane build in the middle of an on-chain top-up.
    struct SlowBuild {
        lanes: StaticSources<ScriptedSource>,
        slow: Address,
        delay: Duration,
        built: std::sync::atomic::AtomicBool,
    }

    impl crate::SourceProvider for SlowBuild {
        type Source = ScriptedSource;

        fn discover(&self, hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
            self.lanes.discover(hash)
        }

        fn connect<'a>(
            &'a self,
            holder: &'a crate::Holder,
        ) -> crate::SourceFuture<'a, StreamCandidate<ScriptedSource>> {
            let slow = holder.provider == self.slow;
            Box::pin(async move {
                if slow {
                    tokio::time::sleep(self.delay).await;
                    self.built.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                self.lanes.connect(holder).await
            })
        }
    }

    /// A lane build still in flight when the loop decides to return is awaited,
    /// not dropped mid-way.
    #[tokio::test(start_paused = true)]
    async fn a_lane_build_in_flight_is_awaited_before_acquire_returns() -> anyhow::Result<()> {
        let data = blob(1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&la));
        let b = ScriptedSource::new(data)?.paying(Arc::clone(&lb));
        let (root, total) = (a.root(), a.total_bytes());
        let (store, _dir) = fresh_store(root, total);
        let provider = SlowBuild {
            lanes: StaticSources::new(vec![
                candidate(a, la, 0xA1, None),
                candidate(b, lb, 0xB2, None),
            ])?,
            slow: Address::repeat_byte(0xB2),
            delay: Duration::from_secs(5),
            built: std::sync::atomic::AtomicBool::new(false),
        };
        let mut set = SourceSet::new(&provider, root, Arc::default(), provider.lanes.holders());
        let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
        let drive = drive_config();
        acquire(
            AcquireTarget {
                store: &store,
                hash: root,
                total_bytes: total,
                ranges: &[(0, total)],
            },
            &mut set,
            &AcquireEnv {
                pacer: &BudgetPacer::new(),
                funder: &no_topups(),
                drive: &drive,
                max_lanes: 2,
                stop: &stop,
                on_progress: None,
                ledgers: None,
                pacing: None,
                max_blob_bytes: 0,
            },
        )
        .await?;
        assert!(
            provider.built.load(std::sync::atomic::Ordering::SeqCst),
            "the slow build ran to its end before acquire returned"
        );
        Ok(())
    }

    /// A top-up another fetch of the run credits to a shared lane context
    /// reaches this acquire: a source priced out below the new deposit is
    /// usable again, so the acquire completes instead of stopping.
    #[tokio::test(start_paused = true)]
    async fn a_sibling_top_up_on_a_shared_lane_revives_a_priced_out_source() -> anyhow::Result<()> {
        /// Static lanes whose discovery stands in for a sibling entry: from
        /// its second call on, it tops up the shared lane context before it
        /// answers. The first call runs as the fetch starts, before any lane
        /// has run dry.
        struct SiblingTopUp {
            lanes: StaticSources<ScriptedSource>,
            ctx: Arc<Mutex<PoolContext>>,
            calls: std::sync::atomic::AtomicU32,
        }

        impl crate::SourceProvider for SiblingTopUp {
            type Source = ScriptedSource;

            fn discover(&self, hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
                if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
                    self.ctx.lock().unwrap().deposit = U256::from(200u32);
                }
                self.lanes.discover(hash)
            }

            /// The build takes a second, so the first discovery ends before
            /// any lane names the deposit.
            fn connect<'a>(
                &'a self,
                holder: &'a crate::Holder,
            ) -> crate::SourceFuture<'a, StreamCandidate<ScriptedSource>> {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    self.lanes.connect(holder).await
                })
            }
        }

        let data = blob(1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let ctx = Arc::new(Mutex::new(ctx_with(0xA1, U256::from(100u32))));
        let a = ScriptedSource::new(data.clone())?
            .paying(Arc::clone(&la))
            .fault_once_after(0, || {
                anyhow::Error::new(PoolExhausted {
                    gap_start: 0,
                    gap_len: 1,
                })
            });
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        let provider = SiblingTopUp {
            lanes: StaticSources::new(vec![candidate_ctx(a, la, Arc::clone(&ctx), None)])?,
            ctx,
            calls: std::sync::atomic::AtomicU32::new(0),
        };
        let mut set = SourceSet::new(&provider, root, Arc::default(), provider.lanes.holders());
        let stop = StopPolicy::new(false, Some(Duration::from_mins(5)), Arc::default());
        let drive = drive_config();
        acquire(
            AcquireTarget {
                store: &store,
                hash: root,
                total_bytes: total,
                ranges: &[(0, total)],
            },
            &mut set,
            &AcquireEnv {
                pacer: &BudgetPacer::new(),
                funder: &no_topups(),
                drive: &drive,
                max_lanes: 1,
                stop: &stop,
                on_progress: None,
                ledgers: None,
                pacing: None,
                max_blob_bytes: 0,
            },
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        Ok(())
    }

    /// An acquire that starts with no holder discovers one and completes.
    #[tokio::test(start_paused = true)]
    async fn an_empty_set_discovers_its_holders_and_completes() -> anyhow::Result<()> {
        let data = blob(1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&la));
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?;
        let mut set = SourceSet::new(&provider, root, Arc::default(), Vec::new());
        let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
        let drive = drive_config();
        acquire(
            AcquireTarget {
                store: &store,
                hash: root,
                total_bytes: total,
                ranges: &[(0, total)],
            },
            &mut set,
            &AcquireEnv {
                pacer: &BudgetPacer::new(),
                funder: &no_topups(),
                drive: &drive,
                max_lanes: 4,
                stop: &stop,
                on_progress: None,
                ledgers: None,
                pacing: None,
                max_blob_bytes: 0,
            },
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn only_the_listed_ranges_are_fetched() -> anyhow::Result<()> {
        let data = blob(8 * 1024 * 1024);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data)?.paying(Arc::clone(&la));
        let (root, total) = (a.root(), a.total_bytes());
        let (store, _dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None)])?;
        let mut set = SourceSet::new(&provider, root, Arc::default(), provider.holders());
        let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
        let drive = drive_config();
        let mib = 1024 * 1024;
        acquire(
            AcquireTarget {
                store: &store,
                hash: root,
                total_bytes: total,
                ranges: &[(0, mib), (4 * mib, mib)],
            },
            &mut set,
            &AcquireEnv {
                pacer: &BudgetPacer::new(),
                funder: &no_topups(),
                drive: &drive,
                max_lanes: 4,
                stop: &stop,
                on_progress: None,
                ledgers: None,
                pacing: None,
                max_blob_bytes: 0,
            },
        )
        .await?;
        assert!(
            a.opened_ranges()
                .iter()
                .all(|&(s, l)| s + l <= mib || (s >= 4 * mib && s + l <= 5 * mib)),
            "{:?}",
            a.opened_ranges()
        );
        let present = ranges_content_len(&store.present_ranges().await?, total);
        assert_eq!(present, 2 * mib, "exactly the two listed ranges landed");
        Ok(())
    }

    // ---- the size is a hint: grow and shrink the bound ----

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn a_claim_with_nothing_past_it_grows_by_one_seed() {
        use super::{SEED, grown_bound};
        let gib = 1 << 30;
        assert_eq!(grown_bound(gib, gib, 0), gib + SEED);
        // A claim off the SEED grid rounds up to it.
        let gb = 1_000_000_000;
        let want = (gb + SEED).div_ceil(SEED) * SEED;
        assert_eq!(grown_bound(gb, gb, 0), want);
        assert_eq!(want % SEED, 0);
    }

    #[test]
    fn bytes_past_the_claim_grow_the_bound_twice_as_far() {
        use super::{SEED, grown_bound};
        let (gb, mb): (u64, u64) = (1_000_000_000, 1_000_000);
        let want = (gb + 100 * mb).div_ceil(SEED) * SEED;
        assert_eq!(grown_bound(gb, gb, 50 * mb), want);
    }

    #[test]
    fn a_grown_bound_saturates_near_the_top() {
        use super::grown_bound;
        assert_eq!(grown_bound(u64::MAX - 10, 0, 0), u64::MAX);
        assert_eq!(grown_bound(1, 0, u64::MAX), u64::MAX);
        assert_eq!(grown_bound(1, u64::MAX, 1), u64::MAX);
    }

    /// The bounds the growth rule steps through from a first claim of `c0`
    /// while every source honestly serves a `truth`-byte blob: each round
    /// fetches up to the bound, so everything past `c0` below it is present.
    fn predicted_growth(c0: u64, truth: u64) -> Vec<u64> {
        let mut bounds = Vec::new();
        let mut bound = c0;
        while bound < truth {
            bound = super::grown_bound(bound, c0, bound.saturating_sub(c0));
            bounds.push(bound);
        }
        bounds
    }

    /// Acquire the whole blob into `store` from `provider`, recording every
    /// `(position, total)` the progress callback reports.
    async fn acquire_recording<S: BlobSource>(
        store: &ClientRangedStore,
        provider: &StaticSources<S>,
        root: [u8; 32],
        lanes: usize,
    ) -> (anyhow::Result<()>, Vec<(u64, u64)>) {
        let samples: Arc<Mutex<Vec<(u64, u64)>>> = Arc::default();
        let cb_samples = Arc::clone(&samples);
        let on_progress: Box<crate::ProgressCallback> = Box::new(move |position, total| {
            if let Ok(mut s) = cb_samples.lock() {
                s.push((position, total));
            }
        });
        let result = run_acquire_with(
            store,
            provider,
            root,
            &BudgetPacer::new(),
            &no_topups(),
            Knobs {
                on_progress: Some(&on_progress),
                ..Knobs::lanes(lanes)
            },
        )
        .await;
        let samples = samples.lock().map(|s| s.clone()).unwrap_or_default();
        (result, samples)
    }

    /// Honest lanes over `data`, one per provider byte in `providers`.
    fn honest_lanes(
        data: &[u8],
        providers: &[u8],
    ) -> anyhow::Result<(Vec<ScriptedSource>, StaticSources<ScriptedSource>)> {
        let mut sources = Vec::new();
        let mut candidates = Vec::new();
        for &p in providers {
            let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
            let src = ScriptedSource::new(data.to_vec())?.paying(Arc::clone(&ledger));
            candidates.push(candidate(src.clone(), ledger, p, None));
            sources.push(src);
        }
        Ok((sources, StaticSources::new(candidates)?))
    }

    /// A first claim far below the blob grows by the rule until a leg
    /// verifies the true final chunk, and the fetch completes byte-identical.
    #[tokio::test(start_paused = true)]
    async fn a_too_small_claim_grows_until_the_true_end_is_proven() -> anyhow::Result<()> {
        let (c0, truth) = (MIB, 70 * MIB);
        let data = blob(truth as usize);
        let (sources, provider) = honest_lanes(&data, &[0xA1, 0xB2])?;
        let root = sources
            .first()
            .map(ScriptedSource::root)
            .unwrap_or_default();
        let (store, dir) = fresh_store(root, c0);
        let (result, samples) = acquire_recording(&store, &provider, root, 2).await;
        result?;
        assert_eq!(store.proven(), Some(truth));
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        let grown: std::collections::BTreeSet<u64> = samples
            .iter()
            .map(|&(_, total)| total)
            .filter(|&total| total != c0 && total != truth)
            .collect();
        let predicted: std::collections::BTreeSet<u64> = predicted_growth(c0, truth)
            .into_iter()
            .filter(|&b| b != truth)
            .collect();
        assert_eq!(grown, predicted, "one growth round per the rule");
        assert_eq!(predicted.len(), 1);
        Ok(())
    }

    /// A first claim past the blob shrinks once a leg verifies the true final
    /// chunk: the fetch completes and the bound ends at the proven size.
    #[tokio::test(start_paused = true)]
    async fn a_too_big_claim_shrinks_when_a_leg_proves_the_end() -> anyhow::Result<()> {
        let (c0, truth) = (200 * MIB, 70 * MIB);
        let data = blob(truth as usize);
        let (sources, provider) = honest_lanes(&data, &[0xA1, 0xB2])?;
        let root = sources
            .first()
            .map(ScriptedSource::root)
            .unwrap_or_default();
        let (store, dir) = fresh_store(root, c0);
        let (result, _) = acquire_recording(&store, &provider, root, 2).await;
        result?;
        assert_eq!(store.proven(), Some(truth));
        assert_eq!(store.bound(), truth, "the bound ends at the proven size");
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        Ok(())
    }

    /// A proven size clips the work: nothing at or past it stays pending, a
    /// range across it is cut there, and a lane whose range starts past it is
    /// cancelled.
    #[test]
    fn a_proven_size_clips_pending_and_in_flight_work() -> anyhow::Result<()> {
        use std::collections::VecDeque;
        use std::sync::atomic::Ordering;

        use decdn_bao_range::align_range;

        let old = 200 * MIB;
        let proven = 70 * MIB;
        let pending = VecDeque::from(vec![
            align_range(0, 10 * MIB, old)?,
            align_range(60 * MIB, 20 * MIB, old)?,
            align_range(100 * MIB, 20 * MIB, old)?,
        ]);
        let mut work = super::Work::new(pending, false);
        let past = work.add_lane(None, old);
        let across = work.add_lane(None, old);
        if let Some(slot) = work.in_flight.get_mut(past) {
            *slot = Some((90 * MIB, 10 * MIB));
        }
        if let Some(slot) = work.in_flight.get_mut(across) {
            *slot = Some((65 * MIB, 10 * MIB));
        }
        work.clip(proven)?;
        let pending: Vec<(u64, u64)> = work
            .pending
            .iter()
            .map(|seg| (seg.fetch_start(), seg.fetch_end()))
            .collect();
        assert_eq!(pending, vec![(0, 10 * MIB), (60 * MIB, proven)]);
        assert!(pending.iter().all(|&(_, end)| end <= proven));
        assert_eq!(
            work.in_flight.get(across).copied().flatten(),
            Some((65 * MIB, 5 * MIB))
        );
        let cancelled = |i: usize| {
            work.cancel
                .get(i)
                .is_some_and(|h| h.flag.load(Ordering::Acquire))
        };
        assert!(cancelled(past), "a lane past the proven size stops");
        assert!(!cancelled(across), "a lane across it finishes at it");
        Ok(())
    }

    /// A source that signs a size one byte short is never believed: its size
    /// is only the first claim, the honest sources prove the true one, and the
    /// fetch completes.
    #[tokio::test(start_paused = true)]
    async fn a_lying_small_first_claim_never_fails_the_fetch() -> anyhow::Result<()> {
        let truth = 3 * MIB + 12_345;
        let data = blob(truth as usize);
        let mut candidates = Vec::new();
        let mut root = [0; 32];
        for (p, signs) in [(0xA1, truth - 1), (0xB2, truth), (0xC3, truth)] {
            let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
            let src = ScriptedSource::new(data.clone())?
                .signing_size(signs)
                .paying(Arc::clone(&ledger));
            root = src.root();
            candidates.push(candidate(src, ledger, p, None));
        }
        let provider = StaticSources::new(candidates)?;
        let (store, dir) = fresh_store(root, truth - 1);
        let (result, _) = acquire_recording(&store, &provider, root, 3).await;
        result?;
        assert_eq!(store.proven(), Some(truth));
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        Ok(())
    }

    /// Coverage is a preference (#2225): two partial holders whose coverage
    /// leaves the last block to nobody still complete the fetch. One of them
    /// takes that block, served by pull-through, while the other is still on
    /// its own covered block.
    #[tokio::test(start_paused = true)]
    async fn blocks_no_lane_covers_are_taken_by_a_partial_holder() -> anyhow::Result<()> {
        let total = 2 * DISCOVERY_BLOCK_BYTES + MIB;
        let data = blob(total as usize);
        let n = num_blocks(total);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?
            .slow_to_start(Duration::from_secs(5))
            .paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);
        let provider = StaticSources::new(vec![
            candidate(src_a.clone(), ledger_a, 0xA1, Some(cov(n, &[0]))),
            candidate(src_b.clone(), ledger_b, 0xB2, Some(cov(n, &[1]))),
        ])?;
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_topups(),
            2,
            None,
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);

        let last_block = 2 * DISCOVERY_BLOCK_BYTES;
        let took_last = src_a
            .timeline()
            .into_iter()
            .chain(src_b.timeline())
            .filter(|&(start, _, _)| start >= last_block)
            .map(|(_, opened, _)| opened)
            .min()
            .ok_or_else(|| anyhow::anyhow!("nobody opened the uncovered block"))?;
        let covered_done = src_b
            .timeline()
            .into_iter()
            .filter_map(|(start, _, finished)| (start < last_block).then_some(finished).flatten())
            .max()
            .ok_or_else(|| anyhow::anyhow!("B finished no covered leg"))?;
        assert!(
            took_last < covered_done,
            "the uncovered block started before the covered work ended"
        );
        Ok(())
    }

    // ---- the caller's size cap bounds every claim ----

    /// Acquire `data`'s blob from one honest lane into a store sized by
    /// `claim`, under `cap`. Returns the result, every progress total, and
    /// the store.
    async fn acquire_capped(
        data: &[u8],
        claim: u64,
        cap: u64,
    ) -> anyhow::Result<(
        anyhow::Result<()>,
        Vec<u64>,
        ClientRangedStore,
        tempfile::TempDir,
    )> {
        let (sources, provider) = honest_lanes(data, &[0xA1])?;
        let root = sources
            .first()
            .map(ScriptedSource::root)
            .unwrap_or_default();
        let (store, dir) = fresh_store(root, claim);
        let totals: Arc<Mutex<Vec<u64>>> = Arc::default();
        let seen = Arc::clone(&totals);
        let on_progress: Box<crate::ProgressCallback> = Box::new(move |_, total| {
            if let Ok(mut t) = seen.lock() {
                t.push(total);
            }
        });
        let result = run_acquire_with(
            &store,
            &provider,
            root,
            &BudgetPacer::new(),
            &no_topups(),
            Knobs {
                on_progress: Some(&on_progress),
                max_blob_bytes: cap,
                ..Knobs::lanes(1)
            },
        )
        .await;
        let totals = totals.lock().map(|t| t.clone()).unwrap_or_default();
        Ok((result, totals, store, dir))
    }

    /// A first claim above the cap is clamped to it before any work is sized:
    /// no bound past the cap is ever planned, and a blob within the cap
    /// completes.
    #[tokio::test(start_paused = true)]
    async fn a_claim_above_the_cap_is_clamped_to_it() -> anyhow::Result<()> {
        let data = blob(3 * MIB as usize);
        let cap = 8 * MIB;
        let (result, totals, store, dir) = acquire_capped(&data, u64::MAX / 2, cap).await?;
        result?;
        assert!(totals.iter().all(|&t| t <= cap), "{totals:?}");
        assert_eq!(store.proven(), Some(3 * MIB));
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        Ok(())
    }

    /// Growth never raises the bound past the cap: a short claim grows to the
    /// cap at most, and a blob within it completes.
    #[tokio::test(start_paused = true)]
    async fn growth_stops_at_the_cap() -> anyhow::Result<()> {
        let data = blob(3 * MIB as usize);
        let cap = 5 * MIB;
        let (result, totals, store, _dir) = acquire_capped(&data, MIB, cap).await?;
        result?;
        assert!(totals.iter().all(|&t| t <= cap), "{totals:?}");
        assert!(
            totals.contains(&cap),
            "the claim grew to the cap: {totals:?}"
        );
        assert_eq!(store.proven(), Some(3 * MIB));
        Ok(())
    }

    /// A blob that holds bytes past the cap ends the item with `BlobTooLarge`
    /// once the bound reaches the cap with no size proven, whether the claim
    /// was short or above the cap.
    #[tokio::test(start_paused = true)]
    async fn a_blob_past_the_cap_ends_the_item() -> anyhow::Result<()> {
        let data = blob(3 * MIB as usize);
        let cap = 2 * MIB;
        for claim in [MIB, 3 * MIB] {
            let (result, totals, _store, _dir) = acquire_capped(&data, claim, cap).await?;
            let err = result
                .err()
                .ok_or_else(|| anyhow::anyhow!("a blob past the cap must fail"))?;
            assert!(
                err.downcast_ref::<crate::BlobTooLarge>().is_some(),
                "{err:#}"
            );
            assert_eq!(classify(&err), Fault::Fatal(FatalScope::Item));
            assert!(totals.iter().all(|&t| t <= cap), "{totals:?}");
        }
        Ok(())
    }

    /// A blob exactly at the cap completes: the cap is a ceiling, not a
    /// bound it must stay under.
    #[tokio::test(start_paused = true)]
    async fn a_blob_exactly_at_the_cap_completes() -> anyhow::Result<()> {
        let data = blob(2 * MIB as usize);
        let (result, _, store, _dir) = acquire_capped(&data, 3 * MIB, 2 * MIB).await?;
        result?;
        assert_eq!(store.proven(), Some(2 * MIB));
        Ok(())
    }

    /// A cap off the chunk-group grid: a leg that ends at the cap is served to
    /// its group's end and can prove a size past the cap. That proven size
    /// still ends the item with `BlobTooLarge`.
    #[tokio::test(start_paused = true)]
    async fn a_proven_size_past_an_unaligned_cap_ends_the_item() -> anyhow::Result<()> {
        let data = blob(2 * MIB as usize + 100);
        let cap = 2 * MIB + 50;
        let (result, _, store, _dir) = acquire_capped(&data, 3 * MIB, cap).await?;
        let err = result
            .err()
            .ok_or_else(|| anyhow::anyhow!("a proven size past the cap must fail"))?;
        assert!(
            err.downcast_ref::<crate::BlobTooLarge>().is_some(),
            "{err:#}"
        );
        assert_eq!(classify(&err), Fault::Fatal(FatalScope::Item));
        assert_eq!(store.proven(), Some(2 * MIB + 100));
        Ok(())
    }

    // ---- a busy lane grows for a faulted lane's remainder (#2231) ----

    /// Counts what a [`super::LaneWiden`] granted and got back.
    #[derive(Default)]
    struct WidenCount {
        granted: std::sync::atomic::AtomicUsize,
        released: std::sync::atomic::AtomicUsize,
        /// The most grants held at once.
        peak: std::sync::atomic::AtomicUsize,
        /// Each `grow` call's kind and whether it granted, in call order.
        calls: Mutex<Vec<(super::GrowFor, bool)>>,
    }

    impl WidenCount {
        fn granted(&self) -> usize {
            self.granted.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn released(&self) -> usize {
            self.released.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn peak(&self) -> usize {
            self.peak.load(std::sync::atomic::Ordering::SeqCst)
        }

        /// Each `grow` call's kind and whether it granted, in call order.
        fn calls(&self) -> Vec<(super::GrowFor, bool)> {
            self.calls.lock().map(|c| c.clone()).unwrap_or_default()
        }

        /// Count one `grow` call of `kind` that granted a stream or not.
        fn grew(&self, kind: super::GrowFor, granted: bool) {
            use std::sync::atomic::Ordering;
            self.granted
                .fetch_add(usize::from(granted), Ordering::SeqCst);
            let held = self.granted().saturating_sub(self.released());
            self.peak.fetch_max(held, Ordering::SeqCst);
            if let Ok(mut calls) = self.calls.lock() {
                calls.push((kind, granted));
            }
        }
    }

    /// A lane's widen hooks over `room` stream permits, like the CLI's permit
    /// grant: `grow` hands out only what is free now, and `release` frees one.
    fn counting_widen(room: usize) -> (super::LaneWiden, Arc<WidenCount>) {
        use std::sync::atomic::Ordering;
        let count = Arc::new(WidenCount::default());
        let (on_grow, on_release) = (Arc::clone(&count), Arc::clone(&count));
        let widen = super::LaneWiden::new(
            move |kind| {
                let held = on_grow.granted().saturating_sub(on_grow.released());
                let give = held < room;
                on_grow.grew(kind, give);
                give
            },
            move || {
                on_release.released.fetch_add(1, Ordering::SeqCst);
            },
        );
        (widen, count)
    }

    /// Static lanes that report every source fault the set records.
    struct FaultLog<'a> {
        inner: &'a StaticSources<ScriptedSource>,
        faulted: Mutex<Vec<Address>>,
    }

    impl crate::SourceProvider for FaultLog<'_> {
        type Source = ScriptedSource;

        fn discover(&self, hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
            self.inner.discover(hash)
        }

        fn connect<'b>(
            &'b self,
            holder: &'b crate::Holder,
        ) -> crate::SourceFuture<'b, StreamCandidate<ScriptedSource>> {
            self.inner.connect(holder)
        }

        fn on_source_fault(&self, holder: &crate::Holder) {
            if let Ok(mut f) = self.faulted.lock() {
                f.push(holder.provider);
            }
        }
    }

    /// Static lanes plus holders pushed after the start, each after its own
    /// delay, and a count of discovery calls. Discovery returns `found`.
    struct Arriving<'a> {
        inner: &'a StaticSources<ScriptedSource>,
        late: Mutex<Option<Vec<(Duration, crate::Holder)>>>,
        found: Vec<crate::Holder>,
        discovers: std::sync::atomic::AtomicUsize,
    }

    impl<'a> Arriving<'a> {
        fn new(
            inner: &'a StaticSources<ScriptedSource>,
            late: Vec<(Duration, crate::Holder)>,
            found: Vec<crate::Holder>,
        ) -> Self {
            Self {
                inner,
                late: Mutex::new(Some(late)),
                found,
                discovers: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn discovers(&self) -> usize {
            self.discovers.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl crate::SourceProvider for Arriving<'_> {
        type Source = ScriptedSource;

        fn discover(&self, _hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
            self.discovers
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let found = self.found.clone();
            Box::pin(async move { Ok(found) })
        }

        fn connect<'b>(
            &'b self,
            holder: &'b crate::Holder,
        ) -> crate::SourceFuture<'b, StreamCandidate<ScriptedSource>> {
            self.inner.connect(holder)
        }

        fn arrivals(&self) -> Option<crate::SourceStream<'_, crate::Holder>> {
            use futures_util::StreamExt as _;
            let late = self.late.lock().ok()?.take()?;
            Some(Box::pin(futures_util::stream::iter(late).then(
                |(after, holder)| async move {
                    tokio::time::sleep(after).await;
                    holder
                },
            )))
        }
    }

    /// `provider`'s static holder for the lane built with `byte`.
    fn holder_of(
        provider: &StaticSources<ScriptedSource>,
        byte: u8,
    ) -> anyhow::Result<crate::Holder> {
        provider
            .holders()
            .into_iter()
            .find(|h| h.provider == Address::repeat_byte(byte))
            .ok_or_else(|| anyhow::anyhow!("no holder {byte:#x}"))
    }

    /// A holder pushed one second in takes a free lane beside a busy one, and
    /// both lanes deliver.
    #[tokio::test(start_paused = true)]
    async fn a_late_holder_takes_a_free_lane() -> anyhow::Result<()> {
        let data = blob(32 * MIB as usize);
        let (la, lb) = (
            Arc::new(PoolLedger::new(Cumulative::default())),
            Arc::new(PoolLedger::new(Cumulative::default())),
        );
        let a = busy(&data, &la)?;
        let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
        let (root, total) = (a.root(), a.total_bytes());
        let lanes = StaticSources::new(vec![
            candidate(a.clone(), la, 0xA1, None),
            candidate(b.clone(), lb, 0xB2, None),
        ])?;
        let provider = Arriving::new(
            &lanes,
            vec![(Duration::from_secs(1), holder_of(&lanes, 0xB2)?)],
            Vec::new(),
        );
        let (store, dir) = fresh_store(root, total);
        acquire_over(&store, &provider, vec![holder_of(&lanes, 0xA1)?], root, 2).await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        assert!(a.delivered_bytes() > 0);
        assert!(b.delivered_bytes() > 0, "the late holder delivered");
        Ok(())
    }

    /// The only starting holder refuses every stream. With a holder still to
    /// arrive, the loop asks for no discovery and completes from the arrival.
    #[tokio::test(start_paused = true)]
    async fn a_late_holder_rescues_a_faulted_start() -> anyhow::Result<()> {
        let data = blob(8 * MIB as usize);
        let (la, lb) = (
            Arc::new(PoolLedger::new(Cumulative::default())),
            Arc::new(PoolLedger::new(Cumulative::default())),
        );
        let a = ScriptedSource::new(data.clone())?
            .refusing_opens_from(0, || anyhow::anyhow!("scripted refusal"))
            .paying(Arc::clone(&la));
        let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
        let (root, total) = (a.root(), a.total_bytes());
        let lanes = StaticSources::new(vec![
            candidate(a, la, 0xA1, None),
            candidate(b, lb, 0xB2, None),
        ])?;
        let provider = Arriving::new(
            &lanes,
            vec![(Duration::from_secs(2), holder_of(&lanes, 0xB2)?)],
            Vec::new(),
        );
        let (store, dir) = fresh_store(root, total);
        acquire_over(&store, &provider, vec![holder_of(&lanes, 0xA1)?], root, 1).await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        assert_eq!(
            provider.discovers(),
            0,
            "no discovery while arrivals are open"
        );
        Ok(())
    }

    /// Arrivals that end with nothing hand the loop back to discovery.
    #[tokio::test(start_paused = true)]
    async fn discovery_resumes_once_arrivals_end() -> anyhow::Result<()> {
        let data = blob(8 * MIB as usize);
        let (la, lb) = (
            Arc::new(PoolLedger::new(Cumulative::default())),
            Arc::new(PoolLedger::new(Cumulative::default())),
        );
        let a = ScriptedSource::new(data.clone())?
            .refusing_opens_from(0, || anyhow::anyhow!("scripted refusal"))
            .paying(Arc::clone(&la));
        let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
        let (root, total) = (a.root(), a.total_bytes());
        let lanes = StaticSources::new(vec![
            candidate(a, la, 0xA1, None),
            candidate(b, lb, 0xB2, None),
        ])?;
        let provider = Arriving::new(&lanes, Vec::new(), vec![holder_of(&lanes, 0xB2)?]);
        let (store, dir) = fresh_store(root, total);
        acquire_over(&store, &provider, vec![holder_of(&lanes, 0xA1)?], root, 1).await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        assert!(
            provider.discovers() >= 1,
            "discovery ran once arrivals ended"
        );
        Ok(())
    }

    /// Acquire the whole blob into `store` over `provider`'s lanes, with the
    /// lane watchdog on.
    async fn acquire_over<P: crate::SourceProvider>(
        store: &ClientRangedStore,
        provider: &P,
        holders: Vec<crate::Holder>,
        root: [u8; 32],
        lanes: usize,
    ) -> anyhow::Result<()> {
        let total = store.total_bytes();
        let mut set = SourceSet::new(provider, root, Arc::default(), holders);
        let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
        let drive = drive_config();
        let (pacer, funder) = (BudgetPacer::new(), no_topups());
        let whole = [(0, total)];
        acquire(
            AcquireTarget {
                store,
                hash: root,
                total_bytes: total,
                ranges: &whole,
            },
            &mut set,
            &AcquireEnv {
                pacer: &pacer,
                funder: &funder,
                drive: &drive,
                max_lanes: lanes,
                stop: &stop,
                on_progress: None,
                ledgers: None,
                pacing: None,
                max_blob_bytes: 0,
            },
        )
        .await
    }

    /// A lane that dies after its first MiB and refuses every later stream.
    fn dead_after_first_mib(
        data: &[u8],
        ledger: &Arc<PoolLedger>,
    ) -> anyhow::Result<ScriptedSource> {
        Ok(ScriptedSource::new(data.to_vec())?
            .with_fault_after(MIB as usize, || anyhow::anyhow!("scripted reset"))
            .refusing_opens_from(1, || anyhow::anyhow!("scripted refusal"))
            .paying(Arc::clone(ledger)))
    }

    /// A lane busy with a long range: every leg pauses 5 s after its first MiB.
    fn busy(data: &[u8], ledger: &Arc<PoolLedger>) -> anyhow::Result<ScriptedSource> {
        Ok(ScriptedSource::new(data.to_vec())?
            .stall_after(MIB, Duration::from_secs(5))
            .paying(Arc::clone(ledger)))
    }

    /// Three lanes, no reserve: one node dies mid-range while the other two
    /// are busy with long ranges. A busy lane that covers the dead node's
    /// remainder takes it on one extra stream, so the remainder starts before
    /// either busy lane finishes its own range, and the grant comes back.
    #[tokio::test(start_paused = true)]
    async fn a_dead_nodes_remainder_is_taken_by_a_busy_covering_lane() -> anyhow::Result<()> {
        let data = blob(48 * MIB as usize);
        let ledgers: Vec<Arc<PoolLedger>> = (0..3)
            .map(|_| Arc::new(PoolLedger::new(Cumulative::default())))
            .collect();
        let [la, lb, lc] = ledgers.as_slice() else {
            anyhow::bail!("three ledgers");
        };
        let a = dead_after_first_mib(&data, la)?;
        let b = busy(&data, lb)?;
        let c = busy(&data, lc)?;
        let (widen_b, count_b) = counting_widen(1);
        let (widen_c, count_c) = counting_widen(1);
        let mut cand_b = candidate(b.clone(), Arc::clone(lb), 0xB2, None);
        cand_b.widen = Some(widen_b);
        let mut cand_c = candidate(c.clone(), Arc::clone(lc), 0xC3, None);
        cand_c.widen = Some(widen_c);
        let provider = StaticSources::new(vec![
            candidate(a.clone(), Arc::clone(la), 0xA1, None),
            cand_b,
            cand_c,
        ])?;
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        acquire_over(&store, &provider, provider.holders(), root, 3).await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);

        let (dead_start, _, _) = a
            .timeline()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("A opened its range"))?;
        let dead_end = dead_start + total / 3;
        let remainder_opened = b
            .timeline()
            .into_iter()
            .chain(c.timeline())
            .filter(|&(start, _, _)| start > dead_start && start < dead_end)
            .map(|(_, opened, _)| opened)
            .min()
            .ok_or_else(|| anyhow::anyhow!("a busy lane took the remainder"))?;
        let first_finish = |src: &ScriptedSource| {
            src.timeline()
                .first()
                .and_then(|&(_, _, finished)| finished)
        };
        let busy_done = first_finish(&b)
            .into_iter()
            .chain(first_finish(&c))
            .min()
            .ok_or_else(|| anyhow::anyhow!("the busy lanes finished their ranges"))?;
        assert!(
            remainder_opened < busy_done,
            "the remainder started before either busy lane finished its own range"
        );
        let granted = count_b.granted() + count_c.granted();
        assert!(granted >= 1, "a busy lane grew one extra stream");
        assert_eq!(
            count_b.released() + count_c.released(),
            granted,
            "every extra stream gave its grant back"
        );
        Ok(())
    }

    /// A node that refuses the extra stream is not cooled for it: the refusal
    /// stops only that stream, the lane's own worker takes the range once its
    /// own range ends, the fetch completes, and the grant comes back.
    #[tokio::test(start_paused = true)]
    async fn a_fault_on_an_extra_stream_does_not_cool_the_node() -> anyhow::Result<()> {
        let data = blob(32 * MIB as usize);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = dead_after_first_mib(&data, &la)?;
        // B's second open is the extra stream: it refuses it.
        let b = busy(&data, &lb)?.refusing_open(1, || anyhow::anyhow!("scripted overload"));
        let (widen, count) = counting_widen(1);
        let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
        cand_b.widen = Some(widen);
        let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
        let provider = FaultLog {
            inner: &lanes,
            faulted: Mutex::new(Vec::new()),
        };
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        acquire_over(&store, &provider, lanes.holders(), root, 2).await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);

        // A returns after its cooldown, faults again, and B may be asked again
        // once its first extra stream has ended: at most one at a time.
        assert!(count.granted() >= 1, "B grew an extra stream");
        assert_eq!(
            count.released(),
            count.granted(),
            "every grant came back, the refused one included"
        );
        let faulted = provider
            .faulted
            .lock()
            .map(|f| f.clone())
            .unwrap_or_default();
        assert!(
            !faulted.contains(&Address::repeat_byte(0xB2)),
            "the refused extra stream must not cool B: {faulted:?}"
        );
        assert!(
            faulted.contains(&Address::repeat_byte(0xA1)),
            "A's death is A's"
        );
        Ok(())
    }

    /// Open `gate` once `ready` holds, checking each virtual millisecond.
    async fn open_when(ready: impl Fn() -> bool, gate: &tokio::sync::watch::Sender<bool>) {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        gate.send_replace(true);
    }

    /// How many times `provider` recorded a fault for the node `byte` names.
    fn faults_of(provider: &FaultLog<'_>, byte: u8) -> usize {
        provider
            .faulted
            .lock()
            .map(|f| {
                f.iter()
                    .filter(|&&p| p == Address::repeat_byte(byte))
                    .count()
            })
            .unwrap_or_default()
    }

    /// Acquire over `provider` while `release` opens the scenario's gates in
    /// order. A gate that never opens ends the test with an error.
    async fn acquire_gated(
        store: &ClientRangedStore,
        provider: &FaultLog<'_>,
        root: [u8; 32],
        release: impl std::future::Future<Output = ()>,
    ) -> anyhow::Result<()> {
        let fetch = acquire_over(store, provider, provider.inner.holders(), root, 2);
        let (fetched, ()) = tokio::time::timeout(
            Duration::from_mins(5),
            futures_util::future::join(fetch, release),
        )
        .await
        .map_err(|_| anyhow::anyhow!("the fetch ended or stalled before a gate opened"))?;
        fetched
    }

    /// A lane's last live worker faults for the lane (#2231): when the lane's
    /// own worker has faulted and stopped, a fault on its extra stream cools
    /// the node too.
    #[tokio::test(start_paused = true)]
    async fn an_extra_stream_that_outlives_its_lane_faults_for_the_lane() -> anyhow::Result<()> {
        let data = blob(32 * MIB as usize);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = dead_after_first_mib(&data, &la)?;
        // B's first two readers (its own range, then the extra stream opened
        // for A's remainder) fault after 4 MiB. The own leg's fault waits
        // until the extra stream has opened, and the extra's fault waits
        // until the own worker's fault is recorded, so the extra faults with
        // no own worker left.
        let (own_gate, own_held) = tokio::sync::watch::channel(false);
        let (extra_gate, extra_held) = tokio::sync::watch::channel(false);
        let b = ScriptedSource::new(data.clone())?
            .fault_times_after(2, 4 * MIB as usize, || anyhow::anyhow!("scripted reset"))
            .holding_fault(0, own_held)
            .holding_fault(1, extra_held)
            .paying(Arc::clone(&lb));
        let (widen, count) = counting_widen(1);
        let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
        cand_b.widen = Some(widen);
        let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
        let provider = FaultLog {
            inner: &lanes,
            faulted: Mutex::new(Vec::new()),
        };
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        let release = async {
            open_when(|| b.opened_ranges().len() >= 2, &own_gate).await;
            open_when(|| faults_of(&provider, 0xB2) >= 1, &extra_gate).await;
        };
        acquire_gated(&store, &provider, root, release).await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);

        assert!(count.granted() >= 1, "B grew an extra stream");
        assert_eq!(count.released(), count.granted());
        assert_eq!(
            faults_of(&provider, 0xB2),
            2,
            "the own worker's fault and the last live worker's fault both cool B"
        );
        Ok(())
    }

    /// A node is charged once per outage: after its lane's own worker faulted
    /// and was charged, its extra stream refused with no verified byte in
    /// between does not charge it again.
    #[tokio::test(start_paused = true)]
    async fn an_outage_charges_the_node_once() -> anyhow::Result<()> {
        let data = blob(32 * MIB as usize);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = dead_after_first_mib(&data, &la)?;
        // B's own leg faults after 4 MiB, once the extra stream for A's
        // remainder has opened. The extra is refused only once the own
        // worker's fault is recorded, with no byte in between.
        let (own_gate, own_held) = tokio::sync::watch::channel(false);
        let (extra_gate, extra_held) = tokio::sync::watch::channel(false);
        let b = ScriptedSource::new(data.clone())?
            .fault_once_after(4 * MIB as usize, || anyhow::anyhow!("scripted reset"))
            .refusing_open(1, || anyhow::anyhow!("scripted overload"))
            .holding_fault(0, own_held)
            .holding_fault(1, extra_held)
            .paying(Arc::clone(&lb));
        let (widen, count) = counting_widen(1);
        let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
        cand_b.widen = Some(widen);
        let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
        let provider = FaultLog {
            inner: &lanes,
            faulted: Mutex::new(Vec::new()),
        };
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        let release = async {
            open_when(|| b.opened_ranges().len() >= 2, &own_gate).await;
            open_when(|| faults_of(&provider, 0xB2) >= 1, &extra_gate).await;
        };
        acquire_gated(&store, &provider, root, release).await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);

        assert!(count.granted() >= 1, "B grew an extra stream");
        assert_eq!(count.released(), count.granted());
        assert_eq!(faults_of(&provider, 0xB2), 1, "one outage, one charge");
        Ok(())
    }

    // ---- growth retries, and a lane's lease ends with its worker (#2230) ----

    /// A lane's widen hooks over `free`, stream permits it shares with
    /// sibling fetches the test plays: `grow` takes only permits free now,
    /// and `release` puts one back. An extra stream leaves `keep` permits
    /// free, as the CLI keeps one back for a sibling's first stream; a
    /// restart may take the last one.
    fn shared_widen(
        free: &Arc<std::sync::atomic::AtomicUsize>,
        keep: usize,
    ) -> (super::LaneWiden, Arc<WidenCount>) {
        use std::sync::atomic::Ordering;
        let count = Arc::new(WidenCount::default());
        let (on_grow, on_release) = (Arc::clone(&count), Arc::clone(&count));
        let (take, give) = (Arc::clone(free), Arc::clone(free));
        let widen = super::LaneWiden::new(
            move |kind| {
                let keep = match kind {
                    super::GrowFor::Restart => 0,
                    super::GrowFor::Extra => keep,
                };
                let taken = take
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                        (n > keep).then(|| n.saturating_sub(1))
                    })
                    .is_ok();
                on_grow.grew(kind, taken);
                taken
            },
            move || {
                give.fetch_add(1, Ordering::SeqCst);
                on_release.released.fetch_add(1, Ordering::SeqCst);
            },
        );
        (widen, count)
    }

    /// When a node dies, the busy lane has no free stream, and a sibling
    /// fetch frees one later, the loop asks for growth again on its retry
    /// clock: the remainder starts on an extra stream before the dead node
    /// comes back from its cooldown to fault again.
    #[tokio::test(start_paused = true)]
    async fn growth_is_asked_again_once_a_stream_frees() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let data = blob(32 * MIB as usize);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = dead_after_first_mib(&data, &la)?;
        let b = busy(&data, &lb)?;
        let free = Arc::new(AtomicUsize::new(0));
        let (widen, count) = shared_widen(&free, 0);
        let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
        cand_b.widen = Some(widen);
        let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
        let provider = FaultLog {
            inner: &lanes,
            faulted: Mutex::new(Vec::new()),
        };
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        // A sibling fetch gives its permit back a while after A's fault, when
        // the growth request A's fault made has already found none.
        let release = async {
            while faults_of(&provider, 0xA1) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            free.fetch_add(1, Ordering::SeqCst);
        };
        acquire_gated(&store, &provider, root, release).await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);

        let (dead_start, _, _) = a
            .timeline()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("A opened its range"))?;
        let dead_end = dead_start + total / 2;
        let extra_opened = b
            .timeline()
            .into_iter()
            .skip(1)
            .filter(|&(start, _, _)| start > dead_start && start < dead_end)
            .map(|(_, opened, _)| opened)
            .min()
            .ok_or_else(|| anyhow::anyhow!("B took A's remainder"))?;
        let (_, refault, _) = a
            .timeline()
            .get(1)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("A came back from its cooldown"))?;
        assert!(
            extra_opened < refault,
            "the remainder started on the retry clock, before A came back to fault again"
        );
        assert!(count.granted() >= 1, "B grew an extra stream");
        assert_eq!(count.released(), count.granted());
        Ok(())
    }

    /// A pass arms the growth retry when a `grow` granted nothing, or when a
    /// range had no lane to ask while a running lane has a [`LaneWiden`]. A
    /// fetch with no `LaneWiden`, such as `decdn fetch`, arms none for a range
    /// with no lane to ask, and a pass with nothing waiting clears the wait.
    #[test]
    fn a_pass_retries_while_a_lane_can_still_grow() {
        use super::{GROWTH_RETRY, Growth, GrowthPass};
        let pass = |refused, no_candidate, growable| GrowthPass {
            waiting: vec![(0, MIB)],
            refused,
            no_candidate,
            growable,
        };
        let now = super::Instant::now();
        let cases = [
            (pass(0, 1, false), None),
            (pass(0, 1, true), Some(now + GROWTH_RETRY)),
            (pass(1, 0, false), Some(now + GROWTH_RETRY)),
        ];
        for (p, want) in cases {
            let mut growth = Growth::new(now);
            growth.passed(now, [0; 32], p);
            assert_eq!(growth.retry_at, want);
        }
        let mut growth = Growth::new(now);
        growth.passed(now, [0; 32], pass(0, 1, true));
        let empty = GrowthPass {
            waiting: Vec::new(),
            refused: 0,
            no_candidate: 0,
            growable: true,
        };
        growth.passed(now, [0; 32], empty);
        assert_eq!(growth.retry_at, None);
        assert!(growth.waiting.is_empty());
    }

    /// A lane's own worker drains every queued range in turn and never ends
    /// between them, so no worker end asks for growth. The first pass runs
    /// as the lane starts, before its worker holds a range, and finds no busy
    /// lane to ask. The loop asks again on its retry clock, and that one pass
    /// takes every stream the lane's hook grants for the queue (#2252).
    #[tokio::test(start_paused = true)]
    async fn a_queue_one_lane_drains_grows_an_extra_stream() -> anyhow::Result<()> {
        let data = blob(16 * MIB as usize);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data)?
            .slow_finish(Duration::from_secs(2))
            .paying(Arc::clone(&la));
        let (widen, count) = counting_widen(2);
        let mut cand_a = candidate(a.clone(), la, 0xA1, None);
        cand_a.widen = Some(widen);
        let provider = StaticSources::new(vec![cand_a])?;
        let (root, total) = (a.root(), a.total_bytes());
        let (store, _dir) = fresh_store(root, total);
        let mut set = SourceSet::new(&provider, root, Arc::default(), provider.holders());
        let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
        let drive = drive_config();
        let runs: Vec<(u64, u64)> = (0..6).map(|i| (i * 2 * MIB, MIB)).collect();
        acquire(
            AcquireTarget {
                store: &store,
                hash: root,
                total_bytes: total,
                ranges: &runs,
            },
            &mut set,
            &AcquireEnv {
                pacer: &BudgetPacer::new(),
                funder: &no_topups(),
                drive: &drive,
                max_lanes: 1,
                stop: &stop,
                on_progress: None,
                ledgers: None,
                pacing: None,
                max_blob_bytes: 0,
            },
        )
        .await?;
        let present = ranges_content_len(&store.present_ranges().await?, total);
        assert_eq!(present, 6 * MIB, "every queued run landed");

        let legs = a.timeline();
        let most_at_once = legs
            .iter()
            .map(|&(_, at, _)| {
                legs.iter()
                    .filter(|&&(_, opened, finished)| {
                        opened <= at && finished.is_none_or(|end| at < end)
                    })
                    .count()
            })
            .max()
            .unwrap_or(0);
        assert_eq!(
            most_at_once, 3,
            "the own stream and both granted extras ran at once: {legs:?}"
        );
        let opened: Vec<_> = legs.iter().map(|&(_, at, _)| at).collect();
        assert_eq!(
            opened.get(1),
            opened.get(2),
            "one growth pass opened both extra streams: {legs:?}"
        );
        assert_eq!(count.peak(), 2, "the lane held both granted streams");
        // The growth pass asks for extra streams. The own worker may later
        // park while its extras finish and take its stream back as a
        // restart.
        let first_grants: Vec<_> = count
            .calls()
            .into_iter()
            .filter(|(_, granted)| *granted)
            .map(|(kind, _)| kind)
            .take(2)
            .collect();
        assert_eq!(
            first_grants,
            [super::GrowFor::Extra, super::GrowFor::Extra],
            "a busy lane asks for extra streams: {:?}",
            count.calls()
        );
        assert_eq!(count.released(), count.granted());
        Ok(())
    }

    /// A lane with a [`super::LaneWiden`] gives its lease back when its own
    /// worker faults, while the fetch still runs, so a sibling fetch can use
    /// that stream. When the lane starts again in the same acquire, it does so
    /// on a stream its `grow` grants, and gives that back too.
    #[tokio::test(start_paused = true)]
    async fn a_lanes_lease_is_released_when_its_own_worker_ends() -> anyhow::Result<()> {
        let data = blob(32 * MIB as usize);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        // A faults once, 1 MiB in, and serves every later stream. B is busy
        // long enough for A to come back from its cooldown.
        let a = ScriptedSource::new(data.clone())?
            .fault_once_after(MIB as usize, || anyhow::anyhow!("scripted reset"))
            .paying(Arc::clone(&la));
        let b = busy(&data, &lb)?;
        let dropped_a = Arc::new(Mutex::new(None));
        let (widen, count) = counting_widen(1);
        let mut cand_a = candidate(a.clone(), la, 0xA1, None);
        cand_a.lease = LaneLease::new(DropStamp(Arc::clone(&dropped_a)));
        cand_a.widen = Some(widen);
        let provider = StaticSources::new(vec![cand_a, candidate(b.clone(), lb, 0xB2, None)])?;
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        let released_mid_fetch = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let on_progress = {
            let seen = Arc::clone(&released_mid_fetch);
            let dropped_a = Arc::clone(&dropped_a);
            move |_position: u64, _total: u64| {
                if dropped_a.lock().is_ok_and(|at| at.is_some()) {
                    seen.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
        };
        run_acquire_with(
            &store,
            &provider,
            root,
            &BudgetPacer::new(),
            &no_topups(),
            Knobs {
                on_progress: Some(&on_progress),
                ..Knobs::lanes(2)
            },
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);

        assert!(
            released_mid_fetch.load(std::sync::atomic::Ordering::SeqCst),
            "A's lease was released while the fetch still ran"
        );
        assert!(
            a.timeline().len() >= 2,
            "A started again after its cooldown"
        );
        assert!(
            count.granted() >= 1,
            "A started again on a stream its grow granted"
        );
        assert_eq!(
            count.peak(),
            1,
            "A's restart holds one grant, and A runs no extra beside it"
        );
        assert_eq!(count.released(), count.granted());
        Ok(())
    }

    /// A lane with a [`super::LaneWiden`] gives its lease back while its own
    /// worker parks with nothing to take, so a sibling fetch can use that
    /// stream while a peer lane drains the rest (#2252). A finishes its half
    /// fast; B's half is too small to steal from and stalls, so A parks.
    #[tokio::test(start_paused = true)]
    async fn a_parked_lane_gives_its_stream_back() -> anyhow::Result<()> {
        let data = blob(24 * MIB as usize);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&la));
        let b = busy(&data, &lb)?;
        let dropped_a = Arc::new(Mutex::new(None));
        let (widen, count) = counting_widen(1);
        let mut cand_a = candidate(a.clone(), la, 0xA1, None);
        cand_a.lease = LaneLease::new(DropStamp(Arc::clone(&dropped_a)));
        cand_a.widen = Some(widen);
        let provider = StaticSources::new(vec![cand_a, candidate(b.clone(), lb, 0xB2, None)])?;
        let (root, _) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, a.total_bytes());
        let released_mid_fetch = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let on_progress = {
            let seen = Arc::clone(&released_mid_fetch);
            let dropped_a = Arc::clone(&dropped_a);
            move |_position: u64, _total: u64| {
                if dropped_a.lock().is_ok_and(|at| at.is_some()) {
                    seen.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
        };
        run_acquire_with(
            &store,
            &provider,
            root,
            &BudgetPacer::new(),
            &no_topups(),
            Knobs {
                on_progress: Some(&on_progress),
                ..Knobs::lanes(2)
            },
        )
        .await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);

        assert_eq!(a.timeline().len(), 1, "A fetched its half on one stream");
        assert!(
            released_mid_fetch.load(std::sync::atomic::Ordering::SeqCst),
            "A's lease was released while B still drained its half"
        );
        assert!(
            count
                .calls()
                .iter()
                .all(|(kind, _)| *kind == super::GrowFor::Restart),
            "a parked lane takes a stream back only as a restart: {:?}",
            count.calls()
        );
        assert_eq!(count.released(), count.granted());
        Ok(())
    }

    /// A node that refuses every extra stream of a busy lane is asked less
    /// and less often: the lane waits from `GROWTH_RETRY`, doubling, before
    /// it is asked again, whatever else wakes the loop.
    #[tokio::test(start_paused = true)]
    async fn a_node_refusing_every_extra_stream_is_asked_less_often() -> anyhow::Result<()> {
        let data = blob(32 * MIB as usize);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = dead_after_first_mib(&data, &la)?;
        let b = ScriptedSource::new(data.clone())?
            .stall_after(MIB, Duration::from_secs(20))
            .refusing_opens_from(1, || anyhow::anyhow!("scripted overload"))
            .paying(Arc::clone(&lb));
        let (widen, count) = counting_widen(1);
        let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
        cand_b.widen = Some(widen);
        let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
        let (root, total) = (a.root(), a.total_bytes());
        let (store, _dir) = fresh_store(root, total);
        let fetch = acquire_over(&store, &lanes, lanes.holders(), root, 2);
        let ended = tokio::time::timeout(Duration::from_secs(16), fetch).await;
        assert!(ended.is_err(), "B's own range outlasts the window");

        let extras: Vec<tokio::time::Instant> = b
            .timeline()
            .into_iter()
            .skip(1)
            .map(|(_, opened, _)| opened)
            .collect();
        let gaps: Vec<Duration> = extras
            .windows(2)
            .filter_map(|pair| Some(pair.get(1)?.duration_since(*pair.first()?)))
            .collect();
        assert!(
            (3..=5).contains(&extras.len()),
            "B's extra stream was asked at 0, 1, 3, 7 and 15 s: {gaps:?}"
        );
        for pair in gaps.windows(2) {
            if let [shorter, longer] = pair {
                assert!(
                    *longer >= *shorter * 2,
                    "each wait doubles the last: {gaps:?}"
                );
            }
        }
        assert_eq!(count.released(), count.granted());
        Ok(())
    }

    /// A busy partial holder whose node cannot serve a faulted lane's
    /// remainder by pull-through refuses it on its extra stream with
    /// `NotFound`. Those refusals bar it from pull-through as on its own
    /// stream, so it is asked for the remainder at most
    /// [`ABSENT_AFTER_NOT_FOUND`] times while the bar lasts.
    ///
    /// [`ABSENT_AFTER_NOT_FOUND`]: crate::source_set::ABSENT_AFTER_NOT_FOUND
    #[tokio::test(start_paused = true)]
    async fn extra_stream_refusals_of_an_uncovered_range_bar_the_lane() -> anyhow::Result<()> {
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let data = blob(total as usize);
        let n = num_blocks(total);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = dead_after_first_mib(&data, &la)?;
        let b = ScriptedSource::new(data.clone())?
            .stall_after(MIB, Duration::from_mins(1))
            .refusing_blocks(&[0], || {
                anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
            })
            .paying(Arc::clone(&lb));
        let (widen, count) = counting_widen(1);
        let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, Some(cov(n, &[1])));
        cand_b.widen = Some(widen);
        let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
        let root = a.root();
        let (store, _dir) = fresh_store(root, total);
        let fetch = acquire_over(&store, &lanes, lanes.holders(), root, 2);
        let ended = tokio::time::timeout(Duration::from_secs(50), fetch).await;
        assert!(ended.is_err(), "B's own range outlasts the window");

        let block0 = b
            .opened_ranges()
            .into_iter()
            .filter(|&(start, _)| start < DISCOVERY_BLOCK_BYTES)
            .count();
        assert!(block0 >= 1, "B grew an extra stream for A's remainder");
        assert!(
            block0 <= crate::source_set::ABSENT_AFTER_NOT_FOUND as usize,
            "B was asked for the uncovered remainder {block0} times"
        );
        assert_eq!(count.released(), count.granted());
        Ok(())
    }

    /// A lane that faults and finds no free stream to start again on tries
    /// again each `GROWTH_RETRY`, not on a growing build backoff, and starts
    /// once a sibling fetch frees a stream. The freed stream is the last free
    /// one, which an extra stream may not take: the start asks for it as a
    /// restart. Every grant comes back.
    #[tokio::test(start_paused = true)]
    async fn a_lane_refused_a_stream_starts_again_once_one_frees() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let data = blob(16 * MIB as usize);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(data.clone())?
            .fault_once_after(MIB as usize, || anyhow::anyhow!("scripted reset"))
            .paying(Arc::clone(&la));
        let free = Arc::new(AtomicUsize::new(0));
        let (widen, count) = shared_widen(&free, 1);
        let mut cand_a = candidate(a.clone(), la, 0xA1, None);
        cand_a.widen = Some(widen);
        let lanes = StaticSources::new(vec![cand_a])?;
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        let freed_after = Duration::from_secs(20);
        let freed = async {
            tokio::time::sleep(freed_after).await;
            free.fetch_add(1, Ordering::SeqCst);
        };
        let started = tokio::time::Instant::now();
        let fetch = acquire_over(&store, &lanes, lanes.holders(), root, 1);
        let (fetched, ()) = tokio::time::timeout(
            Duration::from_mins(5),
            futures_util::future::join(fetch, freed),
        )
        .await
        .map_err(|_| anyhow::anyhow!("the fetch stalled"))?;
        fetched?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);

        let (_, restarted, _) = a
            .timeline()
            .get(1)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("A started again"))?;
        let late = restarted
            .duration_since(started)
            .saturating_sub(freed_after);
        assert!(
            late <= super::GROWTH_RETRY,
            "A started again within a retry of the freed stream, {late:?} after"
        );
        let calls = count.calls();
        let restarts = calls
            .iter()
            .filter(|(kind, _)| *kind == super::GrowFor::Restart)
            .count();
        assert!(
            restarts <= 25,
            "A asked for a stream to start again on about once a second: {restarts}"
        );
        let granted: Vec<_> = calls
            .iter()
            .filter(|(_, granted)| *granted)
            .map(|(kind, _)| *kind)
            .collect();
        assert_eq!(granted, [super::GrowFor::Restart], "{calls:?}");
        assert_eq!(count.released(), count.granted());
        Ok(())
    }

    /// The most extra streams busy lane B holds at once while two dead lanes'
    /// remainders wait, with a hook that grants up to `grants` streams at once,
    /// as `(peak, granted, released)`.
    async fn peak_extras_for_two_remainders(
        grants: usize,
    ) -> anyhow::Result<(usize, usize, usize)> {
        let data = blob(48 * MIB as usize);
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let lc = Arc::new(PoolLedger::new(Cumulative::default()));
        let lb = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = dead_after_first_mib(&data, &la)?;
        let c = dead_after_first_mib(&data, &lc)?;
        let b = busy(&data, &lb)?;
        let (widen, count) = counting_widen(grants);
        let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
        cand_b.widen = Some(widen);
        let lanes = StaticSources::new(vec![
            candidate(a.clone(), la, 0xA1, None),
            candidate(c.clone(), lc, 0xC3, None),
            cand_b,
        ])?;
        let (root, total) = (a.root(), a.total_bytes());
        let (store, dir) = fresh_store(root, total);
        acquire_over(&store, &lanes, lanes.holders(), root, 3).await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("b"))?, data);
        Ok((count.peak(), count.granted(), count.released()))
    }

    /// A busy lane runs as many extra workers as its hook grants, one per
    /// waiting range: with two dead lanes' remainders queued it holds more
    /// than one extra stream at once, never more than the hook grants, and
    /// with a hook that grants one it never holds more (#2252).
    #[tokio::test(start_paused = true)]
    async fn a_busy_lane_runs_as_many_extra_streams_as_granted() -> anyhow::Result<()> {
        let (peak, granted, released) = peak_extras_for_two_remainders(3).await?;
        assert!(
            (2..=3).contains(&peak),
            "B held several extra streams at once, within its grant: {peak}"
        );
        assert_eq!(released, granted);

        let (peak, granted, released) = peak_extras_for_two_remainders(1).await?;
        assert!(granted >= 1, "B grew an extra stream");
        assert_eq!(
            peak, 1,
            "B held no more extra streams than its hook granted"
        );
        assert_eq!(released, granted);
        Ok(())
    }

    /// A store that fails `missing_ranges` once `fail` is set: a local disk or
    /// store fault in the middle of a unit. A nonzero `shrink_to` moves the
    /// bound down to it at the next `missing_ranges`, before the query runs: a
    /// leg that proves a smaller size after the caller read the bound.
    struct FailingMissing {
        inner: ClientRangedStore,
        fail: std::sync::atomic::AtomicBool,
        shrink_to: std::sync::atomic::AtomicU64,
    }

    impl RangedStore for FailingMissing {
        fn total_bytes(&self) -> u64 {
            self.inner.total_bytes()
        }

        fn present_ranges(&self) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
            self.inner.present_ranges()
        }

        fn missing_ranges(
            &self,
            byte_offset: u64,
            byte_len: u64,
        ) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Box::pin(async {
                    Err(decdn_bao_range::RangedStoreError::Backend(Box::from(
                        "injected store read fault",
                    )))
                });
            }
            let shrink = self.shrink_to.swap(0, std::sync::atomic::Ordering::SeqCst);
            if shrink > 0 {
                self.inner.set_bound(shrink);
            }
            self.inner.missing_ranges(byte_offset, byte_len)
        }

        fn admit(
            &self,
            range: decdn_bao_range::AlignedRange,
            bao_bytes: bytes::Bytes,
        ) -> decdn_bao_range::RangedFuture<'_, ()> {
            self.inner.admit(range, bao_bytes)
        }

        fn read(&self, offset: u64, len: u64) -> decdn_bao_range::RangedFuture<'_, bytes::Bytes> {
            self.inner.read(offset, len)
        }

        fn is_complete(&self) -> decdn_bao_range::RangedFuture<'_, bool> {
            self.inner.is_complete()
        }

        fn finalize(&self) -> decdn_bao_range::RangedFuture<'_, ()> {
            self.inner.finalize()
        }
    }

    impl crate::source::IngestStore for FailingMissing {
        fn ingest_stream<'a, R>(
            &'a self,
            range: &'a decdn_bao_range::AlignedRange,
            reader: R,
            on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
            claimed_total: u64,
        ) -> core::pin::Pin<Box<dyn core::future::Future<Output = anyhow::Result<R>> + 'a>>
        where
            R: crate::BaoRangeReader + 'a,
        {
            crate::source::IngestStore::ingest_stream(
                &self.inner,
                range,
                reader,
                on_progress,
                claimed_total,
            )
        }

        fn flush_present_record(&self) -> crate::source::SourceFuture<'_, ()> {
            crate::source::IngestStore::flush_present_record(&self.inner)
        }

        fn proven(&self) -> Option<u64> {
            self.inner.proven()
        }

        fn set_bound(&self, bound: u64) {
            self.inner.set_bound(bound);
        }
    }

    /// A store that cannot read its own record while the watchdog checks a
    /// stalled unit is this process's fault (#2213): the acquire ends with
    /// that error as a command-wide fatal, and the source is not cooled.
    #[tokio::test(start_paused = true)]
    async fn a_store_read_error_in_the_watchdog_is_our_fault() -> anyhow::Result<()> {
        let data = blob(8 * MIB as usize);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(data)?
            .stall_after(MIB, Duration::from_mins(1))
            .paying(Arc::clone(&ledger));
        let (root, total) = (src.root(), src.total_bytes());
        let (inner, _dir) = fresh_store(root, total);
        let store = FailingMissing {
            inner,
            fail: std::sync::atomic::AtomicBool::new(false),
            shrink_to: std::sync::atomic::AtomicU64::new(0),
        };
        let provider = StaticSources::new(vec![candidate(src, ledger, 0xA1, None)])?;
        let health: Arc<PeerHealth> = Arc::default();
        let mut set = SourceSet::new(&provider, root, Arc::clone(&health), provider.holders());
        let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
        let drive = drive_config();
        let (pacer, funder) = (BudgetPacer::new(), no_topups());
        let whole = [(0, total)];
        let env = AcquireEnv {
            pacer: &pacer,
            funder: &funder,
            drive: &drive,
            max_lanes: 1,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        };
        let fetch = acquire(
            AcquireTarget {
                store: &store,
                hash: root,
                total_bytes: total,
                ranges: &whole,
            },
            &mut set,
            &env,
        );
        // The unit stalls after its first MiB; the store breaks before the
        // lane watchdog's next check.
        let breaks = async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            store.fail.store(true, std::sync::atomic::Ordering::SeqCst);
            std::future::pending::<()>().await;
        };
        let err = tokio::select! {
            res = fetch => res.err().ok_or_else(|| anyhow::anyhow!("a broken store must end it"))?,
            () = breaks => unreachable!("never resolves"),
        };
        assert!(
            format!("{err:#}").contains("injected store read fault"),
            "{err:#}"
        );
        assert_eq!(classify(&err), Fault::Fatal(FatalScope::Command), "{err:#}");
        assert_eq!(
            health.health(Address::repeat_byte(0xA1)),
            Health::Healthy { streak: 0 },
            "the source is not cooled for our fault"
        );
        Ok(())
    }

    /// With no size proven, the end the fetch knows is the smaller of the
    /// first claim and the bound; a proven size replaces both.
    #[tokio::test]
    async fn the_known_end_is_the_claim_or_bound_until_a_size_is_proven() -> anyhow::Result<()> {
        let data = blob(3 * MIB as usize);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(data)?.paying(Arc::clone(&ledger));
        let root = src.root();
        let (store, _dir) = fresh_store(root, 8 * MIB);
        assert_eq!(super::known_end(&store, MIB), MIB, "a short claim");
        assert_eq!(super::known_end(&store, 16 * MIB), 8 * MIB, "the bound");

        let provider = StaticSources::new(vec![candidate(src, ledger, 0xA1, None)])?;
        run_acquire(
            &store,
            &provider,
            root,
            8 * MIB,
            &BudgetPacer::new(),
            &no_topups(),
            1,
            None,
        )
        .await?;
        assert_eq!(super::known_end(&store, MIB), 3 * MIB, "the proven size");
        Ok(())
    }

    /// A leg proves a smaller size between a caller's bound read and its
    /// missing-ranges query. The query clips to the smaller bound: a range past
    /// it misses nothing, and a range across it misses only the bytes below it.
    /// Neither is a store fault.
    #[tokio::test]
    async fn a_bound_that_shrinks_under_a_missing_query_clips_it() -> anyhow::Result<()> {
        let (inner, _dir) = fresh_store([7; 32], 8 * MIB);
        let store = FailingMissing {
            inner,
            fail: std::sync::atomic::AtomicBool::new(false),
            shrink_to: std::sync::atomic::AtomicU64::new(2 * MIB),
        };
        let past = super::store_missing(&store, 4 * MIB, 2 * MIB).await?;
        assert!(past.is_empty(), "a range past the new bound misses nothing");

        store
            .shrink_to
            .store(MIB, std::sync::atomic::Ordering::SeqCst);
        let across = super::store_missing(&store, 0, 4 * MIB).await?;
        assert_eq!(ranges_content_len(&across, store.total_bytes()), MIB);
        Ok(())
    }
}
