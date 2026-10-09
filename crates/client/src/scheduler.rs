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
//! queued *steals* from a range in flight ([`steal_split`]). It steals first
//! inside the range's covered suffix: the blocks it covers, from the range's
//! end back to, but not including, the nearest block it does not cover. Of
//! the ranges in flight, it picks the one whose covered suffix misses the
//! most, and takes the aligned tail of that range's missing remainder. So a
//! fast source keeps helping a slow one, and a lane that joins late starts by
//! stealing. The split weighs the remainder by the two lanes' observed rates,
//! so at steady rates one steal leaves both finishing together, unless the
//! covered suffix starts past that split: then the split moves to the
//! suffix's start and the victim keeps more than its share. The split
//! lies past the bytes the victim has already received, and the victim keeps
//! the front of the rest, so it still has work after the steal. It keeps its
//! open stream: the steal lowers the victim's end to the split
//! ([`Work::pick`]), and the victim stops there.
//!
//! One steal reaches outside the stealer's coverage (#2348). When no range has
//! a covered suffix worth a stream, a lane not barred from pull-through steals
//! from a range whose lane runs at no more than a quarter of its own rate
//! ([`SLOW_VICTIM_FACTOR`]), measured over at least [`SLOW_VICTIM_EVIDENCE`].
//! It splits that whole range by the two rates, and its node serves the part
//! of the tail it does not cover by pull-through. So one lane behind a slow
//! path does not set the blob's finish time alone. A lane's own worker parked
//! with nothing to take looks for such a steal every [`STEAL_RECHECK`].
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
//! # Stopping a lane: closing the double-pay
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
//! checkpoint made durable. A cancel re-queues the remainder, and a later
//! pick opens a new stream for it. Three events end a lane's leg before its
//! range ends, and only a steal keeps the open stream:
//!
//! - **Steal.** When a freed worker steals a busy victim's tail `[split, end)`,
//!   `Work::pick` trims the victim's assignment to `[start, split)` and lowers
//!   the victim's end to `split`, both under the `Work` lock. The split lies at
//!   least `MIN_VICTIM_KEEP` past the victim's verified frontier, durable or
//!   not. The victim's leg stops on its open stream once its verified prefix
//!   reaches `split` ([`fill_gap`]): it pays for every byte it received,
//!   closes the stream, and ends its unit as complete, with nothing to
//!   re-queue. So the stolen tail is fetched (and paid for) by ONE source,
//!   apart from the frames past the split the victim's stream already carried
//!   when it stopped, and the victim opens no new stream for the part it
//!   keeps.
//! - **Proven size.** A proven size that leaves a lane's whole range past
//!   the end of the blob cancels that lane's unit (`Work::cancel_victim`).
//! - **Stall / fault.** A lane with no verified progress for the lane watchdog
//!   ([`LANE_WATCHDOG`]), or whose `fill_gap` returns an `Err`, re-queues the
//!   UN-fetched remainder of its range to `pending` and ends. The loop
//!   classifies the fault ([`crate::classify`]): a fatal one ends the acquire
//!   with that typed error, anything else is the [`SourceSet`]'s to act on.
//!   Verified bytes already stored are never refetched.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

use crate::coverage_plan::{
    SourceCoverage, covered_part, covered_suffix_start, covers_byte_range, spread_segments,
};
use crate::credential::{
    CapabilityCause, CredentialSlot, CredentialView, FundingNeeded, QuoteMax, unix_now,
};
use crate::driver::{
    DriveCounters, PRESENT_RECORD_FLUSH_INTERVAL, PacingWait, SharedPool, UnitProgress, WaitReason,
    contiguous_byte_ranges, fill_gap, ranges_content_len,
};
use crate::fault::Fault;
use crate::health::PeerHealth;
use crate::ledgers::LaneLedgers;
use crate::pacer::DownstreamFrontier;
use crate::recovery::{RecoveryGate, SwapStep, after_step};
use crate::segment::{split_evenly, steal_split, without_runs};
use crate::source::{BlobSource, Funder, IngestStore, SourceFuture, SourceStream};
use crate::source_set::{Holder, NoAffordableSource, SourceProvider, SourceSet};
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
/// first wait, which doubles up to a cap, before a lane is asked again after
/// a node refused its extra stream. And it is the wait before a lane that
/// found no free stream tries to start again.
pub const GROWTH_RETRY: Duration = Duration::from_secs(1);

/// The longest wait before a lane is asked for an extra stream again after
/// its node refused extra streams in a row. The wait starts at
/// [`GROWTH_RETRY`] and doubles with each refusal; a verified byte on an
/// extra stream of the lane ends it.
const EXTRA_RETRY_CAP: Duration = Duration::from_secs(30);

/// How many times faster than a range's lane a freed lane must run to steal
/// that range's tail outside its own coverage, by pull-through (#2348). A
/// pull-through adds an upstream hop and its own payment ramp, so the
/// stealer's rate on its last unit can overstate what it fetches by
/// pull-through: at a gap of four, even at half that rate, the two lanes
/// finish the remainder more than twice as soon as the victim would alone.
/// The split by rate leaves the victim about a fifth of the remainder, and
/// at least [`crate::segment::MIN_VICTIM_KEEP`]. The gap is set well above
/// the spread of healthy lanes in #2348 (5–11 MiB/s) and well below the slow
/// lane's (10–24 times slower).
const SLOW_VICTIM_FACTOR: u64 = 4;

/// The least active time a range's rate must be measured over before a
/// slow-victim steal trusts it ([`Unit::rate_sample`]). A leg's credit window
/// starts at one payment interval and grows with what it pays, and its path
/// starts in slow start, so a few seconds of rate read as slow on any cold
/// leg. It equals [`FIRST_BYTE_GRACE`].
const SLOW_VICTIM_EVIDENCE: Duration = FIRST_BYTE_GRACE;

/// How often a lane's own worker, parked with nothing to take, plans a steal
/// again while a range is in flight. A slow-victim steal turns due as the
/// victim's rate gathers evidence, and a victim's progress wakes no parked
/// worker, so the parked worker looks again on this clock. A check that
/// finds no steal due takes no stream.
const STEAL_RECHECK: Duration = Duration::from_secs(5);

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

/// Resolve once worker `i`, parked with nothing to take, has a steal due
/// ([`Work::steal_due`]), looking every [`STEAL_RECHECK`] (#2348). Each look
/// reads its inputs the way the pick does, so the two disagree only when the
/// work state moves in between. A look takes no stream.
///
/// # Errors
///
/// A store error ([`store_missing`]): this process's fault, not the source's.
/// Or the plan's alignment error ([`Work::steal_due`]), which the pick would
/// raise too.
async fn steal_recheck<St>(
    store: &St,
    work: &AsyncMutex<Work>,
    i: usize,
    coverage: &Coverage,
) -> anyhow::Result<()>
where
    St: IngestStore,
{
    loop {
        tokio::time::sleep(STEAL_RECHECK).await;
        let total_bytes = store.total_bytes();
        let missing =
            contiguous_byte_ranges(&store_missing(store, 0, total_bytes).await?, total_bytes);
        if work
            .lock()
            .await
            .steal_due(i, total_bytes, coverage, &missing)?
        {
            return Ok(());
        }
    }
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
    /// Runs the funding recovery step when the candidate set is exhausted with
    /// a source priced out (ADR 003 § Funding recovery).
    pub funder: &'a F,
    /// The fetch's funding recovery state: the progress rule and the settle
    /// window after a top-up. Shared by every entry of a bundle pull and by a
    /// pass that runs again against a replaced pool.
    pub recovery: &'a RecoveryGate,
    /// A delegated fetch's voucher credential, or `None` for a fetch that pays
    /// from its own pool. With a slot, the loop retires the lanes on a swapped
    /// key, publishes the running-low signal, and at the exhausted candidate
    /// set waits for a swap instead of topping up ([`CredentialSlot`]).
    pub credentials: Option<&'a CredentialSlot>,
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
///
/// In turn, the acquire asks a `grow` that refused again within
/// [`GROWTH_RETRY`] while it still wants that stream, unless the lane's node
/// refused its extra streams and the lane waits out that refusal. So a
/// caller can hold a freed stream for a lane that waits, and let the hold
/// lapse once the lane stops asking.
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

/// A range [`Work::pick`] handed to a worker. `uncovered` is set when the
/// range holds a chunk outside the worker's coverage, for its node to serve by
/// pull-through: a pending chunk no running lane covers, or a slow-victim
/// steal's tail. `unit` is the unit's own live state ([`Work::live`]).
struct Picked {
    range: AlignedRange,
    uncovered: bool,
    unit: Arc<Unit>,
}

/// One unit's live state: its worker writes it as it fetches, and a steal
/// reads it and lowers its end. [`Work::pick`] gives every unit a fresh one,
/// so a steal can only touch the unit it trims.
#[derive(Debug)]
struct Unit {
    /// The unit's verified bytes, verified frontier and first-byte time,
    /// which [`fill_gap`] writes.
    progress: UnitProgress,
    /// The unit's end: `u64::MAX` until a steal lowers it to its split. The
    /// unit's leg then stops there on its open stream ([`fill_gap`]).
    stop_at: AtomicU64,
    /// Nanoseconds the unit spent parked on the consumer after its first
    /// verified byte, over the parks that have ended ([`ParkedWait`]).
    parked: AtomicU64,
    /// When the park in progress began, in nanoseconds after the unit's
    /// first verified byte; `u64::MAX` while the unit is not parked. The
    /// unit's rate leaves out this park too, so a lane parked on the consumer
    /// now does not read as slow.
    park_open_at: AtomicU64,
}

impl Unit {
    fn new() -> Self {
        Self {
            progress: UnitProgress::default(),
            stop_at: AtomicU64::new(u64::MAX),
            parked: AtomicU64::new(0),
            park_open_at: AtomicU64::new(u64::MAX),
        }
    }

    /// The unit's rate so far, in bytes per second; `None` before its first
    /// verified byte. It counts from that byte and leaves out the time the
    /// unit spent parked on the consumer, so a leg's open, a cold first
    /// byte, and the consumer's pace stay out of it.
    fn rate(&self) -> Option<u64> {
        self.rate_sample().map(|(rate, _)| rate)
    }

    /// The unit's rate so far ([`Unit::rate`]) and the active time it is
    /// measured over: the time since its first verified byte, less the time
    /// it spent parked on the consumer, the park in progress included. A
    /// slow-victim steal trusts a rate only over [`SLOW_VICTIM_EVIDENCE`] of
    /// that time.
    fn rate_sample(&self) -> Option<(u64, Duration)> {
        let first = self.progress.first_byte.get()?;
        let verified = self.progress.verified.load(Ordering::Relaxed);
        let since_first = first.elapsed();
        // The open park first, then the ended ones: the reverse of the order
        // `ClosePark` writes them in. A cleared marker (`Acquire`, paired with
        // its `Release`) shows the ended park in `parked` too, so a park that
        // closes between the two reads is left out once or twice, never not
        // at all.
        let open = match self.park_open_at.load(Ordering::Acquire) {
            u64::MAX => Duration::ZERO,
            at => since_first.saturating_sub(Duration::from_nanos(at)),
        };
        let parked = Duration::from_nanos(self.parked.load(Ordering::Relaxed));
        let active = since_first.saturating_sub(parked).saturating_sub(open);
        let millis = active.as_millis();
        if verified == 0 || millis == 0 {
            return None;
        }
        let rate = u64::try_from(u128::from(verified).saturating_mul(1000) / millis).ok()?;
        Some((rate, active))
    }
}

/// Which rule a steal took its tail by ([`Work::plan_steal`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StealKind {
    /// The tail lies in the victim range's covered suffix
    /// ([`covered_suffix_start`], #2303).
    CoveredSuffix,
    /// The victim runs at a small fraction of the stealer's rate
    /// ([`SLOW_VICTIM_FACTOR`]), and the stealer's node serves the part of
    /// the tail outside its coverage by pull-through (#2348).
    SlowVictim,
}

impl StealKind {
    /// The kind's name in a steal's log line.
    const fn as_str(self) -> &'static str {
        match self {
            Self::CoveredSuffix => "covered_suffix",
            Self::SlowVictim => "slow_victim",
        }
    }
}

/// A steal [`Work::plan_steal`] would make: the victim's worker slot, the
/// aligned tail the stealer takes, and the rule it takes it by.
#[derive(Debug)]
struct PlannedSteal {
    /// The victim's worker slot.
    victim: usize,
    /// The aligned tail of the victim's range ([`steal_split`]).
    tail: AlignedRange,
    /// The rule the steal takes the tail by.
    kind: StealKind,
}

/// Per-lane interrupt: an edge-triggered wakeup ([`Notify`]) plus a `flag`
/// that says the wakeup means "cancel", not a stale permit. The canceller sets
/// `flag` and wakes the victim under the `Work` lock; the victim clears it on
/// its next `Work::pick`, also under the lock, so the two never race.
struct CancelHandle {
    /// `true` once [`Work::cancel_victim`] cancels this lane's unit; the
    /// victim must stop.
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
/// The first window of each gap is [`FIRST_BYTE_GRACE`] (or `deadline`, if
/// longer), so a cold miss whose first byte waits on the node's own upstream
/// is not a stall. After it, a gap between verified bytes shorter than the
/// deadline never trips; a source that stops trips it within one to two
/// deadlines.
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
    let mut window = deadline.max(FIRST_BYTE_GRACE);
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
    /// A proven size left the range past the end of the blob, or a
    /// consumption-paced lane gave its range back for an earlier one:
    /// re-queue the remainder and stay live (pick again).
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
    /// The lane signs under a key the credential slot no longer holds. The
    /// worker paid for what it received, ended at that voucher boundary, and
    /// re-queued the rest. The loop drops the lane and builds a new one under
    /// the current credential.
    Retired,
}

/// What [`run_worker`] hands back to the loop.
struct WorkerEnd {
    /// The lane's payee.
    provider: Address,
    /// How the worker ended.
    end: LaneEnd,
    /// Whether the worker verified any byte.
    delivered: bool,
    /// When the worker last verified a byte of a range that holds a chunk
    /// outside its coverage: its node serves that part by pull-through.
    pulled_through: Option<Instant>,
    /// The `(offset, len)` pieces of ranges wholly inside its coverage in
    /// which the worker verified bytes: its node holds those blocks
    /// ([`SourceSet::record_covered_served`]).
    covered_served: Vec<(u64, u64)>,
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
    /// `cancel[victim]` when a proven size leaves its range past the end; the
    /// victim clears it on its next `pick`.
    cancel: Vec<Arc<CancelHandle>>,
    /// Per-slot live state of the unit in flight. A steal reads the victim's
    /// rate and frontier off it, and lowers its end to the split under the
    /// same lock as its trim: the victim stops its open stream there
    /// ([`fill_gap`]), with no new stream for the part it keeps.
    live: Vec<Arc<Unit>>,
    /// Per-slot rate over the worker's last unit that verified a byte, in
    /// bytes per second: the stealer's side of a steal's split.
    rates: Vec<Option<u64>>,
    /// Per-lane count of units started. Every [`Work::pick`] by worker `i`
    /// bumps `units[i]`, so a number names one unit, and
    /// [`Work::cancel_victim`] cancels only the unit it names.
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
            live: Vec::new(),
            rates: Vec::new(),
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

    /// Set lane `lane`'s probed coverage, and its extra workers', to
    /// `coverage`. A lane that holds the whole blob keeps its coverage.
    fn set_coverage(&mut self, lane: usize, coverage: &Coverage) {
        for i in 0..self.coverage.len() {
            if self.lane_of.get(i) == Some(&lane)
                && self.measured.get(i) == Some(&true)
                && let Some(c) = self.coverage.get_mut(i)
                && c != coverage
            {
                c.clone_from(coverage);
            }
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
        self.push_unit_state();
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
    /// workers at once. Its unit count carries on, so a cancel made against
    /// an earlier unit cannot cancel it.
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
            if let Some(rate) = self.rates.get_mut(slot) {
                *rate = None;
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
        self.push_unit_state();
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

    /// Give a new slot its unit state: an idle unit and no rate yet.
    fn push_unit_state(&mut self) {
        self.live.push(Arc::new(Unit::new()));
        self.rates.push(None);
    }

    /// Record `rate`, worker `i`'s rate over the unit it just ended.
    fn record_rate(&mut self, i: usize, rate: Option<u64>) {
        if let (Some(slot), Some(rate)) = (self.rates.get_mut(i), rate) {
            *slot = Some(rate);
        }
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
    /// range in flight it would steal from now ([`Work::plan_steal`], #2303).
    /// That check counts every byte in flight past its victim's received
    /// frontier as missing, and gives the lane no rate, so it declines only a
    /// steal that the size floors or the lane's coverage rule out. With no
    /// rate, it never counts a slow-victim steal (#2348): only a running
    /// lane's parked worker makes one, so the loop starts no lane for it.
    fn has_work_for(&self, coverage: &Coverage, total_bytes: u64, take_uncovered: bool) -> bool {
        if self
            .pending
            .iter()
            .any(|seg| !covered_part(coverage, seg.chunk_ranges(), total_bytes).is_empty())
            || (take_uncovered && self.uncovered(total_bytes))
        {
            return true;
        }
        let mut in_flight: Vec<(u64, u64)> = self.in_flight.iter().flatten().copied().collect();
        in_flight.sort_unstable();
        // The ranges in flight are valid ranges of the blob, so the plan
        // raises no alignment error; an error reads as no work.
        matches!(
            self.plan_steal(total_bytes, coverage, &in_flight, None, None),
            Ok(Some(_))
        )
    }

    /// Whether worker `i`, parked with nothing to take, has a steal due now
    /// ([`Work::plan_steal`]) at its rate over its last unit, with the
    /// slow-victim rule open unless its lane is barred from pull-through.
    /// `missing` is the store's missing byte runs. Changes nothing.
    ///
    /// # Errors
    ///
    /// As [`steal_split`]: the error [`Work::pick`] would raise on the same
    /// plan.
    fn steal_due(
        &self,
        i: usize,
        total_bytes: u64,
        coverage: &Coverage,
        missing: &[(u64, u64)],
    ) -> anyhow::Result<bool> {
        let (stealer_rate, slow_from) = self.stealer(i);
        Ok(self
            .plan_steal(total_bytes, coverage, missing, stealer_rate, slow_from)?
            .is_some())
    }

    /// What worker `i` steals with: its rate over its last unit, and its slot
    /// as `slow_from` unless its lane is barred from pull-through
    /// ([`Work::plan_steal`]).
    fn stealer(&self, i: usize) -> (Option<u64>, Option<usize>) {
        let stealer_rate = self.rates.get(i).copied().flatten();
        (stealer_rate, (!self.barred(i)).then_some(i))
    }

    /// Whether the range worker `owner` runs is slow beside worker `i`'s
    /// `stealer_rate` (#2348): `owner` fetches for another lane, its unit's
    /// rate is measured over at least [`SLOW_VICTIM_EVIDENCE`], and
    /// [`SLOW_VICTIM_FACTOR`] times that rate is at most `stealer_rate`. A
    /// lane's extra worker shares the lane's node and path, so a lane never
    /// counts its own as slow.
    fn slow_victim(&self, owner: usize, i: usize, stealer_rate: u64) -> bool {
        let (Some(lane), Some(own_lane)) = (self.lane_of.get(owner), self.lane_of.get(i)) else {
            return false;
        };
        if lane == own_lane {
            return false;
        }
        let Some((rate, over)) = self.live.get(owner).and_then(|unit| unit.rate_sample()) else {
            return false;
        };
        over >= SLOW_VICTIM_EVIDENCE
            && rate.max(1).saturating_mul(SLOW_VICTIM_FACTOR) <= stealer_rate
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
    /// steal the aligned tail of the missing remainder of the range in flight
    /// whose covered suffix ([`covered_suffix_start`]) misses the most, split
    /// by the two lanes' rates ([`steal_split`]), or, with none, of a range
    /// whose lane is slow beside this worker's ([`Work::slow_victim`]), whose
    /// tail its node serves by pull-through where it does not cover it. The
    /// split lies past the victim's received frontier.
    /// The steal trims the victim to end at that split, so no other freed
    /// worker can re-steal the same tail, and lowers the victim's end
    /// (`Unit::stop_at`, [`Work::live`]) to the split, so the victim stops its
    /// open stream there. `missing` is the store's missing byte runs, read
    /// just before the pick; it only overstates what is missing, since bytes
    /// that land after the read are never taken away. Records the choice in
    /// `in_flight[i]`, and gives the unit fresh live state.
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
        // A fresh live state for the unit, so a steal can only lower the end
        // of the unit it trims.
        let unit = Arc::new(Unit::new());
        match self.live.get_mut(i) {
            Some(live) => *live = Arc::clone(&unit),
            None => anyhow::bail!("worker index {i} out of range for unit state"),
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
                    uncovered: false,
                    unit,
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
                uncovered,
                unit,
            }));
        }
        if !steal {
            *self.slot_mut(i)? = None;
            return Ok(None);
        }
        self.steal(i, total_bytes, coverage, missing, unit)
    }

    /// The steal a worker over `coverage` that runs at `stealer_rate` would
    /// make now ([`PlannedSteal`]): its victim's slot in `in_flight`, the
    /// aligned tail it takes ([`steal_split`]), and the rule it takes it by.
    /// `None` when no range in flight has a tail worth a fresh stream.
    /// Changes nothing.
    ///
    /// Each candidate victim's received prefix reads as delivered, not
    /// missing: its checkpoints may not be durable yet, but its open stream
    /// already carried those bytes, and a split behind them would hand them
    /// to the stealer too. `steal_split` returns WHICH remaining range it
    /// split, so the caller trims that exact victim: no second argmax to agree
    /// with. The split declines a steal that would leave the victim no missing
    /// byte: that steal takes the victim's whole remaining work, and the
    /// victim, with nothing left, would steal it straight back.
    ///
    /// The stealer steals inside a range's covered suffix first (#2303).
    /// When no range has one worth a stream, and `slow_from` names the
    /// stealer's slot (its lane is not barred from pull-through), it steals
    /// from a range whose lane is slow beside its own rate
    /// ([`Work::slow_victim`], #2348): the tail may then lie outside its
    /// coverage, and its node serves that part by pull-through.
    ///
    /// # Errors
    ///
    /// As [`steal_split`].
    fn plan_steal(
        &self,
        total_bytes: u64,
        coverage: &Coverage,
        missing: &[(u64, u64)],
        stealer_rate: Option<u64>,
        slow_from: Option<usize>,
    ) -> anyhow::Result<Option<PlannedSteal>> {
        let (owners, remaining): (Vec<usize>, Vec<(u64, u64)>) = self
            .in_flight
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| slot.map(|r| (idx, r)))
            .unzip();
        let received: Vec<(u64, u64)> = owners
            .iter()
            .zip(&remaining)
            .filter_map(|(&owner, &(start, _))| {
                let frontier = self
                    .live
                    .get(owner)?
                    .progress
                    .frontier
                    .load(Ordering::SeqCst);
                (frontier > start).then(|| (start, frontier - start))
            })
            .collect();
        let missing = without_runs(missing, &received);
        let victim_rate = |k: usize| {
            owners
                .get(k)
                .and_then(|&owner| self.live.get(owner))
                .and_then(|unit| unit.rate())
        };
        let covered = steal_split(
            &remaining,
            &missing,
            total_bytes,
            |s, l| covered_suffix_start(coverage, s, l, total_bytes),
            stealer_rate,
            victim_rate,
        )?;
        if let Some((k, tail)) = covered {
            return Ok(owners.get(k).map(|&victim| PlannedSteal {
                victim,
                tail,
                kind: StealKind::CoveredSuffix,
            }));
        }
        let (Some(i), Some(rate)) = (slow_from, stealer_rate) else {
            return Ok(None);
        };
        // Only the slow ranges, whole: the stealer's node serves what it does
        // not cover by pull-through. Each keeps its index into `owners`, in
        // one list, so the split's index maps back to its own victim.
        let slow: Vec<(usize, (u64, u64))> = owners
            .iter()
            .zip(&remaining)
            .enumerate()
            .filter(|&(_, (&owner, _))| self.slow_victim(owner, i, rate))
            .map(|(k, (_, &range))| (k, range))
            .collect();
        let slow_remaining: Vec<(u64, u64)> = slow.iter().map(|&(_, range)| range).collect();
        let planned = steal_split(
            &slow_remaining,
            &missing,
            total_bytes,
            |s, _| Some(s),
            stealer_rate,
            |j| slow.get(j).and_then(|&(k, _)| victim_rate(k)),
        )?;
        Ok(planned.and_then(|(j, tail)| {
            let victim = *owners.get(slow.get(j)?.0)?;
            Some(PlannedSteal {
                victim,
                tail,
                kind: StealKind::SlowVictim,
            })
        }))
    }

    /// The steal arm of [`Work::pick`]: nothing pending worker `i` can serve.
    /// Hands `unit`, the unit's own live state, to the stolen range.
    ///
    /// # Errors
    ///
    /// As [`Work::pick`].
    fn steal(
        &mut self,
        i: usize,
        total_bytes: u64,
        coverage: &Coverage,
        missing: &[(u64, u64)],
        unit: Arc<Unit>,
    ) -> anyhow::Result<Option<Picked>> {
        // Nothing pending this worker can serve: every remaining byte is
        // either in flight on a busy worker or outside this worker's own
        // coverage. Steal the aligned tail of the missing remainder of the
        // range in flight whose covered suffix misses the most (#2303), or,
        // with none and the lane free to pull through, of a range whose lane
        // is slow beside this one (#2348). `in_flight[i]` is `None` here
        // (cleared before this pick), so this worker is excluded from the
        // remaining set and never steals from itself. The split weighs the
        // remainder by the two lanes' rates: this worker's over its last
        // unit, and the victim's on its unit so far.
        let (stealer_rate, slow_from) = self.stealer(i);
        let Some(PlannedSteal { victim, tail, kind }) =
            self.plan_steal(total_bytes, coverage, missing, stealer_rate, slow_from)?
        else {
            *self.slot_mut(i)? = None;
            return Ok(None);
        };

        // Trim the victim to end at the split point, so a later freed worker sees
        // the shortened tail and cannot re-steal the part this worker just took:
        // at most one lane owns any range, by construction, in the work-state.
        // Under the same lock, lower the victim's end to the split: its leg
        // stops there on its open stream, and the victim opens no new stream
        // for the part it keeps.
        //
        // Every branch that cannot complete that trim DECLINES the steal instead
        // of proceeding. Handing out `tail` with the victim untrimmed would leave
        // two workers owning overlapping ranges, and both would pay for the
        // overlap: the exact double-pay the trim exists to prevent.
        let Some(victim_unit) = self.live.get(victim).map(Arc::clone) else {
            *self.slot_mut(i)? = None;
            return Ok(None);
        };
        let trimmed = match self.in_flight.get_mut(victim) {
            Some(Some((start, len))) if victim != i && tail.fetch_start() > *start => {
                let end = start.saturating_add(*len);
                *len = tail.fetch_start() - *start;
                Some((*start, end))
            }
            _ => None,
        };
        let Some((victim_start, victim_end)) = trimmed else {
            *self.slot_mut(i)? = None;
            return Ok(None);
        };
        // Lower the end, then read the victim's frontier again. Both sides
        // order their two accesses `SeqCst`: the victim publishes each
        // verified group's end, then reads its own end (`fill_gap`). So
        // either the victim sees the lowered end before it verifies past the
        // split, or this read sees a frontier at or past the split.
        victim_unit
            .stop_at
            .fetch_min(tail.fetch_start(), Ordering::SeqCst);
        let frontier = victim_unit.progress.frontier.load(Ordering::SeqCst);
        let tail = if frontier < tail.fetch_start() {
            tail
        } else if frontier >= tail.fetch_end() {
            // The victim received its whole range before it saw the lowered
            // end, and its leg drains to the end: nothing is left to steal.
            if let Some(Some((_, len))) = self.in_flight.get_mut(victim) {
                *len = victim_end - victim_start;
            }
            *self.slot_mut(i)? = None;
            return Ok(None);
        } else {
            // The victim verified past the split before it saw the lowered
            // end. It stops at the frontier read here, or one chunk group
            // past it: start the tail at that frontier, so the two overlap by
            // at most one group and leave no byte unowned.
            if let Some(Some((_, len))) = self.in_flight.get_mut(victim) {
                *len = frontier - victim_start;
            }
            align_range(frontier, tail.fetch_end() - frontier, total_bytes)?
        };
        let stealer = self.providers.get(i).copied().flatten();
        let victim_provider = self.providers.get(victim).copied().flatten();
        let victim_range = self.in_flight.get(victim).copied().flatten();
        let victim_rate = victim_unit.rate();
        match kind {
            StealKind::CoveredSuffix => tracing::debug!(
                stealer = ?stealer,
                victim = ?victim_provider,
                kind = kind.as_str(),
                split = tail.fetch_start(),
                stolen = tail.fetch_len(),
                victim_range = ?victim_range,
                victim_frontier = frontier,
                stealer_rate,
                victim_rate,
                "stole the tail of a lane's missing remainder"
            ),
            // Rare, and the cure for a tail one slow lane would set alone:
            // visible without debug logs.
            StealKind::SlowVictim => tracing::info!(
                stealer = ?stealer,
                victim = ?victim_provider,
                kind = kind.as_str(),
                split = tail.fetch_start(),
                stolen = tail.fetch_len(),
                victim_range = ?victim_range,
                victim_frontier = frontier,
                stealer_rate,
                victim_rate,
                "stole the tail of a lane's missing remainder"
            ),
        }

        // A covered-suffix tail lies inside the stealer's coverage; a
        // slow-victim tail may not, and its node serves that part by
        // pull-through.
        let uncovered =
            !covers_byte_range(coverage, tail.fetch_start(), tail.fetch_len(), total_bytes);
        *self.slot_mut(i)? = Some((tail.fetch_start(), tail.fetch_len()));
        Ok(Some(Picked {
            range: tail,
            uncovered,
            unit,
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

    /// Cancel worker `victim`'s unit `unit`: its `fill_gap` is dropped, and it
    /// re-queues the missing bytes of its range and picks again. A proven size
    /// that leaves the unit's whole range past the end of the blob cancels it
    /// this way ([`Work::clip`]). The signal fires only while the victim still
    /// runs `unit`. A victim that has since finished, faulted, or re-queued
    /// holds no slot or runs a later unit, and cancelling that unit would be
    /// wrong. Set the flag then wake it, both under the caller's `Work` lock,
    /// serialized against the victim's own `pick` reset. `notify_one` stores a
    /// permit if the victim is not parked yet, so the signal is never lost.
    fn cancel_victim(&self, victim: usize, unit: u64) {
        let same_unit = self.units.get(victim) == Some(&unit)
            && self.in_flight.get(victim).is_some_and(Option::is_some);
        if !same_unit {
            tracing::debug!(victim, unit, "the unit to cancel has ended; cancel skipped");
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
pub(crate) fn block_runs(coverage: &Coverage) -> String {
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
    /// The unit in flight, whose rate leaves out the time parked here.
    unit: &'a Unit,
}

/// Ends a unit's park when its wait ends or is dropped: adds the park to the
/// unit's parked time when the unit's rate clock ran at its start, then
/// clears the park in progress with `Release`. [`Unit::rate_sample`] reads
/// the two in the reverse order, so a read between the two steps leaves the
/// park out twice and reads the unit as faster, never as slower.
struct ClosePark<'a>(&'a Unit, Instant, bool);

impl Drop for ClosePark<'_> {
    fn drop(&mut self) {
        let Self(unit, parked_at, counted) = *self;
        if counted {
            let nanos = u64::try_from(parked_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
            unit.parked.fetch_add(nanos, Ordering::Relaxed);
        }
        unit.park_open_at.store(u64::MAX, Ordering::Release);
    }
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
            let parked_at = Instant::now();
            // A parked worker holds no open leg, so it verifies nothing while
            // parked: the whole wait stays out of the unit's rate, once its
            // rate clock runs, both while it lasts and after it ends.
            let first_byte = self.unit.progress.first_byte.get().copied();
            if let Some(first) = first_byte {
                let open_at = u64::try_from(parked_at.saturating_duration_since(first).as_nanos())
                    .unwrap_or(u64::MAX - 1);
                self.unit.park_open_at.store(open_at, Ordering::Relaxed);
            }
            let _closed = ClosePark(self.unit, parked_at, first_byte.is_some());
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
struct Engine<'a, St, Pc> {
    /// The store every lane writes. Its bound is the planner's size, read
    /// fresh at each use: it grows while no size is proven and shrinks to a
    /// proven size.
    store: &'a St,
    hash: [u8; 32],
    pacer: &'a Pc,
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
    /// The delegated fetch's credential: a worker on a lane whose key it no
    /// longer holds ends at its next voucher boundary.
    credentials: Option<&'a CredentialSlot>,
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
async fn run_worker<St, S, Pc>(
    engine: &Engine<'_, St, Pc>,
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
    // await the handle OUTSIDE the `Work` lock while `Work::cancel_victim`
    // signals it under the lock.
    let (my_coverage, handle) = {
        let w = work.lock().await;
        match (w.coverage.get(i).cloned(), w.cancel.get(i).map(Arc::clone)) {
            (Some(c), Some(h)) => (c, h),
            _ => anyhow::bail!("worker index {i} out of range for lane slots"),
        }
    };
    let mut delivered = false;
    let mut pulled_through = None;
    let covered_served: Mutex<Vec<(u64, u64)>> = Mutex::new(Vec::new());
    let take_served = || {
        std::mem::take(
            &mut *covered_served
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    };
    // Per-worker resume/quote state. The pool's spend is NOT in here: it is a
    // property of the one shared pool and lives in `pool`.
    let mut counters = DriveCounters::new();
    // Wake every parked peer: this worker changed the work state.
    let wake = || progress_wake.notify_waiters();
    // Under consumption pacing, this worker's wait records when it is parked, so
    // `yield_to_front` can move it to an earlier re-queued range.
    let lane_parked = AtomicBool::new(false);
    let lane_parked_wake = Notify::new();
    // The spans this lane delivered and did not pay for when a funding refusal
    // ended its last worker. The lane owes them, so when it runs again its own
    // worker bills them first, although the store holds the bytes (ADR 003 §
    // Funding recovery).
    if !extra && !retired(engine.credentials, &lane) {
        let owed = lane.ledger.take_unpaid(hash);
        for (index, &(start, len)) in owed.iter().enumerate() {
            let settled = tokio::select! {
                biased;
                res = fill_gap(
                    store,
                    &lane.source,
                    engine.pacer,
                    &lane.ctx,
                    &lane.ledger,
                    hash,
                    start,
                    len,
                    &mut counters,
                    // The bytes are present: the bar already counts them.
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(engine.pool),
                    None,
                ) => res.map(Some),
                () = cancelled(&handle) => Ok(None),
            };
            let (unreached, err) = match settled {
                Ok(Some(())) => continue,
                // A cancel leaves this span and the rest owed.
                Ok(None) => (index, None),
                // Whatever ended the fill, this span stays owed from the
                // gap's paid frontier on. `take_unpaid` merges, so a part the
                // fill's own funding refusal also noted bills once.
                Err(err) => {
                    let end = start.saturating_add(len);
                    let paid = counters.gap_paid_frontier.unwrap_or(start).max(start);
                    lane.ledger.note_unpaid(hash, paid, end);
                    (index.saturating_add(1), Some(err))
                }
            };
            // The spans this worker did not reach stay owed.
            for &(rest, rest_len) in owed.iter().skip(unreached) {
                lane.ledger
                    .note_unpaid(hash, rest, rest.saturating_add(rest_len));
            }
            let Some(err) = err else { break };
            work.lock().await.park(i);
            wake();
            return Ok(WorkerEnd {
                provider,
                end: LaneEnd::Faulted {
                    err: Some(err),
                    range: crate::source_set::LaneRange {
                        offset: start,
                        len,
                        landed: 0,
                        past_end: false,
                        uncovered: false,
                    },
                    piece_at: start,
                    at: Instant::now(),
                },
                delivered,
                pulled_through,
                covered_served: take_served(),
                extra,
            });
        }
    }
    loop {
        // A swapped credential retires this lane: its last unit stopped at a
        // voucher boundary, paid for what it received. The rest goes back to
        // the queue, and the loop builds a lane under the new key.
        if retired(engine.credentials, &lane) {
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
            return Ok(WorkerEnd {
                provider,
                end: LaneEnd::Retired,
                delivered,
                pulled_through,
                covered_served: take_served(),
                extra,
            });
        }
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
            covered_served: take_served(),
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
            uncovered,
            unit,
        }) = picked
        else {
            // Nothing to start right now. An extra worker ends here. A lane's
            // own worker ends only when no lane holds work; otherwise it
            // parks: a peer's range is still draining toward a requeue or a
            // splittable size. It wakes on a peer's change to the work state,
            // or once a steal turns due on the `STEAL_RECHECK` clock (#2348):
            // a slow victim's rate gathers evidence as it streams, and its
            // progress wakes nothing.
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
            tokio::select! {
                () = parked.as_mut() => {}
                res = steal_recheck(store, work, i, &my_coverage) => res?,
            }
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
        // Drive each still-missing gap OUTSIDE the lock, racing it against a
        // cancel and the stall watchdog. Dropping the `fill_gap` future on either
        // leaves the store's checkpointed prefix intact. `in_flight[i]` stays the
        // whole picked range, trimmed only by a peer's steal, so the steal's
        // split (read from the store's missing runs) and this worker's
        // `requeue_missing` (the missing bytes of that trimmed range) agree. A
        // steal also lowers `stop_at`, and the gap in flight ends at the split.
        let mut terminal: Option<UnitOutcome> = None;
        // The start of the gap the unit ended on, for an overshoot check.
        let mut piece_at = r_start;
        let lane_wait = engine.pacing.map(|p| ParkedWait {
            inner: p.pacing_wait,
            parked: &lane_parked,
            parked_wake: &lane_parked_wake,
            unit: &unit,
        });
        for (g_start, g_len) in gaps {
            // A retired lane opens no new leg.
            if retired(engine.credentials, &lane) {
                break;
            }
            piece_at = g_start;
            let verified = &unit.progress.verified;
            let before = verified.load(Ordering::Relaxed);
            let outcome = {
                let fill = fill_gap(
                    store,
                    &lane.source,
                    engine.pacer,
                    &lane.ctx,
                    &lane.ledger,
                    hash,
                    g_start,
                    g_len,
                    &mut counters,
                    Some(engine.on_progress),
                    // The shared whole-blob delivered counter: every lane folds its
                    // own leg deltas in, so the bar reads one monotonic position.
                    Some(engine.progress_agg),
                    // This unit's progress: the watchdog judges its verified
                    // bytes, and a steal splits past its frontier.
                    Some(&unit.progress),
                    // Consumption pacing (#1848): with a `WindowPacer`, gate this
                    // lane against the shared consumer cursor so it never runs more
                    // than one read-ahead window ahead of what the consumer read.
                    // `None` keeps the eager, unbounded fan-out.
                    lane_wait.as_ref().map(|w| w as &dyn PacingWait),
                    engine.pacing.map(|p| p.downstream),
                    // The aggregate spend the deposit gate subtracts, which
                    // this lane must not treat as its own.
                    Some(engine.pool),
                    // The end a peer's steal lowers to its split.
                    Some(&unit.stop_at),
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
                    // this range back (it re-queues like a cancel) and take that
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
                    err = watchdog(store, g_start, g_len, engine.watchdog, verified) => {
                        UnitOutcome::Faulted(err)
                    }
                    // Never ends: on a swap it lowers the unit's end, and the
                    // leg stops at its next voucher boundary.
                    () = retire_on_swap(engine.credentials, &lane, &unit, g_start) => {
                        UnitOutcome::Cancelled
                    }
                }
            };
            let gap_landed = verified.load(Ordering::Relaxed).saturating_sub(before);
            if gap_landed > 0 {
                delivered = true;
                if uncovered {
                    pulled_through = Some(Instant::now());
                } else {
                    covered_served
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push((g_start, gap_landed));
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

        // The unit's verified bytes across its gaps, for the fault's log line.
        let landed = unit.progress.verified.load(Ordering::Relaxed);
        if landed > 0 {
            work.lock().await.record_rate(i, unit.rate());
        }
        match terminal {
            // Every gap filled, up to a peer's split if one stole the tail:
            // free the lane and pick again.
            None | Some(UnitOutcome::Completed) => {
                let split = unit.stop_at.load(Ordering::Acquire);
                if split < r_start.saturating_add(r_len) {
                    tracing::debug!(
                        %provider,
                        start = r_start,
                        split,
                        "a lane stopped its stream at a steal split"
                    );
                }
                // A retired lane keeps its range for the loop top, which
                // re-queues what it did not receive.
                if !retired(engine.credentials, &lane) {
                    work.lock().await.clear(i)?;
                }
            }
            // Cancelled: re-queue the remainder and stay live. The
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
                    covered_served: take_served(),
                    extra,
                });
            }
        }
        // An extra worker takes one piece, then gives its stream back. A
        // retired one first re-queues what it did not receive (loop top).
        if extra && !retired(engine.credentials, &lane) {
            work.lock().await.end_extra(i);
            wake();
            return Ok(WorkerEnd {
                provider,
                end: LaneEnd::Idle,
                delivered,
                pulled_through,
                covered_served: take_served(),
                extra,
            });
        }
        wake();
    }
}

/// Whether `lane` signs under a key `slot` no longer holds.
fn retired<S>(slot: Option<&CredentialSlot>, lane: &StreamCandidate<S>) -> bool {
    slot.is_some_and(|slot| slot.is_stale(&lane.ctx))
}

/// Retire `unit` at its next voucher boundary once `slot` no longer holds
/// `lane`'s key: lower its end to the chunk group its frontier reaches, at or
/// past `start`. The leg in flight stops there on its open stream, paid for
/// what it received ([`crate::BlobSource::stop`]). Never completes.
async fn retire_on_swap<S>(
    slot: Option<&CredentialSlot>,
    lane: &StreamCandidate<S>,
    unit: &Unit,
    start: u64,
) {
    if let Some(slot) = slot {
        loop {
            // The generation first: a swap after it wakes the wait below.
            let generation = slot.generation();
            if slot.is_stale(&lane.ctx) {
                // `SeqCst`, as a steal orders its end against the frontier.
                let frontier = unit.progress.frontier.load(Ordering::SeqCst).max(start);
                let group = decdn_bao_range::CHUNK_GROUP_BYTES;
                let end = frontier.div_ceil(group).saturating_mul(group);
                unit.stop_at.fetch_min(end, Ordering::SeqCst);
                break;
            }
            slot.swapped_since(generation).await;
        }
    }
    std::future::pending::<()>().await;
}

/// One worker's future: [`run_worker`], holding `hold`, what its stream
/// holds ([`Hold`]): a lane's own worker releases the lane's lease as it
/// ends, and every grant a [`LaneWiden`] made for it is given back.
async fn worker<St, S, Pc>(
    engine: &Engine<'_, St, Pc>,
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

/// Raise the pool's deposit to `lane`'s, which a lane that just built reads
/// from its row: the first lane names the deposit, and a later one carries any
/// refill its build made. `raise` lifts the watch and every lane context the
/// acquire shares (the run registry's, or its own started lanes'), so a lane
/// built before the refill stops gating on the old deposit.
fn raise_to_lane<S>(raise: &dyn Fn(U256), lane: &StreamCandidate<S>) {
    let seen = lane
        .ctx
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .deposit;
    raise(seen);
}

/// Wait until `slot` takes a swap past `generation`; never, without a slot.
async fn swap_after(slot: Option<&CredentialSlot>, generation: u64) {
    match slot {
        Some(slot) => slot.swapped_since(generation).await,
        None => std::future::pending().await,
    }
}

/// Everything signed under `slot`'s current key: what earlier fetches signed,
/// from the application's local lane records ([`CredentialSlot::record_spend`]),
/// plus what this fetch's lanes signed past their recorded priors, from the
/// run registry's lanes when there is one and from `lanes` otherwise.
pub(crate) fn key_spend<S>(
    slot: &CredentialSlot,
    lanes: &[Arc<StreamCandidate<S>>],
    ledgers: Option<&LaneLedgers>,
) -> U256 {
    let signer = slot.current().signer.address();
    let this_fetch = match ledgers {
        Some(reg) => reg.signed_by(signer),
        None => lanes
            .iter()
            .filter(|lane| {
                lane.ctx
                    .lock()
                    .is_ok_and(|ctx| ctx.client_signer.address() == signer)
            })
            .map(|lane| crate::credential::signed_past_prior(&lane.ctx, &lane.ledger))
            .fold(U256::ZERO, U256::saturating_add),
    };
    slot.recorded_spend(signer).saturating_add(this_fetch)
}

/// The local view of `slot`'s current capability against the work left
/// ([`CredentialView`]): everything signed under its key ([`key_spend`]),
/// and the missing bytes of `ranges` at the highest quote.
///
/// # Errors
///
/// A store query failure.
async fn credential_view<St, S>(
    slot: &CredentialSlot,
    store: &St,
    ranges: &[(u64, u64)],
    lanes: &[Arc<StreamCandidate<S>>],
    ledgers: Option<&LaneLedgers>,
    quotes: &QuoteMax,
) -> anyhow::Result<CredentialView>
where
    St: IngestStore,
{
    let credential = slot.current();
    let spent_by_key = key_spend(slot, lanes, ledgers);
    let remaining = ranges_content_len(&missing_chunks(store, ranges).await?, store.total_bytes());
    Ok(CredentialView::of(
        &credential,
        spent_by_key,
        remaining,
        quotes,
        unix_now(),
    ))
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
/// is larger. The gate is evaluated at each `fill_gap` leg boundary; the hard
/// backstop against a node redeeming past the deposit stays on-chain.
///
/// # Funding recovery (ADR 003 § Funding recovery)
///
/// A lane never adds funds. A source whose next voucher the deposit cannot
/// cover, or that refuses the pool `Unfunded`, is priced out at the current
/// deposit while other sources keep serving. Only when the candidate set is
/// exhausted with a source priced out does the loop run one step through
/// [`AcquireEnv::funder`], under [`AcquireEnv::recovery`]'s progress rule. A
/// step that raises the deposit credits it to every lane the view covers, and
/// the priced-out sources become usable again for one more pass. During the
/// settle window after a top-up, a source's funding refusal holds it briefly
/// instead of pricing it out again.
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
///   unanimous verdict of the sources, the former once no recovery step can
///   raise the deposit;
/// - [`crate::PoolReplaced`] when the recovery step opened a new pool: the
///   caller runs the remaining work again against it;
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

    // The pool deposit every lane draws on, as the loop last saw it. Each built
    // lane raises it to the deposit its row read (the first names it, a later
    // one carries any refill its build made); a funding recovery step
    // publishes the new value.
    let (deposit_tx, mut deposit_rx) = tokio::sync::watch::channel(U256::ZERO);
    // The recovery steps that topped up or settled, as this loop last saw
    // them: a later one a sibling entry takes answers this loop's exhausted
    // set too.
    let mut top_ups_seen = env.recovery.top_ups();
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
    // Lift the pool's deposit to at least a value a lane build read: never
    // lowers one a concurrent top-up credited above it.
    let raise = |seen: U256| {
        match env.ledgers {
            Some(reg) => reg.raise_all(seen),
            None => {
                for (ctx, _) in pool_lanes
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .iter()
                {
                    let mut ctx = ctx.lock().unwrap_or_else(PoisonError::into_inner);
                    ctx.deposit = ctx.deposit.max(seen);
                }
            }
        }
        deposit_tx.send_if_modified(|current| {
            let raised = seen > *current;
            if raised {
                *current = seen;
            }
            raised
        });
    };
    // The highest rate and voucher interval any lane was quoted: the price of
    // the work left, for a delegated fetch's running-low signal.
    let quotes = QuoteMax::default();
    let pool = SharedPool {
        spent: &spent,
        quotes: Some(&quotes),
    };
    // The credential generation the loop's lanes were built under, and
    // whether a node refused the capability itself since then.
    let mut generation_seen = env.credentials.map_or(0, CredentialSlot::generation);
    let mut capability_refused = false;

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
    // The highest position reported so far: each rise is newly verified bytes
    // for the recovery gate's progress rule.
    let verified_seen = AtomicU64::new(base_present);
    // Every verified byte ticks the stop clock and the recovery gate, then
    // reaches the caller.
    let on_progress = |position: u64, total: u64| {
        let before = verified_seen.fetch_max(position, Ordering::AcqRel);
        if position > before {
            env.recovery.record_verified(position - before);
        }
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
        work: &work,
        progress_wake: &progress_wake,
        progress_agg: &progress_agg,
        on_progress: &on_progress,
        pool: &pool,
        watchdog,
        pacing: env.pacing,
        credentials: env.credentials,
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

            // A swapped credential: every built lane on the old key drops, so
            // each provider's next start builds a lane under the new one. The
            // running lanes retire themselves at their next voucher boundary.
            // What the sources refused under the old key says nothing about
            // the new one.
            if let Some(slot) = env.credentials
                && slot.generation() != generation_seen
            {
                generation_seen = slot.generation();
                sources.drop_lanes(|lane| slot.is_stale(&lane.ctx));
                ready.retain(|(_, lane)| !slot.is_stale(&lane.ctx));
                health.clear_unaffordable();
                capability_refused = false;
                let view = credential_view(slot, store, &want.ranges, &started.0, env.ledgers, &quotes)
                    .await?;
                slot.report(view.event());
            }

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
                // may have set or lifted a source's bar from pull-through, or
                // dropped a block a source refused from its coverage, since
                // the last pass.
                sources.expire_pull_through_bars(now);
                for (&provider, &slot) in &slots {
                    w.set_no_uncovered(slot, sources.no_pull_through(provider));
                    if let Some(coverage) =
                        sources.holder(provider).and_then(|h| h.coverage.as_ref())
                    {
                        w.set_coverage(slot, coverage);
                    }
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

            // The exhausted candidate set: no lane runs or builds, and a fresh
            // discovery found nothing new. With a source priced out, this is the
            // one place the loop adds funds (ADR 003 § Funding recovery). A
            // raised deposit makes the priced-out sources usable for one more
            // pass.
            if running.is_empty() && connecting.is_empty() && discovering.is_none() {
                let only_uncovered = only_uncovered_left(sources, &*work.lock().await, total_bytes);
                if let Some(err) = sources.exhausted(deposit, only_uncovered) {
                    if err.downcast_ref::<NoAffordableSource>().is_none() {
                        return Err(err);
                    }
                    // A delegated fetch cannot add funds. When its capability
                    // no longer pays, it waits for the application to swap
                    // in a new one; when the capability still pays, only the
                    // pool owner can help.
                    if let Some(slot) = env.credentials {
                        let view = credential_view(
                            slot,
                            store,
                            &want.ranges,
                            &started.0,
                            env.ledgers,
                            &quotes,
                        )
                        .await?;
                        slot.report(view.event());
                        let pool = slot.pool_id();
                        let Some(cause) = view
                            .cause()
                            .or(capability_refused.then_some(CapabilityCause::Revoked))
                        else {
                            return Err(err.context(FundingNeeded::PublisherPool { pool }));
                        };
                        match env.recovery.swap_step(slot, generation_seen).await {
                            // The loop top rebuilds the lanes under it.
                            SwapStep::Swapped => continue,
                            SwapStep::NoProgress | SwapStep::TimedOut => {
                                return Err(
                                    err.context(FundingNeeded::NewCapability { pool, cause })
                                );
                            }
                        }
                    }
                    // A sibling entry may have priced a shared source out
                    // at a deposit above this loop's view of it.
                    let seen = sources.priced_out_at(deposit);
                    let stepped = env
                        .recovery
                        .step(
                            env.funder,
                            seen,
                            &mut top_ups_seen,
                            || pool_deposit(&deposit_rx, &started.0),
                            spent(),
                        )
                        .await;
                    let raised = after_step(stepped, err)?;
                    if raised > deposit {
                        credit(raised)?;
                    }
                    if raised <= seen {
                        // A step that settles leaves the deposit where it is:
                        // the pool already held it, and the sources' refusals
                        // were their stale view of it. Ask them again; the
                        // settle window holds a refusal that comes back
                        // before their chain watchers catch up.
                        health.clear_unaffordable();
                    }
                    continue;
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
                    let WorkerEnd { provider, end, delivered, pulled_through, covered_served, extra } = match end {
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
                        sources.record_covered_served(provider, &covered_served);
                        charged.remove(&provider);
                    }
                    if let Some(at) = pulled_through {
                        sources.record_pull_through(provider, at);
                    }
                    // A retired lane drops; its provider's next start builds a
                    // lane under the current credential.
                    if let (LaneEnd::Retired, Some(slot)) = (&end, env.credentials) {
                        sources.drop_lanes(|lane| slot.is_stale(&lane.ctx));
                    }
                    if let LaneEnd::Faulted { err: Some(err), .. } = &end
                        && crate::fault::refuses_capability(err)
                    {
                        capability_refused = true;
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
                            // Only an uncovered range: a node's per-signer live
                            // cap, which extra streams hit, refuses as a plain
                            // `NotFound` too, and inside the coverage that says
                            // nothing about the blocks it holds.
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
                        } else if !(env.recovery.settling(at)
                            && sources.hold_while_settling(provider, &err, at))
                        {
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
                                raise_to_lane(&raise, &lane);
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
                // A swapped credential: the loop top rebuilds the lanes.
                () = swap_after(env.credentials, generation_seen) => {}
                // A leg proved a size: the loop top clips the work to it.
                () = bound_moved.notified() => {}
                flushed = poll_opt(&mut record), if record.is_some() => {
                    record = None;
                    flushed?;
                }
                _ = flush.tick() => {
                    if let Some(slot) = env.credentials {
                        let view = credential_view(
                            slot,
                            store,
                            &want.ranges,
                            &started.0,
                            env.ledgers,
                            &quotes,
                        )
                        .await?;
                        slot.report(view.event());
                    }
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
mod tests;
