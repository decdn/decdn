//! The sources of one blob (ADR 039 § Source set and selection).
//!
//! A `SourceSet` holds every provider known to hold the blob, the lane built
//! for each, and when each may be tried again. It never drops a source: a
//! delivery fault cools it (in the command-wide [`PeerHealth`]), a price
//! refusal parks it until the deposit rises, and a lane-build or discovery
//! error backs off and retries. A partial holder that keeps refusing ranges
//! outside its coverage is barred from pull-through and serves only the
//! blocks it covers. A source that refuses the blob as larger than its size
//! ceiling is excluded for this blob. It ends a fetch only on a unanimous
//! verdict: every known source is priced out, or every one says it does not
//! hold the blob, refused it as too large, or is barred from the only work
//! left, and a fresh discovery found nothing new.
//!
//! The state is synchronous. The acquire loop runs the `connect` and `discover`
//! futures itself, so lanes keep streaming while a lane builds or discovery
//! runs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alloy::primitives::{Address, U256};
use decdn_protocol::Coverage;
use decdn_protocol::client::StreamError;
use tokio::time::{Duration, Instant};

use crate::UpstreamRefused;
use crate::fault::{Fault, LaneBuildFault, classify};
use crate::health::PeerHealth;
use crate::source::{BlobSource, SourceFuture, SourceStream};
use crate::streamer::StreamCandidate;

/// The first wait before a failed lane build is retried.
pub const BUILD_RETRY_BASE: Duration = Duration::from_secs(1);
/// The longest wait before a failed lane build is retried.
pub const BUILD_RETRY_CAP: Duration = Duration::from_secs(30);
/// The first wait between discoveries.
pub const DISCOVERY_BASE: Duration = Duration::from_secs(5);
/// The longest wait between discoveries.
pub const DISCOVERY_CAP: Duration = Duration::from_mins(5);

/// One provider that holds (part of) the blob.
#[derive(Debug, Clone)]
pub struct Holder {
    /// The provider's on-chain address: the payment lane's payee and the key
    /// of its health.
    pub provider: Address,
    /// The blocks it serves: its advertised coverage less any block dropped
    /// after repeated refusals inside it, or
    /// `None` for the whole blob.
    pub coverage: Option<Coverage>,
    /// The probed round-trip time. Lower starts first.
    pub rtt_ms: f64,
    /// Whether a probe reported that this provider holds the blob. A probed
    /// holder is never marked absent: on the wire a `NotFound` also means load
    /// shed, a per-signer cap, or a pool the node cannot confirm yet. To a
    /// whole holder it is a delivery fault only. A partial holder's counts
    /// toward dropping the refused blocks from its coverage (inside it) or
    /// barring it from pull-through (outside it), each for a while. A provider the probe did not report as a holder (a
    /// pull-through or proxy-warming target) counts as absent after
    /// [`ABSENT_AFTER_NOT_FOUND`] such answers with no verified byte between
    /// them.
    pub probed_holder: bool,
}

/// The range a lane held when it faulted ([`SourceSet::record_fault`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneRange {
    /// The range's first byte.
    pub offset: u64,
    /// The range's length in bytes.
    pub len: u64,
    /// The bytes of the range that verified before the fault.
    pub landed: u64,
    /// The piece that faulted starts at or past the end the fetch knows: the
    /// proven size, or with none proven the smaller of the first size claim
    /// and the bound. The bound can overshoot the blob, so a `NotFound` for
    /// such a piece is an overshoot refusal: it cools the provider and never
    /// counts toward marking it absent ([`ABSENT_AFTER_NOT_FOUND`]).
    pub past_end: bool,
    /// The range lies wholly outside the coverage of the lane that held it:
    /// the lane took it for its node to serve by pull-through. A `NotFound`
    /// for such a range from a probed partial holder counts toward barring
    /// that holder from pull-through ([`SourceSet::no_pull_through`]).
    pub uncovered: bool,
}

/// How many `NotFound` answers in a row, with no verified byte between them,
/// mark a provider that is not a probed holder as absent. The same count of
/// `NotFound` answers for ranges outside a probed partial holder's coverage
/// bars it from pull-through ([`SourceSet::no_pull_through`]), and for one
/// block inside it drops that block from its coverage for a while.
pub const ABSENT_AFTER_NOT_FOUND: u32 = 3;

/// How long a bar from pull-through set by `NotFound` answers lasts. A node
/// also answers `NotFound` when it sheds miss load or its chain view is stale,
/// so the bar ends after the longest cooldown and the holder takes one
/// uncovered range again as a probe. It is also how long a block dropped from
/// a partial holder's coverage stays out, and the quiet time after which a
/// block's count of covered refusals starts again.
pub const PULL_THROUGH_BAR: Duration = crate::health::COOL_CAP;

/// Where a [`SourceSet`] finds holders and builds their lanes.
pub trait SourceProvider: Send + Sync {
    /// The paid source a built lane fetches from.
    type Source: BlobSource;

    /// Find and probe the current holders of `hash`.
    fn discover(&self, hash: [u8; 32]) -> SourceFuture<'_, Vec<Holder>>;

    /// Holders found after the set started, pushed as they are found. The
    /// stream ends when no more will come. The acquire loop takes it once,
    /// when it starts, and asks for no discovery while it is open. `None`,
    /// the default, is a provider with nothing to push.
    fn arrivals(&self) -> Option<SourceStream<'_, Holder>> {
        None
    }

    /// Build the paid lane to `holder`. An error here is chain-side: the set
    /// retries it later and never blames the holder.
    fn connect<'a>(&'a self, holder: &'a Holder)
    -> SourceFuture<'a, StreamCandidate<Self::Source>>;

    /// Called once for each delivery fault `holder` raises.
    fn on_source_fault(&self, _holder: &Holder) {}
}

impl<P: SourceProvider> SourceProvider for &P {
    type Source = P::Source;

    fn discover(&self, hash: [u8; 32]) -> SourceFuture<'_, Vec<Holder>> {
        (**self).discover(hash)
    }

    fn arrivals(&self) -> Option<SourceStream<'_, Holder>> {
        (**self).arrivals()
    }

    fn connect<'a>(
        &'a self,
        holder: &'a Holder,
    ) -> SourceFuture<'a, StreamCandidate<Self::Source>> {
        (**self).connect(holder)
    }

    fn on_source_fault(&self, holder: &Holder) {
        (**self).on_source_fault(holder);
    }
}

/// No known source can be paid from the pool's deposit, the top-up budget is
/// spent, and a fresh discovery found no cheaper source.
#[derive(Debug)]
pub struct NoAffordableSource {
    /// The pool deposit every source refused at.
    pub deposit: U256,
}

impl std::fmt::Display for NoAffordableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "no provider's next voucher fits the pool's deposit of {} (micro-USDC); top up the pool",
            self.deposit
        )
    }
}

impl std::error::Error for NoAffordableSource {}

/// Every known source says it does not hold the blob, and a fresh discovery
/// found no other holder. The acquire loop raises it as context on the last
/// refusal that marked a source absent, so the error chain also holds that
/// [`UpstreamRefused`].
#[derive(Debug)]
pub struct NoSourceHasBlob;

impl std::fmt::Display for NoSourceHasBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no provider holds this blob; check the hash")
    }
}

impl std::error::Error for NoSourceHasBlob {}

/// A backoff that doubles from `base` to `cap`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Backoff {
    /// When the next try may run.
    pub(crate) next_at: Instant,
    /// The failures in a row so far.
    pub(crate) attempts: u32,
}

impl Backoff {
    /// No failure yet: the next try may run at `now`.
    pub(crate) const fn new(now: Instant) -> Self {
        Self {
            next_at: now,
            attempts: 0,
        }
    }

    /// One more failure at `now`: wait `base`, doubled for each earlier
    /// failure in a row, at most `cap`.
    pub(crate) fn fail(self, now: Instant, base: Duration, cap: Duration) -> Self {
        let attempts = self.attempts.saturating_add(1);
        let wait = base
            .saturating_mul(2u32.saturating_pow(attempts.saturating_sub(1)))
            .min(cap);
        Self {
            next_at: now + wait,
            attempts,
        }
    }
}

/// The sources of one blob.
pub struct SourceSet<'p, P: SourceProvider> {
    provider: &'p P,
    hash: [u8; 32],
    health: Arc<PeerHealth>,
    holders: Vec<Holder>,
    lanes: HashMap<Address, Arc<StreamCandidate<P::Source>>>,
    build_retry: HashMap<Address, Backoff>,
    /// When each built lane that found no free stream may start again
    /// ([`Self::start_refused`]).
    start_retry: HashMap<Address, Instant>,
    absent: HashSet<Address>,
    /// Each non-holder's `NotFound` answers since its last verified byte.
    not_found: HashMap<Address, u32>,
    /// Each probed partial holder's `NotFound` answers for ranges outside its
    /// coverage since it last served such a range.
    pull_through_not_found: HashMap<Address, u32>,
    /// When each probed partial holder last said `NotFound` to a range
    /// outside its coverage. A verified byte outside its coverage clears its
    /// count and bar only when it lands after this, so a worker that ends
    /// late cannot lift a bar set after its byte.
    pull_through_refused_at: HashMap<Address, Instant>,
    /// When each probed partial holder last served a verified byte outside its
    /// coverage. A `NotFound` for such a range that came before this is stale
    /// and does not count, so the order in which the loop takes worker ends
    /// cannot change the bar. A tie goes to the refusal.
    pull_through_served_at: HashMap<Address, Instant>,
    /// Probed partial holders that said `NotFound` to
    /// [`ABSENT_AFTER_NOT_FOUND`] ranges outside their coverage, each with
    /// the time its bar ends ([`PULL_THROUGH_BAR`] after the refusal that set
    /// it). Each one serves only the blocks it covers until then.
    no_pull_through: HashMap<Address, Instant>,
    /// Each probed partial holder's `NotFound` answers for ranges inside its
    /// coverage, keyed by every discovery block the refused part of a range
    /// touches, with the time of the last one counted. Answers within
    /// [`crate::health::COOL_BASE`] of the last one count once, a count whose
    /// last answer is [`PULL_THROUGH_BAR`] old starts again, and a verified
    /// byte in the block clears it ([`SourceSet::record_covered_served`]). So
    /// transient refusals (load shed, a stale chain view) do not add up.
    covered_not_found: HashMap<(Address, u32), (u32, Option<Instant>)>,
    /// Blocks each probed partial holder advertised but refused
    /// [`ABSENT_AFTER_NOT_FOUND`] times, each with the time its drop ends
    /// ([`PULL_THROUGH_BAR`] after the refusal that set it). Its coverage
    /// claim for them is likely stale (evicted, or a record wider than its
    /// store), so they leave its coverage until then, rediscovery included.
    stale_blocks: HashMap<Address, HashMap<u32, Instant>>,
    /// The coverage each holder with a dropped block last advertised: what
    /// its coverage returns to as each drop ends.
    advertised: HashMap<Address, Coverage>,
    /// How many times in a row each probed partial holder has been barred
    /// from pull-through. A first bar can come from transient refusals that
    /// also read as `NotFound` (load shed, a stale chain view), so only a
    /// holder barred again after its probe counts as one that cannot serve
    /// the work left.
    pull_through_bars: HashMap<Address, u32>,
    /// Probed partial holders that refused the blob as larger than their size
    /// ceiling. The ceiling is a stable node policy (ADR 005), so neither a
    /// verified byte nor a rediscovery lifts this bar. Each one serves only
    /// the blocks it covers.
    too_large_pull_through: HashSet<Address>,
    /// Providers other than partial holders that refused the blob as larger
    /// than their size ceiling (`StreamError::BlobTooLarge`). The ceiling is a
    /// stable node policy (ADR 005), so none of them starts again for this
    /// blob, and a rediscovery does not lift it. A node applies the ceiling
    /// only to a pull-through, so the same refusal from a partial holder bars
    /// only its pull-through ([`Self::no_pull_through`]).
    too_large: HashSet<Address>,
    /// The refusal that last marked a source absent, barred it from
    /// pull-through or excluded it as too small for the blob: the cause the
    /// [`NoSourceHasBlob`] stop carries.
    last_absent: Option<UpstreamRefused>,
    discovery: Option<Backoff>,
    /// Bumped each time a source newly joins `absent`, `no_pull_through`,
    /// `too_large_pull_through` or `too_large`.
    mark_epoch: u64,
    /// The deposit and mark epoch of the last successful discovery. A
    /// unanimous stop needs a discovery at the current pair.
    discovered_at: Option<(U256, u64)>,
    /// The deposit and mark epoch of the last discovery attempt, success or
    /// failure. Gates the unanimous-stop bypass in [`Self::wants_discovery`]
    /// so a failing discovery still backs off instead of spinning.
    looked_at: Option<(U256, u64)>,
}

impl<P: SourceProvider> std::fmt::Debug for SourceSet<'_, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceSet")
            .field("holders", &self.holders.len())
            .field("lanes", &self.lanes.len())
            .finish_non_exhaustive()
    }
}

impl<'p, P: SourceProvider> SourceSet<'p, P> {
    /// A set over the `holders` the command already resolved for `hash`.
    #[must_use]
    pub fn new(
        provider: &'p P,
        hash: [u8; 32],
        health: Arc<PeerHealth>,
        holders: Vec<Holder>,
    ) -> Self {
        let mut set = Self {
            provider,
            hash,
            health,
            holders: Vec::new(),
            lanes: HashMap::new(),
            build_retry: HashMap::new(),
            start_retry: HashMap::new(),
            absent: HashSet::new(),
            not_found: HashMap::new(),
            pull_through_not_found: HashMap::new(),
            pull_through_refused_at: HashMap::new(),
            pull_through_served_at: HashMap::new(),
            no_pull_through: HashMap::new(),
            covered_not_found: HashMap::new(),
            stale_blocks: HashMap::new(),
            advertised: HashMap::new(),
            pull_through_bars: HashMap::new(),
            too_large_pull_through: HashSet::new(),
            too_large: HashSet::new(),
            last_absent: None,
            discovery: None,
            mark_epoch: 0,
            discovered_at: None,
            looked_at: None,
        };
        set.merge(holders);
        set
    }

    /// The provider seam, borrowed for as long as the set's provider lives, so
    /// the acquire loop can run its futures while it mutates the set.
    #[must_use]
    pub const fn provider(&self) -> &'p P {
        self.provider
    }

    /// The blob these sources hold.
    #[must_use]
    pub const fn hash(&self) -> [u8; 32] {
        self.hash
    }

    /// The command-wide health the set records into.
    #[must_use]
    pub const fn health(&self) -> &Arc<PeerHealth> {
        &self.health
    }

    /// Every known holder: the starting ones and those discovery added.
    #[must_use]
    pub fn holders(&self) -> &[Holder] {
        &self.holders
    }

    /// The known holder for `provider`.
    #[must_use]
    pub fn holder(&self, provider: Address) -> Option<&Holder> {
        self.holders.iter().find(|h| h.provider == provider)
    }

    /// The nearest source that may start now and is not already `running`. A
    /// source that refused the blob as too large never starts again.
    #[must_use]
    pub fn next_to_start(
        &self,
        now: Instant,
        deposit: U256,
        running: &HashSet<Address>,
    ) -> Option<Holder> {
        self.holders
            .iter()
            .filter(|h| !running.contains(&h.provider))
            .filter(|h| !self.too_large.contains(&h.provider))
            .filter(|h| {
                self.build_retry
                    .get(&h.provider)
                    .is_none_or(|b| now >= b.next_at)
            })
            .filter(|h| {
                self.start_retry
                    .get(&h.provider)
                    .is_none_or(|at| now >= *at)
            })
            .filter(|h| self.health.usable(h.provider, now, deposit))
            .min_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms))
            .cloned()
    }

    /// The lane already built for `provider`.
    #[must_use]
    pub fn cached_lane(&self, provider: Address) -> Option<Arc<StreamCandidate<P::Source>>> {
        self.lanes.get(&provider).cloned()
    }

    /// Take every built lane out of the set, keyed by provider. A caller that
    /// ran [`crate::first_open()`] on this set hands the lanes on to the set its
    /// fetch builds, so the fetch reuses them instead of building them again.
    pub fn take_lanes(&mut self) -> Vec<(Address, Arc<StreamCandidate<P::Source>>)> {
        self.lanes.drain().collect()
    }

    /// Record the result of `connect` for `provider`. A failure schedules a
    /// retry and comes back as a [`LaneBuildFault`].
    ///
    /// # Errors
    ///
    /// The build error, wrapped in [`LaneBuildFault`].
    pub fn lane_built(
        &mut self,
        provider: Address,
        built: anyhow::Result<StreamCandidate<P::Source>>,
        now: Instant,
    ) -> anyhow::Result<Arc<StreamCandidate<P::Source>>> {
        match built {
            Ok(lane) => {
                self.build_retry.remove(&provider);
                let lane = Arc::new(lane);
                self.lanes.insert(provider, Arc::clone(&lane));
                Ok(lane)
            }
            Err(err) => {
                let prior = self
                    .build_retry
                    .get(&provider)
                    .copied()
                    .unwrap_or(Backoff::new(now));
                self.build_retry
                    .insert(provider, prior.fail(now, BUILD_RETRY_BASE, BUILD_RETRY_CAP));
                Err(anyhow::Error::new(LaneBuildFault(err)))
            }
        }
    }

    /// Hold `provider`'s built lane back until `until`: it found no free
    /// stream to start again on ([`crate::LaneWiden`]). Unlike a failed
    /// build, the wait does not grow, so the lane starts again soon after a
    /// stream frees. Returns `true` for the first refusal since the lane last
    /// started.
    pub(crate) fn start_refused(&mut self, provider: Address, until: Instant) -> bool {
        self.start_retry.insert(provider, until).is_none()
    }

    /// Clear `provider`'s start hold: its built lane took a stream.
    pub(crate) fn start_taken(&mut self, provider: Address) {
        self.start_retry.remove(&provider);
    }

    /// Record a refusal on an extra stream of `provider`'s lane
    /// ([`crate::LaneWiden`]) for `range`, given at `at`, while the lane's own
    /// worker runs or the node is already charged for this outage. The node
    /// does not cool for it. A `NotFound` for a range outside the coverage of
    /// a probed partial holder counts toward barring it from pull-through,
    /// and a size-ceiling refusal bars it at once, as on its own stream. The
    /// scheduler passes only uncovered ranges here: an extra stream's covered
    /// `NotFound` is most often the node's per-signer live cap.
    pub(crate) fn record_extra_refusal(
        &mut self,
        provider: Address,
        err: &anyhow::Error,
        range: LaneRange,
        at: Instant,
    ) {
        let Some(refused) = err.downcast_ref::<UpstreamRefused>() else {
            return;
        };
        if crate::fault::says_absent(err) && !range.past_end {
            self.record_not_found(provider, refused, Some(&range), at);
        }
        if matches!(refused.error(), StreamError::BlobTooLarge) {
            self.record_too_large(provider, refused);
        }
    }

    /// Record the fault `provider`'s lane ended with, and return its class.
    ///
    /// Each fault is logged at info where it is recorded: the provider, the
    /// blob, the lane's `range` and the bytes of it that landed when the lane
    /// held one, and the error. A fetch that recovers reports no lane fault,
    /// so this line is the record of it.
    pub fn record_fault(
        &mut self,
        provider: Address,
        err: &anyhow::Error,
        range: Option<LaneRange>,
        now: Instant,
        deposit: U256,
    ) -> Fault {
        let fault = classify(err);
        let hash = blake3::Hash::from_bytes(self.hash).to_hex();
        if let Some(LaneRange {
            offset,
            len,
            landed,
            past_end,
            uncovered,
        }) = range
        {
            tracing::info!(
                %provider,
                %hash,
                offset,
                len,
                landed,
                past_end,
                uncovered,
                ?fault,
                error = %format_args!("{err:#}"),
                "a lane faulted; its remainder goes to the other lanes"
            );
        } else {
            tracing::info!(
                %provider,
                %hash,
                ?fault,
                error = %format_args!("{err:#}"),
                "a source faulted"
            );
        }
        match fault {
            Fault::Source => {
                if crate::fault::says_absent(err)
                    && !range.is_some_and(|r| r.past_end)
                    && let Some(refused) = err.downcast_ref::<UpstreamRefused>()
                {
                    self.record_not_found(provider, refused, range.as_ref(), now);
                }
                if let Some(refused) = err.downcast_ref::<UpstreamRefused>()
                    && matches!(refused.error(), StreamError::BlobTooLarge)
                {
                    self.record_too_large(provider, refused);
                }
                if let Some(holder) = self.holder(provider) {
                    self.provider.on_source_fault(holder);
                }
            }
            Fault::Fatal(_) | Fault::Unaffordable | Fault::Transient => {}
        }
        self.health.record(provider, fault, now, deposit);
        fault
    }

    /// Count a `NotFound` from `provider` for a piece below the end the fetch
    /// knows ([`LaneRange::past_end`]). A provider that is not a probed
    /// holder is marked absent once its count reaches
    /// [`ABSENT_AFTER_NOT_FOUND`]. A probed holder is never marked absent.
    /// For a partial holder, an answer for a range outside its coverage
    /// ([`LaneRange::uncovered`]) counts toward barring it from pull-through,
    /// and one for a range inside it counts toward dropping each block the
    /// refused part of the range touches from its coverage. To a whole holder the answer is a
    /// delivery fault only.
    fn record_not_found(
        &mut self,
        provider: Address,
        refused: &UpstreamRefused,
        range: Option<&LaneRange>,
        now: Instant,
    ) {
        let Some(holder) = self.holder(provider) else {
            return;
        };
        if holder.probed_holder {
            if holder.coverage.is_some() {
                match range {
                    Some(range) if range.uncovered => {
                        self.record_pull_through_refusal(provider, refused, now);
                    }
                    Some(range) => self.record_covered_refusal(provider, refused, range, now),
                    None => {}
                }
            }
            return;
        }
        let count = self.not_found.entry(provider).or_insert(0);
        *count = count.saturating_add(1);
        if *count < ABSENT_AFTER_NOT_FOUND {
            return;
        }
        let newly = self.absent.insert(provider);
        self.marked(newly, refused);
    }

    /// Record that `provider` refused the blob as larger than its size
    /// ceiling. A node applies the ceiling only when it pulls the blob
    /// through, and serves the blocks it holds without it. So a partial
    /// holder is barred from pull-through only, and keeps the blocks it
    /// covers. Any other source never starts again for this blob.
    fn record_too_large(&mut self, provider: Address, refused: &UpstreamRefused) {
        let partial = self
            .holder(provider)
            .is_some_and(|holder| holder.coverage.is_some());
        let newly = if partial {
            self.too_large_pull_through.insert(provider)
        } else {
            self.too_large.insert(provider)
        };
        self.marked(newly, refused);
    }

    /// Count a `NotFound` from probed partial holder `provider` for `range`,
    /// which its coverage claims, given at `at`. The node refuses the whole
    /// range at its first chunk it does not hold, and the refusal does not say
    /// which chunk that is, so the count goes to every block `range` touches.
    /// A block that reaches [`ABSENT_AFTER_NOT_FOUND`] answers leaves the
    /// holder's coverage for [`PULL_THROUGH_BAR`]: its ranges go to the other
    /// sources, and the holder keeps the blocks it was not refused. A
    /// `NotFound` can also be a transient refusal, so the drop ends, and the
    /// block's count starts again ([`Self::expire_pull_through_bars`]).
    fn record_covered_refusal(
        &mut self,
        provider: Address,
        refused: &UpstreamRefused,
        range: &LaneRange,
        at: Instant,
    ) {
        // The bytes before `landed` verified: only the part after them is refused.
        let refused_from = range.offset.saturating_add(range.landed);
        let refused_end = range.offset.saturating_add(range.len);
        let until = at.checked_add(PULL_THROUGH_BAR).unwrap_or(at);
        let mut dropped = Vec::new();
        for block in blocks_of(refused_from, refused_end) {
            let (count, last_at) = self
                .covered_not_found
                .entry((provider, block))
                .or_insert((0, None));
            match *last_at {
                // One burst (the own stream and its extras refused together, or
                // one load-shed moment) counts once.
                Some(last) if at < last + crate::health::COOL_BASE => continue,
                Some(last) if last + PULL_THROUGH_BAR <= at => *count = 0,
                _ => {}
            }
            *count = count.saturating_add(1);
            *last_at = Some(at);
            if *count >= ABSENT_AFTER_NOT_FOUND
                && self
                    .stale_blocks
                    .entry(provider)
                    .or_default()
                    .insert(block, until)
                    .is_none()
            {
                dropped.push(block);
            }
        }
        if dropped.is_empty() {
            return;
        }
        let hash = blake3::Hash::from_bytes(self.hash).to_hex();
        tracing::info!(
            %provider,
            %hash,
            blocks = ?dropped,
            "a partial holder keeps refusing blocks its coverage claims; dropping them from \
             its coverage for a while"
        );
        if let Some(coverage) = self.holder(provider).and_then(|h| h.coverage.clone()) {
            self.advertised.entry(provider).or_insert(coverage);
        }
        self.narrow(provider);
        self.marked(true, refused);
    }

    /// Record bytes `provider` verified in `ranges` (`(offset, len)`) of its
    /// own coverage: it holds those blocks after all, so their counts of
    /// covered refusals clear.
    pub(crate) fn record_covered_served(&mut self, provider: Address, ranges: &[(u64, u64)]) {
        for &(offset, len) in ranges {
            for block in blocks_of(offset, offset.saturating_add(len)) {
                self.covered_not_found.remove(&(provider, block));
            }
        }
    }

    /// Set `provider`'s coverage to what it last advertised, less the blocks
    /// dropped from it now. A holder whose coverage is the whole blob keeps it.
    fn narrow(&mut self, provider: Address) {
        let Some(advertised) = self.advertised.get(&provider) else {
            return;
        };
        if self.holder(provider).is_some_and(|h| h.coverage.is_none()) {
            return;
        }
        let stale = self.stale_blocks.get(&provider);
        let narrowed = without_blocks(advertised, |b| stale.is_some_and(|s| s.contains_key(&b)));
        if let Some(holder) = self.holders.iter_mut().find(|h| h.provider == provider) {
            holder.coverage = Some(narrowed);
        }
    }

    /// Count a `NotFound` from probed partial holder `provider` for a range
    /// outside its coverage, given at `at`. At [`ABSENT_AFTER_NOT_FOUND`]
    /// answers the holder is barred from pull-through. An answer older than
    /// the holder's last verified byte outside its coverage is stale and does
    /// not count.
    fn record_pull_through_refusal(
        &mut self,
        provider: Address,
        refused: &UpstreamRefused,
        at: Instant,
    ) {
        if self
            .pull_through_served_at
            .get(&provider)
            .is_some_and(|served_at| *served_at > at)
        {
            return;
        }
        let refused_at = self.pull_through_refused_at.entry(provider).or_insert(at);
        *refused_at = (*refused_at).max(at);
        let count = self.pull_through_not_found.entry(provider).or_insert(0);
        *count = count.saturating_add(1);
        if *count < ABSENT_AFTER_NOT_FOUND {
            return;
        }
        let until = at.checked_add(PULL_THROUGH_BAR).unwrap_or(at);
        let newly = self.no_pull_through.insert(provider, until).is_none();
        if newly {
            let bars = self.pull_through_bars.entry(provider).or_insert(0);
            *bars = bars.saturating_add(1);
        }
        self.marked(newly, refused);
    }

    /// Lift each `NotFound` bar from pull-through whose time ended by `now`,
    /// and restart its holder's count of refusals. The holder takes one
    /// uncovered range again as a probe; [`ABSENT_AFTER_NOT_FOUND`] more
    /// refusals bar it again. A size-ceiling bar does not end. Each block
    /// dropped from a holder's coverage whose time ended returns to it the
    /// same way, with its count restarted.
    pub fn expire_pull_through_bars(&mut self, now: Instant) {
        let ended: Vec<Address> = self
            .no_pull_through
            .iter()
            .filter(|&(_, until)| *until <= now)
            .map(|(provider, _)| *provider)
            .collect();
        for provider in ended {
            self.no_pull_through.remove(&provider);
            self.pull_through_not_found.remove(&provider);
        }
        let mut restored = Vec::new();
        for (&provider, blocks) in &mut self.stale_blocks {
            let before = blocks.len();
            blocks.retain(|&block, until| {
                let keep = *until > now;
                if !keep {
                    self.covered_not_found.remove(&(provider, block));
                }
                keep
            });
            if blocks.len() != before {
                restored.push(provider);
            }
        }
        for provider in restored {
            self.narrow(provider);
            if self
                .stale_blocks
                .get(&provider)
                .is_some_and(HashMap::is_empty)
            {
                self.stale_blocks.remove(&provider);
                self.advertised.remove(&provider);
            }
        }
    }

    /// Record `refused` as the cause of a mark that excludes a source, and
    /// bump the mark epoch when the mark is `newly` set.
    fn marked(&mut self, newly: bool, refused: &UpstreamRefused) {
        self.last_absent = Some(refused.clone());
        if newly {
            self.mark_epoch = self.mark_epoch.saturating_add(1);
        }
    }

    /// Record a verified byte from `provider`: it holds the blob after all.
    /// A bar from pull-through stays: a partial holder serves the blocks it
    /// covers whether or not it serves the others.
    pub fn record_progress(&mut self, provider: Address) {
        self.absent.remove(&provider);
        self.not_found.remove(&provider);
        self.health.record_progress(provider);
    }

    /// Record a byte from `provider` verified at `at` on a range outside its
    /// coverage: it serves such ranges by pull-through after all, so its
    /// count of `NotFound` answers for them clears, and so does the bar they
    /// set. A byte verified no later than its last such answer changes
    /// nothing. A bar from a size-ceiling refusal stays.
    pub fn record_pull_through(&mut self, provider: Address, at: Instant) {
        let served_at = self.pull_through_served_at.entry(provider).or_insert(at);
        *served_at = (*served_at).max(at);
        if self
            .pull_through_refused_at
            .get(&provider)
            .is_some_and(|refused_at| *refused_at >= at)
        {
            return;
        }
        self.pull_through_not_found.remove(&provider);
        self.pull_through_refused_at.remove(&provider);
        self.no_pull_through.remove(&provider);
        self.pull_through_bars.remove(&provider);
    }

    /// Whether `provider` is barred from pull-through: a probed partial
    /// holder that said `NotFound` to [`ABSENT_AFTER_NOT_FOUND`] ranges
    /// outside its coverage, with no such range served since and its bar not
    /// yet ended ([`Self::expire_pull_through_bars`]), or that refused the
    /// blob as too large. It still serves the blocks it covers and takes no
    /// range outside them. A discovery that reports wider coverage for it
    /// clears a `NotFound` bar; a size-ceiling bar stays for the blob.
    #[must_use]
    pub fn no_pull_through(&self, provider: Address) -> bool {
        self.no_pull_through.contains_key(&provider)
            || self.too_large_pull_through.contains(&provider)
    }

    /// Whether a discovery should run now.
    ///
    /// A unanimous set (every known source excluded or item-marked) is driven
    /// by the deposit/mark-epoch pair it last looked at and the discovery
    /// backoff alone, regardless of `running`/`uncovered`: a source that becomes startable
    /// again only because its delivery cooldown expired must not stop a
    /// unanimously-stuck set from looking for a better one. A deposit and mark
    /// epoch the set has not looked at yet skips the backoff once, so a
    /// unanimous set always gets one fresh look before it stops; once looked
    /// at, the normal backoff applies. Otherwise (not unanimous), a discovery
    /// runs when no source is running and none can start, or a missing range
    /// has no usable holder, and the backoff allows it.
    #[must_use]
    pub fn wants_discovery(
        &self,
        now: Instant,
        deposit: U256,
        running: usize,
        uncovered: bool,
    ) -> bool {
        let unanimous = self.all_excluded(deposit, false) || self.all_item_marked(false);
        if unanimous {
            if self.looked_at != Some((deposit, self.mark_epoch)) {
                return true;
            }
            return self.discovery.is_none_or(|b| now >= b.next_at);
        }
        let starved = running == 0 && self.next_to_start(now, deposit, &HashSet::new()).is_none();
        if !(starved || uncovered) {
            return false;
        }
        self.discovery.is_none_or(|b| now >= b.next_at)
    }

    /// Record a discovery's result. New holders join; known ones update their
    /// coverage and RTT. An error keeps every known holder and backs off.
    /// Bringing in at least one genuinely new provider resets the backoff, so
    /// the set looks again soon rather than waiting out the doubled wait; any
    /// other outcome (an error, or a discovery that found nothing new) doubles
    /// it as usual.
    pub fn discovery_done(
        &mut self,
        found: anyhow::Result<Vec<Holder>>,
        now: Instant,
        deposit: U256,
    ) {
        match found {
            Ok(holders) => {
                let brought_new = holders.iter().any(|h| {
                    self.holders
                        .iter()
                        .all(|known| known.provider != h.provider)
                });
                self.merge(holders);
                self.discovered_at = Some((deposit, self.mark_epoch));
                self.discovery = if brought_new {
                    None
                } else {
                    Some(self.next_discovery_backoff(now))
                };
            }
            Err(err) => {
                tracing::debug!(error = %decdn_common::redact::sanitize_err_chain(&err), "discovery failed");
                self.discovery = Some(self.next_discovery_backoff(now));
            }
        }
        self.looked_at = Some((deposit, self.mark_epoch));
    }

    /// Merge holders the provider pushed ([`SourceProvider::arrivals`]). New
    /// holders join and known ones update their coverage and RTT, as in
    /// [`Self::discovery_done`]. A pushed holder is not a discovery attempt,
    /// so the discovery backoff is unchanged.
    pub fn holders_arrived(&mut self, holders: Vec<Holder>) {
        self.merge(holders);
    }

    /// The next discovery backoff after an attempt at `now`.
    fn next_discovery_backoff(&self, now: Instant) -> Backoff {
        let prior = self.discovery.unwrap_or(Backoff {
            next_at: now,
            attempts: 0,
        });
        prior.fail(now, DISCOVERY_BASE, DISCOVERY_CAP)
    }

    /// The next instant a source may start or a retry may run.
    #[must_use]
    pub fn next_wake(&self, now: Instant) -> Option<Instant> {
        let cooling = self
            .holders
            .iter()
            .filter_map(|h| self.health.cooling_until(h.provider, now));
        let builds = self
            .build_retry
            .values()
            .map(|b| b.next_at)
            .chain(self.start_retry.values().copied())
            .filter(|t| *t > now);
        let discovery = self.discovery.map(|b| b.next_at).filter(|t| *t > now);
        let bars = self.no_pull_through.values().copied().filter(|t| *t > now);
        cooling.chain(builds).chain(discovery).chain(bars).min()
    }

    /// The unanimous stop, if a discovery ran at this deposit and mark epoch and:
    /// every known source can serve none of the work left (the item ends), or
    /// every known source is priced out or can serve none of it, with at least
    /// one priced out, and the top-up budget cannot change that (the command
    /// ends).
    ///
    /// A source can serve none of the work left when it says the blob is
    /// absent, when it refused the blob as too large, or when it is barred
    /// from pull-through
    /// ([`Self::no_pull_through`]) and `only_uncovered_left` says no work left
    /// lies inside the coverage of a barred holder.
    #[must_use]
    pub fn exhausted(
        &self,
        deposit: U256,
        topups_left: bool,
        only_uncovered_left: bool,
    ) -> Option<anyhow::Error> {
        if self.holders.is_empty() || self.discovered_at != Some((deposit, self.mark_epoch)) {
            return None;
        }
        if self.all_item_marked(only_uncovered_left) {
            return Some(match &self.last_absent {
                Some(refused) => anyhow::Error::new(refused.clone()).context(NoSourceHasBlob),
                None => anyhow::Error::new(NoSourceHasBlob),
            });
        }
        if !self.all_excluded(deposit, only_uncovered_left) {
            return None;
        }
        (!topups_left).then(|| anyhow::Error::new(NoAffordableSource { deposit }))
    }

    /// Whether every known source can serve none of the work left
    /// ([`Self::cannot_serve`]).
    fn all_item_marked(&self, only_uncovered_left: bool) -> bool {
        !self.holders.is_empty()
            && self
                .holders
                .iter()
                .all(|h| self.cannot_serve(h.provider, only_uncovered_left))
    }

    /// Whether every known source is priced out at `deposit` or can serve
    /// none of the work left, with at least one actually priced out. A set
    /// that is unanimous only on the latter is handled by
    /// [`Self::all_item_marked`] instead, so this is the affordability verdict
    /// even when it is mixed with absence marks.
    fn all_excluded(&self, deposit: U256, only_uncovered_left: bool) -> bool {
        !self.holders.is_empty()
            && self
                .holders
                .iter()
                .any(|h| self.is_unaffordable(h.provider, deposit))
            && self.holders.iter().all(|h| {
                self.cannot_serve(h.provider, only_uncovered_left)
                    || self.is_unaffordable(h.provider, deposit)
            })
    }

    /// Whether `provider` can serve none of the work left: it says the blob is
    /// absent, it refused the blob as too large, or `only_uncovered_left` says
    /// no work left lies inside the coverage of a barred holder and the holder
    /// is barred for good: by a size-ceiling refusal, or by `NotFound` again
    /// after the probe that followed its first bar.
    fn cannot_serve(&self, provider: Address, only_uncovered_left: bool) -> bool {
        let barred_for_good = self.too_large_pull_through.contains(&provider)
            || (self.no_pull_through.contains_key(&provider)
                && self
                    .pull_through_bars
                    .get(&provider)
                    .is_some_and(|bars| *bars >= 2));
        self.absent.contains(&provider)
            || self.too_large.contains(&provider)
            || (only_uncovered_left && barred_for_good)
    }

    /// Whether `provider`'s health says it is priced out at `deposit`.
    fn is_unaffordable(&self, provider: Address, deposit: U256) -> bool {
        matches!(
            self.health.health(provider),
            crate::health::Health::Unaffordable { at_deposit } if deposit <= at_deposit
        )
    }

    /// Add new holders and update known ones. A provider a probe now reports
    /// as a holder loses its absent mark and its `NotFound` count. A probe
    /// that reports a block the holder did not cover before also clears its
    /// pull-through count and bar: the block it refused may be one it now
    /// holds.
    ///
    /// A block a holder's coverage claimed but it refused
    /// ([`Self::record_covered_refusal`]) stays out of the coverage a probe
    /// reports again until its drop ends: the record that claimed it is the
    /// likely stale one.
    ///
    /// A probe that reports a holder as whole replaces its partial record, so
    /// it also ends every block drop and count of covered refusals it had.
    fn merge(&mut self, holders: Vec<Holder>) {
        for mut holder in holders {
            if holder.coverage.is_none() {
                let provider = holder.provider;
                self.stale_blocks.remove(&provider);
                self.advertised.remove(&provider);
                self.covered_not_found.retain(|(p, _), _| *p != provider);
            }
            if let (Some(coverage), Some(stale)) = (
                holder.coverage.as_ref(),
                self.stale_blocks.get(&holder.provider),
            ) {
                self.advertised.insert(holder.provider, coverage.clone());
                holder.coverage = Some(without_blocks(coverage, |b| stale.contains_key(&b)));
            }
            if holder.probed_holder {
                self.absent.remove(&holder.provider);
                self.not_found.remove(&holder.provider);
                let wider = self.holder(holder.provider).is_some_and(|known| {
                    covers_more(holder.coverage.as_ref(), known.coverage.as_ref())
                });
                if wider {
                    self.pull_through_not_found.remove(&holder.provider);
                    self.pull_through_refused_at.remove(&holder.provider);
                    self.no_pull_through.remove(&holder.provider);
                    self.pull_through_bars.remove(&holder.provider);
                }
            }
            match self
                .holders
                .iter_mut()
                .find(|h| h.provider == holder.provider)
            {
                Some(known) => *known = holder,
                None => self.holders.push(holder),
            }
        }
    }
}

/// The discovery blocks the byte range `[start, end)` touches; none for an
/// empty range.
fn blocks_of(start: u64, end: u64) -> std::ops::Range<u32> {
    if start >= end {
        return 0..0;
    }
    let block_bytes = decdn_protocol::discovery_block_bytes();
    let block_of = |offset: u64| u32::try_from(offset / block_bytes).unwrap_or(u32::MAX);
    block_of(start)..block_of(end - 1).saturating_add(1)
}

/// `coverage` less the blocks `dropped` names.
fn without_blocks(coverage: &Coverage, dropped: impl Fn(u32) -> bool) -> Coverage {
    let num_blocks = coverage
        .covered_blocks()
        .last()
        .map_or(0, |b| b.saturating_add(1));
    Coverage::from_block_indices(
        num_blocks,
        coverage.covered_blocks().filter(|&b| !dropped(b)),
    )
}

/// Whether coverage `new` holds a block `old` does not. `None` is the whole
/// blob.
fn covers_more(new: Option<&Coverage>, old: Option<&Coverage>) -> bool {
    match (new, old) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(new), Some(old)) => new.covered_blocks().any(|block| !old.covers(block)),
    }
}

/// A [`SourceProvider`] over a fixed set of prebuilt lanes: for SDK callers
/// that dial their own providers, and for tests. Discovery returns the same
/// set; each lane is handed out once. So a static set serves one blob's
/// [`SourceSet`]: a [`crate::Downloader`] over it fetches one target, and a
/// second target finds no lane to build.
pub struct StaticSources<S> {
    holders: Vec<Holder>,
    lanes: std::sync::Mutex<HashMap<Address, StreamCandidate<S>>>,
}

impl<S> std::fmt::Debug for StaticSources<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticSources")
            .field("holders", &self.holders.len())
            .finish_non_exhaustive()
    }
}

impl<S> StaticSources<S> {
    /// Wrap `candidates`, keyed by each one's on-chain provider. Each one is
    /// a probed holder ([`Holder::probed_holder`]).
    ///
    /// # Errors
    ///
    /// No candidate at all (its discovery could never find one), or two
    /// candidates naming the same provider.
    pub fn new(candidates: Vec<StreamCandidate<S>>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !candidates.is_empty(),
            "static sources need at least one candidate to fetch from"
        );
        let mut holders = Vec::with_capacity(candidates.len());
        let mut lanes = HashMap::with_capacity(candidates.len());
        for (rank, candidate) in candidates.into_iter().enumerate() {
            let provider = candidate
                .ctx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .provider;
            let rtt_ms = f64::from(u32::try_from(rank).unwrap_or(u32::MAX));
            holders.push(Holder {
                provider,
                coverage: candidate.coverage.clone(),
                rtt_ms,
                probed_holder: true,
            });
            anyhow::ensure!(
                lanes.insert(provider, candidate).is_none(),
                "two candidates name provider {provider}"
            );
        }
        Ok(Self {
            holders,
            lanes: std::sync::Mutex::new(lanes),
        })
    }

    /// The holders, in the order the candidates were given.
    #[must_use]
    pub fn holders(&self) -> Vec<Holder> {
        self.holders.clone()
    }

    /// The same sources, with no probe reporting any of them as a holder:
    /// pull-through targets.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn not_probed(mut self) -> Self {
        for holder in &mut self.holders {
            holder.probed_holder = false;
        }
        self
    }
}

impl<S: BlobSource> SourceProvider for StaticSources<S> {
    type Source = S;

    fn discover(&self, _hash: [u8; 32]) -> SourceFuture<'_, Vec<Holder>> {
        let holders = self.holders.clone();
        Box::pin(async move { Ok(holders) })
    }

    fn connect<'a>(&'a self, holder: &'a Holder) -> SourceFuture<'a, StreamCandidate<S>> {
        let taken = self
            .lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&holder.provider)
            .ok_or_else(|| anyhow::anyhow!("no static lane for {}", holder.provider));
        Box::pin(async move { taken })
    }
}

#[cfg(test)]
mod tests {
    use super::{DISCOVERY_BASE, Holder, NoAffordableSource, SourceProvider, SourceSet};
    use crate::fault::{Fault, LaneBuildFault};
    use crate::health::{COOL_BASE, PeerHealth};
    use crate::source::{ScriptedSource, SourceFuture, ctx_with};
    use crate::streamer::StreamCandidate;
    use crate::{Cumulative, LaneLease, PoolLedger};
    use alloy::primitives::{Address, U256};
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use tokio::time::{Duration, Instant};

    const A: Address = Address::repeat_byte(0xA1);
    const B: Address = Address::repeat_byte(0xB2);

    fn holder(provider: Address, rtt_ms: f64) -> Holder {
        Holder {
            provider,
            coverage: None,
            rtt_ms,
            probed_holder: true,
        }
    }

    /// A provider the probe did not report as a holder: a pull-through target.
    fn non_holder(provider: Address, rtt_ms: f64) -> Holder {
        Holder {
            probed_holder: false,
            ..holder(provider, rtt_ms)
        }
    }

    struct FakeProvider {
        discover: Mutex<Vec<anyhow::Result<Vec<Holder>>>>,
        faults: Mutex<Vec<Address>>,
    }

    impl SourceProvider for FakeProvider {
        type Source = ScriptedSource;
        fn discover(&self, _hash: [u8; 32]) -> SourceFuture<'_, Vec<Holder>> {
            let next = self
                .discover
                .lock()
                .ok()
                .and_then(|mut d| d.pop())
                .unwrap_or_else(|| Ok(Vec::new()));
            Box::pin(async move { next })
        }
        fn connect<'a>(
            &'a self,
            _holder: &'a Holder,
        ) -> SourceFuture<'a, StreamCandidate<ScriptedSource>> {
            Box::pin(async { anyhow::bail!("tests call lane_built directly") })
        }
        fn on_source_fault(&self, holder: &Holder) {
            if let Ok(mut f) = self.faults.lock() {
                f.push(holder.provider);
            }
        }
    }

    fn provider(discover: Vec<anyhow::Result<Vec<Holder>>>) -> FakeProvider {
        FakeProvider {
            discover: Mutex::new(discover),
            faults: Mutex::new(Vec::new()),
        }
    }

    fn candidate() -> anyhow::Result<StreamCandidate<ScriptedSource>> {
        let src = ScriptedSource::new(vec![7u8; 1024])?;
        Ok(StreamCandidate {
            source: src,
            ctx: Arc::new(Mutex::new(ctx_with(0xA1, U256::ZERO))),
            ledger: Arc::new(PoolLedger::new(Cumulative::default())),
            coverage: None,
            lease: LaneLease::new(()),
            widen: None,
        })
    }

    /// A pushed holder joins the set and leaves the discovery backoff alone.
    #[test]
    fn holders_arrived_adds_a_holder_without_touching_discovery() {
        let provider = provider(Vec::new());
        let mut set = SourceSet::new(&provider, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        set.discovery_done(Ok(Vec::new()), Instant::now(), U256::ZERO);
        let backoff = set.discovery.map(|b| b.next_at);
        let looked = set.looked_at;
        let discovered = set.discovered_at;

        set.holders_arrived(vec![holder(B, 20.0)]);

        assert_eq!(set.holders().len(), 2);
        assert!(set.holder(B).is_some());
        assert_eq!(set.discovery.map(|b| b.next_at), backoff);
        assert_eq!(set.looked_at, looked);
        assert_eq!(set.discovered_at, discovered);
    }

    /// A pushed holder the set already knows updates in place.
    #[test]
    fn holders_arrived_updates_a_known_holder_in_place() {
        let provider = provider(Vec::new());
        let mut set = SourceSet::new(&provider, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        set.holders_arrived(vec![holder(A, 4.0)]);
        assert_eq!(set.holders().len(), 1);
        assert_eq!(set.holder(A).map(|h| h.rtt_ms), Some(4.0));
    }

    /// Two lanes on one provider would run two concurrent voucher streams on
    /// one `(signer, provider)` watermark, so the static set refuses them.
    #[test]
    fn two_candidates_for_one_provider_are_refused() -> anyhow::Result<()> {
        let result = super::StaticSources::new(vec![candidate()?, candidate()?]);
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn the_nearest_usable_source_starts_first() {
        let p = provider(vec![]);
        let set = SourceSet::new(
            &p,
            [0; 32],
            Arc::default(),
            vec![holder(B, 30.0), holder(A, 10.0)],
        );
        let now = Instant::now();
        let first = set.next_to_start(now, U256::ZERO, &HashSet::new());
        assert_eq!(first.map(|h| h.provider), Some(A));
        let running = HashSet::from([A]);
        let second = set.next_to_start(now, U256::ZERO, &running);
        assert_eq!(second.map(|h| h.provider), Some(B));
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_fault_cools_the_source_and_reports_it() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        let now = Instant::now();
        let fault = set.record_fault(A, &anyhow::anyhow!("reset"), None, now, U256::ZERO);
        assert_eq!(fault, Fault::Source);
        assert!(
            set.next_to_start(now, U256::ZERO, &HashSet::new())
                .is_none()
        );
        assert_eq!(set.next_wake(now), Some(now + COOL_BASE));
        assert!(
            set.next_to_start(now + COOL_BASE, U256::ZERO, &HashSet::new())
                .is_some()
        );
        assert_eq!(
            p.faults.lock().map(|f| f.clone()).unwrap_or_default(),
            vec![A]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_lane_build_error_retries_later_without_cooling_the_source() -> anyhow::Result<()> {
        let p = provider(vec![]);
        let health: Arc<PeerHealth> = Arc::default();
        let mut set = SourceSet::new(&p, [0; 32], Arc::clone(&health), vec![holder(A, 10.0)]);
        let now = Instant::now();
        let err = set
            .lane_built(A, Err(anyhow::anyhow!("rpc timed out")), now)
            .err()
            .ok_or_else(|| anyhow::anyhow!("a failed build must be an error"))?;
        assert!(err.downcast_ref::<LaneBuildFault>().is_some());
        assert!(health.usable(A, now, U256::ZERO));
        assert!(
            set.next_to_start(now, U256::ZERO, &HashSet::new())
                .is_none()
        );
        assert_eq!(set.next_wake(now), Some(now + super::BUILD_RETRY_BASE));
        let later = now + super::BUILD_RETRY_BASE;
        assert!(
            set.next_to_start(later, U256::ZERO, &HashSet::new())
                .is_some()
        );
        let lane = set.lane_built(A, candidate(), later)?;
        assert!(Arc::ptr_eq(
            &lane,
            &set.cached_lane(A)
                .ok_or_else(|| anyhow::anyhow!("cached"))?
        ));
        Ok(())
    }

    /// A built lane that finds no free stream waits until the time its
    /// caller names, and no longer however often it is refused: the wait
    /// does not grow like a build's. Only the first refusal since the lane
    /// last started reports itself, and a start clears the wait.
    #[tokio::test(start_paused = true)]
    async fn a_lane_refused_a_stream_waits_a_flat_retry() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        let none_running = HashSet::new();
        let retry = Duration::from_secs(1);
        let mut now = Instant::now();
        for refusal in 0..4 {
            assert_eq!(set.start_refused(A, now + retry), refusal == 0);
            assert!(set.next_to_start(now, U256::ZERO, &none_running).is_none());
            assert_eq!(set.next_wake(now), Some(now + retry));
            now += retry;
            assert!(set.next_to_start(now, U256::ZERO, &none_running).is_some());
        }
        set.start_taken(A);
        assert_eq!(set.next_wake(now), None);
        assert!(
            set.start_refused(A, now + retry),
            "a refusal after a start is the first of a new run"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_discovery_error_backs_off_and_keeps_the_known_sources() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        let now = Instant::now();
        set.record_fault(A, &anyhow::anyhow!("reset"), None, now, U256::ZERO);
        assert!(
            set.wants_discovery(now, U256::ZERO, 0, false),
            "starved: A is cooling"
        );
        set.discovery_done(Err(anyhow::anyhow!("registry rpc down")), now, U256::ZERO);
        assert!(set.holder(A).is_some());
        // `uncovered = true` keeps the want alive after A's 2 s cooldown ends,
        // so these two lines test the 5 s discovery backoff alone.
        assert!(!set.wants_discovery(now, U256::ZERO, 0, true));
        assert!(set.wants_discovery(now + DISCOVERY_BASE, U256::ZERO, 0, true));
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_merges_new_holders() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        let now = Instant::now();
        set.discovery_done(Ok(vec![holder(A, 12.0), holder(B, 5.0)]), now, U256::ZERO);
        assert_eq!(
            set.next_to_start(now, U256::ZERO, &HashSet::new())
                .map(|h| h.provider),
            Some(B)
        );
        assert!(set.holder(A).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn every_source_unaffordable_stops_only_after_a_fresh_discovery() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(
            &p,
            [0; 32],
            Arc::default(),
            vec![holder(A, 10.0), holder(B, 20.0)],
        );
        let now = Instant::now();
        let dep = U256::from(100u64);
        let dry = || {
            anyhow::Error::new(crate::driver::PoolExhausted {
                gap_start: 0,
                gap_len: 1,
            })
        };
        set.record_fault(A, &dry(), None, now, dep);
        set.record_fault(B, &dry(), None, now, dep);
        assert!(set.exhausted(dep, true, false).is_none(), "top-ups left");
        assert!(
            set.exhausted(dep, false, false).is_none(),
            "no discovery at this deposit yet"
        );
        assert!(set.wants_discovery(now, dep, 0, false));
        set.discovery_done(Ok(vec![]), now, dep);
        let err = set.exhausted(dep, false, false);
        assert!(err.is_some_and(|e| e.downcast_ref::<NoAffordableSource>().is_some()));
        assert!(
            set.exhausted(dep + U256::from(1u64), false, false)
                .is_none(),
            "a top-up revives"
        );
    }

    fn not_found() -> anyhow::Error {
        anyhow::Error::new(crate::UpstreamRefused::mid_stream(
            decdn_protocol::client::StreamError::NotFound,
        ))
    }

    /// Record `times` `NotFound` answers from `provider`.
    fn say_not_found<P: SourceProvider>(
        set: &mut SourceSet<'_, P>,
        provider: Address,
        times: u32,
        now: Instant,
    ) {
        for _ in 0..times {
            set.record_fault(provider, &not_found(), None, now, U256::ZERO);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn every_source_saying_not_found_ends_the_item() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(
            &p,
            [0; 32],
            Arc::default(),
            vec![non_holder(A, 10.0), non_holder(B, 20.0)],
        );
        let now = Instant::now();
        say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
        assert!(
            set.exhausted(U256::ZERO, true, false).is_none(),
            "B has not answered"
        );
        say_not_found(&mut set, B, super::ABSENT_AFTER_NOT_FOUND, now);
        assert!(
            set.exhausted(U256::ZERO, true, false).is_none(),
            "no discovery since the last mark"
        );
        assert!(
            set.wants_discovery(now, U256::ZERO, 0, false),
            "unanimity skips the backoff"
        );
        set.discovery_done(Ok(vec![]), now, U256::ZERO);
        let err = set.exhausted(U256::ZERO, true, false);
        assert!(err.is_some_and(|e| e.downcast_ref::<super::NoSourceHasBlob>().is_some()));
    }

    /// A probed holder's `NotFound` is a delivery fault only: it may mean load
    /// shed or a per-signer cap, so it never marks the holder absent.
    #[tokio::test(start_paused = true)]
    async fn a_probed_holder_saying_not_found_is_never_absent() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        let now = Instant::now();
        say_not_found(&mut set, A, 10, now);
        set.discovery_done(Ok(vec![]), now, U256::ZERO);
        assert!(set.exhausted(U256::ZERO, true, false).is_none());
        let cooled = set.health().cooling_until(A, now).is_some_and(|until| {
            set.next_to_start(until, U256::ZERO, &HashSet::new())
                .is_some()
        });
        assert!(cooled, "the holder cools and comes back");
    }

    /// A probed holder of `blocks` of a four-block blob only.
    fn partial(provider: Address, rtt_ms: f64, blocks: &[u32]) -> Holder {
        Holder {
            coverage: Some(decdn_protocol::Coverage::from_block_indices(
                4,
                blocks.iter().copied(),
            )),
            ..holder(provider, rtt_ms)
        }
    }

    /// Record `times` `NotFound` answers from `provider` for `range`.
    fn say_not_found_in<P: SourceProvider>(
        set: &mut SourceSet<'_, P>,
        provider: Address,
        range: super::LaneRange,
        times: u32,
        now: Instant,
    ) {
        for _ in 0..times {
            set.record_fault(provider, &not_found(), Some(range), now, U256::ZERO);
        }
    }

    /// A range in discovery block 1, inside or outside the lane's coverage.
    const fn block1(uncovered: bool) -> super::LaneRange {
        super::LaneRange {
            offset: 64 << 20,
            len: 64 << 20,
            landed: 0,
            past_end: false,
            uncovered,
        }
    }

    /// A range in discovery block 0, inside the lane's coverage.
    const fn block0() -> super::LaneRange {
        super::LaneRange {
            offset: 0,
            len: 64 << 20,
            landed: 0,
            past_end: false,
            uncovered: false,
        }
    }

    /// A partial holder that says `NotFound` three times to ranges outside
    /// its coverage is barred from pull-through. Refusals inside its coverage
    /// never bar it, a verified byte inside its coverage keeps the bar, and a
    /// verified byte outside it lifts the bar.
    #[tokio::test(start_paused = true)]
    async fn three_uncovered_refusals_bar_a_partial_holder_from_pull_through() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
        let now = Instant::now();
        say_not_found_in(&mut set, A, block0(), 10, now);
        assert!(!set.no_pull_through(A), "refusals inside its coverage");
        say_not_found_in(
            &mut set,
            A,
            block1(true),
            super::ABSENT_AFTER_NOT_FOUND - 1,
            now,
        );
        assert!(!set.no_pull_through(A), "two answers are not enough");
        say_not_found_in(&mut set, A, block1(true), 1, now);
        assert!(set.no_pull_through(A));
        set.record_progress(A);
        assert!(set.no_pull_through(A), "a covered byte keeps the bar");
        set.record_pull_through(A, now + Duration::from_millis(1));
        assert!(!set.no_pull_through(A), "an uncovered byte lifts the bar");
    }

    /// #2281: a partial holder that refuses a block its coverage claims, three
    /// times, loses that block from its coverage and keeps the others. It is
    /// not barred from pull-through, a probe that reports the stale claim
    /// again does not bring the block back, and the drop ends with its time.
    #[tokio::test(start_paused = true)]
    async fn three_covered_refusals_drop_the_block_from_a_partial_holders_coverage() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(
            &p,
            [0; 32],
            Arc::default(),
            vec![partial(A, 10.0, &[0, 1]), holder(B, 20.0)],
        );
        let now = Instant::now();
        let at = say_covered_not_found(
            &mut set,
            A,
            block1(false),
            super::ABSENT_AFTER_NOT_FOUND - 1,
            now,
        );
        assert!(covers(&set, A, 1), "two answers are not enough");
        let at =
            say_covered_not_found(&mut set, A, block1(false), 1, at + crate::health::COOL_BASE);
        assert!(!covers(&set, A, 1), "the refused block leaves the coverage");
        assert!(covers(&set, A, 0), "the block it serves stays");
        assert!(
            !set.no_pull_through(A),
            "a covered refusal is not a pull-through bar"
        );
        assert!(
            set.holder(B).is_some_and(|h| h.coverage.is_none()),
            "the whole holder is untouched"
        );

        set.merge(vec![partial(A, 10.0, &[0, 1])]);
        assert!(!covers(&set, A, 1), "a probe repeating the stale claim");
        assert!(covers(&set, A, 0));

        // A `NotFound` may be transient, so the drop ends with the bar's time,
        // and the count starts again.
        let later = at + super::PULL_THROUGH_BAR;
        set.expire_pull_through_bars(later);
        assert!(covers(&set, A, 1), "the drop ended");
        say_covered_not_found(&mut set, A, block1(false), 2, later);
        assert!(covers(&set, A, 1), "the count restarted with the drop");
    }

    /// Whether `provider`'s coverage in `set` holds `block`.
    fn covers<P: SourceProvider>(set: &SourceSet<'_, P>, provider: Address, block: u32) -> bool {
        set.holder(provider)
            .and_then(|h| h.coverage.as_ref())
            .is_some_and(|c| c.covers(block))
    }

    /// Record `times` covered `NotFound` answers from `provider` for `range`,
    /// a cooldown apart from `start`, as a lane that retries after each gives
    /// them. Returns the time of the last one.
    fn say_covered_not_found<P: SourceProvider>(
        set: &mut SourceSet<'_, P>,
        provider: Address,
        range: super::LaneRange,
        times: u32,
        start: Instant,
    ) -> Instant {
        let mut at = start;
        for n in 0..times {
            if n > 0 {
                at += crate::health::COOL_BASE;
            }
            say_not_found_in(set, provider, range, 1, at);
        }
        at
    }

    /// #2281: covered refusals spread out more than [`PULL_THROUGH_BAR`] apart,
    /// as sparse load shedding gives, never add up to a drop.
    ///
    /// [`PULL_THROUGH_BAR`]: super::PULL_THROUGH_BAR
    #[tokio::test(start_paused = true)]
    async fn sparse_covered_refusals_never_drop_a_block() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0, 1])]);
        let mut at = Instant::now();
        for _ in 0..5 {
            say_not_found_in(&mut set, A, block1(false), 1, at);
            at += super::PULL_THROUGH_BAR;
        }
        assert!(covers(&set, A, 1));
    }

    /// #2281: one burst of covered refusals (the own stream and its extras
    /// refused at one load-shed moment) counts once, and a verified byte in the
    /// block clears its count.
    #[tokio::test(start_paused = true)]
    async fn a_burst_counts_once_and_a_served_byte_clears_the_count() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0, 1])]);
        let now = Instant::now();
        say_not_found_in(&mut set, A, block1(false), 10, now);
        let at = say_covered_not_found(
            &mut set,
            A,
            block1(false),
            1,
            now + crate::health::COOL_BASE,
        );
        assert!(covers(&set, A, 1), "a burst and one more answer are two");

        set.record_covered_served(A, &[(64 << 20, 1024)]);
        say_covered_not_found(&mut set, A, block1(false), 2, at + crate::health::COOL_BASE);
        assert!(covers(&set, A, 1), "the served byte restarted the count");
    }

    /// #2281: a refusal mid-range counts only toward the blocks after the bytes
    /// that landed: the node served the ones before.
    #[tokio::test(start_paused = true)]
    async fn a_mid_range_refusal_counts_only_the_blocks_after_what_landed() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0, 1])]);
        let spanning = super::LaneRange {
            offset: 0,
            len: 128 << 20,
            landed: 64 << 20,
            past_end: false,
            uncovered: false,
        };
        say_covered_not_found(
            &mut set,
            A,
            spanning,
            super::ABSENT_AFTER_NOT_FOUND,
            Instant::now(),
        );
        assert!(covers(&set, A, 0), "block 0 landed");
        assert!(!covers(&set, A, 1));
    }

    /// #2281: a probe that reports a holder with a dropped block as whole ends
    /// the drop, so its end does not turn the holder partial again.
    #[tokio::test(start_paused = true)]
    async fn a_holder_reported_whole_keeps_the_whole_blob_when_a_drop_ends() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0, 1])]);
        let now = Instant::now();
        let at = say_covered_not_found(
            &mut set,
            A,
            block1(false),
            super::ABSENT_AFTER_NOT_FOUND,
            now,
        );
        assert!(!covers(&set, A, 1));

        set.merge(vec![holder(A, 10.0)]);
        set.expire_pull_through_bars(at + super::PULL_THROUGH_BAR);
        assert!(
            set.holder(A).is_some_and(|h| h.coverage.is_none()),
            "the whole holder stays whole"
        );
    }

    /// An uncovered byte verified before the refusal that set the bar is
    /// stale: a sibling worker that ends after the bar does not lift it.
    #[tokio::test(start_paused = true)]
    async fn an_uncovered_byte_from_before_the_bar_keeps_it() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
        let served_at = Instant::now();
        let refused_at = served_at + Duration::from_millis(5);
        say_not_found_in(
            &mut set,
            A,
            block1(true),
            super::ABSENT_AFTER_NOT_FOUND,
            refused_at,
        );
        assert!(set.no_pull_through(A));
        set.record_pull_through(A, served_at);
        assert!(set.no_pull_through(A), "a byte from before the bar");
        set.record_pull_through(A, refused_at);
        assert!(set.no_pull_through(A), "a byte at the same instant");
        set.record_pull_through(A, refused_at + Duration::from_millis(1));
        assert!(!set.no_pull_through(A), "a byte after the bar");
    }

    /// The loop can take a worker's end after a sibling's: a refusal older
    /// than an uncovered byte the loop already took is stale and does not
    /// count, so the bar does not depend on which end the loop takes first.
    #[tokio::test(start_paused = true)]
    async fn a_refusal_older_than_a_taken_byte_does_not_count() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
        let refused_at = Instant::now();
        let served_at = refused_at + Duration::from_millis(5);
        set.record_pull_through(A, served_at);
        say_not_found_in(
            &mut set,
            A,
            block1(true),
            super::ABSENT_AFTER_NOT_FOUND,
            refused_at,
        );
        assert!(
            !set.no_pull_through(A),
            "refusals before the byte are stale"
        );
        say_not_found_in(
            &mut set,
            A,
            block1(true),
            super::ABSENT_AFTER_NOT_FOUND,
            served_at + Duration::from_millis(1),
        );
        assert!(set.no_pull_through(A), "refusals after the byte count");
    }

    /// A `NotFound` bar ends [`super::PULL_THROUGH_BAR`] after the refusal
    /// that set it, and the count restarts: the holder takes an uncovered
    /// range again, and [`super::ABSENT_AFTER_NOT_FOUND`] more refusals bar it
    /// again. The loop wakes when a bar ends.
    #[tokio::test(start_paused = true)]
    async fn a_not_found_bar_ends_after_its_time_and_the_count_restarts() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
        let now = Instant::now();
        say_not_found_in(
            &mut set,
            A,
            block1(true),
            super::ABSENT_AFTER_NOT_FOUND,
            now,
        );
        let ends = now + super::PULL_THROUGH_BAR;
        let just_before = now + super::PULL_THROUGH_BAR.saturating_sub(Duration::from_millis(1));
        assert_eq!(
            set.next_wake(just_before),
            Some(ends),
            "once its cooldown passes, the loop wakes when the bar ends"
        );
        set.expire_pull_through_bars(just_before);
        assert!(set.no_pull_through(A), "not yet");
        set.expire_pull_through_bars(ends);
        assert!(!set.no_pull_through(A), "the bar ends");
        say_not_found_in(
            &mut set,
            A,
            block1(true),
            super::ABSENT_AFTER_NOT_FOUND - 1,
            ends,
        );
        assert!(!set.no_pull_through(A), "the count restarted");
        say_not_found_in(&mut set, A, block1(true), 1, ends);
        assert!(set.no_pull_through(A), "barred again");
    }

    /// A size-ceiling bar on a partial holder stays for the blob: neither an
    /// uncovered byte nor a rediscovery with wider coverage lifts it.
    #[tokio::test(start_paused = true)]
    async fn a_size_ceiling_bar_survives_pull_through_and_wider_coverage() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
        let now = Instant::now();
        set.record_fault(A, &too_large(), Some(block1(true)), now, U256::ZERO);
        assert!(set.no_pull_through(A));
        set.record_pull_through(A, now + Duration::from_millis(1));
        assert!(set.no_pull_through(A), "an uncovered byte keeps it");
        set.discovery_done(Ok(vec![partial(A, 10.0, &[0, 1])]), now, U256::ZERO);
        assert!(set.no_pull_through(A), "wider coverage keeps it");
        set.expire_pull_through_bars(now + super::PULL_THROUGH_BAR * 10);
        assert!(set.no_pull_through(A), "time does not end it");
    }

    /// A holder of the whole blob is never barred: no range lies outside its
    /// coverage.
    #[tokio::test(start_paused = true)]
    async fn a_whole_holder_is_never_barred_from_pull_through() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        let now = Instant::now();
        say_not_found_in(&mut set, A, block1(true), 10, now);
        assert!(!set.no_pull_through(A));
    }

    /// A discovery that reports a block the barred holder did not cover
    /// before lifts the bar. One that reports the same coverage keeps it.
    #[tokio::test(start_paused = true)]
    async fn wider_coverage_from_discovery_lifts_the_bar() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
        let now = Instant::now();
        say_not_found_in(
            &mut set,
            A,
            block1(true),
            super::ABSENT_AFTER_NOT_FOUND,
            now,
        );
        set.discovery_done(Ok(vec![partial(A, 10.0, &[0])]), now, U256::ZERO);
        assert!(set.no_pull_through(A), "the same coverage keeps the bar");
        set.discovery_done(Ok(vec![partial(A, 10.0, &[0, 1])]), now, U256::ZERO);
        assert!(!set.no_pull_through(A), "block 1 is new");
        say_not_found_in(
            &mut set,
            A,
            block1(true),
            super::ABSENT_AFTER_NOT_FOUND - 1,
            now,
        );
        assert!(!set.no_pull_through(A), "the count starts again");
    }

    /// A barred holder ends the item, with the refusal that barred it, only
    /// once no work left lies inside its coverage and a discovery found no
    /// one else.
    #[tokio::test(start_paused = true)]
    async fn a_barred_holder_with_only_uncovered_work_left_ends_the_item() -> anyhow::Result<()> {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
        let now = Instant::now();
        say_not_found_in(
            &mut set,
            A,
            block1(true),
            super::ABSENT_AFTER_NOT_FOUND,
            now,
        );
        assert!(
            set.exhausted(U256::ZERO, true, true).is_none(),
            "no discovery since the bar"
        );
        set.discovery_done(Ok(vec![]), now, U256::ZERO);
        assert!(
            set.exhausted(U256::ZERO, true, true).is_none(),
            "a first bar may come from load shed: the holder gets its probe"
        );
        let later = now + super::PULL_THROUGH_BAR;
        set.expire_pull_through_bars(later);
        assert!(!set.no_pull_through(A), "the bar ends");
        say_not_found_in(
            &mut set,
            A,
            block1(true),
            super::ABSENT_AFTER_NOT_FOUND,
            later,
        );
        set.discovery_done(Ok(vec![]), later, U256::ZERO);
        assert!(
            set.exhausted(U256::ZERO, true, false).is_none(),
            "work inside its coverage is left"
        );
        let err = set
            .exhausted(U256::ZERO, true, true)
            .ok_or_else(|| anyhow::anyhow!("only uncovered work is left"))?;
        assert!(
            err.downcast_ref::<super::NoSourceHasBlob>().is_some(),
            "{err:#}"
        );
        assert!(
            err.downcast_ref::<crate::UpstreamRefused>().is_some(),
            "{err:#}"
        );
        Ok(())
    }

    fn too_large() -> anyhow::Error {
        anyhow::Error::new(crate::UpstreamRefused::mid_stream(
            decdn_protocol::client::StreamError::BlobTooLarge,
        ))
    }

    /// A proxy that refuses the blob as larger than its size ceiling never
    /// starts again for this blob, even once its cooldown ends and a
    /// discovery reports it again, while the holder still starts. A set where
    /// every source refused the blob as too large ends the item.
    #[tokio::test(start_paused = true)]
    async fn a_proxy_refusing_the_blob_as_too_large_is_excluded() -> anyhow::Result<()> {
        let p = provider(vec![]);
        let mut set = SourceSet::new(
            &p,
            [0; 32],
            Arc::default(),
            vec![non_holder(A, 10.0), holder(B, 20.0)],
        );
        let now = Instant::now();
        let fault = set.record_fault(A, &too_large(), None, now, U256::ZERO);
        assert_eq!(fault, Fault::Source);
        set.discovery_done(
            Ok(vec![non_holder(A, 10.0), holder(B, 20.0)]),
            now,
            U256::ZERO,
        );
        let later = now + super::DISCOVERY_CAP;
        assert_eq!(
            set.next_to_start(later, U256::ZERO, &HashSet::new())
                .map(|h| h.provider),
            Some(B),
            "the holder starts and the proxy does not"
        );
        assert!(
            set.next_to_start(later, U256::ZERO, &HashSet::from([B]))
                .is_none(),
            "the proxy never starts again"
        );
        assert!(
            set.exhausted(U256::ZERO, true, false).is_none(),
            "B is left"
        );

        set.record_fault(B, &too_large(), None, later, U256::ZERO);
        set.discovery_done(Ok(vec![]), later, U256::ZERO);
        let err = set
            .exhausted(U256::ZERO, true, false)
            .ok_or_else(|| anyhow::anyhow!("every source refused the blob as too large"))?;
        assert!(
            err.downcast_ref::<super::NoSourceHasBlob>().is_some(),
            "{err:#}"
        );
        assert!(
            err.downcast_ref::<crate::UpstreamRefused>()
                .is_some_and(|r| matches!(
                    r.error(),
                    decdn_protocol::client::StreamError::BlobTooLarge
                )),
            "{err:#}"
        );
        Ok(())
    }

    /// A node applies its size ceiling only to a pull-through. So a partial
    /// holder that refuses the blob as too large is barred from pull-through
    /// only: once its cooldown ends it starts again for the blocks it covers.
    /// A whole holder that refuses it never starts again.
    #[tokio::test(start_paused = true)]
    async fn a_partial_holder_refusing_the_blob_as_too_large_loses_only_pull_through() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(
            &p,
            [0; 32],
            Arc::default(),
            vec![partial(A, 10.0, &[0]), holder(B, 20.0)],
        );
        let now = Instant::now();
        let fault = set.record_fault(A, &too_large(), Some(block1(true)), now, U256::ZERO);
        assert_eq!(fault, Fault::Source);
        assert!(set.no_pull_through(A), "one refusal bars its pull-through");

        let later = now + crate::health::COOL_CAP;
        assert_eq!(
            set.next_to_start(later, U256::ZERO, &HashSet::new())
                .map(|h| h.provider),
            Some(A),
            "the partial holder still starts for the blocks it covers"
        );
        set.discovery_done(Ok(vec![]), later, U256::ZERO);
        assert!(
            set.exhausted(U256::ZERO, true, false).is_none(),
            "work inside its coverage is left"
        );

        set.record_fault(B, &too_large(), None, later, U256::ZERO);
        assert!(!set.no_pull_through(B), "a whole holder has no bar");
        let latest = later + crate::health::COOL_CAP;
        assert!(
            set.next_to_start(latest, U256::ZERO, &HashSet::from([A]))
                .is_none(),
            "the whole holder never starts again"
        );
    }

    /// A bound that overshoots the blob sends pieces past its true end, and an
    /// honest node refuses them with `NotFound`. Such overshoot refusals from a
    /// non-holder cool it but never mark it absent, so the item does not end
    /// with a wrong "check the hash".
    #[tokio::test(start_paused = true)]
    async fn overshoot_refusals_never_mark_a_non_holder_absent() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
        let now = Instant::now();
        let overshoot = super::LaneRange {
            offset: 64 << 20,
            len: 64 << 20,
            landed: 0,
            past_end: true,
            uncovered: false,
        };
        for _ in 0..10 {
            let fault = set.record_fault(A, &not_found(), Some(overshoot), now, U256::ZERO);
            assert_eq!(fault, Fault::Source);
        }
        set.discovery_done(Ok(vec![]), now, U256::ZERO);
        assert!(
            set.exhausted(U256::ZERO, true, false).is_none(),
            "overshoot refusals never mark the node absent"
        );
        assert!(
            set.health().cooling_until(A, now).is_some(),
            "the node cools"
        );
    }

    /// A non-holder that says `NotFound` three times ends the item, and the
    /// stop names the refusal: it downcasts to both the stop and the refusal.
    #[tokio::test(start_paused = true)]
    async fn a_non_holder_saying_not_found_three_times_ends_the_item() -> anyhow::Result<()> {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
        let now = Instant::now();
        say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND - 1, now);
        set.discovery_done(Ok(vec![]), now, U256::ZERO);
        assert!(
            set.exhausted(U256::ZERO, true, false).is_none(),
            "two answers are not enough"
        );
        say_not_found(&mut set, A, 1, now);
        set.discovery_done(Ok(vec![]), now, U256::ZERO);
        let err = set
            .exhausted(U256::ZERO, true, false)
            .ok_or_else(|| anyhow::anyhow!("three answers end the item"))?;
        assert!(
            err.downcast_ref::<super::NoSourceHasBlob>().is_some(),
            "{err:#}"
        );
        assert!(
            err.downcast_ref::<crate::UpstreamRefused>().is_some(),
            "{err:#}"
        );
        assert_eq!(
            crate::fault::classify(&err),
            Fault::Fatal(crate::fault::FatalScope::Item)
        );
        Ok(())
    }

    /// A verified byte resets a non-holder's `NotFound` count.
    #[tokio::test(start_paused = true)]
    async fn a_byte_between_not_found_answers_resets_the_count() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
        let now = Instant::now();
        say_not_found(&mut set, A, 2, now);
        set.record_progress(A);
        say_not_found(&mut set, A, 2, now);
        set.discovery_done(Ok(vec![]), now, U256::ZERO);
        assert!(set.exhausted(U256::ZERO, true, false).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_holder_from_discovery_keeps_the_item_alive() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
        let now = Instant::now();
        say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
        set.discovery_done(Ok(vec![holder(B, 5.0)]), now, U256::ZERO);
        assert!(set.exhausted(U256::ZERO, true, false).is_none());
    }

    /// A discovery whose probe now reports an absent provider as a holder
    /// clears its absent mark.
    #[tokio::test(start_paused = true)]
    async fn a_probe_that_reports_the_blob_clears_an_absent_mark() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
        let now = Instant::now();
        say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
        set.discovery_done(Ok(vec![holder(A, 10.0)]), now, U256::ZERO);
        assert!(set.exhausted(U256::ZERO, true, false).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn progress_clears_a_not_found_mark() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
        let now = Instant::now();
        say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
        set.record_progress(A);
        set.discovery_done(Ok(vec![]), now, U256::ZERO);
        assert!(set.exhausted(U256::ZERO, true, false).is_none());
    }

    /// A discovery that fails while the set is unanimous must still back off:
    /// it must not spin every tick just because every source is marked absent.
    #[tokio::test(start_paused = true)]
    async fn a_failed_discovery_backs_off_even_while_unanimous() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(
            &p,
            [0; 32],
            Arc::default(),
            vec![non_holder(A, 10.0), non_holder(B, 20.0)],
        );
        let now = Instant::now();
        say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
        say_not_found(&mut set, B, super::ABSENT_AFTER_NOT_FOUND, now);
        set.discovery_done(Err(anyhow::anyhow!("registry rpc down")), now, U256::ZERO);
        assert!(!set.wants_discovery(now, U256::ZERO, 0, false));
        assert!(!set.wants_discovery(
            now + DISCOVERY_BASE - Duration::from_millis(1),
            U256::ZERO,
            0,
            false
        ));
        assert!(set.wants_discovery(now + DISCOVERY_BASE, U256::ZERO, 0, false));
    }

    /// A unanimously stuck set keeps wanting a fresh discovery even once an
    /// individual source's delivery cooldown clears and it looks startable
    /// again: the `starved` gate must not mask the unanimous verdict.
    #[tokio::test(start_paused = true)]
    async fn a_unanimous_absent_set_still_wants_discovery_once_cooldowns_clear() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
        let now = Instant::now();
        say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
        set.discovery_done(Err(anyhow::anyhow!("registry rpc down")), now, U256::ZERO);
        // Three faults in a row cool A until 8 s after `now`.
        let later = now + super::DISCOVERY_CAP;
        // A's delivery cooldown is long over by `later`, so it is
        // startable again and `running == 0` no longer implies "starved".
        assert!(
            set.next_to_start(later, U256::ZERO, &HashSet::new())
                .is_some(),
            "the source itself is startable again"
        );
        assert!(
            set.wants_discovery(later, U256::ZERO, 1, false),
            "but the set is still unanimously absent, so discovery must still be wanted"
        );
    }

    /// A mixed set — one source priced out, another saying it does not hold
    /// the blob — still ends the command once the top-up budget is spent: an
    /// absent source is excluded from the affordability verdict too, and at
    /// least one source being priced out is what makes it a command-ending
    /// `NoAffordableSource` rather than the pure "nobody has it" case.
    #[tokio::test(start_paused = true)]
    async fn a_mixed_unaffordable_and_absent_set_stops_once_topups_are_spent() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(
            &p,
            [0; 32],
            Arc::default(),
            vec![holder(A, 10.0), non_holder(B, 20.0)],
        );
        let now = Instant::now();
        let dep = U256::from(100u64);
        let dry = anyhow::Error::new(crate::driver::PoolExhausted {
            gap_start: 0,
            gap_len: 1,
        });
        set.record_fault(A, &dry, None, now, dep);
        for _ in 0..super::ABSENT_AFTER_NOT_FOUND {
            set.record_fault(B, &not_found(), None, now, dep);
        }
        set.discovery_done(Ok(vec![]), now, dep);
        assert!(set.exhausted(dep, true, false).is_none(), "top-ups left");
        let err = set.exhausted(dep, false, false);
        assert!(err.is_some_and(|e| e.downcast_ref::<NoAffordableSource>().is_some()));
    }

    /// A discovery that brings in a genuinely new provider resets the backoff,
    /// so the set looks again soon rather than waiting out the doubled wait.
    #[tokio::test(start_paused = true)]
    async fn discovering_a_new_holder_resets_the_backoff() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        let now = Instant::now();
        set.discovery_done(Err(anyhow::anyhow!("registry rpc down")), now, U256::ZERO);
        assert_eq!(set.next_wake(now), Some(now + DISCOVERY_BASE));
        set.discovery_done(Ok(vec![holder(B, 5.0)]), now, U256::ZERO);
        assert_eq!(
            set.next_wake(now),
            None,
            "a fresh holder resets the discovery timer"
        );
    }
}
