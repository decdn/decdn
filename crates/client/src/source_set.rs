//! The sources of one blob (ADR 039 § Source set and selection).
//!
//! A `SourceSet` holds every provider known to hold the blob, the lane built
//! for each, and when each may be tried again. It never drops a source: a
//! delivery fault cools it (in the command-wide [`PeerHealth`]), a price
//! refusal parks it until the deposit rises, and a lane-build or discovery
//! error backs off and retries. A partial holder that keeps refusing ranges
//! outside its coverage is barred from pull-through and serves only the
//! blocks it covers. A source that declines this fetch is excluded for this
//! blob. A refusal never ends a fetch on its own (ADR 039 §Failure handling):
//! the fetch ends when every known source declined it, or on a unanimous
//! verdict after a fresh discovery found nothing new: every known source is
//! priced out (funding needed), or every one says it does not hold the blob,
//! declined it, or is barred from the only work left (not-found).
//!
//! The state is synchronous. The acquire loop runs the `connect` and `discover`
//! futures itself, so lanes keep streaming while a lane builds or discovery
//! runs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alloy::primitives::{Address, U256};
use decdn_protocol::Coverage;
use decdn_protocol::client::{StreamError, VoucherRejectReason};
use tokio::time::{Duration, Instant};

use crate::UpstreamRefused;
use crate::fault::{Fault, LaneBuildFault, classify};
use crate::health::PeerHealth;
use crate::source::{BlobSource, SourceFuture, SourceStream};
use crate::streamer::StreamCandidate;

/// The first wait before a failed lane build is retried.
pub(crate) const BUILD_RETRY_BASE: Duration = Duration::from_secs(1);
/// The longest wait before a failed lane build is retried.
pub(crate) const BUILD_RETRY_CAP: Duration = Duration::from_secs(30);
/// The first wait between discoveries.
pub(crate) const DISCOVERY_BASE: Duration = Duration::from_secs(5);
/// The longest wait between discoveries.
pub(crate) const DISCOVERY_CAP: Duration = Duration::from_mins(5);

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
#[doc(hidden)]
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
    /// The range holds a chunk outside the coverage of the lane that held
    /// it: the lane took it for its node to serve that part by pull-through,
    /// as a pending chunk no running lane covers or as a slow-victim steal's
    /// tail. A `NotFound` for such a range from a probed partial holder
    /// counts toward barring that holder from pull-through
    /// ([`SourceSet::no_pull_through`]).
    ///
    /// Only a lane built with measured coverage can hold an uncovered range.
    /// A lane built without it (a pinned holder, or a provider the probe did
    /// not report as a holder) covers the whole blob, so this is always
    /// `false` there, and `false` never says that the node holds the range.
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
pub(crate) const PULL_THROUGH_BAR: Duration = crate::health::COOL_CAP;

/// Where a [`SourceSet`] finds holders and builds their lanes.
#[doc(hidden)]
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

/// Funding needed: no known source serves at the pool's current funding, a
/// fresh discovery found no other source, and no funding recovery step could
/// raise the deposit (ADR 003 § Funding recovery).
#[derive(Debug)]
pub struct NoAffordableSource {
    /// The pool deposit every source refused at.
    pub deposit: U256,
}

impl std::fmt::Display for NoAffordableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "funding needed: no provider serves at the pool's deposit of {} (micro-USDC)",
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
#[cfg_attr(not(feature = "test-util"), non_exhaustive)]
pub struct NoSourceHasBlob;

impl std::fmt::Display for NoSourceHasBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no provider holds this blob; check the hash")
    }
}

impl std::error::Error for NoSourceHasBlob {}

/// Why one source declined this fetch (ADR 005 §Open-time refusal classes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclineReason {
    /// The node refused the stream `Declined`.
    Refused,
    /// The node rejected this client's voucher mid-stream for a reason no
    /// watermark bundle heals, and that is not a funding reason.
    Voucher(VoucherRejectReason),
}

/// Every known source declined this fetch (ADR 039 §Failure handling). The
/// fetch ends at once, without a fresh discovery. `reasons` are the reasons the
/// client collected, each once, in the order it first saw them.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "test-util"), non_exhaustive)]
pub struct NoNodeWillServe {
    /// The reasons the sources declined, each once.
    pub reasons: Vec<DeclineReason>,
}

impl std::fmt::Display for NoNodeWillServe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vouchers: Vec<String> = self
            .reasons
            .iter()
            .filter_map(|r| match r {
                DeclineReason::Voucher(reason) => Some(format!("{reason:?}")),
                DeclineReason::Refused => None,
            })
            .collect();
        let refused = self.reasons.contains(&DeclineReason::Refused);
        match (refused, vouchers.is_empty()) {
            (_, true) => {
                f.write_str("no node will serve this: every node declined this blob for this fetch")
            }
            (false, false) => write!(
                f,
                "no node will serve this: every node rejected this client's vouchers ({})",
                vouchers.join(", ")
            ),
            (true, false) => write!(
                f,
                "no node will serve this: every node declined this blob or rejected this \
                 client's vouchers ({})",
                vouchers.join(", ")
            ),
        }
    }
}

impl std::error::Error for NoNodeWillServe {}

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
#[doc(hidden)]
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
    /// Probed partial holders that refused a range outside their coverage
    /// `Declined` (ADR 005 §Open-time refusal classes), most often for their
    /// size ceiling, which a node applies only to a pull-through. Neither a
    /// verified byte nor a rediscovery lifts this bar for the fetch. Each one
    /// serves only the blocks it covers.
    declined_pull_through: HashSet<Address>,
    /// Providers that declined this fetch: other than a partial holder's
    /// pull-through refusal, a `Declined` refusal, or a mid-stream voucher
    /// rejection that no bundle heals and that names no funding reason. None
    /// of them starts again for this blob in this fetch, and a rediscovery
    /// does not lift it (ADR 039 §Failure handling).
    declined: HashSet<Address>,
    /// The reasons the declining providers gave, each once: what the
    /// [`NoNodeWillServe`] stop names.
    decline_reasons: Vec<DeclineReason>,
    /// The refusal that last marked a source absent, barred it from
    /// pull-through or declined the fetch: the cause the
    /// [`NoSourceHasBlob`] stop carries.
    last_absent: Option<UpstreamRefused>,
    discovery: Option<Backoff>,
    /// Bumped each time a source newly joins `absent`, `no_pull_through`,
    /// `declined_pull_through` or `declined`.
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
            declined_pull_through: HashSet::new(),
            declined: HashSet::new(),
            decline_reasons: Vec::new(),
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
    /// source that declined this fetch never starts again.
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
            .filter(|h| !self.declined.contains(&h.provider))
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

    /// Drop every built lane `stale` names, so the next start of its provider
    /// builds a new one. A swapped credential retires the lanes on the old key
    /// this way.
    pub(crate) fn drop_lanes(&mut self, stale: impl Fn(&StreamCandidate<P::Source>) -> bool) {
        self.lanes.retain(|_, lane| !stale(lane));
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
    /// and a `Declined` refusal or an unhealed voucher rejection declines it
    /// at once, as on its own stream. The scheduler passes only uncovered
    /// ranges here: an extra stream's covered `NotFound` is most often the
    /// node's per-signer live cap.
    pub(crate) fn record_extra_refusal(
        &mut self,
        provider: Address,
        err: &anyhow::Error,
        range: LaneRange,
        at: Instant,
    ) {
        if let Some(reason) = crate::fault::declining_rejection(err) {
            self.record_declined(provider, DeclineReason::Voucher(reason), None);
            return;
        }
        let Some(refused) = err.downcast_ref::<UpstreamRefused>() else {
            return;
        };
        if crate::fault::says_absent(err) && !range.past_end {
            self.record_not_found(provider, refused, Some(&range), at);
        }
        if matches!(refused.error(), StreamError::Declined) {
            self.record_declined(provider, DeclineReason::Refused, Some(refused));
        }
    }

    /// Record the fault `provider`'s lane ended with, and return its class.
    ///
    /// Each fault is logged where it is recorded, at warn for a fault an
    /// operator watches for ([`Fault::Source`], [`Fault::Fatal`]) and at info
    /// for a deposit wait or a chain-side retry (see `Fault::warns`). The line
    /// carries the provider, the blob, the lane's `range` and the bytes of it
    /// that landed when the lane held one, the error, and the provider's
    /// record in the set when the fault is logged: `probed_holder` and its
    /// `coverage` as block runs (`whole blob` with none measured, `unknown`
    /// for a provider the set has no record of). A rediscovery can change the record after the lane
    /// started. A fetch that recovers reports no lane fault, so this line is
    /// the record of it.
    #[allow(clippy::cognitive_complexity)] // Two log lines, each expanded at two levels.
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
        let (probed_holder, coverage) = self.holder(provider).map_or_else(
            || (false, "unknown".to_owned()),
            |holder| {
                (
                    holder.probed_holder,
                    holder
                        .coverage
                        .as_ref()
                        .map_or_else(|| "whole blob".to_owned(), crate::scheduler::block_runs),
                )
            },
        );
        if let Some(LaneRange {
            offset,
            len,
            landed,
            past_end,
            uncovered,
        }) = range
        {
            crate::fault::warn_or_info!(
                fault.warns(),
                %provider,
                %hash,
                offset,
                len,
                landed,
                past_end,
                uncovered,
                probed_holder,
                %coverage,
                ?fault,
                error = %decdn_common::redact::sanitize_err_chain(err),
                "a lane faulted; its remainder goes to the other lanes"
            );
        } else {
            crate::fault::warn_or_info!(
                fault.warns(),
                %provider,
                %hash,
                probed_holder,
                %coverage,
                ?fault,
                error = %decdn_common::redact::sanitize_err_chain(err),
                "a source faulted"
            );
        }
        let left_the_hash = match fault {
            // This client's voucher, not the node's delivery: the node declines
            // this fetch, and its peer-store record keeps no failure stamp.
            Fault::Source => match crate::fault::declining_rejection(err) {
                Some(reason) => {
                    self.record_declined(provider, DeclineReason::Voucher(reason), None)
                }
                None => self.record_source_fault(provider, err, range.as_ref(), now),
            },
            Fault::Fatal(_) | Fault::Unaffordable | Fault::Transient => false,
        };
        // A decline scopes to this hash (ADR 005 §Open-time refusal classes).
        // The health is shared across a bundle's entries, so a node that left
        // this hash does not cool for the others.
        if !left_the_hash {
            self.health.record(provider, fault, now, deposit);
        }
        fault
    }

    /// Hold `provider` off briefly instead of pricing it out, when `err` is a
    /// funding refusal that arrives while a funding recovery step's top-up
    /// settles ([`crate::RecoveryGate::settling`]): the node's chain watcher
    /// may not have seen the new deposit yet. Returns whether it held the
    /// source; any other fault is the caller's to record.
    pub fn hold_while_settling(
        &mut self,
        provider: Address,
        err: &anyhow::Error,
        now: Instant,
    ) -> bool {
        if classify(err) != Fault::Unaffordable {
            return false;
        }
        tracing::info!(
            %provider,
            hash = %blake3::Hash::from_bytes(self.hash).to_hex(),
            error = %format_args!("{err:#}"),
            "a source refused the pool's funding while a funding recovery step settles; \
             asking it again"
        );
        self.health
            .hold(provider, now + crate::recovery::SETTLE_STEP);
        true
    }

    /// The pool spend the built lanes committed, summed.
    #[must_use]
    pub(crate) fn lanes_spent(&self) -> U256 {
        self.lanes
            .values()
            .map(|lane| lane.ledger.committed().amount)
            .fold(U256::ZERO, U256::saturating_add)
    }

    /// The highest deposit any built lane's pool context holds now. A lane
    /// shares its context with sibling entries through the run's lane
    /// registry, so a sibling's funding recovery step shows here.
    #[must_use]
    pub(crate) fn lanes_deposit(&self) -> U256 {
        self.lanes
            .values()
            .filter_map(|lane| lane.ctx.lock().ok().map(|ctx| ctx.deposit))
            .fold(U256::ZERO, U256::max)
    }

    /// The deposit this set is exhausted at: `deposit`, or the highest deposit
    /// a known source is priced out at when that is higher. Bundle entries
    /// share their sources' health, so a sibling entry's lane can price a
    /// source out at a deposit above the one this set's own lanes report, and
    /// before this set builds a lane at all.
    #[must_use]
    pub(crate) fn priced_out_at(&self, deposit: U256) -> U256 {
        self.holders
            .iter()
            .filter_map(|h| match self.health.health(h.provider) {
                crate::health::Health::Unaffordable { at_deposit } => Some(at_deposit),
                crate::health::Health::Healthy { .. } | crate::health::Health::Cooling { .. } => {
                    None
                }
            })
            .fold(deposit, U256::max)
    }

    /// Every built lane.
    #[must_use]
    pub(crate) fn built_lanes(&self) -> Vec<Arc<StreamCandidate<P::Source>>> {
        self.lanes.values().cloned().collect()
    }

    /// Raise every built lane's pool context to at least `deposit`, after a
    /// funding recovery step.
    pub(crate) fn credit_lanes(&self, deposit: U256) {
        for lane in self.lanes.values() {
            let mut ctx = lane
                .ctx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ctx.deposit = ctx.deposit.max(deposit);
        }
    }

    /// Record a delivery fault of `provider`'s: a `NotFound` toward marking it
    /// absent, a `Declined` refusal as a bar, and a failure stamp in its
    /// peer-store record. A `Declined` refusal that removes the provider from
    /// this hash ([`Self::record_declined`]) is not a delivery fault: it
    /// stamps nothing, and the call returns `true`.
    fn record_source_fault(
        &mut self,
        provider: Address,
        err: &anyhow::Error,
        range: Option<&LaneRange>,
        now: Instant,
    ) -> bool {
        if crate::fault::says_absent(err)
            && !range.is_some_and(|r| r.past_end)
            && let Some(refused) = err.downcast_ref::<UpstreamRefused>()
        {
            self.record_not_found(provider, refused, range, now);
        }
        if let Some(refused) = err.downcast_ref::<UpstreamRefused>()
            && matches!(refused.error(), StreamError::Declined)
            && self.record_declined(provider, DeclineReason::Refused, Some(refused))
        {
            return true;
        }
        if let Some(holder) = self.holder(provider) {
            self.provider.on_source_fault(holder);
        }
        false
    }

    /// Count a `NotFound` from `provider` for a piece below the end the fetch
    /// knows ([`LaneRange::past_end`]). A provider that is not a probed
    /// holder is marked absent once its count reaches
    /// [`ABSENT_AFTER_NOT_FOUND`]. A probed holder is never marked absent.
    /// For a partial holder, an answer for a range that holds a chunk outside
    /// its coverage ([`LaneRange::uncovered`]) counts toward barring it from
    /// pull-through, and one for a range wholly inside it counts toward
    /// dropping each block the refused part of the range touches from its
    /// coverage. To a whole holder the answer is a delivery fault only.
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

    /// Record that `provider` declined this fetch for `reason`. A partial
    /// holder's `Declined` refusal is most often its size ceiling, which a
    /// node applies only when it pulls the blob through, so a partial holder
    /// is barred from pull-through only and keeps the blocks it covers. Any
    /// other source, and any source that rejected this client's voucher,
    /// never starts again for this blob in this fetch. Returns whether the
    /// provider left this blob: `false` for a partial holder's pull-through
    /// bar.
    fn record_declined(
        &mut self,
        provider: Address,
        reason: DeclineReason,
        refused: Option<&UpstreamRefused>,
    ) -> bool {
        let partial = self
            .holder(provider)
            .is_some_and(|holder| holder.coverage.is_some());
        let left = !(partial && reason == DeclineReason::Refused);
        let newly = if left {
            self.declined.insert(provider)
        } else {
            self.declined_pull_through.insert(provider)
        };
        if !self.decline_reasons.contains(&reason) {
            self.decline_reasons.push(reason);
        }
        if newly {
            self.mark_epoch = self.mark_epoch.saturating_add(1);
        }
        if let Some(refused) = refused {
            self.last_absent = Some(refused.clone());
        }
        left
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
    /// nothing. A bar from a `Declined` refusal stays.
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
    /// yet ended ([`Self::expire_pull_through_bars`]), or that refused such a
    /// range `Declined`. It still serves the blocks it covers and takes no
    /// range outside them. A discovery that reports wider coverage for it
    /// clears a `NotFound` bar; a `Declined` bar stays for the fetch.
    #[must_use]
    pub fn no_pull_through(&self, provider: Address) -> bool {
        self.no_pull_through.contains_key(&provider)
            || self.declined_pull_through.contains(&provider)
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

    /// The stop for an exhausted candidate set (ADR 039 §Failure handling), by
    /// the first rule that applies:
    ///
    /// 1. Every known source declined this fetch: [`NoNodeWillServe`] at once,
    ///    with no fresh discovery.
    /// 2. After a discovery at this deposit and mark epoch, every known source
    ///    is priced out or can serve none of the work left, with at least one
    ///    priced out: [`NoAffordableSource`]. The caller runs its funding
    ///    recovery step ([`crate::RecoveryGate::step`]) before it ends the fetch
    ///    "funding needed".
    /// 3. After such a discovery, every known source can serve none of the
    ///    work left: [`NoSourceHasBlob`], not-found.
    ///
    /// A source can serve none of the work left when it says the blob is
    /// absent, when it declined this fetch, or when it is barred from
    /// pull-through ([`Self::no_pull_through`]) and `only_uncovered_left` says
    /// no work left lies inside the coverage of a barred holder.
    #[must_use]
    pub fn exhausted(&self, deposit: U256, only_uncovered_left: bool) -> Option<anyhow::Error> {
        if self.all_declined(only_uncovered_left) {
            return Some(anyhow::Error::new(NoNodeWillServe {
                reasons: self.decline_reasons.clone(),
            }));
        }
        if self.holders.is_empty() || self.discovered_at != Some((deposit, self.mark_epoch)) {
            return None;
        }
        if self.all_excluded(deposit, only_uncovered_left) {
            return Some(anyhow::Error::new(NoAffordableSource { deposit }));
        }
        if self.all_item_marked(only_uncovered_left) {
            return Some(match &self.last_absent {
                Some(refused) => anyhow::Error::new(refused.clone()).context(NoSourceHasBlob),
                None => anyhow::Error::new(NoSourceHasBlob),
            });
        }
        None
    }

    /// Whether every known source declined this fetch: a `Declined` refusal
    /// or an unhealed voucher rejection, or, with `only_uncovered_left`, a
    /// partial holder's `Declined` pull-through refusal.
    fn all_declined(&self, only_uncovered_left: bool) -> bool {
        !self.holders.is_empty()
            && self.holders.iter().all(|h| {
                self.declined.contains(&h.provider)
                    || (only_uncovered_left && self.declined_pull_through.contains(&h.provider))
            })
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
    /// none of the work left, with at least one priced out that could serve
    /// the work at a higher deposit: a top-up cannot help a source that
    /// declined this blob or holds none of the work left. A set
    /// that is unanimous only on the latter is handled by
    /// [`Self::all_item_marked`] instead, so this is the affordability verdict
    /// even when it is mixed with absence marks.
    fn all_excluded(&self, deposit: U256, only_uncovered_left: bool) -> bool {
        !self.holders.is_empty()
            && self.holders.iter().any(|h| {
                self.is_unaffordable(h.provider, deposit)
                    && !self.cannot_serve(h.provider, only_uncovered_left)
            })
            && self.holders.iter().all(|h| {
                self.cannot_serve(h.provider, only_uncovered_left)
                    || self.is_unaffordable(h.provider, deposit)
            })
    }

    /// Whether `provider` can serve none of the work left: it says the blob is
    /// absent, it declined this fetch, or `only_uncovered_left` says
    /// no work left lies inside the coverage of a barred holder and the holder
    /// is barred for good: by a `Declined` refusal, or by `NotFound` again
    /// after the probe that followed its first bar.
    fn cannot_serve(&self, provider: Address, only_uncovered_left: bool) -> bool {
        let barred_for_good = self.declined_pull_through.contains(&provider)
            || (self.no_pull_through.contains_key(&provider)
                && self
                    .pull_through_bars
                    .get(&provider)
                    .is_some_and(|bars| *bars >= 2));
        self.absent.contains(&provider)
            || self.declined.contains(&provider)
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
mod tests;
