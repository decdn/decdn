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
use crate::source::{BlobSource, SourceFuture};
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
    /// The blocks it holds, or `None` for the whole blob.
    pub coverage: Option<Coverage>,
    /// The probed round-trip time. Lower starts first.
    pub rtt_ms: f64,
    /// Whether a probe reported that this provider holds the blob. A
    /// `NotFound` from a probed holder is a delivery fault only: on the wire
    /// it also means load shed, a per-signer cap, or a pool the node cannot
    /// confirm yet. A provider the probe did not report as a holder (a
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
/// bars it from pull-through ([`SourceSet::no_pull_through`]).
pub const ABSENT_AFTER_NOT_FOUND: u32 = 3;

/// Where a [`SourceSet`] finds holders and builds their lanes.
pub trait SourceProvider: Send + Sync {
    /// The paid source a built lane fetches from.
    type Source: BlobSource;

    /// Find and probe the current holders of `hash`.
    fn discover(&self, hash: [u8; 32]) -> SourceFuture<'_, Vec<Holder>>;

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
struct Backoff {
    next_at: Instant,
    attempts: u32,
}

impl Backoff {
    fn fail(self, now: Instant, base: Duration, cap: Duration) -> Self {
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
    absent: HashSet<Address>,
    /// Each non-holder's `NotFound` answers since its last verified byte.
    not_found: HashMap<Address, u32>,
    /// Each probed partial holder's `NotFound` answers for ranges outside its
    /// coverage since it last served such a range.
    pull_through_not_found: HashMap<Address, u32>,
    /// Probed partial holders that said `NotFound` to
    /// [`ABSENT_AFTER_NOT_FOUND`] ranges outside their coverage. Each one
    /// serves only the blocks it covers.
    no_pull_through: HashSet<Address>,
    /// Providers that refused the blob as larger than their size ceiling
    /// (`StreamError::BlobTooLarge`). The ceiling is a stable node policy
    /// (ADR 005), so none of them starts again for this blob, and a
    /// rediscovery does not lift it.
    too_large: HashSet<Address>,
    /// The refusal that last marked a source absent, barred it from
    /// pull-through or excluded it as too small for the blob: the cause the
    /// [`NoSourceHasBlob`] stop carries.
    last_absent: Option<UpstreamRefused>,
    discovery: Option<Backoff>,
    /// Bumped each time a source newly joins `absent`, `no_pull_through` or
    /// `too_large`.
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
            absent: HashSet::new(),
            not_found: HashMap::new(),
            pull_through_not_found: HashMap::new(),
            no_pull_through: HashSet::new(),
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
                let prior = self.build_retry.get(&provider).copied().unwrap_or(Backoff {
                    next_at: now,
                    attempts: 0,
                });
                self.build_retry
                    .insert(provider, prior.fail(now, BUILD_RETRY_BASE, BUILD_RETRY_CAP));
                Err(anyhow::Error::new(LaneBuildFault(err)))
            }
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
                    let uncovered = range.is_some_and(|r| r.uncovered);
                    self.record_not_found(provider, refused, uncovered);
                }
                if let Some(refused) = err.downcast_ref::<UpstreamRefused>()
                    && matches!(refused.error(), StreamError::BlobTooLarge)
                {
                    let newly = self.too_large.insert(provider);
                    self.marked(newly, refused);
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
    /// Inside its coverage the answer is a delivery fault only. For a range
    /// outside the coverage of a partial holder (`uncovered`), the answer
    /// counts toward barring it from pull-through.
    fn record_not_found(&mut self, provider: Address, refused: &UpstreamRefused, uncovered: bool) {
        let Some(holder) = self.holder(provider) else {
            return;
        };
        if holder.probed_holder {
            if uncovered && holder.coverage.is_some() {
                self.record_pull_through_refusal(provider, refused);
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

    /// Count a `NotFound` from probed partial holder `provider` for a range
    /// outside its coverage. At [`ABSENT_AFTER_NOT_FOUND`] answers the holder
    /// is barred from pull-through.
    fn record_pull_through_refusal(&mut self, provider: Address, refused: &UpstreamRefused) {
        let count = self.pull_through_not_found.entry(provider).or_insert(0);
        *count = count.saturating_add(1);
        if *count < ABSENT_AFTER_NOT_FOUND {
            return;
        }
        let newly = self.no_pull_through.insert(provider);
        self.marked(newly, refused);
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

    /// Record a verified byte from `provider` on a range outside its
    /// coverage: it serves such ranges by pull-through after all, so its
    /// count of `NotFound` answers for them clears, and so does its bar.
    pub fn record_pull_through(&mut self, provider: Address) {
        self.pull_through_not_found.remove(&provider);
        self.no_pull_through.remove(&provider);
    }

    /// Whether `provider` is barred from pull-through: a probed partial
    /// holder that said `NotFound` to [`ABSENT_AFTER_NOT_FOUND`] ranges
    /// outside its coverage, with no such range served since. It still serves
    /// the blocks it covers and takes no range outside them. A discovery that
    /// reports wider coverage for it clears the bar.
    #[must_use]
    pub fn no_pull_through(&self, provider: Address) -> bool {
        self.no_pull_through.contains(&provider)
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
                tracing::debug!("discovery failed: {err:#}");
                self.discovery = Some(self.next_discovery_backoff(now));
            }
        }
        self.looked_at = Some((deposit, self.mark_epoch));
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
            .filter(|t| *t > now);
        let discovery = self.discovery.map(|b| b.next_at).filter(|t| *t > now);
        cooling.chain(builds).chain(discovery).min()
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
    /// absent, it refused the blob as too large, or it is barred from
    /// pull-through and `only_uncovered_left` says no work left lies inside
    /// the coverage of a barred holder.
    fn cannot_serve(&self, provider: Address, only_uncovered_left: bool) -> bool {
        self.absent.contains(&provider)
            || self.too_large.contains(&provider)
            || (only_uncovered_left && self.no_pull_through.contains(&provider))
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
    fn merge(&mut self, holders: Vec<Holder>) {
        for holder in holders {
            if holder.probed_holder {
                self.absent.remove(&holder.provider);
                self.not_found.remove(&holder.provider);
                let wider = self.holder(holder.provider).is_some_and(|known| {
                    covers_more(holder.coverage.as_ref(), known.coverage.as_ref())
                });
                if wider {
                    self.pull_through_not_found.remove(&holder.provider);
                    self.no_pull_through.remove(&holder.provider);
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

    /// A partial holder that says `NotFound` three times to ranges outside
    /// its coverage is barred from pull-through. Refusals inside its coverage
    /// never bar it, a verified byte inside its coverage keeps the bar, and a
    /// verified byte outside it lifts the bar.
    #[tokio::test(start_paused = true)]
    async fn three_uncovered_refusals_bar_a_partial_holder_from_pull_through() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
        let now = Instant::now();
        say_not_found_in(&mut set, A, block1(false), 10, now);
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
        set.record_pull_through(A);
        assert!(!set.no_pull_through(A), "an uncovered byte lifts the bar");
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
