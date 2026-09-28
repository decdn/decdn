//! The sources of one blob (spec § Unit 1).
//!
//! A `SourceSet` holds every provider known to hold the blob, the lane built
//! for each, and when each may be tried again. It never drops a source: a
//! delivery fault cools it (in the command-wide [`PeerHealth`]), a price
//! refusal parks it until the deposit rises, and a lane-build or discovery
//! error backs off and retries. It ends a fetch only on a unanimous verdict:
//! every known source is priced out, every one signs a different size, or
//! every one says it does not hold the blob, and a fresh discovery found
//! nothing new.
//!
//! The state is synchronous. The acquire loop runs the `connect` and `discover`
//! futures itself, so lanes keep streaming while a lane builds or discovery
//! runs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alloy::primitives::{Address, U256};
use decdn_protocol::Coverage;
use tokio::time::{Duration, Instant};

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
}

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

/// Every known source signs a size other than the one this item is keyed by.
#[derive(Debug)]
pub struct NoSourceAgreesOnSize;

impl std::fmt::Display for NoSourceAgreesOnSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("every provider signs a different size than the one this item expects")
    }
}

impl std::error::Error for NoSourceAgreesOnSize {}

/// Every known source says it does not hold the blob, and a fresh discovery
/// found no other holder.
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
    wrong_size: HashSet<Address>,
    absent: HashSet<Address>,
    discovery: Option<Backoff>,
    /// Bumped each time a source newly joins `absent` or `wrong_size`.
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
            wrong_size: HashSet::new(),
            absent: HashSet::new(),
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

    /// The known holder for `provider`.
    #[must_use]
    pub fn holder(&self, provider: Address) -> Option<&Holder> {
        self.holders.iter().find(|h| h.provider == provider)
    }

    /// The nearest source that may start now and is not already `running`.
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
            .filter(|h| !self.wrong_size.contains(&h.provider))
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
    pub fn record_fault(
        &mut self,
        provider: Address,
        err: &anyhow::Error,
        now: Instant,
        deposit: U256,
    ) -> Fault {
        let fault = classify(err);
        match fault {
            Fault::WrongSize => {
                if self.wrong_size.insert(provider) {
                    self.mark_epoch = self.mark_epoch.saturating_add(1);
                }
            }
            Fault::Source => {
                if crate::fault::says_absent(err) && self.absent.insert(provider) {
                    self.mark_epoch = self.mark_epoch.saturating_add(1);
                }
                if let Some(holder) = self.holder(provider) {
                    self.provider.on_source_fault(holder);
                }
            }
            Fault::Fatal(_) | Fault::Unaffordable | Fault::Transient => {}
        }
        self.health.record(provider, fault, now, deposit);
        tracing::debug!(%provider, ?fault, "source fault: {err:#}");
        fault
    }

    /// Record a verified byte from `provider`: it holds the blob after all.
    pub fn record_progress(&mut self, provider: Address) {
        self.absent.remove(&provider);
        self.health.record_progress(provider);
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
        let unanimous = self.all_excluded(deposit) || self.all_item_marked();
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
    /// every known source says the blob is absent or signs the wrong size (the
    /// item ends), or every known source is priced out, disagrees on size, or
    /// says the blob is absent, with at least one priced out, and the top-up
    /// budget cannot change that (the command ends).
    #[must_use]
    pub fn exhausted(&self, deposit: U256, topups_left: bool) -> Option<anyhow::Error> {
        if self.holders.is_empty() || self.discovered_at != Some((deposit, self.mark_epoch)) {
            return None;
        }
        if self.all_item_marked() {
            return Some(if self.absent.is_empty() {
                anyhow::Error::new(NoSourceAgreesOnSize)
            } else {
                anyhow::Error::new(NoSourceHasBlob)
            });
        }
        if !self.all_excluded(deposit) {
            return None;
        }
        (!topups_left).then(|| anyhow::Error::new(NoAffordableSource { deposit }))
    }

    /// Whether every known source says the blob is absent or signs the wrong size.
    fn all_item_marked(&self) -> bool {
        !self.holders.is_empty()
            && self
                .holders
                .iter()
                .all(|h| self.absent.contains(&h.provider) || self.wrong_size.contains(&h.provider))
    }

    /// Whether every known source is priced out at `deposit`, disagrees on
    /// size, or says the blob is absent, with at least one actually priced
    /// out. A set that is unanimous only on size or absence is handled by
    /// [`Self::all_item_marked`] instead, so this is the affordability verdict
    /// even when it is mixed with size or absence marks.
    fn all_excluded(&self, deposit: U256) -> bool {
        !self.holders.is_empty()
            && self
                .holders
                .iter()
                .any(|h| self.is_unaffordable(h.provider, deposit))
            && self.holders.iter().all(|h| {
                self.wrong_size.contains(&h.provider)
                    || self.absent.contains(&h.provider)
                    || self.is_unaffordable(h.provider, deposit)
            })
    }

    /// Whether `provider`'s health says it is priced out at `deposit`.
    fn is_unaffordable(&self, provider: Address, deposit: U256) -> bool {
        matches!(
            self.health.health(provider),
            crate::health::Health::Unaffordable { at_deposit } if deposit <= at_deposit
        )
    }

    fn merge(&mut self, holders: Vec<Holder>) {
        for holder in holders {
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

/// A [`SourceProvider`] over a fixed set of prebuilt lanes: for SDK callers
/// that dial their own providers, and for tests. Discovery returns the same
/// set; each lane is handed out once.
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
    /// Wrap `candidates`, keyed by each one's on-chain provider.
    ///
    /// # Errors
    ///
    /// Two candidates naming the same provider.
    pub fn new(candidates: Vec<StreamCandidate<S>>) -> anyhow::Result<Self> {
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
    use super::{
        DISCOVERY_BASE, Holder, NoAffordableSource, NoSourceAgreesOnSize, SourceProvider, SourceSet,
    };
    use crate::fault::{Fault, LaneBuildFault};
    use crate::health::{COOL_BASE, PeerHealth};
    use crate::source::{ScriptedSource, SourceFuture, ctx_with};
    use crate::streamer::StreamCandidate;
    use crate::{Cumulative, LaneLease, PoolLedger, SignedSizeMismatch};
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
            first_unit: None,
            lease: LaneLease::new(()),
        })
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
        let fault = set.record_fault(A, &anyhow::anyhow!("reset"), now, U256::ZERO);
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
        set.record_fault(A, &anyhow::anyhow!("reset"), now, U256::ZERO);
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
        set.record_fault(A, &dry(), now, dep);
        set.record_fault(B, &dry(), now, dep);
        assert!(set.exhausted(dep, true).is_none(), "top-ups left");
        assert!(
            set.exhausted(dep, false).is_none(),
            "no discovery at this deposit yet"
        );
        assert!(set.wants_discovery(now, dep, 0, false));
        set.discovery_done(Ok(vec![]), now, dep);
        let err = set.exhausted(dep, false);
        assert!(err.is_some_and(|e| e.downcast_ref::<NoAffordableSource>().is_some()));
        assert!(
            set.exhausted(dep + U256::from(1u64), false).is_none(),
            "a top-up revives"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn every_source_disagreeing_on_size_ends_the_item() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        let now = Instant::now();
        let wrong = anyhow::Error::new(SignedSizeMismatch {
            signed: 1,
            expected: 2,
        });
        assert_eq!(
            set.record_fault(A, &wrong, now, U256::ZERO),
            Fault::WrongSize
        );
        assert!(
            set.next_to_start(now, U256::ZERO, &HashSet::new())
                .is_none()
        );
        set.discovery_done(Ok(vec![]), now, U256::ZERO);
        let err = set.exhausted(U256::ZERO, true);
        assert!(err.is_some_and(|e| e.downcast_ref::<NoSourceAgreesOnSize>().is_some()));
    }

    fn not_found() -> anyhow::Error {
        anyhow::Error::new(crate::UpstreamRefused::mid_stream(
            decdn_protocol::client::StreamError::NotFound,
        ))
    }

    #[tokio::test(start_paused = true)]
    async fn every_source_saying_not_found_ends_the_item() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(
            &p,
            [0; 32],
            Arc::default(),
            vec![holder(A, 10.0), holder(B, 20.0)],
        );
        let now = Instant::now();
        set.record_fault(A, &not_found(), now, U256::ZERO);
        assert!(
            set.exhausted(U256::ZERO, true).is_none(),
            "B has not answered"
        );
        set.record_fault(B, &not_found(), now, U256::ZERO);
        assert!(
            set.exhausted(U256::ZERO, true).is_none(),
            "no discovery since the last mark"
        );
        assert!(
            set.wants_discovery(now, U256::ZERO, 0, false),
            "unanimity skips the backoff"
        );
        set.discovery_done(Ok(vec![]), now, U256::ZERO);
        let err = set.exhausted(U256::ZERO, true);
        assert!(err.is_some_and(|e| e.downcast_ref::<super::NoSourceHasBlob>().is_some()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_holder_from_discovery_keeps_the_item_alive() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        let now = Instant::now();
        set.record_fault(A, &not_found(), now, U256::ZERO);
        set.discovery_done(Ok(vec![holder(B, 5.0)]), now, U256::ZERO);
        assert!(set.exhausted(U256::ZERO, true).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn progress_clears_a_not_found_mark() {
        let p = provider(vec![]);
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        let now = Instant::now();
        set.record_fault(A, &not_found(), now, U256::ZERO);
        set.record_progress(A);
        set.discovery_done(Ok(vec![]), now, U256::ZERO);
        assert!(set.exhausted(U256::ZERO, true).is_none());
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
            vec![holder(A, 10.0), holder(B, 20.0)],
        );
        let now = Instant::now();
        set.record_fault(A, &not_found(), now, U256::ZERO);
        set.record_fault(B, &not_found(), now, U256::ZERO);
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
        let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
        let now = Instant::now();
        set.record_fault(A, &not_found(), now, U256::ZERO);
        set.discovery_done(Err(anyhow::anyhow!("registry rpc down")), now, U256::ZERO);
        let later = now + DISCOVERY_BASE;
        // A's delivery cooldown (2 s) is long over by `later` (5 s), so it is
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
            vec![holder(A, 10.0), holder(B, 20.0)],
        );
        let now = Instant::now();
        let dep = U256::from(100u64);
        let dry = anyhow::Error::new(crate::driver::PoolExhausted {
            gap_start: 0,
            gap_len: 1,
        });
        set.record_fault(A, &dry, now, dep);
        set.record_fault(B, &not_found(), now, dep);
        set.discovery_done(Ok(vec![]), now, dep);
        assert!(set.exhausted(dep, true).is_none(), "top-ups left");
        let err = set.exhausted(dep, false);
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
