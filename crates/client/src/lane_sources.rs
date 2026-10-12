//! The [`SourceProvider`] a paid fetch runs its acquire loop over: where
//! `decdn fetch` and every entry of a `bundle pull` find a blob's holders and
//! build their paid lanes.
//!
//! Discovery is [`resolve_target_node`]; a lane is one provider's pool lane
//! (`build_multi_lane`). The provider records each built lane's watermark
//! handle, so the command persists what every lane paid once the fetch ends,
//! and files each delivery fault in the peer store the next run's fast path
//! reads.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::Address;
use decdn_incentive::PoolId;
use iroh::RelayUrl;

use crate::discovery::NodeCandidate;
use crate::funding::PoolFunder;
use crate::lanes::{
    FaceLaneHandle, LaneDeps, LanePayer, build_multi_lane, lane_target, queue_face_watermarks,
};
use crate::session::{DiscoveryOptions, ProxyWarmingParams};
use crate::source::{SourceFuture, SourceStream};
use crate::stream_cap::LaneStreamCap;
use crate::targets::{
    LateProbes, ProbeOpts, ProbeOutcome, ProbeRound, ResolvedTargets, now_secs,
    resolve_target_node, spawn_harvest,
};
use crate::{
    CredentialSlot, Holder, LaneLease, LaneLedgers, PeerHealth, PeerSource, PoolContext,
    RecoveryGate, SourceProvider, SourceSet, StopPolicy, StreamCandidate, first_open,
};

/// A fetch's first size claim ([`LaneSources::first_claim`]): a hint the fetch
/// grows or shrinks as verified bytes land.
#[derive(Debug)]
pub struct FirstClaim {
    /// The claimed size.
    pub total_bytes: u64,
    /// Every holder known when the claim was made.
    pub holders: Vec<Holder>,
}

/// `hint` as the first claim over `holders`, or, with no hint, what `open`
/// learns from a header-only open. A hint needs no open.
///
/// # Errors
///
/// The error `open` ends with.
async fn first_claim_or_open<F, Fut>(
    hint: Option<u64>,
    holders: Vec<Holder>,
    open: F,
) -> anyhow::Result<FirstClaim>
where
    F: FnOnce(Vec<Holder>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<FirstClaim>>,
{
    match hint {
        Some(total_bytes) => Ok(FirstClaim {
            total_bytes,
            holders,
        }),
        None => open(holders).await,
    }
}

/// What a failed fetch's lanes saw, for explaining the failure to the user
/// ([`LaneSources::fault_context`]).
#[derive(Debug, Clone)]
pub struct FaultContext {
    /// The context of the first lane built; `None` before any lane is built.
    pub first_lane: Option<Arc<Mutex<PoolContext>>>,
    /// The pool the run's lanes pay from, once a lane is built.
    pub pool_id: Option<PoolId>,
    /// Whether the fetch pays from a delegated pool under a capability.
    pub delegated: bool,
}

/// The [`SourceProvider`] a paid fetch runs its acquire loop over.
pub struct LaneSources<'a, P> {
    deps: &'a LaneDeps<'a, P>,
    discovery: &'a DiscoveryOptions,
    relays: &'a [RelayUrl],
    /// What every lane signs under: the owner key, or the delegated slot it
    /// reads at each lane build.
    payer: LanePayer<'a>,
    voucher_dom: &'a Eip712Domain,
    /// The slot generation whose key's earlier spend was last recorded in the
    /// slot ([`Self::seed_spend`]); `u64::MAX` before the first record.
    seeded: AtomicU64,
    /// Serializes each lane's pool open-or-reuse: across one fetch's
    /// concurrently built lanes, which all open or reuse the one pool, and
    /// across sibling fetches on that pool (`bundle pull`).
    open_lock: Option<&'a tokio::sync::Mutex<()>>,
    /// The run's shared voucher-ledger registry (`bundle pull`); `None` for a
    /// solo fetch.
    ledgers: Option<&'a LaneLedgers>,
    /// The run's per-provider stream cap (`bundle pull`): a lane, the first
    /// open's included, holds one of its provider's permits while its own
    /// worker runs, and takes a free one again to start again.
    lane_cap: Option<&'a LaneStreamCap>,
    /// The fetch's funding recovery gate: the first open and the acquire loop
    /// share it, and so does every entry of a bundle pull and a pass that runs
    /// again against a replaced pool.
    recovery: &'a Arc<RecoveryGate>,
    /// Every holder's node, keyed by its on-chain provider address.
    nodes: Mutex<HashMap<Address, NodeCandidate>>,
    peer_store: crate::PeerStore,
    /// Every built lane's watermark handle, in build order.
    handles: Mutex<Vec<FaceLaneHandle>>,
    /// The lane that answered the first open, handed to the fetch's first
    /// `connect` for its provider.
    parked: Mutex<HashMap<Address, StreamCandidate<PeerSource<'a>>>>,
    /// The size hint of the last probe that gave one
    /// ([`ResolvedTargets::size_hint`]): the fetch's first claim.
    size_hint: Mutex<Option<u64>>,
    /// The pending probes of the streamed round the holders came from
    /// ([`ResolvedTargets::late`]), until [`SourceProvider::arrivals`] takes
    /// them.
    late: Mutex<Option<LateProbes>>,
}

impl<P> std::fmt::Debug for LaneSources<'_, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneSources")
            .field("payer", &self.payer)
            .field("relays", &self.relays.len())
            .finish_non_exhaustive()
    }
}

impl<'a, P> LaneSources<'a, P>
where
    P: alloy::providers::Provider + Clone,
{
    /// A provider over the fetch's shared `deps`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        deps: &'a LaneDeps<'a, P>,
        discovery: &'a DiscoveryOptions,
        relays: &'a [RelayUrl],
        payer: LanePayer<'a>,
        voucher_dom: &'a Eip712Domain,
        open_lock: Option<&'a tokio::sync::Mutex<()>>,
        ledgers: Option<&'a LaneLedgers>,
        lane_cap: Option<&'a LaneStreamCap>,
        recovery: &'a Arc<RecoveryGate>,
    ) -> Self {
        Self {
            deps,
            discovery,
            relays,
            payer,
            voucher_dom,
            seeded: AtomicU64::new(u64::MAX),
            open_lock,
            ledgers,
            lane_cap,
            recovery,
            nodes: Mutex::new(HashMap::new()),
            peer_store: crate::PeerStore::open(&deps.site.data_dir),
            handles: Mutex::new(Vec::new()),
            parked: Mutex::new(HashMap::new()),
            size_hint: Mutex::new(None),
            late: Mutex::new(None),
        }
    }

    /// The holders `targets` names, indexing each one's node for `connect`.
    /// A probe's size hint is kept for [`Self::first_claim`], and a streamed
    /// round's pending probes for [`SourceProvider::arrivals`].
    pub fn holders_from(&self, targets: &ResolvedTargets) -> Vec<Holder> {
        if let Some(hint) = targets.size_hint {
            *self
                .size_hint
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(hint);
        }
        if let Some(late) = targets.late.take() {
            *self.late.lock().unwrap_or_else(PoisonError::into_inner) = Some(late);
        }
        holders_from(targets, &mut self.lock_nodes())
    }

    /// The [`Holder`] for a late `join` ([`admit_late`]), counted as joined.
    /// `None` when the join's operator is already in the set.
    fn register_late(&self, join: LateJoin) -> Option<Holder> {
        let holder = admit_late(&mut self.lock_nodes(), join)?;
        if let Some(marks) = self.deps.marks {
            marks.holder_joined();
        }
        Some(holder)
    }

    /// The funder a fetch over these sources runs its funding recovery step
    /// through: the pool the run's first built lane pays from. A delegated fetch has
    /// a zero working deposit, so its step has no funding path.
    pub fn funder(&self) -> PoolFunder<'_, P> {
        PoolFunder {
            contract: self.deps.contract,
            rpc: self.deps.rpc,
            store: self.deps.store.as_ref(),
            owner: self.deps.self_address,
            signer: match self.payer {
                LanePayer::Owned(signer) => Arc::clone(signer),
                LanePayer::Delegated(slot) => slot.current().signer,
            },
            deployment: self.deps.chain.deployment(),
            token: self.deps.token,
            payment_pool_addr: self.deps.chain.payment_pool,
            working_deposit: self.deps.deposit.working_deposit,
            max_approve: self.deps.deposit.max_approve,
            funding: self.deps.funding,
        }
    }

    /// A delegated fetch's credential slot, shared by every lane the fetch
    /// builds, with its current key's earlier spend recorded
    /// ([`Self::seed_spend`]). `None` for a fetch that pays from its own pool.
    pub fn credentials(&self) -> Option<CredentialSlot> {
        let LanePayer::Delegated(slot) = self.payer else {
            return None;
        };
        self.seed_spend(slot);
        Some(slot.clone())
    }

    /// Record in `slot` what earlier fetches signed under its current key, at
    /// every provider the buyer store holds a lane for: the capability's
    /// spend so far. Read once per slot generation, so a swapped-in key is
    /// counted from its own records.
    fn seed_spend(&self, slot: &CredentialSlot) {
        let generation = slot.generation();
        if self.seeded.swap(generation, Ordering::Relaxed) == generation {
            return;
        }
        let signer = slot.current().signer.address();
        let pool_id = slot.pool_id();
        let row = self
            .deps
            .store
            .get_by_pool_id(pool_id)
            .unwrap_or_else(|error| {
                // Counting from zero overstates the headroom, so a spent cap
                // can read as the publisher's pool needing funds.
                tracing::warn!(
                    %pool_id,
                    %error,
                    "could not read the buyer store; this key's earlier spend counts as zero"
                );
                None
            });
        let recorded = row.map_or(alloy::primitives::U256::ZERO, |row| {
            row.lanes()
                .filter(|(lane, _)| lane.signer == signer)
                .fold(alloy::primitives::U256::ZERO, |acc, (_, progress)| {
                    acc.saturating_add(progress.last_amount)
                })
        });
        slot.record_spend(signer, recorded);
    }

    /// The fetch's funding recovery gate.
    pub const fn recovery(&self) -> &Arc<RecoveryGate> {
        self.recovery
    }

    /// The fetch's first size claim (#2218): the probe's size hint when a
    /// probe gave one ([`Self::holders_from`]), with no open at all. Without a
    /// hint (a pinned `--node-id`, the peer-store fast path), a header-only
    /// open of one of `holders` learns the size one holder signs
    /// ([`first_open`]), with the acquire loop's recovery: a holder that
    /// faults cools and another is tried until one answers, a fault only the
    /// user can fix ends it, and `stop` gives up. The open's pull is dropped
    /// once its header is read. The lane that answered is parked for the
    /// fetch's `connect`, so the fetch reuses it; every other lane the open
    /// built drops, and with it any stream permit it held. Returns the claim
    /// and every holder known, those the open's discovery found included.
    ///
    /// # Errors
    ///
    /// Any error [`first_open`] returns.
    pub async fn first_claim(
        &self,
        hash: [u8; 32],
        holders: Vec<Holder>,
        health: &Arc<PeerHealth>,
        stop: &StopPolicy,
    ) -> anyhow::Result<FirstClaim> {
        let hint = *self
            .size_hint
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        first_claim_or_open(hint, holders, |holders| {
            self.open_for_header(hash, holders, health, stop)
        })
        .await
    }

    /// The size one of `holders` signs, from a header-only open
    /// ([`Self::first_claim`]).
    async fn open_for_header(
        &self,
        hash: [u8; 32],
        holders: Vec<Holder>,
        health: &Arc<PeerHealth>,
        stop: &StopPolicy,
    ) -> anyhow::Result<FirstClaim> {
        let mut set = SourceSet::new(self, hash, Arc::clone(health), holders);
        let funder = self.funder();
        let credentials = self.credentials();
        let opened = first_open(
            &mut set,
            stop,
            &funder,
            self.recovery,
            credentials.as_ref(),
            |lane| async move {
                let (header, _whole) = lane.source.open_whole(hash).await?;
                let provider = lane
                    .ctx
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .provider;
                self.record_open(provider, header.rate_per_mb);
                Ok(header.total_bytes)
            },
        )
        .await;
        let answered = opened.as_ref().ok().map(|&(provider, _)| provider);
        if let Some((provider, lane)) = keep_answering(set.take_lanes(), answered) {
            self.parked
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(provider, lane);
        }
        let holders = set.holders().to_vec();
        opened.map(|(_signer, total_bytes)| FirstClaim {
            total_bytes,
            holders,
        })
    }

    /// Persist every built lane's voucher watermark, and return once the
    /// writes land ([`queue_face_watermarks`]).
    pub async fn persist_watermarks(&self) {
        if self.queue_watermarks().await.is_ok() {
            return;
        }
        let (pools, providers): (Vec<_>, Vec<_>) = self
            .handles
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|h| (h.pool_id.to_string(), h.provider.to_string()))
            .unzip();
        tracing::warn!(
            lanes = providers.len(),
            pools = %pools.join(","),
            providers = %providers.join(","),
            "the voucher watermark write for {} lane(s) did not finish: it panicked or the \
             runtime cancelled it; the next reuse may re-sign a stale watermark, which its \
             provider rejects",
            providers.len()
        );
    }

    /// Persist every built lane's voucher watermark without waiting for the
    /// writes: the settle a dropped fetch runs from its drop guard.
    pub fn persist_watermarks_detached(&self) {
        drop(self.queue_watermarks());
    }

    /// Read every built lane's watermark from its ledger now, and queue the
    /// writes in that order ([`queue_face_watermarks`]).
    fn queue_watermarks(&self) -> tokio::sync::oneshot::Receiver<()> {
        let handles = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        queue_face_watermarks(
            self.deps.writes,
            self.deps.store,
            self.deps.self_address,
            &handles,
        )
    }

    /// What the fetch's lanes saw, for a caller that explains a failure.
    pub fn fault_context(&self) -> FaultContext {
        FaultContext {
            first_lane: self
                .handles
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .first()
                .map(|h| Arc::clone(&h.ctx)),
            pool_id: self.deps.funding.pool_id(),
            delegated: matches!(self.payer, LanePayer::Delegated(_)),
        }
    }

    /// File a lane's first answer in the peer store: its quoted rate, with the
    /// failure stamp cleared.
    fn record_open(&self, provider: Address, rate_per_mb: u64) {
        let Some(node_id) = self.lock_nodes().get(&provider).map(|n| n.node_id) else {
            return;
        };
        self.file(PeerEvent::Open {
            node_id,
            rate_per_mb,
        });
    }

    /// Queue `event` on the command's ordered writes: off the runtime thread,
    /// and behind every record any entry of the command filed before it
    /// ([`PeerEvent::write`]).
    fn file(&self, event: PeerEvent) {
        let store = self.peer_store.clone();
        self.deps.writes.queue(move || event.write(&store));
    }

    fn lock_nodes(&self) -> std::sync::MutexGuard<'_, HashMap<Address, NodeCandidate>> {
        self.nodes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `holder`'s lane: the one the first open parked, with the permit it
    /// already holds, or a fresh build. When the run caps streams per
    /// provider, a fresh lane holds one of its provider's permits
    /// ([`lane_lease`]), taken before the build and never waited for. Every
    /// lane it returns, the parked one included, takes an extra stream, or
    /// the stream it starts again on after its lease is given back, only
    /// from the provider's permits that are free; at a cap of 3 or more an
    /// extra stream leaves the last free one for a sibling entry, and a lane
    /// refused a stream claims the next permit that frees
    /// ([`LaneStreamCap::widen`]).
    async fn build(&self, holder: &Holder) -> anyhow::Result<StreamCandidate<PeerSource<'a>>> {
        let parked = self
            .parked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&holder.provider);
        let mut lane = if let Some(lane) = parked {
            lane
        } else {
            let lease = lane_lease(self.lane_cap, holder.provider).await?;
            let mut lane = self.build_fresh(holder).await?;
            lane.lease = lease;
            lane
        };
        if lane.widen.is_none()
            && let Some(cap) = self.lane_cap
        {
            lane.widen = Some(cap.widen(holder.provider).await);
        }
        Ok(lane)
    }

    /// Build `holder`'s lane and record its watermark handle.
    async fn build_fresh(
        &self,
        holder: &Holder,
    ) -> anyhow::Result<StreamCandidate<PeerSource<'a>>> {
        let candidate = self
            .lock_nodes()
            .get(&holder.provider)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no known node for provider {}", holder.provider))?;
        let addr = self.discovery.pinned.and_then(|pinned| pinned.addr);
        let target = lane_target(&candidate, addr, self.relays);
        // A lane built after a swap records the new key's earlier spend first.
        if let LanePayer::Delegated(slot) = self.payer {
            self.seed_spend(slot);
        }
        let lane = build_multi_lane(
            self.deps,
            self.payer,
            self.voucher_dom,
            holder.provider,
            target,
            self.open_lock,
            self.ledgers,
        )
        .await?;
        self.deps.funding.note_pool(lane.pool_id);
        self.handles
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(FaceLaneHandle {
                pool_id: lane.pool_id,
                provider: lane.provider,
                signer: lane.signer,
                prior_amount: lane.prior_amount,
                ledger: Arc::clone(&lane.ledger),
                ctx: Arc::clone(&lane.ctx),
            });
        let mut candidate = StreamCandidate::new(lane.source, lane.ctx, lane.ledger);
        candidate.coverage.clone_from(&holder.coverage);
        Ok(candidate)
    }
}

/// What a lane holds for its provider under the run's stream cap: one of the
/// provider's permits, or nothing without a cap.
///
/// It never waits. A lane that waited for one provider's permit while its
/// fetch held another's could deadlock against a sibling entry doing the
/// reverse. With every permit taken it fails instead, and the source set backs
/// the build off and retries it, holding nothing meanwhile.
///
/// # Errors
///
/// Every permit for `provider` is taken.
async fn lane_lease(cap: Option<&LaneStreamCap>, provider: Address) -> anyhow::Result<LaneLease> {
    let Some(cap) = cap else {
        return Ok(LaneLease::default());
    };
    cap.try_permit(provider)
        .await
        .map(LaneLease::new)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "every stream permit for provider {provider} is taken; the lane builds once \
                 one frees"
            )
        })
}

/// Of the lanes a first open built, the one whose provider `answered`, owned.
/// Every other lane drops here, and with it its lease, so a provider that
/// faulted the first open does not keep its stream permit from sibling
/// entries.
fn keep_answering<L>(
    lanes: Vec<(Address, Arc<L>)>,
    answered: Option<Address>,
) -> Option<(Address, L)> {
    let answered = answered?;
    lanes
        .into_iter()
        .find(|(provider, _)| *provider == answered)
        .and_then(|(provider, lane)| Arc::try_unwrap(lane).ok().map(|lane| (provider, lane)))
}

impl<'a, P> SourceProvider for LaneSources<'a, P>
where
    P: alloy::providers::Provider + Clone,
{
    type Source = PeerSource<'a>;

    fn discover(&self, hash: [u8; 32]) -> SourceFuture<'_, Vec<Holder>> {
        Box::pin(async move {
            // A fresh look: skip the peer store's fast path. A rediscovery
            // settles its round and drops the tail.
            let mut opts = self.discovery.clone();
            opts.rediscover = true;
            let probe = ProbeOpts {
                marks: None,
                round: ProbeRound::Settle,
            };
            let targets = resolve_target_node(
                &opts,
                (self.deps.chain, self.deps.site),
                self.deps.endpoint,
                self.relays,
                hash,
                probe,
            )
            .await?;
            Ok(self.holders_from(&targets))
        })
    }

    /// The late answers of the streamed round the holders came from: each
    /// holder, and each non-holder the warming rule picks ([`late_join`]), as
    /// it answers. Every verified answer's probe sample is harvested into the
    /// peer store once the round ends.
    fn arrivals(&self) -> Option<SourceStream<'_, Holder>> {
        use futures_util::StreamExt as _;
        let LateProbes {
            tail,
            best_holder_rtt_ms,
            warming,
        } = self
            .late
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()?;
        let dir = self.deps.site.data_dir.clone();
        Some(Box::pin(futures_util::stream::unfold(
            (tail, best_holder_rtt_ms, Vec::new()),
            move |(mut tail, mut best, mut samples)| {
                let dir = dir.clone();
                async move {
                    while let Some(outcome) = tail.next().await {
                        if let Some(sample) = outcome.sample() {
                            samples.push(sample);
                        }
                        if let Some(holder) = late_join(outcome, &mut best, warming)
                            .and_then(|join| self.register_late(join))
                        {
                            return Some((holder, (tail, best, samples)));
                        }
                    }
                    if !samples.is_empty() {
                        drop(spawn_harvest(&dir, Vec::new(), samples));
                    }
                    None
                }
            },
        )))
    }

    fn connect<'b>(
        &'b self,
        holder: &'b Holder,
    ) -> SourceFuture<'b, StreamCandidate<Self::Source>> {
        Box::pin(self.build(holder))
    }

    fn on_source_fault(&self, holder: &Holder) {
        let Some(node_id) = self.lock_nodes().get(&holder.provider).map(|n| n.node_id) else {
            return;
        };
        self.file(PeerEvent::Failure {
            node_id,
            at_secs: now_secs(),
        });
    }
}

/// One peer-store record a fetch files about a node.
#[derive(Debug, Clone, Copy)]
enum PeerEvent {
    /// The node answered a lane's first open at `rate_per_mb`.
    Open {
        node_id: iroh::PublicKey,
        rate_per_mb: u64,
    },
    /// The node faulted a lane at `at_secs`.
    Failure {
        node_id: iroh::PublicKey,
        at_secs: u64,
    },
}

impl PeerEvent {
    /// Write this record to `store`. A refused write is logged at debug: the
    /// peer store is a hint for the next run's selection, not this fetch's.
    fn write(self, store: &crate::PeerStore) {
        match self {
            Self::Open {
                node_id,
                rate_per_mb,
            } => {
                if let Err(e) = store.record_open(&node_id, rate_per_mb) {
                    tracing::debug!(
                        %node_id,
                        "could not record a first open in the peer store: {e:#}"
                    );
                }
            }
            Self::Failure { node_id, at_secs } => {
                if let Err(e) = store.record_failure(&node_id, at_secs) {
                    tracing::debug!(
                        %node_id,
                        "could not record a source fault in the peer store: {e:#}"
                    );
                }
            }
        }
    }
}

/// A late probe answer that joins the running fetch.
pub(crate) enum LateJoin {
    /// A verified holder.
    Holder(crate::discovery::Probed),
    /// A verified non-holder the warming rule picks as a proxy.
    Proxy(crate::discovery::WarmingCandidate),
}

/// Whether a late probe `outcome` joins the running fetch. A verified holder
/// always joins and lowers `best_holder_rtt_ms`. A verified non-holder joins
/// as a proxy when the warming rule
/// ([`crate::discovery::proxy_warming_order`]) picks it against the best
/// holder known so far. Nothing else joins.
pub(crate) fn late_join(
    outcome: ProbeOutcome,
    best_holder_rtt_ms: &mut f64,
    warming: ProxyWarmingParams,
) -> Option<LateJoin> {
    match outcome {
        ProbeOutcome::Holder(holder, _) => {
            *best_holder_rtt_ms = best_holder_rtt_ms.min(holder.rtt_ms);
            Some(LateJoin::Holder(holder))
        }
        ProbeOutcome::NonHolder(candidate, _) => {
            let picked = warming.enabled
                && !crate::discovery::proxy_warming_order(
                    *best_holder_rtt_ms,
                    warming.rtt_threshold_ms,
                    warming.margin_ms,
                    std::slice::from_ref(&candidate),
                )
                .is_empty();
            picked.then_some(LateJoin::Proxy(candidate))
        }
        ProbeOutcome::Unreachable
        | ProbeOutcome::RateLimited
        | ProbeOutcome::Unverifiable
        | ProbeOutcome::Unusable => None,
    }
}

/// The [`Holder`] for a late `join`, its node indexed in `nodes` for
/// `connect`, or `None` when `nodes` already has a node of the join's
/// operator: one node per operator, and the node the set already has answered
/// first, as in [`holders_from`]. A holder carries its probed coverage; a
/// warming proxy carries none and is not a probed holder.
pub(crate) fn admit_late(
    nodes: &mut HashMap<Address, NodeCandidate>,
    join: LateJoin,
) -> Option<Holder> {
    let (candidate, holder) = match join {
        LateJoin::Holder(probed) => {
            let holder = Holder {
                provider: probed.candidate.eth_address,
                coverage: Some(probed.coverage),
                rtt_ms: probed.rtt_ms,
                probed_holder: true,
            };
            (probed.candidate, holder)
        }
        LateJoin::Proxy(proxy) => {
            let holder = Holder {
                provider: proxy.eth_address,
                coverage: None,
                rtt_ms: proxy.rtt_ms,
                probed_holder: false,
            };
            let candidate = NodeCandidate {
                node_id: proxy.node_id,
                eth_address: proxy.eth_address,
                region_hint: None,
                multiaddrs: proxy.multiaddrs,
            };
            (candidate, holder)
        }
    };
    if nodes.contains_key(&candidate.eth_address) {
        return None;
    }
    nodes.insert(candidate.eth_address, candidate);
    Some(holder)
}

/// The holders `targets` names, in its order, one per operator: a candidate
/// whose on-chain provider already has a holder is skipped, so the nearer
/// node by rank wins (the rule of [`crate::discovery::admit_sources`]).
/// Each holder carries its probed coverage (`None`, a full holder, when
/// nothing was measured) and its probed RTT, or its rank in the order when it
/// was not probed. A candidate with measured coverage, and a pinned
/// `--node-id`, is a probed holder ([`Holder::probed_holder`]); a
/// pull-through or proxy-warming target and a peer-store fast-path candidate
/// are not. Each holder's node goes into `nodes`, keyed by provider, and
/// replaces the node a rediscovery found for that operator before, so the next
/// lane build dials the node the latest probe ranked. A lane already built
/// keeps its node.
pub(crate) fn holders_from(
    targets: &ResolvedTargets,
    nodes: &mut HashMap<Address, NodeCandidate>,
) -> Vec<Holder> {
    let rtt: HashMap<_, f64> = targets
        .probed_samples
        .iter()
        .map(|&(node_id, rtt_ms, _)| (node_id, rtt_ms))
        .collect();
    let mut holders: Vec<Holder> = Vec::with_capacity(targets.candidates.len());
    for (rank, candidate) in targets.candidates.iter().enumerate() {
        if holders.iter().any(|h| h.provider == candidate.eth_address) {
            continue;
        }
        let rtt_ms = rtt
            .get(&candidate.node_id)
            .copied()
            .unwrap_or_else(|| f64::from(u32::try_from(rank).unwrap_or(u32::MAX)));
        let coverage = targets.coverage_by_node.get(&candidate.node_id).cloned();
        holders.push(Holder {
            provider: candidate.eth_address,
            probed_holder: targets.pinned || coverage.is_some(),
            coverage,
            rtt_ms,
        });
        nodes.insert(candidate.eth_address, candidate.clone());
    }
    holders
}

#[cfg(test)]
mod tests;
