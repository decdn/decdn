//! The [`SourceProvider`] a paid fetch runs its acquire loop over: where
//! a blob `decdn get` and every entry of a bundle pull find a blob's holders and
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
use decdn_bao_range::AlignedRange;
use decdn_incentive::{LaneKey, PoolId};
use iroh::RelayUrl;
use tokio_util::sync::CancellationToken;

use crate::buyer_pool::WatermarkRow;
use crate::discovery::NodeCandidate;
use crate::driver::streamed_first_leg;
use crate::funding::PoolFunder;
use crate::lanes::{
    FaceLaneHandle, LaneDeps, LanePayer, build_multi_lane, lane_target, queue_face_watermarks,
};
use crate::session::{DiscoveryOptions, ProxyWarmingParams};
use crate::sink::PullReader;
use crate::source::{PrimedSource, SourceFuture, SourceStream};
use crate::stream_cap::LaneStreamCap;
use crate::streamer::stream_pacer;
use crate::targets::{
    LateProbes, ProbeOpts, ProbeOutcome, ProbeRound, ResolvedTargets, now_secs, pinned_targets,
    resolve_target_node, spawn_harvest,
};
use crate::{
    CredentialSlot, Holder, LaneLease, LaneLedgers, PeerHealth, PeerSource, PoolContext,
    RecoveryGate, SourceProvider, SourceSet, StopPolicy, StreamCandidate, UpstreamPullHeader,
    first_open, is_range_past_end,
};

/// The first leg the drive after a [`LaneSources::first_claim`] opens, so the
/// claim's size open pulls that leg and the drive adopts the pull.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirstLeg {
    /// A download under a [`crate::BudgetPacer`]: the whole blob.
    Whole,
    /// A streamed drive ([`crate::Streamer`]) of a byte window under its
    /// read-ahead: its first leg is cut to the read-ahead window.
    Streamed {
        /// The byte window, `(offset, len)`; a `len` of `0` runs to the end.
        window: (u64, u64),
        /// The streamer's read-ahead ([`crate::PullConfig::read_ahead_bytes`]).
        read_ahead: u64,
    },
}

/// A size no blob reaches, for the leg a streamed size open pulls before it
/// knows the size: a leg that ends inside the blob ends at the same byte for
/// every larger size, and a node clamps a pull that runs past the end.
const UNBOUNDED_SIZE: u64 = 1 << 62;

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
/// learns from the header of a whole-blob open. A hint needs no open.
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

/// The span a size open of `first_leg` pulls for `hash`, or `None` when the
/// size comes from a whole open: a download, the empty blob, or a window that
/// holds no byte.
///
/// A streamed drive's first leg is cut to its read-ahead window
/// ([`streamed_first_leg`]). Its end does not depend on the size while the
/// blob runs past it, so the span is the leg of an unbounded blob. A node
/// clamps a pull that runs past the end, so on a shorter blob the pull is the
/// clamped leg. A holder serves the empty blob only as a whole, and refuses
/// any other range of it.
pub(crate) fn size_open_span(
    hash: [u8; 32],
    first_leg: FirstLeg,
    max_blob_bytes: u64,
) -> Option<AlignedRange> {
    let FirstLeg::Streamed { window, read_ahead } = first_leg else {
        return None;
    };
    if hash == decdn_protocol::EMPTY_ROOT {
        return None;
    }
    streamed_first_leg(
        &stream_pacer(read_ahead),
        window,
        max_blob_bytes,
        UNBOUNDED_SIZE,
    )
}

/// The range the streamed drive's first leg asks for, when the pull of `span`
/// at the signed `total_bytes` is that leg; `None` otherwise.
///
/// The streamer sizes its store at the size the cap on held bytes lets the
/// window reach ([`crate::scheduler::claim_reach`]), and the leg's range
/// carries that size, so the range is built against it. The pull is the leg
/// only when the node served exactly the leg's span: the span clamped to the
/// signed size.
pub(crate) fn primed_first_leg(
    first_leg: FirstLeg,
    max_blob_bytes: u64,
    span: &AlignedRange,
    total_bytes: u64,
) -> Option<AlignedRange> {
    let FirstLeg::Streamed { window, read_ahead } = first_leg else {
        return None;
    };
    let store_bytes = total_bytes.min(crate::scheduler::claim_reach(&[window], max_blob_bytes));
    streamed_first_leg(
        &stream_pacer(read_ahead),
        window,
        max_blob_bytes,
        store_bytes,
    )
    .filter(|leg| {
        leg.fetch_start() == span.fetch_start()
            && leg.fetch_end() == span.fetch_end().min(total_bytes)
    })
}

/// Open the whole blob for its signed header: a download's first leg, primed
/// at `align_range(0, 0, n)`, which holds for every `n`.
///
/// # Errors
///
/// The open's fault.
async fn open_whole_primed(
    source: &PeerSource<'_>,
    hash: [u8; 32],
) -> anyhow::Result<(UpstreamPullHeader, PullReader, Option<AlignedRange>)> {
    let (header, whole) = source.open_whole(hash).await?;
    let range = decdn_bao_range::align_range(0, 0, header.total_bytes).ok();
    Ok((header, whole, range))
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
    /// What every lane signs under: the owned payer's lane key, or the
    /// delegated slot it reads at each lane build.
    payer: LanePayer<'a>,
    voucher_dom: &'a Eip712Domain,
    /// The slot generation whose key's earlier spend was last recorded in the
    /// slot ([`Self::seed_spend`]); `u64::MAX` before the first record.
    seeded: AtomicU64,
    /// Serializes each lane's pool open-or-reuse: across one fetch's
    /// concurrently built lanes, which all open or reuse the one pool, and
    /// across sibling fetches on that pool (a bundle pull).
    open_lock: Option<&'a tokio::sync::Mutex<()>>,
    /// The run's shared voucher-ledger registry (a bundle pull); `None` for a
    /// solo fetch.
    ledgers: Option<&'a LaneLedgers>,
    /// The run's per-provider stream cap (a bundle pull): a lane, the first
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
    /// start of its provider ([`SourceProvider::built`]), or to the first
    /// `connect` for it.
    parked: Mutex<HashMap<Address, StreamCandidate<PrimedSource<PeerSource<'a>>>>>,
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
            // The owner key of an owned run, read from the payer the lanes sign
            // under. A delegated run has nothing to fund, and names its key.
            owner: match self.payer {
                LanePayer::Owned(payer) => payer.owner().address(),
                LanePayer::Delegated(_) => self.deps.self_address,
            },
            deployment: self.deps.chain.deployment(),
            token: self.deps.token,
            payment_pool_addr: self.deps.chain.payment_pool,
            working_deposit: self.deps.deposit.working_deposit,
            max_approve: self.deps.deposit.max_approve,
            open: self.deps.deposit.open,
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

    /// Record in `slot` what earlier fetches signed under its current key: the
    /// delegate lanes this data dir records for the key in the capability's
    /// pool on the fetch's deployment, at every provider. Read once per slot
    /// generation, so a swapped-in key is counted from its own records; a
    /// failed read is tried again at the next lane build.
    ///
    /// A lane the run's registry already holds counts only the prior it was
    /// built with. The run's spend past that prior is counted from the
    /// registry ([`LaneLedgers::signed_by`]), and a sibling entry of a bundle
    /// may already have written it to the store, so the record would count it
    /// twice.
    fn seed_spend(&self, slot: &CredentialSlot) {
        let generation = slot.generation();
        if self.seeded.load(Ordering::Relaxed) == generation {
            return;
        }
        let signer = slot.current().signer.address();
        let pool_id = slot.pool_id();
        let deployment = self.deps.chain.deployment();
        let lanes = match self.deps.store.delegate_lanes(deployment, pool_id, signer) {
            Ok(lanes) => lanes,
            Err(error) => {
                // The headroom reads high until a read succeeds, so a spent cap
                // can read as the publisher's pool needing funds.
                tracing::warn!(
                    %pool_id,
                    %signer,
                    chain_id = deployment.chain_id,
                    payment_pool = %deployment.payment_pool,
                    %error,
                    "could not read this key's delegate lanes from the buyer store; its \
                     earlier spend is not counted against the capability until a read succeeds"
                );
                return;
            }
        };
        let recorded = lanes.iter().fold(
            alloy::primitives::U256::ZERO,
            |acc, (provider, progress)| {
                let lane = LaneKey {
                    pool_id,
                    signer,
                    provider: *provider,
                };
                let earlier = self
                    .ledgers
                    .and_then(|ledgers| ledgers.prior_amount(lane))
                    .unwrap_or(progress.last_amount);
                acc.saturating_add(earlier)
            },
        );
        slot.record_spend(signer, recorded);
        self.seeded.store(generation, Ordering::Relaxed);
    }

    /// The fetch's funding recovery gate.
    pub const fn recovery(&self) -> &Arc<RecoveryGate> {
        self.recovery
    }

    /// The fetch's first size claim (#2218): the probe's size hint when a
    /// probe gave one ([`Self::holders_from`]), with no open at all. Without a
    /// hint (the peer-store fast path, or a pinned `--node-id` whose probe gave
    /// no size), an open of `first_leg` on one of `holders` learns the size one
    /// holder signs from its header ([`first_open`]), with the acquire loop's
    /// recovery: a holder that faults cools and another is tried until one
    /// answers, a fault only the user can fix ends it, and `stop` gives up.
    /// The open's pull is primed on the lane for its next open alone
    /// ([`PrimedSource::prime_next`]) once its header is read: the drive's
    /// first leg on the lane is the range the open pulled
    /// ([`FirstLeg`]), so it adopts the pull within two seconds of the
    /// open, and any other first leg closes it before opening its own, so the
    /// node never holds the floor it reserved for the pull beside the leg's.
    /// A lane that opens nothing closes it when it drops. The lane that
    /// answered is parked for the fetch, which starts it with no build
    /// ([`SourceProvider::built`]) before any other lane builds, so the
    /// fetch plans the first leg for that lane alone; every other lane the
    /// open built drops, and with it any stream permit it held. Returns the claim
    /// and every holder known, those the open's discovery found included.
    ///
    /// # Errors
    ///
    /// Any error [`first_open`] returns.
    pub async fn first_claim(
        &self,
        hash: [u8; 32],
        holders: Vec<Holder>,
        first_leg: FirstLeg,
        health: &Arc<PeerHealth>,
        stop: &StopPolicy,
    ) -> anyhow::Result<FirstClaim> {
        let hint = *self
            .size_hint
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        first_claim_or_open(hint, holders, |holders| {
            self.open_for_header(hash, holders, first_leg, health, stop)
        })
        .await
    }

    /// The size one of `holders` signs, from the header of an open of
    /// `first_leg` whose pull stays primed on its lane ([`Self::first_claim`]).
    async fn open_for_header(
        &self,
        hash: [u8; 32],
        holders: Vec<Holder>,
        first_leg: FirstLeg,
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
                let opened_at = tokio::time::Instant::now();
                let (header, pull, range) = self
                    .open_first_leg(lane.source.inner(), hash, first_leg)
                    .await?;
                let provider = lane
                    .ctx
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .provider;
                self.record_open(provider, header.rate_per_mb);
                let total_bytes = header.total_bytes;
                // The node has already written into the pull, so it is parked
                // for the drive's first leg to adopt instead of dropped unread;
                // another first leg closes it first. A pull with no range to
                // prime closes here, and the drive opens its own.
                if let Some(range) = range {
                    lane.source.prime_next(hash, range, header, pull, opened_at);
                } else {
                    tracing::debug!(
                        total_bytes,
                        "the size open's pull is not the drive's first leg; not primed"
                    );
                    drop((header, pull));
                }
                Ok(total_bytes)
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

    /// Open `first_leg` of `hash` on `source` for its signed header. Returns the
    /// header, the pull, and the range the drive's first leg asks for when the
    /// pull is that leg.
    ///
    /// The span comes from [`size_open_span`], and the range to prime from
    /// [`primed_first_leg`]. A window that starts past the end serves nothing
    /// ([`is_range_past_end`]), and the size comes from a whole open instead.
    ///
    /// # Errors
    ///
    /// The open's fault.
    async fn open_first_leg(
        &self,
        source: &PeerSource<'a>,
        hash: [u8; 32],
        first_leg: FirstLeg,
    ) -> anyhow::Result<(UpstreamPullHeader, PullReader, Option<AlignedRange>)> {
        let max_blob_bytes = self.deps.max_blob_bytes;
        let Some(span) = size_open_span(hash, first_leg, max_blob_bytes) else {
            return open_whole_primed(source, hash).await;
        };
        let (header, pull) = match source
            .open_span(hash, span.fetch_start(), span.fetch_len())
            .await
        {
            Ok(opened) => opened,
            Err(e) if is_range_past_end(&e) => return open_whole_primed(source, hash).await,
            Err(e) => return Err(e),
        };
        let range = primed_first_leg(first_leg, max_blob_bytes, &span, header.total_bytes);
        Ok((header, PullReader::new(pull), range))
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

    /// The buyer-store record every lane's watermark goes to: the owner's
    /// pool row, or the delegate lanes on the fetch's deployment.
    fn watermark_row(&self) -> WatermarkRow {
        match self.payer {
            LanePayer::Owned(payer) => WatermarkRow::Owned(payer.owner().address()),
            LanePayer::Delegated(_) => WatermarkRow::Delegated(self.deps.chain.deployment()),
        }
    }

    /// Read every built lane's watermark from its ledger now, and queue the
    /// writes in that order ([`queue_face_watermarks`]).
    fn queue_watermarks(&self) -> tokio::sync::oneshot::Receiver<()> {
        let handles = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        queue_face_watermarks(
            self.deps.writes,
            self.deps.store,
            self.watermark_row(),
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

    /// Take `provider`'s lane that the first open parked, if it is still
    /// parked. The first open built it through [`Self::build`], so it already
    /// holds what `build` gives every lane.
    fn take_parked(
        &self,
        provider: Address,
    ) -> Option<StreamCandidate<PrimedSource<PeerSource<'a>>>> {
        self.parked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&provider)
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
    async fn build(
        &self,
        holder: &Holder,
        ended: &CancellationToken,
    ) -> anyhow::Result<StreamCandidate<PrimedSource<PeerSource<'a>>>> {
        let mut lane = if let Some(lane) = self.take_parked(holder.provider) {
            lane
        } else {
            let lease = lane_lease(self.lane_cap, holder.provider).await?;
            let mut lane = self.build_fresh(holder, ended).await?;
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

    /// Build `holder`'s lane and record its watermark handle. `ended` fires
    /// once the fetch the lane serves has ended ([`SourceProvider::connect`]).
    async fn build_fresh(
        &self,
        holder: &Holder,
        ended: &CancellationToken,
    ) -> anyhow::Result<StreamCandidate<PrimedSource<PeerSource<'a>>>> {
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
            ended,
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
        let source = PrimedSource::new(lane.source);
        let mut candidate = StreamCandidate::new(source, lane.ctx, lane.ledger);
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
    type Source = PrimedSource<PeerSource<'a>>;

    fn discover(&self, hash: [u8; 32]) -> SourceFuture<'_, Vec<Holder>> {
        Box::pin(async move {
            // A pinned node is the only holder a rediscovery can find, and only
            // the first size claim reads a probe's hint, so it probes nothing.
            if let Some(pinned) = self.discovery.pinned {
                return Ok(self.holders_from(&pinned_targets(pinned.node_id, pinned.provider)));
            }
            // A fresh look: skip the peer store's fast path. A rediscovery
            // settles its round and drops the tail.
            let mut opts = self.discovery.clone();
            opts.rediscover = true;
            let probe = ProbeOpts {
                marks: None,
                round: ProbeRound::Settle,
                known_size: None,
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
        ended: &'b CancellationToken,
    ) -> SourceFuture<'b, StreamCandidate<Self::Source>> {
        Box::pin(self.build(holder, ended))
    }

    /// The lane the first open parked for `provider`: the acquire loop
    /// starts it before any other lane builds, so the drive plans its first
    /// leg for it alone and that leg adopts the primed pull.
    fn built(&self, provider: Address) -> Option<StreamCandidate<Self::Source>> {
        self.take_parked(provider)
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
        | ProbeOutcome::ClockSkewed
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
