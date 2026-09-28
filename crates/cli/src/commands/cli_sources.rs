//! The CLI's [`SourceProvider`]: where `decdn fetch` and `bundle pull` find a
//! blob's holders and build their paid lanes.
//!
//! Discovery is the CLI's own ([`fetch::resolve_target_node`]); a lane is one
//! provider's pool lane ([`fetch::build_multi_lane`]). The provider records
//! each built lane's watermark handle, so the command persists what every lane
//! paid once the fetch ends, and files each delivery fault in the peer store
//! the next run's fast path reads.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use decdn_client::discovery::NodeCandidate;
use decdn_client::source::SourceFuture;
use decdn_client::{
    Holder, LaneLease, LaneLedgers, NoAffordableSource, PeerHealth, PeerSource, PoolContext,
    SourceProvider, SourceSet, StopPolicy, StreamCandidate, first_open,
};
use decdn_common::cli;
use decdn_incentive::{CapabilityGrant, PoolId};
use iroh::RelayUrl;

use super::bundle_pull::LaneStreamCap;
use super::fetch::{self, CliFunder, DriveFetchDeps, FaceLaneHandle, FunderPool, ResolvedTargets};

/// The [`SourceProvider`] a CLI fetch runs its acquire loop over.
pub(crate) struct CliSources<'a, P> {
    deps: &'a DriveFetchDeps<'a, P>,
    common: &'a cli::ClientFetchArgs,
    relays: &'a [RelayUrl],
    grant: Option<&'a CapabilityGrant>,
    signer: &'a Arc<PrivateKeySigner>,
    voucher_dom: &'a Eip712Domain,
    /// Serializes each lane's pool open-or-reuse against sibling fetches on
    /// the same pool (`bundle pull`); `None` for a solo fetch.
    open_lock: Option<&'a tokio::sync::Mutex<()>>,
    /// The run's shared voucher-ledger registry (`bundle pull`); `None` for a
    /// solo fetch.
    ledgers: Option<&'a LaneLedgers>,
    /// The run's per-provider stream cap (`bundle pull`): a lane holds one of
    /// its provider's permits while the fetch runs.
    lane_cap: Option<&'a LaneStreamCap>,
    /// Every holder's node, keyed by its on-chain provider address.
    nodes: Mutex<HashMap<Address, NodeCandidate>>,
    peer_store: decdn_client::PeerStore,
    /// Every built lane's watermark handle, in build order.
    handles: Mutex<Vec<FaceLaneHandle>>,
    /// Lanes built for the first open, handed to the fetch's first `connect`
    /// for their provider.
    parked: Mutex<HashMap<Address, StreamCandidate<PeerSource<'a>>>>,
    /// The pool the first built lane pays from.
    pool_id: OnceLock<PoolId>,
    /// Set while [`Self::signed_size`] runs the first open.
    sizing: AtomicBool,
}

impl<'a, P> CliSources<'a, P>
where
    P: alloy::providers::Provider + Clone,
{
    /// A provider over the fetch's shared `deps`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        deps: &'a DriveFetchDeps<'a, P>,
        common: &'a cli::ClientFetchArgs,
        relays: &'a [RelayUrl],
        grant: Option<&'a CapabilityGrant>,
        signer: &'a Arc<PrivateKeySigner>,
        voucher_dom: &'a Eip712Domain,
        open_lock: Option<&'a tokio::sync::Mutex<()>>,
        ledgers: Option<&'a LaneLedgers>,
        lane_cap: Option<&'a LaneStreamCap>,
    ) -> Self {
        Self {
            deps,
            common,
            relays,
            grant,
            signer,
            voucher_dom,
            open_lock,
            ledgers,
            lane_cap,
            nodes: Mutex::new(HashMap::new()),
            peer_store: decdn_client::PeerStore::open(&deps.chain.data_dir),
            handles: Mutex::new(Vec::new()),
            parked: Mutex::new(HashMap::new()),
            pool_id: OnceLock::new(),
            sizing: AtomicBool::new(false),
        }
    }

    /// The holders `targets` names, indexing each one's node for `connect`.
    pub(crate) fn holders_from(&self, targets: &ResolvedTargets) -> Vec<Holder> {
        holders_from(targets, &mut self.lock_nodes())
    }

    /// The funder a fetch over these sources tops the pool up through: the
    /// pool the first built lane pays from.
    pub(crate) const fn funder(&self) -> CliFunder<'_, P> {
        CliFunder {
            contract: self.deps.contract,
            rpc: self.deps.rpc,
            store: self.deps.store,
            owner: self.deps.self_address,
            pool_id: FunderPool::FirstLane(&self.pool_id),
            token: self.deps.token,
            payment_pool_addr: self.deps.chain.payment_pool,
            max_approve: self.deps.chain.max_approve,
        }
    }

    /// The blob's signed size, from a header-only open of one of `holders`
    /// ([`first_open`]), with the acquire loop's recovery: a holder that
    /// faults cools and another is tried until one answers, a fault only the
    /// user can fix ends it, and `stop` gives up. The open's pull is dropped
    /// once its header is read. Every lane the open built is parked for the
    /// fetch's `connect`, so the fetch reuses it.
    ///
    /// # Errors
    ///
    /// Any error [`first_open`] returns.
    pub(crate) async fn signed_size(
        &self,
        hash: [u8; 32],
        holders: Vec<Holder>,
        health: &Arc<PeerHealth>,
        stop: &StopPolicy,
    ) -> anyhow::Result<u64> {
        let mut set = SourceSet::new(self, hash, Arc::clone(health), holders);
        self.sizing.store(true, Ordering::Release);
        let opened = first_open(&mut set, stop, |lane| async move {
            let (header, _whole) = lane.source.open_whole(hash).await?;
            let provider = lane
                .ctx
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .provider;
            self.record_open(provider, header.rate_per_mb);
            Ok(header.total_bytes)
        })
        .await;
        self.sizing.store(false, Ordering::Release);
        let mut parked = self.parked.lock().unwrap_or_else(PoisonError::into_inner);
        for (provider, lane) in set.take_lanes() {
            if let Ok(lane) = Arc::try_unwrap(lane) {
                parked.insert(provider, lane);
            }
        }
        drop(parked);
        opened.map(|(_, total_bytes)| total_bytes)
    }

    /// Persist every built lane's voucher watermark.
    pub(crate) fn persist_watermarks(&self) {
        let handles = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        fetch::persist_face_watermarks(self.deps.store, self.deps.self_address, &handles);
    }

    /// Reconnect `err` to what the user can do about it: an unbound or
    /// underfunded cache miss ([`fetch::annotate_unbound_cache_miss`]), and a
    /// pool no provider's voucher fits — the owner-side remedy on a delegated
    /// pool ([`fetch::annotate_delegated_exhaustion`]), else the top-up
    /// command.
    pub(crate) fn annotate(&self, err: anyhow::Error) -> anyhow::Error {
        let first_ctx = self
            .handles
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .first()
            .map(|h| Arc::clone(&h.ctx));
        let err = match first_ctx {
            Some(ctx) => annotate_with(err, &ctx),
            None => err,
        };
        if self.grant.is_some() {
            return fetch::annotate_delegated_exhaustion(err);
        }
        if err.downcast_ref::<NoAffordableSource>().is_some() {
            let pool = self
                .pool_id
                .get()
                .map_or_else(|| "<pool id>".to_string(), ToString::to_string);
            return err.context(format!(
                "top up with: decdn pool top-up --pool {pool} --amount-micro-usdc <MICRO_USDC>"
            ));
        }
        err
    }

    /// File a lane's first answer in the peer store: its quoted rate, with the
    /// failure stamp cleared.
    fn record_open(&self, provider: Address, rate_per_mb: u64) {
        let Some(node_id) = self.lock_nodes().get(&provider).map(|n| n.node_id) else {
            return;
        };
        if let Err(e) = self.peer_store.record_open(&node_id, rate_per_mb) {
            tracing::debug!(%node_id, "could not record a first open in the peer store: {e:#}");
        }
    }

    fn lock_nodes(&self) -> std::sync::MutexGuard<'_, HashMap<Address, NodeCandidate>> {
        self.nodes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `holder`'s lane: the one the first open parked, or a fresh build. When
    /// the run caps streams per provider, a lane the fetch takes holds one of
    /// its provider's permits; a lane built for the first open holds none, so
    /// that open never waits on a permit while it holds another.
    async fn build(&self, holder: &Holder) -> anyhow::Result<StreamCandidate<PeerSource<'a>>> {
        let parked = self
            .parked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&holder.provider);
        let mut lane = match parked {
            Some(lane) => lane,
            None => self.build_fresh(holder).await?,
        };
        if let Some(cap) = self.lane_cap
            && !self.sizing.load(Ordering::Acquire)
        {
            lane.lease = LaneLease::new(cap.permit(holder.provider).await?);
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
        let target = fetch::lane_target(&candidate, self.common.addr, self.relays);
        let lane = fetch::build_multi_lane(
            self.deps,
            self.grant,
            self.signer,
            self.voucher_dom,
            holder.provider,
            target,
            self.open_lock,
            self.ledgers,
        )
        .await?;
        let _ = self.pool_id.set(lane.pool_id);
        self.handles
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(FaceLaneHandle {
                pool_id: lane.pool_id,
                provider: lane.provider,
                prior_amount: lane.prior_amount,
                ledger: Arc::clone(&lane.ledger),
                ctx: Arc::clone(&lane.ctx),
            });
        Ok(StreamCandidate {
            source: lane.source,
            ctx: lane.ctx,
            ledger: lane.ledger,
            coverage: holder.coverage.clone(),
            first_unit: None,
            lease: LaneLease::default(),
        })
    }
}

/// [`fetch::annotate_unbound_cache_miss`] against the lane context `ctx`.
fn annotate_with(err: anyhow::Error, ctx: &Mutex<PoolContext>) -> anyhow::Error {
    match ctx.lock() {
        Ok(guard) => fetch::annotate_unbound_cache_miss(err, &guard),
        Err(_) => err,
    }
}

impl<'a, P> SourceProvider for CliSources<'a, P>
where
    P: alloy::providers::Provider + Clone,
{
    type Source = PeerSource<'a>;

    fn discover(&self, hash: [u8; 32]) -> SourceFuture<'_, Vec<Holder>> {
        Box::pin(async move {
            // A fresh look: skip the peer store's fast path.
            let mut args = self.common.clone();
            args.rediscover = true;
            let targets = fetch::resolve_target_node(
                &args,
                self.deps.chain,
                self.deps.endpoint,
                self.relays,
                hash,
            )
            .await?;
            Ok(self.holders_from(&targets))
        })
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
        if let Err(e) = self
            .peer_store
            .record_failure(&node_id, fetch::now_secs_cli())
        {
            tracing::debug!(%node_id, "could not record a source fault in the peer store: {e:#}");
        }
    }
}

/// The holders `targets` names, in its order, one per on-chain provider: each
/// holder's probed coverage (`None`, a full holder, when nothing was measured)
/// and its probed RTT, or its rank in the order when it was not probed. Each
/// holder's node goes into `nodes`, keyed by provider; the first node named for
/// a provider wins.
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
        holders.push(Holder {
            provider: candidate.eth_address,
            coverage: targets.coverage_by_node.get(&candidate.node_id).cloned(),
            rtt_ms,
        });
        nodes
            .entry(candidate.eth_address)
            .or_insert_with(|| candidate.clone());
    }
    holders
}

#[cfg(test)]
mod tests {
    use super::holders_from;

    #[test]
    fn holders_carry_coverage_and_rtt_and_index_their_nodes() {
        let (targets, a, b) = crate::commands::fetch::tests_support::two_holder_targets();
        let mut nodes = std::collections::HashMap::new();
        let holders = holders_from(&targets, &mut nodes);
        assert_eq!(holders.len(), 2);
        assert!(
            holders
                .iter()
                .any(|h| h.provider == a.eth_address && h.rtt_ms > 0.0)
        );
        assert!(nodes.contains_key(&b.eth_address));
        let a_holder = holders
            .iter()
            .find(|h| h.provider == a.eth_address)
            .map(|h| h.coverage.clone());
        assert_eq!(
            a_holder,
            Some(targets.coverage_by_node.get(&a.node_id).cloned()),
            "a probed holder carries its measured coverage"
        );
    }
}
