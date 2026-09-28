//! The CLI's [`SourceProvider`]: where `decdn fetch` and `bundle pull` find a
//! blob's holders and build their paid lanes.
//!
//! Discovery is the CLI's own ([`fetch::resolve_target_node`]); a lane is one
//! provider's pool lane ([`fetch::build_multi_lane`]). The provider records
//! each built lane's watermark handle, so the command persists what every lane
//! paid once the fetch ends, and files each delivery fault in the peer store
//! the next run's fast path reads.

use std::collections::HashMap;
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
use super::fetch::{self, CliFunder, DriveFetchDeps, FaceLaneHandle, ResolvedTargets};

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
    /// The run's per-provider stream cap (`bundle pull`): a lane, the first
    /// open's included, holds one of its provider's permits while the fetch
    /// runs.
    lane_cap: Option<&'a LaneStreamCap>,
    /// Every holder's node, keyed by its on-chain provider address.
    nodes: Mutex<HashMap<Address, NodeCandidate>>,
    peer_store: decdn_client::PeerStore,
    /// Every built lane's watermark handle, in build order.
    handles: Mutex<Vec<FaceLaneHandle>>,
    /// The lane that answered the first open, handed to the fetch's first
    /// `connect` for its provider.
    parked: Mutex<HashMap<Address, StreamCandidate<PeerSource<'a>>>>,
    /// The pool the first built lane pays from.
    pool_id: OnceLock<PoolId>,
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
            pool_id: &self.pool_id,
            token: self.deps.token,
            payment_pool_addr: self.deps.chain.payment_pool,
            max_approve: self.deps.chain.max_approve,
        }
    }

    /// The blob's signed size, from a header-only open of one of `holders`
    /// ([`first_open`]), with the acquire loop's recovery: a holder that
    /// faults cools and another is tried until one answers, a fault only the
    /// user can fix ends it, and `stop` gives up. The open's pull is dropped
    /// once its header is read. The lane that answered is parked for the
    /// fetch's `connect`, so the fetch reuses it; every other lane the open
    /// built drops, and with it any stream permit it held. Returns the size
    /// and every holder the open knows, those its discovery found included.
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
    ) -> anyhow::Result<(u64, Vec<Holder>)> {
        let mut set = SourceSet::new(self, hash, Arc::clone(health), holders);
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
        let answered = opened.as_ref().ok().map(|&(provider, _)| provider);
        if let Some((provider, lane)) = keep_answering(set.take_lanes(), answered) {
            self.parked
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(provider, lane);
        }
        let known = set.holders().to_vec();
        opened.map(|(_, total_bytes)| (total_bytes, known))
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
        with_top_up_hint(err, self.pool_id.get().copied())
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

    /// `holder`'s lane: the one the first open parked, with the permit it
    /// already holds, or a fresh build. When the run caps streams per
    /// provider, a fresh lane holds one of its provider's permits
    /// ([`lane_lease`]), taken before the build and never waited for.
    async fn build(&self, holder: &Holder) -> anyhow::Result<StreamCandidate<PeerSource<'a>>> {
        let parked = self
            .parked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&holder.provider);
        if let Some(lane) = parked {
            return Ok(lane);
        }
        let lease = lane_lease(self.lane_cap, holder.provider).await?;
        let mut lane = self.build_fresh(holder).await?;
        lane.lease = lease;
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

/// Name the top-up command on a self-owned pool no provider's voucher fits.
fn with_top_up_hint(err: anyhow::Error, pool_id: Option<PoolId>) -> anyhow::Error {
    if err.downcast_ref::<NoAffordableSource>().is_none() {
        return err;
    }
    let pool = pool_id.map_or_else(|| "<pool id>".to_string(), |id| id.to_string());
    err.context(format!(
        "top up with: decdn pool top-up --pool {pool} --amount-micro-usdc <MICRO_USDC>"
    ))
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

/// The holders `targets` names, in its order, one per operator: a candidate
/// whose on-chain provider already has a holder is skipped, so the nearer
/// node by rank wins (the rule of [`decdn_client::discovery::admit_sources`]).
/// Each holder carries its probed coverage (`None`, a full holder, when
/// nothing was measured) and its probed RTT, or its rank in the order when it
/// was not probed. A candidate with measured coverage, and a pinned
/// `--node-id`, is a probed holder ([`Holder::probed_holder`]); a
/// pull-through or proxy-warming target and a peer-store fast-path candidate
/// are not. Each holder's node goes into `nodes`, keyed by provider.
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
        nodes
            .entry(candidate.eth_address)
            .or_insert_with(|| candidate.clone());
    }
    holders
}

#[cfg(test)]
mod tests {
    use super::{holders_from, lane_lease, with_top_up_hint};
    use crate::commands::bundle_pull::LaneStreamCap;
    use alloy::primitives::{Address, B256, U256};
    use decdn_client::NoAffordableSource;

    /// Two entries with crossed providers under a cap of one stream each: X
    /// holds A and wants B while Y holds B and wants A. Neither waits: each
    /// build fails at once and backs off holding nothing new, and once X
    /// finishes and frees A, Y takes it.
    #[tokio::test]
    async fn crossed_entries_never_wait_for_a_lane_permit() -> anyhow::Result<()> {
        let (a, b) = (Address::repeat_byte(0xA1), Address::repeat_byte(0xB2));
        let cap = LaneStreamCap::new(1);
        let x_holds_a = lane_lease(Some(&cap), a).await?;
        let y_holds_b = lane_lease(Some(&cap), b).await?;

        let crossed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            (
                lane_lease(Some(&cap), b).await,
                lane_lease(Some(&cap), a).await,
            )
        })
        .await
        .map_err(|_| anyhow::anyhow!("a lane build waited for a permit"))?;
        assert!(
            crossed.0.is_err() && crossed.1.is_err(),
            "both permits are held"
        );

        drop(x_holds_a);
        let y_takes_a = lane_lease(Some(&cap), a).await?;
        drop((y_takes_a, y_holds_b));
        assert!(lane_lease(None, a).await.is_ok(), "no cap, no permit");
        Ok(())
    }

    /// Only the lane that answered the first open is kept; every other lane's
    /// lease drops, so its provider's permit frees for sibling entries.
    #[tokio::test]
    async fn only_the_answering_lane_keeps_its_permit() -> anyhow::Result<()> {
        let (a, b) = (Address::repeat_byte(0xA1), Address::repeat_byte(0xB2));
        let cap = LaneStreamCap::new(1);
        let lanes = vec![
            (a, std::sync::Arc::new(lane_lease(Some(&cap), a).await?)),
            (b, std::sync::Arc::new(lane_lease(Some(&cap), b).await?)),
        ];
        let kept = super::keep_answering(lanes, Some(b));
        assert_eq!(kept.as_ref().map(|(p, _)| *p), Some(b));
        assert!(
            lane_lease(Some(&cap), a).await.is_ok(),
            "A's permit is free"
        );
        assert!(
            lane_lease(Some(&cap), b).await.is_err(),
            "B's lane holds its own"
        );
        drop(kept);
        assert!(super::keep_answering::<()>(Vec::new(), None).is_none());
        Ok(())
    }

    /// A self-owned pool no provider's voucher fits names the top-up command
    /// with the pool id; any other error passes through.
    #[test]
    fn a_self_owned_pool_no_voucher_fits_names_the_top_up_command() {
        let pool = B256::repeat_byte(7);
        let err = anyhow::Error::new(NoAffordableSource {
            deposit: U256::from(5u32),
        });
        let hinted = format!("{:#}", with_top_up_hint(err, Some(pool)));
        assert!(
            hinted.contains(&format!(
                "decdn pool top-up --pool {pool} --amount-micro-usdc"
            )),
            "{hinted}"
        );
        let other = with_top_up_hint(anyhow::anyhow!("reset"), Some(pool));
        assert_eq!(other.to_string(), "reset");
    }

    /// Two nodes of one operator yield one holder: the nearer one by rank.
    #[test]
    fn one_operator_yields_one_holder_the_nearer_by_rank() {
        let node = |seed: u8| decdn_client::discovery::NodeCandidate {
            node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            eth_address: Address::repeat_byte(0xAA),
            region_hint: None,
            multiaddrs: alloy::primitives::Bytes::new(),
        };
        let (near, far) = (node(1), node(2));
        let targets = crate::commands::fetch::ResolvedTargets {
            candidates: vec![near.clone(), far],
            coverage_by_node: std::collections::HashMap::new(),
            probed_samples: Vec::new(),
            pinned: false,
        };
        let mut nodes = std::collections::HashMap::new();
        let holders = holders_from(&targets, &mut nodes);
        assert_eq!(holders.len(), 1, "one holder per operator");
        assert!(
            holders.iter().all(|h| !h.probed_holder),
            "no probe reported the blob"
        );
        assert_eq!(
            nodes.get(&Address::repeat_byte(0xAA)).map(|n| n.node_id),
            Some(near.node_id),
            "the nearer node by rank is the operator's holder"
        );
    }

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
        assert!(holders.iter().all(|h| h.probed_holder));
    }

    /// A pinned `--node-id` counts as a holder: its `NotFound` never marks it
    /// absent.
    #[test]
    fn a_pinned_node_is_a_probed_holder() {
        let (mut targets, _, _) = crate::commands::fetch::tests_support::two_holder_targets();
        targets.coverage_by_node.clear();
        let mut nodes = std::collections::HashMap::new();
        assert!(
            holders_from(&targets, &mut nodes)
                .iter()
                .all(|h| !h.probed_holder)
        );
        targets.pinned = true;
        assert!(
            holders_from(&targets, &mut nodes)
                .iter()
                .all(|h| h.probed_holder)
        );
    }
}
