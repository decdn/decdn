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

/// A fetch's first size claim ([`CliSources::first_claim`]): a hint the fetch
/// grows or shrinks as verified bytes land.
pub(crate) struct FirstClaim {
    /// The claimed size.
    pub(crate) total_bytes: u64,
    /// Every holder known when the claim was made.
    pub(crate) holders: Vec<Holder>,
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

/// The [`SourceProvider`] a CLI fetch runs its acquire loop over.
pub(crate) struct CliSources<'a, P> {
    deps: &'a DriveFetchDeps<'a, P>,
    common: &'a cli::ClientFetchArgs,
    relays: &'a [RelayUrl],
    grant: Option<&'a CapabilityGrant>,
    signer: &'a Arc<PrivateKeySigner>,
    voucher_dom: &'a Eip712Domain,
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
    /// The size hint of the last probe that gave one
    /// ([`ResolvedTargets::size_hint`]): the fetch's first claim.
    size_hint: Mutex<Option<u64>>,
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
            size_hint: Mutex::new(None),
        }
    }

    /// The holders `targets` names, indexing each one's node for `connect`.
    /// A probe's size hint is kept for [`Self::first_claim`].
    pub(crate) fn holders_from(&self, targets: &ResolvedTargets) -> Vec<Holder> {
        if let Some(hint) = targets.size_hint {
            *self
                .size_hint
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(hint);
        }
        holders_from(targets, &mut self.lock_nodes())
    }

    /// The funder a fetch over these sources tops the pool up through: the
    /// pool the first built lane pays from.
    pub(crate) fn funder(&self) -> CliFunder<'_, P> {
        CliFunder {
            contract: self.deps.contract,
            rpc: self.deps.rpc,
            store: self.deps.store,
            owner: self.deps.self_address,
            pool_id: &self.pool_id,
            token: self.deps.token,
            payment_pool_addr: self.deps.chain.payment_pool,
            max_approve: self.deps.chain.max_approve,
            funding: self.deps.funding,
        }
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
    pub(crate) async fn first_claim(
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
        let holders = set.holders().to_vec();
        opened.map(|(_signer, total_bytes)| FirstClaim {
            total_bytes,
            holders,
        })
    }

    /// Persist every built lane's voucher watermark, and return once the
    /// writes land ([`fetch::queue_face_watermarks`]).
    pub(crate) async fn persist_watermarks(&self) {
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
    pub(crate) fn persist_watermarks_detached(&self) {
        drop(self.queue_watermarks());
    }

    /// Read every built lane's watermark from its ledger now, and queue the
    /// writes in that order ([`fetch::queue_face_watermarks`]).
    fn queue_watermarks(&self) -> tokio::sync::oneshot::Receiver<()> {
        let handles = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        fetch::queue_face_watermarks(
            self.deps.writes,
            self.deps.store,
            self.deps.self_address,
            &handles,
        )
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
    /// extra stream leaves the last free one for a sibling entry
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
            lease: LaneLease::default(),
            widen: None,
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
                None,
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
        self.file(PeerEvent::Failure {
            node_id,
            at_secs: fetch::now_secs_cli(),
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
    fn write(self, store: &decdn_client::PeerStore) {
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

/// The holders `targets` names, in its order, one per operator: a candidate
/// whose on-chain provider already has a holder is skipped, so the nearer
/// node by rank wins (the rule of [`decdn_client::discovery::admit_sources`]).
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
mod tests {
    use super::{PeerEvent, holders_from, lane_lease, with_top_up_hint};
    use crate::commands::bundle_pull::LaneStreamCap;
    use alloy::primitives::{Address, B256, U256};
    use decdn_client::NoAffordableSource;

    /// Two entries of one command file records for the same node through the
    /// command's ordered writes: entry A's first open, then entry B's fault.
    /// The fault stays stamped, even with both queued behind a slow write.
    #[tokio::test]
    async fn an_open_and_a_later_fault_from_two_entries_land_in_order() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        decdn_client::PeerStore::open(dir.path()).upsert_identity(
            &decdn_client::discovery::NodeCandidate {
                node_id,
                eth_address: Address::repeat_byte(3),
                region_hint: None,
                multiaddrs: alloy::primitives::Bytes::new(),
            },
            1_000,
        )?;
        let writes = crate::commands::ordered_writes::OrderedWrites::default();
        let (release, hold) = std::sync::mpsc::channel::<()>();
        writes.queue(move || {
            let _ = hold.recv();
        });
        // Each entry opens its own store handle, as each entry's sources do.
        let entry_a = decdn_client::PeerStore::open(dir.path());
        let entry_b = decdn_client::PeerStore::open(dir.path());
        writes.queue(move || {
            PeerEvent::Open {
                node_id,
                rate_per_mb: 4,
            }
            .write(&entry_a);
        });
        writes.queue(move || {
            PeerEvent::Failure {
                node_id,
                at_secs: 2_000,
            }
            .write(&entry_b);
        });
        release.send(())?;
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            writes.queue_awaitable(|| ()),
        )
        .await??;
        let landed = decdn_client::PeerStore::open(dir.path())
            .get(&node_id)
            .ok_or_else(|| anyhow::anyhow!("the record is missing"))?;
        assert_eq!(landed.rate_per_mb, Some(4));
        assert_eq!(landed.last_failure_at_secs, Some(2_000));
        Ok(())
    }

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
            size_hint: None,
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

    /// A rediscovery that ranks another node of an operator first replaces
    /// the node the operator's next lane build dials.
    #[test]
    fn a_rediscovery_refreshes_an_operators_node() {
        let node = |seed: u8| decdn_client::discovery::NodeCandidate {
            node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            eth_address: Address::repeat_byte(0xAA),
            region_hint: None,
            multiaddrs: alloy::primitives::Bytes::new(),
        };
        let targets =
            |n: decdn_client::discovery::NodeCandidate| crate::commands::fetch::ResolvedTargets {
                candidates: vec![n],
                coverage_by_node: std::collections::HashMap::new(),
                probed_samples: Vec::new(),
                pinned: false,
                size_hint: None,
            };
        let mut nodes = std::collections::HashMap::new();
        holders_from(&targets(node(1)), &mut nodes);
        holders_from(&targets(node(2)), &mut nodes);
        assert_eq!(
            nodes.get(&Address::repeat_byte(0xAA)).map(|n| n.node_id),
            Some(node(2).node_id)
        );
    }

    /// The first claim of `targets`, counting the header-only opens it makes.
    async fn claim_of(
        targets: &crate::commands::fetch::ResolvedTargets,
    ) -> anyhow::Result<(u64, usize)> {
        let opens = std::sync::atomic::AtomicUsize::new(0);
        let mut nodes = std::collections::HashMap::new();
        let holders = holders_from(targets, &mut nodes);
        let claim = super::first_claim_or_open(targets.size_hint, holders, |holders| {
            opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                Ok(super::FirstClaim {
                    total_bytes: 7,
                    holders,
                })
            }
        })
        .await?;
        Ok((
            claim.total_bytes,
            opens.load(std::sync::atomic::Ordering::SeqCst),
        ))
    }

    /// Holders whose probe gave a size hint take it as the first claim, with
    /// no whole-blob open for the header (#2218).
    #[tokio::test]
    async fn the_first_claim_comes_from_the_probe_hint_without_a_whole_blob_open()
    -> anyhow::Result<()> {
        let (targets, _, _) = crate::commands::fetch::tests_support::two_holder_targets();
        let hint = targets
            .size_hint
            .ok_or_else(|| anyhow::anyhow!("the fixture's probe gives a hint"))?;
        assert_eq!(claim_of(&targets).await?, (hint, 0));
        Ok(())
    }

    /// A pinned `--node-id` was never probed, so it has no hint and opens one
    /// pull for the header's size.
    #[tokio::test]
    async fn a_pinned_node_without_a_hint_opens_for_the_header() -> anyhow::Result<()> {
        let (mut targets, _, _) = crate::commands::fetch::tests_support::two_holder_targets();
        targets.pinned = true;
        targets.size_hint = None;
        assert_eq!(claim_of(&targets).await?, (7, 1));
        Ok(())
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
